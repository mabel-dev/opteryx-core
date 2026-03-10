# Carchar
## Draken-Adjacent Execution Engine Design for Fast Joins and Aggregations

## Purpose

Carchar is not a generic hash map.

Carchar is a disposable execution primitive for two operator families:

1. hash join
2. grouped aggregation

Carchar is also part of Opteryx's removal of Arrow from the internal execution engine.

That means the primary target is Draken-native execution state:

1. Draken morsels as the data being indexed for joins
2. Draken-native aggregate state for grouped aggregation
3. Arrow-anchored internals treated as transitional compatibility paths, not the design center

The requirement is not "implement a better map". The requirement is:

1. build very quickly
2. probe or update very quickly
3. keep the hot working set small
4. batch work aggressively
5. throw the structure away after the operator finishes

This document rewrites the design around that requirement.

---

## Problem

Opteryx currently has hash-heavy operator paths, but the implementation boundary is still too low-level and still carries Arrow-era assumptions:

1. joins use a generic-style `hash -> row list` structure
2. grouped aggregation wants `hash -> group state`
3. the current thinking is still centered on "what hash table should we use?"
4. too much of the current shape is influenced by transitional Arrow-facing execution paths rather than the engine Opteryx is moving toward

That is the wrong level of abstraction.

The engine already knows several things a generic map does not:

1. keys are already hashed into canonical `uint64` values
2. work arrives in batches, not as random single-key operations
3. the structure is build-use-burn
4. deletes are unnecessary
5. reordering within a batch may be acceptable if it improves locality
6. join and aggregation have different payload behavior even when their key index is similar
7. the long-term engine representation is Draken-native, not Arrow-native

The design should exploit those facts directly.

---

## Design Summary

Carchar v2 is a three-stage execution engine:

1. partition
2. build
3. seal and probe or update

At a high level:

```text
input batch
  -> radix partition by hash bits
  -> partition-local build state
  -> sealed partition-local probe layout
  -> batch probe / batch aggregate update
  -> discard
```

This is intentionally a staged engine design, not a single in-memory container design.

---

## Core Assumptions

These assumptions are design constraints, not implementation details.

1. The 64-bit hash is the key.
2. Upstream hashing is already specialized and remains outside Carchar.
3. Carchar does no hashing of source values.
4. Carchar does not support deletes.
5. Carchar instances are cheap and disposable.
6. The fast path is batch-based, not scalar.
7. Build and probe are different phases and should use different layouts if that is faster.

Implication:

The right question is not "what is the best general hash map layout?".

The right question is "what sequence of layouts and passes gives the fastest join and group-by operator?".

---

## Goals

1. Beat the current Abseil-backed operator paths on join build and probe throughput.
2. Reuse the same top-level engine model for grouped aggregation.
3. Keep the key index as small and cache-friendly as possible.
4. Separate hot metadata from colder payload state.
5. Make batch locality a first-class optimization target.
6. Support partition-local memory accounting and future spill.
7. Integrate with the existing SIMD dispatch pattern already used in Opteryx.
8. Align with Draken-native morsel/state layouts rather than preserving Arrow-era internal boundaries.
9. Help remove Arrow from the internal engine hot path.

---

## Lessons From The Current Prototype

The current prototype has already taught us something important:

1. build throughput can beat the current Abseil-backed path by a wide margin
2. probe throughput is still too flat across workloads
3. the flatness is a warning sign that the inner loop is still behaving too much like a scalar probe engine

Observed probe behavior has the shape of:

1. scalar control scanning
2. per-slot or per-candidate branching
3. key comparison happening too early
4. dispatch and layout decisions that are still too close to the inner loop

That matters because it means the remaining problem is not "just tune a few constants".
The remaining problem is that the probe path is still not operating at the right unit of work.

The practical lessons so far are:

1. batch build is already good enough to justify the engine direction
2. the default probe path should remain on the fastest real kernel, even if that means a logical seal instead of a physical one
3. a sealed layout is only valid if it is measurably faster than the mutable-build-path probe it replaces
4. partitioning is not automatically a win; too many partitions can add overhead faster than they remove it
5. the next wins will come from a better probe engine, not from more generic hash-table polish

---

## Non-Goals

1. Generic dictionary semantics
2. Deletes or tombstone-heavy reuse
3. Long-lived mutable structures
4. Python-facing container APIs
5. Exact source-key revalidation after hash match
6. Preserving input order inside internal operator phases if a faster equivalent ordering exists

---

## Execution Model

### Stage 1: Partition

Input keys are partitioned by radix bits from the canonical `uint64` hash.

Recommended initial design:

1. choose partition bits from the upper hash bits
2. scatter keys and associated row ids or row references into partition-local buffers
3. process partitions independently

Why partition first:

1. smaller local working sets
2. better cache behavior
3. fewer long probe walks
4. easier parallelism later
5. easier partition-local spilling later
6. cheaper sealing because each local structure is smaller

This is different from buckets.

Radix partitioning is a top-level execution strategy.
Buckets are the internal layout of each partition-local table.

They are complementary.

In the target engine, the partition input is expected to be Draken-native batch state, not Arrow row materialization.

### Stage 2: Build

Each partition is built using a write-optimized local structure.

The build structure does not need to be the final probe structure.

Build mode should optimize for:

1. append-heavy insertion
2. minimal allocation churn
3. compact payload handle creation
4. low branch cost

### Stage 3: Seal

Once build is complete, each partition is converted into a read-optimized structure.

This is the key design change.

Abseil has to be good while the table is still mutable.
Carchar does not.

Sealing should:

1. compact metadata
2. finalize bucket layout
3. pack payload handles densely
4. freeze probe-relevant state
5. eliminate build-only bookkeeping

Revision from prototype results:

1. seal is a performance technique, not a mandatory ceremony
2. if a physical sealed layout is slower than the build-path probe kernel, Carchar should perform a logical seal and keep the faster structure
3. "sealed" means "no further mutation", not necessarily "copied into a second table"
4. the engine should permit `logical-seal` and `physical-seal` modes and choose based on measured benefit

### Stage 4: Probe or Aggregate Update

After sealing:

1. probe batches are partitioned by the same radix bits
2. work is executed partition-by-partition
3. probing and aggregation are batch operations, not scalar map lookups

---

## Operator Modes

### Join Mode

Join mode maps:

```text
hash -> build-side payload handle
```

The payload handle resolves to Draken-native build-side row information or morsel-local row references.

Join-specific behavior:

1. duplicate build keys are expected
2. one-to-one and one-to-few must be cheap
3. result materialization should happen after lookup, not during lookup

### Aggregation Mode

Aggregation mode maps:

```text
hash -> group state handle
```

The payload handle resolves to Draken-native fixed aggregate state and optional output-key materialization state.

Aggregation-specific behavior:

1. one key corresponds to one mutable aggregate state
2. updates should be in-place
3. the hot path should not touch wide variable-length output state

---

## Data Layout

The design uses two physical layers.

### 1. Hot Index

Per partition, the hot index contains:

1. control bytes or fingerprints
2. lane-local payload handles
3. optionally full keys or key verification data if required by the exact lane layout

The hot index must stay narrow.

This is the structure touched on almost every probe.

### 2. Cold Payload Storage

The payload storage is separate and append-only.

Join payloads and aggregate payloads have different layouts, but both should be arena-backed and disposable.

Where possible, payload handles should resolve directly into Draken-owned state or Draken-adjacent arenas, rather than inventing new Arrow-shaped intermediate structures.

The hot index should not touch arena-heavy state unless a candidate match has already survived the cheap filter stages.

---

## Bucketed Partition-Local Table

The current open-addressed single-global-table design should be replaced with fixed-size buckets inside each partition.

Recommended starting point:

1. 16 lanes per bucket
2. SIMD-friendly control-byte layout
3. compact handle array
4. overflow to next bucket group only when necessary

Why buckets:

1. one cache line or a few adjacent lines contain an entire decision unit
2. SIMD control compare becomes natural
3. sequential bucket stepping is easier to prefetch
4. batch probing can group by bucket more easily than by raw slot position

The important point is not the exact lane count.
The important point is that the unit of work becomes "probe one bucket", not "walk one slot at a time through a global table".

Revision from prototype results:

The first bucketed sealed prototype did not improve probe speed.

That does not invalidate bucketing as a direction, but it does sharpen the requirement:

1. bucketing must reduce inner-loop branching, not just change memory layout
2. control scanning must stay bucket-local and mostly branchless
3. candidate elimination must happen before full key compare whenever possible
4. a bucket design that still behaves like scalar lane-by-lane probing is not good enough

---

## Payload Design

### Join Payloads

Join payloads should optimize for the common duplicate patterns.

Recommended layout:

1. inline single-row case
2. inline double-row case
3. optional small fixed inline array for a few more rows
4. overflow blocks only for larger duplicate chains

Example logical payload classes:

1. `single`
2. `double`
3. `small-inline`
4. `overflow-chain`

This keeps the probe-count and small-match path out of dynamic allocation territory.

### Aggregate Payloads

Aggregate payloads should be fixed-width whenever possible.

Recommended layout:

1. payload handle indexes a dense aggregate state slab
2. aggregate states are operator-specific and tightly packed
3. output-key materialization is separate from hot aggregate state when practical

This keeps grouped aggregation from paying row-list costs it does not need.

---

## Batch-First API

The scalar interface is only for testing and debugging.

The real API should be batch-oriented.

Recommended primitives:

```text
partition_batch(keys, row_refs, partition_bits)
build_partition_join(partition_keys, partition_rows)
build_partition_groups(partition_keys, partition_rows_or_values)
seal_partition()
probe_partition_batch(partition_probe_keys) -> handles or counts
update_partition_batch(partition_probe_keys, aggregate_inputs)
```

The intended caller for these APIs is a Draken-adjacent execution path operating on morsels and typed state arrays, not a Python-facing or Arrow-row-facing layer.

Join wrappers:

```text
build_join_state(build_hashes, build_row_ids)
probe_join_state(probe_hashes) -> payload_handles
materialize_join_matches(payload_handles, probe_rows)
```

Aggregation wrappers:

```text
build_group_state(group_hashes, aggregate_inputs)
update_group_state(group_hashes, aggregate_inputs)
finalize_group_state() -> output
```

The key point is that Carchar should expose engine operations, not `dict` operations.

---

## Ordering and Locality

Batch order is a performance tool.

After partitioning, Carchar should be free to reorder locally if it preserves operator semantics.

Recommended ordering strategy:

1. first scatter by partition
2. optionally scatter again by bucket id inside each partition
3. then probe or update in that clustered order

Why:

1. better bucket reuse
2. more effective prefetch
3. lower cache miss rate
4. better batching of payload-handle fetches

This does not require a full sort.
A cheap counting or radix scatter is sufficient.

---

## SIMD Strategy

SIMD should operate at the bucket level, not as a tiny helper inside a scalar probe loop.

Required pattern:

1. select the probe kernel with the existing `simd::select_dispatch` machinery
2. dispatch once per operator or per partition operation
3. run a whole bucket probe kernel, not a per-byte helper

Desired ISA strategy:

1. AVX2: full 16-lane bucket scan
2. NEON: full 16-lane bucket scan or equivalent prefilter plus fast candidate reduction
3. scalar: efficient fallback for unsupported platforms

Important rule:

Do not put the function-pointer dispatch boundary inside the tight inner lane loop.

Additional rule from prototype results:

Do not hide a scalar algorithm inside SIMD-looking code.

In practice, the probe kernel must avoid:

1. scanning control bytes and then re-entering a lane-by-lane scalar branch tree
2. performing full key equality checks too early
3. paying per-candidate control flow that flattens throughput across workloads

The target probe structure is:

1. SIMD compare of the entire bucket control/fingerprint area
2. mask reduction to candidate lanes and empty lanes
3. cheap reject of most non-matching buckets
4. full key compare only for the surviving candidate lanes

If a SIMD path does not preserve that shape, it is not solving the real problem.

---

## Partition Count

Partition count should be an execution tuning knob, not a fixed structural rule.

Initial recommendation:

1. start with a small set of tested partition counts such as `256`, `1024`, `4096`
2. choose from row count and estimated distinct count
3. tune separately for join-heavy and group-by-heavy workloads

Why not hard-code `8192` immediately:

1. too many tiny partitions waste overhead on smaller inputs
2. partition count should reflect actual data scale and cardinality
3. good partitioning is workload-sensitive

The design should allow the engine to choose, not force one count globally.

Revision from prototype results:

1. the current benchmark favors very low partition counts, including a single-partition default
2. partitioning should therefore be adaptive, not ideological
3. the engine should start from "no partitioning unless it helps" rather than assuming many partitions are inherently faster
4. partitioning should be turned up when it reduces working-set pressure enough to pay for its own scatter/gather overhead

---

## Memory Model

Memory accounting must be partition-local and phase-aware.

Track at least:

1. partition buffers before build
2. build-state key index bytes
3. sealed key index bytes
4. join payload bytes
5. aggregate state bytes
6. optional output-key materialization bytes
7. temporary reorder buffers

This is needed for:

1. realistic benchmarking
2. future spill decisions
3. deciding when to seal or repartition

---

## Null Semantics

### Join

Join mode inherits current join behavior:

1. rows with null join keys are removed before Carchar sees them
2. Carchar does not represent null join rows internally

### Group By

Group-by mode accepts already-hashed null-bearing keys.

The grouping contract remains:

1. the upstream hash is the key
2. equal hashes belong to the same group

No secondary equality check is added in the hot path.

---

## Build and Seal Details

### Build State

Build state should be optimized for insertion rate, not final probe speed.

Likely ingredients:

1. append-only payload arenas
2. local bucket occupancy tracking
3. minimal metadata needed to produce a sealed partition

### Sealed State

Sealed state should be optimized for:

1. bucket scan speed
2. payload handle density
3. batch probe/update locality
4. SIMD friendliness

The current design should explicitly allow different layouts for build and sealed phases.

That is a primary optimization lever, not an implementation detail.

That distinction also helps decouple Carchar from Arrow-era internal data flow:

1. build can consume Draken-native morsel slices
2. seal can emit a probe-optimized engine layout with no Arrow dependency
3. probe and aggregate update can remain fully inside compiled engine structures

Prototype lesson:

different layouts are allowed, but not required.

The real invariant is:

1. build and probe may use different layouts
2. they should only diverge when the probe layout is actually faster
3. an inferior physical sealed layout must not replace a superior mutable probe path just because the architecture diagram says "seal"

---

## Join-Specific Fast Path

Join probing should separate three costs:

1. find matching payload handle
2. inspect duplicate count
3. materialize result rows

Only step 1 belongs in the hottest bucket probe path.

Step 2 should touch a compact count or inline payload record.

Step 3 should happen later, after candidate resolution.

This matters because current benchmarks can easily conflate:

1. lookup speed
2. duplicate accounting
3. row-list materialization

Another lesson from current measurements:

Even when materialization is removed, the remaining probe curve can still stay unnaturally flat.

That usually means the hot path is still dominated by:

1. control-flow overhead
2. repeated branchy candidate handling
3. full-key checks happening earlier than they should

So the next join fast path should explicitly target:

1. branch reduction
2. later key verification
3. multi-key batch probe scheduling
4. better reuse for repeated or clustered probe keys

The engine design should keep those stages distinct.

---

## Group-By-Specific Fast Path

Grouped aggregation should split:

1. find or create group handle
2. update aggregate state
3. materialize output keys and finalize output

Only steps 1 and 2 belong in the hot execution path.

Output-key materialization should remain cold unless a specific query needs it early.

---

## Concurrency

Initial design target:

1. one writer per partition-local state
2. no fine-grained shared-map synchronization

Parallelism should come from:

1. partition parallelism
2. worker-local build states
3. merge or finalize above the local partition layer

Do not pay concurrency overhead inside the local structure until a real execution path requires it.

---

## Integration Plan

### Phase 1

Introduce the batch partitioner and partition-local join build path in a Draken-adjacent execution boundary.

Scope:

1. join build side only
2. existing upstream hashing stays unchanged
3. keep Arrow-facing compatibility only at the outer operator edge where still required
4. keep the internal Carchar state and payload model Arrow-free

Success criterion:

join build throughput clearly beats the current Abseil path, and probe remains on the fastest available kernel.

### Phase 2

Introduce an optional probe-specialized mode for joins.

Scope:

1. build layout and probe layout may diverge
2. batch probe becomes the primary interface
3. bucket ordering and prefetch become available
4. Draken-native internal execution becomes the expected path, not a later refinement

Success criterion:

the probe-specialized path is measurably faster than the mutable-build-path probe.

### Phase 3

Use the same engine model for grouped aggregation.

Scope:

1. partition-local group state
2. aggregate update kernels behind payload handles
3. optional output-key materialization state
4. direct fit with Draken group-by execution state

Success criterion:

group-by state creation and update outperforms the current prototype path.

---

## Benchmarking Requirements

Benchmark the operator, not the container.

Required measurements:

1. join build rows/sec
2. join probe rows/sec
3. aggregation update rows/sec
4. matched rows/sec
5. groups created/sec
6. bytes per build-side row
7. bytes per created group
8. cache-miss profile if available
9. cycles per row if available
10. probe throughput shape across duplication/cardinality regimes, not just a single average

Required workload axes:

1. high duplication
2. medium duplication
3. high cardinality
4. small payloads
5. large payloads
6. null-heavy group-by inputs
7. ClickBench-like join and aggregation patterns

Benchmark variants should include:

1. current Abseil-backed join path
2. mutable Carchar build layout
3. sealed Carchar probe layout
4. grouped aggregation state path once implemented

---

## Risks

1. A partitioning scheme that is too fine-grained can lose to overhead.
2. A sealing phase that is too expensive can erase probe gains.
3. Join payload materialization can dominate if not kept out of the probe path.
4. Group-by output-key storage can become the true memory bottleneck.
5. SIMD code can regress if the dispatch boundary is placed too low in the hot path.

---

## Recommended Implementation Boundary

The implementation should mirror the execution stages:

1. partitioner
2. partition-local build state
3. sealed partition-local probe state
4. join payload arena
5. aggregate state payload arena
6. batch wrappers for join and group-by operators

These boundaries should be placed in the compiled engine near Draken-native morsel and state handling, not around Arrow compatibility layers.

That is a better boundary than:

1. generic index class
2. generic payload class
3. generic map-like wrapper

The engine should be organized around operator phases, not around generic container concepts.

---

## Final Position

Carchar should now be treated as a disposable operator engine:

1. partition the work
2. build local state
3. seal for fast read/update
4. process in batches
5. discard

The implementation should stop optimizing the current flat-table design as if the goal were "slightly faster hashmap probing".

The design goal is faster joins and faster grouped aggregation in Opteryx.

It is also a step in removing Arrow from the internal engine hot path.

That means the right architecture is:

1. batch-first
2. partition-first
3. bucketed local tables
4. build layout distinct from probe layout
5. hot key index separated from colder payload state

If Carchar follows that design, it has a real chance to outrun Abseil on Opteryx workloads rather than merely approximate it.
