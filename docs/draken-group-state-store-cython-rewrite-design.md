# Draken Group State Store Cython Rewrite Design

## Purpose

This rewrite exists to make grouped aggregation a real engine primitive.

It must follow the same architectural direction as the rest of the engine:

- Python is the glue, not the motor
- Arrow is an interface, not an engine
- performance matters more than convenience
- failure is better than silent degradation

This also needs to align with the Carchar direction already established in
[carchar-execution-engine-design.md](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/docs/carchar-execution-engine-design.md):

- Carchar is not a generic hash map
- Carchar is a disposable Draken-adjacent execution primitive
- Carchar was intended to serve both joins and grouped aggregation

So the rewrite should not create an unrelated “group by engine”.

It should create a Cython/Draken aggregation engine whose state index is Carchar-backed.

## Problem

The current grouped aggregation path is still too Python-centric.

Profiling shows the hot costs are:

- `ShuffleGroupByOperationV2._rows_to_vectors()`
- `ShuffleGroupByOperationV2.ingest()`
- Arrow evaluation fallback in `DrakenAggregateAndGroupNode`

The current structure still pays for:

- Python wrapper orchestration in [group_state_store.py](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/operators/group_state_store.py)
- Python row tuples returned by `finalize_rows()`
- Python row-to-column transpose in `_rows_to_vectors()`
- repeated `vector_from_sequence(...)` conversion from Python lists
- generic fallback ingest paths in [group_state_store.pyx](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/compiled/aggregations/group_state_store.pyx) that still use Python dicts, Python tuples, and Python state objects

That means the current backend is only partially compiled.

It is not yet a Draken-native grouped aggregation engine.

## Design Constraints

These come from the engine principles and should be treated as hard constraints:

1. Python must not do hot-path aggregation work.
2. No new NumPy usage.
3. No silent Python fallback implementation for unsupported compiled shapes.
4. Unsupported shapes should fail visibly or stay on an explicit existing fallback path chosen by the planner.
5. No duplicated Python/Cython business logic unless explicitly required.
6. No dynamic dispatch in hot loops.
7. Execution should remain Draken-native internally.
8. Group-by should share the same execution philosophy as Carchar join:
   - build
   - update/probe
   - finalize
   - discard

## Requirement

The requirement is not:

- “make `ShuffleGroupByOperationV2` less slow”

The requirement is:

- blazing fast engine behind grouped aggregation

## Proposed Direction

Replace the current Python-led group state orchestration with a compiled Draken-adjacent aggregation engine built around Carchar.

At a high level:

```text
Draken morsel
  -> optional Draken-native expression projection
  -> Carchar-backed state ingest/update
  -> optional logical seal
  -> direct chunked Draken morsel finalize
  -> discard
```

## Core Rewrite

### New Engine

Introduce a new compiled engine:

- `CarcharGroupStateEngine`

This becomes the design center.

`ShuffleGroupByOperationV2` becomes a very thin façade or disappears entirely.

### Core API

```cython
cdef class CarcharGroupStateEngine:
    cpdef void ingest(self, Morsel morsel)
    cpdef void seal(self)
    cpdef object finalize_morsels(self, Py_ssize_t chunk_size=*)
```

Possible optional APIs:

```cython
cpdef Morsel finalize_one(self)
cpdef object stats(self)
```

The important point is:

- no `finalize_rows()` on the hot path

## Why Carchar

Carchar was already designed to support:

- `hash -> join payload handle`
- `hash -> aggregate state handle`

For group-by, the natural mapping is:

```text
canonical uint64 key hash -> aggregate state index
```

The state index then resolves into typed aggregate state arrays.

That gives grouped aggregation the same benefits as the new join work:

- small hot index
- Draken-adjacent state
- batch operation
- disposable lifecycle

## Internal Layout

Split the engine into:

1. hot key index
2. cold aggregate state

## Current Layout Diagram

```mermaid
flowchart LR
    A[Draken Morsel<br/>group key vectors<br/>aggregate value vectors]
    B[Morsel.hash(group key columns)<br/>canonical uint64 hash per row]
    C[CarcharIndex<br/>hash -> state_index]
    D[Shared Payload Arena<br/>group key storage]
    E[Aggregate State Arrays<br/>count/sum/min/max/avg/seen]
    F[Distinct Side State<br/>FlatHashSet per group/agg when needed]
    G[Finalize<br/>decode payload + read state arrays]
    H[Draken Output Morsels]

    A --> B
    B --> C
    C -->|lookup/insert| D
    C -->|lookup/insert| E
    C -->|lookup/insert| F
    D --> G
    E --> G
    F --> G
    G --> H
```

Current payload record format in the shared key arena:

- fixed-width key part:
  - `1 byte valid flag`
  - `8 bytes normalized fixed-width value`
- variable-length key part:
  - `1 byte valid flag`
  - `varint length`
  - raw bytes

This is intentional:

- fixed-width primitives are stored directly; they do not need `zpp_bits`-style packing
- variable-length values use compact varint lengths instead of the earlier fixed 4-byte prefix
- Carchar remains unchanged and still stores only `hash -> state_index`
- `state_index` points into this payload arena plus the aggregate state arrays

The important boundary is:

- Carchar does **not** store the full group key inline
- Carchar stores `hash -> state_index`
- `state_index` resolves into:
  - the payload arena for key materialization
  - aggregate state arrays
  - distinct side state where required

That is the same architectural pattern as the join work:

- Carchar is the hot index
- operator-specific arenas hold the real payload/state
- Draken vectors feed the engine
- finalize reconstructs Draken-native output from native state

### 1. Hot Key Index

Use Carchar as the group key index:

- key: canonical `uint64` hash
- payload: `state_index`

This is disposable, build-use-burn state.

The key index should stay small and hot.

### 2. Aggregate State Arrays

Store aggregate state in typed side arrays owned by the engine.

Examples:

- `count[]`
- `sum_i64[]`
- `sum_f64[]`
- `min_i64[]`
- `max_i64[]`
- `avg_sum_f64[]`
- `avg_count[]`
- `seen[]`

Distinct operations need side structures, but they should still be compiled structures rather than Python `set` by default.

### 3. Group Key Materialization State

The engine also needs a way to emit the original group key columns during finalize.

Primary design:

1. group key columns are `zpp_bits` encoded into a compact key store
2. Carchar maps canonical `uint64` hash -> `state_index`
3. `state_index` resolves both:
   - aggregate state
   - encoded group-key payload for finalize

This is preferred to per-type variant key handling because it allows:

- arbitrary group key combinations
- one compiled key-store path
- fewer specialized finalize branches
- better locality than Python tuple/object keys

Constraints:

- no Python tuple keys as the primary state representation
- key-store memory must be explicitly bounded
- oversized key stores should fail visibly with a query-too-large style error, not silently degrade

Initial safeguard:

- add a configurable hard limit for encoded key-store bytes
- fail once the store exceeds that limit
- start with an operational default such as `1 GiB`, then tune from real workloads

## Ingest Model

### Batch-Based Updates

Per morsel:

1. get group key columns from Draken vectors
2. hash them into canonical `uint64` keys using specialized compiled hashing
// this should use the existing `Morsel.hash(...)` functionality
3. use Carchar to find or insert `state_index`
4. update typed aggregate state arrays in compiled loops

This should happen entirely in Cython/C++ for supported shapes.

### Kernel Families

Use explicit specialized kernels, not generic dynamic dispatch in hot loops.

Initial kernel families:

- single-key `int64`
- single-key narrow integer
- single-key dictionary
- single-key constant
- multi-aggregate fixed-width

Later:

- multi-key fixed-width
- typed distinct kernels

If a query shape is not supported by the compiled engine, planner/operator selection should choose the fallback path explicitly.

It should not silently degrade inside the engine.

## Finalize Model

### Remove Python Row Materialization

Current slow path:

1. backend builds Python rows
2. Python transposes rows into lists
3. Python constructs vectors from lists

Rewrite target:

1. allocate typed output vectors for one chunk
2. walk state arrays directly
3. write aggregate results and group keys directly into the vectors
4. emit `Morsel.from_vectors(...)`

### Chunked Output

Output should be chunked:

- default `64K` groups per morsel

That keeps:

- memory bounded
- downstream streaming behavior intact
- finalize cost amortized

### Seal

Like Carchar join, grouped aggregation may optionally use a logical or physical seal before finalize.

Seal is not required as a ceremony.

Use it only if it improves:

- finalize locality
- output emission
- memory accounting

## Node Integration

`DrakenAggregateAndGroupNode` should remain Python glue only.

Target flow:

1. planner selects Draken grouped aggregation
2. node performs only plan-level coordination
3. node calls into one compiled engine object
4. at EOS, node yields compiled output morsels

The node should not own:

- row transposition
- aggregate state assembly
- finalize column construction

Those belong in the compiled engine.

## Expression Handling

Expression evaluation is still a separate concern, but the backend decision is now
planner-owned rather than engine-owned.

Current rule:

- if the Draken/Carchar grouped path is selected, unsupported grouped-expression
  shapes fail visibly
- the engine no longer silently falls back to Arrow or the legacy backend at runtime

Long-term:

- keep moving common group-key and aggregate-input expression evaluation into
  Draken-native execution

The important design rules are:

- do not contaminate the new group state engine with Arrow-first assumptions
- do not reintroduce silent runtime backend switching inside the operator

## Migration Plan

## Current Progress

The rewrite is no longer just a design.

There is now a working compiled engine boundary in:

- [carchar_group_state_engine.pyx](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/compiled/aggregations/carchar_group_state_engine.pyx)

And the Python wrapper in:

- [group_state_store.py](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/operators/group_state_store.py)

is now a thin facade over that engine instead of owning the hot-path finalize logic itself.

### What Phase 1 Has Delivered So Far

Implemented:

- compiled `CarcharGroupStateEngine`
- explicit engine modes:
  - uninitialized
  - Carchar-backed typed mode
  - constant-key typed mode
- single-key grouped aggregation for:
  - `Int64Vector`
  - `IntegerVector`
  - `ConstantVector`
  - `DictionaryVector`
  - `StringVector`
- multi-key grouped aggregation for:
  - fixed-width integer-only keys on the compiled Carchar path
  - object-backed keys on the compiled Carchar path
- one-aggregate compiled support for:
  - `COUNT(*)`
  - `COUNT(col)`
  - `SUM`
  - `MIN`
  - `MAX`
  - `AVG`
- one-aggregate compiled `COUNT(DISTINCT)` support for:
  - fixed-width numeric distinct inputs
  - string distinct inputs via hashed value stream
  - dictionary-backed distinct inputs via hashed value stream
- single-key fixed-width multi-aggregate compiled mode
- single-key multi-aggregate compiled mode where one aggregate is `COUNT(DISTINCT)`
- multi-key fixed-width multi-aggregate compiled mode
- multi-key fixed-width single-aggregate compiled mode
- Carchar-backed `hash -> state_index`
- shared native encoded key payload arena for:
  - single-key string and string-dictionary groups
  - mixed multi-key groups containing fixed-width and string-like keys
  - pure fixed-width single-key groups
  - pure fixed-width multi-key groups
  - finalize-time reconstruction of supported key columns without Python key tuples
- direct compiled chunked finalize for supported shapes
- widened Draken-native expression preparation for grouped execution:
  - native non-temporal `BINARY_OPERATOR` handling inside the Draken evaluator boundary
  - native `NodeType.EXPRESSION_LIST` handling for CASE/IIF-style parameter lists
  - native passthrough for result vectors such as `IntegerVector` and `TimeVector`
- grouped-node telemetry for expression preparation:
  - `feature_groupby_draken_eval_native`
- strict no-runtime-fallback behavior:
  - unsupported grouped shapes now fail explicitly instead of switching backend inside the engine
  - unsupported grouped-expression preparation now fails explicitly instead of going through Arrow
- focused Phase-1 correctness coverage in:
  - [test_shuffle_group_by_phase1.py](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/unit/operators/test_shuffle_group_by_phase1.py)

Validated state:

- focused Phase-1 tests are passing
- constant-key grouped aggregation now hits a compiled mode instead of falling back
- the constant-column benchmark shows a very large group-by win on that shape
- dictionary-key grouped `COUNT(*)` is materially faster than materializing keys first
- dictionary-key grouped `COUNT(DISTINCT)` is now materially faster than the materialized-key path with parity holding
- focused multi-aggregate `COUNT(DISTINCT)` coverage is now on the compiled path and passing
- ClickBench grouped query battery is green on the current Draken/Carchar grouped path

### What Is Not Done Yet

Still missing from the original Phase-1 vision:

- unified `zpp_bits` key store for all supported key kinds
- broader exact distinct state strategies if the current hashed-value distinct mode becomes insufficient for some types
- full replacement of the remaining fixed-width key side stores with the planned unified encoded key store
- broader typed finalize paths for object-heavy outputs without `vector_from_sequence(...)`
- broader native grouped-expression coverage for any remaining unsupported query shapes

The important nuance is:

- the implementation has proven the engine boundary and the state-index shape
- but the remaining work is now about de-objectifying the supported multi-key path and broadening native expression coverage, not about proving the engine boundary

### Phase 1

Build the minimum useful Cython/Carchar engine for:

- single group key
- fixed-width key types
- `COUNT(*)`
- `COUNT(col)`
- `SUM`
- `MIN`
- `MAX`
- `AVG`

Output:

- direct chunked Draken `Morsel`

This should replace the current Python `_rows_to_vectors()` path entirely for those shapes.

### Phase 2

Add:

- multi-aggregate kernels
- dictionary-key kernels
- constant-key kernels
- typed distinct kernels

### Phase 3

Add:

- multi-key compiled layouts
- better key storage strategies
- optional seal/finalize-locality improvements

## Lessons From The First Implementation

The first real implementation changed the shape of the plan in a few important ways.

### 1. Narrow Compiled Slices Work Better Than Premature Generalization

The engine moved faster once the implementation focused on:

- one group key
- one aggregate
- fixed-width inputs

instead of trying to land:

- multi-key support
- encoded key store
- distinct
- mixed aggregate sets

all at once.

This should continue to guide the later phases.

### 2. Backend Choice Must Stay Outside The Engine

The compiled engine works best when it does one of two things:

- takes the supported shape completely
- fails explicitly so the planner can choose a different backend up front

The implementation should keep avoiding:

- partial compiled execution with silent Python rescue inside the hot loop
- silent Arrow fallback during grouped expression preparation

This is now an implemented rule for both grouped aggregation and inner join.

### 3. Null And Key-Validity Semantics Are A Real Risk Area

The first remaining fast-path bug was not arithmetic.

It was:

- losing group-key validity state
- which caused correct aggregates to emit null keys

So later phases must assume that:

- null/key validity handling is part of engine design, not just test cleanup

Every new kernel family should land with null-heavy correctness coverage immediately.

### 4. Direct Finalize Is Already The Correct Direction

The compiled finalize path proved the central point of the rewrite:

- direct `Morsel.from_vectors(...)`
- no Python row tuples
- no Python `_rows_to_vectors()` for supported shapes

That should remain the hard rule for every future compiled slice.

### 5. Constant And Simple Key Specializations Are Worth Doing Early

The constant-key slice produced a very large benchmark win quickly.

That is a useful lesson for later phases:

- cheap high-certainty specializations should be taken early
- they are not “premature optimization” if they remove obvious fallback costs

### 6. The Encoded Key Store Was The Right Direction, But It Landed In Stages

The original design correctly identified an encoded key store as the long-term answer for:

- arbitrary keys
- multi-key grouping
- one general finalize path

The implementation showed that Phase 1 did not need to start there.

For the early slices, typed key-state storage is a good temporary strategy because it lets us:

- validate the Carchar state-index model
- validate compiled ingest/finalize
- add kernels incrementally

That said, the engine has now crossed the next step:

- supported string and mixed-key grouped paths use a shared encoded payload arena behind `state_index`
- Carchar still maps `hash -> state_index`
- the arena, not the index, is what changed

So the remaining key-store work is narrower now:

- remove the leftover placeholder fixed-width side columns that are still carried for compatibility/debug paths
- replace the remaining transitional dual-store logic with the payload arena as the only key-store model

The lesson is not "delay the encoded store indefinitely".
The lesson is "land it where it removes real object pressure first".

### 7. `COUNT(DISTINCT)` Needs Its Own State Design

The implementation confirmed that `COUNT(DISTINCT)` should not be treated as a small variant of:

- `COUNT`
- `SUM`
- or generic multi-aggregate state

It needs dedicated per-group distinct state.

The first compiled slice now uses per-group compiled sets:

- direct numeric value identity for fixed-width numeric inputs
- hashed value identity for string and dictionary-backed inputs

The next slice extended that same idea to mixed aggregate sets by adding:

- per-aggregate distinct side state in the flattened multi-aggregate layout
- scalar aggregate state and distinct aggregate state living side by side for the same group

This was the fastest way to land an exact-enough compiled path for current engine semantics, but it also clarified the design:

- distinct state is structurally different from scalar aggregate state
- distinct should remain a dedicated engine concern in later phases
- multi-aggregate support with `COUNT(DISTINCT)` needed explicit layout decisions, not just one more mode flag
- flattened state layouts remain workable, but only if distinct state is added as a separate side structure rather than forced into the scalar arrays

### 8. Dictionary Performance Depends On Avoiding Per-Row Materialization

The dictionary-key and dictionary-distinct work showed that the main win did not come from “support dictionaries” in the abstract.

It came from not materializing Python/string values on every row.

The practical rule for later phases is:

- only materialize dictionary/object keys on insert miss
- use hashed/coded batch paths for the hot ingest loop wherever semantics allow

That applies equally to:

- dictionary group keys
- dictionary distinct inputs
- future encoded-key-store work

### 9. Single-Aggregate And Multi-Aggregate Finalize Paths Must Stay Honest

One of the recent bugs was not in ingest.

It was in finalize:

- the single-aggregate path still assumed numeric key materialization
- while the engine had already started supporting object-backed key sidecars

That is a useful warning for later phases:

- every new ingest capability must be matched by the corresponding finalize path
- single-aggregate and multi-aggregate finalize branches need to stay behaviorally aligned
- object-backed and typed-key-backed output paths should be reviewed together

### 10. Expression Evaluation Coverage Matters More Than The Old `_needs_arrow_eval` Flag Suggested

The recent grouped-node work showed that the expensive part was not just “function
evaluation exists”.

The real issue was that a few specific expression families were still enough to push
the whole grouped node back through Arrow:

- arithmetic `BINARY_OPERATOR` group keys like `ClientIP - 1`
- CASE-style parameter lists represented as `NodeType.EXPRESSION_LIST`

The practical lesson is:

- grouped execution should treat expression preparation as an engine feature with
  explicit coverage and telemetry
- every remaining Arrow-eval escape hatch should now be tracked using:
  - `feature_groupby_draken_eval_native`
  - `feature_groupby_draken_eval_arrow_fallback`

### 11. One-Key Wins Are Real, But ClickBench Movement Now Depends More On Multi-Key Coverage

The one-key engine work was necessary and it is paying off for the supported slices.

But it is no longer enough to expect a major ClickBench shift from more one-key
specializations alone.

Many important ClickBench grouped queries are still shaped around:

- multi-key grouping
- expression keys combined with multi-key grouping
- broader string aggregate combinations

So the next major suite-level win is more likely to come from compiled multi-key
coverage than from widening one-key modes further.

### 12. Multi-Key Support Needed Two Separate Steps

The first useful multi-key path was not the final encoded-key-store design.

It was:

- keep the Carchar `hash -> state_index` model
- remove the unconditional Python tuple/object key path for fixed-width multi-key groups
- store fixed-width multi-key columns as columnar sidecars

That turned out to be the right intermediate step because it:

- cuts the obvious object-key tax on common fixed-width multi-key queries
- keeps finalize columnar
- leaves the encoded key store as a later replacement rather than a prerequisite

## Updated Priorities For The Next Phases

The next implementation order should be:

1. compiled multi-key grouping for fixed-width keys
2. broader native grouped-expression coverage for any remaining unsupported shapes
3. broader string aggregate coverage for real ClickBench query mixes
4. unify the remaining fixed-width key stores into the encoded key arena
5. broader distinct-state strategies if needed beyond the current hashed-value path
6. seal/locality work only after real grouped query profiling justifies it

This is a better order than the original plan because it prioritizes:

- real grouped query shapes
- already-proven high-value compiled slices
- remaining fallback triggers seen in current tests and benchmarks
- widening compiled coverage
- removing actual fallback frequency

before spending more time widening compatibility without reducing the remaining storage split.

## Implementation Plan

This is the concrete implementation order for the rewrite.

### Step 0: Lock The Boundary

Objective:

- stop growing the Python wrapper further

Work:

1. keep [group_state_store.py](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/operators/group_state_store.py) as a façade only
2. do not add new Python fallback logic there
3. treat [group_state_store.pyx](/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/opteryx/compiled/aggregations/group_state_store.pyx) as transitional and scheduled for replacement

Success condition:

- all new engine work lands in compiled modules, not in the Python wrapper

### Step 1: Introduce The New Engine Skeleton

Objective:

- create the new compiled aggregation engine without changing planner behavior yet

Files:

- `opteryx/compiled/aggregations/carchar_group_state_engine.pyx`
- `opteryx/compiled/aggregations/carchar_group_state_engine.pxd`
- optional generated kernel includes under:
  - `opteryx/compiled/aggregations/carchar_group_state_kernels/`

Core API:

```cython
cdef class CarcharGroupStateEngine:
    cpdef void ingest(self, Morsel morsel)
    cpdef void seal(self)
    cpdef object finalize_morsels(self, Py_ssize_t chunk_size=65536)
```

Success condition:

- engine imports and builds
- no planner/node integration yet

### Step 2: Implement The Carchar State Index

Objective:

- use Carchar for `hash -> state_index`

Work:

1. add a group-state-specific Carchar payload mode:
   - canonical key hash
   - `state_index` payload
2. add `find_or_insert_state_index(...)` style API if needed
3. keep this boundary C++/Cython only

Design notes:

- use existing `Morsel.hash(...)` for canonical hash generation
- do not re-hash source values inside the engine

Success condition:

- for a batch of hashed keys, engine can allocate or look up stable `state_index` values

### Step 3: Add Encoded Group-Key Store

Objective:

- materialize output group keys without Python tuples or variant-heavy state

Status:

- partially implemented
- active today for string-like and mixed-key grouped paths
- still incomplete for pure fixed-width key shapes

Work:

1. add a `zpp_bits`-encoded key store owned by the engine
2. on first insert of a new group:
   - encode group key columns
   - append into key store
   - associate encoded-key handle with `state_index`
3. track total key-store bytes
4. enforce hard limit with visible failure

Success condition:

- finalize can reconstruct output group key columns from the encoded store
- no Python tuple keys are needed
- fixed-width and string-like group keys share the same underlying payload arena

### Step 4: Implement Phase-1 Aggregate State Arrays

Objective:

- cover the common hot grouped aggregate shapes with typed compiled state

Status:

- partially done
- one-aggregate state arrays are working for the current fixed-width slice
- multi-aggregate compiled state is still missing

Phase-1 supported functions:

- `COUNT(*)`
- `COUNT(col)`
- `SUM`
- `MIN`
- `MAX`
- `AVG`

Phase-1 supported grouping:

- one group key
- fixed-width or encoded key store-backed key emission

State arrays:

- `count[]`
- `sum_i64[]`
- `sum_f64[]`
- `min_i64[]`
- `max_i64[]`
- `min_f64[]`
- `max_f64[]`
- `avg_sum_f64[]`
- `avg_count[]`
- `seen[]`

Success condition:

- ingest runs without Python dict/tuple state for supported shapes

### Step 5: Implement Batch Ingest Kernels

Objective:

- update state arrays directly from Draken vectors in compiled loops

Kernel families:

1. single-key `int64`
2. single-key narrow integer
3. single-key constant
4. single-key dictionary
5. multi-aggregate fixed-width

Rules:

- explicit specialization only
- no dynamic dispatch inside the row loop
- no silent generic Python fallback in the hot path

Success condition:

- supported group-by shapes stay entirely in compiled ingest

Current status:

- implemented:
  - single-key `Int64Vector`
  - single-key `IntegerVector`
  - single-key `ConstantVector`
- not yet implemented:
  - single-key `DictionaryVector`
  - multi-aggregate fixed-width kernels

### Step 6: Implement Direct Chunked Finalize

Objective:

- delete Python row materialization from the supported hot path

Work:

1. allocate output vectors for one chunk
2. walk `state_index` space directly
3. finalize aggregate state into typed output buffers
4. decode `zpp_bits` key payload into output key vectors
5. emit `Morsel.from_vectors(...)`

Important:

- `finalize_rows()` should not be used for supported shapes
- `_rows_to_vectors()` should not be called

Success condition:

- `finalize_morsels()` emits Draken `Morsel` chunks directly

Current status:

- implemented for the current supported slices
- still not available for unsupported shapes, which explicitly fall back to legacy finalize paths

### Step 7: Integrate With `DrakenAggregateAndGroupNode`

Objective:

- switch the node to the new compiled engine for supported shapes

Work:

1. instantiate `CarcharGroupStateEngine` instead of the old wrapper/backend pair
2. keep the node responsible only for:
   - planner semantics
   - expression prep
   - explicit fallback routing
3. preserve existing explicit fallback behavior for unsupported shapes

Success condition:

- no behavior change for unsupported queries
- supported grouped-agg queries use the new engine end-to-end

Current status:

- not done yet
- current wrapper/engine boundary is ready for this
- this should wait until multi-aggregate fixed-width support exists so the node integration lands on a meaningfully useful compiled slice

### Step 8: Add Telemetry And Validation

Objective:

- make the next bottleneck obvious

Telemetry:

- `time_groupby_hash_keys`
- `time_groupby_state_lookup`
- `time_groupby_state_update`
- `time_groupby_finalize_emit`
- `time_groupby_finalize_vector_write`
- `time_groupby_finalize_morsel_build`
- `groupby_engine_mode`
- `groupby_key_store_bytes`
- `groupby_key_store_limit_hit`

Validation:

1. correctness tests against existing grouped-aggregation outputs
2. null semantics
3. dictionary and constant key correctness
4. large-group-count memory-limit failure tests
5. real workload performance comparison

### Step 9: Expand Coverage

After phase-1 is stable, add:

1. multi-key compiled support
2. typed `COUNT_DISTINCT`
3. additional dictionary-specialized kernels
4. optional seal/finalize-locality optimizations

## Phase-1 Deliverable

Phase-1 is complete when all of the following are true:

1. supported grouped-agg queries no longer use Python `_rows_to_vectors()`
2. supported grouped-agg queries no longer use Python tuple/dict state
3. state lookup is Carchar-backed
4. output is emitted as Draken `Morsel` chunks directly
5. unsupported shapes fail visibly or stay on an explicit existing fallback path

## Task Lists

These are the concrete task lists for each phase.

### Phase 1 Task List

Objective:

- replace the current Python-heavy grouped aggregation hot path for the common fixed-width cases

Tasks:

1. Add new compiled module skeleton:
   - `opteryx/compiled/aggregations/carchar_group_state_engine.pyx`
   - `opteryx/compiled/aggregations/carchar_group_state_engine.pxd`
2. Export the new engine from the compiled aggregations package.
3. Add a Cython/C++ boundary for the Carchar group-state index.
4. Add engine construction inputs:
   - group-by columns
   - aggregation specs
   - key-store byte limit
   - chunk size
5. Reuse `Morsel.hash(...)` for canonical group-key hashing.
6. Implement `state_index` allocation and lookup through Carchar.
7. Implement the encoded key store:
   - append encoded group keys on first insert
   - retain handle by `state_index`
   - track byte usage
   - fail when limit exceeded
8. Implement typed aggregate state arrays for:
   - `COUNT(*)`
   - `COUNT(col)`
   - `SUM`
   - `MIN`
   - `MAX`
   - `AVG`
9. Implement phase-1 ingest kernels for:
   - single-key `Int64Vector`
   - single-key `IntegerVector`
   - single-key `ConstantVector`
   - single-key `DictionaryVector`
   - multi-aggregate fixed-width values
10. Implement direct chunked finalize:
    - allocate output vectors
    - finalize aggregate state directly into buffers
    - decode encoded keys into output vectors
    - emit `Morsel.from_vectors(...)`
11. Add explicit telemetry counters:
    - `time_groupby_hash_keys`
    - `time_groupby_state_lookup`
    - `time_groupby_state_update`
    - `time_groupby_finalize_vector_write`
    - `time_groupby_finalize_morsel_build`
    - `groupby_key_store_bytes`
12. Wire `DrakenAggregateAndGroupNode` to use the new engine for supported shapes only.
13. Leave explicit fallback routing in place for unsupported shapes.
14. Remove supported shapes from Python `_rows_to_vectors()` path.
15. Add correctness tests for:
    - null handling
    - grouped aggregates on fixed-width keys
    - dictionary keys
    - constant keys
    - oversized key-store failure
16. Add performance comparisons against the current grouped aggregation path on ClickBench-like queries.

Exit criteria:

1. `_rows_to_vectors()` is not used for supported phase-1 shapes.
2. `finalize_rows()` is not the hot-path contract for supported phase-1 shapes.
3. performance on real grouped workloads is materially better than the current path.

Progress notes:

- complete:
  - module skeleton
  - facade boundary
  - Carchar `hash -> state_index`
  - single-key `Int64Vector`
  - single-key `IntegerVector`
  - single-key `ConstantVector`
  - one-aggregate compiled finalize
- remaining:
  - single-key `DictionaryVector`
  - multi-aggregate fixed-width values
  - encoded key store
  - node integration

Practical Phase-1 exit should now be interpreted as:

1. supported one-key grouped queries stop using Python finalize/transposition
2. common one-key multi-aggregate queries gain compiled coverage
3. constant and dictionary keyed grouped queries stop falling back unnecessarily

### Phase 2 Task List

Objective:

- widen compiled coverage so the planner can keep more grouped queries on the Draken/Carchar path

Tasks:

1. Add multi-key encoded-key ingestion support.
2. Add typed multi-key state lookup kernels.
3. Add typed `COUNT_DISTINCT` kernels for common fixed-width inputs.
4. Add more dictionary-specialized kernels for grouped aggregation.
5. Add multi-aggregate specialization for common real query shapes:
   - `COUNT + SUM`
   - `COUNT + MIN + MAX`
   - `COUNT + AVG`
6. Add better finalize support for mixed aggregate output types.
7. Improve encoded-key reconstruction for multi-column output.
8. Add per-kernel telemetry so we can see which specialization was selected.
9. Expand correctness tests for:
   - multi-key grouping
   - distinct aggregation
   - mixed aggregate sets
10. Expand performance tests to include higher-cardinality and wider-group queries.

Exit criteria:

1. common multi-key grouped aggregation queries stay on the compiled path.
2. `COUNT_DISTINCT` no longer relies on Python set/object state for supported typed shapes.
3. fallback frequency is substantially reduced on real workloads.

Updated note:

- the first item to pull forward from this phase is likely `DictionaryVector` support if benchmark evidence keeps showing it is a frequent grouped key shape
- the first item to defer is multi-key if real workloads are still mostly one-key dominated

### Phase 3 Task List

Objective:

- improve locality, memory accounting, and finalize efficiency once the new engine is the normal path

Tasks:

1. Evaluate logical vs physical `seal()` for grouped aggregation finalize.
2. Add optional state compaction or finalize-locality optimization if measurement justifies it.
3. Improve encoded-key storage efficiency:
   - compression opportunities
   - smaller handles
   - reduced decode overhead
4. Add memory accounting and operator-visible diagnostics:
   - state bytes
   - key-store bytes
   - spill/limit triggers
5. Decide whether to add partitioned group-state execution for very large group cardinalities.
6. Move more expression preparation into Draken-native execution where justified.
7. Tune chunk sizing for finalize and downstream morsel flow.
8. Add larger-scale performance benchmarks and stress tests.

Exit criteria:

1. grouped aggregation locality and memory behavior are measured and tunable.
2. the engine has clear operational limits and failure modes.
3. the grouped aggregation path is architecturally aligned with the long-term Carchar execution model.

## Telemetry

Add explicit engine telemetry so the next hotspot is obvious:

- `time_groupby_hash_keys`
- `time_groupby_state_lookup`
- `time_groupby_state_update`
- `time_groupby_finalize_emit`
- `time_groupby_finalize_vector_write`
- `time_groupby_finalize_morsel_build`
- `groupby_engine_mode`
- `groupby_specialized_kernel_hits`
- `groupby_specialized_kernel_failures`

This is preferable to relying on Python profiler traces after the fact.

## What Gets Deleted

The rewrite should aim to remove these from the hot path:

- `ShuffleGroupByOperationV2._rows_to_vectors()`
- `finalize_rows()` as the primary output contract
- Python row tuples for grouped state output
- Python dict/tuple generic state for supported shapes

## Recommendation

Do not keep iterating on the current Python-heavy wrapper shape.

The correct rewrite is:

1. keep Python for planning and operator coordination
2. move grouped aggregation state into a compiled Draken-adjacent engine
3. back the group state index with Carchar
4. emit Draken morsels directly

That matches the engine principles and the intended role of Carchar.
