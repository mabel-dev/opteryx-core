# M4 — Morsel-Driven Parallel Execution (DuckDB model)

> Status: **DESIGN — prototype-validated.** Aligned to DuckDB's morsel-driven
> model (`docs/DUCKDB_PARALLELISM_REFERENCE.md`); the Spark-style materialized
> shuffle + stage barriers are **explicitly rejected** (slow: a full intermediate
> materialization and a hard barrier per breaker). Both prior open questions
> (reduce model, probe-side shuffle) are resolved by this alignment.

## Context

The 6-month engine rewrite's final leap. Today the data executor
(`opteryx/managers/execution/parallel_engine.py`) only parallelises **single-scan**
pipelines; anything multi-scan (every join, set-op) falls to `_serial_stream` and
runs **single-threaded**. `identify_segments` already cuts the plan into segments
at breakers, but nothing consumes that graph as a schedule.

Goal: **morsel-driven parallelism** — the plan is cut into pipelines at breakers;
a pipeline is run by N identical tasks racing through the *same* operator chain on
*disjoint* morsels; parallelism is a property of the pipeline, never of an
operator. There is **no exchange operator and no materialized cross-pipeline
shuffle** — partitioning is folded into each sink's thread-local state.

Runtime is free-threaded CPython 3.14t (`sys._is_gil_enabled()` is False) — real
threads, no GIL. Residual per-object refcount contention is avoided by keeping
morsels in C++ (below), not by locking.

## Why not Spark

Spark materializes a partitioned intermediate to a shuffle buffer, hard-barriers,
then the next stage reads it. That is the model we were drifting toward (explicit
Exchange operator, `$bin` matrix, blocking shuffle, shuffle-both-join-sides) and
it is **slow**: a full intermediate materialization and a stage barrier on every
breaker. DuckDB avoids all of it — partitioning lives inside the sink, pipelines
stream, ordering is an event dependency, and the probe side never materializes.
We take the DuckDB model.

## Prime constraint — the serial path is the asset (locked)

We are already **~2× DuckDB per core**. The entire gap to DuckDB is the serial
ceiling, not single-thread efficiency. So this rewrite is **not a speed recipe** —
it removes a self-imposed ceiling on an already-superior core. The prime directive:
**spend cores without spending the single-thread lead.** The model only needs to be
a coherent way to scale; it must not tax the thing we already won.

This makes the cost model **DOP-proportional, not fixed**:

- **Partition degree scales with DOP.** Radix fan-out, thread-local fan-out,
  Combine, and the k-way merge tail appear ONLY when there is parallelism to pay
  for them. At `DOP=1` there is **1 partition**.
- **`DOP=1` ≡ today's serial path.** The whole local/global apparatus collapses to
  the current lean single-instance operator when there is one worker — no radix
  overhead, no Combine, no merge. The model is *invisible* at one core.
- This is the **opposite of DuckDB**, which pays radix-partitioning even
  single-threaded. We do not — our single-thread path is the asset to protect, and
  we are not chasing DuckDB's per-core number (we already beat it).

**Gate: single-thread (`DOP=1`) must be within noise of the current serial engine.**
A single-thread regression means the rewrite is wrong — the model is meant to be
free at one core and multiplicative above it. Above `DOP=1`, scaling efficiency is
the only metric that matters.

## Model (locked)

- **Pipeline** = source → 0..N streaming operators → one sink. The unit of
  scheduling and parallelism. A **breaker** is a sink: it fully consumes its input
  before producing output, ending one pipeline and beginning the next.
- **Within a pipeline, morsels stream** end-to-end through the operator chain;
  there is no materialized shuffle between operators.
- **Ordering is an event dependency, not a barrier-lock.** Build-before-probe and
  aggregate-before-read-out are edges in an event DAG (two-counter events:
  runnable when dependencies finish, complete when tasks finish). Independent
  pipelines overlap.
- **No exchange operator.** Every partitioning consumer (agg, join-build,
  distinct) radix-partitions *its own input* inside its sink. A breaker that
  re-groups by a new key just partitions by that key internally — the upstream
  breaker's read-out streams into it. Nothing materializes a cross-pipeline
  shuffle.
- **Source parallelism** is row-group handout under a short cursor lock; the scan
  is the only "partitioning" that isn't key-based.

## Partitioning is internal to the sink (locked)

The lock-free substrate (DuckDB §12, §14), folded into each breaker:

- **`GlobalSinkState` once / `LocalSinkState` per task.** Hot-path mutation
  touches only local state.
- **`Sink(chunk, local)`** ingests into thread-local, **radix-partitioned** state
  (hash the key, partition by the high/low bits into `radix` partitions).
- **`Combine(local → global)`** at task end is a **partition-aligned pointer
  hand-off** — move each local partition's fragment into the global partition's
  list. **O(partitions), no rehash.** (Not the O(groups) per-group merge we have
  today — that is the bottleneck this design exists to avoid.)
- **Read-out is a parallel `Source`** — each task finalizes-and-scans one
  partition; no group spans two partitions, so there is no serial global merge.
- `radix` bits are fixed for v1; **raising them under memory pressure** is the
  spill hook (deferred).

## Per-breaker (locked)

- **Aggregate** — thread-local radix HT; **bounded-adaptive local pre-agg**: low
  cardinality stays local (shuffle ~NDV), and on overflow the local table flushes
  (DuckDB's `Abandon()`), degrading gracefully. Partition-aligned Combine,
  parallel read-out. Holistic aggs (MEDIAN/COUNT DISTINCT) skip the local table.
  *(This resolves the former OPEN reduce-model question.)*
- **Inner/equi join — stream the probe.** Build side: thread-local
  `JoinHashTable`s, merged into **one shared immutable table** (parallel
  pointer-table init + finalize). Probe side: a **streaming operator** in the
  probe pipeline that looks up the read-only table — **no probe partitioning, no
  probe materialization.** Probe parallelism is inherited from the probe scan's
  morsels. *(This resolves the former OPEN probe-shuffle question — DuckDB's way.)*
  Outer joins track unmatched build rows for emission. **Keyless joins (CROSS /
  non-equi): serial fallback.**
- **Sort** — parallel **local sort** per task + a **k-way merge** tail (a
  deliberate partial serial tail).
- **Distinct** — like aggregate (radix-partitioned dedup, parallel read-out).

## Degree of parallelism (locked — data-bounded)

DOP per pipeline = `min(source morsels, operator hints, scheduler threads)` — the
principled version of a row-floor gate. Tiny inputs never oversubscribe and pay no
recombination tax, with **no tuned floor constant**. Supersedes the fixed
`resolve_worker_count` + `PARALLEL_MIN_ROWS`.

**Partition degree is derived from DOP** (see Prime constraint): 1 partition at
`DOP=1`, scaling up only with worker count — so the partitioning apparatus is never
paid on the serial path. A breaker at `DOP=1` is the current single-instance
operator, unchanged.

## The operator contract — a C++-first rewrite (the real work)

The scheduler is the easy half (prototype-proven). The body of work is rewriting
breakers to the local/global contract, in C++:

| Axis | Today | Target |
|---|---|---|
| State | one instance owns everything; scheduler clones it | `GlobalSinkState` once + `LocalSinkState` per task |
| Merge | today's serial path does **no** merge (row-routes disjoint key slices); the reverted hash-partition experiment merged per-group — **O(groups)**, the wall this design exists to avoid | partition-aligned hand-off — **O(partitions)**. **At high cardinality the pointer hand-off alone does NOT scale** (G-A: 1.0–1.5×); the Sink must *route raw to partitions on overflow* (Abandon) so the single aggregation pass happens **once per partition in parallel read-out** (G-A: 4.8× @ DOP8). Abandon is the mechanism, not the pointer hand-off. |
| Drive | one synchronous `push() nogil` | `Sink`/`Combine`/`Finalize` phases |
| Read-out | finalize on the instance, ~serial | parallel `Source`, one partition per task |

Rule established by the prototype: **bind orchestration to Python, never the
morsel.** Morsels stay C++ (`CxxMorsel` / `shared_ptr[CxxMorsel]` — the carrier
flip already does this); the per-task push loop runs in C++ with the GIL released;
only the scheduler crosses into Python. This **dissolves the free-threaded
refcount question** (no PyObject per row) and the nanobind type clash.

**Hard prerequisite (not a parallel convergence): the GIL must be genuinely off
the operator-chain push *bodies* before Stage 1.** The carrier flip
(`docs/M4_CPP_MORSEL_DESIGN.md`) is structurally landed — the chain currency is
`shared_ptr[CxxMorsel]` and the push signatures are `noexcept nogil` — but the
bodies still run under a transitional `with gil:` (only grouped-agg ingest has been
released so far). Building local/global breakers on a chain that still takes the GIL
per body means every scaling measurement is taken against a GIL ceiling and model
wins are indistinguishable from lock contention. Finish the carrier-flip GIL
release first; then start Stage 1.

## Deferred from DuckDB (deliberate v1 cuts — not Spark-isms)

- **Cooperative `BLOCKED` / `InterruptState` async machine** — our tasks run to
  completion; no in-pipeline I/O overlap or backpressure yet. §19.4 says it is
  what makes a GIL-off carrier fully pay; revisit after v1.
- **Spill / memory-awareness** — v1 is in-memory. Cheap to add later: DuckDB
  spills by **raising radix bits inside the sink** (more, smaller partitions,
  processed/spilled one at a time) and by spilling probe data to the external
  probe path — both stay within this model, no new architecture.

## Prototype validation (`scratch/ddb_proto/`)

Isolated (reuses only the draken Morsel + headers). Python scheduler (event DAG,
dependencies, data-bounded DOP) + C++-native operators (`_cops.cpp`). Morsels never
cross into Python; GIL off; results checked vs oracles.

| Shape | Demo | Proven | DuckDB-aligned? |
|---|---|---|---|
| Stateless (scan/filter/project) | `demo_cpp.py` | fused C++ push loop, no per-morsel cast | ✓ |
| Sort breaker | `demo_sort.py` | local sort + k-way merge | ✓ |
| Aggregate breaker | `demo_agg.py` / `demo_agg_bench.py` | *correctness* of thread-local radix HT + O(partitions) Combine + parallel read-out; **G-A scaling** showed thread-local HTs flat at high card, route-on-abandon = 4.8× | ✓ shape; scaling needs Abandon (see G-A) |
| Inner join breaker | `demo_join.py` (old, Spark-style) → `demo_join_stream.py` (G-B) | **stream-the-probe**: shared immutable table + streaming probe + parallel outer-unmatched; correct, probe 6.19× @ DOP8 | ✓ (redone — G-B passed) |

The join demo has been rebuilt to match the model (shared-table build + streaming
probe) in `demo_join_stream.py` — see Gate G-B (passed).

## Substrate to reuse

`CppThreadPool`; `pull_one` (reentrant scan); `_clone_op`; `identify_segments` +
`OperatorParallelism`; `PipelineContext` (terminate / `_exc`; note
`set_expected_input_closes` does **not** exist yet — only the
`_expected_input_closes` attribute — the scheduler must add the setter); exported
C-ABI handles (`cxx_morsel_raw_ptr`,
`cxx_take_c`) for out-of-tree C++. The agg engine's `merge()` is the embryo of
`Combine` but must become **radix-aligned (O(partitions))**. **Supersedes**
`_ScatterCollectEngine`, `_DistinctCollector`, `_parallel_engines`, the four
`_find_parallel_*` finders. (No exchange operator / `$hash`/`$bin` columns / shuffle
matrix — those were the Spark design and are dropped.)

## Components to build

1. **Event-DAG scheduler** — pipelines from `identify_segments`, two-counter
   events, dependency edges (build-before-probe, agg-before-read-out),
   data-bounded DOP, producer/consumer pools.
2. **C++ breaker rewrites** to local/global + `Sink`/`Combine`/`Finalize`, each
   **radix-partitioning its own input internally**:
   - aggregate (bounded-adaptive, O(partitions) Combine, parallel read-out)
   - join (thread-local tables → shared table → **streaming probe**)
   - distinct (like aggregate)
   - sort (local sort + k-way merge)
3. k-way merge kernel for sort.
4. Parallel read-out `Source` role on breakers.
5. Retire the single-scan finders/seams.

## v1 parallel surface (explicit scope)

What this plan makes parallel: **grouped/ungrouped aggregate, distinct, inner/equi
join (streaming probe), sort.** What stays **serial in v1**, by deliberate cut:

- **CROSS / non-equi joins** (keyless — no partition key).
- **Window functions** (`WindowNode`).
- **Set-op `ALL`** (INTERSECT/EXCEPT ALL ride on ROW_NUMBER + semi/anti join; they
  inherit join parallelism only once Stage 4 lands, and the ROW_NUMBER prefix stays
  serial).
- **LIMIT-only / subquery / projection-only** segments.

This is stated so "the join parallelised but my windowed query is still
single-threaded" is a documented boundary, not a surprise. None of these are
architecturally blocked — they are out of v1 scope.

## De-risking gates (prototype-tier, before the matching engine stage)

The prototype proved the *shapes run correctly vs. an oracle*. It did **not** prove
the two things this project can actually fail on. Each gate below is cheap
(prototype / microbench, no engine rewrite) and **must pass before its engine stage
begins** — so a failure costs a microbench, not a breaker rewrite.

- **Gate G-A → blocks Stage 2. ✅ PASSED (conditionally) — `scratch/ddb_proto/demo_agg_bench.py`.**
  Microbenched the high-cardinality regime (580k groups, 9.6M rows) at DOP 1/2/4/8.
  Result, and the condition it imposes on Stage 2:
  - **Thread-local full hash tables do NOT scale** (1.0–1.5×; fat radix *worse* at
    0.51×). With round-robin morsels every thread's local HT covers most of the key
    space → total build work multiplies by ~DOP, `sink` wall-clock never shrinks.
    This faithfully reproduces the prior reverted result.
  - **Route-raw + aggregate-once (the design's `Abandon()`) scales: 2.33× / 3.14× /
    4.80× at DOP 2/4/8.** The Sink only hashes+routes raw keys to partition buffers;
    the single aggregation pass runs once per partition in the parallel read-out.
  - **Verdict:** Stage 2 is justified **only if built as route-on-abandon, not
    thread-local full HTs.** Bounded-adaptive Abandon is load-bearing, not optional.
  - **Wide-payload qualifier — CLOSED.** Re-ran with a multi-aggregate payload
    (COUNT+SUM+MIN+MAX, the Q23/Q31/Q33 shape): route-on-abandon scales **4.87×** at
    DOP8 (vs 4.80× on bare COUNT(*)) while the HT strawman gets *worse* (0.51×) — the
    bigger per-group state multiplies harder. HT and route cross-check **identical**
    (580k groups, 2.784M rows), so the win is not a corner cut.
  - **String keys:** at the aggregate breaker a draken string key reduces to a
    fixed-width hash slot (hash-only equality, hash computed at scan), so it behaves
    as the int-hash case already proven — not a separate risk for the breaker. (The
    real-data Q34/Q13 string-key flatness is end-to-end and likely scan-bound, a
    separate axis the agg breaker does not own.)
  *Why this gate existed:* intra-op agg parallelism was already tried and **reverted**
  — round-robin (0.94×) and hash-partition (0.62–0.72×) both **lost** on ClickBench
  because recombination cost ≈ aggregation cost at high NDV.
- **Gate G-B → blocks Stage 4. ✅ PASSED — `scratch/ddb_proto/demo_join_stream.py`.**
  Rebuilt the join as **stream-the-probe** (the old `demo_join.py` is the disowned
  Spark-style co-partitioned shuffle). Proven in isolation:
  - (a) thread-local build collection → ONE shared **immutable** table (finalize);
  - (b) **streaming probe, no probe materialization** — probe parallelism inherited
    from the probe scan's morsels;
  - (c) **parallel emission of unmatched build rows for outer joins** (per-build-row
    matched flag set during probe, then a parallel range-claimed pass).
  - **Correctness:** inner match count/sum AND outer unmatched-build count/sum match
    the oracle, including build keys that never probe.
  - **Probe scaling:** 2.14× / 4.21× / **6.19×** at DOP 2/4/8 on a fixed read-only
    table (12.8M probe rows) — near-linear, no recombination tail (there is nothing
    to recombine on the probe side; that is the model's advantage over the shuffle).
  - **Verdict:** the highest-risk stage now has a validated prototype. Stage 4 builds
    this shape (keyless/non-equi joins still fall back to serial per the design).

## Implementation plan (staged)

Sequencing falls out of two lessons — the scheduler is the easy half, and `DOP=1`
must never regress — plus the ClickBench read: start where headroom *and* our lead
concentrate (the heavy agg/scan band, Q17–Q35). Each stage ships green; the gate is
the same every time.

**Cross-cutting gate (every stage):** `make q` (190) + tpch (22) + clickbench (43)
green · result-identical to serial (`workers=1` vs `N`) · **`DOP=1` within noise of
today's serial path** · orchestration crosses to Python, never the morsel.

| Stage | Scope | Primary area | Risk |
|---|---|---|---|
| **0 · Scheduler skeleton + harness** | Build the DOP-sweep `make` target **first** (it certifies every later stage). Then: event-DAG + pipeline decomposition from `identify_segments`; data-bounded DOP; drive the **existing** operators. **NOT a no-op** — data-bounded DOP replaces `resolve_worker_count` + `PARALLEL_MIN_ROWS` (a live 262 144-row floor), which *is* a worker-count behavioral change. Either keep the floor constants through Stage 0, or prove the DOP formula reproduces today's worker decisions across the q/tpch/clickbench corpus before declaring green. | `opteryx/managers/execution/` (reuse `PipelineContext` — add the missing `set_expected_input_closes` setter —, `drive_scan`, `CppThreadPool`) | low–medium — no operator changes, but the DOP switch is behavioral |
| **1 · Aggregate → contract, `DOP=1`** | Carrier-flip GIL-off bodies done (hard prereq). Agg exposes `Global/LocalSinkState` + `Sink/Combine/Finalize` in C++; `DOP=1` ⇒ 1 partition ⇒ current path. | `operators/grouped_aggregate_hashed/` + draken `GroupHashEngine` | medium — contract on the real engine |
| **G-A** | ✅ *Combine microbench gate — PASSED conditionally (above). Verdict: route-on-abandon scales (4.8×), thread-local full HTs do not.* | `scratch/ddb_proto/demo_agg_bench.py` | — gate |
| **2 · Aggregate parallelism on** | **Route-on-abandon** at `DOP>1` (G-A condition): Sink hashes+routes raw to partitions, aggregation runs **once per partition in parallel read-out**; **O(partitions) Combine**; bounded-adaptive (small/low-card stays local pre-agg, overflow → Abandon). First real win. Re-confirm vs wide payloads + string keys. | same | medium-high — merge/read-out correctness + the win |
| **3 · Distinct** | Mirror of agg (radix dedup, parallel read-out). | `operators/distinct/` | medium |
| **G-B** | ✅ *Stream-the-probe prototype gate — PASSED (above). Correct inner+outer; probe scales 6.19× @ DOP8, no probe materialization.* | `scratch/ddb_proto/demo_join_stream.py` | — gate |
| **4 · Inner/equi join** | Multi-scan pipeline DAG (build-before-probe dependency); thread-local build tables → one shared table → **streaming probe**; keyless → serial. Unlocks join-heavy queries. | `operators/hashed_inner_join/` + join engines | high — multi-input, ordering, engine rewrite |
| **5 · Sort** | Parallel local sort + k-way merge tail. | `operators/sort/` + new k-way merge kernel | medium |
| **6 · Cleanup** | Retire the single-scan finders/seams (`_find_parallel_*`, `_ScatterCollectEngine`, `_DistinctCollector`, `_parallel_engines`). | `parallel_engine.py` | low |

**Critical path:** 0 → 1 → G-A → 2 → G-B → 4 (skeleton+harness, then the agg
contract, prove the Combine win, turn agg on, prove stream-the-probe, then the
multi-scan unlock). 3 and 5 are independent after 0. G-B is independent of 1–3 and
should run as early as there is capacity.

**Validation harness (Stage 0's first deliverable, not an afterthought):** extend
the prototype's `DOP=1` gate into a `make` target — a DOP sweep (1/2/4/8) proving
both serial parity at `DOP=1` and scaling above it, on the Q17–Q35 band first. No
stage can be certified without it; that is why it leads Stage 0.

**Deferred (named, not on the path):** radix-join-as-second-strategy (above
`DOP=1`); the cooperative `BLOCKED` async machine; spill (slots into the sink via
radix-bit raising). None blocks v1.

## Verification

- **Single-thread no-regression gate (primary).** `DOP=1` must be within noise of
  the current serial engine on `make q` / tpch / clickbench — the model must be free
  at one core. A serial regression fails the rewrite regardless of parallel gains.
- `make q` (190) + tpch (22) + shapes + clickbench (43) green; result-identical to
  serial; workers=1 vs N identical.
- DuckDB oracle on multi-join + GROUP BY + ORDER BY, incl. NULL keys, multi-col
  keys, outer joins, empty inputs.
- ClickBench timing: parallel agg/join must **win** (validates bounded-adaptive
  pre-agg and stream-the-probe).
- Skew probe: confirm parallel read-out balances; a mega-partition degrades
  gracefully (doesn't crash/corrupt).
