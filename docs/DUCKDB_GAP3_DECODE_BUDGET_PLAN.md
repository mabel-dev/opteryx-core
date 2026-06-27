# Gap #3 — Fold Decode into the One Scheduler (Thread-Budget) — RATIFIED PLAN

> **UPDATE 2026-06-27 — Phase 1 BUILT, MEASURED NEUTRAL, REVERTED.** The shared-pool
> change was implemented and validated on the closest-to-prod hardware available (a 6-core
> x86 i5-8500, built free-threaded). On a hash join, per-scan vs shared pool was **identical**
> (5.4 cores@8, 2.47×): a hash join's two scans run **sequentially** (build then probe), so
> only one decode pool is ever active and the N×pool oversubscription this phase targets never
> occurs (the join's cores@1 of 1.8 is *below* a single scan's 2.6). The concurrent-scan case
> (UNION ALL) that *could* oversubscribe is blocked by an unrelated planner bug
> (`projection.pyx:60` None+list). Verdict: perf-neutral + strictly more complex → reverted.
> The measured gap-#3 pain remains the **single-scan** string-agg ceiling (Phase 2 below).
> Phases 2–4 are unaffected by this; Phase 1 should not be revived without a demonstrated
> concurrent-scan oversubscription win.

**Status:** Ratified plan. Derived from §3 of
[`DUCKDB_MATURITY_GAP_OVERVIEW.md`](DUCKDB_MATURITY_GAP_OVERVIEW.md), with the gap
re-verified against current source (2026-06-26) and the overview's claims corrected.
**Scope:** The two-uncoordinated-CPU-pools problem (parquet decode vs execution).
**Owner decision points:** recorded in §6; resolved decisions noted inline.

---

## 0. Verdict — the gap is real

Two CPU-bound thread pools run concurrently and never coordinate their core budgets:

| Pool | Where (verified) | Size |
|---|---|---|
| **Execution** | `CppThreadPool` per scheduler ([`scheduler_engine.py:406`](../opteryx/managers/execution/scheduler_engine.py)); DOP = `resolve_worker_count` ([`parallel_engine.py:104`](../opteryx/managers/execution/parallel_engine.py)) | `max(1, min(cpu-2, 8))` — caps at 8, reserves 2 cores |
| **Decode** | per-scan `BS::light_thread_pool` in `ParquetIOPipeline` ([`io_pipeline.hpp:621`](../rugo/src/parquet/io_pipeline.hpp)) | local `min(16, max(8, cpu-2))`; GCS `128` ([`config.py:156-166`](../opteryx/config.py)) |

The decode pool is **per `IpcRowGroupSource`** — one `CppIOPipeline` (hence one
`ParquetIOPipeline`, hence one decode pool) is constructed per scan in `open_ipc_source`
([`pool_reader.pyx:1241`](../opteryx/connectors/parquet_io/pool_reader.pyx)). So a plan
with *N* parquet scans spins up *N* decode pools.

On an 8-vCPU Cloud Run box (prod target, CLAUDE.md §6):

- Execution DOP = `min(8-2, 8)` = **6**.
- Each scan's decode pool = `min(16, max(8, 6))` = **8**.
- **Single-scan string agg:** 8 decode + 6 exec = **14 threads on 8 cores** (~1.75× over).
- **2-table join:** 2 decode pools = 16 decode + 6 exec = **22 threads on 8 cores**.

Decode (UTF-8 string decode) is the wide, expensive side and saturates first, starving
execution DOP. This reproduces §4.2 of
[`PARALLEL_ENGINE_DESIGN.md`](PARALLEL_ENGINE_DESIGN.md): route-ON string `URL` agg stays
**1.78× / 7.35 cores** (decode pinned 1.0×→1.0×, saturated) versus int-key
**4.51× / 8.19 cores** where decode is cheap and leaves cores for exec DOP.

A reserved-but-**unwired** flag exists for exactly this work:
`FEATURE_PARQUET_THREAD_SCHEDULER` ([`config.py:227`](../opteryx/config.py)) — zero
consumers in the tree.

## 0.1 Corrections to the overview's §3

Two claims in the overview were checked and are inaccurate; the plan below uses the
corrected facts.

1. **In-flight bound.** §3 says the moodycamel queue is bounded to `decode_workers + 2`.
   That value is the **flow-control `in_flight_limit`**
   ([`pool_reader.pyx:1235`](../opteryx/connectors/parquet_io/pool_reader.pyx)); the C++
   `queue_capacity` is hardcoded `1024`. Back-pressure reasoning must use `in_flight_limit`,
   not the queue size.

2. **GIL on the pull path.** §3 says `pull_one`'s dequeue wait releases the GIL. Current
   source disagrees — [`_operators.pyx:918`](../opteryx/operators/_operators.pyx): *"the
   pull itself is GIL-held today (S-B.2 makes it nogil)."* This is a factual correction only;
   it is **not** a blocker for Phase 2a — the decode is native C++ and releases the GIL around
   the heavy work like every other native kernel (see §3 Phase 2a).

Everything else in §3 verified accurate against current source.

---

## 1. DuckDB's mechanism (the target)

From [`DUCKDB_PARALLELISM_REFERENCE.md`](DUCKDB_PARALLELISM_REFERENCE.md):

- **One `TaskScheduler`, `nproc-1` threads** (§7.1) — one CPU-bound budget per query.
- Two *queue types* (regular compute + async/IO, §7.3), but **regular workers steal from
  all pools** (§7.5): I/O is segregated for *latency*, not given its own CPU army.
- **The parquet scan is the source phase** (§13): morsel handout at row-group granularity
  under one short cursor lock; the compute worker then **decodes its own row group** inside
  `GetData()`. Decode is not a separate pool — it is work the consuming worker does, so
  vectors stay hot in that worker's cache.
- **I/O latency hidden by `BLOCKED` + async** (§10-11): a worker waiting on bytes never
  *holds* a CPU thread.

Net target: **decode and execution share one fixed CPU budget; I/O latency is hidden by
async, not by a fat dedicated decode pool.** Exact inversion of Opteryx today.

---

## 2. The reframe that drives sequencing (read before building)

Per [`feedback_profile_and_ceiling_before_optimizing`] and
[`feedback_say_no_before_building`], each phase must be tied to the workload it actually
moves — *not* to the headline number by association:

- The **headline proof signature** (string agg, **1.78× / 7.35 cores**) is a **single-scan**
  workload. **Phase 1 (shared pool) does nothing for it** — there is one decode pool either
  way. Phase 1 fixes **multi-scan join oversubscription** only.
- **Phase 1 alone does not bound threads to cores.** A 2-table join goes 16+6=22 → 8+6=14
  on an 8-core box: better, still oversubscribed. Only **Phase 2 (one CPU budget)** bounds it.

So Phase 1 and Phase 2 target **different workloads**. The plan keeps them honest by giving
each its own before/after metric (§5).

**Ratified ordering: Phase 1 first** (overview's sequencing) — it is the cheapest change
and removes the join multiplication — *with the explicit caveat that the single-scan ceiling
does not move until Phase 2.*

---

## 3. The phased design

### Phase 0 — Reproduce the baseline (no code change) — REQUIRED FIRST

Re-measure on current tree, capturing **cores-used and DOP scaling**, not wall-clock alone:

| Probe | Targets | Expected "before" |
|---|---|---|
| single-scan string-key agg (`URL` agg) | Phase 2 ceiling | ~1.78× / 7.35 cores, decode saturated |
| 2-table join cores-used | Phase 1 oversubscription | ~22 CPU-hungry threads on 8 cores |
| int-key agg (control) | decode-is-cheap envelope | ~4.5× / ~8 cores |

This confirms the ceiling is where §4.2 says and attaches an honest before/after to each
phase. No phase ships without its own measured pair.

**MEASURED (2026-06-26, [`dev/gap3_decode_baseline.py`](../dev/gap3_decode_baseline.py),
full `scratch.hits`, free-threaded 3.14.5t, 18-core dev box, route ON):**

| shape | cores@1 | speedup@8 | cores@8 | §4.2 route-ON |
|---|---|---|---|---|
| int key (control) | 1.2 | **4.79×** | 8.2 | 4.51× / 8.19 |
| string key `URL` | 2.7 | **1.77×** | 7.5 | 1.78× / 7.35 |
| string key `SearchPhrase` | 1.8 | **1.95×** | 5.2 | 1.90× / 5.25 |

All three reproduce §4.2 within noise — the decode-budget ceiling is confirmed on current
source. `cores@1` is the smoking gun: the decode-bound `URL` scan already burns **2.7 cores
at a single execution worker** (decode pool active regardless of DOP) vs **1.2** for the int
control, so execution DOP is inert (1.77× vs 4.79×). Note the box is **18 cores**, not the
8-vCPU prod target — the *absolute* core counts are platform-specific; the *signature*
(string DOP-inert while int scales) is what transfers. This is the Phase-2 "before."

**MEASURED — Phase-1 multi-scan oversubscription** ([`dev/gap3_join_baseline.py`](../dev/gap3_join_baseline.py),
filtered self-join on near-unique `WatchID`, `CounterID=109363` → ~397k rows, ~1:1, no blowup):

| shape | cores@1 | speedup@8 | cores@8 | wall@1 |
|---|---|---|---|---|
| 1-scan ref (1 decode pool) | 3.0 | 1.02× | 3.1 | 0.062s |
| 2-scan self-join (2 decode pools) | 2.7 | 2.30× | **12.2** | 1.564s |

The N×decode-pool multiplication is real: one scan tops out at ~3.1 cores, the 2-scan join
demands **12.2** — the second decode pool (16 threads of its own) plus join compute. On this
18-core box 12.2 < 18 so it still scales (2.30×); on the 8-vCPU prod target a 12.2-core demand
is ~1.5× oversubscribed — the dev box understates prod pain. **Phase-1 "before": 12.2 cores@8.**
The shared-pool "after" is the *same* join query; success is peak core demand dropping toward
the one-pool envelope without a wall regression.

> **Engine note:** this measurement unmasked a pre-existing one-line bug — the LATMAT pass-1
> dict-skip branch at `parquet_read.pyx:1434` called `self.record_pass1_skipped()` (no such
> method on `ParquetReadNode`) instead of `self.scan_readings.record_pass1_skipped()`. Fixed
> (architect-approved) so the filtered join could run. Unrelated to the decode-budget design;
> flagged here only because Phase-0 surfaced it.

### Phase 1 — One decode pool per query, not per scan (cheap, immediate)

Hoist the `BS::light_thread_pool` out of per-`IpcRowGroupSource` ownership into a
**query-scoped shared pool**, injected into every `open_ipc_source`. `decode_pool_` is
already a `std::shared_ptr` ([`io_pipeline.hpp:621`](../rugo/src/parquet/io_pipeline.hpp)) —
the change is to *share one instance* across all scans in a plan instead of constructing one
per scan ([`pool_reader.pyx:1241`](../opteryx/connectors/parquet_io/pool_reader.pyx)).

- **Surface:** `io_pipeline.hpp`, `pool_reader.pyx` (C++/Cython — right side of §2).
- **Effect:** kills the *N×* decode-pool multiplication on every join / multi-scan plan.
- **Caveat (surfaced):** does **not** move the single-scan headline; does **not** bound total
  threads to cores. It removes one of the two over-subscription factors.
- **Lifetime risk to design:** the shared pool must outlive every scan that holds a handle
  and be torn down once per query, not once per scan. Pin ownership to the query/scheduler
  scope, not the first `IpcRowGroupSource`.

### Phase 2 — One CPU budget across decode + execution (the real fix)

Make decode draw from the **same** budget as execution so total CPU-bound threads are
bounded by cores. **Both options remain open in this plan** (ratified): **2a is the
destination, 2b is a permitted lower-risk staging step.** Pick at implementation time after
Phase 1 lands and Phase 0 numbers are in hand.

- **2a — decode-on-pull (DuckDB source phase; destination).** The execution worker that
  calls `pull_one(scan)` does the decompress+convert itself instead of blocking on a separate
  pool's queue. The only CPU pool left is the execution pool; the row-group cursor
  (`_scan_mtx`, already present, already makes `is_concurrent_pull_safe()` True for single-pass
  — [`parquet_read.pyx:943-954`](../opteryx/operators/parquet_read/parquet_read.pyx)) is the
  single sync point — DuckDB §13. I/O *fetch* of compressed bytes stays a small async prefetch
  ahead of the cursor; the CPU-heavy decode moves onto the consuming worker (best cache
  locality).
  - **Requires:** splitting `ParquetIOPipeline`'s conflated fetch+decompress+decode into
    (i) async fetch and (ii) a *synchronous* decode entrypoint callable on the calling thread.
  - **No GIL coupling.** The synchronous decode is native C++ and releases the GIL around the
    heavy work — the established engine pattern ([`string_kernels_gil_release`],
    [`cpp_morsel_design`]). One worker decoding does not block the others. It is **not**
    coupled to S-B.2: decode-on-pull removes the prefetch queue entirely, so the "make the
    dequeue-wait nogil" concern S-B.2 addresses does not apply to this path. (`pull_one` being
    GIL-held *today* is a property of the queue it replaces, not a constraint on 2a.)

- **2b — shared scheduler, decode as tasks (staging step).** Keep the prefetch-ahead
  structure, but submit decode tasks to the **same** scheduler pool that runs execution (the
  `CppThreadPool`/`Executor`), sized once to cores, with decode and execution as two task
  classes the scheduler interleaves and work-steals (reference §7.3, §7.5).
  `FEATURE_PARQUET_THREAD_SCHEDULER` ([`config.py:227`]) is the natural switch (currently
  unwired). Lower structural risk (keeps the pipeline), but keeps two task systems.
  - **Note on the flag:** per [`feedback_no_gating_as_an_answer`] the flag is an
    *implementation staging switch*, not a ship-with-it default. 2b is not "done" while the
    win lives behind an off-by-default flag — the flag exists to stage the cutover, then the
    one-budget path becomes the default and the old pool is deleted.

### Phase 3 — Separate I/O concurrency from CPU decode (GCS) — deferred

The GCS `128` "workers" ([`config.py:166`]) conflate **network-RTT hiding** (latency-bound,
wants high concurrency) with **CPU decode** (core-bound, ≤ cores). Under one CPU budget these
split: an **async fetch ring** (high concurrency, ~no CPU) feeds a **bounded decode** drawing
from the one execution budget — DuckDB's async-I/O-queue + `BLOCKED` return (§7.3, §10-11).
The `IO_POOL_SLOT_*` machinery ([`config.py:208-211`]) is the embryo of the fetch ring.

- **Cross-item dependency:** this is the concrete consumer of **#1 Phase E (cooperative
  `BLOCKED`)** — the scan fetch becomes a `BLOCKED`-able source. Out of scope for #3; flagged
  so Phase 2's decode entrypoint is shaped to allow a `BLOCKED` return later.
- **Gate:** only build if GCS scans show I/O starvation *after* Phase 2. Local-disk
  ClickBench does not need it.

### Phase 4 — Retire the static "reserve 2 cores" guess (consequence)

Once decode draws from the one budget, the `cpu-2` reservation and the cap of 8 in
`resolve_worker_count` ([`parallel_engine.py:104`]) stop compensating for an uncoordinated
pool. DOP becomes DuckDB's **min over operator hints, clamped to scheduler threads** (§8),
where the scan's hint is "how many row groups I can hand out concurrently." Per
[`feedback_never_silently_override_user_intent`] an explicit user-set DOP must still be obeyed
(warn, don't clamp); only the *default* heuristic falls away.

---

## 4. Native vs orchestration

| Phase | Surface | New native code? |
|---|---|---|
| 0 — baseline | `dev/` profiling only | No (measurement) |
| 1 — shared decode pool | `io_pipeline.hpp`, `pool_reader.pyx` | **Yes** (C++) |
| 2a — decode-on-pull | `io_pipeline.hpp` (split fetch/decode), `parquet_read.pyx` | **Yes** (C++/Cython) |
| 2b — decode as scheduler tasks | scheduler + `pool_reader.pyx`, flag wiring | **Yes** |
| 3 — async fetch ring | rugo fetch ring + `BLOCKED` source | **Yes** (C++) |
| 4 — drop the reservation | `parallel_engine.py` (`resolve_worker_count`) | No |

Unlike #1, this item is almost entirely native — it lives in rugo and the scan operator,
exactly where CLAUDE.md §2 says execution belongs.

---

## 5. Proof metrics (per phase — not wall-clock alone)

| Phase | Before | Success |
|---|---|---|
| 1 | 2-table join ~22 CPU-hungry threads on 8 cores | cores-used drops toward ≤ exec-DOP + shared-decode; no per-scan multiplication |
| 2 | string `URL` agg **1.78× / 7.35 cores**, decode saturated | cores-used climbs toward the int-key **~8-core / ~4.5×** envelope for *string* keys; exec DOP no longer starved |
| 4 | DOP from magic constants | DOP = min-over-hints, explicit user DOP still obeyed |

Measure cores-used and agg DOP scaling per
[`feedback_profile_and_ceiling_before_optimizing`]. No benchmark spam — one probe at a time,
reuse on-disk data, architect drives ([`feedback_benchmark_hygiene`]).

---

## 6. Decisions

| # | Decision | Resolution |
|---|---|---|
| D1 | Phase 1 vs Phase 2 ordering | **Phase 1 first** (with single-scan-caveat surfaced) |
| D2 | Phase 2 mechanism | **Keep both open** — 2a destination, 2b staging; choose after Phase 1 + Phase 0 numbers |
| D3 | Deliverable | This document |

No further architect decisions are open. Two items previously listed here were resolved as
non-decisions:

- **Decode GIL ("coupling to S-B.2")** — not a decision. The synchronous decode is native C++
  and releases the GIL around the heavy work like every other native kernel; decode-on-pull
  removes the prefetch queue, so S-B.2's nogil-dequeue concern does not apply. See §3 Phase 2a.
- **Phase 3 (GCS fetch ring)** — not an architect choice; **evidence-gated**. Build only if the
  Phase 2 measurement shows GCS I/O starvation (local ClickBench won't). Resolved by data, not
  ratification. (It also depends on #1 Phase E cooperative `BLOCKED` — a cross-item prerequisite.)

---

## 7. Invariants preserved

- **`is_concurrent_pull_safe` stays the correctness gate**
  ([`parquet_read.pyx:954`]): decode must not change which mode is concurrency-safe.
  Two-pass LATMAT stays serial-pull; only single-pass parallelises — unchanged.
- **§11 vector contract**: decode produces the same Draken vectors regardless of *which*
  thread runs it; decode-on-pull must yield **byte-identical morsels** to today's prefetch
  path.
- **DOP=1 byte-identity** (#1's prime constraint) is unaffected — this changes *where* decode
  runs, never the rows.
- **No silent overrides** ([`feedback_never_silently_override_user_intent`]): Phase 4 may
  retire the default reservation, never an explicit user DOP.
- **No ship-behind-a-flag** ([`feedback_no_gating_as_an_answer`]): `FEATURE_PARQUET_THREAD_SCHEDULER`
  is a cutover stage, not a permanent off-by-default win.

---

## 8. Sequencing

1. **Phase 0** — reproduce baseline; attach before/after to each phase.
2. **Phase 1** — shared per-query decode pool; measure cores-used on a 2-table join.
3. **Phase 2** — 2a decode-on-pull (or 2b as a staging step); lift the single-scan ceiling.
4. **Phase 3** — only if GCS shows I/O starvation after Phase 2.
5. **Phase 4** — drop the reservation once the budget is real.
