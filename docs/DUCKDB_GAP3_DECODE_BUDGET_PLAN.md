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

#### Phase 2b mechanism, chosen — priority-tagged single pool (not a WIP-watching sidecar)

2b's open question was *how* a shared pool avoids decode starving behind a flood of
aggregate tasks (or vice versa). Ruled out: a separate service polling WIP counts and
re-ordering a queue — that's a feedback controller (needs damping/hysteresis to avoid
oscillation) solving a problem the vendored pool already solves structurally.

**Mechanism:** `third_party/bshoshany/BS_thread_pool.hpp` already ships this.
`light_thread_pool` (currently used by both the decode pool, `io_pipeline.hpp:835`, and the
exec `BSThreadPoolBridge`, `bs_pool_bridge.hpp:214`) is not a separate stripped-down
implementation — it is `using light_thread_pool = thread_pool<tp::none>;`
([`BS_thread_pool.hpp:1405`](../third_party/bshoshany/BS_thread_pool.hpp)), the *same*
template with the `tp::priority` feature flag off. With the flag on, the internal queue
becomes a real `std::priority_queue<pr_task>` ([`BS_thread_pool.hpp:2371`]) and every
submitted task carries an `int8_t` priority (`pr_task`, [`BS_thread_pool.hpp:407`]). The
pool always services the highest-priority *ready* task next — no monitoring loop, no
polling, no rebalancing step. Tag decode tasks high priority, aggregate tasks normal, done.

The starvation risk is asymmetric and both directions are already covered by *existing*
mechanisms, not new ones:
- **Decode starving aggregate** (decode floods the queue): decode submission is already
  rate-limited by `in_flight_limit = decode_workers + 2` ([`pool_reader.pyx:1327`]) — decode
  can never have more than a small, bounded number of tasks in flight+queued, so it cannot
  flood a shared queue even without priority.
- **Aggregate starving decode** (aggregate floods the queue): this is what priority actually
  fixes — decode tasks jump ahead of any queued aggregate backlog the instant a worker frees.

**Open, unproven, must be measured not assumed:** `std::priority_queue` push/pop is
O(log n) vs O(1) for the current `std::queue`, plus a per-task comparison. For row-group-scale
decode tasks (tens of ms) this is almost certainly noise; for anything finer-grained it might
not be. This is exactly the kind of claim [`feedback_profile_dont_hypothesise`] says to
measure, not assume — hence the prototype below.

#### Phase 2b prototype scope (agreed 2026-07-07, before any production wiring)

**Goal:** prove the priority-tagged single-pool mechanism removes the string-key decode
ceiling *in isolation*, on a synthetic task mix, before touching `io_pipeline.hpp` or
`pool_reader.pyx` ownership (the real wiring in §"Requires" above is a separate, larger
task gated on this prototype's result — same relationship Phase 0 has to Phase 1/2).

**What gets built (throwaway, `dev/`, never packaged, never imported by production code):**
- A small standalone Cython+C++ shim (`dev/_priority_pool_proto.pyx` +
  a tiny header) that instantiates `BS::thread_pool<BS::tp::priority>` directly — no changes
  to `io_pipeline.hpp`, `bs_pool_bridge.hpp`, or any production `.pyx`. Built by a one-off
  `setup.py`-style compile invoked from a `dev/` driver script, same convention as the
  now-cleaned-up `dev/gap3_decode_baseline.py`.
- A synthetic workload generator, not a real query: "decode-like" tasks (busy-loop calibrated
  to the measured cost profile of string decode — cheap-CPU int-like vs expensive-CPU
  string-like variants) submitted in bursts capped at `in_flight_limit`-style admission, and
  "aggregate-like" tasks (busy-loop calibrated to hashing/keying cost) submitted in a flood,
  to reproduce the exact contention shape Phase 0 measured on real `URL`/`SearchPhrase` GROUP
  BYs without needing the real engine wired up.

**Three configurations to compare head-to-head, same synthetic mix each time:**
1. **Baseline** — today's shape: two independent pools sized 16 (decode) + 8 (exec) on the
   same cores, no coordination.
2. **Unified, no priority** — one pool sized to core count, `tp::none` (plain FIFO). Isolates
   the effect of *just* merging the budget, no priority — this is the control that separates
   "unifying pools helps" from "priority specifically helps."
3. **Unified, priority** — one pool sized to core count, `tp::priority`, decode tasks tagged
   high / aggregate tasks tagged normal.

**Metrics (cores-used + latency, not wall-clock alone — same discipline as Phase 0):**
- Decode-task p50/p99 wait-to-start latency **under a heavy aggregate backlog** — should stay
  flat in config 3, and visibly degrade in configs 1 and 2 as backlog depth grows (this is the
  actual claim being tested).
- Total cores-used for the combined synthetic mix at each config, targeting config 3 ≤ core
  count (no oversubscription) with throughput ≥ config 1 (no regression from unifying).
- Raw submit/dispatch overhead per task at both task granularities, isolating whether
  `tp::priority`'s O(log n) queue costs anything measurable at decode-task (coarse) and
  aggregate-partition (finer) granularity.

**Gate to proceed to real wiring (2b build, or fold into 2a's decision):** config 3 must show
flat decode latency under backlog (the starvation claim holds) AND no measurable throughput
regression vs config 1 on this dev box. If either fails, report why and stop — do not wire
production code on a prototype that didn't clear its own gate.

**MEASURED (2026-07-07, [`dev/priority_pool_proto.cpp`](../dev/priority_pool_proto.cpp),
18-core dev box, standalone C++20 build, 3 runs, synthetic 100 decode-tasks-at-2ms/cap-10
vs 2000 agg-tasks-at-0.2ms-flooded — see file header for exact workload):**

| config | wall (ms) | decode p50 wait | decode p99 wait | cores-used |
|---|---|---|---|---|
| A — baseline, 2 pools (16+8) | ~51 | ~0.01ms | **~1.3–2.0ms** | ~11.8 |
| B — unified, no priority (FIFO) | ~43 | ~0.01ms | **~21.5–22.6ms** | ~14.1 |
| C — unified, `tp::priority` | ~34 | ~0.01–0.06ms | **~0.2–1.1ms** | ~17.8 |

v1's arbitrary constants (fixed 2ms decode / 0.2ms agg, register-only spin, one shape)
made priority look like a clean win on every axis. That's a mechanism proof, not a safety
proof — a synthetic shape can hide effects that only show up at real task durations. Built
a v2 to check.

**v2 — calibrated from THIS SESSION's real telemetry, two contrasting shapes, jittered
cost, memory-touching work body** ([`dev/priority_pool_proto_v2.cpp`](../dev/priority_pool_proto_v2.cpp)).
Task counts/costs/byte-sizes pulled directly from the `native_op_stats` captured earlier
this session (not estimated): **Q23-like** (`Title LIKE` + agg — decode-dominant, agg
near-free: scan 792 calls/6829ms total, agg 357 calls/13ms total) and **Q34-like**
(`GROUP BY URL` — agg-dominant: scan 396 calls/487ms total, agg 588 calls/8778ms total).
±40% jitter per task (row-groups aren't uniform size); work body streams/rewrites a
calibrated-size buffer (FNV-mix) instead of spinning in registers, so it contends for
memory bandwidth/cache the way real decode does. 2 runs, pattern stable both times:

| shape | config | wall (ms) | decode p50 wait | decode p99 wait | cores-used |
|---|---|---|---|---|---|
| Q23-like (decode-heavy, agg negligible) | A baseline | ~530–1220 | ~0.002ms | ~0.03ms | ~9.9 |
| | B unified, no priority | ~530–1220 | ~0.001ms | ~0.02–0.03ms | ~9.9 |
| | C unified, priority | ~530–1230 | ~0.002ms | ~0.02–0.03ms | ~9.9 |
| Q34-like (agg-dominant) | A baseline | ~815–1850 | ~0.005ms | ~0.6–0.9ms | ~8.4 |
| | B unified, no priority | **~425–950** | ~0.001ms | **~380–860ms** | ~17.4 |
| | C unified, priority | **~420–915** | ~1.3–2.1ms | **~6.1–9.8ms** | ~17.7 |

**Verdict is real, but no longer a clean sweep — this is exactly what v2 was for.**

- **Q23-like (decode-heavy): all three configs are indistinguishable.** With agg work this
  small (13ms total) there's no backlog to starve decode against, so unifying the pool
  neither helps nor hurts. Confirms priority is *safe* on the shape where it isn't needed —
  a real no-harm check v1 never ran (it only had one shape).
- **Q34-like (agg-dominant): priority is still a large, clear win over both alternatives on
  throughput** — wall time roughly **halves** vs today's two-pool baseline (e.g. 916ms vs
  1847ms), and cores-used climbs from ~8.4 to ~17.7 of 18 (baseline leaves cores idle;
  unifying doesn't). **And it decisively beats naive FIFO unification on decode latency** —
  p99 wait drops from ~380–860ms (FIFO starves decode almost completely under this backlog)
  to ~6–10ms (priority protects it).
- **BUT priority's decode p99 (~6–10ms) is measurably WORSE than the dedicated two-pool
  baseline's (~0.6–0.9ms) — a real, non-zero cost v1's tiny synthetic tasks were too short
  to expose.** Root cause, not a bug: `BS::thread_pool` reorders the *queue*, it cannot
  *preempt* a task already running on a worker. If a decode task arrives while all 18
  workers are mid-flight on a ~15ms aggregate task (Q34-like's real per-call cost), it must
  wait for a worker to free — bounded by roughly one in-flight aggregate task's length, not
  by queue position. v1's aggregate tasks (0.2ms) made this bound invisible; v2's real
  ~15ms aggregate tasks make it a measurable ~6–10ms tail.

**Reframed gate verdict: PASS for Q34-like (net win, decode tail degrades but stays an
order of magnitude below the FIFO failure mode and well below the 15ms aggregate-task
scale it's bounded by), PASS-BY-DEFAULT for Q23-like (no measurable difference either way).
Not a blocker, but a genuine, previously-invisible design input for the real 2b build**:
because the pool is cooperative not preemptive, decode's worst-case wait is bounded by the
*longest single in-flight aggregate task*, not by priority alone. That argues for bounding
aggregate task granularity (chunking large aggregate work into smaller sub-tasks) as a
companion to priority tagging, not priority as a complete fix by itself — worth carrying
into the real wiring design rather than assuming priority alone closes the gap. This is
also a natural fit: the shipped route-on-abandon aggregate already scatters work into
per-morsel/per-partition tasks ([[native_scheduler_rewrite_design]]); the real build should
check those units are small enough to keep this bound tight, not introduce new chunking
machinery from scratch.

**Gate PASSED, clearly, all three runs consistent (not noise):**
- **Starvation claim holds and inverts the risk.** Config B (unify-without-priority) is the
  one that actually starves decode — p99 wait balloons to ~22ms, ~10x the decode task's own
  cost, exactly the risk flagged before priority was proposed. Priority (config C) fixes it:
  p99 wait **drops below the baseline's own two-pool number** (~0.2–1.1ms vs baseline's
  ~1.3–2.0ms) — decode is better protected sharing one prioritised pool than it is with a
  whole pool nominally "reserved" for it.
- **No throughput regression — the opposite.** Config C's wall time (~34ms) beats both B
  (~43ms) and A (~51ms), and cores-used climbs to ~17.8 of 18 (~99%), vs baseline's ~11.8
  (~66%) — baseline leaves cores idle because its two pools can't share slack; one pool with
  ready work always keeps more workers busy.
- **The O(log n) priority-queue tax is not measurable at this scale.** Raw dispatch-overhead
  microbenchmark (task body = no-op) showed priority *faster* than plain FIFO by ~8–9% at
  both n=100 and n=2000 (almost certainly cache/allocator noise at these sizes, not a real
  priority-queue speedup) — either way, the concern that `tp::priority` costs something
  measurable at decode/agg granularity is not supported by this measurement.

**Verdict: cleared to proceed to the real 2b build** (or fold into weighing 2a vs 2b at
implementation time, per D2) — `thread_pool<tp::priority>`, decode tagged `pr::high`,
aggregate tagged `pr::normal`, is the mechanism. Caveat carried forward honestly: this is a
synthetic task-mix microbenchmark on one 18-core dev box, not the real engine on real
ClickBench data — the *next* gate is Phase 2's real-workload measurement (§0 Proof metrics,
Phase 2 row) once this mechanism is actually wired into `ParquetIOPipeline`/`BSThreadPoolBridge`.

**v3 — two more real shapes, chosen as predicted best/worst case, plus a third task
class (SORT)** ([`dev/priority_pool_proto_v3.cpp`](../dev/priority_pool_proto_v3.cpp)).
Picked **Q19-like** (`UserID, minute, SearchPhrase, COUNT(*)`) as the predicted *most
beneficial* case — decode is cheap (240ms) against a huge, well-reducing aggregate
(6626ms, 198M rows → 155M groups), a ~28x decode:agg imbalance (vs Q34-like's ~18x), so
more idle decode-pool capacity for unification to reclaim. Picked **Q33-like**
(`WatchID, ClientIP, COUNT(*)`, no filter) as the predicted *most degenerate* — near-zero
cardinality reduction (99M rows → 99M groups, no cheap-merge savings) plus a genuine third
pipeline stage, SORT, that also processes the full unreduced set (567ms/769 calls) — a
task class neither v1 nor v2 modelled. Sort tasks were added at `pr::normal` (same as agg
— only decode is upstream/latency-sensitive by design). 3 runs each, pattern stable:

| shape | config | wall (ms) | decode p99 wait | cores-used |
|---|---|---|---|---|
| Q19-like | A baseline | 796–1386 | 0.27–3.14ms | ~8.3 |
| | B unified, no priority | 410–733 | 387–684ms | ~17.5 |
| | C unified, priority | 397–721 | 2.6–4.4ms | ~17.8 |
| Q33-like | A baseline | 922–1615 | **0.09–0.15ms** | ~8.1 |
| | B unified, no priority | 456–804 | 443–789ms | ~17.8 |
| | C unified, priority | 448–802 | **1.5–8.4ms** | ~17.8 |

**The "most degenerate" prediction was only half right — worth reporting honestly rather
than forcing the story.** In absolute terms, Q33-like's decode tail under priority (up to
~8.4ms) lands in the same ballpark as Q19-like's (up to ~4.4ms) — not dramatically worse.
Both shapes show the same ~1.9–2.1x wall-time win and the same jump from ~8 to ~17.8
cores-used — the "beneficial" mechanism (reclaiming idle decode-pool capacity) shows up
about equally on both, which the raw ~28x vs ~18x imbalance predicted correctly.

Where the prediction *did* land: **relatively**, not absolutely. Q33-like's *baseline*
decode p99 is consistently tiny (~0.1ms — its own dedicated pool is barely touched, only
91ms of decode work total) while its *priority* p99 spikes as high as 8.4ms in one run —
close to a 100x relative jump, vs Q19-like's baseline-to-priority jump of roughly 10-15x.
So Q33-like is the shape where decode's tail latency degrades *by the largest multiple*,
even though the absolute millisecond figures end up comparable to Q19-like's. The extra
SORT task class didn't visibly break anything on its own (no new failure mode appeared),
but it didn't disprove the concern either — the added backlog volume is consistent with,
not distinct from, the general "more normal-priority backlog = larger decode tail" pattern
already established by Q34-like in v2.

**Net across all four real shapes tested (Q23/Q34/Q19/Q33): the mechanism holds up.** Every
agg-dominant shape shows a ~2x wall-time win and ~2x core-utilization win; the one
decode-dominant shape (Q23-like) is neutral; decode's tail latency under priority stays in
the single-digit-to-low-double-digit millisecond range across all of them — never
approaching the FIFO failure mode (hundreds of ms), and always paid for by a wall-time win
several times larger in absolute terms. No shape tested so far falsifies the Phase 2b
mechanism; the granularity-bound caveat from v2 stands as the one thing to verify in the
real build, not a newly discovered blocker.

**Explicitly out of scope for the prototype:** no changes to `ParquetIOPipeline` ownership,
no `FEATURE_PARQUET_THREAD_SCHEDULER` wiring, no real parquet scan involved. Those are the
2b *build* (§4 table), which this prototype gates but does not itself perform.

#### Phase 2b real build — design (2026-07-07)

**Architect decision: proceed with 2b (not 2a), directly — D2 resolved.** This section
supersedes "keep both open" for the purpose of this build; 2a (decode-on-pull) stays the
longer-term destination if 2b's ceiling proves insufficient, but is not being scoped now.

**Sequencing discovery that sets the shape of the build:** the exec pool
(`CppThreadPool`) is constructed at
[`compiler.py:1546`](../opteryx/managers/execution/compiler.py) — *after*
`compile_to_native(plan)` at line 1513, which is where scans (and therefore
`ParquetIOPipeline`) get built. There is nothing to inject into a scan yet at the point
scans are constructed today. This is not a two-file tweak; it is a construction-order
change plus a handle threaded through several call sites, so the build is staged to put
the riskiest, most self-contained piece first.

**Ownership model:** query-scoped, matching the exec pool's existing lifetime exactly —
one shared `BS::thread_pool<BS::tp::priority>` per query, constructed once (where
`CppThreadPool` is constructed today), handed to every scan the query opens, torn down
once the query completes (same point `CppThreadPool` is torn down today). Not
session-scoped, not process-wide — no cross-query sharing, no new lifetime class to
reason about beyond what already exists for the exec pool.

**Priority scheme (as prototyped and measured in v1–v3):** decode tasks →
`BS::pr::high`; every existing exec-pool task (aggregate readout, sort, join probe,
window, distinct) → `BS::pr::normal`, unchanged from today's implicit FIFO-equivalent
priority. No new priority tiers, no per-operator tuning — matches exactly what was
measured, nothing invented beyond the prototype.

**Standalone-rugo compatibility (non-negotiable — rugo/ stays opteryx-free per
[[feedback_parquet_draken_purity]]):** `ParquetIOPipeline`'s existing
`(int decode_workers, size_t queue_capacity)` constructor is KEPT, unchanged, for the
standalone `rugo` wheel and any caller that doesn't inject a pool — it still
self-constructs its own priority-capable pool internally (harmless: a pool with only
one priority in use behaves identically to `tp::none`). A NEW constructor overload is
*added*, not a replacement. `BS::thread_pool<tp::priority>` is a vendored third-party
type (`third_party/bshoshany/`), not an opteryx type — accepting a handle to it doesn't
create an opteryx dependency in rugo.

**Staged build, riskiest/most self-contained piece first:**

**Step 1 — `ParquetIOPipeline` ownership model, in isolation. No `compiler.py` changes.**
- `rugo/src/parquet/io_pipeline.hpp`: change `decode_pool_`'s type from
  `shared_ptr<BS::light_thread_pool>` to `shared_ptr<BS::thread_pool<BS::tp::priority>>`
  (same underlying vendored template, feature flag on — see the mechanism section
  above). Add `bool owns_pool_` (default `true`).
- New constructor: `ParquetIOPipeline(shared_ptr<BS::thread_pool<BS::tp::priority>> pool, size_t queue_capacity = 256)` — takes ownership of nothing, sets `owns_pool_ = false`.
- `submit_row_group` (both overloads): `detach_task(..., BS::pr::high)` instead of the
  default-priority call — this is the one-line mechanism landing, gated behind the
  rest of the ownership fix so it isn't tested in a vacuum.
- **The correctness-critical fix:** `wait_shutdown()` currently calls
  `decode_pool_->wait()`, which blocks until **every** task in the pool finishes — safe
  today because the pool is exclusive, wrong once it's shared (would block a scan's
  shutdown on an unrelated aggregate task, or another query's work, finishing first).
  Replace with a condition-wait on the existing `pending_work_` atomic reaching zero
  (already incremented/decremented per submitted/finished row-group — it exists for
  the cancellation path, just isn't the drain condition today). Only call
  `decode_pool_->wait()` when `owns_pool_` is true (the standalone-constructor path,
  where waiting for "everything in the pool" is correct because the pool is exclusive).
- **Gate:** a new unit test proves the shared-pool path does NOT block on unrelated
  work — construct a shared pool, saturate it with long-running non-decode tasks,
  submit a `ParquetIOPipeline` with a few short decode tasks against the *same* pool,
  assert the pipeline's `wait_shutdown()` returns once its OWN tasks finish, not when
  the unrelated backlog drains. This is the one thing v1–v3's synthetic prototypes
  never verified (they had no real `ParquetIOPipeline` in them at all) and the one
  most likely to hide a real bug if skipped.
- Existing standalone-constructor path: unchanged behaviour, `make q` + rugo's own
  test suite must stay green with zero deltas (this step must be invisible to every
  caller that doesn't opt into pool injection).

**✅ Step 1 LANDED (2026-07-07).** `rugo/src/parquet/io_pipeline.hpp`: `decode_pool_`
retyped to `shared_ptr<BS::thread_pool<BS::tp::priority>>`; `owns_pool_` flag added
(default `true`); new injecting constructor `ParquetIOPipeline(shared_ptr<...> pool,
size_t queue_capacity=256)` added alongside the unchanged standalone
`(int decode_workers, size_t queue_capacity)` constructor; both `submit_row_group`
overloads now `detach_task(..., BS::pr::high)`; `wait_shutdown()` branches on
`owns_pool_` — `true` keeps the original `decode_pool_->wait()`, `false` drains on
`pending_work_` reaching zero via the existing `queue_mutex_`/`queue_cv_` (every
decrement site already notified it — no new synchronisation primitive added).

Gates: `make c` clean compile, zero errors. `make q` 190/190. A real GROUP BY query
against `scratch.hits_rugo_262k` run end-to-end through the modified (but
still-standalone-constructor, `owns_pool_=true`) path — correct results, confirming
the retype to a priority-capable pool is behaviourally inert when only one priority is
ever used, exactly as designed.

The injected-pool path isn't reachable through Python yet (Step 2 wires that) — the
real `ParquetIOPipeline` also can't be exercised standalone for this specific test,
since `decode_row_group`'s body links against rugo's separately-compiled decode/
compression `.cpp` sources, not header-only. So the shared-pool shutdown-drain
correctness property was proven via
[`dev/shutdown_drain_proto.cpp`](../dev/shutdown_drain_proto.cpp) — a throwaway C++
test reproducing the EXACT synchronisation pattern now in `wait_shutdown()` (same
`pending_work_` atomic, same `queue_mutex_`/`queue_cv_`, same notify-per-decrement),
comparing old (`pool->wait()`) vs new (drain-own) behaviour on a shared pool
backlogged with 40 unrelated 50ms tasks against a pool of 4 workers (expected old
behaviour: blocks ~500ms) plus 5 near-instant decode tasks:

| | wait time | |
|---|---|---|
| OLD (`pool->wait()` on the shared pool) | 500.0 / 500.1ms | blocks on the whole unrelated backlog — the bug |
| NEW (drain own `pending_work_`) | 50.0 / 0.0ms | near-instant, backlog still genuinely running in the background |

2 runs, both confirm: NEW is 10-500x faster than OLD on this shape, and critically the
backlog was still in flight when NEW returned (verified by then calling `pool->wait()`
separately to drain it) — proving the fix, not just a coincidentally-fast measurement.

**Step 2 — wire it into the query engine. Only starts once Step 1's gate is green.**
- `src/cpp/bs_pool_bridge.hpp` / `opteryx/compiled/thread_pool.pxd`: switch
  `BSThreadPoolBridge`'s internal pool to `BS::thread_pool<BS::tp::priority>`; add a
  `priority` parameter (default `BS::pr::normal`) to `submit_native`; add an accessor
  returning the raw `shared_ptr<BS::thread_pool<BS::tp::priority>>` (e.g.
  `pool_handle()`) so the handle can be extracted and handed to rugo — `CppThreadPool`
  (Cython) gets a matching `cdef` accessor.
- `compiler.py`: move `pool = CppThreadPool(dop, "engine")` to construction time
  *before* `compile_to_native(plan)`, and thread `pool` (the existing Python-visible
  `CppThreadPool` instance — no new cross-language object needed) as a parameter into
  `compile_to_native` and down into whichever of the scan-construction call sites in
  `pool_reader.pyx` actually fire.
- `pool_reader.pyx`: the ~6 call sites that build `ParquetIOPipeline` gain an optional
  `CppThreadPool pool=None` parameter (Cython, typed via `cimport` of
  `thread_pool.pxd`, so the C++ handle crosses the .pyx/.pyx boundary directly, no
  Python-level marshaling of a raw pointer). When provided, call
  `pool.pool_handle()` and use Step 1's new injecting constructor; when `None`
  (EXPLAIN-only paths, tests that construct scans directly, any caller outside the
  main query-execution path), fall back to the existing self-constructing constructor
  — unchanged behaviour.
- **Gate:** `make q` 190/190 at DOP=1 (byte-identical — this must not change a single
  answer), `test_concurrency_guard.py` green (no lost concurrency), then the real
  before/after this whole arc has been building toward: run Q19/Q23/Q33/Q34 on the
  actual engine, not the synthetic proxy, and compare against both the DOP-cap-only
  baseline (already shipped) and the synthetic prototype's predictions. This is the
  first point in the whole investigation where a number stops being "the mechanism
  proven in isolation" and becomes "the real measured win."

**⛔ Step 2 PLUMBING LANDED. TWO real bugs found live-testing it — one FIXED, one is
a genuine structural design flaw, still open (2026-07-07).** Everything in Step 2's
bullet list above was built exactly as scoped: priority-capable `BSThreadPoolBridge` +
`pool_handle()` accessor (`src/cpp/bs_pool_bridge.hpp`,
`opteryx/compiled/thread_pool.pxd`/`.pyx`, using a new `PriorityPool` type alias so
Cython can name the non-type-template-parameter instantiation), the injecting
`ParquetIOPipeline` constructor overload declared in `pool_reader.pxd`, `pool`
threaded as an optional parameter through `open_native_scan_plan` →
`_native_scan_plan` → `_Compiler.__init__` → `compile_to_native`, and the exec pool's
construction in `execute_native` moved before `compile_to_native` so a handle exists
at scan-compile time. `make c` compiles clean, zero errors.

**Bug #1 (FOUND + FIXED) — `-std=` ABI mismatch across `.so` boundaries.** Turning
the wiring on segfaulted the first query, inside `std::priority_queue::emplace`
(called from `ParquetIOPipeline::submit_row_group`'s `detach_task`). `setup.py`
already carried a comment on the `_operators` Extension (line ~655) warning about
exactly this class of bug from a PRIOR incident: "BS::thread_pool cross-.so ABI
mismatch... caused by differing -std=/feature-macro flags." `grep`ping confirmed it:
`thread_pool.pyx`'s Extension hardcoded `extra_compile_args=["-O3", "-std=c++17"]`
while every other extension touching `PriorityPool` (`pool_reader`, `_operators`)
compiles at `-std=c++20` via the shared `CPP_FLAGS`. `BS_thread_pool.hpp` branches
internally on `__cplusplus`/`__cpp_lib_move_only_function` (which `move_only_function`
implementation it uses), so a `-std=c++17`-compiled `PriorityPool` has a genuinely
different memory layout than the `-std=c++20`-compiled one every other `.so` expects.
**Fix:** `thread_pool`'s `extra_compile_args` changed to `CPP_FLAGS` (matching
everything else), with a comment recording why. Rebuilt (had to `touch` the sources —
an `-std=` flag change alone doesn't trigger setuptools' incremental rebuild), and the
crash was GONE: `make q` 190/190, real queries ran correctly with the pool genuinely
shared. `morsel_queue`/`http_client` still hardcode `-std=c++17` too — not touched
(neither shares a `__cplusplus`-conditional-layout type across `.so`s the way
`PriorityPool` does, as far as known; worth a wider audit some day, out of scope here).

**Bug #2 (FOUND, NOT FIXED) — reentrant thread-pool self-deadlock. Structural, not a
residual implementation bug.** With bug #1 fixed, `make q` hung completely (not a
crash — zero CPU, no progress) specifically at `MAX_EXECUTION_WORKERS=1`. `lldb -p
<pid> -o "bt all"` on the hung process showed the real mechanism precisely:
- The pipeline driver thread is blocked in `BSThreadPoolBridge::wait_native()` →
  `pool->wait()`, waiting for **every** task in the shared pool to finish.
- The pool's *only* worker (DOP=1) is itself blocked, mid-task, inside
  `NativeParquetScanSource::get_morsel` → `ParquetIOPipeline::wait_and_get_result()`
  — waiting for a **decode task submitted to that same pool** to complete.
- That decode task can never run: there is no free worker to service it — the only
  worker is the one blocking on it.

This is a textbook reentrant-pool deadlock: a task running on a pool (the exec
worker) cannot safely **block** waiting for another task submitted to that *same*
pool (decode) unless a worker is guaranteed free to service it. `bs_pool_bridge.hpp`
already documents this exact class of hazard (`spawn_detached_native_task`'s
docstring, about a driver recursively submitting to its own pool causing a prior
SIGSEGV) — Step 2 reintroduces it via a different path: exec workers doing a
*blocking* scan-pull, of decode work now living on the same pool they run on. **Not
a DOP=1-only issue** — higher DOP has more slack so it's statistically less likely,
not actually safe; a query with enough simultaneously-blocking scan-pulls across
workers could saturate a shared pool into the same state at any DOP.

**This reframes the whole mechanism, not just this build.** None of the v1–v3
synthetic prototypes (§Phase 2b prototype scope above) could have caught this — they
modeled a separate producer thread submitting decode+agg tasks, never a pool *worker*
blocking on a task submitted to its own pool. The prototypes proved the priority
*scheduling* mechanism works; they never modeled the real engine's blocking scan-pull
call shape, which is where the actual danger turns out to live. A real fix needs
either reserved decode-only capacity carved out of the shared pool (so exec workers
can never fully starve it), or the 2a "decode-on-pull" model — decode runs
synchronously on the calling thread, no cross-task blocking wait at all, sidestepping
this class of bug by construction — not a local patch to the current design.

**Current tree state (honest, not silently broken):** `pool=None` is hard-coded at
the one call site in `_native_scan_plan` (`compiler.py`), with a comment recording
both bugs and pointing here. `self._pool` exists on `_Compiler` and is threaded
correctly everywhere else — the plumbing (and bug #1's fix) are real and kept, not
reverted; only the actual `pool=self._pool` hand-off is disabled. `make q` 190/190,
`make c` clean, no hung processes, on this state (re-verified after the revert).
Bug #1's fix (`thread_pool`'s `-std=` correction) is independently correct and worth
keeping regardless of Step 2's fate — it's a latent ABI landmine for anything else
that ever shares a `PriorityPool`/`BS::thread_pool` handle across these `.so`
boundaries, not something specific to this feature. NOT YET DONE: DOP=1
byte-identity re-check, `test_concurrency_guard.py` re-run, and the real
Q19/Q23/Q33/Q34 before/after — none meaningful until bug #2 has a real design fix,
not just a revert.

**Explicitly not in this build:** 2a (decode-on-pull) is not being scoped or built now.
`FEATURE_PARQUET_THREAD_SCHEDULER` stays unwired (superseded by this design, not
activated by it). GCS/Phase 3 fetch-ring work is untouched. Sort and every other
exec-pool task type get `pr::normal` uniformly — no per-operator priority tuning, that
was never measured and isn't part of what's being shipped.

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

---

## 9. Session outcome (2026-07-07) — what shipped, what's parked, what it unlocks

Phase 2b was built end-to-end, measured on real workloads, and **pool sharing was
disabled** (`pool=None` in `_native_scan_plan`) — it was neutral-to-slightly-negative
on every realistic payload (positive only under absurd decode-worker over-provisioning;
−68% at DOP=1). Root finding: the premise doesn't hold — decode never contends for
cores. It is **memory-bandwidth-bound** (wide-string decode maxes ~4.7 cores) or
**idle** (narrow decode on agg-heavy queries); it is never the CPU-core hog the shared
budget was meant to tame. The bottleneck is the **aggregate** (10–80× decode self-time;
keying/probe is DRAM-latency-bound). See §Phase 2b measurement tables above.

### 9.1 Kept and shipped (independent wins)

- **`_MAX_WORKER_CAP` 8 → 16** (`compiler.py`). Real ~1.1–1.7× on agg-heavy ClickBench
  queries on >10-core boxes; **zero prod impact** — the cap never binds at the 8-vCPU
  target (`min(cpu-2, 16)` = `cpu-2` there). Shipped, gated (`make q` 190/190).
- **thread_pool extension `-std=c++17` → `CPP_FLAGS` (c++20)** (`setup.py`). Fixes a real
  latent ABI-mismatch: `BS::thread_pool` branches on `__cplusplus`, so a `PriorityPool`
  compiled at a different `-std` than the `.so`s it's shared with had a divergent memory
  layout → segfault in `std::priority_queue::emplace`. This is a **general** safety fix
  for *any* cross-`.so` sharing of a `BS::thread_pool`, not specific to this feature.
  Keep regardless of Phase 2b's fate.

### 9.2 Kept but dormant (parked capability — sharing OFF)

The C++ plumbing remains compiled in; with `pool=None` the injecting path is never
taken. Decision: **keep-both** (the injection seam and the deadlock-safe help-loop are
inseparable — injection without the help-loop re-arms the reentrant-pool deadlock, so it
is keep-both or drop-both; kept). What's parked:

- **Priority-capable pools** (`thread_pool<tp::priority>` via the `PriorityPool` alias)
  across the exec `BSThreadPoolBridge` and rugo's `ParquetIOPipeline`, with a
  `priority` parameter on `submit_native` and `pr::high` on decode submits. Behaviorally
  identical to the old pools while only one priority is used.
- **Injectable decode pool** — `ParquetIOPipeline`'s injecting constructor + `owns_pool_`
  + `BSThreadPoolBridge::pool_handle()` + the `pool` parameter threaded through
  `open_native_scan_plan` → `_native_scan_plan` → `compile_to_native` → `execute_native`
  (which now constructs the exec pool *before* compilation so a handle exists in time).
- **Deadlock-safe help-loop / claimable-queue** in `ParquetIOPipeline`: `submit_row_group`
  enqueues a claimable `WorkItem` + dispatches a claiming ticket; a puller blocked in
  `wait_and_get_result` decodes a pending item itself rather than stall; `tickets_inflight_`
  gives shared-pool teardown a spin-drain (no notify-after-free). Correct and gated.

Loose end: an unused `inline_decodes_` counter (never wired to telemetry) — dead, remove
on next touch. Also: the C++ diffs are entangled with pre-existing uncommitted work on
these files, so a full drop-both revert needs a `git diff` carve first.

### 9.3 Opportunities this creates

The parked infra + the diagnosis are a down-payment on the *actually*-useful directions,
not the (shelved) shared-budget one:

1. **The help-loop is 80% of 2a (decode-on-pull).** 2a is "the pulling thread decodes";
   `wait_and_get_result`'s help-loop already does exactly that when no result is ready.
   If 2a is ever revisited (for remote/GCS where the decode/fetch profile differs, or for
   architectural collapse of the two-pool model), the hardest primitive already exists and
   is deadlock-safe.
2. **Task prioritization is now available engine-wide** (priority pools + `submit_native`
   priority arg). Any future latency-sensitive task ordering (not just decode) has the
   mechanism ready — it's a general scheduler capability now, not a one-off.
3. **Cross-`.so` `BS::thread_pool` sharing is now safe** (the `-std` fix). Removes a
   latent landmine for any future work that hands a pool/task across extension boundaries.
4. **The real levers are now mapped, not guessed** (this is the biggest opportunity).
   Measured: ClickBench is ~73% aggregate / ~15% decode; decode is bandwidth-bound-or-idle;
   the aggregate keying/probe is DRAM-latency-bound. So the changes that would actually
   move the metric are **(a) cache-resident radix aggregation** (partition so partial
   tables fit L2 — DuckDB's design; attacks the 73% and relieves the DRAM pressure that
   caps everything) and **(b) lazy/late string materialization** (decode fewer wide-string
   bytes → free memory bandwidth). Threading/pool changes are the wrong lever and are now
   ruled out with data. Notably, cache-resident agg would *also* retroactively make pool
   sharing pay off — it flips the two sides onto different bottleneck resources (agg→cache,
   decode→DRAM) so overlapping them finally helps.

**Next-effort recommendation:** point at the aggregate (start with a perf-counter check
to confirm the keying loop is cache/DRAM-bound, then radix aggregation), not at more
decode/scheduler threading. See the ClickBench aggregate findings in the
`q16_review_findings` / `q33_keystore_quadratic_growth` memories for prior groundwork.
