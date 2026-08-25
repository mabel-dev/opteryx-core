# Execution Profiling — gap analysis against Impala's runtime profile

Status: **design note, pending architect ruling. Nothing built.**
Scope: what our execution profiling should capture next, benchmarked against
Impala's runtime-profile model.

Out of scope (separate threads from the same Impala review): the
reservation-based buffer pool, runtime min/max filters, Parquet dictionary and
page-index pruning, vendoring Impala's decode headers.

Companion documents: [`EXECUTION_TRACING_DESIGN.md`](EXECUTION_TRACING_DESIGN.md)
(the span tracer), [`ENGINE_INSTRUMENTATION.md`](ENGINE_INSTRUMENTATION.md)
(the WP-INSTR GIL/purity harness).

---

## 0. Headline

**The active/wait split is not our biggest hole — it is our biggest
*unsurfaced* measurement.** We already record, always-on, per plan node, both
wall time (`exec_ns`) and thread-CPU time (`cpu_ns`); the difference is
blocked/descheduled time. Impala needs an elaborate wait-timer scheme partly
because it has no equivalent per-operator CPU clock. We have one and do not
print it.

The genuine blind spots are narrower and more specific than "active vs wait":

* `Sink::combine()` and `Sink::finalize()` are timed by **nothing** — not
  `OpStats`, not a span. For a hash aggregate that is where the cross-worker
  merge and the result construction happen.
* Worker **straggler skew** at the pipeline barrier is unmeasured.
* **Peak memory per node** does not exist in any form.

So the ranking below puts *surfacing what we already measure* first, then
closing the three structural holes, and challenges the assumption that a new
active/wait subsystem is the top item.

---

## 1. What we capture today (verified against the tree, not assumed)

The codebase keeps **two separate channels** and they must not be conflated.

### 1.1 Telemetry — always-on aggregates, every query

Backbone: `opteryx/models/query_telemetry.py` (`QueryTelemetry`), surfaced via
`Session.telemetry`.

| Reading | Where it is produced | What it is |
|---|---|---|
| `native_op_stats` | `OpStats`, [`operator.hpp:50-70`](../src/cpp/engine/operator.hpp) | per-plan-node `records_in/out`, `bytes_in/out`, `calls`, `execution_time` (wall ns), `cpu_time` (thread-CPU ns) |
| `native_pipeline_stats` | [`engine.hpp:1140-1144`](../src/cpp/engine/engine.hpp) | per-pipeline `wall_time`, `cpu_time` (process CPU), `dop` |
| `native_scan_facts` | scan sources | files/row-groups read + pruned, columns, pre-filter rows |
| `io_scan_diagnostics` | `pool_reader.pyx:700-715` | **per scan**: `bytes_fetched` (real compressed IO), `http_request_count`, `http_retries`, `worker_blocked_ns`, `queue_high_watermark`, inline decodes |
| `billing_bytes` | `planner/data_processed.py` | plan-time dense **logical** bytes (the billing meter — not an IO measurement) |
| `gil_held_ns`, `worker_gil_sites` | WP-INSTR, `OPTERYX_INSTRUMENT_ENGINE` | execution-path Python re-entry |
| `scan_sources`, `scan_residual_reasons` | plan time | which Source each scan got, and why |

Counters are bracketed **per morsel, never per row**, at three sites in the
drive loop:

* sink — [`executor.hpp:108-117`](../src/cpp/engine/executor.hpp)
* operator — [`executor.hpp:128-137`](../src/cpp/engine/executor.hpp)
* source pull — [`executor.hpp:155-163`](../src/cpp/engine/executor.hpp)

Harvest is **one Python crossing at teardown**:
`NativePlan.collect_op_stats()` → `compiler.py:5378-5412`, folded by plan-node
identity (last-row-wins for the terminal stage; `calls`/times summed).

`exec_ns` is **SELF time** — the recursive downstream forward is excluded.
It is summed across every `dop` worker, so it is not comparable to wall clock.

### 1.2 Trace — opt-in event stream, only when armed

Armed by `OPTERYX_TRACE=1` or `SET trace TO true`
(`opteryx/variables.py:165`). Native span recording, one Python crossing at
teardown, delivered on `Session.trace()` as a `TraceBundle` — **never** on
telemetry (that separation was an explicit architect correction; see
`EXECUTION_TRACING_DESIGN.md` §9a).

* Primitive: `draken/core/trace.hpp`, reached through the cross-`.so` bridge
  `draken/core/trace_bridge_c.h` (one compiled home in `draken_native.so`).
* Categories live in one place, `trace_bridge_c.h:37-52`.
* **Emitted today:** `TC_SOURCE_PULL`, `TC_OP_EXEC`, `TC_SINK`
  (`executor.hpp`); `TC_IO_WAIT` (`native_parquet_scan_source.hpp:805`,
  `native_latmat_scan_source.hpp:259,466`); `TC_QUEUE_WAIT`, `TC_IO_REQUEST`,
  `TC_DECODE` (`io_pipeline.hpp:2520-2531`); `TC_QUEUE_STALL`
  (`io_pipeline.hpp:2836`).
* **Declared but never emitted:** `TC_COMBINE`, `TC_BUFFER_RESIDENT` (no such
  stage exists), `TC_DECODE_PHASE`.
* Interpretation: `opteryx/tracing/spans.py` (wire format → records),
  `opteryx/tracing/timelines.py` (chart-shaped views). `dev/io_waterfall/`
  renders.

`TC_IO_WAIT` is already a **nested sub-span inside `TC_SOURCE_PULL`**, and
`timelines.py:216-228` already reports `io_wait_ns` on its own axis per node —
i.e. an active/wait split for the scan exists in the trace channel today.

### 1.3 How it reaches a user

**`EXPLAIN ANALYZE` already is an ExecSummary.** Built at
`serial_engine.py:330-460`; run against a real query it produces:

```
tree                          | details                          | est_rows | est_bytes |  rows | time_ms | self_ms
Heap Sort                     | LIMIT = 5, ORDER = COUNT(*) DESC |        5 |        97 |     5 |   0.025 |   0.025
└─ Grouped Aggregate (Hashed) | AGGREGATE (COUNT(*)) GROUP BY .. |    99998 |   1942933 | 63557 |   4.515 |   4.515
   └─ Parquet Read            | testdata/flat/formats/parquet    |    99998 |   2742917 | 75730 |   9.705 |   9.705
```

plus `OPTIMIZATIONS` and `REWRITE TRACE` blocks. Also available:
`opteryx/utils/mermaid.py` (per-node overlay, incl. `cpu_time_ms`) and the raw
`Session.telemetry` dict.

---

## 2. Item by item against Impala

### 2.1 Active time vs wait time, per operator — **have it, don't show it**

**Where our blocking points actually are.** Read from the executor, not
assumed:

| Blocking point | Site | Instrumented today? |
|---|---|---|
| Source blocked on the IO pipeline | `native_parquet_scan_source.hpp:805` (`wait_and_get_result`) | **Yes** — `TC_IO_WAIT` span (trace); folded into `exec_ns` and excluded from `cpu_ns` (telemetry) |
| IO consumer with nothing to do | `io_pipeline.hpp:2836` (`queue_cv_.wait`) | **Yes** — `TC_QUEUE_STALL` span; also `worker_blocked_ns` in `io_scan_diagnostics` |
| Item queued, no worker yet | `io_pipeline.hpp:2520` | **Yes** — `TC_QUEUE_WAIT` span |
| Sink blocked on downstream consumer (bounded `MorselQueue`, `morsel_queue.hpp:67`) | `executor.hpp:115` | **Partly** — inside the sink's `exec_ns`, not separable; `cpu_ns` reveals it as blocked |
| `Sink::combine()` — cross-worker merge | [`executor.hpp:174`](../src/cpp/engine/executor.hpp) | **No. Nothing at all.** |
| `Sink::finalize()` — result construction | [`executor.hpp:202`](../src/cpp/engine/executor.hpp) | **No. Nothing at all.** |
| Worker barrier / straggler skew | `executor.hpp:197`, `241` | **No** |
| Build side of a join | — | **Not a wait.** `engine.hpp:run()` executes pipelines **strictly one at a time, in creation order**. The build pipeline completes before the probe pipeline starts. Impala's "blocked on the build side" category has no analogue here and should not be ported. |

**Measured, on a real query** (`GROUP BY user_name` over
`testdata/flat/formats/parquet`, this box):

```
node        exec_ms   cpu_ms   rows_in   rows_out
6cU376pf      6.593    6.593         0     100000   scan   — CPU-bound
8mltsKlo      6.034    6.026    100000      83606   agg    — CPU-bound
yISbV6t4     27.339    2.195     83606          0   exit   — 92% BLOCKED
```

The exit node spent 92% of its wall time blocked on consumer backpressure.
That is exactly the disambiguation the architect asked for, it is already in
telemetry, and **`EXPLAIN ANALYZE` prints neither column** — it prints
`time_ms` and `self_ms`, which are the same number on the native path (a
documented no-op, `serial_engine.py:341-345`).

**Verdict: have it. The work is surfacing, not measuring** — plus closing
combine/finalize/skew.

### 2.2 Peak memory per operator node — **do not have it, and it is not cheap**

Nothing in the tree tracks peak memory per node. What exists:

* Per-aggregate **budget** counters — `array_agg_budget_used()`
  (`native_group_sinks.hpp:999`), `emit_budget_used()`
  (`native_cidr_emit.hpp:69`), median's equivalent — charged against the
  ceilings in [`agg_budgets.hpp`](../src/cpp/engine/agg_budgets.hpp). These are
  **process-global atomics, not per-node, not per-query high-water marks**.
  They are admission control, not accounting.
* `bytes_in`/`bytes_out` on `OpStats` are **a formula, not a measurement**:
  `rows * columns * 8` (`executor.hpp:68-72`). They cannot stand in for memory.
* mimalloc v3.3.2 is vendored (`build_common.py:156-184`) as a preload library,
  so `mi_process_info()` could give a **process** high-water. That is a
  query-level number at best; it cannot attribute to a node.

This is the item with a real dependency chain: it connects to the open
buffering-aggregate memory guards, and any future reservation-based memory
model would want to be the thing that reports it rather than a parallel
mechanism bolted alongside. **That model is explicitly not designed here** —
noted as a dependency, not resolved.

**Verdict: worth having, but it needs a ruling on the unit of account
(D2) before anyone writes code.**

### 2.3 Compact per-query summary — **largely have it; add four columns**

`EXPLAIN ANALYZE` is our ExecSummary and it fits on a screen.

**The estimated-vs-actual column is already there and already earning its
keep** — in the sample above, the scan estimated 99,998 rows and delivered
75,730, and the aggregate estimated 99,998 groups against 63,557. The
correlation is free: `create_physical_plan`
(`opteryx/planner/physical_planner/__init__.py`) preserves the logical plan's
`nid` 1:1, so `statistics_refresh`'s per-node estimate lines up with the
executed node with no extra identity mapping.

**One honesty problem in the existing surface.** `refresh_statistics` runs
*opportunistically* (only when a strategy asks, plus `result_size_guard`), and
`_est_row_count` (`serial_engine.py:359`) returns `0` for a node it never
reached. `0` is also a legitimate estimate. The column cannot today distinguish
"estimated zero rows" from "never estimated" — a proxy-state ambiguity. Needs
D3.

Missing columns, all of which we already hold: `cpu_ms`, `wait_ms`, `dop`,
`peak_mem`.

**Verdict: cheapest high-value change in this document.**

### 2.4 Time-series counters — **decline**

Recommend **not** building sampling.

* Impala samples because a query fans out over a cluster and the coordinator
  cannot otherwise see a remote fragment's shape over time. We are a single
  process, and pipelines run **one at a time** (`engine.hpp:run()`). The
  temporal structure Impala reconstructs by sampling, we can observe directly.
* When tracing is armed we already emit a **full event stream with real
  start/end timestamps**. Any time series Impala samples — bytes read over
  time, rows/sec, concurrency — is *derivable by post-processing spans*, at
  full fidelity rather than at sample resolution. `timelines.py` already does
  exactly this for IO concurrency via a sweep line (`_max_concurrent`).
* A sampler needs a thread that wakes on a timer and reads counters other
  threads are writing. That is a new concurrency surface on the execution path
  for a signal we can already reconstruct. The tracer has burned us twice on
  exactly this class of thing (dangling thread-local arenas, `corr_id`
  collisions — `EXECUTION_TRACING_DESIGN.md` §9b, §9d).

The one thing spans genuinely cannot reconstruct is **memory over time**,
because we do not sample memory at all — but that is §2.2's problem, and the
answer there is a high-water mark, not a series.

**Verdict: no. Derive from spans; do not sample.**

### 2.5 Everything else in their model

| Impala item | Us |
|---|---|
| Rows in / rows out per node | **Have.** `records_in`/`records_out`, always-on, and `rows` in `EXPLAIN ANALYZE`. Selectivity falls straight out. |
| Bytes read vs bytes decoded | **Half.** Bytes read is real and per-scan (`io_scan_diagnostics.bytes_fetched`, compressed at the wire). Bytes *decoded* is not measured — `bytes_out` is the `rows*cols*8` formula. Two numbers named alike, one measured and one modelled. |
| Spill counters | **Decline — nothing spills.** `SORT_SPILL_DESIGN.md` is a design; `grep spill src/cpp/engine` returns nothing. A spill counter that is structurally always zero is worse than no counter. |
| Thread counts | **Have, unsurfaced.** `dop` per pipeline in `native_pipeline_stats`; absent from `EXPLAIN ANALYZE`. |
| Per-fragment/host breakdown | **N/A.** Single process. |

---

## 3. Measured costs (this box only)

Baseline established before proposing anything, on the dev box
(Darwin 25.5.0, Apple Silicon), 1M-iteration loop, `-O2`:

| Clock | Cost / read | Effective granularity |
|---|---|---|
| `clock_gettime(CLOCK_MONOTONIC)` | **26.1 ns** | **1000 ns** |
| `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` | **107.5 ns** | 83 ns |

Two consequences, both load-bearing:

1. **The engine already pays ~266 ns per stage per morsel** in always-on clock
   reads (2× monotonic + 2× thread-CPU at each of the three bracket sites).
   That is the established, accepted price of an always-on counter here, and it
   calibrates every proposal below: anything at or under per-morsel granularity
   is inside a budget the codebase already pays.
2. **1 µs is the wall-clock floor on this platform.** Sub-microsecond spans
   quantise to 0 or 1000 ns — visible in the trace, where short source pulls
   report exactly `duration_ns: 1000`. **This rules out per-kernel or
   per-column timing** as a proposal: the measurement would be mostly
   quantisation noise. Per-morsel is the right granularity and happens to
   already be where we are.

Not measured, and not claimed: Linux/x86 vDSO clock costs (likely cheaper,
~20-25 ns for both, but unverified here); the end-to-end overhead of any
proposal in §4 — none of them exist yet.

**One further measurement, reported with its caveat.** On a 100k-row
`ORDER BY` with `dop=16`, spans covered **2.6%** of the available worker-time
in the query window (6.6 ms covered of 251 ms available; window 15.7 ms ×
16 workers). Read honestly: this query is far too small to fill a 16-worker
pool, so most of that 97.4% is genuine idleness, **not** hidden work. It is not
evidence that combine/finalize are expensive. It *is* evidence for the thing
this document keeps returning to: **nothing we record today attributes a
worker's idle time to a cause**, and on a query where that idleness is a real
bottleneck rather than an artefact of scale, we would have no way to tell the
difference from this data.

---

## 4. Prioritised recommendations

Ordered by (value ÷ cost), not by Impala's ordering.

### P1 — Surface the active/wait split we already record
**What.** Add `cpu_ms`, `wait_ms`, and `dop` columns to `EXPLAIN ANALYZE`.
**Where.** `opteryx/managers/execution/serial_engine.py:330-460` (the column
builders and the `columns`/`vectors` lists at `:450-460`). Data source is
`native_op_stats[identity]["cpu_time"]`, already harvested; `dop` from
`native_pipeline_stats`.
**Overhead.** **Zero on the execution path.** No native change; this is
plan-time rendering of numbers already in hand.
**Gating.** None — always on, because the counters behind it are always on.
**Behaviour change.** None. Adds columns to a diagnostic surface.
**Why first.** It converts a measurement we pay for on every query and never
read into the single most-asked diagnostic answer.

### P2 — Time `combine()` and `finalize()`
**What.** Bracket the two untimed sinks with the existing pattern, into new
`OpStats` fields (`combine_ns`, `finalize_ns`), and emit the already-declared
`TC_COMBINE` span.
**Where.** [`executor.hpp:174`](../src/cpp/engine/executor.hpp) (`combine`,
per worker) and [`:202`](../src/cpp/engine/executor.hpp) (`finalize`, once).
**Overhead.** `combine` runs **once per worker per pipeline**, `finalize`
**once per pipeline** — not per morsel. At 266 ns per bracket that is
~4 µs per pipeline at dop=16. Immeasurable against any real query.
**Gating.** **Always-on.** It is far below the price already paid per morsel,
and a switch here would be exactly the kind of kill switch that has hidden
load-bearing behaviour in this codebase before.
**Behaviour change.** None — reads a clock either side of an existing call.
**Why.** This is a real attribution hole, not a refinement. A hash aggregate's
cross-worker merge and its result construction are currently charged to no
plan node at all; they land inside the pipeline's `wall_ns` and vanish.

### P3 — Worker skew at the pipeline barrier
**What.** Stamp each worker's first-activity and last-activity monotonic
timestamps in `WorkerCtx`; report per pipeline
`skew_ns = max(last) - min(last)` and the idle fraction.
**Where.** `WorkerCtx` (`executor.hpp:75-82`), written in `run_worker`
(`executor.hpp:86-175`), harvested alongside `collect_pipeline_stats`.
**Overhead.** **Two clock reads per worker per pipeline** (~52 ns), plus 16
bytes of `WorkerCtx`. Nothing per morsel.
**Gating.** Always-on.
**Behaviour change.** None.
**Why.** This is the only cheap answer to "did one worker hold the barrier",
which the per-operator sums structurally cannot express — `exec_ns` is summed
across workers and loses the distribution entirely.

### P4 — Peak memory per node
**Blocked on D2.** Do not build before the unit of account is ruled on. When
it is, the scoped first version is a per-node high-water mark maintained by the
sinks that already charge budget counters
(`native_group_sinks.hpp:999`, `native_cidr_emit.hpp:69`, median's), reported
as **explicitly unknown** — never `0` — for every node that does not
participate. Overhead would be one relaxed atomic max per allocation event
already being counted; the cost is the plumbing, not the clock.

### P5 — Name the modelled bytes honestly
`bytes_in`/`bytes_out` are `rows*cols*8`, a formula (`executor.hpp:68-72`),
sitting beside `bytes_fetched`, a measurement. Either measure them or rename
them to say they are an estimate. No execution cost either way. Small, and a
name that misdirects is a documented failure mode here.

### P6 — Declined, with reasons
* **Time-series sampling** — derivable from spans; adds a concurrency surface
  for no new signal (§2.4).
* **Spill counters** — nothing spills (§2.5).
* **Per-kernel / per-column timing** — below the 1 µs clock floor (§3).
* **"Blocked on join build" category** — pipelines are serial; the state does
  not exist (§2.1).

---

## 5. Decisions needed from the architect

**D1 — Is `wait_ns = exec_ns - cpu_ns` an acceptable definition of wait?**
It conflates *blocked on IO/backpressure* with *descheduled because the pool is
oversubscribed*. Both are "not doing work", but only the first is a bottleneck
in the operator. Recommendation: print `cpu_ms` and `time_ms` as two honest
columns and let the reader subtract, rather than publishing a derived `wait_ms`
that hides which of the two it is. Cheap either way; this is a
truthfulness-of-naming call, not a cost call.

**D2 — Peak memory: what is the unit of account?**
(a) morsel bytes flowing through a node — cheap, but that is throughput, not
residency, and would mislead; (b) high-water of operator-**owned state**,
reported per node by the sinks that hold it — honest, but per-operator plumbing
and covers only the operators that opt in; (c) allocator-level via mimalloc —
process-wide only, cannot attribute to a node. Recommendation: **(b)**, with a
hard rule that a non-participating node reports *unknown*, never `0`.
Note the dependency: whatever reservation-based memory model arrives later
should own this reading rather than run beside it.

**D3 — Should `EXPLAIN ANALYZE` force `refresh_statistics`?**
Today estimates are opportunistic and a never-estimated node renders `est_rows
= 0`, indistinguishable from a genuine estimate of zero. Options: force the
refresh for `EXPLAIN` (costs plan time on a diagnostic path only), or render
unknown distinctly (e.g. null). Recommendation: **render unknown distinctly**,
and additionally force the refresh under `ANALYZE` — the est-vs-actual audit is
worth plan time on a statement whose entire purpose is diagnosis. Without this,
the cardinality-audit value of the column is silently partial.

**D4 — Do `combine`/`finalize` get their own `OpStats` fields, or fold into
`exec_ns`?** Folding makes the existing `execution_time`/`self_ms` numbers move
— a published surface changing meaning. Recommendation: **separate fields**,
so no existing reading shifts and the new cost is visibly new.

**D5 — Confirm P2/P3 are always-on, ungated.** They are ~4 µs per pipeline
against a per-morsel budget the engine already pays. Recommendation: always-on.
Flagging it explicitly because it is the opposite of the tracer's design, which
is env-gated for good reason (the tracer is per-morsel and unbounded in
memory; these are per-pipeline and fixed-size).

**D6 — Does anything here belong in the trace channel rather than telemetry?**
P2's `TC_COMBINE` span is trace; P2's counters and P3's skew are telemetry.
Stated explicitly so the two channels do not blur again — telemetry is
aggregates for every query, trace is an opt-in event stream.
