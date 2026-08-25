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
2. **1 µs is the floor of `clock_gettime`, not of the hardware.**
   Sub-microsecond spans quantise to 0 or 1000 ns — visible in the trace, where
   short source pulls report exactly `duration_ns: 1000`. §6 measures the ARM
   architectural counter at **~17 ns** granularity, so this is a property of the
   call we chose, not a limit we are stuck with. See §6.3 for what that
   invalidates.

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
* **Per-kernel / per-column timing** — declined for **scope**, not for
  resolution. The resolution objection this document originally made is dead:
  §6 measures a 17 ns-granularity counter costing ~1.6 ns to read, which would
  measure a kernel call perfectly well. Nothing here proposes it; it is no
  longer blocked by the clock.
* **"Blocked on join build" category** — pipelines are serial; the state does
  not exist (§2.1).

---

## 5. Decisions — architect's rulings (2026-08-25)

**Ruled.** D1 accepted as-is (`exec - cpu` is good enough; no separate
`wait_ms` column invented). D2: whatever is indicative — a per-node peak-memory
figure need not be exact, it needs to point at the right node. D3: yes — render
"never estimated" distinctly AND force the refresh under `ANALYZE`. D4:
separate `OpStats` fields, no existing reading moves. D5: P2/P3 always-on,
ungated. D6: no — nothing here moves between the telemetry and trace channels.

**Open:** D7 (§6.4), raised by the clock work below. Also open: **P1 and
Lever 1 pull against each other** — P1 puts `cpu_ms` on every node, Lever 1
stops measuring it at operators. They reconcile only if `cpu_ms` reads *n/a*
for operator-role nodes (defensible: an operator cannot block, so the number
carries no information there). That is a surface decision, not an
implementation detail, and is called out here rather than left to be discovered
after the fact. Recommended sequencing in §7.

The original statements of D1-D6 follow, for the record.

### Original statements

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

---

## 6. Reducing the always-on 266 ns per stage per morsel

Asked by the architect, 2026-08-25. Measured, not reasoned about.

### 6.1 Where the cost actually is

Micro-benchmark reproducing `executor.hpp`'s exact bracket shape (clock reads +
the same relaxed atomic adds), 2M iterations, `-O2`, three runs on the dev box
(Darwin 25.5.0, Apple Silicon):

| Variant | ns / stage | vs current |
|---|---|---|
| **A — current**: 2× `CLOCK_MONOTONIC` + 2× `CLOCK_THREAD_CPUTIME_ID` | **247 / 248 / 259** | 1× |
| **B** — drop the CPU clock, wall only | **34 / 35 / 35** | **7×** cheaper |
| **C** — B, plus chain the boundary read (1 clock read per stage) | **18 / 19 / 19** | **13×** cheaper |
| **D** — C's read count using the ARM architectural counter | **1.6 / 1.6 / 1.7** | **150×** cheaper |
| **E** — atomics only, no clock (the floor) | **1.6 / 1.9 / 1.6** | — |

The headline: **the thread-CPU clock is ~86% of the cost.**
`CLOCK_THREAD_CPUTIME_ID` is 107 ns per read against `CLOCK_MONOTONIC`'s 26 ns;
it is read twice per stage per morsel, at every stage.

Variant D is **indistinguishable from the atomics-only floor** — the arch
counter read disappears into the pipeline.

### 6.2 It is not only a cost problem — the instrument inflates its own reading

Measuring what `exec_ns` *reports* for a completely empty stage body:

* current bracket shape: **118–122 ns per call**
* wall-clock only: **15–16 ns per call**

Because `c0 = telem_cpu_now_ns()` is read **inside** the `t0…t1` wall bracket
(`executor.hpp:108-117`, `:128-137`, `:155-163`), roughly **120 ns of every
operator's reported `exec_ns` is the instrument timing itself.** On the
cheap operator in the sample query from §1.3 — 24 µs over 65 calls, ~369 ns per
call — that is about a third of the reported figure. The counter is not merely
costing us time; at the low end it is materially wrong.

### 6.3 Three independent levers

**Lever 1 — read the CPU clock only where blocking is possible. (247 → ~34 ns.)**
An `Operator::execute()` is pure in-memory compute over a morsel; it has no IO,
no queue, no lock. It cannot block, so `cpu_ns ≈ exec_ns` by construction — and
that is what the live readings show: scan `6.593 / 6.593`, aggregate
`6.034 / 6.026`. The only site that genuinely diverges is the one that blocks:
the exit sink, `27.339 / 2.195`. So keep `cpu_ns` at `Source::get_morsel`
(`executor.hpp:155-163`) and `Sink::sink` (`:108-117`), and drop it from the
operator bracket (`:128-137`).

*What is given up:* an operator descheduled by an oversubscribed pool would no
longer be distinguishable from one doing work. That signal does not disappear
from the system — `native_pipeline_stats` already carries `cpu_time / wall_time`
per pipeline, which is the mean number of cores that pipeline kept busy
(`engine.hpp:1140-1144`) — but it drops from per-operator to per-pipeline
granularity. Given the D1 ruling this looks like the right trade; it is
nonetheless a real reduction in resolution and should be an explicit choice,
not a side effect of a cost fix.

**Lever 2 — chain the boundary read. (34 → ~18 ns.)** Each stage's closing
timestamp is the next stage's opening timestamp; the only thing between them is
a handful of relaxed atomic adds. Read once per boundary instead of twice.
Costs a ~2 ns attribution smear (the inter-stage atomics get charged to the
following stage) and makes the bracket code meaningfully harder to read for a
13 ns saving on top of Lever 1. **Recommended only if Lever 3 is rejected** —
it is the awkward middle option.

**Lever 3 — read the architectural counter instead of `clock_gettime`.
(→ ~1.6 ns, the floor.)** On this box `mrs cntvct_el0` costs ~1.6 ns and its
frequency register `cntfrq_el0` reports 1 GHz, with an **observed granularity of
17 ticks ≈ 17 ns** — that is **16× cheaper and 59× finer** than
`clock_gettime(CLOCK_MONOTONIC)`. This is the lever that makes the always-on
per-stage timer effectively free.

What it requires, honestly:

* **Per-architecture, guarded** — `cntvct_el0` (ARM64), `rdtscp` (x86-64),
  `rdtime` (RISC-V), with `clock_gettime` as the fallback for anything
  unguarded. Contract §6 contemplates exactly this shape of code.
* **Runtime frequency conversion.** `cntfrq_el0` is 1 GHz on this Apple Silicon
  box but is commonly 24 MHz on Linux/ARM server parts (≈41.67 ns/tick). Ticks
  must be stored raw and converted once at drain, from the frequency read at
  runtime — never a compile-time constant.
* **x86 caveat, unmeasured by me.** `rdtscp` needs invariant TSC (universal on
  modern parts) and, unlike ARM's architecturally system-consistent `cntvct`, has
  a cross-socket skew history. A worker migrating cores mid-bracket could yield a
  bogus delta. Single-socket Cloud Run is not exposed to this, but I have not
  measured x86 and am not claiming a figure for it.
* **It does not replace the CPU clock** — it is a wall-clock source. Lever 1 is
  what removes the CPU-clock cost; Lever 3 removes what remains.

### 6.3a Lever 0 — reorder the four reads. Free, and fixes §6.2 outright.

Found after the levers above and strictly better than any of them for the
*accuracy* problem. The bracket is currently `t0, c0, [work], t1, c1`, so the
107 ns CPU-clock read sits inside the wall bracket. Reorder to
`c0, t0, [work], t1, c1` and the wall bracket contains only the work.

Measured, two runs, same harness:

| | `exec_ns` reported for an EMPTY stage | cost |
|---|---|---|
| current order | 120.4 / 115.2 ns | 226.3 / 226.5 ns/stage |
| reordered | **15.9 / 15.7 ns** | 224.3 / 224.7 ns/stage |

**The cost is identical** (inside run-to-run noise) and `exec_ns` — the number
`EXPLAIN ANALYZE` publishes as `time_ms`/`self_ms` — becomes **7.5× more
accurate**. `cpu_ns` inflation moves the other way (120 → 131 ns) because the
CPU bracket now contains two wall reads instead of one; that is the right trade,
since `exec_ns` is the published figure and `cpu_ns` is the one Lever 1 may
remove from operators anyway.

This is a two-line reorder at each of the three bracket sites, costs nothing,
and needs no ruling beyond "yes".

**Delivered 2026-08-25.** One consequence, measured after landing and recorded
because it is visible on the surface: with the wall bracket now clean and the
CPU bracket carrying two ~26 ns wall reads per call, `cpu_ms` can slightly
**exceed** `time_ms` on nodes doing almost nothing. Measured across three real
queries:

| node cost | `cpu/wall` | reading |
|---|---|---|
| >= 45 us per call | 0.95 - 1.07 | trustworthy |
| ~1 us per call | 1.5 - 1.9 | instrument overhead, totals in single-digit us |
| the blocked exit sink | **0.09** | the real signal — 91% blocked, exactly what P1 exists to show |

Deliberately **not clamped**. `min(cpu_ms, time_ms)` would make the artifact
invisible rather than bounded, and the rows it affects total microseconds.
Lever 1, if it lands, removes the artifact from operator rows entirely.

### 6.4 D7 — new decision needed: does the trace share the new clock?

`EXECUTION_TRACING_DESIGN.md` §3.1 names **one shared monotonic epoch across the
engine and rugo** as the linchpin — without it, IO spans and operator spans do
not lie on the same axis and the waterfall is fiction. Today both sides reach
`draken_trace_now_ns()`.

If the always-on `OpStats` timers move to a tick counter while the tracer stays
on `clock_gettime`, the engine holds **two time domains**. Options:

1. **Move both.** Cleanest axis, but the tick source must then cross the `.so`
   bridge into rugo, and the drain must convert every span. Largest change.
2. **Move only the `OpStats` sums; leave the tracer on `clock_gettime`.**
   Smallest change, and the sums are aggregates that never need to line up with
   a span on a timeline. The cost is two clock domains coexisting, which is the
   sort of thing that reads as accidental later unless it is written down.
3. **Do Lever 1 only, leave the clock alone.** Gets 7× of the available ~150×,
   changes no clock semantics, and leaves the tracer untouched.

Recommendation: **(3) first, then (1) as its own piece of work.** Lever 1 is a
deletion — strictly less code, no new arch-specific paths, no second time
domain, and it fixes the §6.2 self-inflation at the same time. It captures the
large majority of the win for essentially no design risk. Lever 3 is genuinely
attractive but it is a clock change in a system whose tracer explicitly depends
on there being exactly one clock, and it deserves to be decided on its own
merits rather than folded into a cost reduction.

### 6.5 What none of this changes

No lever alters observable engine behaviour. Every one of them is a clock read
either side of a call that already happens, feeding counters nothing in the
execution path reads back.


---

## 7. Recommended sequencing (2026-08-25)

**Do now — all ruled, no open questions, nothing depends on evidence we lack:**

1. ~~**Lever 0** (§6.3a) — reorder the four clock reads.~~ **DELIVERED
   2026-08-25.** `executor.hpp`, all three brackets, plus a read-order contract
   documented at `telem_cpu_now_ns`.
2. ~~**P1** — `cpu_ms` + `dop` columns in `EXPLAIN ANALYZE`, plus the D3 fix.~~
   **DELIVERED 2026-08-25.** `EXPLAIN ANALYZE` now renders
   `tree | details | est_rows | est_bytes | rows | time_ms | cpu_ms | self_ms | dop`.
   `dop` needed one teardown-only native addition (`Engine::OpReading::dop`,
   `engine.hpp`) because `PipelineReading` is keyed by display name, which is not
   unique. Unknown estimates render NULL, not 0; `ANALYZE` forces
   `refresh_statistics` (`planner/__init__.py`). `make q` 462/462, `tests/sql`
   1927 passed / 1 xfailed.
3. ~~**P2** — time `combine()`/`finalize()` into separate `OpStats` fields (D4),
   always-on (D5).~~ **DELIVERED 2026-08-25.** `OpStats::combine_ns` /
   `finalize_ns` (`operator.hpp`), bracketed at `executor.hpp`'s combine site and
   in `run_pipeline_impl` around `finalize`. `TC_COMBINE` is finally emitted and
   a new `DRAKEN_TC_FINALIZE = 12` appended to the span vocabulary. Surfaced as
   one `merge_ms` column in `EXPLAIN ANALYZE`, with the combine/finalize split
   kept in `native_op_stats`.

   **WALL ONLY — combine/finalize CPU is deliberately NOT added to `cpu_ns`.**
   `exec_ns` excludes their wall time, so charging `cpu_ns` with their CPU would
   make `cpu_ns` systematically exceed `exec_ns` on any real breaker and break
   the `time_ms` vs `cpu_ms` comparison P1 had just established. The two keep
   measuring the same window. A blocked/running split for combine (it can contend
   on the global sink) would be a `combine_cpu_ns` field, not a reading smuggled
   into an existing one.

   **First real reading:** on a `GROUP BY ... ORDER BY ... LIMIT`, the Heap Sort
   showed `time_ms 0.012` against `merge_ms 0.508` — the breaker cost was **42x
   the operator's own reported time**, and had been attributable to nothing.

4. ~~**P3** — per-worker first/last timestamps for barrier skew.~~ **DELIVERED
   2026-08-25.** `WorkerCtx::t_first_ns/t_last_ns`, stamped via an RAII
   `FinishStamp` so a worker that returns early on error still records a finish
   (a skew reading that is only correct on the success path is wrong exactly when
   something has gone wrong). Reduced to `PipelineSkew{skew_ns, barrier_idle_ns,
   workers}` in `run_pipeline_impl` once every worker has finished — it is a
   property of the SET of workers, so no worker can compute it. Surfaced on
   `native_pipeline_stats` as `skew_time`/`barrier_idle_time`; **not** an
   `EXPLAIN ANALYZE` column, because it is per-pipeline and that table is
   per-node.

   **First real reading:** a single-file parquet scan at `dop=16` reported
   `wall 13.066ms, skew 12.654ms, barrier_idle 188.079ms` — one worker did
   essentially all the work while fifteen parked immediately. `exec_ns` is summed
   across workers and structurally cannot show this.

**Defer, deliberately:**

* **Lever 1** (drop the CPU clock from the operator bracket). The evidence that
  operator `cpu_ns` never earns its 213 ns is currently *two operators on one
  100k-row local query*. That is thin, and this codebase's own rule is to
  profile rather than hypothesise. P1 costs nothing and surfaces exactly the
  per-node `cpu_ms` needed to decide this on real workloads — including
  clickbench and a GCS-backed query. Decide it with that data, not this.
  Note also that Lever 0 already removes the *accuracy* argument for Lever 1,
  leaving only the 213 ns cost argument, which is ~0.3% of a real operator call.
* **Lever 3 / D7** (architectural counter). Own piece of work — it is a clock
  change in a system whose tracer depends on there being exactly one clock.
* **P4** (peak memory per node). Unblocked by the D2 ruling ("whatever is
  indicative"), but it is the largest piece here and wants the smaller items
  landed first.


---

## 8. Follow-up left open by the Lever 0 / P1 landing (2026-08-25)

* ~~The `rows`/`time_ms`/`cpu_ms`/`self_ms`/`dop` filler cells on the
  OPTIMIZATIONS and REWRITE TRACE heading rows still render `0`, not NULL.~~
  **DONE 2026-08-25** (architect: yes). A row that is not a plan node now
  renders NULL in **every** numeric column, via a single `_append_no_reading()`
  helper — previously seven `append(0)`/`append(0.0)` calls duplicated at four
  sites, which is how the two columns drifted apart in the first place.

* **Unresolved, one layer down: a plan node with no native reading is still
  indistinguishable from one that read zero.** `mermaid._collect_node_stats`
  always returns a dict per explained node and overlays `native_op_stats` onto
  it only when that identity was harvested, so `records_out` is `0` both for a
  node that genuinely emitted no rows and for one the native harvest never
  covered. Fixing it needs the overlay to signal *whether* it fired, not a
  change at the rendering layer — out of scope for P1, and noted so the NULL
  work is not mistaken for complete.
* **Lever 1 evidence is now collectable.** `cpu_ms` per node ships, so the
  question "does operator-level `cpu_ns` ever earn its 213 ns?" can be answered
  from clickbench and a real GCS query rather than from the two operators on one
  local query this document had to reason from.


---

## 9. Reading `skew_ns` — what it does and does not distinguish

A first draft of this counter's documentation claimed skew separates "one
straggler among busy workers" from "not enough work to fill the pool". The first
real measurement disproved that: a single-file scan at `dop=16` produced a skew
of 12.654 ms out of a 13.066 ms pipeline *because fifteen workers found nothing
to claim and exited immediately*, which is the "not enough work" case presenting
as a very large skew.

What it actually tells you:

* **Large skew** — the work did not distribute. Either one straggler among busy
  workers, or most workers finding nothing while one did everything. Both are
  fixed the same way: hand out more, smaller units of work.
* **Near-zero skew with low `cpu/wall`** — every worker was equally idle, so the
  pipeline is bounded by something upstream, not by how its work was split.

`barrier_idle_ns` is what the spread cost, as opposed to how wide it was.
