# WP-6 — GIL-Held Profile of the Operator Chain

**Status:** evidence delivered (M2). **Date:** 2026-06-15. **Tool:** [`dev/gil_profile.py`](../dev/gil_profile.py).

## Question

Before parallelising execution (M4/M5), measure what fraction of execution
wall-time the **main thread holds the GIL**. The GIL-held fraction is the ceiling
on what naive thread-per-pipeline parallelism can win *today*, before the Phase-9
nogil Morsel surface lands: time the main thread holds the GIL is time other
worker threads cannot run Python/Cython, no matter how many cores exist.

## Method (zero dependencies — honours CLAUDE.md §4)

No `pip install`. A pure-stdlib background sampler (the `gil_load` technique):
loop `t0=monotonic; sleep(δ); t1=monotonic`; the excess latency `(t1-t0) − δ` is
the time the sampler waited to **reacquire** the GIL after its sleep expired —
i.e. time the main thread held the GIL and would not yield. The GIL switch
interval is lowered to 50µs during sampling so a held-off sampler reacquires
promptly (otherwise the default 5ms quantum over-counts held time — the first
draft, before this fix, reported filter at 6% vs the corrected 22%).

```
GIL-held    = Σ excess / Σ wall      (main thread serial: Python + GIL-holding C)
GIL-released = 1 − GIL-held          (nogil kernels, parquet decode, IO wait)
```

Measured **warm** (OS page cache hot, 2 warmups) so IO wait ≈ 0 and the released
fraction reflects compute/decode, not disk. 3 reps averaged; numbers stable to
±2–3 points across runs.

## Results — full ClickBench (`scratch.hits`, ~100M rows) + TPC-H sf001

| class       | wall_ms | GIL-held | **GIL-released** | dominant operators (self_ms) |
|-------------|--------:|---------:|-----------------:|------------------------------|
| distinct    |     ~550 |    ~25% | **~75%** | Ungrouped Aggregate (COUNT DISTINCT) ~485 |
| group-agg   |     ~230 |    ~41% | **~59%** | Grouped Aggregate (Hashed) ~190 |
| scan/decode |      ~75 |    ~50% | **~48%** | Parquet Read ~27, Ungrouped Aggregate ~25 |
| filter      |     ~415 |    ~77% | **~22%** | **Filter ~260**, Parquet Read ~123 |
| sort/topN   |    ~1050 |    ~88% | **~12%** | Grouped Aggregate ~615, Filter ~253 |
| join        |     ~5.8 |    ~38% | (unreliable) | Parquet Read (TPC-H sf001 too small) |

Queries: `SUM(ResolutionWidth)` (scan); `COUNT(*) WHERE SearchPhrase<>''`
(filter); `GROUP BY RegionID` (group-agg); `COUNT(DISTINCT UserID)` (distinct);
`GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` (sort); `orders ⋈ customer`
(join).

## Findings

1. **GIL behaviour is strongly operator-dependent — there is no single answer.**
   The released fraction spans **12% (sort) to 75% (distinct)**. Any blanket
   "parallelise the engine" decision would be wrong; the unit of decision is the
   operator.

2. **Clear parallelism wins: COUNT(DISTINCT) (~75%) and grouped aggregation
   (~59%).** Both spend most of their time in nogil hash kernels. These are
   exactly the M5 targets (partition-parallel grouped agg / distinct) — the
   measurement endorses that plan.

3. **Clear GIL-bound operators: sort/topN with string grouping (~12%) and
   filter on a string predicate (~22%).** Thread-parallelism would barely move
   these today; they are gated on the Phase-9 nogil Morsel surface.

4. **The filter result is a specific, actionable anomaly.** `SearchPhrase <> ''`
   spends ~260ms of self-time at only ~22% released — a string predicate that is
   ~78% GIL-held. A simple string-inequality kernel *should* be nogil. This is
   worth investigating directly: if the string comparison path holds the GIL in
   a hot loop, fixing it is **both a serial speed-up and a parallelism unlock**.
   (Flagged as a follow-up, not yet diagnosed.)

5. **Scan/decode (~48% released) is not the operator-chain win it looks like.**
   The released half is largely parquet decode, which already runs on C++ worker
   threads. Parallelising the *operator chain* above the scan is a separate axis.

## Methodology limits (read before over-trusting a number)

- **Per-morsel orchestration is GIL-held and counted against every operator.**
  The push dispatch, `drive_scan` loop, and `Morsel` object lifecycle run under
  the GIL and wrap every kernel. So the measured released fraction is a *lower
  bound* on a kernel's nogil-ness, and the GIL-held floor includes orchestration
  that thread-per-pipeline cannot escape (each worker re-runs it). This is the
  structural reason the Phase-9 nogil Morsel surface matters: it lifts the floor.
- **Short queries are planning/orchestration-dominated.** scan (~75ms) and
  especially join (~5.8ms) include non-trivial Python planning; trust only the
  >200ms queries (filter, group-agg, distinct, sort).
- **The join number is unusable** — TPC-H sf001 is too small. Measuring join GIL
  behaviour needs a larger build/probe (sf1+ or a clickbench self-join). **Gap.**
- Decode counts as released (correct — already parallel); the dominant-operator
  column is how you attribute a high released fraction to decode vs operator work.

## Go / no-go for M4/M5

- **Do not** pursue naive thread-per-pipeline across all operators: filter and
  sort are GIL-bound and would contend on the GIL for orchestration.
- **Do** prioritise **partition-parallel grouped aggregation and
  count-distinct** (M5) — the measured winners, ~59% and ~75% released.
- **Investigate the string-filter GIL hold (finding 4)** — potentially a cheap
  serial + parallel double-win independent of the larger parallelism work.
- **Close the join measurement gap** before committing to parallel joins.
- The GIL-bound operators (filter/sort) wait on the **Phase-9 nogil Morsel
  surface**; revisit them after it lands.

## Reproduce

```
python dev/gil_profile.py                                   # full data
OPTERYX_GIL_DATASET=testdata.clickbench_tiny python dev/gil_profile.py   # quick smoke
```
