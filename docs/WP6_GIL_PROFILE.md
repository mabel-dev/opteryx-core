# WP-6 — GIL-Held Profile of the Operator Chain

**Status:** evidence delivered (M2), then acted on. **Date:** 2026-06-15. **Tool:** [`dev/gil_profile.py`](../dev/gil_profile.py).

> **Update (2026-06-15, post-profile).** This profile did not just measure — it
> drove fixes. The string-filter anomaly (original finding 4) was diagnosed and
> fixed, and a broad sweep released the GIL across ~56 hot draken nanobind ops
> (take/mask/slice/compare/sum/min/max/arithmetic/in_list/between …). The
> releases were then audited for thread-safety (one real race fixed —
> `logical_type_intern`) and covered by a concurrency stress test
> (`tests/draken/test_gil_release_concurrency.py`). **The go/no-go below has
> flipped: filter and sort are no longer GIL-bound.** The results table carries
> both the original baseline and the post-fix numbers. Building the parallel
> executor itself (M4) is out of scope for this thread.

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

## Results — full ClickBench (`scratch.hits`, ~100M rows) + TPC-H sf1

GIL-**released** fraction, before and after the post-profile nogil work. Higher
is more parallelisable. (Warm cache, 3 reps, ±2–3 points.)

| class       | wall_ms | GIL-released (baseline) | **GIL-released (post-fix)** | dominant operators (self_ms) |
|-------------|--------:|------------------------:|----------------------------:|------------------------------|
| distinct    |     ~515 | ~75% | **~75%** | Ungrouped Aggregate (COUNT DISTINCT) ~467 |
| group-agg   |     ~235 | ~59% | **~74%** | Grouped Aggregate (Hashed) ~193 |
| scan/decode |      ~80 | ~48% | **~65%** | Parquet Read ~34, Ungrouped Aggregate ~26 |
| filter      |     ~400 | ~22% | **~50%** | **Filter ~253**, Parquet Read ~118 |
| sort/topN   |    ~1040 | ~12% | **~53%** | Grouped Aggregate ~611, Filter ~250 |
| join (sf1)  |     ~280 |  (sf001 too small) | **~54%** | **Inner Join Draken ~236**, Filter ~14 |

Every measured operator class is now **≥50% GIL-released**. The two formerly
GIL-bound classes moved the most: **filter 22%→50%** and **sort/topN 12%→53%**.
group-agg and scan also rose (59%→74%, 48%→65%) as the sweep released the
aggregate/reduction kernels; distinct was already kernel-bound and is unchanged.

**Join measurement gap (originally open) is now CLOSED.** Re-measured at TPC-H
sf1 (the original join row used sf001, too small to trust). Two joins:
`orders(1.5M) ⋈ customer(150k)` → ~51% released at ~77ms; `lineitem(6M) ⋈
orders(1.5M)` → **~54% released at ~281ms** (the reliable one — >200ms, and the
Inner Join is the dominant operator at ~236ms self). The hashed join's
build+probe releases the GIL substantially; joins are in the same parallelisable
band as filter/sort, **not GIL-bound.**

Queries: `SUM(ResolutionWidth)` (scan); `COUNT(*) WHERE SearchPhrase<>''`
(filter); `GROUP BY RegionID` (group-agg); `COUNT(DISTINCT UserID)` (distinct);
`GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` (sort); `lineitem ⋈ orders`
(join, sf1).

## Findings

1. **GIL behaviour was strongly operator-dependent — and that pointed straight
   at the fixes.** The original baseline spanned 12% (sort) to 75% (distinct);
   the gap between operators *was the finding*. The low outliers turned out not
   to be intrinsic — they were nogil-safe kernels that simply hadn't released the
   GIL. Once released, the spread collapsed: every class is now ≥50%.

2. **The original "GIL-bound" verdict for filter and sort was wrong — they were
   un-released, not un-parallelisable.** filter (22%→50%) and sort/topN
   (12%→53%) were dominated by kernels (string compare, the `filter_mask` gather)
   that were already nogil-safe C++ but called with the GIL held. Releasing the
   GIL — not waiting on the Phase-9 nogil Morsel surface — recovered them.

3. **The aggregate/distinct wins held and grew.** distinct stayed ~75% (already
   kernel-bound); grouped aggregation rose 59%→74% as the sum/min/max reduction
   kernels were released. These remain the strongest partition-parallel targets.

4. **The filter anomaly (original finding 4) is diagnosed and fixed.**
   `SearchPhrase <> ''` was ~78% GIL-held not because the string compare was
   slow, but because (a) the compare kernel was called without `with nogil:` and,
   dominantly, (b) the per-column `filter_mask` gather (`Vector.mask`) ran its
   whole pure-C++ index-build + take under the GIL. Releasing both took the
   *gather* from 72%→22% held in isolation and the whole operator from 22%→50%
   released. The double-win predicted here landed.

5. **Scan/decode (now ~65% released) is still not the operator-chain win it
   looks like.** Much of the released fraction is parquet decode, already on C++
   worker threads; the rise to 65% reflects the released `SUM` reduction kernel.
   Parallelising the *operator chain* above the scan is a separate axis.

6. **Releasing the GIL is necessary but not sufficient — it must be safe.** The
   sweep exposed shared state the GIL had been silently serialising. A
   thread-safety audit of everything reachable from the released kernels found
   the selection/validity buffer pools, ops table, allocator, and kernel registry
   already safe, and one real data race: `logical_type_intern` (a process-global
   `std::deque` with no lock), reached off-GIL via `vecresult_to_owner` for
   timestamp results. Fixed with a mutex; covered by
   `tests/draken/test_gil_release_concurrency.py` (16 threads, results verified
   against a single-threaded reference). **Lesson for M4: a clean GIL-released
   fraction is a ceiling, not a guarantee — each released path needs its shared
   state audited.**

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
- **The join gap is now CLOSED** — re-measured at TPC-H sf1 (`lineitem ⋈ orders`,
  ~281ms, ~54% released, Inner Join the dominant operator). The original sf001
  number was too small to trust; sf1 gives a reliable >200ms build/probe.
- Decode counts as released (correct — already parallel); the dominant-operator
  column is how you attribute a high released fraction to decode vs operator work.

## Measured kernel scaling (not just the ceiling)

The released fractions above are a *ceiling*. [`dev/parallel_scaling.py`](../dev/parallel_scaling.py)
measures *actual* speedup: one shared input vector, N OS threads each running
released kernels, speedup = T(1)/T(N). This is **kernel-layer** scaling (no engine
orchestration) — the precondition for M4, not end-to-end query scaling.

5M-row INT64, 16 work units, 18-core Apple Silicon:

| workload (kernel)            | 2 thr | 4 thr | 8 thr | 16 thr |
|------------------------------|------:|------:|------:|-------:|
| keying (`hash_shaped`)       | 1.9× | 3.3× | 4.6× | 4.7× |
| agg (`sum`)                  | 1.9× | 3.6× | 5.1× | 5.6× |
| filter (`compare_scalar`+`mask`) | 1.9× | 3.2× | 5.5× | **7.9×** |

The releases deliver real parallelism — **near-linear to 4 threads** (83–95%
efficiency) — then flatten as these memory-bandwidth-heavy kernels saturate DRAM,
not the GIL. So the plateau is a *lower bound* on the GIL win. `filter` scales
best (7.9×): fitting, since it was the worst GIL offender pre-fix and now does the
most nogil work per unit (compare + gather).

## Go / no-go for M4/M5 (revised post-fix)

The original verdict ("do not thread-per-pipeline; filter and sort are
GIL-bound") **no longer holds** — those operators were un-released, not
un-parallelisable, and the releases have landed (and are thread-safe).

- **Thread-per-pipeline is no longer ruled out by the kernels.** Every measured
  class is ≥50% released, so the per-operator GIL ceiling is no longer the
  blocker it was. The string-filter / nogil-Morsel-surface dependency that gated
  filter and sort is **removed**.
- **The remaining ceiling is per-morsel orchestration, not the kernels.** With
  the kernels released, the residual GIL-held fraction (e.g. filter's ~50%) is
  now dominated by the push dispatch / `drive_scan` / `Morsel`-lifecycle floor
  (see Methodology limits) — Python that every worker re-runs. Lifting *that* is
  the engine-layer job (out of scope for this thread), and it is what now bounds
  thread-per-pipeline scaling.
- **Still prioritise partition-parallel grouped aggregation and count-distinct**
  — the highest released fractions (~74% / ~75%) and the cleanest first targets.
- **A clean GIL-released fraction is a ceiling, not a guarantee.** Before relying
  on any released path under M4 threads, audit its shared state (as was done for
  the kernels here) — the engine/orchestration layer has *not* been audited.
- **Close the join measurement gap** (sf001 too small) before parallel joins.
- **The kernels are proven to scale** (4.7×–7.9× on 8–16 threads, see Measured
  kernel scaling). The remaining work to realise this end-to-end is the engine
  layer: a parallel executor + the per-morsel orchestration floor — out of scope
  for this thread, but no longer blocked by the kernels.

## Reproduce

```
python dev/gil_profile.py                                   # full data
OPTERYX_GIL_DATASET=testdata.clickbench_tiny python dev/gil_profile.py   # quick smoke
```
