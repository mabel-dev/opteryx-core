# A/B Performance: Recently-Fixed SQL Constructs vs Their Workarounds

**Status:** findings only — no fix attempted here.
**Measured:** 2026-08-22, against **local `main`** (opteryx-core 0.9.78, in-process,
catalog-backed `home.*` datasets over GCS).
**Supersedes:** the 2026-08-22 revision measured against deployed `jobs.opteryx.app`.
That revision's top recommendation (band-join pushdown) had already landed on `main`
and is now measured, not proposed.
**Audience:** whoever picks up planner/join-execution work next.

## Summary

| # | Pair | Fixed form | Workaround | Verdict (local main) | Was (deployed) |
|---|------|-----------:|-----------:|----------------------|---------------:|
| 1 | Two `APPROX_PERCENTILE`, same column | **2.42s** | 4.88s | fix 2.0x faster | 1.5x faster |
| 2 | IPV4 `<<= '…/32'` vs `CAST(… AS VARCHAR)` | 2.83s | **1.63s** | live: fix 1.7x slower; **local: parity or better** | no difference |
| 3 | Subscript in `WHERE` vs outer subquery | 1.92s | 1.83s | neutral (~5%, noise) | fix 18% slower |
| 4 | `EXISTS` in `SELECT` vs bucketed semi-join | 8.26s | **7.17s** | fix 1.15x slower | fix 6.8x slower |
| 5 | `CAST` in `ON` vs cast hoisted to CTE | 98.4s | **70.6s** | fix 1.39x slower | 1.3x slower |
| 6 | As-of join unbucketed vs hand-bucketed | 70.6s -> **18.3s** | 8.2s -> **5.9s** | 8.6x -> **3.1x** (see note) | 58x |

Three of the six moved:

* **Pair 4 collapsed from 6.8x to 1.15x.** The `EXISTS`-in-`SELECT` form is now
  roughly at parity with the hand-written bucketed semi-join. This was the
  document's #2 priority; it is no longer a priority.
* **Pair 6 improved from 58x to 8.6x, then to 3.1x** after the 2026-08-22/23
  estimator fixes, but is *not* fixed. Still the largest gap. The second
  improvement came with NO plan change — the join still emits 2.55 billion rows;
  a join estimate of 3 had been disabling the build-side consolidation fast path
  (`decide_consolidation`, native_join2.hpp — now reported per join sink as
  `join_build_diagnostics` in query telemetry). See
  [BAND_JOIN_PROPOSAL.md](BAND_JOIN_PROPOSAL.md).
* **Pair 2 reversed on live data** — the native IPV4 predicate is consistently
  slower than the VARCHAR cast there — but **does not reproduce on local
  fixtures**, where it is never slower and prunes 10x the IO when the layout
  allows. See Pair 2; the construct is exonerated, the live table is not.

## Method

* **Local `main`, in-process**, timing wall-clock around
  `session.execute_to_morsels(...)` with every morsel consumed. No job queue, no
  client poll interval, so the 1–2.4s of client overhead that distorted the
  previous revision is absent. Compiled artifacts verified newer than all sources
  (no rebuild needed).
* Datasets are **live and growing**, and are now much larger than when the
  deployed numbers were taken: `home.network.netflow` **4,598,301** rows (was
  ~2.5M), `home.network.dns` **271,767** rows (was ~134k). **Absolute times are
  therefore not comparable across the two columns above — only the ratios are.**
* **Pairs 1-3: medians of 3 interleaved rounds**, plus a discarded warm-up round.
  Arm order is **reversed on alternate rounds** so within-round order bias cannot
  masquerade as an effect.
* **Pairs 4-6: single runs.** They are slow; the surviving effects (8.6x) are far
  outside run-to-run noise. Pair 5's 1.39x is the one result that would benefit
  from repetition.
* Pair 5's workaround and Pair 6's unbucketed arm are **the same query**, run once
  and reported in both pairs.

## Pair 1 — two `APPROX_PERCENTILE` on one column (fix is faster)

2.42s vs 4.88s, **2.0x**, and the fixed arm won all 3 rounds. One scan feeding two
sketches instead of two scans. Better than the 1.5x seen deployed, consistent with
the larger table making the shared scan worth more. Nothing to do here.

## Pair 2 — IPV4 predicate: does NOT reproduce off cloud data

The live-data result stands as measured (2.83s vs 1.63s, fixed arm slower in all
3 interleaved rounds), but a local investigation on purpose-built fixtures says
the construct itself is not the cause. **On local parquet the pushed form is
never slower, and is 10x cheaper in IO when the data is prunable.**

Fixtures (rugo-written parquet, zstd + dictionary + bloom filters, 57 distinct
`192.168.4.x` addresses like the live table; target `192.168.4.114` = 1.7% of
rows). Timings are fresh-process per run, interleaved with arm order reversed
between rounds — an in-process loop is useless here, a warm buffer pool collapses
every arm to ~9ms:

| fixture | `<<= '…/32'` | `CAST … = '…'` | bytes fetched (pushed / not) |
|---|---:|---:|---|
| 5M rows, 10 files, addresses interleaved | 104ms | 104ms | 20.5MB / 20.5MB |
| 40M rows, 80 files, interleaved | 141ms | 146ms | 164MB / 164MB |
| 10M rows, 1000 files, interleaved | 167ms | 164ms | 41.4MB / 41.4MB |
| 10M rows, **no dictionary** | 112ms | 117ms | 44.0MB / 44.0MB |
| 5M rows, **addresses segregated by file** | **7.3ms** | 10.8ms | **1.85MB / 18.4MB** |

What the plans show:

* `<<= '192.168.4.114/32'` is rewritten to `src_addr = 3232236658` on the raw
  UINT32 and **is pushed** — telemetry reports
  `optimization_predicate_pushdown_into_scan: 1` and
  `predicate rewriter cidr to range: applied`; the scan emits 87,435 rows.
  A plain `src_addr = 3232236658` produces an identical plan and identical
  timings, so the CIDR rewrite itself costs nothing.
* `CAST(src_addr AS VARCHAR) = '192.168.4.114'` reports
  `predicate pushdown declined` and materialises all rows into a `Filter`.
* All arms select `NativeParquetScanSource` and return identical answers.

**The pruning result is the important one, and it is conditional.** Pushdown buys
IO only when the address column is zone-map-separable across files. On the
interleaved fixtures — where every file's min/max spans the whole host range, as
on a chronologically-written netflow table — `bytes_fetched` and `enqueue_count`
are *identical* between the pushed and unpushed arms. The pushed predicate then
filters data that was fetched anyway, and the two arms converge. Segregate the
addresses by file and the same predicate reads 1.85MB in 2 IO ops instead of
18.4MB in 20.

So the local answer is: no defect in the construct, and no local reproduction of
the regression. The live gap is a property of that table or that deployment, not
of `<<=`. **Next step must be run against the deployed service** (deliberately not
done here): capture `EXPLAIN ANALYZE` plus `io_scan_diagnostics.bytes_fetched` and
`enqueue_count` for both arms on `home.network.netflow`. If the pushed arm fetches
the same bytes and still loses 1.2s, the cost is in the remote decode path; if it
fetches *more* or issues more ops, pruning is actively misfiring there. Either way
the local fixtures above rule out the planner rewrite and the parquet scan filter.

## Pair 3 — subscript in a filter predicate (now neutral)

1.92s vs 1.83s. The deployed run showed the fixed form losing all 3 rounds by ~18%;
locally the margin is ~5% and the fixed arm won one round of three. Treat as noise
at this size. The CSE question the previous revision raised (is `SPLIT(...)[-3]`
evaluated twice, once in the projection and once in the filter?) is still worth a
one-off look, but there is no longer a measured cost attached to it.

## Pair 4 — `EXISTS` in SELECT list: 6.8x gap has collapsed to 1.15x

8.26s (fixed) vs 7.17s (bucketed rewrite). Both queries unchanged from the previous
revision. The natural formulation is now essentially at parity with the hand-tuned
one, and **no longer warrants planner work.**

Caveat on the numbers, which applies to the earlier revision too: **the two arms
are not row-equivalent by construction.** The workaround adds
`src_addr <<= '192.168.0.0/16' AND NOT dst_addr <<= '192.168.0.0/16'` (outbound
external flows only) and counts `COUNT(DISTINCT b.event_time)`, while the fixed
form counts blocked lookups having any matching flow. Their `connected_anyway`
columns disagree accordingly (e.g. 282 vs 219 on the top row). This is a timing
comparison of two plausible spellings of a question, not a correctness oracle.

## Pairs 5 and 6 — the as-of join, and what the profile actually says

The remaining real problem, and the previous revision's diagnosis of it was wrong.

`EXPLAIN ANALYZE` of the unbucketed form (70.6s total):

| node | rows out | self_ms |
|------|---------:|--------:|
| Heap Sort (LIMIT 10) | 10 | 0.0 |
| Grouped Aggregate (Hashed) | 5,359 | 180 |
| Filter `event_time <= flow_start AND …` | 4,814,246 | 7,204 |
| **Inner Join Draken** (est_rows: **3**) | **2,558,819,423** | **66,290** |
| Projection (flows) | 1,279,999 | 8 |

The hash join emits **2.56 billion rows — 94% of runtime — and the filter directly
above it discards 99.8% of them.** Same failure mode as before, one order of
magnitude smaller.

**The previous revision's proposed fix would not have fixed this.** It argued the
`Eq`-only gate in `correlated_filters.py` blocked range transport across the
inequality. That gate is gone — `_TRANSPORTED_BOUNDS` now carries bounds for
`Lt`/`LtEq`/`Gt`/`GtEq` with the delta shift
([correlated_filters.py:134](../opteryx/planner/optimizer/strategies/correlated_filters.py:134))
— and the explosion persists, because **range transport cannot help this shape.**
Both sides span the same 24-hour window, so the derived bound on `event_time` is
`[min(flow_start) - 20s, max(flow_start)]` — the whole table. A necessary-condition
range prunes nothing when the two ranges coincide.

The actual cause is the join key. `client` is an IP address with ~57 distinct values
across 1.28M flows and 271k lookups; the equi-join on it is inherently quadratic.
The hand-bucketed rewrite is fast (8.2s) because adding a minute bucket to the join
key makes it selective — it converts the time *predicate* into part of the join
*key*. No amount of predicate transport does that.

**What would fix it is a real as-of/band join execution strategy** — interval
partitioning on the join key, or a sort-merge over the time dimension — so the
20-second window bounds the probe instead of filtering its output. That is
execution work, not optimizer-rule work.

Both arms were verified to return the same result (one row differs by 256 in a
count, from data ingested during the 62 seconds between runs), so the bucketing
rewrite is a valid oracle for any future fix.

### Pair 5 is a different effect than the previous revision claimed

It is not about where the `CAST` sits. `EXPLAIN` on the two arms:

* **Cast in `ON`** (98.4s): the theta conjuncts are **absorbed into the join** —
  plan is a **Nested Loop Join**, no Filter above it.
* **Cast hoisted to CTEs** (70.6s): absorption **declines** — plan is a hash
  `Inner Join Draken` with the theta left as a `Filter` above it.

So the 1.39x is the cost of the nested-loop residual versus hash-join-then-filter
on this data — and **the absorbed plan is the slower of the two.** Two things follow:

1. Theta absorption fires on one spelling of this query and declines on the
   other. Both are INNER joins with an `on` and a two-relation comparison of
   identifiers, literals, arithmetic and casts, which
   `_is_absorbable_theta` admits
   ([predicate_pushdown.py:383](../opteryx/planner/optimizer/strategies/predicate_pushdown.py:383)).
   Something about the CTE/subquery naming boundary is failing the
   `len(predicate.relations) == 2 and join_relations == set(predicate.relations)`
   gate at
   [predicate_pushdown.py:1430](../opteryx/planner/optimizer/strategies/predicate_pushdown.py:1430).
   Worth establishing which, because the inconsistency is invisible to users.
2. Absorption is being applied without costing it. Turning a hash join into a
   nested loop is not unconditionally a win, and here it is a 1.39x loss.

Also note the join's `est_rows` is **3** against 2.56 billion actual. Whatever
decides between hash and nested loop is deciding on an estimate that is off by nine
orders of magnitude.

## Verified fixed (context — do not re-report)

All still pass on local `main`:

* IPV4 host-route predicate in `WHERE` returning 0 rows (was a silent wrong answer)
* `EXISTS`/`IN` in the `SELECT` list — and now at performance parity, see Pair 4
* Transposed-argument error message for `TIME_BUCKET`
* `EXTRACT(EPOCH FROM ...)` and `DATE_TRUNC`
* Expressions (`CAST`) in `JOIN` conditions — but see Pair 5
* Two `APPROX_PERCENTILE` calls on the same column
* Subscripts in filter predicates
* `VALUES` inside a CTE
* `GENERATE_SERIES` and `NTILE`

## Still open

1. **As-of / band join execution strategy** (Pair 6) — 8.6x, and the only large
   gap left. Needs interval partitioning or sort-merge, not predicate transport.
   Do not re-attempt the range-pushdown route; it is implemented and measured not
   to help this shape.
2. **Theta-absorption inconsistency and cost** (Pair 5) — fires on one spelling,
   declines on another, and when it fires here it is 1.39x slower. Two separate
   questions: why the CTE spelling declines, and whether absorption should be
   costed rather than unconditional.
3. **Join cardinality estimate of 3 vs 2.56 billion actual** — plausibly upstream
   of (1) and (2), since it is what any cost-based choice would be reading.
4. **IPV4 `<<=` 1.7x slower than a VARCHAR cast on live data only** (Pair 2). The
   predicate does reach the scan — confirmed locally, along with 10x IO pruning
   when the layout allows and parity when it does not. Needs one deployed-side
   `EXPLAIN ANALYZE` + `io_scan_diagnostics` capture on `home.network.netflow` to
   say whether the pushed arm fetches the same bytes (remote decode cost) or more
   (pruning misfiring). Not a planner-rewrite or parquet-scan bug.
5. **`SELECT 1 INTERSECT SELECT 1`** -> `InvalidInternalStateError: Unexpected
   logical node encountered during physical planning: Intersect`. `INTERSECT` over
   real relations is correct; only constant-only selects fail.
6. **IPV4 literal in `VALUES`** -> `ValueError: vector_from_sequence: unsupported
   dtype name 'UINT32'`. Repro:
   `SELECT * FROM (VALUES ('192.168.4.136'::IPV4)) AS v(ip);`
   Blocks building synthetic IPV4 fixtures locally.
7. **Expose execution time in the API.** `finished_at - started_at` is only
   obtainable from `/jobs/recent` or an in-flight 202; neither the COMPLETED
   `/status` response nor `/results` carries a duration. A `duration_ms` on the
   results response would have prevented the wrong conclusions in the first
   revision of this document.

## Suggested priority

1. **Pair 6** — as-of join execution. The only remaining large gap.
2. **Pair 5 / estimate** — items 2 and 3, which are entangled. Cheap to
   investigate, and item 3 may be the root of both.
3. **Pair 2** — IPV4 predicate. Blocked on one deployed-side measurement; the
   local fixtures have taken it as far as they can.
4. Items 5 and 6 — small, self-contained, clear repros.
