# Proposal: Band Join (range-matched equi-join)

**Status:** proposal — not agreed, not started. Architect's call on every "Open
decision" below before any code moves.
**Author's evidence:** measurements of 2026-08-22, see
[SQL_CONSTRUCT_AB_PERFORMANCE.md](SQL_CONSTRUCT_AB_PERFORMANCE.md) pairs 5 and 6.

## The problem, measured

A join carrying an equality *and* a two-sided inequality is executed as the
equality alone, with the inequality demoted to a `Filter` above it.

```sql
FROM flows f INNER JOIN lookups l ON f.client = l.client
WHERE l.event_time <= f.flow_start
  AND l.event_time > f.flow_start - INTERVAL '20' SECOND
```

`EXPLAIN ANALYZE`, 70.6s total:

| node | rows out | self_ms |
|------|---------:|--------:|
| Grouped Aggregate | 5,359 | 180 |
| Filter `event_time <= flow_start AND …` | 4,814,246 | 7,204 |
| **Inner Join Draken** (est_rows: **3**) | **2,558,819,423** | **66,290** |
| Projection (flows) | 1,279,999 | 8 |

`client` has ~57 distinct values across 1.28M flows and 272k lookups, so the
equi-join alone pairs every flow with every one of its client's ~4,700 lookups:
2.56 billion pairs, 94% of runtime. The 20-second band then keeps roughly 1 pair
in 500 and the other 2.55 billion are discarded by the node directly above.

Hand-adding a minute bucket to the join key — which is what a user is forced to
do today — runs the same question in **5.9s, a 3.1x difference**, and returns the
same answer.

⚠️ **The gap was 8.6x when this proposal was first written (70.6s vs 8.2s) and is
3.1x after the 2026-08-22/23 estimator fixes** (medians of 3 interleaved runs:
unbucketed 19.44 / 18.00 / 18.27, bucketed 7.34 / 5.92 / 5.27). The fan-out is
unchanged — `EXPLAIN ANALYZE` still shows 2,547,359,010 rows out of the join —
so the plan did not change. What changed is that a join estimate of 3 was
DISABLING an execution fast path: `decide_consolidation`
([native_join2.hpp](../src/cpp/engine/native_join2.hpp)) declines when
`est_rows` is too small for the one-off consolidation cost to amortise, and
without consolidation the probe takes the dense per-row gather instead of
`emit_build_dict`. The comment there puts the win for a string-carrying payload
(this join carries `domain`) at 8-13x. That was inferred from the gate's
arithmetic and the timing, because there was no instrumentation; it is now
reported per join sink as `join_build_diagnostics` in the query's telemetry
(outcome, the `est_rows` decided on, and the actual build row count), so the
next occurrence is read rather than deduced.

**Weigh the proposal against 3.1x, not 8.6x.** It is still the largest measured
gap, and the mechanism below is unaffected: the band join removes 2.55 billion
row emissions rather than making them cheaper. But the prize is a third of what
it first appeared.

### Why the existing routes do not fix this

Three mechanisms already in the tree were checked and none of them addresses it:

* **Range transport** (`correlated_filters.py`) now carries bounds across
  `Lt`/`LtEq`/`Gt`/`GtEq`, not just `Eq`. It cannot help here: both sides span
  the same 24-hour window, so the derived bound on `event_time` is
  `[min(flow_start) - 20s, max(flow_start)]` — the whole table. A necessary
  condition prunes nothing when the two ranges coincide.
* **Theta absorption** (`predicate_pushdown.py`) folds the inequality into the
  `ON` clause and the join re-types to nested loop. Measured on this query: 98.4s
  versus 70.6s. It is *slower* than leaving the filter above the hash join.
* **Data layout.** Pruning would need addresses clustered by file; in netflow,
  time order and address order are uncorrelated by nature. This is not fixable by
  sorting.

## The proposal

Partition the build side by the equi-key as now, but keep each partition's rows
**sorted by the band column**. A probe row then bounds its window with two binary
searches and emits the contiguous slice between them:

```
lo = lower_bound(rows, flow_start - 20s)
hi = upper_bound(rows, flow_start)
emit (build_row, probe_row) for build_row in rows[lo:hi]
```

Output is the 4.8M rows the filter would have kept, produced directly. The 2.56
billion intermediate pairs never exist, and the `Filter` node above the join is
consumed rather than run.

This is the exact form of the user's bucketing workaround, without the
approximation: bucketing is a coarse hash-based way of getting the same
locality, which is why it already delivers 8.6x. The sorted-slice form gets it
exactly, and needs no `UNION ALL` duplicating the build side into the adjacent
bucket to cover windows that straddle a boundary.

## Most of this already exists

The ASOF join is the same machine with one bound instead of two.
[native_join2.hpp:1206](../src/cpp/engine/native_join2.hpp:1206):

```cpp
int64_t match_row(const std::vector<int64_t>& rows, const AsofKey& k,
                  const Join2BuildGlobal& g) const {
    switch (op) {
        case AsofOp::GtEq: {   // largest build <= k
            auto it = std::upper_bound(rows.begin(), rows.end(), k, cmp2);
            return it == rows.begin() ? none : *(it - 1);
        }
        ...
```

Already built and in use:

* `Join2BuildGlobal::asof_keys` / `asof_str_ptr` / `asof_wide` — the per-build-row
  order key, `sort_num_key`-normalized so unsigned `<` is value order, with
  string and DECIMAL128 variants.
* `asof_index` + `asof_sorted` — hash → per-equi-group row list, sorted once at
  first probe under a `std::once_flag` (`ensure_sorted`).
* `set_asof_build_sink` / `add_asof_probe` in
  [engine.hpp:512](../src/cpp/engine/engine.hpp:512) — the plumbing that captures
  the order key on the build side.
* `build_output(probe_in, build_rows, probe_rows, err)` — takes **parallel arrays
  of (build_row, probe_row)**. The ASOF probe pushes one pair per probe row; the
  inner probe pushes a fan-out. A band probe pushes a run. The emit path needs no
  change at all.

What is missing is a two-bound mode that emits many rows instead of one, and a
planner rule to route at it.

## What changes

**Engine.** A probe operator that is `Join2AsofProbeOperator` with `match_row`
replaced by a `match_range` returning `[lo, hi)`, pushing one `(build_row,
probe_row)` pair per row in the run, and honouring `kBatch` mid-run (a single
probe row can produce more than a batch of output, which the ASOF probe never
could — this is the one genuinely new control-flow case).

**Compiler.** A `_compile_band_join` beside `_compile_asof_join`, reading a lower
and an upper bound expression plus their inclusivity, and the equi keys. Mode is
`JoinMode::Inner`, not the `LeftOuter` ASOF hardcodes.

**Planner.** A strategy that recognises, on an INNER join, an equi-key plus a
pair of cross-relation inequalities on one column of each side that together form
a bounded band, and re-types the join. This must coordinate with theta absorption
in `predicate_pushdown.py`, which currently claims one of those shapes and turns
it into a nested loop.

## Correctness obligations

* **Bound inclusivity is per-side and must survive.** `<=` / `>` in the example is
  `upper_bound` on one end and `lower_bound` on the other; swapping them shifts
  the answer by the rows exactly on the boundary. The four combinations need
  tests, not inspection.
* **NULLs.** A NULL band value on either side matches nothing, matching the
  current filter's behaviour (`WHERE` discards UNKNOWN). The ASOF probe's
  `sort_row_valid` guards are the precedent and should be kept verbatim.
* **One-sided bands are ASOF-with-fan-out, not this.** `l.event_time <=
  f.flow_start` with no lower bound selects a prefix of the sorted run, which is
  unbounded in size and usually a worse plan than the hash join. Requiring
  **both** bounds is a deliberate restriction, not an oversight.
* **INNER only**, at least initially — the same line theta absorption draws, for
  the same reason: an outer join must preserve unmatched rows, and a band that
  emits nothing for a probe row is not the same as no match on the key.
* **The filter above the join must be consumed, not duplicated.** Leaving it in
  place is correct but pointless; removing it when the band is not exactly
  equivalent is a wrong answer. Whichever is chosen must be one decision, not two.

## Cost, and the thing that blocks costing it

The band form is not unconditionally better. It sorts each equi group once
(cheap here: 272k rows across 57 groups) and gives up the CSR fast path the inner
probe uses. When the band is wide enough to select most of each group, it is the
hash join plus overhead.

**The estimates cannot currently make that call.** Re-measured 2026-08-22 after
the scan-selectivity fix (`selectivity.py` now interpolates a range across the
column's known min/max instead of charging a flat 0.25 per bound):

| node | before | after | actual |
|------|-------:|------:|-------:|
| Parquet Read / Projection (flows) | 9 | 765,009 | 1,279,999 |
| **Inner Join Draken** | 3 | 283,238 -> **7,616,547** | **2,547,359,010** |
| Filter | 1 | 94,412 | 4,814,246 |

The scan is now 1.7x low — effectively fixed, from 142,000x. **The join was still
9,000x low, and no longer had bad input to blame.**

### The join term, diagnosed and part-fixed (2026-08-22)

Re-measured on a self-contained repro of the same shape against the live catalog
(`netflow` filtered to one day, 1,486,781 rows; `dns` likewise, 86,743 rows).
The true output was computed exactly and cheaply as `SUM(|L_v| * |R_v|)` over the
per-key counts rather than by running the join:

| term | before | after | actual |
|------|-------:|------:|-------:|
| **Inner Join Draken** | 462,275 | **12,771,447** | **2,295,861,762** |
| Filter above it | 154,091 | 4,257,149 | 4,814,246 |

**What the divisor actually was.** The formula is `|L| x |R| / tdom` as
documented, and `tdom` was **278,985 — the `dns` table's pre-filter row count**,
not an NDV at all. `_equi_key_classes` estimates `tdom` per side and takes the
max; the `netflow` side reported *nothing*, so it fell through to
`min(left.domain_row_count, right.domain_row_count)`, and that stand-in then won
the `max` against the `dns` side's genuinely measured NDV of 55.

**Why the side reported nothing.** `CAST(src_addr AS VARCHAR)` is a computed
column, and `Project` was a pure pass-through in `statistics_refresh`, so the
derived identity carried no statistics. `src_addr`'s measured NDV of **10,087**
was sitting in the scan statistics one node below, unread. The cast is injective
— distinct UINT32s render as distinct dotted quads — so that NDV crosses it
exactly. `Project` now carries NDV across casts that provably cannot map two
values onto one; everything else still carries nothing, because a derived NDV
that is only an upper bound is this same stand-in problem one level down.

Measured effect: the join term goes from 4,967x low to **180x low**, and the
change is **inert on TPC-H, TPC-DS and ClickBench** — the q-error harness reports
0 operators added, removed or moved across all three suites, since none of them
join on a cast-derived key.

**The residual 180x is frequency skew, and it is not a bug in this path.** With
`tdom = max(10,087, 55) = 10,087` the textbook formula assumes the 55 `dns`
clients are a uniform 0.55% sample of the flows. Measured: those clients hold
**371,400 of 1,485,683 flow rows — 25%**. The value sets are contained; the
frequencies are not remotely uniform. This is the recorded open item that
1/NDV equality estimation runs 78-150x under on skewed keys, and it needs
per-value frequency information (an MCV list or a join histogram) rather than a
different combination rule. Note in particular that `max` is not the culprit and
must not be swapped for `min`: dividing by the measured 55 would land within
1.02x here purely by luck, and a filtered dimension with one surviving key would
then divide by 1 and predict a full cross product.

So, on choosing between hash-then-filter and the band form:

1. **Unconditional for the shape** — RECOMMENDED. If a bounded band on an
   equi-join is never materially worse than hash-then-filter, route it always and
   do not consult the estimator. Immune to the join term being wrong, which it
   demonstrably still is.
2. **Costed** — needs a band selectivity estimate (roughly
   `band_width / key_time_span`) AND the join cardinality fixed first. Still
   blocked after the fix above: an estimator answering 12.8 million for a node
   that emits 2.3 billion would pick the hash join every time, just as one
   answering 283,238 would.

Option 1 is worth a measurement before it is worth an argument: construct the
adversarial case (a band wide enough to select most of the group) and see how
much it actually loses.

## Alternatives considered

* **Teach the planner to emit the bucketing rewrite.** Delivers 8.6x today with
  no engine work. Rejected as a destination: it needs a bucket width chosen from
  statistics we do not have, it doubles the build side via `UNION ALL`, and it
  leaves a residual filter that must still be exactly right. It is the workaround
  mechanised, and it would have to be unpicked later.
* **Sort-merge both sides.** Asymptotically the cleanest, no build-side hash at
  all. Rejected for now because it needs a global sort on both inputs and a new
  operator, where the proposal above reuses a build sink, an index, a sort, and
  an emit path that all already exist.
* **Interval/range index on the build side.** Equivalent to the proposal for a
  one-dimensional band; strictly more machinery for no gain here.

## Validation

The bucketed rewrite is a **verified oracle**: both forms were run and agreed
row-for-row (one count differed by 256, from rows ingested during the 62 seconds
between the two runs). Any implementation must reproduce it exactly, on the four
bound-inclusivity combinations, with NULLs on both sides, and with an empty band.

The performance target is the bucketed form's 8.2s or better on the Pair 6 query,
against 70.6s today.

## Open decisions for the architect

1. **Unconditional for the shape, or costed?** (§Cost). Recommendation:
   unconditional, after measuring the adversarial wide-band case. The scan
   estimate was fixed on 2026-08-22 and the join term's missing-statistics half
   with it, but the join is **still 180x low** on frequency skew alone, so the
   estimator remains untrustworthy for this choice. This recommendation is
   unchanged by the estimator work, not merely un-revisited.
2. **Who owns the shape recognition** — a new optimizer strategy, or an extension
   of the theta-absorption arm in `predicate_pushdown.py` that currently claims
   this shape and produces a nested loop? These two must not both fire.
3. **Does this get SQL surface**, or is it planner-only? An explicit band-join
   syntax is a separate question from optimising the shape users already write;
   the proposal assumes planner-only.
4. **Which side builds.** The build side is the one sorted by the band column;
   `join_ordering` currently chooses on cardinality and NDV. The band form may
   want a different rule, and that interacts with the row-count guard added in
   the Rule 3 work.
