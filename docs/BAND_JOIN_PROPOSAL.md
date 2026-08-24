# Proposal: Band Join (range-matched equi-join)

**Status:** AGREED AND DELIVERED, 2026-08-23. The architect ruled on all four open
decisions (recorded in [Open decisions](#open-decisions-for-the-architect)) and the
band join is implemented, measured and tested. The body below is kept as WRITTEN,
including the parts measurement later contradicted — see
[What shipped](#what-shipped-2026-08-23) for the delta. Notably, the adversarial
wide-band case the cost section treats as the open risk was measured and the band
does NOT lose there; it wins by 3.7-4.3x.
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

✅ **Both obligations met — see [What shipped](#what-shipped-2026-08-23-plus-the-nested-loop-fixes-of-2026-08-24).**
Correctness is pinned by `tests/sql/test_band_join_execution.py` against a genuine
unoptimised oracle (`disable_predicate_pushdown`), covering all four bound-inclusivity
combinations — whose row counts must DIFFER from each other, or the fixture has no row
on that edge and the test pins nothing — plus NULLs on both sides AND in both bounds,
an empty band, an inverted band, and one probe row spanning ~5 output batches. Verified
to bite: negating bound inclusivity in `_recognize_band` fails all four combinations.

⛔ The 8.2s target was NOT measured against the live catalog — that needs cloud data
this work did not run. It was measured on a local fixture of the same shape and size
(`testdata/band_scale`), where the band form answers in **0.375s** against **154.0s**
for hash + `Filter`. The ratio (418x) is the claim; the absolute seconds are not
comparable to the 70.6s/8.2s figures above, which were taken on different hardware
against a live, growing table.

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

---

## What shipped (2026-08-23, plus the nested-loop fixes of 2026-08-24)

### Rulings

1. **Unconditional for the shape.** Not costed. Vindicated by measurement below —
   there is no measured crossover to cost against.
2. **`join_ordering` owns recognition**, at its existing retype site
   ([join_ordering.py:396](../opteryx/planner/optimizer/strategies/join_ordering.py:396)),
   as `if band -> "band" elif non-equi -> "nested loop"`. One decision point, so the
   band and the theta-nested-loop cannot both claim the shape — structurally, not by
   coordination. `predicate_pushdown` keeps only MOVING the predicate into `on`.
3. **Planner-only.** No SQL surface.
4. **`join_ordering`'s build-side rule stands**, untouched, including the Rule 3
   row-count guard.

### The prerequisite nobody had spotted

The recogniser reads `node.on`, and `PredicatePushdownStrategy` runs before
`JoinOrderingStrategy` — so a band only reaches recognition if theta absorption put
it there. It was DECLINING on the CTE spelling, which is the spelling this
document's own motivating query uses. Traced at the relations gate: a relation
crossing a CTE boundary is known by several names (the user's alias AND the minted
`$view-XXXX`), and the gate demanded the predicate name EXACTLY the two names the
join names.

| spelling | `predicate.relations` | `join_relations` | before |
|---|---|---|---|
| plain aliases | `{f, l}` | `{f, l}` | absorbed |
| CTE | `{f, l, $view-a, $view-b}` | *identical* | **DECLINED** |
| subquery | `{f, $planets, l}` | `{f, l}` | **DECLINED** |

Fixed for the CTE case by adopting the gate the CROSS arm already uses (names one
leg, names the other, names nothing outside), keeping the strict two-name form for
the case where the two legs SHARE a name and an intersection cannot attribute it.
Across TPC-H(22) + TPC-DS(99) + JOB(113), **6 plans change**, all consuming a Filter
into a keyed join, all answers identical, all 1.02-1.21x FASTER.

🔴 **The subquery spelling still declines** and is deliberately out of scope: it
needs column availability as ground truth, not a looser set test.

### Measured

Local fixture of this document's shape (`testdata/band_small`, regenerated by
`dev/generate_band_fixtures.py` — the perf fixtures are gitignored: 120k flows, 25k
lookups, 57 clients, both spanning the same 24h window so range transport prunes
nothing). Medians of 3 scoring rounds, warm-up discarded, arm order reversed on
alternate rounds, fresh process per run, nothing else on the box. All arms return
**identical answer digests** at every width.

| band width | hash + `Filter` | nested loop residual | **band** | band vs `Filter` |
|---|---:|---:|---:|---:|
| 20s | 2.358s | 2.369s | **0.117s** | **20.2x** |
| 300s | 2.401s | 2.364s | 0.126s | 19.1x |
| 3600s | 2.527s | 2.516s | 0.202s | 12.5x |
| 21600s | 3.141s | 3.130s | 0.518s | 6.1x |
| 86400s | 3.765s | 3.720s | 1.022s | 3.7x |
| 172800s (**adversarial**) | 3.731s | 3.716s | **1.017s** | **3.7x** |

Measured 2026-08-24, AFTER the nested-loop fixes below. The nested-loop arm is now at
parity with hash + `Filter` at every width — before those fixes the same sweep had it
1.15-1.16x slower (2.719s and 4.194s at the two ends). The `Filter` and band arms moved
by under 4% across the two sweeps, which is this fixture's run-to-run spread.

**The adversarial case does not exist.** At 172,800s the band spans twice the data's
whole range: `EXPLAIN ANALYZE` confirms both arms emit the SAME **26,194,532** rows,
so the band prunes NOTHING and pays for the sort and the bisects anyway — and it is
still **3.7x** faster. The reason is that the band replaces PER-PAIR predicate
evaluation with per-probe-row bisection plus a pure gather: on identical output, the
join node is **641.7ms** against the nested loop's **3,379.5ms** (both re-measured
2026-08-24, after the nested-loop fixes; before them the nested loop's node was
3,925.7ms, so those fixes narrow this margin without closing it).

⛔ **This removes the premise §Cost is built on.** The section reasons about a
crossover where "the band is the hash join plus overhead". No such crossover was
found; the overhead is smaller than the per-pair work it removes.

#### At full scale

`testdata/band_scale` — 1.28M flows x 272k lookups over 57 clients, the ~6.1 BILLION
pairs the equi-join would otherwise form, which is this document's Pair 6 shape.
All arms return the identical answer digest.

| arm | time | vs band |
|---|---:|---:|
| nested loop residual (the plan when absorption fires) — *before* the 08-24 fixes | 301.727s | 816x |
| nested loop residual — **after** those fixes | 159.278s | **431x** |
| hash join + `Filter` above (this document's 70.6s baseline) | 154.009s / 155.262s / 158.125s | **418x** |
| **band join** | **0.375s / 0.364s** | — |

⛔ Read the two nested-loop rows as ONE arm measured twice, not as a choice. The
absorbed plan was 301.7s and is now 159.3s; nothing about the band changed between
them. The band arm and the `Filter` arm are untouched by those fixes — neither
`_compile_band_join` nor the no-residual path reads the code that changed.

✅ **Theta absorption WAS 1.95x slower than hash-then-filter at this scale**
(301.7s vs 154.6s) — and that turned out to be fixable overhead, not an algorithmic
trade-off, so the "should absorption be costed" question raised here is CLOSED with
the answer "no". `nested_loop` and `inner` are the SAME build/probe (both
`JoinMode::Inner`); `nested_loop` only moves the filter inside. Two costs, both
VARCHAR-shaped, fixed 2026-08-24 in `compiler.py`:

1. `residual = node.on` re-checked the KEYED equality on every emitted pair — the hash
   key had already decided it. Now stripped by `_residual_without_keyed_equalities`.
2. Any residual disabled payload pruning outright, so the pair gather carried every
   column of both legs. It now keeps `live | residual operands`.

Single-variable at 6.1bn pairs, identical answer digests throughout:

| | time |
|---|---:|
| neither fix | 301.7s |
| residual strip only | 213.3s |
| **both** | **159.3s** |
| hash + `Filter` — what it was losing to | 158.1s |

Absorption is now at parity, so the CTE gate fix above carries no cost exposure. Full
suite: **167 failures against a 168 baseline, zero new**.

⛔ Two traps, both of which cost time here. **`EXPLAIN` does not invoke the compiler**,
so tracing anything in `compiler.py` and running `EXPLAIN` prints nothing and looks
exactly like dead code — trace real execution instead. And **a `COUNT(*)` query cannot
see the pruning fix**, because `live` is empty and empty means UNKNOWN, so pruning is
off either way; it measures exactly zero there and 1.34x on a query with a live payload.

### Known gaps

🔴 **Bound inversion is not implemented.** The band column must land on the BUILD
leg, which is `join_ordering`'s existing cardinality choice — so whether the
optimisation fires depends on which relation is smaller. A two-sided band with
literal offsets IS invertible (`l.t <= f.t AND l.t > f.t - 20s` IS
`f.t >= l.t AND f.t < l.t + 20s`), which is what the ruling on decision 4 relies on
to leave the build-side rule untouched. Until it lands, a probe-side band declines
to today's plan and is counted as
`optimization_band_join_declined_probe_side`. Pinned by
`test_a_probe_side_band_declines_and_still_answers`.

🔴 **Cross-type bands decline.** The band column and both bounds must carry the same
`ColumnType`, because the bisect assumes both sides normalise to the same order.
ASOF solves this with a coercion CAST; taking that here means re-arguing the four
inclusivity edges against the coerced type first.

### Where it lives

* Recognition + retype — `join_ordering.py` (`_recognize_band`, `_band_or_nested_loop`)
* Leg attribution — `join_helpers.band_operand_leg`, beside `hoistable_operand_leg`
* Plan node — `opteryx/operators/band_join/band_join.pyx`
* Compile — `compiler._compile_band_join`; bounds materialised as synthetic probe
  columns via `_add_computed`, the `_compile_asof_join` precedent, so the payload and
  the declared output are untouched
* Engine — `BandProbeOperator` in `native_join2.hpp`, sharing `SortedGroupProbeOperator`
  with the ASOF probe; build side reuses `set_asof_build_sink` unchanged
* Tests — `tests/sql/test_band_join_execution.py`
