# Exact, order-independent float SUM / AVG — design note

Status: **PROPOSAL — awaiting architect decisions (§8). Nothing built, nothing measured.**
Date: 2026-11-22. Prompted by ArrowMetal's exact grouped Float64 sum (128-bit fixed point).

## 1. Problem

Float `SUM`/`AVG` results depend on the order partial sums are combined in, and that
order is not fixed: which worker gets which morsel, and the order workers merge, vary
run to run. Integer and DECIMAL `SUM` are already exact (`__int128`); float `SUM` is a
plain `double +=`.

The variation is visible to users, not just in the last bit: TPC-DS Q75 returns
different rows run to run because a float SUM feeds a `LIMIT` cut (recorded 2026-10-15
in the groupjoin work). The same applies to anything that sorts, compares, joins or
groups on a float aggregate.

## 2. Where float sums accumulate today

| Site | Code | Notes |
|---|---|---|
| Ungrouped / AggCell lane | `AggCell::fsum`, [native_group_sinks.hpp:449](../src/cpp/engine/native_group_sinks.hpp) | per-row `agg2_update` (:908, :920), DISTINCT-operand fold (:2732) |
| Ungrouped morsel fold | `agg2_sum_morsel` (:780) → `draken::ops::float_sum` | 8 independent double lanes (`FpSumAcc`, [float_ops.h:507](../draken/ops/float_ops.h)) |
| Grouped lane | `GBLanes::f64` (:3482) for `SumF`/`AvgF` | per-row (:3722), pair path (:3906), few-groups 4-lane acc (:5422), (:5527) |
| Partition / worker merge | `D.f64[m] += S.f64[e]` (:6491, :7494) | **the main source of the variation** |
| GROUP BY spill | `gb_each_i64_lane` (:4263) | `SumF`/`AvgF` are spillable 8-byte lanes |
| Groupjoin | `x * rows[i].d` ([native_groupjoin.hpp:555](../src/cpp/engine/native_groupjoin.hpp)) | multiplies an already-rounded sum by the join multiplicity |
| Window frames | `sum_f64 += sign * d` ([native_window_frame.hpp:418](../src/cpp/engine/native_window_frame.hpp)) | running add-then-subtract: deterministic per partition, but loses accuracy (add 1e20, later subtract 1e20, small values gone) |

`AVG` reads the `SUM` lane, so fixing `SUM` fixes `AVG`.

## 3. Options considered

- **A. Leave it; document it.** SQL doesn't require reproducible float sums. This doesn't
  fix Q75.
- **B. Fix the merge order** (key partials by morsel ordinal, merge in ordinal order).
  Rejected: needs per-group state per morsel, and the morsel-to-worker assignment still
  decides each worker's running sum.
- **C. Compensated summation** (Kahan, Neumaier, double-double). Rejected: the error is
  smaller, but the result still depends on order, so Q75 would still vary. It would look
  fixed without being fixed.
- **D. Exact accumulator, rounded once at emit (recommended).** The exact sum of a set
  of doubles doesn't depend on order, so every plan, worker count and merge order gives
  the same bits, and the result is correctly rounded.

## 4. Proposed representation (D)

Two exact forms, chosen per group by the data:

**D-inline: fixed-point window** — `(__int128 A, int16 E)`, value = `A · 2^E`.
- Decompose `x = m · 2^e` (`m` a signed integer, `|m| < 2^53`), stripping trailing zero
  bits from `m` (`ctz`), so whole numbers and round values cost little headroom.
- If `e >= E`, add `m << (e-E)`. If `e < E`, shift `A` left by `E-e` and set `E = e`.
  Both steps are overflow-checked; neither ever rounds.
- It stays exact while `max|running sum| / min|value| < ~2^75` (≈ 3.8e22). Example: values
  down to 0.01 can total up to ~3.8e20 before the inline form runs out.
- Lane cost is 16 + 2 bytes against 8 today. `GBLanes` already has a spillable `i128`
  lane and `i64` lanes, so this reuses existing lane types: mantissa in `i128`, exponent
  and sticky flags in an `i64`.

**D-full: superaccumulator** (Radford Neal's "small superaccumulator": ~67 × 64-bit
chunks, ≈ 0.5 KB). Exact for every finite double at any count.
- An inline group whose next add or shift would overflow **promotes** to D-full
  out-of-line. Promotion moves the exact value, so the final bits are identical whichever
  form a group ends in.
- That makes this a change of representation, not a fallback: no path rounds early or
  answers differently. It still needs a ruling (§8 Q3).
- Ungrouped sinks and window frames have one accumulator each, so they can use D-full
  directly and skip the inline form.

**Specials** — sticky per-group flags, independent of order:
- any NaN → NaN
- both +Inf and −Inf → NaN
- one sign of infinity → ±Inf
- otherwise the exact sum.

This matches IEEE sequential summation except where a running double overflows midway:
today that gives ±Inf even when the final sum is finite; exact gives the finite answer.
Zero comes out +0.0, which `fp_canon` already does.

**Emit** — round `A · 2^E` (or the superaccumulator) to the nearest double, ties to even.
Converting `__int128` → `double` then calling `ldexp` is correct except when the result
is subnormal (double rounding), so that case gets explicit rounding. Overflow → ±Inf.

**Merge** — align exponents and add, or add chunk-wise for D-full. Both are exact, so
merge order no longer matters.

**Groupjoin** — multiply the exact form by the integer multiplicity: an overflow-checked
`int128 × d`, promoting on overflow, then round once. Today's `x * d` rounds twice.

**Window frames** — a running D-full accumulator, where subtraction is exact. This also
fixes the add-then-subtract accuracy loss.

## 5. Not covered

`STDDEV`/`VAR_*`/`CORR` stay order-dependent. Their shift `K` is the first value a cell
sees, which is itself order-dependent, and making the moments exact would need exact
squares (106-bit products) — a separate, larger change. `draken::ops::float_sum` (used
by `Vector.sum()`) is deterministic for a given vector and needs no change unless
ungrouped SUM keeps calling it.

## 6. Cost (to be measured, not assumed)

- **Hot path:** today one FP add into an 8-byte lane. Inline: extract the bits, `ctz`,
  compare exponents, a shift and an overflow-checked int128 add — several times the
  instructions, with one data-dependent branch (does the exponent move?), which is rare
  once a group's smallest value has been seen.
- **Cache:** the SumF/AvgF lane footprint grows ~2.25x. This matters most for
  high-cardinality float GROUP BY.
- **Reach:** ClickBench sums integer columns, so float SUM is barely exercised there. The
  queries that will move are float-heavy TPC-DS/TPC-H and user workloads.
- **Ungrouped D-full:** Neal's design adds each value to a chunk picked by its exponent,
  with an occasional carry pass. It needs measuring against `FpSumAcc`'s 8 SIMD-friendly
  lanes.

## 7. Validation plan

- **Oracle:** Python's `math.fsum` is correctly rounded, so it is an exact oracle with no
  numpy needed. Unit tests compare `SUM` against `math.fsum` on adversarial inputs:
  cancellation (`1e308, 1, -1e308`), subnormals, ±`DBL_MAX`, mixed infinities, NaN,
  exponents spanning beyond 2^75 (forces promotion), a promotion in the middle of a merge,
  and groupjoin multiplicities.
- **Order independence:** the same data shuffled across 1/2/4/8/16 workers, and run
  through the spill path, must give bit-identical results.
- **Q75:** N back-to-back runs must give identical rows.
- **Benchmarks:** A/B (interleaved, not A/A) on TPC-DS and TPC-H plus a float-heavy
  high-cardinality GROUP BY, reporting the per-query delta. ClickBench as a check that
  nothing moved.
- **Reference fuzzer:** last-bit differences against DuckDB are expected; confirm its
  float tolerance absorbs them. SLT renders floats to 3 decimals, so expectations should
  not move apart from the rare rounding-boundary case.

## 8. Decisions for the architect

1. **Do it at all?** The alternative is option A: document that float SUM can vary run
   to run.
2. **Default or opt-in?** Exact as the semantics of `SUM`/`AVG` on floats, or as a
   separate function (e.g. `EXACT_SUM`) with `SUM` unchanged. Recommendation: default,
   provided the measured cost on float-heavy benchmarks is acceptable.
3. **Two-tier representation** (inline window that promotes to a superaccumulator).
   Is this acceptable under "no fallbacks", given both forms are exact and give the same
   answer? The alternative is D-full for every group, at ~0.5 KB per group, which is not
   viable at 100M groups.
4. **Spilling promoted groups:** serialise the superaccumulator (fixed 0.5 KB record), or
   refuse spill when a promoted group exists (loud)?
5. **Accept the semantics change:** results become correctly rounded (last-bit
   differences from earlier releases and from DuckDB), and an intermediate overflow no
   longer gives ±Inf.
6. **STDDEV/VAR/CORR** stay order-dependent for now (§5). Accept, or scope them in?
