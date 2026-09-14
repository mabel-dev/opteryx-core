# Semi / Anti Join Cardinality — Design & Decision

**Status:** LANDED 2026-09-14. §4.1 (per-side NDVs), §4.2 (NDV responds to
filters, plus the `base_distinct_count` domain channel), §4.3 (narrowing
sides) and §3 (the model) are all in. The semi/anti branch no longer returns
`left.row_count`. §5 remains deliberately out of scope and is where the
REMAINING Q21 semi error lives.
**Date:** 2026-09-13. FK-as-hint ruled 2026-09-13; §7 Q2/Q3 ruled 2026-09-14;
§3 rewritten to the in-domain formula 2026-09-14.
**Scope:** `_join_stats`' semi/anti branch in
`opteryx/planner/optimizer/statistics_refresh.py`, and the `semi`/`anti` arms of
`opteryx/planner/cost_estimation/join_cardinality.py`.

---

## 1. The defect

`statistics_refresh._join_stats` handles all five semi/anti spellings
(`left semi`, `left anti`, `left anti null-aware`, `left semi not-distinct`,
`left anti not-distinct`) in one branch that returns:

```python
row_count_estimate=left.row_count
```

That asserts a join whose entire purpose is to reduce produces **no reduction**.
It is a false claim about the relation, independent of any query. Two further
problems in the same branch:

- The comment justifies it on COLUMNS ("semi/anti emit only left-side columns;
  right contributes nothing"), which is true and is a statement about the
  schema. The row count does not follow from it.
- The telemetry note hardcodes `key_count=0` while the node carries
  `left_columns` / `right_columns`. A reader of `join_estimates` cannot tell a
  keyed semi join from a keyless one.

TPC-H Q21 at SF100 is where this was found, not what it is for: `LEFT SEMI`
estimated 15,012,825 against an actual 7,048,027 (2.1x over) and `LEFT ANTI`
estimated the same 15,012,825 against an actual 396,100 (38x over). The
estimates are load-bearing — `SemiJoinPushdownStrategy` declines on this
arithmetic (`_est_rows`, `semi_join_pushdown.py:87`) and join costing reads it
generally.

## 2. Why the existing unreachable arms are not the fix

`estimate_join_cardinality` already has `semi` and `anti` arms
(`join_cardinality.py:312-315`) and `_VALID_JOIN_TYPES` already admits both.
They are unreachable: `_JOIN_TYPE_FOR_CARDINALITY` (`statistics_refresh.py:105`)
has no entries for them and the branch returns at line 1046, before the key lookup
at line 1078. Wiring them up as they stand would be a regression, not a fix:

| arm | formula | behaviour |
|---|---|---|
| `semi` | `min(L, inner)` | `inner = L·R/ndv`, so it only reduces when `R < ndv` — never for a fact-table right side. Returns `L`: today's answer. |
| `anti` | `max(0, L − inner)` | collapses to the `max(1, …)` floor whenever `inner >= L`, which is the common case. Replaces a 15M over-estimate with a **1-row** under-estimate. |

Both must be replaced, not merely reached.

## 3. The model — one formula

**Revised 2026-09-14 (architect).** An earlier draft of this section had three
rungs with a containment ratio `min(1, ndv_right / ndv_left)` at its centre.
That form is wrong for the dominant case and is NOT what is specified here; see
§3.4 for why it was replaced, so the same formula is not proposed again.

### 3.1 The formula

**LANDED 2026-09-14** as `_match_fraction` / `_semi_estimate` in
`join_cardinality.py`, reached through `_join_stats`' `_SEMI_ANTI_ESTIMATOR`
branch. `KeyStats` grew a `live_ndv` field to carry the post-filter count
alongside the domain `ndv`; the five planner spellings became five join types
in `_VALID_JOIN_TYPES` rather than flags, because they are five different
answers. The old `semi`/`anti` arms (`min(left, inner)` / `max(0, left-inner)`)
are GONE, not bypassed — see §3.4.

Two implementation notes that are contract, not detail:

* **No occupancy bound is applied to semi/anti.** `apply_occupancy_bound` caps
  a PRODUCT OF DIVISORS by collapsing the class list into one synthetic pair.
  A match fraction is not a divisor product, and the collapsed pair carries no
  `live_ndv` — applying it would silently turn every composite semi/anti key
  into "cannot estimate".
* **The complement truncates once.** `int(semi)` and `int(left − semi)` floor
  independently and lose a row whenever `semi` has a fractional part, so
  "every left row either matched or did not" stops being true of the numbers
  actually RETURNED. The anti arms subtract the already-truncated semi.


A semi join emits the left rows whose key is present in the right relation. The
fraction of left keys so present is estimated by measuring the right's LIVE key
set against the shared key DOMAIN both sides draw from:

```
match_fraction = min(1.0, ndv_right_live / domain_ndv)

semi = |L| · (1 − null_fraction_left) · match_fraction
anti = |L| − semi
```

- `ndv_right_live` — distinct keys the right relation holds NOW, after any
  filter. This is the number §4.2's scaling produces.
- `domain_ndv` — the size of the key domain, measured BEFORE any filter. This
  is the number §4.2's `base_distinct_count` channel preserves.

### 3.2 Where `domain_ndv` comes from

`domain_ndv = max(base_ndv_left, base_ndv_right)` — the same "tdom stands in
for `max(ndv_left, ndv_right)`" convention `_equi_key_classes` already applies,
evaluated PRE-filter instead of post.

**A declared foreign key overrides the max with the referenced side's domain.**
The FK asserts that the left's keys are drawn from the right key's domain, so
that domain is the denominator — `domain_ndv = base_ndv_right`. This is the
whole of the FK hint's role: it does not select a branch or short-circuit the
estimate, it names which domain the ratio is against. Ruled 2026-09-13; the
hint/fact gate in §3.3 still applies to it.

**Interaction with the NDV stand-in, which biases semi DOWN.** When the left's
key NDV is absent it is stood in for by a relation size
(`NdvProvenance.DOMAIN_STANDIN`). A fact table whose key NDV is a stand-in
"looks like" 150M distinct values, inflating `max(...)`, shrinking
`match_fraction`, and under-estimating a semi join that in truth matches
everything. Two mitigations, both already present: a declared FK replaces the
max outright, and per-side `ndv_provenance` (§4.1, landed) is what lets this
branch SEE that the left number was never counted. A stand-in on the left with
no FK is the case to distrust; this relates to the open
`keystats_tdom_standin_defeats_occupancy_guard` item.

### 3.3 Limit cases the formula already covers

| situation | `ndv_right_live` | result |
|---|---|---|
| right unfiltered, FK declared | `= domain_ndv` | `match = 1` → semi = \|L\|, anti = 0 |
| right filtered | `< domain_ndv` | the ratio, continuously |
| right holds a small slice of a large domain | `<< domain_ndv` | `match → 0` → semi → 0, anti → \|L\| |

The unfiltered-FK row is the reason this replaces three rungs with one: the
"provable" case is the formula's LIMIT, not a separate branch. Nothing has to
decide which rung applies.

**A hint may never become a fact.** Unchanged from the 2026-09-13 ruling and it
still governs the FK path above: an FK-derived number is constructed as
`row_count_estimate`, never `row_count_metric`, and must not reach file
pruning, `COUNT` answered from the manifest, or limit elimination — the things
`Manifest.stats_are_authoritative` already gates (see
`postgres_connector.py:397`). A wrong assertion must degrade to a bad plan,
never to a wrong answer. Note that anti = 0 under an FK is a real row count
being claimed from a user assertion, which is exactly why it is an estimate.

### 3.4 Why the containment form was rejected

`min(1, ndv_right / ndv_left)` measures the right against the LEFT rather than
against the domain. A filtered right side still holds far more distinct keys
than the left has rows, so the ratio pins at 1.0, semi returns `|L|` and anti
returns 0 — floored to 1 by §4.4. Measured on Q21's anti at SF100:

| formula | semi | anti |
|---|---|---|
| containment `min(1, ndv_R/ndv_L)` | 15,012,825 | **0** |
| in-domain `ndv_R_live / domain_ndv` | 14,731,460 | 281,365 |
| ACTUAL | 7,048,027 | 396,100 |

The containment form replaces a 38x over-estimate with a floor — a new false
claim in the opposite direction, which is not a partial fix. (The semi column
is discussed in §6: neither formula moves it, because its reduction is the
unmodelled residual of §5.)

### 3.5 Nulls, and the two variants that are not this formula

**The complement is exact, and is the null-safe form.** Every left row either
matches or does not, so `anti = |L| − semi` is arithmetic rather than a second
estimate. It is also the form that handles nulls correctly: a NULL left key
never matches, so it always survives anti and never survives semi.
`_effective_rows` already strips the left null fraction before the estimate, so
subtracting puts those rows back. Writing anti directly as
`|L|·(1−nf)·(1−match_fraction)` looks more natural and silently drops them.
**Do not write the direct form.**

The complement does amplify relative error: when anti is genuinely small, an
absolute error in semi becomes a large relative error in anti. That is a
property to state, not to correct — the alternative is two estimates that do
not sum to `|L|`.

**`left semi not-distinct` / `left anti not-distinct`** are INTERSECT/EXCEPT,
where NULL is an ordinary value equal to itself (IS NOT DISTINCT FROM). They
take the formula WITHOUT the left-null exclusion, because a NULL left key does
match there.

**`left anti null-aware`** is `NOT IN`, and is not this formula at all: a single
NULL in the right key makes every comparison UNKNOWN and the join emits ZERO
rows. It needs its own branch, keyed on the right side's null fraction:

- right null fraction known and `> 0` → the floor (§4.4), meaning zero.
- right null fraction unknown (`None`) → cannot establish it; use the formula.

The current code lumps all five spellings together.


## 4. Prerequisites and consequences

### 4.1 Per-side NDVs are destroyed today (blocks §3)

`_equi_key_classes` computes `side_tdoms` per side and then writes
`tdom = max(...)` into **both** `KeyStats` (`statistics_refresh.py:976-987`).
So the two sides are indistinguishable and §3 returns exactly today's
non-reducing answer.

**The fix is to stop collapsing, and it is arithmetically neutral.** Every
current consumer of a pair already takes `max(left.ndv, right.ndv)` back out:
`_key_selectivity` (`join_cardinality.py:87`) and `apply_occupancy_bound`
(`join_cardinality.py:175`). Null fractions are already per-side. So
inner / outer / cross / asof estimates come out **byte-identical**; only the new
semi/anti arm reads the two apart. `ndv_provenance` becomes per-side as a
side-effect, which is a correctness improvement in its own right — today a
MEASURED left tying an unmeasured right is reported under one label.

**LANDED 2026-09-14.** Verified by exhaustive comparison rather than
inspection: 6,765,120 combinations of (per-side NDV x per-side provenance x
domain sizes x row counts x join type), single-key and two-class composite,
pushed through `apply_occupancy_bound` and `estimate_join_cardinality` under
both the old collapsed pairs and the new per-side pairs — **zero differences**.

One consequence that is NOT free, found by that check and fixed with it:
`apply_occupancy_bound`'s `any_measured` read `left.ndv_is_measured or
right.ndv_is_measured`. While both slots carried the same collapsed provenance
that expression meant "was the side supplying the MAX measured?"; with per-side
provenance it would silently have become "was EITHER side measured?", so a
MEASURED 100 opposite a DOMAIN_STANDIN 1000 would have suppressed the widening
even though the 1000 is the factor actually multiplied into `composite`. The
test now asks the question the code means — is the side that SUPPLIED the
factor measured, ties counting either side — which reproduces the old
behaviour exactly and is what the docstring already claimed.

Three existing tests asserted the collapse itself
(`left_key.ndv == right_key.ndv`). Their docstrings show the invariant they
protect is the DIVISOR, `max(ndv_left, ndv_right)`, which is unchanged; the
assertions were moved onto the divisor and extended to pin each side's own
value. They were not weakened: each still fails against the pre-change source.

### 4.2 NDV does not respond to filters (blocks §3)

`_filter_stats` never calls `_cap_ndvs` on its result, and
`_narrow_filter_columns` only touches columns the predicate actually names. A
`WHERE l_receiptdate > l_commitdate` drops lineitem 600M → 15M rows while
`l_orderkey.distinct_count` stays at its base 150M — an NDV larger than the row
count of the relation carrying it.

Harmless today, because nothing divides one NDV by another. **Fatal under §3:** both sides read the base NDV, the ratio is 1.0, and anti returns the floor
of 1. That is a new false claim, not a partial fix.

**RULED 2026-09-14: both, layered.** They are not alternatives — they answer
different questions and compose:

| | treatment | role |
|---|---|---|
| **(b)** | NDV responds to selectivity, e.g. `ndv · (1 − (1 − s)^(rows/ndv))` | the MODEL — an estimate of how many distinct values survived |
| **(a)** | cap NDV at row count | the INVARIANT — a relation cannot hold more distinct values than it has rows |

Order matters: scale first, then cap. The cap is a backstop on the model's
output, not a substitute for it — applied alone it leaves `ndv_L = ndv_R` on
the common shape and §3 inert. The model applied without the cap can exceed the
row count and would hand §3 a ratio built from an impossible number. `_cap_ndvs` is the existing implementation of the invariant; the
scaling is `surviving_distinct_count`.

**LANDED 2026-09-14**, with two findings that are the substance of it.

**It was inert where it mattered.** Implemented in `_filter_stats` alone, the
scaling never ran on a real plan: a pushed-down predicate is folded into the
SCAN, `_filter_stats` takes its `applied_any is False` early return, and
neither the scaling nor the cap executes. Measured on
`lineitem WHERE l_receiptdate > l_commitdate`: rows fell 60,175 -> 20,058 at
the scan while `l_orderkey` still reported 60,000 distinct values — more
distinct values than rows. `_scan_stats` has TWO such reduction sites (the
leaf-local fold and `node.predicates`); both needed the same treatment. Do not
assume a filter's statistics are applied at the Filter node.

**Scaling in place fed a POST-filter NDV to the join-key divisor, and that is a
documented must-not.** `_build_equiv_tdoms` warns that the divisor reads
PRE-filter sizes because "a key domain is a property of the relation as stored;
a filter removes ROWS, not the values the key column could hold", and that
reading the post-filter number "charges the filter's selectivity a second time
inside the divisor". That warning is written about the row-count FALLBACK; the
same error simply arrived through `distinct_count` instead once it became a
post-filter number. Measured on the TPC-DS Q54 shape in
`test_predicate_pushdown_across_barrier`: the filtered dimension stopped
predicting any reduction and DPccp led with the UNFILTERED 100,000-row customer
table — the leading join went from 8,206 rows to 2,150,243, with scaling-only
and cap-only both reproducing it independently.

The fix is what `base_distinct_count` exists for: the three join-divisor sites
(`plan_adapter._key_stats`, `_build_equiv_tdoms`, `_equi_key_classes`) read
`domain_distinct_count`; everything measuring the LIVE relation (equality
selectivity, group-by cardinality, join-output NDV) keeps reading
`distinct_count`. Join order and the full failure set return to the pre-change
state exactly, and the scaling still moves — the two numbers now go to
different consumers instead of one number going to both.

### 4.3 `_intersect_join_keys` defaults the wrong way for anti — LANDED 2026-09-14

`_NARROWABLE_JOIN_SIDES.get(estimator_type, ("left", "right"))`
(`statistics_refresh.py:1172`) defaults to narrowing **both** sides. Routing
semi/anti through it without adding entries would narrow the anti join's left
key to the intersection — the exact opposite of what an anti join emits, and
the class of error the function's own docstring warns about (a consumer reading
that range as truth transports it onto a scan and drops rows).

Required entries:

- `"semi": ("left",)` — a semi join emits only matching left rows, so the left
  key IS bounded by the intersection. The right is not emitted at all.
- `"anti": ()` — anti emits the left rows that did NOT match. Their keys lie in
  the COMPLEMENT of the intersection; narrowing to it would describe a relation
  the join never produces.

### 4.4 The floor stays at 1

`estimate_join_cardinality` returns `max(1, int(result))` so a zero estimate
cannot propagate as a multiplicative zero through downstream cost arithmetic.
The unfiltered-FK limit (§3.3) and null-aware NOT IN (§3.5) both want to say
zero. They must accept the floor of 1 and say so
in the telemetry note rather than special-casing it — a 1 here means "we
believe zero", and the floor is a deliberate existing contract.

### 4.5 Telemetry

`_join_note` must receive the real `key_count` (the pre-occupancy-bound class
count, matching what the equi-join path already reports at line 1108), not the
hardcoded `0`. Which branch produced the number, and what `domain_ndv` was,
should be recoverable from
`join_estimates`, or the same investigation happens again.

## 5. Explicitly out of scope

- **Residual selectivity.** `_join_stats` hardcodes
  `extra_predicates_selectivity=1.0` for every join type (line 1118). Q21's
  semi join reduces almost entirely through its correlated residual
  (`l2.l_suppkey <> l1.l_suppkey`), so nothing in this document moves that
  estimate. Opening that channel is a separate change and a separate decision.
- **`JoinBuildShapeStrategy` / `declined_no_estimate`.** A separate root cause:
  `_BUILD_PAYLOAD_JOINS` (`join_build_shape.py:62`) omits semi/anti, so the
  compiler passes `-1` and `decide_consolidation` tests `est_rows < 0` FIRST
  (`native_join2.hpp:909`), before any payload check. Its stated rationale —
  semi/anti "have no build gather to improve" — is false when a correlated
  residual is present, because `compiler.py:4417` sets
  `semi_no_payload = mode in semi_anti_modes and filter_residual is None`. That
  is a real defect, unrelated to the model, and needs its own call.
- **DPccp.** Semi/anti edges do not reach the join-ordering enumeration; not
  touched here.

## 6. Verification

- The neutrality claim in §4.1 is testable directly: every non-semi/anti
  estimate must be byte-identical across the change. That is the gate on it, not
  a judgement call.
- Unit coverage for each case in §3.3/§3.5 and for the `_NARROWABLE_JOIN_SIDES` entries in
  §4.3, including the anti-must-not-narrow case.
- `make q` must pass. Note the pre-existing failure baseline: `tests/unit`
  ~118, 11 in `test_groupby_advanced.py`, 5 in `tests/misc`.
- Q21 at SF100 is a diagnostic, NOT an acceptance criterion. Under this design
  the SEMI estimate provably does not move (`ndv_left <= 15M` against a right
  side with `ndv(l_orderkey) = 150M` gives `min(1, >=10) = 1.0`); its error is
  the unmodelled residual in §5. Do not read an unchanged Q21 semi as a failed
  implementation.

## 7. Decision needed from the architect

1. ~~**Rung 1.**~~ **RULED 2026-09-13:** declared foreign keys are usable as
   estimation hints — with no or limited data, a user-asserted relationship is
   information. An FK-derived number is an ESTIMATE, and is gated out of
   pruning / counting / limit elimination exactly as `stats_are_authoritative`
   gates manifest hints. Folded into §3.2/§3.3; no longer open. The FK's role
   narrowed on 2026-09-14: it names the ratio's denominator rather than
   selecting a branch.
2. ~~**§4.2.**~~ **RULED 2026-09-14:** BOTH, layered — the selectivity response
   is the model, the row-count cap is the invariant applied to its output.
   Folded into §4.2; no longer open. Neither is implemented yet.
3. ~~**Sequencing.**~~ **RULED 2026-09-14:** land §4.1 first. Done — see §4.1.

**Nothing in this document is open.** §3, §4.1, §4.2 and §4.3 are all landed.

**What is NOT fixed, by design.** §5's exclusions stand, and one of them is the
larger half of the original Q21 report: the SEMI estimate does not move. Its
right side is unfiltered lineitem, which covers the whole key domain, so the
match fraction is 1.0 and the model correctly says every left row matches. The
actual reduction to 7,048,027 comes from the correlated residual
`l2.l_suppkey <> l1.l_suppkey`, and `extra_predicates_selectivity` is still
hardcoded to 1.0 for every join type. Opening that channel is a separate
decision. The ANTI estimate is what this work moves.

Also still open and unrelated to the model: `JoinBuildShapeStrategy`'s
`declined_no_estimate` (§5), whose stated rationale is false when a correlated
residual forces a build payload.
