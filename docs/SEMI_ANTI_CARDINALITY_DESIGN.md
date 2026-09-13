# Semi / Anti Join Cardinality — Design & Decision

**Status:** PROPOSED — awaiting architect decision. NOTHING IMPLEMENTED.
**Date:** 2026-09-13. Rung 1 ruled 2026-09-13 (see §3); §7 Q2/Q3 still open.
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

## 3. Proposed model — three rungs, in order

### Rung 1 — the hint channel (declared foreign key)

**Architect ruling, 2026-09-13:** an informational foreign key is a usable
estimation hint. With no or limited statistics, a user asserting a relationship
is information, and anything that yields an estimate beats no estimate. It is
NOT a proof and is never treated as one — see the gate below.

What the hint actually supplies is the premise rung 2 already needs. Rung 2's
containment ratio assumes every left key is drawn from the right key's domain;
today it assumes that with no evidence at all, and it is badly wrong in the
other direction when the two key sets are disjoint (semi should be ~0,
containment says `|L|`). A declared FK is the evidence for that premise. The
two rungs are therefore not alternatives:

| right side | with declared FK | without |
|---|---|---|
| unreduced (`row_count == domain_row_count`) | every left key is present → semi = \|L\|, anti = 0 (floored, §4.4) | rung 2's ratio, containment merely assumed |
| reduced (a filter ran) | rung 2's ratio, containment **justified** | rung 2's ratio, containment assumed |

A filter on the right does not void the hint, it bounds what the hint can say:
the FK describes the BASE relations, so once the right is reduced the question
becomes how many keys survived — which is exactly the NDV-under-filter problem
in §4.2, in KEY space. Degrading by surviving ROW fraction
(`right.row_count / right.domain_row_count`) is wrong and must not be used:
many rows share a key, so the surviving row fraction is not the surviving key
fraction.

**The gate — a hint may never become a fact.** This is the same split the
codebase already enforces for manifest statistics: `Manifest.stats_are_authoritative`
lets hint counts feed plan estimation while pruning, counting and limit
elimination use only authoritative numbers (see
`postgres_connector.py:397`). The same rule applies here:

- An FK-derived row count is constructed as `row_count_estimate`, **never**
  `row_count_metric`. A wrong assertion must degrade to a bad plan, never to a
  wrong answer.
- It must not reach anything that acts on a number being true — file pruning,
  `COUNT` answered from the manifest, limit elimination. Those are already
  gated by `stats_are_authoritative`; an FK-derived estimate must not be
  laundered past that gate by arriving through the join estimator instead.

### Rung 2 — the ratio (containment and uniformity)

The fraction of left keys present in the right. Reached whenever rung 1's exact
case does not apply — i.e. always, except an unreduced right side under a
declared FK:

```
match_fraction = min(1.0, ndv_right / ndv_left)
semi = |L| · (1 − null_fraction_left) · match_fraction
anti = |L| − semi
```

Two properties worth pinning:

- **The complement is exact, given semi.** Every left row either matches or does
  not; `anti = |L| − semi` is arithmetic. It is also the form that handles
  nulls correctly — a NULL left key never matches, so it always survives anti
  and never survives semi. `_effective_rows` already strips the left null
  fraction before the inner estimate, so subtracting puts those rows back.
  Writing anti directly as `|L|·(1−nf)·(1−ratio)` looks more natural and
  silently drops them. **Do not write the direct form.**
- **The complement amplifies relative error.** When anti is genuinely small, an
  absolute error in semi becomes an enormous relative error in anti. This is
  why rung 1's exact case is worth having: it is the one shape where anti is
  stated without inheriting semi's error.

### Rung 3 — `left anti null-aware` is not either of the above

`left anti null-aware` is `NOT IN` semantics: a single NULL in the right key
makes every comparison UNKNOWN and the join emits **zero** rows. That is
neither the complement nor the ratio, and the current code lumps it in with the
other four. It needs its own branch keyed on `right` null fraction:

- right null fraction known and `> 0` → estimate the floor (see §4.4).
- right null fraction unknown (`None`) → cannot prove it; fall to rung 2.

`left semi not-distinct` / `left anti not-distinct` are INTERSECT/EXCEPT, where
NULL is an ordinary value that equals itself (IS NOT DISTINCT FROM). They take
rung 2 **without** the left-null exclusion, because a NULL left key does match
there.

## 4. Prerequisites and consequences

### 4.1 Per-side NDVs are destroyed today (blocks rung 2)

`_equi_key_classes` computes `side_tdoms` per side and then writes
`tdom = max(...)` into **both** `KeyStats` (`statistics_refresh.py:976-987`).
So `ndv_right / ndv_left ≡ 1.0` and rung 2 returns exactly today's
non-reducing answer.

**The fix is to stop collapsing, and it is arithmetically neutral.** Every
current consumer of a pair already takes `max(left.ndv, right.ndv)` back out:
`_key_selectivity` (`join_cardinality.py:87`) and `apply_occupancy_bound`
(`join_cardinality.py:175`). Null fractions are already per-side. So
inner / outer / cross / asof estimates come out **byte-identical**; only the new
semi/anti arm reads the two apart. `ndv_provenance` becomes per-side as a
side-effect, which is a correctness improvement in its own right — today a
MEASURED left tying an unmeasured right is reported under one label.

This is worth doing on its own terms, independent of the rest of this document.

### 4.2 NDV does not respond to filters (blocks rung 2)

`_filter_stats` never calls `_cap_ndvs` on its result, and
`_narrow_filter_columns` only touches columns the predicate actually names. A
`WHERE l_receiptdate > l_commitdate` drops lineitem 600M → 15M rows while
`l_orderkey.distinct_count` stays at its base 150M — an NDV larger than the row
count of the relation carrying it.

Harmless today, because nothing divides one NDV by another. **Fatal under rung
2:** both sides read the base NDV, the ratio is 1.0, and anti returns the floor
of 1. That is a new false claim, not a partial fix.

Two candidate treatments — **this is the open decision, see §7 Q2**:

| | treatment | effect on rung 2 |
|---|---|---|
| **(a)** | cap NDV at row count after a filter | honest as a bound, but gives `ndv_L = ndv_R` on the common shape → ratio 1.0 → rung 2 still inert |
| **(b)** | make NDV respond to selectivity, e.g. `ndv · (1 − (1 − s)^(rows/ndv))` | the ratio has something real to read; a genuinely larger model change |

Rung 2 is only meaningful under (b). Under (a) the branch is honest but does
nothing, which is still strictly better than today's false claim.

### 4.3 `_intersect_join_keys` defaults the wrong way for anti

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
Rungs 1 and 3 both want to say zero. They must accept the floor of 1 and say so
in the telemetry note rather than special-casing it — a 1 here means "we
believe zero", and the floor is a deliberate existing contract.

### 4.5 Telemetry

`_join_note` must receive the real `key_count` (the pre-occupancy-bound class
count, matching what the equi-join path already reports at line 1108), not the
hardcoded `0`. Which rung produced the number should be recoverable from
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
- Unit coverage for each rung and for the `_NARROWABLE_JOIN_SIDES` entries in
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
   gates manifest hints. Folded into §3 rung 1; no longer open.
2. **§4.2.** Treatment (a) cap, or (b) NDV responds to selectivity? Rung 2 is
   inert under (a) — honest, but no movement.
3. **Sequencing.** §4.1 is neutral and independently correct; it can land alone
   and be verified by the byte-identical gate. Land it first, or hold it until
   the model it unblocks is agreed?
