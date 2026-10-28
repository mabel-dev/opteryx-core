# GROUP BY top-k fusion through a computed Projection

Status: **IMPLEMENTED 2026-10-28** (architect ruled R1 = O2, compiler; see "As built" at the end).
Extends
`docs/GROUPBY_TOPK_FUSION_DESIGN.md` (v1 shipped 2026-09-26), whose "Which ClickBench queries arm"
section already names this case and defers it as "a separate ruling".

## 1. Problem

ClickBench Q35 (0-indexed; Q36 in the older doc):

    SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c
    FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3
    ORDER BY c DESC LIMIT 10

`GroupKeyReductionStrategy` rewrites the GROUP BY to `ClientIP` and inserts a Project above the
aggregate that recomputes `ClientIP - n`. The plan is Heap Sort <- Projection <- Grouped Aggregate.
`_arm_groupby_topk` (compiler.py) refuses to arm because the Projection EVALUATES something, so the
aggregate emits all 9,762,046 groups, the Projection evaluates three arithmetic columns over them
(30 ms thread time) and the Heap Sort sorts them (40 ms thread time). Q31/Q32, whose projection is
pass-through, arm and emit 2,560 / 20,480 rows.

Measured 2026-10-28 (Mac, read-only): the last ~12 ms of Q35's 135 ms runs at 8-12 busy cores
(sink and merge are saturated). Rewriting the query by hand as "top-k GROUP BY ClientIP, then
project" ran at B/A 0.927 (8/9 interleaved rounds faster, identical top-10 counts and ClientIPs):
Q35 about 1.12x -> about 1.05x DuckDB 2.0.0.

## 2. Why v1 refuses, and what that rule protects

`_arm_groupby_topk` docstring: "A real computation evaluated on fewer rows could stop raising an
error the unfused plan raises." That is a correctness rule and is kept. The proposal below does not
weaken it: it only arms when the computation CANNOT raise, decided from plan-time types alone
(the standing ruling: an eligibility gate must be provably safe from the plan, never from data).

Note GroupKeyReduction itself preserves the error set: the expression is a function of the key, so a
row raises iff its group's key raises. Pruning groups before the Projection is the first step that
could drop an error.

## 3. Options

| | O1 optimizer swap | O2 relax the compiler check (recommended) | O3 do nothing |
|---|---|---|---|
| Change | New rule: swap HeapSort and Projection so the Projection sits ABOVE the HeapSort (the sort keys must pass through it unchanged) | `_arm_groupby_topk` accepts projection columns in a provably non-raising class | none |
| Where | `opteryx/planner/optimizer/strategies/` (plan shape changes, EXPLAIN changes) | `compiler.py`, Python, compile time | |
| Projection then runs on | 10 rows | ~41k candidate rows (64 partitions x up to 64 buckets x k=10 upper bound) - negligible | 9.76M groups |
| Blast radius | Wide: interacts with ProjectionPushdown, ProjectFusion, LimitPushdown, ORDER BY alias resolution, OFFSET | One function, one new helper | |
| Needs the same safety rule | Yes (the swap also evaluates on fewer rows) | Yes | n/a |
| Native/.so change | none | none (selection, lane compaction, `topk_pruned` already exist) | |

Recommendation: O2. O1 is the more general rewrite but its extra gain (projection on 10 rows instead
of ~41k) is ~0.1 ms, and it widens the change to the whole planner.

## 4. The safe class (v1), decided at plan time from types

A projection column in the chain between the HeapSort and the GROUP BY is accepted if it is one of:

1. a GROUP BY output already in the stream, or a LITERAL (accepted today);
2. NEW: `BINARY_OPERATOR` `+` or `-` with exactly one operand an IDENTIFIER that is a GROUP BY
   output of integer type of at most 32 bits (INT8/16/32, UINT8/16/32) and the other an integer
   LITERAL with |value| < 2^31, AND the expression's schema type is INT64.

Proof of non-raising: |operand| < 2^32 and |literal| < 2^31, so the exact result is below 2^33 and
cannot overflow INT64; there is no division, cast or function call, so no other error path; NULL
propagates row-wise and is not an error. The type check must read the expression's resolved
`schema_column` type, not assume it (observed 2026-10-28: `INT32 col - 1` binds INT64, `INT32 + 1`
is INT64; each width pair must be confirmed in the test battery and anything else falls out of the
class).

Not accepted (unarmed, as today): INT64/UINT64 column +/- literal (can overflow: the engine raises
"INT64 addition overflow"), `*`, `/`, `%`, CAST, any FUNCTION, CASE, nested expressions, two
identifier operands, volatile expressions, anything over a non-GROUP-BY column.

## 5. Everything else stays as v1

- No HAVING, no ROLLUP/CUBE/GROUPING SETS, no filter between the aggregate and the sort.
- ORDER BY must LEAD with rankable aggregates matched by identity (so `ORDER BY c` arms;
  `ORDER BY ClientIP - 1` has no leading aggregate and stays unarmed). `ties` is set when it
  continues past them. NULL/NaN placement uses draken's comparator, unchanged.
- OFFSET: k = limit + offset, unchanged.
- The HeapSort is still compiled unchanged and remains the exact final order, so the aggregate only
  ever reduces its input.
- Each ProjectionNode in the chain is checked independently; one failing node leaves it unarmed.

## 6. Change surface

- `compiler.py`: replace the "every projected column is a GROUP BY output or a literal" test with
  "or satisfies `_projection_column_cannot_raise(col, gb_outputs)`" (new helper, section 4).
  Fail closed on any unknown type/node.
- Native, `native_group_sinks.hpp`: none.
- Telemetry: reuse `topk_pruned`. Optional counter for how many projections were looked through.
- No new optimizer strategy, no env switch, no fallback.

## 7. Expected gain, honestly

Ceiling measured at B/A 0.927 on Q35; O2 should land at about 0.93-0.95 (the Projection still runs on
the ~41k candidates). It helps one of the 43 ClickBench queries by about 7%, taking one of the three
group-by queries still behind DuckDB to roughly parity-plus-5%. The pattern is general
(`GROUP BY k ... ORDER BY agg LIMIT n` with derived integer columns), but v1's class is deliberately
narrow: function projections (DATE_TRUNC, EXTRACT, UPPER...) are far more common in user queries and
need section 9 R3.

## 8. Tests and validation

- Battery: extend `tests/integration/sql_battery/test_groupby_topk_fusion.py`; each case compares the
  armed plan with arming disabled and asserts `topk_pruned > 0` / `== 0`.
  - Arms: Q35 shape; INT8/16/32 and UINT8/16/32 keys with +/- literals; literal first (`5 - k`);
    several derived columns; two projection nodes; ASC and DESC; OFFSET; ties at the boundary;
    NULL aggregate ordering.
  - Does NOT arm (assert `topk_pruned == 0` and identical answers): INT64 key +/- literal;
    `k * 2`; `k / 0`; `k % 2`; `CAST(k AS INT8)`; any function; CASE; `a + b`; volatile
    (`RANDOM()`); a projection over a non-key column; HAVING; ORDER BY on the derived column.
  - A raising control: an INT64 key whose `k + 1` overflows must STILL raise with LIMIT present.
- Performance: private bench tree (untested edits in `main` hit other sessions; Python edits count),
  `dev/ab_bench.py` interleaved, >= 11 rounds, alternating order. Target Q35. Controls Q31, Q32, Q34
  (already armed, expect ~1.0) and Q20/Q25/Q29 (non-group-by). Report against DuckDB with
  `make clickbench-interleave QUERIES=35,32,31`.
- Sign-off: `make q` and `make slt`. Mac only until x86 access returns (per-arch results owed).

## 9. Rulings needed from the architect

- R1. O2 (compiler, recommended) or O1 (optimizer rewrite)?
- R2. Safety rule: provably non-raising only (recommended, matches the standing eligibility ruling),
  or accept deferring evaluation past ORDER BY/LIMIT as other engines do (PostgreSQL postpones
  expensive target-list evaluation past the sort when a LIMIT is present - stated from general
  knowledge, verify before quoting)? The latter hides errors and I recommend against it.
- R3. v2: a `cannot_raise` property on function registrations so `DATE_TRUNC`, `EXTRACT`,
  `UPPER`, comparisons etc. can arm. That touches the function registry and the generated
  `reference/` catalogs (generators only, no hand edits) - a separate design if wanted.
- R4. Provenance vs structure: GroupKeyReduction knows which projection it inserted and could tag it;
  I recommend the structural check in section 4 instead, because it also covers user-written
  projections and needs no cross-pass state.

## 10. Not changed / out of scope

Group identity (hash only), OFFSET-without-LIMIT, Q41's small-partition selection cost (open ruling
in the v1 doc), Q32/Q35 merge and probe work (measured: Q35's probe is L2-resident and cheap,
`docs/GROUPBY_PROBE_EXPERIMENT_DESIGN.md` section 11).

## 11. As built (2026-10-28)

- `compiler.py`: `_Compiler._projection_cannot_raise` (new) plus `_TOPK_NARROW_INT_TYPES`; the
  `_arm_groupby_topk` projection check now accepts GROUP BY outputs, literals, or a column that
  satisfies the helper. Python, compile time, no native change, no switch, no fallback. Rulings as
  recommended: R2 provably non-raising only, R3 (per-function `cannot_raise`) not built, R4
  structural check.
- Limitation, documented in the tests: projections stacked through subqueries carry their columns in
  `NESTED` wrappers, which the matcher does not look through (it never did, for pass-through columns
  either). Those shapes stay unarmed and correct.
- Tests (`tests/integration/sql_battery/test_groupby_topk_fusion.py`, now 62 cases): 9 arm-through
  cases (INT8/16/32, UINT16/32, `5 - k`, several derived columns, the largest admitted literal,
  OFFSET, ORDER BY continuing onto a key) all fail on the unchanged compiler and pass with the
  change; 12 not-armed shapes (INT64 key, `*`, `/`, `%`, CAST, ABS, `k + k`, nested, float literal,
  literal >= 2**31, ORDER BY the derived column, HAVING, stacked-NESTED); a per-width check that
  `+/- 2147483647` at both ends of INT8/16/32 and UINT8/16/32 binds INT64 and is exact; and two
  raise-survives-LIMIT controls (INT64 overflow, narrowing CAST).
- Measured (Mac, private A/B trees, `dev/ab_bench.py`, 11 interleaved rounds, only compiler.py
  differs): Q35 133.0 -> 122.2 ms, B/A **0.918**, B faster 11/11. Controls: Q15 0.988, Q20 0.995,
  Q25 0.993, Q29 1.001, Q31 1.002, Q32 0.998, Q34 0.999 (all inside noise). Q35's armed and unarmed
  answers are identical. x86 is owed (no access).
- Sign-off on `main`: `make q` and `make slt` pass; the top-k, OFFSET-fusion and ClickBench result
  batteries pass (107 tests).
