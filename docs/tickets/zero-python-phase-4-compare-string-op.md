# Ticket: Zero-Python Expression Engine — Phase 4 (BC_COMPARE string-op elimination)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–3 landed:
> `docs/tickets/zero-python-phase-1-result-wrap.md`,
> `docs/tickets/zero-python-phase-2-is-null.md`,
> `docs/tickets/zero-python-phase-3-extraction.md`. This is Phase 4.

## Problem

BC_COMPARE has two paths at runtime:

```python
# evaluation.pyx ~line 2274
if slot.op_code != OP_UNKNOWN:
    compare_result = draken_compare_int(slot.op_code, py_left, py_right, ...)
else:
    compare_result = draken_compare(<str>slot.compare_op_str, py_left, py_right, ...)
```

The fast path (`draken_compare_int`) takes a pre-resolved integer op_code.
The slow path (`draken_compare`) takes a Python string and walks an
`if op == "..."` chain inside `comparisons.pyx`.

The `OP_UNKNOWN` fallback fires whenever the op-string is not present in
`_OP_CODE` (in `opteryx/expression/evaluator/_impl.pyx:61`). The current
`_OP_CODE` dict covers 18 standard ops (Eq, NotEq, Lt, Gt, LtEq, GtEq,
InList, NotInList, Like, NotLike, ILike, NotILike, RLike, NotRLike,
InStr, NotInStr, IInStr, NotIInStr).

It does **not** cover these 15 ops, which therefore fall through to the
string-keyed slow path on every comparison:

```
AnyOpEq        AnyOpNotEq      AnyOpGt       AnyOpLt
AnyOpGtEq      AnyOpLtEq       AllOpEq       AllOpNotEq
AtArrow        ArrayContainsAll AtQuestion
AnyOpLike      AnyOpNotLike    AnyOpILike    AnyOpNotILike
```

Per-morsel cost of the slow path:
- Python-string `if op == "..."` chain (15 string compares worst case).
- `_OP_CODE.get(op, 0)` Python dict lookup (cheap but unnecessary).
- `OP_UNKNOWN` check + `NotImplementedError` raise machinery.

`OP_UNKNOWN` should be unreachable at runtime: bind time has full
knowledge of the op-string and can either resolve to an integer or
fail-fast.

## Goal

After Phase 4:
- Every supported compare op-string resolves to a non-zero `OP_*` integer
  at bind time.
- `OP_UNKNOWN` (0) becomes a bind-time error, never a runtime branch.
- The `cpdef draken_compare(str op, ...)` function is **deleted**.
- The BC_COMPARE executor's `else: compare_result = draken_compare(...)`
  fallback is deleted.
- All current AnyOp\* / AllOp\* / AtArrow / ArrayContainsAll / AtQuestion
  / AnyOpLike-family dispatch logic moves into `draken_compare_int`
  (typed cdef, op_code int) or a small set of named cdef helpers.
- `slot.compare_op_str` is no longer read by BC_COMPARE. (The field
  stays on `BytecodeInstr` because BC_BINARY_OP still uses it —
  Phase 6 removes it.)

## Scope

**In scope**
- `opteryx/expression/evaluator/_impl.pyx` (around line 38):
  - Add `OP_*` DEFs for the 15 missing ops. Number them 19+ contiguously
    (current OP_NOT_I_IN_STR=18). Reserve a small gap for future ops if
    you want; do not renumber existing ones.
  - Extend `_OP_CODE` dict with the new entries.
  - Extend `_DRAKEN_CMP_OP[]` and `_DRAKEN_CMP_OP_FLIPPED[]` array
    dimensions (currently `[19]`) to cover the new max. All new entries
    are `-1` (own kernel — Draken's ordinal compare doesn't handle these).
  - Verify `_verify_node_type_constants` still works; if it asserts on
    array length, update.
- `opteryx/expression/evaluator/comparisons.pyx`:
  - Add `op_code` branches in `draken_compare_int` for the 15 new codes.
    Lift the body of each `if op == "..."` branch from `draken_compare`
    into `draken_compare_int`, keyed by the new `OP_*` int.
  - The AnyOp\* / AllOp\* / AtArrow / ArrayContainsAll / AtQuestion
    branches are **not** vector-type-dispatched (they're op-keyed nanobind
    kernels). Handle them **before** the per-type dispatch in
    `draken_compare_int`.
  - The AnyOpLike-family **is** vector-type-dispatched (string family
    → `_string_anyop_like`; otherwise → `vector_anyop_like`). Keep that
    inner dispatch but key the outer branch on int.
  - Delete `draken_compare` (the `cpdef` at ~line 296). Delete the
    `_NEGATED_OPS` dict if it has no other callers (verify).
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - In `_NT_COMPARISON_OPERATOR` (around line 407), replace
    `op_code_val = <int>op_codes.get(op_str, 0)` with a fail-fast
    resolver:
    ```cython
    op_code_val = <int>op_codes.get(op_str, 0)
    if op_code_val == 0:
        raise NotImplementedError(
            f"compiled_expression: unknown comparison op {op_str!r}"
        )
    ```
  - Remove `bc._hold(op_str)` and `slot.compare_op_str = <PyObject*>op_str`
    for BC_COMPARE — no longer needed. (Leave the BC_BINARY_OP path's
    `slot.compare_op_str` alone — that one's Phase 6.)
- `opteryx/expression/evaluator/evaluation.pyx` BC_COMPARE executor
  (~line 2261 — inlist-inline branch; ~line 2274 — normal branch):
  - Delete the `else: compare_result = draken_compare(...)` fallback.
  - Delete the `if slot.op_code != OP_UNKNOWN:` gate — just call
    `draken_compare_int` unconditionally.
  - The `slot.compare_op_str` read on the BC_COMPARE path can be deleted.

**Out of scope**
- BC_BINARY_OP's string-op dispatch — Phase 6.
- `draken_between` (still cpdef, separate from compare) — leave it.
- Removing `_NEGATED_OPS` Python dict — only if it becomes unused;
  if `draken_compare_int` still needs the same negation map, keep it.
- Annotating the executor `nogil` — Phase 8.

## What `draken_compare_int` will look like

Today's structure (`draken_compare_int`, ~line 218 in comparisons.pyx):
1. Negation normalization for NotEq, NotInList, NotLike, etc.
2. Type-flip for `scalar OP vector`.
3. Per-vector-type dispatch (`_int64_compare`, `_float64_compare`,
   `_decimal_compare`, `_bool_compare`, `_string_compare`, etc.).

After Phase 4:
1. **Direct op_code branches** for the 15 new ops, **before** the
   negation/type-dispatch logic. These ops aren't type-dispatched:

   ```cython
   if op_code == OP_ANYOP_EQ:
       left_nb = (<Vector>left)._nb if isinstance(left, Vector) else left
       right_nb = (<Vector>right)._nb if isinstance(right, Vector) else right
       return BoolVector(vector_anyop_eq(literal=left_nb, column=right_nb))
   if op_code == OP_ANYOP_NOT_EQ:
       ...
   # ... repeat for each
   ```

2. Existing negation + type-flip + per-type dispatch — unchanged for
   op_code 1..18.

The AnyOpLike-family inner dispatch on `get_vector_type(left) == STRING`
stays as today (it's a real type check, not a type-name string compare).

## Resolution table (op-string → new OP_* int)

Pick contiguous integers starting at 19. Suggested numbering:

| op-string         | OP_* | maps to kernel                  |
|-------------------|-----:|---------------------------------|
| `AnyOpEq`         | 19   | `vector_anyop_eq(literal=, column=)` |
| `AnyOpNotEq`      | 20   | `vector_anyop_neq(literal=, column=)` |
| `AnyOpGt`         | 21   | `vector_anyop_gt`               |
| `AnyOpLt`         | 22   | `vector_anyop_lt`               |
| `AnyOpGtEq`       | 23   | `vector_anyop_gte`              |
| `AnyOpLtEq`       | 24   | `vector_anyop_lte`              |
| `AllOpEq`         | 25   | `vector_allop_eq`               |
| `AllOpNotEq`      | 26   | `vector_allop_neq`              |
| `AtArrow`         | 27   | `_json_at_arrow`                |
| `ArrayContainsAll`| 28   | `_json_array_contains_all`      |
| `AtQuestion`      | 29   | `_json_at_question`             |
| `AnyOpLike`       | 30   | string→`_string_anyop_like`, else `vector_anyop_like` |
| `AnyOpNotLike`    | 31   | string→`_string_anyop_like(...).not_vector()`, else `vector_anyop_like(..., True)` |
| `AnyOpILike`      | 32   | string→`_string_anyop_like(ignore_case=True)`, else `vector_anyop_ilike` |
| `AnyOpNotILike`   | 33   | string→`_string_anyop_like(...).not_vector()`, else `vector_anyop_ilike(..., True)` |

Cross-check with `opteryx/expression/operators.pyx` or wherever the
binder produces these op-strings (the binder produces them — verify they
match exactly; case-sensitive).

## Verification

- `make c` clean.
- `make q` 100/100 (currently 137/137).
- `grep -n 'draken_compare\b' opteryx/expression/evaluator/*.pyx
  opteryx/expression/evaluator/*.py` — only `draken_compare_int` and
  `draken_compare_dv` should remain. The cpdef `draken_compare(str op, …)`
  must be gone.
- `grep -n 'compare_op_str' opteryx/expression/evaluator/evaluation.pyx
  opteryx/compiled/expression/compiled_expression.pyx` — should appear
  only in BC_BINARY_OP regions (Phase 6 territory), not BC_COMPARE.
- `grep -n 'OP_UNKNOWN' opteryx/expression/evaluator/evaluation.pyx` —
  should appear only in the bind-time fail-fast check
  (`compiled_expression.pyx`); the runtime BC_COMPARE branches on it
  must be deleted.
- Spot tests:
  - `SELECT name FROM $planets WHERE id = 3` (OP_EQ — sanity, fast path)
  - `SELECT name FROM $planets WHERE id IN (1, 3, 5)` (OP_IN_LIST)
  - `SELECT name FROM $planets WHERE name LIKE 'M%'` (OP_LIKE)
  - An AnyOp test — e.g. `SELECT name FROM testdata.astronauts WHERE 'STS-119 (Discovery)' = ANY(missions)` (AnyOpEq with array RHS). Verify the binder produces `AnyOpEq` here; if not, find a query that does.
  - An AtArrow test (JSON `@>`); construct one inline if no JSON test data: `SELECT '{"a":1,"b":2}'::JSON @> '{"a":1}'`.
  - The Phase 1/3 regression checks:
    - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` → `[7, 5, 5]`
    - `SELECT missions[0] FROM testdata.astronauts LIMIT 3` (array extract)
- Microbench: time a query mix that hits both the standard compares
  (OP_EQ etc.) and at least one AnyOp\* path. Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No new Python on the execute path.** Phase 4 *removes* Python; do
  not introduce any new `getattr` / `isinstance` / `type(...).__name__`
  in the hot region. (The existing scalar-vs-Vector `isinstance` from
  Phase 1 stays — it's the acknowledged residual.)
- **Fail fast.** Bind-time op resolution must raise; no silent
  `OP_UNKNOWN` at runtime.
- **No fallbacks.** Do not leave the `else: draken_compare(...)` branch
  as a safety net.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **Cython code must be typed.** New cdef branches in
  `draken_compare_int` use `cdef object left_nb, right_nb` etc.
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/_impl.pyx` — `_OP_CODE` dict + `OP_*`
  DEFs (~lines 38–67), `_DRAKEN_CMP_OP[]` array (~lines 82–125).
  Verify the array size keyword (`[19]` today). If C array length needs
  to grow, all initialisers below it shift; do the renumber carefully.
- `opteryx/expression/evaluator/comparisons.pyx`:
  - `draken_compare_int` at ~line 218 — extend.
  - `draken_compare` at ~line 296 — delete.
  - `_NEGATED_OPS` at ~line 53 — delete if unused, otherwise leave.
  - The imports at the top — drop any nanobind imports that become
    unreferenced (`vector_anyop_eq`, `vector_anyop_neq`, etc., now used
    only from `draken_compare_int`).
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - `_NT_COMPARISON_OPERATOR` emit at ~line 407. Add fail-fast resolver;
    remove `slot.compare_op_str` for the BC_COMPARE path.
- `opteryx/expression/evaluator/evaluation.pyx` BC_COMPARE executor at
  ~line 2261 (inlist-inline branch) and ~line 2274 (normal branch).
  Both have the `if slot.op_code != OP_UNKNOWN ... else: draken_compare(...)`
  pattern — delete the else branches.
- `opteryx/expression/evaluator/__init__.py` — verify nothing re-exports
  `draken_compare` (the str-keyed). If it does, drop.

## Tests

- `make q` (137/137).
- All spot queries return correct results.
- Phase 1 / Phase 3 regression-check queries still pass.

If you spot an AnyOp* op-string that doesn't actually round-trip through
the binder (i.e. no SQL syntax produces it), surface in the PR — that's
a candidate for deletion in a follow-up. But for this phase, port every
existing branch from `draken_compare` to keep the surface intact.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 1 / 2 / 3 tickets — particularly the bind-time-flag and
   int-dispatch precedents.
3. `opteryx/expression/evaluator/_impl.pyx` end to end. It's short. The
   `_DRAKEN_CMP_OP[]` array semantics matter.
4. `opteryx/expression/evaluator/comparisons.pyx` end to end. Understand
   the existing op-dispatch shape before mirroring it in
   `draken_compare_int`.
5. `opteryx/expression/evaluator/evaluation.pyx:2240–2340` — the
   BC_COMPARE executor (both the inlist-inline and normal branches).
6. `opteryx/compiled/expression/compiled_expression.pyx:395–435` — the
   `_NT_COMPARISON_OPERATOR` emit.

## Definition of done

- 15 new `OP_*` DEFs added in `_impl.pyx`.
- `_OP_CODE` dict + `_DRAKEN_CMP_OP[]` / `_DRAKEN_CMP_OP_FLIPPED[]`
  arrays extended; existing entries unchanged.
- `draken_compare_int` handles all 33 op_codes; the 15 new ones dispatch
  to the same kernels the deleted `draken_compare` did.
- `draken_compare` (cpdef, string-keyed) deleted.
- Bind-time `_NT_COMPARISON_OPERATOR` fails loud on unresolvable
  op-string.
- BC_COMPARE executor has a single `compare_result = draken_compare_int(...)`
  call site per branch; no `if/else` on `OP_UNKNOWN`.
- `slot.compare_op_str` no longer read by BC_COMPARE.
- `grep` checks in §Verification return as specified.
- `make q` 100/100.
- Microbench numbers in PR description.

## Side-notes to surface in PR

- The pre-existing **COUNT(*) + WHERE returns 0** aggregate bug (raised
  in Phase 3 ticket's PR notes) is still outstanding — not affected by
  this phase. Confirm `SELECT COUNT(*) FROM $planets WHERE id > 5`
  still returns 0 (the broken state) after Phase 4; if for any reason
  this phase changes that, surface it.
- `tests/unit/expression/test_map_access_operator.py` imports the now
  deleted `MapAccessOp`; that test file needs updating or deleting in a
  separate cleanup ticket. Out of scope here.
- If during Phase 4 you discover an op-string in
  `_NT_COMPARISON_OPERATOR`'s upstream node-string that we genuinely
  don't support (no `_OP_CODE` entry, no `draken_compare` branch), fail
  loud at bind time — surface in PR, do not paper over.
