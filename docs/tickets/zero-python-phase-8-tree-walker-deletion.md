# Ticket: Zero-Python Expression Engine — Phase 8 (tree-walker deletion + constant-folding switch)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–7 have landed:
> see `docs/tickets/zero-python-phase-{1..7}-*.md`. This is Phase 8 —
> the final phase as currently planned.

## Scope adjustment from the original plan

The plan's Phase 8 had two halves: "annotate executor `nogil`" and
"delete the tree-walker". The first half is **not achievable in this
phase** for an architectural reason:

After Phases 1–7, BC_FUNCTION / BC_CAST / BC_CASE / BC_EXTRACTION /
BC_BINARY_OP all dispatch to a bind-time-resolved Python callable via
`(<object>slot.callable_ref)(...)`. That single `PyObject_Call` per
morsel requires the GIL. Full executor `nogil` requires replacing
each kernel call with a C function-pointer invocation through a C ABI
for nanobind methods — a separate architectural piece that the plan
explicitly deferred ("the call into the resolved kernel itself remains
a PyObject_Call; eliminating that requires a C-callable kernel ABI
which is out of scope").

**Phase 8 as scoped here:**
1. Switch constant-folding off the tree-walker to `execute_bytecode`.
2. Delete the tree-walker family entirely.
3. Delete BC_LEGACY opcode + `_NT_LEGACY` fallback emit.
4. Inventory what would still be needed to make the executor `nogil`
   end-to-end. Document in the audit / plan as Phase 9 if non-trivial.

The "nogil-ready" annotation work is moved to a follow-up phase
(call it Phase 9 or "Phase 8b"); the scope is to design and ship the C
function-pointer kernel ABI.

## Problem

Two surfaces remain to retire:

**(1) Constant folding still uses the tree-walker.** In
`opteryx/planner/optimizer/strategies/constant_folding.py:22, 289`,
`opteryx.expression.evaluate(root, table)[0]` is called against a
synthetic single-row table to fold constants at plan time. The
`evaluate` chain enters
`opteryx/expression/__init__.pyx:_inner_evaluate` → recursively into
`evaluate_draken` / `_eval_value` / `evaluate_case` (legacy) /
`apply_bounded_function`.

This is plan-time, so per-morsel overhead doesn't matter. What *does*
matter is that this is the **only remaining production caller** of the
tree-walker family. As long as it exists, the tree-walker code is
live, can never be deleted, and continues to drift out of sync with
the bytecode executor (e.g. when new node types are added,
constant-folding stops working until tree-walker support is added too —
a contributor maintenance tax that bytecode already pays).

**(2) The tree-walker itself.** In
`opteryx/expression/evaluator/evaluation.pyx`:

- `_eval_value` (line 244)
- `evaluate_draken` (line 808, cpdef)
- `evaluate_and_append_draken` (~line 949)
- `_unary_draken` (~line 336)
- `_eval_cast_draken` (~line 158)
- `_eval_function_draken` (~line 175)
- `_eval_binary_op_draken` (now in `arithmetic.pyx` after Phase 6)
- the lazy-import module-level globals (`_cast_factory_fn`,
  `_binary_ops_fn` etc.) that exist only to back this family

In `opteryx/expression/evaluator/case_eval.pyx`:

- `_decide` (line 68)
- `_compute` (line 118)
- `_assemble` (line 149) — now dead code in the bytecode path after
  Phase 7 (the closure in `build_case_fn` inlines the dispatch)
- `evaluate_case` (line 196) — the legacy public entry

In `opteryx/expression/evaluator/function_execution.pyx`:

- `apply_bounded_function` (line 14) — reachable only from tree-walker
  `_eval_function_draken`

In `opteryx/expression/__init__.pyx`:

- `_inner_evaluate` and module-level `evaluate` — the public entry
  points used by `constant_folding`

In `opteryx/expression/binary_operators.pyx`:

- `binary_operations` (~line 35) — kept after Phase 6 *only* for
  `_inner_evaluate` to call. After this phase: deletable.
- `_OP_CODE_MAP`, the local op-string→int dict that mirrors
  `_BOP_CODE` — deletable with `binary_operations`.

Plus the BC_LEGACY opcode + `_NT_LEGACY` fallback emit in
`compiled_expression.pyx` that exists for unsupported node types —
after this phase, every supported node type must compile, period.

## Goal

After Phase 8:
- `constant_folding.py` uses `build_bytecode(lower(node))` +
  `execute_bytecode(bc, synthetic_morsel)`. No `opteryx.expression.evaluate(...)` call.
- The tree-walker family (~700+ lines across `evaluation.pyx`,
  `case_eval.pyx`, `__init__.pyx`, `function_execution.pyx`,
  `binary_operators.pyx`, `arithmetic.pyx`) is **deleted**.
- BC_LEGACY opcode + `_NT_LEGACY` fallback emit deleted. Bind-time
  produces a fully compiled bytecode or fails explicitly. No runtime
  fallback path exists.
- A short audit / inventory section is added to
  `docs/zero_python_expression_engine.md` describing the remaining
  per-morsel `PyObject_Call` instances (one per BC_FUNCTION /
  BC_CAST / BC_EXTRACTION / BC_CASE / BC_BINARY_OP fallback) so the
  next phase can target them.

## Scope

**In scope**
- `opteryx/planner/optimizer/strategies/constant_folding.py` — switch
  from `evaluate(root, table)` to a synthetic-morsel bytecode path.
- `opteryx/expression/__init__.pyx` —
  - Delete `_inner_evaluate` (or reduce to a tiny shim that calls
    bytecode; surface choice).
  - Delete module-level `evaluate` (or keep as plan-time shim).
- `opteryx/expression/evaluator/evaluation.pyx`:
  - Delete `_eval_value`, `evaluate_draken`,
    `evaluate_and_append_draken`, `_unary_draken`,
    `_eval_cast_draken`, `_eval_function_draken`.
  - Delete the lazy-import module-level fn caches that backed these
    (the `_cast_factory_fn`, `_binary_ops_fn` blocks).
- `opteryx/expression/evaluator/case_eval.pyx`:
  - Delete `_decide`, `_compute`, `_assemble`, `evaluate_case`.
  - Keep `_decide_compiled`, `_compute_compiled`, `build_case_fn`,
    and the assemble kernels.
- `opteryx/expression/evaluator/arithmetic.pyx`:
  - Delete `_eval_binary_op_draken`. Keep `resolve_binary_op` and
    the bind-time helpers.
- `opteryx/expression/evaluator/function_execution.pyx`:
  - Delete `apply_bounded_function`. Drop the file if it becomes
    empty.
- `opteryx/expression/evaluator/__init__.py`:
  - Remove all tree-walker re-exports: `evaluate_draken`,
    `evaluate_and_append_draken`, `apply_bounded_function`,
    `draken_compare` (if any lingers).
- `opteryx/expression/binary_operators.pyx`:
  - Delete `binary_operations`, `_OP_CODE_MAP`. Likely the entire
    file becomes deletable (only `BINARY_OPERATORS` /
    `EXTRACTION_OPERATORS` sets remain — relocate them to wherever the
    binder reads from; e.g. a 10-line `opteryx/expression/operator_catalog.py`).
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - Delete `_NT_LEGACY` / BC_LEGACY emit path. Every unhandled node
    type raises at bind time.
- `opteryx/expression/evaluator/_impl.pyx` — remove the textual
  `include "..."` of any leaf file you deleted.

**Out of scope**
- Annotating `execute_bytecode` as `nogil` — moved to a follow-up phase
  (Phase 9) which needs a C function-pointer ABI for kernels.
- Fixing the `assemble_fixed` no-ELSE-INT crash (Phase 7 finding) —
  separate ticket.
- Fixing the `COUNT(*) WHERE` returns-0 aggregate bug (Phase 2
  finding) — separate ticket.
- Cleanup of `tests/unit/expression/test_map_access_operator.py`
  (Phase 3 finding) — separate ticket.

## Constant-folding switch

Today (`opteryx/planner/optimizer/strategies/constant_folding.py:289`):

```python
result = evaluate(root, table)[0]
```

`table` here is a synthetic single-row morsel constructed earlier in
the function. After Phase 8:

```python
from opteryx.compiled.expression.compiled_expression import lower, build_bytecode
from opteryx.expression.evaluator import execute_bytecode

bc = build_bytecode(lower(root))
result_vector = execute_bytecode(bc, table)
# Extract scalar from length-1 vector.
result = result_vector.to_pylist()[0]
```

The `to_pylist()[0]` form pulls the single value out — same shape as
the old `evaluate(root, table)[0]` indexing (which returned a
length-1 vector and the `[0]` extracted the scalar).

Verify the existing constant-folding tests cover:
- LITERAL + LITERAL (e.g. `1 + 2` → `3`)
- Function on literals (`UPPER('hello')` → `'HELLO'`)
- CAST on literal (`CAST('123' AS INTEGER)` → `123`)
- Date arithmetic on literals (`DATE '2024-01-01' + INTERVAL '1 day'`)

If any constant-folding test fails after the switch, the bug is more
likely in the binder failing to populate `schema_column.type` on
synthetic literal nodes (the resolvers from Phase 4 / 5 / 6 require
it) than in the bytecode itself. Surface in PR.

## BC_LEGACY / _NT_LEGACY deletion

Today (`compiled_expression.pyx`, search for `_NT_LEGACY` or
`BC_LEGACY`):

```cython
# Fallback for any node type not explicitly handled.
slot.opcode = BC_LEGACY
slot.source_node = <PyObject*>src
```

After Phase 8 this branch is **deleted**. Every node type the
compiler encounters must have an explicit case. Unhandled node types
raise `NotImplementedError` at bind time. The BC_LEGACY opcode itself
can be removed from `compiled_expression.pxd`'s enum (verify no other
opcode-number references break) or left in place as a hole — surface.

The BC_LEGACY executor in `evaluation.pyx` (~line 2480 area; search
for `BC_LEGACY`) — delete the case. Without it, the `if opcode == BC_LEGACY:`
branch is unreachable; remove cleanly.

## Verification

- `make c` clean. **Verify a fresh build compiles** before
  `make q` (Phase 4 lesson, repeated).
- `make q` 100/100 (currently 137/137).
- Symbol checks:
  - `grep -rn '_eval_value\|evaluate_draken\|_unary_draken\|_eval_cast_draken\|_eval_function_draken\|_eval_binary_op_draken\|apply_bounded_function\|evaluate_and_append_draken' opteryx/ --include='*.py' --include='*.pyx' --include='*.pxd' --include='*.pxi'`
    — should return zero non-comment matches in production code.
  - `grep -rn '_inner_evaluate' opteryx/ --include='*.py' --include='*.pyx'`
    — zero matches (or one match if you kept a thin shim with that name).
  - `grep -rn 'from opteryx.expression import evaluate\b' opteryx/ --include='*.py'`
    — zero matches (the `constant_folding.py` import must be gone).
  - `grep -rn 'BC_LEGACY\|_NT_LEGACY' opteryx/compiled/expression/ opteryx/expression/evaluator/ --include='*.pyx' --include='*.pxd'`
    — should return zero matches (or only enum-declaration comments).
- Spot tests (representative sample across all phases):
  - `SELECT 1 + 2` (Phase 6)
  - `SELECT name FROM $planets WHERE id = 3` (Phase 4)
  - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` (Phase 1/5)
  - `SELECT missions[0] FROM testdata.astronauts LIMIT 3` (Phase 3)
  - `SELECT name FROM $planets WHERE name IS NOT NULL` (Phase 2)
  - `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4` (Phase 7)
  - `SELECT * FROM $planets WHERE id > 1 AND id < 5` (constant fold of literals)
- Verify constant-folding still works on a query whose plan it should
  reshape (e.g. `SELECT * FROM $planets WHERE 1 + 1 = 2` should
  optimise out the WHERE clause).
- Microbench: end-to-end query latency on `make clickbench` should
  not regress (constant folding is plan-time; bytecode there is a
  one-shot).

## Inventory of remaining per-morsel PyObject_Calls

This is the deliverable for Phase 8's audit half. Once tree-walker is
gone, the remaining Python on the execute path is exactly these:

| Opcode         | Call                                     | Source line                                |
|----------------|------------------------------------------|--------------------------------------------|
| BC_FUNCTION    | `callable_obj(*args)`                    | `evaluation.pyx:2363/2370/...`             |
| BC_EXTRACTION  | `(<object>slot.callable_ref)(...)`       | `evaluation.pyx:2515-2522` (one of 4)      |
| BC_CAST        | `(<object>slot.callable_ref)(py_left)`   | `evaluation.pyx:2544`                      |
| BC_CASE        | `(<object>slot.callable_ref)(morsel)`    | `evaluation.pyx:2565`                      |
| BC_BINARY_OP   | `(<object>slot.callable_ref)(left, right)` | `evaluation.pyx:2386`                    |

All five share the same shape: one `PyObject_Call` per opcode
invocation per morsel. The kernels they invoke are themselves
nanobind C++ functions — the Python boundary is the call protocol,
not the kernel body.

Capture this in `docs/zero_python_expression_engine.md` (add a "Post-
Phase-8 state" section) so the next phase's scoping is concrete.

## Constraints (from CLAUDE.md)

- **Broken-but-honest beats green-but-fake.** If a constant-folding
  test fails after the switch, surface and diagnose; don't paper over
  by keeping `evaluate(...)` as a fallback.
- **Fail fast.** Bind-time must produce a complete bytecode or raise.
  No BC_LEGACY safety net.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/planner/optimizer/strategies/constant_folding.py` — line 22
  (import) and line 289 (call site).
- `opteryx/expression/__init__.pyx` — `_inner_evaluate` (~line 500+),
  module-level `evaluate`.
- `opteryx/expression/evaluator/evaluation.pyx` — the tree-walker
  family (~line 158–973).
- `opteryx/expression/evaluator/case_eval.pyx` — `_decide`, `_compute`,
  `_assemble`, `evaluate_case` (lines 68–214).
- `opteryx/expression/evaluator/arithmetic.pyx` — `_eval_binary_op_draken`.
- `opteryx/expression/evaluator/function_execution.pyx` —
  `apply_bounded_function`. If the file becomes empty, drop it (and
  remove from `_impl.pyx` includes).
- `opteryx/expression/binary_operators.pyx` — `binary_operations`,
  `_OP_CODE_MAP`. Likely the whole file goes; relocate the two
  `*_OPERATORS` sets if needed.
- `opteryx/expression/evaluator/__init__.py` — drop tree-walker
  re-exports.
- `opteryx/expression/evaluator/_impl.pyx` — drop `include` lines
  for deleted leaf files.
- `opteryx/compiled/expression/compiled_expression.pyx` — search for
  `_NT_LEGACY` / `BC_LEGACY` and delete the emit + opcode
  declaration.
- `opteryx/compiled/expression/compiled_expression.pxd` — remove the
  `BC_LEGACY` enum value if you can without renumbering.

## Tests

- `make q` (137/137) with **fresh build**.
- All spot queries return correct values.
- All Phase 1–7 regression queries still pass.
- Existing constant-folding tests (search for `test_constant_folding`
  in `tests/`) pass.
- `make clickbench` does not regress (plan-time work is now
  bytecode-based; the bytecode compile is small and amortised).

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 1–7 tickets — particularly Phase 7's `build_case_fn`
   shape and Phase 6's `resolve_binary_op` shape (constant-folding
   will exercise both).
3. `opteryx/planner/optimizer/strategies/constant_folding.py` end to
   end. It's short.
4. `opteryx/expression/__init__.pyx` `_inner_evaluate` — understand
   the full set of node types it handles. Each one must be reachable
   via `build_bytecode(lower(node))`.
5. `opteryx/expression/evaluator/evaluation.pyx:244–973` — the
   tree-walker family. Read what's being deleted before you delete it
   (avoid accidentally removing something the bytecode emitter still
   imports — e.g. `is_scalar`, `_coerce_temporal_scalar_for_arrow` —
   verify these are not used outside the tree-walker family before
   deleting).

## Definition of done

- `constant_folding.py` uses `execute_bytecode` over a synthetic
  morsel. Zero `from opteryx.expression import evaluate` imports in
  production code.
- Tree-walker family **deleted**: `_eval_value`, `evaluate_draken`,
  `evaluate_and_append_draken`, `_unary_draken`, `_eval_cast_draken`,
  `_eval_function_draken`, `_eval_binary_op_draken`, `_decide`,
  `_compute`, `_assemble`, `evaluate_case`, `apply_bounded_function`,
  `_inner_evaluate`, module-level `evaluate`, `binary_operations`,
  `_OP_CODE_MAP`.
- BC_LEGACY opcode + `_NT_LEGACY` emit deleted from
  `compiled_expression.pyx`. Bind-time produces bytecode or raises.
- `grep` checks in §Verification return zero matches.
- `make c` clean; `make q` 100/100 with fresh build.
- `make clickbench` non-regressing.
- Inventory of remaining per-morsel `PyObject_Call`s appended to
  `docs/zero_python_expression_engine.md`.

## Side-notes to surface in PR

- The `assemble_fixed` no-ELSE + INTEGER-result crash (Phase 7
  finding) — pre-existing; confirm Phase 8 hasn't changed its
  reproduction. Needs separate ticket.
- The `COUNT(*) WHERE` returns-0 aggregate bug (Phase 2 finding) —
  pre-existing; confirm unchanged. Needs separate ticket.
- `tests/unit/expression/test_map_access_operator.py` imports the
  deleted `MapAccessOp` (Phase 3 finding) — needs cleanup ticket.
- After Phase 8, three other files likely have test cleanup:
  `tests/test_draken_comparisons.py`, `tests/draken/test_phase3_array_ops.py`,
  `tests/draken/test_phase1_evaluator.py` — all imported the now-deleted
  `draken_compare`. Verify and surface.
- Phase 9 candidate: make the executor `nogil` end-to-end by
  replacing each `PyObject_Call` with a C function-pointer call via a
  kernel ABI. The five call sites are catalogued in §Inventory.
  Scope and feasibility analysis is a separate piece of work; this
  ticket only sets up the audit.
