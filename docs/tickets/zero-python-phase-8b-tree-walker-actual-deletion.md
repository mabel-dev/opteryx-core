# Ticket: Zero-Python Expression Engine — Phase 8b (tree-walker actual deletion)

> Part of `docs/zero_python_expression_engine.md`. Phase 8 landed the
> constant-folding switch to bytecode (the architecturally hard half)
> but left ~700 lines of now-dead tree-walker code in place. Phase 8b
> deletes them.

## What Phase 8 left behind

Phase 8's DoD said:

> Tree-walker family **deleted**: `_eval_value`, `evaluate_draken`,
> `evaluate_and_append_draken`, `_unary_draken`, `_eval_cast_draken`,
> `_eval_function_draken`, `_eval_binary_op_draken`, `_decide`,
> `_compute`, `_assemble`, `evaluate_case`, `apply_bounded_function`,
> `_inner_evaluate`, module-level `evaluate`, `binary_operations`,
> `_OP_CODE_MAP`. BC_LEGACY opcode + `_NT_LEGACY` emit deleted.

The agent shipped the constant-folding switch (`constant_folding.py`
now uses `build_bytecode` + `execute_bytecode`) and updated
`__init__.py` exports — but **did not delete the source code**.

Verified dead: grep confirms the only external reference to
`_eval_value` is the BC_LEGACY emit at
`compiled/expression/compiled_expression.pyx:779`, whose own comment
says *"BC_LEGACY should now be unreachable for all supported node
types."* Every other call to a tree-walker function comes from
another tree-walker function — a closed cycle.

This ticket: cut the cycle. Delete the dead code.

## Goal

After Phase 8b:
- The tree-walker functions are gone from source.
- BC_LEGACY opcode and `_NT_LEGACY` fallback emit are gone — bind-time
  produces a complete bytecode or raises.
- The "inventory of remaining per-morsel PyObject_Calls" from Phase 8
  is appended to `docs/zero_python_expression_engine.md`.
- `make q` 137/137 with fresh build. No behaviour change — this is pure
  deletion of unreachable code.

## Scope

**In scope — delete**
- `opteryx/expression/evaluator/evaluation.pyx`:
  - `_eval_cast_draken` (~line 153)
  - `_eval_function_draken` (~line 192)
  - `_eval_binary_op_draken` if it lives here (verify; otherwise it
    was moved to `arithmetic.pyx` during Phase 6 and lives there)
  - `_eval_value` (~line 244)
  - `_unary_draken` (~line 336)
  - `evaluate_draken` (~line 870 cpdef)
  - `evaluate_and_append_draken` (~line 990 cpdef)
  - The lazy-import module-level fn caches (`_cast_factory_fn`,
    `_binary_ops_fn` etc. around line 146)
  - The BC_LEGACY executor case in `execute_bytecode` (search for
    `if opcode == BC_LEGACY` or `BC_LEGACY` in the case chain — usually
    after BC_CASE in line order)
- `opteryx/expression/evaluator/case_eval.pyx`:
  - `_decide` (line 68)
  - `_compute` (line 118)
  - `_assemble` (line 149) — dead since Phase 7 inlined the dispatch
    into `_case_fn`
  - `evaluate_case` (line 196)
  - Update the module docstring (line 15) — the "two entry points"
    description is no longer accurate; only `build_case_fn` survives.
- `opteryx/expression/evaluator/arithmetic.pyx`:
  - `_eval_binary_op_draken` (cpdef, around the file's top half)
- `opteryx/expression/evaluator/function_execution.pyx`:
  - `apply_bounded_function` (line 14)
  - If the file becomes empty (or just the `__all__` and the
    `is_draken_vector` re-export), delete the file. Update
    `_impl.pyx`'s `include "function_execution.pyx"` accordingly.
- `opteryx/expression/__init__.pyx`:
  - `_inner_evaluate` (~line 500)
  - module-level `evaluate` (~line 840 — search `def evaluate(`)
- `opteryx/expression/binary_operators.pyx`:
  - `binary_operations` (~line 35)
  - `_OP_CODE_MAP` (~line 19)
  - `_dispatch_arithmetic_operation`, `_to_bytes_or_vec`,
    `_ARITHMETIC_OPS` if they survived earlier phases
  - Keep `BINARY_OPERATORS` and `EXTRACTION_OPERATORS` sets — the
    binder still reads these. (If the rest of the file goes, those
    two could be relocated to a 10-line
    `opteryx/expression/operator_catalog.py`; surface that choice
    in PR or leave the trimmed file as-is.)
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - The `_NT_LEGACY` / BC_LEGACY fallback emit block at line 778–789.
    Replace with: `raise NotImplementedError(f"compiled_expression: unsupported node type {nt}")`
- `opteryx/compiled/expression/compiled_expression.pxd`:
  - `BC_LEGACY` enum value — delete if numerically safe (no
    other opcode is referenced by its numeric value relative to
    BC_LEGACY). Surface choice in PR.
- `opteryx/expression/evaluator/_impl.pyx`:
  - Drop the `include` line for any leaf file you deleted.
- `opteryx/expression/evaluator/__init__.py`:
  - Drop `evaluate_and_append_draken` re-export — no remaining
    production caller after Phase 8's `constant_folding.py` switch.
    Verify with grep before deleting.

**In scope — write**
- `docs/zero_python_expression_engine.md` — append a "Post-Phase-8b
  state" section with the table of remaining per-morsel
  `PyObject_Call` sites. Phase 8's ticket provides the exact text
  (see its §"Inventory of remaining per-morsel PyObject_Calls"). Copy
  it in.

**Out of scope (separate tickets)**
- Phase 9: C function-pointer kernel ABI to enable true `nogil`
  executor. Phase 8b only sets the stage by documenting the call
  sites.
- The accumulated side-issues:
  - `assemble_fixed` no-ELSE-INT segfault (Phase 7 finding)
  - `COUNT(*) WHERE` returns-0 aggregate bug (Phase 2 finding)
  - 4 test files importing deleted symbols (Phases 3 & 4 findings)

## Verification

- `make c` clean **fresh build** (Phase 4 lesson, repeated for the
  N-th time).
- `make q` 100/100 (currently 137/137).
- Symbol checks (production code only):
  - `grep -rn '_eval_value\|evaluate_draken\|_unary_draken\|_eval_cast_draken\|_eval_function_draken\|_eval_binary_op_draken\|apply_bounded_function\|evaluate_and_append_draken\|_inner_evaluate' opteryx/ --include='*.py' --include='*.pyx' --include='*.pxd' --include='*.pxi' | grep -v '__pycache__' | grep -v 'build/' | grep -vE '^\s*#|\"\"\"'`
    — **zero non-comment matches**.
  - `grep -rn 'evaluate_case\b' opteryx/ --include='*.py' --include='*.pyx' | grep -v '__pycache__' | grep -v 'build/'`
    — should match only `build_case_fn` / `_decide_compiled` /
    `_compute_compiled` (the compiled-bytecode path that survives).
  - `grep -rn 'BC_LEGACY\|_NT_LEGACY' opteryx/ --include='*.py' --include='*.pyx' --include='*.pxd' | grep -v '__pycache__' | grep -v 'build/'`
    — zero non-comment matches (or only enum-removal in the .pxd if
    you kept the value as a hole).
  - `grep -rn '_inner_evaluate\|^def evaluate\b' opteryx/expression/__init__.pyx`
    — zero matches.
- Spot tests (one per phase, plus constant-folding):
  - `SELECT 1 + 2` (Phase 6)
  - `SELECT name FROM $planets WHERE id = 3` (Phase 4)
  - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` (Phase 1/5)
  - `SELECT missions[0] FROM testdata.astronauts LIMIT 3` (Phase 3)
  - `SELECT name FROM $planets WHERE name IS NOT NULL LIMIT 3` (Phase 2)
  - `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4` (Phase 7)
  - `SELECT * FROM $planets WHERE 1 + 1 = 2 LIMIT 2` (constant fold, Phase 8)
- Bind-time unsupported-node behaviour: write or find a query whose
  node type doesn't have a `_linearize` branch (if any exist). It
  should now raise at plan time, not silently emit BC_LEGACY. If
  every node type IS supported (likely), this is just code-removal.
- `make clickbench` non-regressing — deletion only, no perf change.

## Constraints (from CLAUDE.md)

- **Pure deletion**. Phase 8b changes no behaviour, adds no features.
  Any test that worked before this PR must work after. Any test that
  was broken before (Phase 7's `assemble_fixed`, Phase 2's `COUNT(*)`)
  remains broken — fixing them is **out of scope**.
- **Fail fast.** The BC_LEGACY fallback in `compiled_expression.pyx`
  becomes an explicit raise — that's the point.
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Risk — there is one

The grep checks above are the safety net. The actual risk is something
indirectly importing one of the tree-walker symbols via a *different*
name (e.g. `from opteryx.expression.evaluator.evaluation import *` ).
Run the symbol grep before deleting; surface any unexpected hit. Do
not delete a function whose grep returns external matches — fix the
caller first or surface the case.

If you find an external caller you didn't expect, **stop**. Either:
- The caller is plan-time-only and harmless — switch it to bytecode
  the same way `constant_folding.py` was, then delete.
- The caller is hot-path and the tree-walker was secretly reachable —
  this is a Phase 8 oversight; surface for the architect.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 8's ticket — particularly its §"Inventory" section (you
   copy that into the plan doc).
3. Grep results from §Verification — run the greps **before**
   deleting, save the output, do the deletions, run the greps again.

## Definition of done

- All tree-walker functions listed in §Scope are deleted from source.
  Grep confirms zero non-comment matches in production code.
- BC_LEGACY / `_NT_LEGACY` deleted from
  `compiled_expression.pyx`. The "everything else" fallback now
  raises `NotImplementedError` at bind time.
- `function_execution.pyx` either deleted (if empty) or trimmed to
  just `is_draken_vector` re-export.
- `binary_operators.pyx` reduced to the two `*_OPERATORS` sets
  (or those relocated and the file deleted).
- `evaluation.pyx`, `case_eval.pyx`, `arithmetic.pyx` reduced to the
  bytecode-path code only.
- `_impl.pyx` `include` list updated.
- `__init__.py` re-exports updated.
- `docs/zero_python_expression_engine.md` appended with the
  Post-Phase-8b state (per-morsel `PyObject_Call` inventory).
- `make c` clean; `make q` 100/100 with fresh build.
- `make clickbench` non-regressing.
- LOC delta in PR description: roughly `-700 LOC`. Surface the exact
  number.

## Notes for the architect

After Phase 8b lands, the zero-Python expression engine work is
**at its end-state under the current architecture**. The five
remaining per-morsel `PyObject_Call` instances (in BC_FUNCTION /
BC_EXTRACTION / BC_CAST / BC_CASE / BC_BINARY_OP fallbacks) cannot
be eliminated without a C function-pointer kernel ABI for nanobind
methods.

That's a Phase 9 question. The Phase 8b deliverable is the inventory
documenting where those call sites live; the scoping of how to replace
them with a C ABI is a fresh design discussion, not a continuation of
the current ticket train.
