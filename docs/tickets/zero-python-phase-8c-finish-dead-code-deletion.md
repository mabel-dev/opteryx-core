# Ticket: Zero-Python Expression Engine — Phase 8c (finish dead-code deletion)

> Part of `docs/zero_python_expression_engine.md`. Phase 8b deleted the
> bulk of the tree-walker but left a handful of functions unreachable
> in source. This ticket finishes that cleanup — pure deletion, no
> behaviour change. Closes Phase 8b's DoD.

## What's left

Verified inventory (2026-05-28, post-Phase-8b):

| Symbol | File | Line | Type |
|---|---|--:|---|
| `_decide` | `opteryx/expression/evaluator/case_eval.pyx` | 63 | cdef |
| `_compute` | `opteryx/expression/evaluator/case_eval.pyx` | 113 | cdef |
| `_assemble` | `opteryx/expression/evaluator/case_eval.pyx` | 149 | cdef |
| `evaluate_case` | `opteryx/expression/evaluator/case_eval.pyx` | 201 | def |
| `apply_bounded_function` | `opteryx/expression/evaluator/function_execution.pyx` | 14 | def |
| `_OP_CODE_MAP` | `opteryx/expression/binary_operators.pyx` | 19 | dict |
| `binary_operations` | `opteryx/expression/binary_operators.pyx` | 35 | def |
| `BC_LEGACY` enum value | `opteryx/compiled/expression/compiled_expression.pxd` | 128 | `= 99` |
| BC_LEGACY emit (the "everything else" fallback) | `opteryx/compiled/expression/compiled_expression.pyx` | ~784 | code block |

Verified dead:
- No production-code call site outside the tree-walker family.
- The only remaining references are docstring mentions
  (`evaluation.pyx:519`, `expression/__init__.pyx:328` — both
  describe-the-old-system comments, harmless to leave or update).
- `evaluate_and_append_draken` (the cpdef function) is already gone
  from `evaluation.pyx`. The evaluator `__init__.py` no longer
  exports it. Only stale doc references remain.
- BC_LEGACY executor case in `evaluation.pyx` is already gone — bind
  time can still emit BC_LEGACY but the executor won't handle it →
  any query that hits the fallback would already crash. Bind-time
  deletion closes that gap.
- `_inner_evaluate` and module-level `evaluate` in
  `opteryx/expression/__init__.pyx` are already gone. No work
  needed there beyond updating the one docstring mention.

## Goal

After Phase 8c:
- All symbols in the table above are deleted from source.
- BC_LEGACY emit becomes a bind-time `raise NotImplementedError`.
- The `BC_LEGACY` enum value can be left in the `.pxd` as a hole
  (value `99`, unused) OR removed — surface choice in PR. Removing
  it is risky only if some external `.pxi` references it by name;
  grep first.
- The two stale docstring references to `evaluate_and_append_draken`
  are updated or deleted (trivial).
- `make q` 137/137 unchanged; this is pure dead-code removal.

## Scope

**Delete**
- `opteryx/expression/evaluator/case_eval.pyx`:
  - `_decide` (lines 63–110)
  - `_compute` (lines 113–146)
  - `_assemble` (lines 149–188)
  - `evaluate_case` (lines 201–214)
  - The module docstring's "Two entry points" description (lines
    13–15) — currently misleading; only `build_case_fn` survives.
  - Imports that no longer have callers after the deletions (verify
    with grep before removing).
- `opteryx/expression/evaluator/function_execution.pyx`:
  - `apply_bounded_function` (lines 14–48).
  - If `is_draken_vector` re-export at line 11 is the only thing
    left, **delete the entire file**. Remove its `include "function_execution.pyx"`
    line from `_impl.pyx`. Update any caller importing
    `is_draken_vector` from this path to import from
    `opteryx.utils.vector_types` directly.
- `opteryx/expression/binary_operators.pyx`:
  - `_OP_CODE_MAP` (line 19).
  - `binary_operations` (line 35).
  - Imports that no longer have callers after deletion (verify).
  - **Keep**: `BINARY_OPERATORS` and `EXTRACTION_OPERATORS` sets
    (the binder reads these). If those are the only thing left,
    consider relocating them to a 10-line
    `opteryx/expression/operator_catalog.py` and deleting
    `binary_operators.pyx` entirely — **surface this choice in PR**;
    either is fine.
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - The "Everything else — LEGACY" fallback at lines 778–789.
    Replace with:
    ```cython
    # Bind-time invariant: every supported node type has an explicit
    # branch above. Reaching here is a planner/compiler bug.
    raise NotImplementedError(
        f"compiled_expression: unsupported node type {nt}"
    )
    ```
- `opteryx/compiled/expression/compiled_expression.pxd`:
  - `BC_LEGACY = 99` enum value at line 128. Delete if safe (no
    external numeric reference). If you're unsure, **leave it as
    a hole** — keeping it doesn't hurt, only `make c` clean matters.

**Update (one-liners)**
- `opteryx/expression/evaluator/evaluation.pyx:519` — docstring
  comment "Replaces evaluate_and_append_draken at execution time" —
  rephrase to past tense or delete the sentence. (The function it
  describes is already gone.)
- `opteryx/expression/__init__.pyx:328` — docstring comment "Used by
  the Cython evaluate_and_append_draken to skip nodes" — same
  treatment.

**Append (one paragraph)**
- `docs/zero_python_expression_engine.md` — add a "Post-Phase-8b/8c
  state" section with the per-morsel `PyObject_Call` inventory from
  Phase 8's ticket (the agent who did Phase 8 didn't add this). Copy
  the table verbatim from the Phase 8 ticket's §"Inventory of
  remaining per-morsel PyObject_Calls".

**Out of scope**
- Phase 9: C function-pointer kernel ABI for `nogil` executor.
- The two pending correctness tickets
  (`bug-count-star-where-returns-zero.md`,
  `bug-assemble-fixed-no-else-int-segfault.md`).
- The 4 test files importing deleted symbols
  (`test_map_access_operator.py`, `test_draken_comparisons.py`,
  `test_phase3_array_ops.py`, `test_phase1_evaluator.py`).
  Separate cleanup ticket (which doesn't exist yet — flag in PR).

## Safety check before each deletion

Run this grep before deleting each symbol. **Zero non-self matches
required**:

```bash
SYM=...   # e.g. _decide, _compute, evaluate_case, etc.
grep -rn "\b$SYM\b" /Users/justin/Nextcloud/opteryx-core/opteryx \
    /Users/justin/Nextcloud/opteryx-core/draken \
    --include='*.py' --include='*.pyx' --include='*.pxd' --include='*.pxi' \
  | grep -v __pycache__ | grep -v build/ \
  | grep -v "$SOURCE_FILE_BEING_EDITED"
```

If the grep returns anything outside the file you're deleting from
**stop and surface in PR** — the symbol has an unexpected caller.

For `BINARY_OPERATORS` and `EXTRACTION_OPERATORS` (which **stay**),
confirm they're still referenced by the binder before you finish —
if they're not, they can go too.

## Verification

- `make c` clean **fresh build** (Phase 4 lesson, repeated for the
  N-th time).
- `make q` 100/100 (currently 137/137).
- Symbol checks:
  ```
  grep -rn '\b_decide\b\|\b_compute\b\|\b_assemble\b\|\bevaluate_case\b\|\bapply_bounded_function\b\|\bbinary_operations\b\|\b_OP_CODE_MAP\b' \
    /Users/justin/Nextcloud/opteryx-core/opteryx \
    --include='*.py' --include='*.pyx' --include='*.pxd' \
    | grep -v __pycache__ | grep -v build/
  ```
  — should return zero matches (except possibly comments in the plan
  doc, which are fine).
- `grep -rn '\bBC_LEGACY\b' opteryx/ --include='*.py' --include='*.pyx' --include='*.pxd' | grep -v __pycache__ | grep -v build/`
  — zero matches (or only the enum-as-hole declaration if you kept it).
- Cross-phase spot tests (representative — verify cross-phase work
  still passes):
  - `SELECT 1 + 2`
  - `SELECT name FROM $planets WHERE id = 3`
  - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3`
  - `SELECT missions[0] FROM testdata.astronauts LIMIT 3`
  - `SELECT name FROM $planets WHERE name IS NOT NULL LIMIT 3`
  - `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4`
  - `SELECT * FROM $planets WHERE 1 + 1 = 2 LIMIT 2` (constant folding)
- `make clickbench` non-regressing.
- LOC delta in PR description: roughly `-300` to `-500` (Phase 8b
  already took the big chunk; this is the tail).

## Constraints (from CLAUDE.md)

- **Pure deletion.** Phase 8c changes no behaviour. Any test that
  worked before this PR works after.
- **Fail fast.** BC_LEGACY's "everything else" fallback becomes an
  explicit raise — that's the point.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/case_eval.pyx` — lines 63–214.
  Keep `build_case_fn`, `_decide_compiled`, `_compute_compiled`,
  `_sub_morsel`, and the module-level helpers above line 60.
- `opteryx/expression/evaluator/function_execution.pyx` — verify
  whether deleting `apply_bounded_function` empties the file.
- `opteryx/expression/evaluator/_impl.pyx` — `include` list. Drop
  any line for a now-deleted leaf file.
- `opteryx/expression/binary_operators.pyx` — lines 19, 35. Keep
  the two `*_OPERATORS` sets.
- `opteryx/compiled/expression/compiled_expression.pyx` — lines
  778–789 (the LEGACY fallback emit).
- `opteryx/compiled/expression/compiled_expression.pxd` — line 128
  (BC_LEGACY enum).
- `opteryx/expression/evaluator/evaluation.pyx` — line 519 (stale
  docstring).
- `opteryx/expression/__init__.pyx` — line 328 (stale docstring).
- `docs/zero_python_expression_engine.md` — append section.

## Tests

- `make q` 137/137 with **fresh build**.
- Cross-phase spot tests all pass.
- `make clickbench` non-regressing.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md` and the Phase 8b /
   Phase 8 tickets.
2. Run the grep commands in §Verification **before** deleting
   anything. Save the output. If anything in your safety checks
   surprises you, surface and stop.

## Definition of done

- All symbols in the inventory table are deleted from source.
- BC_LEGACY emit replaced with an explicit `raise NotImplementedError`.
- BC_LEGACY enum value either deleted from the `.pxd` or
  documented as a kept-hole in PR description.
- `function_execution.pyx` either deleted (file empty) or trimmed
  to the `is_draken_vector` re-export only.
- `binary_operators.pyx` trimmed to the two `*_OPERATORS` sets, OR
  deleted entirely with the sets relocated to a new
  `operator_catalog.py` (PR surfaces choice).
- Two stale docstrings (`evaluation.pyx:519`,
  `__init__.pyx:328`) updated/deleted.
- `docs/zero_python_expression_engine.md` appended with the
  Post-Phase-8b/8c inventory section.
- `make c` clean; `make q` 100/100 with fresh build.
- `make clickbench` non-regressing.
- LOC delta in PR description.

## Notes for the architect

After Phase 8c lands, the zero-Python expression engine work train
is **fully closed** under the current architecture. The five
remaining per-morsel `PyObject_Call` instances (in BC_FUNCTION /
BC_EXTRACTION / BC_CAST / BC_CASE / BC_BINARY_OP fallbacks) require a
C function-pointer kernel ABI — a fresh architectural design piece,
not a continuation of this train.

Three correctness tickets remain in the queue:
- `docs/tickets/bug-count-star-where-returns-zero.md`
- `docs/tickets/bug-assemble-fixed-no-else-int-segfault.md`
- (To be written) test-file cleanup for 4 files importing deleted
  symbols.

The Phase 8 architectural milestone is shipped; Phase 8c just makes
the source code match the architecture.
