# Ticket: Zero-Python Phase 9f — Cleanup (delete dead flags, resolver closures, callable_ref)

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design).
> Implements Decision 6 (delete result-wrap flags) + general dead-code
> removal. **Depends on 9c + 9e** — the executor must be fully on
> C-pointer dispatch and nogil before the Python dispatch machinery
> can be removed. Final ticket in the Phase 9 train.

## Goal

Delete the Python dispatch machinery that 9a–9e made dead:
- The `BC_RESULT_*` result-wrap flags (Decision 6).
- The `slot.callable_ref` field reads for the 5 opcodes (now unused —
  the slot carries `kernel_fn`/`ctx_ptr` instead).
- The Python resolver closures (`resolve_cast`'s closure returns,
  `resolve_binary_op`'s `_build_*_closure` functions, `build_case_fn`'s
  `_case_fn` closure) — replaced by the C kernels + context structs.

After 9f, the only Python on the path is the result-wrap at executor
exit (which is the morsel boundary, by design — not debt).

## Locked decision

**Decision 6**: delete `BC_RESULT_NEEDS_NB_WRAP`,
`BC_RESULT_WRAP_AS_BOOL`, `BC_RESULT_NO_DV`. They were only read by the
Python-callable dispatch, which 9c removed.

## Scope

**In scope — delete**
- `opteryx/compiled/expression/compiled_expression.pyx:73-75` — the
  three `BC_RESULT_*` flag definitions.
- Every `slot.flags & BC_RESULT_*` read in `evaluation.pyx` (the 5
  opcode branches — 9c should have removed the wrap blocks; verify and
  delete any residual).
- `slot.callable_ref` population for the 5 opcodes in
  `compiled_expression.pyx` (9b kept it set; now unused). The struct
  field itself can stay if any *other* opcode uses it — grep; if none,
  delete the field from the `.pxd` too (note the struct-size change).
- The resolver closures, now that bind-time builds C contexts instead:
  - `opteryx/expression/casts.pyx` — the closure-returning paths of
    `resolve_cast` (the `lambda` / nested-`def` returns). The resolver
    becomes "return (c_kernel_ptr, ctx)" instead of a Python callable.
    Verify what 9b actually consumes and align.
  - `opteryx/expression/evaluator/arithmetic.pyx` —
    `_build_arithmetic_closure`, `_build_string_concat_closure`,
    `_build_bitwise_closure`, the `_ip_in_cidr_kernel` /
    `_date_interval_kernel` / `_interval_wrapper` closures inside
    `resolve_binary_op`.
  - `opteryx/expression/evaluator/case_eval.pyx` — `build_case_fn`'s
    `_case_fn` Python closure (the C `case_kernel` from 9c replaces it).
    `_decide_compiled` / `_compute_compiled` may survive if the C
    kernel calls them — verify against 9c's implementation.

**Important**: 9f deletes only what 9a–9e genuinely orphaned. Before
deleting each symbol, grep for live callers. If 9b/9c kept something
alive intentionally (e.g. a resolver still used at bind time to *build
the context*), it stays. Surface any ambiguity.

**Out of scope**
- Any behaviour change. 9f is pure dead-code removal; `make q` and
  `make clickbench` must be identical before/after.
- The two correctness bugs (separate tickets).

## Verification

- `make c` clean fresh build.
- `make q` 100/100.
- Symbol checks (zero live references):
  - `grep -n 'BC_RESULT_' opteryx/ -r --include='*.pyx' --include='*.pxd'`
    → only the deletion in your diff; zero live reads.
  - `grep -n 'callable_ref' opteryx/expression/evaluator/evaluation.pyx
    opteryx/compiled/expression/compiled_expression.pyx` → zero (or
    documented survivor for a non-Phase-9 opcode).
  - `grep -n '_build_arithmetic_closure\|_build_string_concat_closure\|_build_bitwise_closure\|_case_fn' opteryx/`
    → zero (or documented survivors that bind-time still needs).
- Value-checked spot tests across all 5 opcodes still pass (carry the
  9c list).
- `make clickbench` non-regressing vs the 9e baseline.
- Final audit: append to `docs/zero_python_expression_engine.md` a
  "Phase 9 complete" section noting the executor is nogil with zero
  per-morsel PyObject_Call. The per-morsel-PyObject_Call inventory
  table (added in 8c) now shows zero rows.

## Constraints (CLAUDE.md)

- **Pure deletion** — no behaviour change.
- **Fail fast** — anything that was a fallback is gone; bind-time
  raises on unresolvable kernels (already true after 9b).
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. Phase 9 design §Post-design (Decision 6).
2. 9b ticket (what populates `callable_ref` vs `kernel_fn`) and 9c
   ticket (what stopped reading `callable_ref`).
3. The resolver files: `casts.pyx:resolve_cast`,
   `arithmetic.pyx:resolve_binary_op`, `case_eval.pyx:build_case_fn`.
   Compare against what 9b/9c actually consume — delete only the
   orphaned closures.
4. Run the symbol-check greps **before** deleting, save output.

## Definition of done

- `BC_RESULT_*` flags deleted; zero live reads.
- `callable_ref` reads for the 5 opcodes deleted; field removed from
  `.pxd` if no other opcode uses it (struct-size note in PR).
- Orphaned resolver closures deleted; bind-time-still-needed resolvers
  retained and documented.
- All value-checked spot tests pass; behaviour identical.
- `docs/zero_python_expression_engine.md` gets a "Phase 9 complete"
  section; the PyObject_Call inventory shows zero rows.
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.

## End of the train

When 9f lands, the Zero-Python Expression Engine initiative is
complete: plan-time Python compiles to bytecode; `execute_bytecode`
runs `nogil` with zero per-morsel Python. The architect's goal from
2026-05-27 is delivered.

Remaining independent items (NOT part of this train):
- `bug-count-star-where-returns-zero.md`
- `bug-assemble-fixed-no-else-int-segfault.md`
- Test-file cleanup (4 files importing Phase 3/4 deleted symbols)
- Value-checked test-coverage expansion (recurring need surfaced
  across the train; `make q` is shape-only)
