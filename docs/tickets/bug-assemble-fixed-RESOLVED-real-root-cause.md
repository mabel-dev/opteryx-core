# RESOLVED: "assemble_fixed SIGBUS" — real root cause was the bytecode executor, not assemble_fixed

> Closes `bug-assemble-fixed-no-else-int-segfault.md` and the three
> corrective rounds (`bug-assemble-fixed-completion-3.md` et al.). Those
> tickets named the wrong file. The crash was in the **bytecode
> executor's pure-bitmap fall-back path**, not in
> `case_helpers.pyx::assemble_fixed`. This is why three rounds of edits
> to `assemble_fixed` could not fix it — there was nothing wrong there.

## Status: FIXED

- `make c` clean.
- `make et` → **35 passed** (was 29); repros A and B present as real,
  value-checked tests and passing.
- `make q` → 137/137 (100%).
- Repros print the expected lists (below). No SIGBUS.
- Not committed (per CLAUDE.md).

## The two repros (now pass)

```sql
SELECT CASE WHEN id > 4 THEN NULL ELSE id END FROM $planets LIMIT 6  → [1, 2, 3, 4, None, None]
SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4            → [1, None, None, None]
```

Controls (unchanged):
```sql
SELECT CASE WHEN id < 100 THEN id END FROM $planets LIMIT 4  → [1, 2, 3, 4]
SELECT CASE WHEN id < 100 THEN 1   END FROM $planets LIMIT 4  → [1, 1, 1, 1]
SELECT CASE WHEN id<3 THEN 'a' ELSE 'b' END FROM $planets     → ['a','a','b','b']
```

## Real root cause

A bare `THEN id` / `ELSE id` CASE result lowers to a **single
`BC_LOAD_COL`** over an INT column. `build_bytecode`
(`compiled_expression.pyx`) flags any bytecode whose opcodes are all in
`_PURE_BITMAP_OPCODES` (which includes `BC_LOAD_COL`) as
`is_pure_bitmap=True`, deferring the column-type check to a runtime
pre-pass. So a bare INT-column load was routed into `evaluate_bitmap`
(the nogil boolean-predicate VM). The pre-pass correctly detected the
non-`BoolVector` column and tried to fall back to the general executor —
but the fall-back path had two defects:

### Defect 1 — `except -1` sentinel collision (the SIGBUS)

`_execute_bytecode_prepass` (`evaluation.pyx`) was declared
`cdef int ... except -1`, yet it *returns* `-1` as its legitimate
"column is not a BoolVector → caller must fall back" signal. Cython
treats the declared `except` value as the error sentinel: when the
function returned `-1`, the generated code assumed an exception had been
raised and tried to propagate a non-existent one → `EXC_BAD_ACCESS` in
`PyException_GetTraceback` (the original crash's `address=0x28`, frame
#0). The corrupted single-frame backtrace was this mis-propagation, not
a bad `memcpy`.

**Fix:** declare `except? -2`. `-2` is never returned, so `-1` and `0`
are valid values; a *real* exception (e.g. `MemoryError`) still
propagates because Cython sets the error and returns the sentinel, and
the `?` makes Cython disambiguate via `PyErr_Occurred()`.

### Defect 2 — infinite recursion in the fall-back

`evaluate_bitmap`'s fall-back did `return execute_bytecode(bc, morsel)`
with `bc.is_pure_bitmap` **still True**, so `execute_bytecode`
re-dispatched straight back into `evaluate_bitmap` → unbounded
recursion. (Masked until now because Defect 1 crashed first.)

**Fix:** clear `bc.is_pure_bitmap = False` before falling back. Column
types are schema-bound and stable for the life of a `CompiledBytecode`,
so this is permanent and correct — and a minor perf win (skips the
now-known-futile bitmap pre-pass on every later morsel).

### Defect 3 — return-type coercion

`evaluate_bitmap` was `cpdef BoolVector`; the fall-back legitimately
returns a non-bool `Vector` (an INT CASE-result column). Cython tried to
coerce → `TypeError`. **Fix:** relaxed to `cpdef object` (the bitmap
path still returns a `BoolVector`).

## Files changed

- `opteryx/expression/evaluator/evaluation.pyx` — the three fixes above
  (`_execute_bytecode_prepass` signature; `evaluate_bitmap` fall-back
  flag flip; `evaluate_bitmap` return type).
- `tests/test_expression_engine.py` — removed the gamed
  `test_case_when_with_null_literal_and_else_constant` (it used
  `ELSE 88` constant to avoid triggering repro A's `ELSE id` column
  path); added `test_case_when_null_then_else_column` (A),
  `test_case_when_column_result_no_else` (B),
  `test_case_when_column_result_all_match` (control).

`case_helpers.pyx::assemble_fixed` was **not** the cause. The
DRAKEN_NULL handling a prior round added there is left in place: it is
now legitimately exercised by repro A's `THEN NULL` branch and produces
correct output.

## Lessons for the misdiagnosis

- The diagnosis in the prior tickets ("DRAKEN_NULL part dereferenced",
  "unmatched scatter") was asserted as fact and told the agent not to
  re-investigate. It was wrong. A corrupted, single-frame backtrace is a
  **stack overflow / exception-machinery** signature, not a data-pointer
  deref — that pointed at control flow, not `memcpy`.
- The fix was found by instrumenting downward from the call site
  (`assemble_fixed` → `_compute_compiled` → `execute_bytecode` →
  `evaluate_bitmap` → pre-pass) until the last line that printed before
  the crash. No amount of editing `assemble_fixed` could have worked.
