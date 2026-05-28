# Ticket: Zero-Python Expression Engine — Phase 7 (CASE inner-loop natives)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–6 have landed:
> see `docs/tickets/zero-python-phase-{1..6}-*.md`. This is Phase 7.

## Problem

BC_CASE calls into `opteryx/expression/evaluator/case_eval.pyx` through
a closure built at bind time by `build_case_fn`. The closure walks
WHEN conditions and THEN/ELSE results in a three-phase model: **decide
→ compute → assemble**. Each phase has per-morsel Python overhead
that survived the earlier phases:

1. **`_sub_morsel(morsel, indices)`** (`case_eval.pyx:49`) — called
   per CASE branch per morsel. Python loop over column names; per
   column: `key = n if isinstance(n, bytes) else n.encode()`, then
   `morsel.column(key).take(indices)`. That's ~3 Python ops × N
   columns × ~3 branches × every morsel.

2. **`_assemble(...)` dispatch** (`case_eval.pyx:154`) — per CASE
   evaluation, walks `parts` to find the first non-None, then
   `isinstance(first, BoolVector)` plus
   `getattr(first, "type", None) in (VARCHAR, NVARCHAR)` to pick
   between `assemble_bool` / `assemble_flat_string` / `assemble_fixed`.
   The output type is known at bind time from the THEN-branch
   result expressions' inferred types — the runtime walk is pure
   overhead.

3. **The closure invocation itself** (`callable_ref(morsel)`) — single
   `PyObject_Call` per CASE per morsel. Acceptable boundary; same
   shape as BC_FUNCTION / BC_CAST after Phase 1.

This phase eliminates (1) and (2). (3) remains as the bind-time-to-
runtime boundary, consistent with how the other opcodes leave it.

## Goal

After Phase 7:
- `_sub_morsel`'s per-column Python loop is replaced with a single
  typed Cython helper on `Morsel` that takes a row-index buffer and
  returns a new `Morsel` in one cdef pass.
- `_assemble`'s output-type dispatch happens **at bind time**.
  `build_case_fn` accepts a pre-resolved assembly kernel pointer.
  Runtime `_assemble` becomes a direct call to the resolved kernel.
- The runtime CASE path has zero `isinstance` / `getattr` /
  `type(x).__name__` per morsel.
- Legacy tree-walker `evaluate_case` / `_decide` / `_compute` paths
  remain for now (Phase 8 deletes them along with the rest of the
  tree-walker).

## Scope

**In scope**
- `draken/morsels/_morsel_shim.pyx` — add a typed multi-column take
  helper. Public surface: `cpdef Morsel take_rows(self, indices)` or
  `cpdef Morsel take_rows_int32(self, int32_t[::1] indices)` —
  surface in PR which signature; recommendation is typed memoryview
  for zero-Python in the loop body.
- `opteryx/expression/evaluator/case_eval.pyx`:
  - Replace `_sub_morsel` body with a single call to the new Morsel
    helper. The function itself can stay as a one-liner cdef wrapper
    (or callers inline the new method directly).
  - Move `_assemble`'s output-type dispatch to bind time.
    Add a new typed dispatcher `_assemble_with_kernel(parts, …,
    assemble_kernel)` that just calls `assemble_kernel(...)` with no
    runtime type sniffing.
  - Update `build_case_fn(cond_bcs, result_bcs, else_bc,
    assemble_kernel, else_orso)` — new params for the pre-resolved
    assembly kernel and the result type (for null-case fallback).
  - The legacy `evaluate_case` / `_decide` / `_compute` paths stay
    unchanged — they're tree-walker only (plan-time). Phase 8
    deletes them.
- `opteryx/compiled/expression/compiled_expression.pyx`
  `_NT_CASE` emit (~line 634) — resolve the CASE's output type from
  `src.results[i].inferred_type` (first non-None) or `src.else_result.inferred_type`.
  Select one of the three assemble kernels at bind time. Pass to
  `build_case_fn`.

**Out of scope**
- BC_CASE executor changes — already minimal after Phase 1.
- Annotating the executor `nogil` — Phase 8.
- Deleting the tree-walker `evaluate_case` / `_decide` / `_compute` —
  Phase 8.
- Adding new CASE features (CASE expression alternative form, etc.).

## The `Morsel.take_rows` helper

Today `morsel.take(indices)` exists but takes a `list` of indices and
loops in Python (`draken/morsels/_morsel_shim.pyx:300`):

```cython
def take(self, indices):
    cdef Morsel result = _make_morsel()
    result._col_names = list(self._col_names)
    cdef int n = len(self._columns)
    idx_list = list(indices)
    for i in range(n):
        nb_taken = (<Vector>self._columns[i])._nb.take(idx_list)
        result._nb.append(nb_taken)
        result._columns.append(Vector(nb_taken))
    return result
```

That's already mostly typed; the Python parts are `list(indices)` and
`list(...)` and per-column `Vector(nb_taken)` (one allocation per
column). For CASE we call this per-branch on `live` (an `array('i')`).

The CASE caller does:

```cython
sub = _sub_morsel(morsel, live)
```

Which today re-derives column names and calls `.column(key).take(indices)`
per column. Phase 7's win is using `Morsel.take(indices)` directly — it
already returns the same shape. So `_sub_morsel` simplifies to:

```cython
cdef inline Morsel _sub_morsel(Morsel morsel, indices):
    return morsel.take(indices)
```

Verify the existing `Morsel.take` accepts an `array('i')` (int32) — the
CASE code constructs `live` with `_make_range_int32`. If it doesn't,
either update `Morsel.take` to accept memoryviews or add `take_rows`.

If `Morsel.take` is per-call overhead acceptable for CASE (it does a
column-loop and `Vector(nb_taken)` per column), no further work. If
not, add a new cpdef that wraps the loop in `nogil` and takes a typed
memoryview. Surface your choice in PR.

## Bind-time assembly resolution

Today `_assemble` (called from `_case_fn` in `build_case_fn`) does
runtime walk:

```cython
first = None
for p in parts:
    if p is not None:
        first = p
        break
if first is None:
    first = else_part
if first is None:
    return _draken_native.vector_null_from_length(n)

if isinstance(first, BoolVector):
    return assemble_bool(...)
first_type = getattr(first, "type", None)
if first_type in (_draken_native.VARCHAR, _draken_native.NVARCHAR):
    return assemble_flat_string(...)
return assemble_fixed(...)
```

At bind time, the binder knows the inferred type of every CASE THEN-
branch result and the ELSE result. They must all agree (or the binder
rejects the query). So we can resolve **once**:

```python
# Bind time, in _NT_CASE emit:
result_orso = first_non_none(
    [r.inferred_type for r in src.results] + [src.else_result.inferred_type if src.else_result else None]
)
if result_orso is OrsoTypes.BOOLEAN:
    assemble_kernel = assemble_bool
elif result_orso in (OrsoTypes.VARCHAR, OrsoTypes.BLOB):
    assemble_kernel = assemble_flat_string
else:
    assemble_kernel = assemble_fixed
```

Pass `assemble_kernel` to `build_case_fn`. The runtime closure calls it
directly:

```cython
def _case_fn(morsel):
    n = morsel.num_rows
    branch_id, rows_per_branch, unmatched, pos_in_branch = \
        _decide_compiled(cond_bcs, morsel)
    parts, else_part = \
        _compute_compiled(result_bcs, else_bc, morsel, rows_per_branch, unmatched)
    if not parts and else_part is None:
        return _draken_native.vector_null_from_length(n)
    return assemble_kernel(parts, else_part, branch_id, rows_per_branch, unmatched, pos_in_branch, n)
```

(Hand-wave above: the three `assemble_*` helpers have *slightly*
different signatures. Reconcile at bind time. The current `_assemble`
already does this — `assemble_flat_string` uses `pos_in_branch` while
`assemble_bool`/`assemble_fixed` use `rows_per_branch`/`unmatched`.
Either standardise their signatures, or build a small dispatching
closure per kernel that adapts.)

## Files (verify before editing)

- `draken/morsels/_morsel_shim.pyx` — `Morsel.take` at ~line 300.
  Confirm it accepts the index buffer shape CASE uses (`array('i')`).
  Add `take_rows` cpdef if you need a typed-memoryview variant.
- `opteryx/expression/evaluator/case_eval.pyx`:
  - `_sub_morsel` (~line 49) — simplify or delete.
  - `_decide_compiled` (~line 223) — replace `_sub_morsel(morsel, live)`
    with `morsel.take(live)` (or new helper).
  - `_compute_compiled` (~line 251) — same.
  - `_assemble` (~line 154) — split into the small `_assemble_with_kernel`
    or remove entirely.
  - `build_case_fn` (~line 279) — new params `assemble_kernel, n_handler_for_empty`.
    The `_case_fn` closure calls `assemble_kernel` directly.
- `opteryx/compiled/expression/compiled_expression.pyx`
  `_NT_CASE` emit (~line 634) — resolve `assemble_kernel` from the
  inferred result type. Pass to `build_case_fn`.
- `opteryx/compiled/vector_ops/case_helpers.pyx` — the assemble kernels
  themselves (`assemble_bool`, `assemble_fixed`, `assemble_flat_string`).
  No change in scope; verify their signatures so the bind-time
  resolver picks the right one.

## Verification

- `make c` clean. **Verify a fresh build compiles** before `make q`
  (Phase 4 lesson, repeated again here — do not skip).
- `make q` 100/100.
- Symbol checks:
  - `grep -n 'isinstance(first\|getattr(first' opteryx/expression/evaluator/case_eval.pyx`
    — should be zero matches (the `_assemble` runtime walk is gone).
  - `grep -n 'for n in names' opteryx/expression/evaluator/case_eval.pyx`
    — should be zero matches (per-column Python loop is gone).
- Spot tests:
  - `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets` (varchar output)
  - `SELECT CASE WHEN id < 5 THEN 1 ELSE 0 END FROM $planets` (integer / fixed output)
  - `SELECT CASE WHEN id < 5 THEN TRUE ELSE FALSE END FROM $planets` (bool output)
  - `SELECT CASE WHEN id IS NULL THEN 'null' WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets` (multi-branch)
  - `SELECT CASE WHEN id < 5 THEN id END FROM $planets` (no ELSE — exercises null-fill)
  - Nested: `SELECT CASE WHEN id < 5 THEN CAST(id AS VARCHAR) ELSE name END FROM $planets`
  - All Phase 1-6 regression queries still pass.
- Microbench: time a query like
  `SELECT CASE WHEN id < 5 THEN 'small' WHEN id < 7 THEN 'medium' ELSE 'big' END FROM testdata.astronauts`
  (full table, multi-branch + varchar). Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No new Python on the execute path.** The runtime CASE region must
  contain zero `getattr` / `isinstance` / `type(...).__name__` per
  morsel. (The `isinstance(legacy_result, Vector)` on BC_CASE's
  result wrap, kept by Phase 1, *can be deleted* after Phase 7 — the
  assemble kernels return nanobind Vectors deterministically. Surface
  in PR.)
- **Fail fast.** Bind-time output-type resolution fails loud if the
  THEN/ELSE branches don't agree on a type or if the type doesn't
  map to a known assemble kernel.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **Cython code must be typed.**
- **`make c` clean before claiming completion.** Caught Phase 4
  silently; check it here.
- **Do not commit.**

## Tests

- `make q` (137/137) with **fresh build**.
- All spot queries return correct results.
- Phase 1–6 regression queries still pass.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 1–6 tickets.
3. `opteryx/expression/evaluator/case_eval.pyx` end to end.
4. `opteryx/compiled/vector_ops/case_helpers.pyx` — the three
   assemble kernels and their signatures.
5. `draken/morsels/_morsel_shim.pyx` — focus on `take` (~line 300)
   and `select` / `from_vectors`.
6. `opteryx/compiled/expression/compiled_expression.pyx:630–660` —
   current `_NT_CASE` emit (post-Phase 1).
7. `opteryx/expression/evaluator/evaluation.pyx` BC_CASE executor
   (~line 2564 after Phase 6) — verify the flag set at bind time
   still matches what the executor reads.

## Definition of done

- `_sub_morsel` simplified to a one-line wrapper around the Morsel-level
  take helper, OR deleted (with `_decide_compiled` / `_compute_compiled`
  calling `morsel.take(indices)` directly).
- `_assemble` runtime walk removed; replaced by a typed dispatcher that
  calls the bind-time-resolved kernel.
- `build_case_fn` signature accepts the pre-resolved
  `assemble_kernel` (plus any context needed for the empty-parts
  fallback case).
- Bind-time `_NT_CASE` resolves the output type from
  `inferred_type` on the THEN/ELSE branches and selects the assemble
  kernel.
- `make c` clean; `make q` 100/100 with fresh build.
- Microbench numbers in PR description.

## Side-notes (carry forward in PR)

- Cleanup tickets still pending:
  - `tests/unit/expression/test_map_access_operator.py` imports
    deleted `MapAccessOp` (Phase 3).
  - `COUNT(*) FROM x WHERE …` returns `0` — pre-existing aggregate bug
    (Phase 2 finding). Needs own ticket.
- BC_CASE result-wrap gate: after Phase 7, the assemble kernels return
  nanobind Vectors consistently. The Phase-1 `isinstance` gate on the
  wrap (in BC_CASE executor) **can be deleted** — same pattern as
  Phase 5 did for BC_CAST. Confirm and do, or surface in PR.
- If during Phase 7 you find a CASE-branch result type that doesn't
  map to one of the three assemble kernels, **stop and surface**. The
  binder should have rejected such queries; if it didn't, that's a
  binder bug worth flagging.
- The legacy `evaluate_case` / `_decide` / `_compute` family stays
  (tree-walker, plan-time only). Phase 8 deletes them together with
  the rest of the tree-walker.
