# Ticket: Zero-Python Expression Engine — Phase 2 (`_is_null_as_boolvector` C rewrite)

> Part of the plan in `docs/zero_python_expression_engine.md`. **Read that
> plan first.** Phase 1 (result-wrap cleanup) landed; see
> `docs/tickets/zero-python-phase-1-result-wrap.md` for the precedent on
> bind-time flag patterns. This ticket is Phase 2.

## Problem

`SELECT … WHERE col IS NULL` (and `IS NOT NULL`) hits the bytecode
executor's BC_UNARY_OP opcode. The current path is:

```
BC_UNARY_OP (evaluation.pyx:2343)
  → _unary_op_kernel(op_code, py_left)   (evaluation.pyx:377)
     → _is_null_as_boolvector(vec)        (type_coercion.pyx:176)
        → _is_dictionary_encoded_vector(vec)   (type_coercion.pyx:36)
           → _dictionary_arrow_type(vec)        (type_coercion.pyx:29)
              → type(vec).__module__.startswith("draken.vectors.")
              → getattr(vec, "dictionary_value_type", None)
        → getattr(vec, "is_null_boolvector", None)
        → getattr(vec, "is_null_with_nan", None)
        → getattr(vec, "is_null", None)
        → isinstance(vec, _FIXED_BUFFER_VECTORS)
        → vec.null_bitmap()    # returns Python bytes (copies)
        → bool_vector_from_inverted_null_bitmap(bytes, n)  # nanobind kernel
           # reads bytes via Python buffer protocol; re-allocates output
```

Per IS NULL / IS NOT NULL evaluation, on every morsel:
- 1× `type(...).__module__.startswith(...)` (Python string op)
- 4× `getattr(...)` (Python attribute lookups walking through descriptor
  protocol)
- 1× `isinstance(...)` (cheap, C-level — but unnecessary)
- 1× `vec.null_bitmap()` (Python method call returning a fresh `bytes`
  object — copies the validity bitmap)
- 1× nanobind kernel call (re-allocates the output bitmap)

The dictionary-encoded branch is **dead code on modern draken** — modern
vectors are unified `Vector` instances with a `selection` array, never the
legacy per-class shims with `dictionary_value_type` etc.

IS NULL is in the inner predicate loop of many real queries; this dispatch
runs once per morsel per `IS NULL` predicate. It is squarely on the hot
path Phase 8 wants nogil — so every Python op here has to go.

## Goal

After Phase 2:
- `_is_null_as_boolvector` is deleted.
- BC_UNARY_OP for UOP_IS_NULL / UOP_IS_NOT_NULL calls a `cdef` kernel that
  reads `DrakenVector.validity` directly and constructs the output
  BoolVector via `bool_vector_from_bits` (which we already use in joins —
  see `opteryx/operators/nested_loop_join/nested_loop_join.pyx:69`).
- Zero `getattr` / `type(...).__module__` / Python method calls on the IS
  NULL / IS NOT NULL path.
- The legacy-dictionary helper chain is deleted in full.

## Scope

**In scope**
- `opteryx/expression/evaluator/type_coercion.pyx` — delete
  `_is_null_as_boolvector` (lines 176–208), `_is_dictionary_encoded_vector`
  (line 36), `_dictionary_arrow_type` (line 29), and the
  `_FIXED_BUFFER_VECTORS` constant (line 173). Remove the
  `bool_vector_from_int8_mask` / `bool_vector_from_inverted_null_bitmap`
  imports that no longer have callers.
- `opteryx/expression/evaluator/evaluation.pyx` —
  - Add a `cdef BoolVector _is_null_from_dv(DrakenVector* dv, bint negate)`
    helper.
  - Replace the `_is_null_as_boolvector(vec)` call inside `_unary_op_kernel`
    (lines 385–392) with the typed cdef helper.
  - Same change in `_unary_draken` (lines 340–347) — same tree-walker
    function pattern.
  - Verify nothing else still calls `_is_null_as_boolvector`. If something
    does (plan-time only — `evaluate_draken` / constant folding), update
    those call sites too or leave them; surface in the PR.

**Out of scope**
- BC_UNARY_OP's other op codes (IS TRUE / IS FALSE / IS EMPTY / IS NOT EMPTY /
  BITWISE_NOT). Their kernels also call `_nb_vec_unwrap` which is Phase 4
  territory.
- Annotating the executor `nogil` — Phase 8.
- Touching IS NULL handling on the plan-time tree-walker `_eval_value` /
  `evaluate_draken` paths *unless they share `_unary_draken`* (which they
  do — the same change covers both).

## The kernel to write

`_is_null_from_dv(DrakenVector* dv, bint negate)` produces a `BoolVector`
of length `dv.length`:

- `negate = 0` (IS NULL): output bit = 1 where the input row is null
  (validity bit = 0).
- `negate = 1` (IS NOT NULL): output bit = 1 where the input row is valid.

Cases on `dv.validity`:
1. **`dv.validity == NULL`** — all rows are valid.
   - IS NULL: output is all zeros (bitmap = `0x00…00`).
   - IS NOT NULL: output is all ones up to the logical row count, tail bits
     above `dv.length` masked to zero.
2. **`dv.validity != NULL`** — copy + maybe-invert.
   - Allocate `out = malloc((dv.length + 7) / 8)`.
   - Copy `dv.validity` into `out`.
   - If `negate == 0` (IS NULL): `out[k] = ~out[k]` for each byte.
   - Mask the tail: zero any bits beyond `dv.length & 7` in the final byte.

Construct the result with `bool_vector_from_bits(out, NULL, dv.length)`
(see `draken/vectors/bool_vector.pxd:65` for the signature; already
imported in `evaluation.pyx:59`). Pass `NULL` for `null_bitmap` because
the IS NULL result itself has no nulls — every row is either NULL or NOT
NULL, never unknown.

### Memory ownership

`bool_vector_from_bits` takes ownership of the `out` buffer — it is freed
when the BoolVector is collected. **Allocate with `malloc`**, not
`PyMem_Malloc` or stack — look at the existing call sites in
`evaluation.pyx` for the pattern (lines around 671–677 — the
`_bv_truth_test_native` helper does exactly this, including the tail
mask).

### Reference implementation

The closest existing template is `_bv_truth_test_native` in
`opteryx/expression/evaluator/evaluation.pyx` (~line 640). It allocates an
output bitmap, walks the input bitmap, handles validity, masks the tail,
and wraps via `bool_vector_from_bits`. Phase 2's kernel is simpler — no
truth-table dispatch, just copy + maybe-invert. Copy that structure.

## Wiring

`_unary_op_kernel` currently dispatches on op_code:

```cython
# evaluation.pyx:385–392
if op_code == UOP_IS_NULL:
    return _is_null_as_boolvector(vec)
if op_code == UOP_IS_NOT_NULL:
    is_null_bv = <BoolVector>_is_null_as_boolvector(vec)
    is_null_dv = is_null_bv.unified()
    nn_rows = is_null_dv.length
    nn_nbytes = (<Py_ssize_t>nn_rows + 7) >> 3
    return _bv_not_native(is_null_bv, nn_nbytes, nn_rows)
```

The IS NOT NULL branch builds IS NULL then bitwise-NOTs it — that's a
wasted second pass. After Phase 2, IS NOT NULL is one pass with `negate=1`:

```cython
# After:
cdef BoolVector _is_null_from_dv(DrakenVector* dv, bint negate) noexcept:
    ...

# In _unary_op_kernel:
if op_code == UOP_IS_NULL:
    return _is_null_from_dv((<Vector>vec)._dv, 0)
if op_code == UOP_IS_NOT_NULL:
    return _is_null_from_dv((<Vector>vec)._dv, 1)
```

Same pattern in `_unary_draken` for the tree-walker path. The `(<Vector>vec)._dv`
typed access is C-level — no Python.

`_dv` is `const DrakenVector*`. Cast away const when passing to the kernel
(the kernel only reads from `dv`, but C++ const propagation makes the
signature awkward; cast at the call site as we do elsewhere in this file).

## Verification

- `make c` clean compile.
- `make q` 100/100 (currently 137/137).
- `grep -nE 'getattr\(|type\(.*\)\.__module__' opteryx/expression/evaluator/type_coercion.pyx`
  — should return zero matches.
- `grep -rn '_is_null_as_boolvector\|_is_dictionary_encoded_vector\|_dictionary_arrow_type'`
  in `opteryx/` and `draken/` — should return zero matches (excluding
  this ticket and the deletion in your own diff).
- Spot tests (verify each returns the right answer, not just non-crash):
  - `SELECT name FROM $planets WHERE name IS NULL` — empty result
  - `SELECT name FROM $planets WHERE name IS NOT NULL` — all 9 planets
  - `SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL`
    — non-zero (astronauts who are alive)
  - `SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NOT NULL`
    — non-zero (astronauts who have died)
  - Verify the IS NULL and IS NOT NULL counts on `death_date` sum to
    `COUNT(*)` of `testdata.astronauts` (sanity).
- Microbench: time `SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL`
  before/after. Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No Python on hot path.** The new kernel is `cdef` only; no `object`
  parameters, no `getattr`, no `isinstance` inside it.
- **No fallbacks.** If the input isn't a Cython `Vector` with non-NULL
  `_dv`, fail loud with a `TypeError`. There is no "if it's an old-draken
  legacy dict vector, fall back to attribute walking" branch — that branch
  is the one we're deleting.
- **Fail fast.** If `(<Vector>vec)._dv == NULL`, raise immediately. This is
  a planner bug if it happens, not a runtime condition to handle.
- **Cython code must be typed.** Annotate the helper: `cdef BoolVector
  _is_null_from_dv(DrakenVector* dv, bint negate) noexcept:`. Locals are
  `cdef uint32_t n`, `cdef Py_ssize_t nbytes`, `cdef uint8_t* out`, etc.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **Memory**: allocate with `malloc`; on allocation failure raise
  `MemoryError` and free anything partially-allocated.
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/type_coercion.pyx` — delete sections
  noted above. Lines may have shifted; verify with
  `grep -n '_is_null_as_boolvector\|_is_dictionary_encoded_vector\|_dictionary_arrow_type'`.
- `opteryx/expression/evaluator/evaluation.pyx` —
  - Add `_is_null_from_dv` near `_bv_truth_test_native` (~line 640).
  - Update `_unary_op_kernel` (~line 377) — UOP_IS_NULL and UOP_IS_NOT_NULL
    branches.
  - Update `_unary_draken` (~line 340) — `IsNull` and `IsNotNull` string
    op branches. Note: `_unary_draken` is plan-time only (constant folding)
    — keeping it correct is required even though it's not hot path.
- `draken/vectors/bool_vector.pxd:65` — declaration of
  `bool_vector_from_bits`. Already imported in `evaluation.pyx:59`; no
  change needed, just confirm.

## Tests

Beyond the spot queries above:
- `make q` (137/137).
- Run any query that uses `IS NULL` in the corpus — they should all still
  pass.
- Edge cases (manual verification):
  - Vector with `validity == NULL` (all valid): IS NULL → all-false,
    IS NOT NULL → all-true.
  - Vector with all-null validity (bitmap all zeros): IS NULL → all-true,
    IS NOT NULL → all-false.
  - Vector of length 0: IS NULL → length-0 BoolVector (no crash on
    `malloc(0)`).
  - Vector of length 1, 7, 8, 9 (tail-bit edge cases): the byte beyond
    `length` must be zero.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md` cover-to-cover.
2. `docs/tickets/zero-python-phase-1-result-wrap.md` — Phase 1 establishes
   the bind-time-flag pattern. Phase 2 doesn't add new flags (the work is
   in a kernel, not the executor's dispatch table) — but the pattern of
   "delete the Python helper, replace with typed cdef" is the same.
3. `opteryx/expression/evaluator/evaluation.pyx` from ~line 600 to ~line
   700 — read `_bv_truth_test_native`, `_bv_not_native`,
   `_bv_any_native`, `_bv_all_native`. These are the templates: typed
   `cdef`, `malloc`, byte-loops, tail-mask, `bool_vector_from_bits`.
4. `opteryx/expression/evaluator/type_coercion.pyx` end to end — most of
   it is being deleted; understand what's there before swinging the axe.
5. `draken/vectors/bool_vector.pxd` — `bool_vector_from_bits` signature.
6. `draken/core/buffers.h` — `DrakenVector` struct (`length`, `validity`
   fields).

## Definition of done

- `_is_null_as_boolvector` deleted.
- `_is_dictionary_encoded_vector` deleted.
- `_dictionary_arrow_type` deleted.
- `_FIXED_BUFFER_VECTORS` constant deleted (if no other callers).
- `bool_vector_from_int8_mask` / `bool_vector_from_inverted_null_bitmap`
  imports removed from `type_coercion.pyx` (other modules may still
  import them — leave those imports alone).
- New `cdef BoolVector _is_null_from_dv(DrakenVector* dv, bint negate)`
  exists, called from `_unary_op_kernel` for both UOP_IS_NULL and
  UOP_IS_NOT_NULL.
- Same wiring in `_unary_draken` for tree-walker path.
- `grep` checks in §Verification return zero matches.
- All five spot queries return correct answers.
- `make q` 100/100.
- Microbench numbers in PR description.

## Notes on what comes next

Phase 3 (BC_EXTRACTION bind-time resolution) will need:
- `slot.left_type_code` carrying the operand's vector type, so we can
  bind-time-select the right native extraction kernel.

Phase 4 (BC_COMPARE string-op elimination) will need:
- All op_str → op_code resolution moved to bind time.
- `_unary_draken`'s string-op switch (`"IsTrue"`, `"IsFalse"`, etc.) is in
  the same family but lives only on the plan-time path; Phase 8 deletes
  it wholesale.

If during Phase 2 you spot something that obviously belongs in a later
phase, leave it. Note in the PR.
