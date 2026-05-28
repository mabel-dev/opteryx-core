# Ticket: Zero-Python Expression Engine — Phase 1 (result-wrap cleanup)

> Part of the plan in `docs/zero_python_expression_engine.md`. **Read that
> plan first** — it sets the boundary (Python at plan time, zero Python at
> execute time) and the eight phases. This ticket is Phase 1.

## Problem

The bytecode executor in `opteryx/expression/evaluator/evaluation.pyx`
(function `execute_bytecode`) is the per-morsel hot path. Today, for every
opcode that produces a Vector result, it does Python protocol work to figure
out *what kind* of vector came back and how to wrap it:

- **BC_COMPARE** falls into a Python-keyed unwrap/wrap pattern. The kernel
  helpers `draken_compare_int` / `draken_compare` in
  `opteryx/expression/evaluator/comparisons.pyx` call `_nb_vec_unwrap(v)`
  (which does `getattr(v, "_nb", None)`) on every operand and
  `_wrap_nb_bool_result(result)` (which does `isinstance(result, BoolVector)`)
  on every result. Both are per-fallback-comparison cost.
- **BC_FUNCTION** does `type(legacy_result).__name__ == "Vector"` per call
  to decide whether to wrap a nanobind Vector, followed by
  `if legacy_result.type == _draken_native.DrakenType.BOOL` to choose
  BoolVector vs Vector, then a final `isinstance(legacy_result, Vector)` to
  decide whether to extract `_dv`. Three Python operations per function
  call.
- **BC_CAST / BC_CASE / BC_EXTRACTION** each end with
  `if isinstance(legacy_result, Vector): dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv else: dv_stack[sp] = NULL`.
  The isinstance is per morsel; the kernel return type is in fact known
  at bind time, so this dispatch is wasted work.

Every one of these is per-morsel runtime Python on the path that Phase 8
will eventually run `nogil`. They have to go before the executor can be
annotated `nogil`.

## Goal

After Phase 1: zero `getattr` / `isinstance` / `type(x).__name__` calls in
the result-handling regions of BC_COMPARE, BC_FUNCTION, BC_CAST, BC_CASE,
BC_EXTRACTION. Result-type dispatch is moved to bind time, encoded as bit
flags in the `BytecodeInstr` slot.

## Scope

**In scope**
- `opteryx/expression/evaluator/comparisons.pyx` — remove `_nb_vec_unwrap`
  and `_wrap_nb_bool_result`. Replace the call sites with typed `cdef` casts
  and direct `BoolVector(...)` construction.
- `opteryx/expression/evaluator/evaluation.pyx` — for BC_FUNCTION,
  BC_CAST, BC_CASE, BC_EXTRACTION, replace runtime type checks with
  flag-bit reads from `slot.flags`.
- `opteryx/compiled/expression/compiled_expression.pyx` and the matching
  `.pxd` — define new bit flags, set them at bind time when emitting the
  corresponding opcode.

**Out of scope (separate phases)**
- The kernel call itself (`PyObject_Call` overhead) — Phase 6 / Phase 8.
- BC_COMPARE's string-op fallback (`draken_compare()` keyed by op string)
  — Phase 4.
- BC_EXTRACTION wrapper deletion (`MapAccessOp` / `ArrowOp` / `LongArrowOp`)
  — Phase 3.
- BC_CAST closure specialisation — Phase 5.
- `_is_null_as_boolvector` rewrite — Phase 2.
- Annotating the executor as `nogil` — Phase 8.

Stay strictly inside scope. If a fix you find is in scope for a later
phase, leave it — note it in the PR description, don't take it on.

## The model to copy

The BC_FUNCTION executor at `evaluation.pyx:2357–2437` already encodes
`is_nb_callable` at bind time in `slot.bool_value` and uses
`(<Vector>_slot_to_pyobj(...))._nb` — a typed Cython cast — to unwrap
without `getattr`. That is the pattern. Every Phase 1 change follows it.

## Approach

### Step 1 — Add result-wrap flag bits

In `opteryx/compiled/expression/compiled_expression.pxd`, add to the
existing flag-bit DEFs (look for `BC_CMP_INLIST_INLINE` etc.):

```cython
# Result-handling flags (read by execute_bytecode after kernel return):
DEF BC_RESULT_NEEDS_NB_WRAP = 0x10  # result is a raw nanobind Vector → wrap in shim
DEF BC_RESULT_WRAP_AS_BOOL  = 0x20  # wrap as BoolVector (else Vector); valid only with NEEDS_NB_WRAP
DEF BC_RESULT_NO_DV         = 0x40  # result has no DV* (constant / scalar / not a vector) → store NULL in dv_stack
```

Exact bit positions: verify nothing else uses them. Bits 0x01–0x08 are
already used by `BC_CMP_*` flags in BC_COMPARE; pick non-overlapping bits.

### Step 2 — Bind time: set the flags

For each opcode emit site in `compiled_expression.pyx`, set the flags
based on what the kernel is known to return:

- **NT_FUNCTION** (BC_FUNCTION): if the kernel is an nb_func (already
  detected; sets `slot.bool_value`), set `BC_RESULT_NEEDS_NB_WRAP`. If the
  kernel's return type is `DrakenType.BOOL` (read from the bound function's
  signature metadata — `func_ref_meta.selected_overload.kernel.return_type`
  or similar; verify the exact attribute name), also set
  `BC_RESULT_WRAP_AS_BOOL`. Non-nb_func kernels return Cython Vectors
  already; clear both bits.
- **NT_CAST** (BC_CAST), **NT_EXTRACTION_OPERATOR** (BC_EXTRACTION),
  **NT_CASE** (BC_CASE): today's wrappers return nanobind Vectors. Until
  Phases 3 / 5 replace them, set `BC_RESULT_NEEDS_NB_WRAP`. The bool-ness
  is determined by the target type (CAST), kernel choice (EXTRACTION), or
  THEN-branch result type (CASE). Resolve at bind time; set
  `BC_RESULT_WRAP_AS_BOOL` accordingly.

If you can't determine bool-ness at bind time for a given opcode emit
site, **stop and surface that to the architect** — do not fall back to a
runtime check.

### Step 3 — Runtime: replace Python checks with flag reads

For each opcode's result-handling region in `evaluation.pyx`, replace the
isinstance / getattr / type-name dispatch with reads from `slot.flags`:

**BC_FUNCTION** — replace lines around 2425–2435:

```cython
# Before:
if is_nb_callable and type(legacy_result).__name__ == "Vector":
    if legacy_result.type == _draken_native.DrakenType.BOOL:
        legacy_result = BoolVector(legacy_result)
    else:
        legacy_result = Vector(legacy_result)
anchor[sp] = legacy_result
if isinstance(legacy_result, Vector):
    dv_stack[sp] = (<Vector>legacy_result).unified()
else:
    dv_stack[sp] = NULL

# After:
if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
    if slot.flags & BC_RESULT_WRAP_AS_BOOL:
        legacy_result = BoolVector(legacy_result)
    else:
        legacy_result = Vector(legacy_result)
anchor[sp] = legacy_result
if slot.flags & BC_RESULT_NO_DV:
    dv_stack[sp] = NULL
else:
    dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
```

**BC_CAST / BC_CASE / BC_EXTRACTION** — same pattern. The existing
`isinstance(legacy_result, Vector)` ternary is replaced with the
`BC_RESULT_NO_DV` flag check. The eager wrap (if needed) moves up to where
`anchor[sp]` is set.

### Step 4 — comparisons.pyx cleanup

In `opteryx/expression/evaluator/comparisons.pyx`:

- Delete `_nb_vec_unwrap` (line 40). At every call site, the operand is
  either a Cython `Vector`/`BoolVector` (from the executor's stack
  anchor) or a Python scalar (BC_LOAD_LIT). Replace
  `_nb_vec_unwrap(left)` with a typed inline check:

  ```cython
  cdef object _left_nb
  if isinstance(left, Vector):
      _left_nb = (<Vector>left)._nb
  else:
      _left_nb = left   # scalar / literal
  ```

  This still has one `isinstance`, but it's necessary because BC_LOAD_LIT
  legitimately produces scalars. **Do not** try to also remove this — the
  scalar-vs-vector flag for compare operands is Phase 4 territory.

  Note: if profiling shows this `isinstance` is hot, escalate to the
  architect. The expected win is removing the `getattr` (which is a
  Python attribute lookup with descriptor protocol), not the `isinstance`
  (which is a C-level type check via `Py_TYPE` comparison).

- Delete `_wrap_nb_bool_result` (line 46). Both call paths know they
  receive a nanobind BOOL vector — wrap unconditionally with
  `BoolVector(result)`. If a call site has uncertainty (verify each one),
  fail-fast with a `TypeError` rather than guess.

### Step 5 — verify and finish

- `make c` clean compile.
- `make q` 100/100.
- `grep -nE 'getattr\(|isinstance\(|__name__' opteryx/expression/evaluator/evaluation.pyx`
  — the only matches in BC_COMPARE / BC_FUNCTION / BC_CAST / BC_CASE /
  BC_EXTRACTION result-handling regions should be **zero**. (Other
  regions — bind-time helpers, error formatters — are fine.)
- `grep -nE 'getattr\(|isinstance\(' opteryx/expression/evaluator/comparisons.pyx`
  — the only remaining `isinstance` should be the scalar-vs-Vector one in
  the unwrap region (acknowledged above).
- Microbench: pick one TPC-H-ish query that hits each affected opcode,
  time it before/after. Numbers in the PR description.

## Constraints (from CLAUDE.md)

- **Correctness is non-negotiable.** A flag mis-set at bind time
  silently corrupts the result type — every emit site must be reviewed
  individually, not pattern-matched.
- **Fail fast.** If a kernel returns an unexpected type at runtime, do not
  add a "compatibility wrap" — raise. The bind-time flags are the source
  of truth.
- **No fallbacks.** Do not leave the old isinstance check "as a safety
  net". Delete it.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **No `object` parameters** on the hot path — typed Cython only.
- **Cython code must be typed.** New `cdef` locals where needed.
- **No new Python on the execute path.** Phase 1 *removes* Python — adding
  any (e.g. a new `getattr` for some edge case) is a hard reject.
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/evaluation.pyx` — BC_FUNCTION at ~line
  2357, BC_EXTRACTION at ~line 2432, BC_CAST at ~line 2457, BC_CASE at
  ~line 2479. Verify with `grep -n 'BC_FUNCTION\|BC_EXTRACTION\|BC_CAST\|BC_CASE'`.
- `opteryx/expression/evaluator/comparisons.pyx` — `_nb_vec_unwrap` at
  line 40, `_wrap_nb_bool_result` at line 46. Call sites at lines 98,
  133, 182, 220, 265–311 (verify).
- `opteryx/compiled/expression/compiled_expression.pyx` — `_NT_FUNCTION`
  emit (~line 480–515), `_NT_CAST` emit (~line 522), `_NT_EXTRACTION_OPERATOR`
  emit (~line 566), `_NT_CASE` emit (~line 604). Verify with `grep -n '_NT_'`.
- `opteryx/compiled/expression/compiled_expression.pxd` — flag DEFs and
  `BytecodeInstr` struct. The `flags` field is already `int` (32 bits) —
  no struct change needed; just pick free bits.

## Tests

- `make q` must pass (137/137 at time of writing).
- Spot tests for each opcode:
  - **BC_COMPARE**: `SELECT * FROM testdata.planets WHERE name LIKE 'M%'`
    (triggers the Python fallback path that uses comparisons.pyx)
  - **BC_FUNCTION**: `SELECT LENGTH(name) FROM testdata.planets` (calls
    an nb_func kernel returning Integer64); `SELECT LOWER(name) FROM testdata.planets`
    (returns Varchar); `SELECT name LIKE 'M%' FROM testdata.planets`
    (returns Bool — exercises `BC_RESULT_WRAP_AS_BOOL`).
  - **BC_CAST**: `SELECT CAST(missions AS VARCHAR) FROM testdata.astronauts LIMIT 5`
    (the query that crashed in the precursor work — must still work).
  - **BC_EXTRACTION**: `SELECT missions[0] FROM testdata.astronauts LIMIT 5`
    (array map-access); a `->` JSON query if you can find or construct
    a JSON column in test data.
  - **BC_CASE**: `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM testdata.planets`
- Anything that previously hit the isinstance path and got `dv_stack[sp] = NULL`
  must continue to produce identical results — verify by diffing a
  ClickBench sample with the new build.

## Definition of done

- Three new flag bits defined: `BC_RESULT_NEEDS_NB_WRAP`,
  `BC_RESULT_WRAP_AS_BOOL`, `BC_RESULT_NO_DV`.
- BC_FUNCTION, BC_CAST, BC_CASE, BC_EXTRACTION result handling reads
  `slot.flags`, not `type(x)` / `isinstance` / `getattr`.
- `_nb_vec_unwrap` deleted from `comparisons.pyx`.
- `_wrap_nb_bool_result` deleted from `comparisons.pyx`.
- `grep` in step 5 of the approach returns no offending matches.
- `make q` 100/100.
- Microbench numbers in PR description (one query per affected opcode).
- PR description notes any in-scope-for-a-later-phase items you spotted
  but did not take on.

## Pre-flight reading

Before writing any code:

1. Read `docs/zero_python_expression_engine.md` cover-to-cover. The
   eight-phase plan and the boundary definition are the contract.
2. Read `opteryx/expression/evaluator/evaluation.pyx` from line 1750
   (top of the bytecode-executor section) to the end. Get the executor's
   shape in your head before changing any single opcode.
3. Read `opteryx/expression/evaluator/comparisons.pyx` end to end.
4. Read `opteryx/compiled/expression/compiled_expression.pyx` from line
   480 (NT_FUNCTION emit) to ~line 625 (NT_CASE emit).
5. Skim `opteryx/compiled/expression/compiled_expression.pxd` for the
   struct layout and existing flag DEFs.

If anything in the plan or this ticket contradicts the source code,
**stop and surface it**. The source wins; the doc gets updated.
