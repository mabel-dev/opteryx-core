# Ticket: Zero-Python Phase 9c — Executor C-pointer dispatch for the 5 opcodes

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design).
> Implements Decision 3 (hybrid signatures) and Decision 4 (VecResult
> into dv_stack). **Depends on 9a (C kernels) and 9b (slot carries
> ctx_ptr/kernel_fn).** This is the ticket that actually removes the 5
> `PyObject_Call` sites.

## BLOCKING carry-forward from 9a — validity merge

9a's parity test is green (48/48) but **parity only proves the C kernel
matches its nanobind binding — not SQL correctness.** The arithmetic
kernels still hardcode `result.validity = nullptr` with a
`// TODO: merge validity bitmaps from inputs`
(`draken/ops/kernels/binary_op_arithmetic.cpp:56,85,126` and likely the
other arithmetic/bitwise kernels). They **drop input nulls** — `NULL + 5`
would wrongly produce a non-null result. The parity test missed this
because the nanobind reference shares the bug.

Once 9c wires these kernels onto the executor path, this becomes
directly observable and testable. **9c MUST:**
- Implement validity merging in the affected kernels (output null where
  either input is null), OR confirm the DV fast path already covers the
  null case and these fallback kernels are genuinely unreachable for
  nullable inputs (prove it, don't assume).
- Add **value-checked** executor-level tests:
  - `SELECT NULL + 1` → `NULL`
  - `SELECT id + NULL FROM $planets` → all `NULL`
  - `SELECT a + b` where one column has nulls → null-propagated result
  - Same for `-`, `*`, `/`, `%`, bitwise ops, string concat.
- This is a correctness gate for 9c, not optional. A green parity test
  is not evidence of null-correctness.

## Goal

Rewrite the BC_FUNCTION, BC_EXTRACTION, BC_CAST, BC_BINARY_OP, and
BC_CASE executor branches in `evaluation.pyx` to call the C kernel
function pointer (`slot.kernel_fn`, set by 9b) instead of the Python
callable (`slot.callable_ref`). Store the returned `VecResult` directly
in `dv_stack[sp]`; no Python wrap until the executor's final return.

After 9c, the executor still holds the GIL (the `nogil` annotation is
9e). But there are zero `PyObject_Call` instances in these branches —
**except where `BC_INSTR_C_NATIVE` is not set** (see dispatch rule).

## Dispatch rule — branch on `BC_INSTR_C_NATIVE` (set by 9b)

9b (verified complete) populates `slot.kernel_fn` + `slot.ctx_ptr` and
sets the `BC_INSTR_C_NATIVE` (0x1000) flag **only when a C kernel was
resolved**. Critically, it does NOT set it for:
- **BC_FUNCTION** — function C kernels are carved out to **9a-fn**
  (not yet built). These slots keep `callable_ref` (Python path).
- Any cast/binary_op/extraction combo with no registry kernel (rare;
  9b fail-fasts on *supported* combos, so this only covers genuinely
  unsupported ones).

Therefore each of the 5 branches must dispatch on the flag:

```cython
if slot.flags & BC_INSTR_C_NATIVE:
    # C path — cast slot.kernel_fn to the opcode typedef, call, store VecResult
    ...
else:
    # Python fallback — existing callable_ref path (unchanged)
    legacy_result = (<object>slot.callable_ref)(...)
    ...
```

This is cleaner than the original plan (which assumed every slot goes
C-native): it lets BC_FUNCTION stay on Python until 9a-fn lands while
every other opcode goes C-native now. When 9a-fn registers function
kernels, BC_FUNCTION slots will get `BC_INSTR_C_NATIVE` set at bind
time and the same branch routes them to C — **no further 9c change
needed for functions.**

**Do not delete the `callable_ref` Python branches in 9c** — they're
the live path for BC_FUNCTION until 9a-fn. 9f deletes them, after
9a-fn makes them unreachable.

## Locked decisions implemented

- **Decision 3 (hybrid)**: 5 function-pointer typedefs, per-opcode cast:
  ```cython
  ctypedef VecResult (*extr_fn_t)(void* ctx, const DrakenVector* v, const DrakenVector* key) noexcept nogil
  ctypedef VecResult (*cast_fn_t)(void* ctx, const DrakenVector* v) noexcept nogil
  ctypedef VecResult (*binop_fn_t)(void* ctx, const DrakenVector* l, const DrakenVector* r) noexcept nogil
  ctypedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs) noexcept nogil
  ctypedef VecResult (*case_fn_t)(void* ctx, const Morsel* morsel) noexcept nogil
  ```
- **Decision 4**: `VecResult.dv` (or build a `DrakenVector*` from the
  `VecResult` fields via the arena) → `dv_stack[sp]`; `anchor[sp] = None`.
  Result wrapped to a Python Vector only at executor exit.

## Approach per opcode

For each of the 5 branches:
1. `dv_left_ptr = dv_stack[sp]` etc. (already done).
2. Cast `slot.kernel_fn` to the opcode's typedef.
3. Call it with `slot.ctx_ptr` + the `DrakenVector*` args + arena.
4. Take the returned `VecResult`, materialise into the executor arena
   as a `DrakenVector*`, store in `dv_stack[sp]`, `anchor[sp] = None`.
5. Check the error sentinel (9a's mechanism — `data == NULL` or an
   error code). On error, re-acquire GIL (if released) and raise.

**BC_CASE** is special: its C kernel re-enters the executor for sub-
morsels. The C `case_kernel` needs a nogil entry point
`execute_bytecode_c(bc, morsel)` — that's defined here (a `cdef nogil`
shell around the existing inner loop) and consumed by 9d's nogil
Morsel surface. If `execute_bytecode_c` can't be fully nogil until 9d
lands, BC_CASE may temporarily hold the GIL — note in PR, finalise in
9e.

## Error propagation

9a returns a sentinel `VecResult` on kernel failure. The executor
must detect it and raise a Python exception. Since 9c still holds the
GIL (nogil is 9e), raising is straightforward. When 9e makes the loop
nogil, the error check sets a flag and breaks to a GIL-reacquire
point that raises — design the error path now so 9e doesn't have to
restructure it.

## Scope

**In scope**
- `opteryx/expression/evaluator/evaluation.pyx` — the 5 opcode
  branches (~2370 BC_FUNCTION, ~2386 BC_BINARY_OP, ~2503 BC_EXTRACTION,
  ~2541 BC_CAST, ~2564 BC_CASE). Replace `PyObject_Call` with C
  function-pointer dispatch.
- New `cdef ... execute_bytecode_c(...)` nogil-capable entry point for
  BC_CASE re-entry (the body is the existing inner loop; the cpdef
  `execute_bytecode` becomes a thin GIL-holding shell around it).
- The 5 function-pointer typedefs.
- VecResult → arena DrakenVector* materialisation helper (if not
  already present).

**Out of scope**
- `nogil` annotation of the whole loop — 9e (but write the branches
  nogil-compatible: no Python ops inside them).
- Morsel nogil methods — 9d (BC_CASE's sub-morsel take depends on it).
- Deleting `callable_ref` and the resolver closures — 9f.

## Verification

- `make c` clean fresh build.
- `make q` 100/100.
- **Value-checked** spot tests across all five opcodes (not shape-only):
  - BC_FUNCTION: `SELECT LENGTH(name), LOWER(name) FROM $planets LIMIT 3`
  - BC_EXTRACTION: `SELECT missions[0] FROM testdata.astronauts LIMIT 3`
  - BC_CAST: `SELECT CAST(id AS VARCHAR), CAST(id AS DOUBLE) FROM $planets LIMIT 3`
  - BC_BINARY_OP: `SELECT id + 1, id * 2, name || '!' FROM $planets LIMIT 3`
  - BC_CASE: `SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4`
  - Chained: `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3`
- `grep -n 'callable_ref' evaluation.pyx` — appears only in the
  **BC_FUNCTION Python-fallback branch** (live until 9a-fn) and any
  unsupported-combo fallback. CAST/BINARY_OP/EXTRACTION must have **no**
  live `callable_ref` call. Surface the remaining references in PR.
- `make clickbench` — should **improve** (per-morsel PyObject_Call
  eliminated on the cast/binary-op/extraction paths; function path
  improves later with 9a-fn). Report deltas.

### Direct C-native proof (the gate 9b's test lacked)

9b's verification test only asserted "query returns rows," which is
true whether a slot is C-native or silently Python (9b was
behaviour-neutral). 9c makes the C path **observable**, so close the
gap here: add a test asserting the C path actually executed, e.g. a
per-opcode telemetry counter (`c_native_kernel_calls`) incremented in
the C-dispatch branch, asserted non-zero after a cast/binary-op/
extraction query. This guarantees the slot didn't fall back to Python
unnoticed. Without it, a future regression that clears
`BC_INSTR_C_NATIVE` would silently revert to the (still-present) Python
branch and the value-checked tests would stay green. Surface the
counter mechanism in the PR.

## Constraints (CLAUDE.md)

- **No Python in the 5 branches.** After 9c, `grep` for `PyObject_Call`
  / `(<object>...)(` in these branches returns zero.
- **Fail fast** — kernel error sentinel → raise. No silent NULL result.
- **Cython typed** — function-pointer casts are explicit.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. Phase 9 design §Post-design (Decisions 3, 4, 5).
2. 9a ticket + kernel inventory.
3. 9b ticket — how `ctx_ptr`/`kernel_fn` are populated.
4. `evaluation.pyx` — the 5 branches + `_slot_to_pyobj` +
   the existing arena materialisation (`draken_frame_arena_*`).
5. `draken/ops/vec_result.h` — the return struct.

## Definition of done

- All 5 branches call the C kernel via `slot.kernel_fn`; zero
  `PyObject_Call` in them.
- `VecResult` stored directly in `dv_stack`; `anchor[sp] = None`.
- `execute_bytecode_c` nogil-capable re-entry exists for BC_CASE.
- Error sentinel handled (raises at GIL boundary).
- Value-checked spot tests pass for all 5 opcodes + chained.
- `make c` clean; `make q` 100/100; `make clickbench` improved (report).
