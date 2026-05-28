# Ticket: Zero-Python Phase 9b — BytecodeInstr struct extension + bind-time wiring

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design).
> Implements Decision 2 (closure context) and Decision 5 (struct ABI
> break). **Depends on 9a** — the C kernel functions must exist before
> bind-time can point at them.

## Goal

Extend `BytecodeInstr` with two fields (`ctx_ptr`, `kernel_fn`) and
change the bind-time emitters for the five opcodes (BC_FUNCTION,
BC_EXTRACTION, BC_CAST, BC_CASE, BC_BINARY_OP) to:
1. Resolve the C kernel function pointer (from 9a's kernel table).
2. Allocate and populate the kernel's context struct.
3. Store both in the slot.

This ticket does **not** change the executor (9c). After 9b, the slots
carry both the old `callable_ref` (Python) AND the new `ctx_ptr` /
`kernel_fn` (C). The executor still uses `callable_ref`; 9c flips it.
That keeps 9b independently shippable and `make q`-green.

## Locked decisions implemented

- **Decision 2**: add `void* ctx_ptr` and `void* kernel_fn` to
  `BytecodeInstr`. Context lifetime ≥ `CompiledBytecode`, kept alive in
  `_held_refs`.
- **Decision 5**: struct grows; add an explicit `sizeof(BytecodeInstr)`
  assertion in a unit test; grep-verify no external `BytecodeInstr`
  consumer.

## Scope

**In scope**
- `opteryx/compiled/expression/compiled_expression.pxd`:
  - Add `void* ctx_ptr` and `void* kernel_fn` to the `BytecodeInstr`
    ctypedef struct.
- `opteryx/compiled/expression/compiled_expression.pyx`:
  - In each of the 5 opcode emitters (`_NT_FUNCTION`, `_NT_CAST`,
    `_NT_EXTRACTION_OPERATOR`, `_NT_CASE`, `_NT_BINARY_OPERATOR`):
    resolve the C kernel pointer, build the context struct, store
    `slot.kernel_fn` and `slot.ctx_ptr`. **Keep `slot.callable_ref`
    set as before** (executor still uses it until 9c).
  - Context structs must be heap-allocated and kept alive. Wrap each
    in a tiny Python object (or a `bytes`/`bytearray` backing buffer)
    appended to `bc._held_refs`. Surface the chosen keep-alive
    mechanism in the PR.
- A unit test asserting `sizeof(BytecodeInstr)` equals the new
  documented value, and that `ctx_ptr`/`kernel_fn` round-trip
  correctly for one opcode of each shape.
- Grep audit (Decision 5 / Question 5): confirm no code outside
  `opteryx/expression/evaluator/` and `opteryx/compiled/expression/`
  reads `BytecodeInstr` fields directly.

**Out of scope**
- Executor changes — 9c.
- Morsel nogil — 9d.
- Deleting `callable_ref` / resolver closures — 9f.

## How to get the C kernel pointer at bind time — USE THE REAL 9a API

9a (verified complete: 48/48 parity green) shipped a concrete registry.
**Do not invent a new resolution mechanism** — consume what exists:

```python
from draken.ops.kernels._kernel_registry import (
    lookup_kernel,            # name:str -> (fn_int, ctx_int|None)
    alloc_cast_timestamp_ctx, # unit:int -> ctx_int
    alloc_binary_op_ctx,      # op_code:int -> ctx_int
    alloc_extraction_ctx,     # sub_op_code:int -> ctx_int
    free_context,             # ctx_int -> None
)
```

- `lookup_kernel(name)` returns the C function pointer as an opaque
  Python int (and a ctx int or None). Store the int in `slot.kernel_fn`
  (cast `<void*>` ). Verified working:
  `lookup_kernel("draken_cast_int64_to_string")` → non-null.
- **Kernel names are the registry keys** (from `kernel_registry.cpp`):
  `"draken_add"`, `"draken_subtract"`, `"draken_multiply"`,
  `"draken_divide"`, `"draken_modulo"`, `"draken_bitwise_or"` (… and
  `_and/_xor/_shift_left/_shift_right`), `"draken_cast_<src>_to_<dst>"`
  (31 cast keys), `"draken_string_concat"`,
  `"draken_temporal_interval_op"`, `"draken_date_minus_date"`,
  `"draken_interval_interval_op"`, `"draken_ip_in_cidr"`,
  `"draken_map_access_string"`, `"draken_array_map_access"`,
  `"draken_json_extract"`, `"draken_pointer_extract"`. **Confirm the
  full list against `kernel_registry.cpp`'s map** — it's the source of
  truth.
- **The resolvers must produce these exact key strings.** `resolve_cast`
  / `resolve_binary_op` / the BC_EXTRACTION sub-op mapping currently
  return Python callables; 9b changes them to return the kernel **name
  string** (→ `lookup_kernel`) plus the context-allocator call. Align
  the key scheme exactly; a mismatch silently yields
  `lookup_kernel` → `(None, None)` and bind-time must fail-fast on that
  (raise; do not store a null `kernel_fn`).
- **Parameterised kernels**: call the matching `alloc_*_ctx(...)` to get
  the context int, store in `slot.ctx_ptr`. Keep it alive (see below)
  and ensure it's freed when the bytecode is collected (wire
  `free_context` into `CompiledBytecode.__dealloc__` or a held-ref
  finaliser — surface the chosen lifetime mechanism in the PR).

Note: the registry currently returns `ctx == None` for non-parameterised
kernels (`kernel_registry_lookup` sets `*out_ctx = nullptr`); the
context for parameterised kernels comes from the explicit
`alloc_*_ctx` calls, not from `lookup_kernel`. Verify this composition
against `_kernel_registry.pyx` before wiring.

For parameterised kernels (cast unit, binary op_code), allocate the
context struct, populate it, store the pointer in `slot.ctx_ptr`, and
keep the backing memory alive in `_held_refs`.

## Verification

- `make c` clean fresh build.
- `make q` 100/100 — behaviour unchanged (executor still uses
  `callable_ref`; the new fields are populated but unread).
- `sizeof(BytecodeInstr)` assertion test passes.
- Grep audit returns no external `BytecodeInstr` consumers (or
  documents any found).
- `make clickbench` non-regressing.

## Constraints (CLAUDE.md)

- **Fail fast** — bind-time raises if a kernel can't be resolved to a
  C pointer. No fallback to Python-only.
- **No `object` on the hot data layout** — `ctx_ptr`/`kernel_fn` are
  `void*`, not Python objects.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. Phase 9 design §Post-design.
2. 9a's ticket + its delivered kernel inventory table.
3. `compiled_expression.pxd` — current `BytecodeInstr` layout.
4. `compiled_expression.pyx` — the 5 emitters (`_NT_FUNCTION` ~480,
   `_NT_CAST` ~552, `_NT_EXTRACTION_OPERATOR` ~594, `_NT_CASE` ~730,
   `_NT_BINARY_OPERATOR` ~470). Verify line numbers.

## Definition of done

- `BytecodeInstr` has `ctx_ptr` + `kernel_fn`; `sizeof` assertion test
  green.
- All 5 emitters populate the new fields with valid C pointers +
  contexts; `callable_ref` still set.
- Context backing memory kept alive in `_held_refs`.
- Grep audit clean.
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.
