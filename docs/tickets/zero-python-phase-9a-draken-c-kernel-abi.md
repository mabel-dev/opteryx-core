# Ticket: Zero-Python Phase 9a — Draken parallel C kernel ABI

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md`,
> §Post-design). **Read the locked decisions first** — this ticket
> implements Decision 1 (parallel C ABI) and Decision 3 (hybrid
> signatures). 9a is the foundation; 9b/9c depend on its output.

## Goal

Every built-in kernel that the bytecode executor calls (today via
`PyObject_Call` to a nanobind binding) gains an `extern "C"` C function
with a fixed signature, registered so the executor can call it with no
Python. The nanobind Python binding is **retained** as a thin wrapper
(Decision 1a) so tests, REPL, and external callers keep working.

This ticket produces **only the draken-side C ABI** — the headers,
implementations, and a C-level ABI test. It does NOT touch the opteryx
executor (that's 9c) or the `BytecodeInstr` struct (that's 9b).

## Locked decisions this ticket implements

- **Decision 1a**: each kernel has both an `extern "C"` C function and a
  nanobind binding (3-line forwarding wrapper).
- **Decision 3 (hybrid signatures)**: the C functions use these exact
  signatures (the executor in 9c will cast function pointers to these):
  - BC_EXTRACTION: `VecResult (*)(void* ctx, const DrakenVector* vector, const DrakenVector* key)`
  - BC_CAST: `VecResult (*)(void* ctx, const DrakenVector* vector)`
  - BC_BINARY_OP: `VecResult (*)(void* ctx, const DrakenVector* left, const DrakenVector* right)`
  - BC_FUNCTION: `VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)`
  - (BC_CASE's kernel lives in opteryx, not draken — out of scope for 9a.)
- **Decision 4**: kernels return `VecResult` (already defined in
  `draken/ops/vec_result.h`). No Python wrapping.

## The kernels in scope

Enumerate every native kernel the five opcodes resolve to today. From
the Phase 9 design's call-site table, the draken-side kernels are:

**BC_EXTRACTION** (4 sub-ops, already native nanobind):
- `vector_map_access_string` — `opteryx/compiled/nanobind/vector_special.cpp`
- `vector_array_map_access` — `draken/draken_native.cpp` (added in the
  earlier MapAccess work)
- `vector_json_extract` — `opteryx/compiled/nanobind/vector_json.cpp`
  (used by both Arrow `->` and LongArrow `->>`)

**BC_CAST** — the kernels `resolve_cast` returns:
- `vector_cast_int64_to_string`, `vector_cast_bool_to_string`,
  `vector_cast_date_to_string`, `vector_cast_timestamp_to_string`,
  `vector_cast_float64_to_string`, `vector_cast_int64_to_float64`,
  `vector_cast_integer_to_float64`, `vector_cast_bool_to_float64`,
  `vector_cast_string_to_float64`, `vector_cast_*_to_int`,
  `vector_cast_*_to_bool`, `vector_cast_int64_to_timestamp`,
  `vector_date32_to_timestamp`, `vector_timestamp_to_date32`.
  (Full list: enumerate from `opteryx/expression/casts.pyx:resolve_cast`
  and `opteryx/compiled/nanobind/vector_casts.cpp`.)

**BC_BINARY_OP** — arithmetic/bitwise/concat kernels:
- The `vec.add/sub/mul/div/mod` methods (currently invoked via getattr
  in `resolve_binary_op`'s `_build_arithmetic_closure`). These are
  draken Vector methods — need C entry points.
- `vector_bitwise_or/and/xor/shift_left/shift_right` —
  `opteryx/compiled/nanobind/vector_bitwise.cpp`.
- `vector_concat` — `opteryx/compiled/nanobind/vector_selection_concat`.
- `vector_ip_in_cidr` — `opteryx/compiled/nanobind/vector_misc`.

**BC_FUNCTION** — every built-in SQL function kernel. **This is the
largest set.** Enumerate from `opteryx/expression/functions/`. Many
are nanobind (`nb_func`), some are cpdef.

**First action**: produce the complete enumerated list as the first
deliverable of this ticket (a markdown table: kernel name, current
location, signature category). Surface it in the PR before writing the
C functions, so the architect can confirm completeness.

## Approach

### Step 1 — Enumerate (deliverable, surface in PR)

Grep `resolve_cast`, `resolve_binary_op`, the BC_EXTRACTION resolution
table, and the function-kernel registry. Produce the kernel inventory
table. Flag any kernel that:
- Is a Python closure with no underlying C++ function (these need a C
  function written from scratch, not just a forwarding wrapper).
- Has C++-exception-throwing behaviour (Risk 1 — needs out-of-band
  error propagation; see §Errors).

### Step 2 — C ABI header convention

Per Decision 1a, place C ABI headers under `draken/ops/kernels/`
(new directory). Each kernel family gets a header:

```c
// draken/ops/kernels/cast_kernels.h
#pragma once
#include "ops/vec_result.h"
#include "core/buffers.h"

#ifdef __cplusplus
extern "C" {
#endif

// 1-arg cast: vector → vector. ctx unused for plain casts; non-NULL for
// parameterised casts (e.g. timestamp unit) — see cast_ctx below.
VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* v);
VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* v);
// ... one per cast pair

#ifdef __cplusplus
}
#endif
```

The existing nanobind kernel bodies move into the `extern "C"`
functions (or the C function wraps the existing C++ body). The
nanobind binding becomes:

```cpp
m.def("vector_cast_int64_to_string", [](nb::object v) -> nb::object {
    VecResult r = draken_cast_int64_to_string(nullptr, unwrap(v));
    return wrap(vecresult_to_owner(r));
});
```

### Step 3 — Context structs for parameterised kernels

Decision 2 (closure context) is implemented in 9b/9c, but 9a must
**define the context struct types** the kernels expect. Examples:

```c
// cast with a timestamp unit
struct cast_ctx {
    int unit;   // 0=none, 1=ns, 2=us, 3=ms, 4=s, 5=days
};

// binary op needs the op_code to pick add/sub/mul/...
struct binary_op_ctx {
    int op_code;  // BOP_PLUS .. BOP_SHIFT_RIGHT
};
```

For kernels with no state (most casts, all bitwise), `ctx` is unused —
pass `NULL`. Document per-kernel whether `ctx` is read.

### Step 4 — Arithmetic kernels (the getattr-closure case)

`resolve_binary_op`'s `_build_arithmetic_closure` currently does
`getattr(left_nb, "add")(right_nb)` — calling draken Vector's `add` /
`sub` / `mul` / `div` / `mod` methods. These methods are backed by
C++ in draken. Expose them as:

```c
// draken/ops/kernels/arith_kernels.h
VecResult draken_binary_arith(void* ctx, const DrakenVector* l, const DrakenVector* r);
// ctx → binary_op_ctx{op_code}; dispatches to the right arith op internally.
```

The implementation reads `ctx->op_code`, dispatches to draken's
existing `draken_arithmetic` (the C++ op-table entry point used by the
DV fast path). **The DV fast path already proves these are C-callable**
— `draken_arithmetic_dv` in the executor calls them. 9a's job is to
expose a uniform `(ctx, left, right) → VecResult` wrapper around that
existing machinery for the *fallback* path (mixed types, DECIMAL, etc.
that the DV fast path rejects).

### Step 5 — Errors (Risk 1)

C++ kernels can throw. The C ABI must not let exceptions cross the
`extern "C"` boundary. Two options, pick per kernel:
- Wrap the body in `try { … } catch (const std::exception& e) { … }`
  and return a `VecResult` with a sentinel (e.g. `type = DRAKEN_NULL`,
  `data = nullptr`) plus set a thread-local error string the caller
  reads.
- Add an `error_code` out-param: `VecResult fn(void* ctx, …, int* err)`.

**Recommendation**: thread-local error slot + `VecResult` with
`data == nullptr` sentinel, matching how the DV fast path arena
signals failure. The executor (9c) checks for the sentinel and
raises a Python exception at the GIL boundary. Confirm the exact
mechanism with the architect if the DV fast path's pattern doesn't
generalise.

### Step 6 — ABI test

A C/C++ unit test (gtest/doctest or a standalone `main`) that:
- Calls each C kernel directly (no Python) with hand-built
  `DrakenVector`s.
- Asserts the `VecResult` matches the nanobind binding's output for the
  same input (parity).
- Exercises the error path (a kernel that throws → sentinel
  `VecResult`, no crash).

## Scope

**In scope**
- New `draken/ops/kernels/` directory with C ABI headers + impls (or
  refactor existing nanobind `.cpp` files to expose `extern "C"`
  functions + thin bindings).
- Context struct type definitions (`cast_ctx`, `binary_op_ctx`, etc.).
- The C-level ABI parity test.
- The kernel inventory table (Step 1 deliverable).
- `setup.py` updates if new source files are added.

**Out of scope**
- `BytecodeInstr` struct changes — 9b.
- Executor rewrites — 9c.
- BC_CASE kernel (lives in opteryx) — 9c.
- Morsel nogil surface — 9d.
- Deleting the resolver closures — 9f.

## Verification

- `make c` clean fresh build.
- `make q` 100/100 (9a changes draken internals + adds C functions;
  the nanobind bindings still forward, so behaviour is unchanged).
- The new C ABI test passes (parity C-function vs nanobind binding
  for every kernel).
- `make clickbench` non-regressing (9a alone doesn't change the
  executor path; this is a sanity check that the nanobind forwarding
  wrappers didn't add overhead).

## Constraints (from CLAUDE.md)

- **No `object` in compiled paths**, **no upward `import opteryx` from
  draken core** (§2/§3) — the C kernels are draken-internal.
- **Fail fast** — error path returns a clear sentinel; no silent
  degradation.
- **PyArrow / NumPy banned** in draken.
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Pre-flight reading

1. `docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design.
2. `draken/ops/vec_result.h` — the `VecResult` struct (the return type).
3. `draken/ops/hash.h` — how the DV fast path calls C++ kernels
   (`draken_arithmetic`, `draken_compare`); the op-table pattern.
4. `opteryx/compiled/nanobind/vector_casts.cpp`,
   `vector_bitwise.cpp`, `vector_json.cpp`, `vector_special.cpp` —
   the kernels being given C ABIs.
5. `opteryx/expression/casts.pyx:resolve_cast` and
   `opteryx/expression/evaluator/arithmetic.pyx:resolve_binary_op` —
   the resolvers that currently return Python callables; their kernel
   choices define the C ABI surface.
6. `draken/draken_native.cpp` `vector_array_map_access` — an existing
   C++ kernel to model the `extern "C"` extraction on.

## Definition of done

- Kernel inventory table produced and in PR (Step 1).
- Every in-scope kernel has an `extern "C"` C function with the
  Decision-3 signature for its opcode category.
- Context struct types defined for parameterised kernels.
- Nanobind bindings reduced to thin forwarding wrappers; their Python
  surface is unchanged.
- Error path returns a sentinel `VecResult` (no exception crosses the
  C boundary).
- C ABI parity test passes.
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.

## Notes for 9b/9c

- 9b extends `BytecodeInstr` with `ctx_ptr` + `kernel_fn` and sets them
  at bind time, pointing `kernel_fn` at the C functions this ticket
  creates.
- 9c rewrites the executor to cast `kernel_fn` to the Decision-3
  signature and call it. The signatures defined here are the contract.
- If during 9a you discover a kernel that genuinely can't be made
  C-callable (e.g. it depends on Python-level state with no C
  equivalent), **stop and surface** — it may force a UDF-policy
  discussion or a kernel rewrite that's out of 9a's scope.
