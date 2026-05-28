# Ticket: Zero-Python Expression Engine — Phase 9 (C function-pointer kernel ABI — DESIGN)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–8c retired
> every per-morsel `PyObject_Call` *except five*: the kernel invocation
> in BC_FUNCTION, BC_EXTRACTION, BC_CAST, BC_CASE, BC_BINARY_OP fallbacks.
>
> **This is a DESIGN ticket, not an implementation ticket.** It surfaces
> the architectural choices that need to land before any code does. The
> implementation is broken out into follow-up tickets once the design
> is locked.

## Goal

Replace the 5 remaining per-morsel `PyObject_Call` sites with C function-
pointer dispatch. Once done, annotate `execute_bytecode` as `nogil` end-
to-end. The expression engine then runs with **zero Python at execute
time** — the architect's stated goal at the start of this train.

## The 5 call sites (post-Phase-8c)

| Opcode         | File:Line                          | Current call                                               | Kernel origin                                                                          |
|----------------|-----------------------------------:|------------------------------------------------------------|----------------------------------------------------------------------------------------|
| BC_FUNCTION    | `evaluation.pyx:~2370` (one of 4 arity branches) | `callable_obj(*args)`                          | nb_func from `opteryx/compiled/nanobind/*.cpp` OR cpdef from `opteryx/expression/functions/implementations/*.pyx` |
| BC_EXTRACTION  | `evaluation.pyx:2515-2522` (4 sub-ops) | `(<object>slot.callable_ref)(py_left_nb, …)`         | 4 native nanobind kernels (`vector_map_access_string`, `vector_array_map_access`, `vector_json_extract`) |
| BC_CAST        | `evaluation.pyx:~2544`             | `(<object>slot.callable_ref)(py_left)`                     | resolved by `resolve_cast` — mix of nanobind, cpdef, and closures bound to args        |
| BC_CASE        | `evaluation.pyx:~2565`             | `(<object>slot.callable_ref)(morsel)`                      | the `_case_fn` Python closure capturing `(cond_bcs, result_bcs, else_bc, kernel_type)` |
| BC_BINARY_OP   | `evaluation.pyx:~2386`             | `(<object>slot.callable_ref)(left, right)` (fallback path) | resolved by `resolve_binary_op` — closures over op_code + native kernel                 |

The DV fast paths (`draken_compare_dv`, `draken_arithmetic_dv`) are
already C-callable. They stay unchanged.

## The hard parts

### 1. Nanobind kernels are not C function pointers

nanobind wraps C++ functions in Python callables. There's no public
nanobind API to recover the C++ function pointer. Three options:

- **(a) Parallel C ABI for built-in kernels.** Each kernel has both
  its nanobind Python binding (kept for tests, REPL, external
  consumers) AND an `extern "C"` C function with a fixed signature
  registered in a C table. Most of the work; cleanest result.
- **(b) Cython cpdef wrappers around nanobind.** A cpdef function
  calls the nanobind kernel; Cython generates a C function for the
  cpdef body that we can take a pointer to. Saves rewriting kernels
  but doesn't actually eliminate the PyObject_Call — it just moves
  it inside the cpdef. **Rejected.**
- **(c) Drop nanobind for execute-time kernels.** Built-in kernels
  become plain `extern "C"` functions; nanobind bindings deleted.
  Tests that used the nanobind surface need to call through a thin
  C-to-Python wrapper. Biggest blast radius; cleanest end state.

**Recommendation: (a).** Each kernel header declares the C ABI;
implementation file provides both bodies. Nanobind binding is a
3-line wrapper around the C function. Tests keep working.

### 2. Closures with captured state (CAST, CASE, BINARY_OP)

Most resolved kernels in Phases 5/6/7 are Python closures capturing
state — cast `unit`, CASE's compiled-bytecode lists, binary-op's
`op_code` and method name. C function pointers can't capture
context.

Standard C pattern: `(*kernel_fn)(void* ctx, /* args */)`. `ctx` is a
`void*` cast to the kernel's expected struct. The struct lifetime is
the same as the `CompiledBytecode`'s `_held_refs` list — i.e., from
bind time to the bytecode being garbage-collected.

We add two fields to `BytecodeInstr`:
```cython
void*  ctx_ptr      # kernel context (struct lifetime ≥ bytecode)
void*  kernel_fn    # function pointer; cast at call site to known signature
```

`slot.callable_ref` becomes unused for these opcodes (still used by
no one else; can stay or be deleted). The bytecode's `_held_refs`
list keeps a Python object holding the C struct backing memory alive.

### 3. Kernel signature uniformity

The five opcodes have different shapes:
- BC_FUNCTION: variadic (0–N args)
- BC_EXTRACTION: fixed 2 args (vector, key)
- BC_CAST: fixed 1 arg (vector)
- BC_CASE: fixed 1 arg (morsel)
- BC_BINARY_OP: fixed 2 args (left, right)

Three options:

- **(a) One uniform signature**:
  ```c
  VecResult (*kernel_fn)(void* ctx, const DrakenVector* const* args, uint32_t nargs);
  ```
  Pros: single executor branch can dispatch all five. Cons: variadic
  marshalling cost for the common fixed-arity cases.
- **(b) Per-opcode signatures.** Each opcode's executor branch casts
  the function pointer to the right type. More executor code; cleaner
  performance.
- **(c) Hybrid.** Fixed signatures for BC_EXTRACTION / BC_CAST /
  BC_BINARY_OP (2/1/2); variadic for BC_FUNCTION; BC_CASE keeps a
  morsel-shaped signature.

**Recommendation: (c)**. Performance matters; per-opcode typing
costs ~5 typedefs and ~5 casts. The variadic case is unavoidable
for BC_FUNCTION; the rest get fixed signatures.

### 4. Result handling

Kernels return `VecResult` (already a C struct in draken). The
result-wrap flags (`BC_RESULT_NEEDS_NB_WRAP` / `BC_RESULT_WRAP_AS_BOOL`)
exist for the **Python-callable** kernels that return nanobind
Vectors. With C kernels, the result IS the `VecResult` — store its
`DrakenVector` directly in `dv_stack[sp]`, set `anchor[sp] = None`.

The Python-Vector wrap happens only at the very end of
`execute_bytecode`, when the final stack slot is returned (via
`_slot_to_pyobj`). That code stays.

`BC_RESULT_*` flags become dead — delete after Phase 9.

### 5. BC_CASE re-entry to `execute_bytecode`

BC_CASE's `_case_fn` closure re-enters `execute_bytecode` for each
condition and result branch. With nogil executor, the re-entry must
also be nogil — which it can be, since `execute_bytecode` is the
function being annotated.

The CASE kernel becomes a C function with this rough shape:
```c
struct case_ctx {
    CompiledBytecode** cond_bcs;
    uint32_t           n_conds;
    CompiledBytecode** result_bcs;
    CompiledBytecode*  else_bc;       // may be NULL
    AssembleKind       assemble_kind;
};

VecResult case_kernel(const case_ctx* ctx, const Morsel* morsel) nogil;
```

The kernel calls `execute_bytecode_c(bc, sub_morsel)` (a new nogil
entry point that bypasses the Python-callable cpdef shell) for each
branch. `Morsel.take` for sub-morsels must also be C-callable —
**that's another piece of work**, surface separately.

### 6. UDF policy reaffirmation

The architect already said: **no UDFs**. Phase 9 cements this — the C
ABI has no Python escape hatch. If a SQL function isn't a built-in
shipped kernel, bind-time fails. Confirm before implementation
starts.

### 7. GIL release boundaries

After Phase 9:
- `execute_bytecode` entry: release GIL.
- Inner loop: nogil.
- Exit: re-acquire GIL to construct the Python Vector wrapper around
  the final result.
- BC_CASE recurses: stays nogil end-to-end.
- Morsel I/O at the boundaries (`morsel.num_rows`, `morsel.column`):
  needs to be C-callable in the inner loop. Likely a small surface;
  audit during implementation scoping.

## Design decisions to be made before implementation

These are the architect calls. Each has my recommendation; the
architect picks (or substitutes a different option).

1. **Nanobind kernel ABI** — (a) parallel C ABI / (b) cpdef wrappers
   [rejected] / (c) drop nanobind. Recommendation: **(a)**.
2. **Closure context passing** — confirm the `(ctx_ptr, kernel_fn)`
   pair-in-slot approach. Recommendation: **yes**.
3. **Signature uniformity** — (a) variadic / (b) per-opcode / (c)
   hybrid. Recommendation: **(c)**.
4. **Result handling** — confirm `VecResult` directly into `dv_stack`,
   no Python wrap until the executor's final return. Recommendation:
   **yes**.
5. **`BytecodeInstr` struct change** — adds `ctx_ptr` and `kernel_fn`
   fields. This breaks the ABI for any external consumer; verify
   none. Recommendation: **yes, with explicit size assertion**.
6. **BC_FUNCTION result-wrap flags** — delete after Phase 9
   (`BC_RESULT_NEEDS_NB_WRAP`, `BC_RESULT_WRAP_AS_BOOL`,
   `BC_RESULT_NO_DV`). Recommendation: **yes**.
7. **Morsel I/O nogil surface** — what gets `nogil` annotations on
   `Morsel`? At minimum: `num_rows` property, `column(name)`,
   `take(indices)`. Surface during implementation; this design ticket
   doesn't lock the list.

## Estimated effort

Once design is locked:

| Sub-ticket | Scope                                                                                       | Estimate |
|------------|---------------------------------------------------------------------------------------------|---------:|
| 9a         | Draken: parallel C ABI for built-in kernels. Header + impl + ABI test                       | 3–5 days |
| 9b         | Opteryx: `BytecodeInstr` struct extension + bind-time changes for the 5 opcodes              | 2–3 days |
| 9c         | Opteryx: executor branch rewrites for the 5 opcodes — per-opcode typed function-pointer call | 2–3 days |
| 9d         | Morsel nogil surface — `take_rows_nogil`, `num_rows_c`, column lookup-by-index               | 1–2 days |
| 9e         | nogil annotation of `execute_bytecode` + thread-safety stress test                          | 1–2 days |
| 9f         | Cleanup: delete `BC_RESULT_*` flags, the resolver closures, etc.                            | 1 day    |

**Total: 2–3 weeks** of focused work. Higher risk than any prior
phase because the kernel-ABI design touches every built-in function.

## Risks

1. **Nanobind kernels with C++ exception throws** — the C ABI must
   either propagate errors out-of-band (e.g. `VecResult.error_code`)
   or wrap C++ exceptions at the boundary. The DV fast paths already
   handle this via `arena`-allocated error slots; pattern is known.
2. **Closure context lifetime** — if a `CompiledBytecode` is shared
   across queries (it shouldn't be, but verify), context structs
   freed at bytecode dealloc could be referenced from in-flight
   morsels. Standard pattern: contexts live as long as the
   `_held_refs` list.
3. **Cython 3 / Python 3.13 nogil compatibility** — confirm Cython
   3.2.5 supports the nogil shape we want. The bitmap fast paths
   already use it (`c_execute_bytecode_inner`), so this is low risk.
4. **`Morsel.take` for sub-morsels in CASE** — currently a Python
   `def` method. Either expose a nogil cdef variant or accept a one-
   PyObject_Call-per-CASE-branch boundary. Surface decision.
5. **External callers** of the 5 kernels — any test, REPL, or other
   code path that imports e.g. `vector_json_extract` expects the
   Python callable. The parallel-C-ABI approach (option 1a) keeps
   the Python callable for them; verify before deleting.

## What this ticket does NOT do

- It doesn't write any code. The ticket exists to lock the design.
- It doesn't lock the test plan. The implementation sub-tickets each
  carry their own verification.
- It doesn't promise a delivery date. The architect's other
  priorities (the two correctness bugs in queue, the test-suite
  expansion) may take precedence.

## Open questions for the architect

- [ ] Approve recommendations 1–6 above, or substitute alternatives.
- [ ] Confirm UDF policy (no UDFs in any form) for Phase 9 record.
- [ ] Decide whether the correctness bugs
      (`bug-count-star-where-returns-zero.md`,
      `bug-assemble-fixed-no-else-int-segfault.md`) ship before or
      after Phase 9 starts. Either order is fine; Phase 9 doesn't
      touch the affected code.
- [ ] Decide whether to write 9a–9f as separate tickets up-front or
      one-at-a-time as 9a lands and the design assumptions firm up.
- [ ] Verify nobody outside opteryx imports the 5 kernel surfaces
      directly (or accept the breakage and document migration).

## Side-context (carried from Phase 8 train)

- Two correctness bugs in tickets:
  `docs/tickets/bug-count-star-where-returns-zero.md`,
  `docs/tickets/bug-assemble-fixed-no-else-int-segfault.md`. Both
  independent of Phase 9.
- Test-file cleanup pending (4 files importing deleted Phase 3/4
  symbols). No ticket yet — flag.
- `make q` is shape-only; value-checked test coverage expansion was
  mentioned across multiple tickets as a recurring need. Worth a
  separate ticket if not already on a roadmap.

## Definition of "design locked"

This ticket is complete when:
- All seven "design decisions" above have an architect-confirmed
  answer (yes / no / alternative).
- The seven open questions are answered.
- The sub-ticket breakdown (9a–9f) is either accepted as written or
  restructured per architect preference.
- A `Post-design` section is appended to this ticket with the
  locked answers, so the implementation sub-tickets can cite them.

Implementation starts only after that lock.

---

## Post-design: Locked Decisions

### Design Decision Lockdown

**Decision 1: Nanobind kernel ABI → (a) Parallel C ABI (APPROVED)**

Each built-in kernel has both:
- Its `extern "C"` C function (registered in a kernel table, callable from `execute_bytecode` without PyObject_Call).
- Its nanobind Python binding (3-line wrapper for tests, REPL, external API).

Implementation: Phase 9a writes C headers in `draken/ops/kernels/` with `extern "C"` signatures. Nanobind bindings become trivial forwarding. Tests consume nanobind surface unchanged.

**Decision 2: Closure context passing → YES (APPROVED)**

Mechanism:
- Add `ctx_ptr` (void*) and `kernel_fn` (void*) fields to `BytecodeInstr`.
- Context struct lifetime ≥ `CompiledBytecode`, kept alive in bytecode's `_held_refs` list.
- Executor casts `ctx_ptr` to the kernel's context struct type before calling.

Applies to: BC_CAST (unit state), BC_CASE (cond/result bytecode arrays), BC_BINARY_OP (op_code + fallback kernel).

**Decision 3: Signature uniformity → (c) Hybrid (APPROVED)**

Fixed per-opcode signatures:
- **BC_FUNCTION**: `VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)` — variadic.
- **BC_EXTRACTION**: `VecResult (*)(void* ctx, const DrakenVector* vector, const DrakenVector* key)` — 2 args.
- **BC_CAST**: `VecResult (*)(void* ctx, const DrakenVector* vector)` — 1 arg.
- **BC_BINARY_OP**: `VecResult (*)(void* ctx, const DrakenVector* left, const DrakenVector* right)` — 2 args.
- **BC_CASE**: `VecResult (*)(void* ctx, const Morsel* morsel)` — morsel-shaped.

Executor: 5 typedefs in `evaluator/evaluation.pyx` + per-opcode cast. ~5 casts is acceptable cost for type safety.

**Decision 4: Result handling → YES (APPROVED)**

Kernels return `VecResult` struct (already C-native in draken).
- Store `VecResult.dv` directly in `dv_stack[sp]`.
- Set `anchor[sp] = None` (no Python object holding the DrakenVector).
- Python wrap happens only at executor exit via `_slot_to_pyobj` for the final result.
- `BC_RESULT_NEEDS_NB_WRAP`, `BC_RESULT_WRAP_AS_BOOL`, `BC_RESULT_NO_DV` flags become dead code; delete in Phase 9f.

**Decision 5: BytecodeInstr struct ABI break → YES, with assertion (APPROVED)**

Action:
- Add `ctx_ptr` (void*) and `kernel_fn` (void*) fields to `BytecodeInstr` in `compiled_expression.pxd`.
- Insert explicit `sizeof(BytecodeInstr)` size assertion in a unit test (Phase 9b).
- Struct size change is intentional and documented.
- Grep scan in Phase 9b verifies no external wheels/APIs directly import `BytecodeInstr` (low risk; it's internal to evaluator).

**Decision 6: Delete result-wrap flags → YES (APPROVED)**

Delete in Phase 9f (cleanup):
- `BC_RESULT_NEEDS_NB_WRAP`
- `BC_RESULT_WRAP_AS_BOOL`
- `BC_RESULT_NO_DV`

These flags are only referenced by Python-callable kernel dispatch, which is gone after Phase 9c.

**Decision 7: Morsel I/O nogil surface → To be scoped in Phase 9d (APPROVED)**

Minimum set for Phase 9d audit:
- `num_rows` property (read-only).
- `column(idx)` or `column(name)` (column access by index or name).
- `take(indices)` (sub-morsel extraction; must be nogil for BC_CASE inner loop).

Exact set of nogil candidates determined during Phase 9d scoping. Start with those three; expand if needed.

---

### Open Question Resolutions

**Question 1: Approve recommendations 1–6 → APPROVED**

All six design decisions above approved as written. No substitutions.

**Question 2: Confirm UDF policy (no UDFs) → CONFIRMED**

- **UDFs are forbidden in Phase 9.**
- The C ABI has no Python escape hatch.
- Any SQL function must be a built-in shipped kernel; bind-time resolution fails otherwise (raise clear error: "Unknown function: X; only built-in functions are supported").
- No user-defined functions, no closure fallbacks, no dynamic resolution paths.
- Update bind-time validation in `opteryx/expression/operations/__init__.pyx` to enforce this.

**Question 3: Correctness bugs ordering → Parallel track (APPROVED)**

The two pending bugs are independent of Phase 9:
- `bug-count-star-where-returns-zero.md`
- `bug-assemble-fixed-no-else-int-segfault.md`

Either ship before Phase 9 or in parallel. Phase 9 does not wait for these.

**Question 4: Sub-ticket breakdown (9a–9f) → Write upfront (APPROVED)**

Write all six sub-tickets before implementation begins:
- **9a**: Draken C ABI (3–5 days)
- **9b**: BytecodeInstr struct + bind-time (2–3 days)
- **9c**: Executor rewrites (2–3 days)
- **9d**: Morsel nogil surface (1–2 days)
- **9e**: nogil annotation + stress test (1–2 days)
- **9f**: Cleanup (1 day)

Rationale: 9a unblocks 9b and 9c; upfront tickets clarify dependencies and allow parallel work on independent sections. Total 2–3 weeks.

**Question 5: External kernel imports → Verify in Phase 9b (APPROVED)**

Audit plan (Phase 9b):
- Grep for direct imports of these kernels from outside `opteryx/expression/evaluator/`:
  - `vector_json_extract`
  - `vector_map_access_string`
  - `vector_array_map_access`
  - Other BC_EXTRACTION, BC_CAST, BC_FUNCTION kernels
- If found in tests: provide thin Python shim wrappers for backwards compat during transition.
- If found elsewhere (wheels, external code): document as breaking change. Expect none (internal API).

**Question 6: Morsel I/O audit → Phase 9d (APPROVED)**

Scoping task in Phase 9d:
- Enumerate every Morsel method/property accessed inside `execute_bytecode` or called by executor branches (especially BC_CASE).
- Mark candidates for `cdef nogil` annotation.
- Prioritize `take(indices)` (called from BC_CASE kernel); scope the rest.
- Surface exact set and nogil-candidacy in 9d ticket description.

---

### Pre-implementation Verification Checklist

- [ ] **Phase 9a readiness**: Enumerate all built-in kernels (`draken/ops/`, `opteryx/compiled/`) with current C++ implementations.
- [ ] **External consumer audit**: Grep for external imports of `BytecodeInstr`, kernel tables, or kernel function surfaces.
- [ ] **Morsel audit**: Full enumeration of Morsel methods used in bytecode execution (support for 9d scoping).
- [ ] **Cython 3.2.5 nogil**: Confirm `cdef nogil` annotations on long-running functions work with current Cython version (low risk; `c_execute_bytecode_inner` already uses this).
- [ ] **GIL semantics**: Document GIL re-acquisition point in executor exit and BC_CASE recursion boundaries.
- [ ] **Error propagation**: Audit how C++ exceptions in nanobind kernels are caught and converted to `VecResult.error_code` (pattern exists in DV fast paths).

---

### Design Lockdown Summary

**Status: LOCKED**

All seven design decisions and seven open questions have architect-approved answers. The design is ready for implementation.

**Next step**: Write Phase 9a–9f sub-tickets with these locked decisions as reference. Implementation may begin once sub-tickets are written and prioritized.

**Readiness**: Yes. 9a (draken C ABI) is the critical path; it unblocks 9b and 9c. Estimated delivery: 2–3 weeks of focused work starting from 9a green light.
