# 05 — Request: native C-callable kernels for BC_FUNCTION / BC_CAST dispatch

> **From:** eval-PM (`opteryx/expression/evaluator/`)
> **To:** the PM that owns the function/cast compute kernels
> (`opteryx/compiled/nanobind/` for functions; `draken/ops/kernels/` for
> casts).
> **Why:** Phase 3 (doc 03) is the last open phase of the eval-engine
> refactor. Its in-scope part is done; the remaining piece — a C-level
> indexed dispatch table — is blocked on a native kernel surface that
> lives outside `opteryx/expression/`. This is the formal request,
> mirroring doc 03's "Phase 4 — kernel interface deliverables" section.
> The eval-PM will **not** reach across the boundary to build these.

## What's already true (so you know the boundary)

The eval engine framework is complete: `DrakenVector*` stack, frame
arena, and native dispatch for BC_COMPARE (`draken_compare_dv`),
BC_BINARY_OP (`draken_arithmetic_dv`), and the boolean combinators
(`c_*_bitmap`). BC_FUNCTION / BC_CAST / BC_EXTRACTION / BC_UNARY / BC_CASE
remain Python-mediated: the executor unpacks `slot.callable_ref` and does
a Python call. That is correct, just not GIL-free.

Phase 3's in-scope orchestration is already removed (bind-time `nb_func`
flag, typed `._nb` access, direct per-arity dispatch). The only thing
left is to replace the Python call with a C-level indexed call — and that
needs the kernels below.

## What the eval engine needs

A **C-callable kernel surface** the executor can invoke without the GIL,
plus a way to resolve a bytecode instruction to a function pointer at
bind time. Concretely:

### 1. Calling convention (per doc 03, Decision C)

```c
typedef VecResult (*eval_fn_t)(const DrakenVector* const* args,
                               uint32_t arity,
                               uint32_t n_rows,
                               DrakenFrameArena* arena);
```

- `VecResult` and `DrakenFrameArena` are the existing types
  (`draken/ops/vec_result.h`, `draken/core/frame_arena.h`).
- Result buffers allocated from `arena` (or owned and adoptable into it),
  matching the `draken_compare_dv` ownership pattern already wired.
- Error → `VecResult.data == NULL` sentinel; the executor raises.
- **Null + shape correctness is part of the contract**, not an add-on:
  each kernel must handle all-null, partial-null, and dense/constant/dict
  input shapes (§11). The eval engine will gate every wired kernel with
  value-checked tests covering these — a kernel that only works on dense
  non-null input is not done.

### 2. Resolution mechanism — the decision you must make

This is the crux the eval-PM hit and could not resolve in scope. **The
compute lives in per-op `.so` files** (`opteryx/compiled/nanobind/
vector_string_case.so`, `vector_math.so`, … — 21+ separate extensions),
while the bind-time resolver
(`compiled_expression.pyx::_resolve_kernel_and_context`) looks up names in
the **draken-side registry** (`draken_native.so`). A draken-registry
entry **cannot** point at an opteryx per-op symbol — different binaries.

So the executor cannot get a function pointer to (e.g.) `UPPER`'s compute
today. You need to choose how the pointer crosses the `.so` boundary.
Options the eval-PM sees (your call, your component):

- **(a) PyCapsule per op module** — each op `.so` exports its
  `eval_fn_t` pointer(s) as a `PyCapsule`; bind-time imports the module
  and reads the capsule into `slot.kernel_fn`. Fits the current per-op
  layout; no relocation.
- **(b) Consolidate the kernels into one registry binary** the resolver
  already links (draken's, or a new opteryx-side one) — move/expose the
  compute there.
- **(c) Direct cimport per op** in the executor (like
  `draken_arithmetic_dv`) — only viable if the symbols are in a binary
  the executor's `.so` links against.

The eval-PM has no preference it can act on — this is a layout/ownership
decision for the kernel owner. Whatever you pick, the eval side needs:
**(name or function_id) → `eval_fn_t` pointer, resolvable at bind time.**

> ⚠️ Note for the kernel owner: `draken/ops/kernels/function_*.cpp` are
> currently **hollow** — ~94 `vector_X_impl` forward-declarations with
> **zero definitions**, excluded from the build (see the kernel audit,
> `docs/tickets/zero-python-phase-9-KERNEL-AUDIT.md`). The *real* compute
> already exists in `opteryx/compiled/nanobind/` (e.g. `impl_uppercase`
> in `vector_string_case.cpp`). The work is **extract a shared C core +
> expose it via `eval_fn_t`**, not write compute from scratch. Please
> don't resurrect the hollow scaffolding as the delivery — that was the
> inert-code anti-pattern the audit flagged.

### 3. Kernels needed, prioritized

The eval engine wires them incrementally as you deliver, gated by
value-checked tests each. Suggested order (highest frequency first):

1. **String functions** — UPPER, LOWER, LENGTH, TRIM, REVERSE, SUBSTRING,
   CONCAT (compute exists in `vector_string_*.cpp`).
2. **Casts** — the `draken_cast_*` kernels are **already real and
   registered** in draken (51 registry entries); these may be wireable
   first and most cheaply, since only the dispatch decision (2) blocks
   them, not missing compute.
3. **Math / temporal / json / array** functions — as bandwidth allows.

Functions that are genuinely Python (e.g. `sha224` via hashlib) stay
Python-mediated; no kernel requested for those.

## What the eval-PM will do once a kernel + resolution land

Per doc 03, the closer is small:

1. Add `function_id` (or keep name-resolution) to the bind-time path so
   `slot.kernel_fn` is set for resolved functions (the hook already
   exists at `compiled_expression.pyx` NT_FUNCTION ~L604; it currently
   always misses because nothing is registered).
2. Add the C-native branch to BC_FUNCTION / BC_CAST in `evaluation.pyx`:
   gather `const DrakenVector* const* args`, call `eval_fn_t`, fold
   `VecResult` into the arena / wrap at frame exit, raise on the NULL
   sentinel.
3. Value-checked `make et` tests per kernel (non-null / all-null /
   partial-null / dense+dict shapes) + a counter proving the C path is
   taken (so inert wiring can't pass green).
4. `make q` 137/137 holds.

No re-architecture — the framework is in place.

## Status

- **Eval-PM: blocked on the resolution decision (§2) and at least one
  delivered kernel.** Until then, BC_FUNCTION / BC_CAST stay
  Python-mediated — correct behaviour, the documented fallback.
- The eval-engine refactor (Phases 1, 2, 4 + Phase 3 in-scope) is
  otherwise complete; `make q` 137/137.
