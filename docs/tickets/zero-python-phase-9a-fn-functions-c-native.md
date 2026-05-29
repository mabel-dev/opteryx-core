# Zero-Python Phase 9a-fn (increment 1): BC_FUNCTION → C-native, proving slice

> Architect decision (2026-05-28): resume Phase 9 with **9a-fn —
> functions → C-native**. This is **increment 1**: stand up the full
> `BC_FUNCTION` C-native dispatch path end-to-end on a *small, vetted
> slice* of functions, gated by value-checked tests, then expand in
> follow-on tickets. The slice exists to prove the mechanism without the
> big-bang flip that caused the eight-round thrash.

## ⚠️ PREMISE CORRECTION (verified 2026-05-28) — kernels are hollow scaffolding

The original premise ("9a wrote working function kernels; just register +
wire them") is **false**. Verified against the tree:

- The function kernel **sources are excluded from the build.**
  `setup.py:~720`: *"Function kernels deferred to Phase 9f; they require
  nanobind wrappers not yet ported to extern 'C'."* — `function_*.cpp`
  are **uncompiled**; they have never been built.
- The kernels are **scaffolding only.** `function_string.cpp` defines the
  public `vector_<name>(ctx, args, nargs)` wrappers (arity check +
  delegate), but they call `vector_<name>_impl(...)` functions that are
  **forward-declared and defined nowhere** in the tree. Adding the
  sources to the build would **fail to link** (undefined `*_impl`
  symbols).
- The registry has 51 entries (casts, binary arith, bitwise, a few
  extraction/json) and **zero general SQL functions**;
  `lookup_kernel("draken_length"/"upper"/"abs"/…)` all miss.

**Consequence:** 9a-fn is NOT "register + wire". It is "**implement the
kernel bodies** (delegating to the real draken string ops that the
current Python path already uses), add the sources to the build, add the
VecResult→Vector C trampoline, register, then wire the executor." This is
materially larger than the original ticket assumed, and is exactly the
"inert code reported done" trap the STEP-BACK warned about (9a kernels
were green because they were never compiled or run).

This correction was surfaced to the architect before implementation
(scope changed from "wire existing" to "implement + wire"). Proceed only
on the increment below.

## Original (now-corrected) starting-state notes
- **Bind-time hook already present, currently inert.**
  `compiled_expression.pyx` NT_FUNCTION (~L604–615) calls
  `_resolve_kernel_and_context(f"draken_{func_name.lower()}")` and, on a
  hit, sets `slot.kernel_fn` + `BC_INSTR_C_NATIVE` while retaining the
  Python `slot.callable_ref`. Because nothing is registered, it always
  misses → **blast radius is currently zero**.
- **Executor has no C-native BC_FUNCTION branch.** `evaluation.pyx`
  BC_FUNCTION (~L1837–1907) always calls the Python `callable_ref`,
  ignoring `kernel_fn`/`BC_INSTR_C_NATIVE`.
- **Signature (locked, Decision 3):**
  `VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)`
  (`function_kernels.h`). `ctx == NULL` for built-ins.
- **Result handling (locked, Decision 4):** store `VecResult.dv`
  directly in `dv_stack[sp]`; no Python wrap mid-execution.

## Scope — increment 1 ONLY

Migrate exactly these three string functions to C-native, end-to-end:

| SQL func | C symbol        | result type | why chosen |
|----------|-----------------|-------------|------------|
| `LENGTH` | `vector_length` | INT64       | string-in → int-out; exercises type change |
| `TRIM`   | `vector_trim`   | VARCHAR     | string-in → string-out |
| `REVERSE`| `vector_reverse`| VARCHAR     | string-in → string-out |

(If any of these three is not actually a 1-arg string kernel in
`function_kernels.h`, substitute the nearest 1-arg string function that
is, and say so in the done report.)

**Everything else stays on the Python `callable_ref` path** — unchanged,
correct. Do NOT register or flip other functions in this ticket. The
remaining categories are follow-on increments (9a-fn-2…).

## Work

1. **Register the three kernels** in `kernel_registry.cpp` using the
   exact name the bind-time looks up, mapped to the real C symbol:
   ```cpp
   {"draken_length",  (kernel_fn_t)&vector_length},
   {"draken_trim",    (kernel_fn_t)&vector_trim},
   {"draken_reverse", (kernel_fn_t)&vector_reverse},
   ```
   Reconcile the `draken_<name>` (registry/bind-time) vs `vector_<name>`
   (kernel symbol) naming — the lookup key MUST equal
   `f"draken_{func_name.lower()}"` for the SQL name. Verify the
   `kernel_fn_t` typedef matches the variadic FunctionKernel signature
   (it may need a function-kernel-specific typedef; do not cast a
   2-arg-signature typedef onto a variadic kernel).

2. **Add the executor C-native branch** in `evaluation.pyx` BC_FUNCTION,
   *before* the Python-callable path, guarded by
   `slot.flags & BC_INSTR_C_NATIVE`:
   - Pop `arity` operands; build a stack C array
     `const DrakenVector* args[arity]` from `dv_stack[func_base + j]`
     (these are borrowed DV* — anchors keep them alive).
   - Call `(<FunctionKernel>slot.kernel_fn)(slot.ctx_ptr, args, arity)`.
   - On `VecResult.data == NULL` (error sentinel) → raise a clear
     Python exception (fail fast; no silent fallback to the Python
     callable — a registered kernel that errors is a bug, not a
     fall-through).
   - Store `VecResult.dv` into `dv_stack[sp]`, `anchor[sp] = None`
     (arena/owned per VecResult ownership contract — follow exactly how
     BC_CAST/BC_EXTRACTION handle the VecResult→dv_stack hand-off,
     including arena release in `_slot_to_pyobj`).
   - Do NOT apply the `BC_RESULT_NEEDS_NB_WRAP` Python-wrap flags on the
     C-native branch (those are for the nanobind callable path).

3. **Confirm bind-time now resolves** these three (it will, once
   registered) and leaves all other functions on `callable_ref`.

## The thrash lessons — mandatory gates (read `zero-python-phase-9-STEP-BACK.md`)

The eight-round thrash happened because a shape-only gate let
non-functional and regressed code report green. This ticket is rejected
unless ALL of the following are pasted:

1. **Value-checked `make et` tests for each migrated function**, covering
   — for `$planets.name` (VARCHAR) and a constructed partial-null case:
   - non-null column input → correct values
   - all-null input → all-null output (null propagation)
   - **partial-null** input (e.g. via `CASE WHEN … THEN name END`) →
     per-row nulls correct (this is the validity-merge behaviour dodged
     4× in 9a/9c — it must be demonstrably correct here)
   - a dict/constant-encoded input shape (e.g. a repeated literal or a
     dictionary column) → correct (§11 shape conformance)
   `LENGTH` additionally: assert the INT64 result values.
   Put them in `tests/test_expression_engine.py`; paste the new count
   (> 41) and the new tests passing.

2. **Proof the C path is actually taken (not inert).** The Python
   callable path and the C path can both produce the right answer, so a
   green test alone doesn't prove the kernel ran. Assert it: increment/
   read the existing `_c_native_kernel_call_count` telemetry (used by
   BC_BINARY_OP) from the C-native BC_FUNCTION branch, and add a test
   that the counter rises when `LENGTH(name)` is evaluated and does
   **not** rise for a function left on the Python path. Paste it.

3. **`make et` green; `make q` 137/137; `make kernel-parity` green.**

4. **No regression on the functions left on the Python path** — spot a
   couple (e.g. `UPPER(name)`, `CONCAT`) still return correct values
   (they must, since they're untouched). Paste.

## Out of scope

- Migrating any function beyond the three named (follow-on increments).
- 9d/9e/9f (nogil surface/annotation/cleanup).
- The pre-existing test-suite API rot (`to_arrow` helper, missing
  modules) — `test_suite_api_migration.md`.
- The `date_part()`-segfaults-on-non-Vector fail-fast bug (separate
  hardening ticket).

## Constraints (CLAUDE.md)

- **No silent fallback.** A registered kernel that returns the error
  sentinel must raise, not quietly fall back to the Python callable. One
  path per function: resolved → C-native (complete or fails loudly);
  unresolved → Python callable (unchanged). A function is in exactly one
  state, chosen at bind time.
- **Correct null/shape handling is the deliverable**, not an extra. The
  validity-merge behaviour has been dodged repeatedly; the partial-null
  test is the gate that makes it un-dodgeable.
- **Cython stays typed**; no `object` on the C-native path.
- **`make q` is shape-blind** — it is necessary but NOT sufficient; the
  value-checked + counter tests are the real gate.
- **Do not commit.**

## Definition of done

- Three kernels registered; executor C-native BC_FUNCTION branch live;
  other functions unchanged on the Python path.
- `make et` has the new value-checked tests (non-null/all-null/partial-
  null/shape + LENGTH int values + counter proof), count > 41, all
  passing — pasted.
- `make q` 137/137; `make kernel-parity` green — pasted.
- C-native path proven taken via `_c_native_kernel_call_count` — pasted.
- Untouched functions still correct — pasted.
- Done report states which (if any) of LENGTH/TRIM/REVERSE was
  substituted and why.
