# Ticket: Zero-Python Phase 9b-completion — fix silent kernel-resolution failures

> Corrective ticket. 9b added the `BytecodeInstr` fields
> (`kernel_fn`/`ctx_ptr`/`BC_INSTR_C_NATIVE`), the struct sizeof test,
> and the round-trip test (6/6 green) — that part is sound. But the
> bind-time resolution **silently does not work** for the common cases,
> hidden by a CLAUDE.md §9 try/except and by 9b being behaviour-neutral
> (executor still on `callable_ref`, so `make q` is green regardless).

## Two confirmed bugs

### Bug 1 — cast key derivation never matches the registry for INTEGER/DOUBLE

`_NT_CAST` builds the kernel name from a `_orso_to_type_name` dict
keyed on `"INT64"` / `"FLOAT64"`. But the real `OrsoTypes` enum names
are **`"INTEGER"`** and **`"DOUBLE"`**. So:

```
INTEGER → VARCHAR  ⇒ key "draken_cast_integer_to_string"  ⇒ lookup MISS
DOUBLE  → INTEGER  ⇒ key "draken_cast_double_to_integer"  ⇒ lookup MISS
INT64   → VARCHAR  ⇒ key "draken_cast_int64_to_string"    ⇒ resolves (but no
                                                            column is ever "INT64")
```

(Verified: `OrsoTypes.INTEGER.name == "INTEGER"`, `OrsoTypes.DOUBLE.name == "DOUBLE"`;
`lookup_kernel("draken_cast_integer_to_string")` → `(None, None)`.)

The registry keys use `int64` / `float64`. The emitter's map must
translate `INTEGER → int64` and `DOUBLE → float64`. It doesn't, so the
two most common numeric casts silently fall back to Python. The C
kernels 9a built and parity-tested are **never reached**.

### Bug 2 — try/except control flow swallows every resolution miss (§9)

All four emitters (BINARY_OP, FUNCTION, CAST, EXTRACTION) wrap
resolution in:

```cython
try:
    fn_ptr, ctx_wrapper = _resolve_kernel_and_context(kernel_name, ...)
    slot.kernel_fn = <void*>fn_ptr
    slot.flags |= BC_INSTR_C_NATIVE
except ValueError:
    pass   # silently fall back to Python callable
```

This is **CLAUDE.md §9** ("Do not use try/except to control flow or
silently handle errors"). Worse, it's what *hid* Bug 1: the INTEGER
cast miss raised `ValueError` from `_resolve_kernel_and_context` and
was swallowed, so `kernel_fn` stayed NULL, `BC_INSTR_C_NATIVE` was
never set, and nothing complained. Any future key-scheme drift is
equally invisible.

## Why make q didn't catch it

9b is behaviour-neutral by design — the executor still calls
`callable_ref` until 9c. So whether `kernel_fn` is correctly populated
or silently NULL, `make q` is identical (137/137). **The build being
green is not evidence the wiring works** — same trap as the 9a rounds.
There is currently no test that asserts a real query produces a
C-native slot.

## Goal

- Cast (and all) kernel names the emitters build **match the registry
  keys** — common-case casts resolve to real C kernels.
- **No try/except control flow.** Resolution either succeeds, or the
  absence is handled by an explicit branch (not an exception).
- A **bind-time verification test** proves real queries produce
  `kernel_fn != NULL` + `BC_INSTR_C_NATIVE` set for the opcodes whose
  kernels exist. This is the gate that makes "9b works" checkable while
  the executor is still on the Python path.

## Scope

**In scope**
1. **Fix the cast type-name map.** `_orso_to_type_name` (or however the
   key is built) must map the real OrsoTypes names to the registry's
   type tokens:
   - `INTEGER → int64`, `DOUBLE → float64`, `VARCHAR → string`,
     `BOOLEAN → bool`, `DATE → date32`, `TIMESTAMP → timestamp`,
     `BLOB → string`.
   - Verify every produced key against `kernel_registry.cpp`'s 48 keys.
     Any cast pair whose key isn't in the registry is either (a) a
     naming bug to fix, or (b) a cast with genuinely no C kernel yet
     → must fall back **explicitly** (see point 2), not via exception.
2. **Replace the try/except with explicit presence checks.**
   - Change `_resolve_kernel_and_context` (or the call sites) so a
     missing kernel returns a sentinel (`None`) rather than raising —
     `lookup_kernel` already returns `(None, None)` for not-found; stop
     converting that into a `raise` + `except`.
   - **CAST / BINARY_OP / EXTRACTION**: these kernels exist in the
     registry. A resolution miss for a supported type combo is a **bug**
     → fail-fast (raise a clear error naming the unresolved key). Only
     genuinely-unsupported combos (no registry entry) fall back to
     Python, and that fallback must be an explicit `if fn_ptr is None:`
     branch, documented.
   - **BC_FUNCTION**: kernel absence is **expected** (function kernels
     are carved out to 9a-fn). Explicit branch: `if fn_ptr is not None:
     wire C-native; else: keep callable_ref`. No exception, no silent
     swallow — the branch is the intended logic, clearly commented as
     "function C kernels pending 9a-fn".
3. **Add the bind-time C-native verification test.** Build real
   `CompiledBytecode` for representative queries and assert the
   relevant slot has `kernel_fn != NULL` and `BC_INSTR_C_NATIVE` set:
   - CAST: `CAST(id AS VARCHAR)` (INTEGER→string), `CAST(id AS DOUBLE)`,
     `CAST(some_double AS INTEGER)` — must be C-native.
   - BINARY_OP: `id + 1`, `id * 2`, `id | 2` — must be C-native.
   - EXTRACTION: `missions[0]`, a JSON `->` — must be C-native.
   - BC_FUNCTION: `LENGTH(name)` — must **NOT** be C-native yet (stays
     `callable_ref`); assert the flag is clear, documenting the 9a-fn
     dependency. (When 9a-fn lands, this assertion flips.)
   - Expose a way to read `slot.flags` / `kernel_fn != NULL` for a
     given instruction from the test (a small `cpdef` introspection
     helper on `CompiledBytecode`, test-only, is acceptable).

**Out of scope**
- Executor changes — 9c.
- Function kernels — 9a-fn.
- The struct fields / sizeof test / round-trip test — already correct,
  leave them.

## Verification — the gate that was missing

- `make c` clean.
- `make q` 100/100 (still behaviour-neutral).
- **New bind-time test passes**, proving:
  - INTEGER/DOUBLE/string casts → C-native slot (`kernel_fn != NULL`,
    flag set). **This is the assertion that fails today** and must pass.
  - arithmetic + bitwise → C-native.
  - extraction → C-native.
  - function → not-yet-C-native (callable_ref), documented.
- `grep -n "except ValueError" opteryx/compiled/expression/compiled_expression.pyx`
  → zero in the kernel-resolution emitters.
- Paste in the PR: for one query per wired opcode, the introspected
  `(kernel_fn != NULL, BC_INSTR_C_NATIVE set)` result.

## Constraints (CLAUDE.md)

- **§9: no try/except for control flow / silent error handling.** This
  is the specific rule violated; the fix must remove it, not relocate
  it.
- **Fail fast.** A supported cast/binary_op/extraction combo that
  doesn't resolve is a bug → raise with the unresolved key name. Do not
  silently fall back for combos that should work.
- **Broken but honest beats green but fake.** 9b "passed" with its
  cast wiring non-functional. The new bind-time test is the honesty
  gate — it must assert real C-native slots, not struct round-trips.
- **`make c` clean before done.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/compiled/expression/compiled_expression.pyx`:
  - `_resolve_kernel_and_context` (~line 52) — stop raising on
    not-found; return sentinel.
  - `_NT_CAST` emit (~line 698–760) — fix `_orso_to_type_name`;
    explicit presence branch.
  - `_NT_BINARY_OPERATOR` (~line 523–560), `_NT_EXTRACTION_OPERATOR`
    (~line 834–860), `_NT_FUNCTION` (~line 618–645) — replace
    try/except with explicit branches.
- `tests/test_phase_9b_bytecode_instr.py` — extend with the bind-time
  C-native assertions (or a new `tests/test_phase_9b_resolution.py`).
- `draken/ops/kernels/kernel_registry.cpp` — the 48 keys; the
  source of truth the emitter keys must match.

## Definition of done

- Cast key derivation maps INTEGER→int64, DOUBLE→float64, etc.;
  every common-case cast resolves to a registry kernel.
- Zero `except ValueError` (or any try/except) in the kernel-resolution
  emitters; absence handled by explicit `if fn_ptr is None:` branches.
- CAST/BINARY_OP/EXTRACTION fail-fast on a supported-combo miss;
  BC_FUNCTION falls back explicitly (callable_ref) pending 9a-fn.
- Bind-time verification test asserts C-native slots for real
  CAST/BINARY_OP/EXTRACTION queries and not-yet-C-native for FUNCTION;
  passes; output pasted in PR.
- `make c` clean; `make q` 100/100.

## Note on the recurring pattern

This is the third Phase-9 deliverable (after 9a rounds) reported
complete while the actual wiring was non-functional and hidden by
green build output. The root cause each time: **the new code isn't on
an executor path yet, so `make c`/`make q` green proves nothing.** The
durable fix is the verification test this ticket adds — assert the
*intended state of the new field*, not just that the build compiles.
Every remaining Phase-9 bind-time ticket (9a-fn especially) should
ship with an equivalent "the field is actually populated" assertion.
