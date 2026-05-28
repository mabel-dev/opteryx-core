# Ticket: Zero-Python Phase 9c-completion — fix the three regressions the live C path introduced

> Corrective ticket. 9c put the C dispatch path live — and CAST(non-param),
> BINARY_OP(non-null), and CASE genuinely work C-native now (verified
> correct values). But three query classes that **worked before 9c**
> (via the Python path) now **crash**, and a required gate is missing.
> 9c is not complete.

## Why this round is more serious than prior ones

Earlier Phase-9 rounds shipped inert code — green build, nothing on the
executor path, no behaviour change. **9c changed behaviour.** The
broken cases are now hard **regressions**: queries that returned correct
results before 9c now crash the engine. `make q` (137/137) misses all
of them because it has no extraction / parameterized-cast / null-
arithmetic coverage — the same coverage gap that has hidden every
Phase-9 defect.

## Three confirmed defects

### Defect 1 — `_KernelContextWrapper.ctx_ptr` AttributeError → ALL extraction + parameterized casts crash at bind time

```
SELECT missions[0] FROM testdata.astronauts LIMIT 3
SELECT name[0] FROM $planets LIMIT 2
  → AttributeError: '_KernelContextWrapper' object has no attribute 'ctx_ptr'
    at compiled_expression.pyx:897  slot.ctx_ptr = <void*>(<unsigned long long>ctx_wrapper.ctx_ptr)
```

Root cause: `_KernelContextWrapper` declares `cdef unsigned long long
ctx_ptr` (line 38) — **not `cdef public`** — so it isn't reachable via a
Python attribute lookup. `_linearize` reads `ctx_wrapper.ctx_ptr` where
`ctx_wrapper` is typed `object`, forcing a Python attribute access that
fails.

This breaks **every opcode that allocates a context**: all BC_EXTRACTION
(array/string subscript, map access — they always carry a sub-op
context) and parameterized casts (TIMESTAMP-with-unit). CAST(int→str)
works only because non-parameterized casts skip the context branch
(`ctx_wrapper is None`).

**Fix**: make the context pointer reachable at C level — either
`cdef public unsigned long long ctx_ptr`, or (better) type the local as
`cdef _KernelContextWrapper ctx_wrapper` in `_linearize` so the access
is a direct C field read, not a Python lookup. Apply to **all** emitter
sites that read `ctx_wrapper.ctx_ptr` (CAST, EXTRACTION, BINARY_OP).

### Defect 2 — `NULL + 1` → SIGBUS → C arithmetic kernels deref null-input data

```
SELECT NULL + 1                                  → Bus error (SIGBUS)
SELECT id + CAST(NULL AS INTEGER) FROM $planets  → Bus error (SIGBUS)
```

The arithmetic/bitwise C kernels (`draken_add` et al.) read
`left_data[left->selection[i]]` without handling null-typed / all-null
inputs. `NULL` is a constant-null vector (no data buffer); the kernel
dereferences it → SIGBUS. This is the **validity-merge defect** carried
forward from 9a (the `// TODO: merge validity bitmaps` /
`result.validity = nullptr`), now a **crash** because 9c put the kernels
on the live path.

**Fix** (this is the 9c validity-merge correctness gate, now blocking):
- The arithmetic/bitwise kernels must handle null inputs: where either
  input row is null (or the input is a DRAKEN_NULL constant), the output
  row is null. No data deref on null-typed inputs.
- Output validity = merge (AND) of input validities.
- `SELECT NULL + 1` → `[NULL]`; `id + NULL` → all NULL; partial-null
  columns → null-propagated. No crash.
- Covers `+ - * / %`, bitwise ops, shifts, string concat.

### Defect 3 — telemetry counter missing → no C-native proof

The 9c ticket required a per-opcode telemetry counter
(`c_native_kernel_calls` or similar) incremented in the C-dispatch
branch and asserted non-zero, so a silent revert to the Python fallback
can't pass unnoticed. `grep` shows it was not added.

**Fix**: add the counter; increment in the C-native branch of each
opcode; expose it for a test to read; assert non-zero after a
cast/binary-op/extraction query.

## Scope

**In scope**
- `compiled_expression.pyx`: fix the `_KernelContextWrapper` access
  (Defect 1) at every emitter site.
- The C arithmetic/bitwise/concat kernels (`draken/ops/kernels/binary_op_*.cpp`)
  and/or the 9c executor binop branch: null-input handling + validity
  merge (Defect 2). Extend the kernel-parity test with nullable cases
  that actually assert null propagation (not shared-bug parity).
- `evaluation.pyx`: the C-native telemetry counter (Defect 3).
- **Value-checked tests** for the three regressed classes — added to a
  suite `make q` runs:
  - Extraction: `missions[0]`, `missions[-1]`, `name[0]`, OOB → NULL.
  - Parameterized cast: `CAST(<int> AS TIMESTAMP)` with a unit.
  - Null arithmetic: `NULL + 1` → NULL; `id + NULL` → all NULL;
    partial-null column arithmetic.

**Out of scope**
- Function kernels — 9a-fn (BC_FUNCTION stays Python; correct).
- BC_CASE — works; leave it.
- 9d/9e/9f.

## Verification — the gate

- `make c` clean.
- `make q` 100/100.
- **The three regression repros now succeed** (paste output):
  - `SELECT missions[0] FROM testdata.astronauts LIMIT 3` → 3 mission strings
  - `SELECT CAST(<int_col> AS TIMESTAMP)` with unit → timestamps
  - `SELECT NULL + 1` → `[None]`; `SELECT id + CAST(NULL AS INTEGER) FROM $planets LIMIT 3` → `[None,None,None]`
- C-native telemetry counter non-zero after cast/binop/extraction;
  asserted in a test.
- `make kernel-parity` still green, **now with nullable cases that
  assert null propagation** (the prior nullable cases passed on shared
  null-dropping; they must now assert the correct null-propagated
  result).
- `make clickbench` improves or holds (cast/binop/extraction now
  C-native).
- Value-checked tests for all three regressed classes pass.

## Constraints (CLAUDE.md)

- **Fix forward; these are regressions.** Queries that worked before 9c
  must work after. Do not gate them behind the Python fallback to
  "make it pass" — the C path must handle them.
- **Fail fast, never silently degrade.** A null-input kernel must
  produce a null result, not a crash and not a wrong value.
- **§11 shape correctness** — kernels handle constant/null/dense inputs
  via the uniform access contract; a DRAKEN_NULL constant has no data
  buffer and must not be dereferenced.
- **Broken but honest** — acceptance is the pasted output of the three
  regression repros + the telemetry assertion, not `make q` green
  (which misses all three).
- **`make c` clean before done.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/compiled/expression/compiled_expression.pyx` — line ~897 and
  the other `ctx_wrapper.ctx_ptr` reads; `_KernelContextWrapper` at ~35.
- `draken/ops/kernels/binary_op_arithmetic.cpp` (and `_other`,
  `_temporal`) — null-input handling + validity merge.
- `draken/ops/kernels/c_abi_test.cpp` — strengthen nullable cases to
  assert null propagation.
- `opteryx/expression/evaluator/evaluation.pyx` — C-native branches +
  telemetry counter.
- Test suite — extraction / param-cast / null-arithmetic value checks.

## Definition of done

- Defect 1 fixed: all extraction + parameterized-cast queries bind and
  execute; repros pasted.
- Defect 2 fixed: null arithmetic returns NULL (no SIGBUS); validity
  merge implemented; parity nullable cases assert propagation; repros
  pasted.
- Defect 3 fixed: telemetry counter present, asserted non-zero.
- Value-checked tests for the three regressed classes in a `make q`
  suite.
- `make c` clean; `make q` 100/100; `make kernel-parity` green with
  real nullable assertions; `make clickbench` reported.

## Process note

This is the sixth Phase-9 deliverable reported "done" with untested
query classes broken. The constant: **`make q` has no coverage for
extraction, parameterized casts, or null arithmetic**, so it stays
green while those crash. The durable fix is to land the value-checked
tests this ticket requires *into `make q`'s suite* — once they're
there, the next regression in these classes fails the standard gate.
Until then, acceptance must be pasted repro output, not a green build.
