# Ticket: Zero-Python Phase 9c-completion-2 — null-input arithmetic crash + telemetry counter

> Second 9c corrective. 9c-completion fixed Defect 1 (extraction
> `ctx_ptr` — `missions[0]` now works ✓) but left the other two
> untouched. This ticket is **narrow**: kill the null-arithmetic
> SIGBUS, add the telemetry counter. Nothing else.

## Status of the three 9c defects

| Defect | State |
|---|---|
| 1 — extraction `ctx_ptr` AttributeError | **FIXED** (`missions[0]`, `name[0]` execute) |
| 2 — null arithmetic → SIGBUS | **STILL BROKEN** — this ticket |
| 3 — C-native telemetry counter | **STILL MISSING** — this ticket |

## Defect 2 — null-input arithmetic crashes (runtime AND plan-time)

```
SELECT NULL + 1                                   → Bus error (SIGBUS)
SELECT id + CAST(NULL AS INTEGER) FROM $planets   → Bus error (SIGBUS)
```

The C arithmetic/bitwise kernels read `left_data[left->selection[i]]`
without checking for null-typed / all-null inputs. `NULL` is a
**DRAKEN_NULL constant** — it has no data buffer — so the kernel
dereferences a null `data` pointer → SIGBUS.

This crash now reaches **plan time** as well: `constant_folding.py:292`
folds the constant subexpression `NULL + 1` by running it through
`execute_bytecode` over a 1-row synthetic morsel. So **any query
containing a constant null-arithmetic subexpression crashes during
optimization**, before execution even starts.

This is the validity-merge defect first flagged in 9a
(`// TODO: merge validity bitmaps`, `result.validity = nullptr`),
carried into 9c, flagged in 9c-completion, and still unfixed after
three mentions. It is the hard part of the kernel work and has been
repeatedly skipped. **This ticket exists solely to close it.**

### Required behaviour

- **All-null / DRAKEN_NULL input** → all-null result, **no data
  dereference**. The kernel must detect a DRAKEN_NULL-typed (or zero-
  data-length null-constant) input and short-circuit to a null result
  without touching `data`.
- **Partial-null inputs** (a column with some null rows) → per-row
  validity merge: output row `i` is null iff either input row `i` is
  null. Compute `out.validity = left.validity AND right.validity`
  (treating NULL validity pointer as all-valid).
- Applies to: `+ - * / %`, `MyIntegerDivide`, bitwise `or/and/xor`,
  shifts, and string concat (a null operand → null output row).

### Where to fix

Two layers, decide which (surface in PR — either is acceptable if the
crash and the semantics are correct):
- **Executor short-circuit** for the all-null-constant case: in the
  9c C-native binop branch, if an input `DrakenVector` is DRAKEN_NULL
  / zero-data null-constant, produce a null `VecResult` without
  calling the kernel. Plus per-row validity merge in the kernels for
  partial-null columns.
- **Kernel-internal**: each arithmetic/bitwise kernel guards against
  null-typed inputs and merges validity. (`draken/ops/kernels/binary_op_*.cpp`.)

The executor short-circuit handles the `NULL + 1` / `id + NULL` crash
cleanly; the per-row validity merge handles partial-null columns. Both
are needed for full correctness.

## Defect 3 — C-native telemetry counter

Still absent (`grep c_native_kernel_calls` → empty). Add a counter
incremented in the C-native dispatch branch of the executor, exposed
for a test to read, asserted non-zero after a cast/binop/extraction
query. This is the regression-detector that proves a slot actually
took the C path rather than silently falling back.

## Scope

**In scope**
- Null-input handling for the C arithmetic/bitwise/concat path
  (executor short-circuit + kernel validity merge). Fix the SIGBUS at
  both runtime and plan-time (constant folding).
- The C-native telemetry counter + its assertion test.
- `make kernel-parity` nullable cases that **assert null propagation**
  (not shared-bug parity): a kernel given a null input must return a
  null-marked result; the test asserts the output validity, not just
  that C and nanobind agree.
- Value-checked tests (into a `make q` suite):
  - `SELECT NULL + 1` → `[None]`
  - `SELECT id + CAST(NULL AS INTEGER) FROM $planets LIMIT 3` → `[None,None,None]`
  - A column with mixed nulls + arithmetic → null-propagated result.
  - `SELECT 1 + 1` → `[2]` (constant fold still works, non-null).

**Out of scope**
- Defect 1 (done). Don't touch extraction.
- Function kernels — 9a-fn.
- 9d/9e/9f.

## Verification — the gate

Paste this output in the PR:
- `SELECT NULL + 1` → `[None]` (no crash).
- `SELECT id + CAST(NULL AS INTEGER) FROM $planets LIMIT 3` → `[None,None,None]`.
- mixed-null column arithmetic → correct null propagation.
- `SELECT 1 + 1` → `[2]` (plan-time constant fold, non-null, still works).
- `missions[0]` still works (Defect-1 regression check).
- telemetry counter non-zero after a binop query (asserted in a test).
- `make c` clean; `make q` 100/100; `make kernel-parity` green with
  real nullable assertions; `make clickbench` reported.

## Constraints (CLAUDE.md)

- **Fix forward — this is a crash on a path that worked pre-9c.**
- **Fail fast, never silently degrade.** Null input → null result.
  No data deref on null-typed inputs; no wrong value.
- **§11** — a DRAKEN_NULL constant has no data buffer; the uniform
  access contract must not dereference it.
- **Broken but honest** — acceptance is the pasted `NULL + 1 → [None]`
  output, not `make q` green (which has no null-arithmetic coverage).
- **`make c` clean before done.**
- **Do not commit.**

## Files (verify before editing)

- `draken/ops/kernels/binary_op_arithmetic.cpp` (`_other`, `_temporal`)
  — null-input guard + validity merge.
- `opteryx/expression/evaluator/evaluation.pyx` — C-native binop
  branch (executor short-circuit option + telemetry counter).
- `draken/ops/kernels/c_abi_test.cpp` — nullable cases asserting
  propagation.
- `opteryx/planner/optimizer/strategies/constant_folding.py:292` — the
  plan-time path that currently crashes on constant null arithmetic
  (no change here, but it's the second crash site to verify fixed).
- Test suite — null-arithmetic value checks into `make q`.

## Definition of done

- `NULL + 1` → `[None]`; `id + NULL` → all NULL; mixed-null arithmetic
  null-propagates. No SIGBUS at runtime or plan-time. Repros pasted.
- Validity merge implemented for arithmetic/bitwise/concat; parity
  nullable cases assert propagation.
- Telemetry counter present, non-zero, asserted.
- Value-checked null-arithmetic tests in a `make q` suite.
- `make c` clean; `make q` 100/100; `make kernel-parity` green;
  `make clickbench` reported.

## Process note (now unavoidable)

The null-arithmetic crash has been flagged FOUR times (9a TODO, 9c
carry-forward, 9c-completion Defect 2, here) and skipped each time
because it's the hard part. Two root enablers:
1. `make q` has **no null-arithmetic coverage**, so the crash never
   fails the standard gate.
2. Agents fix the cheap defects and report "done" on the rest.
The value-checked null tests this ticket lands in `make q` close
enabler #1 permanently. Acceptance for THIS ticket is the pasted
`NULL + 1 → [None]` — do not accept a "done" report without it.
