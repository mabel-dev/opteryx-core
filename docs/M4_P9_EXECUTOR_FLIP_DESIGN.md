# P9 — Phase-9 Executor Flip: Design

Status: **design, awaiting architect sign-off. No code cut.**
Date: 2026-06-16
Prerequisite stage for the C++-first morsel initiative (see
`docs/M4_CPP_MORSEL_DESIGN.md` Part E). Goal: make the expression bytecode VM
dispatch through nogil C kernels instead of Python closures, so the operators
that drive it (`filter`, `project`) can later run nogil.

---

## 1. The audit — what is real vs stub TODAY

Registry: `draken/ops/kernels/kernel_registry.cpp` (55 entries). Executor:
`opteryx/expression/evaluator/evaluation.pyx`. Binder:
`opteryx/compiled/expression/compiled_expression.pyx`.

### 1.1 Kernels by reality

| Group | Registered | Real? | Evidence |
|---|---|---|---|
| Cast — numeric/bool/string/most temporal (27) | yes | **REAL** | `cast_numeric.cpp`, `cast_string.cpp`, `cast_temporal.cpp` bodies |
| Cast — `date32→timestamp`, `timestamp→date32` (2) | yes | **STUB** | `cast_temporal.cpp:207,211` "not yet implemented" |
| Cast — dispatch helpers / parameterized / identity | yes | REAL | `cast_dispatch.cpp` |
| Binary arithmetic — add/sub/mul/div/mod, `binary_arith` (INT64/FLOAT64) | yes | **REAL** | `binary_op_arithmetic.cpp` (notimpl=0) |
| Binary other — bitwise ×5, `string_concat`, `ip_in_cidr` (7) | yes | **STUB** | `binary_op_other.cpp` (all "not yet implemented") |
| Binary temporal — `temporal_interval_op`, `date_minus_date`, `interval_interval_op` (3) | yes | **STUB** | `binary_op_temporal.cpp` (all stubs) |
| Extraction — map/array/json/pointer (4) | yes | **STUB** | `extraction.cpp` (all 4 "not yet implemented") |
| Function — string/arith/bool/util/hash/json/similarity/array/temporal | **NO** | bodies delegate to undefined `*_impl` | `function_*.cpp` forward-declare `vector_*_impl` (extern "C") that are **not defined**; real compute lives in `opteryx/compiled/nanobind/*.cpp` as nanobind functions |

**Key trap:** several **stub kernels are in the registry** (bitwise, temporal,
extraction). They are harmless only because the executor doesn't yet dispatch
them. The moment a branch is flipped to honour `BC_INSTR_C_NATIVE`, those stubs
would return error sentinels and break live queries. **Removing stubs from the
registry is part of the gate (§3).**

### 1.2 Executor dispatch state (`evaluation.pyx`)

| Opcode | Honours `BC_INSTR_C_NATIVE`? | Current path |
|---|---|---|
| **BC_CAST** | **YES** (`:2005` calls `(<cast_fn_t>slot.kernel_fn)(ctx, dv)`) | C-native, closure fallback |
| **BC_BINARY_OP** | NO — explicitly reverts (`:1833` "binop reverts to resolved kernel"; `:1836` calls `callable_ref`) | Python closure |
| **BC_FUNCTION** | NO (`:1877` always `callable_ref`) | Python/nanobind closure |
| **BC_EXTRACTION** | NO — hardcoded `_vector_*` nanobind dispatch on `op_code` | nanobind, GIL-held |
| Boolean combinators / compare predicate VM | n/a | already nogil (`c_and_bitmap`, `draken_compare_dv`) |

### 1.3 Binder dispatch state (`compiled_expression.pyx`) — the good news

The **registry-as-gate pattern already exists** for two of the three:

- **BC_CAST** (`:763–800`): resolves the closure into `callable_ref` AND tries
  the C kernel; sets `kernel_fn` + `BC_INSTR_C_NATIVE` only when found, keeping
  the closure as fallback. **Already gated** — the old "all-or-nothing" fear is
  not how the code actually works today.
- **BC_FUNCTION** (`:707–711`): tries `_resolve_kernel_and_context("draken_<fn>")`;
  sets `kernel_fn` + `BC_INSTR_C_NATIVE` if found, else keeps `callable_ref`.
  **Already gated** — just needs real registered kernels + an executor branch.
- **BC_BINARY_OP** (`:625`): resolves only the Python closure into `callable_ref`;
  **does NOT attempt a registry lookup.** Needs the gate added (mirror the
  function/cast pattern).
- **BC_EXTRACTION** (`:901–905`): sets `kernel_fn` + `BC_INSTR_C_NATIVE` already,
  but the executor ignores it and hardcodes nanobind calls.

**Conclusion: the "flip" is not all-or-nothing.** The registry is the single
source of truth for "is this kernel real and nogil?"; the binder already conditions
`BC_INSTR_C_NATIVE` on registry membership for cast/function. The work is (a) make
the registry honest (only-real), (b) add the missing executor branches, (c) add the
binder gate for binary-op, (d) fill in real kernels incrementally — each addition
is independently green.

---

## 2. The gate (makes P9 incremental, not big-bang)

**Invariant: the registry contains ONLY real, correct, nogil kernels.** A kernel
is registered ⟺ it produces the byte-identical answer to the closure path with the
GIL released. Corollary: `kernel_registry_lookup(name)` succeeding is the authority
to set `BC_INSTR_C_NATIVE`; a miss means "stay on the Python closure."

This converts the all-or-nothing trap into a per-kernel switch:
- Binder: for every BC_CAST / BC_BINARY_OP / BC_FUNCTION / BC_EXTRACTION, attempt
  registry lookup; set `BC_INSTR_C_NATIVE` + `kernel_fn` (+`ctx_ptr`) on hit, keep
  `callable_ref` on miss. (Cast/function already do this; add for binary-op.)
- Executor: each opcode branch checks `BC_INSTR_C_NATIVE`; if set, call `kernel_fn`
  (nogil-eligible), else run the existing closure path unchanged.
- Both paths always present → suite green regardless of how many kernels are real.

First action under the gate is **honesty-restoring, not feature work**: remove the
7 binary-other + 3 binary-temporal + 4 extraction **stub** entries from the
registry so no binder can mark them C-native before they're real. The cast gate is
already type-combo-aware: `_c_native_cast(source, target)` (`opteryx/expression/casts.py`)
is an explicit allow-list of real source→target pairs, and the binder only sets
`BC_INSTR_C_NATIVE` when that returns a hit AND the registry has the kernel
(`compiled_expression.pyx:794` keeps `callable_ref` as the fallback for every cast).
Confirm the `date32↔timestamp` stub pairs are absent from `_c_native_cast`'s
allow-list (or drop them) so they stay on the closure until real. **`_c_native_cast`
is the model for the binary-op gate in R2** — a per-(op, left_type, right_type)
allow-list, not a bare op-name lookup.

---

## 3. Work breakdown (each item independently gated: `make q` 182 / tpch 22 / clickbench 43 identical)

**P9.0 — Gate + registry honesty (no behaviour change).**
- Drop all stub entries (binary-other 7, binary-temporal 3, extraction 4,
  cast date32↔timestamp 2) from `kernel_registry.cpp`.
- Add the binder registry-lookup gate for BC_BINARY_OP (mirror BC_FUNCTION).
- Add a parity-assertion in the C-ABI test that every registered kernel returns a
  non-error VecResult on a representative input (catches "registered but stub").
- Verify that nothing newly dispatches C-native yet (only casts already do).
  Suite identical. (The `get_c_native_kernel_call_count` telemetry this step
  originally named has been DELETED: its single increment site was inside a
  binary op's all-null short-circuit, so it never measured dispatch, and that
  branch is on the Cython VM the native engine no longer runs.)

**P9.1 — Executor branch: BC_BINARY_OP C-native.**
- Add the `if (slot.flags & BC_INSTR_C_NATIVE)` branch calling
  `(<binop_fn_t>slot.kernel_fn)(ctx, dv_left, dv_right)` (typedef already exists,
  `evaluation.pyx:649`), result-adopt into the arena like BC_CAST.
- Now INT64/FLOAT64 arithmetic (the only real binary kernels) goes nogil; everything
  else stays on the closure via the gate. Differential test: C-native vs closure
  byte-identical on arithmetic-heavy queries (tpch Q01, clickbench arithmetic).

**P9.2 — Executor branch: BC_EXTRACTION C-native** (after the 4 extraction kernels
are made real — port compute from the nanobind `_vector_map_access_string` etc.).
- Replace the hardcoded `_vector_*` dispatch with `kernel_fn` dispatch under the gate.

**P9.3 — Function kernels: port `*_impl` backends to `extern "C"` + register,
incrementally by family.**
- Extract compute from `opteryx/compiled/nanobind/vector_*.cpp` into draken
  `*_impl` functions (the function_*.cpp wrappers already forward-declare them),
  following the established C′ pattern (compute in draken, nanobind = thin shim —
  see `phase_9c_cast_kernels` and `draken-consumer-edge-pattern`).
- Register each family as it lands; the executor BC_FUNCTION branch (P9.4) picks
  them up via the gate. Order families by hot-path value (string > arithmetic >
  others) and by ClickBench/tpch coverage.

**P9.4 — Executor branch: BC_FUNCTION C-native.**
- Add the `BC_INSTR_C_NATIVE` branch calling the function-kernel signature
  `VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)`;
  marshal the `arity` operands from the stack as a `const DrakenVector*` array
  (no PyObject boxing). Closure fallback for unported functions via the gate.

**P9.5 — Remaining binary/temporal kernels** (bitwise, string_concat, ip_in_cidr,
temporal arith) made real + registered, picked up automatically by P9.1's branch.
Decimal/temporal arithmetic that currently lives in closures stays closure until
a real kernel lands (the gate covers it).

**P9.6 — CASE / IIF path** (`evaluation.pyx:2062` still `callable_ref`): assess
whether the assemble-kernel can go C-native; may stay closure if low-value. Surface
to architect if it blocks full filter/project nogil.

**P9.7 — Verify the VM is fully nogil for the common path.** Add a test/telemetry
assertion that a representative filter/project workload dispatches 100% C-native
(no `callable_ref` calls) so Stage S3 (operator nogil conversion) has a clean base.

---

## 4. Decisions (LOCKED 2026-06-16)

- **R3 — port ALL function families** in P9. No hot-path-only subset; every
  family's compute moves out of `opteryx/compiled/nanobind/*.cpp` into draken
  `*_impl` `extern "C"` backends + registered. Honest note: this layer was
  *scaffolded* (the `function_*.cpp` wrappers + `*_impl` forward decls + the
  "9a-fn deferred" label exist) but never finished — the compute never left
  nanobind, so nothing was registered or dispatched. P9.3 finishes it.
- **R4 — 100% C-native is the target.** The VM must dispatch every common-path
  opcode through `kernel_fn`. Exceptions (a specific op staying on the closure)
  will be *entertained* but must be **surfaced to the architect with evidence** —
  never assumed and never self-justified. P9.7 asserts 0 `callable_ref` calls on
  the filter/project workload; any residual closure call is a defect to report,
  not to rationalize.

## 5. Risks to surface

- **R1 — Result-ownership symmetry.** C-native kernels return `VecResult` (owns
  `draken_malloc` buffers, may embed validity for string output). The BC_CAST branch
  already handles arena-adoption vs Vector-wrap; BINARY_OP/FUNCTION branches must
  follow the same `validity_embedded`/`ts_unit` handling (`vec_result.h`) exactly.
  String/array results don't arena-adopt — they wrap as Vector. No new ABI.
- **R2 — Type-coverage parity.** The closures handle promotions (narrow-int→INT64,
  int/float→FLOAT64, decimal, temporal) that some kernels don't yet. The gate makes
  this safe (unhandled combos → closure), but means the binder must register a kernel
  ONLY for the exact type-combos it correctly handles, else the gate routes a bad
  combo C-native. **Binder lookup key must encode operand types** (e.g.
  `draken_add` only marked C-native for INT64/FLOAT64 operands), not just the op name.
- **R5 — Scale of P9.3 (function porting).** Dozens of functions across 9 families,
  each extracting compute out of nanobind into a draken `*_impl` + registering it.
  This is the bulk of P9 (locked R3 = port all). Sequence by family; each family is
  independently green via the gate. Keep nanobind as the thin shim per the C′ pattern
  ([[draken-consumer-edge-pattern]]) — do not duplicate compute.
