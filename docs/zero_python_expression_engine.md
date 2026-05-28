# Zero-Python Expression Engine — Delivery Plan

> Status: DRAFT. Architect-approved goal; phasing and decisions captured here for
> implementation. Companion to the audit in this same directory.

## 0. Goal (architect, 2026-05-27)

> **Plan time**: Python compiles the expression to bytecode.
> **Execute time**: the engine executes the compiled bytecode with **zero Python**.

There are exactly two phases. Anything Python-shaped at execute time is debt
this plan retires.

### Decisions

1. **UDFs are not supported.** No `BC_PYFUNC` opcode, no slow path, no Python
   callable invocation per morsel. Functions are exclusively built-in kernels
   we ship.
2. **Built-in kernels are invoked through a C function-pointer registry.** No
   `PyObject_Call` per morsel for our own functions. The nanobind / cpdef
   call protocol is removed from the execute-time path entirely.
3. **No separate evaluation engine for the optimizer.** The constant-folding
   pass switches to `execute_bytecode` over a synthetic 1-row morsel. The
   tree-walker (`_eval_value` / `evaluate_draken` and family) is deleted.

## 1. The boundary

```
                       Python (plan time)                C/C++ (execute time)
        ┌──────────────────────────────────────┐  ┌──────────────────────────────┐
SQL ──► │ parse → bind → optimize → compile_to │─►│ execute_bytecode(bc, morsel) │─► morsel
        │ bytecode                             │  │                              │
        └──────────────────────────────────────┘  └──────────────────────────────┘
                                                  ▲                              ▲
                                                  │   GIL released on entry      │
                                                  │   No Python ops per row      │
                                                  │   No Python ops per morsel   │
                                                  │   No PyObject_Call           │
                                                  │   No getattr / isinstance    │
                                                  │   No string dispatch         │
                                                  └──────────────────────────────┘
                                                  morsel handle in,  morsel handle out
                                                  (Python objects wrapping C data —
                                                   touched only at entry/exit)
```

**Inside the boundary**, the executor sees:
- A `CompiledBytecode*` — array of `BytecodeInstr` C structs.
- A `Morsel*` — array of `DrakenVector*`.
- A stack of `DrakenVector*` slots + a C function-pointer-indexed kernel table.

Nothing else. Specifically, **not**:
- Python callable objects in `slot.callable_ref`. After this work the field
  holds a C function pointer.
- Python wrappers (`MapAccessOp`, `ArrowOp`, `LongArrowOp`, `_inner` cast
  closure, etc.). Deleted.
- Python type-name strings on the hot path (`array.type.name in (...)`, etc.).
  Type information is encoded as integer codes in the slot struct.
- The Cython `Vector` shim on the executor's stack. Stack slots are
  `DrakenVector*` only. The shim survives at the morsel-boundary for Python
  consumers; the executor never touches it.

## 2. Starting state (verified 2026-05-27)

Reference: the audit in this directory documents every per-morsel Python call
in the engine today. Headline items:

- **BC_COMPARE fallback** — `_nb_vec_unwrap(v)` (getattr per call),
  `_wrap_nb_bool_result` (isinstance per call), `draken_compare()`
  string-op dispatch when `op_code == OP_UNKNOWN`.
- **BC_BINARY_OP non-arithmetic** — `_binary_op_from_vecs` → `call_arithmetic_op`
  → kernel-registry dict lookup → Python callable invocation.
- **BC_UNARY_OP IS NULL** — `_is_null_as_boolvector` does
  `type(vec).__module__.startswith(...)`, a 3-attr `getattr` chain, an
  `isinstance(vec, _FIXED_BUFFER_VECTORS)`, and a `vec.null_bitmap()` Python
  method call per evaluation.
- **BC_FUNCTION** — `callable_obj(*args)` via Python call protocol; the
  callable is a Python object (nb_func or cpdef). After: `type(result).__name__ == "Vector"`
  + conditional wrap.
- **BC_EXTRACTION** — `MapAccessOp` / `ArrowOp` / `LongArrowOp` Python `def`
  wrappers. `_json_key_constant` re-derives a known-at-bind-time key per
  morsel. Type-name string dispatch per call.
- **BC_CAST** — Python `_inner` closure with per-morsel string-keyed type
  dispatch on `_type`. For non-vector inputs, a row-loop list comprehension.
- **BC_CASE** — `_sub_morsel` does per-column Python `.take()` per branch.
  `_assemble` does `isinstance` / `getattr` output-type dispatch.
- **Tree-walker survivors** — `_eval_value`, `evaluate_draken`,
  `evaluate_and_append_draken`, `apply_bounded_function`, legacy `evaluate_case`,
  `_unary_draken`, `_eval_cast_draken`, `_eval_function_draken`,
  `_eval_binary_op_draken`. **Unreachable from `execute_bytecode`**; only
  entered via `opteryx.expression.evaluate()` from
  `planner/optimizer/strategies/constant_folding.py` at plan time.

## 3. Invariants (binding for every phase)

- **`execute_bytecode` signature stays stable**: `(CompiledBytecode bc, Morsel morsel) -> Vector`.
  Caller-visible contract does not change.
- **`make q` passes at the end of every phase.** A phase that breaks regression
  does not land. Per-phase gates listed below are mandatory, not aspirational.
- **No new Python on the execute-time path.** Adding `getattr` / `isinstance`
  / string compares in this region during a phase is a hard reject — phases
  remove Python, never add it.
- **CLAUDE.md §3, §11** — Cython code must be typed, no `object` parameters
  on hot paths; vector shape correctness must hold for every kernel (uniform
  `data[selection[i]]` access).
- **No Python list / dict / set passed through the executor** — the previously
  shipped fix to `vector_map_access_array` is the precedent. Same rule for
  every kernel result.

## 4. Phased delivery

Eight phases. Each ships independently behind `make q`. Phases 1–5 are
sub-day to 2 days each; Phase 6 is the largest single piece; Phases 7–8 finish
the executor and delete the tree-walker.

### Phase 1 — Result-wrap cleanup *(sub-day)*

**Scope**
- BC_COMPARE: replace `_nb_vec_unwrap(v)` (getattr) with the typed
  `(<Vector>x)._nb` pattern used elsewhere in the executor.
- BC_COMPARE: drop the `_wrap_nb_bool_result` isinstance — wrap unconditionally
  at the boundary; the kernel return type is known.
- BC_FUNCTION: encode the result-wrap class (BoolVector vs Vector) in
  `slot.flags` at bind time. Delete `type(result).__name__ == "Vector"`.
- BC_CAST / BC_CASE / BC_EXTRACTION: same — eliminate the per-morsel
  isinstance check by encoding result type in the slot.

**Files**
- `opteryx/expression/evaluator/comparisons.pyx`
- `opteryx/expression/evaluator/evaluation.pyx` (executor)
- `opteryx/compiled/expression/compiled_expression.pyx` (bind-time encode)

**Exit**
- `make q` 100/100.
- Zero `getattr` / `isinstance` calls in BC_COMPARE / BC_FUNCTION /
  BC_CAST / BC_CASE / BC_EXTRACTION result-handling regions.

### Phase 2 — `_is_null_as_boolvector` C rewrite *(sub-day)*

**Scope**
- Rewrite `_is_null_as_boolvector` as a C-level kernel reading
  `dv->validity` directly and emitting a BoolVector. Three cases by
  `dv->validity`: NULL (all valid), present (copy + invert), and the
  dictionary-encoded legacy branch — **delete** the dict branch; legacy on
  modern draken.
- Update BC_UNARY_OP IS NULL / IS NOT NULL to call the new kernel directly.
- Delete `_dictionary_arrow_type` and `_is_dictionary_encoded_vector` — both
  dead code on modern vectors.

**Files**
- `opteryx/expression/evaluator/type_coercion.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`

**Exit**
- IS NULL / IS NOT NULL produces zero `getattr` / `isinstance` / module-name
  inspection per morsel.
- `make q` 100/100; clickbench ≥ baseline (IS NULL is in the hot path of
  several queries).

### Phase 3 — BC_EXTRACTION bind-time resolution *(sub-day)*

**Scope**
- Add `slot.left_type_code` semantics for the operand's vector-type at bind
  time. Bind-time `_NT_EXTRACTION_OPERATOR` selects the resolved C kernel
  from `(op, operand_type)` and stores the pointer directly in
  `slot.callable_ref`.
- Store the pre-extracted scalar key (bytes for JSON, int for map-access) in
  `slot.literal_obj` as a raw scalar — no length-1 key vector built at bind
  time, no `key[0]` per morsel.
- BC_EXTRACTION executor: direct C call. The Python wrappers
  `MapAccessOp` / `ArrowOp` / `LongArrowOp` are **deleted**.
- Resolution table — one entry per supported `(extraction_op, operand_type)`:
  - `(MapAccess, VARCHAR|NVARCHAR|VARBINARY)` → `vector_map_access_string`
  - `(MapAccess, ARRAY)` → `vector_array_map_access` (already native)
  - `(Arrow, VARCHAR|VARBINARY)` → `vector_json_extract`
  - `(LongArrow, VARCHAR|VARBINARY)` → `vector_json_extract`

**Files**
- `opteryx/compiled/expression/compiled_expression.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`
- `opteryx/expression/binary_operators.pyx` — delete `MapAccessOp`,
  `ArrowOp`, `LongArrowOp` (or reduce to bind-time-only resolution helpers)

**Exit**
- BC_EXTRACTION executor body fits in ≤ 10 lines: unwrap, direct call, store.
- `make q` 100/100.

### Phase 4 — BC_COMPARE string-op elimination *(~1 day)*

**Scope**
- Pre-resolve **every** `op_str` → `op_code` at bind time
  (`compiled_expression.pyx`). The set of compare ops is fixed and small.
- Delete `draken_compare()` (the string-keyed cpdef variant) entirely.
  Only `draken_compare_int(op_code, …)` survives.
- `OP_UNKNOWN` becomes unreachable — assert at bind time that resolution
  succeeded; fail-fast if not.

**Files**
- `opteryx/compiled/expression/compiled_expression.pyx`
- `opteryx/expression/evaluator/comparisons.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`

**Exit**
- Zero string-keyed dispatch in BC_COMPARE.
- `make q` 100/100.

### Phase 5 — BC_CAST closure specialisation *(~1–2 days)*

**Scope**
- Replace the `cast()` factory's return (a Python `_inner` closure) with a
  bind-time **resolver**: given `(source_type, target_type, args)`, return a
  C function pointer to the specialised kernel.
- Implement the resolution table for every supported cast pair. Each entry
  is a C function symbol (some already exist as nanobind / cpdef:
  `vector_cast_int64_to_string`, `vector_cast_bool_to_string`,
  `vector_cast_int64_to_timestamp`, `vector_cast_string_to_float64`, etc.).
- For casts currently implemented as per-row Python loops
  (DECIMAL, generic `OrsoTypes[t].parse`), either:
  - Add a native vector cast kernel; or
  - Surface the missing-kernel case as a bind-time error
    ("unsupported cast"). Per the architect's "fail fast" rule the right
    answer is the kernel, not a runtime fallback.
- BC_CAST executor: direct C call; no Python closure.
- Delete `cast()` factory's `_inner` closure entirely.

**Files**
- `opteryx/expression/casts.pyx` (becomes thin: a resolver + the cast
  registry)
- `opteryx/compiled/expression/compiled_expression.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`

**Exit**
- BC_CAST executor body fits in ≤ 10 lines.
- All previously working casts still work; unsupported casts fail at bind
  time with a clear message.
- `make q` 100/100.

### Phase 6 — Arithmetic kernel registry → C table *(~3–5 days)*

**Scope**
- Replace `OPERATOR_FUNCTION_MAP` (Python dict keyed on op-string) and
  `call_arithmetic_op` (Python dict lookup + Python callable invocation)
  with an indexed C array of function pointers.
- Dimension the table on `[op_code][left_type_code][right_type_code]`. Op
  set is fixed (`Plus`, `Minus`, `Multiply`, `Divide`, `Modulo`,
  `MyIntegerDivide`, `StringConcat`, `BitwiseOr/And/Xor`, `ShiftLeft/Right`).
- The bind-time compiler looks up the kernel pointer and stores it in
  `slot.callable_ref` (already typed as a function pointer per Phase 5
  groundwork).
- Delete `_binary_op_from_vecs`, `call_arithmetic_op`, `binary_operations`
  and `OPERATOR_FUNCTION_MAP`.
- The interval-arithmetic side-table (`INTERVAL_KERNELS`) becomes part of
  the same C-indexed table.

**Files**
- `opteryx/expression/evaluator/arithmetic.pyx`
- `opteryx/expression/evaluator/arithmetic_dispatch.pyx` — likely deleted
  in full, contents absorbed
- `opteryx/expression/binary_operators.pyx` — reduce to bind-time resolver
- `opteryx/compiled/expression/compiled_expression.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`

**Exit**
- Zero Python dict lookups on the binary-op execute path.
- Zero Python callable invocations on the binary-op execute path.
- `make q` 100/100; clickbench ≥ baseline.

### Phase 7 — CASE inner-loop natives *(~2 days)*

**Scope**
- `_sub_morsel` becomes a C function taking a column-index array and a
  row-index buffer, performing a single multi-column take in C and
  returning a `Morsel*`. No Python loop over column names; no per-column
  Python `.take()`.
- `_assemble` output type is resolved at bind time from the THEN/ELSE
  expressions' bound types. The runtime `isinstance(first, BoolVector)` /
  `getattr(first, "type", None)` dispatch is gone.
- `build_case_fn`'s closure becomes a struct holding compiled-bytecode
  pointers + the pre-resolved assembly kernel pointer. BC_CASE executor
  reads the struct and dispatches in C.

**Files**
- `opteryx/expression/evaluator/case_eval.pyx`
- `opteryx/compiled/vector_ops/case_helpers.*` (or wherever `assemble_*`
  helpers live)
- `opteryx/compiled/expression/compiled_expression.pyx`
- `opteryx/expression/evaluator/evaluation.pyx`

**Exit**
- BC_CASE executor body: dispatch through the pre-resolved struct, no
  Python ops.
- `make q` 100/100.

### Phase 8 — Nogil executor + tree-walker deletion *(~2–3 days)*

**Scope (part A — executor nogil)**
- Annotate `execute_bytecode` inner loop as `nogil`. Release the GIL on
  function entry, re-acquire only at the morsel-boundary (vector
  construction at return).
- Validate via thread-sanitizer (or single-threaded benchmark) that no
  Python-protocol calls remain inside the nogil region. The compiler will
  enforce most of it — any `object` access inside `with nogil:` is a hard
  error.

**Scope (part B — tree-walker deletion)**
- Switch `planner/optimizer/strategies/constant_folding.py` to use
  `execute_bytecode` against a synthetic 1-row morsel. Constant folding's
  current call chain — `opteryx.expression.evaluate()` →
  `evaluate_and_append_draken` → `_eval_value` / `evaluate_draken` — is
  replaced with `build_bytecode(lower(node))` + `execute_bytecode(bc, morsel)`.
- Delete (all in `opteryx/expression/evaluator/evaluation.pyx`):
  - `_eval_value` and its `cdef` helpers
  - `evaluate_draken` (cpdef)
  - `evaluate_and_append_draken`
  - `_unary_draken`, `_eval_cast_draken`, `_eval_function_draken`,
    `_eval_binary_op_draken`
- Delete `apply_bounded_function` (`opteryx/expression/evaluator/function_execution.pyx`)
  if no plan-time caller remains; otherwise leave behind a single 5-line
  cpdef wrapper for that one caller.
- Delete the legacy `evaluate_case` entry in `case_eval.pyx`; keep
  `build_case_fn` / `_decide_compiled` / `_compute_compiled`.
- Update `opteryx/expression/__init__.pyx`'s `_inner_evaluate` and the
  module-level `evaluate` function to route through bytecode (or delete
  them if no plan-time caller remains after the constant-folding switch).
- BC_LEGACY opcode + `_NT_LEGACY` fallback in
  `compiled_expression.pyx:_linearize` — delete. Bind time must produce a
  fully compiled bytecode or fail explicitly; runtime fallback is gone.

**Exit**
- `execute_bytecode` body runs nogil end-to-end (GIL acquired only at
  return for vector construction).
- ~700 lines of tree-walker code deleted.
- `make q` 100/100; `make clickbench` ≥ baseline + a measurable improvement
  on hashed-join-heavy queries (nogil unlocks operator-level parallelism
  the GIL currently blocks).

## 5. Risks

1. **Cast kernel coverage gap.** Phase 5 may surface casts that have no
   native vector kernel today (e.g. DECIMAL parse). Resolution: enumerate
   the missing pairs in Phase 5 prep; either implement the kernels or
   confirm with the architect that we can fail at bind time. Do not
   reintroduce a per-row Python fallback.
2. **Arithmetic kernel-table dimensionality.** `[op][left_type][right_type]`
   has gaps (most int×int paths exist, but e.g. `decimal128 + interval`
   may not). Pre-flight in Phase 6: enumerate every populated cell in
   `OPERATOR_FUNCTION_MAP` today and ensure parity in the C table.
3. **Nogil correctness.** Cython is unforgiving — any accidental Python op
   inside `with nogil:` is a compile error, but a release of the GIL
   followed by a deeply-nested code path can hide a re-entry. Validate with
   a focused stress test (many queries × concurrent workers) at Phase 8.
4. **Constant-folding semantics.** Switching to `execute_bytecode` over a
   1-row synthetic morsel must produce **identical** values for every
   foldable constant subexpression. Add a parity test (old `evaluate()` vs
   new `execute_bytecode`) at Phase 8 entry; remove `evaluate()` only after
   parity is green on the full SLT corpus.
5. **`make q` regressions appearing late.** Phase 6 touches the largest
   code surface. Land it behind a phase-flag branch if necessary; do not
   merge a half-converted arithmetic registry.

## 6. Out of scope

- Adding new SQL features (decimal128 promotion, new function kernels,
  parameterised types) — separate work.
- Optimising the bind-time compiler itself — Python at plan time is fine.
- Operator-level parallelism that nogil unlocks — Phase 8 makes it possible;
  realising it is a follow-on.
- The Cython `Vector` shim at the Python boundary — survives this work
  unchanged; only the executor stops touching it.

## 7. Definition of done

- `execute_bytecode` runs nogil end-to-end. The Cython compiler is the
  proof: `with nogil:` over the dispatch loop refuses to compile if any
  Python op remains.
- The audit document's hot-path Python inventory has zero remaining items.
  Tree-walker section is empty (file deleted).
- `make q` 100/100; `make clickbench` ≥ baseline.
- Build no longer references: `_eval_value`, `evaluate_draken`,
  `evaluate_and_append_draken`, `apply_bounded_function`, legacy
  `evaluate_case`, `_inner` cast closure, `MapAccessOp`, `ArrowOp`,
  `LongArrowOp`, `call_arithmetic_op`, `OPERATOR_FUNCTION_MAP`,
  `binary_operations`, `_binary_op_from_vecs`, `_dictionary_arrow_type`,
  `_is_dictionary_encoded_vector`, BC_LEGACY opcode, `_NT_LEGACY` fallback.

## 8. Post-Phase-8b/8c state

After Phase 8b/8c (tree-walker deletion complete), the remaining Python on the
execute-time path is exactly these five per-morsel `PyObject_Call` sites:

| Opcode         | Call                                     | Location                                   |
|----------------|------------------------------------------|--------------------------------------------|
| BC_FUNCTION    | `callable_obj(*args)`                    | `evaluation.pyx:2363/2370/...`             |
| BC_EXTRACTION  | `(<object>slot.callable_ref)(...)`       | `evaluation.pyx:2515-2522` (one of 4)      |
| BC_CAST        | `(<object>slot.callable_ref)(py_left)`   | `evaluation.pyx:2544`                      |
| BC_CASE        | `(<object>slot.callable_ref)(morsel)`    | `evaluation.pyx:2565`                      |
| BC_BINARY_OP   | `(<object>slot.callable_ref)(left, right)` | `evaluation.pyx:2386`                    |

All five share the same shape: one `PyObject_Call` per opcode invocation per
morsel. The kernels they invoke are nanobind C++ functions — the Python boundary
is the call protocol, not the kernel body.

Phase 9 (executor `nogil` end-to-end) targets these five sites by replacing each
`PyObject_Call` with a C function-pointer invocation via a kernel ABI. The
inventory above catalogs them for scoping.

## 9. Open items for the architect

- [ ] Confirm Phase 5 stance on missing-cast kernels: implement vs.
      bind-time error. Default is "implement"; flag only the casts where
      a real implementation would be disproportionate.
- [ ] Confirm Phase 8 stance on `opteryx.expression.evaluate()` — delete
      outright (replace constant-folding callers) or keep as a thin shim
      over `execute_bytecode`?
- [ ] Phase 9: executor `nogil` end-to-end via C function-pointer kernel ABI.
      Design and feasibility analysis for the five remaining per-morsel
      `PyObject_Call` sites.
