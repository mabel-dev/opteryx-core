# Ticket: Zero-Python Expression Engine — Phase 3 (BC_EXTRACTION bind-time resolution)

> Part of the plan in `docs/zero_python_expression_engine.md`. Read it
> first. Phases 1 and 2 have landed
> (`docs/tickets/zero-python-phase-1-result-wrap.md`,
> `docs/tickets/zero-python-phase-2-is-null.md`). This is Phase 3.

## Problem

BC_EXTRACTION today calls a Python wrapper (`MapAccessOp` / `ArrowOp` /
`LongArrowOp` in `opteryx/expression/binary_operators.pyx`) per morsel.
Each wrapper:

1. Does runtime type-name dispatch:
   - `key.type.name not in ("INT64", "INT32", "INT16", "INT8")` (MapAccess)
   - `key.type.name not in ("VARCHAR", "NVARCHAR", "VARBINARY")` (Arrow/LongArrow,
     inside `_json_key_constant`)
   - `array.type.name in ("VARCHAR", "NVARCHAR", "VARBINARY")` vs `"ARRAY"`
     (MapAccess)
2. Re-extracts a constant key per morsel via `key[0]` (Python `__getitem__`
   on a length-1 vector) — the key value was known at bind time.
3. Calls the resolved native kernel.

All of step 1 and step 2 is per-morsel Python on the hot path. The native
kernels (`vector_map_access_string`, `vector_array_map_access`,
`vector_json_extract`) already exist and are pure C++/nanobind — no kernel
work is needed.

Today's bind-time emit in `opteryx/compiled/expression/compiled_expression.pyx`
(`_NT_EXTRACTION_OPERATOR` around line 594) stores the **wrapper** in
`slot.callable_ref`, then a pre-built length-1 key vector in
`slot.literal_obj`. After Phase 3 it stores the **resolved native kernel**
and a sub-op flag identifying which kernel shape — and the executor calls
that kernel directly.

## Goal

After Phase 3:
- The Python wrappers `MapAccessOp` / `ArrowOp` / `LongArrowOp` (and
  `_json_key_constant`) are deleted from `binary_operators.pyx`.
- BC_EXTRACTION executor body is a small switch on a sub-op flag and a
  direct call to the resolved native kernel — no Python wrapper invocation.
- No runtime `.type.name` lookups, no runtime `key[0]` indexing.
- The `EXTRACTION_OPERATORS` set entry in `binary_operators.pyx` (line 183)
  remains for the *binder*'s benefit (it identifies these op-strings); the
  executor never imports it.

## Scope

**In scope**
- `opteryx/compiled/expression/compiled_expression.pyx` — rewrite the
  `_NT_EXTRACTION_OPERATOR` emit (~line 594) to:
  - Read the operand's vector-type from `node.left.schema_column.type`
    (via `OrsoTypes`) or `node.left.value`'s `.type`.
  - Resolve the kernel + sub-op flag at bind time. Fail-fast (raise
    `ValueError` or `IncorrectTypeError`) if the operand type doesn't
    match a supported kernel.
  - For MapAccess on ARRAY: store the scalar `int(extr_key)` in
    `slot.bool_value` (re-purposed for int storage — see *Carrying the
    scalar* below) or in `slot.literal_obj` as a Python `int`. **Surface
    your choice in the PR** — both work.
  - For MapAccess on string: keep the length-1 INT64 key vector pattern
    (kernel needs a vector). Store in `slot.literal_obj`.
  - For Arrow / LongArrow: store the **raw key bytes** as a Python `bytes`
    object in `slot.literal_obj`. No length-1 string vector.
- `opteryx/expression/evaluator/evaluation.pyx` BC_EXTRACTION executor
  (~line 2439) — replace the single call site with a switch on the new
  sub-op flag and direct calls to the four resolved kernels.
- `opteryx/expression/binary_operators.pyx` — delete `MapAccessOp` (~line
  52), `ArrowOp` (~line 40), `LongArrowOp` (~line 46),
  `_json_key_constant` (~line 30). Remove their entries from
  `OPERATOR_FUNCTION_MAP` (lines 177–179). `EXTRACTION_OPERATORS` (line
  183) **stays** — it's used by the binder.
- `opteryx/expression/evaluator/evaluation.pyx:283` (tree-walker
  `_eval_value`'s NT_EXTRACTION_OPERATOR branch) — also calls
  `MapAccessOp` etc.; update to call the same resolution logic. Tree-walker
  is plan-time only (constant folding) but kept correct.

**Out of scope**
- Phase 1 still-pending result-wrap of CAST/CASE/EXTRACTION using bind-time
  flag unconditionally — the runtime `isinstance` gate is fine here for
  now (the kernels DO return consistent nanobind Vectors so technically
  this gate could go, but the precedent is set in the other two opcodes;
  leaving it consistent is fine until Phase 5/7 land).
- BC_FUNCTION / BC_CAST / BC_CASE — separate phases.
- Annotating the executor `nogil` — Phase 8.

## Sub-op flag design

Add to `compiled_expression.pyx` (alongside `BC_RESULT_*` flags):

```cython
# BC_EXTRACTION sub-op codes (stored in slot.op_code, lower 8 bits):
DEF BC_EXTR_MAP_STRING = 1   # vector_map_access_string(vec, key_vec_int64)
DEF BC_EXTR_MAP_ARRAY  = 2   # vector_array_map_access(vec, key_int64)
DEF BC_EXTR_JSON_PTR   = 3   # vector_json_extract(vec, key_bytes)  ['->']
DEF BC_EXTR_JSON_KEY   = 4   # vector_json_extract(vec, key_bytes)  ['->>']
```

Use `slot.op_code` to carry the sub-op — it's a free int field on
`BytecodeInstr` (currently only used by BC_COMPARE / BC_BINARY_OP /
BC_UNARY_OP / BC_BETWEEN, which aren't BC_EXTRACTION; verify there's no
clash). Don't pile this into `slot.flags` — `flags` already carries the
result-wrap bits from Phase 1.

Note: `JSON_PTR` and `JSON_KEY` both currently dispatch to
`vector_json_extract`, but they're separated here so a future split (e.g.
`->>` becoming text-stripped) doesn't need a new opcode.

## Carrying the scalar (MapAccess on ARRAY)

`vector_array_map_access` takes `(vec_obj, int64 index)`. The scalar
index needs to land somewhere on the slot. Two options:

**Option A** — store the int in `slot.literal_obj` as a Python `int`:
```cython
# bind time:
slot.literal_obj = <PyObject*>(<object>int(extr_key))
bc._hold(<object>slot.literal_obj)
# runtime:
legacy_result = _vector_array_map_access(py_left_nb, <object>slot.literal_obj)
```

**Option B** — repurpose `slot.bool_value` (currently `int` for BC_FUNCTION
nb_func flag, unused for BC_EXTRACTION). Store the int directly:
```cython
# bind time:
slot.bool_value = <int>int(extr_key)   # signed 32-bit on most platforms
# runtime:
legacy_result = _vector_array_map_access(py_left_nb, <int64_t>slot.bool_value)
```

Option B is faster (no PyObject wrap/unwrap) but limits the index to int32
range. Array indices in practice are tiny; signed int32 is plenty.
**Recommendation: Option B.** Verify `slot.bool_value` is `int` (32-bit) in
`compiled_expression.pxd` — if it's wider, even better. Surface the
choice in the PR.

## Executor (rewritten BC_EXTRACTION)

```cython
if opcode == BC_EXTRACTION:
    sp -= 1
    dv_left_ptr = dv_stack[sp]
    py_left = _slot_to_pyobj(dv_left_ptr, anchor[sp], arena)
    # Unwrap Cython shim to nanobind Vector for the native kernel call.
    if isinstance(py_left, Vector):
        py_left_nb = (<Vector>py_left)._nb
    else:
        py_left_nb = py_left

    if slot.op_code == BC_EXTR_MAP_STRING:
        legacy_result = _vector_map_access_string(py_left_nb, <object>slot.literal_obj)
    elif slot.op_code == BC_EXTR_MAP_ARRAY:
        legacy_result = _vector_array_map_access(py_left_nb, <int64_t>slot.bool_value)
    elif slot.op_code == BC_EXTR_JSON_PTR:
        legacy_result = _vector_json_extract(py_left_nb, <object>slot.literal_obj)
    elif slot.op_code == BC_EXTR_JSON_KEY:
        legacy_result = _vector_json_extract(py_left_nb, <object>slot.literal_obj)
    else:
        raise NotImplementedError(f"BC_EXTRACTION: unknown sub-op {slot.op_code}")

    # Result wrap — same Phase 1 pattern.
    if (slot.flags & BC_RESULT_NEEDS_NB_WRAP) and not isinstance(legacy_result, Vector):
        if slot.flags & BC_RESULT_WRAP_AS_BOOL:
            legacy_result = BoolVector(legacy_result)
        else:
            legacy_result = Vector(legacy_result)
    anchor[sp] = legacy_result
    if isinstance(legacy_result, Vector):
        dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
    else:
        dv_stack[sp] = NULL
    sp += 1
    continue
```

The `if isinstance(py_left, Vector)` on the input side is the one
acknowledged-residual isinstance from Phase 1 (scalar-vs-Vector
distinction can only go away when the executor stack carries a typed
"slot kind" tag — Phase 4 / Phase 8 territory). Leave it.

The native kernels are imported at the top of `evaluation.pyx`:
```cython
from opteryx.compiled.nanobind.vector_special import vector_map_access_string as _vector_map_access_string
from opteryx.compiled.nanobind.vector_json import vector_json_extract as _vector_json_extract
from draken.draken_native import vector_array_map_access as _vector_array_map_access
```

Verify these symbols are already imported there (they're imported in
`binary_operators.pyx` today — moving the imports is the right cleanup).

## Operand type resolution at bind time

In `_NT_EXTRACTION_OPERATOR`:

```cython
# Already in scope:
extr_op_str = <object>node.value           # "MapAccess" / "Arrow" / "LongArrow"
extr_key    = <object>node.right.value     # the literal key value

# New: resolve operand type from schema_column on the left operand node.
left_node = <object>node.left           # the Python Node
left_sc   = left_node.schema_column     # binder-populated SchemaColumn
left_orso = left_sc.type                # OrsoTypes enum

# Sub-op + kernel selection:
if extr_op_str == "MapAccess":
    if left_orso == _OrsoTypes_ARRAY:
        sub_op = BC_EXTR_MAP_ARRAY
        slot.bool_value = <int>int(extr_key)
    elif left_orso in _STRING_FAMILY:   # VARCHAR / NVARCHAR / BLOB
        sub_op = BC_EXTR_MAP_STRING
        key_vec = _draken_native.vector_from_constant(int(extr_key), 1)
        bc._hold(key_vec)
        slot.literal_obj = <PyObject*>key_vec
    else:
        raise IncorrectTypeError(
            f"MapAccess: operand must be ARRAY or string family; got {left_orso!r}"
        )
elif extr_op_str == "Arrow":
    if left_orso not in _STRING_FAMILY:
        raise IncorrectTypeError(
            f"-> requires a string/JSON operand; got {left_orso!r}"
        )
    sub_op = BC_EXTR_JSON_PTR
    key_bytes = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
    bc._hold(key_bytes)
    slot.literal_obj = <PyObject*>key_bytes
else:  # LongArrow
    if left_orso not in _STRING_FAMILY:
        raise IncorrectTypeError(
            f"->> requires a string/JSON operand; got {left_orso!r}"
        )
    sub_op = BC_EXTR_JSON_KEY
    key_bytes = extr_key if isinstance(extr_key, bytes) else extr_key.encode("utf-8")
    bc._hold(key_bytes)
    slot.literal_obj = <PyObject*>key_bytes
```

Define `_STRING_FAMILY` near `_ensure_orso_types()`:
```cython
cdef tuple _STRING_FAMILY = ()  # filled in _ensure_orso_types
# inside _ensure_orso_types:
_STRING_FAMILY = (_OrsoTypes_VARCHAR, _OrsoTypes_NVARCHAR, _OrsoTypes_BLOB)
```

Look up the exact `OrsoTypes` enum names in
`opteryx/types/__init__.py` or wherever — they may be `VARCHAR / BLOB` or
something else. Verify; do not guess.

If `node.left.schema_column` is `None` (binder failure), raise — do not
fall back to runtime dispatch.

## Verification

- `make c` clean.
- `make q` 100/100 (currently 137/137).
- `grep -nE 'MapAccessOp|ArrowOp|LongArrowOp|_json_key_constant'`
  in `opteryx/expression/binary_operators.pyx` — only `EXTRACTION_OPERATORS`
  set entries (string literals) should remain.
- `grep -rn 'MapAccessOp\|ArrowOp\|LongArrowOp\|_json_key_constant' opteryx/`
  — should return zero non-string-literal matches.
- Spot tests:
  - `SELECT missions[0] FROM testdata.astronauts LIMIT 5` (ARRAY MapAccess)
  - `SELECT missions[-1] FROM testdata.astronauts LIMIT 5` (negative index)
  - `SELECT missions[10] FROM testdata.astronauts LIMIT 5` (OOB → all NULL)
  - `SELECT name[0] FROM $planets LIMIT 5` (string MapAccess — single
    character extraction)
  - A JSON `->` query if a JSON column is available in test data; if not,
    construct one inline (`SELECT '{"a":1}'::JSON -> 'a'`). Verify the
    extraction syntax matches the binder.
  - `SELECT LENGTH(missions[0]) FROM testdata.astronauts LIMIT 3` — the
    chained-expression test (Phase 1's regression case).
- Microbench: time `SELECT missions[0] FROM testdata.astronauts` on the
  full table. Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No Python on hot path.** The executor's BC_EXTRACTION region must
  contain zero `getattr` / `type(...).__name__` / `.type.name` /
  `key[0]` Python operations. Same `grep` constraint as Phases 1/2.
- **No fallbacks.** Bind-time must resolve a kernel or fail. There is no
  "if unresolvable, fall back to Python wrapper at runtime" path.
- **Fail fast.** If `schema_column` is missing on the left operand at
  bind time, raise immediately — this is a planner bug.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **Cython code must be typed.** New cdef locals where needed.
- **Do not commit.**

## Files (verify before editing)

- `opteryx/compiled/expression/compiled_expression.pyx` — `_NT_EXTRACTION_OPERATOR`
  emit at ~line 594. Verify with `grep -n '_NT_EXTRACTION_OPERATOR'`.
- `opteryx/compiled/expression/compiled_expression.pxd` — confirm
  `BytecodeInstr.op_code` and `.bool_value` fields are present and of the
  right size (sub-op flag uses `op_code`; integer key uses `bool_value`).
- `opteryx/expression/evaluator/evaluation.pyx` — BC_EXTRACTION at ~line
  2439; also `_eval_value`'s NT_EXTRACTION_OPERATOR branch at ~line 283.
- `opteryx/expression/binary_operators.pyx` — full file. Most of it
  (lines 30–66) is being deleted. `_dispatch_arithmetic_operation`,
  `binary_operations`, `_unsupported_bitwise_op` and
  `OPERATOR_FUNCTION_MAP` stay (Phase 6 will retire those).
- `opteryx/expression/evaluator/__init__.py` / `.pyx` — verify nothing
  re-exports `MapAccessOp` / `ArrowOp` / `LongArrowOp`. If it does, drop
  the re-exports.

## Tests

- `make q` (137/137).
- All five spot queries above produce correct results.
- Regression-check: confirm the Phase 1 chained-expression queries still
  work:
  - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` → `[7, 5, 5]`
  - `SELECT UPPER(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` → `['MERCURY', 'VENUS', 'EARTH']`

## Pre-flight reading

1. `docs/zero_python_expression_engine.md` — the plan.
2. `docs/tickets/zero-python-phase-1-result-wrap.md` — the bind-time-flag
   precedent and the `BC_RESULT_*` bit pattern.
3. `docs/tickets/zero-python-phase-2-is-null.md` — the typed-cdef-kernel
   precedent.
4. `opteryx/expression/binary_operators.pyx` — read all of it. Lines 1–66
   are being deleted; the rest stays.
5. `opteryx/compiled/nanobind/vector_special.cpp` — `vector_map_access_string`
   signature.
6. `opteryx/compiled/nanobind/vector_json.cpp` — `vector_json_extract`
   signature.
7. `draken/draken_native.cpp` — search for `vector_array_map_access` to
   see the function added in the precursor work; it takes
   `(nb::object vec, int64_t index)`.
8. `opteryx/expression/evaluator/evaluation.pyx:283–310` (the tree-walker
   NT_EXTRACTION_OPERATOR branch) — the legacy code does the runtime
   type-name dispatch this ticket eliminates; understand it before deleting.
9. `opteryx/compiled/expression/compiled_expression.pyx:594–627` (current
   `_NT_EXTRACTION_OPERATOR` emit) — what you're replacing.

## Definition of done

- Bind-time `_NT_EXTRACTION_OPERATOR` resolves `(op_str, operand_type)`
  → kernel + sub-op flag. Fails loud if unresolvable.
- BC_EXTRACTION executor has a single `if/elif` chain on `slot.op_code`
  dispatching to the four native kernels. No Python wrapper.
- `MapAccessOp`, `ArrowOp`, `LongArrowOp`, `_json_key_constant` deleted
  from `binary_operators.pyx`.
- Their entries removed from `OPERATOR_FUNCTION_MAP`.
- `EXTRACTION_OPERATORS` set retained (binder consumer).
- Tree-walker `_eval_value` NT_EXTRACTION_OPERATOR branch updated to use
  the same resolution logic (plan-time only; kept correct).
- `make q` 100/100.
- All five spot queries return correct values.
- Phase 1 regression-check queries still pass.
- Microbench numbers in PR description.

## Notes on what comes next

Phase 4 (BC_COMPARE string-op elimination) is the next phase. It builds
on the `slot.op_code` int-dispatch pattern this phase consolidates for
BC_EXTRACTION. The result-wrap `isinstance` gates from Phase 1 stay until
Phases 5 / 7 land native kernels with consistent return types — at which
point they collapse to direct flag reads.

## Side-finding to surface in PR

While verifying Phase 2 we found that `COUNT(*) FROM x WHERE …` returns
`0` for *any* WHERE clause — `COUNT(col)`, `SUM(col)`, and unfiltered
`COUNT(*)` all work correctly. **This is not introduced by Phase 2** (the
IS NULL kernel produces correct per-row results; `WHERE col IS NULL`
filtering works), and `make q` does not catch it because the shape tests
only check `(rows, cols)` not values. Probably a pre-existing bug in the
ungrouped aggregate's literal-count accumulation path
(`UngroupedAggregateNode._has_literals` /
`_LiteralAggState.update(num_rows)` in
`opteryx/operators/aggregate/aggregate_node.pyx`). Out of scope for
Phase 3; needs its own ticket.
