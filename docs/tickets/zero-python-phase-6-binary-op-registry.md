# Ticket: Zero-Python Expression Engine — Phase 6 (BC_BINARY_OP kernel registry → bind-time resolution)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–5 have landed:
> see `docs/tickets/zero-python-phase-{1..5}-*.md`. This is Phase 6 — the
> largest single piece of the plan.

## Problem

BC_BINARY_OP today has two paths:

1. **DV fast path** (`evaluation.pyx:2370`) — `draken_arithmetic_dv` for
   `Plus/Minus/Multiply/Divide/Modulo` on supported types. Pure C, no
   Python. Stays.
2. **Fallback path** (`evaluation.pyx:2387`) — calls
   `_binary_op_from_vecs(op_code, left, right, …, <str>slot.compare_op_str, …)`
   which internally calls `call_arithmetic_op(op_str, left, right)` →
   `get_arithmetic_kernel(left_type, right_type, op)` → resolved kernel.

The fallback executes this chain per morsel:

- `arithmetic.pyx:_binary_op_from_vecs` — per-morsel type checks on
  left/right (e.g. `if left_type == VectorType.INTERVAL`), string-keyed
  special cases (`if op_code == BOP_STRING_CONCAT`,
  `if op_code == BOP_MINUS and left_is_date and right_is_date`).
- `arithmetic_dispatch.pyx:call_arithmetic_op` — per-morsel
  `is_draken_vector(left/right)` + `get_vector_type(left/right)` +
  DECIMAL→FLOAT64 materialisation + `get_arithmetic_kernel(...)` dict
  lookup + `try: kernel(left, right) except (TypeError, …)` flow
  control.
- `arithmetic_kernels.py:_make_kernel.kernel` — per-morsel
  `_unwrap(left)` + `_unwrap(right)` (two `getattr(v, "_nb", None)`),
  `getattr(left_nb, method_name, None)` (Python attribute lookup), then
  `method(right_nb)`.

Plus `binary_operations` (in `binary_operators.pyx`) is the tree-walker
entry point and adds another layer of the same dispatch with
`OPERATOR_FUNCTION_MAP.get(operator)` and string-keyed branches.

Everything in the fallback is bind-time-resolvable: the operator
(`Plus`, `BitwiseOr`, `StringConcat`, etc.), the left/right operand
types (from `node.left.schema_column.type`), and the special cases
(interval combinations, BitwiseOr-on-VARCHAR for CIDR, date arithmetic)
are all known to the binder.

## Goal

After Phase 6:
- `_binary_op_from_vecs`, `call_arithmetic_op`, `binary_operations`,
  `OPERATOR_FUNCTION_MAP`, `_KERNELS` (in
  `draken/vectors/arithmetic_kernels.py`), `get_arithmetic_kernel`,
  `_OP_TO_METHOD`, `_make_kernel`, `_unwrap`, `_dispatch_arithmetic_operation`,
  `_to_string_vec` (in arithmetic.pyx) — **all deleted**.
- BC_BINARY_OP fallback path is a single `(<object>slot.callable_ref)(left, right)`
  call — same shape as BC_FUNCTION / BC_CAST after Phase 5.
- The kernel registry becomes a **bind-time resolver** that returns a
  pre-bound callable based on `(op_code, left_type_code, right_type_code)`
  plus any required parameters (e.g. coercion closures for DECIMAL,
  per-morsel-length closures for StringConcat on scalar operands).
- The DV fast path stays unchanged.
- The tree-walker `_eval_binary_op_draken` in `evaluation.pyx` uses the
  same resolver path. Plan-time only — kept correct.

**Important boundary:** the resolved kernel is still invoked via
`PyObject_Call` (a single Python call per morsel). Replacing the call
itself with a C function-pointer table is **out of scope** for Phase 6
(it requires a C ABI for the nanobind kernel methods, which is a
separate architectural piece). Phase 6 eliminates everything *around*
the kernel call: the dict lookups, the type checks, the getattr
chains. Phase 8's nogil work will need to grow to accommodate this one
remaining Python call — or a follow-up phase makes it a C pointer.

## Scope

**In scope**
- `opteryx/expression/casts.pyx`-style **bind-time resolver** added,
  either in `arithmetic.pyx` or a new tiny module. Public API:
  `resolve_binary_op(op_code: int, left_orso, right_orso) -> callable`.
- `opteryx/compiled/expression/compiled_expression.pyx` —
  `_NT_BINARY_OPERATOR` emit (~line 470). Today it stores `slot.op_code`
  (already int) and `slot.compare_op_str` (for the fallback). After:
  resolve at bind time, store the resolved kernel in `slot.callable_ref`.
  `slot.compare_op_str` is no longer needed for BC_BINARY_OP — can be
  cleared (do not delete the struct field; it's struct-shared with
  other opcodes that don't use it for this purpose).
- `opteryx/expression/evaluator/evaluation.pyx` —
  - BC_BINARY_OP executor at ~line 2364: replace the
    `_binary_op_from_vecs(...)` fallback call with
    `(<object>slot.callable_ref)(py_left, py_right)`. Use the Phase 1
    `BC_RESULT_NEEDS_NB_WRAP` pattern for the wrap.
  - Tree-walker `_eval_binary_op_draken` (~line 220) — replace
    `binary_operations(...)` call with the resolver path.
- **Deletions** (full files / large blocks):
  - `opteryx/expression/evaluator/arithmetic.pyx` —
    `_binary_op_from_vecs`, `_to_string_vec`, `_date_minus_date_draken`,
    `_date_interval_op_draken`. Keep `_eval_binary_op_draken` (tree-walker
    NT_BINARY_OPERATOR entry, updated to use resolver).
  - `opteryx/expression/evaluator/arithmetic_dispatch.pyx` — entire
    file: `call_arithmetic_op`. Update any `include`/`import` of it.
  - `opteryx/expression/binary_operators.pyx` —
    `_dispatch_arithmetic_operation`, `binary_operations`,
    `_unsupported_bitwise_op`, `_to_bytes_or_vec`, `OPERATOR_FUNCTION_MAP`,
    `_ARITHMETIC_OPS`, `BINARY_OPERATORS`. The whole `binary_operators.pyx`
    file likely becomes empty or near-empty — verify and delete if so.
    `EXTRACTION_OPERATORS` set already moved to bind-time (Phase 3) but
    the set itself stayed for the binder — verify whether it still has
    a consumer.
  - `draken/vectors/arithmetic_kernels.py` — entire file:
    `_OP_TO_METHOD`, `_NUMERIC`, `_unwrap`, `_make_kernel`, `_KERNELS`,
    `get_arithmetic_kernel`. Move its kernel-building logic into the
    new `resolve_binary_op` (the `_make_kernel` factory is what produces
    the wrappers — its body becomes part of the resolver's specialised
    closures).

**Out of scope**
- Replacing the kernel-call's `PyObject_Call` with a C function pointer
  — separate effort (would need a C ABI for nanobind methods).
- Adding new arithmetic capabilities (decimal128 promotion, new type
  pairs).
- Annotating the executor `nogil` — Phase 8.
- Removing the existing DV fast path (`draken_arithmetic_dv`) — that's
  the optimised path and stays.

## Resolution table

The resolver returns a callable for each `(op_code, left_orso, right_orso)`
combination. Output of `resolve_binary_op` is one of:
- A reference to an existing native kernel (nanobind function from
  `opteryx.compiled.nanobind.vector_*`).
- A small closure that bakes in the type coercion / unwrap / dispatch.
- `None` / raise if the pair is unsupported (fail at bind time).

### Arithmetic ops (Plus/Minus/Multiply/Divide/Modulo)

| left_orso       | right_orso      | kernel                                                                           |
|-----------------|-----------------|----------------------------------------------------------------------------------|
| INTEGER/DOUBLE  | INTEGER/DOUBLE  | `lambda l, r: getattr(l._nb if has_nb else l, method)(r._nb if has_nb else r)` — but inline `getattr` should be replaced with a typed cdef wrapper |
| DECIMAL         | any numeric     | closure: materialise DECIMAL → FLOAT64 (once per call), then call float64 kernel |
| any numeric     | DECIMAL         | symmetric                                                                        |
| DATE            | INTERVAL        | `_date_interval_op_kernel(op_str, left, right)` (today in arithmetic.pyx)        |
| INTERVAL        | DATE            | symmetric                                                                        |
| TIMESTAMP       | INTERVAL        | same                                                                             |
| INTERVAL        | TIMESTAMP       | symmetric                                                                        |
| DATE/TIMESTAMP  | DATE/TIMESTAMP  | (Minus only) `_date_minus_date_kernel`                                          |

### Bitwise (BitwiseOr/And/Xor/ShiftLeft/Right)

| left_orso       | right_orso      | kernel                                                                           |
|-----------------|-----------------|----------------------------------------------------------------------------------|
| INTEGER         | INTEGER         | `_vector_bitwise_or` / `_and` / `_xor` / `_shift_left` / `_shift_right`           |
| VARCHAR         | VARCHAR         | (BitwiseOr only — IP-in-CIDR) `vector_ip_in_cidr`                                |

### String

| left_orso       | right_orso      | kernel                                                                           |
|-----------------|-----------------|----------------------------------------------------------------------------------|
| any             | any             | (StringConcat) closure: coerce non-string operands via `vector_varchar_from_constant`, then `vector_concat`. n inferred from the string operand. |

### Integer divide

| left_orso       | right_orso      | kernel                                                                           |
|-----------------|-----------------|----------------------------------------------------------------------------------|
| INTEGER         | INTEGER         | `MyIntegerDivide` — verify whether there's a native kernel; if not, this is a kernel-to-implement candidate. Flag in PR. |

### Interval-interval

| left_orso       | right_orso      | kernel                                                                           |
|-----------------|-----------------|----------------------------------------------------------------------------------|
| INTERVAL        | INTERVAL        | Plus/Minus only; resolve via `INTERVAL_KERNELS` (today in `opteryx/expression/intervals.pyx`) — fold into resolver |

If during Phase 6 the resolver encounters a pair that today's
`OPERATOR_FUNCTION_MAP` / `INTERVAL_KERNELS` / `get_arithmetic_kernel`
handled but Phase 6 doesn't, **stop and surface in PR**. Do not fall
back to runtime dispatch.

## Resolver signature

```cython
cpdef object resolve_binary_op(int op_code, left_orso, right_orso):
    """Bind-time resolver: return a callable for binary_op(left, right).

    op_code is a BCBinaryOpCode int (BOP_PLUS..BOP_SHIFT_RIGHT).
    left_orso, right_orso are OrsoTypes or None.

    The returned callable signature: kernel(left_vector, right_vector) → Vector.

    Raises NotImplementedError if no kernel handles the (op_code,
    left_orso, right_orso) combination.
    """
```

If you place this in a new module (e.g.
`opteryx/expression/evaluator/binary_op_resolve.pyx`) update the
build/setup.py to include it, and add a textual `include` in `_impl.pyx`
if you want it in the same compile unit.

If you place it in `arithmetic.pyx` (current home of related logic),
just add it there.

## Bind-time emit (compiled_expression.pyx)

Today (~line 470 of `_NT_BINARY_OPERATOR`):

```cython
slot.op_code = <int>_BOP_CODE.get(bin_op_str, BOP_UNKNOWN)
slot.compare_op_str = <PyObject*>bin_op_str  # still needed for call_arithmetic_op
```

After Phase 6:

```cython
slot.op_code = <int>_BOP_CODE.get(bin_op_str, BOP_UNKNOWN)
if slot.op_code == BOP_UNKNOWN:
    raise NotImplementedError(
        f"compiled_expression: unknown binary op {bin_op_str!r}"
    )

# Resolve the operand types from schema_column on the source nodes.
left_orso = (<object>node.left.source_node).schema_column.type \
    if node.left != NULL and (<object>node.left.source_node).schema_column is not None \
    else None
right_orso = (<object>node.right.source_node).schema_column.type \
    if node.right != NULL and (<object>node.right.source_node).schema_column is not None \
    else None

# Bind-time resolution. Fail fast on unsupported pairs.
from opteryx.expression.evaluator.arithmetic import resolve_binary_op
binop_kernel = resolve_binary_op(slot.op_code, left_orso, right_orso)
bc._hold(binop_kernel)
slot.callable_ref = <PyObject*>binop_kernel

# Wrap-flag setup as per Phase 1.
slot.flags |= BC_RESULT_NEEDS_NB_WRAP   # kernels return nanobind Vectors
# WRAP_AS_BOOL is false for binary ops (results are never BOOL).
```

The `slot.compare_op_str` assignment for BC_BINARY_OP can be deleted —
no caller reads it. (The field itself stays on `BytecodeInstr` because
other opcodes use it.)

## Runtime — BC_BINARY_OP executor

Today (~line 2387):

```cython
legacy_result = _binary_op_from_vecs(
    slot.op_code,
    py_left, py_right,
    slot.left_type_code, slot.right_type_code,
    <str>slot.compare_op_str,
    num_rows,
)
anchor[sp] = legacy_result
dv_stack[sp] = (<Vector>legacy_result).unified()
```

After Phase 6:

```cython
legacy_result = (<object>slot.callable_ref)(py_left, py_right)
# Phase 1 result-wrap pattern.
if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
    if slot.flags & BC_RESULT_WRAP_AS_BOOL:
        legacy_result = BoolVector(legacy_result)
    else:
        legacy_result = Vector(legacy_result)
anchor[sp] = legacy_result
if isinstance(legacy_result, Vector):
    dv_stack[sp] = <DrakenVector*>(<Vector>legacy_result)._dv
else:
    dv_stack[sp] = NULL
```

The `isinstance` on the last block is the same residual gate as Phase
1/3/5 elsewhere. Keep it; consistent.

## Tree-walker (plan-time)

`evaluate_draken`'s NT_BINARY_OPERATOR branch in `evaluation.pyx`
(currently ~line 880) calls `_eval_binary_op_draken` which calls
`binary_operations`. Update the chain to use `resolve_binary_op`:

```cython
# In _eval_binary_op_draken:
left = _eval_value(node.left, morsel)
right = _eval_value(node.right, morsel)
# Resolve at the call site — slow, but plan-time only.
left_orso = getattr(getattr(node.left, "schema_column", None), "type", None)
right_orso = getattr(getattr(node.right, "schema_column", None), "type", None)
kernel = resolve_binary_op(<int>_BOP_CODE.get(node.value, 0), left_orso, right_orso)
return kernel(left, right)
```

The two `getattr`s are tolerable here — plan-time, called rarely from
constant folding. The hot path (BC_BINARY_OP) resolves once at bind
time.

## Verification

- `make c` clean. **Verify a fresh build compiles** before running
  `make q` (Phase 4 lesson).
- `make q` 100/100.
- Symbol checks:
  - `grep -rn 'call_arithmetic_op\|_binary_op_from_vecs\|binary_operations\|OPERATOR_FUNCTION_MAP\|get_arithmetic_kernel\|_OP_TO_METHOD' opteryx/ draken/ --include='*.py' --include='*.pyx'`
    — should return zero matches (excluding the deletions in your own
    diff and any historical comment references).
  - `grep -rn 'from draken.vectors.arithmetic_kernels' opteryx/ --include='*.py' --include='*.pyx'`
    — zero matches.
  - `arithmetic_dispatch.pyx` should be **deleted** (or empty if the
    build doesn't permit deletion mid-PR; surface in PR).
- Spot tests:
  - `SELECT 1 + 2` (Plus, INT+INT, DV fast path)
  - `SELECT 1.5 + 2.5` (Plus, DOUBLE+DOUBLE, DV fast path)
  - `SELECT 'a' || 'b'` (StringConcat — fallback path)
  - `SELECT id | 2 FROM $planets LIMIT 3` (BitwiseOr — fallback path)
  - `SELECT id << 1 FROM $planets LIMIT 3` (ShiftLeft — fallback path)
  - `SELECT id // 2 FROM $planets LIMIT 3` (MyIntegerDivide — verify it works)
  - A DECIMAL arithmetic test if you have decimal data:
    `SELECT CAST(id AS DECIMAL(10,2)) + 1 FROM $planets LIMIT 3`
  - A date+interval: `SELECT DATE '2024-01-01' + INTERVAL '1 day'`
  - A date-date: `SELECT DATE '2024-01-10' - DATE '2024-01-01'`
  - All Phase 1/3/4/5 regression queries must still pass:
    - `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3`
    - `SELECT missions[0] FROM testdata.astronauts LIMIT 3`
    - `SELECT COUNT(*) FROM $planets WHERE id = 3` (returns 1 — sanity)
- Microbench: time
  `SELECT id + 1 FROM testdata.astronauts` (DV fast path — should be unchanged)
  AND
  `SELECT name || '!' FROM testdata.astronauts` (StringConcat fallback —
  should improve).
  Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No new Python on execute path.** BC_BINARY_OP executor body must
  contain **zero** `get_vector_type` / `is_draken_vector` / `getattr` /
  dict lookups per morsel. (The `isinstance(legacy_result, Vector)` for
  dv_stack assignment stays — same as Phase 1/3/5.)
- **Fail fast.** Bind-time `resolve_binary_op` raises on unresolvable
  `(op_code, left_orso, right_orso)`. No silent runtime fallback.
- **No `try/except` for control flow** — banned. The current
  `try: kernel(left, right); except (TypeError, …): return None` in
  `arithmetic_dispatch.pyx:71` **is exactly the anti-pattern CLAUDE.md
  §9 forbids**. Phase 6 deletes it; resolution is bind-time or fail.
- **No `hasattr`** — banned.
- **Cython code must be typed.**
- **`make c` clean before claiming completion.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/arithmetic.pyx`:
  - `_eval_binary_op_draken` (~line 30) — update to use resolver.
  - `_binary_op_from_vecs` (~line 118) — **delete**.
  - `_to_string_vec` (~line 19) — **delete** (or relocate inline into
    the StringConcat closure inside the resolver).
  - `_date_minus_date_draken`, `_date_interval_op_draken` — keep as
    kernels the resolver references, OR fold into resolver closures.
- `opteryx/expression/evaluator/arithmetic_dispatch.pyx` — **delete in
  full**. Remove from setup.py if listed.
- `opteryx/expression/binary_operators.pyx` — strip down. Likely the
  whole file becomes deletable. Surface in PR.
- `draken/vectors/arithmetic_kernels.py` — **delete in full**.
  Migrate the `_make_kernel` logic into a typed cdef helper inside
  `resolve_binary_op`.
- `opteryx/expression/intervals.pyx` — `INTERVAL_KERNELS` table.
  Likely stays (it's already a typed lookup), but its callers move to
  the resolver.
- `opteryx/compiled/expression/compiled_expression.pyx` —
  `_NT_BINARY_OPERATOR` emit (~line 470). Resolve at bind time.
- `opteryx/expression/evaluator/evaluation.pyx`:
  - BC_BINARY_OP executor (~line 2364) — simplify.
  - Tree-walker NT_BINARY_OPERATOR branch (~line 880-ish) — point at
    resolver.
- `opteryx/expression/evaluator/__init__.py` — verify no re-exports of
  the deleted symbols. Remove if found.

## Tests

- `make q` (137/137) with **fresh build**.
- All spot queries return correct values.
- Phase 1/3/4/5 regression queries still pass.
- `tests/unit/expression/test_arithmetic*.py` (if any) — verify; update
  if they import deleted symbols.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 1–5 tickets.
3. `opteryx/expression/evaluator/arithmetic.pyx` end to end.
4. `opteryx/expression/evaluator/arithmetic_dispatch.pyx` end to end
   (small, ~77 lines).
5. `opteryx/expression/binary_operators.pyx` end to end.
6. `draken/vectors/arithmetic_kernels.py` end to end (small, ~80 lines).
7. `opteryx/expression/intervals.pyx` — focus on `INTERVAL_KERNELS`
   and how it's keyed.
8. `opteryx/compiled/expression/compiled_expression.pyx:455–500` —
   `_NT_BINARY_OPERATOR` emit.
9. `opteryx/expression/evaluator/evaluation.pyx:2358–2400` — BC_BINARY_OP
   executor; understand the DV fast path before touching the fallback.

## Definition of done

- `resolve_binary_op(op_code, left_orso, right_orso)` exists and
  resolves every `(op, type_pair)` the existing dispatch handled.
- BC_BINARY_OP executor fallback path is one `callable_ref(left, right)`
  call + Phase-1 result-wrap. No internal dispatch.
- Bind-time `_NT_BINARY_OPERATOR` calls the resolver and fails loud on
  unresolvable pairs.
- These symbols **deleted** (zero remaining callers in production
  code; tests may reference them — flag for cleanup):
  `call_arithmetic_op`, `_binary_op_from_vecs`, `binary_operations`,
  `_dispatch_arithmetic_operation`, `_unsupported_bitwise_op`,
  `_to_bytes_or_vec`, `_to_string_vec`, `OPERATOR_FUNCTION_MAP`,
  `BINARY_OPERATORS`, `_ARITHMETIC_OPS`, `_OP_TO_METHOD`, `_NUMERIC`,
  `_unwrap` (in arithmetic_kernels.py), `_make_kernel`, `_KERNELS`,
  `get_arithmetic_kernel`.
- The `try: kernel(left, right); except (TypeError, ValueError, AttributeError)`
  flow-control block from `arithmetic_dispatch.pyx:71` is **deleted**.
  Banned by CLAUDE.md §9.
- `make c` clean; `make q` 100/100 with **fresh build**.
- Microbench numbers in PR description.

## Side-notes (carry forward in PR)

- Cleanup tickets still pending:
  - `tests/unit/expression/test_map_access_operator.py` imports the
    deleted `MapAccessOp` (Phase 3).
  - `COUNT(*) FROM x WHERE …` returns `0` — pre-existing aggregate bug
    (Phase 2 finding). Needs its own ticket.
- Phase 6 surfaces another residual: the `OrsoTypes[t].parse` row-loop
  in cast (Phase 5 §Side-notes) — verify it's still flagged.
- If you discover an arithmetic pair the resolver can't handle that
  real queries hit, **stop and surface**. The plan's stance is "implement
  native kernels where reasonable; flag where disproportionate". Same
  as Phase 5.
- The `PyObject_Call` to the resolved kernel remains the one Python op
  in BC_BINARY_OP's fallback. A future phase (or a separate effort) can
  replace it with a C function-pointer ABI. Phase 6 explicitly does not
  attempt this.
