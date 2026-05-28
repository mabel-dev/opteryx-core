# Ticket: Zero-Python Expression Engine — Phase 5 (BC_CAST closure specialisation)

> Part of `docs/zero_python_expression_engine.md`. Phases 1–4 have landed:
> `docs/tickets/zero-python-phase-1-result-wrap.md` through
> `docs/tickets/zero-python-phase-4-compare-string-op.md`. This is Phase 5.

## Problem

BC_CAST at runtime calls a Python closure (`_inner`) produced by the
`cast()` factory in `opteryx/expression/casts.pyx:387`. Per morsel, the
closure does:

1. **Per-morsel Python string dispatch on `_type`**: `if _type == "TIMESTAMP"`,
   `elif _type == "DATE"`, `if _type == "ARRAY"`, `if _type == "VECTOR"`,
   then `if is_draken_vector_fn(arr): if _type in ("DOUBLE", "FLOAT"): …`.
   Each branch is a Python string compare.
2. **Per-morsel `is_draken_vector_fn(arr)` check** (line 472).
3. **Per-morsel `get_vector_type(arr)` dispatch** (lines 434, 457).
4. **Per-row Python loops** for the residual (`[caster(i, **kwargs) for i in arr]`
   at line 482, plus the `_parse_array_value` and `parse_timestamp_value`
   loops for ARRAY/TIMESTAMP).
5. **A Python closure invocation itself** via `(<object>slot.callable_ref)(py_left)`
   in the executor.

The bind-time information needed to skip all of (1–3) is already
available — the binder knows the source operand's `schema_column.type`
and the target `_type` string. Resolution at bind time eliminates the
runtime dispatch.

The architect's stance (open item #1 from `zero_python_expression_engine.md`):
**implement native kernels where reasonable; flag only the casts where a
real implementation would be disproportionate**.

## Goal

After Phase 5:
- `cast()` becomes a **resolver**: given `(source_type, target_type, args, unit)`,
  return one of (a) a pre-existing native kernel callable, (b) a
  specialised closure pre-bound to args (only for genuinely
  parameterised casts like `DECIMAL(p,s)` and `VARCHAR(length)`),
  (c) a bind-time error for unsupported pairs.
- The `_inner` closure with its per-morsel string-dispatch is **deleted**.
- BC_CAST executor stays as it is — calls `slot.callable_ref(py_left)` —
  but `callable_ref` now points to a kernel or small specialised
  closure, not a dispatch-everything `_inner`.
- For source types that are bind-time-known to *equal* the target type
  (the no-op case), bind time resolves to a passthrough kernel and
  doesn't emit BC_CAST at all (optional optimisation; surface in PR if
  you do it).

## Scope

**In scope**
- `opteryx/expression/casts.pyx` — rewrite `cast()` (line 387) as a
  resolver. Delete the `_inner` closure. Keep the per-pair helpers
  (`cast_to_double`, `cast_to_int`, `cast_to_boolean`, `cast_to_varchar`,
  `cast_to_date`, `cast_to_boolean`, etc.) as the kernels the resolver
  returns. Their internal `if v_type == STRING: return arr` no-op checks
  can be **deleted** — the resolver handles that at bind time.
- `opteryx/compiled/expression/compiled_expression.pyx` —
  `_NT_CAST` emit at ~line 522. Pass the source operand's
  `schema_column.type` to the resolver alongside the target type.
- `opteryx/expression/evaluator/evaluation.pyx` BC_CAST executor at
  ~line 2461 — should require no change (the call is already
  `(<object>slot.callable_ref)(py_left)`). The `BC_RESULT_NEEDS_NB_WRAP`
  flag set by Phase 1 still applies; the runtime `isinstance` gate that
  Phase 1 left in place **can now be deleted** — Phase 5 makes the
  kernel return type deterministic (always nanobind Vector for native
  kernels; specialised closure wraps to nanobind too).
- Tree-walker `_eval_cast_draken` in `evaluation.pyx` (~line 158) —
  also calls the `cast()` factory; same resolver path.

**Out of scope**
- BC_BINARY_OP arithmetic kernel registry — Phase 6.
- CASE inner-loop natives — Phase 7.
- Annotating the executor `nogil` — Phase 8.
- Adding any *new* SQL cast capabilities. The set of supported casts
  stays exactly what it is today.

## Resolution table

Build this table once at module-load. Key: `(source_orso, target_str)`.
Value: a `(kernel_callable, needs_args_closure: bool)` pair. The
resolver looks up the pair; if found and `needs_args_closure` is False,
return the kernel directly. If True, build a small closure that bakes
in `args` / `unit`.

Direct-mapped pairs (no args needed):

| source            | target                       | kernel                                                          |
|-------------------|------------------------------|-----------------------------------------------------------------|
| any → any (same)  | passthrough                  | identity (`lambda x: x` is fine; bind-time avoids if possible)  |
| INT64             | VARCHAR / BLOB / VARBINARY   | `vector_cast_int64_to_string`                                   |
| INT8/16/32        | VARCHAR                      | `vector_cast_int64_to_string` (after promote — verify or wrap)  |
| BOOLEAN           | VARCHAR                      | `vector_cast_bool_to_string`                                    |
| DATE              | VARCHAR                      | `vector_cast_date_to_string`                                    |
| TIMESTAMP         | VARCHAR                      | `vector_cast_timestamp_to_string`                               |
| DOUBLE / FLOAT    | VARCHAR                      | `_draken_native_casts.vector_cast_float64_to_string`            |
| INT64             | DOUBLE / FLOAT               | `vector_cast_int64_to_float64`                                  |
| INT8/16/32        | DOUBLE / FLOAT               | `vector_cast_integer_to_float64`                                |
| BOOLEAN           | DOUBLE / FLOAT               | `vector_cast_bool_to_float64`                                   |
| VARCHAR / BLOB    | DOUBLE / FLOAT               | `_draken_native_casts.vector_cast_string_to_float64`            |
| (numeric)         | INTEGER / BIGINT             | `cast_to_int` *(today's Python wrapper — verify it has a vector kernel inside; if not, this is a *fallback row-loop case)* |
| (numeric)         | BOOLEAN                      | `cast_to_boolean`                                               |
| INT64             | TIMESTAMP                    | `vector_cast_int64_to_timestamp(arr, unit=…)` — needs `unit`, so closure |
| DATE              | TIMESTAMP                    | `vector_date32_to_timestamp`                                    |
| TIMESTAMP         | DATE                         | `vector_timestamp_to_date32`                                    |
| DATE              | DATE                         | passthrough                                                     |

**Closure-required pairs** (need bake-in args/unit at bind time):

| source        | target              | closure body                                                  |
|---------------|---------------------|---------------------------------------------------------------|
| INT64         | TIMESTAMP[unit]     | `lambda arr: vector_cast_int64_to_timestamp(arr, unit=UNIT)`  |
| anything      | DECIMAL(p, s)       | row-loop + `decimal_quantizer` (today's residual path)        |
| anything      | ARRAY(element_type) | `[_parse_array_value(i, element_type) for i in arr] → vector_array_from_sequence` |
| anything      | VECTOR              | `[caster(i, **kwargs) for i in arr] → vector_fp16_from_sequence` |
| anything      | VARCHAR(length)     | row-loop only if length-constraint is enforced; else direct kernel |
| anything else | (residual)          | `[OrsoTypes[t].parse(i) for i in arr] → _cast_result_to_draken` — **flag in PR** as candidate for a native kernel |

The "residual" path (`OrsoTypes[t].parse` row-loop, line 482 of
casts.pyx) is the one the architect was concerned about. Today it
catches casts that don't have a native kernel. Surface every
`(source, target)` pair that hits this residual in your PR. The
architect will decide which to implement natively next.

Verify each kernel's signature matches what the resolver hands it. For
example, `vector_cast_int64_to_string` expects an unwrapped nanobind
Vector — you may need to wrap it in a thin closure that calls
`_unwrap_nb(arr)`. See `cast_to_varchar` (line 222) for the existing
unwrap pattern.

## Resolver signature

```cython
cpdef object resolve_cast(source_orso, target_type, args=(), unit=None):
    """Return a callable that casts a vector from source_orso → target_type.

    Called once per CAST node at bind time. The returned callable takes
    a single argument (the vector to cast) and returns the cast result.

    Raises NotImplementedError if no kernel is registered for the pair.
    """
```

Where the existing `cast(arr, _type, args=(), unit=None)` factory used
to be. The new `cast` function disappears (or becomes a thin wrapper
around `resolve_cast` for any plan-time tree-walker caller — verify;
constant folding may need it).

## Bind-time emit (compiled_expression.pyx)

Today (line 567 area):

```cython
cast_kernel = _cast_factory(None, cast_target_type, cast_params, unit=cast_unit)
```

After Phase 5:

```cython
# Read the source operand's type from its schema_column.
src_node = <object>node.left.source_node
src_sc = src_node.schema_column
source_orso = src_sc.type if src_sc is not None else None

cast_kernel = _resolve_cast(source_orso, cast_target_type, cast_params, unit=cast_unit)
```

If `_resolve_cast` raises, propagate (fail-fast at bind time).

If you can bind-time-determine the cast is a no-op (source_orso ==
target_orso, no args), **emit no opcode** — replace BC_CAST with a
passthrough. Skip this if it complicates the diff; just have the
resolver return a passthrough closure for the same effect.

## Runtime — BC_CAST executor

Phase 1 left this in place:

```cython
if (slot.flags & BC_RESULT_NEEDS_NB_WRAP) and not isinstance(legacy_result, Vector):
    if slot.flags & BC_RESULT_WRAP_AS_BOOL:
        legacy_result = BoolVector(legacy_result)
    else:
        legacy_result = Vector(legacy_result)
```

The `not isinstance(legacy_result, Vector)` guard was needed because
some casts returned the input Cython Vector unchanged (no-op case).
After Phase 5:
- The no-op case is bind-time-eliminated (passthrough closure returns
  the input, which is already a Cython Vector → `BC_RESULT_NEEDS_NB_WRAP`
  flag is **not** set for passthrough).
- All other resolved kernels return nanobind Vectors deterministically.

So the gate can be deleted:

```cython
if slot.flags & BC_RESULT_NEEDS_NB_WRAP:
    if slot.flags & BC_RESULT_WRAP_AS_BOOL:
        legacy_result = BoolVector(legacy_result)
    else:
        legacy_result = Vector(legacy_result)
```

Set `BC_RESULT_NEEDS_NB_WRAP` correctly at bind time based on whether
the resolved kernel returns nanobind or Cython:
- All native kernels (`vector_cast_*`) → nanobind → set the flag.
- Passthrough closure (no-op cast) → Cython → don't set.
- Specialised closures (DECIMAL / ARRAY / VECTOR row-loops) — verify
  what each returns; today they go through `_cast_result_to_draken`
  which returns a nanobind Vector → set the flag.

`BC_RESULT_WRAP_AS_BOOL` is true iff target type is BOOLEAN.

## Verification

- `make c` clean. **Verify a fresh build compiles** before running
  `make q` — Phase 4 was masked by stale `.so` files.
- `make q` 100/100.
- `grep -n 'def _inner\|return _inner' opteryx/expression/casts.pyx`
  — should return zero matches.
- `grep -n 'if _type ==' opteryx/expression/casts.pyx` — only the
  bind-time resolver should branch on `_type` strings; the per-morsel
  closure must not.
- Spot tests (covering the resolution table):
  - `SELECT CAST(id AS VARCHAR) FROM $planets LIMIT 3` (INT64 → VARCHAR)
  - `SELECT CAST(name AS VARCHAR) FROM $planets LIMIT 3` (VARCHAR → VARCHAR, no-op)
  - `SELECT CAST(id AS DOUBLE) FROM $planets LIMIT 3` (INT64 → DOUBLE)
  - `SELECT CAST(id AS BOOLEAN) FROM $planets LIMIT 3` (INT64 → BOOL)
  - `SELECT CAST('3.14' AS DOUBLE)` (VARCHAR → DOUBLE)
  - `SELECT CAST(missions AS VARCHAR) FROM testdata.astronauts LIMIT 3` (ARRAY → VARCHAR — the Phase 1 regression case)
  - `SELECT CAST(1234567890 AS TIMESTAMP)` if your test data supports it (INT64 → TIMESTAMP, exercises the `unit`-bound closure)
  - `SELECT CAST(id AS DECIMAL(10, 2)) FROM $planets LIMIT 3` (DECIMAL with precision/scale closure)
  - Chained: `SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` (Phase 1's regression test)
  - Chained: `SELECT UPPER(CAST(name AS VARCHAR)) FROM $planets LIMIT 3` (Phase 1's regression test)
- Microbench: time `SELECT CAST(id AS VARCHAR) FROM testdata.astronauts`
  (full table) before/after. Numbers in PR description.

## Constraints (from CLAUDE.md)

- **No new Python on the execute path.** The runtime BC_CAST executor
  region must contain zero `if _type ==` / `get_vector_type(arr)` /
  `is_draken_vector_fn(arr)` per-morsel calls.
- **Fail fast.** Unsupported `(source, target)` pairs raise at bind
  time. No runtime fallback to `OrsoTypes[t].parse` row-loop for an
  unforeseen case — those are flagged in PR for follow-up.
- **No `try/except` for control flow** — banned.
- **No `hasattr`** — banned.
- **Cython code must be typed.** New cdef locals where needed.
- **Build must compile fresh.** Run `make c` after every meaningful
  edit. Do not rely on cached `.so` files (Phase 4 caught this).
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/casts.pyx` — entire `cast()` factory (lines
  387–488) is being rewritten. The per-target helpers
  (`cast_to_double` ~125, `cast_to_int` ~161, `cast_to_varchar` ~222,
  `cast_to_boolean` ~263, `cast_to_date` ~298, etc.) stay — they
  become the kernels the resolver returns.
- `opteryx/compiled/expression/compiled_expression.pyx` —
  `_NT_CAST` emit at ~line 552. Update the call site to pass
  `source_orso`.
- `opteryx/expression/evaluator/evaluation.pyx` —
  - BC_CAST executor at ~line 2461 — delete the `isinstance` gate
    (per §Runtime above).
  - `_eval_cast_draken` (tree-walker) at ~line 158 — update the
    `cast()` call. Tree-walker is plan-time only; correctness matters
    but perf doesn't.
- `opteryx/expression/evaluator/__init__.py` — verify no re-export of
  the old `cast` symbol. If something imports `from opteryx.expression.casts import cast`,
  consider keeping a thin compatibility wrapper or updating callers.

## Tests

- `make q` 137/137 with a **freshly compiled** `.so` (always
  `make c` first).
- All spot queries return correct results.
- Phase 1/3/4 regression queries still pass.
- The `tests/unit/test_casts.py` (if it exists — verify) still passes,
  or is updated to the new API.

## Pre-flight reading

1. `docs/zero_python_expression_engine.md`.
2. Phase 1–4 tickets.
3. `opteryx/expression/casts.pyx` end to end — every cast helper, every
   parse_*, every native kernel import. The resolution table in this
   ticket may be incomplete; cross-check.
4. `opteryx/compiled/expression/compiled_expression.pyx:520–582` — the
   current `_NT_CAST` emit.
5. `opteryx/compiled/nanobind/vector_casts.cpp` (or wherever the native
   `vector_cast_*_to_*` functions live) — confirm signatures.
6. `opteryx/expression/evaluator/evaluation.pyx:2458–2480` — the
   current BC_CAST executor (post-Phase-1 + Phase-4 state).

## Definition of done

- `cast()` factory rewritten as `resolve_cast(source_orso, target_type, args, unit)`.
- `_inner` closure with per-morsel string dispatch deleted.
- Resolution table covers every cast pair the existing implementation
  handled. Pairs that fall through to the `OrsoTypes[t].parse` row-loop
  (the residual path at line 482) are flagged in the PR — each
  candidate for a future native kernel.
- Bind-time `_NT_CAST` passes the source operand's `OrsoType` to the
  resolver. Fails loud on unresolvable pairs.
- BC_CAST executor's `isinstance(legacy_result, Vector)` gate is
  deleted; the wrap reads `BC_RESULT_NEEDS_NB_WRAP` directly.
- Tree-walker `_eval_cast_draken` uses the same resolver path.
- `make c` clean. **`make q` 100/100 with a fresh build.**
- Microbench numbers in PR description.

## Side-notes to surface in PR

- Cleanup ticket candidate: `tests/unit/expression/test_map_access_operator.py`
  still imports the deleted `MapAccessOp` (Phase 3). Out of scope here.
- Outstanding pre-existing bug: `SELECT COUNT(*) FROM x WHERE …`
  returns `0` (Phase 2 finding). Not affected by Phase 5; should be its
  own ticket. Confirm still reproduces after Phase 5.
- Possible follow-up: if the resolver finds the source operand's
  `schema_column.type` is `None` (binder didn't populate it), today's
  code falls back to runtime `is_draken_vector_fn(arr)`. After Phase 5
  the resolver will fail at bind time. If this breaks any real query,
  that's a planner / binder bug — surface it. Do not paper over by
  re-adding runtime dispatch.
- If during Phase 5 you discover a `(source, target)` pair that no
  existing kernel handles AND that real queries hit, **stop and surface
  it**. The architect prefers implementing a native kernel over keeping
  a Python row-loop fallback, but the choice is theirs.
