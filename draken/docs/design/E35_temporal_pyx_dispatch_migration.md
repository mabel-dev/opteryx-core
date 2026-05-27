# E.35 — Migrate `temporal.pyx` from stale typed-Vector subclass names to `DrakenType` dispatch

> **Status:** TODO.
>
> **Goal:** replace every `__class__.__name__` string-match against deleted
> typed-Vector subclass names in
> `opteryx/expression/functions/implementations/temporal.pyx` with proper
> `DrakenType` dispatch on the underlying nanobind handle's `.type`
> attribute.
>
> **Why:** the file currently checks vectors by string-comparing
> `arr.__class__.__name__` against names like `"TimestampVector"`,
> `"Date32Vector"`, `"Integer64Vector"`, `"ConstantVector"`,
> `"StringVector"`. **Every one of those classes was deleted** in the
> draken rebuild — they don't exist anymore. The only string that
> matches anything today is `"Vector"`, which is also what the raw
> nanobind class is named, so the check can't even distinguish the shim
> from the raw handle. Result: most branches are dead code and the live
> branches dispatch on a useless tautology. Some error paths fire when
> they shouldn't ("expects TimestampVector or Date32Vector input, got
> Vector") because everything is now named "Vector".
>
> **Predecessor:** `hasattr` removal pass on this file (2026-05-27) which
> exposed the broader stale-typename rot. Seven hasattr calls gone;
> structural type dispatch still wrong.

---

## 1. The pattern to replace

Throughout the file, dispatching looks like this:

```cython
vector_type = arr.__class__.__name__

if vector_type == "Integer64Vector":
    raise InvalidFunctionParameterError(...)

if vector_type not in ("TimestampVector", "Date32Vector", "Vector"):
    raise InvalidFunctionParameterError(...)
```

Replace with:

```cython
from draken.draken_native import DrakenType as _DrakenType  # module-level

if arr._nb.type == _DrakenType.INT64:
    raise InvalidFunctionParameterError(
        "EXTRACT(...) cannot operate on INTEGER values. ..."
    )

if arr._nb.type not in (_DrakenType.TIMESTAMP64, _DrakenType.DATE32):
    raise InvalidFunctionParameterError(
        "EXTRACT(...) expects TIMESTAMP or DATE input, got %s." % arr._nb.type
    )
```

Notes:
- `arr._nb` gets the nanobind handle from the Cython shim Vector.
  Direct access (no hasattr) — caller contract is that the dispatch
  layer always passes the shim. If anyone violates that, `AttributeError`
  fires loudly.
- `arr._nb.type` returns a `DrakenType` enum member (verified via
  `dir()` on the nanobind Vector class — `.type` is exposed as a
  property). Inline the access; **do not introduce a `cdef object`
  local to hold it.** These are `def` functions (Python-edge), so
  locals are Python objects by default — `cdef object` adds nothing
  and reads as the §3 anti-pattern. Plain inline access is the right
  shape.
- Comparison via enum equality is exact and runtime-cheap.
- Error messages move to **SQL-level terminology**: "TIMESTAMP" /
  "DATE" / "INTEGER", not the now-deleted internal class names. Users
  see types they recognise.

If a function reads `.type` more than 2–3 times and inlining gets
unwieldy, use a plain Python local (no `cdef` annotation):

```cython
t = arr._nb.type
if t == _DrakenType.INT64: ...
elif t == _DrakenType.TIMESTAMP64: ...
elif t == _DrakenType.DATE32: ...
```

This is a normal Python local in a `def` function — already an
object, no `cdef` needed, not a §3 issue.

## 2. Functions in scope (all 7 in this file)

Each gets the same migration shape:

| Function | Accepted types | Reject types | Notes |
|---|---|---|---|
| `date_part(part, arr)` | TIMESTAMP64, DATE32 | INT64 (loud), others | Existing INT64 error message stays (good UX) |
| `trunc_date(arr, part)` | DATE32 | others | Already specialised — verify only DATE32 expected |
| `trunc_timestamp(arr, part)` | TIMESTAMP64 | others | Already specialised — verify only TIMESTAMP64 expected |
| `date_diff(part, start, end)` | TIMESTAMP64, DATE32 (auto-converted) | INT64 (loud), others | `_to_timestamp_vector` helper migrates with it |
| `date_format(dates, pattern)` | TIMESTAMP64, DATE32 | others | Check whether DATE32 is supported by `vector_date_format`; if not, convert first |
| `date_floor(dates, magnitude, units)` | TIMESTAMP64 (likely DATE32 too) | others | Also has stale ConstantVector/Int32Vector checks on `magnitude` and `units` — see §3 |
| `unixtime(array)` | TIMESTAMP64, DATE32 | others | Existing error message uses `vector_type` — update to use SQL-level names |

`time_diff` is a one-liner that delegates to `date_diff`; no change needed.
`from_unixtimestamp` is pure Python over a list; no Vector handling; no change.

## 3. The `magnitude` / `units` scalar-extraction pattern

`date_floor` has this:

```cython
mag_type = magnitude.__class__.__name__
if mag_type in ("ConstantVector", "Integer64Vector", "Int32Vector"):
    magnitude = magnitude[0]
```

Same stale-class problem. The dispatch layer passes constant-shape
vectors that are still Vector-class but with `data_length == 1`. The
correct test is "is this a Vector?" then take `[0]`, else assume it's
already a scalar.

Replace with:

```cython
from draken.vectors.vector import Vector as _Vector  # module-level
# or use isinstance(magnitude, (Vector,)) directly if cimported

if isinstance(magnitude, _Vector):
    magnitude = magnitude[0]
magnitude = int(magnitude)
```

Same shape for `units`.

## 4. What's explicitly NOT in scope

- Other files in `opteryx/expression/functions/implementations/` — only
  `temporal.pyx`. If similar rot exists in other files, surface as a
  follow-up; don't fold them in.
- Restructuring functions from `def` to `cpdef`/`cdef`. They're
  Python-edge functions per the consumer-edge-pattern memory. Stay
  `def`.
- Touching the underlying C′ extensions (`vector_date_part`,
  `vector_date_trunc`, `vector_date_format`, etc.). Those are
  draken-side; if their input contract changes, that's a separate
  ticket.
- Adding new `DrakenType` enum values. The required values
  (`TIMESTAMP64`, `DATE32`, `INT64`) already exist.
- Changing the registrar wiring — these functions stay registered the
  same way; only their internal type dispatch changes.

## 5. STOP conditions

- File count > 1. Only `temporal.pyx` is in scope. If the migration
  needs you to touch the registrar, the nanobind extensions, or other
  function files, **stop and surface**.
- You introduce a `hasattr` call. The previous pass deleted seven;
  don't re-add.
- You introduce `cdef object` (the file is `.pyx`; the functions are
  `def` so `object` parameters are legitimate at the edge, but
  internal cdef vars must be typed).
- You introduce an inline Python import inside a function body. All
  imports go to module level (the existing `from
  opteryx.compiled.nanobind.vector_temporal_arith import ...` lines
  inside function bodies are hot-path-Python-imports — hoist them too
  while you're in there).
- You find yourself migrating semantics, not just dispatch. e.g.
  changing what `trunc_date` returns or how `date_diff` computes —
  **stop**. This ticket is pure dispatch migration. Behaviour stays
  identical.
- The change exceeds ~80 lines net. The file is ~180 lines; net diff
  should be smaller than the file because the new pattern is more
  concise than the old. If it's growing, you're refactoring beyond
  scope.

## 6. Discipline reminders

- **No `hasattr`** (CLAUDE.md §9).
- **No `try/except` for flow control** (CLAUDE.md §9). The existing
  code has none; don't introduce any.
- **No `cdef object` anywhere** — not on locals, not on parameters,
  not on returns. These are `def` (Python-edge) functions; locals
  are Python objects by default. Adding `cdef object` is redundant
  AND reads as the §3 anti-pattern. Use plain assignment for locals.
- **No git commands.**
- **Error messages use SQL-level terminology** ("TIMESTAMP",
  "DATE", "INTEGER"), not internal Python class names. Users don't
  know "Integer64Vector" exists.

## 7. Acceptance

Run and report verbatim:

1. `grep -nE "__class__\.__name__|TimestampVector|Date32Vector|Integer64Vector|ConstantVector|StringVector|Int32Vector" opteryx/expression/functions/implementations/temporal.pyx`
   — must return zero matches (no stale class-name references remain).
2. `grep -n "hasattr" opteryx/expression/functions/implementations/temporal.pyx`
   — zero.
3. `grep -n "from opteryx.compiled.nanobind" opteryx/expression/functions/implementations/temporal.pyx | wc -l`
   — these imports should be at module top, not inline; show the line
   numbers and confirm they're all in the top ~30 lines of the file.
4. `make draken && make compile 2>&1 | tail -5` — builds.
5. `make dt 2>&1 | tail -3` — passes (no regression).
6. Smoke test each migrated function in a SQL query:
   ```sql
   SELECT EXTRACT(YEAR FROM ts_col),
          DATE_TRUNC('month', ts_col),
          DATE_TRUNC('day', date_col),
          DATEDIFF('day', start_ts, end_ts),
          DATE_FORMAT(ts_col, '%Y-%m-%d'),
          UNIXTIME(ts_col)
     FROM <a table with timestamp + date columns>
   ```
   All should return reasonable results, no `InvalidFunctionParameterError`
   for valid timestamp/date inputs.
7. `git diff --stat HEAD` — exactly one file changed.

## 8. Reporting back

- The seven acceptance outputs above.
- A note on whether `date_format` needed a DATE32→TIMESTAMP64
  conversion step (depends on whether `vector_date_format` accepts
  DATE32 directly — check `opteryx/compiled/nanobind/vector_temporal_arith.cpp`).
- A note on whether `date_floor` accepts DATE32 input. The existing
  code doesn't gate; verify against the underlying
  `vector_floor_temporal` extension.
- Any surprises in error-message wording — particularly whether tests
  depend on the old wording (search `tests/` for "TimestampVector" /
  "Date32Vector" / "expects TimestampVector"; update tests too if
  found, in this same diff).

## 9. After this lands

The pattern this ticket establishes — `arr._nb.type ==
_DrakenType.TIMESTAMP64` rather than `arr.__class__.__name__ ==
"TimestampVector"` — is the model for migrating any other Python-edge
function file with the same rot. Candidates surfaced by grep:

```
grep -rln 'Integer64Vector\|TimestampVector\|Date32Vector\|StringVector\|Float64Vector\|ConstantVector' opteryx/expression/functions/implementations/
```

If that grep returns other files, they're follow-up tickets in the same
shape. Not this ticket; surface them in §8 reporting.
