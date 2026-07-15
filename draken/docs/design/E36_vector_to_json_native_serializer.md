# E.36 — Native `Vector._to_json()` column serializer + move the shared value renderer into draken

> **Status:** IN PROGRESS.
> - **Step 1 — move the shared renderer into draken: DONE** (verified by a
>   clean `rugo_native` recompile; rugo output byte-identical).
> - **Step 2 — `render_json_column` helper + `Vector._to_json()` binding:
>   DONE & TESTED.** `tests/rugo/test_vector_to_json.py` (7 tests) asserts
>   `_to_json()` equals `write_jsonl` column-for-column across all scalar
>   types, escaping, nulls, NaN→null, dates, RFC-3339 timestamps, decimals,
>   arrays, sliced (non-identity selection), and empty columns.
> - **Step 3 — the `jobs.opteryx` `/results` consumer: DONE** (§4). Both the
>   verbose and non-verbose paths now render each column with `_to_json()` and
>   splice the pre-rendered `data` fragment into the response bytes, replacing
>   the per-column `to_pylist()` + pydantic/`json` re-encode. Verified
>   end-to-end against a mock manifest (valid JSON, `+00:00` timestamps,
>   `identity` still stripped from `sensor_data`).
>
> **Wire-format decision (§7) is made: the output matches rugo's JSON writer by
> construction** — `_to_json` calls the same `render_json_scalar` as
> `write_jsonl`, so `/results` values render identically to `/download`.
>
> **Goal:** let a single draken `Vector` render its own values to a JSON array
> as one native `bytes` object — `Vector._to_json() -> bytes` producing
> `[v0,v1,…,vN]` — with **zero per-value Python object boxing**. This replaces
> the `Vector.column(name).to_pylist()` → pydantic/`json.dumps` path that the
> `jobs.opteryx` `/jobs/{id}/results` endpoint uses to build result payloads,
> where up to 10 000 × N `PyObject`s are allocated only to be immediately
> re-serialized to text.
>
> **Why now:** the `/results` payload builder is the last hot path in that
> service that still materializes the whole column set as Python objects. The
> sibling `/download` endpoint was already moved onto rugo's native
> `write_csv` / `write_jsonl` morsel writers; `/results` returns a *columnar*
> JSON shape (`{name, type, values:[…]}` per column), for which no native
> writer existed. The value-formatting logic those native writers use is,
> however, exactly what a column-to-JSON-array serializer needs — so the work
> is mostly *reuse*, not new rendering code.
>
> **Predecessor:** the rugo CSV/JSONL native writers (`_text_render.hpp`,
> `_value_format.hpp`) and the draken `_vector_shim.pyx` (E.24/E.25).

---

## 1. The shared code that moved (Step 1 — done)

The per-value/per-cell text renderer lived in `rugo/src/_value_format.hpp`.
It is **pure** — it depends only on draken types (`core/buffers.h`,
`core/string_slot.h`) and vendored ryu (`third_party/ulfjack/ryu`), with no
rugo, no Python, no yyjson. It exposes:

- `fmt_int64 / fmt_double / fmt_date / fmt_timestamp / fmt_time / fmt_decimal*`
  — scalar → text, matching OData/RFC-3339 conventions.
- `json_string` / `csv_field` — JSON and RFC-4180 escaping.
- `render_json_scalar(out, dv, i, unit, scale)` — the JSON representation of
  the scalar at logical row `i` of a `DrakenVector` (honours the
  `data[selection[i]]` indirection and the validity bitmap).

Two consumers now need it:

1. **rugo** — its `_text_render.hpp` morsel writers (`csv_write`, `jsonl_write`)
   `#include` it and call `render_json_scalar` / the `fmt_*` helpers.
2. **draken** — the new `Vector._to_json()` (Step 2) needs the same renderer so
   its output is **byte-identical** to what rugo's `write_jsonl` produces for
   the same values.

The dependency direction in this repo is strict: **rugo depends on draken,
never the reverse.** draken's build include-dirs do not contain `rugo/src`,
and that boundary is deliberate. A header that is now shared by both, and that
depends only on draken + third-party, therefore belongs in **draken**.

**Move performed:**

| from | to |
| --- | --- |
| `rugo/src/_value_format.hpp` | `draken/interop/value_format.hpp` |

(`draken/interop/` already houses the other "draken → external representation"
headers — `draken_to_arrow.h`, `arrow_c_data_interface.h`. A JSON/CSV text
renderer is the same kind of export surface.)

Reference updates in the same change:

- `rugo/src/_text_render.hpp`: `#include "_value_format.hpp"` →
  `#include "interop/value_format.hpp"` (resolved via `-I draken`, already on
  rugo's include path).
- `rugo/src/jsonl/_jsonl_writer.pyx` and `_jsonl_writer.pxi`: the
  `cdef extern from "_value_format.hpp"` block → `"interop/value_format.hpp"`.
  (The CSV writer reaches the renderer only through `_text_render.hpp`, so no
  extern change there — only its header-path comment.)
- `build_common.py`: the rugo_native `depends=[…]` entry
  `"rugo/src/_value_format.hpp"` → `"draken/interop/value_format.hpp"`.
- Inside the moved header, the ryu include changed from the depth-coincidental
  `"../../third_party/ulfjack/ryu/ryu.h"` to `"ryu.h"` (the
  `third_party/ulfjack/ryu` dir is on every consuming extension's include
  path), and the file banner now states it is shared with draken's Vector
  serializer.

This step is a **pure relocation**: rugo's rendered output is unchanged
byte-for-byte, so it is verifiable in isolation by rebuilding
(`make draken`) and running the rugo writer tests.

### 1.1 Deliberate deferral — the `rugo_text` namespace name

The renderer's C++ namespace is `rugo_text`. Renaming it to something
draken-owned (e.g. `draken_text`) would ripple through `_text_render.hpp` and
four rugo `.pyx`/`.pxi` `cdef extern … namespace "rugo_text"` declarations, all
of which reference it. To keep this step a reviewable no-op move, the namespace
name is **kept as-is**. A draken header exposing a `rugo_text` symbol is a
naming wart, tracked here as a follow-up (rename is mechanical, same-namespace);
it is intentionally *not* bundled with this change.

---

## 2. `render_json_column` — the new shared helper (Step 2a)

`render_json_scalar` renders one *scalar* cell and returns `"null"` for
`DRAKEN_ARRAY` (arrays are handled separately in `_text_render.hpp`'s
`ej_array`, which walks the offset buffer and calls `render_json_scalar` on
child elements). A column serializer needs the whole array `[…]` wrapper plus
array support, so add one pure helper next to `render_json_scalar` in
`draken/interop/value_format.hpp`:

```cpp
// Append the JSON array  [v0,v1,…,v(nrows-1)]  for every logical row of `dv`.
// child/cunit/cscale are used only when dv->type == DRAKEN_ARRAY.
inline void render_json_column(std::string& out, const DrakenVector* dv,
                               const DrakenVector* child, int unit, int scale,
                               int cunit, int cscale, size_t nrows) {
  out.push_back('[');
  for (size_t i = 0; i < nrows; i++) {
    if (i) out.push_back(',');
    if (dv->type == DRAKEN_ARRAY) {
      // same offset walk as _text_render.hpp::ej_array
      if (!row_valid(dv->validity, i)) { out.append("null"); continue; }
      const int32_t* offs = (const int32_t*)dv->data;
      uint32_t p = dv->selection[i];
      int32_t s = offs[p], e = offs[p + 1];
      out.push_back('[');
      for (int32_t k = s; k < e; k++) {
        if (k > s) out.push_back(',');
        render_json_scalar(out, child, (size_t)k, cunit, cscale);
      }
      out.push_back(']');
    } else {
      render_json_scalar(out, dv, i, unit, scale);
    }
  }
  out.push_back(']');
}
```

This is intentionally **single-threaded and cast-kernel-free** — `/results` is
capped at 10 000 rows, so the dictionary-dedup cast path and thread pool that
`_text_render.hpp` uses for large morsel exports are not worth their
dependencies here. It keeps `Vector._to_json()` dependent only on the pure
renderer.

(Optional, non-blocking cleanup: rugo's `ej_array` could be refactored to call
`render_json_column`'s inner branch to remove the duplicated offset walk.)

---

## 3. `Vector._to_json()` — the binding (Step 2b)

In `draken/vectors/_vector_shim.pyx`, mirror the extern + `bytes` pattern the
jsonl writer already uses. Everything the renderer needs beyond the raw
`DrakenVector*` — the temporal unit and decimal scale, and the ARRAY child —
is carried on the nanobind handle `self._nb`, exactly as `_jsonl_writer.pyx`
reads it:

```cython
cdef extern from "interop/value_format.hpp" namespace "rugo_text" nogil:
    void render_json_column(string& out, const DrakenVector* dv,
                            const DrakenVector* child, int unit, int scale,
                            int cunit, int cscale, size_t nrows)

def _to_json(self):
    """Serialize this column's values to a JSON array as one bytes object.

    Byte-identical to the values rugo's write_jsonl would emit for the same
    column, without materializing per-value Python objects.
    """
    cdef const DrakenVector* dv = self._dv
    cdef const DrakenVector* child = NULL
    cdef int unit = 0, scale = 0, cunit = 0, cscale = 0
    if dv == NULL:
        return b"[]"
    # unit/scale live on the nanobind logical descriptor (see _jsonl_writer.pyx)
    u = self._nb.logical_type_unit
    if u is not None: unit = _unit_code(u)
    sc = self._nb.logical_type_scale
    if sc is not None: scale = <int>sc
    if dv.type == DRAKEN_ARRAY and self._nb.array_child_type is not None:
        cv = Vector(self._nb.array_child)
        child = cv.unified()
        cu = cv._nb.logical_type_unit
        if cu is not None: cunit = _unit_code(cu)
        csc = cv._nb.logical_type_scale
        if csc is not None: cscale = <int>csc
    cdef string out
    render_json_column(out, dv, child, unit, scale, cunit, cscale, dv.length)
    return PyBytes_FromStringAndSize(out.data(), out.size())
```

`_unit_code` is the same 4-value mapping already defined in the jsonl writer
(`s→0, ms→1, us→2, ns→3`); factor it out or duplicate the four-line helper.

Build wiring: add `draken/interop/value_format.hpp` to the `depends` of the
`vectors.vector` extension in `build_common.py` (its include-dirs already
resolve `interop/value_format.hpp` and `ryu.h`, so no new include dir is
needed).

---

## 4. Consumer integration in `jobs.opteryx` (Step 3)

Today `get_job_results` does, per column:

```python
field["values"] = full_table.column(name).to_pylist()   # 10k×N PyObjects
```

then hands the whole response (including `data`) to pydantic
`model_dump(mode="json")` and FastAPI/`json.dumps`. Pre-rendered JSON bytes
cannot pass through pydantic (it would escape them as a string), so the
`data` block is assembled with the raw fragments spliced in:

```python
# each column: values is already a JSON array literal (bytes)
parts = []
for field in manifest_columns:
    values = full_table.column(field["name"])._to_json()   # bytes: b"[…]"
    parts.append(
        b'{"name":' + _json_dumps(field["name"]).encode()
        + b',"type":' + _json_dumps(field["type"]).encode()
        + b',"values":' + values + b'}'
    )
data_fragment = b'[' + b','.join(parts) + b']'
```

`data_fragment` is then spliced into the response byte buffer alongside the
small scalar fields (`total_rows`, `execution_id`, …). This fits the raw
`Response(content=…, media_type="application/json")` shape the non-verbose
path already returns; the verbose path splices the same `data_fragment` into
its serialized model. Empty result sets (`accumulated_morsels` empty) keep the
manifest's declared columns with `"values":[]`.

---

## 5. Why this is faster

The current path, per `/results` request, for the payload bulk:

1. `to_pylist()` allocates one `PyObject` per cell (int/float/str/date/…) —
   up to 10 000 × N objects, plus the list objects.
2. pydantic validates the `data` field against `List[Dict[str, Any]]`.
3. `model_dump(mode="json")` re-walks every one of those objects converting
   datetimes/etc. to JSON-friendly types.
4. FastAPI/`json.dumps` walks them a third time to produce bytes.

The native path formats each value's text **once**, straight from the columnar
buffer into a contiguous `std::string`, and hands the caller one `bytes`. No
per-value `PyObject`, no pydantic pass over the values, no re-encode. The
scalar metadata (`total_rows`, timings, …) is tiny and still goes through the
normal path.

---

## 6. Testing

- **Native (`draken/tests/native/test_vector_json.py`-style):** per-type
  coverage — int8/16/32/64, float32/64 (incl. NaN/Inf → `null`), bool, date,
  timestamp (each unit), time32/64, decimal64, decimal128, varchar/varbinary,
  null column, and ARRAY. For each, assert
  `json.loads(v._to_json()) == v.to_pylist()` **modulo the two documented
  format deltas in §7**. Exercise all encodings — dense, dict, constant,
  sliced — since correctness depends on honouring `data[selection[i]]`, and an
  empty vector (`b"[]"`).
- **Cross-writer parity:** assert `_to_json()` for each column of a morsel
  equals the corresponding values in that morsel's `write_jsonl` output,
  confirming the shared renderer really is shared.
- **rugo regression:** the Step-1 move must leave `write_csv` / `write_jsonl`
  byte-identical — run the existing rugo writer tests.
- **jobs.opteryx:** compare old vs new `/results` response bytes on a mock
  manifest (the harness used during investigation), for verbose and
  non-verbose, including a date/timestamp/decimal column.

---

## 7. Wire format — matches rugo (decided)

**The target is rugo's JSON format.** `Vector._to_json()` calls the same
`render_json_scalar` that `write_jsonl` uses, so `/results` column values are
identical to what `/download` emits for the same data — consistency by
construction, not by a parallel format decision.

As a consequence, two types change versus the *previous* `/results` path (the
`to_pylist()` → pydantic serializer), both now aligned to rugo:

| type | previous `/results` (`to_pylist` → pydantic) | rugo (`render_json_scalar`) |
| --- | --- | --- |
| **timestamp** | `2024-01-01T12:00:00Z` | `2024-01-01T12:00:00+00:00` |
| **decimal** | `123` (unscaled int) | rendered via `fmt_decimal` with the column's `logical_type_scale` |

Dates, integers, booleans, floats, strings, and nulls are unchanged. These are
downstream payload changes to announce to `/results` consumers, not options.

> Note on decimals: correct scaled rendering depends on the morsel's
> `logical_type_scale` being populated on the column (the same input
> `write_jsonl` relies on). Where the parquet reader does not carry a scale
> through, rugo renders the unscaled integer — `_to_json` inherits exactly that
> behaviour, since matching rugo is the whole point. Any scale-propagation fix
> belongs in the reader/morsel metadata, shared by both endpoints.

---

## 8. Scope / non-goals

- Not changing `/download` (already native).
- Not adding threading or the dictionary-dedup cast path to the vector-level
  serializer (unnecessary at ≤10 000 rows).
- Not renaming the `rugo_text` namespace (see §1.1).
- Not touching ranged/partial GCS reads of parquet parts — a separate, larger
  optimization for the same endpoint.
```
