# Draken Arrow Eradication Plan

Arrow usage in draken is legitimate **only** at explicit conversion boundaries:
- `to_arrow()` — output to Arrow (expected)
- `from_arrow()` / `vector_from_arrow()` — input from Arrow (expected)
- `iter_from_arrow()` / `arrow_type_to_draken()` — interop utilities (expected)
- `ArrowVector` — fallback wrapper for unimplemented types (expected)

The five items below are illegitimate: Arrow used as an internal processing intermediary or as a hard module-load dependency.

---

## Vector Representation Guidance

For semantic embeddings, draken should not lean further into generic
`ArrayVector` semantics.

Recommended direction:

1. introduce a dedicated native `VectorVector` for dense semantic embeddings
2. use Arrow only as the interchange/storage boundary
3. represent embeddings in Arrow as:

```text
FixedSizeList<float32, N>
```

Practical implication:

1. `vector_from_arrow()` should eventually recognize fixed-size float lists as
   vectors, not just generic arrays
2. internal vector-distance kernels should consume `VectorVector`, not
   `ArrayVector`
3. this keeps embedding semantics explicit and avoids baking semantic-vector
   behavior into generic nested-array execution paths

---

## Issue 1 — `morsel.pyx:67` Top-level `import pyarrow as pa`

**Problem:** The only top-level (module-level) Arrow import in draken. Draken fails to load entirely if pyarrow is absent, even for code paths that never touch Arrow.

**Used at:** `_is_boolean_mask()` (line 1228), the DrakenType→Arrow type mapper (line 1562), and `to_arrow()` (line 1742 — already has its own local import).

**Fix:** Remove the top-level import. Add `import pyarrow as pa` lazily inside each function body that uses `pa.` without its own local import: `_is_boolean_mask()` and the type-mapper function. `to_arrow()` already imports locally.

> Note: lazy imports don't remove Arrow as a dependency — they ensure it is only required when a boundary function is actually called, not on module load. The goal is that draken loads cleanly without pyarrow installed.
---

## Issue 2 — `morsel.pyx:967` Last-resort Arrow slice fallback

**Problem:** When `vec.take(indices_view)` and `vec.take(py_indices)` both raise, the code silently round-trips through Arrow: `vec.to_arrow()` → slice → `vector_from_arrow()`. Arrow is used to paper over missing native `take()` implementations.

**Fix:** Delete the last-resort block. Replace with:
```python
raise NotImplementedError(f"{type(vec).__name__} does not implement take()")
```
This surfaces the real gap — whichever vector type triggers it needs a native `take()`.

---

## Issue 3 — `array_vector.pyx:482` `from_sequence()` uses Arrow as constructor

**Problem:** `from_sequence(data)` converts a Python list-of-lists into an `ArrayVector` by doing `pa.array(data)` → `from_arrow()`. Arrow is the parsing engine, not a boundary.

**Fix:** Rewrite natively:
1. Walk `data` once — flatten sublists into a flat string list, build `int32_t` offsets, build list-level null bitmap.
2. Build a `StringVector` via `StringVectorBuilder.append_bytes()`.
3. Call `array_vector_from_parts()` directly (already exists for this purpose; used by the Parquet reader).

---

## Issue 4 — `interop/arrow.pyx:220` `vector_from_sequence()` Arrow fallback for strings

**Problem:** After typed memoryview paths (int64, float64, bool) and constant detection, `vector_from_sequence()` falls back to `pa.array(data)` → `vector_from_arrow()` for string/binary sequences. Arrow is used for type inference and construction.

**Fix:** Add a native string path before the fallback:
1. If the first non-None element is `str` or `bytes`, build a `StringVector` directly via `StringVectorBuilder`.
2. If elements are `list`, call the fixed `from_sequence()` from Issue 3.
3. If truly unhandleable, raise `TypeError` — no Arrow fallback.

> Note: this is interop code — if the native path becomes complex, revert and keep the Arrow fallback. Arrow in interop is acceptable; this issue is lower priority than Issues 1–3 and 5.
---

## Issue 5 — `interval_vector.pyx:637` `apply_to_temporal()` returns `pa.array`

**Problem:** `apply_to_temporal()` is a pure Cython compute function (interval arithmetic). Its last line is `return pa.array(rows, type=pa.timestamp("us"))` — Arrow used to wrap the output. Callers receive a `pa.Array` from what should be a Draken function.

**Fix:** Eliminate the Python `rows` list entirely. Preallocate a `TimestampVector` buffer upfront (`out_len` elements), write `result_microseconds` directly into `ptr.data` each iteration (same pattern as `_build_int64_vector()` in the JSONL reader), set null bits for skipped rows, and return the `TimestampVector`. Zero Python lists, zero Arrow.
