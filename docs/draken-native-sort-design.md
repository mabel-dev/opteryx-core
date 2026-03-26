# Draken-native Sort Operator – Opteryx Design

## Goal

Replace the existing Arrow-backed `SortNode` implementation with a **Draken-native sort operator** that:

- operates on **Draken `Morsel` objects** (the engine’s in-memory batch unit)
- supports **multi-column `ORDER BY`** with stable and deterministic ordering
- keeps **rows aligned across columns** by computing a single permutation vector and applying it to all columns
- enables **per-column specialization** via Draken `Vector` implementations
- minimizes **memory movement** and avoids concatenating all data into one Arrow table
- integrates cleanly into the **Draken execution engine (`opteryx/operators`)

This document is intended to be practical for Opteryx/Draken implementers: it references the existing code paths that will be replaced (`SortNode`) and the patterns to follow (`HeapSortNode`, `Morsel.take()`).

---

## Where this lives in Opteryx

### Current (Arrow-based) implementation

- `opteryx/operators/sort_node.py` collects incoming `pyarrow.Table` morsels, concatenates them, and uses `pyarrow.Table.sort_by()` to produce sorted output.
- This is the implementation that should be replaced by the Draken-native sort.

### Existing Draken-native analog (top‑N)

- `opteryx/operators/heap_sort_node.py` already performs sorting on `Morsel` objects and uses Draken `Vector` primitives.
- It is a good reference for how to implement a fully Draken-native sort operator.

### Planner integration

- The physical planner creates `SortNode` for `ORDER BY` (see `opteryx/planner/physical_planner.py`).
- A Draken-native sort operator should remain compatible with the existing planner output (same `order_by` parameter format).

---

## Draken primitives you must use

### Morsel (`opteryx.compiled.draken.morsels.morsel.Morsel`)

A `Morsel` is the fundamental batch unit in the engine. Key properties/methods:

- `morsel.num_rows` – number of rows
- `morsel.column(name)` – gets a `Vector` for a column name
- `morsel.take(indices)` – reorders all columns by the given index list (in-place)
- `morsel.to_arrow()` – converts the morsel to an Arrow table (cursor boundary)

A Draken-native sort operator should accept `Morsel` objects as input/output and use `morsel.take(permutation)` to apply the sort.

### Vector (`opteryx.compiled.draken.vectors.vector.Vector`)

Each column in a `Morsel` is a `Vector`. For sorting, the relevant APIs are:

- `Vector.take(indices)` — reorder values by index array (same API used by `Morsel.take()`)
- `Vector.compress()` / `Vector.compress_into(buf)` — produce fixed-width integer keys used for radix/packed key sorting
- `Vector.hash_into(buf)` — produce hash-based keys (useful for mixed-type or composite keys)

The sort operator should rely on these methods rather than converting vectors to Python lists.

---

## Sort specification (ORDER BY)

The query planner passes sort information into `SortNode` as:

- `order_by`: `[(Node, direction), ...]`

`SortNode` resolves that into a **mapped order** of `(column_name, direction)` via:

- `column.schema_column.identity` for expression evaluation results
- positional sorting when the order-by item is a literal integer

A Draken-native implementation should operate on this mapped order (col name + direction) the same way.

---

## Execution flow (Draken-native)

### 1) Local sort (per-morsel)

For each incoming `Morsel`:

1. Determine the sort key vectors from the mapped order
2. Build a **permutation vector** (int32 row indices in sorted order)
3. Apply the permutation to the morsel using `morsel.take(perm)`
4. Emit the sorted morsel downstream

This is the primary implementation goal.

> Note: The existing Arrow-based `SortNode` concatenates all morsels and sorts once. The Draken-native design should avoid that to keep memory usage bounded and enable streaming.

### 2) Optional global merge sort (future)

If a fully global order is required (true total ordering across all morsels), a second stage can merge sorted morsels via a k-way merge:

- Maintain a cursor into each morsel
- Compare only the key columns
- Release rows in global order

This is a separate operator (e.g., `MergeSortNode`) that can be added after the local sort stage.

---

## Permutation key building (Draken style)

### Permutation vector definition

A permutation vector is an `int32` array where:

- `perm[i]` is the original row index for the row that should appear at position `i` in sorted output

After building `perm`, the final step is:

```python
morsel.take(perm)
```

### Key extraction / encoding

The goal is to build a sortable representation of each key column without materializing Python objects.

#### Fixed-width types (ints, floats, timestamps)

Use `Vector.compress()` or `Vector.compress_into(buf)` to get an `int64` (or `int32`) representation.

This compressed buffer can be radix-sorted efficiently.

#### Dictionary-encoded types

If the key vector is dictionary-encoded, we can sometimes use the dictionary IDs directly. Otherwise, we fall back to decoding or comparison.

#### Strings

Use a prefix-based key (e.g., first 8 bytes) for a fast path, then perform a tie-breaking compare on the remaining values.

#### Multi-column (composite) keys

Two patterns:

1. **Stable multi-pass sort** – sort by the least significant key first, using a stable sort for each key.
2. **Packed key sort** – encode multiple keys into a single sortable value (e.g., 64-bit packed key) and radix sort.

A performant Draken implementation should prefer (2) where possible, but (1) is acceptable as a first pass.

---

## Null ordering and direction

Null ordering is determined by the query semantics (NULLS FIRST / NULLS LAST). In Opteryx this is derived from the planner and mapped into the key comparison.

Implementation options:

- Use a sentinel compressed value for nulls (e.g., `INT64_MIN`) and ensure it respects NULLS FIRST/LAST
- Use a separate nulls mask and perform a stable partition after sorting

Direction (`ASC` / `DESC`) should be applied per key, consistent with SQL semantics.

---

## Column specialization (Draken Vector optimizations)

Sorting performance comes from specializing key extraction and comparisons.

Common vector specializations:

- **Fixed-width vectors** (`Int64Vector`, `Float64Vector`, `TimestampVector`) → radix-sort on `compress()` output
- **String vectors** → prefix key + tie-breaker compare
- **Dictionary vectors** → use dictionary ids where safe; otherwise fall back to compare
- **ArrowVector fallback** → use PyArrow compute (acceptable but slower)

The overall sort operator should be generic, but should allow per-vector fast paths.

---

## Draken implementation notes (code pointers)

### Replace `SortNode` Arrow logic

Current `SortNode.execute()` (in `opteryx/operators/sort_node.py`):

- collects incoming Arrow tables into `self.morsels`
- concatenates them with `pyarrow.concat_tables`
- calls `table.sort_by(mapped_order)`
- slices output into chunks of `CHUNK_SIZE`

The Draken-native implementation should instead:

1. Receive `Morsel` inputs (and/or Arrow tables that can be converted to `Morsel` via `Morsel.from_arrow()`)
2. For each morsel, compute a permutation vector and call `morsel.take(perm)`
3. Yield sorted `Morsel` objects downstream (cursor layer can convert to Arrow as needed)

### Model after `HeapSortNode`

`HeapSortNode` already includes:

- `_sorted_indices()` which computes a list of row indices based on Python sorting (correct but slow)
- `_materialize_rows()` which takes indices and uses `morsel.take()` to apply them

A new `SortNode` should replace the Python sort in `_sorted_indices()` with a Draken-native argsort implementation.

---

## Testing

Existing tests to reference:

- `tests/unit/operators/test_heap_sort_dictionary_fastpath.py` (covers several vector types and order-by behavior)

New tests should validate:

- Multi-column `ORDER BY` sorting
- Null ordering (NULLS FIRST / NULLS LAST)
- Stability (rows with equal key values preserve input order)
- Behavior when input is a `Morsel` vs. when input is an Arrow table (conversion step)

---

## Memory / performance notes

- The primary temporary allocation is the permutation buffer (`int32[row_count]`).
- If possible, reuse a buffer across morsels to avoid repeated allocation.
- Avoid creating Python lists of key values; use Draken buffers (Cython-level) wherever possible.

---

## Non-goals

This design does not cover:

- distributed / multi-node sorting
- external-memory (disk-backed) sorting
- streaming top‑K (already handled by `HeapSortNode`)

---

## Next steps

1. Implement a Draken-native `SortNode` that sorts each morsel using `morsel.take()`.
2. Add unit tests covering `SortNode` behavior on Draken `Morsel` inputs.
3. Optionally add a k-way merge stage for full global ordering.

