# Draken Morsel Sort Permutation – Design

## Goal

Add a `morsel.sort()` method that returns a **permutation array** of row indices in sorted order, without reordering the morsel itself. This is the run-generation primitive for sort-based aggregation and the Draken-native `SortNode`.

```python
uint32[] perm = morsel.sort(column_names: list[bytes], ascending: list[bool])
```

`perm[i]` is the original row index that belongs at position `i` in sorted output. The caller applies it via the existing `morsel.take(perm)` if reordering is needed, or walks it directly during a sort-aggregate scan.

Morsels are assumed to be at most 1M rows. The permutation array is always in-memory; multi-morsel ordering is handled by the k-way merge layer.

---

## Where it lives

`morsel.pyx` is already 1700+ lines. The implementation goes in a new Cython file:

```
opteryx/compiled/sort/morsel_sort.pyx
```

`Morsel` exposes a thin wrapper:

```cython
# morsel.pyx
cpdef object sort(self, list column_names, list ascending):
    from opteryx.compiled.sort.morsel_sort import morsel_sort_permutation
    return morsel_sort_permutation(self, column_names, ascending)
```

The compiled extension follows the existing `make_draken_extension()` pattern in `setup.py`.

---

## Return type

`uint32[::1]` — a C-contiguous memoryview of unsigned 32-bit row indices.

- 1M rows × 4 bytes = 4 MB; fits in L3 cache
- `uint32` is sufficient for morsels up to ~4B rows, well above the 1M design limit
- Compatible with the existing `morsel.take()` signature

---

## Algorithm selection by column type

All column types go through `compress_into(int64_t[::1] out_buf, offset)` for key extraction. This writes a sortable `int64` representation into a pre-allocated buffer. For strings, `compress_into()` encodes the **first 7 bytes** of the string as an `int64` (zero-padded). This makes radix sort the primary algorithm for all types, with comparison sort applied only as a tie-breaker for strings that share a 7-byte prefix.

| Encoding | Key extraction | Sort algorithm |
|---|---|---|
| Fixed-width numeric — dense `Int64`, `Float64`, date, timestamp, bool | `compress_into()` → `int64` buffer | LSD radix sort (8 passes) |
| Dictionary-encoded (any value type) | extract dict codes directly as `uint8`/`uint16`/`uint32` | radix sort on codes (1–4 passes) |
| Dense string ≤ 7 chars | `compress_into()` → exact `int64` key | LSD radix sort — no tie-breaking needed |
| Dense string > 7 chars (some rows) | `compress_into()` → 7-byte prefix key | Radix sort; `memcmp` comparison sort within same-prefix bins |

### Fixed-width numeric

Call `compress_into()` to fill an `int64` key buffer. Run LSD radix sort over the key buffer to produce the permutation. 8 passes over 1M `int64` values; the 256-entry count array fits in L1 cache.

### Dictionary-encoded columns

Dictionary codes are small integers (`uint8`, `uint16`, or `uint32` depending on dictionary size). Extract codes directly from the vector internals — do not call `compress_into()`. Radix sort on codes: 1 pass for `uint8`, 2 for `uint16`, 4 for `uint32`.

This is **correct for GROUP BY**: same code → same value → same group. It is **not correct for ORDER BY** (codes are insertion-ordered, not value-ordered). ORDER BY on dictionary columns is a follow-on.

### Dense strings

`compress_into()` encodes the first 7 bytes of each string as an `int64` (zero-padded for shorter strings). This gives an exact sort key for strings of 7 characters or fewer, and a prefix key for longer strings.

**Phase 1 — radix sort on prefix keys:**

Call `compress_into()` to fill an `int64` key buffer, then run the standard LSD radix sort. This produces a permutation where rows are correctly ordered if their 7-byte prefixes differ, and grouped together (in arbitrary relative order) if their prefixes match.

**Phase 2 — comparison sort within same-prefix bins:**

Scan the permutation for runs of equal prefix keys. For each run:

- If all strings in the run are ≤ 7 chars: they are identical (same zero-padded int64 = same short string). No tie-breaking needed.
- If any string in the run is > 7 chars: sort that sub-slice of the permutation using `std::sort` with a `memcmp` comparator on the full string content.

In practice, most string columns either have short values (fully resolved by phase 1) or long values that diverge within the first 7 bytes (also fully resolved by phase 1). Same-prefix bins are typically small, so the comparison sort cost is low.

```
for each run [lo, hi) in perm where prefix_key[perm[lo]] == prefix_key[perm[lo+1]] == ...:
    if any string in run has length > 7:
        std::sort(perm + lo, perm + hi, memcmp_comparator)
```

The `memcmp` comparator receives two `uint32` indices, looks up `(data + offsets[i], offsets[i+1] - offsets[i])` for each, and compares with `memcmp` followed by length comparison for ties.

---

## Multi-column sort: LSD stable passes

Sort from **last column to first** using a **stable sort** at each step (Least Significant Digit / LSD approach). Each pass preserves the relative order of equal elements established by prior passes.

```
stable_sort(perm, keys[col_n])    # least significant
...
stable_sort(perm, keys[col_1])    # most significant
```

The radix sort is inherently stable when the scatter phase processes input left-to-right.

For a mixed ASC/DESC multi-column sort, **descending is handled per-column by flipping the key bits** before that column's radix pass — no change to the sort algorithm.

---

## Null handling

`compress_into()` writes `INT64_MIN` for null values. This produces the correct SQL default ordering without a separate null-partition pass:

| Direction | Null key value | Sort position | SQL default |
|---|---|---|---|
| ASC | `INT64_MIN` (unchanged) | first | NULLS LAST (non-default; see below) |
| DESC | `INT64_MAX` (after bit-flip) | last | NULLS FIRST (non-default; see below) |

> **Note:** The SQL standard default is NULLS LAST for ASC and NULLS FIRST for DESC (PostgreSQL behaviour). Using `INT64_MIN` as the null sentinel gives NULLS FIRST for ASC, which is the opposite. A future `null_last: bool` parameter per column can correct this by using `INT64_MAX` as the sentinel for ascending nulls. V1 leaves this as NULLS FIRST for ASC; callers that require SQL-standard null ordering should pass the flag when it is added.

---

## Descending keys

For numeric columns (including compressed int64 keys from `compress_into`):

1. Convert signed `int64` to unsigned interpretation by XORing with `0x8000000000000000` (flips sign bit so negatives sort after positives in unsigned order).
2. For descending: additionally XOR with `0xFFFFFFFFFFFFFFFF` (inverts all bits).
3. Net transform for descending signed int64: `key ^= 0x7FFFFFFFFFFFFFFF`.

Apply this transform to the key buffer **before** the radix pass for that column. No change to the radix sort implementation itself.

For dense string columns with descending order: apply the bit-flip to the prefix key buffer before phase 1 (same as numeric), and negate the `memcmp` comparator return value in phase 2.

---

## Radix sort internals

Standard LSD counting sort — no external dependencies:

```
allocate perm[N] = [0, 1, ..., N-1]   # uint32, PyMem_Malloc
allocate tmp[N]                         # uint32, scratch buffer
allocate count[256]                     # on stack, 1 KB

for each byte position b in 0..key_width-1:
    zero count[]
    for i in 0..N:
        count[(keys[perm[i]] >> (b * 8)) & 0xFF]++
    prefix-sum count[] in place
    for i in 0..N:
        byte_val = (keys[perm[i]] >> (b * 8)) & 0xFF
        tmp[count[byte_val]++] = perm[i]
    swap perm ↔ tmp

free tmp
return perm
```

Memory: two `uint32[N]` allocations (`perm` and `tmp`) via `PyMem_Malloc`, matching the existing Draken allocation pattern. The `count[256]` array is stack-allocated per pass.

Access pattern: `keys[perm[i]]` is random-access in the scatter phase. For 1M × 8-byte keys the key buffer is 8 MB; it stays warm in L3 across the 8 passes of a single column's sort.

---

## Function signature (Cython)

```cython
# opteryx/compiled/sort/morsel_sort.pyx

def morsel_sort_permutation(
    morsel,           # Morsel instance
    list column_names,  # list[bytes] — column names in sort priority order (index 0 = most significant)
    list ascending,     # list[bool]  — one entry per column
) -> object:            # returns array('I') / uint32[::1] memoryview
```

Internally dispatches per column to:

- `_radix_sort_int64(uint32[::1] perm, int64_t[::1] keys, bint asc)` — fixed-width numeric and dense strings (phase 1)
- `_radix_sort_codes(uint32[::1] perm, codes_buf, int code_width, bint asc)` — dictionary
- `_tiebreak_strings(uint32[::1] perm, string_data, int32_t[::1] offsets, int64_t[::1] prefix_keys, bint asc)` — dense string phase 2: `memcmp` sort within same-prefix bins

For dense strings, `_radix_sort_int64` runs first on the prefix keys, then `_tiebreak_strings` scans for same-prefix runs and sorts each with `std::sort`. Columns are processed in **reverse order** (last to first) to implement LSD correctly.

---

## Non-goals

- **Dictionary ORDER BY semantic correctness**: codes are insertion-ordered; alphabetic ORDER BY on dictionary strings requires sorting the dictionary first. Follow-on work.
- **NULLS FIRST / NULLS LAST as an explicit parameter**: fixed at NULLS FIRST for ASC in V1.
- **External sort / spill**: this function operates on a single in-memory morsel. Multi-morsel global ordering uses the existing `ShuffleMergeSortOperation` k-way merge.
- **Partially-sorted input optimisation** (timsort-style): future enhancement.
- **Distributed / multi-node sort**: out of scope.
