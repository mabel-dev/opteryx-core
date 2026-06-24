# Sorted Dictionary Optimization

Status: in progress. Authoritative spec for the sorted-dictionary feature.

## Goal

When a Parquet dictionary's unique values are written in ascending order, a
reader can turn range/equality predicates into a **contiguous code interval**:
two binary searches over the dictionary, then an integer range compare on the
code stream — no per-row value materialization. The property flows end to end:

```
rugo writer (sort dict, set is_sorted)
   -> parquet file (DictionaryPageHeader.is_sorted = true)
   -> rugo reader (read is_sorted -> DecodedColumn.dict_ordered)
   -> draken vector (DRAKEN_DICT_KEYS_SORTED flag)
   -> compare / min-max / order-by kernels exploit it
```

The file-level flag dies at the scan boundary; the draken flag carries the win
into execution. That is why both halves exist.

## Locked decisions

1. **Skip floats entirely.** Sortable dict types are INT32/INT64 (incl.
   DATE32, TIMESTAMP64) compared numerically, and BYTE_ARRAY compared by
   unsigned-byte lexicographic order (matches the column-stats ordering and
   Parquet's BYTE_ARRAY ordering). FLOAT/DOUBLE dictionaries are never sorted
   and never advertise `is_sorted` — NaN and -0.0 break monotonic code ranges.
   BOOL/FLBA/DECIMAL/INTERVAL are not dictionary-encoded, so they are out of
   scope automatically.
2. **Sort on write (WORM).** Both the auto-build and the preserve paths sort
   the dictionary at write time. The preserve path pays an `O(N)` code remap;
   that is accepted (write once, read many). Disable via `dictionary=False`.
3. **Phase 1 consumer is the compare/range kernel** (highest traffic, cleanest
   data flow). MIN/MAX and ORDER BY follow in Phase 2.

## Correctness contract (CLAUDE.md §11)

`DRAKEN_DICT_KEYS_SORTED` is bit 2 of `DrakenVector.flags`
(`draken/core/buffers.h`; bits 2..7 were reserved).

- **Pure hint.** The uniform path `data[selection[i]]` must produce the
  identical answer with or without the flag. A sorted fast-path whose result
  differs is a bug, never an optimization.
- **Trust is absolute.** If set, the dictionary *must* be ascending, or every
  binary-search consumer silently returns wrong answers.
- **Default false.** Only an operation that *guarantees* the order sets it.
- **Scoped to dict shape** (`draken_is_dict`). Meaningless on dense/constant.

## Components

### Writer — `rugo/src/parquet/_parquet_writer.hpp`
`build_dict_column` sorts the dictionary (one place, covers both auto-build and
preserve). It computes a permutation `perm[new_code] = old_code`, emits the
dictionary page values in `perm` order, remaps each row's code through the
inverse, and writes `DictionaryPageHeader.is_sorted = true` for sortable types.
Stats and bloom are computed from the original codes before this remap, so they
are unaffected.

### Reader — `rugo/src/parquet/`
- `decode_page.cpp`: the dictionary-page-header parser must read field 3
  (`is_sorted`) — currently skipped, so `dict_ordered` is always false.
- `DecodedColumn.dict_ordered` already exists and is assigned from the header
  (`decode_column.cpp`), and is exposed in `parquet_reader.pxd`.
- `parquet_reader.pyx`: when `dict_ordered`, set `DRAKEN_DICT_KEYS_SORTED` on
  the constructed vector (via the `draken_vector_own_dict_*` bridge ctors).

### Draken — `draken/core/buffers.h`
- `#define DRAKEN_DICT_KEYS_SORTED (1u << 2)`.
- predicate `draken_dict_is_sorted(v)`.
- dict ctors propagate the bit; it travels automatically via `CxxColumn.view`
  (POD copy) and `VecResult.flags` -> `vecresult_to_owner`.

### Consumers
| Consumer | File | Sorted fast-path |
|---|---|---|
| range/compare | `draken/ops/int64_compare.h`, `string_compare.h` | predicate -> code interval via lower/upper_bound; integer range compare per row |
| MIN/MAX | `draken/ops/int64_reductions.h` | min/max present code, single value lookup |
| ORDER BY | `opteryx/compiled/morsel_ops/sort.pyx` | skip the dict remap; codes are already sort keys |
| decode-time chunk skip | `rugo/src/parquet/decode_column.cpp` | extend `DictSkipPredicate` to ranges via binary search |

## Transformation preservation

Two distinct properties — do not conflate:
- `DRAKEN_DICT_KEYS_SORTED` — about the dictionary; valid only on dict shape.
- (future) row-order sortedness on a dense vector — out of scope.

Today take/slice/mask/filter all rebuild to dense, so the dict (and its flag)
is dropped — dense output is simply not dict-shaped, so there is nothing to
retain. The flag becomes preservable only once an op keeps dict shape (subset
codes, same `data`). That is Phase 4 and is independent of the rest.

| Operation | Today | Flag rule |
|---|---|---|
| compare / min-max / sort-keys | reads dict | consumes; does not carry |
| take / slice / mask / filter | rebuild to dense | flag not applicable (dict-ness gone) |
| concat of differing dicts | rebuild to dense | cleared |
| future dict-rebuild / re-dict | new order | must clear unless re-sorted |

## Phasing

- **Phase 1** — flag end to end + compare/range consumer. Writer sort, wire
  read, draken flag, compare kernel. Tests: pyarrow/DuckDB still read the
  files; compare output identical with flag on vs forced off.
- **Phase 2** — MIN/MAX + ORDER BY consumers.
- **Phase 3** — decode-time range chunk-skip.
- **Phase 4 (optional)** — dict-preserving filter/take that carries the flag.
