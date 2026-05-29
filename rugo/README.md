# Rugo

Rugo is Opteryx Core's internal Parquet and JSONL reader. It is optimized for metadata-driven
planning, targeted column decoding, and direct output into Draken vectors — no PyArrow, no NumPy on
the read path.

Rugo is built as part of this repository. It is not separately installable or separately versioned.
It depends on Draken (see `../draken/README.md`); decoded columns are returned as `draken` `Vector`s
and grouped into `Morsel`s.

---

## Build

Rugo is compiled by the top-level extension build:

```bash
make compile   # full rebuild
make c         # incremental rebuild
```

The repository requires Python 3.13 for supported local builds.

---

## Modules

The compiled extensions are exposed through two import packages:

| Package                | Purpose                                  |
|------------------------|------------------------------------------|
| `rugo.parquet_reader`  | Parquet metadata + column decoding       |
| `rugo._jsonl`          | JSONL schema inference + decoding        |
| `rugo.converters`      | Schema conversion helpers (Orso)         |

---

## Parquet

```python
from rugo.parquet_reader import read_metadata, read_parquet

# Schema-only metadata (footer parse, no column data)
meta = read_metadata("testdata/planets/planets.parquet")
# -> {"num_rows": int, "schema_columns": [ {name, physical_type, logical_type, nullable}, ... ]}

# Decode columns from an in-memory buffer
with open("testdata/planets/planets.parquet", "rb") as f:
    morsels = read_parquet(f.read(), column_names=["id", "name"])
# -> list[draken Morsel]  (one Morsel per row group), or None on failure
```

### Metadata

| Function | Returns |
|----------|---------|
| `read_metadata(path: str)` | `{num_rows, schema_columns}` |
| `read_metadata_from_bytes(data: bytes)` | same |
| `read_metadata_from_memoryview(mv: memoryview)` | same (memoryview must be contiguous) |

`schema_columns` is a list of dicts: `{"name", "physical_type", "logical_type", "nullable"}`.

### Decoding

```python
read_parquet(data, column_names=None)
```

- `data` — `bytes`, `bytearray`, or `memoryview` holding the Parquet file.
- `column_names` — list of column names to decode, or `None` for all columns.
- Returns a `list[Morsel]` (one per row group), or `None` if reading failed. On partial failure an
  individual column within a Morsel may be `None`.
- Designed for serial use; Opteryx parallelises by running multiple `read_parquet` calls concurrently
  across files.

### Compatibility check

| Function | Returns |
|----------|---------|
| `can_decode(path: str)` | `bool` — quick compatibility signal (not a guarantee) |
| `can_decode_from_memory(data)` | `bool` — same, for an in-memory buffer |

### Range-read / single-column decode

For fine-grained I/O (fetch only the column-chunk byte ranges you need), decode metadata first, then
decode chunks individually:

| Function | Returns |
|----------|---------|
| `decode_column_from_chunk(chunk_bytes, col_stats, row_mask=None)` | `draken Vector` or `None` |
| `decode_column_from_chunk_to_python(chunk_bytes, col_stats)` | `list` or `None` |
| `decode_column_from_memory(data, column_name, row_group_stats, row_group_index)` | `list` or `None` |
| `decode_value(physical_type, logical_type, raw, prefer_text)` | single Python value |

`col_stats` is the per-column stats dict for the matching row group. `row_mask` is an optional
`uint8` bitmap used to skip rows during decode.

### Bloom filters

```python
test_bloom_filter(path, bloom_offset, bloom_length, value)  # -> bool
```

Evaluates a column bloom filter at the given byte offset/length for a candidate `value`.

### Telemetry

Decode timing is exposed for profiling:

```python
from rugo.parquet_reader import (
    reset_telemetry, get_telemetry,        # Cython-side phase timings
    reset_cpp_telemetry, get_cpp_telemetry # C++-side phase timings
)
```

`get_telemetry()` reports vector-construction timings per type plus call/row-group/column counts.
`get_cpp_telemetry()` reports C++ phases: `metadata_s`, `decompress_s`, `dict_parse_s`, `prescan_s`,
`page_parallel_s`, `rle_s`, `val_expand_s`, `mask_filter_s`, `validity_bmp_s`, `calls`.

### Supported decode subset

The active decode path targets flat primitive columns and is intentionally narrower than the metadata
reader:

| Area | Support |
|------|---------|
| Physical types | `int32`, `int64`, `float32`, `float64`, `boolean`, `byte_array` |
| Compression | `UNCOMPRESSED`, `SNAPPY`, `ZSTD` |
| Encodings | `PLAIN`, dictionary pages, `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` |
| Input | in-memory `bytes` / `memoryview`, with column selection |

See `src/README.md` for the authoritative supported subset and limitations.

---

## JSONL

```python
from rugo._jsonl import read_jsonl, get_jsonl_schema

result = read_jsonl(
    "testdata/example.jsonl",
    columns=["id", "name"],
    predicates=[("status", "==", "active")],
    use_threads=True,
)
if result["success"]:
    print(result["num_rows"])
    for vec in result["columns"]:   # list[draken Vector]
        ...
```

### `read_jsonl`

```python
read_jsonl(
    data,                      # file path (str) or buffer (bytes/bytearray/memoryview)
    columns=None,              # list[str] to project, or None for all
    predicates=None,           # list[(column, op, value)]; op in ==, !=, <, <=, >, >=
    explicit_schema=None,      # provide a schema instead of inferring
    infer_schema=True,
    infer_sample_size=5,       # rows sampled for type inference
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True,
    use_threads=True,          # SIMD-accelerated parallel scan/interpret
    min_rows_per_thread=2048,
)
```

Returns a result dict:

| Key | Value |
|-----|-------|
| `success` | `bool` |
| `column_names` | `list[str]` |
| `num_rows` | `int` (rows passing predicates) |
| `columns` | `list[draken Vector]` |
| `schema` | `dict[str, str]` — column name → inferred type |
| `error` | `str` (present when `success` is `False`) |

Inferred type strings: `int64`, `double`, `boolean`, `string`, `bytes`, `object`, `null`, `array<T>`.

### `get_jsonl_schema`

```python
get_jsonl_schema(data, sample_size=5)
# -> {"columns": [ {"name": str, "type": str, "nullable": True}, ... ]}
```

Infers the schema from the first `sample_size` rows.

---

## Converters

```python
from rugo.parquet_reader import read_metadata
from rugo.converters import rugo_to_orso_schema

meta   = read_metadata("data.parquet")
schema = rugo_to_orso_schema(meta, schema_name="my_table")  # -> Orso RelationSchema
```

---

## Source Layout

```
rugo/
├── __init__.py
├── parquet_reader.pxd        # Cython declarations for the parquet extension
├── _jsonl/                   # import package for the compiled JSONL extension
├── converters/               # schema/format conversion helpers (Orso)
└── src/
    ├── parquet/              # metadata, decode, compression, bloom filters, vendored zstd/lz4
    ├── _jsonl/               # active C++/Cython JSONL reader
    ├── jsonl/                # legacy JSONL bindings
    └── parquet_spec/         # vendored Apache Parquet format specification
```

---

## Notes

- Rugo is internal engine infrastructure. Prefer querying through `opteryx.session()` unless you are
  working on scan, metadata, or I/O internals.
- `can_decode(...)` is a quick compatibility signal, not a guarantee that every selected column will
  decode successfully.
- On partial decode failure, a selected column may be returned as `None` — fail loud, do not assume
  success.
- The read path is pure C++/Cython and Draken-native; no PyArrow or NumPy is involved.
