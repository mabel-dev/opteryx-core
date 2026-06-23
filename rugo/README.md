# Rugo

Rugo is Opteryx Core's internal file engine for Parquet, JSONL, and CSV — reading **and** writing. Zero external dependencies — no PyArrow, no NumPy on any read or write path. Compiled as Cython/C++ extensions. Readers emit Draken vectors; the writer consumes Draken Morsels and emits well-formed, PyArrow-readable Parquet. Not separately installable or versioned; built as part of this repository. Depends on Draken (`../draken/README.md`).

See [`_example.py`](_example.py) for a runnable read + write example.

---

## Build

```bash
make compile   # full rebuild
make c         # incremental rebuild
```

Python 3.14 (free-threaded, `3.14.5t`) is used for local builds.

---

## Modules

| Package               | Purpose                                              |
|-----------------------|------------------------------------------------------|
| `rugo.parquet`        | **Unified Parquet read/write facade** (recommended)  |
| `rugo.parquet_reader` | Low-level Parquet metadata + column decoding         |
| `rugo.parquet_writer` | Low-level Parquet writing (Morsel → bytes)           |
| `rugo.jsonl`          | JSONL read (`read_jsonl`) + write (`write_jsonl`)    |
| `rugo.csv`            | CSV read (`read_csv`) + write (`write_csv`)          |
| `rugo.converters`     | Schema/format conversion helpers (Orso)              |

---

## Parquet

`rugo.parquet` is the recommended surface: one symmetric module for reading and
writing that accepts a filename or an in-memory buffer, streams row-group
Morsels, applies predicate pushdown, and writes Morsels back to bytes.

### Quick start

```python
from rugo import parquet

# Schema-only metadata (footer parse, no column data). Path OR bytes.
meta = parquet.read_metadata("testdata/planets/planets.parquet")
print(meta.num_rows)                       # 9
print([c.name for c in meta.schema_columns])

# Streaming read: one Morsel per row group. `columns` projects; `filters`
# prune whole row groups via footer statistics (rows in surviving groups are
# NOT filtered — apply row-level predicates downstream).
with parquet.read_parquet(
    "testdata/planets/planets.parquet",
    columns=["id", "name"],
    filters=[("id", ">", 4)],              # ops: = == != < <= > >= in "not in"
) as reader:
    for morsel in reader:
        print(morsel.column(b"name").to_pylist())

# Write a Draken Morsel to Parquet bytes (ZSTD by default; "none" to disable).
data = parquet.write_parquet(morsel, compression="zstd")
with open("out.parquet", "wb") as f:
    f.write(data)
```

### `rugo.parquet` API

| Function | Returns |
|----------|---------|
| `read_parquet(source, columns=None, filters=None)` | context manager yielding one Morsel per surviving row group |
| `read_metadata(source)` | `ParquetMetadata` (`num_rows`, `schema_columns`) |
| `write_parquet(morsel, compression="zstd")` | `bytes` (whole file) |
| `write_parquet_with_bounds(morsel, compression="zstd")` | `(bytes, {col_index: (min, max)})` |

`source` is a filename (`str`) or `bytes`/`bytearray`/`memoryview`. `filters`
is a list of `(column, op, value)`; pruning is at row-group granularity.

### Low-level API (`rugo.parquet_reader`)

Most callers should use `rugo.parquet` above. The low-level module is exposed for
fine-grained control.

#### Metadata

| Function | Returns |
|----------|---------|
| `read_metadata(path: str)` | `ParquetMetadata(num_rows, schema_columns)` (typed object) |
| `read_metadata_from_bytes(data: bytes)` | same |
| `read_metadata_from_memoryview(mv: memoryview)` | same (memoryview must be contiguous) |
| `read_rowgroup_stats(data)` | `list[{num_rows, columns:[{name, physical_type, logical_type, min, max, null_count}]}]` — per-row-group stats for pushdown |

`schema_columns` is a tuple of `SchemaColumn(name, physical_type, logical_type, nullable)`.
`read_rowgroup_stats` `min`/`max` are raw stat bytes (or `None`); decode with `decode_value`.

#### Decode

```python
read_parquet(data, column_names=None, row_group_mask=None)
```

- `data` — `bytes`, `bytearray`, or `memoryview` holding the full Parquet file.
- `column_names` — `list[str]` to project, or `None` for all columns.
- `row_group_mask` — optional iterable, one truthy/falsy entry per row group; a
  falsy entry skips decoding that row group (predicate pushdown). `rugo.parquet`'s
  `filters=` builds this from `read_rowgroup_stats`.
- Returns `list[Morsel]` (one per decoded row group), or `None` on failure. On
  partial decode failure an individual column within a Morsel may be `None`.

#### Compatibility

| Function | Returns |
|----------|---------|
| `can_decode(path: str)` | `bool` — quick compatibility signal, not a guarantee |
| `can_decode_from_memory(data)` | `bool` — same, for an in-memory buffer |

#### Fine-grained / range decode

| Function | Description |
|----------|-------------|
| `decode_column_from_chunk(chunk_bytes, col_stats, row_mask=None)` | Decode a single column chunk to a Draken Vector; `row_mask` is an optional `uint8` bitmap |
| `decode_column_from_chunk_to_python(chunk_bytes, col_stats)` | Decode a single column chunk to a Python list |
| `decode_column_from_memory(data, column_name, row_group_stats, row_group_index)` | Decode one column from a full in-memory file, by row-group index |
| `decode_value(physical_type, logical_type, raw, prefer_text)` | Decode a single raw Parquet value to a Python scalar |

`col_stats` is the per-column stats dict for the matching row group from `read_metadata`.

#### Bloom filters

```python
bloom_filter_maybe_contains(path, bloom_offset, bloom_length, value)  # -> bool
```

Evaluates a column bloom filter at the given byte offset/length for a candidate `value`. Bloom filter offsets and lengths are exposed in the per-column metadata returned by `read_metadata`.

#### Telemetry

| Function | Description |
|----------|-------------|
| `reset_telemetry()` | Reset Cython-side phase timing accumulators |
| `get_telemetry()` | Return Cython-side timings per type, plus call/row-group/column counts |
| `reset_cpp_telemetry()` | Reset C++-side phase timing accumulators |
| `get_cpp_telemetry()` | Return C++-side timings: `metadata_s`, `decompress_s`, `dict_parse_s`, `prescan_s`, `page_parallel_s`, `rle_s`, `val_expand_s`, `mask_filter_s`, `validity_bmp_s`, `calls` |

### Supported decode subset

| Area | Support |
|------|---------|
| Physical types | `int32`, `int64`, `float32`, `float64`, `boolean`, `byte_array` |
| Compression | `UNCOMPRESSED`, `SNAPPY`, `ZSTD` |
| Encodings | `PLAIN`, dictionary pages (`PLAIN_DICTIONARY` / `RLE_DICTIONARY`), `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` |
| Input | In-memory `bytes` / `memoryview` with column selection |

### Writing

`rugo.parquet_writer` (and the `rugo.parquet` facade) serialize a Draken Morsel
to a well-formed, PyArrow-readable Parquet file.

```python
from rugo.parquet_writer import write_parquet, write_parquet_with_bounds
data = write_parquet(morsel, compression="zstd")          # -> bytes
data, bounds = write_parquet_with_bounds(morsel)          # + per-column min/max
```

| Area | Support |
|------|---------|
| Column types | INT8/16/32/64 (→INT64), FLOAT32 (→DOUBLE), FLOAT64, BOOL, VARCHAR/NVARCHAR/VARBINARY, VARIANT (→STRING), DATE32, TIME32/64, TIMESTAMP64 (µs/ms/ns), INTERVAL (FLBA-12), DECIMAL/DECIMAL128 (FLBA), ARRAY/LIST of those (int/float/bool/string elements), all-null (→INT32). FP16 not yet. |
| Encoding | `PLAIN` values, `RLE` definition levels, one data page per column chunk |
| Compression | `ZSTD` (default) or uncompressed |
| Statistics | per-column min/max/null_count + `column_orders` (so readers trust them) |
| Bloom filters | split-block (SBBF), XXH64, on equality-friendly columns; `bloom_filters=True\|False\|[names]` |
| Layout | single row group per Morsel |

Unsupported column types fail loud (no silent skip). Nested LIST/MAP/STRUCT and
dictionary-encoded *output* are not yet implemented.

### Limitations

- Not a full Parquet replacement reader; decode support is intentionally narrow.
- `GZIP`, `LZO`, `BROTLI`, `LZ4`, and `LZ4_RAW` compression codecs are not implemented in the decode path.
- `INT96` is not supported for value decoding in `read_parquet(...)`.
- `FIXED_LEN_BYTE_ARRAY` value decoding is not implemented.
- Decode logic is built around `DATA_PAGE` (V1); `DATA_PAGE_V2` is not handled.
- Decode reads from a single data-page path per column chunk; files requiring full multi-page streaming decode may return partial or failed column results.
- Nested, list, and map-heavy files are not a primary decode target; flat primitive columns are the intended shape.
- On partial decode failure, individual columns may be returned as `None`.
- Metadata extraction is broad, but known edge cases remain around list/nested column naming normalisation.

### Performance

Metadata reads (schema + row-group stats, no column data) are fast and comparable to PyArrow. The high-level `read_parquet()` / `rugo.parquet` path is a serial utility: it reconstructs Draken vectors from decoded columns via the native sequence constructors and materializes through Python, so it is correctness-first, not the performance path. Opteryx's production scan path (`opteryx.connectors.parquet_io`) builds vectors zero-copy off-GIL and is independent of this API. (The Draken vector-migration gaps E.28-1..3,5..9 that previously stubbed this path are fixed; only nested/array decode, E.28-4, remains.)

---

## JSONL

### Writing

```python
from rugo.jsonl import write_jsonl
data = write_jsonl(morsel)   # -> bytes, one JSON object per row
```

Native (no pyarrow); value formatting in C++. Doubles use `std::to_chars`
(shortest round-trip); dates/timestamps render ISO-8601 strings; decimals are
JSON numbers; arrays render as JSON arrays (null list / empty list / null
element are all distinguished); nulls are `null`.

### Quick start

```python
from rugo.jsonl import get_jsonl_schema, read_jsonl

# Infer schema from sample rows
schema = get_jsonl_schema("testdata/example.jsonl", sample_size=5)
# -> {"columns": [{"name": str, "type": str, "nullable": True}, ...]}

# Read from file path with projection and predicate pushdown
result = read_jsonl(
    "testdata/example.jsonl",
    columns=["id", "name"],
    predicates=[("status", "==", "active")],
)
if result["success"]:
    print(result["num_rows"])
    for vec in result["columns"]:  # list[draken Vector]
        print(vec.to_pylist())

# Read from bytes input
with open("testdata/example.jsonl", "rb") as f:
    result = read_jsonl(f.read(), columns=["id"])
```

### API: read_jsonl

```python
read_jsonl(
    data,                       # file path (str) or buffer (bytes/bytearray/memoryview)
    columns=None,               # list[str] to project, or None for all
    predicates=None,            # list[(column, op, value)]; op in ==, !=, <, <=, >, >=
    explicit_schema=None,       # provide a schema dict instead of inferring
    infer_schema=True,
    infer_sample_size=5,        # rows sampled for type inference
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True,
    use_threads=True,           # SIMD-accelerated parallel scan/interpret
    min_rows_per_thread=2048,
)
```

Return dict:

| Key | Value |
|-----|-------|
| `success` | `bool` |
| `column_names` | `list[str]` |
| `num_rows` | `int` — rows passing predicates |
| `columns` | `list[draken Vector]` |
| `schema` | `dict[str, str]` — column name → inferred type string |
| `error` | `str` — present only when `success` is `False` |

Inferred type strings: `int64`, `double`, `boolean`, `string`, `bytes`, `object`, `null`, `array<T>`.

### API: get_jsonl_schema

```python
get_jsonl_schema(data, sample_size=5)
# -> {"columns": [{"name": str, "type": str, "nullable": True}, ...]}
```

Infers the schema from the first `sample_size` rows. Returns `{"columns": []}` on failure; does not raise.

### Performance

116 MB, 1.5 M rows, 5 cols. PyArrow `read_json` multithreaded.

| Query shape | Rugo | PyArrow |
|-------------|------|---------|
| `SELECT *` | ~67 ms | ~53 ms |
| `SELECT one_col` | ~33 ms | ~53 ms |
| `SELECT col WHERE id < 150k` (~10% pass) | ~15 ms | ~53 ms |
| `SELECT col WHERE id < 15k` (~1% pass) | ~7 ms | ~53 ms |

`SELECT *` is bulk-bound — PyArrow's bulk materialiser has an edge. The analytical shapes — project + filter — are 1.2–5×+ faster; the advantage grows with selectivity and table width.

### Caveats

- String/object-heavy fields are often returned as `bytes` (binary-preserving), not eagerly decoded Python `str`/`dict` values.
- Mixed or deeply nested array-object content may fall back to raw JSON text/bytes in edge cases.
- Schema inference is sampled (`infer_sample_size` rows only); pass `explicit_schema` when the schema is known to avoid mismatches on heterogeneous files.

---

## CSV

### Writing

```python
from rugo.csv import write_csv
data = write_csv(morsel, delimiter=",", header=True)   # -> bytes (RFC 4180)
```

Native (no pyarrow); fields are quoted per RFC 4180 (delimiter/quote/newline →
quoted, quotes doubled), nulls are empty fields, and ARRAY columns render as a
(quoted) JSON array. Shares the C++ value formatter with the JSONL writer.

### Quick start

```python
from rugo.csv import read_csv

# Basic read — all columns
result = read_csv("testdata/data.csv")

# Projection
result = read_csv("testdata/data.csv", columns=["col1", "col2"])

# Predicate pushdown
result = read_csv("testdata/data.csv", columns=["name"], predicates=[("age", ">", 30)])

# TSV variant
result = read_csv("testdata/data.tsv", delimiter="\t")

if result["success"]:
    for vec in result["columns"]:
        print(vec.to_pylist())
```

### API: read_csv

```python
read_csv(
    data,               # file path (str) or buffer (bytes/bytearray/memoryview)
    columns=None,       # list[str] to project, or None for all
    predicates=None,    # list[(column, op, value)]; op in ==, !=, <, <=, >, >=
    delimiter=",",      # field separator character
    has_header=True,    # whether the first row is a header
    use_threads=True,   # parallel scan
)
```

Parameter table:

| Parameter | Type | Description |
|-----------|------|-------------|
| `data` | `str` / `bytes` / `bytearray` / `memoryview` | File path or in-memory buffer |
| `columns` | `list[str]` or `None` | Columns to project; `None` returns all |
| `predicates` | `list[tuple]` or `None` | Filter predicates applied before typed build |
| `delimiter` | `str` | Single-character field separator |
| `has_header` | `bool` | Whether row 0 is a header row |
| `use_threads` | `bool` | Enable parallel scan |

Return dict:

| Key | Value |
|-----|-------|
| `success` | `bool` |
| `column_names` | `list[str]` |
| `num_rows` | `int` — rows passing predicates |
| `columns` | `list[draken Vector]` |

Type inference cascade per field: `int64` → `float64` → `VARCHAR` → `null` (empty field).

### Performance

Measured against `pyarrow.csv.read_csv`. The expensive step is typed column build; rugo makes it survivor-only, which matters only when there is something to skip.

**Narrow file — 3 cols, 1 M rows, 12.6 MB** (minimal projection benefit):

| Query shape | Rugo | PyArrow |
|-------------|------|---------|
| `SELECT *` | ~7 ms | ~3 ms |
| `SELECT 2 cols` | ~6 ms | ~3 ms |
| `WHERE id > P90 (~10% pass)` | ~6 ms | ~4 ms |
| `WHERE id > P99 (~1% pass)` | ~5 ms | ~3 ms |

**Wide file — 50 cols, 200 k rows, 55 MB** (projection and predicate advantage visible):

| Query shape | Rugo | PyArrow |
|-------------|------|---------|
| `SELECT *` | ~26 ms | ~17 ms |
| `SELECT 2 cols` | ~9 ms | ~7 ms |
| `SELECT * WHERE score > P90 (~10% pass)` | ~13 ms | ~27 ms |
| `SELECT * WHERE score > P99 (~1% pass)` | ~10 ms | ~23 ms |
| `SELECT 2 cols WHERE score > P90` | ~8 ms | ~27 ms |

On narrow files PyArrow is faster across the board. On wide files with filtering, rugo is 2–3×+ faster. The crossover is driven by how many columns can be skipped and how many rows are eliminated before typed column build.

### Known limitations

- Field length is capped at 65,535 bytes (`uint16_t` index); fields exceeding this limit are silently truncated.
- Type inference is speculative from sampled values; there is no schema override parameter — inferred types may be wrong on heterogeneous columns.
- Predicate operator set is fixed: `==`, `!=`, `<`, `<=`, `>`, `>=`.

---

## Converters

```python
from rugo.parquet_reader import read_metadata
from rugo.converters import rugo_to_orso_schema

meta   = read_metadata("data.parquet")
schema = rugo_to_orso_schema(meta, schema_name="my_table")  # -> Orso RelationSchema
```

---

## Source layout

```
rugo/
├── __init__.py
├── parquet_reader.pxd          # Cython declarations for the parquet extension
├── jsonl/                      # import package for the compiled JSONL extension
├── csv/                        # import package for the compiled CSV extension
├── converters/                 # schema/format conversion helpers (Orso)
└── src/
    ├── parquet/                # metadata, decode, compression, bloom filters, vendored zstd/lz4
    ├── jsonl/                  # C++/Cython JSONL reader
    ├── csv/                    # C++/Cython CSV reader
    └── parquet_spec/           # vendored Apache Parquet format specification
```

---

## Notes

- Rugo is internal engine infrastructure. Prefer `opteryx.session()` unless working on scan, metadata, or I/O internals.
- `can_decode(...)` is a quick compatibility signal, not a guarantee that every selected column will decode successfully.
- On partial decode failure, a selected column may be returned as `None` — fail loud, do not assume success.
- The read path is pure C++/Cython and Draken-native; no PyArrow or NumPy is involved.
