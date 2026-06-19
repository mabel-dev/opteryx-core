# Rugo

Rugo is Opteryx Core's internal file reader for Parquet, JSONL, and CSV. Zero external dependencies — no PyArrow, no NumPy on any read path. Compiled as Cython/C++ extensions. All readers emit Draken vectors. Not separately installable or versioned; built as part of this repository. Depends on Draken (`../draken/README.md`).

---

## Build

```bash
make compile   # full rebuild
make c         # incremental rebuild
```

Python 3.13 required for local builds.

---

## Modules

| Package               | Purpose                                         |
|-----------------------|-------------------------------------------------|
| `rugo.parquet_reader` | Parquet metadata + column decoding              |
| `rugo.jsonl`          | JSONL schema inference + decoding               |
| `rugo.csv`            | CSV schema inference + decoding                 |
| `rugo.converters`     | Schema/format conversion helpers (Orso)         |

---

## Parquet

### Quick start

```python
from rugo.parquet_reader import read_metadata, read_parquet

# Footer parse only — no column data read
meta = read_metadata("testdata/planets/planets.parquet")
print(meta["num_rows"])        # e.g. 9
print(meta["schema_columns"])  # list of {name, physical_type, logical_type, nullable}

# Decode selected columns into Draken Morsels
with open("testdata/planets/planets.parquet", "rb") as f:
    morsels = read_parquet(f.read(), column_names=["id", "name"])
    # morsels is list[Morsel] (one per row group), or None on failure
    for morsel in morsels:
        for vec in morsel.vectors:
            print(vec.type, vec.length)
```

### API

#### Metadata

| Function | Returns |
|----------|---------|
| `read_metadata(path: str)` | `{num_rows, schema_columns}` |
| `read_metadata_from_bytes(data: bytes)` | same |
| `read_metadata_from_memoryview(mv: memoryview)` | same (memoryview must be contiguous) |

`schema_columns` is a list of dicts: `{"name": str, "physical_type": str, "logical_type": str, "nullable": bool}`.

#### Decode

```python
read_parquet(data, column_names=None)
```

- `data` — `bytes`, `bytearray`, or `memoryview` holding the full Parquet file.
- `column_names` — `list[str]` to project, or `None` for all columns.
- Returns `list[Morsel]` (one per row group), or `None` on failure. On partial decode failure an individual column within a Morsel may be `None`.

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
test_bloom_filter(path, bloom_offset, bloom_length, value)  # -> bool
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

Metadata reads (schema + row-group stats, no column data) are fast and comparable to PyArrow. Decode benchmarks for the full column path (`decode_column_from_chunk`) are pending completion of the Draken vector migration (E.28); the high-level `read_parquet()` API is not the production path — Opteryx drives `decode_column_from_chunk` directly, which bypasses the stubs.

---

## JSONL

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
