# Rugo [![PyPI](https://img.shields.io/pypi/v/rugo.svg)](https://pypi.org/project/rugo/) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Thin · light · opinionated for low resource usage

**[Rugo](https://rugo.dev) is the file layer extracted from the [Opteryx SQL engine](https://github.com/mabel-dev/opteryx-core). It was built to keep memory use low by pushing filters as early as possible — skipping columns you don't need and pruning row groups before any data is decoded.**

---

## Why Rugo?

### Thin by design. Opinionated about what not to load.

Rugo was built as the file layer for the Opteryx SQL engine, where the constraint was simple: read as little data as possible, hold as little in memory as possible. Column projection and row-group pruning happen before any decoding. The result is a library that is fast on selective queries, tiny on disk, and carries no PyArrow, Pandas, or NumPy into your environment.

| Metric                      | Rugo     | PyArrow       |
|-----------------------------|----------|---------------|
| Installed footprint         | 17 MB    | 124 MB        |
| Runtime dependencies        | zero     | Arrow C++ runtime |
| Cold import time           | 5 ms     | 29 ms         |
| Schema read (footer only)  | 0.02 ms  | 0.05 ms       |

> Measured on Python 3.14, Apple M-series. Import times on a cold process (first load off disk).

---

### Serverless-first

AWS Lambda and GCP Cloud Functions bill by memory and package size. At 17 MB installed and a 5 ms cold import, Rugo fits where PyArrow's 124 MB footprint doesn't.

---

### Tiny container images

At 16× smaller than PyArrow, Rugo meaningfully shrinks image size, speeds scale-out, and keeps you well clear of AWS Lambda's 250 MB unzipped layer limit.

---

### Read less, go faster

Column projection and row-group pruning are first-class citizens. Skip the columns you don't need. Skip the row groups that can't match. Rugo's advantage grows with selectivity.

---

### No surprise dependencies

The wheel bundles everything it needs — [Draken](https://draken.dev/), the columnar substrate, ships inside. `pip install rugo` is the entire dependency story.

**Where PyArrow is faster:** full-table scans with no filtering. Rugo does not compete on decode throughput — it competes on how little it has to decode in the first place.

---

## Quickstart

One install, three formats.

Rugo reads and writes Parquet, CSV, and JSONL. The API is the same shape across all three: pass a path or bytes, get columnar data back.

### Installation

```bash
pip install rugo
```

Pre-built wheels bundle Draken — there is nothing else to install. Rugo has **zero runtime dependencies**.

**Requirements:**

- Python 3.11+
- A platform with a published wheel (Linux x86-64/aarch64, macOS arm64). For other platforms, see [Building from source](#building-from-source).

---

## Data model

Rugo speaks **Draken**, the bundled columnar substrate:

- A **Vector** is a single typed column. Call `vector.to_pylist()` to get a Python list of its values.
- A **Morsel** is a batch of rows across several columns (a chunk of a table). Call `morsel.column(b"name")` to get a column Vector (note the **bytes** key), and `len(morsel)` for the row count.

Readers return Morsels (Parquet) or a result dict whose `columns` are Vectors (CSV, JSONL). The writers consume a Morsel. A read → write round-trip:

```python
from rugo import parquet
from rugo.csv import write_csv
from rugo.jsonl import write_jsonl

with parquet.read_parquet("planets.parquet") as reader:
    for morsel in reader:                          # one Morsel per row group
        csv_bytes    = write_csv(morsel)           # -> bytes (RFC 4180)
        jsonl_bytes = write_jsonl(morsel)          # -> bytes (one JSON object per row)
        pq_bytes     = parquet.write_parquet(morsel)   # -> bytes (ZSTD)
```

---

## Parquet

`rugo.parquet` is the recommended surface: one symmetric module for reading and writing that accepts a filename or an in-memory buffer, streams row-group Morsels, applies predicate pushdown, and writes Morsels back to bytes.

### Quick start

```python
from rugo import parquet

# Schema-only metadata (footer parse, no column data). Path OR bytes.
meta = parquet.read_metadata("planets.parquet")
print(meta.num_rows)                      # 9
print([c.name for c in meta.schema_columns])

# Streaming read: one Morsel per row group. `columns` projects; `filters`
# prune whole row groups via footer statistics (rows in surviving groups are
# NOT filtered — apply row-level predicates downstream).
with parquet.read_parquet(
     "planets.parquet",
    columns=["id", "name"],
    filters=[("id", ">", 4)],               # ops: = == != < <= > >= in "not in"
) as reader:
    for morsel in reader:
        print(morsel.column(b"name").to_pylist())

# Write a Draken Morsel to Parquet bytes (ZSTD by default; "none" to disable).
data = parquet.write_parquet(morsel, compression="zstd")
with open("out.parquet", "wb") as f:
    f.write(data)
```

### `rugo.parquet` API

| Function                                      | Returns                                                    |
|-----------------------------------------------|------------------------------------------------------------|
| `read_parquet(source, columns=None, filters=None)` | context manager yielding one Morsel per surviving row group |
| `read_metadata(source)`                      | `ParquetMetadata` (`num_rows`, `schema_columns`)           |
| `write_parquet(morsel, compression="zstd")` | `bytes` (whole file)                                        |
| `write_parquet_with_bounds(morsel)`         | `(bytes, {col_index: (min, max)})`                         |

`source` is a filename (`str`) or `bytes`/`bytearray`/`memoryview`. `filters` is a list of `(column, op, value)`; pruning is at row-group granularity.

---

### Low-level API (`rugo.parquet_reader`)

Most callers should use `rugo.parquet` above. The low-level module is exposed for fine-grained control.

#### Metadata

| Function                                  | Returns                                                                                   |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
| `read_metadata(path: str)`               | `ParquetMetadata(num_rows, schema_columns)` (typed object)                               |
| `read_metadata_from_bytes(data: bytes)` | same                                                                                      |
| `read_metadata_from_memoryview(mv: memoryview)` | same (memoryview must be contiguous)                                |
| `read_rowgroup_stats(data)`              | `list[{num_rows, columns:[{name, physical_type, logical_type, min, max, null_count}]}]` — per-row-group stats for pushdown |

`schema_columns` is a tuple of `SchemaColumn(name, physical_type, logical_type, nullable)`. `read_rowgroup_stats` `min`/`max` are raw stat bytes (or `None`); decode with `decode_value`.

#### Decode

```python
read_parquet(data, column_names=None, row_group_mask=None)
```

- `data` — `bytes`, `bytearray`, or `memoryview` holding the full Parquet file.
- `column_names` — `list[str]` to project, or `None` for all columns.
- `row_group_mask` — optional iterable, one truthy/falsy entry per row group; a falsy entry skips decoding that row group (predicate pushdown). `rugo.parquet`'s `filters=` builds this from `read_rowgroup_stats`.
- Returns `list[Morsel]` (one per decoded row group), or `None` on failure. On partial decode failure an individual column within a Morsel may be `None`.

#### Compatibility

| Function                              | Returns                           |
|---------------------------------------|-----------------------------------|
| `can_decode(path: str)`              | `bool` — quick compatibility signal, not a guarantee |
| `can_decode_from_memory(data)`       | `bool` — same, for an in-memory buffer |

#### Fine-grained / range decode

| Function                                                    | Description                                                                                   |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `decode_column_from_chunk(chunk_bytes, col_stats, row_mask=None)` | Decode a single column chunk to a Draken Vector; `row_mask` is an optional `uint8` bitmap |
| `decode_column_from_chunk_to_python(chunk_bytes, col_stats)` | Decode a single column chunk to a Python list                                                |
| `decode_column_from_memory(data, column_name, row_group_stats, row_group_index)` | Decode one column from a full in-memory file, by row-group index                      |
| `decode_value(physical_type, logical_type, raw, prefer_text)` | Decode a single raw Parquet value to a Python scalar                                          |

`col_stats` is the per-column stats dict for the matching row group from `read_metadata`.

#### Bloom filters

```python
bloom_filter_maybe_contains(path, bloom_offset, bloom_length, value)   # -> bool
```

Evaluates a column bloom filter at the given byte offset/length for a candidate `value`. Bloom filter offsets and lengths are exposed in the per-column metadata returned by `read_metadata`.

---

### Supported decode subset

| Area        | Support                                                                                             |
|-------------|-----------------------------------------------------------------------------------------------------|
| Physical types  | `int32`, `int64`, `float32`, `float64`, `boolean`, `byte_array`                                    |
| Compression    | `UNCOMPRESSED`, `SNAPPY`, `ZSTD`                                                                   |
| Encodings      | `PLAIN`, dictionary pages (`PLAIN_DICTIONARY` / `RLE_DICTIONARY`), `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` |
| Input          | Path, or in-memory `bytes` / `memoryview`, with column selection                                   |

---

### Writing

`rugo.parquet_writer` (and the `rugo.parquet` facade) serialize a Draken Morsel to a well-formed, PyArrow-readable Parquet file.

```python
from rugo.parquet_writer import write_parquet, write_parquet_with_bounds
data = write_parquet(morsel, compression="zstd")           # -> bytes
data, bounds = write_parquet_with_bounds(morsel)           # + per-column min/max
```

| Area              | Support                                                                                                         |
|-------------------|-----------------------------------------------------------------------------------------------------------------|
| Column types      | INT8/16/32/64 (→INT64), FLOAT32 (→DOUBLE), FLOAT64, BOOL, VARCHAR/NVARCHAR/VARBINARY, VARIANT (→STRING), DATE32, TIME32/64, TIMESTAMP64 (µs/ms/ns), INTERVAL (FLBA-12), DECIMAL/DECIMAL128 (FLBA), ARRAY/LIST of those (int/float/bool/string elements), all-null (→INT32). FP16 not yet. |
| Encoding          | `PLAIN` values, `RLE` definition levels, one data page per column chunk                                         |
| Compression        | `ZSTD` (default) or uncompressed                                                                                |
| Statistics         | per-column min/max/null_count + `column_orders` (so readers trust them)                                       |
| Bloom filters      | split-block (SBBF), XXH64, on equality-friendly columns; `bloom_filters=True\|False\|[names]`                     |
| Layout             | single row group per Morsel                                                                                    |

Unsupported column types fail loud (no silent skip). Nested LIST/MAP/STRUCT and dictionary-encoded *output* are not yet implemented.

---

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

---

### Performance

Metadata reads (schema + row-group stats, no column data) are fast and comparable to PyArrow. The high-level `read_parquet()` path is correctness-first: it reconstructs Draken vectors from decoded columns and materializes through Python, so it is a serial utility rather than a throughput benchmark. The emphasis is on *reading less* — projection and row-group pruning — not on raw bulk scan speed.

#### Wide file (50 cols, 200k rows, 55 MB)

| Query shape                              | Rugo  | PyArrow |
|------------------------------------------|-------|---------|
| `SELECT *`                               | ~26 ms | ~17 ms  |
| `SELECT 2 cols`                          | ~9 ms  | ~7 ms   |
| `SELECT * WHERE score > P90 (~10% pass)` | ~13 ms | ~27 ms  |
| `SELECT * WHERE score > P99 (~1% pass)` | ~10 ms | ~23 ms  |
| `SELECT 2 cols WHERE score > P90`       | ~8 ms  | ~27 ms  |

On narrow files PyArrow is faster across the board. On wide files with filtering, Rugo is 2–3×+ faster — the crossover is driven by how many columns can be skipped and how many rows are eliminated before the typed column build.

---

## JSONL

### Quick start

```python
from rugo.jsonl import get_jsonl_schema, read_jsonl, write_jsonl

# Infer schema from sample rows
schema = get_jsonl_schema("example.jsonl", sample_size=5)
# -> {"columns": [{"name": str, "type": str, "nullable": True}, ...]}

# Read from a file path with projection and predicate pushdown
result = read_jsonl(
     "example.jsonl",
    columns=["id", "name"],
    predicates=[("status", "==", "active")],
)
if result["success"]:
    print(result["num_rows"])
    for vec in result["columns"]:           # list of Draken Vectors
        print(vec.to_pylist())

# Read from bytes input
with open("example.jsonl", "rb") as f:
    result = read_jsonl(f.read(), columns=["id"])

# Write a Morsel to JSONL bytes (one JSON object per row)
data = write_jsonl(morsel)
```

### `read_jsonl`

```python
read_jsonl(
    data,                        # file path (str) or buffer (bytes/bytearray/memoryview)
    columns=None,                # list[str] to project, or None for all
    predicates=None,             # list[(column, op, value)]; op in ==, !=, <, <=, >, >=
    explicit_schema=None,        # provide a schema dict instead of inferring
    infer_schema=True,
    infer_sample_size=5,         # rows sampled for type inference
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True,
    use_threads=True,            # SIMD-accelerated parallel scan/interpret
    min_rows_per_thread=2048,
)
```

Return dict:

| Key              | Value                                                    |
|------------------|----------------------------------------------------------|
| `success`        | `bool`                                                   |
| `column_names`   | `list[str]`                                              |
| `num_rows`       | `int` — rows passing predicates                          |
| `columns`        | `list` of Draken Vectors                                 |
| `schema`         | `dict[str, str]` — column name → inferred type string    |
| `error`          | `str` — present only when `success` is `False`          |

Inferred type strings: `int64`, `double`, `boolean`, `string`, `bytes`, `object`, `null`, `array[T]`.

---

### `get_jsonl_schema`

```python
get_jsonl_schema(data, sample_size=5)
# -> {"columns": [{"name": str, "type": str, "nullable": True}, ...]}
```

Infers the schema from the first `sample_size` rows. Returns `{"columns": []}` on failure; does not raise.

---

### Writing

`write_jsonl(morsel)` returns bytes, one JSON object per row. Value formatting is done in C++: doubles use shortest round-trip (`std::to_chars`); dates/timestamps render ISO-8601 strings; decimals are JSON numbers; arrays render as JSON arrays (null list / empty list / null element are all distinguished); nulls are `null`.

---

### Performance

116 MB, 1.5 M rows, 5 cols, versus PyArrow `read_json` (multithreaded):

| Query shape                          | Rugo   | PyArrow |
|--------------------------------------|--------|---------|
| `SELECT *`                           | ~67 ms | ~53 ms  |
| `SELECT one_col`                     | ~33 ms | ~53 ms  |
| `SELECT col WHERE id < 150k` (~10% pass) | ~15 ms | ~53 ms  |
| `SELECT col WHERE id < 15k` (~1% pass)    | ~7 ms  | ~53 ms  |

Bulk `SELECT *` is materialiser-bound — PyArrow has an edge. The analytical shapes — project + filter — are 1.2–5×+ faster, and the advantage grows with selectivity and table width.

---

### Caveats

- String/object-heavy fields are often returned as `bytes` (binary-preserving), not eagerly decoded Python `str`/`dict` values.
- Mixed or deeply nested array-object content may fall back to raw JSON text/bytes in edge cases.
- Schema inference is sampled (`infer_sample_size` rows only); pass `explicit_schema` when the schema is known to avoid mismatches on heterogeneous files.

---

## CSV

### Quick start

```python
from rugo.csv import read_csv, write_csv

result = read_csv("data.csv")                                           # all columns
result = read_csv("data.csv", columns=["col1", "col2"])                 # projection
result = read_csv("data.csv", columns=["name"], predicates=[("age", ">", 30)])
result = read_csv("data.tsv", delimiter="\t")                           # TSV variant

if result["success"]:
    for vec in result["columns"]:           # list of Draken Vectors
        print(vec.to_pylist())

# Write a Morsel to CSV bytes (RFC 4180)
data = write_csv(morsel, delimiter=",", header=True)
```

### `read_csv`

```python
read_csv(
    data,                # file path (str) or buffer (bytes/bytearray/memoryview)
    columns=None,        # list[str] to project, or None for all
    predicates=None,     # list[(column, op, value)]; op in ==, !=, <, <=, >, >=
    delimiter=",",       # field separator character
    has_header=True,     # whether the first row is a header
    use_threads=True,    # parallel scan
)
```

| Parameter  | Type                  | Description                                               |
|------------|-----------------------|-----------------------------------------------------------|
| `data`     | `str` / `bytes` / `bytearray` / `memoryview` | File path or in-memory buffer                           |
| `columns`  | `list[str]` or `None` | Columns to project; `None` returns all                   |
| `predicates` | `list[tuple]` or `None` | Filter predicates applied before typed build            |
| `delimiter`| `str`                 | Single-character field separator                         |
| `has_header`| `bool`                | Whether row 0 is a header row                           |
| `use_threads`| `bool`               | Enable parallel scan                                    |

Return dict:

| Key              | Value                                                      |
|------------------|------------------------------------------------------------|
| `success`        | `bool`                                                    |
| `column_names`   | `list[str]`                                               |
| `num_rows`       | `int` — rows passing predicates                           |
| `columns`        | `list` of Draken Vectors                                 |

Type inference cascade per field: `int64` → `float64` → `VARCHAR` → `null` (empty field).

---

### Writing

`write_csv(morsel, delimiter=",", header=True)` returns RFC 4180 bytes: fields are quoted when they contain the delimiter/quote/newline (quotes doubled), nulls are empty fields, and ARRAY columns render as a (quoted) JSON array. The CSV and JSONL writers share the same C++ value formatter.

---

### Performance

Measured against `pyarrow.csv.read_csv`. The expensive step is typed column build; Rugo makes it survivor-only, which pays off when there is something to skip.

**Narrow file — 3 cols, 1 M rows, 12.6 MB:**

| Query shape                              | Rugo  | PyArrow |
|------------------------------------------|-------|---------|
| `SELECT *`                               | ~7 ms  | ~3 ms   |
| `SELECT 2 cols`                          | ~6 ms  | ~3 ms   |
| `WHERE id > P90 (~10% pass)`             | ~6 ms  | ~4 ms   |
| `WHERE id > P99 (~1% pass)`             | ~5 ms  | ~3 ms   |

**Wide file — 50 cols, 200 k rows, 55 MB:**

| Query shape                              | Rugo  | PyArrow |
|------------------------------------------|-------|---------|
| `SELECT *`                               | ~26 ms | ~17 ms  |
| `SELECT 2 cols`                          | ~9 ms  | ~7 ms   |
| `SELECT * WHERE score > P90 (~10% pass)` | ~13 ms | ~27 ms  |
| `SELECT * WHERE score > P99 (~1% pass)` | ~10 ms | ~23 ms  |
| `SELECT 2 cols WHERE score > P90`       | ~8 ms  | ~27 ms  |

On narrow files PyArrow is faster across the board. On wide files with filtering, Rugo is 2–3×+ faster — the crossover is driven by how many columns can be skipped and how many rows are eliminated before the typed column build.

---

### Known limitations

- Field length is capped at 65,535 bytes (`uint16_t` index); longer fields are silently truncated.
- Type inference is speculative from sampled values; there is no schema-override parameter — inferred types may be wrong on heterogeneous columns.
- Predicate operator set is fixed: `==`, `!=`, `<`, `<=`, `>`, `>=`.

---

## Design notes

- **No PyArrow, no NumPy.** Every read and write path is pure C++/Cython and Draken-native. Output Parquet is still standard and PyArrow-readable.
- **Fail loud.** `can_decode(...)` is a quick compatibility signal, not a guarantee; on partial decode failure a selected column may be returned as `None` — check, don't assume success.
- **Read less.** The advantage over bulk readers comes from projection and predicate/row-group pruning, not raw scan throughput.

---

## Example notebook

[`space_missions.ipynb`](https://github.com/mabel-dev/opteryx-core/blob/main/rugo/space_missions.ipynb) walks through a complete workflow on a real dataset:

- Download a Parquet file and inspect its schema with `read_metadata`
- Filter launches by company with row-group pruning and row-level predicate
- Aggregate total spend per company across streaming morsels
- Write filtered results to JSONL and read them back

---

## Building from source

End users should `pip install rugo` and use the published wheels. To build from the [opteryx-core](https://github.com/mabel-dev/opteryx) source tree (Rugo is developed there alongside Draken and the Opteryx engine):

```bash
python rugo/setup.py bdist_wheel     # build the standalone Rugo wheel (from repo root)
```

For in-place development of the whole tree, use the repository's `make compile`.

---

## License

Apache-2.0. Rugo is part of the [Opteryx](https://opteryx.dev/) project.
