# Rugo

Rugo is Opteryx Core's internal Parquet and JSONL reader. It is optimized for metadata-driven planning, targeted column decoding, and direct output into engine-friendly column vectors.

Rugo is built as part of this repository. It is not currently treated as a separately installable or separately versioned package.

## What Rugo Is Used For

- Fast Parquet footer and row-group metadata reads
- Column selection and predicate-planning metadata
- Targeted Parquet data decoding for the supported primitive subset
- JSONL schema inference, projection, and predicate pushdown
- Native integration with Draken vectors and Opteryx schema objects

## Build

Use the top-level build:

```bash
make compile
```

For a faster incremental rebuild:

```bash
make c
```

The repository requires Python 3.13 for supported local builds.

## Python Entry Points

The low-level modules are exposed through `rugo.parquet_reader` and `rugo._jsonl`.

```python
from rugo.parquet_reader import read_metadata
from rugo.parquet_reader import read_parquet

metadata = read_metadata("testdata/planets/planets.parquet")
columns = read_parquet(open("testdata/planets/planets.parquet", "rb").read())
```

JSONL:

```python
from rugo._jsonl import read_jsonl

result = read_jsonl(
    "testdata/example.jsonl",
    columns=["id", "name"],
    use_threads=True,
)
```

`read_jsonl(...)` returns a result dictionary containing success state, column names, row count, vectors, and inferred schema.

## Parquet API Surface

Current public functions in `rugo.parquet_reader` include:

| Function | Purpose |
|----------|---------|
| `read_metadata(path)` | Read Parquet metadata from a file path |
| `read_metadata_from_bytes(data)` | Read metadata from a bytes object |
| `read_metadata_from_memoryview(mv)` | Read metadata from a memoryview |
| `can_decode(path)` | Quick compatibility check for the current decoder |
| `can_decode_from_memory(data)` | Compatibility check for an in-memory buffer |
| `test_bloom_filter(path, bloom_offset, bloom_length, value)` | Evaluate a column bloom filter |
| `read_parquet(data, column_names=None)` | Decode selected columns from an in-memory Parquet buffer |

See `rugo/src/README.md` for the current supported subset and limitations.

## Supported Parquet Decode Subset

The active decode path targets flat primitive columns:

| Area | Current support |
|------|-----------------|
| Physical types | `int32`, `int64`, `float32`, `float64`, `boolean`, `byte_array` |
| Compression | `UNCOMPRESSED`, `SNAPPY`, `ZSTD` |
| Encodings | `PLAIN`, dictionary pages, `DELTA_BINARY_PACKED`, `DELTA_BYTE_ARRAY` |
| Input | In-memory `bytes`/`memoryview`, with column selection |

This is intentionally narrower than the Parquet metadata reader.

## Source Layout

```
rugo/
├── __init__.py
├── _jsonl/                  # Import package for the compiled JSONL extension
├── converters/              # Schema and format conversion helpers
└── src/
    ├── parquet/             # Parquet metadata, decoding, compression, bloom filters
    ├── jsonl/               # JSONL reader bindings
    ├── _jsonl/              # C++ JSONL implementation
    ├── parquet/vendor/      # Vendored compression dependencies
    └── parquet_spec/        # Vendored Apache Parquet format specification
```

## Notes

- Rugo is internal engine infrastructure; prefer querying through `opteryx.session()` unless you are working on scan, metadata, or I/O internals.
- `can_decode(...)` is a quick compatibility signal, not a guarantee that every selected column will decode successfully.
- On partial decode failure, a selected column may be returned as `None`.
