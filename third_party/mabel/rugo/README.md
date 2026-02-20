# rugo: Current strengths and limitations

This is an internal, performance-focused reader used by Opteryx.
Today, rugo is strongest as a **Parquet metadata reader** with a
**targeted Parquet data decoder** and a **fast JSONL reader**.

## Strengths

### Parquet metadata (strongest path)

- Fast metadata reads from file path, `bytes`, and `memoryview`.
- Useful controls for fast planning paths:
  - `schema_only=True`
  - `include_statistics=False`
  - `max_row_groups=<n>`
- Rich per-column metadata is exposed, including:
  - physical/logical type
  - encodings and compression codec
  - min/max/null/distinct stats
  - row-group/page offsets
  - bloom filter offsets/lengths
  - key-value metadata
- Bloom filters can be queried directly with `test_bloom_filter(...)`.

### Parquet data decode (supported subset)

- Reads directly from in-memory buffers (`bytes`/`memoryview`), with column selection.
- Supported physical types:
  - `int32`, `int64`, `float32`, `float64`, `boolean`, `byte_array`
- Supported compression codecs:
  - `UNCOMPRESSED`, `SNAPPY`, `ZSTD`
- Supported encodings in the current decode path:
  - `PLAIN`
  - dictionary pages (`PLAIN_DICTIONARY` / `RLE_DICTIONARY`)
  - `DELTA_BINARY_PACKED` for integer columns
  - `DELTA_BYTE_ARRAY` for byte array/string columns

### JSONL

- Fast schema inference and projection pushdown.
- Handles `bytes`, `memoryview`, and other contiguous buffer inputs.
- Supports scalar types plus arrays/objects, with configurable parsing behavior for arrays/objects.

## Current limitations

### Parquet decode limitations

- Not a full Parquet replacement reader; decode support is intentionally narrow.
- `GZIP`, `LZO`, `BROTLI`, `LZ4`, `LZ4_RAW` are not implemented in the decode path.
- `INT96` is not supported for value decoding in `read_parquet(...)`.
- `FIXED_LEN_BYTE_ARRAY` value decoding is not implemented.
- Decode logic is currently built around `DATA_PAGE` (V1). `DATA_PAGE_V2` is not handled.
- Decode currently reads from a single data-page path per column chunk; files requiring full multi-page streaming decode may return partial/failed column results.
- Nested/list/map-heavy files are not a primary decode target; flat primitive columns are the intended shape.
- On partial decode failure, individual columns may be returned as `None`.
- `can_decode(...)` is a quick compatibility check, not a strict guarantee that every selected column will decode successfully.

### Metadata caveats

- Metadata extraction is broad, but known edge cases remain around list/nested column naming normalization.

### JSONL caveats

- String/object-heavy fields are often intentionally returned as `bytes` (binary-preserving behavior), not eagerly decoded Python `str`/`dict` values.
- Mixed/nested array-object content may fall back to raw JSON text/bytes in edge cases.
