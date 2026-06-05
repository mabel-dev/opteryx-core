# JSONL Reader Architecture

A multithreaded, pushdown-aware JSONL reader that emits typed Draken vectors. The whole
file is read once into a shared read-only buffer; scan, document-mapping and column build
all run in parallel over byte ranges of that one buffer with no per-range copies.

## Pipeline

```
read_jsonl() [_jsonl_reader.pyx]
  ├─ default (use_threads=True): read whole file into one bytes buffer
  │     ↓
  │  interpret_jsonl_threaded() [field_span.cpp]   ← splits buffer into newline-aligned
  │     │                                             ranges, runs each on a BS::thread_pool
  │     └─ per range: scan_structural_markers() [structural_scan.cpp]  (NEON, ~3500 MB/s)
  │                   interpret_jsonl()           [field_span.cpp]
  │                     ├─ build_map()            [interpreter.cpp]  ← markers → FieldSpans
  │                     └─ finalize_records()                        ← projection + predicate
  │        … then merge per-range records in order →
  │
  └─ fallback (use_threads=False): JsonlReader::next_chunk() [jsonl_reader.cpp]
        reads 64MB chunks; process_buffer() truncates to the last newline so a record
        straddling a chunk boundary is carried, not double-counted.
  ↓
_build_vectors_from_chunks() [_jsonl_reader.pyx]
  └─ single chunk (common): parse_all_columns() [column_builder.cpp]
        one thread-pool task per column (nogil): extract_column() → parse_typed_column()
        → ParsedColumn (draken_malloc buffers, NO Python)
     then wrap_column() per column under the GIL → owns the buffers in a Draken Vector
        via draken_vector_own_raw / draken_vector_own_string.
  ↓
Returns typed Draken Vectors (INT64 / FLOAT64 / BOOL / VARCHAR, real nulls)
```

## Pushdown (the structural edge over a row parser)

When `columns` / `predicates` are given, `build_map` builds the document map for **only the
projected ∪ predicate columns**:

- **Column matching is exact-byte, not hashed.** `MapBuilder` compares each key against the
  (few) wanted columns by length + first byte + `memcmp`. Measured faster than XXH3 +
  set-lookup *and* exact — no collision risk.
- **Projection skip.** Once a record's wanted columns are all found, the rest of the record
  is skipped to the next newline.
- **Inline filter.** A predicate column is evaluated the instant its value is emitted; on
  failure the record is dropped and skipped *there* — failing rows never reach their later
  columns. `finalize_records` remains the authoritative pass (handles rows missing the
  predicate column, and multiple predicates per column, e.g. `id > 10 AND id < 40`).
- **Materialisation is survivor-only**: only rows that pass the filter are typed-parsed.

## Typed value reader

`parse_typed_column` speculates int64 → widens to float64 → falls back to VARCHAR; bool tries
true/false → VARCHAR. The type prediction is never load-bearing (a parse miss falls back).
Float parsing uses the vendored `fast_float` (bounded; `strtod` is banned — it over-reads the
separator-less slice buffer). Single-chunk reads parse directly from the file buffer (no copy).

## Performance (116 MB, 1.5M rows, 5 cols; vs PyArrow `read_json` multithreaded)

| query | ours | PyArrow |
|-------|------|---------|
| `SELECT *` | ~67 ms | ~53 ms |
| `SELECT one_col` | ~33 ms | 53 ms (must read all) |
| `SELECT col WHERE id<150k` (10% pass) | ~15 ms | 53 ms |
| `SELECT col WHERE id<15k` (1% pass) | ~7 ms | 53 ms |

`SELECT *` is bulk-bound (PyArrow's strength); the analytical shapes — project + filter — are
1.2–5×+ faster, and the win grows with selectivity and table width.

## Files

| File | Purpose |
|------|---------|
| `structural_scan.{hpp,cpp}` | NEON marker scan; templated `scan_structural<Emit>` |
| `interpreter.{hpp,cpp}` | `build_map` / `MapBuilder` state machine + pushdown |
| `field_span.{hpp,cpp}` | `interpret_jsonl`, `finalize_records`, `interpret_jsonl_threaded` |
| `value_parser.{hpp,cpp}` | predicate evaluation; `parse_*` delegate to `fast_parse_*` |
| `fast_parsers.hpp` | bounded int/float parsers (`fast_float`) |
| `column_builder.{hpp,cpp}` | extract + parse columns; `parse_all_columns` (parallel), `wrap_column` |
| `jsonl_reader.{hpp,cpp}` | sequential 64MB chunk reader (fallback path) |
| `_jsonl_reader.pyx` | Python API; orchestration only (no per-row Python) |
