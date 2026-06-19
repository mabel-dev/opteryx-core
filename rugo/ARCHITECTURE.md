# Rugo — Architecture

Internal architecture reference for the Parquet, JSONL, and CSV readers.

---

## Parquet

### Overview

- Reads from in-memory buffer (bytes/memoryview). No file I/O inside the decoder.
- Two-pass design: metadata (footer parse, no column data) and decode (targeted column-chunk decode).
- Zero external dependencies — zstd and lz4 are vendored.

### Supported subset

| Area | Support |
|------|---------|
| Physical types | int32, int64, float32, float64, boolean, byte_array |
| Compression | UNCOMPRESSED, SNAPPY, ZSTD |
| Encodings | PLAIN, dictionary pages (PLAIN_DICTIONARY / RLE_DICTIONARY), DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY |
| Input | in-memory bytes / memoryview, with column selection |

### Design notes

- DATA_PAGE (V1) is the primary decode path. DATA_PAGE_V2 is not handled.
- INT96 and FIXED_LEN_BYTE_ARRAY value decoding not implemented.
- Nested / list / map columns are not a primary decode target.
- On partial decode failure, an individual column within a Morsel may be None.
- `can_decode()` is a compatibility signal, not a strict guarantee.
- Decode is designed for serial use per file. Opteryx parallelises by calling `read_parquet` concurrently across files.

### Source files

| File | Purpose |
|------|---------|
| `parquet_reader.pyx` | Cython Python edge; orchestration only |
| `metadata.{hpp,cpp}` | Footer parse; no column data read |
| `decode.{hpp,cpp}` | Top-level decode dispatch |
| `decode_column.cpp` | Per-column-chunk decode coordinator |
| `decode_encodings.{hpp,cpp}` | PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY |
| `decode_page.{hpp,cpp}` | DATA_PAGE (V1) framing and repetition/definition levels |
| `page_value_decoder.{hpp,cpp}` | Typed value materialisation from page buffers |
| `compression.{hpp,cpp}` | UNCOMPRESSED / SNAPPY / ZSTD decompression |
| `bloom_filter.{hpp,cpp}` | Bloom filter probe for row-group skip |
| `vendor/` | Vendored zstd and lz4 |

---

## JSONL

### Pipeline

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

### Pushdown

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

### Typed value reader

`parse_typed_column` speculates int64 → widens to float64 → falls back to VARCHAR; bool tries
true/false → VARCHAR. The type prediction is never load-bearing (a parse miss falls back).
Float parsing uses the vendored `fast_float` (bounded; `strtod` is banned — it over-reads the
separator-less slice buffer). Single-chunk reads parse directly from the file buffer (no copy).

### Performance

116 MB, 1.5M rows, 5 cols; vs PyArrow `read_json` multithreaded:

| Query | Ours | PyArrow |
|-------|------|---------|
| `SELECT *` | ~67 ms | ~53 ms |
| `SELECT one_col` | ~33 ms | 53 ms (must read all) |
| `SELECT col WHERE id<150k` (10% pass) | ~15 ms | 53 ms |
| `SELECT col WHERE id<15k` (1% pass) | ~7 ms | 53 ms |

`SELECT *` is bulk-bound (PyArrow's strength); the analytical shapes — project + filter — are
1.2–5×+ faster, and the win grows with selectivity and table width.

### Source files

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

---

## CSV

### Pipeline

```
read_csv() [_csv_reader.pyx]
  ├── read whole file into one bytes buffer
  │
  ├── Phase 1: header parse (sequential)
  │     structural scan to first unquoted \n
  │     → column_names[], ordinal map, num_cols
  │     → if has_header=False: generate col_0 … col_{N-1}, treat row 0 as data
  │
  ├── Phase 2: safe-split discovery (sequential, cheap)
  │     SIMD scan → collect all " and \n positions
  │     walk that subset tracking quote FSM state
  │     → list of \n byte offsets where in_quoted_field == false
  │     → divide among N threads (approximately equal byte spans)
  │
  ├── Phase 3: parallel span extraction  [csv_row_map.cpp]
  │     per thread: run quote FSM over marker stream for its range
  │                 for each row: emit one CsvFieldSpan per requested column
  │     merge in range order (flat concatenation — no shared state)
  │     → CsvRowMap: per-column span arrays for requested columns only
  │
  ├── Phase 4: predicate evaluation (if predicates given)
  │     for each predicate column: parse raw bytes from spans + compare
  │     → survivor bitmap (one bit per row)
  │
  └── Phase 5: column build (nogil, one thread-pool task per column)
        for each projected column: walk spans, skip non-survivor rows
        parse: int64 → widen to float64 → fall back to VARCHAR
        empty field → null
        → ParsedColumn (draken_malloc buffers, no Python)
     wrap_column() under GIL per column → DrakenVectors returned to caller
```

### Structural characters

Five structural characters (vs JSONL's 9 — simpler LUT, faster NEON kernel):

| Byte | Marker    | Role |
|------|-----------|------|
| `\n` | NEWLINE   | Row separator (only when not inside a quoted field) |
| `\r` | CR        | CRLF prefix — consumed silently when followed by `\n`; ignored otherwise |
| `,`  | DELIMITER | Field separator (configurable; `\t` gives TSV for free) |
| `"`  | QUOTE     | Opens and closes quoted fields |
| `\`  | BACKSLASH | Escape prefix inside quoted fields |

NEON kernel: five `vceqq_u8` comparisons OR'd in three levels — one fewer than JSONL.
The delimiter is a runtime `uint8_t`; `vdupq_n_u8(ctx.delimiter)` and the LUT are built
once at reader construction (stored in `CsvParseContext`).

### Quote-state machine

Five states drive field/row boundary detection and escape handling:

```
FIELD_START
  "           → QUOTED          (value_start = next byte; open quote not in value)
  delimiter   → emit empty field, stay FIELD_START
  \n          → emit empty field + row end, FIELD_START
  CR          → peek buffer[pos+1]: if \n, skip CR (NEWLINE does row end); else ignore
  *           → UNQUOTED        (value_start = current byte)

UNQUOTED
  delimiter   → emit field span [value_start, pos); FIELD_START
  \n          → emit field span [value_start, pos); row end; FIELD_START
  CR          → peek buffer[pos+1]: if \n, emit field [value_start, pos); row end; else ignore
  *           → stay UNQUOTED

QUOTED
  \           → ESCAPE_IN_QUOTED
  "           → DOUBLE_QUOTE_PENDING
  * (incl. delimiter, \n, \r) → stay QUOTED  ← commas and newlines NOT terminators here

ESCAPE_IN_QUOTED
  * (any byte) → stay QUOTED    ← next byte is literal, including " delimiter \n

DOUBLE_QUOTE_PENDING
  "           → stay QUOTED,    mark has_escape=true  (RFC 4180 "" → ")
  delimiter   → emit field span; FIELD_START  (close quote was real)
  \n / CR+\n  → emit field span; row end; FIELD_START
  *           → back to UNQUOTED  (malformed; liberal parse — treat as continuation)
```

Key invariant: `\"` → ESCAPE_IN_QUOTED consumes the `"` without entering
DOUBLE_QUOTE_PENDING, so the field remains open. Both `\"` and `""` are resolved
simultaneously with no per-file configuration.

### Field spans

```cpp
struct CsvFieldSpan {
    uint32_t start;      // byte offset into buffer (first byte of raw value, after open quote)
    uint16_t length;     // raw byte count (before unescape, excluding close quote)
    bool     was_quoted; // field opened with "  →  unescape may be needed
    bool     has_escape; // contains \" or "" sequence  →  unescape IS needed
};
```

#### ⚠ FIELD LENGTH CAP: 65,535 BYTES (uint16_t)

**Fields longer than 65,535 bytes are truncated to 65,535 bytes.**

This is an explicit, documented design trade-off: `uint16_t` halves the span-table
footprint versus `uint32_t`. Analytical CSV fields are almost universally short; the
65 KiB ceiling covers every realistic case except free-text blobs embedded in CSV
(which should be in Parquet or JSONL instead).

If a field is truncated, its `length` is clamped to `UINT16_MAX`. The value stored
in the output vector will be the first 65,535 bytes of the field, silently. Callers
that need full fidelity on arbitrary-length text must not use this reader.

### Column span storage

Only requested (projected ∪ predicate) columns are stored. For a 100-column file with
`SELECT a, b WHERE c > 5`, three span arrays are built — not one hundred.

```cpp
struct CsvRowMap {
    uint32_t                               num_rows;       // total rows (incl. filtered)
    uint32_t                               num_cols;       // columns inferred from header/row-0
    std::vector<std::string>               column_names;   // from header, or col_0…col_{N-1}
    std::vector<uint32_t>                  request_cols;   // file ordinals of requested columns
    std::vector<std::vector<CsvFieldSpan>> column_spans;   // [i] = spans for request_cols[i]
};
```

### Threading model

1. **Header parse** — sequential; fast (one row).
2. **Safe-split discovery** — sequential walk over the (small) subset of `"` and `\n` marker
   positions found by SIMD. Tracks quote state to identify which `\n`s are real row separators.
   Divides those positions approximately evenly among N threads.
3. **Span extraction** — one thread per range; each runs the full quote FSM independently.
   Ranges are newline-aligned so no thread starts mid-row. Results are merged in range order
   by flat concatenation (no shared state, no locks).
4. **Column build** — one thread-pool task per projected column; runs without the GIL.
   Wrap step is serial under the GIL (same pattern as JSONL).

### Predicate pushdown vs PyArrow

PyArrow CSV reader reads and parses all columns regardless of projection or filtering.

| Query pattern | PyArrow | Ours |
|---|---|---|
| `SELECT *` | all cols built | all cols built — parity |
| `SELECT 2 cols FROM 100-col file` | 100 cols | 2 span arrays built and typed |
| `WHERE 1% selectivity` | all rows materialised | 1% reach typed column build |
| `SELECT a WHERE b > 5` (100-col file) | parse all → filter | spans for b → filter → build a survivors |

Typed column build is the expensive step. Making it survivor-only (same pattern as JSONL)
is the primary mechanism for beating PyArrow on filtered queries.

### Unescape

Triggered only when `has_escape == true` (uncommon). Zero-copy fast path for clean fields.

| Sequence | Result |
|---|---|
| `\"` | `"` (backslash consumed) |
| `""` | `"` (pair collapsed) |
| `\\` | `\` |
| `\x` (any other x) | `x` (liberal: backslash absorbed) |

Unescape always writes into an owned buffer; the span's start/length cannot be used
directly for escaped fields. `has_escape` gates this cost to the minority of fields
that need it.

### Shared code with JSONL

| Artifact | How used |
|---|---|
| `fast_parsers.hpp` | included directly; `fast_parse_int64`, `fast_float` wrapper |
| `BS_thread_pool.hpp` | same thread pool for column build phase |
| `ParsedColumn` struct pattern | copied into `csv_column_builder.hpp` |
| `wrap_column()` pattern | reimplemented for CSV spans (different input shape) |

`csv` does not depend on `jsonl` at link time; shared headers are included by path.

### Source files

| File | Purpose |
|------|---------|
| `core/csv_parse_context.hpp` | config struct: delimiter, has_header, projections, predicates; LUT |
| `core/csv_scan.hpp` | structural scan: NEON/AVX2/scalar, 5-char LUT, runtime delimiter |
| `core/csv_row_map.hpp/.cpp` | header parse + safe-split + FSM span extraction + predicate eval |
| `core/csv_column_builder.hpp/.cpp` | typed column build from CsvFieldSpan arrays |
| `_csv_reader.pyx` | thin Cython Python edge (no per-row Python) |
