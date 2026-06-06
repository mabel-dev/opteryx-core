# CSV Reader Architecture

A multithreaded, pushdown-aware CSV reader that emits typed Draken vectors. The whole
file is read once into a shared read-only buffer; header parse, safe-split discovery,
span extraction, and column build all run over that one buffer with no per-range copies.

## Pipeline

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

## Structural characters (5 vs JSONL's 9 — simpler LUT, faster NEON kernel)

| Byte | Marker        | Role |
|------|---------------|------|
| `\n` | NEWLINE       | Row separator (only when not inside a quoted field) |
| `\r` | CR            | CRLF prefix — consumed silently when followed by `\n`; ignored otherwise |
| `,`  | DELIMITER     | Field separator (configurable; `\t` gives TSV for free) |
| `"`  | QUOTE         | Opens and closes quoted fields |
| `\`  | BACKSLASH     | Escape prefix inside quoted fields |

NEON kernel: five `vceqq_u8` comparisons OR'd in three levels — one fewer than JSONL.
The delimiter is a runtime `uint8_t`; `vdupq_n_u8(ctx.delimiter)` and the LUT are built
once at reader construction (stored in `CsvParseContext`).

## Quote-state machine

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

## Field spans

```cpp
struct CsvFieldSpan {
    uint32_t start;      // byte offset into buffer (first byte of raw value, after open quote)
    uint16_t length;     // raw byte count (before unescape, excluding close quote)
    bool     was_quoted; // field opened with "  →  unescape may be needed
    bool     has_escape; // contains \" or "" sequence  →  unescape IS needed
};
```

### ⚠ FIELD LENGTH CAP: 65,535 BYTES (uint16_t)

**Fields longer than 65,535 bytes are truncated to 65,535 bytes.**

This is an explicit, documented design trade-off: `uint16_t` halves the span-table
footprint versus `uint32_t`. Analytical CSV fields are almost universally short; the
65 KiB ceiling covers every realistic case except free-text blobs embedded in CSV
(which should be in Parquet or JSONL instead).

If a field is truncated, its `length` is clamped to `UINT16_MAX`. The value stored
in the output vector will be the first 65,535 bytes of the field, silently. Callers
that need full fidelity on arbitrary-length text must not use this reader.

## Column span storage

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

## Threading model

1. **Header parse** — sequential; fast (one row).
2. **Safe-split discovery** — sequential walk over the (small) subset of `"` and `\n` marker
   positions found by SIMD. Tracks quote state to identify which `\n`s are real row separators.
   Divides those positions approximately evenly among N threads.
3. **Span extraction** — one thread per range; each runs the full quote FSM independently.
   Ranges are newline-aligned so no thread starts mid-row. Results are merged in range order
   by flat concatenation (no shared state, no locks).
4. **Column build** — one thread-pool task per projected column; runs without the GIL.
   Wrap step is serial under the GIL (same pattern as JSONL).

## Predicate pushdown — where the wins over PyArrow are

PyArrow CSV reader reads and parses all columns regardless of projection or filtering.

| Query pattern | PyArrow | Ours |
|---|---|---|
| `SELECT *` | all cols built | all cols built — parity |
| `SELECT 2 cols FROM 100-col file` | 100 cols | 2 span arrays built and typed |
| `WHERE 1% selectivity` | all rows materialised | 1% reach typed column build |
| `SELECT a WHERE b > 5` (100-col file) | parse all → filter | spans for b → filter → build a survivors |

Typed column build is the expensive step. Making it survivor-only (same pattern as JSONL)
is the primary mechanism for beating PyArrow on filtered queries.

## Unescape

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

## Reuse from `_jsonl/core/`

| Artifact | How used |
|---|---|
| `fast_parsers.hpp` | included directly; `fast_parse_int64`, `fast_float` wrapper |
| `BS_thread_pool.hpp` | same thread pool for column build phase |
| `ParsedColumn` struct pattern | copied into `csv_column_builder.hpp` |
| `wrap_column()` pattern | reimplemented for CSV spans (different input shape) |

`_csv` does not depend on `_jsonl` at link time; shared headers are included by path.

## Files

| File | Purpose |
|------|---------|
| `core/csv_parse_context.hpp` | config struct: delimiter, has_header, projections, predicates; LUT |
| `core/csv_scan.hpp` | structural scan: NEON/AVX2/scalar, 5-char LUT, runtime delimiter |
| `core/csv_row_map.hpp/.cpp` | header parse + safe-split + FSM span extraction + predicate eval |
| `core/csv_column_builder.hpp/.cpp` | typed column build from CsvFieldSpan arrays |
| `_csv_reader.pyx` | thin Cython Python edge (no per-row Python) |
