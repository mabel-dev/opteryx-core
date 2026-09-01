# Parquet Writer Design (Rugo)

Status: Phase 0 + Phase 1 LANDED (uncommitted) — Phases 2/3 pending decisions
Date: 2026-06-23

## Status log

- **Phase 0 (Thrift Compact Protocol writer) — DONE.**
  `rugo/src/parquet/_thrift_writer.hpp` (namespace `rugo_pq_write`). Verified by
  round-tripping its output through the reader's own `thrift.hpp`
  (`scratch/test_thrift_writer.cpp`).
- **Phase 1 (PLAIN encoder + flat schema + footer + Cython edge) — DONE.**
  - Core: `rugo/src/parquet/_parquet_writer.hpp` (header-only, plain-buffer in /
    bytes out; no DrakenVector coupling).
  - Edge: `rugo/src/parquet/parquet_writer.pyx` + `.pxd` → `rugo.parquet_writer`
    extension (added to `setup.py`). `write_parquet(morsel) -> bytes`.
  - Verified: real opteryx query → Morsel → `write_parquet` → **PyArrow reads
    back exact values, types, and null placement**; rugo's own `read_metadata`
    parses the footer. Tests: `tests/rugo/test_native_parquet_writer.py` (6,
    green). Scope: INT64/FLOAT64/BOOL/VARCHAR/NVARCHAR/VARBINARY, PLAIN,
    UNCOMPRESSED, single row group.

- **Phase 2a (ZSTD compression) — DONE.** Architect approved vendoring the
  zstd **compress** sources (zstd 1.5.7 `lib/compress/*.c`, byte-identical to
  upstream, renamed `.cpp` like the decompress set; single-threaded — no
  zstdmt/pool/threading). Added `get_zstd_compress_sources()` in `setup.py`;
  writer extension now links common + decompress + compress. Codec threaded
  through the core (`HAVE_ZSTD`-gated; unknown/zstd-without-build = hard error,
  never a silent fallback). `write_parquet(morsel, compression="zstd"|"none")`,
  **ZSTD default**. Verified: PyArrow reads `codec=ZSTD`, exact values, ~12×
  on compressible data. Tests now 9, green.

- **Phase 2b (statistics) — DONE.** Per-column-chunk min/max/null_count emitted
  as Statistics field 12: `null_count`(3), `max_value`(5), `min_value`(6),
  `is_*_value_exact`(7/8). Ordering matches the reader's pruning comparison —
  signed INT64, IEEE DOUBLE (NaN excluded from min/max), unsigned-byte
  lexicographic BYTE_ARRAY, bool. `FileMetaData.column_orders`(7) emitted as
  TypeDefinedOrder so readers trust the v2 stats. Verified: PyArrow reads exact
  min/max/null_count (incl. all-null → null_count only; all-valid →
  null_count 0); rugo's own footer parser still green. Tests now 10.

**Phase 2 COMPLETE.**

- **Phase 3 (date / timestamp / decimal logical types) — DONE.** DATE32 →
  INT32+DATE; TIMESTAMP64 → INT64+TIMESTAMP (unit read from the descriptor:
  us/ms/ns mapped losslessly, seconds→micros; `isAdjustedToUTC` emitted);
  DECIMAL (int64) and DECIMAL128 (`__int128`) → FIXED_LEN_BYTE_ARRAY,
  big-endian unscaled (width 8/16). Full `LogicalType` union written
  (DateType / TimestampType{unit} / DecimalType{scale,precision}) plus the
  matching ConvertedType + SchemaElement scale/precision. Logical params read
  from the nanobind handle (`v._nb.logical_type_unit/scale/precision`);
  missing descriptor on a parameterized type fails loud. Decimal stats order
  by numeric unscaled value (signed `__int128`), not raw bytes. Verified end
  to end through opteryx → PyArrow (date32 / timestamp[us] / decimal128 incl.
  a 2¹⁰⁰ int128 value), with stats. Tests now 13.

- **Bloom filters — DONE.** Split-block bloom filters (`_bloom_writer.hpp`)
  written per equality-friendly column (ints, strings/binary, date, timestamp,
  decimal; never float/bool). XXH64 (seed 0, vendored `third_party/cyan4973`)
  over each value's PLAIN-encoded bytes; power-of-2 block count (so block
  selection is identical to the rugo reader AND canonical parquet/PyArrow);
  FPP 1%, NDV-sized. `[BloomFilterHeader][bitset]` written after the data
  pages, recorded in ColumnMetaData `bloom_filter_offset`(14)/`length`(15).
  API: `write_parquet(morsel, bloom_filters=True | False | [names])`, default
  True. Verified: present values probe True via the reader's `test_bloom_filter`
  (no false negatives), absent probe False, PyArrow still reads the file.
  Reader side gained `read_rowgroup_stats` exposure of bloom offset/length.

- **Extended type coverage — DONE.** FLOAT32 (lossless widen → DOUBLE),
  VARIANT (→ BYTE_ARRAY/STRING, JSON text), TIME32 (→ INT32 + TIME MILLIS),
  TIME64 (→ INT64 + TIME MICROS/NANOS), INTERVAL (→ FIXED_LEN_BYTE_ARRAY(12) of
  little-endian months/days/millis + ConvertedType INTERVAL; sub-ms dropped —
  parquet's inherent resolution; no min/max stats since INTERVAL sort order is
  UNKNOWN). Verified via PyArrow (FLOAT32→double, TIME32→time32[ms],
  TIME64→time64[us], INTERVAL bytes correct). ARRAY/LIST is the remaining
  unsupported type (fails loud) — needs repetition levels; next.

- **ARRAY / LIST — DONE.** Canonical 3-level parquet LIST encoding:
  `OPTIONAL group(LIST) { REPEATED group "list" { OPTIONAL "element" } }`,
  max def=3 (0=null list, 1=empty, 2=null element, 3=present), max rep=1.
  Generic RLE level encoder (rep bw=1, def bw=2); present element values
  PLAIN-encoded. Element types: int (→INT64), float (→DOUBLE), bool, string.
  Nested arrays / exotic element types fail loud. Stats/bloom omitted for list
  leaves. Verified via PyArrow: `[[1,2,3],[],None,[4,None,6]]` and
  `[["x","yy"],None,["z"]]` round-trip exactly (null list / empty / null
  element all preserved). The writer's "flat columns" simplification is now
  lifted for single-level lists. FP16 is the only remaining unsupported type.

**Phases 0–3 COMPLETE, and the pyarrow `write_morsel` cutover is DONE.**

- **`DRAKEN_NULL` (all-null typeless columns) — RESOLVED.** Emitted as an
  all-null **INT32** column (architect-approved). The edge synthesizes an
  all-null validity mask; no values are written.

- **`write_morsel` cutover — DONE.**
  `opteryx/connectors/parquet_io/parquet_writer.py` no longer imports pyarrow.
  `write_morsel()` calls `rugo.parquet_writer.write_parquet_with_bounds` (ZSTD),
  writes bytes atomically (tmp + `os.replace`), and builds the `FileEntry`
  min/max bounds from the writer's stats. Bounds cover plain
  int/float/bool/utf8-string columns (matching the old effective contract — the
  old `_serialize_bound` raised on decimal/date/timestamp anyway); logical-typed
  and VARBINARY columns are omitted. This also **fixed a broken path**: the old
  `write_morsel` called `morsel.to_arrow()`, which no longer exists. Tests:
  `tests/storage/test_parquet_writer.py` rewritten to the current morsel API
  (11) + `tests/rugo/test_native_parquet_writer.py` (12) — 23 green. No pyarrow
  import remains in opteryx production code for writing.

  NOTE: the INSERT data path (`insert.pyx` → `write_morsel`) is currently
  blocked **upstream** of the writer by pre-existing engine breakage (VALUES /
  Function-Dataset planning `Expected list, got tuple`; `_drain_pipeline(...,
  collect=)`; INSERT column-list parsing). Unrelated to this work; in files
  under active change elsewhere.

## 1. Goal

Add a parquet **writer** to `rugo/` so Opteryx can emit well-formed parquet
files **without PyArrow**. This is the inverse of the existing reader under
`rugo/src/parquet/`.

Explicitly **not** a standards-complete writer. We deliberately narrow the
encoding and compression surface to the minimum that:

1. Round-trips losslessly through rugo's own reader, and
2. Is readable by PyArrow / DuckDB (the verification oracle).

Anything outside that surface fails loud — no silent degradation, no partial
encodings (CLAUDE.md §1, §9).

### Non-negotiable requirement

**Every file we write MUST be readable by PyArrow.** This is the primary
acceptance criterion, not a soft goal. A file that rugo can read but PyArrow
cannot is a **defect**, not a partial success — it means our Thrift/encoding is
self-consistent but wrong against the spec. If a supported type or value cannot
be written in a PyArrow-readable way, the writer fails loud rather than emitting
a file only rugo can read.

## 2. Scope (ratified)

| Dimension        | Supported                                                            | Rejected (fail loud)                          |
|------------------|---------------------------------------------------------------------|-----------------------------------------------|
| Page encoding    | `PLAIN` (all types); `RLE` (definition levels); `PLAIN_DICTIONARY` for strings (Phase 4, optional) | DELTA*, BYTE_STREAM_SPLIT, BIT_PACKED         |
| Compression      | `UNCOMPRESSED`, **`ZSTD` (default)**                                 | snappy, gzip, lz4, brotli                     |
| Types            | int64, float64, bool, varchar/nvarchar/varbinary, decimal (FLBA), date32, timestamp(µs) | nested LIST / MAP / STRUCT                     |
| Nullability      | flat columns, definition level 0/1                                  | repetition levels (no nesting)                |
| Layout           | one row group per morsel, **one data page per column chunk**         | multi-page chunks, bloom filter, column index |

Ratified decisions:
- **ZSTD is the only codec and the default.** Uncompressed available as an
  explicit opt-out.
- **Decimals written as FIXED_LEN_BYTE_ARRAY** (sign-extended big-endian),
  matching the reader's decimal decode path. Applies to both decimal tiers
  (`DRAKEN_DECIMAL` int64-backed, `DRAKEN_DECIMAL128` int128-backed).
- **Timestamps written as micros** (`TIMESTAMP(isAdjustedToUTC, MICROS)`),
  physical INT64. Unit is read from the `TIMESTAMP64` logical descriptor; a
  non-µs unit fails loud rather than silently truncating.
- **API returns `bytes` first.** Streaming/file-handle sink deferred.

The two big simplifiers — **single data page per column chunk** and **no
repetition levels** — remove the bulk of the complexity while still producing
files the reader and Arrow/DuckDB accept.

## 3. What the reader gives us for free

The writer is built as the structural inverse of existing read-side code:

| Read side                                              | Write side (to build)                          |
|--------------------------------------------------------|------------------------------------------------|
| `thrift.hpp` — Compact Protocol reader                 | Compact Protocol **writer**                    |
| `metadata.hpp` structs (FileStats, RowGroupStats, ColumnStats, SchemaElement) | reuse structs; build + serialize them |
| `compression.cpp` — zstd **decompress** (vendored)     | zstd **compress** (same vendored lib)          |
| `rle_decoder.hpp` — RLE level decode                   | RLE level **encode**                           |
| `decode_column.cpp` — PLAIN/dict value decode          | PLAIN value **encode**                         |
| `parquet_reader.pyx` — vector ⟵ DecodedColumn          | vector ⟶ column data                           |

DrakenType enum (authoritative: `draken/core/buffers.h`):
`DRAKEN_INT64=4`, `DRAKEN_DECIMAL=5`, `DRAKEN_FLOAT64=21`, `DRAKEN_DATE32=30`,
`DRAKEN_TIMESTAMP64=40`, `DRAKEN_BOOL=50`, `DRAKEN_VARCHAR=60`,
`DRAKEN_NVARCHAR=63`, `DRAKEN_VARBINARY=64`, `DRAKEN_DECIMAL128=103`.

## 4. File layout produced

```
PAR1                                  ← 4-byte magic header
<data page>   column 0  (def levels RLE + PLAIN values, optionally zstd)
<data page>   column 1
...
<FileMetaData>                        ← Thrift Compact Protocol
<footer length : LE uint32>
PAR1                                  ← 4-byte magic trailer
```

One row group (the morsel). One column chunk per column, each a single data
page (v1 page header). Column chunk metadata records offsets, codec,
encodings, and statistics.

## 5. Type → parquet mapping

| DrakenType            | Physical type             | Logical / converted type     | Notes |
|-----------------------|---------------------------|------------------------------|-------|
| INT64                 | INT64                     | —                            | |
| FLOAT64               | DOUBLE                    | —                            | |
| BOOL                  | BOOLEAN                   | —                            | bit-packed values per spec |
| VARCHAR / NVARCHAR    | BYTE_ARRAY                | STRING                       | NVARCHAR carries UTF-8 |
| VARBINARY             | BYTE_ARRAY                | —                            | no logical annotation |
| DATE32                | INT32                     | DATE                         | days since epoch |
| TIMESTAMP64           | INT64                     | TIMESTAMP(µs)                | unit from logical descriptor; non-µs → error |
| DECIMAL (int64)       | FIXED_LEN_BYTE_ARRAY      | DECIMAL(p,s)                 | min width to hold value, BE sign-extended |
| DECIMAL128 (int128)   | FIXED_LEN_BYTE_ARRAY      | DECIMAL(p,s)                 | up to 16 bytes |

Types outside this table fail loud at schema-build time.

## 6. Build phases

### Phase 0 — Thrift Compact Protocol writer
Mirror `thrift.hpp`: `WriteVarint`, zigzag `WriteI32/WriteI64`, field headers
with delta field-id encoding, struct begin/stop, list/binary writers.
Foundation for everything downstream. Unit-tested against `thrift.hpp` by
write-then-read of synthetic structs.

### Phase 1 — PLAIN encoders + flat schema + round trip
- Vector → PLAIN page bytes for INT64, FLOAT64, BOOL, BYTE_ARRAY.
- RLE encoder for definition levels (nullable flat columns).
- SchemaElement tree builder (flat).
- Emit header, one uncompressed data page per column, single row group,
  FileMetaData footer + trailer.
- **Exit gate:** write → `read_parquet()` → vectors equal originals,
  including nulls; **and the same bytes open in PyArrow** with matching
  values. PyArrow-readability is gated from Phase 1 onward, not deferred.

### Phase 2 — Statistics + ZSTD
- Min/max/null_count computed during encoding, written into ColumnStats
  (the reader already consumes these for row-group pruning).
- zstd **compress** path added alongside the existing decompress in
  `compression.cpp`, using the vendored zstd. ZSTD becomes the default.

### Phase 3 — Logical types: decimal + temporal
- SchemaElement logical-type annotations: DATE, TIMESTAMP(µs), DECIMAL(p,s).
- FLBA decimal encoder (BE sign-extend, inverse of reader decimal path) for
  both decimal tiers.
- DATE32 (INT32) and TIMESTAMP64 (INT64 µs) encoders.
- **Exit gate:** types survive round trip through both rugo and DuckDB.

### Phase 4 — (optional) Dictionary encoding for strings
- Dictionary page + RLE_DICTIONARY codes for BYTE_ARRAY.
- Only if write size / throughput measurements justify it; PLAIN strings ship
  first. Measure before building (CLAUDE.md §3).

## 7. Edge & API

C++ core does the work; thin typed Cython edge in a new
`rugo/src/parquet/parquet_writer.pyx` (typed-only, **no `object` params or
returns** — CLAUDE.md §3). Native files prefixed `_` where colocated.

```python
write_parquet(morsel, *, compression: str = "zstd") -> bytes
#   compression ∈ {"zstd", "none"}
```

Returns the complete file as `bytes`; the caller decides the destination
(local fs, GCS), mirroring `read_parquet(data)` taking bytes. A streaming sink
for large outputs is a later addition, not in this design.

## 8. Verification

Self-round-trip (write → `read_parquet`) is necessary but **not sufficient** —
it hides symmetric Thrift/encoding bugs. The gate is the **external oracle**:

1. `write_parquet(morsel)` → bytes.
2. Read back through rugo's reader → assert values + types match input.
3. Read the **same bytes** through PyArrow and DuckDB (Arrow permitted in
   `tests/`) → assert identical values, types, null counts.

Coverage matrix: every supported type × {all-valid, with-nulls, all-null} ×
{zstd, none}. `make q` must pass.

## 9. Open questions / deferred

- Multi-page column chunks (large columns) — deferred; single page for now.
- Row-group sizing policy (one-morsel-one-rowgroup vs target byte size) —
  deferred; revisit when writing multi-morsel outputs.
- Bloom filters / column index — out of scope.
- Nested types (LIST/MAP/STRUCT) — out of scope; would require repetition
  levels and is a separate initiative.
- Streaming/file-handle sink — deferred (bytes first).
