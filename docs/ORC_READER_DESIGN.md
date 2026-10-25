# ORC reader — basic support (Iceberg data files and folders of `.orc`)

Status: BUILT 2026-10-25 (§12) — D1–D7 accepted as recommended. Scope ruled: **basic support
first**; the single-format dataset rule stays (a dataset is all ORC or all something else).
Date: 2026-10-25
Repos: `rugo` (decoder), `opteryx-core` (scan Source, planning), `opteryx-iceberg` (already stamps `orc`)
Companion: `docs/ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md` (the Avro scan path this copies),
`docs/AVRO_READER_DESIGN.md` (the reader this is modelled on).
Specs read 2026-10-25: ORC v1 specification (orc.apache.org/specification/ORCv1) and the Iceberg
table spec, Appendix A "ORC" (format/spec.md on `main`).

Facts below marked **[verify]** are written from memory of `orc_proto.proto` or from the Iceberg
Java writer's behaviour and must be checked against the proto file / a real file before building.

---

## 0. Why

ORC is one of the three legal Iceberg data file formats. A catalog manifest can stamp a file `orc`
(`opteryx-iceberg` now does), and the scan then refuses it by name: "ORC … has no scan reader".
There is no ORC code anywhere in the tree. This doc is the reader that removes that refusal, and —
as for Avro — makes a folder of `.orc` files a dataset.

## 1. Scope

**In (basic):**
- A native decoder in rugo: file tail, postscript, footer, stripe footers, the stream codecs, the
  integer / boolean / byte RLEs, strings (direct and dictionary), timestamps, decimals, dates.
- Primitive columns only (§3.3). Projection skips unprojected columns' streams without decoding.
- Compression NONE, ZLIB, SNAPPY, LZ4, ZSTD.
- A native Source (`NativeOrcScanSource`) shaped like the Avro one, an `ORC Reader` operator,
  `SCAN_READERS[ORC]`, the `.orc` suffix.
- Two scan shapes, as Avro (§5): an Iceberg table (columns matched by `iceberg.id`) and a folder
  (matched by name against the first file).
- Local files only (the D3 ruling, as Avro).

**Out (raises, naming the file and the feature):**
- `UNION` columns (STRUCT / LIST / MAP are read since 2026-10-25 — docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §8). Nanosecond timestamp units. Encrypted files. LZO.
- Stripe pruning from column statistics and bloom filters (§6.4) — a later step.
- Row-index based splitting inside a stripe (§6.3).
- Predicate pushdown, limit, top-N: declined at bind time exactly as for Avro, so they stay above
  the scan.
- Position / equality deletes, deletion vectors.
- Writing ORC. rugo gains a reader only.
- Authenticated remote files.

## 2. What exists

| Piece | Where | Use |
|---|---|---|
| Codecs | `third_party/libdeflate`, `snappy`, `zstd`, `lz4` | All four are vendored and already linked into rugo and the engine. ORC's chunk codecs are the *raw block* forms Avro and Parquet already decode: raw deflate (no zlib header), snappy raw block (no CRC trailer, unlike Avro), LZ4 block, zstd frame. `rugo/src/parquet/compression.hpp` `DecompressInto` covers snappy / zstd / LZ4_RAW; Avro's `inflate_raw` covers deflate. |
| Hand-written metadata decode | `rugo/src/parquet/thrift.hpp` | Precedent for decoding a self-describing metadata format with no dependency. ORC's footer is protobuf; the subset needed is small (§3.1) and gets a hand-written reader in the same spirit. **No protobuf library** (CLAUDE.md §4). |
| Whole-file scan Source | `src/cpp/engine/native_avro_scan_source.hpp` | The model: own decode pool, a file opened once under a lock-free claim, batches claimed by workers, bounded read-ahead, morsels onto a ready queue, errors via `ErrCtx` code 3. |
| Straight-to-Draken sinks | `rugo/src/avro/avro_reader.hpp` (`AvroColumn`), `native_varchar_pool_decode.hpp` | Output is `DrakenVector`s built from `draken_malloc` buffers, never an Arrow/NumPy intermediate. |
| Width conversion | `src/cpp/engine/ladder_widening.hpp` | Shared INT16→INT32→INT64 / FLOAT32→FLOAT64 conversion. See §4.3 for why this is load-bearing. |
| Format registry | `opteryx/models/dataset_format.py` | `SCAN_READERS`, `SUFFIX_TO_FORMAT`. ORC is one entry in each. |
| Scan wiring | `physical_planner/__init__.py` (`_avro_scan_inputs`), `OpteryxTable._declare_reader_capabilities`, `BaseTable.data_files_match_field_ids` | The two-shape dispatch and the pushdown declines are written once for Avro; ORC extends them (§5, §7). |
| Iceberg stamp | `opteryx-iceberg/manifests.py` | Already reads `data_file.file_format`, lower-cases it, accepts `orc`. Nothing to do. |
| Test writer | `pyarrow.orc.write_table` (pyarrow 25, test-only) | An *independent* ORC writer (the Apache C++ library). Controls compression, stripe size, `file_version`, row-index stride, dictionary threshold. Better than the Avro fixtures, which were fastavro. It cannot set type attributes, so it cannot write `iceberg.id` (§9). |

## 3. Spec surface

### 3.1 File tail

Layout, last bytes first: `psLen` (1 byte, < 256), `PostScript` (never compressed), `Footer`,
`Metadata` (stripe statistics), then stripes. The file starts with the 3 bytes `ORC`. The reader
reads the last 16 KiB, takes `psLen`, parses the postscript, then decompresses Footer and Metadata
(each is a compressed stream like any other). A larger footer than the first read means one more
read.

PostScript fields used: `footerLength` (1), `compression` (2), `compressionBlockSize` (3),
`version` (4), `metadataLength` (5), `magic` (8000, must be `ORC`). CompressionKind:
NONE 0, ZLIB 1, SNAPPY 2, LZO 3, LZ4 4, ZSTD 5.

Footer, the fields used **[verify field numbers against orc_proto.proto]**: `contentLength`,
`stripes` (`StripeInformation`: offset, indexLength, dataLength, footerLength, numberOfRows),
`types` (`Type`: kind, subtypes, fieldNames, maximumLength, precision, scale, attributes),
`numberOfRows`, `rowIndexStride`. Everything else (statistics, writer, encryption) is parsed past
or ignored; **a footer that names encryption is refused**.

StripeFooter: `streams` (kind, column, length), `columns` (`ColumnEncoding`: kind, dictionarySize),
`writerTimezone`. Stream order in the file is whatever the footer says; stream *offsets* are the
running sum of the lengths, index streams first. This is what makes projection free: the offset of
every stream is known from the stripe footer alone, so a column that is not asked for is never read,
let alone decompressed.

### 3.2 Compression chunks

Without compression a stream is raw bytes. With it, a stream is a sequence of chunks, each led by
a 3-byte little-endian header `compressedLength * 2 + isOriginal`; `isOriginal` set means the chunk
is stored uncompressed. A chunk decompresses to at most `compressionBlockSize` bytes. Chunks are
independent, and an RLE run may straddle a chunk boundary — the decoders therefore read from a
*stream cursor* that pulls the next chunk on demand, not from a flat buffer.

### 3.3 Types (basic set)

| ORC kind | Iceberg type | Draken | Basic |
|---|---|---|---|
| BOOLEAN | boolean | BOOL | yes |
| BYTE, SHORT | — (folders only) | INT8, INT16 | yes |
| INT | int | INT32 | yes |
| LONG | long | INT64 | yes |
| FLOAT, DOUBLE | float, double | FLOAT32, FLOAT64 | yes |
| STRING, VARCHAR, CHAR | string | VARCHAR | yes |
| BINARY | binary | VARBINARY | yes |
| DATE | date | DATE32 | yes |
| TIMESTAMP | timestamp | TIMESTAMP64 (µs) | yes, with the §4.4 condition |
| TIMESTAMP_INSTANT | timestamptz | TIMESTAMP64 (µs, UTC) | yes |
| DECIMAL | decimal(P,S) | DECIMAL / DECIMAL128 | yes (P ≤ 38) |
| LONG + `iceberg.long-type=TIME` | time | TIME64 | yes |
| BINARY + `iceberg.binary-type=UUID` | uuid | UUID (16 bytes; any other length fails naming it) | built 2026-10-25 (UUID_AND_NESTED_COLUMNS_DESIGN.md UD1) |
| BINARY + `iceberg.binary-type=FIXED`, `iceberg.length` | fixed(L) | VARBINARY, every value checked against L | built 2026-10-25 (UD7) |
| TIMESTAMP[_INSTANT] + `iceberg.timestamp-unit=NANOS` | timestamp_ns | — | **refused** |
| STRUCT, MAP | nested | NVARCHAR — one JSON object per row (a MAP's keys must be strings) | built 2026-10-25 (UUID_AND_NESTED §8) |
| LIST of a plain scalar | nested | ARRAY<scalar> | built 2026-10-25 |
| LIST of anything else | nested | NVARCHAR — one JSON array per row | built 2026-10-25 |
| UNION | nested | — | **refused** |

Iceberg ORC attributes (spec, Appendix A): the column id is the ORC type attribute `iceberg.id`;
`iceberg.required="true"` marks a required column; `iceberg.timestamp-unit` defaults to MICROS when
absent. Iceberg ORC writers *must* truncate timestamps to microseconds.

## 4. Decode

### 4.1 Integer, boolean and byte streams

- **Byte RLE**: header 0..127 = run of `header+3` copies of the next byte; −128..−1 = `|header|`
  literal bytes.
- **Boolean**: byte RLE over bits packed MSB first.
- **Integer RLE v1** (`DIRECT`, `DICTIONARY`): runs (`header 0..127` = length−3, a signed delta byte,
  a base varint) and literals (`header −128..−1`). Varints are base-128 little-endian, zigzag when
  signed.
- **Integer RLE v2** (`DIRECT_V2`, `DICTIONARY_V2`): the top two bits of the first byte select
  **Short Repeat**, **Direct**, **Patched Base**, **Delta**. Widths come from the spec's 5-bit
  encoded-width table (1–64 bits). All four are needed — a Java-written file uses all of them within
  one column. Patched Base is the one with sharp edges (the patch gap/value packing, the base's sign
  in its high bit) and gets the most test attention.

Each is a pull decoder over a stream cursor (§3.2) that fills a `uint64`/`int64` run buffer a vector
at a time; the type-specific sink narrows or widens it. Decoding a *present* stream first gives the
validity bitmap and the count of values the DATA stream actually holds (null rows have no entry).

### 4.2 Strings

- **Direct**: `LENGTH` (RLE) + `DATA` (concatenated UTF-8). Built into the arena/slot layout the
  Avro and JSONL Sources already produce.
- **Dictionary**: `DICTIONARY_DATA` + `LENGTH` for the entries, `DATA` = per-row indices. The
  dictionary is sorted by UTF-8 bytes. This maps onto the unified vector shape (CLAUDE.md §11):
  the dictionary is the data, the indices are `selection` — no per-row string copy — and
  `DRAKEN_DICT_KEYS_SORTED` can be set because the format guarantees the order **[verify the
  guarantee holds for the C++ writer too, not only Java; if not, do not set the hint]**. This is
  the uniform path; no shape-specialised kernel is introduced.

### 4.3 Narrowing, widening, and the RETAGWIDTH trap

ORC stores INT and LONG as the same RLE; the *type* says the width. An Iceberg table that widened a
column (`int`→`long`, `float`→`double`, decimal precision) reads an old file whose ORC type is the
narrower one. The sink must **convert** to the table's width. Re-tagging a narrower buffer as the
wider type produced wrong, run-to-run varying answers once before (the Parquet pass-1 retag bug,
fixed through `ladder_widening.hpp`). The ORC sinks use that shared conversion and nothing of their
own; a promotion outside the ladder raises.

### 4.4 Timestamps — the one real semantic risk

ORC stores `DATA` = signed seconds since 2015-01-01 00:00:00 and `SECONDARY` = nanoseconds, with the
low three bits encoding trailing decimal zeros (`zeros > 2` → stored value `n/10^zeros << 3 | zeros−1`).
Two traps that are not in the prose spec and must be handled and tested:

1. **Negative seconds.** The Java reader adjusts the seconds when `seconds < 0` and nanos are
   non-zero (the encoding truncates toward zero) **[verify against the C++ and Java readers]**.
2. **Writer time zone.** Legacy `TIMESTAMP` (no `INSTANT`) is *wall-clock in the writer's zone*;
   `StripeFooter.writerTimezone` names it. `TIMESTAMP_INSTANT` is an absolute instant.

Basic support takes the narrow, honest rule: `TIMESTAMP_INSTANT` is converted with no zone logic.
`TIMESTAMP` is accepted only when `writerTimezone` is `UTC` (which I expect Iceberg writers to set
**[verify against a Spark/Trino-written Iceberg file]**) and is **refused, naming the zone**
otherwise. No tz database is vendored. This needs the real-file check before the doc is ruled.

### 4.5 Decimals

`DATA` = unbounded zigzag base-128 varints of the unscaled value, `SECONDARY` = the scale per value
(signed RLE). The reader converts to the declared `scale` of the type; a value whose own scale
differs is rescaled when exact and raises when it would lose digits. P ≤ 18 → int64; 19..38 → int128.

### 4.6 Dates and other

`DATE` = days since 1970-01-01 (RLE). `FLOAT`/`DOUBLE` = IEEE little-endian, no RLE. `BYTE` = byte RLE.

## 5. Two scan shapes (as Avro)

Told apart by the connector's existing `data_files_match_field_ids` declaration, which becomes a
format-neutral `data_files_match_field_ids` (renamed from `avro_matches_field_ids`, D5).

| | Iceberg table (`OpteryxTable`) | Folder of `.orc` (`FileSystemTable`) |
|---|---|---|
| Relation | the table's schema | the first file's footer |
| Match columns by | `iceberg.id` on each top-level type | name |
| Reader schema | built from the table | the first file's |
| Guard | every top-level field must carry `iceberg.id`, else the query fails | — |
| Remote files | refused at plan time | refused at plan time |

**The id guard is not optional.** ORC is name-based and Iceberg renames columns freely; matching a
renamed column by name returns NULLs without an error. Avro produced exactly that bug in testing
(`ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md` §9). Here the guard lives in the ORC reader's own
resolution, so it is not a separate engine hack: a reader schema that carries ids against a file
whose types carry none is an error.

The Iceberg spec notes Iceberg adapts by *renaming* reader fields to the writer's names; this
reader matches by id directly and never relies on that.

## 6. Engine

### 6.1 Placement

```
rugo/src/orc/
  orc_proto.{hpp,cpp}     hand-written protobuf subset (varint, length-delimited, packed)
  orc_stream.hpp          chunk cursor over a decompressed stream (§3.2)
  orc_rle.{hpp,cpp}       byte / bool / int RLE v1 and v2
  orc_reader.{hpp,cpp}    OrcFile: tail, footer, stripes, type tree, reader-schema resolution,
                          per-stripe decode into OrcColumn (the AvroColumn analogue)
  _orc_column_wrap.*      the Python edge (rugo.orc.read_orc, read_metadata) for tests and rugo users
src/cpp/engine/native_orc_scan_source.hpp     NativeOrcScanSource
opteryx/operators/orc_read/                   OrcReadNode (planning only; no Python read path)
opteryx/connectors/orc_io/                    reader-schema construction and type mapping
```

rugo and Draken must run without Python (CLAUDE.md §2): `rugo/src/orc/` is pure C++ with a thin
Python edge, as `rugo/src/avro/` is. rugo stays opteryx-free; the engine Source and the planning
pieces live in core.

### 6.2 The Source

Copied from `NativeAvroScanSource`, changing the unit of work. The Source owns its decode pool;
a file is loaded once (local: memory-mapped; the file is complete in memory, so stripes are
addressable by offset), compiled against the reader schema, and framed into **stripes**. Workers
claim stripes; a finished stripe becomes morsels on the ready queue; bounded read-ahead
(`decode_workers + 2` stripes claimed-but-unconsumed) bounds memory. Morsel order is not guaranteed.
Errors go through `ErrCtx` code 3, which the engine already scrubs to a path-free user message, and
log the detail.

### 6.3 Granularity

A stripe is the unit (Java default 64 MiB, so typically tens of thousands to millions of rows). A
stripe is decoded whole per projected column and emitted as morsels of at most 65,536 rows. A file
with one stripe therefore decodes on one thread. That is accepted for basic support: the RLE streams
cannot be entered mid-stream without the row index (`ROW_INDEX` streams hold the positions), and
using it is the "row-index splitting" deferred in §1. COUNT(*) reads no stream at all — the row count
is in the stripe information.

### 6.4 Deferred: pruning

Stripe statistics (the `Metadata` section) and bloom filters would let a Source skip stripes. Not
built. The manifest-level file pruning that already works for every format is unaffected; Iceberg's
ORC writer records column metrics in the manifest where its Avro writer records none **[verify on a
real file]**, so file pruning should in practice work for ORC tables.

## 7. Planning wiring

- `dataset_format.py`: `ORC = "ORC"`, `.orc` in `SUFFIX_TO_FORMAT`, `SCAN_READERS[ORC] = "ORC Reader"`.
- Physical planner: an `ORC Reader` branch beside the Avro one, with `_orc_scan_inputs` choosing the
  shape from the connector declaration.
- `OpteryxTable._declare_reader_capabilities`: ORC joins AVRO in declining predicate, limit, top-N
  and timestamp-retag pushdown (the class-level flags describe Parquet; a pushed predicate is
  *removed* from the plan, so accepting one the reader ignores returns wrong answers).
- `FileSystemTable`: `_read_orc_schema` from the first file's footer; `can_push` already returns
  False for any format not listed.
- Table-side types: `opteryx-iceberg` already refuses nested types at schema time, so the
  nested refusal is first hit in the folder shape.

## 8. Errors (fail fast, no fallbacks)

Each names the dataset, the file and the feature: bad magic; postscript length out of range; a
compression kind of LZO or an unknown one; a chunk header past the end of its stream; a chunk that
decompresses past `compressionBlockSize`; an encryption section; a nested or refused type in the
projection; a `TIMESTAMP` whose writer zone is not UTC (naming the zone); a missing `iceberg.id`
on a table scan; a column absent from a file that the table requires; a promotion outside the
ladder; a DECIMAL value that does not fit its declared precision; a run past its stream's length; a
remote file. Corrupt data is never skipped and never zero-filled.

## 9. Tests

- **Folder shape, independent writer**: `pyarrow.orc.write_table` (test-only; CLAUDE.md §4 allows
  pyarrow in `tests/`), across compression {none, zlib, snappy, lz4, zstd}, `file_version`
  {0.11, 0.12}, small `stripe_size` for many stripes, small `compression_block_size` to force
  chunk-straddling runs, dictionary on/off, row-index stride, nulls at 0 % / 50 % / 100 %. Every
  type in §3.3. Values are the values written; no oracle but the input.
- **RLE unit tests** without files: hand-built v1/v2 byte sequences for each sub-encoding, including
  Patched Base with patches and a run crossing a chunk boundary.
- **Fuzz**: truncation and bit-flip at every offset of a small file must raise, never crash or hang
  (the Avro corpus/fuzz targets are the model).
- **Iceberg shape**: pyarrow cannot write `iceberg.id`. Two options, D2 below. Until one is chosen
  the id-matching, evolution (rename / add / drop / reorder) and the id guard are not testable
  end to end.
- **Round trip with the engine**: `SELECT … FROM '<folder>'`, projection, filter, COUNT, LIMIT,
  several files and stripes, evolution across files by name, each refusal by message — the shape of
  `tests/unit/connectors/test_avro_folder_dataset.py`.
- **Real writer evidence**: one Spark- or Trino-written Iceberg ORC file. Same gap as Avro, and here
  it also settles §4.4.
- `make q` and `make slt` before sign-off; native deletions would add `make dt st rt`.

## 10. Size and order

Rough build order, each step testable alone: proto subset and tail → chunk cursor and codecs →
byte/bool RLE → int RLE v1 → int RLE v2 → primitive sinks and strings → timestamps / decimals →
reader-schema resolution and the id guard → `rugo.orc` edge and its tests → the Source →
planning, registry, `FileSystemTable` → Iceberg shape and tests. The RLE v2 decoder and the
timestamp/zone rules are the risk; the Source and wiring are mechanical because Avro already
proved them.

## 11. Decisions for the architect

| # | Question | Recommendation |
|---|---|---|
| D1 | A `READ_ORC('path')` function, as `READ_AVRO` exists? | **REVERSED 2026-10-25**: built, because an anonymous ORC object has no other way into SQL (`ANONYMOUS_REMOTE_READS_DESIGN.md` §2.2). Originally: No in this step.
| D2 | Fixtures for `iceberg.id` (pyarrow cannot set type attributes) | Rewrite the footer of a pyarrow-written file in the test helper (decompress → append the `attributes` to each `Type` → recompress → patch lengths). Smaller than writing an ORC writer, and it keeps pyarrow as the data writer. Alternative: a minimal ORC writer in rugo, which also gives round-trip tests but is real scope. |
| D3 | `TIMESTAMP` (no instant) with a non-UTC writer zone | Refuse, naming the zone (§4.4). Honouring zones needs a tz database, which is a dependency decision. |
| D4 | Sorted-dictionary hint on ORC string dictionaries | Set `DRAKEN_DICT_KEYS_SORTED` only if the C++ writer provably sorts too; otherwise leave it off (a missing hint is "don't know", never "false"). |
| D5 | Rename `data_files_match_field_ids` to `data_files_match_field_ids` | Yes — one attribute, introduced this week, now serving two formats. |
| D6 | Stripe-level parallelism only (§6.3) | Accept for basic support; row-index splitting is the follow-up if single-stripe files matter. |
| D7 | UUID and `fixed(L)` columns | Refused, consistent with Avro; `opteryx-iceberg` binds UUID as VARCHAR and has no `fixed`, so the table side must be ruled first. |

---

## 12. What was built (2026-10-25)

All of §10's steps, in order. Where the build differs from the text above:

- **Whole-stream decompression, not a chunk cursor (§3.2, §6.1).** `orc_stream.hpp` was not
  written. A stream is decompressed into one buffer (`decompress()` in `orc_reader.cpp`) and the
  RLE decoders read from flat memory, so a run straddling a chunk boundary is a non-event and the
  decoders have no refill path. Memory is bounded by one stream (≤ one stripe's column).
- **One morsel per stripe (§6.3).** A stripe is decoded whole and emitted whole, not cut into
  65,536-row morsels. A single-stripe file decodes on one thread, as accepted in D6; downstream
  operators see stripe-sized morsels, as they do Parquet row groups.
- **The carrier is `rugo::avro::AvroColumn`** (and the Python edge `wrap_avro_column`, and the
  Source's `avro_detail::build_column`), reused rather than copied; `native_orc_scan_source.hpp`
  includes the Avro Source header for it. A neutral rename of the carrier is a later cleanup.
- **UTC writer names (§4.4, D3).** A `TIMESTAMP` is accepted when `writerTimezone` is one of
  UTC, GMT, Etc/UTC, Etc/GMT, Etc/Universal, Universal, Zulu, Etc/Zulu, Etc/UCT, UCT, Z. pyarrow
  (the Apache C++ writer) always records `GMT`, whatever the process zone, so it can neither
  produce nor test a non-UTC file; the refusal is tested by rewriting that one string in an
  uncompressed footer.
- **Reader columns are the projected columns only** (`orc_io.reader_columns`), unlike Avro's
  full-table reader schema: ORC needs no schema for a zero-column scan (row counts are in the
  footer), so nothing beyond the projection is ever matched.
- **A folder with an unreadable column raises at bind time** (`orc_io.header_schema`), naming
  the column and why (LIST / MAP / STRUCT / UNION, nanosecond units, uuid / fixed binary). The
  alternative, omitting it from the relation, would drop a column silently.
- **D5 done:** `avro_matches_field_ids` is now `data_files_match_field_ids`.

Files: `rugo/src/orc/{orc_proto,orc_rle,orc_reader}.{hpp,cpp}`, `_orc_reader.pxi`, `rugo/orc/`;
`src/cpp/engine/native_orc_scan_source.hpp`; `opteryx/operators/orc_read/`;
`opteryx/connectors/orc_io/`; registry, planner, compiler, `FileSystemTable` and
`OpteryxTable._declare_reader_capabilities` wiring.

### 12.1 Verified against a real writer, and not

Verified against pyarrow.orc (Apache ORC C++): every type in §3.3 under all five codecs;
RLE v1 and v2 (file versions 0.11 / 0.12); direct and dictionary strings; nulls at 0 / 50 /
100 %; patched-base, delta, short-repeat and direct runs (a column built to hit each); 64-bit
extremes; multi-stripe files; the nanosecond encoding (zeros field = zeros − 1); truncation
at every offset (always raises) and 300 random bit-flips (never crashes). The RLE v2 decoders
also pass the byte examples in the ORC specification.

**Not verified, and why it matters:**
- **Pre-1970 timestamps.** The seconds adjustment (§4.4 trap 1) is written from the Java
  reader's behaviour. Arrow's ORC adapter writes *negative nanoseconds* for pre-1970 values
  (truncating division), so a pyarrow file cannot serve as an oracle: this reader raises on
  one ("nanoseconds out of range") rather than guessing. A Spark/Trino-written file is needed.
- **Real Iceberg ORC files.** The `iceberg.id` fixtures are pyarrow data with a rewritten footer
  (D2), not a file an Iceberg engine produced. The attribute spelling comes from the spec.
- **Hive-era files** (DECIMAL with per-value scales, CHAR padding) and **LZO**: not exercised.
- **Performance.** Not measured. The RLE bit-unpacking is a straightforward byte-at-a-time loop,
  and every decode allocates per-column temporaries; this is a correctness build.

### 12.2 Tests

`tests/rugo/test_orc_reader.py` (33), `tests/unit/connectors/test_orc_folder_dataset.py` (10),
`opteryx-iceberg/tests/test_orc_data_files.py` (14) with `tests/_orc_data_files.py` (footer
rewrite). The Avro suite's "ORC is refused by name" test was removed: it no longer is.
