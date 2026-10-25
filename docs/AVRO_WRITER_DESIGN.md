# Avro writer (rugo) — design and plan

Status: §12 RULED 2026-10-16 (every recommendation stands as written). Phases 1–5 BUILT 2026-10-16 (§13) — the plan is complete. Signed off 2026-10-16: `make q` and `make slt` green.
Date: 2026-10-15 (plan), 2026-10-16 (rulings)
Reader design (the other half of this format): `docs/AVRO_READER_DESIGN.md`.
Parquet writer precedent (the API and build shape this follows): `docs/PARQUET_WRITER_DESIGN.md`.

## 1. Goal

Give rugo an Avro object-container **writer**: Morsel in, standard Avro file out,
readable by Apache Avro (Java/Python), fastavro, Spark, Hive, Kafka tooling — and by
rugo's own reader, value for value. Native C++ end to end (no Python per value, no
PyArrow, no NumPy), the same posture as the reader (§20 of the reader doc: the engine
is judged on it).

The reader doc listed this as scope C ("only if opteryx ever commits to Iceberg
tables"). This plan is for the rugo file engine, not Iceberg: the standalone `rugo`
wheel claims "read + write" for its formats and Avro is the one it only reads.

## 2. Scope

In:
- Container (magic, metadata map, sync, blocks), every codec the reader reads:
  `null`, `deflate`, `snappy`, `zstandard`, `bzip2`. `xz` stays out (reader refuses it).
- Flat records of scalars + the logical types in §5, ARRAY columns (incl. nested
  arrays), every column nullable (`["null", T]`), NULLs via validity.
- One-shot `write_avro(morsel) -> bytes`, streaming `open_avro_writer(sink)` /
  `write_avro_stream(iter, sink)`, CLI (`rugo convert x.parquet y.avro`).
- Parallel block encoding (per-block work units, like the reader's per-batch decode).

Out (refused, loud, naming the column) — see §5 for each:
- Types Avro cannot carry without loss or that the reader cannot read back:
  INTERVAL, VECTOR_FP16, UINT64, nanosecond TIMESTAMP/TIME.
- Column names that are not valid Avro names. No sanitising.
- Maps, records, enums as OUTPUT types: Draken has no struct/map/enum vector, so
  nothing produces them. (VARIANT is JSON text → `string`.)
- Schema evolution, aliases, defaults, non-null defaults: a writer writes one schema.
- Writing from the SQL engine. RULED 2026-10-10: Avro is available to the engine only
  through `READ_AVRO`, never as a stored-dataset format. No engine write path is added
  and the writer is built into `rugo_native` only (not the operators extension).

## 3. Placement and dependencies

```
rugo/src/avro/avro_writer.hpp / .cpp     pure C++: schema JSON emit, row encoding, blocks, codecs
rugo/src/avro/_avro_writer.pxi           Cython edge in rugo_native: Morsel -> DrakenVector* + descriptors
rugo/avro/__init__.py                    facade: write_avro / open_avro_writer / write_avro_stream
rugo/cli/_common.py                      ".avro" in _EXT_TO_FORMAT + write/read dispatch
```

Reuse, not duplication: `avro_container.hpp` gains the inverse of `read_header` /
`BlockReader` (header emit, block framing); `avro_varint.hpp` gains the encoders
(`write_long`, `zigzag_len`); `avro_schema.hpp` is NOT reused for emission (the
writer's schema is a direct function of the DrakenTypes; emitting JSON text from a
table is ~80 lines and needs no node tree). Codecs:

| Codec | Compress with | Already in the rugo_native build? |
|---|---|---|
| `deflate` | libdeflate `libdeflate_deflate_compress` (raw RFC 1951) | vendored (`third_party/libdeflate/lib/deflate_compress.c`), **not compiled** — add the TU to `build_common.py` (no new vendoring) |
| `zstandard` | `ZSTD_compress` | yes (`get_zstd_compress_sources`, for the parquet writer) |
| `snappy` | `snappy::RawCompress` + 4-byte big-endian CRC32 of the uncompressed bytes | yes (`snappy.cc` is the whole library); CRC32 from libdeflate's `crc32.c`, already compiled |
| `bzip2` | `BZ2_bzBuffToBuffCompress` | yes (`_bzip2_lib.c` includes `compress.c`) |

No new third-party code. The core is pure C++ over `draken/core/buffers.h` (no Python,
as rugo must run without it); the only Python is the edge that unpacks a Morsel.

## 4. Input contract (what the core takes)

The core takes `const DrakenVector*` per column plus a small descriptor
(name, logical unit / scale / precision, ARRAY child vectors) — NOT copied-out plain
buffers as the parquet writer's `ColumnInput` does. Values are read through the uniform
`data[selection[i]]` path (CLAUDE.md §11); a dict- or constant-shaped column is encoded
through the same loop with no shape test. (A dict-shape fast path — compute string
lengths per unique value once, gather per row — is a possible later optimisation and
needs the architect's agreement first; it is not in this plan.)

The edge reads the logical descriptor exactly as the parquet edge does
(`v._nb.logical_type_kind / unit / scale / precision`, `array_child`), fails loud when a
parameterised type arrives without one, and keeps every child Vector alive for the
call.

## 5. Type mapping (DrakenType → Avro)

| DrakenType | Avro type | Notes |
|---|---|---|
| BOOL | `boolean` | from the bit-packed vector |
| INT8 / INT16 / INT32 | `int` | |
| INT64 | `long` | |
| UINT8 / UINT16 | `int` | widened; reads back INT32 |
| UINT32 | `long` | widened; reads back INT64 |
| UINT64 | **refused** (D6) | Avro has no unsigned 64; a value > INT64_MAX written as `long` reads back wrong with no error |
| FLOAT32 / FLOAT64 | `float` / `double` | IEEE little-endian; NaN/Inf allowed |
| DATE32 | `int` + `date` | |
| TIME32 (s, ms) | `int` + `time-millis` | s → ms exact |
| TIME64 (us) | `long` + `time-micros` | |
| TIME64 (ns) | **refused** | Avro has no time-nanos; truncating is silent loss |
| TIMESTAMP64 (s, ms) | `long` + `timestamp-millis` | s → ms exact |
| TIMESTAMP64 (us) | `long` + `timestamp-micros` | |
| TIMESTAMP64 (ns) | **refused** (D7) | `timestamp-nanos` exists in Avro 1.12 but our reader refuses it (reader D8); writing what we cannot read is not offered |
| DECIMAL (p ≤ 18) | `bytes` + `decimal(p, s)` | minimal-length two's-complement big-endian (the Java/fastavro form) |
| DECIMAL128 (p ≤ 38) | `bytes` + `decimal(p, s)` | same |
| VARCHAR / NVARCHAR | `string` | bytes written as held, not UTF-8 validated (symmetric with reader D7) |
| VARBINARY | `bytes` | |
| VARIANT | `string` | JSON text; reads back VARCHAR (as parquet: STRING, no JSON annotation) |
| UUID | `string` + `uuid`, canonical lowercase hyphenated | spec-canonical form; alternative `fixed(16)` is Iceberg's (D5) |
| ARRAY<T> | `array` of the mapped T, items `["null", T]` | nested ARRAY → nested `array`; the reader returns nested arrays as JSON text, a plain-scalar array as ARRAY |
| NULL (typeless all-null) | `["null", "int"]`, every row null | same answer as the parquet writer (architect-approved there); a bare `null` field type is valid Avro but our reader refuses it |
| INTERVAL | **refused** | Avro `duration` is fixed(12) months/days/millis and our reader refuses it |
| VECTOR_FP16 | **refused** | as the parquet writer |
| IPV4 (UINT32 + descriptor) | `long` + field attribute `"opteryx.type": "IPV4"` (D9) | Avro permits unknown attributes; without one the column reads back as INT64 with no error — the exact parquet bug fixed with its key_value side channel |

Timestamps are UTC instants in Avro; a descriptor offset is not representable and is
dropped, as parquet's `isAdjustedToUTC` round trip does today.

**Nullability (D2):** every top-level field is `["null", T]` (null branch first, the
Java/fastavro order), whether or not the column has a validity mask. A Morsel carries no
declared nullability, and the streaming writer fixes its schema on the first batch — a
required field would make a later batch with NULLs unwritable. Cost: one byte per
value (the branch index). ARRAY items likewise `["null", T]`.

**Names (D14):** an Avro name is `[A-Za-z_][A-Za-z0-9_]*`. SQL names such as
`count(*)`, `Rocket.Name`, `Mission Status` are refused naming the column; duplicates
are refused. No sanitising — a renamed column is hidden behaviour, and a file Java
cannot open is a broken file. The root record is `{"type":"record","name":<record_name>,
"namespace":"rugo"}` with `record_name` a parameter (default in D15).

## 6. Encoding design

Avro is row-major; the input is column-major. Per-row-per-column dispatch (a `switch`
on type inside the row loop) is the pattern CLAUDE.md §2/§3 forbids. Instead the block
is encoded **column at a time** with per-row write cursors:

1. **Length pass** (per column, type-specialised loop): the encoded size of each row's
   value — union byte + fixed width, or union byte + `zigzag_len(n) + n` for strings /
   bytes / decimals, computed from `str_length(slot)` without touching payloads;
   `zigzag_len` is `clz`-based, no loop. Accumulate into `row_len[r]`.
2. **Prefix sum** → `row_off[r]`; the block payload size is known exactly; allocate once.
3. **Write pass** (per column, type-specialised loop): write at `cursor[r]`, advance
   `cursor[r]`. Reads stream through one input column; writes land in ascending row
   order inside the block buffer.

ARRAY columns run the same two passes over elements (one array block: `count`, items,
`0`). The whole block is then compressed as one unit, framed `count | size | payload |
sync`. Two passes over the input, zero reallocation, no per-value dispatch.

**Blocks (D4, perf default + measurement):** rows are cut into blocks BEFORE encoding
so blocks are independent work units: bytes/row is estimated from the column widths
plus a sum of string lengths (one pass over slots — cheap), `rows_per_block =
block_bytes / est`, capped at 65,536 rows so the reader's whole-block batching stays
granular. Default `block_bytes` = 1 MiB uncompressed (Java's default is 64 KiB,
fastavro's 16 KiB — both small for columnar readers; to be measured in §9). A block
never spans two `write_batch` calls.

**Parallelism (D13):** blocks are encoded + compressed on a pool (`max_threads`, 0 =
every core, as `read_avro`), results concatenated in order — the JSONL writer's
per-worker-chunk shape. Measured, not assumed (§9).

**Sync marker:** 16 random bytes per file (`std::random_device`), overridable with a
`sync_marker` argument so tests can compare streaming vs one-shot output byte for byte.

**Header metadata:** `avro.schema`, `avro.codec`, and `rugo.created_by` =
`rugo.__writer_id__` (the string the parquet footer already carries; D10).

## 7. API

```python
from rugo import avro

data = avro.write_avro(morsel, codec="deflate", block_bytes=1 << 20,
                       max_threads=0, record_name="record")        # -> bytes

with open("out.avro", "wb") as f:
    with avro.open_avro_writer(f.write, codec="deflate") as w:     # sink: callable taking bytes
        for batch in morsels:
            w.write_batch(batch)                                   # schema fixed by the first batch

n_rows = avro.write_avro_stream(morsels, f.write, codec="zstandard")
```

- `codec` ∈ `null | deflate | snappy | zstandard | bzip2`; anything else → ValueError
  naming it. Default: D3.
- A later batch whose names or types differ from the first → ValueError (no coercion).
- Zero columns → ValueError. Zero rows → a valid header-only file.
- Context manager: a clean exit writes nothing more (Avro has no footer — every block is
  final); an exception leaves the sink as it is. `close()` is idempotent.
- CLI: `.avro` in `_EXT_TO_FORMAT`, so `convert` / `merge` / `split` write Avro. Note
  `detect_format` is shared with the READ verbs, which have no Avro branch today
  (`info`, `schema`, `preview`, … refuse `.avro`): wiring `read_metadata` /
  `open_reader` to `rugo.avro` is a dozen lines and is included unless ruled out (D12).

## 8. Build

- `build_common.py` rugo_native: add `rugo/src/avro/avro_writer.cpp` and
  `third_party/libdeflate/lib/deflate_compress.c` (a C TU; the mixed-extension path
  already compiles libdeflate's C files), headers in the depends list. Not added to
  `setup.py`'s operators extension (§2: no engine write path).
- `rugo_native.pyx`: `include "avro/_avro_writer.pxi"`.
- `rugo/avro/__init__.py`, `rugo/README.md` (§Avro says "rugo has no Avro writer" in
  three places), `dev/generate_avro_bench.py` docstring: update.
- Release: rugo version bump + README table; the `release-rugo.yaml` `avro` floor suite
  already installs fastavro / Apache avro, so the writer tests run there as they are.

## 9. Verification

Tests: `tests/rugo/test_avro_writer.py`, in `make rt`.
- **Oracles:** every file written is read by fastavro AND Apache `avro` (both already
  test dependencies) and compared value for value to the Morsel; and read back by
  `rugo.avro.read_avro` (round trip). Decimal / date / timestamp compared as Python
  `Decimal` / `date` / `datetime`.
- **Types:** one test per row of §5 incl. the refusals (exact messages); NULLs in every
  type; strings ≤ 12 and > 12 bytes (inline vs arena); dict- and constant-shaped input
  vectors; nested ARRAY; the all-null typeless column.
- **Container:** every codec; a block size sweep produces ≥ 2 blocks and the reader's
  `batches_read` sees them; zero rows; `sync_marker` honoured.
- **Streaming:** N batches == one-shot of the combined Morsel byte for byte (fixed
  sync); schema mismatch on batch 2 raises; the sink is called once per block.
- **CLI:** `convert space_missions.parquet → .avro` reads back equal to the parquet.
- **Corpus:** rewrite `testdata/avro/space_missions.avro` through the writer and compare
  with the reader to the original (the file has nested `Rocket`, which reads back as
  JSON text — the rewritten file holds it as a string column; the test states that).
- Writer input is trusted (a Morsel), so no fuzz target; the reader's fuzz corpus
  gains rugo-written files.

Bench: `dev/bench_avro_writers.py` — 1M rows × the bench schema's flat subset,
rugo vs fastavro write time per codec; block-size sweep (64 KiB / 256 KiB / 1 MiB /
4 MiB) on file size and on `read_avro` time, threads 1 vs all. The default in §6 is
whatever that measures best; baseline kept durably (not in the scratchpad —
reader doc §22 trap).

Sign-off: `make q` + `make slt` (no engine change, but the rule is the rule).

## 10. Size and phases

| Part | Lines (approx) |
|---|---|
| Schema JSON emit + name validation | 120 |
| Varint / zigzag encoders, length helpers | 60 |
| Column encoders (length pass + write pass, per type, ARRAY) | 600 |
| Blocks, codecs, header, framing, thread pool | 300 |
| Cython edge + facade + CLI | 300 |
| Tests + bench | 500 |

Phases, each ending green on its tests:
1. Container + `null` codec + schema emit + scalars (bool/int/long/float/double/
   string/bytes) + nullable unions; one-shot `write_avro`; fastavro/Apache/own-reader
   oracles.
2. Logical types (date, time, timestamp, decimal, uuid), NULL column, every refusal.
3. Codecs: deflate (new TU), zstandard, snappy + CRC, bzip2.
4. ARRAY columns (incl. nested).
5. Streaming writer, CLI, README/docs; parallel blocks + the §9 measurements that fix
   the defaults.

## 11. Explicit refusals (summary)

UINT64 · INTERVAL · VECTOR_FP16 · nanosecond TIMESTAMP / TIME · invalid or duplicate
Avro field names · zero columns · schema change between streamed batches · unknown
codec · `xz`.

## 12. Decisions for the architect — RULED 2026-10-16: each recommendation below stands

| # | Question | Recommendation |
|---|---|---|
| D2 | Every field `["null", T]` regardless of validity? | **Yes** — stable across streamed batches; 1 byte/value. Alternative: a `nullable=` parameter (per column or all), required fields when given. |
| D3 | Default codec | **`deflate`** (the spec's required codec; every consumer has it; Confluent S3 sink has no zstandard). Alternative: `zstandard` for speed, consistent with the parquet default. |
| D4 | Block size default | **1 MiB uncompressed, ≤ 65,536 rows**, then set from the §9 sweep — a perf default, not a ruling. |
| D5 | UUID form | **`string` + `uuid`** (spec canonical). Alternative: `fixed(16)` + `uuid` (Iceberg's). |
| D6 | UINT64 | **Refuse.** Alternative: `bytes` `decimal(20, 0)` — exact and universally readable, but reads back DECIMAL128. |
| D7 | Nanosecond timestamps | **Refuse** (symmetric with the reader's refusal of `timestamp-nanos`). Alternative: lift reader D8 and write/read `timestamp-nanos`. |
| D9 | IPV4 side channel | **Write `"opteryx.type": "IPV4"` on the field and have the reader honour it** (small reader change, this work). Alternative: refuse IPV4. Writing a bare `long` is the parquet bug and is not offered. |
| D10 | `rugo.created_by` header entry | **Yes** (parquet precedent). |
| D12 | CLI read verbs gain Avro too | **Yes**, since `.avro` in the extension map reaches them. |
| D13 | Parallel block encoding in v1 | **Yes**, measured; drop it if it does not pay. |
| D14 | Invalid Avro names | **Refuse**, no sanitising. |
| D15 | Root record name | default `record`, namespace `rugo`; `record_name=` parameter. |

## 13. Build log

### Phase 1 — BUILT 2026-10-16

`rugo/src/avro/avro_writer.{hpp,cpp}` (pure C++), `rugo/src/avro/_avro_writer.pxi`
(edge, included in `rugo_native.pyx`), `rugo.avro.write_avro`, `tests/rugo/test_avro_writer.py`
(15 tests; oracles fastavro + Apache `avro` + `rugo.avro.read_avro`).

- Encoding is as §6: per-column length pass → prefix sum → per-column write pass with
  per-row cursors; the encoders are instantiated per type × validity presence
  (`IntEnc<T>`, `FixedEnc<T>`, `BoolEnc`, `StrEnc`), so the row loops carry no
  dispatch. After the write pass every row's cursor is checked against the next row's
  start — a `len`/`put` disagreement is a `logic_error`, never a corrupt block.
- Blocks are cut by cumulative encoded bytes (`block_bytes` floor, `kMaxBlockRows` cap)
  from the exact per-row lengths, not an estimate — the length pass already runs over
  the whole batch, so the estimate in §6 was unnecessary.
- Types: BOOL, INT8/16/32, UINT8/16 → `int`; INT64, UINT32 → `long`; FLOAT32/64;
  VARCHAR/NVARCHAR/VARIANT → `string`; VARBINARY → `bytes`. UINT64, INTERVAL and
  VECTOR_FP16 refused as §5; every logical type, ARRAY and the typeless NULL column
  refused with "does not write yet" until their phase.
- **Deflate pulled forward from phase 3** so the ruled default codec (D3) is real from
  the first build: `third_party/libdeflate/lib/deflate_compress.c` added to the
  rugo_native build (already vendored; nothing new), level 6. snappy / zstandard /
  bzip2 are refused by name ("not written yet").
- Header: `avro.schema`, `avro.codec`, `rugo.created_by` (D10). Sync marker random, or
  the 16 bytes given as `sync_marker` (tests compare two writes byte for byte).
- The `AvroWriter` class already has the streaming shape (`write_batch` × n,
  `take_pending`); phase 5 adds the Python streaming edge over it.
- Dict- and constant-shaped input vectors are written through the uniform path and
  tested (values from `dictionary_encode()`).

### Phase 2 — BUILT 2026-10-16

Logical types, the NULL column and the IPV4 attribute, per §5 and the rulings.

- The writer now chooses an encoder per field from (physical type, logical descriptor)
  once, in `choose()`, and both passes dispatch on it (`Enc`), not on the physical type:
  the engine's `CAST(... AS TIME)` yields INT64 storage with a TIME descriptor, and
  TIME32 / TIME64 / INT32 / INT64 storage are all admitted for TIME.
- DATE32 → `int` + `date`. TIME: unit s / ms → `int` + `time-millis` (s × 1000, int32
  range checked); us → `long` + `time-micros`. TIMESTAMP64: s / ms → `long` +
  `timestamp-millis` (s × 1000, overflow checked); us → `long` + `timestamp-micros`.
  Nanoseconds refused (D7) with "cast it to microseconds first". A TIME / TIMESTAMP /
  DECIMAL physical type arriving WITHOUT its descriptor is refused, not guessed.
- DECIMAL / DECIMAL128 → `bytes` + `decimal(p, s)`, minimal-length two's-complement
  big-endian (`decimal_be<Width>`); the test checks the byte forms of 127 / 128 / −128.
- UUID → `string` + `uuid`, the canonical 36-char lowercase text from
  `draken/core/uuid_text.h` (the one UUID text form).
- Typeless NULL column → `["null", "int"]`, every row the null branch (as parquet).
- IPV4 (UINT32 + descriptor) → `{"type":"long","opteryx.type":"IPV4"}` (D9). Reader
  side (reader doc §30): `Node::opteryx_type`, `OutKind::Ipv4`, opcode `R_IPV4`
  (refuses a value outside 0..4294967295), `draken_vector_own_raw_logical` at the Python
  edge, the descriptor in `NativeAvroScanSource`, `logical_kind` 5 in `column_types()`,
  `opteryx.connectors.avro_io` binds `IPV4`. Verified end to end:
  `SELECT ip FROM READ_AVRO(...)` on a rugo-written file is typed IPV4. The attribute
  on any other type is ignored, as the spec says.
- Observed, outside this work: draken's `to_pylist()` for DECIMAL128 builds Decimals
  through the thread's decimal context, so a value wider than 28 digits is ROUNDED at
  the default precision (`1234567890123456789012345.67890` → `…345.679`). The bytes in
  the vector are exact; only the Python rendering loses digits. fastavro and Apache avro
  do the same. The test widens the context; the draken behaviour is reported, not
  changed here.
- 22 tests in `tests/rugo/test_avro_writer.py`; `make q` green; `tests/rugo/` + the
  engine READ_AVRO tests green (2,855). `make slt` not run.

### Phase 3 — BUILT 2026-10-16

Codecs `snappy`, `zstandard`, `bzip2` (deflate came with phase 1). No new vendoring: all
three compressors were already linked into `rugo_native` (snappy.cc is the whole library;
zstd's compress sources serve the parquet writer; `_bzip2_lib.c` includes `compress.c`).

- snappy: `snappy::RawCompress` + the big-endian CRC32 of the UNCOMPRESSED block, computed
  with the same `rugo::compression::crc32_update` the reader verifies with. A test flips
  one payload byte and shows the reader's CRC / corruption check catches it.
- zstandard: `ZSTD_compressCCtx` at `ZSTD_CLEVEL_DEFAULT` (3), one context per writer,
  one frame per block.
- bzip2: `BZ2_bzBuffToBuffCompress` (block size 9, default work factor), one stream per
  block; libbzip2's BZ_NO_STDIO internal-error hook is now readable through
  `take_bz_internal_error()` (avro_container.hpp) and checked after every call, as the
  reader does. Blocks over the 32-bit length limit are refused.
- Tests: every codec through all three oracles with multi-block files; the raw bytes of a
  bzip2 / zstandard block decompress with the stock Python libraries to the `null`-codec
  block exactly.

Measured once (200,000 rows × INT64 / VARCHAR / FLOAT64 / DATE, Mac, best of 3 writes;
a single point, not a benchmark):

| codec | bytes | write | rugo read | fastavro read |
|---|---|---|---|---|
| null | 10,174,306 | 2.5 ms | 2.0 ms | 276 ms |
| deflate (libdeflate 6) | 1,907,285 | 39.3 ms | 3.0 ms | 277 ms |
| snappy | 3,033,909 | 7.3 ms | 2.6 ms | 275 ms |
| zstandard (3) | 1,450,379 | 8.7 ms | 2.4 ms | 278 ms |
| bzip2 (9) | 1,394,308 | 447 ms | 35.9 ms | 370 ms |

Data point for D3 (default stays `deflate`, ruled for interoperability): on this input
zstandard writes 4.5× faster than deflate and 24% smaller. The read side is a wash
except bzip2.

`make q` green; `tests/rugo/` + engine READ_AVRO tests green (2,862). `make slt` not run.

### Phase 4 — BUILT 2026-10-16

ARRAY columns, nested to any depth.

- `FieldSpec::elements` is the element chain below an ARRAY (the top array's element
  type, then — while that is an ARRAY — its element, down to the scalar leaf);
  `WriteColumn::levels` the matching child vectors, walked at the edge through
  `array_child` / `array_child_type` exactly as the text writers' `_fill_array_levels`.
  `FieldPlan` (enc, leaf, depth) replaces the per-field `Enc` byte.
- Schema: `{"type":"array","items":["null", T]}`, nested as deep as the chain; the leaf is
  chosen with the same `choose_scalar` as a top-level column, so an array of any admitted
  scalar — including the logical types (ARRAY<TIMESTAMP> from `ARRAY_AGG`) — is written.
- Encoding: `ArrEnc<Leaf>` is templated on the leaf encoder; the only per-element branch
  is "is this level the leaf"; the leaf's switch runs once per column (`with_leaf`). An
  array is `count`, items (`["null", T]` each), `0`; an empty array is `0`. Offsets are
  int32 `[length + 1]` on the array vector's `data`, elements addressed through the
  child's own `selection` (uniform access). The array rows and element validity bits are
  the nullability; a NULL-typed leaf writes every element as the null branch.
- Validation before any byte: the batch's child chain must match the schema's (length
  and types), each level's final offset must stay inside its child, string leaves must
  hold their payloads. A column with no child vector (array_child_type None) is admitted
  as `array<["null","int"]>` when every row is null or empty — the parquet writer's
  convention; `vector_array_from_sequence` always builds a typed child, so that path has
  no test fixture and is UNTESTED (the engine may produce such a vector; parquet_writer.pxi
  handles the same case).
- Reader side, as the reader doc §17.2 already says: an array of a plain scalar reads back
  as ARRAY (tested value for value); an array of arrays of a plain scalar reads back as a nested ARRAY (since
  2026-10-25; it was JSON text when this was written); an array of a logical type was REFUSED by the reader when this was written; since
  2026-10-25 it reads back as a JSON array (UUID_AND_NESTED_COLUMNS_DESIGN.md §9).
- 34 tests; `make q` green; `tests/rugo/` + engine READ_AVRO tests green (2,867). `make slt`
  not run.

### Phase 5 — BUILT 2026-10-16

Streaming writer, CLI, parallel block encoding, and the measurements that set the defaults.

**Parallel encoding (D13).** `write_batch` now runs in three steps, and the bytes do not
depend on the thread count (tested for every codec, 1 / 8 / all threads, byte for byte):

1. Length pass by row range: 65,536-row tasks, each running every column's length pass
   over its range into the shared `row_len` (disjoint slices).
2. Block cuts from `row_len`, sequential and cheap; they depend only on the lengths.
3. Each block is encoded and compressed by a worker into its own chunk (`BlockEncoder`:
   block buffer, cursors, compressed scratch, libdeflate / zstd contexts, reused across
   blocks and batches); chunks are appended in file order.

`WriteOptions::max_threads` (Python `max_threads=0` = every core). `run_tasks` claims tasks
in order and, after a failure, rethrows the failure of the LOWEST task index — every lower
task was already claimed and runs to its end, so the error is the one a serial run raises
(tested: two out-of-range values in different blocks; 1, 2, 8 and all threads all name the
first). A batch is atomic: it is validated and fully encoded before any chunk is
published, so a failing batch writes nothing.

**Streaming.** `rugo.avro.open_avro_writer(sink, ...)` → `write_batch(morsel)` per batch;
`write_avro_stream(morsels, sink, ...)` returns the rows written. `AvroWriter::write_batch`
now takes the batch's `FieldSpec`s and refuses one that differs from the first batch's
(count, then name by position, then physical type + Avro type + encoder), naming the
column; nothing reaches the sink and the writer stays usable. The sink gets the header,
then one call per block (§9). Decisions taken here, surfaced for review:

- A writer that never saw a batch raises ValueError on a clean exit, and
  `write_avro_stream` of an empty iterable raises: there is no schema, so there is no file.
  A zero-row first batch writes a valid header-only file.
- If the sink raises, the writer closes: the chunks it missed cannot be replayed, and any
  later block would make a corrupt file. An exception inside the `with` body also closes
  it; the sink keeps a valid file of the complete blocks it received (Avro has no footer).
- **Deviation from §9:** blocks never span two batches (§6), so "N streamed batches == the
  one-shot write of the combined Morsel, byte for byte" holds only for ONE batch. The tests
  assert that (header + one sink call per block, bytes equal to `write_avro`) and that a
  stream of uneven batches — including 1-row and 0-row batches — reads in all three oracles
  as the concatenation.

**CLI (D12).** `.avro` is in the extension map: `info`, `schema`, `columns`, `count`,
`preview`, `diff` read it; `convert`, `merge`, `split` (incl. `--format avro`) write it.
`rugo.avro.read_metadata` is header-only, so the CLI wraps it (`_AvroCliMetadata` in
`rugo/cli/_common.py`): `num_rows` from a read of no columns (block headers only, nothing
decoded), column types from `read_avro_column_types` rendered with draken's
`type_display_name` — the names a Morsel's columns report. Avro reads refuse predicates
(no verb passes any). The footer verbs (`describe`, `stats`, `inspect`) refuse `.avro` as
they refuse CSV/JSONL.

**Measurements** (`dev/bench_avro_writers.py`; 1,000,000 rows × the 20-column bench file
`testdata/avro_bench/events_1000000_null.avro`; Mac, 6 P + 12 E cores, load ~3–4; arms in
alternating processes; results kept in `scratch/avro_writer_ab/`):

- A first baseline, taken while the load average was 8–15, spread 50–230% between rounds
  and was discarded; only its file sizes (deterministic) were used.
- Restructure at ONE thread vs the phase-4 build (frozen copy of its package in
  `scratch/avro_writer_ab/base_pkg`), 9 writes per arm, medians: null 133.6 → 130.1 ms
  (0.974), deflate 1,879 → 1,862 ms (0.991), zstandard 658 → 649 ms (0.986), files
  byte-identical. The first run of this A/B showed null **+9.8%**: each block's output
  vector grew by appends and the final 16-byte sync append reallocated it, copying the
  block a second time. Reserving the exact size once per block removed it (above).
- Threads and block size (write median of 3; read = `read_avro` all cores / 1 thread):

| codec | block | write 1 thr | write 6 thr | write all | bytes | read all | read 1 thr |
|---|---|---|---|---|---|---|---|
| null | 64 KiB | 138 ms | 39 ms | 18 ms | 268,376,807 | 21 ms | 168 ms |
| null | 256 KiB | 124 ms | 28 ms | 15 ms | 268,312,484 | 21 ms | 167 ms |
| null | 1 MiB | 125 ms | 28 ms | 16 ms | 268,296,632 | 20 ms | 166 ms |
| null | 4 MiB | 135 ms | 37 ms | 30 ms | 268,292,473 | 20 ms | 166 ms |
| deflate | 64 KiB | 1,829 ms | 327 ms | 144 ms | 124,922,703 | 39 ms | 409 ms |
| deflate | 256 KiB | 1,885 ms | 339 ms | 152 ms | 122,146,188 | 37 ms | 396 ms |
| deflate | 1 MiB | 1,916 ms | 345 ms | 158 ms | 121,452,728 | 35 ms | 391 ms |
| deflate | 4 MiB | 1,935 ms | 357 ms | 180 ms | 121,271,012 | 36 ms | 389 ms |
| zstandard | 64 KiB | 609 ms | 110 ms | 48 ms | 128,728,782 | 32 ms | 322 ms |
| zstandard | 256 KiB | 638 ms | 114 ms | 50 ms | 124,474,557 | 31 ms | 306 ms |
| zstandard | 1 MiB | 661 ms | 121 ms | 56 ms | 122,849,900 | 30 ms | 305 ms |
| zstandard | 4 MiB | 681 ms | 134 ms | 74 ms | 122,434,092 | 30 ms | 303 ms |

- Parallel pays: at 1 MiB, 6 threads write 4.4× (null) to 5.6× (deflate) faster than one,
  all 18 cores 8× to 12× (ratios past ~6 threads are this Mac's E-cores).
- **Block size: the 1 MiB default stands.** 256 KiB writes 4–12% faster on all cores but
  its files are 0.6% (deflate) to 1.3% (zstandard) larger and read 1–5% slower; 4 MiB
  saves only 0.15–0.35% of size and writes slower on all cores (fewer blocks to share
  out). 64 KiB costs 3–5% in size. Files are written once and read many times.
- Leave-one-out of the parallel LENGTH pass (forced serial, all cores, 12 writes per arm):
  null 24.9 → 45.3 ms (1.82×), deflate 157 → 178 ms (1.13×), zstandard 57 → 77 ms
  (1.35×). It stays.
- For the record: the same rows are 268 MB as a rugo `null`-codec file vs 191 MB as the
  fastavro-written source — every field carries a union byte (`["null", T]`, D2), the
  source's enums are written as strings, and its nested record and map as JSON text (what
  rugo reads them as).

Tests: 47 in `tests/rugo/test_avro_writer.py`, Avro added to `tests/rugo/test_cli.py`;
`make q` green; `tests/rugo/` + engine READ_AVRO tests green; `make slt` green (sign-off).

## 14. Rulings, round 2 (2026-10-16) — faster reads from the write side

Asked which write-side levers make Avro reads faster (Avro has no row groups or column
blocks), the architect ruled:

| # | Ruling | Built as |
|---|---|---|
| D2′ | Plain fields when a column has no nulls (revises D2) | one-shot writes only: a column with no null in the batch is plain `T`, no union byte per value; a stream keeps `["null", T]` (it cannot see later batches — the original D2 reason). Array ITEMS keep their union. The typeless NULL column always has it. |
| D5′ | UUID as fixed(16) (reverses D5) | `{"type":"fixed","name":"uuid","namespace":"rugo","size":16,"logicalType":"uuid"}`, defined once, `"rugo.uuid"` after; the 16 stored bytes, no text |
| D16 | Decimals: a performance decision (→ implementer picks + measures) | fixed at the smallest width for the precision (Java's / Iceberg's rule: smallest n with 10^p − 1 ≤ 2^(8n−1) − 1), named `decimal_<p>_<s>`, defined once. A value whose bits do not fit that width (outside the declared precision) is refused, never truncated. |
| D17 | Low-cardinality strings as enums, read into a Draken dictionary vector; "we can do this on rewrite this file" | one-shot writes (`write_avro(dictionary=True)`, the default, as parquet's): a VARCHAR / NVARCHAR column becomes `enum` `<field>_enum` when every value is a valid Avro name (the spec's symbol rule), ≤ 4,096 distinct values, and the enum (a code per row + symbols in the header) is smaller than the strings; symbols in byte order. Streams never write enums. The READ side needed nothing: rugo and `READ_AVRO` already return an enum as a dictionary-encoded VARCHAR (`is_dict`, one entry per symbol). |
| — | "File stats in the header — I didn't think Avro had stats?" | Correct: the spec has none. The header's metadata map takes any key (as `rugo.created_by` does), so stats would be a rugo-only convention other readers ignore — and only a one-shot write could fill it. Not built. |

**D16 measurement** (the bench file's `price`, DECIMAL(12, 2), 1,000,000 rows, written both
ways by fastavro, read by rugo; 9 rotated rounds, quiet machine):

| encoding | file | decode `price`, 1 thread | skip `price`, 1 thread |
|---|---|---|---|
| bytes (minimal) | 10,830,324 B | 8.58 ms | 7.53 ms |
| fixed(6) | 11,984,052 B | 6.95 ms (−19%) | 5.52 ms (−27%) |

fixed costs 1.15 bytes a row on these values — about 0.4% of the whole 20-column file.

**Interop notes.** fastavro and Apache's Python `avro` 1.12.2 hand back a fixed(16) uuid as
its 16 bytes (Apache warns it ignores `uuid` on fixed, which the 1.12 spec allows; Java
1.12 and Iceberg read it). Both read an enum as its symbol string, a fixed decimal as
`Decimal`.

**Measured: files written by this build vs the signed-off phase-5 build** (each read by its
own build; 1,000,000-row bench file at defaults; arms in alternating processes, 9 reads
per case, quiet machine — the phase-5 arm reproduces the earlier quiet baseline within 2%).
Narrow = `id, amount, country, event`. On this file 17 of 20 columns are written required,
`event` (7 symbols) and `country` (20) become enums, `price` fixed(6).

| codec | projection | threads | phase 5 | now | ratio |
|---|---|---|---|---|---|
| null | all | 1 | 167.5 ms | 128.3 ms | 0.766 |
| null | all | all | 20.9 ms | 17.7 ms | 0.847 |
| null | narrow | 1 | 105.8 ms | 61.8 ms | 0.584 |
| null | narrow | all | 9.5 ms | 4.6 ms | 0.480 |
| deflate | all | 1 | 395.6 ms | 338.9 ms | 0.857 |
| deflate | all | all | 36.5 ms | 32.2 ms | 0.883 |
| deflate | narrow | 1 | 331.7 ms | 270.0 ms | 0.814 |
| deflate | narrow | all | 25.2 ms | 20.2 ms | 0.801 |

Files: null 268,296,632 → 244,734,367 B (0.912), deflate 121,452,728 → 116,808,250 B (0.962).
A first run of this A/B overlapped another session's TPC-H SF100 run (load 12–16) and was
discarded; it pointed the same way (ratios 0.59–0.90).

D2′, D5′ and D17 are rulings, not optional performance parts, so they were not ablated
one by one; D16 (the one performance choice) was measured on its own above.

Signed off 2026-10-16: `make q` green, `tests/rugo/` + engine READ_AVRO tests green (2,902),
`make slt` green (64/64). Uncommitted.

### 14.1 Write cost of round 2 (measured 2026-10-16) — ✅ fixed in §14.2

Write A/B, this build vs the phase-5 build (1,000,000-row bench file, 1 MiB blocks, 9 writes
per arm in alternating processes, quiet machine):

| codec | 1 thread | all cores |
|---|---|---|
| null | 140.6 → 136.8 ms (0.973) | 25.9 → 44.2 ms (1.706) |
| deflate | 1,979 → 1,773 ms (0.896) | 163.9 → 167.5 ms (1.022) |
| zstandard | 681.8 → 625.7 ms (0.918) | 61.0 → 79.4 ms (1.302) |

On one thread round 2 writes faster (fewer bytes to encode and compress). On all cores it
adds ~18–20 ms: the enum analysis (`build_enum`) runs serially, before the parallel
passes. Toggled, not inferred — all cores, `dictionary=True` vs `False`: null 35.5 vs
14.4 ms, zstandard 73.9 vs 54.4 ms. Per column (1M rows, dense input): a column that
BECOMES an enum costs ~10 ms (`event` 10.7, `country` 9.8 — ~10 ns a row: a string hash and
probe per row); a rejected column exits at its first non-name value (≤ 0.3 ms). Writing that
column on all cores takes ~1.3 ms, so the analysis is ~8× the column's own write.

Input shape matters: the analysis memoises by physical value, so a dict-shaped column
(what parquet / the engine usually hand over) hashes each distinct value once and the
per-row work is an array lookup. These measurements used dense input (`Morsel.combine`
densifies).

### 14.2 Enum analysis: slot-keyed and parallel (BUILT 2026-10-16)

Ruled: key short strings by their 16-byte slot in the vendored ankerl flat map, and run
the analysis over row ranges in parallel like the length pass. `build_enum` now takes
`max_threads` (the write's) and runs:

0. a serial precheck of the first 256 non-null values against the symbol rule — the usual
   rejection (a non-name early) still starts no thread;
1. rows by 64k range: mark the physical values non-null rows use (a relaxed `atomic_ref`
   store — rows of a dictionary-shaped column share them) and sum the string sizes;
2. physical values by 64k range: per-range distinct sets — a string of ≤ 12 bytes keyed by
   its 16-byte slot (length + inline bytes, zero-padded: equal slots = equal strings), a
   longer one by content — each new value checked against the symbol rule once, a range
   past 4,096 rejecting the column;
3. serial merge of the per-range sets (cap re-checked globally), size test, symbols sorted;
4. codes by physical range.

The result does not depend on the thread count (tested 1 / 2 / 8 / all, byte for byte).
New tests: long (> 12-byte) symbols across ranges; a non-name past the precheck; a cap
exceeded only across ranges; a dictionary whose unused entry is a non-name.

⛔ **Trap found by the A/B:** ankerl's `mix(a, b)` is a 128-bit multiply of its two words.
A string of ≤ 4 bytes has an all-zero high slot word, so `mix(lo ^ C, hi)` hashed every
2-letter country code to 0 — at one thread `country`'s analysis went 8.6 → 17.5 ms (2×
SLOWER than the std::unordered_map it replaced) while `event` (8-byte words) got faster.
Each word is now xored with its own non-zero constant; `country` 17.5 → 2.4 ms.

Measured (A/B vs the round-2 build frozen in `scratch/avro_writer_ab/r2_base_pkg`;
1,000,000-row bench file; 15 writes per arm in alternating processes; quiet machine):

| | before | after | ratio |
|---|---|---|---|
| analysis, all cores: `event` / `country` | 11.0 / 9.9 ms | 0.22 / 0.68 ms | |
| analysis, 1 thread: `event` / `country` | 5.8 / 8.6 ms | ≈ 0 / 2.4 ms | |
| write null, all cores | 36.5 ms | 15.8 ms | 0.433 |
| write zstandard, all cores | 75.0 ms | 54.5 ms | 0.727 |
| write deflate, all cores | 163.2 ms | 142.5 ms | 0.873 |
| write null, 1 thread | 130.0 ms | 116.5 ms | 0.897 |
| write zstandard, 1 thread | 622.1 ms | 611.1 ms | 0.982 |
| write deflate, 1 thread | 1,763 ms | 1,754 ms | 0.995 |

Against the phase-5 writer (before round 2), the all-core writes are now at or below its
times (null 15.5–25.9 → 15.8 ms, zstandard 56–61 → 54.5 ms, deflate 158–164 → 142.5 ms),
and round 2's files read 12–52% faster (§14).

Signed off 2026-10-16: `make q` green, `tests/rugo/` + engine READ_AVRO tests green (2,906),
`make slt` green (64/64). Uncommitted.

**ClickBench note.** `make clickbench-avro`'s shards (fastavro, dev/clickbench/parquet_to_avro.py)
already write every field required, have no decimal or UUID, and hold text as `bytes`
(VARBINARY in hits_rugo), so no round-2 change applies; a rugo rewrite of one shard has the
same schema and differs only in block size (64,000 B → 1 MiB) and compressor (−7% bytes).

### 14.3 rugo vs fastavro vs Apache `avro`, writing (measured 2026-10-16) — closes §9's comparison

Harness in the rugo.dev repo: `benchmarks/measure_avro_write.py` + `avro_write_one.py` (the
reader benchmark's method: one venv, a fresh process per (writer, codec), 5 reps, output to
an in-memory buffer). Data: the site's 200,000-row, 20-field event rows (`gen_avro.py`),
read by rugo and written once by rugo; that schema and those rows are what all three
write. rugo starts from a Morsel, fastavro and avro from a list of dicts (their input);
building either is not timed. Each at its default block size. rugo from this tree
(`--rugo-src`, Python 3.14; NOT the PyPI 0.5.2, which has no writer), fastavro 1.13.1,
avro 1.12.2. Mac, 6 P + 12 E cores, quiet. Medians:

| codec | rugo (all cores) | rugo (1 thread) | fastavro | avro (Apache) |
|---|---|---|---|---|
| null | 5.2 ms | 22.5 ms | 1,070.7 ms | 7,362.9 ms |
| deflate | 34.2 ms | 357.6 ms | 1,634.3 ms | 8,188.3 ms |
| zstandard | 13.9 ms | 120.3 ms | 1,174.5 ms | 7,683.6 ms |

One thread against fastavro: 48× (null), 4.6× (deflate), 9.8× (zstandard); against avro
327×, 23×, 64×. On every core: 206×, 48×, 85× and 1,416×, 239×, 553×. Sizes (deflate):
rugo 23,353,640 B, fastavro 25,362,648 B, avro 23,849,307 B. Results:
rugo.dev `benchmarks/results/2026-10-16-avro-write.json`.
