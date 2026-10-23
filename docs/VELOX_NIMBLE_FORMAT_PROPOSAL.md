# Reading and writing Velox files (Nimble first, DWRF as the alternative)

Status: PROPOSAL — awaiting ratification. Nothing is built.
Date: 2026-10-23
Sources: facebookincubator/nimble `main` (2026-10-23) — `dwio/nimble/tablet/{Footer.fbs,
Postscript.{h,cpp},FileLayout.h,Constants.h,TabletWriter.cpp,Compression.cpp,StripeGroup.h}`,
`dwio/nimble/velox/{Schema.fbs,Stats.fbs,Metadata.fbs,SchemaTypes.h,ChunkedStream.cpp,FieldWriter.cpp}`,
`dwio/nimble/encodings/{common/EncodingPrefix.h,*Encoding.h}`, `dwio/nimble/common/Types.h`;
facebookincubator/velox `main` `velox/dwio/{dwrf/proto/dwrf_proto.proto,nimble/}`.
Every byte-level claim below was read from that source, not from a spec: Nimble has no
written specification and its README says it has "no stability or versioning guarantees".

## 0. What "Velox files" means — decision D0

Velox has no single file format. Its `dwio` tree reads five: `parquet`, `orc`, `text`,
`dwrf` and `nimble`. Two are Velox's own:

| | DWRF | Nimble |
|---|---|---|
| What it is | Meta's fork of ORC (2014→). Protobuf metadata, ORC stripes/streams, RLEv1/v2, flat maps, encryption. | Meta's new format (2023→, ex-"Alpha"). FlatBuffers metadata, per-stream cascading encodings, flat maps. Presto C++ reads it; Velox vendors it in-tree (`velox/dwio/nimble`). |
| Metadata codec | protobuf (`dwrf_proto.proto`) | FlatBuffers (`Footer.fbs`, `Schema.fbs`, …) |
| Zone maps / pruning | per-column file + stripe + row-index min/max/sum, bloom filters | **none** (see §2.6) |
| Types | ORC kinds + `TypeStorage` extensions (DATE, DECIMAL, VARCHAR, CHAR…) | 13 scalars + TimestampMicroNano + containers; **no DATE, DECIMAL, INT128, TIME, UUID** |
| Stability | frozen in practice (10+ years of files) | major version 0, "no guarantees", experimental layouts live in `main` |
| Read by | Velox, Presto (Java+C++), Spark via Gluten; not by stock ORC readers | Velox, Presto C++. Nothing else. |

Parquet is already the interop path to Velox: rugo writes Parquet, Velox/Axiom read it (the
ClickBench `velox/` entry queries a Parquet file through Axiom's Hive connector). So this
work only makes sense as support for a **native** Velox format.

**D0 — ratify one:**
- (a) **Nimble** — the format Velox is being built around; the one the "Velox file" phrase
  most plausibly means; the one with no third-party reader, so ours would be the second
  implementation in existence. This document designs (a).
- (b) **DWRF** — far better interop (stats, Presto Java, Gluten), a format that will not
  move under us, but it is "ORC with Meta's extensions" and the engineering is mostly
  ORC: RLEv2, protobuf, dictionary/direct streams, row indexes. §10 sketches it.
- (c) Both, Nimble first. Nothing in the Nimble design below is reused by DWRF except the
  opteryx wiring (§5) and the test strategy (§7).

Recommendation: (a). If the real goal is "Presto/Velox can read opteryx tables with
pruning", say so — that is (b), and I would not build (a).

## 1. Scope

**In:**
- rugo: a C++ Nimble **reader** and **writer**, opteryx-free, no Python on the decode or
  encode path, draken vectors in and out. Python edge for `rugo.nimble.read_nimble` /
  `write_nimble` / `read_metadata`, CLI `info` / `convert`.
- opteryx: `.nimble` as a dataset format (scan) with a native Source, and Nimble as a
  writer target where a dataset's format says so.
- Files written by Velox/Nimble's default writer must read; files we write must read in
  Velox (verified, §7).

**Out:**
- FlatMap, SlidingWindowMap, ArrayWithOffsets (deduplicated arrays), nested Row/Map
  columns beyond what §3 says. Refused by name, never silently flattened.
- Experimental encodings (type ≥ 12), the stream-major stripe-group layout, varint row
  counts, `MetaInternal`/`OpenZL` compression. Refused by name.
- Nimble's optional index sections (`columnar.indexes`: cluster/sorted/hash indexes),
  `columnar.chunk.stats`, `columnar.features`, `columnar.shared_dictionaries`. Skipped on
  read (they are self-contained optional sections); never written.
- Encryption: Nimble has none. (DWRF does; out of scope for (b) too.)

## 2. The format, as we would implement it

All integers little-endian.

### 2.1 Postscript — last 20 bytes
```
u32 footer_size | u8 footer_compression | u8 checksum_type | u64 checksum | u16 major | u16 minor | u16 magic
```
magic `0xA1FA`; current writer major 0 minor 1; reader rejects major > 0. `checksum_type`
0 = XXH3_64 (only value). The checksum covers **every byte from file start through the
first 5 bytes of the postscript** (footer_size + footer_compression). We verify it on
open (file-level, so one pass over the bytes we already hold) — D4 decides whether that is
default-on.

### 2.2 Footer — FlatBuffer `Footer` immediately before the postscript
```
Footer { row_count:u64; stripes:MetadataSection; stripe_groups:[MetadataSection]; optional_sections:OptionalMetadataSections }
MetadataSection { offset:u64; size:u32; compression_type:u8; uncompressed_size:u32 }
OptionalMetadataSections { names:[string]; offsets:[u64]; sizes:[u32]; compression_types:[u8]; uncompressed_sizes:[u32] }
```
Every metadata blob (footer, stripes, stripe groups, optional sections) is either raw or
`Zstd` (the reference writer compresses above 64 kB). Zstd payloads carry a
**u32 uncompressed-size prefix** before the frame (`ZstdCompression::compress`).

### 2.3 Stripes and stripe groups
```
Stripes     { row_counts:[u32]; offsets:[u64]; sizes:[u32]; group_indices:[u32] }
StripeGroup { stripe_count:u32; stream_offsets:[u32]; stream_sizes:[u32] }   // stripe-major, stripe_count × stream_count, padded with 0
```
A stripe is the independent unit (like a Parquet row group): its streams are contiguous
at `offsets[i]`, and `StripeGroup` gives each stream's offset (relative to stripe start)
and size. A stream absent from a stripe has size 0. A stripe group blob whose FlatBuffer
identifier is `"SGL1"` is the experimental stream-major layout → refuse.

### 2.4 Schema — optional section `columnar.schema`, FlatBuffer `Schema`
DFS list of `SchemaNode { kind:u8; children:u32; name:string; offset:u32; attributes:[StringPair] }`.
`offset` is the **stream id** (index into `StripeGroup.stream_offsets`). Kinds we accept:
Int8…UInt64, Float, Double, Bool, String, Binary, TimestampMicroNano (= two streams: micros
int64 nullable + nanos uint16), Row (its own nulls stream, omitted when no nulls), Array
(lengths stream + child). `attributes` is a per-node string bag "for cross-format per-column
metadata" — §3 uses it. Root is always a Row.

Other optional sections: `columnar.metadata` (`Metadata{entries:[{key,value}]}`, free-form
user KV — we write `writer=rugo/<id>`), `columnar.stats` (`Stats{raw_size:u64}` — that is
all it holds).

### 2.5 Streams, chunks, encodings
A stream is a sequence of **chunks**: `u32 length | u8 compression_type | payload`.
`compression_type` applies to the whole chunk payload (`Uncompressed` or `Zstd` with the
size prefix; `Lz4`, `MetaInternal`, `OpenZL` → refuse). The default Velox writer writes
`Uncompressed` chunks (`chunkCompression.type = Uncompressed`) and chunks a stream when
its raw size crosses 512 KiB–20 MiB, so **multi-chunk streams are the normal case**, not an
edge case.

A chunk payload is one **encoding tree**. Every encoding starts with the 6-byte prefix
`u8 encoding_type | u8 data_type | u32 row_count` (varint row count is an opt-in
experimental writer flag that is **not detectable from the file** → we read fixed only;
an unparseable tree is a hard error naming the stream). Layouts we implement:

| type | layout after the prefix | decode target |
|---|---|---|
| 0 Trivial numeric | `u8 compression` + `row_count × sizeof(T)` | own/copy into dense draken buffer |
| 0 Trivial string | `u8 compression` + `u32 blob_offset` + FixedBitWidth **lengths child** + character blob | offsets by prefix sum; blob owned |
| 0 Trivial bool | `u8 compression` + bitmap | draken BOOL |
| 1 RLE | `u32 runs_size` + runs child (lengths) + run values (child / inline) | **expanded** — RLE does not exist past the scan boundary (CLAUDE.md §11) |
| 2 Dictionary | `u32 alphabet_size` + alphabet child + indices child | draken **dict shape** (alphabet = data, indices = selection) — the COMPRESSAWARE directive, no expansion |
| 3 FixedBitWidth | `u8 compression` + `T baseline` + `u8 bit_width` + packed | unpack + baseline → dense |
| 4 Sentinel | sentinel value + child | child decode, then validity where value == sentinel |
| 5 Nullable | `u32 child_size` + non-null child + bool child (nulls) | validity bitmap from the bool child; values scattered by the uniform `data[selection[i]]` path |
| 6 SparseBool | indices of set/unset bits | BOOL |
| 7 Varint | varints | dense |
| 8 Delta | deltas | dense |
| 9 Constant | one value | draken **constant shape** |
| 10 MainlyConstant | bool "is common" child + other-values child | dense (blend) |
| 11 Prefix | prefix-compressed sorted strings | strings |
| ≥ 12 | ALP, PFOR, SimdForBitpack, BlockBitPacking, SubIntSplit, FrequencyPartition, FOR, Fsst, Huffman, DeltaBlock, SharedDictionary | **refused by name** |

Children recurse (cascading): e.g. Nullable(Dictionary(Trivial, FixedBitWidth)). The
decode is a small recursive-descent interpreter over the prefix bytes, with the leaf
materialisers above; depth is bounded (we cap at 4 and refuse deeper).

### 2.6 What Nimble does not have
**No column statistics.** Not at file, stripe or chunk level: no min/max, no NDV, no
null counts (except the opt-in chunk index, which has null counts only). Consequences:
- Footer-based pruning is impossible for files we did not write. Our manifest stats for
  a Nimble dataset come from (i) our writer computing them at write time, exactly as the
  rugo Parquet writer does for the opteryx manifest, or (ii) a full decode at manifest
  build for foreign files. D5.
- Predicates push into the Source as an in-scan prefilter (the Avro §26 pattern), never as
  a skip.

## 3. Type mapping (Draken ⇄ Nimble) — decision D1

| Draken | Nimble kind | notes |
|---|---|---|
| INT8/16/32/64, UINT8/16/32/64 | Int8…UInt64 | exact |
| FLOAT32 / FLOAT64 | Float / Double | exact |
| BOOL | Bool | exact |
| VARCHAR / NVARCHAR | String | NVARCHAR flag lost unless tagged (below) |
| VARBINARY / UUID | Binary | UUID lost unless tagged |
| TIMESTAMP64 | TimestampMicroNano | µs exact; ns: micros + nanos uint16 (exact); s/ms exact (×1000); tz offset lost unless tagged |
| DATE32 | — | no DATE kind. Int32 + tag, or refuse |
| DECIMAL (p ≤ 18) | — | Int64 + tag `{precision,scale}`, or refuse |
| DECIMAL128 | — | no 128-bit scalar; **refuse** |
| TIME32/TIME64, INTERVAL | — | Int32/Int64 + tag, or refuse |
| VARIANT | String + tag | JSON text |
| ARRAY<scalar> | Array(child) | exact |
| VECTOR_FP16, NULL | — | refuse (NULL: write as Nullable Int8 all-null + tag, or refuse) |
| Nimble Row/Map/FlatMap/SlidingWindowMap/ArrayWithOffsets (reading) | — | refuse by name in v1 (Avro precedent would be nested→JSON VARIANT; D2) |

"Tag" = a `SchemaNode.attributes` pair, e.g. `("opteryx.logical", "DATE")`,
`("opteryx.logical", "DECIMAL(12,2)")`, `("opteryx.tz_minutes", "60")`. Velox ignores the
bag and sees the physical integer; **we** round-trip. Readers of a Velox-written file get
physical types only.

**D1 — ratify one:** (a) tag-and-write for DATE/DECIMAL≤18/TIME/INTERVAL/UUID/NVARCHAR/tz,
refuse DECIMAL128/VECTOR_FP16 (recommended: it is what our Parquet writer does with
IPV4 — "the descriptor lives on the schema"); or (b) refuse every type Nimble cannot
express natively, so a Nimble file never carries meaning only opteryx can read.

**D2:** nested Row/Map on read — refuse (recommended for v1) or JSON-as-VARIANT (Avro's
rule, costs a JSON serialiser in the hot path).

## 4. Placement, dependencies, build

- `rugo/src/nimble/` (C++17, opteryx-free, draken allocator):
  - `flatbuf.hpp` — a **hand-written minimal FlatBuffers reader + builder**: root offset,
    vtable lookup, scalar/vector/string/table-vector accessors, a builder for the six tables
    we emit. The precedent is Parquet's Thrift: `rugo/src/parquet/thrift.hpp` (259 lines)
    and `_thrift_writer.hpp` (188). No `flatc`, no generated code, **no vendored
    FlatBuffers library** (§4 of CLAUDE.md — vendoring only by agreement; there is nothing
    to vendor for the few tables we touch).
  - `postscript.hpp`, `footer.{hpp,cpp}` (Footer/Stripes/StripeGroup/optional sections,
    metadata decompression), `schema.{hpp,cpp}` (DFS tree ⇄ ColumnType-ish descriptors,
    attributes), `chunk.hpp` (chunk walker), `encodings_decode.{hpp,cpp}` (§2.5 table),
    `encodings_encode.{hpp,cpp}` (writer side, §6), `nimble_reader.{hpp,cpp}`,
    `nimble_writer.{hpp,cpp}`, `_nimble_column_wrap.cpp` (buffers → draken Vector, GIL
    edge only), `_nimble_reader.pxi` / `_nimble_writer.pxi` included from
    `rugo/src/rugo_native.pyx` as Avro's are.
  - `rugo/nimble/__init__.py`: `read_nimble(source, columns=None)`, `read_metadata(source)`,
    `write_nimble(morsel, ...) -> bytes`, `open_nimble_writer(...)` (streaming, one stripe
    per `write`), mirroring `rugo/avro/__init__.py`. CLI: `info`, `schema`, `count`,
    `preview`, `convert` learn `.nimble`.
- Dependencies, all already vendored: **zstd** (chunks + metadata), **xxHash**
  (`third_party/cyan4973`, XXH3_64 streaming for the checksum), **lz4** (listed in the
  enum; refused in v1 anyway). Zero new third-party code.
- Build: sources added to the shared rugo extension definition in `build_common.py`, so
  the standalone `rugo` wheel and `opteryx_core` get it identically. Header-only edits
  need `make compile` (the `make c` trap).

## 5. opteryx wiring

Reading (catalog / filesystem datasets):
- `opteryx/models/dataset_format.py`: `".nimble": NIMBLE`, `SCAN_READERS[NIMBLE] = "Nimble Reader"`.
  Datasets stay single-format by decree.
- Planning node `NimbleReadNode` (sibling of the skene/avro read nodes): resolves files,
  reads the schema section of the first file for the relation schema (`header_schema`
  pattern of `opteryx/connectors/avro_io`), translates the projection to **stream ids**.
- Execution: `src/cpp/engine/native_nimble_scan_source.hpp`, sibling of
  `native_avro_scan_source.hpp`. Unit of work = **(file, stripe)**; one open per file
  (footer, stripes, groups, schema parsed once, shared); each worker claims stripes,
  decodes only the projected streams' chunks, emits one morsel per stripe (stripes larger
  than the morsel cap split by chunk boundaries or by row range — chunk row counts are in
  every prefix, so a row-range split is a cheap skip). Projection-only pushdown; predicate
  as in-scan prefilter (Avro §26). COUNT(*) from `Stripes.row_counts` without touching data.
  Compiled by a `_compile_nimble_scan` branch next to `_compile_avro_scan`.
- Nothing Python on the per-morsel path. Pure-Python fallback: none.

Writing:
- Today `data_file_stream` is Parquet-only by construction (one row group per batch of the
  open file). Nimble gets the same shape: one **stripe** per batch, one file per stream
  session, `close()` returns the native file row (path, rows, bytes, stats) the manifest
  needs. Which writer a sink opens is decided by the relation's declared format. **D3:**
  is Nimble a writable catalog format in v1, or write-only via `rugo.nimble` / CLI
  `convert` until a consumer asks? Recommended: v1 = rugo API + CLI; catalog write in a
  second step once the reader has been proven against Velox output.

## 6. Writer design (rugo)

- File = N stripes, **one stripe group** (kRaw), optional sections `columnar.schema`,
  `columnar.metadata`; no stats/index sections. Metadata blobs zstd above 64 kB (the
  reference writer's `kMetadataCompressionThreshold`), else raw. The reference writer also
  closes a stripe group once its offsets/sizes tables pass 8 MB (`kMetadataFlushThreshold`);
  we write one group and refuse to exceed that size (a file that large is a `rows_per_stripe`
  misconfiguration, reported as such). Checksum XXH3_64 streamed over everything written.
- Stripe = one `write()` call (one morsel / batch). Default from the sink is our row-group
  default; the reference writer flushes at 256 MiB raw — a knob (`rows_per_stripe`), never
  a heuristic.
- One chunk per stream per stripe unless the stream's raw size exceeds 20 MiB, then split
  at that boundary (so Velox's readers see what their own writer produces).
- Encoding selection is **static on the draken shape**, not a sampling policy:
  - constant shape → `Constant`;
  - dict shape (`draken_is_dict`) → `Dictionary(alphabet = Trivial, indices = FixedBitWidth)`;
  - dense → `Trivial` (numeric: raw; string: FixedBitWidth lengths + blob; bool: bitmap);
  - any nulls → wrapped in `Nullable(values, Trivial bool nulls)`;
  - chunk compression `Zstd` level 1 for string blobs and numerics over a size floor,
    `Uncompressed` below it — a measured floor, set in the first benchmark pass.
  Row node nulls stream omitted (we never write a null top-level row). No RLE/Delta/
  Varint on write in v1: they buy little for the shapes our vectors already carry and each
  is a second encoder to maintain. If measurement says a `FixedBitWidth` pass over narrow
  dense ints wins, it is added then.
- Timestamps: TIMESTAMP64 → micros (int64 Nullable) + nanos (uint16) as `FieldWriter.cpp`
  does; the nanos stream is all-zero for s/ms/µs sources (a `Constant` chunk, 1 value).
- Manifest stats (min/max/NDV/null count) computed from the vectors at write time, as the
  Parquet writer's `compute_stats` does — the file itself cannot carry them (§2.6).

## 7. Conformance — how we know it is right

Round-tripping our own files proves consistency, not conformance ("green but fake").
Conformance needs files **Velox wrote** and Velox reading files **we wrote**.
- No local Velox: building Nimble needs clang 15/16 and ~4 GB per TU; `pyvelox` on PyPI is
  an x86-only alpha with no evidence of Nimble bindings.
- Proposal: a one-off build of Nimble's tools (`NimbleDump`, `NimbleDsl` — both in
  `dwio/nimble/tools/`) plus a tiny C++ `nimble_roundtrip` on the x86 box. From it we
  check in, under `tests/data/nimble/velox_written/`: every scalar kind, nulls at every
  density, strings dense/dict/constant, timestamps µs/ns, arrays, multi-stripe,
  multi-chunk streams, a zstd-compressed footer, an all-null column, an empty file, plus
  the `NimbleDump` text of each as the golden metadata. Our writer's output is read back
  there with `VeloxReader` and the row digests compared. The box is used for generation
  and verification only; the test suite runs on the committed fixtures.
- Negative fixtures: a stream-major stripe group, an ALP chunk, an `OpenZL` chunk, a
  FlatMap column, a corrupted checksum — each must fail naming the construct.
- `make q` carries the fixture tests; `make slt` gets a `nimble` group mirroring the
  skene/parquet one for sign-off.

## 8. Risks, stated

1. **Moving target.** Major 0, "no guarantees", experimental layouts and 11 experimental
   encodings in `main`, Meta's internal writer may default to encodings we refuse. Our
   refusals are loud and name the construct; the cost is that a Nimble file from next
   year's Velox may refuse to open. Accepting this is part of D0(a).
2. **No pruning for foreign files** (§2.6). A Nimble dataset not written by us scans
   everything unless the manifest build decodes it.
3. **Type gaps.** DECIMAL128 cannot be written at all; DATE/DECIMAL/TIME/UUID only survive
   through our attribute tags (D1).
4. **Nested data** is Nimble's reason to exist (wide ML tables, flat maps). v1 refuses it;
   if the consumer wants flat maps this proposal is undersized.
5. **FlatBuffers by hand.** Small, but the reader must tolerate unknown fields and
   appended fields (the format relies on vtable-based forward compatibility; our reader
   must look fields up by vtable slot, never by fixed offset).

## 9. Phasing

- P1 rugo reader: postscript/footer/schema, Trivial/Nullable/Dictionary/Constant/
  FixedBitWidth/RLE, scalars + strings + timestamps + arrays, multi-chunk; fixtures from
  §7; `read_nimble`, CLI `info`. Refusals for everything in §1 "Out".
- P2 rugo writer (§6) + Velox read-back verification; `write_nimble`, `convert`.
- P3 opteryx scan: format registry, read node, native Source, compiler branch, `make slt`
  group.
- P4 (only after D3): catalog write path, manifest stats at write.
- Remaining legacy encodings (Sentinel, SparseBool, Varint, Delta, MainlyConstant, Prefix)
  land in P1 only if the Velox-written fixtures contain them; otherwise refused and added
  on first sighting.

Each phase ends on `make q`; P2 and P3 on `make slt`. Perf is measured per arch after P3
against the rugo Parquet mirror of the same data; a Nimble scan slower than Parquet is a
finding, not a reason to tune blind.

## 10. If D0 = DWRF instead

Same wiring (§5, §7) but a different core: protobuf metadata (hand-written reader as
for Thrift), PostScript (< 255 bytes, compression kind, `compressionBlockSize`), Footer
(types tree, stripes, file statistics, `rowIndexStride`), StripeFooter (streams by kind:
PRESENT/DATA/LENGTH/DICTIONARY_DATA/…; encodings DIRECT/DICTIONARY/_V2/MAP_FLAT), ORC
RLEv1 + RLEv2 integer coders, byte-RLE for PRESENT/bools, compression blocks
(ZLIB/SNAPPY/LZ4/ZSTD; LZO refused), per-stripe and row-index min/max → real pruning into
the manifest. Richer type surface (DATE/DECIMAL via `TypeStorage`). Roughly ORC-reader
effort plus Meta's deltas; we would still refuse encryption and flat maps in v1.

## 11. Decisions requested

- **D0** target: Nimble / DWRF / both. (Recommended: Nimble, unless the goal is Presto
  interop with pruning.)
- **D1** logical types Nimble lacks: tag-and-write vs refuse. (Recommended: tag.)
- **D2** nested Row/Map on read: refuse vs JSON VARIANT. (Recommended: refuse in v1.)
- **D3** catalog write path in v1, or rugo API + CLI only. (Recommended: API + CLI.)
- **D4** verify the XXH3 file checksum on every open: yes (one pass over bytes already in
  memory) or off by default. (Recommended: yes.)
- **D5** foreign Nimble files in a catalog: no stats (scan everything) vs decode at
  manifest build. (Recommended: no stats, stated in the manifest.)
- **D6** the x86 box as the one-off Velox build host for §7 fixtures.
