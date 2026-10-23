# Reading and writing Vortex files

Status: PROPOSAL — awaiting ratification. Nothing is built.
Date: 2026-10-23
Sources: vortex-data/vortex `develop` (pushed 2026-10-23), Apache-2.0 —
`vortex-file/flatbuffers/vortex-file/footer.fbs`, `vortex-layout/flatbuffers/vortex-layout/layout.fbs`,
`vortex-array/flatbuffers/vortex-array/{array,dtype}.fbs`, `vortex-file/src/{lib,strategy}.rs`,
`vortex-layout/src/layouts/{zoned,dict,chunked,flat,struct_}/mod.rs`,
`vortex-array/src/arrays/{primitive,dict,varbinview}/vtable/mod.rs`, `vortex-btrblocks/src/builder.rs`,
`vortex-ffi/{Cargo.toml,cinclude/vortex.h}`, root `Cargo.toml`; docs.vortex.dev `specs/file-format`,
`specs/dtype-format`, `specs/versioning/editions`, `getting-started/cpp`; ClickHouse PR
ClickHouse/ClickHouse#112950 (open). Container and metadata claims below come from the `.fbs`
files. Vortex has **no written byte-level spec for array encodings**: the Rust source is the
spec. Where I have not read the source, I say so (marked *unverified*).

## 0. How we implement it — decision D0

Vortex is no longer a moving target at the container level. The file format has been
"stable since 0.36.0". Readers keep support for every component in a frozen **edition**
(`core2025.05.0` → `core2026.08.3`, §2.7). Forward compatibility, where an old reader opens a
newer writer's file, is "planned before 1.0" and not implemented today. The project moved to
LF AI & Data (Aug 2025). DuckDB ships read/write (Jan 2026), and ClickBench carries
`datafusion-vortex` and `duckdb-vortex` entries (`scratch/ClickBench-167/`).

There are two ways to get it:

| | (a) Native C++ in rugo | (b) Link the Rust `vortex` crate through `vortex-ffi` |
|---|---|---|
| What we build | A second implementation of the format: FlatBuffers + protobuf metadata, layouts, a fixed set of array decoders and encoders | A C++ shim over `vortex.h` (`vx_data_source`, `vx_scan`, `vx_partition_next`, `Writer`) and Arrow C Data → draken import |
| Dependencies | none new: zstd, lz4, miniz/libdeflate are already vendored | the vortex workspace: arrow-rs 59.2 (`arrow-array`, `-schema`, `-cast`, `-ipc`, …), `object_store`, `async-*`, `flatbuffers`, `prost`, `fastlanes`, `fsst-rs`, … ClickHouse's PR needed **127 vendored crates** |
| Lives in | `rugo/` → both the rugo and opteryx_core wheels | `src/` (opteryx_core only). The rugo wheel is "no Rust" by construction, so standalone rugo gets no Vortex |
| Data reaches the engine as | draken vectors built directly from the encoded buffers. Dict and constant arrive as draken shapes, and bit-packed codes unpack straight into `selection` (COMPRESSAWARE) | Arrow C Data arrays. `vx_array_canonicalize` decompresses everything, so the encodings that are Vortex's reason to exist are gone before we see them |
| Execution ownership | ours: the native scan source claims work on our scheduler | Vortex runs its own runtime (`vx_runtime_set_worker_threads`) inside our execution — §2 of CLAUDE.md says the engine owns scheduling |
| API stability | the file format (frozen editions) | the C API: "treat as entirely unstable" |
| Encoding coverage | what we implement; everything else is refused by name | complete, including future editions |
| Time to correct reads | months (§9) | weeks |

ClickHouse took (b). Their notes are a warning, not a template. Reads started at a
**26.8× slower geomean than their Parquet** on ClickBench and reached ~1.2× after tuning, with
selective queries still 1.5–5.5× slower. Review found float pushdown using total ordering
(`-0.0` and `NaN` rows wrongly pruned), and decode allocations bypassing their memory limit.
Writing is single-threaded.

**Recommendation: (a).** The reason to read Vortex is its encodings: FastLanes bit-packing,
FoR, ALP, FSST, dict, run-end, and 8192-row zone maps. Route (b) throws those away at the
boundary and puts a second scheduler inside ours. Route (a) is the Parquet/Avro precedent.
Its cost is an encoding surface that grows every edition, so §1 bounds it and §9 starts by
measuring which encodings real files actually contain.

If the goal is only "opteryx can open a `.vortex` file someone hands us", that is (b), and
§10 sketches it.

## 1. Scope

**In:**
- rugo: a C++ Vortex **reader** and **writer**. Both are opteryx-free, with no Python on the
  decode or encode path, and take draken vectors in and out. A Python edge provides
  `rugo.vortex.read_vortex` / `write_vortex` / `read_metadata`, and the CLI gets `info` /
  `schema` / `count` / `convert`.
- Reader: every component of the frozen `core` editions that the reference writer's
  default strategy emits (§2.6), confirmed by the P0 census (§9), plus both stats layouts
  (`vortex.stats` for pre-0.84 files and `vortex.zoned` for current ones).
- opteryx: `.vortex` as a dataset format, scanned by a native Source, with zone-map pruning
  done in-scan and file-level statistics feeding the manifest.
- Conformance in both directions: files written by Vortex read correctly in rugo, and files
  rugo writes read correctly in Vortex (§7).

**Out (refused by name, never silently degraded):**
- `vortex.pco`. Pcodec has no C/C++ implementation; it is Rust-only. The default writer
  excludes it, but the "compact" preset uses it.
- `vortex.onpair`, `vortex.variant`, `vortex.parquet.variant`, `vortex.map`: the 2026-08
  editions, revisited on first sighting.
- Every draft edition: `tensor*`, `spatial*`, `json*`, `zstd2026.02.0` (`vortex.zstd_buffers`),
  `preview*`.
- Encryption (`EncryptionSpec` is an empty table today), the proposed WASM-embedded decoders,
  the IPC stream format, and CUDA.
- Union and Variant dtypes, and Map until D7.

## 2. The format, as we would implement it

All integers are little-endian. There are two metadata codecs. **FlatBuffers** carries the
structure: postscript, footer, layout tree, array tree, dtype. **Protobuf** (prost) carries
per-encoding and per-layout metadata blobs and the `ScalarValue` bytes inside statistics.
Both decoders are hand-written and minimal. The precedent is `rugo/src/parquet/thrift.hpp`.

### 2.1 File envelope
```
"VTXF" | segments… | [user metadata segs] | [DType fb] | Layout fb | [Statistics fb] | Footer fb | Postscript fb | u16 version | u16 postscript_len | "VTXF"
```
The postscript is at most 65 528 bytes (`u16::MAX − 8`). One tail read of 64 KiB covers the
8-byte EOF and the postscript, and usually the footer too. That is two round trips worst
case, by design.

### 2.2 Postscript
`Postscript { dtype, layout (required), statistics, footer (required): PostscriptSegment; metadata: [PostscriptMetadata] }`
`PostscriptSegment { offset:u64; length:u32; alignment_exponent:u8; _compression; _encryption }`
User metadata holds at most 16 entries, each keyed by a non-empty UTF-8 key of ≤ 64 bytes. It
is not read by default. The dtype segment is optional: a writer may use `exclude_dtype`, in
which case the reader must be given the dtype. We refuse such files unless the catalog
supplies a dtype.

### 2.3 Footer: the dictionaries everything else indexes into
```
Footer { array_specs:[{id:string}]; layout_specs:[{id:string}]; segment_specs:[SegmentSpec];
         compression_specs:[{scheme:u8}]; encryption_specs:[{}] }
struct SegmentSpec { offset:u64; length:u32; alignment_exponent:u8; _compression:u8; _encryption:u16 }
CompressionScheme: None=0, LZ4=1, ZLib=2, ZStd=3
```
Encoding IDs are **strings** such as `"fastlanes.bitpacked"`, dictionary-encoded per file. On
open, every `array_specs` / `layout_specs` id is resolved against our registry, and **any
unknown id fails the open, naming the id**. Failing at open, not at first decode, is what
makes "refused by name" cheap and honest. The `_compression` field of `SegmentSpec` is
"reserved, not used in the current version". We refuse a file that sets it to non-zero.

### 2.4 Layout tree
`Layout { encoding:u16 → layout_specs; row_count:u64; metadata:[ubyte] (prost); children:[Layout]; segments:[u32] → segment_specs }`

| Layout | Children | What the reader does |
|---|---|---|
| `vortex.struct` | one per field (+ validity) | projection: walk only the projected fields |
| `vortex.chunked` | row partitions | locate the chunks covering a row range (binary search on cumulative `row_count`) |
| `vortex.flat` | none; one segment holding a serialized Array (§2.5) | decode |
| `vortex.dict` | values layout + codes layout. Metadata `DictLayoutMetadata{codes_ptype, is_nullable_codes, all_values_referenced}` | decode the values once per column per file, then the codes per chunk → a draken **dict-shape** vector |
| `vortex.stats` (core2025.05.0) / `vortex.zoned` (core2026.08.0, current default) | data + `zones` table: one row of aggregates per zone (`min`, `max`, `null_count`, `nan_count`, `bounded_min/max`) | prune: evaluate the predicate's falsification against `zones`, then forward surviving row ranges to `data` |

**This is the main structural difference from Parquet.** The default writer splits the root
struct into columns *before* repartitioning (§2.6). Each column is then chunked and coalesced
to its own byte target, so **chunk boundaries do not line up across columns**. There are no
row groups. A scan unit is a *row range*. Each projected column decodes the chunks that
overlap the range and slices them. Zone boundaries (8192 rows) are the only
cross-column-aligned grid, because zoning runs on fixed 8192-row repartitions.

### 2.5 Arrays inside a flat segment
```
Array     { root: ArrayNode; buffers: [Buffer] }
ArrayNode { encoding:u16 → array_specs; metadata:[ubyte] (prost); children:[ArrayNode]; buffers:[u16]; stats:ArrayStats }
struct Buffer { padding:u16; alignment_exponent:u8; compression:u8 (None=0, LZ4=1); length:u32 }
ArrayStats { min, max, sum: [ubyte] (protobuf ScalarValue); min/max_precision (Inexact|Exact); is_sorted; is_strict_sorted; is_constant; null_count; uncompressed_size_in_bytes; nan_count }
```
An array is a tree of encodings. For example, `dict(codes = fastlanes.bitpacked(…), values = vortex.fsst(…))`
is a compressed dictionary whose codes are bit-packed and whose values are FSST strings. Each
node's metadata is a prost message, or empty for `vortex.primitive`. Buffers carry their own
alignment and optional LZ4. *Unverified:* the exact framing of message versus buffers inside
the flat segment (where the flatbuffer sits, and its length prefix). P0 confirms it from
`vortex-array/src/serde.rs`.

`ArrayStats` arrive per node at no extra cost. `is_sorted`, `is_constant` and exact `min`/`max`
map straight onto draken `flags` hints, such as `DRAKEN_DICT_KEYS_SORTED` when a dict's values
node says `is_sorted`. A hint that is absent stays "don't know".

### 2.6 What the reference writer emits by default
From `vortex-file/src/strategy.rs` (`WriteStrategyBuilder::build`), outermost first:
1. A table strategy splits the root struct into columns.
2. Each column is repartitioned to multiples of **8192 rows** (`row_block_size`).
3. `ZonedStrategy` computes the 8192-row zone table, compressed and stored flat.
4. `DictStrategy` dictionary-encodes a column where a probe compression says it pays. The
   values are compressed and stored flat, and the codes continue down the pipeline.
5. Chunks are coalesced to `data_block_target_bytes`. The source comment justifies ~1 MiB
   segments for S3 (Durner et al., VLDB 16).
6. Each chunk is compressed with the BtrBlocks-style cascading compressor. In `Default` mode
   it **excludes Zstd and Pco**. `IntDictScheme` is excluded because step 4 already applies
   dictionaries.
7. Nearby chunks are buffered to 2 MiB, then chunked → flat.

So the encodings a default-written file can contain are the BtrBlocks scheme set minus Zstd
and Pco. The census in P0 replaces this reasoning with counts.

### 2.7 Editions and the v1 reader set

| Edition (min crate) | Components | v1 |
|---|---|---|
| core2025.05.0 (0.36) | `fastlanes.bitpacked`, `fastlanes.for`, `vortex.alp`, `vortex.alprd`, `vortex.bool`, `vortex.bytebool`, `vortex.chunked`, `vortex.constant`, `vortex.datetimeparts`, `vortex.decimal`, `vortex.decimal_byte_parts`, `vortex.dict`, `vortex.ext`, `vortex.fsst`, `vortex.list`, `vortex.null`, `vortex.primitive`, `vortex.runend`, `vortex.sparse`, `vortex.struct`, `vortex.varbin`, `vortex.varbinview`, `vortex.zigzag`. Layouts `chunked`, `dict`, `flat`, `stats`, `struct`. Dtypes `date`, `time`, `timestamp` | **all** |
| core2025.06.0 (0.40) | `vortex.pco`, `vortex.sequence`, `vortex.zstd` | `sequence`, `zstd`. **Refuse `pco`** |
| core2025.10.0 (0.54) | `fastlanes.rle`, `vortex.fixed_size_list`, `vortex.listview`, `vortex.masked` | all |
| core2026.08.0 (0.84) | layout `vortex.zoned`. Aggregates `min`, `max`, `null_count`, `nan_count`, `bounded_min/max` | all (current default files need it) |
| core2026.08.1 / .2 / .3 (0.84–0.85) | `vortex.onpair`, `vortex.map`, `vortex.variant`, `vortex.parquet.variant`, dtype `vortex.uuid` | `uuid` (→ DRAKEN UUID). Refuse the rest |

**Open in the source:** `fastlanes` exports `bitpacked_v2_id`, `for_v2_id`, and
`decimal_byte_parts_v2_id`. The v2 variants carry per-block bit widths and per-chunk
references. They do not appear by those names on the editions page. P0 establishes their id
strings and editions. If current default files use them, they are v1.

### 2.8 Decoding notes per encoding (the real work)
- **fastlanes.bitpacked / for / rle**: FastLanes' "unified transposed layout", 1024-value
  blocks interleaved across lanes. It is designed to auto-vectorise on any SIMD width.
  Bit-packed arrays carry *patches* (exceptions) as a sparse child. The decoder is a
  specialised template per bit width × output type, with NEON and AVX2 verified by
  disassembly. This one decoder decides whether Vortex scans beat our Parquet scans.
- **vortex.dict** codes are a primitive child, often itself bit-packed. They unpack directly
  into the draken `selection` buffer (uint32), and the values become the dict `data`. No
  materialisation.
- **vortex.constant** → draken constant shape. **vortex.sequence** (`a + i·b`) materialises,
  and for `a=0, b=1` it is the identity selection.
- **vortex.runend** and **fastlanes.rle** expand at the scan boundary. RLE does not exist past
  the scan (§11 of CLAUDE.md).
- **vortex.sparse**: fill value plus patch indices and values.
- **vortex.alp / alprd**: floats as scaled integers plus exceptions, or real-double split.
- **vortex.fsst**: a symbol table of ≤ 255 symbols of ≤ 8 bytes, codes into a German-string
  arena. Over a dict's values this happens once per file per column.
- **vortex.varbinview** uses 16-byte Umbra views: `len` plus 12 inline bytes, or `len` +
  prefix + `buffer_index` + `offset`. Draken's `DrakenStringSlot` is the same idea with one
  arena and no `buffer_index`. Single-buffer views copy across as slots. Multi-buffer views
  rebase offsets into one arena.
- **vortex.zigzag, datetimeparts** (days/seconds/subseconds → TIMESTAMP64),
  **decimal_byte_parts** (most-significant part plus lower parts → DECIMAL / DECIMAL128),
  **masked** (validity wrapper), **zstd** (vendored zstd), **bytebool**: small decoders.
- Buffer-level LZ4 (`Buffer.compression = 1`) uses vendored lz4. Segment ZLib/ZStd/LZ4 are
  reserved and refused (§2.3).

## 3. Type mapping (Draken ⇄ Vortex DType) — decision D1

| Vortex | Draken | Notes |
|---|---|---|
| Null | NULL | |
| Bool | BOOL | `bytebool` decodes to bits |
| Primitive I8–I64 / U8–U64 | INT8–INT64 / UINT8–UINT64 | |
| Primitive F16 | — | **refuse** (no scalar fp16 type; VECTOR_FP16 is an embedding type) |
| Primitive F32 / F64 | FLOAT32 / FLOAT64 | |
| Decimal(p ≤ 18, s) / (p ≤ 38) | DECIMAL / DECIMAL128 | p > 38 (i256) → refuse |
| Utf8 | VARCHAR | written as Utf8 |
| Binary | VARBINARY | |
| Ext `vortex.date` (storage I32 days) | DATE32 | |
| Ext `vortex.time` | TIME32 / TIME64 by unit | |
| Ext `vortex.timestamp` (unit, tz) | TIMESTAMP64 | tz semantics follow our existing Parquet `isAdjustedToUTC` handling |
| Ext `vortex.uuid` | UUID | |
| List / FixedSizeList / ListView | ARRAY | D7 |
| Struct (nested, non-root) | — | D7: refuse vs VARIANT (JSON) |
| other Extension ids | — | **refuse by name**. Never read as their storage type |
| Map, Union, Variant | — | refuse |

**D1:** NVARCHAR, INTERVAL and VARIANT have no Vortex dtype. There are two options. Option 1
tags them as `Extension{id:"opteryx.<type>", storage}`. Vortex readers would then refuse the
column, because unknown extensions need a registered plugin. Option 2 refuses them on write.
Recommendation: **refuse on write**. A file only opteryx can read is a skene job, not a Vortex
job.

## 4. Placement, dependencies, build

- `rugo/src/vortex/` (C++17, opteryx-free, draken allocator):
  - `flatbuf.hpp`: a minimal hand-written FlatBuffers reader and builder. It does vtable
    lookup by slot, never by fixed offset, so appended fields stay tolerated. The Nimble
    proposal designs the same file. **D5:** share it (`rugo/src/common/flatbuf.hpp`) if both
    land.
  - `proto.hpp`: a minimal protobuf wire decoder and encoder (varint, zigzag, fixed64,
    length-delimited, unknown-field skip) for prost metadata and `ScalarValue`.
  - `envelope.{hpp,cpp}` (EOF, postscript, footer, registry resolution),
    `dtype.{hpp,cpp}`, `layout.{hpp,cpp}` (tree, row-range planning across unaligned column
    chunks, zone pruning), `array_decode.{hpp,cpp}` plus one TU per encoding family
    (`fastlanes_*.hpp`, `alp.hpp`, `fsst.hpp`, …), `array_encode.{hpp,cpp}`,
    `vortex_reader.{hpp,cpp}`, `vortex_writer.{hpp,cpp}`, `_vortex_column_wrap.cpp` (GIL edge
    only), and `_vortex_reader.pxi` / `_vortex_writer.pxi` included from
    `rugo/src/rugo_native.pyx`, as Avro's are.
  - `rugo/vortex/__init__.py` mirrors `rugo/avro/__init__.py`. The CLI learns `.vortex`.
- Dependencies, all already in `third_party/`: zstd, lz4, miniz/libdeflate. **D4:** CWI's C++
  references for FastLanes, ALP and FSST are MIT-licensed, and vendoring them needs
  agreement. Recommendation: **write our own**. The decoders are small, they must produce
  draken buffers directly, and FastLanes needs our own NEON/AVX2 verification anyway. The
  references serve as test oracles only.
- Build: the sources are added to the shared rugo extension definition in
  `build_common.py`, so both wheels get them identically. Header-only edits need
  `make compile` (the `make c` trap).

## 5. opteryx wiring

Reading:
- `opteryx/models/dataset_format.py`: `".vortex": VORTEX`, with
  `SCAN_READERS[VORTEX] = "Vortex Reader"`. Datasets stay single-format.
- Planning node `VortexReadNode` reads the dtype segment of the first file for the relation
  schema, and translates the projection to struct-layout field paths.
- Execution: `src/cpp/engine/native_vortex_scan_source.hpp`, a sibling of
  `native_avro_scan_source.hpp`, compiled by `_compile_vortex_scan` next to
  `_compile_avro_scan` (`opteryx/managers/execution/compiler.py`). The file is opened once,
  and its footer, layout tree and dtype are parsed and shared.
  - **Unit of work = (file, row range aligned to 8192)**, sized to the morsel cap. Workers
    claim ranges. For each projected column a worker decodes the overlapping chunks and
    slices them (§2.4). Dict values are decoded once per column per file and shared
    read-only.
  - **Zone pruning in-scan:** pushed predicates are evaluated against each column's `zones`
    table before any data segment is fetched. Ranges no zone survives are never read. Float
    zones follow IEEE semantics using `nan_count`. We never trust total-order min/max for
    `NaN` or `-0.0`, which is the bug ClickHouse's review found.
  - The predicate then runs as an in-scan prefilter (Avro §26). COUNT(*) comes from the root
    `row_count` with no data read.
- Manifest: file-level `FileStatistics.field_stats` (per-field min/max/null_count) feed the
  catalog manifest at registration. Unlike Nimble, **foreign Vortex files carry real
  pruning stats**.
- Nothing Python on the per-morsel path. No pure-Python fallback.

Writing: as for Nimble. `data_file_stream` is Parquet-only today. A Vortex sink would write
one file per stream session, and `close()` would return the manifest row. **D6:** catalog
write in v1, or rugo API + CLI `convert` only. Recommendation: API + CLI first.

## 6. Writer design (rugo)

The rule is that we write only frozen `core` components ≤ `core2025.10.0`, so **any Vortex ≥
0.54 reads our files**. We do not emit `vortex.zoned` (0.84) unless D2 accepts that floor. We
write `vortex.stats`, which every reader since 0.36 understands.

- Tree: `struct` → per column `stats` (8192-row zones: min, max, null_count, nan_count) →
  `chunked` → `flat`. Chunks are one per `write()` batch, split at a byte ceiling when the
  batch is large. We do not repartition columns independently in v1. Aligned chunks are
  simpler for us and legal for Vortex.
- Encodings are chosen **statically from the draken shape**. There is no BtrBlocks-style
  sampling cascade:
  - constant shape → `vortex.constant`;
  - dict shape (`draken_is_dict`) → `vortex.dict(codes = primitive at the narrowest
    u8/u16/u32, values = the dense encoding of the value type)`;
  - dense integers and dates → `vortex.primitive`; FoR + bit-packing is added only if a
    measurement says it pays (D2);
  - dense floats → `vortex.primitive`;
  - strings → `vortex.varbinview` (slots map one-to-one, §2.8);
  - bool → `vortex.bool`; nulls → validity child;
  - TIMESTAMP64 → `vortex.ext(timestamp)` over primitive; DECIMAL → `vortex.decimal`.
- File statistics are written (`FileStatistics`), and so are the per-node `ArrayStats` we
  already know from the vector (`is_constant`, `is_sorted` from `flags`, min/max computed as
  the Parquet writer's `compute_stats` does).
- User metadata `writer` = `rugo/<build id>`, the same trust key as the Parquet writer's
  `IsTrustedRugoWriter`.

## 7. Conformance — how we know it is right

Round-tripping our own files proves consistency, not conformance. Unlike Nimble, the oracle
is cheap here. The `vortex-data` Python package (official, 0.89.0 on PyPI, wheels for macOS
arm64, Linux x86_64/aarch64)
can generate and verify fixtures from `dev/` and `tests/` without building Rust. **D3:** add
it as a **dev/test-only** dependency, pinned to one version. It never appears in `opteryx/`,
`rugo/` or `draken/`.
- `dev/vortex_fixtures.py` writes, at the pinned version and with the *default* strategy, every
  dtype in §3; nulls at 0 / 1 / 50 / 99 / 100 %; dict, constant and high-cardinality strings;
  floats with NaN and ±0.0; sorted and unsorted ints; multi-chunk columns with unaligned chunk
  boundaries; one file with the `compact` preset (Pco, which must be refused); an
  `exclude_dtype` file; and an empty file. It also dumps each file's encoding tree as the
  golden metadata. The output is checked in under `tests/data/vortex/vortex_written/`.
- A 0.36-era file (`vortex.stats`) and a ≥ 0.84 file (`vortex.zoned`) are both included,
  so both stats layouts are covered.
- Our writer's output is read back with `vortex-data` in `dev/`, and row digests are
  compared.
- Negative fixtures: a Pco column, an unknown extension id, a draft-edition encoding, a
  truncated tail, and a non-zero `SegmentSpec._compression`. Each must fail naming the
  construct.
- `make q` carries the fixture tests, and `make slt` gets a `vortex` group for sign-off.
- Real-world corpus: the ClickBench `hits.vortex` produced by `scratch/ClickBench-167/*-vortex/load`.

## 8. Risks, stated

1. **The encoding surface grows each edition.** core2026.08.0 → .3 added six components in two
   months. With no forward compatibility, a file from next quarter's default writer may use a
   component we refuse. Our refusal happens at open and names the id. The cost is real, and
   accepting it is part of D0(a).
2. **The Rust source is the spec** for encoding metadata and buffer layout, so every decoder is
   read from source and pinned by fixtures. The v2 FastLanes/decimal ids (§2.7) are the known
   unknown.
3. **Two hand-written metadata codecs** (FlatBuffers and protobuf). Both are small, but both
   must tolerate unknown and appended fields.
4. **Unaligned column chunks.** Row-range assembly across columns costs slicing work that
   Parquet row groups avoid. It could dominate on wide projections. We measure it before
   tuning it.
5. **FastLanes decode speed is the crux.** A scalar decoder would make Vortex slower than our
   Parquet path. A Vortex scan slower than the rugo Parquet mirror of the same data is a
   finding, not something to tune blind.
6. **The writer is deliberately simple.** It produces files that are legal but larger than the
   reference writer's. If the ask is "write files as small as Vortex's own", the static writer
   is undersized and BtrBlocks becomes a separate proposal.

## 9. Phasing

- **P0 census (no engine code):** generate the §7 fixtures and dump the encoding-id
  histogram of `hits.vortex` and the synthetic set. Confirm the flat-segment framing, the v2
  ids, and the prost schemas of every encoding in the histogram. Output: the final v1
  encoding list for ratification.
- **P1 rugo reader:** envelope, dtype, layouts, the §2.7 v1 set, and refusals. Ships
  `read_vortex`, `read_metadata`, and CLI `info` / `schema` / `count`.
- **P2 opteryx scan:** format registry, read node, native Source, zone pruning, manifest
  stats, and the `make slt` group.
- **P3 rugo writer** (§6), with read-back through `vortex-data`. Ships `write_vortex` and
  `convert`.
- **P4 (only after D6):** the catalog write path.

Every phase ends on `make q`, and P1–P3 also on `make slt`. Perf is measured per arch after P2
on ClickBench `hits.vortex`, against the rugo Parquet mirror and the `duckdb-vortex` entry.

## 10. If D0 = (b) FFI instead

The vortex workspace would be vendored at a pinned tag, built by setuptools-rust/cargo as a
static library (`vortex-ffi` exposes `staticlib`), and linked into an opteryx_core extension
in `src/`. That needs Rust 1.95 (`rust-version`). A C++ source would wrap
`vx_data_source` → `vx_scan` (projection and filter as `vx_expression`, row range, limit) →
`vx_partition_next` per worker, with arrays imported through the Arrow C Data Interface into
draken. That import is a new Arrow-C importer: arrow-rs is not PyArrow, but it is
still the Arrow memory model at our boundary. Vortex's runtime threads would be capped to
1 and driven from our workers. Our predicates would have to be translated into Vortex
expressions, carrying the float and temporal semantic gaps ClickHouse hit. Writing would go
through `Writer::open` / `push` / `finish`, giving the reference BtrBlocks compression for
free. Standalone rugo would get nothing. It is faster to the first correct read, with
everything after that outside our control.

## 11. Decisions requested

- **D0** route: native rugo implementation / FFI to the Rust crate. (Recommended: native.)
- **D1** opteryx types with no Vortex dtype (NVARCHAR, INTERVAL, VARIANT): refuse on write
  or tag as an extension. (Recommended: refuse.)
- **D2** writer floor and encodings: ≤ core2025.10.0 with `vortex.stats`, static
  shape-driven encodings, FoR/bit-packing only if measured. (Recommended: yes.)
- **D3** `vortex-data` as a pinned dev/test-only dependency for fixtures and read-back.
  (Recommended: yes.)
- **D4** vendor CWI's FastLanes/ALP/FSST C++ or write our own. (Recommended: write our own,
  and use the references as oracles.)
- **D5** share the hand-written FlatBuffers codec with the Nimble work if both proceed.
- **D6** catalog write path in v1, or rugo API + CLI only. (Recommended: API + CLI.)
- **D7** nested data (List/FixedSizeList/ListView → ARRAY on read; nested Struct refused or
  read as VARIANT). (Recommended: lists in P1, nested struct refused.)
