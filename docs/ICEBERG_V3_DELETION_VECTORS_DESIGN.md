# Iceberg v3 deletion vectors (Puffin) — read support

Status: DESIGN — D0, D1, D1b ruled 2026-10-23 (support it; our own Puffin reader; a full Puffin file reader with the DV decoder on top). D2–D8 open. Nothing is built.
Date: 2026-10-23
Repos: `opteryx-iceberg` (planning), `rugo` (native decoder), `opteryx-core` (handoff — only if D3/D4 say so)
Spec: Iceberg table spec (`format/spec.md`, "Deletion Vectors", "Scan Planning"), Puffin spec
(`format/puffin-spec.md`, `deletion-vector-v1`), RoaringFormatSpec (portable 32-bit layout,
64-bit extension). Spec text quoted below was read from apache/iceberg `main` on 2026-10-23.

---

## 0. Precondition — is there a consumer? (decision D0)

Today a v3 table that has had a merge-on-read DELETE/UPDATE/MERGE (Trino always; Spark when
`write.delete.mode=merge-on-read`) is **refused, loudly**, by
`opteryx-iceberg`. This doc turns that refusal into a correct read.

**D0 — rule one:**
- (a) Reading v3 merge-on-read tables is a goal now → build this doc.
- (b) No v3 table we need to read has deletes → do not build; the refusal stands and is correct.

**RULED 2026-10-23: (a), support it.** Still open: whether DVs ship alone or as part of a v3-read programme (below).

**Deletion vectors alone do not make v3 tables readable** (see §9, found while reading).
D0 also decides whether DVs ship on their own, with every other v3 feature refused loudly, or
as one piece of a "v3 read" programme. My recommendation: DVs on their own, *plus* a
format-version check that refuses v3 features we don't handle (§9). Without that check, a v3
table that has no deletes is read today with no gate at all.

---

## 1. Scope

**In:**
- Planning: recognise DV entries in v3 delete manifests and match them to data files using the
  spec's rules (§3).
- Our own Puffin file reader in rugo (C++, no Python on the decode path): footer, blob directory,
  compressed footer and blobs (§4.1). The `deletion-vector-v1` decoder sits on top of it (§4).
- Handing the decoded positions to the existing merge-on-read machinery in core, which already
  subtracts per-file deleted ordinals natively (`src/cpp/engine/delete_admission.hpp`).

**Out:**
- **v2 position-delete *files*.** These stay refused when they apply to a data file that has no DV (D5).
- **Equality deletes.** These stay refused, as they are today.
- **Writing Puffin**, whether DVs or statistics. opteryx-iceberg is read-only.
- **Theta-sketch statistics blobs** (`apache-datasketches-theta-v1`). They are a separate track and
  need no decoder; see the conversation that produced this doc.
- **v4 colocated DVs** (`data_file.deletion_vector`, Parquet leaf manifests). v4 is still in
  development upstream. It reuses the same Puffin blob, addressed by the same (path, offset, length),
  so the decoder in §4 carries over unchanged. D3 decides whether the manifest handoff carries over too.

---

## 2. What exists today

| Piece | Where | State |
|---|---|---|
| Delete-manifest planning | `opteryx-iceberg/opteryx_iceberg/manifests.py` | Reads delete entries (`content` 1/2). `_DeleteIndex` (`:229`) matches position deletes by `file_path` bounds or by partition. Any match → `NotImplementedError` (`:350`). Equality → refused (`:337`). |
| DVs specifically | same | **Not recognised as DVs.** A DV is `content=1` with `file_format="puffin"`. Today it carries no `file_path` bounds, so it lands in the partition bucket and refuses every data file in its partition that has seq ≤ the DV's. Conservative, loud, correct. `referenced_data_file`, `content_offset` and `content_size_in_bytes` are not read at all. |
| opteryx manifest | `IcebergDataset._manifest_entries` (`dataset.py:331`) | Emits no `delete_file_path` / `deleted_record_count`, so core sees no deletes. |
| Core MOR handoff | `opteryx_connector.py:942` | At binding: `native.unresolved_deletes()` → `self.table.delete_vectors_for(pending)` → `native.resolve_deletes(...)`. Fail-closed both sides. The decoded manifest (deletes included) is cached per manifest list, so once per snapshot per process. |
| `Dataset.delete_vectors_for` | `opteryx_catalog/catalog/metastore.py:124` | Contract: `{data_file_path: sorted ordinals}`. **IcebergDataset does not implement it** (base raises `NotImplementedError`). |
| Native subtraction | `delete_admission.hpp`, `_arm_delete_admission` (`compiler.py:5038`), `DeleteAdmissionHandle` (`operators/parquet_read/delete_admission.pyx:23`) | Builds a per-row-group mask from ascending **uint32** ordinals. A group with no deletes stays on the undisturbed path; a fully deleted group is never submitted. An ordinal ≥ the file's row count raises. |
| Stats safety | `statistics_only_response.py:711`, `limit_files_pruning.py:103`, `group_limit_key_bound.py:267` | Each one already backs off when `manifest.has_deletes()`. Min/max bounds stay physical, a superset of the live rows, so pruning stays correct. |
| CRC-32 (IEEE) | `rugo/src/compression/stream_decompress.hpp:162` `crc32_update` | Exists. ARMv8 CRC instructions where available. Reused, not re-written. |
| Roaring | `src/cpp/engine/native_roaring32.hpp` | Aggregate state only. Deliberately has **no** serialisation format. Not reused (different job, wrong repo: rugo must not depend on core). |

So the engine half of the job exists already. The missing pieces are (1) planning that knows what
a DV is, (2) a decoder, and (3) a way to get Puffin coordinates and positions across the
`delete_vectors_for` boundary.

---

## 3. Planning — `opteryx-iceberg/opteryx_iceberg/manifests.py`

### 3.1 Read the v3 fields

Add these to the delete-entry columns: `data_file.file_format` (101),
`data_file.referenced_data_file` (143), `data_file.content_offset` (144) and
`data_file.content_size_in_bytes` (145). In a v2 manifest they get null defaults through
`_with_defaults`, the same way `content` and `sequence_number` are already defaulted for v1.
Data manifests don't need them.

### 3.2 Classify `content = 1` entries

- `file_format == "puffin"` → **DV**. `referenced_data_file`, `content_offset` and
  `content_size_in_bytes` are spec-required for a DV; any of them null → raise (malformed
  table, named).
- otherwise → **position delete file**: the existing `_DeleteIndex` path, unchanged.

### 3.3 Match DVs to data files — spec rules, literally

From spec "Scan Planning", a DV in a delete manifest applies to a data file when **all** hold:
- data file `file_path` == DV `referenced_data_file`
- data file data sequence number **≤** DV data sequence number (equal = same commit, applies)
- data file partition (spec id and values) == DV partition

Also: "*when a DV is present, readers can safely ignore matching position delete files*." So a data
file with an applying DV skips the position-delete check. A data file without one keeps today's
refusal (D5).

Index: `dict[referenced_data_file → list[DV entry]]`. This is an exact-path lookup with no bounds
comparison, which also makes it cheaper than the current position-delete path.

Anomalies (D7):
- More than one DV applies to one data file → the spec says "at most one deletion vector per data
  file in a snapshot". Recommend **raise**.
- Path matches but the partition differs, or seq is lower → spec-literal means "does not apply".
  A data file has one partition and immutable paths, so a path match with a partition mismatch can
  only be a writer bug. Recommend **raise** on partition mismatch. Follow the spec on seq ("does not
  apply"): that rule is defined behaviour, not a bug.

### 3.4 Carry it out

`ManifestDataFile` gains `dv_path`, `dv_offset`, `dv_length` and `dv_cardinality` (the DV entry's
`record_count`, which for a DV is its cardinality: spec field 103 "the cardinality of a deletion
vector"). `_manifest_entries` emits `delete_file_path = dv_path` and
`deleted_record_count = dv_cardinality`, plus whatever D3 adds. `record_count` stays physical. Core
already computes live = `record_count − deleted_record_count`.

### 3.5 `IcebergDataset.delete_vectors_for(entries)`

For each pending entry: a ranged read of `[offset, offset + length)` from the Puffin file through
the table's FileIO. Then `rugo.puffin.read_deletion_vector(blob, cardinality)` (§4), returning per D4.

IO default (a perf choice, not a ruling): group by Puffin path, coalesce adjacent ranges in a file,
and fetch the groups in parallel on a `ThreadPoolExecutor`, as manifests already are. To measure
before tuning: binding latency against DV count on a Spark-written table.

**No Puffin footer read** (D2). The spec requires the manifest's `content_offset`/`content_size_in_bytes`
to "exactly match the `offset` and `length` stored in the Puffin footer". The blob also verifies
itself: a length prefix, magic and CRC (§4), checked against the manifest's cardinality.

---

## 4. Native reader — `rugo/src/puffin/`

Puffin is a file format. That puts it in rugo's charter, next to the Avro reader. It must stay
opteryx-free: the standalone `rugo` wheel ships it.

Two layers, both ours (D1, D1b ruled): a **full Puffin file reader** (§4.1), and the
`deletion-vector-v1` decoder on top of it (this section). No vendored CRoaring or third-party Puffin code.

`rugo/src/puffin/deletion_vector.{hpp,cpp}` exposes one function. It takes a blob span and the
expected cardinality and returns ascending positions or an error string. Python edge: the
`rugo.puffin` module, `read_deletion_vector(blob: bytes, cardinality: int)`, which drops the GIL for
the decode. The blob it takes is the uncompressed blob payload: `deletion-vector-v1` is never
compressed, and if the footer says it is, §4.1 raises.

**Blob layout** (Puffin spec, `deletion-vector-v1`):

```
u32 BE  combined length of magic + vector
4 B     magic  D1 D3 39 64
...     vector (Roaring 64-bit portable)
u32 BE  CRC-32 of magic + vector
```

**Vector:** `u64 LE` bitmap count, then for each bitmap, in ascending unsigned key order:
`u32 LE key` (the high 32 bits of the position) followed by one 32-bit Roaring bitmap in the
portable layout:

- cookie `12346` (no run containers): `u32` container count follows. Cookie `12347`
  (low 16 bits): the high 16 bits hold count − 1, followed by a run-flag bitset of ⌈n/8⌉ bytes.
- descriptive header: per container, a `u16` key and a `u16` cardinality − 1.
- offset header (`u32` per container): present for cookie 12346, or when n ≥ 4. Skipped, but its
  length is validated.
- containers: run (flagged) = `u16` nruns, then (start, length − 1) pairs. Bitset = cardinality > 4096,
  8192 bytes. Array = cardinality ≤ 4096, sorted `u16`.

**All three container types are required.** Real writers emit run containers.

**Validation, every failure raises with the reason** (the caller adds the data file's path):
- blob length ≥ 12, and the BE length prefix == blob length − 8
- magic matches; CRC-32 (`crc32_update`, IEEE, the same polynomial as `java.util.zip.CRC32`) matches
- bitmap keys strictly ascending, and the key's MSB is 0 (the spec: positive positions only)
- container keys strictly ascending; array values strictly ascending; run pairs non-overlapping and ascending
- declared cardinalities == decoded cardinalities; total == the manifest's cardinality
- bytes consumed == vector length exactly (no trailing bytes, no short read)
- **any key ≠ 0 → raise** "position ≥ 2³² not supported". `DeleteAdmission` holds `uint32` ordinals.
  A Parquet file past 4 G rows is not a realistic case, and the alternative is widening
  `DeleteAdmission`, which is not done here. Today a 64-bit ordinal would reach the `uint32_t`
  conversion in `DeleteAdmissionHandle` and fail with a bare `OverflowError`. The decoder refuses
  it first, with a message that names the cause.

Positions are emitted ascending: keys ascending × containers ascending × values ascending, so no sort.

### 4.1 The Puffin file reader (D1b — RULED 2026-10-23: full reader)

The architect ruled for a full Puffin file reader, not a DV-blob-only decoder. The DV decoder above
is one blob type on top of it. The same reader later serves `apache-datasketches-theta-v1`
statistics, which are out of this doc's scope.

`rugo/src/puffin/puffin_reader.{hpp,cpp}`. **Bytes in, no IO**, the same as `rugo.avro.read_avro(data)`:
the caller (opteryx-iceberg, through the table's FileIO) fetches the bytes, and the reader parses them.

**File layout** (Puffin spec, version 1):

```
Magic  Blob₁ … Blobₙ  Footer
Footer = Magic  FooterPayload  FooterPayloadSize  Flags  Magic
Magic  = 50 46 41 31 ("PFA1")
```

`FooterPayloadSize` is a 4-byte signed little-endian integer, the payload length after compression.
`Flags` is 4 bytes, and byte 0 bit 0 means the payload is compressed. The footer is
`12 + FooterPayloadSize + 4` bytes, read from the end of the file.

**Surface:**
- `read_footer(tail: bytes, file_size: int) -> FileMetadata`. `tail` is any suffix of the file
  that contains the whole footer. Steps:
  - check the trailing magic, then read `FooterPayloadSize` and `Flags`
  - if `tail` is too short, raise with the exact byte count needed, so the caller re-fetches once
    with no guessing loop
  - check the footer's leading magic
  - decompress the payload when flag bit 0 is set: a single LZ4 frame with content size present,
    per the spec
  - parse the JSON with yyjson
- `FileMetadata`: `blobs: list[BlobMetadata]` and `properties: dict[str, str]`.
  `BlobMetadata` has `type`, `fields: list[int]`, `snapshot_id`, `sequence_number`, `offset`,
  `length`, `compression_codec: str | None` and `properties: dict[str, str]`.
- `read_blob(data: bytes, blob: BlobMetadata) -> bytes`. `data` is exactly the blob's
  `[offset, offset + length)`. The result is decompressed per `compression_codec`: none, `lz4`
  (a single LZ4 frame) or `zstd` (a single zstd frame).
- `read_deletion_vector(blob, cardinality)` (§4), applied to the `read_blob` result.

**Validation, every failure raises with a named reason:**
- both footer magics; `FooterPayloadSize` ≥ 0; the footer fits inside `file_size` with the leading
  file magic before it
- **unknown `Flags` bits set → raise.** Reserved bits mean a writer feature we don't understand.
  Ignoring them could misparse the footer silently.
- the JSON is a `FileMetadata` object: `blobs` is required, and so is every required `BlobMetadata`
  field, with the right JSON type. Unknown keys are ignored, because the spec lets writers add properties.
- every blob range is inside `[4, footer_start)`
- `compression-codec` is absent, `lz4` or `zstd`; anything else raises. The spec says
  "other codecs are not supported".
- decompressed sizes must match the frames' declared content size, and the frame checksums are
  verified by the existing stream decoders
- a `deletion-vector-v1` blob with any `compression-codec` raises (the spec: "not compressed")
- an unknown blob `type` is **not** an error at this layer. `read_blob` returns its bytes, and
  callers dispatch on `type`.

**How the DV path uses it.** The manifest gives each DV's `content_offset` and
`content_size_in_bytes`, which the spec says exactly match the footer's `offset` and `length`. So a
DV can be read without the footer: one ranged read per blob (or per coalesced group), then
`read_deletion_vector`. Whether the DV path *also* reads the footer to cross-check offset, length,
type, codec and `referenced-data-file` is D2. The full reader makes that cross-check possible; it
doesn't make it mandatory.

Everything the full reader needs is already in rugo. It needs no new vendoring:

| Need | Where |
|---|---|
| CRC-32 (IEEE) | `crc32_update`, `rugo/src/compression/stream_decompress.hpp` |
| LZ4 **frame** and zstd frame decode | `stream_decompress.hpp` (frame format, content checksums verified) |
| JSON | yyjson, `third_party/yyjson`, already built for rugo |

---

## 5. The handoff into core

There are two decisions here. Both touch interfaces, so they are rulings rather than defaults.

### D3 — how Puffin coordinates reach `delete_vectors_for`

`unresolved_deletes()` gives the dataset `{file_path, delete_file_path, deleted_record_count}`.
There is no offset and no length.

- **(a) Side map in IcebergDataset.** `plan_data_files` stores data-file path → (puffin path, offset,
  length). No core change. **Defect:** `delete_vectors_for` is not told which snapshot it is
  resolving, and the same immutable data file has a *different* DV in different snapshots. A side
  map keyed by path is wrong as soon as two snapshots of one table are bound on one dataset object
  (time travel next to current, or a self-join across snapshots). Keying the map by snapshot needs
  the snapshot id passed in, which is an interface change anyway.
- **(b) Two nullable BIGINT columns on the opteryx manifest:** `delete_content_offset` and
  `delete_content_size` (null = "the whole file", our catalog's sidecar case). They are carried
  through `ManifestFile`, `unresolved_deletes()` and the catalog's `encode_parquet_manifest`. The
  manifest describes the delete exactly, with no hidden state, and it maps 1:1 onto v4's colocated
  `deletion_vector.offset/size_in_bytes`. Cost: a manifest schema change in core + opteryx-catalog.
- (c) Encode offsets into the `delete_file_path` string. **Rejected:** meaning hidden in a proxy value.

**Recommend (b).** (a) has a correctness hole.

### D4 — how positions travel

Today: Python `list[int]` from `delete_vectors_for`, then `vector<int64_t>` in `resolve_deletes`,
then Python `tuple` in `_arm_delete_admission`, then `vector<uint32_t>` in `DeleteAdmissionHandle`.
That is about 2N boxed Python ints per N deleted rows, built in Python at binding. Our own catalog
pays this today. DVs will make it pay at Spark-table scale (millions of deletes per snapshot is
ordinary).

- **(a) Reuse as-is.** The decoder returns a Python list. No core change, but it extends an
  existing per-deleted-row Python cost to a new, larger source (§1: "do not extend it").
- **(b) Native buffer end to end.** The decoder returns a uint32 buffer.
  `resolve_deletes` / `set_delete_positions` adopt it, and `_arm_delete_admission` hands
  `DeleteAdmission` the manifest's vector directly with no tuple round-trip. The catalog's own
  sidecar path gets the same win once its decoder emits a buffer. That is a follow-up there, not
  this doc.

**Recommend (b).** The DV path should not ship on top of the boxed-int path.

---

## 6. Failure modes — all raise, none degrade

| Condition | Where | Behaviour |
|---|---|---|
| DV entry missing `referenced_data_file`/offset/size | planning | raise, names the delete manifest |
| >1 DV applies to a data file | planning | raise (D7) |
| DV path matches, partition differs | planning | raise (D7) |
| Position delete file applies, no DV | planning | refuse, as today (D5) |
| Equality delete live | planning | refuse, as today |
| Puffin ranged read fails / short | `delete_vectors_for` | raise (FileIO error, path + range) |
| Bad length / magic / CRC / structure | rugo decoder | raise, named reason + data file |
| Decoded cardinality ≠ manifest cardinality | rugo decoder | raise |
| Position ≥ 2³² | rugo decoder | raise |
| Position ≥ file row count | `DeleteAdmission::prepare` | raise (existing) |
| File left without a vector | `resolve_deletes` | raise (existing) |

---

## 7. Tests

The decoder must be tested against bytes from a **real writer**, not only bytes we produced.
Otherwise our reading of the spec is only being checked against itself.

- **rugo Puffin reader tests** (`tests/rugo/test_puffin_reader.py`):
  - footer uncompressed and LZ4-compressed
  - a short `tail` raises with the exact byte count needed
  - blobs uncompressed, `lz4` and `zstd`
  - unknown blob type returned raw
  - one test per §4.1 validation failure (unknown flag bits, unknown codec, a compressed DV, an
    out-of-range blob, a missing required field)
- **rugo DV decoder tests** (`tests/rugo/test_puffin_deletion_vector.py`):
  - hand-built blobs for array, bitset and run containers, both cookies, and the n ≥ 4 offset-header
    case, with empty and full-container edges
  - one test per §4 validation failure
- No numpy in either.
- **Golden interop files** (D8): a small set of Puffin files plus expected positions, written by
  Iceberg Java/Spark (the reference writer) and checked in as fixtures.
- **Planning tests** (`opteryx-iceberg/tests/`):
  - synthetic v3 delete manifests, extending `_synthetic_deletes.py`
  - DV applies; seq equal (same commit) applies; seq lower does not apply
  - DV + stale position delete for the same file → the DV wins, no refusal
  - position delete without DV → refusal stands
  - two DVs → raise; partition mismatch → raise
  - **To verify before relying on it:** whether pyiceberg's manifest writer can write v3 entries
    with fields 143–145. If it can't, the fixture writer writes them itself or the golden tables cover it.
- **End-to-end SQL:**
  - deleted rows absent
  - `COUNT(*)` is not answered from `record_count` (statistics-only response backs off)
  - a fully deleted row group is never fetched
  - time travel to the pre-delete snapshot returns the rows
- Gate: `make q`, then `make slt`; plus the opteryx-iceberg suite (Lakekeeper-backed tests are live
  services, so they need explicit authorisation to run).

---

## 8. Decisions to rule on

| # | Decision | Recommendation |
|---|---|---|
| **D0** | Build at all: is reading v3 MOR tables a goal now? DVs alone, or as part of a v3-read programme? | **RULED: build.** Still open: alone vs programme. Recommend DVs alone, with the §9 format-version gate |
| **D1** | Decoder home and form: `rugo/src/puffin/`, hand-written, no CRoaring | **RULED 2026-10-23: our own Puffin reader** |
| **D1b** | Reader scope: DV blob only, or a full Puffin file reader (§4.1) | **RULED 2026-10-23: full Puffin file reader** (magic, footer, compressed footer JSON, blob directory, compressed blobs), DV decoder on top |
| **D2** | DV path: read blobs by manifest offset/length with blob self-checks, or also read each Puffin footer to cross-check | Direct read, no footer read on the DV path. The blob's length prefix, magic, CRC and cardinality already pin it, and a footer read is one more round trip per Puffin file |
| **D3** | Puffin coordinates across `delete_vectors_for`: side map / manifest columns / string | (b) manifest columns: the side map is wrong across snapshots |
| **D4** | Position transport: boxed Python ints / native buffer | (b) native buffer |
| **D5** | v2 position-delete files (no DV) in upgraded tables | Keep refusing; separate doc if ever needed |
| **D6** | Positions ≥ 2³²: refuse / widen `DeleteAdmission` | Refuse |
| **D7** | Strictness: raise on >1 DV and on path-match/partition-mismatch | Raise on both; seq rule spec-literal |
| **D8** | Golden fixtures from Iceberg Java/Spark checked into tests: who generates them, and where they live | Checked-in fixtures, generated once by a `dev/` script that runs Spark locally |

---

## 9. Found while reading — not in scope, reported for direction

1. **No format-version gate in opteryx-iceberg.** I found no check on `format_version` anywhere
   in `opteryx_iceberg/`. A v3 table without deletes is read today as if it were v2. These v3
   features are **not verified** to be handled:
   - **Column default values** (`initial-default`). A column added with a default must read as that
     default in older data files that lack it. Reading it as NULL is silently wrong.
   - New types: `timestamp_ns` / `timestamptz_ns`, `variant`, `geometry` / `geography`, `unknown`.
   - Row lineage (`_row_id`, `_last_updated_sequence_number`). This only matters if those columns
     are queried.

   I have not run anything to confirm what happens today for each one. If any of them silently
   misreads, it is a correctness bug in what ships now, independent of this doc.
2. **v2 `referenced_data_file` is ignored.** v2 position-delete files may set
   `referenced_data_file` (spec field 143), but `_DeleteIndex` matches only on `file_path` bounds or
   on partition. Today that only makes refusals broader than necessary, so it is still correct. It
   becomes relevant only if D5 ever changes.
