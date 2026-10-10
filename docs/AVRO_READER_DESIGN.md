# Avro reader (scope A) — enough to read Iceberg manifests natively

Status: DESIGN — awaiting ratification. Nothing is built.
Date: 2026-10-10
Context: Apache Avro C++ (and ClickHouse's fork) rejected — it pulls in Boost.
Spec: https://avro.apache.org/docs/current/specification/

## 0. Precondition — the goal (decision D0)

Today core never sees Avro. Per the 2026-10-05 ruling (manifest planning native
moves) every catalog backend serves the **opteryx manifest parquet** via
`manifest_bytes()`. `opteryx-iceberg` builds it with PyIceberg:
`IcebergDataset._manifest_entries` (`opteryx_iceberg/dataset.py:327`) calls
`table.scan().plan_files()`, then decodes bounds with `pyiceberg.conversions.from_bytes`
and ordinalizes them through core's `ColumnType.ordinalize`. Cold cost ≈ 88 ms per
snapshot, all PyIceberg.

PyIceberg's work there is: metadata.json → manifest list (Avro) → manifests (Avro)
→ partition/filter evaluation → delete-file detection → bound decoding. **Avro
decoding is one step of that.** A native Avro reader only removes PyIceberg from
that path if the backend also replaces the other steps.

**D0 — ratify one:**
- (a) Goal is to replace `plan_files()` in `opteryx-iceberg` with native code →
  scope A is the first piece (this doc), backend wiring is §9.
- (b) No such goal → scope A has no consumer today; do not build it.

The rest of this doc assumes (a). The 2026-10-05 contract is unchanged either
way: core keeps consuming the opteryx manifest parquet.

## 1. Scope

**In:** a general Avro object-container-file reader in rugo, C++ only, no Python on
the decode path, sufficient to decode Iceberg v1/v2 manifest lists and manifest
files written by any engine (Spark, Trino, Flink, PyIceberg, Java API).

**Out (this doc):**
- Avro as a scan format in opteryx (DrakenVector builders, connector, pushdown) —
  scope B, later, only on demand.
- Any writer — scope C, only if opteryx ever commits to Iceberg tables.
- Iceberg semantics beyond decoding: partition transforms, delete application,
  metadata.json, bound single-value deserialization (§9 lists who owns these).

Designed so B is an extension (new output sink + edge), not a rewrite.

## 2. Placement and dependencies

- `rugo/src/avro/` — C++ core, opteryx-free (rugo rule). Edge: `rugo/src/avro/_avro_reader.pxi`
  included from `rugo/src/rugo_native.pyx`, same as JSONL/CSV.
- Zero new dependencies. Reuses vendored: **yyjson** (schema JSON), **miniz**
  (raw inflate), **snappy**, **zstd**; `crc32_update` from
  `rugo/src/compression/stream_decompress.hpp` (hardware CRC32, IEEE polynomial —
  the one Avro's snappy trailer uses).
- Allocation via draken's allocator (`draken_malloc`/`draken_free`), as JSONL's
  `ByteArena`.
- No PyArrow, no NumPy.

## 3. Spec surface (what we implement)

### 3.1 Container
- Magic `O b j 0x01`; reject anything else.
- Header metadata: an Avro `map<bytes>`. Required: `avro.schema` (JSON). Optional:
  `avro.codec` (absent = `null`). Other keys retained raw and exposed — Iceberg puts
  `schema`, `partition-spec`, `partition-spec-id`, `format-version`, `content` here.
- 16-byte sync marker.
- Blocks: `long count`, `long byte_size`, `byte_size` bytes, 16-byte sync. Sync
  mismatch = corrupt file → error (no resync, no skipping).

### 3.2 Codecs
| Codec | Implementation |
|---|---|
| `null` | none |
| `deflate` | raw RFC 1951 (no zlib header) — miniz `tinfl` |
| `snappy` | snappy raw block, then a 4-byte **big-endian CRC32 of the uncompressed bytes**; verify, mismatch = error |
| `zstandard` | `ZSTD_decompressDCtx`, one frame per block |
| `bzip2`, `xz`, anything else | **refused**, error names the codec |

### 3.3 Encoding primitives
- `int`/`long`: zigzag varint; reject > 5 / > 10 bytes and out-of-range `int`.
- `float`/`double`: 4/8 bytes little-endian.
- `boolean`: one byte, reject values other than 0/1.
- `bytes`/`string`: `long` length then payload; negative or past-end = error.
  Strings are not UTF-8-validated at decode (a decision, D7).
- `fixed(n)`: n bytes.
- `enum`: `int` index; out of range = error.
- `array`/`map`: blocks of `long count` items; `count < 0` means `|count|` items
  followed by a `long` byte size; `0` ends. Map keys are strings.
- `union`: `long` branch index, then the branch.
- `record`: fields in order, no framing.

### 3.4 Logical types
| Logical | Underlying | Output |
|---|---|---|
| `date` | int | DATE (days) |
| `time-millis` / `time-micros` | int / long | TIME (micros) |
| `timestamp-millis` / `timestamp-micros` | long | TIMESTAMP (micros) |
| `uuid` | string, or fixed(16) (Iceberg) | BLOB 16 bytes |
| `decimal(p,s)` | bytes or fixed(n) | DECIMAL; p ≤ 18 → int64, p ≤ 38 → int128; p > 38 refused |
| `timestamp-nanos`, `local-timestamp-*`, `duration` | — | refused until ruled (D8) |
| unknown `logicalType` | — | ignored, underlying type used (spec-mandated) |

Iceberg's `logicalType: "map"` on `array<record{key,value}>` is decoded as the
array it physically is; the consumer interprets it (§6).

## 4. Schema handling

### 4.1 Parse
yyjson → an immutable schema tree. Named types (`record`, `enum`, `fixed`) are
registered by full name (namespace rules per spec); references resolve to them.
**Recursive types (a name referenced inside its own definition) are refused.**
Field attributes `field-id`, `element-id`, `key-id`, `value-id` are retained.

### 4.2 Resolution (writer schema → reader schema)
The writer schema comes from each file's header. The reader schema is supplied
by the caller (for manifests: a fixed projection, §6). The subset we implement:

- **Field matching:** by `field-id` when both sides carry one; otherwise by
  name. Mixed (one side has ids, the other not) = error.
- **Writer-only field:** compiled to skip ops.
- **Reader-only field:** all-NULL constant if nullable (as the ABSENTNULL ruling);
  error if required. Non-null `default` values: **refused** (D5).
- **Promotions:** int→long, int/long→float/double, float→double,
  string↔bytes. Anything else = error.
- **Aliases:** refused (D5).
- **Unions:** see 4.3.
- **Enum symbol mismatch:** reader enum must contain every writer symbol, else
  error (no `default` symbol support).

### 4.3 Unions
Supported: exactly two branches, one of them `null`, either order
(`["null",T]` or `[T,"null"]`). The null branch index is resolved at compile time;
the decode op writes a validity bit. **All other unions are refused** at compile
time with the schema path in the message.

### 4.4 Where resolution runs (D2)
Resolution and compilation happen **natively, when the reader opens each file**,
against the reader schema the caller passed. Planning supplies only the reader
schema. Rationale: doing it at planning would mean a header read per file at plan
time; compile is µs-scale and native anyway; files of one table may carry
different writer schemas, so it is one program per file regardless.

## 5. Decode model

### 5.1 The program
Resolution produces a flat program: a `std::vector<Op>` of a closed set of
opcodes. Records flatten away entirely — they are concatenation. Illustrative set:

```
LONG    col      INT     col      DOUBLE  col      FLOAT  col
BOOL    col      BYTES   col      FIXED   col n    ENUM   col
DEC_BYTES col    DEC_FIXED col n                   (sign-extend big-endian)
SKIP_VARINT      SKIP_BYTES       SKIP_FIXED n     SKIP_8  SKIP_4
OPT     null_branch, col          (next op is the value; null → validity bit, skip it)
ARRAY_BEGIN list, end_pc          ARRAY_END        (block counts, child offsets)
SKIP_ARRAY end_pc                 (uses the byte-size form when present)
CONST_NULL col                    (reader-only field, emitted once per block, not per row)
```

Each value op appends to its output column (§5.3). There is no per-row consultation
of the schema tree.

### 5.2 The interpreter (D1)
One loop per block: for each of `count` rows, run the program. Dispatch is a
`switch` over `op.code`. **This is per-field dynamic dispatch in a hot loop,
which §2 bans by default** — hence D1. Arguments for it:
- The op sequence is identical for every row; for manifest-width schemas
  (~20–40 ops) the predictor learns it.
- Varint decode, not dispatch, is the expected cost.
- Precedent: the JSONL interpreter (`rugo/src/jsonl/core/interpreter.cpp`).

What cannot be removed: the union branch byte and array block counts are data.

Alternative (if D1 is refused): template-specialised programs for fixed
schemas (the Iceberg manifest/manifest-list schemas are known per format
version) with the switch kept only for the partition struct. More code; I do not
recommend it before a measurement shows dispatch matters.

### 5.3 Output — columnar intermediate (D3)
Scope A outputs a rugo-internal columnar set, not DrakenVectors:
- per leaf column: values buffer (fixed-width) or offsets + byte arena (bytes/string),
  plus a validity bitmap when nullable;
- per array: an offsets buffer (CSR), its child leaves indexed by element.

String/bytes payloads are copied into a per-column arena while the decompressed
block is hot (as JSONL's copied columns), so block buffers can be freed/reused
per block. The edge exposes these as draken vectors (leaves) + int32 offsets.
Scope B would add a DrakenVector sink over the same intermediate.

### 5.4 Parallelism
Blocks are independent once the sync markers are found. Manifests are small
(Spark default ~8 MB target, blocks typically tens of KB), so scope A decodes a
file on one thread; parallelism is **across manifest files** (the manifest list
names many). Intra-file block parallelism is scope B.

### 5.5 Projection
Unprojected fields become skip ops. Avro rows carry no lengths, so skipping still
walks every byte's varint/length — projection saves materialisation, not parsing.
No predicate pushdown in scope A.

## 6. Iceberg manifest use

The reader is general; the Iceberg-specific part is the reader schemas we pass.

**Manifest list** (`manifest_file` records): `manifest_path`, `manifest_length`,
`partition_spec_id`, `content` (v2: 0 data / 1 deletes), `sequence_number`,
`min_sequence_number`, `added_snapshot_id`, file counts, `partitions` (array of
field summaries — projected out in scope A unless partition pruning is wired).

**Manifest** (`manifest_entry`): `status` (0 existing / 1 added / 2 deleted),
`snapshot_id`, `sequence_number`, `file_sequence_number`, and `data_file`:
`content`, `file_path`, `file_format`, `partition` (struct — **schema differs per
table/spec**, which is why a general schema-driven reader is needed), `record_count`,
`file_size_in_bytes`, `column_sizes`, `value_counts`, `null_value_counts`,
`nan_value_counts`, `lower_bounds`, `upper_bounds` (the `map<int,*>` fields are
`array<record{key,value}>` on disk).

The projection matching today's `_manifest_entries` output: `status`,
`data_file.content`, `file_path`, `file_format`, `record_count`,
`file_size_in_bytes`, `null_value_counts`, `lower_bounds`, `upper_bounds`. Everything
else is skip ops. v1 vs v2 differ in fields present/required; resolution by field-id
handles both with one reader schema.

## 7. Errors (fail fast, no fallbacks)

Every one of these is an error naming the file and, where meaningful, the block
index and schema path: bad magic; missing `avro.schema`; unparseable schema;
unsupported codec; refused schema construct (§3.4, §4); sync mismatch; varint
overlong; negative/out-of-range length or count; enum/union index out of range;
decompressed size mismatch; snappy CRC mismatch; block runs past file end; block
`count` disagreeing with bytes consumed (trailing bytes in a block = corrupt).
No resync, no partial results, no Python fallback.

## 8. Explicit refusals (summary)

bzip2, xz codecs · general unions · recursive types · aliases · non-null defaults ·
enum `default` · decimal p > 38 · `timestamp-nanos`, `local-timestamp-*`,
`duration` (pending D8) · resolution promotions beyond §4.2.

## 9. Not in this doc — who owns it (D4)

To actually replace `plan_files()` the backend also needs, outside rugo:
1. metadata.json parse (current snapshot, schemas, partition specs) — yyjson.
2. Walk manifest list → manifests; apply status (drop `deleted` entries) and
   sequence-number inheritance (v2: null → inherit from the manifest list entry).
3. Delete-file detection → keep today's merge-on-read refusal (`_reject_merge_on_read`).
4. Bound decoding: Iceberg single-value serialization → ordinal via
   `ColumnType.ordinalize`.
5. Encoding the opteryx manifest parquet (already `encode_parquet_manifest`).

D4 asks where 1–4 live: Python in `opteryx-iceberg` calling rugo's reader (smaller,
planning-phase Python is allowed by §1), or native. Recommendation: Python in the
backend for 1–3, with the reader returning columns (not per-row objects); 4 native
if it shows in the profile.

## 10. Size and effort

| Part | Lines (approx) |
|---|---|
| Container, block framing, codecs, CRC | 350 |
| Varint/primitive decode, bounds checks | 200 |
| Schema parse (yyjson), named types | 450 |
| Resolution + program compiler | 600 |
| Interpreter + columnar intermediate | 600 |
| Decimal / logical types | 200 |
| Cython edge (`_avro_reader.pxi`) | 300 |
| **Total** | **~2.7k** (C++ ~2.4k, Cython ~0.3k) |

Tests and fixture generators extra. Effort: ~1–2 weeks of focused work, plus
fixture assembly.

## 11. Test strategy

- **Generated (dev/ + tests/, never engine):** fastavro (no NumPy) matrix — every
  codec × every primitive/logical type × nullable both orders × enum × decimal
  bytes/fixed × arrays (including negative-count blocks) × maps × schema-evolution
  pairs (added/removed/promoted fields, field-id vs name). Differential against
  fastavro's decode.
- **Iceberg:** PyIceberg (already a dependency of `opteryx-iceberg`) writes v1 and
  v2 tables; decode their manifest lists/manifests; compare to `plan_files()`.
- **Foreign writers:** Apache Avro's interop data files (one per codec), and
  Spark/Java-written manifest fixtures from Iceberg's test resources, checked in.
- **Refusals:** one test per §8 item asserting the error.
- **Corruption/fuzz:** truncations, flipped sync, overlong varints, huge counts,
  CRC mismatch; a `make fuzz` target over the container decoder.
- `make q` and `make slt` per the contract; native additions ⇒ also dt/st/rt.

## 12. Performance expectation

Not measured — nothing is built. Expectation only: manifest decode is varint-bound;
the win against PyIceberg's ~88 ms cold is planning latency, not throughput.
Baseline (PyIceberg cold/warm per snapshot, on a Spark-written and a
PyIceberg-written table) is taken **before** the first line of code.

## 13. Decisions for the architect

| # | Decision | Recommendation |
|---|---|---|
| D0 | Is replacing PyIceberg's `plan_files()` a goal? (§0) | Your call — gates everything |
| D1 | Per-field `switch` interpreter over a compiled program (§5.2) vs per-schema template specialisation | Switch interpreter; revisit only on profile evidence |
| D2 | Resolve + compile at file open, natively; planning passes only the reader schema (§4.4) | Yes |
| D3 | Output is a rugo-internal columnar intermediate exposed as draken leaves + offsets, not DrakenVector-native nested types (§5.3) | Yes for scope A |
| D4 | Ownership of metadata.json / list walk / deletes / bound decoding (§9) | Python in `opteryx-iceberg` for 1–3; 4 native only if profiled |
| D5 | Refuse aliases and non-null defaults (§4.2) | Refuse |
| D6 | Refuse bzip2/xz (§3.2) | Refuse |
| D7 | No UTF-8 validation of Avro `string` at decode (§3.3) | No validation in A (manifest paths); revisit for B |
| D8 | `timestamp-nanos`, `local-timestamp-*`, `duration` (§3.4) | Refuse in A |
| D9 | Location `rugo/src/avro/`, part of `rugo_native` and so shipped in the standalone rugo wheel | Yes |

## 14. Rulings (2026-10-10)

| # | Ruling |
|---|---|
| D0 | **Iceberg manifests are the entry point.** Avro as a scan format is wanted later (scope B) for logging writers such as Kafka. |
| D1 | Accepted — switch interpreter over the compiled program. |
| D2 | Accepted — resolve + compile natively at file open. |
| D3 | **Open** — costs requested, DrakenVectors preferred (§15). |
| D4 | Accepted "for starters" — Python in `opteryx-iceberg` for metadata.json / list walk / deletes; bound decoding native only if profiled. |
| D5 | Aliases **refused**. Non-null defaults — **open**, user asks whether Draken can carry them (§16). |
| D6 | Accepted — bzip2/xz refused. |
| D7 | Accepted — no UTF-8 validation in A. |
| D8 | Accepted — refused in A. |
| D9 | Accepted "as a starter" — `rugo/src/avro/`, in `rugo_native`. |

### 14.1 Note on D0 — what "Kafka" means for Avro
Two different things, only one of which this reader covers:
- **Kafka Connect sinks** (S3/GCS sink connectors) write Avro **object container
  files** — the format this reader decodes. Scope B covers them.
- **Raw topic messages** use the Confluent wire format: `0x00` + 4-byte schema id +
  a bare Avro datum, no container, schema fetched from a Schema Registry. The
  decode program runs unchanged on bare datums, but framing + registry lookup is a
  connector (network, outside rugo) — **not scope A or B**; a separate decision if
  ever wanted.

## 15. D3 — output: intermediate vs DrakenVectors

Draken has no STRUCT or MAP type; `DRAKEN_ARRAY` is offsets + one child
(`DrakenArrayBuffer`), always dense. Both options share that limit, so nested
Avro needs a representation either way (D3b below).

### Option I — rugo-internal columnar intermediate, converted at the edge
- Decode writes per-leaf buffers (values / offsets+bytes / validity), then the edge
  converts each to a DrakenVector.
- Strings are copied twice (block → arena → German slots); fixed-width once more
  (intermediate → vector buffer).
- A: ~2.7k lines (§10). **B must then add a DrakenVector sink anyway** — the
  intermediate becomes either dead or a permanent extra pass. B ≈ +1.8k.

### Option V — decode straight into DrakenVectors (preferred by the user)
- Each value op's sink is a Draken builder: fixed-width types write the vector's
  data buffer directly; `string`/`bytes` build `DrakenStringArena` slots directly
  (≤12 bytes inline, else arena) while the decompressed block is hot — one copy.
- **Avro gives the row count per block before the rows**, so every builder is
  sized exactly per block — no growth, no over-allocation. Output batch = one or
  more whole blocks (merge up to the morsel size; never split a block).
- `enum` → `draken_vector_from_dict` (symbols are the dictionary, the indices are
  the codes) — native dict shape, no extra work.
- `["null",T]` → validity bitmap; nullable-but-never-null batches leave `validity`
  NULL.
- Array of primitive → `DRAKEN_ARRAY` (offsets + child vector).
- Reader-only fields → `draken_vector_from_constant` (NULL, or the default — §16).
- Lines for A: ~2.9k (the intermediate's ~600 is replaced by ~800 of builders +
  thinner edge). **B drops to ~1.2k** (connector, pushdown, block parallelism, no new
  sink). Total A+B: I ≈ 4.5k vs V ≈ 4.1k, and V has one fewer copy per column.
- Risk: none in the frozen ABI — only existing types and the sanctioned
  `draken_vector_from_*` constructors.

**Recommendation: V.**

### D3b — nested Avro under V (needs a ruling)
| Avro shape | Proposal |
|---|---|
| record inside record | flatten to dotted leaf columns (`data_file.file_path`) |
| array\<primitive\> | `DRAKEN_ARRAY` |
| array\<record\> (incl. Iceberg's `map<int,*>`) | **parallel arrays sharing one offsets buffer**: `lower_bounds.key ARRAY<INT32>`, `lower_bounds.value ARRAY<VARBINARY>`; consumer zips |
| map\<T\> (string keys) | same: `m.key ARRAY<VARCHAR>`, `m.value ARRAY<T>` |
| deeper nesting (array of array, array of record containing array) | refused in A |

Alternative for scope B only: render nested values as JSON text (NVARCHAR), as the
parquet NESTEDJSON ruling does. Not proposed for A — the manifest consumer needs the
keys and bound bytes, not text.

## 16. D5 — non-null defaults on Draken

Yes, Draken can carry them: a reader-only field becomes
`draken_vector_from_constant` holding the default — constant shape, one value per
batch, exactly as ABSENTNULL's null fill but with a value. Per-type cost of parsing
the Avro JSON default (yyjson):

| Type | Default form in schema JSON | Supported |
|---|---|---|
| int/long/date/time/timestamp | JSON number | yes |
| float/double | JSON number (spec also allows `"NaN"`, `"Infinity"`, `"-Infinity"`) | yes |
| boolean | JSON bool | yes |
| string / enum | JSON string / symbol | yes (enum → constant of the symbol) |
| bytes / fixed | JSON string, **each code point 0–255 = one byte** (>255 = error) | yes |
| decimal (bytes/fixed) | same byte-string form, big-endian unscaled | yes |
| `["null",T]` | must be `null` (first branch) | yes = null fill |
| `[T,"null"]` | must be a `T` value | yes |
| record / array / map | JSON object/array | **refused** — no STRUCT/MAP, and ARRAY is always dense (no constant arrays) |

Cost: ~200 lines + one test per row of the table. Relevance: none for Iceberg
manifests (our reader schema has no defaults), real for scope B — Kafka/Schema
Registry evolution typically adds fields *with* defaults, so files written before
the change read the newer schema's default.
**Recommendation:** build the scalar defaults in A (the resolution code is being
written anyway); refuse record/array/map defaults.

## 17. Rulings, round 2 (2026-10-10)

- **D5: build it.** Scalar non-null defaults are in scope A, carried as
  `draken_vector_from_constant`. Record/array/map defaults are refused (§16).
- **D0:** Kafka is directional only. Neither Confluent framing nor a registry is planned.
- **D3:** the user asked whether this is the same thing parquet returns as JSON
  strings. Answered in §17.1; awaiting ruling.

### 17.1 Parquet precedent vs D3b

The I-vs-V choice is independent of JSON: under either option the leaves are
vectors. JSON text only concerns **nested** shapes (D3b). For parquet, the
NESTEDJSON ruling (2026-09-30) is:

| Shape | Parquet today | D3b proposal |
|---|---|---|
| struct / record | NVARCHAR JSON text | dotted leaf columns |
| map | NVARCHAR JSON object (non-string keys refused) | parallel `.key`/`.value` ARRAYs |
| list\<primitive\> | ARRAY | ARRAY (same) |
| list\<struct\> | refused | parallel ARRAYs |

Proposed reconciliation (D3c):
- **Selecting a whole record or map column** (scope B, `SELECT *`) returns JSON text,
  identical to parquet: same rules for member order, null, decimal, date and binary
  base64. One user-visible rule across formats.
- **Projecting a leaf path inside it** (`data_file.file_path`,
  `data_file.lower_bounds.value`) returns plain vectors and parallel ARRAYs. The
  manifest reader always projects by path, so it never sees JSON. JSON for manifests
  would mean parsing JSON per entry in Python, with the binary bounds base64-encoded.
- array\<record\> selected whole: JSON text, so not refused, unlike parquet. Parquet's
  refusal is a decoder gap, not a ruling. Alternatively, refuse it to match parquet.

### 17.2 Revised D3b: use JSON, as parquet does (proposed 2026-10-10)

The user asked "structs are json, maps are json, why not use those?" Agreed. Revised:

| Avro shape | Output |
|---|---|
| record / map, selected whole | NVARCHAR JSON text under the parquet NESTEDJSON rules (member order, `null`, decimal as a bare number, dates as quoted ISO, bytes/fixed as base64 via mabel, NaN/Inf as `null`) |
| array\<primitive\> | ARRAY (as parquet) |
| array\<record\>, array of array | JSON text (parquet refuses list\<struct\>; Avro rows are already row-ordered, so rendering is a direct walk) |
| a dotted scalar leaf named by the caller (`data_file.file_path`) | plain vector — the same as rugo's parquet `resolve_projection`, where a projected name that exactly matches a dotted leaf is read as a plain column (`rugo/src/parquet/nested_json.cpp`) |

- Iceberg's `map<int,*>` is physically `array<record{key,value}>` and renders as a
  JSON array of `{"key":…,"value":…}` objects. Avro's `logicalType:"map"` is not
  special-cased.
- Manifest reader: scalar fields come by dotted path as vectors. `lower_bounds`,
  `upper_bounds` and `null_value_counts` arrive as JSON, which the backend parses in
  Python (D4 "for starters"; base64-decode the bounds). If that shows in the
  profile, fixing it is a later decision.
- Parallel-ARRAY builders are dropped. A JSON renderer is added; ryu and mabel
  base64 are reused. Net A ≈ 2.9k lines unchanged.
