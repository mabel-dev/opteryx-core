# Iceberg Avro and ORC data files — catalog scan support

Status: Avro scan path BUILT 2026-10-25 (§9) — D0–D2 ruled, D3 local-only and D4 authorised 2026-10-25, D5 resolved by declining pushdown. ORC not started. D6 open.
Date: 2026-10-25
Repos: `opteryx-core` (scan dispatch, Avro scan), `opteryx-iceberg` (format stamp), `rugo` (decoders)
Spec: Iceberg table spec, "Data file formats": a data file is Parquet, Avro or ORC, chosen
**per file** (`data_file.file_format`).

---

## 0. Why

An Iceberg table whose data files are Avro or ORC is spec-valid. Today neither can be read
through a catalog scan, and the failure is not clean (§2.3). Avro decoding exists in rugo but is
reachable only as `READ_AVRO('path')`. ORC does not exist.

## 1. Rulings (2026-10-25)

| # | Question | Ruling |
|---|---|---|
| D0 | Mixed-format tables (spec allows per-file format; engine says datasets are single-format) | **Keep the single-format rule for now.** A mixed Iceberg table keeps raising `MixedFormatDatasetError`. Not revisited here. |
| D1 | Order | **Avro on the scan path first, then ORC.** |
| D2 | Bar | **Basic support first.** Anything outside the basic set raises, naming the feature and the file. No fallbacks. |

"Basic" for this doc means: primitive columns, the codecs rugo's Avro reader already decodes,
local files and (once ruled, §4.4) authenticated remote files, no deletes, no nested columns
(§4.3).

## 2. What exists

### 2.1 Core

| Piece | Where | State |
|---|---|---|
| Format registry | `opteryx/models/dataset_format.py:50` | `SCAN_READERS` = PARQUET, JSONL, SKENE. A manifest in any other format raises "has no scan reader" (`physical_planner/__init__.py:206`). |
| Avro decoder | `rugo/src/avro/` | Built. Resolves reader schema against writer schema by **field-id** when both carry one, else by name (`avro_reader.hpp:20`). Reader-only nullable field → NULL. Parallel block decode. |
| Avro scan | `AvroReadNode`, `AvroScanPlan`, `NativeAvroScanSource`, `compiler._compile_avro_scan` (`compiler.py:6853`) | Serves `READ_AVRO` only. Files come from the binder, the reader schema is **the first file's writer schema**, reads are local mmap or **unauthenticated** whole-file GET. |
| Manifest-backed scan, other formats | `_jsonl_scan_inputs`, `_skene_scan_inputs` (`physical_planner/__init__.py:135,170`) | The pattern to copy: the manifest's file list and the schema's read columns reach the reader node as step arguments. |
| Catalog manifest assumption | `opteryx_connector.py:98` | "a catalog manifest is parquet-only … this has to become format-aware." |

### 2.2 `opteryx-iceberg` (read, not changed)

`IcebergDataset._manifest_entries` (`dataset.py:355`) emits `"file_format": "parquet"` as a
**literal for every data file**. `manifests.py` does not read the manifest entry's
`data_file.file_format` at all.

### 2.3 What happens today to an Avro-data-file Iceberg table

It is stamped parquet, routed to the Parquet reader, and fails at the first footer read with a
Parquet-magic error that names neither Avro nor Iceberg. It fails loudly but misattributes the
cause. The stamp is the lie; fixing it is step 1 of §6.

## 3. Shape of the change

Planning is Python and decides the work; the engine does it natively. The Avro scan already
runs natively end to end. What is missing is the **planning-side wiring**: a manifest-backed
Scan has to produce what `READ_AVRO`'s binder produces by hand.

```
opteryx-iceberg  -- stamps data_file.file_format per file ------------------+
                                                                            v
Manifest.file_formats() -> manifest_format() -> SCAN_READERS["AVRO"]  (core, dataset_format.py)
                                                                            v
physical planner: Scan + AVRO -> "Avro Reader" with files + read columns + reader schema
                                                                            v
compiler._compile_avro_scan -> AvroScanPlan -> NativeAvroScanSource (unchanged)
```

## 4. Avro scan path

### 4.1 Format identity (core, no decisions)

`AVRO = "AVRO"` in `dataset_format.py`, `SCAN_READERS[AVRO] = "Avro Reader"`, and the
comment at `opteryx_connector.py:98` corrected. **`.avro` is a data-file suffix** (ruled
2026-10-25): a folder of `.avro` files is a dataset (§4.8). The format also arrives through a
catalog manifest's stamp. Verified: the native manifest decode upper-cases the stamp
(`manifest_decode.hpp`), so `opteryx-iceberg`'s `"avro"` becomes `AVRO`.

### 4.2 The reader schema — the one real design point

`READ_AVRO` reads every file *as the first file's writer schema*. That is wrong for a table:
the **table schema** is the contract. An Iceberg table that has had a column renamed, added or
reordered has files whose Avro field names and order differ from the current schema, and only
`field-id` identifies the column.

Proposal: for a manifest-backed Avro scan, planning builds the reader schema (an Avro record,
JSON) **from the bound relation schema**:

- one field per read column, named by the current column name, carrying `"field-id"` =
  `schema_column.field_id`;
- type from `ColumnType` (§4.3), wrapped `["null", T]` for nullable columns;
- decoder behaviour already specified in `AVRO_READER_DESIGN.md §4.2` and §19.3: id match first,
  a column the file never had becomes NULL if nullable, an error if required.

This is Python producing a JSON string at plan time, the same role the binder plays for
`READ_AVRO` today. No new native code; `AvroScanPlan` already takes `reader_schema` as a string.

Precondition: every column in the relation schema must have a `field_id`. A catalog-supplied
Avro table whose columns carry none raises at plan time.

### 4.3 Types — basic set

| Iceberg | Avro (spec mapping) | Basic |
|---|---|---|
| boolean, int, long, float, double, string, binary, fixed | same | yes |
| date | int / `date` | yes |
| time | long / `time-micros` | yes |
| timestamp, timestamptz | long / `timestamp-micros` | yes |
| decimal(p,s) p ≤ 38 | fixed / `decimal` | yes |
| uuid | fixed(16) / `uuid` | yes |
| timestamp_ns, timestamptz_ns (v3) | `timestamp-nanos` | **refused**, reader refuses it (D8 of the Avro doc) |
| struct, list, map | record, array, array of key/value record | **refused** at plan time |

The nested refusal is deliberate under D2. The Avro reader emits nested values as JSON
(`AVROGAP`), whereas the relation's declared type would be STRUCT/ARRAY; serving one as the
other would be exactly the silent mismatch the contract forbids. Nested support is a separate
ruling on how nested columns surface, shared with whatever the Parquet path does.

### 4.4 File access — decision D3 (open, blocks remote tables)

The native Avro Source reads a local path by mmap, or GETs a URL **with no credentials**.
That is the right security stance for the bare `READ_AVRO` function. Iceberg data files on
GCS or S3 are authenticated, and `JsonlReadNode.native_file_locations` explicitly refuses a
connector-backed remote file (ruling 2026-10-01: no authenticated remote reads for whole-file
sources).

Options:

- **(a) Signed URLs.** The connector's filesystem signs each file (`rewrite_to_signed_url`,
  already used for the `credentials =>` path). No native change. Depends on the catalog's
  filesystem being able to sign; `s3_filesystem.py` documents that it cannot pre-sign today,
  so S3 would need work first. **To verify** for GCS.
- **(b) Auth header on the Source.** Add a header to `AvroScanSpec`, as the Parquet pool reader
  carries `native_auth_header`. Native change, new credential-bearing code path in a whole-file
  GET source.
- **(c) Local files only for now.** Remote Avro tables raise at plan time, naming the file.
  Honest, smallest, and enough to test the whole path against a local warehouse.

Recommendation: **(c) now**, then (a) as its own step once GCS signing is confirmed. (b) only if
(a) is shown not to work. This reverses nothing in the 2026-10-01 ruling; (a) uses its
mechanism. It is nonetheless a credential decision, so it is yours.

### 4.5 Predicates — resolved: declined at bind time

`OpteryxTable` declared pushdown capabilities at class level, describing the Parquet reader. A
pushed predicate or limit is REMOVED from the plan, so an Avro dataset must not accept one.
`OpteryxTable._declare_reader_capabilities` (run at the end of `get_dataset_metadata`, the
same bind-time per-instance gating `FileSystemTable` uses) sets, for an AVRO manifest,
`supports_predicate_pushdown`, `supports_limit_pushdown`, `supports_topn_pushdown` and
`supports_int64_timestamp_retag` all False. Predicates and limits then stay as Filter / Limit
nodes above the scan, which `_arm_two_pass_prefilter` (`compiler.py`, formerly `_arm_avro_prefilter`) already serves natively.
File-level pruning from manifest min/max is format-independent and keeps working (Iceberg's
Avro writer records no column bounds, so in practice an Avro file is read, not pruned).

### 4.8 A folder of `.avro` files is a dataset (ruled 2026-10-25)

`FileSystemTable` discovers `.avro` like any data suffix. The relation is the **first file's
header schema** (`_read_avro_schema`; an Avro file states its schema even with no records), and
every file is read with that schema as its reader schema, **matched by name** - `READ_AVRO`'s
rules, served by the same native source, with no `READ_AVRO` in the SQL. The two shapes of an
Avro scan are told apart by one explicit connector declaration, `data_files_match_field_ids`
(True: `OpteryxTable`, a catalog table, reader schema from the table by field id, files must
carry ids; False: a folder, reader schema from the first file). Local files only, as for a
catalog table (D3). Pushdown is declined (`can_push` is False for AVRO already, and limit /
top-N are Parquet-only), so predicates and limits stay above the scan.

A folder holding `.avro` beside another data format raises `MixedFormatDatasetError`, as any
mixed folder does. A directory that is an Iceberg table's root holds `metadata/*.avro`
manifests beside its data, so pointing the filesystem connector at one now raises that error
instead of ignoring the manifests.

### 4.6 Deletes

A table with delete files is refused at planning in `opteryx-iceberg` today and stays that way.
The deletion-vector programme (`ICEBERG_V3_DELETION_VECTORS_DESIGN.md`) maps ordinals per
Parquet row group; it is not extended to Avro here.

### 4.7 Errors

Each names the dataset, the file and the feature: no field-id on a read column; a nested column
in the projection; an authenticated remote Avro file (until D3); a codec the reader refuses
(`xz`); a required column absent from a file; a type promotion the reader refuses.

## 5. ORC (after Avro)

Designed and built in `docs/ORC_READER_DESIGN.md` (2026-10-25; basic support). A native ORC reader is comparable in scale to the Parquet reader and gets its
own doc, so it can reuse the format
registry, the reader-schema-from-table-schema approach (§4.2; ORC stores Iceberg ids as type
attributes `iceberg.id`) and the same access decision (§4.4).

Scope sketch for "basic": postscript and footer (protobuf, hand-written decode, no dependency),
stripe footers, RLE v1/v2, dictionary string encoding, primitive types, zlib / zstd / snappy,
file- and stripe-level statistics. Refused loudly: nested types, timestamp-with-instant edge
cases, anything else outside that list.

## 6. Build order (all of 1–7 done for Avro; see §9)

1. `opteryx-iceberg`: read `data_file.file_format` from the manifest entry and stamp it; refuse
   anything but parquet / avro / orc by name. (Other repo — your call to authorise.)
2. Core: `AVRO` format, suffix, registry entry, connector comment.
3. Core: physical planner — `Avro Reader` for a Scan; `AvroReadNode` takes manifest inputs
   (files, reader schema) instead of binder-resolved ones.
4. Core: reader schema from relation schema (§4.2), type refusals (§4.3).
5. Core: file access per D3 (local only first).
6. Predicate handling (§4.5).
7. Tests (§7), `make q`, `make slt`.

## 7. Tests

- Iceberg-conformant Avro data files **with `field-id` attributes**. rugo's Avro *writer* does
  not emit them today (`avro_writer.cpp` has no `field-id`), so fixtures are produced in `dev/`
  with an external Avro library, which is allowed outside the engine. Whether to teach the rugo
  writer `field-id` is a separate question and is not assumed.
- Schema evolution: renamed column, added column (NULL), reordered columns, dropped column,
  int→long promotion — each across files of one table.
- Every refusal in §4.7 asserted by message.
- COUNT(*) (zero-column scan), projection, filter above the source, manifest min/max pruning.
- The existing `READ_AVRO` tests must stay green and unchanged: nothing in §4.2 may alter the
  first-file-writer-schema behaviour of the function.

## 8. Decisions

D3 ruled 2026-10-25: **local files only** (option (c)); D4 ruled: **yes**; D5 resolved (§4.5).
Still open:

| # | Question | Recommendation |
|---|---|---|
| D3b | Authenticated remote Avro / ORC files (§4.4) | **Phase 1 built** (catalog tables on `gs://` / `s3://`): `REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md`. Folder datasets on remote storage: phase 2, open. |
| D6 | Nested columns (§4.3) | Refuse now; rule separately with the Parquet nested story |
| D7 | `opteryx-iceberg` maps UUID to VARCHAR; an Avro `uuid` file field (`fixed(16)`) cannot be read as `string`, so a table with a UUID column fails to read from Avro | Rule how UUID surfaces for Iceberg tables (VARCHAR vs the UUID type) |
| D8 | A data file with no `field-id` (Hive/Avro migrations rely on the table's name mapping, `schema.name-mapping.default`) | Refused now; honouring the name mapping is its own piece of work |

## 9. What was built (2026-10-25)

- `opteryx-iceberg`: `manifests.py` reads `data_file.file_format`; `_data_file_format` lower-cases
  it and refuses anything but parquet / avro / orc, naming the file; `dataset.py` stamps it.
  Previously the literal `"parquet"` was stamped for every file.
- `dataset_format.py`: `AVRO`, `SCAN_READERS[AVRO]`, `.avro` suffix (§4.1, §4.8).
- `OpteryxTable._declare_reader_capabilities` (§4.5).
- `avro_io.table_reader_schema` / `table_field_name` (§4.2, §4.3): fields named `f<field_id>`,
  decimals as the `fixed` width Iceberg writes (fixed and bytes do not promote), nested /
  nanosecond / id-less columns left out of the schema and refused by name if projected.
- Physical planner `_avro_scan_inputs`; `AvroReadNode` now takes its files, reader schema and
  field names explicitly for both `READ_AVRO` and a catalog Scan, with local-only
  `native_file_locations` for the latter (§4.4).
- **A field-id guard found by testing.** A file with no `field-id` attributes, read through a
  reader schema that matches by id, resolved every column to NULL and returned five rows of
  NULLs with no error. rugo's resolver falls back to name matching per record (ruled, §19.3 of
  the Avro doc), and the reader's names are `f<id>`. Rather than change that ruling,
  `AvroScanSpec.require_field_ids` (set for a catalog Scan) makes `NativeAvroScanSource` fail the
  query when a file's top-level fields do not all carry one, via the additive
  `AvroFile::writer_fields_have_ids()`. `READ_AVRO` is unaffected.
- Folder datasets (§4.8): `FileSystemTable._read_avro_schema`, `avro_io.first_file_reader_schema`,
  `BaseTable.data_files_match_field_ids`; `AvroReadNode` takes `avro_manifest_scan` (local only) and
  `avro_require_field_ids` separately. Tests: `tests/unit/connectors/test_avro_folder_dataset.py` (7),
  suffix cases in `tests/unit/planner/test_dataset_format.py`.
- Tests: `opteryx-iceberg/tests/test_avro_data_files.py` (16: every primitive type, projection,
  filter, COUNT, LIMIT, NULLs, several files, reordered / renamed / added / dropped fields, int→long,
  and each refusal), `tests/_avro_data_files.py` (fixture writer), format-stamp tests in
  `test_manifest_planning.py`; core `tests/unit/connectors/test_avro_table_reader_schema.py` (21).

### 9.1 Found, not fixed

Two existing `READ_AVRO` tests fail in the current tree, through code this work did not touch (not checked against a clean checkout, which this session cannot make):
`test_glob_file_that_cannot_resolve_fails_naming_it` and `test_corrupt_file_fails_naming_it`.
They expect the file name in the error; `build_terminal_exc` (`_operators.pyx`, code 3) now
replaces a native storage read error with "Unable to read data from storage … The cause has
been logged." That scrub is deliberate (object paths kept out of user-facing text); the tests
were not updated to match.
