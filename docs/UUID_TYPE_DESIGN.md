# UUID column type — design (RULED 2026-10-14)

2026-10-13: **UUID is a first-class type** (option 3 of three — rejected: uuid → VARCHAR
text, uuid → VARBINARY). 2026-10-14: every proposal in §3 (U1–U15) RULED as written.

## 1. Why

UUIDs arrive from Avro (`string`/`fixed(16)` + `logicalType: uuid`), Parquet
(`FIXED_LEN_BYTE_ARRAY(16)` + `UUID` logical type) and Postgres (oid 2950). Today Avro and
Parquet refuse them, Postgres renders them as VARCHAR, and JSON has no uuid type (strings).
A real type stores 16 bytes (vs 36 as text), compares/hashes/sorts as two 64-bit words,
and keeps one meaning regardless of the source format.

## 2. Inventory (what a new type touches)

Template: INTERVAL (16 bytes, non-numeric) needs ~80 lines in 22 files because most
generic paths go through `draken_type_fixed_itemsize`; DECIMAL128 touches ~545 lines but
mostly numeric paths UUID does not need. Estimate: **35–50 real edit sites**.

| Area | Sites |
|---|---|
| Enum / ABI | `draken/core/buffers.h` enum tail + static_assert; `draken_type_fixed_itemsize` (→16); hand-copied width tables `draken/core/lazy_region.cpp:56`, `rugo/src/declared_parse.hpp:80`; `buffers.pxd` name; nanobind enum `.value`; ABI stamp regenerates itself |
| Ops table | `draken/ops/hash.h` `OpsTable::kSize = 108` → **109** (hard-coded); slots copied from INTERVAL: hash, compare scalar/vector, between, in_list, take/slice/materialize/dict_encode (16-byte `i128_*` gathers reuse as-is) |
| Type vocabulary | `draken/logical_type.h` `type_display_name_parts` (persisted in stored schemas); `column_type.pyx` `_CATEGORY_OF` + 256-entry tables; `src/cpp/planner/column_type.hpp`; `LogicalCategory` (+1 member); `opteryx/types/logical_type.py` (`_NAME_OF`, parse, canonical, compatibility); `logical_planner_builders.py` CAST names; `rugo/src/declared_type.cpp` |
| Kernels | compare (`compare_dv.cpp`, `draken_vector_compare_at`, compare boundary); hash/group/join keys (`cxx_morsel_ops.cpp`, `native_join2.hpp`, `kmv_sketch.h`); sort (`sort.hpp` key support + byte-swapped u128 lane); MIN/MAX (`native_group_sinks.hpp`, window frame); IN lists; CASE/COALESCE/IIF (`function_null_conditional.cpp`, `function_kernels.cpp`, binder `_CASE_BLENDABLE_CATEGORIES`, registrar) |
| Casts | `cast_kernels.h` + `kernel_registry.cpp` (IPV4 is the template: string↔type, type→blob); `opteryx/expression/casts.pyx`; `vector_casts.cpp`; literal-vs-column coercion in `logical_planner_builders.py` (IPV4 branch), `constant_folding.py`, `value_parsing.py` |
| Output | `draken/interop/value_format.hpp` `render_json_scalar`; `rugo/src/_text_render.hpp` (CSV/JSONL); Python edge `__getitem__`/`to_pylist`; parquet writer (`parquet_writer.pxi`, `_parquet_writer.hpp`) |
| Readers | parquet FLBA (`decode.cpp:40`, `decode_column.cpp:876`, `pool_reader.pyx:3890`, `io_pipeline.hpp` DirectKind +2); avro (`avro_reader.cpp` 5 refusals); Postgres (`pg_client.cpp:72` oid 2950, `pg_decode.hpp` already renders canonical text) |
| Catalogs | `reference/type_catalog.py` (keyed by category; hand-added like `ipv4`); regenerate via `dev/generate_reference.py`; freshness tests demand an example per type |
| Tests to flip | `tests/unit/connectors/parquet_io/test_flba_decimal.py` (asserts UUID FLBA raises); `tests/rugo/test_avro_reader.py::test_refuses_uuid`; type-enumerating tests (`test_ddl_column_operations.py`, `test_type_coverage_gaps.py`, `test_vector_type_discriminator.py`, `types.slt`) |

**Hazards found:**
- Several `default:` arms emit `null` (`value_format.hpp`, `_text_render.hpp` falls back to
  `ec_null`) or 16 garbage bytes instead of failing. A missed UUID arm would be a silent
  wrong answer. Proposal: every switch touched gets an explicit UUID arm, and those
  defaults are made to fail loud (in scope, since they are the safety net for this work).
- `predicate_pushable.py` treats every `LogicalCategory` as pushable — UUID becomes
  pushable automatically; must be correct, not assumed.
- **Skene writes the DrakenType number into files verbatim** (`skene/include/skene/format.h:887`).
  Whatever number UUID gets is permanent on disk.

## 3. Decisions needed

| # | Question | Ruling (2026-10-14) |
|---|---|---|
| U1 | Physical tag or LogicalKind refinement (as IPV4 refines UINT32)? | **New physical tag `DRAKEN_UUID = 108`** at the enum tail. No 16-byte opaque tag exists to refine: DECIMAL128 is a signed int128 (wrong order, numeric kernels), INTERVAL has (months, µs) semantics. |
| U2 | In-memory byte order | **RFC 9562 network order (the 16 bytes as written in text)**. `memcmp` order = canonical-text order = Postgres order. The sort lane loads two big-endian u64s (byte-swap on little-endian hosts). Equality/hash don't care about order. |
| U3 | SQL spelling | **`UUID`** only (canonical-only rule; sqlparser already emits `Uuid`). Display name `UUID` — persisted, so fixed now. |
| U4 | Text form out | **lowercase, hyphenated, 36 chars** (`550e8400-e29b-41d4-a716-446655440000`), everywhere: display, CSV/JSONL, nested JSON (quoted string), `CAST(... AS VARCHAR)`. |
| U5 | Text form in (`CAST('…' AS UUID)`, literals, Avro string-uuid) | **Strict: exactly the 36-char hyphenated form, either case.** Anything else (braces, no hyphens, urn:uuid:) fails naming the value. Lenient forms can be added later; loosening is safe, tightening isn't. |
| U6 | Casts | VARCHAR↔UUID, BLOB↔UUID (BLOB must be exactly 16 bytes, else fails naming the length). No numeric casts. |
| U7 | `uuid_col = '550e…'` | Literal parsed to UUID at bind (the IPV4 path), so comparison runs on 16 bytes. An unparseable literal fails at bind. |
| U8 | Operations | `= <> < <= > >=`, BETWEEN, IN, IS [NOT] NULL, ORDER BY, GROUP BY, DISTINCT, COUNT(DISTINCT), join keys (hash + ASOF? → **hash only**), MIN/MAX, CASE/COALESCE/IIF. No arithmetic, no string functions without an explicit CAST. |
| U9 | Python edge (`to_pylist`, cursor) | **`uuid.UUID`** objects (like DECIMAL → `decimal.Decimal`). Alternative: str. |
| U10 | Postgres oid 2950 | **Becomes UUID** (today VARCHAR). Behaviour change for Postgres users: `LIKE`/string functions on a uuid column then need a CAST. |
| U11 | Parquet writer | Write `FIXED_LEN_BYTE_ARRAY(16)` + `UUID` logical annotation (spec-standard; readable by DuckDB/Spark/pyarrow). |
| U12 | JSONL / CSV reads | No inference (a uuid-shaped string stays VARCHAR); UUID only via a declared schema (the IPV4 declared-type path). |
| U13 | Skene | **Refuse UUID in the skene writer for now** (fail loud) — the tag number is a permanent on-disk value, and skene's comparator/statistics/encodings need their own decisions. Alternative: full skene support in this work. |
| U14 | Statistics / pruning | **Defer.** No ordinalize for UUID in phase 1 (as DECIMAL128 has none) — no min/max pruning on UUID columns yet; equality pruning via bloom/manifests later. |
| U15 | Generators `UUID()` / `GEN_RANDOM_UUID()` (already listed volatile, not implemented) | **Out of scope**; separate small piece once the type exists. |

## 4. Phases

1. **Type core** — enum, width, OpsTable, compare/hash/sort/gather, MIN/MAX, IN, blends,
   ColumnType/category/spelling, casts + literal coercion, rendering, Python edge, catalog
   regeneration, fail-loud defaults. Tested with `CAST` from VARCHAR (no reader needed).
2. **Readers** — Avro (both forms), Parquet FLBA(16) UUID (direct + dict kinds), Postgres.
3. **Writers** — Parquet UUID annotation; CSV/JSONL text (falls out of phase 1 rendering).
4. **Later** — statistics/pruning, skene, generator functions.

`make dt + st + rt` as well as `make q` + `make slt` after phase 1 (native enum change,
per the DTSTRT rule), and a full `make compile` (ABI change).

## 5. Phase 1 — BUILT (2026-10-14)

`DRAKEN_UUID = 108`; `LogicalCategory.UUID`; canonical `UUID` ColumnType; strict text
form in `draken/core/uuid_text.h` (the one parser/formatter); kernels in
`draken/ops/uuid_ops.h` (hash, memcmp-order compare); 16-byte gathers in
`int128_gather.h` now keep the input tag (they hard-coded DECIMAL128). Casts
VARCHAR/NVARCHAR/VARBINARY ↔ UUID (`cast_string.cpp`, both the C-native and evaluator
paths, and literal folding). A string literal compared with a UUID column is retyped
to a UUID literal in the binder (value, type and schema column together); `IN` lists
pack through the 16-byte membership arm. ORDER BY (int128 lane, sign-flipped memcmp
key), GROUP BY / DISTINCT / joins (hash), COALESCE/IFNULL/IIF and CASE blend UUID with
UUID. CSV/JSONL and nested JSON render lowercase text. Python edge returns `uuid.UUID`.

Built alongside, per §2's hazards: `render_json_scalar` and the CSV/JSONL writer now
FAIL LOUD on a type they cannot render (they wrote `null`/empty before); `jsonl_write`
and `render_json_column` gained `except +` so that failure reaches Python instead of
terminating the process. A third hand-copied width table
(`evaluation.pyx _dv_result_elem_size`) now calls `draken_type_fixed_itemsize`.

**Not built in phase 1:** MIN/MAX over UUID (the aggregate sinks have no
non-numeric extreme path — VARCHAR has none either; refused at plan time with a
clear error). Tests: `tests/sql/test_uuid_type.py` (28).

## 6. Phase 2 — readers BUILT (2026-10-14)

- **Avro** (`rugo/src/avro`): `uuid` on `string` (strict text parse; a malformed value
  fails naming it) and on `fixed(16)` (raw bytes) → DRAKEN_UUID; nested records render
  it as lowercase text; reader-schema defaults supported. `READ_AVRO` types it UUID.
- **Parquet** (`rugo/src/parquet`): `ParseLogicalType` now parses the UUID logical type
  (id 14 — it fell to "skipping unknown logical type" before). FIXED_LEN_BYTE_ARRAY(16)
  + UUID decodes through the existing 16-byte lane with `ReadRaw16` (bytes verbatim,
  no sign extension) and `DecodedColumn.is_uuid`; `materialize_uuid` for standalone
  rugo; `DK_UUID` / `DK_UUID_DICT` for the engine's native scan (footer gate kind
  `"uuid"`); a UUID inside a STRUCT/MAP renders as quoted text (it was base64 while
  the annotation was ignored). Other FLBA (hashes, ...) are still refused.
- **Postgres**: oid 2950 → DRAKEN_UUID (was VARCHAR); the binary wire form is the
  storage form (16 bytes). The type follows the OID, so existing catalog records plan
  as UUID. UUID filters are not pushed to the server (not in PUSHABLE_TYPES).
- **Planner**: `selectivity.hpp comparison_cost` has a UUID arm (it threw for an
  unknown type, refusing the query).

Tests: `tests/unit/connectors/parquet_io/test_flba_uuid.py` (DuckDB oracle, plain +
dict + nested), Avro reader/corpus/READ_AVRO tests, Postgres catalog-schema test. The
live Postgres wire decode is not exercised by any offline test.

## 7. MIN / MAX / ANY_VALUE — BUILT (2026-10-14)

UUID rides the DECIMAL128 extreme lanes (`GBKind::MinMaxD128`, `AggCell.min128/max128`)
as `uuid_order_key` — the memcmp-ordered key with its top bit flipped, so the lanes'
signed compare gives byte order — read in `agg2_read_i128`; `emit_i128_lane_column`
(now told the output type) turns each key back into the 16 bytes via
`uuid_from_order_key`. Grouped and ungrouped. Admitted at plan time
(`compiler.py _uuid_minmax`) and at sink capture for MIN/MAX/ANY_VALUE only — not via
`agg2_operand_supported`, which would also have admitted it to ARRAY_AGG's raw store.
SUM/AVG refused. Windowed MIN/MAX (`OVER`) over UUID is NOT built: the framed-window
sink (`native_window_frame.hpp`) has its own domains and the compiler gate refuses it.
Tests: `tests/sql/test_uuid_type.py`, `test_flba_uuid.py::test_min_max_match_duckdb`.

## 8. Phase 3 — Parquet writer BUILT (2026-10-14)

`LK_UUID` (`_parquet_writer.hpp`, appended to `LogicalKindPq`): a DRAKEN_UUID column is
written FIXED_LEN_BYTE_ARRAY(16), the 16 bytes verbatim, schema LogicalType union
member 14 (empty UUIDType; parquet has no ConvertedType for UUID). Statistics are the
unsigned byte-order min/max — parquet's FLBA type-defined order and the type's own —
stored as the bytes themselves (DECIMAL's signed big-endian path is skipped). The
bloom filter hashes the bytes as written. `patch_columns` maps footer `uuid` back to
LK_UUID and copies donor values verbatim, so rename/add/drop keep the annotation.
No PageIndex for UUID (FLBA is excluded from `page_index_bound_kind`, as DECIMAL is).
rugo's and the engine's own bloom probes still decline FLBA (no pruning on UUID
equality from blooms yet). ARRAY<UUID> is not written. Tests:
`tests/rugo/test_parquet_uuid_write.py` (DuckDB oracle: schema, stats, values,
bloom lookup, patch rename/add).

## 9. CSV / JSONL declared UUID + Postgres verification (2026-10-14)

- `explicit_schema={"col": "UUID"}` (rugo `declared_type.cpp` / `declared_parse.hpp`):
  strict text → DRAKEN_UUID in both readers; a mismatch raises (ValueError for JSONL,
  RuntimeError for CSV, as for every declared type). Undeclared UUID text stays
  VARCHAR (ruling U12 — no inference).
- **Pushed reader predicates on a declared UUID column compare the 16 parsed bytes**
  (`predicate_literal.hpp` `uuid_literal_bytes` / `apply_op_uuid`; CSV `PRED_UUID`;
  JSONL `Predicate::is_uuid`, set in `prepare_predicate` from `explicit_schema`). Found
  while testing: types outside the literal contract (DATE, TIMESTAMP, DECIMAL, IPV4,
  ...) keep "the existing comparison" on the field's RAW TEXT, which for UUID answered
  `= 'ffff...'` with no rows against a stored 'FFFF...'. Now case never matters and all
  six comparisons follow byte (= UUID) order; a non-UUID literal raises. The JSONL
  raw-text PREFILTER (`choose_prefilter_plan`) skips declared UUID columns for the same
  reason. The other out-of-contract declared types still use the raw-text compare —
  unexamined.
- Postgres verified against a throwaway local Postgres 17 (Colima): 2,000 rows with
  nulls — values, full sort order, grouped MIN/MAX + null counts, `=` (either case),
  IN, CAST — all equal Postgres's own answers, via NativePostgresScanSource. The
  existing live suite (system relations only, read-only) passes 34/34. There is no
  automated UUID Postgres test: the live suite has no uuid relation and the repo has
  no Python Postgres client to build one.
