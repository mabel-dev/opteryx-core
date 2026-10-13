# UUID column type — design (DRAFT, awaiting rulings)

2026-10-13. Ruled so far: **UUID is a first-class type** (option 3 of three — rejected:
uuid → VARCHAR text, uuid → VARBINARY). Everything below the rulings line is proposal.

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

| # | Question | Proposal |
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
