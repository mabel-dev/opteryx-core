# UUID and nested columns — one rule for Iceberg tables and `READ_*`

Status: DESIGN — nothing is built. Written 2026-10-25 at the architect's request ("rule how they
surface for Iceberg tables and READ_* (Avro, ORC, Parquet consistency), design doc first").
Repos: `opteryx-core`, `opteryx-iceberg` (the type mapping lives there).
Builds on: `UUID_TYPE_DESIGN.md` (U1–U15, ruled), the nested-as-JSON rulings of 2026-09-30
(Parquet), `AVRO_READER_DESIGN.md` §15–17, `ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md`, `ORC_READER_DESIGN.md`.

---

## 0. Summary

- **UUID is already a first-class type** (`DRAKEN_UUID = 108`, RFC byte order, `uuid.UUID` at the Python
  edge) and the Parquet and Avro readers already produce it. What is missing is *plumbing*:
  `opteryx-iceberg` still binds Iceberg `uuid` as VARCHAR, the Avro table reader schema and the ORC
  reader do not know it. **Binding it as `UUID` is enough for Parquet** — measured, §1.1.
- **Nested columns already have a ruled surface** — `STRUCT` / `MAP` → JSON text (`NVARCHAR`),
  `LIST` of scalars → `ARRAY`. Parquet and Avro implement it; **ORC refuses nested, the Iceberg
  backend refuses nested, and a LIST of STRUCT/MAP is rendered by Avro but refused by Parquet.**
  The design is: one rule, applied everywhere (§3), plus the one genuinely hard question, which is
  *what names the keys of the JSON for an Iceberg table whose nested fields have been renamed* (§3.3).

## 1. Where things stand (measured, 2026-10-25)

| Iceberg type | `opteryx-iceberg` | Parquet file | Avro file (table scan / `READ_AVRO`) | ORC file (table scan / `READ_ORC`) |
|---|---|---|---|---|
| `uuid` | binds **VARCHAR** → the scan refuses it loudly ("footer_gate … fixed_len_byte_array") | reads as `UUID` | file side ✓ (`string` or `fixed(16)` + `uuid`); table side ✗ (not in `avro_io._avro_type`) | ✗ (`iceberg.binary-type=UUID` refused) |
| `fixed(L)` | ✗ `NotImplementedError` | FLBA(L ≠ 16, non-decimal) refused | `fixed` → VARBINARY file side; table side ✗ | ✗ refused |
| `time` | ✗ | not checked | file side ✓ (`time-*` → TIME); table side ✗ (`avro_io` omits TIME) | ✓ (`LONG` + `iceberg.long-type=TIME`) |
| `timestamp_ns` / `timestamptz_ns` (v3) | ✗ | not checked | refused (`timestamp-nanos`) | refused (`iceberg.timestamp-unit=NANOS`) |
| `struct`, `map` | ✗ | ✓ JSON text (`NVARCHAR`) | file side ✓ JSON text; table side ✗ | ✗ refused |
| `list<scalar>` | ✗ | ✓ `ARRAY` | file side ✓ (plain scalars only); table side ✗ | ✗ refused |
| `list<struct|map|list>` | ✗ | ✗ refused ("a LIST of STRUCT/MAP is not rendered as JSON") | file side ✓ JSON text | ✗ refused |
| `variant`, `geometry`, `geography`, `unknown` | ✗ | — | — | — |

Two of these cells matter most and were checked by experiment (in-memory patch of the mapping, nothing
edited):

### 1.1 UUID

A pyiceberg table with a `uuid` column, written to Parquet:
- as shipped: `SELECT id, u FROM t` → `NativeScanRefusedError … column 'u' (kind 'varchar') … has physical
  type 'fixed_len_byte_array'`. **Loud, not wrong.**
- with `UUIDType` bound as `"UUID"` in `opteryx_iceberg/dataset.py`: `SELECT id, u` returns
  `UUID('550e8400-…')` values; `WHERE u = '550e8400-e29b-41d4-a716-446655440000'` returns the row
  (the literal is retyped at bind, U7); `MAX(u)` works. **A one-line mapping, no engine change, for Parquet.**

### 1.2 Nested, through Parquet

A pyiceberg table with `struct<a,b>`, `list<long>`, `map<string,long>` and `list<struct<x>>`, mapped in
memory as `NVARCHAR` / `ARRAY<INT64>` / `NVARCHAR` / `NVARCHAR`:
- `s` → `'{"a":1,"b":"x"}'`, `None`; `l` → `[1, 2, 3]`, `[]`; `m` → `'{"k":5}'`, `None` — **correct**;
- `ls` (`list<struct>`) → `NotImplementedError … a LIST of STRUCT/MAP is not rendered as JSON` — loud.

So for Parquet-backed Iceberg tables the *mechanism* exists; the gaps are the mapping, `list<struct>`,
and the names question below.

## 2. UUID — the rules

| # | Rule |
|---|---|
| UD1 | **Iceberg `uuid` → Opteryx `UUID`.** `opteryx-iceberg` maps it (it currently maps it to VARCHAR, a choice made before the type existed); bounds stay omitted (U14: no ordinal for UUID, no pruning). |
| UD2 | **Avro table scan:** the reader schema field for a `UUID` column is `fixed(16)` with `logicalType: "uuid"` — what Iceberg's Avro writer emits. (`fixed` and `string` do not promote to each other, so a table scan cannot also accept a `string`-uuid file; `READ_AVRO` on a non-Iceberg file already accepts both, because it reads the file's own schema.) |
| UD3 | **ORC:** a `BINARY` column carrying `iceberg.binary-type=UUID` reads as `UUID`; **every value must be exactly 16 bytes or the query fails naming the column.** A `BINARY` without the attribute stays `VARBINARY` — generic ORC (Hive, Spark) has no UUID type, so `READ_ORC` on such a file never invents one. |
| UD4 | **`READ_*` consistency:** `READ_PARQUET` (FLBA(16) + UUID annotation) and `READ_AVRO` already produce `UUID`; `READ_JSONL` / `READ_CSV` keep U12 (no inference — a uuid-shaped string is VARCHAR; `UUID` only via a declared schema). |
| UD5 | Output, casts, comparison, hashing, `MIN`/`MAX`, Python edge: **unchanged** — U2–U9. |

Same-class gaps found while reading (a *type mapping* each, not UUID), offered for the same change:

| # | Question | Recommendation |
|---|---|---|
| UD6 | `time` → `TIME` (µs). Avro (`time-micros`) and ORC (`iceberg.long-type=TIME`) read it; the Iceberg backend and the *Avro table reader schema* don't map it. Parquet `TIME(MICROS)` not checked. | **Yes**, with a Parquet check first. |
| UD7 | `fixed(L)` → `VARBINARY`, length checked per value. Avro and ORC can read it; Parquet FLBA(L) that is neither decimal nor UUID is refused by the engine's reader today. | **Avro and ORC now; Parquet stays refused (loud)** until the FLBA reader is extended — a different change. |
| UD8 | `timestamp_ns` (v3) | **Keep refused everywhere** (no nanosecond timestamp type). |

## 3. Nested columns — the rules

### 3.1 One surfacing rule, for every format and for Iceberg

| Source shape | Opteryx |
|---|---|
| `STRUCT`, `MAP` | `NVARCHAR` — one JSON **object** per row (a MAP's keys must be strings; a non-string key is refused at plan time, as for Parquet) |
| `LIST` of a scalar the engine's `ARRAY` can hold | `ARRAY<scalar>` |
| `LIST` of `STRUCT` / `MAP` / `LIST` (any deeper nesting) | `NVARCHAR` — one JSON **array** per row |
| `LIST` of a scalar `ARRAY` cannot hold (a date, a decimal, an enum…) | `NVARCHAR` JSON array — **proposed**; Avro refuses this today ("no Draken ARRAY child for it") |
| `UNION` | refused, except Avro's `["null", T]` (which is just nullability) |

JSON rendering is the single shared renderer's job (`draken/interop/value_format.hpp`), and its rules
are the 2026-09-30 rulings: members in schema order; a NULL field is `null`; a NULL top-level group is
SQL NULL; DECIMAL a bare number; DATE / TIME / TIMESTAMP quoted ISO text; BINARY quoted base64; NaN and
Infinity `null`; UUID quoted lowercase text (U4). **ORC must call that renderer, not grow a second one.**

### 3.2 What each format needs to meet the rule

| Format | State | Work |
|---|---|---|
| Parquet | STRUCT / MAP / `LIST<scalar>` ✓ | **`LIST<STRUCT|MAP|LIST>` → JSON array** (`nested::assemble`'s Dremel walk, a repetition-level extension). Required for real Iceberg tables: events with arrays of structs are the common shape. |
| Avro | records / maps / arrays-of-nested → JSON ✓ (file side) | the **table-side reader schema** must be able to express a nested column (§3.3); arrays of logical-typed scalars → JSON (if the rule above is accepted). |
| ORC | all nested refused | **decode `STRUCT` / `LIST` / `MAP`** (PRESENT, LENGTH and child streams — columnar, simpler than Dremel) into the shared renderer. `UNION` stays refused. |
| Iceberg backend | all nested refused at schema time | map `struct`/`map` → `NVARCHAR`, `list<scalar>` → `ARRAY<elem>`, `list<nested>` → `NVARCHAR` — **gated on §3.3**. |

### 3.3 The hard question — what names the JSON keys for an Iceberg table?

Iceberg identifies a nested field by **id**; names change. A file written before a rename carries the old
name. A JSON object is made of *names*. Three ways to render a struct column of an Iceberg table:

| | What | Behaviour on a table whose nested field was renamed / added after the file was written |
|---|---|---|
| **(a)** file names | render with the names in each file | **Silently wrong across files**: old files say `old_name`, new files `new_name`; a query cannot tell. Rejected. |
| **(b)** verify, or refuse | the table's nested tree (id → name, recursively) is handed to the scan; each file's nested ids and names must match it exactly; a file that does not fails the query naming the field and file | Correct where it answers, **loud where it cannot**; handles the common case (nested fields never evolved) and never lies. |
| **(c)** remap by id | render with the *table's* names, mapping each file's fields by id; a field a file lacks is `null` | Correct under any evolution (the Iceberg semantics), and the most work: every nested reader takes a reader-side tree. |

**Recommendation: (b) first, (c) when an evolved nested table is actually in front of us.** (b) needs
one new thing: the table's nested tree must reach the scan. Today a column crosses as a flat
`ColumnDescriptor(name, type, nullable, field_id)`; it would gain an optional `nested` field (a frozen
tuple of `(field_id, name, type, children)`), populated by `opteryx-iceberg`, read by the Parquet / Avro /
ORC readers' verification step. **`READ_*` and folder datasets have no ids and no table — the file *is*
the schema, so file names are correct there (the rule is (a)).** (a) is rejected only for Iceberg
tables, where a schema exists to disagree with.

Until (b) is built, **Iceberg nested columns stay refused** (loud), exactly as now. Mapping them early
would be the silent-drift trap above.

### 3.4 Out of scope

Pushdown into nested paths (the column is JSON text; element access uses the existing JSON functions);
`variant`, `geometry`, `geography` (refused, named); `ARRAY<UUID>` writing (open in the UUID doc);
windowed `MIN`/`MAX` over UUID (open in the UUID doc); `s3a://`-style schemes (a different gap).

## 4. Tests

- **UUID:** `opteryx-iceberg` end to end for Parquet (the experiment of §1.1 as a test: select, equality
  with a string literal, `MAX`, `ORDER BY`, `GROUP BY`, a NULL), Avro (a fixture with `fixed(16)` +
  `uuid`), ORC (`BINARY` + attribute; a 15-byte value fails naming the column); `READ_ORC` of a plain
  `BINARY` stays `VARBINARY`.
- **Nested, surfacing:** for each format, the same data (struct, map, `list<long>`, `list<struct>`,
  nested `list<list<…>>`, NULL at each level, an empty list, a unicode key) read through `READ_*` and
  compared with one JSON oracle — the five-way consistency the architect asked for, asserted rather than
  described.
- **Nested, Iceberg (b):** an evolved table (renamed nested field) must **fail naming the field**; an
  unevolved one reads.
- Parquet `LIST<STRUCT>`: random shapes against the Python oracle already used by
  `tests/unit/operators/test_nested_json_scan.py`.
- Everything through `make q`, `make slt`.

## 5. Decisions

| # | Question | Recommendation |
|---|---|---|
| UD1–UD5 | UUID rules above | **Accept.** |
| UD6 | `time` mapping | **Yes**, after a Parquet check. |
| UD7 | `fixed(L)` for Avro and ORC; Parquet stays refused | **Yes.** |
| UD8 | `timestamp_ns` stays refused | **Yes.** |
| ND1 | The single surfacing rule (§3.1) | **Accept.** |
| ND2 | A `LIST` of a non-`ARRAY`-able scalar → JSON array (Avro would stop refusing it) | **Yes.** |
| ND3 | Iceberg nested names: (b) verify-or-refuse now, (c) later | **(b).** Until it lands, Iceberg nested stays refused. |
| ND4 | Parquet `LIST<STRUCT|MAP|LIST>` → JSON | **Yes** — it blocks real Iceberg tables. |
| ND5 | ORC nested decode, through the shared renderer | **Yes.** |
| ND6 | `ColumnDescriptor` gains an optional `nested` tree | **Yes** (an additive field; it is what (b) needs). |

## 6. Build order (after the rulings)

1. **UUID** (small, mostly mapping): `opteryx-iceberg` UUID mapping (+ `time`, `fixed` if UD6 / UD7 accepted);
   `avro_io._avro_type`; ORC `iceberg.binary-type=UUID` / `FIXED`; tests of §4.
2. **ORC nested decode** + the shared-renderer consistency tests across `READ_PARQUET` / `READ_AVRO` /
   `READ_ORC` for the shapes Parquet already supports.
3. **Parquet `LIST<STRUCT|MAP|LIST>`** and **Avro arrays of logical-typed scalars**.
4. **`ColumnDescriptor.nested`** + the verify-or-refuse step in the three readers + `opteryx-iceberg` nested
   mapping; the evolved-table tests.

## 7. Built — step 1 (UUID, time, fixed), 2026-10-25

Accepted UD1–UD8 and ND1–ND6 as recommended; step 1 of §6 is built:

- **UUID**: ORC reads `iceberg.binary-type=UUID` as `DRAKEN_UUID` (every value must be 16 bytes, else the
  failure names the length); the Avro table reader schema writes `fixed(16)` + `logicalType: uuid`;
  `opteryx-iceberg` binds `uuid` as `UUID`. Parquet already read it.
- **fixed(L)**: ORC reads `iceberg.binary-type=FIXED` + `iceberg.length` as VARBINARY and checks every value
  against L (a missing/invalid length, or any other `binary-type`, is refused naming it). The Avro table
  reader schema can only say `bytes`, so rugo's Avro resolver gains **one non-spec promotion**, a file's
  `fixed` read as `bytes` (`avro_reader.cpp`, `promotes`); the file's own fixed size is still enforced when
  both sides are `fixed`. `opteryx-iceberg` binds `fixed(L)` as VARBINARY. Parquet `fixed_len_byte_array`
  that is neither decimal nor uuid is still refused by core's Parquet reader, loudly.
- **Two decimals of one shape** in one Avro reader schema used to define the same named type twice;
  each column's named type is now `f<field id>_type`.
- **`time` is HELD (UD6, the Parquet check failed).** The engine reads a Parquet `TIME` column as
  `INT64` microseconds, and so does `CAST(... AS TIME)` (`INT64` + `LogicalKind::TIME`; the expression VM's
  `cast time->string` expects `TIME64` and rejects it), while the Avro (`time-micros`/`time-millis`) and ORC
  (`iceberg.long-type=TIME`) readers produce `TIME64` (`datetime.time`). One Iceberg type must not mean two
  things by file format, so `opteryx-iceberg` refuses an Iceberg `time` column at schema time, naming it.
  Fixing it is an engine-wide TIME representation question (which of the two is canonical), not a reader fix.
- Tests: `tests/rugo/test_orc_uuid_fixed.py`, `tests/unit/connectors/test_avro_table_reader_schema.py`,
  `tests/unit/connectors/test_read_orc.py`; opteryx-iceberg `tests/test_uuid_time_fixed_columns.py`.

## 8. Built — step 2 (ORC nested decode), 2026-10-25

`STRUCT` / `LIST` / `MAP` ORC columns read, through `READ_ORC`, a folder of `.orc` files, and (once
`opteryx-iceberg` maps nested types — step 4) Iceberg tables:

- **Surfacing, as §3.1:** `STRUCT` / `MAP` → `NVARCHAR` (one JSON object per row); a `LIST` of `BOOLEAN` /
  `INT` / `LONG` / `FLOAT` / `DOUBLE` / string / plain binary → `ARRAY<scalar>`; every other `LIST` (of
  nested, of `BYTE` / `SHORT`, of a date, decimal, timestamp, uuid, fixed …) → `NVARCHAR` (one JSON array).
  `UNION` is refused naming the column. A `MAP` whose keys are not strings is refused naming the key kind.
  Nesting deeper than 64 levels is refused.
- **Decoding** (`rugo/src/orc/orc_reader.cpp`, `NestedNode`): the subtree is decoded top-down. ORC stores a
  child entry only for each NON-NULL parent entry (a struct's children are as long as the struct has
  non-null rows; a list's or map's children as long as the sum of the non-null lengths), so each node's
  entry count is known before it is read. Primitives are decoded by the SAME `decode_leaf` as a top-level
  column (encodings, dictionaries, timestamps, decimals, the Iceberg `uuid` / `fixed` / `time` attributes);
  nothing is decoded twice or differently because it sits inside a struct.
- **Rendering** uses the shared `rugo_text` functions (`json_string`, `fmt_int64`, `fmt_double`/`fmt_float`
  with `fp_canon`, `fmt_decimal`, `fmt_date_quoted`, `fmt_time_quoted`, `fmt_timestamp_quoted`,
  `fmt_uuid_quoted`, base64 via `bintob64`) — the functions the Avro reader's defaults path and the Parquet
  nested renderer call. There is no ORC-specific renderer. Rules: members in schema order; a NULL field is
  `null`; a NULL top-level group is SQL NULL; NaN / Infinity are `null`; DECIMAL a bare number.
- **Plumbing:** `ReaderColumn.child_type` / `FileColumn.child_type`+`nested`, `read_orc_metadata`
  (`nested`, `child_type`), `read_orc` reader tuples take an optional 7th element (the ARRAY element),
  `OrcScanSpec.reader_child_types`, `OrcScanPlan` reader tuples are 7 long, `orc_io` spells NVARCHAR / ARRAY.
- **Tests:** `tests/rugo/test_orc_nested.py` (random shapes × 5 codecs × RLE v1/v2 × direct/dictionary
  strings × many stripes, against a Python oracle), `tests/unit/connectors/test_nested_columns_across_formats.py`
  (the same values through READ_PARQUET, READ_AVRO, READ_ORC, an ORC folder and an Avro folder, against one
  oracle; Parquet skips the two columns it cannot render yet — step 3).
- **Still open:** Parquet `LIST<STRUCT|MAP|LIST>` and Avro arrays of logical-typed scalars (step 3);
  Iceberg nested columns stay refused until the table's nested tree can be verified (step 4); a `LIST` of
  `BYTE` / `SHORT` is JSON here, whereas Parquet's reader may type the same shape as `ARRAY<INT16>` — checked
  only for the shapes the five-way test covers.

## 9. Built — step 3 (Parquet LIST of STRUCT / MAP, Avro arrays of logical scalars), 2026-10-25

- **Parquet `LIST<STRUCT>` / `LIST<MAP>` (any depth of lists above the group) → one JSON array per row.**
  It used to read as **all NULLs, silently**: rugo's footer typed the column `array<int64>` (it took the
  first leaf of the struct, `ResolveArrayLogicalType`), and the leaves' chunk names (`lst.list.element.k`)
  match no column called `lst`, so the scan treated the column as absent from the file. Now
  `metadata.cpp::ListHoldsGroups` reports such a LIST as `json` (as a STRUCT is), the column reaches the
  existing STRUCT/MAP path, and `nested_json.cpp::build_plan` accepts a top-level `array` (its Dremel
  assembler already handled a list at any depth *inside* a struct). Same JSON as ORC and Avro.
- **Avro `array<logical type | enum>` → JSON array** (ND2): `is_json` is now "an array whose items are not
  a plain scalar"; the refusal ("no Draken ARRAY child for it") is gone.
- **RULED ("as per parquet"): a LIST of lists of a plain scalar is a nested `ARRAY`, in every format.**
  ND1/ND4 had said JSON array; the Parquet scan already builds `ARRAY<ARRAY<…>>` deliberately and with
  tests, so ORC and Avro follow it. The rule is now: a LIST (to any depth) whose bottom element is a plain
  scalar is `ARRAY` nested to that depth; a LIST with a STRUCT / MAP at the bottom, or (Avro) a logical
  type or enum, is one JSON array per row. Built: `AvroColumn.child_col` (an Array column that is itself an
  Array's child; wrapped through `draken_vector_own_array_child`, and natively through a recursive
  `array_child_owner`), `AvroColumnType.array_depth`, ORC `FileColumn.array_depth` / `fill_array`, and the
  declared type is `ARRAY(ARRAY(elem))` for ORC and Avro. Known wart, not changed: rugo's Parquet footer
  still reports `list<list<int64>>` as `array<int64>` (depth lost), so Parquet's declared type is one
  level short of what it returns.
- **Tests:** `tests/unit/operators/test_nested_json_scan.py::test_random_lists_of_groups_match_the_oracle`
  (40 random shapes, row groups / dictionary / codecs), `tests/unit/connectors/test_avro_arrays_of_logical_scalars.py`,
  and `lst` joins the five-way test for Parquet.
