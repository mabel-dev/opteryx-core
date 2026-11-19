# Design: Parquet VARIANT (shredded) read + write

**Status:** RULED 2026-11-19. §6 step 1 (whole-value read) BUILT 2026-11-19; steps 2-4 not started.
**Rulings:** D1 yes (JSON text in memory) · D2 yes (automatic shredding, no setting) ·
D3 as recommended (INT64 / own-scale DECIMAL; the rest stored as double, canonical
rendering accepted) · D4 keep (string-form VARIANT files stay readable, no migration) ·
D5 deferred — build first, then decide · D6 as recommended (shredded members in schema
order, then residual).
**Builds on:** `docs/json_variant_type_plan.md` (VARIANT type, `->`/`->>` rulings — unchanged
here), `rugo/src/parquet/nested_json.hpp` (Dremel assembly of nested groups → JSON text),
`JsonlNestedExtractionStrategy` (path extraction pushed into the scan).
**Purpose:** a JSONBench-compliant storage form. JSONBench disallows "flattening JSON into
multiple non-JSON columns at insertion time"; a shredded VARIANT column is still one
JSON-typed column, the same thing ClickHouse's `JSON` type does inside its own storage.

---

## 1. What a Parquet VARIANT is

A representation of **one JSON value per row — any value**: object, array, string, number,
bool, null. Not only objects. It is a superset of JSON: its scalars also include int8–64,
float, double, decimal4/8/16, date, time, timestamp (µs/ns, UTC/NTZ), binary, uuid.

Physically a VARIANT column is a Parquet group annotated with the `VARIANT` logical type:

```
optional group data (VARIANT) {
  required binary metadata;      -- per-row dictionary of object field names
  optional binary value;         -- the value (or the part not shredded), variant binary encoding
  optional <T>    typed_value;   -- the shredded part (absent = unshredded column)
}
```

Variant binary encoding: one header byte per value (basic type: primitive / short string /
object / array), objects store field ids into `metadata` + offsets (field ids sorted by name
when the metadata says its strings are sorted, so lookup is a binary search), arrays store
offsets. No text, no parsing.

**Shredding** puts paths into real typed Parquet columns. For an object, `typed_value` is a
group with one member per shredded field, and each member is again `{value, typed_value}`:

```
optional group data (VARIANT) {
  required binary metadata;
  optional binary value;                      -- residual: fields NOT shredded
  optional group typed_value {
    required group kind   { optional binary value; optional binary typed_value (STRING); }
    required group commit {
      optional binary value;
      optional group typed_value {
        required group collection { optional binary value; optional binary typed_value (STRING); }
        required group operation  { optional binary value; optional binary typed_value (STRING); }
      }
    }
  }
}
```

Per row, at every level: `typed_value` set → the value is the typed column; `value` set → the
value is in binary (the row's type didn't match the shredded type); both set → a partially
shredded object (residual fields in `value`, shredded ones in `typed_value`); both null →
the field is missing (valid only for object members). A JSON `null` is a variant null in
`value`, never a Parquet null.

So `data->'commit'->>'collection'` reads one dictionary-encoded string column with real
statistics, plus residual columns that are usually all-null.

## 2. Where we are today

- `DRAKEN_VARIANT` (65) = **JSON text** in German-string storage. Extraction-only operators.
- JSONL reader emits VARIANT for object columns; one-level `->>` is pushed into rugo.
- Parquet writer writes a VARIANT column as a plain UTF8 string; reading it back yields a
  string unless a catalog declares VARIANT.
- Parquet reader folds STRUCT/MAP groups into JSON text (`nested_json.hpp`), with fixed
  rendering rules (timestamps quoted ISO-8601, binary base64, decimal bare, NaN → `null`).
- No reader or writer understands the `VARIANT` annotation or the variant binary encoding.

## 3. Decisions

**D1 — In-memory representation stays JSON text.** *Recommend: yes.*
The scan renders variant → JSON text for whole-value reads; path reads never render the
document (§4.2). Keeps the frozen DrakenVector ABI, every existing kernel, and the
`->`/`->>` rulings untouched. Cost: `SELECT data` pays a render per row, and extended
scalars (timestamp, binary, uuid, decimal) lose their type in a whole-value read — they
render by the `nested_json.hpp` rules. `->>` already returns text, so path reads lose
nothing. *Alternative:* VARIANT vectors carry variant binary + metadata; faster
whole-value and chained extraction, but a new physical layout, new `->`/`->>` kernels,
and the JSONL reader would need to emit binary. Defer unless whole-value reads matter.

**D2 — Shredding is automatic, no setting.** *Recommend: yes.*
JSONBench runs with default settings, and nobody asked for a knob. The writer infers the
shredding schema per file from the first buffered row group (§5.2). The fixed limits
(path count, depth, presence threshold) are compile-time constants.
*Alternative:* user-declared shredding spec on the table.

**D3 — Number fidelity on shred.** JSON numbers have no type; a shredded path needs one.
`->>` compares *rendered text* (JSONL ruling), so a lossy choice changes answers between
the JSONL and Parquet forms of the same data (`1.10` → `1.1`, `1e2` → `100`).
*Recommend:* integer literals in int64 range → INT64; other numbers with ≤38 significant
digits and no exponent → DECIMAL at the literal's own scale (keeps `1.10`). Both render back
to the original text. What's left — exponent forms (`1e2`), `-0`, beyond int64/decimal38 —
has no type that keeps its text, so either **(a)** store it as a variant double and accept
canonical rendering (`1e2` → `100`), or **(b)** refuse to write the row.
*Recommend (a), documented* — rare, and (b) fails ingest on valid JSON.

**D4 — Existing VARIANT-as-string files.** Once the writer emits real VARIANT, older
files hold VARIANT as UTF8 text. *Recommend:* a catalog column declared VARIANT reads
either form per file (string files → text as today). No rewrite, no migration.

**D5 — How a JSONBench table gets one VARIANT column.** READ_JSONL splits top-level keys
into columns — that *is* flattening, so loading through it is not compliant. The table
must be `data VARIANT` holding the whole line. Needs a way to read a JSONL line as one
VARIANT value. *Options:* a READ_JSONL mode that yields the whole line as one VARIANT
column, or a separate read function. *Architect's call — I haven't found an existing way.*

**D6 — Field order in a reassembled object** (whole-value read of a partially shredded
object). *Recommend:* shredded members in schema order, then residual members in binary
order. JSON object order carries no meaning; `nested_json.hpp` already took "schema order,
cheapest".

## 4. Read path (rugo, native)

### 4.1 Footer + whole-value reads
1. Footer: recognise the `VARIANT` member of the `LogicalType` union and validate the group
   shape (`metadata` required binary, `value` optional binary, `typed_value` optional, the
   nested member groups shaped likewise). Anything else is refused, naming the column —
   never read as a struct.
2. Variant binary decoder (new, C++, no Python): metadata dictionary, all primitive types,
   short strings, objects (binary search when sorted, linear otherwise), arrays. Renders to
   JSON text with the `nested_json.hpp` rules (same `fmt_*`, `json_string`, base64, NaN canon).
3. Whole-value read = Dremel assembly over `metadata` + `value` + the `typed_value` tree (the
   same lockstep walk as `nested_json` — extend that plan, don't build a second assembler),
   merging residual and shredded members per D6. Output: one DRAKEN_VARIANT text value per row.
   Group null → SQL NULL. A top-level Variant null, or a present group with `value` and
   `typed_value` both null (the spec reads that as Variant null where a value is required),
   is SQL NULL too — the same collapse `->` applies to a JSON null. Nested, both stay `null`.

**Built (step 1):** `rugo/src/parquet/variant_binary.hpp` (decoder), VARIANT nodes in
`nested_json.cpp` (plan + assembly), `metadata.cpp` (LogicalType 16 → `variant`),
`_rugo_schema.py` (`variant` → VARIANT). Tests: `tests/unit/connectors/parquet_io/test_variant.py`
(DuckDB-written files: unshredded, auto-shredded, partially shredded).

**Found while building:**
- DuckDB shreds arrays, so the READER handles shredded arrays (and shredded objects inside
  them). Only our writer skips them (§5.2 shortcut).
- DuckDB's objects did not come back in name order, though the spec says field ids are listed
  sorted by name. Path navigation in binary (§4.2) must not binary-search a foreign file's
  object on trust.
- The Parquet scan tagged a VARIANT column's vectors VARCHAR, so results reported VARCHAR
  while the planner bound VARIANT. **Ruled 2026-11-19: tag 65.** `_classify_scan_columns` and
  `_null_fill_type` (`compiler.py`) now pass VARIANT through, as the JSONL reader does.
- TIMESTAMP without time zone renders with `+00:00`, as nested STRUCT leaves already do.

### 4.2 Path reads (`data->'a'->>'b'`)
1. Optimizer: generalise the `JsonlNestedExtractionStrategy` rewrite to Parquet VARIANT
   columns and to **chains** (`->` … `->>`); JSONBench paths are two levels deep and the
   JSONL strategy only takes one token. Same contract: the rewritten column is scan-provided
   and must be **byte-identical** to the unpushed `->`/`->>` over the whole-value rendering —
   tested on/off, like the JSONL strategy.
2. Per file, rugo resolves the path against that file's shredding (files differ):
   - **Fully shredded to a leaf:** read the leaf's `typed_value` and `value`, plus each
     ancestor's `value`. Per row: leaf `typed_value` → render the scalar (`->>`: strings
     unquoted, else text); otherwise the first non-null `value` on the path is decoded and
     navigated in binary. Missing → SQL NULL; variant null → SQL NULL (existing ruling).
   - **Shredded to an ancestor only:** read that ancestor's residual `value` and navigate the
     rest in binary.
   - **Not shredded:** read `metadata` + `value` and navigate in binary. Still no JSON parse.
   A residual column whose chunk has `null_count == num_values` is not decoded.
3. Row-group / page pruning on a pushed `->>` predicate: allowed only when the leaf is a
   STRING shredded column **and** the leaf `value` plus every ancestor `value` are all-null in
   that row group (by statistics). Otherwise some rows live in binary and the stats don't
   cover them. Non-string leaves don't prune in this work (text compares against int stats
   need a canonical-text proof — the "provably safe" rule).

### 4.3 Step 2 build contract — PROPOSED 2026-11-19, awaiting ruling

Found by reading the code the step has to fit into. **P#** are the architect's.

**How a path column reaches rugo.** The Parquet scan asks rugo for columns by NAME
(`names = [sc.name ...]`, `compiler.py`), for `READ_PARQUET` and catalog scans alike. A pushed
path is one more name, in the notation the JSONL pushdown already uses:
`data->'commit'->>'collection'`. rugo parses it (one function, shared with the footer gate),
finds the VARIANT group `data`, and resolves the keys against THAT file's shredding:

- while the current level's `typed_value` is an object group holding the next key, step into
  that field — its sibling `value` (the residual) is NOT read. The spec allows exactly this
  ("readers do not need to read the value column if typed_value fields satisfy the required
  fields"), and it is the whole win: the top-level residual holds everything unshredded.
- keys left over → read that level's `value` only and look the rest up in Variant binary
  (linear key search — DuckDB's objects are not name-ordered).
- no keys left → read that pair's whole subtree and render it (`->`: JSON text; `->>`: a
  string unquoted, anything else as its text; JSON null → SQL NULL — the existing rulings).

Only `metadata` plus those leaves are decoded. Intermediate steps must be `->`; only the
last may be `->>` (`(a->>'b')->>'c'` re-parses a string, a different operation).

**P1 — Scope: `READ_PARQUET` only, for now.** The JSONL pushdown is `READ_JSONL`-only by the
same precedent. A catalog table declared VARIANT can hold its files as plain JSON strings
(today's writer; D4 keeps them readable), and rugo has no JSON parser to walk a path through
text (yyjson is deliberately not linked). On `READ_PARQUET` the column is only bound VARIANT
when the footer says so. A file in the same glob that holds the column as anything else is
REFUSED by the footer gate, naming the file — never NULL-filled. Catalog tables need the
mixed-form answer first (P5).

**P2 — A new declared field on the FunctionDataset step:** `parquet_physical_by_identity`
(identity → the name rugo is asked for), mirroring `jsonl_physical_by_identity`. The scan
uses it where it now uses `sc.name`.

**P3 — Spec-violating files.** A file that repeats a shredded field's name inside the
residual `value` is invalid by the spec. The whole-value read renders both; the path read
sees only the shredded one. The spec itself says such reads "may be inconsistent". Accept.

**P4 — Strategy shape.** The JSONL strategy's rewrite is shared, not copied: its body becomes
the common part, `JsonlNestedExtractionStrategy` keeps its name, flag and one-level rule, and
a sibling `ParquetVariantExtractionStrategy` adds chains, with its own
`FEATURE_DISABLE_PARQUET_VARIANT_EXTRACTION` so on/off equivalence is testable per format.

**P5 — (later, not this step) path reads over string-form files in catalog tables.** Options:
the native Source runs draken's extract kernel on the string column for those files
(per-file, native, same answers by construction), or the compiler un-pushes the whole scan
when any file is string-form. Needed before the writer (step 3) puts VARIANT-form files
into tables that already hold string-form ones.

**Not in this step:** skipping `metadata` when a row group's residual chunks are all-null
(a per-row-group read plan; decide from a profile in step 4), and pruning (step 2b, below).

## 5. Write path (rugo writer, native)

### 5.1 Encoding
JSON text → variant binary encoder (new, C++, yyjson parse — measured faster than our
structural scan, see memory `yyjson_beats_rugo…`). Per-row `metadata` holds that row's
residual field names, sorted; dictionary encoding on the `metadata` column absorbs the
repetition.

### 5.2 Shredding inference (per file)
Over the first buffered row group: walk objects (not arrays — see shortcut), collect
`path → (presence count, type histogram)`. Shred a path when presence and its dominant type
both clear a fixed threshold, up to a fixed path cap and depth cap. Types: string → STRING,
bool → BOOLEAN, numbers per D3. The schema is then fixed for the file; later rows that don't
match go to `value` — always correct, only slower. Typed leaves get normal statistics and
page indexes.

`shortcut: arrays are never shredded, upgrade when a workload filters inside arrays.`

### 5.3 What writes VARIANT
Every rugo Parquet write of a DRAKEN_VARIANT column (CTAS, INSERT, dataset writes, OPTIMIZE
rewrites) — one path, no option.

## 6. Sequencing

1. Read: footer annotation, binary decoder, whole-value assembly. Test against variant
   files from another writer (Apache `parquet-testing` — check it carries shredded cases;
   generating them in `dev/` with another engine is the fallback).
2. Read: chained path pushdown + per-file resolution + string-leaf pruning; on/off equivalence.
3. Write: encoder, inference, round-trip tests (JSONL → Parquet VARIANT → identical `->`/`->>`
   answers and whole-value text, modulo D3(a)).
4. JSONBench: load to `data VARIANT` (needs D5), measure against DuckDB and the published
   ClickHouse numbers, 10M then 1B, on x86 to match their m6i.8xlarge.

## 7. Not doing
- CAST to/from VARIANT, VARIANT comparisons (existing rulings stand).
- Shredding arrays (shortcut above).
- Binary VARIANT in memory (D1 alternative).
