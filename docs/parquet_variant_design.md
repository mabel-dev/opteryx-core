# Design: Parquet VARIANT (shredded) read + write

**Status:** RULED 2026-11-19. §6 step 1 (whole-value read) BUILT 2026-11-19. Step 2 (path
pushdown) BUILT 2026-11-19 for both forms — files that hold the column as a VARIANT and files
that hold it as JSON text — on `READ_PARQUET` and on Parquet tables (§4.3). Pruning (2b),
Step 3 (writer) BUILT 2026-11-20 as ruled (W1–W7 "as recommended", §5.4): every rugo write of a
VARIANT column writes a shredded Parquet VARIANT. D5 BUILT 2026-11-20 (`READ_JSONL(path,
as_variant => 'data')`). Step 4 at 10M BUILT 2026-11-20 (`tests/performance/jsonbench/variant/`): load 4.86GB JSONL →
1.19GB in 20.8s; all 5 answers match DuckDB; hot total 362 ms vs DuckDB's 2,848 ms (Mac ARM,
interleaved). 1B: not to be run (architect, 2026-11-20). Closing the gap to ClickHouse:
`docs/parquet_variant_path_read_design.md` (PROPOSED). x86 EC2 m6i.8xlarge (built from source at
`390db5a`): load 57.7 s, all 5 answers match DuckDB, hot total 880 ms vs DuckDB's 5,568 ms.
**Rulings:** D1 yes (JSON text in memory) · D2 yes (automatic shredding, no setting) ·
D3 as recommended (INT64 / own-scale DECIMAL; the rest stored as double, canonical
rendering accepted) · D4 keep (string-form VARIANT files stay readable, no migration) ·
D5 a READ_JSONL option (2026-11-20; built as `as_variant`, below) · D6 as recommended (shredded members in schema
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
*Ruled 2026-11-20: a READ_JSONL option.* Built as `READ_JSONL(path, as_variant => 'data')`: one
VARIANT column, named by the option, holding each record's `{…}` text. In rugo it is
`ParseContext::whole_record`. The map builder stages the record's span once its line has passed
the usual line rules; no key is matched for it, so a top-level key that shares the column's
name changes nothing. The column is declared VARIANT through `explicit_schema`, so its type
never comes from a key. Blank lines are skipped and the whitespace around a record is trimmed.
A line that isn't exactly one object is malformed, the same as today (`ignore_errors` drops it).
Nothing is sampled at bind time, and `infer_schema` / `infer_sample_size` are refused beside
it. Not built: rugo's standalone `read_jsonl` has no such option (only `prepare_jsonl_context`
takes it); `data->'k'` over an `as_variant` scan is not pushed into the scan, which reads the
whole record (shortcut comment in `jsonl_nested_extraction.py`).

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

### 4.3 Step 2 build contract — BUILT 2026-11-19

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

**P1 — Scope.** First proposed as `READ_PARQUET` only, on the claim that rugo has no JSON
parser and does not link yyjson. **Both were false** (corrected 2026-11-19): rugo's JSONL
reader walks `col->>'k'` through text, and rugo's avro reader/writer call yyjson (symbols from
draken_native). The scope limit fell with P5's ruling: the strategy takes any column bound
VARIANT over Parquet — `READ_PARQUET`'s (the footer declares it) or a table's (the catalog
declares it; `manifest_format` says the files are Parquet). Per file, the footer gate admits
the path when the file holds the base column as a VARIANT or as a string, NULL-fills it when
the file has no such column, and REFUSES — naming the file — when it holds it as anything else.

**P2 — A new declared field on the FunctionDataset and Scan steps:**
`parquet_physical_by_identity` (identity → the name rugo is asked for), mirroring
`jsonl_physical_by_identity`. The scan uses it where it now uses `sc.name`.

**P3 — Spec-violating files.** A file that repeats a shredded field's name inside the
residual `value` is invalid by the spec. The whole-value read renders both; the path read
sees only the shredded one. The spec itself says such reads "may be inconsistent". Accept.

**P4 — Strategy shape.** The JSONL strategy's rewrite is shared, not copied: its body becomes
the common part, `JsonlNestedExtractionStrategy` keeps its name, flag and one-level rule, and
a sibling `ParquetVariantExtractionStrategy` adds chains, with its own
`FEATURE_DISABLE_PARQUET_VARIANT_EXTRACTION` so on/off equivalence is testable per format.

**P5 — path reads over string-form files, and catalog tables. RULED 2026-11-19: build the
text walk in the scan now** (option (b) below), with this cost on the table:

- A pushed path is a column the scan emits, so it is produced for every row the scan emits —
  BEFORE the scan's relocated filter. Unpushed, `->>` runs after the filter, on survivors.
- On a VARIANT-form file that costs nothing: the path is a typed column read like any other.
- On a string-form file there is nothing to read but the text, so the path is a JSON parse per
  emitted row. `SELECT data->>'x' FROM t WHERE ts > …` (a filter the scan prefilter does not
  take: timestamps, decimals, strings) parses every row of every unpruned row group instead of
  the survivors. Every catalog VARIANT table today is string-form — the writer that makes
  VARIANT-form files is step 3.

The options were: **(a)** leave catalog tables unpushed until step 3; **(b)** build the
per-file text walk in the scan (correct for any mix of files; carries the cost above on
string-form files); **(c)** push only when every file of the scan is VARIANT-form, decided
from footers at plan time. (b) was chosen. P1's `READ_PARQUET`-only scope is lifted with it:
the strategy takes any column bound VARIANT over Parquet — `READ_PARQUET`'s, or a table's.

**How the text walk is built.** A path over a file that holds its base column as a string
(`byte_array`, annotated string / JSON or bare — what the scan admits for a whole VARIANT
read) gets a TEXT plan. Every such path over one column shares ONE group in the scan's fold:
the column's chunk is decoded once, each emitted row is parsed once, and each path is walked
through that parse into its own output column (`NestedGroup::plans` / `slots` — a group now
yields one column or several). The parse flags, the walk and the text rule are draken's own
(`ops/json_extract.h`: `ReadPool`, `nav_tokens`, and `value_text`, factored out of
`extract_rows_multi` so the two cannot drift), so the answer is what `->` / `->>` return
unpushed, by construction. A row the scan's prefilter or a row admission drops is never parsed.

**Built (step 2):**
- `variant_binary.hpp`: `find_field` (linear key search) and `string_bytes`.
- `nested_json.cpp`: `parse_variant_path`; PATH plans in `build_plan` (`Builder::path`) —
  metadata plus only the leaves the path needs; `Assembler::path_value` returns what `->` /
  `->>` would from the whole value. TEXT plans for a base held as a string; `assemble_text`
  (one parse per emitted row, every path walked through it). `resolve_projection` takes a path
  name as a group column, and puts every TEXT path over one column in one group.
- `io_pipeline.hpp`: a group folds into one column or several (`NestedGroup::plans`/`slots`,
  `NestedSpec::slot`); the further columns take result slots past the expanded column list.
- **One read per field (ruled 2026-11-19, "share reads").** Every projected column read out of
  one top-level field of a file is one group: several paths into a VARIANT, the VARIANT itself
  beside them, the paths over a JSON-text column. The group's leaves are the ones its plans
  need between them (`shared_leaf_index`), decoded once; each plan assembles from its own
  subset. Before this each VARIANT-form path decoded its own copy of `metadata` (and of
  `value`, when unshredded): on an unshredded 20,000-row file one path held 1,588,983 bytes
  and three held 4,766,949.
- `draken/ops/json_extract.h`: `value_text`, the `->` / `->>` text rule, factored out of
  `extract_rows_multi` and shared with `assemble_text`. No behaviour change.
- Footer gate (`pool_reader.pyx::native_scan_rejection`): kind `"path"` — admitted when rugo
  can plan it for the file (base a VARIANT group or a string), absent (NULL-filled) when the
  file lacks the base, refused otherwise. Fetch blocks for a TEXT path are inferred from its
  base column's chunk, as for the unpushed read.
- `compiler.py`: path names for the scan's reads (`_scan_path_names`, FunctionDataset and
  Scan); a predicate on a path takes no part in statistics pruning, range settlement or
  coverage; no two-pass (latmat) scan.
- `plan_steps.pyx`: `parquet_physical_by_identity` on the FunctionDataset and Scan steps (P2).
- `ParquetVariantExtractionStrategy` + `FEATURE_DISABLE_PARQUET_VARIANT_EXTRACTION`; the JSONL
  strategy's rewrite is now the shared `ScanExtractionStrategy` (P4).
- Tests: `tests/unit/optimizer/test_parquet_variant_extraction.py` — every query on and off,
  over unshredded / auto-shredded / partially shredded files, over globs mixing a VARIANT-form
  file with a JSON-text one (plain and dictionary pages), and over a local-catalog table (two
  JSON-text files; manifest pruning, merge-on-read deletes, a self join). Measured there: a
  path into a shredded field of the partial file holds 4,952 compressed bytes against 72,369
  for the value; five paths over a JSON-text column hold exactly what one does, and so do
  four paths (and the whole value beside two) over an unshredded VARIANT.

**Known limits:**
- *JSON-text files, filter not taken by the scan's prefilter* (a decimal compare, an
  expression such as `id + 1 = 5`): every row the scan emits is parsed, where the unpushed
  extraction parses the filter's survivors. That is P5's accepted cost. It is also visible as
  behaviour when a row is not valid JSON: the pushed read fails on it, the unpushed read only
  if the row survives the filter. Filters the prefilter takes (`id = 5`, `s = 'k'`, `LIKE`)
  drop rows before the parse. Not measured at scale.
- *The whole value read beside a path, on a JSON-text file* (`SELECT data, data->>'kind'`):
  the column's chunk is decoded twice — once as the plain column, once for the paths' group.
  Unpushed it is one decode. (On a VARIANT-form file the two share one read.)
- *Two-pass (latmat) scan*: not used for a scan that carries a path — now including table
  scans of the `WHERE … ORDER BY … LIMIT` shape that used it.
- A string holding invalid UTF-8 is returned as its bytes by a path read over a VARIANT-form
  file, where the unpushed read fails parsing the rendered text.
- A column whose name contains `->` is not pushed.
- The catalog (`opteryx-catalog`, `add_files`) refuses to register a VARIANT-form file ("row
  group is missing column(s) ['data']"), so a TABLE mixing forms cannot exist until step 3
  teaches it; the mixed case is tested through `READ_PARQUET` globs.
- The standalone `rugo.parquet.read_parquet` does not take path names. ~~Asked for `["id",
  "data"]` on a VARIANT file it returned `id` alone~~ — it returns the VARIANT as its JSON text
  since step 3 (W3). A STRUCT/MAP named there still returns nothing, with no error (unchanged).

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

### 5.4 Step 3 build contract — BUILT 2026-11-20 (W1–W7 ruled "as recommended")

Found by reading the writer the step has to fit into. **W#** are the architect's.

**What the writer is today.** `_parquet_writer.hpp` is one `ColumnInput` = one top-level field
= one column chunk; every per-row-group vector (`RGMeta`, `EncodedRowGroup`, statistics,
blooms, page index) is indexed by that one number. The only nesting it knows is LIST
(`is_array`: rep/def levels over a flat element buffer). It cannot write a group with several
leaves, and a scalar's definition level is its validity bit (max def 1). The Cython edge
(`parquet_writer.pxi::_encode`) turns each morsel column into one `ColumnInput`; a VARIANT
vector becomes a UTF8 string column. The streaming writer captures the file's schema from the
first `write_row_group` — which is where "infer from the first row group" already fits.

**W1 — How a VARIANT column enters the writer.** *Recommend:* the edge expands it into its
LEAF columns — `data.metadata`, `data.value`, and a `value`/`typed_value` pair per shredded
field — each an ordinary `ColumnInput` carrying three new things: its `path_in_schema`, its
own definition levels with a `max_def` > 1 (the value encoders, dictionaries, statistics and
page index run unchanged off "present = def == max_def"), and, on the first leaf, the group's
schema subtree for `write_schema`. Chunk index stays equal to `cols` index everywhere, so the
row-group, placement, footer and page-index code is untouched. *Alternative:* a separate
group-column encoder beside the per-column one — a second code path through placement and
footer, for no gain I can see.

**W2 — Everything that writes a VARIANT column changes form at once** (§5.3, already ruled:
one path, no option). Consequence to plan around: a file written by the new writer cannot be
read by any engine released before step 1 — the read side has to be released and deployed
(fleet, Cloud Run, odata) BEFORE this writer is. Nothing here gates it.

**W3 — The standalone `rugo` wheel.** `rugo.parquet.write_parquet` would write VARIANT groups
that `rugo.parquet.read_parquet` cannot read: it has no group handling at all and today drops
a nested column from its result with no error. *Recommend:* compile `nested_json.cpp` into
`rugo_native` and have `read_parquet` return a VARIANT group as its JSON text (a STRUCT/MAP
would come with it, by the same code); at the least it must refuse a group loudly.

**W4 — `patch_columns` and `append_row_groups`.** The column patcher rebuilds every column of
a file from its schema tree and refuses a group ("patching struct columns is not supported
yet") — so it will refuse any file holding a VARIANT, whichever column is patched. *Recommend:*
teach it to carry a group's chunks through byte for byte (it already does that for chunks it
does not touch); patching the VARIANT column itself stays refused. ~~`append_row_groups` correct
as it is~~ — wrong, found while building: it rebuilds its shapes the same way, so it refuses
ANY file holding a VARIANT ("is a STRUCT"), loudly. Its only caller is the catalog's manifest
append, and manifests hold no VARIANT, so it is left as it is.

**W5 — The catalog (`../opteryx-catalog`).** `add_files` refuses a VARIANT-form file ("row
group is missing column(s) ['data']"). Engine writes do not go through it (the engine
describes its own files), so INSERT / CTAS / OPTIMIZE are not blocked; registering an
externally written VARIANT file is. A change in another repository — not started, say if wanted.

**W6 — Duplicate object keys.** JSON text may repeat a key; a Variant object may not. `->`
returns the FIRST match today (yyjson), so the encoder keeps the first and drops the rest: path
answers are unchanged, the whole-value text loses the repeats. *Alternative:* refuse the row.
*Recommend keep-first* — same reasoning as D3(a).

**W7 — A number no double can hold** (`1e999`, `1e-999`). D3(a) stores "the rest" as a double;
these would become infinity or zero, and infinity reads back as `null` — a different value,
not a re-rendering. *Built as: the row is refused, naming the number.* *Alternative:* store
the infinity/zero and accept it. Say if D3(a) was meant to cover it.

**Mine (defaults I will pick and measure, not rulings):** the inference constants — shred an
object field when it is present in at least 1 row in 64 of the rows its parent object is, its
dominant type covers at least half of its occurrences, to a depth of 8 and a cap of 128
shredded fields per file; typed DECIMAL leaves at the dominant scale, precision 18 or 38.

**Encoding rules (settled by D1–D6 and the spec):**
- one parse per row (yyjson, raw numbers); objects' members sorted by name; per-row `metadata`
  = the sorted, unique names used by that row's residual values (empty for a fully shredded row);
- numbers per D3: an integer literal in int64 range → the narrowest int; any other literal of
  ≤ 38 significant digits with no exponent, and not a negative zero → decimal4/8/16 at the
  literal's own scale; the rest → double;
- strings → short string (< 64 bytes) or string; `true`/`false`/`null` → their primitives;
- a row whose text is SQL NULL is a null group; a JSON `null` is a Variant null in `value`.

**Built (2026-11-20), not yet called by the writer:**
- `rugo/src/parquet/variant_encode.hpp` — `classify_number` (D3), `RowEncoder` (JSON value →
  Variant binary; the row's `metadata`), `Seen` (what the first row group holds at each path,
  and the shredding it decides), `Shredder` (rows → the group's leaf columns: per-row
  definition levels and values, in schema order). A row is parsed with draken's own flags and
  parse arena, so what `->` reads is what this writes.
- `rugo/src/parquet/variant_encode_test.cpp`, `make variant-encode-test` (in `make q`, under
  ASan/UBSan): 35 documents against the exact text the DECODER renders from the encoding;
  4,000 rows of 17 shapes shredded by the schema learnt from them and put back together by a
  reference reassembly written from the spec, compared with the unshredded value (numbers as
  their source bytes); the learnt schema, the definition levels, and the refusals.
- ~~Not yet proven: that another reader (DuckDB) reads the result~~ — proven, below.

**Built (2026-11-20), the writer:**
- W1 — `ColumnInput` gains `path`, `groups` (`SchemaGroup`: the group elements written before a
  leaf), `leaf_required`, `levels` + `max_def`. `write_schema` emits the groups (VARIANT
  annotation, `specification_version` 1, on the top group), `write_column_chunk` the leaf's
  path, `page_def_levels` a leaf's own levels in plain and dictionary pages, and row-group and
  page slicing offset them. `rugo/src/parquet/variant_columns.hpp` (`VariantColumn`) turns a
  `Shredder` into the leaves: group depth by parity (VARIANT, then typed_value groups and
  field groups alternating), never by name — a field may itself be named `typed_value`.
- The edge (`parquet_writer.pxi`): a VARIANT is shredded with the shredding learnt from its first
  `max_rows_per_row_group` rows (`write_parquet`), or from the first `write_row_group` and kept
  for the file (`open_parquet_writer`). Field ids go on the VARIANT's group; `field_ids`,
  `write_parquet_with_bounds` (keyed by the morsel's column) and the unique-name check count
  top-level fields, not leaves. `sorted_by`, a named `bloom_filters` entry and a named `alp`
  entry naming a VARIANT are refused; leaves get no bloom filter, no page index (shortcut, in
  `page_index_bound_kind`), no `rugo.sum` and no `rugo.ndv`/`rugo.mcv` entry (those name
  top-level fields). Leaves are dictionary-eligible as any column (Bool excepted).
- W3 — `rugo_native` compiles `nested_json.cpp`; `read_parquet` folds a VARIANT (only a VARIANT —
  a STRUCT/MAP reads as before) into its JSON text, a VARCHAR column
  (`rugo/src/parquet/variant_fold.hpp`). Shortcut: a projection holding a VARIANT decodes
  serially (the parallel reader materializes leaves on its workers before they could be folded).
- W4 — the patcher gives each top-level element its real first chunk (`schema_leaf_count`; it
  used the SCHEMA index, which is wrong after any multi-leaf element) and carries a VARIANT's
  leaves through as leaf columns (`carry_variant_leaves`); a shredded LIST under the VARIANT is
  refused, as is any drop/rename/retype naming the VARIANT.
- Tests: `tests/unit/connectors/parquet_io/test_variant_write.py` (27). DuckDB reads our files
  as it reads its own (every value, every writer option, a streamed file, a patched file);
  opteryx's paths, pushed and not, answer from our file as from DuckDB's; integers past int64
  and 38-digit decimals round-trip exactly (DuckDB and us). The table fixture of
  `test_parquet_variant_extraction.py` now holds both forms — the CTAS file (shredded) and a
  JSON-text file added beside it — which is the W2 transition.
- Found, not changed: DuckDB rewrites an empty object key to `element<n>` when it converts JSON
  to VARIANT (its own files hold `element4`); it reads an empty key from our files correctly.
- Found, NOT a VARIANT issue, FIXED 2026-11-20 (architect: "fix it"): `patch_columns`
  re-declared a REQUIRED column OPTIONAL over pages that carry no definition levels, so a
  patched file with a REQUIRED column (DuckDB writes them for NOT NULL) did not read (DuckDB:
  "Out of buffer"). `shape_from_schema_element` now carries the repetition; a column the patcher
  writes itself (ADD, a re-encoding retype) is OPTIONAL; `append_row_groups` refuses files that
  differ in it. Tests: the REQUIRED block of `tests/storage/test_parquet_patch_columns.py`.
- Not measured: write speed and file size against the JSON-text form; the inference constants.

## 6. Sequencing

1. Read: footer annotation, binary decoder, whole-value assembly. Test against variant
   files from another writer (Apache `parquet-testing` — check it carries shredded cases;
   generating them in `dev/` with another engine is the fallback).
2. Read: chained path pushdown + per-file resolution + string-leaf pruning; on/off equivalence.
3. Write: encoder, inference, round-trip tests (JSONL → Parquet VARIANT → identical `->`/`->>`
   answers and whole-value text, modulo D3(a)).
4. JSONBench: load to `data VARIANT` (`READ_JSONL(..., as_variant => 'data')`), measure against DuckDB and the published
   ClickHouse numbers, 10M then 1B, on x86 to match their m6i.8xlarge.

## 7. Not doing
- CAST to/from VARIANT, VARIANT comparisons (existing rulings stand).
- Shredding arrays (shortcut above).
- Binary VARIANT in memory (D1 alternative).
