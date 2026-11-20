# Design: VARIANT path reads at plain-column cost

**Status:** RULED 2026-11-20 — R1–R5 as recommended (§7). BUILT 2026-11-20, uncommitted:
A (one condition, §3), C (fused only where footers prove it — ruled 2026-11-20), D's admission
(the two-pass half not built — ruled 2026-11-20), E (sorted load + path row-group pruning).
Leave-one-out on the Mac 10M, SORTED load (2026-11-20; 5 rounds, arm order rotated,
identical answers in every arm), best ms Q1..Q5: all 7.6/40.7/26.1/18.4/20.7 = 113.6;
-A 296.8 (2.61x), -C 148.1 (1.30x), -D 144.1 (1.27x; Q4/Q5 ~1.7x — -D also loses path
row-group skipping, which needs the pushed filter). Sorted load: 24.4 s, 0.97 GB (unsorted 1.19).
All five answers equal DuckDB's. x86 EC2 m6i.8xlarge (commit 38b87c9, 2026-11-20, hot = min of runs
2-3): Q1 13 / Q2 60 / Q3 38 / Q4 28 / Q5 32 = 171 ms vs ClickHouse's CURRENT dashboard entry 139
(5/52/26/27/29; the 229 in the repo's older result file is superseded) — 2nd of the dashboard's 10M entries;
load 40.9 s, 0.97 GB; answers equal DuckDB's. Hot only - not JSONBench's official cold shape. Measured as each part landed (Mac 10M, alternating,
identical answers): A 2.25x (517 → 230 ms), C 1.39x on top (239 → 172), D's admission 0.98x
(noise). Found on the way: a top-level column sharing a name with a nested field was refused
(rugo's logical-type map aliased bare names) — FIXED, `test_nested_name_collision.py`.
**Builds on:** `docs/parquet_variant_design.md` (shredded VARIANT read + write, steps 1–4; the
path pushdown contract in §4.2–4.3).
**Purpose:** close the JSONBench gap. The rest of the engine is already fast enough; the cost
is in how a VARIANT path is read.

## 1. The gap, measured

JSONBench 10M, hot, best of 3 (`tests/performance/jsonbench/variant/runner.py`, all answers
equal to DuckDB's):

| | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| x86 m6i.8xlarge, Opteryx | 50 | 202 | 197 | 210 | 221 | 880 |
| x86 m6i.8xlarge, ClickHouse 25.11 (published) | 6 | 102 | 36 | 40 | 45 | 229 |

On the Mac, the same five fields written as **plain Parquet columns** and run through the same
engine — same answers — take **84 ms against 361 ms** on the VARIANT paths (Q1 4.6/22.7,
Q2 28.1/82.1, Q3 12.7/82.7, Q4 18.0/86.4, Q5 20.5/87.1). That 4.3x is the ceiling this design
is aiming at: everything outside the path read is unchanged.

## 2. Where the time goes (EXPLAIN ANALYZE, Mac, 10M; worker-ms summed over 16 workers)

- **Q3:** scan 766 (58%), GROUP BY 332 (25%), Filter 214 (16%). The plain-column Q3 is ~140
  in total.
- **Every path costs a fixed 22–26 ns a row, whatever it holds.** Scan alone: `kind` (3 distinct
  values) 220, `commit.operation` 249, `commit.collection` 260, `did` (millions) 265,
  `time_us` 263. The cost is per row, not per value.
- **Why:** `nested_json.cpp`'s PATH plan (`Assembler::path_value`) runs per row: it reads that
  row's `metadata` entry and parses it (`variant::parse_metadata`), walks the definition levels
  of every leaf on the path, and copies the answer into a fresh byte arena. Its output is
  always a dense VARCHAR — never dictionary-encoded — so the Filter and GROUP BY above it also
  work string by string.
- **`CAST(data->>'time_us' AS INT64)`** parses text that was rendered from an INT64 leaf:
  139 worker-ms in Q3's GROUP BY, on top of the render in the scan.
- **Filters on paths are not pushed into the scan.** A `->>` path is typed NVARCHAR and the
  Parquet gate (`FileSystemTable.PUSHABLE_TYPES`, `filesystem_connector.py`) does not list
  NVARCHAR, so each is "predicate pushdown declined" and runs as a Filter after the scan has
  produced every path for every row.

The JSONBench files meet the safety condition of §3 everywhere: in all 532 row groups every
fallback (`value`) column on the five paths is entirely null, and every record is an object
(an `as_variant` load accepts only lines that are one object). Filter selectivity: the
creates filter keeps 96.2% of rows (Q2, Q3); the post-creates filter keeps 9.0% (Q4, Q5).

## 3. Part A — a shredded string leaf is read as itself (the main lever)

**Scope:** a path read with `->>` that ends at a shredded field whose `typed_value` is a STRING
leaf. Not `->` (its answer is JSON, so a string is quoted and escaped); not non-string leaves
(`->>` renders them to text — see Part C for the typed form).

**What a row's answer is today.** The general path (`nested_json.cpp`, `Builder::path` and
`Assembler::path_value`) reads only the `metadata` leaf and the destination field's
`value` / `typed_value` pair. A parent whose `typed_value` object is null is taken as "the path
leads nowhere" — the spec lets a reader assume a null `typed_value` is not an object, and a
shredded field is never repeated in a parent's residual — so the parents' `value` columns are
never read. Take `data->'commit'->>'collection'`, destination leaf
`data.typed_value.commit.typed_value.collection.typed_value`:

| leaf def | meaning | answer today |
|---|---|---|
| 0 | `data` is NULL | NULL |
| 1, 2 | a parent's `typed_value` object is null | NULL (the path leads nowhere) |
| 3 | `collection.typed_value` null | `collection.value` null → NULL; else the binary value, rendered |
| 4 | the string | the string's bytes |

**The condition, decided per row group (ruled 2026-11-20: this one condition only):** the
destination field's own `value` holds nothing in this row group. Then by the table every row's
answer is the leaf's string (def at max) or NULL — exactly what the general path returns, with no
extra column read. When it fails, that row group takes the general path, unchanged. (The
first draft of this design also required the parents' `value` columns empty and no row stopping
at the root; the general path reads neither, so those checks would add reads and change no
answer.)

**What it emits:** the leaf's decoded column, with validity = "definition level at max", values
scattered to their rows the way the flat optional decode already does. A dictionary-encoded
leaf stays dictionary-encoded: its dictionary and codes go straight through, so the scan emits
a Draken Dict vector, as it does for a plain dictionary column. No `metadata` parse, no copy.
The row mask `assemble` takes is honoured by compacting the codes.

**Where:** `rugo/src/parquet/nested_json.cpp` — the PATH plan gains a pass-through branch,
chosen per row group inside `nested::assemble`; `build_plan` records, per path plan, the leaf
and the `value` leaves the condition reads. Nothing changes above rugo: the column has the same
name, type and values.

**Dictionaries — the JSONL comparison.** The JSONL reader interns nested `->>` text into a
dictionary (`parse_nested_text_dict`) because text has none. Here the file already has one, so
it is passed through — cheaper than interning and with no per-row hash.

## 4. Part B — falls out of A

With dictionary-shaped path columns, the Filter and the GROUP BY get the same input a plain
dictionary column gives them, and whatever their dictionary-aware paths do for plain columns
today they do here. (Not assumed to include grouping on codes: a ClickBench profile found the
GROUP BY's code pass did not fire even for a plain dictionary column.) The plain-column
measurement in §1 already includes exactly that behaviour. No new code is proposed here; it is
listed so the measurement attributes it.

## 5. Part C — `CAST(path ->> AS INT64)` reads the INT64 leaf

**Built as ruled 2026-11-20 ("fuse only when proven")**, not with a fallback cast in the scan —
that would have surfaced a bad value as the scan's redacted read error instead of the CAST error.
The optimizer records `CAST(<path> AS INT64)` (not TRY_CAST, no FORMAT) on the scan step
(`parquet_typed_paths`) when the path is read nowhere else; the compiler, before classifying the
read set, asks `pool_reader.typed_paths_proven` and — only if every file's statistics show the
field's `value` empty in every row group — reads `<path>::INT64` under the CAST's own identity
(swapped in `scan.columns` too), which the CAST above reads (`_bind_precomputed_subexpressions`).
rugo's `typed_through` fails loud if a row group contradicts that. Unproven: unchanged.
`FEATURE_DISABLE_VARIANT_TYPED_PATH`. The text below is the original proposal.

`time_us` is shredded as INT64, rendered to text by `->>`, and parsed back by the CAST.

**Proposal:** the planner fuses `CAST(<path> ->> 'k' AS T)` (T a type a leaf can hold:
INT64 first) into a typed path column, `data->>'time_us'::INT64` in the scan's path notation.
rugo serves it:
- in a row group where §3's condition holds for an INT64 leaf: the INT64 leaf, as is;
- otherwise: the general path's text, through **the engine's own VARCHAR→T cast**, so
  answers and errors stay identical. That cast must be one implementation: verify the engine's
  CAST lowers to draken's (`draken/ops/kernels/cast_string.cpp`); rugo already links draken,
  so it calls the same kernel. If the engine's cast is not draken's, Part C waits (no copy of
  cast semantics into rugo).

**Measured value:** the 139 worker-ms of parse in Q3, plus `time_us`'s share of the scan.

## 6. Part D — filters on paths are pushed into the scan (from the JSONL reader)

**Built:** admission only — `PredicatePushable.can_push(..., text_paths=)` judges this scan's own
`->>` paths as VARCHAR (`predicate_pushdown._scan_category` for the caller's `types`);
`FEATURE_DISABLE_VARIANT_PATH_PUSHDOWN`. Measured 0.98x alone: the filter moves into the scan
workers but every path is still produced. **The two-pass half (item 3) is not built** (ruled
2026-11-20): after A and C its ceiling is about a quarter of Q4/Q5 (≤168 of ~541 worker-ms, the
leaves still decode for every row) and nothing for Q2/Q3.

The JSONL reader takes filters on nested `->>` columns (`JsonlPredicatePushable` lists
NVARCHAR) and applies them while it reads, so a failing row never produces its other columns.
The Parquet equivalent:

1. **Admission:** `FileSystemTable.can_push` admits a comparison on a column that is a
   **pushed path** of this scan (its identity is in `parquet_physical_by_identity`) — not
   NVARCHAR in general, so no plain column's routing changes.
2. **Applied in the scan:** as any pushed predicate is — the relocated native filter in the
   scan workers. Statistics pruning on path columns stays out (that is step 2b's design, §4.2
   item 3 of the VARIANT design).
3. **Two-pass within a VARIANT field:** today all paths over one field share one group, read
   once, assembled together, and the scan takes no two-pass form for paths (design §4.3). The
   change: assemble the predicate's paths first, evaluate the predicate, then assemble the
   field's other paths with the surviving rows as `assemble`'s `row_mask`. The leaves are still
   read and decoded once. For Q4/Q5 that produces `did` and `time_us` for 9% of rows.

This reverses the §4.3 line "no two-pass (latmat) scan" for paths, so it needs a ruling (R3).

## 6b. Part E — sort at load (what ClickHouse's entry does)

**Built:** `tests/performance/jsonbench/variant/load.py` sorts each shard by the five paths.
Pruning: the compiler sends path filters to row-group pruning under their path names (literals as
text); `pool_reader._prune_row_groups` resolves a path to its destination leaf chunk
(`rugo::path_dest_chunks`) and tests the leaf's min/max only in a row group whose field's
`value` chunk holds nothing (`chunk_holds_nothing`). Range settlement and coverage still exclude
paths. Tests `test_variant_path_pruning.py`.

ClickHouse's JSONBench table (`clickhouse/ddl.sql` upstream) declares types for the five query
paths and sorts by them: `ORDER BY (data.kind, data.commit.operation, data.commit.collection,
data.did, fromUnixTimestamp64Micro(data.time_us))` — its primary index. So sorting at load is
within the benchmark's rules. Our load keeps arrival order (≈ time order).

**Measured on plain columns** (Mac, 10M, same engine, both copies 153 row groups, written by the
same sort operator — "time order" is `ORDER BY time_us`, standing in for arrival order; best of
3, alternating, identical answers):

| | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| time order | 3.8 | 26.6 | 11.1 | 17.8 | 19.6 | 79.0 |
| sorted by the five fields | 3.0 | 13.6 | 7.5 | 5.9 | 7.3 | 37.3 |
| | 1.27x | 1.96x | 1.48x | 3.02x | 2.69x | **2.12x** |

Q4 in detail: the scan falls 58.5 → 9.2 worker-ms (only 19 of 153 row groups can hold
`app.bsky.feed.post`, so the rest are skipped by statistics) and the GROUP BY 159 → 28 (a
`did`'s rows arrive together). The sorted file is 97 MB against 123 MB.

**What it takes for VARIANT paths:** an `ORDER BY` on the paths in the load query (cost: the
sort, 1.9 s for 10M plain columns here); the skipping needs row-group pruning on path predicates
(VARIANT design step 2b), which needs Part D's pushdown first. The leaf chunks already carry
min/max statistics. Part A + Part E together aim at plain-sorted cost: 37 ms here against 361
today.

## 7. Rulings needed

- **R1 — Part A as specified:** `->>` over STRING leaves only; the condition per row group
  (§3, corrected to one condition 2026-11-20); general path otherwise. *Ruled: yes.*
- **R2 — Part C:** fused typed path column, with the general path's fallback through the
  engine's own cast kernel. *Recommend: yes, after confirming the engine's cast is draken's;
  otherwise defer C.*
- **R3 — Part D:** admit filters on pushed paths, and a two-pass assemble within a field
  (reverses the §4.3 "no two-pass for paths"). *Recommend: yes.*
- **R5 — Part E:** the JSONBench load sorts by the five query paths, as ClickHouse's entry
  does, and step 2b (row-group pruning on path predicates) is brought into this work. *Recommend:
  yes — 2.1x on plain columns, 3x on Q4/Q5.*
- **R4 — Kill switch and measurement:** one flag for A
  (`FEATURE_DISABLE_VARIANT_LEAF_PASSTHROUGH`), used for on/off equivalence tests and
  leave-one-out. Build A, C and D, then measure leave-one-out on the Mac 10M runner, then one
  EC2 m6i.8xlarge run. *Recommend: as stated.*

## 8. Tests

- **Equivalence, A:** every path, A on and off, identical results, over: files meeting the
  condition (dictionary and plain pages); a row group where the leaf field's `value` holds a
  non-string in one row (condition 1 fails); a row group where an intermediate field is a
  non-object in one row (condition 2); a row group with a row whose root is not an object
  (condition 3, written with `rugo.parquet.write_parquet` from a VARIANT morsel); a scan whose
  row groups mix both; a scan with a row mask; a glob mixing a VARIANT-form and a JSON-text
  file.
- **C:** fused and unfused CAST give identical answers and identical errors, including a row
  group where some `time_us` is held as a string or decimal in `value`.
- **D:** with pushdown on and off, identical answers for `=`, `IN` and `<>`, on a predicate path
  plus other paths of the same field and of another field; the two-pass assembles the other
  paths only for surviving rows (telemetry count).
- `make q`, `make slt`, and the JSONBench runner (it refuses any answer that differs from
  DuckDB's).

## 9. Not in this design

- Statistics pruning on paths (VARIANT design step 2b).
- `->` over a shredded leaf (its answer is JSON text; a dictionary pass-through would need the
  rendered JSON per dictionary entry — possible later, same condition).
- Non-INT64 typed reads in Part C beyond what the first build covers.
- Writer-side level histograms (would let condition 3 be decided from the footer, and the
  `metadata` / `value` chunks skipped from IO; small for JSONBench: ~4 KB per row group).
