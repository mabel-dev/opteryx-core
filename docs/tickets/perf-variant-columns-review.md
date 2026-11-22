# Perf: VARIANT columns — design and implementation review

> Written 2026-11-22 from a read of the code. Step 0 was then run the same day and its
> results are recorded below; nothing else in this ticket is measured. Every claim is tagged:
> **[read]** verified in the source at the cited line ·
> **[doc]** a number already measured and recorded in the design docs ·
> **[measured]** measured for this ticket in Step 0 ·
> **[hypothesis]** a cost I expect from the code but have not measured.
> Nothing here is built. Items that change design are listed under "Rulings needed".

## Summary

A VARIANT path is read 1.45x slower than the same field stored as a plain column, on the
same engine, the same sorted data and the same row-group layout **[measured]**:

| JSONBench 10M, Mac, sorted load, hot, ms (best of 9, interleaved) | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| five fields as plain columns | 3.1 | 17.1 | 8.3 | 8.0 | 9.8 | 46.4 |
| the same five fields as VARIANT paths, A+C+D+E on | 4.8 | 24.8 | 14.5 | 10.8 | 12.6 | 67.4 |
| | 1.53x | 1.44x | 1.74x | 1.35x | 1.28x | **1.45x** |

This ticket was first written against the design doc's numbers (37.3 ms plain, 113.6 ms
VARIANT, about 3x). Those do not reproduce: the repo's own runner
(`tests/performance/jsonbench/variant/runner.py`) gives 71.4 ms today. The prize is 21 ms
on a 67 ms total, not 76 ms.

The cause, from the code: **a path that Part A passes through is still a nested-group read,
not a column read.** It decodes three chunks to produce one column, decodes them unmasked,
and is excluded from the prefilter, the dictionary probe, page pruning, two-pass scans,
column fan-out, range settlement and statistics coverage. Part A removed the per-row
assembly; it did not move the path onto the plain-column decode path. R1 does that.
Step 0 confirms the scan carries the whole difference, and finds the largest single cost
inside it is `pass_through`'s own per-row loop, not the extra chunks.

Priority order:

| | Item | Where the gain is | Size (my estimate) |
|---|---|---|---|
| Step 0 | Attribute the residual | — | **done**, results below |
| R1 | Read a proven path as the leaf column it is | every path query | up to the whole 1.45x |
| R2 | Writer: page index, bloom, NDV/MCV for shredded leaves | selective path filters, planner estimates | needs R1 |
| R3 | Shredding is learnt from the first morsel only | silent slow files | cliff, not a percentage |
| R4 | Writer cost is O(rows × leaves) | INSERT / CTAS / OPTIMIZE / load | unmeasured, likely large |
| R5 | Compaction round-trips every row through JSON text | OPTIMIZE on VARIANT tables | unmeasured |
| R6 | General (non-pass-through) path micro-costs | whole-value reads, `->`, foreign files | small each |
| R7 | String-form files parse every emitted row | every pre-VARIANT catalog table | per table |
| R8 | Pushdown reach | paths the strategy does not take | per query shape |

---

## Step 0 — attribute the residual before building

House rule: profile, don't hypothesise. Before R1:

1. Run the plain-sorted and VARIANT-sorted arms interleaved (`KVSTORE_LOCATION=` blank,
   `top` clean first), same row-group count, best of N, arm order rotated.
2. `EXPLAIN ANALYZE` both arms for Q1–Q5; diff scan worker-ms, Filter, GROUP BY.
3. Per query, record chunks decoded and `io_held_peak_bytes` for both arms. Expectation
   from R1: VARIANT decodes `1 + 2k` chunks per row group for `k` paths, plain decodes `k`.
4. One samply profile of VARIANT Q2 and Q4 (the two shapes: 96% and 9% filter selectivity).

R1 goes ahead only if the scan is where the difference is. If Filter / GROUP BY carry it
instead, this ticket's order is wrong and R1 is re-scoped.

### Step 0 results (2026-11-22) [measured]

Opteryx 0.10.3, Mac, `KVSTORE_LOCATION=` blank. The machine was not fully quiet (Finder at
about 50% of a core, WindowServer and a video player; load average 3 on 18 cores), so only
the interleaved ratios are trusted. Medians agree with the minima (1.23x to 1.77x per query).

**Arms.** VARIANT: `testdata/_downloads/jsonbench/variant/10shards` as loaded (10 files,
160 row groups). Plain: built from those files for this run, one file per shard, the same
sort keys and the same streaming writer, also 160 row groups (119 MB against 973 MB):

```sql
SELECT data->>'kind' AS kind, data->'commit'->>'operation' AS commit_operation,
       data->'commit'->>'collection' AS commit_collection, data->>'did' AS did,
       CAST(data->>'time_us' AS INT64) AS time_us
FROM READ_PARQUET('<shard>')
ORDER BY kind, commit_operation, commit_collection, did, time_us
```

The plain files were written to a temporary directory and are not kept. The five queries
were run with identical text apart from how each field is spelt, and the answers were
asserted equal between arms.

**1. Wall time.** The table in the Summary. 9 rounds after one discarded warm-up, arm order
rotated each round.

**2. Where the difference is: the scan, entirely.** `EXPLAIN ANALYZE`, worker-ms summed over
16 workers, median of 9 interleaved runs:

| | scan time, plain | scan time, VARIANT | scan CPU, plain | scan CPU, VARIANT | GROUP BY time, plain | GROUP BY time, VARIANT |
|---|---|---|---|---|---|---|
| Q1 | 3.2 | 27.5 | 1.8 | 13.3 | 13.4 | 14.2 |
| Q2 | 20.4 | 121.6 | 11.0 | 54.7 | 149.2 | 164.3 |
| Q3 | 11.2 | 105.1 | 6.4 | 31.5 | 65.4 | 62.9 |
| Q4 | 13.9 | 48.4 | 11.1 | 33.2 | 36.9 | 38.0 |
| Q5 | 16.5 | 47.2 | 11.6 | 34.5 | 45.9 | 49.2 |

The scan costs 3x to 9x more worker time on the VARIANT arm; GROUP BY CPU is the same on
both (within 1.1 worker-ms on every query). The wall difference per query is close to the scan time
difference divided by 16. About half of the scan's extra worker time is not CPU (Q2: +101
time, +44 CPU). The engine defines that gap as the source waiting; the profile does not say
on what. **[hypothesis]** it is per-row-group decode latency, with column fan-out off.

**3. Counters** (fresh process per query and arm):

| | row groups read / pruned | prefilter rows in (plain / VARIANT) | bytes fetched, MB (plain / VARIANT) | `io_held_peak_bytes`, MB (plain / VARIANT) |
|---|---|---|---|---|
| Q1 | 160 / 0 | 0 / 0 | 0.0 / 0.8 | 0.0 / 0.6 |
| Q2 | 150 / 10 | 9,830,400 / 0 | 54.1 / 54.7 | 27.0 / 24.6 |
| Q3 | 110 / 50 | 7,208,960 / 0 | 25.5 / 28.6 | 8.4 / 8.7 |
| Q4 | 32 / 128 | 2,097,152 / 0 | 19.1 / 25.5 | 43.0 / 44.5 |
| Q5 | 32 / 128 | 2,097,152 / 0 | 19.1 / 25.5 | 43.0 / 44.5 |

- Row-group pruning (Part E) is identical between the arms.
- The worker prefilter never runs on the VARIANT arm, as R1 says from the code.
- The extra chunks are small in bytes (+1% to +33%). There is no chunks-decoded counter, so
  the `1 + 2k` expectation is not confirmed directly.
- Q3 reports 5 columns read on the VARIANT arm against 4 on the plain arm. Not investigated.

**4. Profile** (samply, 10 s loop per query and arm, plain samples at 1 ms, kernel-leaf
samples excluded; worker-ms of CPU per run summed over all threads):

| Q2 | plain | VARIANT | difference |
|---|---|---|---|
| all on-CPU | 125.2 | 177.0 | +51.8 |
| operators (GROUP BY etc.) | 105.6 | 105.4 | −0.2 |
| `pass_through` (inlined in `rugo::nested::assemble`) | 0 | 33.6 | +33.6 |
| chunk decode (`DecodeColumnFromChunk`, RLE/bit-unpack, validity bitmap) | 5.9 | 15.1 | +9.2 |
| draken kernels (library is stripped, not symbolicated) | 7.9 | 14.6 | +6.7 |

Inside `pass_through` on Q2 (`nested_json.cpp`, 33.6 worker-ms per run):

| Lines | What | worker-ms |
|---|---|---|
| `:1268-1281` | the per-row loop writing each code byte by byte and setting each validity bit | 19.1 |
| `:1234-1235` | scanning the `value` leaf's definition levels to prove it empty | 4.1 |
| `:1240` | the same loop's per-row "is this row present" test | 3.7 |
| `:1259-1261` | copying the dictionary arena, offsets and lengths | 1.8 |
| `:1252`, `:1265` | zero fills | 0.3 |
| — | no line recorded by the debug info | 4.6 |

Q4 (9% of rows survive): on-CPU 49.9 plain against 58.0 VARIANT, +8.1. `assemble` is 5.9
(`pass_through` 5.2, `typed_through` 0.7), `Assembler::int_value` 0.7, draken +2.3.

### What Step 0 changes in this ticket

- **The gate is passed.** The scan carries all of the difference, so R1 is the right target.
- **The size is smaller.** The ceiling for R1 is 1.45x on these queries, 21 ms.
- **The order inside R1 changes.** R1's text below leads with "three chunks for one
  column". Measured, the extra chunks cost about 13 worker-ms on Q2 (decode +9.2, the
  `value` definition-level scan 4.1). The per-row loop in `pass_through` costs 22.8 on its
  own (19.1 + 3.7). Both go away under R1, because a leaf read as a column never enters `pass_through`.
- **A narrower first slice exists.** Rewriting only `pass_through`'s emit (block-copy the
  codes when no row is masked, build validity with the existing SIMD routine, take "`value`
  holds nothing" from the footer, alias the dictionary instead of copying it) touches one
  function and no projection logic. **[hypothesis]** it removes roughly 25 of the 52 extra
  worker-ms on Q2. It does not restore the prefilter, fan-out or masked decode, and it is
  thrown away if R1 is built after it.
- **Still unattributed:** the non-CPU half of the scan's extra worker time, and the +6.7
  worker-ms in the draken kernels on Q2 (a rebuild with symbols would name it).

### R1a — `pass_through` fast path (built 2026-11-22) [measured]

Chosen as the first slice. One hunk in `pass_through` (`rugo/src/parquet/nested_json.cpp`):
when the leaf is dictionary-indexed, no row is masked and the leaf holds an index for every
row (so every row is present), the indices are narrowed to the code width in one pass and
the function returns, with no per-row presence test and no validity bitmap. Everything else
takes the loop that was there before.

Old build against new build, VARIANT arm, a fresh process per cell, order alternated over
10 rounds, each cell best of 7, answers asserted identical:

| ms | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| before | 4.49 | 23.29 | 13.69 | 10.08 | 11.75 | 63.29 |
| after | 3.64 | 20.45 | 11.64 | 9.55 | 11.34 | 56.62 |
| | 0.81x | 0.88x | 0.85x | 0.95x | 0.97x | **0.895x** |

Re-profiled on Q2 with the new build: `pass_through` falls from 33.6 to 8.8 worker-ms per
run, and all on-CPU from 177.0 to 150.8. What is left in `pass_through`: the `value`
definition-level scan 4.1, the dictionary copy 2.1, the narrowing pass 2.0, zero fills 0.3.

Test: `test_a_leaf_present_in_every_row` in
`tests/unit/connectors/parquet_io/test_variant_leaf_passthrough.py` (one-byte and two-byte
codes, switch on against switch off). Breaking the new code on purpose fails that test and
no other, so it was the only coverage of an all-present leaf.

Checks on the main checkout after the rebuild: `make q` passed, `make slt` passed (76 files),
and the 101 tests in `test_variant_leaf_passthrough.py` and
`tests/unit/optimizer/test_parquet_variant_extraction.py` passed.

Not done, from the slice as first described:

- **"`value` holds nothing" from the footer** (4.1 worker-ms on Q2). Built as R1b below.
- **Aliasing the dictionary** (1.8). `AppendBuffer` owns its memory and the leaves reach
  `assemble` const and shared between plans, so this is not a one-function change.
- **Validity by the SIMD routine.** Not needed where the fast path fires (no NULLs). A row
  group with some NULLs in the leaf still takes the per-row loop; that case is not in the
  sorted JSONBench data and was not measured.
- **Four-byte codes** (a dictionary over 65,536 entries in one row group) are handled by a
  single `memcpy` but not covered by a test. The engine hands the writer morsels of at most
  65,536 rows, so a file written here does not hold one; a foreign file can.

### R1b — the footer proves `value` empty (built 2026-11-22, ruling R-a) [measured]

The scan hands `nested::assemble` one byte per leaf: 1 where the footer says the chunk holds
nothing (`chunk_holds_nothing`: `null_count == num_values`). Where that byte is set for the
path's `value` leaf, `pass_through` no longer scans its definition levels. Where the footer
does not say (a file written without statistics), it scans as before. `typed_through`
(`::INT64` paths) is unchanged: it still checks `value` per row and fails on a contradiction.

Files: `nested_json.cpp` / `.hpp` (`assemble` takes `leaf_empty`), `io_pipeline.hpp` (fills
it from each leaf's footer statistics), `variant_fold.hpp` (passes none; it folds whole
values, never a path).

The `value` chunk is still fetched and decoded. Not decoding it is R1: the general path
reads that leaf's levels, so the scan cannot drop the chunk while any plan over the field
may fall back to it.

R1a build against R1a + R1b, on mains power, a fresh process per cell, order alternated over
12 rounds, each cell best of 7, answers asserted identical:

| ms | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| R1a | 3.58 | 20.36 | 11.70 | 9.41 | 11.15 | 56.20 |
| R1a + R1b | 3.47 | 19.84 | 11.13 | 9.19 | 11.23 | 54.86 |
| | 0.97x | 0.97x | 0.95x | 0.98x | 1.01x | **0.976x** |

A first run of this A/B was taken on battery in low-power mode (the same main build that ran
57 ms ran 86 ms) and read 0.98x; it is superseded by the table above. Profiles of Q2 on both
builds, taken in that battery state: the two scan lines cost 6.7 worker-ms per run before
and 0 after; `assemble` falls from 5.3% to 2.7% of on-CPU samples.

**Where the gap to plain columns stands after R1a + R1b** (same harness as Step 0, best of 9
interleaved, on mains):

| ms | Q1 | Q2 | Q3 | Q4 | Q5 | total |
|---|---|---|---|---|---|---|
| plain columns | 2.7 | 16.0 | 7.5 | 7.6 | 9.6 | 43.3 |
| VARIANT paths | 3.8 | 19.8 | 11.3 | 9.6 | 11.1 | 55.5 |
| | 1.42x | 1.24x | 1.49x | 1.27x | 1.16x | **1.28x** (was 1.45x) |

Checks on the main checkout after the rebuild: `make q` passed, `make slt` passed (76 files),
`make rt` passed (3,528 rugo tests), and the 102 VARIANT tests passed.

Tests: `test_a_file_without_statistics` (the footer does not say, so the levels are
scanned and the leaf still passes through). Forcing "footer does not say" to refuse the
pass-through fails that test and no other, which shows files with statistics take the
footer branch and the file without them takes the scan.

---

## R1 — read a proven path as the leaf column it is

### What happens today [read]

For `data->'commit'->>'collection'` on a row group where pass-through holds:

- **Three chunks for one column.** The PATH plan always takes the `metadata` leaf
  (`nested_json.cpp:346-353`) and the destination field's `value` leaf beside the typed
  leaf (`vvalue`, `:254-259`). `k` paths over one field decode `1 + 2k` chunks.
  `pass_through` then never touches `metadata`, and reads `value` only to confirm it is
  empty by scanning its definition levels row by row (`:1229-1237`) — a fact the footer
  already states (`chunk_holds_nothing`, `:1472`), and which path pruning and typed paths
  already trust.
- **Second-class decode.** A leaf of a nested group decodes unmasked, with no dictionary
  retention, no dictionary probe, no page jump, no page search and no length-only decode
  (`io_pipeline.hpp:3936-3959`, `:4023-4024`, `:4047`).
- **The scan loses features for all its columns.** Column fan-out is off for any item
  with a nested group (`io_pipeline.hpp:4301`). The two-pass latmat scan is off for any
  scan carrying a path (`compiler.py:5096-5099`). A predicate on a path ends range
  settlement and coverage (`compiler.py:4403-4447`). Key admission skips nested leaves
  (`io_pipeline.hpp:4577-4581`).
- **Path filters never run on the decode workers.** Two independent blockers:
  a path binds NVARCHAR and `pass1_worker_predicate_admissible` refuses NVARCHAR
  (`pass1_predicate_gate.py`, `_DESCRIPTOR_FREE_TYPES`), so `_arm_scan_prefilter` returns
  False (`compiler.py:4622-4625`); and even if armed, predicate columns are matched by
  chunk name (`io_pipeline.hpp:4350-4353`), which a path name never equals. So the filter
  runs as an ExprFilter after the scan, and Q4/Q5 (9% of rows survive) decode and emit
  `did` and `time_us` for every row. The dictionary probe that drops a whole row group
  when its dictionary lacks the literal never fires either.
- **Pass-through copies.** The dictionary arena, offsets and lengths are copied
  (`nested_json.cpp:1259-1261`), the codes are rewritten a byte at a time per row with
  two branches (`:1268-1281`), and definition levels are materialised as one int32 per
  row first. `emit_col` then copies again, as it does for any column.
- **Typed paths** (`::INT64`) are proven for the whole file at plan time, yet still go
  through the group fold: `typed_through` calls `int_value` per row, which compares
  `d.type == "int64"` as a `std::string` and walks a ladder of `.empty()` tests on every
  value (`:681-700`, `:1345-1349`).

### Proposal

In `resolve_projection` (it has the `FileStats` and the row-group list): when a path's
plan has `pass_leaf >= 0` and the destination field's `value` chunk holds nothing in the
selected row groups (or the field has no `value`), project the path as an **ordinary
column whose chunk is the leaf**, carrying the path's name. No `NestedGroup`, no
`metadata`, no `value`, no fold.

The flat decoder already gives the right NULLs for such a leaf: with `max_rep == 0` it
builds validity as `def == max_def` and counts present values the same way
(`decode_column.cpp:1578`, `:2966-2971`, `:4287-4291`), which is exactly design §3's
table — any ancestor null is NULL. **Assumption:** I verified validity and
`present_count` only; the rest of the flat path (selective decode, the dictionary probe)
needs a test over leaves with `max_def` 2–5 before this is trusted.

What it buys, with no new mechanism: one chunk per path; masked/selective decode; the
worker prefilter (once the type gate admits path columns); the dictionary probe;
latmat; fan-out; key admission; range settlement and coverage for `::INT64` paths
(`MIN/MAX/COUNT` from statistics). `pass_through` and `typed_through` shrink to the
fallback for unproven row groups, or go.

This replaces Part D's two-pass-within-a-field (ruled "not built" 2026-11-20) with the
existing prefilter; it adds no two-pass of its own.

### Open points

- **Per row group or per file.** One file's row groups must agree on the projected leaf
  chunks today (`nested_json.cpp:1629`). First build: whole-file proof (what typed paths
  already do), general path otherwise. Count how many row groups that loses on real
  files before building a per-row-group split.
- **Trusting the footer.** Today a wrong `null_count` is caught at decode
  (`pass_through` re-checks; `typed_through` fails loud). Aliased, nobody reads `value`.
  Pruning and coverage already trust the same statistic. Ruling R-a.
- **Kill switch.** `FEATURE_DISABLE_VARIANT_LEAF_PASSTHROUGH` should cover it so on/off
  equivalence stays testable.

### Measure

Leave-one-out on the 10M runner per house rule; chunks decoded per query; prefilter rows
in/out on Q4/Q5 (zero today for path predicates, by the reading above). Slower ⇒ deleted.

---

## R2 — writer: page index, bloom filters, NDV/MCV for shredded leaves

**[read]** `page_index_bound_kind` returns false for any group leaf
(`_parquet_writer.hpp:2803-2805`, marked `shortcut`). Leaves also get no bloom filter and
no `rugo.ndv` / `rugo.mcv` / `rugo.sum` entry (design §5.4: those name top-level fields).

Consequences once R1 lands: no page pruning or page jumps inside a row group on a path
filter; no bloom skip for point lookups on high-cardinality paths (`did = '…'`); and the
planner has no NDV or MCV for any path column, so join sizing, aggregate presizing and
selectivity on paths are blind (the JOB regression showed how much rides on those).

Proposal: write ColumnIndex/OffsetIndex for typed leaves under the existing single-page
rule; bloom on string leaves under the existing policy; NDV/MCV per typed leaf. The
key-value entries need a name for a leaf — Ruling R-c.

Depends on R1: nothing reads a leaf's index today.

---

## R3 — shredding is learnt from the first morsel only

**[read]** The streaming writer fixes a file's shredding from the first
`write_row_group` call, whatever its size (`parquet_writer.pxi:622-634`, `_shred_known`
set at `:1498`). `write_parquet` uses the first `max_rows_per_row_group` rows. A field
absent from that sample is never shredded in that file: every path into it takes the
binary general path, with no statistics, no pruning and no pass-through. Nothing reports
it.

Two ways to hit it:

- **A small first morsel** — the tail of a filter or join arriving first.
- **A sorted write.** `ORDER BY data->>'kind', …` puts one event type first; if that type
  filled the first row group, the other types' fields would not be shredded. The
  JSONBench load is sorted this way. **[hypothesis]** It escapes because `commit` events
  dominate each shard's first row group — I have not counted.

Proposal:

1. The streaming writer buffers until it holds a full row group before fixing the
   shredding (it already captures the schema at the first write).
2. `write_parquet` has every row in hand: sample evenly across them, not a prefix.
3. Write telemetry: per shredded field, rows that fell to `value`; per file, residual
   bytes against typed bytes. A poor shredding becomes visible.

The inference constants (1 in 64, depth 8, 128 fields) are still unmeasured (design
§5.4 says so). Measure file size and Q1–Q5 at 1/16 and 1/256 before touching them.

---

## R4 — writer cost is O(rows × leaves), not O(values present)

> 2026-11-22: the encode below runs serially on the calling thread (measured in R5).
> Design for running it in parallel: `docs/parquet_variant_parallel_encode_design.md`.

**[read]**

- Every row pushes a definition byte **and** a placeholder (4-byte offset, or an 8/16-byte
  zero) into every leaf under a missing or mismatched pair (`variant_encode.hpp:528-541`,
  `:589`, `:636-637`).
- `VariantColumn::append` then builds a 16-byte `StrSlice` per row for every byte leaf
  and a validity bitmap per leaf with a scalar loop (`variant_columns.hpp:77-93`), and
  frees and reallocates all of it every row group (`strs_.assign(leaves.size(), {})`,
  `:42-43`).
- At least half the leaves are `value` leaves, entirely null when the data fits its
  shredding; each still pays all of the above and then a dictionary / statistics /
  level-encode pass in the column writer.
- Each row's tree is walked twice — once to collect field names, once to write
  (`:438`, `:444`). Both walks sort every object's members
  (`sorted_members`: fill, `stable_sort`, `unique`) and allocate `residual` and `found`
  per object (`:594-596`). `classify_number` runs twice per number. The first row group
  is parsed twice (infer, then encode).

**[hypothesis]** For the JSONBench shredding (~110 leaves, per the load notes) that is
about 110 × 65,536 × ~21 bytes ≈ 150 MB of scratch written per row group, mostly zeros,
because a field present in 1 row of 64 costs the same as one present in all.

Nobody has attributed the load (24.4 s for 10M, sorted). Profile it first. Candidates,
in the order I would try them:

1. All-null leaf fast path: count present values per leaf while encoding; a leaf with
   none skips slices and validity, and the encoder writes levels only.
2. Keep scratch across row groups (`clear`, not `assign`).
3. One walk for a row with no residual — the collect walk exists only to build
   `metadata`, which is empty for a fully shredded row.
4. Reuse `residual` / `found`; cache the number classification between walks.

JSONBench does not score load time. INSERT, CTAS and OPTIMIZE on VARIANT tables pay this
for every row.

---

## R5 — compaction round-trips every row through JSON text

**[read]** D1 keeps VARIANT as JSON text in memory. So OPTIMIZE / CTAS over a shredded
table renders every row to text in the scan (`assemble_one`: metadata parse, every
shredded member visited, text built), and the writer parses that text with yyjson, sorts
members, re-infers and re-shreds. No leaf is ever carried as a leaf.

Not proposing to reopen D1. Proposing one measurement: OPTIMIZE rows/s on a VARIANT
table against the same data as plain columns. If the gap is large, the options (a
leaf-wise rewrite when inputs share a shredding, for one) are a design decision —
Ruling R-d.

### Measured 2026-11-22 [measured]

`OPTIMIZE TABLE` on the local-disk catalog (`tests/tools/local_catalog.py`: the production
compaction path with the metastore in memory and storage on local disk; no services).
Each table is three files of 1M JSONBench rows, built by a CTAS and two INSERTs from the
sorted VARIANT shards, and compacted to one file. Three arms:

- **variant:** the whole record as one VARIANT column.
- **text:** the same record as JSON text in a VARCHAR column. Same information, no VARIANT.
- **plain5:** the five JSONBench fields as plain columns. Holds about an eighth of the data.

A fresh process per cell, arm order rotated, 4 rounds. The driver waited before each
cell until no other engine work ran, the 1-minute load was under 4, and the Mac was on AC
power. Row count and a content checksum were asserted equal before and after.

| arm | MB in | best s | median s | M rows/s | MB/s in |
|---|---|---|---|---|---|
| plain5 | 35.6 | 0.54 | 0.55 | 5.51 | 65.3 |
| text | 336.1 | 8.82 | 8.96 | 0.34 | 38.1 |
| variant | 290.9 | 5.51 | 5.59 | 0.54 | 52.8 |

Per row VARIANT is 10x plain5, but it moves 8x the bytes; per MB it is 1.24x. Against the
same data as text it is faster (0.62x the time).

**Profile** (samply, one cell per arm, samples inside the OPTIMIZE window only):

| | variant | text |
|---|---|---|
| window, s | 5.77 | 9.12 |
| on-CPU, all threads, cpu-s | 13.7 | 12.7 |
| on-CPU, the calling (main) thread, cpu-s | 3.38 | 9.04 |

- **Both arms are bound by one thread, not by CPU.** The compaction used about 2.4 cores
  (variant) and 1.4 cores (text) of 18.
- **zstd at level 7 is the largest CPU item in both** (variant 5.6 cpu-s, text 5.9). The
  compaction sink writes with the `storage` profile, which `zstd_level_for` maps to level 7
  for byte-array columns (4 under `fast`). On VARIANT the ~110 leaves compress on the
  writer pool in parallel. The one text column compresses on the calling thread, which is
  why text is slower than VARIANT.
- **VARIANT's own cost is the writer's encode, on the calling thread:** `Shredder::pair`
  1.29 cpu-s, `Shredder::typed` 0.22, `RowEncoder` (object, sorted_members, stable_sort,
  value, metadata) about 0.6, a vector `emplace_back` 0.22, `classify_number` 0.07. About
  2.9 of the main thread's 3.38 cpu-s, and serial. That is R4's cost, now measured.
- **The JSON-text round trip is small.** Rendering in the scan (`rugo::nested::`,
  `append_quoted_raw`) is about 0.4 cpu-s, on scan workers, off the critical path. This
  section's premise, that the text round trip is the cost, is wrong.
- **Not attributed:** in the text arm, `DataFileStream._write_batch`'s own body takes
  2.16 cpu-s on the main thread.

**What this changes:**

- R-d (a leaf-wise rewrite that skips text) would save the 0.4 cpu-s of rendering, plus
  most of the encode if leaves were carried as they are. The measured target is the
  serial encode, which R4's items address without a design change.
- A narrower first fix: encode a row group's VARIANT rows in parallel chunks on the writer
  pool, or move the encode off the calling thread. Not designed.

**Found in passing, not VARIANT-specific (reported, not acted on):**

- `MorselQueue::put` burns 2.1 cpu-s (variant) and 3.6 cpu-s (text) on scan threads
  while the sink is slow. It looks like spinning on a full queue. Not investigated.
- Any wide single string column compresses serially at level 7 during compaction, on the
  calling thread.

---

## R6 — general path micro-costs (`assemble_one`)

Taken by whole-value reads, `->`, `->>` over non-string leaves, unproven row groups, and
foreign files whose `value` holds data. All **[read]**; each is small alone:

- a. `finish` copies the whole rendered blob from `std::string` into `string_arena`
  (`nested_json.cpp:1047`). Render into the arena.
- b. `parse_metadata` runs for every row (`:901`, `:943`) though `metadata` is
  dictionary-encoded and only a binary value needs it. Parse lazily, or once per entry.
- c. A shredded object writes `,"key":` for each member and truncates it when the member
  is missing (`:851-857`). With 1-in-64 fields most members are missing in most rows.
  Test presence first.
- d. An unselected row is rendered and then discarded (`:1197`). Advance levels only.
- e. `int_value` / `decimal_value` / `float_value` re-derive the leaf's shape per value
  (`:681-733`, including the string compare). Resolve once per leaf per row group.
- f. `->` over a string leaf and `->>` over a dictionary-encoded non-string leaf could
  render once per dictionary entry and keep the codes (design §9 lists it as later).

Do these only if Step 0 or a real workload shows `assemble_one` matters.

---

## R7 — string-form files

**[read]** A file holding the column as JSON text gets a TEXT plan: one yyjson DOM parse
per emitted row, including rows of a dictionary-encoded chunk (`nested_json.cpp:1061`
`shortcut`), and before the relocated filter (P5, accepted 2026-11-19). Every catalog
VARIANT table written before the shredding writer is this form and stays so (D4).

- Cache per dictionary entry — closes the shortcut.
- OPTIMIZE re-encodes, so it converts a string-form table to shredded. That is the
  migration path; it should be stated and its speed measured (see R5).

Low priority unless a hot customer table is string-form.

---

## R8 — pushdown reach

**[read]**

- a. A multi-token path in one literal (`data->>'commit.collection'`, `$.a.b`, `/a/b`)
  is not pushed (`jsonl_nested_extraction.py:97`); the equivalent chain is. Push it as a
  chain when every token is an object key. **Assumption:** draken's tokenisation makes
  the two forms equal — to be confirmed in `ops/json_path.h` before building.
- b. `@?` (key exists) over a VARIANT reads the whole value: render, then parse. The
  definition levels answer it.
- c. Typed paths are INT64 only, `CAST` only, and only when the path is read nowhere
  else under a single cast (`parquet_variant_extraction.py:149`, `:174`). The writer also
  shreds BOOL and DECIMAL. With R1 a leaf read both bare and cast is two columns over one
  chunk.
- d. `data->'k'` over `READ_JSONL(…, as_variant)` is not pushed (`shortcut`,
  `jsonl_nested_extraction.py:268`): the record is materialised whole and parsed again by
  the kernel, though the JSONL reader already has a one-level text walk with dictionary
  interning. Matters only if the stateless JSONBench entry reads through `as_variant` —
  I did not check which form it uses.

---

## Checked and found fine

So nobody re-checks:

- **Footer weight.** 86,118 bytes on a 95 MB JSONBench file (0.09%).
- **RLE-shaped leaves refusing pass-through** (`nested_json.cpp:1228`): unreachable for a
  path leaf — the decoder's run path needs `max_def == 0` (`decode_column.cpp:1942-1944`).
- **The top-level residual `data.value`** is not read by a path plan.
- **Shared leaves** decode once per field across plans (`shared_leaf_index`).
- **`build_plan` runs several times per path per file** (gate, projection, pruning):
  microseconds on a ~110-leaf schema.

---

## Rulings needed

- **R-a (R1):** a proven path is planned from footer statistics and the `value` chunk is
  not read. A footer whose `null_count` is wrong would then give wrong answers instead of
  a loud failure. Pruning and coverage already trust that statistic. *Recommend: trust
  it.* **RULED 2026-11-22: "Trust the footer".** First use is R1b.
- **R-b (R1):** admit this scan's own `->>` path columns to the worker prefilter (they
  bind NVARCHAR, which the 2026-08-08 ruling did not cover). *Recommend: yes, scoped to
  path columns as `_scan_category` already scopes the optimizer gate.*
- **R-c (R2):** how a leaf is named in `rugo.ndv` / `rugo.mcv`, and whether leaves get
  blooms by default.
- **R-d (R5):** only after its measurement — whether compaction may move leaves without
  rendering text.
- **R3** changes when the streaming writer cuts its first row group (it buffers). Say if
  that is not acceptable.

## Sign-off for any item built from this

`make q` while iterating, `make slt` before done; the JSONBench runner (it refuses any
answer that differs from DuckDB's); on/off equivalence under the existing kill switches;
leave-one-out before any claim of a gain.
