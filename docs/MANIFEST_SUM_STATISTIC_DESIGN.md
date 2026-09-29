# SUM Statistic and Statistics-Answered Aggregates — Design

Status: PROPOSED, revision 2 (2026-09-28). Rulings of 2026-09-28 folded in (§2).
Open items are listed in §11.

## 1. Goal

Record an exact integer **sum** per column wherever we record per-column
statistics: per row group in the data file, per file in the manifest. Then
answer aggregates from statistics **everywhere that is provably sound**:

- unfiltered;
- filtered (the covered/residual split);
- grouped (group keys that are constant within a unit);
- at plan time, per file;
- at execution time, per row group.

"Unit" below means a file (plan time, from the manifest) or a row group
(execution time, from the file footer).

## 2. Rulings (2026-09-28)

| # | Ruling |
|---|---|
| D1 | ~~native int128 leaf~~ → **revised 2026-09-28: two optional positional `ARRAY(INT64)` columns, `sums_hi` / `sums_lo`** (the int128's halves), on the `null_counts` encoding. LIST<DECIMAL128> was unsupported in rugo's writer/reader and unconfirmed in draken. |
| D2 | **Integer columns only.** No floats. DECIMAL is also not in scope (read "just int columns" as integers only; say if DECIMAL was meant to be included). |
| D3 | Asked: is there a slot in the Parquet or skene spec? Answered in §4. |
| D4 | **Every integer width and signedness:** INT8/16/32/64 and UINT8/16/32/64. |
| D5 | An overflow is never fatal to the query; it only means the statistic can't be used. |
| D6 | Use it in **every** instance where it is sound: aggregates, filters, grouping, plan time and execution time. |

## 3. The contract: a statistics answer must equal the correct answer

The engine's semantics (`src/cpp/engine/native_group_sinks.hpp`) define the
answer. A statistics answer that differs from a correct scan is a bug, never an
optimisation.

| Aggregate over integer column `c` | Engine | From statistics |
|---|---|---|
| `SUM(c)` | exact `__int128` accumulation. Output is INT64, and a total outside INT64 raises "SUM overflow" | Σ unit sums (int128). If the total is outside INT64, **decline** (D5): the query takes the scan path, and the engine's own behaviour applies |
| `AVG(c)` | `(double)i128 / (double)valid`, FLOAT64 | the same expression, from Σ sums and Σ valid. Bit-identical |
| `COUNT(*)` / `COUNT(c)` | rows / valid rows | Σ rows / Σ(rows − nulls) (exists today, unfiltered only) |
| `MIN(c)` / `MAX(c)` | raw value at the operand's type | the unit bound, only where the ordinal IS the value (the existing gate in `statistics_only_response.py`) |
| zero valid values | SUM/AVG/MIN/MAX are NULL | NULL |

Integer addition is associative, so every row in this table is exact.

### ⛔ P0 prerequisite: engine `SUM`/`AVG` over UINT64 is wrong today

- `agg2_read_raw` (`native_group_sinks.hpp` ~l.569) reads a UINT64 as its int64
  bit pattern.
- `AggCell::isum` is an `__int128`, so `isum += raw` **sign-extends**. Every
  value ≥ 2^63 is added as `value − 2^64`.
- The comment claims "the FINAL reported value is reinterpreted back to
  uint64_t for output". That does not happen: `SumI` emits `DRAKEN_INT64`
  (l.1592).

Result: `SUM(u64)` over values ≥ 2^63 returns a silently wrong number, and
`AVG(u64)` does too. This was found by reading the code; it has **not** been
reproduced.

D4 includes UINT64, and a correct statistics answer would disagree with the
engine's wrong one. So **the engine fix lands first**: zero-extend UINT64 into
the int128 lane. It is a separate, approved change.

## 4. D3 — does the format have a slot for a sum?

### skene: yes, and it is already written

- `skene/include/skene/format.h`, `ColumnStatistics` (per column, per row
  group, in the file footer): `kStatSum = 1u << 3` ("int128; exact types only,
  NEVER floats"), stored in `sum_low` / `sum_high`.
- **The writer already fills it.** `skene/src/statistics.cpp` accumulates
  `__int128` over non-null values for INT8–64 and UINT8–64 (UINT64
  zero-extended, correctly), and also DECIMAL.
- Tests exist: `test_value_order.cpp` covers the sum over non-nulls, a sum past
  int64, and floats getting no sum.
- `skene_native.pyx` exposes `"sum"`.
- **No consumer reads it.** `src/cpp/planner/skene_stats.hpp` (row group →
  file aggregation into the manifest) ignores `kStatSum`, and so does the skene
  scan. Every skene mirror already carries row-group sums, at no format change.

### Parquet: no sum statistic, but there is a spec-sanctioned per-chunk slot

- `Statistics` (parquet.thrift) has only min/max, null_count, distinct_count and
  the exactness flags. There is no sum, and `SizeStatistics` and
  `GeospatialStatistics` don't have one either.
- **The extension slot is `ColumnMetaData.key_value_metadata`** (field 8). It
  holds one list per column chunk, i.e. per column per row group, which is
  exactly the granularity we want.
- **rugo's reader already parses it** (`metadata.cpp:612`, into
  `ColumnStats.key_value_metadata`) whenever statistics are read.
- rugo's writer does **not** emit it today. It writes only the file-level
  field 5, for draken logical descriptors.
- The 2026-08-19 ruling put *descriptors* at file level because schema-only
  reads skip the chunk KV. That reasoning does not apply to a sum: a sum is a
  chunk property, and it is only needed on the statistics read, which does
  parse field 8.

**Proposal for rugo:**
- Emit key `rugo.sum` on every integer, non-LIST column chunk.
- The value is the int128 sum written as signed decimal text. Text is portable
  to any reader, and parsing it is trivial next to the footer read.
- A chunk with no key means "not tracked".
- Trust follows the existing sorting-columns rule: **trusted only when
  `created_by` identifies rugo.** A foreign writer's `rugo.sum` is ignored.
- rugo stays opteryx-free: the key is rugo's own name.

## 5. Storage and producers

### 5.1 One exact-sum reduction, shared

skene already has the kernel (`value_as_int128` plus the accumulation loop in
`skene/src/statistics.cpp`). **Lift it into draken** as a header-only
`draken/ops/exact_sum.h`, and use it from:
- the skene writer (replacing its private copy);
- the rugo parquet writer;
- the opteryx writer's `FileStatsAccumulator`;
- ANALYZE.

That gives one implementation (§11 of the contract: no duplication). The
existing draken `i64_sum` wraps on overflow and is not used for this.

An int128 accumulator cannot overflow at any row count we address:
|INT64_MIN| × 2^32 = 2^95. Folding across files in the manifest is also int128.
A theoretical overflow there is detected with `__builtin_add_overflow`, and the
sum becomes unknown (D5).

### 5.2 Manifest cell

```c++
struct ManifestCell {
    ...
    bool     has_sum = false;   // false = NOT TRACKED, never zero
    __int128 sum = 0;           // integer columns only
};
```

A native accessor (`NativeManifest`) folds the per-file sums into a
relation-wide total.

### 5.3 Persisted manifest format (D1)

> **Superseded by the revised D1 (2026-09-28):** `sums_hi` + `sums_lo`, both `ARRAY(INT64)`, appended last, optional; a file row either tracks sums (one element per column, null = not tracked) or is EMPTY. The text below describes the rejected DECIMAL128 option.

- Add a **new optional column appended last**, `sums`, following the
  `distinct_counts` precedent. Its type is `ARRAY(DECIMAL128)` at scale 0 (a
  scale-0 decimal is an integer, and DECIMAL128 is draken's existing int128
  physical type).
- Old manifests decode unchanged, with every sum unknown.
- **This needs verifying, and building if missing** (per the D1 ruling):
  - draken can build and view an `ARRAY` vector with a DECIMAL128 leaf;
  - rugo writes and reads `LIST<FIXED_LEN_BYTE_ARRAY(16) DECIMAL(38,0)>`;
  - `manifest_encode.hpp` / `manifest_decode.hpp` handle the int128 leaf.
- No new DrakenType is introduced. The enum is part of the frozen ABI.
- **Cross-repo:** the catalog shares `_MANIFEST_COLUMNS`, so it must pass
  `sums` through. That lands in the catalog repo first.
- `SHOW MANIFEST` renders sums as text, the same way bounds are rendered.

### 5.4 Producers

| Producer | Row-group sum | File sum (manifest) |
|---|---|---|
| skene writer | **exists** (`kStatSum`) | `skene_stats.hpp`: Σ row-group sums, only if every row group has `kStatSum` |
| rugo parquet writer | **new:** chunk KV `rugo.sum` (§4) | `manifest_footer.hpp` / rugo `AggColumnStat`: Σ chunk sums, only if every chunk has one and the file is rugo-written |
| opteryx writer (`FileStatsAccumulator`: INSERT, CTAS, compaction) | via the rugo writer it drives | **new:** int128 lane per integer column |
| ANALYZE | n/a | **new:** exact-sum per morsel, folded per file, `builder.set_sum` |
| carry paths (`carry_statistics`, `copy_manifest_row`, `relocate_file`) | n/a | copied with the other value statistics |
| foreign parquet / Iceberg | none | unknown (ANALYZE fills it) |

### 5.5 Invalidation (a sum must never outlive its truth)

| Change | Effect on sums |
|---|---|
| **ALTER COLUMN TYPE** | Integer→integer widening keeps the sum (the value is unchanged). Anything else drops it. |
| **ADD COLUMN … DEFAULT** | Files written before the column existed have an unknown sum. |
| **Column patch (donor pattern)** | Recomputed by the writer. Never carried from the pre-patch row. |
| **DROP STATISTICS** | Clears sums. |
| **Deletes** | A unit with deletes is never *covered* (§6). It is scanned as a boundary unit with its delete vector. This is better than today's whole-query decline. |

## 6. The covered / disjoint / boundary classification (shared by every consumer)

For a unit U and the query's `WHERE` predicate P, each conjunct is classified
against U's statistics.

**Admissible conjunct forms**, on a column whose bounds are exact values
(`ordinal_is_value`: integer family and temporal; UINT64 through its decoded
bound):
- `=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`
- `IN (literals)`
- `IS NULL`, `IS NOT NULL`

Strings are never covered: their bounds are 8-byte-prefix ordinals, not values.

**Classifying one conjunct** (V = the conjunct's value set, [min, max] = U's
bounds):

| Class | Condition |
|---|---|
| covered | `null_count == 0` AND [min, max] ⊆ V. For `IS NOT NULL`: `null_count == 0`. For `IS NULL`: `null_count == rows`. |
| disjoint | [min, max] ∩ V = ∅, OR `null_count == rows` for any conjunct other than `IS NULL` |
| boundary | anything else, including unknown statistics |

**Combining conjuncts, and other rules:**
- A conjunction is **covered** if every conjunct is covered.
- It is **disjoint** if any conjunct is disjoint.
- Otherwise it is **boundary**.
- A predicate containing OR, NOT, a function, or a column comparison is
  **unclassifiable**. The rewrite does not fire, and the plan is unchanged.
- A unit with deletes is never covered.
- A unit missing a statistic that an aggregate needs (for example its sum) is
  demoted to boundary.

**Why the null rule matters:** NULLs fail every predicate except `IS NULL`. So a
unit whose filter column has nulls is never covered by a value predicate.
Infino's implementation appears to skip this check, and would over-count
`COUNT(*)`.

## 7. Consumers (D6)

### 7.1 Plan time, file granularity (manifest), Python optimizer

Planning is Python by charter; this phase only decides the plan.

- **A. Unfiltered.** Extend `StatisticsOnlyResponseStrategy` with `SUM` and
  `AVG` on integer columns. Deletes still decline this whole-query literal
  form.
- **B. Filtered, ungrouped.** `Aggregate(Filter(P), Scan)` becomes

  ```
  Project(combine(literal partials of covered files, residual partials))
    └─ Aggregate(partials) ← Filter(P) ← Scan(boundary files only)
  ```

  - Disjoint files are dropped. This extends manifest pruning to `<>`, IN and
    IS NULL where it doesn't already cover them.
  - Partials: COUNT → Σ, SUM → Σ (with an INT64 check, else decline), AVG →
    (Σsum, Σvalid), MIN/MAX → min/max of bounds.
  - When every file is covered or disjoint, the scan disappears.
- **C. Grouped, constant keys.** For `GROUP BY k1..kn` with aggregates
  admissible under B, a covered file where every key column has
  `min == max` and `null_count == 0` contributes one group partial,
  `(k-values, partials)`. A file where a key column has `null_count == rows`
  contributes the NULL group. Such files leave the scan; everything else is
  residual. The final step merges the literal group partials with the
  residual's group partials by key.
  This needs a **partial→final aggregate plan shape** (§11, O2).

### 7.2 Row-group granularity — REVISED 2026-09-29 (architect): classify at PLAN time, SEED the aggregate

Implemented for parquet (P3):
- `coverage_terms.py` translates the pushed predicate EXACTLY (all conjuncts or
  nothing): signed ints, UINT8/16/32, DATE32; ops = < <= > >= <> IN IS [NOT] NULL
  BETWEEN. The compiler registers an ungrouped aggregate's needs (COUNT(*),
  COUNT(col), SUM/AVG over ints, MIN/MAX over the ordinal==value types) against
  its scan before compiling it.
- `open_native_scan_plan(coverage=...)` classifies every kept row group with the
  native `stats_coverage.hpp` over its footer statistics
  (`parquet_stats_coverage.hpp`; bounds trusted only in rugo-written files).
  Covered row groups are folded into the plan's seed and left out of the work
  list, together with row groups the exact terms prove empty.
- The compiler builds `UngroupedAggSink` with the seed; `finalize` merges it
  through `agg2_merge`, and takes operand types from it when no morsel arrived.
- Kill switch / oracle: `disable_statistics_coverage`. Facts:
  `row_groups_answered_from_statistics`, `row_groups_disjoint_by_statistics`.
- skene (2026-09-29): `SkeneScanPlan.plan_coverage` runs at plan time
  (`SkeneClaimSet::plan_coverage`), opening files through the cross-query reader
  cache so the execution claim build is a cache hit; the Source's claim builder
  skips the plan's excluded row groups before zone and runtime pruning (runtime
  bounds only exist at execution, so claims — block fetch plans — are still laid
  out there, over what the plan left).
- GROUP BY (P4, 2026-09-29, both formats): a covered row group whose every key
  column holds ONE value (min == max with no nulls, or only nulls = the NULL
  group) is folded into that group's partials (`CoverageGroups`,
  `fold_grouped_unit`; one decision entry, `cover_unit`, for both shapes). A
  row group where any key varies is read. Compiler gate
  (`_coverage_group_keys`): a GroupedAggregateHashedNode directly over a scan,
  bare key columns of the ordinal==value types (signed ints, UINT8/16/32,
  DATE32, with no logical descriptor), no ROLLUP/CUBE/GROUPING SETS, and the
  same aggregate set as ungrouped. The seed reaches the sink through
  `Engine::set_groupby_seed` → `GroupBySink::set_seed` (O3). At the top of
  `finalize` it becomes one more queued partition per hash partition it touches,
  with keys hashed through `compute_row_hashes` on a typed key morsel, exactly
  as sunk rows are. The ordinary merge, top-k cut and emit then treat seeded
  groups like sunk ones, and HAVING filters them downstream as usual. With no
  morsel sunk, the seed types the sink; otherwise its types must equal the
  captured ones, or it fails loud. Measured (on/off interleaved, best of 6):
  - `GROUP BY EventDate` on skene: 32→17 ms (862 row groups).
  - `CounterID` top-10: skene 12.7→8.4 ms, rugo 41→16 ms.
  - `GROUP BY AdvEngineID`: skene 16.8→14.0 ms, rugo 43→31 ms.
  - Q8 itself (`WHERE AdvEngineID <> 0`) gains nothing: its covered row
    groups are the all-zero ones, which the predicate already drops as
    disjoint.

The text below is the superseded execution-time proposal.

### 7.2-old Execution time, row-group granularity (native scan)

This is native by charter, the Python/native boundary is crossed once, and it
is where most of the win is. It works on footer statistics alone, so it needs
no manifest and no ANALYZE. It covers every rugo-written parquet file (once §4
lands) and every existing skene file.

- **Planning** hands the scan: the classifiable predicate (the scan already
  receives pushed predicates natively), the aggregate spec, and the target
  sink.
- **Per row group, before any fetch or decode**, the scan classifies it by the
  §6 rules, using that row group's footer statistics (parquet row-group
  Statistics plus `rugo.sum`, or skene `ColumnStatistics`):
  - **disjoint:** skip. This is already done where pruning covers the form.
  - **covered:** hand the sink a **statistics partial** (rows, valid, int128
    sum, min, max per aggregate), plus the constant group key for the grouped
    form. No fetch, no decode.
  - **boundary:** decode normally, filter, and feed rows.
- The ungrouped sink merges statistics partials through the same `AggCell`
  merge it uses between workers. The grouped sink needs a
  "merge one partial row into group k" entry (§11, O3).
- **This supersedes 7.1-B/C wherever the scan is native.** 7.1 still matters for
  whole files: dropping them avoids even opening the footer. The two layers
  compose: files by the manifest, then row groups by the footer.

### 7.3 What this means for ClickBench

- **Q3** (`SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)`, unfiltered) and
  **Q4** (`AVG(UserID)`, needs the int128 sum): every row group is covered, so
  there is no decode at all.
- **Q2** (`COUNT(*) WHERE AdvEngineID <> 0`) and **Q8** (`GROUP BY AdvEngineID
  WHERE AdvEngineID <> 0`): row groups that are all zero are disjoint, row
  groups with `min > 0` are covered, and constant-key row groups are covered
  groups. How much this buys depends on how AdvEngineID is laid out, and is
  **unmeasured**.
- **Q1/Q7:** already answered today.
- String-filtered queries (`<> ''`, LIKE): no change. String bounds are not
  exact.

⛔ **Locally, the dataset must carry sums for any of this to move.**
`scratch/hits_rugo_262k` has no manifest and predates `rugo.sum`, so it has to
be regenerated with the new rugo writer. The skene mirror already carries them.
Prove the knob moves before claiming any gain (see §9).

**Regenerated 2026-09-29.** The corpus was rewritten from itself with rugo
0.9.143 (same files, 64k row groups in blocks of 4; the old tree is kept at
`scratch/hits_rugo_262k.pre_sum`). Best of 6, coverage on/off interleaved:

| Query | Old corpus | New corpus |
|---|---|---|
| Q3 | 78 ms | 1.4 ms (answered from footer sums, 0 row groups read) |
| Q4 | 72 ms | 1.1 ms (answered from footer sums, 0 row groups read) |
| `COUNT/SUM/AVG WHERE EventDate >= 15901` | 80 ms | off 68 ms, on 22.5 ms (692 of 1,061 covered) |
| `GROUP BY EventDate` with `SUM` | 95 ms, nothing covered | off 75 ms, on 29 ms (932 covered) |

Q2 and Q8 cover nothing: no row group holds only non-zero AdvEngineID. With
coverage off, the new corpus is still about 30% faster on Q2 and Q8 (44 → 31 ms,
interleaved, same row groups read, size +0.03%). That is a writer difference,
not the sums, so ClickBench runs taken before and after the regeneration are
not comparable.

The skene mirror was rebuilt from the regenerated corpus the same day, with the
Makefile's `dev/parquet_to_skene.py ... lz4`. It has the same shape (4 files,
1,511 row groups) and is 0.3% larger. Its timings are unchanged (interleaved
old vs new: Q2 21.8/21.7 ms, Q8 22.2/22.2 ms, Q3 1.1/1.0 ms), and P3/P4 cover
the same row groups (rng 647, `GROUP BY EventDate` 862, CounterID 996,
AdvEngineID 430). The old mirror already carried sums, so the rebuild changes
nothing here.

## 8. Phasing

| Phase | Content | Gate |
|---|---|---|
| **P0** | Engine UINT64 SUM/AVG fix | Repro test first; `make q` |
| **P1** | `draken/ops/exact_sum.h` (skene switched onto it); rugo `rugo.sum` chunk KV write and read; skene/rugo row group → file aggregation; manifest cell plus the `sums` column (int128 leaf support); writer, ANALYZE, carry paths; invalidation rules; catalog pass-through | Round-trip tests; writer-throughput ABBA (baseline before the first edit) |
| **P2** | §7.1-A unfiltered SUM/AVG | Equivalence and plan-shape tests |
| **P3** | §6 classifier (one native implementation, called from the planner and the scan) plus §7.2 row-group statistics partials, ungrouped | ClickBench Q2/Q3/Q4 ABBA on regenerated rugo data and on skene |
| **P4** | §7.2 grouped (constant keys) | Q8 ABBA |
| **P5** | §7.1-B/C plan-time file-level rewrite, if P3/P4 leave file-level wins on the table (for example remote, where footers cost a round trip) | Measured before building |

## 9. Tests and measurement

- **Equivalence, per consumer:** the statistics answer equals the answer with
  the path disabled. Cover every integer width and signedness, including
  UINT64 values ≥ 2^63 (after P0). Include nulls, all-null units, empty tables,
  and units with deletes (must be scanned, not covered).
- **Classifier truth table:** every admissible form × {covered, disjoint,
  boundary}. Include the null cases, unknown statistics, and an unclassifiable
  predicate (no rewrite).
- **Knob proof:** telemetry counters for row groups covered / boundary /
  disjoint, and files covered / boundary / disjoint. A test asserts the counts,
  not just the answer.
- **Overflow (D5):** an INT64-overflowing total takes the scan path. The
  statistics path never raises.
- **Trust:** a foreign-written parquet file with a forged `rugo.sum` is
  ignored.
- **Round trip:** manifest int128 values above INT64 and negatives survive
  encode/decode. An old manifest reads back with every sum unknown.
- **Invalidation:** each §5.5 row.
- `make q` for every phase.

## 10. Adjacent issue (reported, not in scope)

`opteryx/planner/optimizer/strategies/statistics_only_response.py:617` uses
`getattr(manifest, "has_deletes", None)`. That is banned, like `hasattr`. P2
touches this file, so fixing it there needs your approval.

## 11. Rulings on the open items (2026-09-28)

- **O1 DECIMAL:** INCLUDED for skene (the writer already emits DECIMAL sums;
  the consumer carries the scale), NOT for parquet (no `rugo.sum` on DECIMAL,
  and none from ANALYZE or the opteryx writer).
- **O2:** DEFERRED, and P5 with it.
- **O3:** the direct partial-merge entry into GroupBySink.
- **O4 / P0:** included in this work.
- **§10 getattr:** fixed while editing the file in P2.

## 11a. Open items as originally raised

- **O1: DECIMAL.** Out of scope as ruled? skene already writes DECIMAL sums, so
  adding it later costs only the scale-carrying consumer logic.
- **O2: the plan-time partial→final aggregate shape** (§7.1-C). No such plan
  shape exists today. P5 needs one; P3/P4 do not, because they work inside the
  sink. Proposal: defer to P5 and decide then.
- **O3: the seam between the scan and the grouped sink.** A covered row group
  hands `(key, partials)` straight into GroupBySink. This couples scan and sink.
  The alternative is a tiny one-row synthetic morsel per covered unit, pre-
  aggregated with a "partial" marker. Proposal: the direct partial-merge entry,
  since it is the same merge the sink already does between workers.
- **O4: the P0 engine fix.** It is outside this feature but blocks D4 UINT64.
  Approve it as a separate change?
