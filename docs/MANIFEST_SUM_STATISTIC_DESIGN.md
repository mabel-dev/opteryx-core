# Manifest SUM Statistic — Design

Status: PROPOSED (2026-09-27) — decisions D1–D6 open for the architect.

## 1. Goal

Record an exact per-file, per-column **sum** in the manifest so that the
optimizer can answer `SUM(col)` and `AVG(col)` from statistics, with no scan.
This is the same method `StatisticsOnlyResponseStrategy` already uses for
`COUNT(*)`, `COUNT(col)`, `MIN` and `MAX`.

It also serves consumers beyond this rewrite:

- the covered/residual range-aggregate rewrite (the infino `covered_agg`
  pattern, §9). There, files wholly inside a filter range contribute their sum
  from stats, and only boundary files are scanned;
- any future estimate that wants a column mean.

**Non-goal (v1):** filtered aggregates, `GROUP BY`, and `SUM(DISTINCT)`.

## 2. The contract: a stats answer must equal the engine's answer

The engine's semantics (`src/cpp/engine/native_group_sinks.hpp`) are the
specification. A statistics answer that differs from the scanned answer is a
bug, never an optimisation (§11 rule, applied here).

| Operand | Engine accumulator | SUM output | AVG output |
|---|---|---|---|
| INT8/16/32/64, UINT8/16/32 | exact `__int128` (`isum`) | INT64. Raises "SUM overflow" if outside INT64 (finalize, line ~2476) | `(double)i128 / (double)valid` |
| DECIMAL (64), DECIMAL128 | exact `__int128` of unscaled raws | bound DECIMAL type | `(double)i128 / (double)valid / 10^scale` |
| FLOAT32/64 | `double` `+=` | FLOAT64 | `fsum / valid` |
| UINT64 | see §10 (suspected engine defect) | — | — |
| BOOL, DATE32, TIME32/64, TIMESTAMP64 | `__int128` (admitted by `agg2_operand_supported`) | — | — |
| zero valid values | — | NULL | NULL |

Consequences for the stored statistic:

- **The integer and decimal sum must be int128 per file.** The only draken
  reduction today (`draken/ops/int64_reductions.h` `i64_sum`) *wraps* on
  overflow, so it is unusable here. A per-file int64 is also not enough:
  `AVG(UserID)` (ClickBench Q4) overflows int64 inside a single file, and the
  engine explicitly supports that case (the comment at `AvgI` finalize).
- **AVG needs the valid count.** It is `record_count - null_count`, per file,
  and both are already in the manifest.
- **An integer/decimal answer is bit-identical to the engine's.** Integer
  addition is associative, and the division is the same expression. Floats are
  not bit-identical (see D2).

## 3. What exists today

- **Manifest rows:** `ManifestCell` (`src/cpp/planner/native_manifest.hpp`)
  holds bounds, `null_count`, lengths, NDV and the sketch. It has no sum.
- **Persisted format:** `_MANIFEST_COLUMNS` (`opteryx/models/manifest_io.py`).
  Per-column stats are positional `ARRAY` columns, and an unknown is a null leaf
  (never zero). `distinct_counts` is the precedent for an **optional column
  appended last**: old manifests simply do not carry it
  (`manifest_decode.hpp:63`), and no stored manifest needs rewriting.
- **Producers of per-file column stats:**
  1. **Writer:** `FileStats` → `FileStatsAccumulator`
     (`src/cpp/planner/file_stats.hpp`). It is fed row group by row group by
     `parquet_writer.py`, and covers INSERT, CTAS and compaction output.
  2. **ANALYZE:** `_analyze_one_file` (`opteryx/operators/table_management/_analyze.py`).
     Its per-morsel reductions are native, and it writes through
     `NativeManifestBuilder.set_counts` / `set_sketch`.
  3. **Footers:** parquet via `manifest_footer.hpp`, and skene via
     `skene_stats.hpp`. Neither format carries a sum.
  4. **Carry paths:** `carry_statistics`, `copy_manifest_row` and
     `relocate_file` copy an existing file's cells (compaction, snapshot
     operations).
- **Consumer:** `statistics_only_response.py`. It handles an unfiltered,
  ungrouped `Aggregate` directly over a `Scan`. When the manifest has deletes,
  only a bare `COUNT(*)` is allowed: the per-column stats describe the physical
  superset.

## 4. Design

### 4.1 One native exact-sum reduction (draken)

New header-only `draken/ops/exact_sum.h`:

```c
// Exact sum of the valid rows of an integer-family / DECIMAL / DECIMAL128 vector.
// Returns false (sum unknown) on int128 overflow — never a wrapped value.
bool draken_exact_sum_i128(const DrakenVector& v, __int128* sum, uint64_t* valid);
```

- It uses the uniform `data[selection[i]]` path. An identity-selection
  contiguous loop is allowed under the 2026-08-06 ratification
  (`count_true`/`mask_indices` class), via the canonical predicate only.
- Widening is at the operand's own width (sign-extend signed, zero-extend
  unsigned ≤32 bits), exactly as `agg2_read_raw` does.
- Overflow is checked with `__builtin_add_overflow` on int128. It is
  practically unreachable, but checking costs nothing on the hot path if done
  per block.

There is **one** implementation, used by both the writer accumulator and
ANALYZE. The engine's `AggCell` keeps its own loop, because it is fused with
MIN/MAX lanes. Converging the engine onto this reduction is not proposed.

### 4.2 Manifest cell

```c++
struct ManifestCell {
    ...
    SumTag   sum_tag = SUM_NONE;   // NONE (unknown) | INT128 | DOUBLE (D2)
    __int128 sum_i = 0;            // integer family / DECIMAL unscaled, at the column's scale
    double   sum_f = 0.0;          // FLOAT only, if D2 admits floats
};
```

`SUM_NONE` means unknown, never zero (the manifest's existing rule).

A native accessor sits beside `total_null_count`:

```c++
// Relation-wide (sum, valid) for `position`, or nullopt when ANY live file's
// sum or null count is unknown, or any file has deletes.
std::optional<SumTotal> NativeManifest::total_sum(size_t position) const;
```

It folds in int128 and returns nullopt on overflow. The `valid` it returns is
`Σ(record_count − null_count)`.

### 4.3 Persisted format

Add **one optional column, appended last**, following the `distinct_counts`
precedent. Old manifests decode unchanged, with every sum unknown. Encoding is
D1.

The catalog repository shares this format (`_MANIFEST_COLUMNS` "shared with the
catalog"). The catalog must pass the new column through untouched. That is a
cross-repo change and has to land there first, or be verified to be
format-agnostic.

`SHOW MANIFEST` gains a `sums` column rendered as text, in the same way bounds
are (`show_morsel`).

### 4.4 Producers

| Producer | v1 | How |
|---|---|---|
| Writer (`FileStatsAccumulator`) | **yes** | Per-column int128 lane plus a `summable` flag set from the physical type. `add()` calls `draken_exact_sum_i128` per row group, and `write()` sets `sum_tag`. Overflow sets the column to unknown for this file. |
| ANALYZE | **yes** | Per morsel, call the same reduction through a Vector method (for example `col.exact_sum()`, returning `(hi, lo, valid)` or None), fold in Python across a handful of morsels (the existing pattern), then `builder.set_sum(row, fid, ...)`. |
| Carry paths | **yes** | `carry_statistics`, `copy_manifest_row` and `relocate_file` copy the sum with the other value statistics. |
| Parquet footer (external files) | no | Parquet has no sum statistic, so the value is unknown. An external dataset gets sums by running ANALYZE. |
| rugo-written parquet footer | D3 | rugo could write per-row-group sums into footer key-value metadata, which would let footer-sourced manifests carry sums with no ANALYZE. |
| skene footer | D3 | This would be a skene format change. It is out of scope unless ratified. |

### 4.5 Invalidation rules (the stat must never outlive its truth)

- **Deletes:** any file with deletes makes `total_sum` nullopt, so the rewrite
  declines. This is the same stance as MIN/MAX.
- **ALTER COLUMN TYPE:** widening *within* the integer family keeps the sum,
  because the value is unchanged. Any other change drops the column's sums to
  unknown. That includes int→DECIMAL (the scale would change), anything→FLOAT,
  and DECIMAL precision/scale changes.
- **ADD COLUMN … DEFAULT:** files written before the column existed have an
  unknown sum. v1 does not compute `default × record_count`.
- **Column patch (donor pattern):** the patched file's cell for that column is
  recomputed by the writer, or unknown. It is never carried from the donor's
  pre-patch row.
- **DROP STATISTICS:** clears sums alongside the other value statistics.

### 4.6 Consumer: `StatisticsOnlyResponseStrategy`

Extend `is_simple_aggregate` to admit:

- `SUM(col)`, where the physical type is in the D4 allowlist, not DISTINCT, with
  no FILTER clause;
- `AVG(col)` under the same conditions.

In `complete()`:

- `total_sum(position)` nullopt → leave the plan untouched.
- `valid == 0` → SUM and AVG are NULL literals of the bound type.
- **SUM(int family):** if the int128 total is outside INT64, **decline**. The
  engine then scans and raises its own "SUM overflow" error. One error, one
  message, one owner; the stats path does not replicate the error. Otherwise
  emit an INT64 literal.
- **SUM(DECIMAL):** emit the unscaled int128 at the column's bound DECIMAL type.
- **AVG:** `(double)i128 / (double)valid`, and for DECIMAL `/ 10^scale`. This is
  the engine's finalize expression, in the same order.
- The result is emitted under the aggregate's identity and bound type, through
  the existing literal-substitution path, so wrappers such as
  `ROUND(AVG(x), 2)` keep working.

This combines with the existing kinds in the same query: ClickBench Q3 is
`SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)`.

## 5. Cost

- **Writer:** one extra pass over each summable column per row group. It could
  be fused into `FileStatsAccumulator::add`'s existing ordinalize loop, but that
  loop reads ordinal keys, not raw values, so fusing it is a second change.
  **A baseline of writer throughput is required before the first edit**
  (CTAS of `hits` to a scratch table, ABBA against the pre-change build).
- **Manifest size:** 16 bytes per summable column per file (int128). Hits has
  about 105 columns, most numeric, so roughly 1.5 KB per file. That is noise
  next to the sketches.
- **Planning:** one int128 fold per referenced column over the live files.

## 6. Expected benefit — measure before claiming

- **ClickBench Q3 and Q4** (Q4 only because the sum is int128). Each is tens of
  milliseconds today, so this is small in absolute terms.
- ⛔ **Prerequisite check:** `scratch/hits_rugo_262k` has no manifest file. The
  local `make clickbench` dataset gets its manifest from parquet footers, which
  carry no sums. **Q3 and Q4 would not change locally unless ANALYZE is run or
  D3 lands.** Before spending, prove this: run ANALYZE on the table, then
  confirm Q1/Q7 already take the stats path and Q3 flips to it.
- The larger payoff is the §9 range rewrite on date-ordered tables, where this
  stat is a precondition.

## 7. Tests

- **Equivalence:** for each admitted type, a stats answer equals the answer with
  `disable_statistics_only_response` set. Cover with nulls, all-null files, an
  empty table, and mixed file sizes.
- **Plan assertion:** the Scan is gone. This proves the path fired, not just
  that the answer matched.
- **Declines:**
  - a manifest where one file has no sum (old format, or an external parquet
    file);
  - deletes present;
  - an INT64 total overflow, where the engine raises "SUM overflow";
  - DISTINCT or FILTER;
  - UINT64, BOOL and temporal columns (until D4 admits them).
- **Round trip:** writer → manifest parquet → decode keeps the int128 exactly,
  including values above INT64 and negatives. An old manifest without the column
  decodes with all sums unknown.
- **Invalidation:** ALTER COLUMN widen int32→int64 keeps the sums, int→DECIMAL
  drops them, and DROP STATISTICS clears them.
- **AVG bit-identity:** a stats AVG equals the engine AVG with `==` on the
  double, for the integer and decimal cases.
- `make q` passes.

## 8. Decisions for the architect

- **D1 — Encoding of the int128 sum in the manifest parquet.** Options:
  - (a) two optional positional `ARRAY(INT64)` columns, `sums_hi` and
    `sums_lo`. This uses existing encode/decode machinery (same shape as
    `null_counts`);
  - (b) one `ARRAY(DECIMAL128)` column, if draken ARRAY and rugo support a
    DECIMAL128 leaf (not verified);
  - (c) one `ARRAY(ARRAY(INT64))` column of `[hi, lo]` pairs.

  *Recommend (a), unless (b) is confirmed working end to end.* A float sum (D2)
  would need an additional `ARRAY(DOUBLE)`.
- **D2 — FLOAT columns.** A manifest float sum adds values in a different order
  from a scan, so the last bits can differ. The engine's own parallel float SUM
  is already order-dependent across partitions, so no bit-exact float contract
  exists today. Options:
  - (a) exclude floats (exact types only);
  - (b) include them, and document that stats-answered float SUM/AVG is as
    order-dependent as a scan.

  *Recommend (a) for v1.* Also note that ClickBench Q3's `ResolutionWidth` is an
  integer.
- **D3 — Producer reach.** Should rugo write per-row-group sums into its parquet
  footer key-value metadata, so that rugo-written files carry sums without
  ANALYZE? Same question for skene (a format change). *Recommend: v1 = writer +
  ANALYZE + carry; decide D3 after measuring v1.*
- **D4 — Type allowlist.**
  - *Recommend v1:* signed integers, UINT8/16/32, DECIMAL and DECIMAL128.
  - *Exclude:* UINT64 (§10), BOOL, and the temporal types. The engine admits
    them to SUM, but stats support for them buys nothing.
- **D5 — Overflow in the stats path.** *Recommend:* decline and let the engine
  raise (§4.6), rather than raising from the optimizer.
- **D6 — Scope of the first increment.** *Recommend:* unfiltered SUM/AVG only.
  The covered/residual range rewrite (§9) is a separate design that consumes
  this stat.

## 9. Follow-on (not this design)

The covered/residual rewrite. For `Aggregate(Filter(range on one column), Scan)`,
classify each file by its bounds on the filter column:

- **disjoint:** contributes nothing;
- **covered:** contributes from stats;
- **boundary:** scanned with the original predicate.

The partials are then combined.

**Additional soundness rule we must have.** A file counts as covered only if its
null count **for the filter column is 0** and its bounds are exact rather than
truncated (so no strings). NULLs fail the predicate, so a covered file with
NULLs would over-count. Infino's implementation appears not to check this.

## 10. Adjacent issues found while researching (not in scope — reported)

1. **Suspected: engine SUM over UINT64.** `agg2_read_raw` reads a UINT64 as its
   int64 bit pattern, and `AggCell::isum` is an `__int128`. So `isum += raw`
   sign-extends, and any value ≥ 2^63 contributes a negative amount. The
   comment at `native_group_sinks.hpp` ~line 569 assumes int64 `+=`
   wrap-equivalence, which does not hold for an int128 accumulator. The INT64
   overflow check at finalize would then judge the wrong number. **Not
   reproduced.**
2. **`statistics_only_response.py:617`** uses `getattr(manifest, "has_deletes",
   None)`. getattr is banned on the same grounds as hasattr.
