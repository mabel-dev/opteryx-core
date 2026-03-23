# Two-Pass Column-Level Late Materialization for Selective Parquet Scans

## Motivation

ClickBench Q24 is:

```sql
SELECT * FROM testdata.clickbench_tiny
WHERE URL LIKE '%google%'
ORDER BY EventTime
LIMIT 10;
```

It runs in ~32 seconds. The hits table has ~105 columns. The predicate is
highly selective: roughly 1–2% of rows contain `google` in the URL column.

The current execution path in `ParquetReadNode`:

1. For each row group: fetch **all** projected columns (105 columns = the
   entire row) via `iter_row_groups`.
2. Apply the predicate on the assembled morsel.
3. Discard ~98–99% of decoded rows.

For every row group where zero rows match, we decoded ~105 columns worth of
bytes and threw them all away. For a table of any size this is the dominant
cost.

Splitting the scan into two passes per row group fixes this:

- **Pass 1** — fetch only the columns referenced in the predicate. Apply the
  predicate. Get a row mask.
- **Pass 2** — only if the mask is non-empty: fetch the remaining projected
  columns. Apply the mask. Assemble the final morsel.

If a row group has zero surviving rows after Pass 1, Pass 2 is skipped
entirely. For Q24 this eliminates ~98% of column decode work.

---

## Prerequisites: What Already Exists

Understanding what is already in place avoids duplicating work in the design.

### 1. LIKE `'%x%'` → `InStr` rewrite

`opteryx/planner/optimizer/strategies/predicate_rewriter.py` already rewrites:

```
URL LIKE '%google%'   →   URL InStr 'google'
```

Condition: no underscores in the pattern, leading `%`, trailing `%`, no `%` in
the interior. `google` satisfies all three. The rewrite produces an `InStr`
COMPARISON_OPERATOR node (`INSTR_REWRITES` dict, confirmed in the file).

`InStr` dispatches to `StringVector.contains(substr, ignore_case=False)` in the
Draken evaluator — a direct substring search, faster than full regex matching.
Case-insensitive variant `IInStr` → `StringVector.contains(substr, True)`.

No new rewrite rules are needed. The two-pass path simply benefits from the
existing rewrite because `InStr` operates on a single column (`URL`), which is
cheap to decode in Pass 1.

### 2. Predicate pushdown

`PredicatePushdownStrategy` already pushes predicates into
`ParquetReadNode.predicates` when the root node is a `COMPARISON_OPERATOR`.
`InStr` is a `COMPARISON_OPERATOR` value. The pushed predicate is therefore
already present on the node before `execute()` runs.

The current `_apply_predicates_to_morsel()` method evaluates pushed predicates
natively over Draken vectors — no Arrow round-trip. This evaluation will be the
Pass 1 gate in the two-pass path.

### 3. `fetch_columns` API

`opteryx/parquet_io/reader.py` exposes:

```python
fetch_columns(
    filesystem, path, rg_idx, column_names,
    cache=None, decoder=None, connector=None,
) -> Dict[str, Any]
```

This function fetches and decodes a specific subset of columns for a single
`(path, rg_idx)` pair. It uses the cached footer to locate byte ranges and
issues a single coalesced `read_ranges()` call. It is already used internally
by `_fetch_columns_task` — the per-row-group unit of work dispatched to the
thread pool inside `iter_row_groups`.

**Pass 2 will call `fetch_columns` directly**, targeting only the columns not
already fetched in Pass 1.

### 4. InMemoryParquetCache

`ParquetReadNode.execute()` already allocates one `InMemoryParquetCache` per
query execution. Footer bytes and decoded column chunks are cached there. Pass 2
will reuse the same cache instance, so the footer is only parsed once and any
column chunks shared between Pass 1 and Pass 2 are not re-fetched.

---

## Architectural Boundary

No change to the existing boundary:

- `parquet_io/reader.py` owns byte-range planning, fetch scheduling, and decode.
  It does not know about SQL predicate semantics.
- `operators/parquet_read_node.py` owns predicate evaluation, row masking, and
  projection assembly. This is where the two-pass execution lives.

The `_is_row_level_evaluable` gate proposed in
`parquet-expanded-predicate-pushdown-design.md` (Part A) is complementary:
the two-pass path does not depend on it but benefits from any predicates it
makes newly pushable, because a wider pushed predicate set means a smaller
Pass 2 footprint.

---

## Design

### Column partition

At the start of `execute()` (before the row-group loop), compute:

```
filter_columns   = {cols referenced in self.predicates}
proj_columns     = {cols in final projection (SELECT clause)}

pass1_columns    = filter_columns
pass2_columns    = proj_columns - filter_columns   # columns only needed for output
all_columns      = proj_columns ∪ filter_columns   # total required set (unchanged)
```

When `pass2_columns` is empty (all projected columns are also filter columns,
e.g. `SELECT URL FROM … WHERE URL LIKE '%x%'`), the path degrades to the
current single-pass behaviour automatically — no special case needed, just skip
the second fetch.

When `pass1_columns` is empty (no pushed predicates), the two-pass path is also
a no-op: fall through to the single-pass path.

### Execution shape

**Current (single pass):**

```
iter_row_groups(all_columns)
│
for each row_group:
    build Morsel from all_columns
    apply predicate → row_mask
    select(output_identity_order)
    cast to schema
    yield
```

**Two-pass execution:**

```
iter_row_groups(pass1_columns)
│
for each row_group:
    p1_morsel = Morsel.from_vectors(pass1_identity_names, pass1_vectors)

    # Retain the raw BoolVector — needed to filter both passes with the same mask.
    mask = evaluate_draken(predicate_root, p1_morsel)   # BoolVector

    if mask.sum() == 0:
        continue                            # ← zero-hit fast path: skip pass 2

    p1_filtered = p1_morsel.filter_mask(mask)

    if len(pass2_columns) == 0:
        # All projected cols already in pass1; no second fetch needed.
        result_morsel = p1_filtered
        → select(output_identity_order) → cast → yield

    else:
        # Fetch remaining projected columns for this (path, rg_idx).
        pass2_result = fetch_columns(
            filesystem, path, rg_idx, pass2_column_names, cache, connector=…
        )
        # Build and filter the Pass 2 morsel with the same mask.
        pass2_morsel   = Morsel.from_vectors(pass2_identity_names, pass2_vectors)
        pass2_filtered = pass2_morsel.filter_mask(mask)

        # Append Pass 2 columns into the already-filtered Pass 1 morsel.
        for col_identity, vector in zip(pass2_identity_names,
                                        [pass2_filtered.column(n) for n in pass2_encoded_names]):
            p1_filtered.append_vector(col_identity, vector)

        result_morsel = p1_filtered
        → select(output_identity_order) → cast → yield
```

**Key API points:**

- `evaluate_draken(predicate_root, morsel)` returns a `BoolVector` mask.
  Calling it directly (rather than delegating to `_apply_predicates_to_morsel`)
  gives us the raw mask to reuse for Pass 2. The existing
  `_apply_predicates_to_morsel` helper is kept for the single-pass fallback
  path; the two-pass loop calls `evaluate_draken` inline.
- `morsel.filter_mask(BoolVector)` is a public `def` method on `Morsel`. It
  applies correct SQL three-valued-logic null semantics (null = row excluded).
- `morsel.append_vector(name, vector)` is a `cpdef` method that appends a
  single column vector in place. Both Pass 1 and Pass 2 morsels have identical
  row counts after filtering with the same mask, so lengths are guaranteed to
  match.
- There is no `merge_columns` method on `Morsel`; column-by-column
  `append_vector` is the correct assembly primitive.

### Function node evaluation

The current `_apply_predicates_to_morsel` prepends a call to
`evaluate_and_append_draken` when `FUNCTION` nodes are present in the predicate
tree (to materialise computed columns referenced by the predicate before
evaluating the comparison). The two-pass loop must replicate this:

```python
function_nodes = get_all_nodes_of_type(predicate_root, (NodeType.FUNCTION,))
if function_nodes:
    p1_morsel = evaluate_and_append_draken(function_nodes, p1_morsel)
mask = evaluate_draken(predicate_root, p1_morsel)
```

Appended function columns are temporary; they live only in the Pass 1 morsel
and are dropped when `select(output_identity_order)` is applied at the end.
Since `pass2_columns` is computed from `proj_columns - filter_columns` (not
from the temporary function columns), no Pass 2 fetch will include them.

### Pass 2 fetch is synchronous in the main loop

`iter_row_groups` fans out all `(path, rg_idx)` Pass 1 units to the thread
pool. The main loop yields Pass 1 results via `as_completed`. When a row group
has survivors, the main loop calls `fetch_columns` directly (synchronously) for
Pass 2 before yielding.

This does not block the thread pool — Pass 1 futures for other row groups
continue completing concurrently. The only cost is that the caller thread is
occupied with the Pass 2 fetch for the current row group while pool workers run
Pass 1 for other row groups. For the expected 1–2% selectivity of Q24, this is
an acceptable trade-off in V1.

A follow-up enhancement (not in scope for V1) is to dispatch Pass 2 as a new
future immediately after the Pass 1 result is processed, allowing full
pipelining.

### Abandonment heuristic

The two-pass split is only beneficial when Pass 1 eliminates a significant
fraction of rows. If the predicate is non-selective (most rows survive Pass 1),
two passes waste time with no reward.

**Heuristic**: maintain a per-file counter `consecutive_pass_through_rgs`. After
each row group:

- If `count(row_mask) == rg_row_count` (all rows survived, no benefit): increment.
- Otherwise (at least one row was eliminated): reset to zero.

When the counter reaches `PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER` (default
5), disable two-pass mode for all remaining row groups in the current file and
fall back to single-pass (read `all_columns` in one call). Reset the counter at
the start of each new file.

Rationale: selectivity of consecutive row groups within a file is usually
similar; if five consecutive groups show zero benefit, the predicate is not
selective enough on this file to justify the overhead.

This mirrors the bloom-filter discard heuristic used by the join path: measure
benefit empirically, stop paying overhead when it is not earned.

### LIMIT interaction

`records_to_read` (the pushed LIMIT) is enforced after Pass 2. If
`records_to_read` drops to zero mid-file, break the row-group loop. No
in-flight Pass 2 futures to cancel (synchronous V1 design).

### Repeated / list column fallback

Row groups that trigger `has_repeated_projection` already route to
`_execute_full_file_fallback`. The two-pass path does not apply there. The
fallback continues to read all columns in one shot as today.

### SELECT * and identity ordering

`SELECT *` causes `proj_columns = all table columns`. The `pass2_columns` set
will be `all_columns - filter_columns`. For Q24 with ~105 columns and one
filter column (`URL`), `pass2_columns` has ~104 columns. The savings are
proportionally large.

`output_identity_order` is computed from `output_schema` (projection-only
columns in schema order). After assembling `result_morsel` from Pass 1 and
Pass 2 pieces, call `result_morsel.select(output_identity_order)` to restore
column order, exactly as the current single-pass path does.

---

## INSTR and the predicate pushdown gap

`LIKE '%x%'` is already rewritten to `InStr` and already pushed. The two-pass
path therefore benefits Q24 automatically — no predicate pushdown changes are
required to make it faster.

However, the `PredicatePushdownStrategy` gate currently only admits predicates
whose root is `COMPARISON_OPERATOR`. This misses compound predicates like:

```sql
WHERE URL LIKE '%google%' AND SearchPhrase <> ''
```

The compound `AND` root is not pushed today; it stays as an upstream
`FilterNode`. See `parquet-expanded-predicate-pushdown-design.md` Part A for
the full treatment. Part A and the two-pass path are independent but
complementary:

- Part A widens the set of pushed predicates.
- The two-pass path uses pushed predicates to avoid decoding projection-only
  columns.

Together they cover compound-predicate queries with wide projections.

---

## Configuration

Two new knobs, both in `opteryx/config.py` and the `Features` class:

| Name | Type | Default | Meaning |
|------|------|---------|---------|
| `FEATURE_PARQUET_LATE_MATERIALIZATION` | bool | `True` | Enable two-pass column fetch when applicable. |
| `PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER` | int | `5` | Consecutive fully-passing row groups before abandoning two-pass mode for the rest of the file. |

The feature is on by default because it is a strict improvement for selective
queries and the abandonment heuristic recovers quickly on non-selective ones.
The flag exists to allow emergency rollback.

---

## Observability

New sensor keys added to `ParquetReadNode.sensors()`:

| Key | Type | Description |
|-----|------|-------------|
| `parquet_latmat_pass1_row_groups` | counter | Row groups where Pass 1 ran. |
| `parquet_latmat_pass2_row_groups` | counter | Row groups where Pass 2 ran (had survivors). |
| `parquet_latmat_skipped_row_groups` | counter | Row groups skipped entirely (zero survivors after Pass 1). |
| `parquet_latmat_abandoned_files` | counter | Files where the abandonment heuristic triggered. |
| `parquet_latmat_pass1_bytes` | counter | Bytes fetched for filter-only columns across all Pass 1 fetches. |
| `parquet_latmat_pass2_bytes` | counter | Bytes fetched for projection-only columns across all Pass 2 fetches. |
| `parquet_latmat_single_pass_bytes` | counter | Bytes fetched after abandonment (single-pass fallback). |

These metrics allow direct measurement of the savings ratio
`parquet_latmat_skipped_row_groups / parquet_latmat_pass1_row_groups` and the byte
reduction
`parquet_latmat_pass1_bytes / (parquet_latmat_pass1_bytes + parquet_latmat_pass2_bytes + parquet_latmat_single_pass_bytes)`.

---

## Implementation Plan

### Phase 1 — Core two-pass in `ParquetReadNode`

Changes are confined to `operators/parquet_read_node.py` unless otherwise noted.

- [ ] **Column partition**: compute `pass1_column_names` and
      `pass2_column_names` at the top of `execute()` from `filter_identity_set`
      and `output_identity_set`.

- [ ] **Feature gate**: if `not config.features.parquet_late_materialization` or
      `len(pass2_column_names) == 0` or `not self.predicates`, fall through to
      the existing single-pass code path unchanged.

- [ ] **Pass 1 column read**: change the `iter_row_groups(...)` call to pass
      `pass1_column_names` instead of `column_names`.

- [ ] **Zero-hit fast path**: after evaluating the predicate mask, if no rows
      survive, `continue` to the next row group without issuing any Pass 2 I/O.
      Increment `parquet_latmat_skipped_row_groups`.

- [ ] **Pass 1 mask retention**: in the two-pass loop, call
      `evaluate_draken(predicate_root, p1_morsel)` directly to obtain the raw
      `BoolVector` mask. Do NOT call `_apply_predicates_to_morsel` here — that
      helper discards the mask and returns only the filtered morsel. Handle
      `FUNCTION` node pre-evaluation with `evaluate_and_append_draken` before
      calling `evaluate_draken`, mirroring the logic inside
      `_apply_predicates_to_morsel`.

- [ ] **Pass 2 fetch**: for row groups with survivors, call
      `fetch_columns(filesystem, path, rg_idx, pass2_column_names, cache,
      connector=connector_type)` synchronously. Account for the returned
      `__bytes_fetched__` and other instrumentation keys in the readings dict.

- [ ] **Morsel assembly**:
      1. `p1_filtered = p1_morsel.filter_mask(mask)`
      2. `pass2_morsel = Morsel.from_vectors(pass2_identity_names, pass2_vectors)`
      3. `pass2_filtered = pass2_morsel.filter_mask(mask)`
      4. For each column in `pass2_filtered`:
         `p1_filtered.append_vector(identity, pass2_filtered.column(encoded_name))`
      5. `result_morsel = p1_filtered`
      Then proceed with `result_morsel.select(output_identity_order)` and
      `_cast_morsel_to_schema` as before.
      Note: `merge_columns` does not exist on `Morsel`; `append_vector` is the
      correct primitive.

- [ ] **Abandonment heuristic**: maintain `consecutive_pass_through` counter per
      file. Reset on file change. Increment when `pass_mask_count ==
      rg_row_count`. When counter reaches
      `config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER`, set a local
      `two_pass_active = False` flag for the rest of the current file. Reset
      flag at the start of each new file. When `two_pass_active` is False, issue
      the full `all_columns` fetch as a single pass (the current behaviour).

- [ ] **Telemetry**: add all sensor keys listed in the Observability section.
      Accumulate `parquet_latmat_pass2_bytes` from `__bytes_fetched__` returned by
      `fetch_columns`.

- [ ] **Config**: add
      `FEATURE_PARQUET_LATE_MATERIALIZATION = bool(get(…, True))` and
      `PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER = int(get(…, 5))` to
      `config.py` and the `Features` class.

- [ ] **Unit test**: verify that for a mock predicate with 0 survivors, the
      `fetch_columns` call for Pass 2 is never issued.

- [ ] **Unit test**: verify that `select(output_identity_order)` produces the
      correct column order after Pass 1 + Pass 2 assembly.

- [ ] **Integration test**: run Q24 with `FEATURE_PARQUET_LATE_MATERIALIZATION=1`
      and assert `parquet_latmat_skipped_row_groups > 0`.

- [ ] **Benchmark**: measure Q24 before and after. Record wall time and
      `parquet_latmat_skipped_row_groups / parquet_latmat_pass1_row_groups` ratio.

- [ ] **Regression check**: run full ClickBench battery. All 42 queries must
      pass. No material regression (> 10%) on non-selective queries (Q01–Q08,
      Q37–Q43).

### Phase 2 — Parallel Pass 2 dispatch (future work, not in scope V1)

Instead of calling `fetch_columns` synchronously in the main loop, submit Pass 2
as a new future to the shared `_RANGE_POOL` immediately after Pass 1 completes.
This allows the pool to overlap Pass 2 I/O for one row group with Pass 1 I/O for
later row groups.

Prerequisite: the main loop must track per-row-group `(pass1_result,
pass2_future)` pairs and re-join them in a second `as_completed` sweep. This is
more involved and should be validated against Phase 1 benchmark data first to
confirm it closes a real gap.

---

## Non-Goals (V1)

1. **Row-level selection within a column chunk**: the two-pass path still
   decodes the full column chunk for surviving row groups. Decoding only the
   rows at specific indices within a chunk (true row-level late materialization)
   requires Parquet RLE-level interception inside rugo. Out of scope.

2. **Row-group pruning for INSTR / LIKE**: Parquet footers store min/max
   statistics, not substring indices. `extract_predicate_stats` correctly skips
   INSTR predicates. Bloom filters over string column contents could allow
   INSTR-based row-group pruning; this is tracked separately in
   `parquet-bloom-footer-mutator-design.md`.

3. **Repeated / list column fallback path**: the full-file fallback already
   applied to repeated columns is unchanged.

4. **IO-process ring path** (`io_process_ring.py`): the ring-based reader uses
   a different scheduler. V1 targets the thread-pool reader only. Porting is a
   follow-on task once V1 is validated.

5. **Expanded predicate pushdown (Part A)**: the `_is_row_level_evaluable` gate
   remains a separate change. The two are additive: Part A makes more predicates
   eligible for pushdown; the two-pass path makes pushed predicates pay for
   fewer column bytes.

---

## Expected Performance Impact

For Q24 specifically:

| Metric | Before | After (estimate) |
|--------|--------|-----------------|
| Row groups with zero matches | ~98–99% decoded in full | skipped in Pass 2 |
| Columns decoded per zero-match row group | 105 | 1 (URL only) |
| Columns decoded per matching row group | 105 | 105 (unchanged) |
| Total column-decode work | 100% | ~3–5% |
| Expected wall-time improvement | baseline | 10–25× |

The estimate is wide because the actual savings depend on the compression ratio
of the URL column vs the aggregate table width, and on the ratio of I/O time
vs CPU decode time. The sensor metrics in Phase 1 will produce the data needed
to narrow this range.

For non-selective queries (e.g. Q01 `SELECT COUNT(*)`), the abandonment
heuristic fires within the first five row groups and falls back to single-pass.
The overhead is five extra `fetch_columns` calls returning full results before
abandonment — negligible relative to total query time.

---

## Exit Criteria

- [ ] Q24 benchmark shows ≥ 10× wall-time improvement with two-pass mode
      enabled vs disabled on the clickbench_tiny dataset.
- [ ] `parquet_latmat_skipped_row_groups / parquet_latmat_pass1_row_groups` ≥ 0.90
      for Q24.
- [ ] ClickBench 42/42 queries pass with `FEATURE_PARQUET_LATE_MATERIALIZATION`
      enabled.
- [ ] No query in the ClickBench battery regresses by more than 10% compared to
      the disabled baseline.
- [ ] Sensor output shows `parquet_latmat_abandoned_files > 0` for non-selective
      queries (confirming the abandonment heuristic is firing).
- [ ] Unit tests cover: zero-survivor skip, correct column ordering after
      assembly, and abandonment counter reset across file boundaries.
