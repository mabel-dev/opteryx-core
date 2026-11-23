# Grouped MIN/MAX Top-k pruning — `GROUP BY g ORDER BY MIN(x) LIMIT k`

Status: **design, nothing built.** Decisions for the architect are in §8.

Motivating query: JSONBench Q4 (`GROUP BY did ORDER BY MIN(time_us) ASC LIMIT 3`), 187 ms at
100M on m6i.8xlarge against ClickHouse's 98 ms. Profile (Mac, duplicated 100M, 2026-11-23):
the GROUP BY is ~90% of CPU — sink 49% (MIN update, first-seen probes, key appends), finalize
merge 35% — and every bit of it is spent on groups that cannot be in the answer.

---

## 0. Summary

A query of the shape

```sql
SELECT g…, MIN(x) AS m FROM t [WHERE p] GROUP BY g… ORDER BY m ASC [, …] LIMIT k [OFFSET o]
```

needs only the `n = k + o` groups with the smallest `MIN(x)`. Once any `n` distinct groups
have been seen, the `n`-th smallest of their current MINs is a **boundary `B`**: a row whose
`x` is strictly greater than `B` can never set the MIN of a group in the result. So such rows
can be dropped before the GROUP BY touches them, and a row group whose footer `min(x) > B` can
be skipped without being fetched or decoded. `MAX(x) … DESC` is the mirror.

This is the existing Top-N runtime boundary (`docs/TOPN_RUNTIME_BOUNDARY_DESIGN.md`,
`src/cpp/engine/topn_boundary.hpp`) with a **new producer**: the GROUP BY sink instead of the
TopN sink. The shared value (`TopNBoundary`, one relaxed atomic that only tightens), the
ordinal space (draken's) and the consumer (the parquet scan's submit-time row-group skip) are
reused unchanged.

For Q4 the data is clustered by the operand: the Bluesky files are in time order, so after the
first morsels `B` is a timestamp inside file 1 and every later file's post row groups have
`min(time_us) > B`. Expected effect: Q4 at 100M decodes the in-flight window plus file 1
instead of ~150 row groups. Not measured — see §5.

---

## 1. Why the existing mechanisms do not cover this

- `TopNScanPushdownStrategy` and the runtime boundary require the HeapSort to read **directly**
  from a single Scan (TOPN §6.1). Q4 has `HeapSort → Projection → Aggregate → Scan`.
- `TopNManifestPruningStrategy` prunes files at plan time from a row-level bound; with GROUP BY
  there is no plan-time bound (it needs `n` *distinct groups*, unknown from statistics).
- `GROUP BY k LIMIT n` key bound (GLKBOUND) orders by the group **key**, not an aggregate.

---

## 2. Correctness argument

Notation: ordinals are draken's (`cxx_ordinal.h`); ASC throughout, DESC mirrors (§2.6).

### 2.1 The boundary is valid from any one worker's partial view

A worker holds groups whose *local* MIN is the minimum over the rows **it** has seen. For every
group, `true MIN ≤ local MIN`. If a worker holds `n` distinct groups whose local MINs are all
`≤ B_w`, then at least `n` groups have `true MIN ≤ B_w`, so the true `n`-th smallest MIN is
`≤ B_w`. Hence `B_w` is a valid boundary, and so is `min over workers of B_w` — the existing
`TopNBoundary::offer` CAS. A stale read is looser, never unsound (TOPN §2.4).

### 2.2 Dropping a row with `x > B` cannot change the result

Let `R` be the true result (the `n` groups with smallest MIN). Each group in `R` has
`true MIN ≤ (n-th smallest MIN) ≤ B`, and its MIN-defining row has `x = true MIN ≤ B` — it is
**kept** (the test is strict). So every group in `R` keeps its exact MIN.

A group not in `R` loses only rows with `x > B`; its computed MIN is `≥` its true MIN `> B`
(or it never forms). It still ranks after the `≥ n` groups with MIN `≤ B`, so it cannot enter
the cut. The downstream HeapSort makes the canonical cut exactly as today.

**Ties.** Rows with `x == B` are kept and row groups with `min == B` are kept, so every group
whose MIN equals the boundary is present with its exact MIN. Secondary ORDER BY keys therefore
break ties exactly as in the un-pushed plan.

### 2.3 Other aggregates make it unsound

A dropped row would still have counted in `COUNT(*)`, `SUM(y)`, `MAX(x)`, … of a group that
**is** in `R` (that group's other rows exceed `B`). So the aggregate list must be **only**
`MIN(x)` (§6.2). Same for HAVING (its predicate would see the partial aggregates) and
GROUPING SETS / ROLLUP.

### 2.4 NULLs

`MIN` ignores NULLs; a group's MIN is NULL iff all its *kept* rows have NULL `x`. Opteryx sorts
NULL lowest: **NULLS FIRST under ASC** (the default for Q4's shape).

- **NULLS LAST** (explicit, or DESC by default): safe as is. A group wrongly left with a NULL
  MIN (its non-null rows were all `> B`, its kept rows NULL) sorts after the `≥ n` non-null
  groups that published `B` and never reaches the cut.
- **NULLS FIRST**: unsafe in general. Group `{x: NULL, 100}` with `B = 50`: dropping `100`
  leaves MIN NULL, which sorts *first* and would enter the result. So under NULLS FIRST the
  mechanism arms only when `x` is **proven non-NULL over every row group the scan will read**
  (D3): column declared NOT NULL, or footer `null_count == 0` (known) on every row group that
  survives plan-time pruning. The proof is checked at plan time; at run time a NULL `x`
  reaching the armed sink is a contradiction and **fails loud** (it means the footer lied).
- The tracker counts **non-null** values only (as TOPN §2.2), so a NULL never tightens `B`.

### 2.5 What the tracker must count

`n` **distinct groups**, not `n` rows: two rows of one group must not both count. Identity is the
row's 64-bit GROUP BY key hash — the same hash identity the sink already uses for groups
(`native_group_sinks.hpp` Pass B), so it is consistent across the dict, partitioned and ticket
paths and across `maybe_flush` windows (gids are not stable across flushes; hashes are).

### 2.6 DESC

`MAX(x) ORDER BY … DESC`: `B` is the `n`-th largest local MAX, rows with `x < B` are dropped,
row groups with `max(x) < B` skipped. NULL rule depends only on placement (DESC default is
NULLS LAST, so the footer proof is not needed).

---

## 3. Producer: the GROUP BY sink

When armed, at sink entry per morsel (before Pass A):

1. **Morsel skip.** If `B` is published and `cxx_ordinal_bounds_c(m, x)` gives `lo > B`, drop
   the morsel. One pass over `x`; catches whole late morsels that row-group skipping missed
   (the in-flight window, §4).
2. **Row filter.** Otherwise, if `B` is published, mask rows with `ord(x) > B` and continue with
   the kept rows. After warm-up almost every surviving morsel is either fully dropped (1) or
   fully kept, so the mask is rarely partial.
3. **Track.** Feed each kept non-null row's `(key hash, ord(x))` to a per-worker tracker: `n`
   slots, distinct by hash; a row whose hash is in the tracker lowers that slot; otherwise, if
   it beats the worst slot, it replaces it. When `n` slots are full, `offer(worst)` to the shared
   `TopNBoundary`. After warm-up only rows with `ord(x) ≤ B` reach this, so it is cold.
4. Pass A/B/C and finalize run unchanged on the kept rows.

Per-row ordinals: draken has `cxx_ordinal_bounds_c` and `cxx_ordinal_topn_c`, but no per-row
ordinal or `ord(x) ≤ B` mask. Needs one new draken C-ABI entry (D4); the engine must not
re-derive ordinals (`sort_num_key` is a different space — TOPN header).

---

## 4. Consumer: the scan

Unchanged: `topn_excludes_row_group` at the parquet submit loop
(`native_parquet_scan_source.hpp`, TOPN §4.1) tests `ord(rg.x.min) > B` plus the NULL rule,
counts `row_groups_skipped`. The only new wiring is that the boundary comes from the GROUP BY
rather than a TopN sink, and that the scan's column for `x` may be a **proven VARIANT path read
as its leaf** (Q4's `CAST(data->>'time_us' AS INT64)`), whose stats are the leaf's
(`nested_json.cpp` `resolve_projection` aliasing). That mapping must be verified before build
(§9, open item 2).

In-flight lag (TOPN §4.4) applies: units submitted before `B` tightens are decoded anyway; the
sink's morsel skip (§3.1) catches them after decode.

---

## 5. Expected effect — not measured

- **Q4, 100M official:** post rows are ~9% of the data, ~1–2 row groups per file, ~150 in all.
  With `B` set inside file 1 after the first morsels, everything outside the in-flight window
  skips. The 49% sink and 35% finalize shares shrink with the groups that never form.
- **Q4, 10M:** ~15 post row groups, most already in flight before `B` exists: small gain.
- **Q5** (`MAX − MIN … DESC`): not monotone in rows — ineligible.
- **Fixture caveat:** the local "100M" is ten hard-linked copies of 10 files, so ten copies of
  file 1 tie at the boundary and are kept. A real 100M set (90 more files) is needed for a
  representative number.

---

## 6. Eligibility (decided in the compiler; any miss ⇒ wire nothing)

1. Plan shape: `HeapSort(n) → [Projection] → Aggregate(GROUP BY) → Scan`, single scan, no join,
   no Filter/HAVING above the aggregate. A WHERE below the aggregate is fine (pushed into the
   scan; the sink only sees rows that pass it).
2. Aggregate list is exactly one `MIN(x)` for ASC (or `MAX(x)` for DESC); the projection may
   repeat it. No DISTINCT aggregates, no GROUPING SETS/ROLLUP/CUBE.
3. The HeapSort's **leading** key is that aggregate, directly or through a whitelisted
   monotone non-decreasing projection: identity, `CAST(INT64 AS TIMESTAMP[u])`, lossless
   integer widening. Secondary keys allowed.
4. `x` is a direct scan column (or proven VARIANT leaf path) under TOPN §6.4–6.5: integers
   (not UINT64), DATE32, TIMESTAMP64; no value-rewriting decode.
5. NULL rule §2.4 (proof required under NULLS FIRST).
6. `1 ≤ n = k + o ≤ 2³²−1`.

---

## 7. Telemetry, switch, tests

- Telemetry: reuse `row_groups_pruned_topn` (marginal skips); add `groupby_topk_rows_dropped`
  and `groupby_topk_morsels_dropped`; plan-time `groupby_topk_armed`.
- Switch: `disable_topn_runtime_boundary` already exists; D6 asks whether this rides on it or
  gets its own.
- Oracle (armed vs disabled, result sort-key columns, full rows where the boundary is unique):
  ASC/DESC; NULLS FIRST/LAST; groups mixing NULL and non-NULL `x` across row groups; the §2.4
  counter-example `{NULL, 100}`; ties at `B` across row groups and workers; secondary ORDER BY
  keys; LIMIT larger than the group count; OFFSET; WHERE matching nothing; row groups without
  stats; dict-path, partitioned and ticket-mode sinks.
- Refusals asserted not armed: a second aggregate, COUNT(*), HAVING, FLOAT/DECIMAL/string `x`,
  non-monotone projection (`-MIN(x)`, `MIN(x) % 10`), NULLS FIRST without proof.
- Contradiction test: armed under footer proof, a NULL `x` arrives ⇒ fails loud.
- Performance: baseline first; interleaved A/B; JSONBench Q4 at 10M and real 100M; ClickBench
  total as the regression check for queries that never arm. Build all parts, then leave-one-out
  (scan skip / morsel skip / row filter).

---

## 8. Decisions

| # | Decision | Recommendation |
|---|---|---|
| **D1** | Proceed at all. Wins where data is clustered by the aggregated column ("first/last activity per entity, top k"); JSONBench Q4 is the measured case. Nothing on TPC-H. | Proceed. |
| **D2** | v1 scope: MIN ASC and MAX DESC, single aggregate, parquet single-pass consumer. | As stated. Skene/latmat consumers and multi-aggregate (e.g. `MIN(x), ANY_VALUE(y)` cases) later, if ever. |
| **D3** | NULLS FIRST policy: (a) plan-time footer/NOT NULL proof + fail-loud runtime contradiction, or (b) refuse NULLS FIRST entirely (then Q4 needs an explicit NULLS LAST, which changes the benchmark SQL — not allowed). | **(a).** Q4 needs it; the footer carries `null_count`; R-a (trust the footer) is the precedent, with fail-loud instead of wrong answers since the sink can see the violation. |
| **D4** | Draken seam for the row filter: new per-row `ord(x) ≤ B` mask entry (`cxx_ordinal_le_mask_c`), or per-row ordinals returned to the engine. | **Mask entry.** One pass, no ordinal buffer, draken keeps the ordinal space. |
| **D5** | Which levels: scan row-group skip (reused), sink morsel skip, sink row filter. TOPN D8 left row-level use out after FILTERING_HEAPSORT mispredicted. | **Build all three, ablate.** Here the row filter replaces probe + MIN + key append per dropped row, not a heap insert — a different trade. Keep only what measures. |
| **D6** | Switch: share `disable_topn_runtime_boundary` or a new `disable_groupby_topk_boundary`. | **New switch** — different producer, independent failure mode. |
| **D7** | Group identity in the tracker: key hash (consistent with Pass B hash identity) or gid. | **Key hash.** gids move across flush windows. |

---

## 9. Open items to verify before build

1. Compiler: how `first_post_ts` (an alias over `CAST(MIN(...))`) reaches the HeapSort's order
   key — confirm the compiler can see through the Projection to the aggregate and the cast.
2. Scan: `topn_excludes_row_group` reads stats by the projected column name; confirm a proven
   VARIANT path aliased to its int64 leaf carries the leaf's min/max/null_count under that name
   in every file, and refuse when any file reads the path as a group (not aliased).
3. Engine wiring: the boundary's producer is currently the TopN sink in the same pipeline
   (`set_topn_sink`, `compiler.py:2912`); the GROUP BY sink and the scan are also one pipeline,
   so the no-barrier argument (TOPN §2.4) carries over — confirm no pipeline break is inserted
   between them (e.g. by spill or ticket mode).
4. Planner row estimate for the filtered VARIANT scan is 99,999 against 8.96M actual at 100M
   (feeds ticket-mode sizing). Unrelated to this design, reported separately.

---

## 10. Code map

| Piece | Where |
|---|---|
| Shared boundary, tracker pattern | `src/cpp/engine/topn_boundary.hpp` |
| Row-group skip (consumer, reused) | `src/cpp/engine/native_parquet_scan_source.hpp` (`topn_excludes_row_group`) |
| GROUP BY sink (new producer) | `src/cpp/engine/native_group_sinks.hpp` (`GroupBySink::sink`, entry before Pass A) |
| Draken ordinal seam | `draken/morsels/cxx_ordinal.h` (+ new mask entry, D4) |
| HeapSort compile / boundary wiring | `opteryx/managers/execution/compiler.py:2912` |
| Plan-shape stamp (pattern to mirror) | `opteryx/planner/optimizer/strategies/topn_scan_pushdown.py` |
| VARIANT leaf aliasing | `rugo/src/parquet/nested_json.cpp` (`aliased_leaf`, `resolve_projection`) |
