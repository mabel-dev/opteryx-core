# Q27 — Filter + Top-N over a High-Selectivity String Filter — Design

**Status:** Investigation + proposal for architect review
**Author:** clickbench scan/filter investigation, 2026-06-21
**Trigger:** ClickBench Q27 is ~12.7× DuckDB (the worst of the remaining >10× set).

```sql
-- Q27
SELECT SearchPhrase
FROM hits
WHERE SearchPhrase <> ''
ORDER BY EventTime, SearchPhrase
LIMIT 10;
```

---

## 1. Measurement (warm, local mmap, 92M-row `scratch.hits`)

`EXPLAIN ANALYZE` self-times:

| operator | rows out | self_ms |
|---|---|---|
| Heap Sort (LIMIT 10, ORDER EventTime, SearchPhrase) | 10 | ~46 |
| Projection (SearchPhrase) | 12.2M | ~1 |
| **Filter (`SearchPhrase IS NOT EMPTY`)** | **12.2M** | **~302–330** |
| Parquet Read (SearchPhrase + EventTime) | 92M | ~44 (summed across workers; parallel) |

Wall ≈ 395–416 ms. DuckDB ≈ 33 ms.

**The filter dominates.** It is *not* the predicate evaluation — that is now
dict-aware and cheap (see §2). It is the cost of **materializing 12.2M survivor
rows** (SearchPhrase dict + EventTime int64) via `morsel.filter_mask`, single
-threaded, before a top-10 sort throws ~12.2M of them away.

---

## 2. What is already good

- **Predicate eval is dict-aware.** SearchPhrase reaches the filter §11
  Dict-shaped (measured `data_length` ~300–900 vs morsel length 12K–124K).
  `impl_string_emptiness` (`opteryx/compiled/nanobind/vector_accessors.cpp`)
  now resolves emptiness once per unique slot and gathers per row (landed
  2026-06-21), so the IS [NOT] EMPTY compute is O(unique)+O(n) cheap.
- **Decode is parallel** and not the wall bottleneck (worker-count sweep flat).

So the remaining cost is structural: **we build a 12.2M-row intermediate for a
LIMIT 10.**

---

## 3. Root causes

### 3.1 The filter is a blocking, fully-materializing operator
`FilterNode._push_impl` (`opteryx/operators/filter/filter.pyx:234`) does
`mask = eval(predicate); filtered = morsel.filter_mask(mask)`. `filter_mask`
compacts every surviving row of every carried column. For Q27 every passing row
(12.2M of 92M) is compacted even though only 10 survive the LIMIT.

### 3.2 The predicate cannot use the C-native nogil filter path
`filter.pyx:249` has a fused eval+mask nogil fast path
(`_filter_morsel_c_native`), but it only fires when `bc.is_all_c_native`.
`IsNotEmpty` compiles to `BC_UNARY_OP`, which is **excluded** from the c-native
opcode set (`compiled_expression.pyx:1211–1232` — only loads, bool combinators,
ordinal `BC_COMPARE`, and fixed-width `BC_BINARY_OP`/`BC_CAST` qualify). So the
filter falls to the GIL VM + `filter_mask` — eval and compaction both GIL-held,
single-threaded.

### 3.3 Filter + Sort + Limit are not fused into a streaming Top-N
`Heap Sort` already implements a bounded heap (LIMIT 10), but it sits *above* a
filter that has already materialized 12.2M rows. There is a `topn_scan_pushdown`
strategy and a `fuse operators heap sort` optimization (both fire here per the
plan's OPTIMIZATIONS list), but neither eliminates the 12.2M-row intermediate
because the **filter** is between the scan and the sort.

### 3.4 Pushing the filter into the scan is blocked by a rewrite
`predicate_rewriter` rewrites `SearchPhrase <> ''` → `IsNotEmpty` *before*
predicate pushdown. `IsNotEmpty`/`IsEmpty` are marked non-pushable
(`connectors/capabilities/predicate_pushable.py:41–42`), so the filter can never
reach the scan's pass-1 (dict-aware, off-GIL, fused with decode). Note: even if
pushed, the scan still materializes survivors for the projection; pushdown alone
does not remove the 12.2M intermediate — it only moves the work off the GIL.

---

## 4. Options

Ordered by leverage. They compose.

### Option A (recommended) — streaming Top-N that consumes the filter
Fuse Filter + Sort + Limit into one operator that keeps only a bounded heap of
`k` (= LIMIT + OFFSET) rows keyed by the ORDER BY, applying the predicate as
rows stream in. Never materializes more than `k` survivors.

- For each morsel: evaluate the (dict-aware) predicate → mask; for the set bits,
  compare the ORDER BY key against the current heap-worst and insert if better.
  Only the ~10 retained rows are ever compacted/copied.
- This is the DuckDB algorithm for this shape and collapses the 330ms filter +
  46ms sort into a single O(n) key-compare pass with O(k) retention.
- Build on the existing Heap Sort operator (it already has the bounded heap);
  give it an optional pushed predicate + the input columns, and drop the
  separate Filter when the pattern is `Filter → (Project) → HeapSort(LIMIT)`.
- Risk: medium. New fused operator / planner rule. Must preserve ORDER BY tie
  semantics (EventTime, then SearchPhrase) and OFFSET. Needs a golden-plan test
  and a DuckDB oracle for ties.

### Option B — make unary string predicates c-native (off-GIL fused filter)
Add `BC_UNARY_OP` (at least IS [NOT] EMPTY / IS [NOT] NULL) to the c-native
nogil filter path so `_filter_morsel_c_native` fuses eval+mask off the GIL.
- Helps every `col <> ''` / `col IS NOT NULL` filter, not just Q27.
- Does **not** remove the 12.2M-row materialization — it only makes it off-GIL
  and lets it overlap. Partial win on its own; strong *with* parallel execution.
- Risk: low–medium. Needs a nogil DV* IS-EMPTY kernel (the current kernel is a
  nanobind GIL function; the nogil inner VM needs a `draken_*` C-ABI entry).

### Option C — parallel execution (M4 central scheduler)
The filter/sort are single-threaded; DuckDB uses all cores. The M4 scheduler
(deferred) would parallelize the filter pass. Architectural; out of scope here
but is the general lever behind Q02/Q08 too.

### Non-option — don't just un-block pushdown
Making `IsNotEmpty` pushable (or not rewriting `<>''`) moves the filter into the
scan but still materializes 12.2M survivors for the projection (§3.4). It helps
only by moving work off the GIL — strictly dominated by Option A.

---

## 5. Recommendation

**Option A** (streaming Top-N consuming the predicate) is the only one that
reaches DuckDB's complexity class for this shape, and it generalizes to every
`WHERE … ORDER BY … LIMIT k` query. **Option B** is a cheaper, independently
useful win (off-GIL unary-string filters) that should land regardless. Suggest:
B first (small, broad), then A (the real Q27 fix).

Estimated Q27 after A: ~50–70 ms (decode-bound + one streaming pass) → ~2× DuckDB.

---

## 6. Test plan

- `make q` (190), tpch (22), clickbench (43) green.
- DuckDB oracle for Q27 incl. tie-breaking on (EventTime, SearchPhrase) and the
  OFFSET variants (Q25/Q26 are the same shape with different keys).
- Golden plan snapshot: `Filter → Project → HeapSort(LIMIT)` collapses to the
  fused operator; non-LIMIT sorts unchanged.
- Bounded-heap correctness: k > input rows, k = 0, all-filtered, no-survivor RG.
