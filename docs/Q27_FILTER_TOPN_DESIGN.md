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
- **The IS [NOT] EMPTY kernel now releases the GIL during compute** (landed
  2026-06-21, mirroring the WP-6 string-case kernels — this kernel had been
  missed in that rollout). The compute runs GIL-free wherever the predicate is
  evaluated: the standalone Filter *and* the scan's pass-1 / single-pass apply
  (both call the same kernel via the bytecode VM). No clock change under the
  default free-threaded run; it matters for GIL-enabled runs and future parallel
  execution.
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

### 3.2 The predicate cannot use the *fused* C-native nogil filter path
`filter.pyx:249` has a fused eval+mask nogil fast path
(`_filter_morsel_c_native`), but it only fires when `bc.is_all_c_native`.
`IsNotEmpty` compiles to `BC_UNARY_OP`, which is **excluded** from the c-native
opcode set (`compiled_expression.pyx:1211–1232` — only loads, bool combinators,
ordinal `BC_COMPARE`, and fixed-width `BC_BINARY_OP`/`BC_CAST` qualify). So the
filter takes the GIL-VM (`execute_bytecode`) + `filter_mask` path rather than the
single nogil eval+mask span.

**Note:** the *compute itself* is no longer GIL-held — the kernel now releases
the GIL (§2). What `BC_UNARY_OP` still misses is the *fused* path that evaluates
the predicate and builds the mask in one nogil span (avoiding a Python
`BoolVector` intermediate). Putting `BC_UNARY_OP` on that path is a real but
separate optimization (Phase-9-magnitude): the C-ABI kernel it would dispatch
(`vector_string_is_not_empty_impl`, `function_string.cpp:41`) is currently
**declared but never defined** (a non-functional stub), `BC_UNARY_OP` sets no
`kernel_fn` in the bytecode builder (`compiled_expression.pyx:754`, unlike
`BC_BINARY_OP`/`BC_CAST`), and the nogil inner VM has no `BC_UNARY_OP` dispatch
(`evaluation.pyx:1686` → rc 99 fallback). So this is not a quick wire-up.

### 3.3 Filter + Sort + Limit are not fused into a streaming Top-N
`Heap Sort` already implements a bounded heap (LIMIT 10), but it sits *above* a
filter that has already materialized 12.2M rows. There is a `topn_scan_pushdown`
strategy and a `fuse operators heap sort` optimization (both fire here per the
plan's OPTIMIZATIONS list), but neither eliminates the 12.2M-row intermediate
because the **filter** is between the scan and the sort.

### 3.4 The filter is *pushable* but gets orphaned by the lone-predicate heuristic
Correction to an earlier draft: `IsNotEmpty`/`IsEmpty` **are** marked pushable —
the base capability map (`predicate_pushable.py:41–42`) has them `False`, but the
concrete connectors override to `True` (`filesystem_connector.py:69–70`,
`opteryx_connector.py:71–72`). The reason Q27's filter stays a standalone
operator is the pushdown strategy's classification: all `UNARY_OPERATOR`
predicates are treated as **metadata-only** and only pushed **when a selective
comparison predicate is also pushing** (`predicate_pushdown.py:791–799`). Q27 has
*only* `IsNotEmpty`, so it is *orphaned* into a standalone Filter
(`:798–802`). The comment's rationale: a lone `col <> ''` "is a net loss when the
predicate doesn't narrow the mask enough to pay back [two-pass] overhead."

Two further facts bound any pushdown fix:
- **Single-pass already applies pushed predicates** (`parquet_read.pyx:1283–1286`,
  `_apply_predicates_to_morsel`), so pushing a lone predicate would *not*
  double-decode — it would filter at the scan in single-pass.
- But the scan goes **two-pass** whenever there are projected columns beyond the
  filter columns (`two_pass_eligible`, `:1069`). For Q27 (`<> ''` ~13% selective)
  two-pass is a *win* (EventTime late-materialized for survivors only); for a
  *non-selective* lone unary with extra projections it is the net loss the
  heuristic guards against. The scan does not currently distinguish
  metadata-vs-selective predicates when choosing the pass count.

So "just relax the orphan heuristic" risks the documented two-pass regression for
non-selective cases. A correct pushdown fix must either force single-pass for a
metadata-only predicate set (safe, but forgoes Q27's late-mat win) or drive the
pass choice from selectivity estimates (previously rejected as gating). With the
kernel now GIL-free (§2), the upside of pushing for Q27 is small anyway — it
would mainly remove one chain operator — so this is best decided alongside the
Top-N work (§4 Option A supersedes the need to push the filter for this shape).

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

### Option B (done) — release the GIL in the IS [NOT] EMPTY kernel
Landed 2026-06-21 (§2). The kernel was the one string predicate missed by the
WP-6 GIL-release rollout; it now drops the GIL during compute everywhere it runs.
A *further* step — putting `BC_UNARY_OP` on the *fused* nogil eval+mask path
(`_filter_morsel_c_native`) — remains, but is Phase-9-magnitude (§3.2: the C-ABI
kernel is an unimplemented stub + builder/VM/gate wiring) and saves only the
Python `BoolVector` intermediate, not the materialization. Deferred.

### Option C — parallel execution (M4 central scheduler)
The filter/sort are single-threaded; DuckDB uses all cores. The M4 scheduler
(deferred) would parallelize the filter pass. Architectural; out of scope here
but is the general lever behind Q02/Q08 too. (This is why Option B's GIL release
is worth having now even with no clock change — it's a prerequisite for the
parallel dividend.)

### Non-option — don't just un-block pushdown
Relaxing the orphan heuristic to push lone `IsNotEmpty` is not a clean win (§3.4):
it risks the two-pass net-loss for non-selective cases, and with the kernel now
GIL-free its remaining upside is just removing one chain operator. Strictly
dominated by Option A.

---

## 5. Recommendation

**Option A** (streaming Top-N consuming the predicate) is the only one that
reaches DuckDB's complexity class for this shape, and it generalizes to every
`WHERE … ORDER BY … LIMIT k` query. The GIL concern (Option B) is already
addressed for the eval; the remaining Q27 clock cost is the 12.2M-row
materialization, which only Option A removes.

Estimated Q27 after A: ~50–70 ms (decode-bound + one streaming pass) → ~2× DuckDB.

---

## 6. Test plan

- `make q` (190), tpch (22), clickbench (43) green.
- DuckDB oracle for Q27 incl. tie-breaking on (EventTime, SearchPhrase) and the
  OFFSET variants (Q25/Q26 are the same shape with different keys).
- Golden plan snapshot: `Filter → Project → HeapSort(LIMIT)` collapses to the
  fused operator; non-LIMIT sorts unchanged.
- Bounded-heap correctness: k > input rows, k = 0, all-filtered, no-survivor RG.
