# Filtering Heap Sort — Fused Filter + Top-N — Design

> **ABANDONED / REVERTED (2026-06-22).** Built end-to-end (operator + optimizer
> rule), correct (`fused == unfused`, make q 190/190), but **measured
> clock-neutral** — it does not speed up Q27. Q27 is bound by the single-threaded
> 92M predicate eval + sort-key build, not the materialization this fusion
> removes (see §8 for the measurements and why the prediction was wrong). The
> implementation was reverted at the architect's call; Q27 needs M4 parallelism,
> the same root cause as Q02/Q08. This doc is kept as the record so it is not
> re-attempted without parallel execution.

**Status:** Design for architect review
**Author:** Q27 follow-up, 2026-06-22
**Supersedes:** Option A of `docs/Q27_FILTER_TOPN_DESIGN.md`
**Trigger:** Q27 builds a 12.2M-row intermediate (`Filter` → `Projection` →
`HeapSort`) only to keep 10 rows. We want to fold the predicate into the heap
sort so the intermediate never exists.

```sql
-- canonical shape (Q25/Q26/Q27 are all this)
SELECT <proj> FROM t WHERE <pred> ORDER BY <keys> LIMIT k;   -- no OFFSET (see §7.4)
```

---

## 1. What we're building

A `HeapSort` operator that owns the whole `Filter → Sort → Limit → Project`
shape: it carries a **pushed predicate** (applied as a survival mask inside the
bounded top-k scan, so a failing row is never compared, inserted, or
materialized) **and the output projection** (so it emits exactly the SELECT
columns). The separate `Filter` *and* `Projection` operators disappear for this
shape.

This is still small surgery on an existing operator, not a new one:
`HeapSortNode` already does the right thing structurally (per-morsel top-k →
buffer → final merge) and already evaluates ORDER BY *expressions*
(`_compiled_evals`). We add two inputs — the predicate and the projection — and
one decision (skip masked rows in the scan).

### 1.1 Why own the projection (the sort-key/output split)

A sort can order by columns that are **not** in the SELECT list. Today the
planner handles this by appending the ORDER BY columns to the projection as a
separate `order_by_columns` list (see `binder/project.py`), carrying them through
the sort as "extra" columns, and stripping them with a Projection *above* the
sort. That transient-sort-key bookkeeping is exactly the complexity an owned
projection removes: the operator distinguishes

- **sort-key inputs** — read + evaluated for the heap comparison, never emitted,
- **output projection** — evaluated only for the surviving ≤k rows and emitted,

so "sort by `EventTime`, output `SearchPhrase`" is one operator's internal
concern. No carry-through column, no post-sort strip projection. This mirrors
DuckDB's Top-N-then-project and is the natural unit for this shape.

---

## 2. Why this is the right base

`HeapSortNode._push_impl` (`heap_sort.pyx:731`) already:

- reduces **each incoming morsel** to its own top-k via `_top_n(morsel)` and
  buffers only that (`self._chunk_buffer`),
- on EOS, `Morsel.combine`s the buffer and runs one final `_top_n`.

So it never holds more than `k × (#morsels)` rows mid-flight, and the final
result is exact. We are *not* introducing a global cross-morsel heap with
dangling row references — we reuse the per-morsel-reduce-then-merge structure,
which already solves the cross-morsel-materialization problem.

The only thing missing is that today the rows are filtered *upstream* (the
`Filter` operator materializes all 12.2M survivors and streams them in). We move
that filter *into* the per-morsel top-k so survivors are never materialized en
masse.

---

## 3. Operator changes (`HeapSortNode`)

### 3.1 New state
```
cdef public object predicate              # CompiledBytecode or None
cdef public list   predicate_columns      # filter column identities (availability checks)
cdef public list   projection             # output expressions/columns, or None = pass through input
cdef public list   _compiled_projection   # compiled output exprs (eval on the k winners)
```
Predicate compiled once at `execute()` (mirror `FilterNode._compiled_filter`, via
`_build_bytecode(_lower_expr(...))`). Projection compiled like the existing
`ProjectionNode` (`compile_eval_nodes` for computed columns; plain identity
columns are a `select`). ORDER BY expressions keep using `_compiled_evals`.

The operator now tracks two column sets against its input: **sort keys** (needed
for the heap compare; may be input columns or order-by expressions) and the
**output projection** (the emitted columns; may be input columns or projection
expressions). Their union is what must be available at the operator's input.

### 3.2 Per-morsel flow (`_push_impl`)
```
if morsel is EOS: <combine buffer, final _top_n, emit> (unchanged)
if predicate is not None:
    mask = execute_bytecode(self.predicate, morsel)   # BoolVector, 3VL: NULL→false
    chunk = self._top_n(morsel, mask)                 # mask-aware (see 3.3)
else:
    chunk = self._top_n(morsel)                       # unchanged
buffer chunk if non-empty
```
`mask` is the same dict-aware, GIL-free predicate result the `Filter` produces
(IS [NOT] EMPTY landed 2026-06-21). Evaluating it here is identical work to the
old `Filter` eval — what we remove is the `filter_mask` take of survivors and the
chain hand-off.

### 3.3 Mask-aware top-k (the crux)
`_top_n` gains an optional `mask` (default None). The contract: **a row whose
mask bit is unset (false or NULL) is invisible to the scan** — never compared,
never inserted, never materialized. Because the heap only ever retains ≤k rows,
`_materialize_rows(morsel, top_indices)` (`heap_sort.pyx:756`, a `morsel.take`)
copies only the k winners — the 12.2M-row take is gone.

`_top_n` fans out to several specialized scans
(`_top_n_single_key_*`, `_top_n_multi_key_*`, `_compressed_top_k`,
`_heap_top_k_multi_vector`, kNN/vector). Threading the mask means: in each scan's
row loop, `if mask is not None and not mask_true(i): continue` before the
heap-compare. To bound risk we land it in tiers (§6).

`mask_true(i)` reads the BoolVector's bit + validity (a row that is NULL under
3VL is *not* selected — same semantics as `Filter`). For the numeric/compressed
paths this stays a tight nogil loop (the mask bitmap + key vector are both native
buffers).

### 3.4 Dict-shape preservation
The winners are materialized with `morsel.take`, which routes to the
compression-aware `str_take`/typed-take kernels (`string_gather.h:470`). For k=10
this is trivial and the §11 shape contract is unchanged.

### 3.5 Projection / emit
The buffer keeps, per surviving row, the **sort-key columns** (needed for the
final cross-morsel merge compare) *and* the **output columns** (needed to emit).
Sort keys are dropped only at the very end. So:

- per morsel: masked top-k → take the ≤k winners carrying (sort keys + the input
  columns the output projection needs) → buffer.
- EOS: `combine` buffer → final masked-free top-k (rows are already survivors) →
  **apply the output projection** to the ≤k final rows (`select` for identity
  columns, `execute_and_append` for computed ones) → emit exactly the SELECT
  columns. Sort-key-only columns (e.g. `EventTime`) are never in the output.

Because the projection runs on ≤k rows, expression cost is irrelevant; the point
is that the *operator* produces the final schema, so no separate Projection node
and no transient sort-key column survive past it (§1.1).

---

## 4. Planner rule (`fuse_filter_into_heapsort`)

A new optimizer strategy (or an extension of `operator_fusion.py`). After the
`Order+Limit → HeapSort` fusion has run (ordering: this rule `requires` the
`heapsort-fused` token), look for:

```
Filter(pred) → Project(out) → HeapSort(limit, order_by)
```

and rewrite to:

```
HeapSort(limit, order_by, predicate=pred, projection=out)    # Filter + Project removed
```

Both the `Filter` and the `Project` collapse into the HeapSort. The `Project`
folded in is the SELECT projection sitting between the filter and the sort; the
HeapSort emits its columns directly (§3.5). Either side may be absent: no Filter
→ plain projecting Top-N; no Project (SELECT * past the sort keys) → predicate-only
fusion as before.

### 4.1 Fusion conditions (all required)
1. The chain is linear (single in/out edges): `Filter` (optional) →
   `Project` (optional) → `HeapSort`.
2. The `HeapSort` has a real `LIMIT` (it always does — it only exists post
   `Order+Limit` fusion, and only for the no-OFFSET case, §7.4).
3. **Availability:** every column the *predicate* references, every *order-by*
   key, and every input column the *projection* needs must be available at the
   HeapSort's input (produced below the `Filter`). The predicate must not
   reference a projection-*computed* column (it runs below the projection). Reuse
   the predicate-pushdown availability logic (`_emitted_identities`).
4. The predicate is a normal row filter (not correlated/subquery); the
   projection contains no aggregates or window functions (it shouldn't here — it
   sits directly under a sort).

### 4.2 Projection handling
The `Project` is **folded into** the HeapSort as its `projection` (§3.5), not
re-parented. The HeapSort reads the union of {predicate cols, order-by keys,
projection input cols} from its input, runs the heap on the order-by keys, and
emits the projection on the ≤k winners. The transient sort-key columns
(`EventTime` for Q27) live only inside the operator — they are neither emitted nor
carried by any downstream node. This is what retires the `order_by_columns`
carry-through + post-sort strip for this shape (§1.1, and §6a for how far to take
it).

### 4.3 If conditions fail
Leave the `Filter`/`Project` where they are. The fusion is a pure optimization;
the unfused plan stays correct (and now has a GIL-free predicate eval anyway).

---

## 5. Correctness

- **3VL / NULL:** `mask_true(i)` selects only rows that are valid AND true —
  byte-identical to `morsel.filter_mask(mask)` semantics. A predicate that is
  NULL for a row excludes it, exactly as the standalone `Filter` does.
- **Tie semantics:** unchanged — the same `_compare_rows_vectors` /
  per-key heap comparisons decide order; the mask only gates *entry*, never
  changes the comparison. ORDER BY `(EventTime, SearchPhrase)` ties resolve as
  today.
- **Multi-key / ASC-DESC / mixed direction:** unchanged — handled by the existing
  `mapped_order` / `_uniform_direction` paths; the mask is orthogonal.
- **Exactness across morsels:** unchanged — per-morsel top-k then final merge is
  the same algorithm, just over masked rows.
- **Result vs unfused:** `fused == Filter → HeapSort` must be bit-identical
  (oracle: run both, and DuckDB). This is the gate.

---

## 6. Implementation tiers (risk control)

**Tier 1 — land the structure (safe, correct, partial win).**
`_top_n(morsel, mask)`: when `mask` is provided and the chosen scan path is not
yet mask-aware, do `morsel = morsel.filter_mask(mask)` then run the existing
`_top_n` unchanged. Removes the separate `Filter` operator + the 12.2M chain
hand-off; **keeps** the per-morsel survivor take. Drop-in, exercises the planner
rule and all correctness tests.

**Tier 2 — mask-aware hot paths (the real win).**
Thread the mask into the row loops of the paths Q25/26/27 hit:
`_top_n_single_key_numeric`, `_compressed_top_k`, `_top_n_multi_key_*`,
`_heap_top_k_multi_vector`. Masked rows are skipped before the heap-compare, so
only ≤k rows are ever taken — the survivor take disappears.

**Tier 3 (optional)** — mask-aware kNN/vector path, if a `WHERE … ORDER BY
distance LIMIT k` workload justifies it; otherwise Tier-1 fallback covers it.

Each tier is independently shippable and independently testable against the
unfused oracle.

---

## 6a. Scope decision — how far to take projection ownership

Owning the projection in HeapSort removes the transient-sort-key dance **for the
Top-N shape**. But the `order_by_columns` carry-through machinery
(`binder/project.py`: append ORDER BY cols to the projection, strip them above
the sort) exists because the **general `Sort` operator** (full sort, no LIMIT)
also orders by non-projected columns. So:

- **Scoped (recommended first):** only `HeapSort` owns its projection. The
  carry-through is *bypassed* for `WHERE…ORDER BY…LIMIT k`, but the machinery
  stays for full sorts. Smaller blast radius; the binder still emits the
  carry-through, the fusion rule just consumes the `Project` so it never runs as
  a node. Net: the complexity is *avoided* on the hot path, not deleted.

- **General (bigger, cleaner):** make the full `Sort` operator own its projection
  too, then **delete** the `order_by_columns` append/strip machinery entirely —
  both Sort and HeapSort take `(projection, order_by)` and emit the projected
  schema. This is the version that actually retires the complexity you flagged,
  but it touches the binder/logical planner's projection+order handling and every
  sort plan, so it needs its own correctness sweep.

Recommendation: build the operator to *accept* an owned projection from day one
(so the data path is identical for both), land the **scoped** planner rule first,
and treat "delete `order_by_columns` by giving `Sort` an owned projection too" as
a fast follow once the Top-N path is proven. That way the operator design doesn't
change between the two — only how many planner sites feed it a projection.

---

## 7. Edge cases & notes

1. **Predicate eval GIL:** still the bytecode VM (`execute_bytecode`). The IS
   [NOT] EMPTY kernel is GIL-free (landed); other predicates vary. Fusion does
   not change predicate GIL behaviour — it removes a *materialization*, which is
   the Q27 cost.
2. **Empty result / all-masked morsel:** `_top_n` returns 0 rows → not buffered
   (existing guard). EOS with empty buffer emits EOS only (existing).
3. **`k` larger than survivors:** correct — the heap simply holds < k.
4. **OFFSET:** `HeapSort` is only created when `not next_node.offset`
   (`operator_fusion.py:73`). OFFSET queries keep `Order+Limit+Offset` and are
   **out of scope** — they don't get a HeapSort to fuse into. (A future
   `Order+Limit+Offset → HeapSort(k=limit+offset)` fusion would extend coverage
   to Q25/Q39/Q41/Q42; separate change.)
5. **Multiple filters:** `split_conjunctive_predicates` may leave several Filter
   nodes; fuse the one directly adjacent (through pass-through projections) and
   leave the rest. They can be conjoined in a later pass.
6. **Vector/kNN top-k (`vector_topk_candidate`):** Tier-1 fallback applies until
   Tier 3; correctness holds.

---

## 8. Expected result — and the measured reality (2026-06-22)

**Predicted:** ~60–90 ms for Q27 (decode + one masked pass) vs 416 ms.

**Measured:** the fusion is **clock-neutral** (390 ms fused vs 391 ms unfused),
and both Tier-2 mask-aware variants were *slower*:

| variant | Q27 min |
|---|---|
| unfused (separate Filter + HeapSort) | 391 ms |
| Tier 1 (fold; filter survivors, then keyed top-k) | 390 ms |
| Tier 2 keyed-with-mask (skip survivors in the native scan) | 437 ms |
| Tier 2 general-comparator-with-mask | 1826 ms |

**Why the prediction was wrong.** The estimate assumed the 290 ms `Filter`
self-time was dominated by *materializing* the 12.2M survivors, so skipping that
take would be a big win. Measurement says otherwise:

- The native keyed top-k path **pre-builds sort-key arrays sized to the full
  (pre-filter) row count** (`_build_string_prefix_keys` over all `n`, an
  `int64[n]`). A mask-aware scan over 92M therefore builds ~736MB of keys —
  *more* expensive than taking the 12.2M survivors first and building keys over
  those. So "skip the take" is a net loss for this path.
- The general (`_compare_rows_vectors`) comparator is per-row Python-method
  dispatch — 4.7× slower; never viable for a 92M scan.
- What's left dominating Q27 is the **single-threaded 92M predicate evaluation +
  key build**, not the materialization. Removing the intermediate doesn't touch
  it.

**So Q27 is single-thread-bound, the same root cause as Q02/Q08** — it needs
parallel execution (M4), not this fusion. The fusion's value is now purely
**structural**: it removes the standalone `Filter` + `Projection` operators and
is the foundation for retiring the `order_by_columns` carry-through (§6a). It
does **not** move the clock and does not get Q27 under 10×.

Tier 2 (mask-aware scan) is **abandoned** — measured slower. Only Tier 1 (fold +
filter-then-rank) is retained.

- Still generalizes structurally to every `WHERE … ORDER BY … LIMIT k`
  (no OFFSET) query; the clock benefit there is also expected to be ~neutral
  until execution parallelizes.

---

## 9. Test plan

- `make q` (190), tpch (22), clickbench (43) green — no regressions.
- **Oracle equality:** for Q25/Q26/Q27 and synthetic cases, assert
  `fused_result == unfused_result` (force-disable fusion via a flag) **and**
  `== DuckDB`, including:
  - tie-heavy ORDER BY (duplicate keys), multi-key mixed ASC/DESC,
  - predicate selectivity extremes (all pass, all fail, ~0 survivors),
  - NULL keys and NULL predicate rows (3VL),
  - `k` > row count, `k` = 0, single-morsel and many-morsel inputs.
- **Golden plan snapshot:** `Filter→Project→HeapSort` collapses to a single
  `HeapSort(predicate=…, projection=…)`; non-LIMIT sorts and OFFSET queries
  unchanged; a predicate referencing a projection-computed column does **not**
  fuse.
- **Projection ownership cases:** ORDER BY a non-projected column
  (`SELECT a WHERE … ORDER BY b LIMIT k` — b must not leak into output); ORDER BY
  an expression (`ORDER BY LENGTH(x)`); SELECT an expression
  (`SELECT a+b … ORDER BY c`); ORDER BY a SELECT alias
  (`SELECT a+b AS k … ORDER BY k` — expression evaluated once, used for both sort
  and output). Each vs the unfused oracle and DuckDB.
- **Dict-shape:** filtered+sorted string output materializes correctly (the k
  winners), spot-checked vs pyarrow.

---

## 10. Risks & rollback

- **Blast radius:** `_top_n` is the hot top-k core; Tier 1 keeps it untouched
  (pre-filter) and only Tier 2 edits the scan loops. Land Tier 1 first.
- **Planner mis-fusion:** the availability check (§4.1.3) is the sharp edge —
  reuse the proven `_emitted_identities` logic, and the golden-plan test guards
  it. A predicate left referencing a dropped/renamed column must *not* fuse.
- **Rollback:** a single flag (`config.features.fuse_filter_into_heapsort`)
  gates the planner rule; off → today's plan, byte-identical.
