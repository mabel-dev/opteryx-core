# GROUP BY — targeted review and redesign (2026-10-12)

Status: **review + proposal, nothing built.** For the architect to rule on (§8).

Scope: the native hashed GROUP BY sink (`src/cpp/engine/native_group_sinks.hpp`,
`GroupBySink` 3384–5901) and the planner inputs it receives. Ungrouped aggregates,
DISTINCT, windows and ROLLUP's expand operator are out of scope except where the
sink's design forces them.

Evidence base: this session's per-query telemetry and `sample` profiles on the Mac
(M5 Pro) against `scratch.hits_rugo_262k`; the September–October measurements in
`docs/GROUPBY_SCALING_LIMITS_ASSESSMENT.md`, the merge/radix PoC record and the
direct-array PoC; two read-only code surveys (sink and planner) made today; the
source of DuckDB (`radix_partitioned_hashtable.cpp`, `aggregate_hashtable.cpp`),
ClickHouse (`Aggregator.{h,cpp}`), Velox (`HashTable.h`, `GroupingSet.cpp`) and
Pivot (`dispatch/src/operations/unary/group/*`), read today; and the Umbra-group
papers in `scratch/papers/`.

---

## 0. Verdict in five lines

1. The **topology is right** and matches every leader: thread-local
   pre-aggregation → hash-partitioned flush → per-partition parallel merge, with a
   direct-array path and dictionary-code path for the easy shapes.
2. The **per-group and per-row costs are not.** Two mechanisms dominate today's
   measured time: (a) keys are copied **row-at-a-time, with a type switch per
   group**, into per-partition stores that are re-created every flush, and
   (b) aggregate lanes and hashes live in `std::vector`s that are **resized and
   zero-filled per morsel per partition** and re-allocated every flush.
3. The finalize merge is a second-order cost everywhere except Q32 (100 M groups),
   and its radix form already works; it was blocked from paying by (a).
4. We throw away the one input the leaders use to choose a strategy: the sink gets
   **no usable group-count estimate** (planner product truncates at 64) and keeps
   no runtime estimate of its own.
5. Catching the leaders is a **storage-layout and batching rewrite inside the
   existing topology**, plus an adaptivity layer. Not a new algorithm. A shared
   "ticket" table (Xue & Marcus 2026) is the only genuinely different architecture
   on the table and is proposed as a later, measured option, not the plan.

---

## 1. Where the time goes today (measured 2026-10-12)

One process per query, hot (third run), DOP 16, `OPTERYX_GB_FINALIZE_PROF=1`.
`probe/apply/merge` are CPU-seconds summed over threads; `wall` is the query.

| Q | shape | groups | wall ms | GB CPU s | hash | probe | apply | merge | what dominates the sample |
|---|---|---|---|---|---|---|---|---|---|
| 08 | RegionID, COUNT(DISTINCT UserID) | 18 | 210 | 2.0 | 0.02 | 0.12 | **1.15** | 0.75 | COUNT(DISTINCT) pair table |
| 15 | UserID COUNT(*) | 17.6 M | 139 | 1.9 | 0.03 | **1.24** | 0.14 | 0.46 | local table insert |
| 16 | UserID, SearchPhrase | 17.6 M | 285 | 4.1 | 0.26 | **3.15** | 0.14 | 0.56 | local insert + string key copy |
| 18 | UserID, minute, SearchPhrase | 26 M | 507 | 6.8 | 0.35 | **4.65** | 0.11 | 1.85 | same, raw mode engaged |
| 32 | WatchID, ClientIP (unique) | 100 M | 689 | 10.1 | 0.34 | **3.43** | 1.20 | **5.12** | `vector` resize/zero-fill 40%, `memmove` 20% |
| 33 | URL | 18 M | 472 | 6.2 | 0.06 | **5.29** | 0.12 | 0.68 | `memmove` of URL bytes into key arena 21% |
| 35 | ClientIP ×4 | 9.7 M | 145 | 1.8 | 0.03 | **1.18** | 0.17 | 0.45 | local insert |

Sample attribution (macOS `sample`, 8 s hot loops):

- **Q33**: top frame is `_platform_memmove` (10.8 k of ~27 k samples). Callers:
  `GroupKeyColumn::put_long` (`native_group_sinks.hpp:228`, the URL bytes copied
  into the partition's arena on *every* new group) 5.8 k; `xzm_realloc` (arena /
  `AppendBuffer` growth copies) 1.5 k; parquet decode 2.0 k. The carchar probe
  itself (`carchar_simd.hpp:3xx`, `carchar_index.hpp`) is ~4 k. **The hash table is
  not the cost; the key copy is.**
- **Q32**: `_platform_memmove` 11.5 k + `_platform_memset` 5.2 k of ~40 k. Callers
  are `std::vector::resize`/reallocation (`vector.h:1361/1364/1365/1111`) from
  `grow_lanes` (:4848, merge side), the per-morsel lane resize (:4471–4476, sink
  side), `gb_lanes_resize` (:2724/:2743) and `hashes.push_back` (:4438, :5201).
  **40 % of the query is allocating, moving and zero-filling vectors.**

Per-row arithmetic for Q32: 10.1 CPU-s / 100 M rows ≈ **100 ns per row** of GROUP BY
work for a 2-int-key COUNT/SUM/AVG. Pivot's published c6a number for the same
query implies roughly a third of that. A cache miss is ~80–100 ns; we are spending
one miss-equivalent per row on work that should be sequential.

What is **not** the problem (so it is not re-proposed):

- Hashing: ≤ 5 % everywhere (E37 carried hashes, draken `cxx_hash_c`).
- Partition skew / flush lock / thread spawn: measured ≤ 4 % (scaling assessment).
- Prefetch, call-overhead removal, slicing passes: all measured flat or negative
  on both arches (notes in the sink at :4405–4430 and memory).
- Direct-array and dict-code paths: already win where they apply (Q07/Q09/Q27).

---

## 2. Review of the current design

### 2.1 What is right and must be kept

| Item | Where | Why it is right |
|---|---|---|
| Thread-local pre-aggregation, 64 hash partitions, parallel per-partition merge | `gb_part` :1829, `flush_locals` :3977, `finalize` :5812 | Same shape as HyPer/Umbra (Leis '14 §4.4), DuckDB (`RadixPartitionedHashTable`), ClickHouse two-level, Pivot. |
| Batched hashing once per morsel; single-key shape-preserving hash | `compute_row_hashes_shaped` :4249 | Correct separation of "ticket" from "update" (Xue & Marcus §2.3). |
| Three-tier table ladder parvi → medius → carchar, merged 16-byte slot | `find_or_insert_group` :3246, carchar H16 | Cache-resident for the small case; one dependent miss on the big case. |
| Direct-array mode for bounded int keys | `da_arm` :3781 | Velox `kArray`, ClickHouse `key8/key16`. Already measured 0.54–0.84 on eligible shapes. |
| Dictionary code pass | :4273–4392 | ClickHouse `LowCardinality`, Velox value-ids. |
| Adaptive raw mode (skip local dedup when it does not reduce) | `maybe_flush` :4023, `kGBRawSwitchRatio` 0.9 | DuckDB `SkipLookups`/`DecideAdaptation`, Velox abandon-partial, Pivot `scatter_raw` at 90 % distinct — identical idea, identical threshold. |
| Radix sub-bucketing of large partitions at merge (`kGBMergeLeaf` 64 k) | `merge_and_emit_partition` :5224 | Pivot's "radix merge targets stay cache-resident"; measured −34 % Q33 merge. |
| Top-k fusion, statistics seeds, group-key reduction, group-limit key bound | compiler / sink | Plan-level wins the leaders also have (ClickHouse `bucket_top_k`, Pivot `GroupLimit::TopK`). |
| SoA lanes, type-templated apply (`gb_apply_row_kind`) | :2685, :2806 | Correct vectorised update step. |

### 2.2 What is wrong (ranked by measured cost)

**W1. Keys are materialised row-at-a-time, with a per-group type switch, on the
probe path.** `GroupKeyColumn::append_row` (:202–236) is called once per new group
per stored key *inside* the probe loop (:4456). It branches on bool / null / string
/ fixed, calls `memcpy` with a runtime `elem_size`, and for strings > 12 bytes copies
the payload into the partition's arena (`put_long`, :228). On Q33 that is 18 M × ~100
bytes of `memmove` from inside a loop whose other work is a hash probe. Every leader
appends new groups **as a batch after the probe pass**: DuckDB
`FindOrCreateGroupsInternal` collects the "empty list", then one
`AppendUnified` per chunk; Velox builds a new-rows vector then stores; Pivot's
`consume_batch` seeds cells per window and writes string bytes into a per-worker
2 MB slab once. In raw mode (:4431–4445) the situation is worse: there is no probe at
all, yet every row still pays the per-row `append_row`. This is the single
mechanism that made both the radix-merge PoC and the batched-raw PoC lose: both
moved entries more than once and each move was a per-row copy.

**W2. Lanes and hashes live in vectors that are resized per morsel per partition
per spec, and the memory they grow into is fresh every flush.** Sink side:
`gb_lanes_resize` is called 64 × nspecs times per morsel (:4471–4476); merge side
`grow_lanes` (:4844) does it again per source. On Q32 this is 40 % of samples
(`memset` + `memmove` under `vector.h`).

Two corrections to the naive reading of that profile, both already on record:

- The sink is **not** on plain `std::vector` for its hot data. `GroupKeyColumn::raw/
  arena` are `draken::AppendBuffer` (2026-10-06); `hashes`/`grows`/numeric lanes are
  `draken::tracked_vector` (a `std::vector` with the memory-accounting allocator,
  :2685–2701, :3209–3211); only the heap-object lanes (strings, sketches) and the
  partition/lane containers are `std::vector`. Swapping container types is **not**
  the fix: GB lanes on `AppendBuffer` with memset-of-new-tail-only was built and
  reverted on 2026-10-06 at geomean 0.996 (memory note `append_buffer_key_store`).
- That revert established the real mechanism: the `memset`/`memmove` is dominated by
  **first-touch page faulting of fresh memory** — every flush hands its partitions
  to `pending` (memory retained until finalize) and starts growing into new pages;
  the merge then allocates a second copy of everything (`merged`, `frag`) and
  touches those pages too. Q32 writes ~8 GB of group state in the sink and again in
  the merge. Zero-fill and growth copies are real but secondary.

The leaders avoid this structurally, not by container choice: Pivot's slabs come
from one **pool of 2 MB slots faulted in at boot** (`bin/src/memory.rs`,
`dispatch/src/memory/mod.rs`; see `docs/C7_PAGE_CACHE_DESIGN.md` §16) and are
written once at final entry layout; DuckDB appends rows to `TupleDataCollection`
blocks and merges by **moving blocks** (`AcquirePartitionedData`), never copying
the local table into the global one; ClickHouse keeps states in an `Arena` with
layout computed once. The fix for us is therefore **reuse** — group blocks
allocated from a per-process recycled slab pool (hot runs never fault; "hot run is
where the wins are"), written once, and moved by pointer through flush → merge →
emit — plus no second copy at merge.

**W3. Every flush constructs 64 fresh `GBPartition`s under the global mutex.** Each
is ~3.8 KB inline (parvi 1.1 KB + medius 2.3 KB value-initialised, carchar header
plus two heap allocations), and `push_back(std::move(part))` copies the inline arrays
again, and again on `pending` vector reallocation (scaling assessment §B measured
~250 MB of copying on Q33 under the lock). DuckDB's `Abandon()`/
`ResetForNewIteration()` **clears the pointer table in place** and keeps the
partitioned data; nothing is reconstructed.

**W4. The sink has no cardinality signal, at plan time or run time.**
`execution_estimates.group_count_estimate` stops multiplying key NDVs once the
product passes `NATIVE_GB_GATE = 64`, so the sink's `ndv_estimate` is a truncated
lower bound used for exactly one decision (arm parvi). The KMV sketches that could
give a real estimate are discarded. There is no runtime estimate either. DuckDB
runs an HLL per thread and at `ADAPTIVITY_THRESHOLD` (1 M rows) decides
`SkipLookups` (>95 % unique) or re-sizes/abandons; Pivot keeps an HLL (P=12) to
size the radix merge targets; ClickHouse accepts a `StatisticsHint`. Our raw-mode
switch looks only at the *last flush window* (65 k rows), which is why Q17 (24 %
distinct globally) tripped at 0.3 and the threshold had to go to 0.9 — a window
cannot see global dedup. (`feedback_hashtable_sizing` stands: an estimate must
choose the **mode**, never pre-size a cache-resident table.)

**W5. The merge does not own its memory either.** `merge_probe` → `map_probe` →
`append_from` per group (:5121), `hashes.push_back` per group, `grow_lanes` per
source. The "keys by reference" variant fixed the copy and then paid 9.4 s of random
per-row `append_from` gathers at emit (merge PoC v2) — because the gather was again
row-at-a-time against ~1,500 sources. The right fix is columnar gather per
(source, run), not abandoning references.

**W6. Finalize scheduling is coarse and index-ordered.** Work unit = partition
(indivisible); radix buckets of one partition run serially on one thread
(:5287–5298); no largest-first; `nt` capped at 16 then widened to DOP. Fine at
≤ 16 threads (measured), 1.4–3× loss arithmetic at 30–190 (assessment §A). DuckDB
assigns per-partition tasks through a state machine; Pivot publishes one
`PartitionJob` per partition to a work-stealing `Injector` and scales the partition
count with the pool (`get_scatter_bucket_count_for_worker`, target 2¹⁷ streams).

**W7. Multi-key hashing and comparison go through the generic path even when the
keys pack into 64 bits.** (WatchID, ClientIP) is 96 bits so it cannot, but
(SearchEngineID, ClientIP) = 48 bits, (TraficSourceID, SearchEngineID, AdvEngineID)
= 48 bits, (WindowClientWidth, WindowClientHeight) = 32 bits can. Velox
`kNormalizedKey` and ClickHouse `keys128/keys256` pack; Pivot has
`IntPairKeyExtractor`. We hash column-by-column and store each key column.

**W8. COUNT(DISTINCT) pairs are re-keyed at every merge level** (combine, scatter
`PairSel`, bucket merge) and pushed back per pair (:1958). Q08's 1.15 s apply is
this. Out of scope for the sink rewrite but it is the next item after it.

**W9. Hygiene found on the way** (not performance, but should go in the same
pass): `kGBArenaChunk` unused (:1904); `OPTERYX_GB_DICT` and
`OPTERYX_GB_FINALIZE_PROF` latched for process life (the FROZENcfg trap the file
warns about at :3526); stale comments at :2668–2675 and :3199–3206; `put_long`
returns a `uint32_t` arena offset with no overflow check (:197); `errs[tid].msg`
stores a pointer into a mutable string (:5881); `OPTERYX_GB_ROUTED` prototype
(COUNT(*) only) still in tree — finish or kill; `OPTERYX_GB_MERGE_THREADS` marked
temporary since 2026-08-29.

---

## 3. What the leaders do, and the one thing each teaches us

| Engine | Mechanism (from source read today) | Lesson for us |
|---|---|---|
| **DuckDB** `GroupedAggregateHashTable` + `RadixPartitionedHashTable` | Pointer table of `ht_entry_t` (salt + 48-bit pointer) over row-oriented `TupleDataCollection`; `FindOrCreateGroups` is **passes**: hash → salt-compare → key-compare → *batch append of new rows* → pointer gather. Local sink capacity 32 k (>2 threads), radix bits 4 → 8 raised when partitions outgrow a block (`MaybeRepartition`), `Abandon()` clears the pointer table and keeps rows. Per-thread HLL; at 1 M rows `DecideAdaptation`: `SkipLookups` if > 95 % unique. Finalize = per-partition `AggregatePartition` tasks. | Batch the append; abandon by clearing, not re-creating; a runtime HLL drives the mode. |
| **ClickHouse** `Aggregator` | Key-shape specialised tables chosen once (`key8/16/32/64`, `keys128/256` packed, `key_string`, `serialized`, nullable and low-cardinality variants); states in an `Arena` behind one `AggregateDataPtr` per key with offsets precomputed; single-level → two-level (256 buckets) at `group_by_two_level_threshold`, buckets merged by workers claiming `next_bucket_to_merge`; `aggregation_in_order` aggregates runs of equal keys without a table; `max_bytes_before_external_group_by` spills two-level buckets to disk. | Pick the key representation **once per query** from the key types; keep the state with the key; sort-order / run-aware mode exists for clustered input. |
| **Pivot** `group/` | Per-worker `AggregatedTable` = `MultiSlabTable` of fixed-stride entries `hash \| key \| value` in 2 MB slabs (no zero-fill, no moves); linear probing, 0.7 load, ×4 growth in place while ≤ `SWITCH_THRESHOLD` 32 768 slots, then **radix**: existing entries scattered by top hash bits into per-partition `StridedScatterRows` (64 KB chunks, first chunk ⅛), later rows scattered **without probing** once a fill is ≥ 90 % distinct (`scatter_raw`); strings in a shared slab arena (`ArenaKey` inline ≤ N bytes else view), never re-copied, output wraps the slabs zero-copy; HLL (P=12) sizes merge targets; partition count scales with workers to ~2¹⁷ streams; merge jobs work-stolen. Key extractors chosen at build: `IntKeyExtractor`, `IntPairKeyExtractor`, `IntStrKeyExtractor`, `StringKeyExtractor`, else row-encoded. | Entries are written **once** at the layout they will be merged and emitted in; strings are copied once into a slab that outlives the query's stages; radix scatter is the growth strategy for int keys past L2 size; partitions scale with the pool. |
| **Umbra / HyPer** (Leis '14 §4.4; Kohn '21) | Fixed-size thread-local pre-aggregation table; on full, **flush to partition overflow buffers** (not re-created); phase 2 builds one table per partition; grouping sets share one pass. | Same topology as ours; confirms the flush-to-buffer discipline. |
| **SQL Server** (Larson et al. 2013/2015, batch mode) | Batch-mode hash aggregate over columnstore: aggregate pushdown into the scan for `MIN/MAX/SUM/COUNT` on ≤ 8-byte types per rowgroup using segment metadata; "partial aggregate" below exchanges; memory-grant-driven spill of partitions. | Our statistics seeds are the analogue; nothing new to take. |
| **Velox** `HashTable` / `GroupingSet` | `kArray` (≤ 2 M entries) → `kNormalizedKey` (small-range keys packed into one 64-bit value) → `kHash` (16-slot buckets, 7-bit tag, 2 cache lines); `decideHashMode` re-decides as ranges grow; partial aggregation **abandoned** when reduction is poor and rows pass through as intermediate. | Normalised keys; mode decided from observed ranges, re-decided on growth. |
| **Xue & Marcus 2026** (ticket tables) | One shared concurrent `Folklore*` table (linear probing, CAS on a ticket word, fuzzy per-thread ticket ranges) maps key → dense ticket; aggregate update is a vector indexed by ticket, thread-local or atomic. Matches/beats partitioned aggregation at low thread counts; ~10× less memory at 10 M groups; needs presizing from an estimate (resize 1.6–5.2× cost) and has no spill. | The genuine alternative architecture. Best fit for Cloud Run (4 vCPU). Not the first step: it needs the estimate (W4) and a key-verification store for strings first. |

Common thread: every leader (i) decides the key representation once, (ii) writes a
new group **once** into fixed-stride, bump-allocated storage that survives every
later stage, (iii) batches the new-group append after the probe pass, and (iv) has a
cardinality estimate driving the mode. We do (i) partially and none of (ii)–(iv).

---

## 4. Tangential ideas (graphs, GPUs, sorting)

These are not the plan; they are the lens that shaped §5.

- **Gather-Apply-Scatter (Pregel/GraphLab) and CSR builds.** A hash-partitioned
  GROUP BY *is* a scatter-by-destination followed by a sequential apply. Graph
  engines never scatter one vertex at a time: they histogram, prefix-sum, then write
  each destination's run contiguously (exactly a radix partition pass). That is the
  shape W1 should take in raw mode: count per partition → offsets → columnar scatter.
  Our join radix build already does this (`native_join2.hpp` stage_h/stage_r); the
  GROUP BY raw path does not.
- **Ligra's sparse/dense frontier switch.** Ligra flips between an index list and a
  bitmap depending on frontier density, every step. The GROUP BY analogue is the
  mode ladder DA → dict codes → hash → raw scatter, re-decided as cardinality is
  observed — not fixed at the first morsel. Velox's `decideHashMode` is the same
  idea. Today our DA mode is chosen once from bounds; raw mode is a one-way switch
  from a 65 k-row window.
- **Beamer's direction-optimising BFS (push vs pull).** Push = each row scatters to
  its group (what we do); pull = each group gathers its rows. Pull is
  "aggregation in order": when a morsel's key column is sorted or run-length
  shaped (CounterID in ClickBench is sorted within files; TPC-H `l_returnflag`
  runs), run detection costs one compare per row and replaces hashing entirely.
  ClickHouse `aggregation_in_order` and elosdb's zone-map clustered aggregation are
  this. Cheap to add as a per-morsel check inside the DA/dict front; payoff is
  shape-dependent and must be measured, not assumed.
- **GPU segmented reduction** (sort-by-key then reduce runs). Our sort-leaf merge
  arm (C) lost to the hash leaf; no change. But GPUs' "reduce in registers before
  touching memory" is the warp-level form of local pre-aggregation on a *tiny*
  table — a 256–1024-entry L1-resident front map that absorbs heavy hitters before
  the partition tables. Parvi/medius are that; keep them, but they must stop being
  re-constructed per flush (W3).
- **CSR as the merged output.** The merge's job is to produce, per partition, one
  dense array of groups plus a dense array of lanes. If the scattered raw rows are
  partitioned by *top* hash bits (Pivot: partition = top bits; slot = next bits), a
  partition maps to a contiguous slot range of **any** power-of-two table, so a merge
  job can walk only its range of each source table — no probe, no re-hash to
  find "my" entries. Our `gb_part` uses bits 32–37 and carchar uses low bits for the
  slot; adopting the top-bits convention gives the same property for free.

---

## 5. Redesign

Principle: **write a group once, at its final layout, as a batch; decide the
representation from data, not from the first morsel.** Same topology, new
storage model, one new adaptivity layer. Everything below is inside
`native_group_sinks.hpp` and the compiler's estimate plumbing; no format, planner
or draken ABI change.

### R1. Group block store (replaces `GBPartition`'s vectors + `GroupKeyColumn`)

Per partition, groups live in **blocks** of a fixed row count (e.g. 4096), each
block a single bump allocation holding, column-major: `hash[]`, one typed key array
per stored key (fixed width: raw values; strings: 16-byte slots), the shared
`grows[]`, and one array per lane per spec. Long-string bytes go to a per-worker
slab arena (2 MB slabs, never moved, outlives flush and merge; Pivot's
`SharedArena`). A block is written sequentially, never resized, never zero-filled
beyond the rows it holds, and is **moved by pointer** at flush, scatter, merge and
emit. The local hash index (parvi/medius/carchar) maps hash → (block, row).

Blocks come from a **per-process slab pool** (fixed slab size, recycled free list,
charged to the draken memory account; released to the OS only under pressure or
after an idle period — same shape as the C7 budget, not Pivot's boot-time prefault,
which assumes it owns the machine). A hot run therefore allocates no new pages for
group state. The merge never copies a group: a merged partition is an index over
the source blocks (R5), so the ~2× page-touch of today's `merged`/`frag` copies goes
away.

What this removes: all of W2 (no fresh pages on hot runs, no second copy at merge,
no `resize`, no zero-fill, no reallocation copies),
the per-flush reconstruction cost of W3 (a flush hands off a list of block
pointers and *clears* the index in place), and the random `append_from` of W5 (a
merge that keeps a winner by reference holds `(block, row)`, and the emit gathers
per block, sequentially).

### R2. Batched new-group append (fixes W1)

Pass B becomes two passes, as in DuckDB: (B1) probe every row, emitting `gid` for
hits and collecting the row indices of **misses** per partition; (B2) for each
partition with misses, append them as a batch: one typed loop per key column
(compile-time width, no per-row type switch; strings = slot copy plus one arena
write per long string), one `hash` loop, lanes initialised by one typed loop per
spec. In **raw mode** B1 is skipped and B2 is a counting-sort scatter over the
morsel (histogram → offsets → columnar writes), i.e. the GAS/radix shape; no hash
table is touched. This is what the 2026-10-01 batched-raw PoC should have been and
was not: that PoC still called `append_rows` through a permutation one row at a
time and lost 12–20 % on canon. The new path must be measured on canon *and*
H2O groupby *and* rugo, and ablated separately from R1 (`feedback_build_all_then_ablate`).

### R3. Flush = clear, not reconstruct (fixes W3)

`flush_locals` publishes the partition's block list and the index's group count,
then **clears** parvi/medius/carchar in place (DuckDB `Abandon`). No
`GBPartition()` construction, no inline-array copies, no allocation under
`g.mtx`; the queue is a deque of slim `{block list, count, flags}` records. The
existing measurement says the lock itself is ≤ 4 %; what this buys is the ~1 KB
zero-fill × 64 × flushes and the 3.4 KB moves, which were inside the lock's hold
time and are part of W2's memset share.

### R4. Cardinality-driven mode ladder (fixes W4; Ligra/Velox/DuckDB)

Two inputs: (a) the planner passes the **real** NDV product (remove the
`NATIVE_GB_GATE` truncation; KMV union is already computed) and the scan row
count; (b) the sink keeps a per-worker `draken::KmvSketch` (the one vendored
estimator, `draken/core/kmv_sketch.h`; a rejected hash costs a single compare
against a register threshold) and unions them in finalize. Because a KMV has set
semantics, it need only be fed hashes of groups that are **new to the local
window** (the probe already knows), not every row; in raw mode every row is fed
and the single-compare reject bounds the cost. (Pivot and DuckDB use an HLL here;
KMV is the house estimator and is cheaper per update on the hit path.) Decisions, re-evaluated
at each flush rather than fixed at morsel 1:

| observed | mode |
|---|---|
| int keys, bounded range ≤ slot budget | direct array (exists) |
| single key arriving dict-shaped, codes ≪ rows | dict code pass (exists) |
| estimated groups ≤ ~L2-sized table per worker | local hash + blocks (R1/R2), never flushed |
| estimated groups ≫ L2 and local dedup ratio < X | **raw scatter** (R2) from now on; no local index |
| string MIN/MAX present | never raw (Pivot's rule: scatter would persist every row's string) |

The estimate chooses the mode and the **merge target size**; it never pre-sizes a
probe table (ruling stands). The 0.9 window ratio stays as the fallback when no
estimate exists.

### R5. Merge over blocks, columnar gather at emit (fixes W5)

`merge_and_emit_partition` keeps its structure (single table ≤ 64 k, else radix
sub-buckets) but: the merged table maps hash → `(block, row)` into the *source*
blocks (no `append_from`, no `hashes.push_back`); lane combine is a typed loop per
spec reading source lane arrays by block; emit walks the winners grouped by source
block and gathers each key column with one typed loop per block (the "batched,
column-at-a-time gather" the v2 PoC note named as the prerequisite). Long strings
are already in slabs that outlive the merge, so the output can reference them
(draken string slots + arena view) rather than copy — same as Pivot's zero-copy
`to_arrow_buffers`.

### R6. Work-stealing finalize units, top-bits partitioning (fixes W6)

Make the finalize unit `(partition, bucket)`, published to a claimable queue
largest-first; buckets of one partition no longer serialise on one thread. Move the
partition bits to the **top** of the hash (Pivot/ClickHouse convention) so that a
partition is a contiguous slot range in any power-of-two local table; carchar's tag
bits (57–63) then move down, which needs the same bit-budget static_assert audit
that the 2026-09-26 change did. Partition count becomes a function of DOP
(64 at ≤ 16, 256 at 32+, Pivot's 2¹⁷-stream target) now that partitions are slim
(R3) — the September "256 partitions +5–25 %" result was the cost of fat
partitions, which R3 removes; re-measure, do not assume.

### R7. Normalised multi-key integers (fixes W7)

At `capture()`, if the stored int keys' widths sum to ≤ 64 bits, pack them into one
`uint64` key column (Velox `kNormalizedKey`, ClickHouse `keys128` idea, Pivot
`IntPairKeyExtractor`): one hash, one 8-byte key store, exact compare, DA-eligible
via the existing mixed-radix layout. Unpack at emit. Covers Q30 (SearchEngineID,
ClientIP), Q39 int triple, Q41 pair, Q35's four ClientIP expressions (after
group-key reduction they are one). Q32 (96 bits) stays on the generic path.

### R8. Run-aware front (Beamer "pull"; ClickHouse in-order) — optional, measured

Inside the dict/DA front, a per-morsel run detector: if the key column is sorted
or has few runs (one compare per row), aggregate per run without a probe. Zero
cost when it does not fire (the check aborts after N transitions). Candidates:
TPC-H Q1, H2O sorted inputs, any `GROUP BY` on the clustering key. Not a
ClickBench win; listed because it is cheap once R1 exists.

### R9. Shared ticket table — later, as a measured mode

After R1–R5, build the Xue & Marcus mode as a *third* strategy selected by R4 when
`DOP ≤ 8` and the estimate is < ~16 M groups: one shared Folklore\* table
(CAS on a ticket word, fuzzy ticket ranges per thread), aggregate lanes indexed by
ticket (thread-local lanes merged by ticket at the end, or atomic for COUNT/SUM).
Removes the duplicate per-worker work (CPU doubles from DOP 2→16 today) and the
partition merge. Needs: the estimate to presize (resize is 1.6–5.2×), key
verification for strings (we accept hash identity, so this is only the storage of
the key by ticket), no spill (none exists today either). elosdb ships this and is
2.4× us on ClickBench. x86 A/B is mandatory: the paper shows SMT favours
partitioned.

### Not proposed

- Pre-sizing probe tables from estimates (ruled; cache-residency wins).
- Software prefetch anywhere (ruled twice, both arches). Note Pivot prefetches
  (`L1_DISTANCE` 16 / `L2_DISTANCE` 48) **and** gets its win from layout; our
  measurements say layout is where ours is too.
- More partitions on the *current* fat `GBPartition` (measured loss).
- Sort-based merge leaf (measured loss).
- JIT; row-oriented (AoS) lanes — SoA lanes are right for the vectorised update
  step and for `topk_select`'s lane swap.

---

## 6. Expected effect (estimates, to be falsified by measurement)

| Fix | Mechanism removed | Evidence it is the cost | Queries |
|---|---|---|---|
| R1+R3 | per-morsel resize/zero-fill, per-flush reconstruction, growth copies | Q32 40 % of samples in `vector` growth/memset; assessment §B 250 MB copies under lock | Q32, Q18, Q16, Q15, Q35, every high-NDV GB |
| R2 | per-row key copy with type switch; raw-mode per-row append | Q33 21 % in `put_long`, raw-mode PoCs lost on exactly this | Q33, Q34, Q13–Q18, Q28 |
| R5 | per-group `append_from`/`push_back` in merge; random emit gather | merge PoC v2: emit 0.26→9.4 s when by reference | Q32, Q18, Q19 |
| R4 | wrong/late mode (Q17 tripping raw at 24 % distinct; DA chosen only from bounds) | raw-switch history; DA PoC shape results | Q17/Q18, Q36-type |
| R6 | serial buckets, index-order partitions | arithmetic only at > 16 threads | c6a.metal / 192-vCPU runs, not the Mac |
| R7 | multi-column hash + multi-column key store | hash 0.3 s on Q18/Q32; two key columns stored | Q30, Q39, Q41 |

If R1–R3 remove most of the memset/memmove share, Q32 goes from ~100 ns/row toward
~40–50; that is the band Pivot's c6a number implies. I am not claiming parity from
this doc — the leave-one-out A/B decides.

---

## 7. Measurement plan (per CLAUDE.md §7 and the memory rulings)

1. **Baseline before the first edit** on both arches: the §1 table plus `sample`
   attribution, on hits_rugo_262k, H2O groupby medium, TPC-H SF10 (Q1/Q3/Q9/Q18),
   canon ClickBench. Record `get_groupby_telemetry` and `FINALIZE_PROF`.
2. Add the missing counters first (W-instrumentation: flush wait/hold, append
   bytes, blocks allocated, merge gather bytes, mode chosen per worker, HLL
   estimate vs truth). Cheap, and R4 cannot be tuned blind.
3. Build R1–R3 **together** (they share the block store), then ablate
   leave-one-out: R2-off (row-at-a-time append onto blocks), R3-off (reconstruct).
   Then R4, R5, R6, R7 each as its own arm. `.so`-swap A/B, ≥ 7 interleaved rounds,
   process per query, placebo arm for layout noise (±4 % floor).
4. Correctness: the existing checksum battery (UserID / (WatchID, ClientIP) / URL /
   (UserID, SearchPhrase) full-group checksums), `make q`, `make slt`, the forced
   stress build (flush 8, leaf 2, switch 0.0) that caught the ROLLUP split bug,
   plus a NULL-key / BOOL-key / dict-vs-dense oracle pass (the two historical
   silent-wrong-answer bugs were in key emit).
5. x86 (i5 box) before any ruling on R6/R7/R9; prod is x86 and the DA/dict results
   already flipped sign between arches once.

Rough effort (one person, this codebase): R1+R2+R3 is the big one — the group
store touches sink, flush, merge, emit, top-k, seeds, COUNT(DISTINCT) pairs and
the DA spill (`da_spill` rebuilds through `append_row`): 2–3 weeks to green with
ablation data. R4 ~3 days (plumbing + HLL + decision table). R5 ~1 week. R6 ~3 days
plus a wide-box run. R7 ~3 days. R8 ~2 days. R9 ~2 weeks and only after the rest.

---

## 8. Decisions — architect's answers (2026-10-12)

| # | Question | Ruling |
|---|---|---|
| 1 | Block layout / block size | performance question, not policy: measure per arch |
| 2 | Strings by reference into output | see note below; copy-at-emit first, reference if emit shows up |
| 3 | Partition bits to top of hash | whenever it makes most sense (with R6) |
| 4 | Change `execution_estimates.py` | yes |
| 5 | Runtime estimator in the sink | yes; use the house MinK/KMV (R4 updated), unless HLL measures faster |
| 6 | Raw mode as columnar scatter | yes — performance wins |
| 7 | Routed prototype / COUNT(DISTINCT) scope | whenever it makes most sense |
| 8 | Ticket table in or out | **IN** (architect, 2026-10-12: "we're adding the estimates") — R9 is part of the programme, sequenced after R1–R5 so it is presized from R4's estimate; x86 A/B before it ships as a default mode |

Note on 2: nothing regressed. The by-reference key path that exists is the
`keyref` carried through the **radix merge** (:3222, :5119, :5202), built
2026-09-25; keys were never referenced into the *output* — `jpc_emit_range`
always copied (one sequential window memcpy per chunk, which is cheap). The costly
part was never the final copy; it was the per-row `append_from` gather *before*
it. R5 fixes the gather; the final copy stays until measured otherwise.

### 8.1 Ticket table (R9): the case for and against

For: it is the only design that removes the per-worker **duplicate work** (CPU
roughly doubles from DOP 2 → 16 on the same high-NDV input because each worker
rediscovers every group) and the partition merge entirely; the paper shows it
matching or beating partitioned aggregation at low thread counts, which is prod
(4 vCPU Cloud Run) rather than the Mac; ~10× less memory at 10 M groups; elosdb,
2.4× us on ClickBench, ships exactly this. Against: it needs a group-count
estimate it can trust to presize (resize is 1.6–5.2× the cost; we will have the
estimate after R4, we do not today); strings need a key store addressed by
ticket (we accept hash identity, so no verification, but the store is new); no
spill path (we have none today either); SMT favours partitioned on x86 in the
paper's own numbers, so it must be an A/B'd *mode*, not a replacement; and it is
orthogonal to R1–R5, which the measured profile says are the cost now. So: out of
this programme's first phase, in as the third mode of R4 once R1–R5 are
measured — if R1–R5 bring Q32 to ~40–50 ns/row the remaining DOP-scaling loss is
the ticket table's exact target.

**Ruled IN 2026-10-12.** R9 stays last in the build order because it depends on R4 (presizing) and on R1's block store (ticket-ordered key store = one block per ticket range). It ships as a mode R4 can select, never as the only path.

## 8.2 Still open (performance questions, settled by measurement)

- Block row count per arch (L2-resident target; EXPRTILE precedent).
- Slab pool size / release policy (charged to the draken account; release under
  pressure or idle — the C7 give-way rule applies).
- Raw-mode switch: KMV-estimated global ratio vs the existing window ratio — pick
  the one that stops Q17-style misfires without delaying Q32/Q33.
- Whether R7 normalised keys pay on x86 (hash share is small; the win is the key
  store width).

Open facts I could not establish and did not assume: Pivot's default
`switch_threshold` beyond the 32 768-slot constant and its exact in-place table
capacity; DuckDB's `LOAD_FACTOR` value and `ht_entry_t` bit split (the H16 note has
it as 16-bit salt + 48-bit pointer); ClickHouse's `chooseAggregationMethod` full
mapping (header truncated); whether Pivot's merge tables are sized from the HLL or
from counts. None of these change the proposal.

---

## 9. As built — R1 + R3 (2026-10-12, uncommitted)

Rulings (architect, 2026-10-12 night, from the leave-one-out runs):

| Part | Ruling | Evidence |
|---|---|---|
| R1 segmented storage, doubling geometry | **kept** | base of everything below |
| Fixed-size segments | **removed** | no faster than doubling; +19–22% peak RSS |
| R2 batched new-group append / columnar raw scatter | **removed** | neutral or slower in three separate ablations (removing it: 0.914 vs 0.915, 0.901 vs 0.936) |
| R3 flush clears tables in place, queues data only | kept (final run re-measures) | earned ~1.4–2.8% |
| Flat-pointer pass C (`pass_c_spec<LanesOf>`, `GBFlatLanes`) | **kept** | apply CPU −5% on Q08/Q09; mixed elsewhere |

What R1 is now (`src/cpp/engine/gb_block_store.hpp`):

- `GBBlockPool`: power-of-two classes, per-thread caches traded in half-batches with
  the shared lists, a lock-free empty check on refill. Retains ≤ 1 GiB (unmeasured
  cap). One global mutex was the top frame on Q32 (+28%); the empty-list refill was
  the second.
- `GBSegArray` / `GBSegVec<T>`: doubling segments, first segment 2^11 rows unhinted,
  a hint from 64 rows up; tail-cursor append, first-segment fast path. No `data()`;
  `run_ptr` / `span` / `copy_out` for contiguous reads.
- `GBArena`: string bytes in chunks doubling 4 KiB → 1 MiB, never moved; a long string
  skips (leaves unallocated) chunks too small for it. A fixed 1 MiB first chunk made
  Q18 request 55 GB from the system per query (peak RSS 2.1×).

Sink changes (`native_group_sinks.hpp`):

- Every merge builds a fresh table hinted to the known total; radix buckets are
  pre-counted and hinted. Merged columns are one segment, so emit and top-k read them
  in place.
- `GBPartition` = `GBPartData` (queued at flush) + tables (index, parvi, medius) kept
  by the worker and cleared in place (`CarcharIndex::clear()` added).
- A flush window's columns take the last window's size (+¼) as their first segment —
  **shrink only** (capped at the 2^11 default). Without it, low-cardinality flush-heavy
  queries (Q08/Q09) paid a 2,048-row segment per lane per window (2.1× RSS, +10%);
  letting it grow overshot after a morsel jumped a window (Q32 RSS 12.1 → 16.7 GB).
- Long-string key stores past 4 GB per partition now fail loud (were a silent u32 wrap).

Remaining compile-time switches (temporary, removed once ruled): `OPTERYX_GB_FLUSH_RECONSTRUCT`
(R3-off), `OPTERYX_GB_NO_SEG_HINT` (window-hint-off); `OPTERYX_GB_STRESS` is the test-only
stress build (flush 8, merge leaf 2, raw switch 0.0) and stays.

Tests: `make gb-block-store-test` (in `make q`, ASan/UBSan, includes cross-thread pool
use); the stress build passes `make q` + `make slt` (its only GROUP BY test failures are
the top-k *engagement* asserts, which a 2-entry merge leaf cannot satisfy — the value
comparisons all pass).

Measurement notes: arms must differ in `_operators` only (sharing BASE's
`pool_reader`/`thread_pool`) — tree-built copies shifted the controls by 4%. Three-round
runs drift ±4% arm-to-arm on the controls; sub-5% calls need ≥ 5 rounds.
