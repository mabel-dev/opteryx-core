# Runtime min/max join filters — design note and feasibility

**Status:** **DELIVERED (skene v1)**, 2026-08-25. §§0–6 are the investigation that preceded it and
are left as written; §7 records how each decision was resolved and §9 is what actually landed.
**Date:** 2026-08-25 (investigation and delivery)
**Scope:** min/max (range) runtime filters from a join build side into the probe-side scan.
**Explicitly out of scope:** bloom filters of any kind (already measured net negative —
`join_bloom_prefilter_measured_net_negative`), the reservation buffer pool, Parquet page-index
filtering.

---

## 0. Summary

The mechanism is sound and the plumbing is unusually clean for us — much cleaner than Impala's,
because our pipelines are strictly serialised, so there is no "wait with a timeout then proceed
unfiltered" machinery to build. Every part the design needs already exists somewhere in the tree:
the barrier, the native ordinal kernel, the row-group zone-map evaluator, and the type-admissibility
predicate.

What is *not* clean is the payoff, and the payoff is entirely a property of **physical data layout**,
not of query shape:

| Suite | Applicability | Measured |
|---|---|---|
| ClickBench | **none** — 43 single-table queries, zero joins | n/a |
| h2o `j1`–`j5` | **none** — joins have no filters, so the build range is the full range | n/a |
| TPC-H (all 22, SF1/SF10) | **effectively none** | every measured build range ≈ 100% of the probe range |
| TPC-DS | **large, on 3 of 4 fact tables** | Q21: **93.3% of the probe scan pruned**; Q39: 77.8% |
| JOB | **large, on `movie_info` / `movie_info_idx`** | 96.5% / 95.0% of the probe scan pruned |

TPC-H gets nothing because its only clustered columns are surrogate keys, and every TPC-H filter is
on a non-key attribute uncorrelated with the surrogate key. Min/max is a terrible summary of a
scattered set: TPC-H Q18's build side is **624 orderkeys out of 15,000,000** and its range still
covers 99.98% of the key space.

TPC-DS and JOB win because they have the shape min/max is actually *for*: a dimension table filtered
to a contiguous window of a key that the fact table is clustered on.

Recommendation in §8.

---

## 1. Where the build side completes

### 1.1 The barrier is total, and it is a property of the Engine, not of the join

`Engine::run()` (`src/cpp/engine/engine.hpp:1113`) executes pipelines **strictly one at a time, in
creation order**:

```cpp
for (auto& pn : pipelines) {
    ...
    pn->result = run_pipeline(p, pdop, err, pool);      // engine.hpp:1141 — blocking
    if (err.code != 0) return;
    if (pn->fill_join2_ref >= 0) {                       // engine.hpp:1146
        join2_refs[pn->fill_join2_ref]->g =
            static_cast<const Join2BuildGlobal*>(pn->result.get());
    }
}
```

`run_pipeline` blocks until every worker has run `combine()` and `finalize()` has run once
(`src/cpp/engine/executor.hpp:178-205`). So at line 1146 the hash table is complete and **nothing of
the next pipeline has started** — not even its `GlobalSourceState`, which is constructed at
`executor.hpp:189` (`p.source->make_global()`) inside the *next* `run_pipeline` call.

That last point is the whole design. It gives us an insertion point that Impala does not have.

### 1.2 The compiler guarantees the probe pipelines are created later

`_compile_join` (`opteryx/managers/execution/compiler.py:3829`) compiles the build leg first and the
probe leg second:

- `bp, blayout = self.compile_node(build_id)` — compiler.py:4019
- `self.nplan.set_join2_build_sink(bp, ...)` — compiler.py:4106
- `pp, playout = self.compile_node(probe_id)` — compiler.py:4114

`new_pipeline()` appends, so **every** pipeline created inside the probe subtree has a higher index
than `bp`, and therefore runs after it. This holds even when the probe leg contains its own
breakers.

### 1.3 Per strategy

| Strategy | Barrier? | Is probe-side pruning *sound*? |
|---|---|---|
| Hash INNER (mode 0, incl. CROSS/`nested_loop`) | **yes** | **yes** — unmatched probe rows are dropped anyway |
| LEFT OUTER (mode 1) | yes | **NO** — probe is the preserved side; pruning deletes answers |
| FULL OUTER (mode 5) | yes | **NO** — same, plus the tail pipeline (compiler.py:4187) |
| SEMI (mode 2) | yes | **yes** |
| ANTI / ANTI_NULL_AWARE (modes 4, 3) | yes | **NO** — these emit the probe rows that *don't* match |
| SEMI/ANTI not-distinct (modes 6, 7) | yes | **NO** — NULL is a matchable value; see §4.3 |
| existence flag (`left existence`, `left existence anti`) | yes | **NO** — every probe row is emitted with a verdict appended |
| **Swapped** RIGHT SEMI / RIGHT ANTI (`_compile_swapped_semi_anti`, compiler.py:4199) | **yes**, three ordered pipelines (build → mark → emit; compiler.py:4220, 4249, 4271) | **yes, for both semi and anti** — the *streamed* leg emits nothing (`Join2MarkSink`); a streamed row outside the build key range can mark nothing either way. This is the large leg, so it is a good target. |
| BAND join (compiler.py:4851) | yes — LEFT = build, RIGHT = probe, inner semantics | yes for the equi key; the *band* column additionally admits a shifted bound (see §6.3) |
| ASOF (compiler.py:4321) | yes | **needs a ruling** — ASOF emit semantics for unmatched probe rows were not established in this investigation |

**So the eligible set is: INNER, SEMI, and the swapped semi/anti streamed leg.** Everything else must
refuse. This is the single most important correctness fact in this note and it is not obvious — an
ANTI join looks superficially like the same shape and pruning it silently deletes rows.

### 1.4 The one case where the barrier does *not* hold

Shared CTE bodies are compiled and run **before** the main plan (compiler.py:5095-5109 — "run()
executes pipelines in creation order, so every body's buffer is fully materialized before any
pipeline that reads it exists"). If the probe leg reads a shared CTE, its scan already ran. Detect
this structurally: the probe pipeline's source is a `BufferSource`, not a scan. Refusing on "probe
pipeline's own source is not a scan" covers it and every other breaker case for free.

---

## 2. Capturing min/max on the build side

### 2.1 It does not fall out for free — be honest about that

`Join2BuildSink::sink` (`src/cpp/engine/native_join2.hpp:631`) calls `compute_row_hashes` and then
loops per row checking `sort_row_valid` on each key column. **A hash destroys order**, so min/max
cannot be read off any of that. `Join2BuildGlobal` (native_join2.hpp:240) stores `row_hashes`,
`row_m`/`row_r` addresses and the CSR — no key values at all.

Capturing min/max is therefore a genuine **second pass over the build key column**. It is a cheap
one — the column is hot from `compute_row_hashes`, it is batch-orientated, and it only touches the
build side (the small side, by construction of `JoinOrderingStrategy`) — but it is not free and
should not be sold as free.

### 2.2 Use draken's ordinal space; do not invent a comparison

`draken_ordinalize(const DrakenVector&, int64_t* out, uint32_t n)`
(`draken/ops/hash.h:738`) is the table-dispatched, order-isomorphic value→int64 kernel, and
`draken_ordinalize_shaped` (`draken/ops/hash.h:763`) is the §11-correct shape-preserving twin — on a
dict-encoded build key it ordinalizes only the `data_length` distinct values, not every row.

This is the right currency for three reasons:

1. **It is already the scan-side currency.** `skene::compute_statistics`
   (`skene/src/statistics.cpp:160-240`) writes `min_ordinal`/`max_ordinal` using the *same*
   `ordinalize_scalar_*` helpers (`skene/src/statistics.cpp:19` — "the SAME"), and the zone-map
   evaluator `skene_zone_excludes` (`src/cpp/engine/native_skene_scan_source.hpp:303`) does nothing
   but int64 comparisons on them.
2. **It is already the plan-time currency.** `Manifest._ordinalize_literal`
   (`opteryx/models/manifest.py:322`) routes through `ColumnType.ordinalize`, i.e. the same scalar
   kernels. So a runtime bound and a pushed literal live in one space by construction — which is
   the *only* acceptable answer to the warning in `ordinal_zone_map_terms`
   (manifest.py:490-498): "A second site deciding any of the above would be a second dialect."
3. **Monotone + equality-preserving is exactly the property pruning needs.** `a < b ⇒ ord(a) ≤
   ord(b)` and `a = b ⇒ ord(a) = ord(b)`. So `rg.max_ordinal < build_min_ordinal` proves no probe
   value in that row group equals any build key. The VARCHAR 8-byte-prefix ordinal is lossy but
   still monotone, so it is *sound* (it can only forgo pruning, never over-prune).

### 2.3 Nulls

`skene::compute_statistics` computes min/max over **non-null values only**, explicitly because
`ORDINAL_NULL` is `INT64_MIN` and including a null would make every nullable column's min
`INT64_MIN` and prune nothing (statistics.cpp:170-173). The build-side capture must do the same, and
for the same reason. The build sink already identifies null keys per row (native_join2.hpp:655-663),
so the null rule is symmetric with the writer's by construction.

### 2.4 Types: where min/max is not well defined

`skene::type_has_min_max` (`skene/src/statistics.h:49-54`) is already exactly the admissibility
predicate we need, and already excludes the right things: DECIMAL128, VARIANT, ARRAY, VECTOR_FP16,
NULL. Reuse it; do not restate it.

Traps specific to this feature, all known sore points in this codebase:

- **Unsigned.** `ordinalize_scalar_u64` (`draken/ops/ordinalize.h:86`) biases by the sign bit so
  the full unsigned range maps onto int64 with order preserved. UINT8/16/32 widen. The writer uses
  the same helpers, so the two agree. The *parquet* path has the same trap solved separately in
  `CompareStatBytes` (`rugo/src/parquet/metadata.cpp:1384-1413`) via
  `StatsLogicalIsUnsigned` — two solutions to one problem, which is a §2 smell if the parquet path
  is also built (see §3.3 and Decision D4).
- **DECIMAL (int64-backed).** The ordinal is the **raw unscaled mantissa**
  (`draken/ops/ordinalize.h:38-42`). Comparing two DECIMAL ordinals is meaningful **only at equal
  scale**. A join between two DECIMAL columns of different scale would already be a binder/cast
  problem, but the filter must not assume the binder fixed it. **Refuse unless both sides carry the
  same scale**, or refuse DECIMAL entirely in v1.
- **DECIMAL128.** No ordinalize kernel, deliberately (`draken/ops/ordinalize.h:44-47`). Refuse.
- **TIMESTAMP64 / TIME32 / TIME64 / DATE32.** These *do* have vector ordinalize kernels — the ops
  table copies the whole INT64/INT32 entry (`draken/ops/hash.h:597-609`), `.ordinalize` included.
  So the native path covers strictly **more** types than `ColumnType.ordinalize` does at the Python
  scalar boundary (manifest.py:326 lists them as unsupported). Good news for time-keyed joins,
  which is where the real-world win lives — but TIMESTAMP64's **unit** lives in a logical
  descriptor, and two TIMESTAMP64 columns at different units ordinalize into different spaces.
  `skene::column_ordinal_at` takes the `LogicalType*` for exactly this reason
  (`skene/src/statistics.h:46`). The filter must compare units, not just physical types.
- **Cross-type keys.** `_join_key_coercions` (compiler.py:4315) materialises a synthetic CAST column
  when the two sides disagree on numeric category. The bound must be captured from the **coerced**
  key (`bkeyout`, compiler.py:4013), and the probe side must be the coerced probe key — which is a
  *computed column*, not a scanned one. See the next point.
- **Computed / cast join keys.** A synthetic CAST key has no scanned column to prune on. The
  filter is only expressible when the probe key identity resolves to a **direct column of the scan's
  own read set**. This is a plan-time structural test in the compiler, and it must be conservative:
  if the probe key is a coerced or expression key, refuse.
- **Multi-column keys.** Per-column min/max is a **bounding box**, and a conjunction of per-column
  bounds is still sound (a match requires equality in *every* key column, so a row group failing
  any one column's bound can hold no match). It is also usually useless — the tightest column
  dominates. Emit one term pair per eligible key column and let the zone map AND them, which it
  already does (`native_skene_scan_source.hpp:279` — "a CONJUNCTION").
- **Floats and NaN.** `_nan_invisible_to_bounds` (referenced at
  `opteryx/connectors/parquet_io/pool_reader.pyx:944`) exists because Parquet keeps NaN out of
  min/max while draken ranks it above every value. Skene's ordinal path canonicalises NaN at
  ingestion (`draken/ops/ordinalize.h:32-36`) so it orders highest consistently — but a float join
  key is a pathological case anyway. Refuse FLOAT32/FLOAT64 keys in v1 rather than reason about it.

### 2.5 Cost

Per build morsel, per eligible key column: one `draken_ordinalize_shaped` call plus an int64 min/max
reduction skipping `ORDINAL_NULL`, into a per-worker reusable scratch buffer; then a
`std::min`/`std::max` merge into `Join2BuildGlobal` under the existing `combine()` mutex (two
scalars, O(1), no new contention). For an INT64 key `ordinalize_widen` is an identity widen, so this
is close to a memory-bandwidth pass over one column of the small side.

**This has not been measured.** If it is built, `bench_join_build_side.cpp` is the existing harness
and the baseline must be taken before the first edit.

A fused `ordinal_minmax` entry in the draken ops table would avoid the scratch buffer entirely, but
that is a new draken kernel surface and a separate decision (D5).

---

## 3. Plumbing: getting a run-time bound into a scan planned earlier

### 3.1 The pushed-LIMIT precedent does **not** generalise

The note asked whether the pushed-LIMIT channel generalises. It does not, and the reason is worth
stating precisely: **`row_limit` is a plan-time constant.** `LimitPushdownStrategy` sets
`scan_node.limit` in Python, the compiler threads it to `set_native_scan_source`
(`opteryx/operators/_operators.pyx:2503`), and `NativeParquetScanSource::row_limit`
(`src/cpp/engine/native_parquet_scan_source.hpp:648`) is `const` for the run. Same for TopN manifest
pruning — `TopNManifestPruningStrategy` runs in the optimizer, entirely at plan time.

Neither is a run-time-discovered bound. There is **no existing channel** for one. The closest thing
in spirit is `limit_submit_cap()` (native_parquet_scan_source.hpp:686), which is a *one-time walk of
the work list performed in `make_global()`* — and that **mechanism** is exactly what we want, even
though the value it carries is not.

### 3.2 Skene — the good path

Skene's row-group pruning is already **native and already at run time**:

- Plan time produces `(physical_column_name, op_code, int64 ordinal)` conjunction terms via
  `Manifest.ordinal_zone_map_terms` (`opteryx/models/manifest.py:483`), attached to `SkeneScanPlan`
  at compiler.py:2816.
- Run time builds the claim list on first `get_morsel` under `std::call_once`
  (`src/cpp/engine/native_skene_scan_source.hpp:544`), which calls
  `SkeneWorkList::build(files, zone, ...)` → `zone_excludes_row_group`
  (native_skene_scan_source.hpp:451) → `skene_zone_excludes` (native_skene_scan_source.hpp:303).

So the channel already exists, is already native, and already runs after the build pipeline. The
addition is:

1. A new engine-owned `RuntimeBound { int64_t lo, hi; uint8_t valid; }` (or a small vector of them),
   filled once by `Engine::run()` right where it fills `join2_refs[...]->g` (engine.hpp:1146).
2. The probe scan's `SkeneZoneMap` gains a second, **runtime** term list the Source appends to
   `zone_` before `build()`: `(col, kSkeneZoneGtEq, lo)` and `(col, kSkeneZoneLtEq, hi)`.
3. The compiler wires the probe scan to the join's ref at compile time — it already knows the probe
   key identity (compiler.py:4118-4123) and the scan's `read_layout`
   (compiler.py:2792) — and resolves *all* type admissibility there, in Python, in the one place
   that knows the column's type. **Nothing about which conjuncts are sound is decided natively.**
   That is the discipline `ordinal_zone_map_terms` demands, kept intact.

Note `SkeneZoneMap` (native_skene_scan_source.hpp:332) is three borrowed parallel vectors owned by
the Cython plan object; the runtime terms would need engine-owned storage with a stable address,
which is the pattern `Engine::skene_scan_filters` / `latmat_owned_ints` (engine.hpp:216-232) already
uses.

### 3.3 Parquet — the awkward path

Parquet row-group pruning is **Python, at plan time**: `open_native_scan_plan`
(`opteryx/connectors/parquet_io/pool_reader.pyx:2174`) fetches footers, calls
`_rg_passes_predicates_native` (pool_reader.pyx:936) per row group, and emits a pruned `work_items`
vector (pool_reader.pyx:2366). By the time `Engine::run()` starts, the work list is fixed.

The good news: `footer_map` retains **every** row group's `ColumnStats`, including all the ones
`work_items` already dropped, and `NativeParquetScanSource::make_global()`
(native_parquet_scan_source.hpp:703) runs on the driver thread at the start of the probe pipeline —
after the build completed. So the same shape works: compute a skip-mask (or a filtered index list)
over `work_items` in `make_global()`, exactly as `limit_submit_cap()` already walks it once there.

The awkward part is the comparison. Parquet `ColumnStats::min`/`max` are **raw bytes** plus
`physical_type`/`logical_type` **strings** (`rugo/src/parquet/metadata.hpp:18-40`). Two options:

- **(a) Compare in stat-byte space.** `CompareStatBytes` (`rugo/src/parquet/metadata.cpp:1384`)
  already does exactly the type-aware, unsigned-aware comparison and already exists — but it is
  `static` (file-local, needs exposing), it has **no DECIMAL handling** (fixed_len_byte_array falls
  through to lexicographic at metadata.cpp:1428, which is wrong for negative big-endian two's
  complement), and it would require encoding the build-side ordinal *back* into parquet stat bytes.
- **(b) Convert stat bytes → ordinal.** A small native `stat_bytes_to_ordinal(physical_type,
  logical_type, bytes)` reusing `ordinalize_scalar_*`, putting parquet in the same ordinal space as
  skene. ~100 lines, one dialect, symmetric with everything else in this note. This is the
  architecturally correct answer and the more expensive one.

Either way the parquet path is a meaningfully larger piece of work than the skene path, and it can
be staged separately. **The benchmarks that show a win (TPC-H/TPC-DS/JOB) all run on skene mirrors**
(`Makefile:514-526`), so a skene-only v1 is measurable end to end.

The trampoline (`StreamingScanSource`) path and the two latmat sources
(`native_latmat_scan_source.hpp`, `native_skene_latmat_scan_source.hpp`) are out of scope for v1 and
must simply not carry the filter.

---

## 4. The correctness contract

### 4.1 Our guarantee is stronger than Impala's, and simpler

Impala's scanners wait a bounded time for a filter and then proceed unfiltered, because its
fragments run concurrently and a filter may arrive after a scan range is already in flight. **We
have no such race.** `Engine::run()` is a serial loop over pipelines (engine.hpp:1125-1156); the
build pipeline has fully completed and `finalize()` has run before the probe pipeline's
`GlobalSourceState` is even constructed (executor.hpp:189).

So the guarantee is not "arrives in time, usually" — it is:

> **A runtime bound is always complete, and always visible, before the probe scan enumerates its
> first row group. There is no late arrival, no timeout, and no partially-populated filter.**

This is enforced structurally, in one place — the pipeline ordering in `Engine::run()` — and it is
already load-bearing for two other features (the FULL OUTER tail, compiler.py:4184-4188, and the
swapped semi/anti mark/emit sequencing, compiler.py:4202-4204), both of which document it as "the
mechanism, not a detail".

### 4.2 What must still be true

1. **Pruning must be a necessary-condition test only.** A surviving row group is still read and
   still joined normally. The filter can only remove row groups that provably hold no matching row.
2. **The join mode must be one that drops unmatched probe rows** (§1.3). This is the failure mode
   that actually bites; it must be a positive allow-list in the compiler, not a deny-list.
3. **Absence must be free.** A missing, invalid, or untracked bound costs a read, never an answer —
   the same rule `skene_zone_excludes` already enforces (`native_skene_scan_source.hpp:305`:
   "untracked, not empty"). A build side that produced zero non-null keys yields `valid = 0`,
   which prunes **nothing**. (Tempting to prune *everything* — an inner join against an empty build
   emits nothing. Do not couple the two: that is an emptiness optimisation with its own soundness
   argument and it belongs in the join, not the scan.)
4. **Ordinal space must be shared with the writer.** Enforced by construction if the capture uses
   `skene::column_ordinal_at` / `draken_ordinalize` rather than anything new.
5. **The min/max must be over non-null build keys**, matching the writer (§2.3).

### 4.3 The `null_equal` hole

Modes 6/7 (`left semi/anti not-distinct`, i.e. INTERSECT/EXCEPT) treat NULL as a value that equals
itself; the build sink inserts NULL-keyed rows on the ordinary path (native_join2.hpp:648-651).
Skene's stats are over non-null values, so a row group whose *non-null* values all fall outside the
bound can still hold NULL rows that legitimately match a NULL build key. **Pruning is unsound under
`null_equal`.** These modes are already excluded by §1.3 for a different reason, but the reason is
independent and both should be stated where the refusal is written.

### 4.4 Testing

- An oracle test that runs each eligible query shape twice — filter armed and filter disabled — and
  asserts **byte-identical result morsels**, not just equal row counts.
- A test per refused mode (LEFT OUTER, FULL OUTER, ANTI, ANTI_NULL_AWARE, not-distinct, existence)
  asserting the filter is *not* armed, in the spirit of
  `tests/storage/test_topn_manifest_pruning.py::test_declined_predicate_disables_the_optimization`.
- The pruning counter must be reported (`row_groups_pruned` already exists on the scan reading —
  see `operations[...]['row_groups_pruned']`), and it must be reported *separately* from plan-time
  pruning so "the plan-time filter was already doing this" is never mistaken for a new win.

---

## 5. Applicability — measured

All measurements below are from this repo's own data on 2026-08-25. Method: read per-row-group
`min_ordinal`/`max_ordinal` from the skene footers (`skene.skene_native.read_metadata`), and measure
the build side's *observed* key range by running the build leg's SQL through the engine. No
estimates, no guesses. **No timing was measured — nothing here claims a speedup, only bytes not
read.**

### 5.1 The precondition nobody states: the probe must be clustered on the join key

Mean row-group range width as a fraction of the column's global range, `testdata/tpch_10_skene`
(width 1.0 = every row group spans the whole range = **no min/max filter can ever prune anything**):

| column | row groups | mean rg width |
|---|---|---|
| `lineitem.l_orderkey` | 237 | **0.0063** |
| `orders.o_orderkey` | 58 | **0.0417** |
| `partsupp.ps_partkey` | 31 | **0.0776** |
| `part.p_partkey` | 8 | 0.2883 |
| `customer.c_custkey` | 6 | 0.3771 |
| `lineitem.l_partkey` | 237 | **1.0000** |
| `lineitem.l_suppkey` | 237 | **1.0000** |
| `orders.o_custkey` | 58 | **1.0000** |
| `partsupp.ps_suppkey` | 31 | **1.0000** |
| all `*_nationkey`, `*_regionkey`, `s_suppkey` | — | **1.0000** |

### 5.2 TPC-H: zero, across the board

Observed build-side key ranges against the probe's global range (SF10):

| shape | build rows | observed build key range | probe range | prunes |
|---|---|---|---|---|
| Q3 `customer⋈orders` (build) → `lineitem.l_orderkey` (probe) | 148,045 (SF1) | `[65, 5999975]` | `[1, 6000000]` | **0 of 16 rg** |
| Q12 `orders` → `lineitem.l_orderkey` | 15,000,000 | `[1, 60000000]` | `[1, 60000000]` | **0** |
| Q4 `lineitem(commit<receipt)` → `orders.o_orderkey` | 37,929,348 | `[1, 60000000]` | `[1, 60000000]` | **0** |
| Q18 `lineitem GROUP BY … HAVING SUM>300` → `orders.o_orderkey` | **624** | `[6882, 59993957]` | `[1, 60000000]` | **0** |
| Q19 `part(brand/size)` → `lineitem.l_partkey` | 7,907 | `[292, 1999902]` | unclustered anyway | **0** |
| Q20 `part(p_name LIKE 'forest%')` → `partsupp.ps_partkey` | 21,551 | `[5, 1999994]` | `[1, 2000000]` | **0** |

Q18 is the instructive one: a build side of **624 keys out of 15M rows** still spans 99.98% of the
key space. This is the fundamental limitation of min/max and no amount of engineering changes it.
Set membership is what would help there — and we have already measured that the probe-side bloom
answer to that does not pay.

Verified end to end: TPC-DS Q21's `inventory` scan reports `row_groups_read=45, row_groups_pruned=0`
today; TPC-H Q3's `lineitem` likewise.

### 5.3 TPC-DS: large, on the fact tables that happen to be clustered

`testdata/tpcds_1_skene`, `inv_date_sk` / `cs_sold_date_sk` clustering measured at mean rg width
0.0222 and 0.1667 respectively; `store_sales.ss_sold_date_sk` and `web_sales.ws_sold_date_sk` are
**1.0000** — unclustered, so they prune nothing regardless.

Percentage of the probe scan that would still be **read** after a perfect runtime min/max filter:

| build-side filter on `date_dim` | build range | `inventory` (45 rg, 11.7M rows) | `catalog_sales` (6 rg, 1.44M rows) | `store_sales` | `web_sales` |
|---|---|---|---|---|---|
| `d_date BETWEEN 2000-02-10 AND 2000-04-10` (Q21) | `[2451585, 2451645]`, 61 rows | **6.7%** (3/45 rg) | — | — | — |
| `d_year = 2001` (Q39) | `[2451911, 2452275]`, 365 rows | **22.3%** (10/45) | 36.4% (2/6) | 100% | 100% |
| `d_year = 2000` | `[2451545, 2451910]` | 20.1% (9/45) | 36.4% | 100% | 100% |
| `d_year = 2000 AND d_moy = 1` | `[2451545, 2451575]`, 31 rows | **2.2%** (1/45) | 18.2% (1/6) | 100% | 100% |
| `d_month_seq BETWEEN 1200 AND 1211` | `[2451545, 2451910]` | 20.1% | 36.4% | 100% | 100% |

The plan for TPC-DS Q21 confirms the required shape exactly — `date_dim` is the build (4,795 est
rows; 61 actual), `inventory` (11,745,000 rows) is the probe and is a direct `Skene Reader`, and the
join is `Inner Join Draken`:

```
7731641 └─ Grouped Aggregate (Hashed)
7731641    └─ Inner Join Draken            <- warehouse ⋈ …
         5       ├─ Skene Reader  warehouse
7731641       └─ Inner Join Draken         <- item ⋈ …
      4500          ├─ Skene Reader  item
30926565          └─ Inner Join Draken     <- date_dim ⋈ inventory   ** the one **
      4795             ├─ Skene Reader  date_dim      (BUILD)
  11745000             └─ Skene Reader  inventory     (PROBE)
```

**32 of the TPC-DS queries in `tests/performance/tpcds/opteryx/queries/` join a clustered `*_date_sk`
column** (28 on `catalog_sales`, 4 on `inventory`: Q21, Q22, Q37, Q39). At SF1 `catalog_sales` has
only 6 row groups, so the granularity caps the win at 5/6; at larger scale it improves.

### 5.4 JOB: large, and for a different reason

`testdata/job_skene`. `movie_info.info_type_id` is strongly clustered (mean rg width 0.0644 over 57
row groups). JOB queries filter `info_type.info = '<one value>'`, which is a single-row build side:

| join | build | build range | probe still read |
|---|---|---|---|
| `info_type(info='rating')` → `movie_info.info_type_id` (14.8M rows, 57 rg) | 1 row | `[101, 101]` | **3.5%** (2/57 rg) |
| `info_type(info='genres')` → `movie_info.info_type_id` | 1 row | `[3, 3]` | 19.4% (11/57) |
| `info_type(info='top 250 rank')` → `movie_info_idx.info_type_id` (1.38M rows, 6 rg) | 1 row | `[112, 112]` | **5.0%** (1/6) |
| `keyword(keyword='character-name-in-title')` → `movie_keyword.keyword_id` | 1 row | `[117, 117]` | 100% (unclustered) |
| `company_name(country_code='[us]')` → `movie_companies.company_id` | 84,843 rows | `[1, 234997]` | 100% |

### 5.5 The generalisable rule

The filter pays when **both** hold:

1. the probe table's physical layout is **clustered on the join key** (mean row-group range width
   materially below 1.0), and
2. the build side's key range is a **contiguous narrow window** of that key — which in practice
   means either a range filter *on the join key itself*, or a filter on a column **correlated** with
   it (a date dimension's `d_year` → `d_date_sk`, an `info_type.info` → `info_type.id`).

Both are properties of the data, not of the SQL. A surrogate key filtered by an uncorrelated
attribute (all of TPC-H) yields nothing. This should be stated in whatever telemetry the feature
carries, so an absent win reads as "the layout doesn't support it", not "the filter is broken".

---

## 6. Interaction with join ordering, the estimator, and existing strategies

### 6.1 `JoinOrderingStrategy` mostly helps, and must not be changed to help more

`join_ordering.py:20-25`: rule 1 puts the larger side on the **right** (probe) when one side is >3×
the other; rule 3 uses key cardinality. The build is the smaller side, which correlates with a
narrower key range, so the existing rule usually points the right way. Q21 and JOB both land
correctly with no change.

Rule 3 (NDV) can override into a shape where the build side is the wide-range one — see
`join_ordering_ndv_rule3_overrides_rowcount`. **Do not add a runtime-filter term to the join-ordering
cost model.** Ordering is load-bearing for absorbed thetas and swapped semi/anti
(`join_ordering_is_load_bearing_for_absorbed_thetas`, `swapped_semi_anti_was_dead_two_bugs_fixed`);
perturbing it to chase a filter that pays only under a data-layout precondition the optimizer cannot
see is a bad trade. The filter should be a pure consumer of whatever ordering it is given.

### 6.2 The plan-time version already exists — and this is precisely the gap it cannot close

`CorrelatedFiltersStrategy` (`opteryx/planner/optimizer/strategies/correlated_filters.py`) already
transports both min/max bounds across an equi-join at **plan time**, from the propagated
`node.statistics` `ColumnRange`, straight onto the opposite leg's `scan.predicates` — the same
channel `PredicatePushdown` feeds. It fires on both Q3 and Q21 ("inner join correlated filter |
applied 2×" in both EXPLAINs).

And it prunes **nothing** in either case, because the planner's per-column range propagation cannot
follow a filter on a *sibling* column. `statistics_are_richer_than_row_count_but_filters_strand`
records that min/max is available for nearly every column — but `d_date BETWEEN …` narrows
`d_date`'s range, not `d_date_sk`'s, and no correlation model connects them.

So the honest one-line statement of what this feature adds:

> It replaces the estimator's per-column range *propagation* — which cannot cross a correlated
> sibling column — with the *observed* range of the actual build keys.

Two consequences: (a) the design should be framed as the runtime tier of an existing strategy, not a
new idea; (b) the telemetry must separate plan-time from runtime pruning or the win will be
misattributed.

### 6.3 Band joins get a bonus

`correlated_filters.py` already handles the band case at plan time (a `l.t > f.start - INTERVAL '20'
SECOND` transports one shifted bound). A band join's build sink already captures a per-row order key
(`asof_keys`, native_join2.hpp:104 / `set_asof_build_sink`), so the band column's runtime min/max is
**genuinely nearly free** there — unlike the equi case, the ordering pass is already being done.
Worth noting; not worth doing in v1.

### 6.4 Not the same thing as `SemiJoinReducer`

`semi_join_reducer.py` warns: "NOT a general 'filter the probe side' rule … Probe misses are already
cheap". That is correct **and it is about a different resource**. A reducer (and a probe-side bloom)
filters rows that have **already been read and decoded**; this filter removes row groups that are
**never fetched or decoded at all**. The bloom result does not transfer, in either direction, and
neither does the reducer's gate.

---

## 7. Decisions — and how each was resolved

| # | Decision | Resolution as built |
|---|---|---|
| **D1** | **Proceed at all**, given that TPC-H, ClickBench and h2o gain nothing and the win is confined to TPC-DS-shaped and JOB-shaped layouts. | **Proceed** (architect, 2026-08-25). |
| **D2** | **Eligible modes.** | Allow-list is exactly `{INNER, SEMI}` plus the swapped semi/anti **streamed** leg, as an explicit positive list in `_Compiler._RUNTIME_BOUND_MODES`. **ASOF refused** — its unmatched-probe-row semantics were not established here, and refusing costs a read. Still open if ASOF is ever wanted. |
| **D3** | **Skene-only v1?** | Yes. The parquet path is untouched and carries no bound. |
| **D4** | If parquet is done: stat-byte comparison vs a `stat_bytes → ordinal` bridge. | **Still open** — deferred with the parquet path. The recommendation stands: one dialect (b). |
| **D5** | **New draken surface?** — fused ops-table kernel vs a scratch buffer. | **Neither, and better than both.** A `cxx_ordinal_bounds_c` C-ABI seam (`draken/morsels/cxx_ordinal.h`) next to the existing `cxx_hash_c`: no new ops-table entry, no scratch buffer in the engine, and — the actual reason it had to be this shape — `ops/hash.h`'s dispatch table is `static inline`, so including it in `_operators.so` would have created a SECOND copy of the table. |
| **D6** | **Type allow-list.** | As recommended, in `_runtime_bound_type_ok`: integers (signed and unsigned), DATE32, BOOL and the string family. FLOAT32/64, DECIMAL, DECIMAL128, TIMESTAMP64, TIME32/64, ARRAY and VARIANT all refuse. |
| **D7** | **Empty build side** → skip the probe pipeline entirely. | Not done, as recommended. An unfilled bound prunes NOTHING; the empty-build optimisation remains a separate thread with its own soundness argument. |
| **D8** | **Multi-column keys.** | One bound per eligible key column; the zone map already ANDs the terms. |
| **D9** | **Telemetry.** | Done, and split as insisted: `row_groups_pruned_runtime` is the runtime filter's **marginal** share (first-proving-term-wins with plan terms ordered first), and is **absent**, not zero, when no bound was wired. Plus a plan-time `runtime_minmax_bounds_wired` count so "did not fire" is distinguishable from "fired and pruned nothing". |

---

## 8. Recommendation (as written before delivery)

**Proceed, narrowly, and only after D1 is answered — because D1 is genuinely a judgement call about
which workloads matter, and it is yours, not mine.**

The case *for*: the mechanism is sound; the barrier is free and already load-bearing elsewhere; the
ordinal space, the zone-map evaluator and the type-admissibility predicate all already exist, so
this is mostly wiring rather than new machinery; the measured wins on TPC-DS Q21 (93.3% of an 11.7M
-row scan) and JOB (96.5% of a 14.8M-row scan) are large and are *data never read*, which is the
best kind of saving we can buy; and 32 of our TPC-DS queries have the shape.

The case *against*: it is worth **nothing** on TPC-H, ClickBench and h2o; the win is contingent on a
data-layout property the planner cannot observe; and the eligible-mode allow-list is a sharp edge
(an ANTI join looks like the same shape and pruning it silently deletes rows).

If it proceeds, the staging I would propose:

1. **Skene only.** Compiler-side eligibility (mode allow-list, direct-scan-column probe key, type
   allow-list) + build-sink ordinal min/max capture + a runtime term list appended to
   `SkeneZoneMap` before `SkeneWorkList::build`. Correctness gates before any timing:
   filter-on/filter-off byte-identical oracle, plus a refusal test per excluded mode.
2. **Measure on TPC-DS Q21/Q22/Q37/Q39 and JOB**, with `make q` + `tests/sql` green, and with a
   baseline taken before the first edit. Report `row_groups_pruned` separately from the plan-time
   figure. Report the build-side capture cost as its own number so a negative case is visible.
3. **Only then** decide D4 and whether parquet is worth the bridge.

Do not build it as a general "runtime filter framework". Impala needs one because its fragments race;
we do not. What we need is one bound, captured in one sink, read in one place.

---

## 9. What landed (skene v1)

### 9.1 The mechanism, end to end

```
BUILD PIPELINE                              ENGINE                    PROBE PIPELINE
Join2BuildSink::sink                                                  NativeSkeneScanSource
  cxx_ordinal_bounds_c(morsel, key)   ->                              ::get_morsel
  per-worker lo/hi                                                      std::call_once:
Join2BuildSink::combine                                                   plan zone terms
  merge under the existing mutex                                          + runtime terms
                                      Engine::run(), between                 (GtEq lo, LtEq hi)
                                      two pipelines:                    -> SkeneClaimSet::build
                                        RuntimeKeyBound{lo,hi,valid=1}     row groups skipped
```

| Piece | Where |
|---|---|
| The neutral bound carrier | `src/cpp/engine/runtime_bound.hpp` (new) |
| Ordinal min/max over non-null rows | `cxx_ordinal_bounds_c`, `draken/draken_native.cpp`; declared in `draken/morsels/cxx_ordinal.h` (new) |
| Capture + per-worker merge | `Join2BuildSink::sink` / `::combine`, `src/cpp/engine/native_join2.hpp` |
| Publish (the barrier) | `Engine::run`, `src/cpp/engine/engine.hpp` — the `fill_join2_ref` block |
| Arm a build sink after the fact | `Engine::set_join2_bound_slots`, `engine.hpp` |
| Wire a scan to a slot | `Engine::add_skene_runtime_bound`, `engine.hpp` |
| Consume | `NativeSkeneScanSource::get_morsel`'s `call_once`, `native_skene_scan_source.hpp` |
| Attribution-aware pruning | `SkeneClaimSet::build` / `zone_excluding_term`, same file |
| Eligibility (ALL of it) | `_Compiler._RUNTIME_BOUND_MODES`, `_runtime_bound_type_ok`, `_wire_runtime_bounds`, `opteryx/managers/execution/compiler.py` |
| Switch | `disable_runtime_minmax_join_filter` (session variable, USER/UNRESTRICTED) / `DISABLE_RUNTIME_MINMAX_JOIN_FILTER` (env) |
| Gate | `tests/integration/test_runtime_minmax_join_filter.py` |

### 9.2 Why `cxx_ordinal_bounds_c` and not a scratch buffer (supersedes D5)

`draken/ops/hash.h`'s dispatch table is `static inline`. Including it in `_operators.so` would give
that object **its own copy of the ops table**, separately constructed — the same class of hazard
`executor.hpp` documents for `BSThreadPoolBridge`. So the capture routes through one `extern "C"`
symbol, exactly as `cxx_hash_c` already does. One table, one definition of a value's ordinal, and
the min/max reduction happens inside draken so the engine allocates nothing.

**Nulls are excluded by VALIDITY, not by sentinel.** `draken_ordinalize` writes `ORDINAL_NULL`
(`INT64_MIN`) for a null row, but `INT64_MIN` is also the honest ordinal of a real INT64 value, so
filtering on the sentinel would drop that value out of the bound and **over-prune**. The validity
bitmap is re-read per row instead — the same choice `skene/src/statistics.cpp` makes.

### 9.3 The correctness contract, as enforced

* **The barrier** is `Engine::run`'s serial pipeline loop, and `_wire_runtime_bounds` additionally
  refuses unless `probe_pipeline > build_pipeline` — a cheap restatement of the invariant the whole
  feature rests on. No timeout, no late arrival, no partially-populated filter, and no atomics.
* **The mode allow-list is positive**, not a deny-list. `{INNER, SEMI}` plus the swapped streamed
  leg; everything else refuses.
* **Absence is free.** An unwired, unfilled or invalid bound contributes no zone term. A build side
  with no non-null keys yields `valid = 0`, which prunes nothing.
* **The probe key must be a direct column of a not-yet-started skene scan.** One structural test
  (`skene_scan_pipelines`, keyed by PIPELINE) covers coerced/computed keys, every breaker between
  the scan and the probe, and the shared-CTE case of §1.4 at once.

### 9.4 Measured, after the fact

Correctness — **233 queries, 0 mismatches**, full result values compared with the filter on and off:

| Suite | Compared | Armed | Runtime-pruned row groups | Scan rows |
|---|---|---|---|---|
| TPC-DS sf1 | 98 (Q51 unsupported, pre-existing) | 92 | 217 | 339.6M → 286.8M (**15.5% fewer**) |
| JOB | 113 | 113 | 1,919 | 2.004bn → 1.771bn (**11.6% fewer**) |
| TPC-H sf1 | 22 | 18 | **0** | 90.9M → 90.9M (**0.0%**) |

TPC-H is the honest cost case: 18 of 22 queries arm a bound, capture it, and prune nothing with it —
exactly as §5.2 predicted.

Q75's raw rows differ run-to-run **with the filter off as well**; that is the known FLOAT64 `SUM`
reassociation (`tpcds_q75_is_nondeterministic`), not this change. Rounded to 4dp, off/off/on are
byte-identical.

Timing, interleaved A/B with the within-round arm order alternating, median of rounds
(SF1, Apple Silicon, warm page cache):

| Suite | Rounds | Filter off | Filter on | |
|---|---|---|---|---|
| TPC-DS sf1 | 9 | 9721 ms | 9348 ms | **-3.8%** |
| TPC-H sf1 | 15 | 839.2 ms | 839.7 ms | **+0.1%** |
| JOB | 5 | 15473 ms | 14576 ms | **-5.8%** |

(TPC-H at 15 rounds because it is the pure-cost arm and needed the resolution; JOB at 5 because its
off arm scans 2 billion rows per round.)

The win is concentrated where the note predicted. TPC-DS: **Q21 -13.2%**, **Q72 -12.6%** (Q72 is
2.9s of the 9.7s suite, so it is most of the -3.8%). JOB, where it is much larger per query:
**8d -62.0%**, **12a -45.1%**, **12b -47.7%**, **8c -47.6%**, **12c -42.0%**, **7b -29.7%**.

**No regression survived re-measurement.** Every apparent one was ≤ +6.2% at 5–9 rounds, and a
15-round interleaved re-run of the largest resolved them all to noise:

| | 5–9 rounds | 15 rounds | bounds armed | row groups pruned |
|---|---|---|---|---|
| TPC-DS Q87 | +10.1% | **-1.6%** | 6 | 4 |
| TPC-DS Q50 | +9.9% | **-0.3%** | 5 | 0 |
| TPC-DS Q43 | +7.1% | **-0.2%** | 2 | 0 |
| TPC-DS Q12 | +5.3% | **-0.6%** | 4 | 0 |
| JOB 28a | +6.2% | **+2.4%** (σ ±23ms on 163ms) | **23** | 0 |
| JOB 27c | +5.8% | **+1.2%** (σ ±11ms on 115ms) | 21 | 0 |
| JOB 3c | +4.8% | **+3.6%** (σ ±12ms on 132ms) | 4 | 0 |

JOB 28a is the worst case the suite offers for pure overhead — **23 bounds armed, none of which
prunes anything** — and it still lands inside its own standard deviation. The noise floor at this
scale is ~±4%, calibrated on TPC-H Q6, which has no join at all and still moved 4.3% between arms.

⛔ **These are SF1, warm-cache, single-machine numbers.** The pruning counts are deterministic and
are the real result; the wall-clock percentages are not a claim about production, where the saving
is network bytes rather than warm mmap pages.

### 9.5 Known limits of v1

* ~~**Parquet carries no bound.**~~ Delivered — see §10.
* **Skene two-pass (latmat) scans carry no bound** — they are simply absent from
  `skene_scan_pipelines`, so they refuse by construction.
* **ASOF refused**, pending a ruling on its unmatched-probe-row semantics.
* **Band joins** get the equi-key bound like any INNER join, but not the shifted *band*-column bound
  of §6.3, which is nearly free there and remains the obvious next increment.
* **The build-side capture cost was not separately measurable at SF1.** TPC-H is the clean
  experiment for it — 18 of 22 queries arm a bound, capture it, and prune nothing with it — and it
  came back **+0.1% over 15 interleaved rounds**, with the per-query spread (±4.5%) no wider on the
  join queries than on join-free Q6. That is "below the noise floor at this scale", NOT "free": a
  large build side at SF100 has not been measured, and the capture is a genuine second pass over the
  build key column (§2.1).
* Downstream `docs.opteryx` / `web.opteryx` regeneration (`make sql-definitions` / `make
  sql-signatures`) has not been run for the new variable; `reference/variables.json` here is
  current.

---

## 10. What landed (parquet)

Delivered 2026-08-31, resolving D3/D4. §3.3's option **(b)** was taken: parquet
statistics are converted FORWARD into draken's ordinal space rather than the
build-side ordinal being encoded back into parquet stat bytes. Option (a) was
rejected for the reason §3.3 already gives — `CompareStatBytes` falls through to
a lexicographic compare for `fixed_len_byte_array`, which is wrong for negative
big-endian two's complement, and adopting it would have inherited that bug.

### 10.1 The mechanism

`stat_bytes_to_ordinal` (`src/cpp/engine/parquet_stat_ordinal.hpp`) turns a
`ColumnStats` min/max into an ordinal. `NativeParquetScanSource::apply_runtime_bounds`
runs in `make_global()` — on the driver thread, at the start of the probe
pipeline, which is after the build pipeline completed and `Engine::run()`
published the bound — and drops work items whose row-group range is disjoint
from the bound. The surviving indices live in `NativeParquetScanGlobal::kept`.

Where skene folds the bound into two extra zone-map terms and reuses its claim
builder, parquet has no shared zone-map machinery on that path, so the pruning
is a work-list filter instead. Both consume the same `RuntimeKeyBound` slots
and the same eligibility test; only the consumer differs.

Three things were deduplicated rather than copied:

* `StatsLogicalIsUnsigned` moved from a file-local `static` in
  `rugo/src/parquet/metadata.cpp` into `metadata.hpp`. Plan-time comparison and
  runtime ordinalization must agree about signedness or the filter prunes row
  groups that genuinely match.
* `SkeneRuntimeBounds` became an alias of a shared `RuntimeBoundSet` in
  `runtime_bound.hpp`, so "parallel arrays" cannot come to mean two things.
* Eligibility condition 2 widened from "the probe pipeline's source is a native
  skene scan" to "…is a native skene scan OR a native parquet scan". The barrier
  argument is about what a pipeline's SOURCE is, not which format it reads, so it
  carries over unchanged. The two-pass latmat sources remain out of scope by
  construction — they are absent from both pipeline maps.

### 10.2 Order of operations, and why

`apply_runtime_bounds` runs BEFORE `limit_submit_cap`. The two interact: a row
group the bound removed cannot contribute rows to a scan-pushed LIMIT, so
counting it toward the cap would freeze the submit frontier too early and the
scan would return fewer rows than the LIMIT asked for.

### 10.3 Deliberate v1 limits

* **INT32/INT64 only** (signed, plus unsigned via the logical type).
  Everything else — `int96`, `float`, `double`, `byte_array`,
  `fixed_len_byte_array`, and so DECIMAL and VARCHAR — returns false from
  `stat_bytes_to_ordinal` and contributes no term. This is the same posture
  `valid == 0` already carries: prunes nothing, costs a read rather than an
  answer. Note this is NARROWER than skene, which does bound VARCHAR keys
  through the lossy 8-byte prefix ordinal. Widening it is a measurement
  question, not a correctness one.
* A short statistic buffer refuses rather than guessing — a malformed footer is
  not a value.
* Every failure mode fails OPEN: unresolvable path, missing statistic,
  unsupported type, unfilled bound all keep the row group.

### 10.4 Measured

`testdata/job` (parquet), `movie_info` ⋈ `info_type` on `info_type_id` —
`movie_info.info_type_id` has a mean row-group range width of 0.103 over 30 row
groups, and `info_type.info = '<one value>'` is a single-row build side, so this
is §5.5's rule satisfied on both counts. Filter on vs off, minimum of 4
interleaved rounds, answers verified identical in every arm:

| build side | bytes off | bytes on | share | time off | time on |
|---|---|---|---|---|---|
| `info = 'genres'` | 329.1 MB | 5.1 MB | **1.5%** | 0.173 s | **0.023 s** |
| `info = 'countries'` | 329.1 MB | 130.9 MB | 39.8% | 0.176 s | 0.164 s |
| `info = 'release dates'` | 329.1 MB | 222.0 MB | 67.4% | 0.189 s | 0.170 s |

`testdata/tpcds_1` (parquet), the positive control the test asserts:
`inventory ⋈ date_dim` on one month prunes **94 of 96 row groups**, where
plan-time pruning gets zero — §6.2's gap, now closed on parquet too.

The spread across the three JOB arms is the point, not the best number: the same
mechanism yields 7.5x, 7% and 10% purely on how contiguous the build side's key
window is. That is a property of the data, exactly as §5.5 says.

### 10.5 Known limits

* The build-side capture cost is still unmeasured at scale (§9.5 carries the
  same caveat for skene); nothing here changes that.
* **Confirmed on x86** (2026-09-01, Debian 12 / gcc 12 / `-march=haswell`,
  Intel i5-8500; the Mac figures are clang / NEON). The pruning is
  BYTE-IDENTICAL across the two — 1.5% / 39.8% / 67.4% on the three JOB arms,
  and 94-of-96 row groups on the TPC-DS control, to the same decimal. That is
  the result that matters: the bound is derived from footer statistics and
  draken ordinals, so an arch-dependent byte figure would have meant a real bug.

  The TIME saved is larger on x86, because the work avoided is worth more on the
  slower box: `genres` goes 0.880s → 0.058s (**15.2x**, against 7.5x on ARM),
  `countries` 0.889s → 0.673s (24%, against 7%), `release dates` 0.905s → 0.784s
  (13%, against 10%). Do not read the absolute times across machines; only the
  ratios are comparable.

  `make q` is 462/462 on both. `tests/sql` on the x86 box is not a clean signal:
  606 of its tests need `pyarrow`, which that pyenv does not carry, and 38 more
  need `testdata/band_join`, which was not synced. No engine failure appeared on
  either platform.
* ClickBench cannot exercise this at all: it is a single-table benchmark with no
  joins. TPC-H prunes zero for the reason §5.2 already establishes. The parquet
  win is a JOB/TPC-DS-shaped one.
