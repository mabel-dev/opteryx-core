# Top-N runtime boundary — row-group skipping for `WHERE … ORDER BY … LIMIT n`

**Status:** **v1 DELIVERED (parquet single-pass + parquet latmat)**, 2026-09-27, with the
architect's recommended decisions (§8). §§0–9 are the design as written; where the build
proved a passage wrong it is corrected in place and marked *[corrected in build]*. §10 is
what landed and what it measured.
**Date:** 2026-09-27
**Trigger:** ClickBench Q24 (3.8× DuckDB), Q25 (2.8×) and Q27 (3.6×) — our three worst
ratios among queries that are not dominated by fixed cost.
**Builds on:** `docs/RUNTIME_MINMAX_FILTER_DESIGN.md` (delivered). Same ordinal space, same
stat converter, same fail-open posture. **Different producer, and no barrier.**

```sql
SELECT <proj> FROM t WHERE <pred> ORDER BY <k1> [, <k2> …] LIMIT n [OFFSET o];
```

---

## 0. Summary

A Top-N query needs only the n best rows. Once n matching rows have been seen, the n-th
best leading-key value is a **boundary**: no row whose leading key is strictly worse can
reach the result. Row-group footers already carry the leading key's min/max. So a row
group whose best possible value is worse than the current boundary can be skipped
without being fetched or decoded.

Today we compute that boundary once, after the scan has read everything:

- **Q24** (two-pass latmat scan): pass 1 decodes URL + EventTime for all 1584 row groups;
  `reduce_to_topn` finds the boundary only after pass 1 has drained. Pass 1 is ~500 of
  ~520 ms of wall time.
- **Q25 / Q27** (single-pass native scan → `TopNSink`): the scan hands ~13M surviving rows
  to the sink, which keeps 10.

The proposal: **the component that sees surviving rows maintains a running boundary, and
the scan checks it before submitting each fetch block.** The boundary only ever tightens,
so a stale read skips less, never wrongly. That removes the need for any barrier, lock or
handshake between the producer and the scan.

Measured potential on `scratch/hits_rugo_262k` (§5): with the real submission order and
the real 18-unit in-flight window, **89.6% of row groups skipped for Q24 and 93.5% for
Q25/Q27.** No wall-clock number is claimed; nothing has been built.

Decisions for the architect are collected in §8.

---

## 1. Why the existing mechanisms do not cover this

| Mechanism | When it decides | Why it misses this shape |
|---|---|---|
| `TopNManifestPruningStrategy` | plan time, file level | Refuses any scan with a predicate: a filtered scan's surviving row count is unknown at plan time, so a threshold derived from `record_count` drops files holding the only survivors (its own docstring measures the wrong answers). |
| Runtime min/max join filter | once, in `make_global()` | Relies on the serial pipeline barrier: the build side is complete before the probe scan starts. Here the producer (the Top-N sink) runs **concurrently** with the scan, in the same pipeline. There is no barrier, and a one-shot check at scan start sees no boundary at all. |
| `reduce_to_topn` (latmat) | once, after pass 1 | Correct, but runs after pass 1 has already read every row group. |
| `TopNSink::compact` | every `max(4n, 65536)` buffered rows per worker | Q24 delivers ~10 survivors per row group, so a worker never reaches 65,536 rows and never compacts mid-scan. A boundary published from compaction would never be published for the query that needs it most. |

So this needs a **new producer** (a boundary tracker that is cheap enough to run per
morsel) and a **new consumption point** (the scan's per-submission loop rather than
`make_global`). The value type, the ordinal space and the stat converter are reused
unchanged.

---

## 2. The correctness argument

Notation, ascending on leading key `k1` (descending is the mirror, §2.3):

- `B` = the n-th smallest **non-null** `k1` among rows that have already been delivered to
  the Top-N (as sink input, or as pass-1 survivors on the latmat path).
- `ord(·)` = draken's ordinal (`draken/ops/ordinalize.h`). For every type admitted in §6
  it is strictly order-preserving, so `ord(a) > ord(b) ⇔ a > b`.

### 2.1 Rows

**Claim.** Any row `r` with non-null `r.k1 > B` cannot be in the final top n.

At least n delivered rows have `k1 ≤ B < r.k1`. `SortKeyCmp` (`draken/morsels/sort.hpp:241`)
decides on the leading key first, and between two non-null values the smaller one comes first
under ASC. So all of those rows strictly precede `r`, whatever the later keys are and however
ties are broken. At least n rows strictly precede `r`, so it cannot be one of the first n.

This holds for **multi-key** sorts (Q27): the argument only uses the leading key, and it uses
it strictly. A row tied with `B` on `k1` is **not** covered and is never skipped.

### 2.2 Row groups

A row group can be skipped iff every row in it is covered by §2.1 or is a NULL that is also
excluded:

```
skip(rg)  ⇔  stats present
          ∧  ord(rg.k1.min) > ord(B)                       -- strict
          ∧  (nulls_last  ∨  rg.k1.null_count is known and == 0)
```

**NULLs.** Placement is per key and independent of direction (`sort.hpp:26-28`; the planner
resolves the SQL default to "NULL is the lowest value": NULLS FIRST under ASC, NULLS LAST under
DESC).

- **NULLS LAST:** a NULL `k1` sorts after every value, so it is strictly worse than a non-null
  `B`. NULL rows never block a skip.
- **NULLS FIRST:** a NULL `k1` precedes every value, so a NULL row beats `B`. A row group can be
  skipped only when its footer proves it holds no NULLs. A missing `null_count` keeps the row
  group.

**The tracker counts non-null rows only**, and that is sound under both placements:

- Under NULLS LAST, NULLs are the worst rows, so the n best non-null values are exactly the
  top n, or the boundary is not yet valid.
- Under NULLS FIRST, NULLs take the best slots, so the true n-th best row is at least as good as
  the n-th best non-null value. A boundary computed without NULLs is therefore looser than the
  true one, which only means skipping less.

The one case this gives up on is n or more NULLs, where the true boundary is itself NULL.
The tracker then simply never becomes valid (fail open).

### 2.3 Descending

Mirror everything: `B` is the n-th **largest** non-null `k1`, and the test is
`ord(rg.k1.max) < ord(B)`. The NULL rule depends only on `nulls_first`, never on direction.

### 2.4 Concurrency: why no barrier is needed

`B` only ever tightens: it decreases under ASC and increases under DESC, because the tracker
only ever admits better values. A scan thread that reads an out-of-date `B` is testing against
a looser boundary, so it skips a **subset** of what the current value would skip. Every
reader, at any moment, is therefore sound. The shared value is one `std::atomic<int64_t>`
plus a valid flag:

- **publish:** a CAS loop that installs the new value only if it is strictly tighter
  (`min` for ASC, `max` for DESC, in ordinal space). Workers publish only when their local
  boundary beats the global one, which is rare after the first few morsels.
- **read:** one relaxed load per submission batch, under the scan's existing `g.mtx`.

This is the key contrast with the join filter (`RUNTIME_MINMAX_FILTER_DESIGN.md` §4.1), where
completeness came from the barrier. Here completeness is **not needed**: a partial boundary is
still a valid boundary.

### 2.5 What must stay true

1. **Necessary-condition test only.** A kept row group is read and sorted exactly as today.
   The downstream `TopNSink` and `reduce_to_topn` still make the canonical cut, so the
   result is the same rows the un-pushed plan would produce.
2. **Absence is free.** No boundary yet, no stats, a type outside §6, or an unconvertible stat
   all keep the row group. This is `valid == 0` from `runtime_bound.hpp`, unchanged.
3. **Same column, same space.** The boundary's ordinal and the footer's ordinal must describe
   the same values. Where the decoder rewrites a value, the compiler refuses (§6, condition 4).
4. **The producer only sees rows that will definitely reach the Top-N.** In v1 that holds
   because the Top-N reads directly from the scan (§4.1).

---

## 3. The producer: a boundary tracker

### 3.1 Shape

Per worker: a bounded max-heap (ASC) of the n best non-null leading-key ordinals seen so far,
plus its current worst element. On each incoming morsel:

1. Ordinalize the leading-key column for that morsel's rows (§3.3).
2. For each non-null ordinal: if the heap has fewer than n entries, push; else if it is
   strictly better than the heap's worst, replace. Almost every row fails this one compare
   after the first few morsels, so the loop is branch-predictable.
3. If the heap holds n entries and its worst is tighter than the global value, CAS-publish.

Memory is O(n) per worker, the same order as the `TopNSink`'s own candidate set.

### 3.2 Where it runs

| Path | Producer | Threads | Consumer |
|---|---|---|---|
| Single-pass parquet scan → `TopNSink` (Q25, Q27) | `TopNSink::sink`, per worker | every exec worker | `NativeParquetScanSource::get_morsel` submit loop |
| Parquet latmat (Q24) | `LatmatScanSource::run_pass1`, per result | **one**: pass 1 runs on the first worker, under `g.mtx` | the same `run_pass1` submit loop |
| Skene single-pass → `TopNSink` | `TopNSink::sink` | every exec worker | `NativeSkeneScanSource` claim loop |
| Skene latmat | `NativeSkeneLatmatScanSource` pass 1 | one | its pass-1 claim loop |

On the latmat paths the producer and consumer are the **same thread**, so the boundary is a
local variable of `run_pass1`, with no atomic and no engine slot. The tracker there consumes
pass-1 survivors, which are exactly the rows `reduce_to_topn` will later see, so §2.5(4) holds
by construction. `reduce_to_topn` and pass 2 are unchanged: a skipped row group simply
contributes no survivors.

### 3.3 Ordinalizing a morsel column

The tracker must produce draken ordinals, because that is the space the footer converter
(`src/cpp/engine/parquet_stat_ordinal.hpp`) and skene's `min_ordinal`/`max_ordinal` are in.
`sort_num_key` (`sort.hpp:116`) is a *different* order-preserving mapping, a uint64 with a sign
flip. Translating one into the other would be the second dialect that
`ordinal_zone_map_terms` warns against.

The engine cannot include `ops/hash.h` (its dispatch table is `static inline`; that is why
`cxx_ordinal_bounds_c` exists). So the tracker needs a draken C-ABI seam. The two options are
Decision D3:

- **(a)** `cxx_ordinal_c(m, col, int64_t* out, n)`: per-row ordinals into a per-worker
  reusable scratch buffer, with the heap kept in the engine.
- **(b)** `cxx_ordinal_topn_c(m, col, n, ascending, int64_t* heap, uint32_t* heap_len)`: a
  fused bounded selection over non-null rows into caller-owned heap state. No scratch buffer,
  and the §11-shape handling (dict: ordinalize `data_length` values, then gather) stays
  inside draken.

Recommendation: **(b)**, for the same reasons D5 chose a fused seam for the join filter.

---

## 4. The consumer: the submit-time check

### 4.1 Parquet single-pass (`native_parquet_scan_source.hpp:1013`)

The submission loop already walks fetch blocks under `g.mtx`. Add, per unit, before it joins a
submission:

```
if boundary.valid() and excluded(unit, boundary.load()): mark skipped; continue
```

`excluded` is §2.2 using `stat_bytes_to_ordinal` over the unit's `RowGroupStats`, the same
call `apply_runtime_bounds` (`:912`) already makes.

**Accounting invariant (the sharp edge).** Today the loop treats every unit up to
`next_to_submit` as one result that will come back: `results_received` counts claims against
it, and the scan is FINISHED when `results_received >= next_to_submit`. A skipped unit
produces no result. Skips must therefore be counted separately, with both the window test and
the finish test using `submitted = next_to_submit - skipped`. If they are not, the scan either
waits forever for results that will never arrive, or finishes early with results still in
flight. That second failure silently loses rows.

**Block granularity.** A kept block with skipped members is still one submission of its
remaining members, so coalescing is unchanged. With grouped column-major files (blocks of 4)
the saving is at row-group granularity for decode and at block granularity for bytes fetched.

**Composition.** Runtime join bounds (`apply_runtime_bounds`) still run once in
`make_global`, and this check runs per submission over what they kept. The two are ANDed.
`limit_submit_cap` only applies to scans with a pushed LIMIT and no predicate, and it is
unaffected.

### 4.2 Parquet latmat (`native_latmat_scan_source.hpp:279`)

`run_pass1` has the same shape of submit loop (`submitted` / `received` / `in_flight_limit`)
and needs the same skip counter. The tracker is updated from each result's pass-1 morsel at
`sort_p1_index`, restricted to survivor positions, before `take_rows`.

### 4.3 Skene

Claims are handed out by `next_claim.fetch_add` (`native_skene_scan_source.hpp:1272`) from a
list built once under `call_once`. The check goes at claim time: a claim whose row groups are
**all** excluded is skipped and the worker takes the next index. A v3 claim is one fetch block
with a precomputed fetch plan, so v1 skips whole claims only and does not trim members. Stats
are already ordinals (`min_ordinal` / `max_ordinal`), so no converter is needed.

### 4.4 In-flight lag

A unit is checked when it is submitted. Units already submitted are not recalled. With
`in_flight_limit` = workers + 2 = 18 locally, the boundary a submission sees is roughly 18
units old. §5 simulates exactly this.

---

## 5. Measured potential

Method: for every row group of `scratch/hits_rugo_262k` (99 files, 1584 row groups of 64k), the
10 smallest EventTime among rows matching each predicate were computed with pyarrow on the dev
side. Admission was then replayed in two orders, with a completion lag of L units: a unit's
survivors only count toward the boundary L units after it was submitted. A unit is skipped when
its footer EventTime min > boundary. EventTime is plain INT64 with no logical type, and every row
group has `null_count = 0`.

The real order is the scan's actual submission order, taken from a native trace of Q25.

| Query | Order | lag 1 | **lag 18** | lag 64 | lag 256 |
|---|---|---|---|---|---|
| Q24 (`URL LIKE '%google%'`) | real | 89.9% | **89.6%** | 88.1% | 77.2% |
| Q24 | by min | 91.9% | 91.9% | 91.9% | 83.4% |
| Q25 / Q27 (`SearchPhrase <> ''`) | real | 94.8% | **93.5%** | 92.0% | 80.9% |
| Q25 / Q27 | by min | 96.9% | 96.3% | 94.8% | 83.8% |

Upper bound (the final boundary checked against every footer): Q24 1456/1584 (91.9%), 72 of 99
files entirely. Q25 1535/1584 (96.9%), 79 of 99 files.

What this says:

- **The in-flight lag costs almost nothing** at the real window (0.3 to 1.3 points).
- **Reordering by min adds 2 to 3 points on this dataset**, because the files are already
  written in roughly time order. That is a property of hits, not of the design. If the listing
  order were reverse-chronological, the real-order column would collapse and reordering would
  be the whole win. This is why reordering is a separate decision (D5), not dropped.
- **Q26 gets nothing.** It sorts by SearchPhrase, and nearly every row group's minimum is `''`,
  which the WHERE removes but the footer counts. Skipping it would need a dictionary-page test
  against both the predicate and the boundary. That is a different mechanism and out of scope.
  We are already at 0.94× DuckDB on Q26.
- **Everything depends on the data being clustered by the sort key.** A table whose layout is
  unrelated to the key has every row group spanning the full range, and nothing is skipped. That
  is the same precondition `RUNTIME_MINMAX_FILTER_DESIGN.md` §5.1 found for joins. Event and log
  tables written in time order are the everyday case this targets.

Not measured: wall time, tracker cost, and any other suite. TPC-H's LIMIT queries all sort
on aggregate or join output, and JOB has no LIMIT queries, so neither can arm this.

---

## 6. Types and eligibility (all decided in the compiler)

Arm only when **every** condition holds; otherwise wire nothing, and both sides stay
byte-for-byte as they are today.

1. The HeapSort reads directly from the scan: the scan carries the Top-N stamp from
   `TopNScanPushdownStrategy`. That strategy already requires the HeapSort to read straight
   from a single Scan. It is what guarantees §2.5(4): no join, no aggregate and no filter node
   between them, so every row the sink sees came from this scan unchanged.
2. The scan's pipeline is the sink's pipeline: `set_topn_sink(p, …)` with `p` present in
   `parquet_scan_pipelines` / `skene_scan_pipelines`. On the latmat path this is internal and
   always true.
3. The leading key is a direct column of the scan's read set, resolved by identity to its
   physical name, the same resolution `_wire_runtime_bounds` does.
4. The column's decode does not rewrite values, so decoded values and footer stats describe
   the same numbers (§2.5(3)). *[corrected in build]* Concretely: `logical_coerce` is 0,
   except DATE32's `LC_DATE`, which is value-preserving (a 32-bit DATE is only retagged; a
   64-bit one is cast to 32 bits, keeping the day count). Lossless integer widening is also
   value-preserving and is admitted, with the one exception in 5. The classifier's
   `widen_types` is the declared width of every integer column, not a flag that widening
   happens, so it is not a usable refusal signal.
5. Type allow-list: `INT8/16/32/64`, `UINT8/16/32`, `DATE32`. Refused, with reasons:
   - `UINT64` *[corrected in build]*: a file may store the column narrower than declared
     and the Source widens it losslessly. For every other integer type both the footer
     statistic and the decoded value ordinalize by a plain widen, so they agree. UINT64
     alone ordinalizes through a sign-bit bias (`ordinalize_scalar_u64`) while a narrower
     unsigned statistic is only widened, so the two would sit in different spaces and the
     test would skip real answers. Admitting it needs the consumer to know the declared type.
   - `FLOAT32/64`: NaN ordering. `sort_num_key` puts NaN highest, while parquet keeps NaN out of
     min/max. That is the `_nan_invisible_to_bounds` problem, and `TopNManifestPruningStrategy`
     refuses floats for the same reason.
   - `TIMESTAMP64`, `TIME32/64`: unit lives in the logical type, and decode may rescale.
     Admissible later if condition 4 is proven to cover it.
   - `DECIMAL`, `DECIMAL128`: scale and missing kernel, as in the join filter's D6.
   - Strings: parquet has no string stat converter (`parquet_stat_ordinal.hpp` v1 limits). Skene
     could use its prefix ordinal, which is sound because it is monotone, but no measured query
     benefits (Q26, §5).
   - `BOOL`: admissible, pointless.
6. `1 ≤ n ≤ 2³²−1` (the tracker counts in uint32). *[corrected in build]* OFFSET **is**
   covered: `LIMIT l OFFSET o` plans as `HeapSort(l + o)` under a Limit that drops the first
   o rows, and the boundary is built on the HeapSort's own n, so it stays sound.

**Multi-key (Q27).** `single_physical_column_topn` (`topn_pushable.py:34`) refuses more than one
key, so Q27's scan is never stamped today, and condition 1 would refuse it. The boundary only
needs the leading key (§2.1). Covering Q27 means stamping the leading key separately from the
single-key latmat spec. That is Decision D4.

---

## 7. Telemetry, switch and tests

**Telemetry** follows the join filter's D9:

- `row_groups_pruned_topn`: this mechanism's **marginal** skips, counted after plan-time
  and join-bound pruning. It is **absent**, not zero, when nothing was armed.
- A plan-time `topn_boundaries_armed` count, so "never armed" is distinguishable from "armed,
  skipped nothing".
- On latmat, pass-1 row groups submitted versus skipped, so the Q24 saving is visible directly.

**Switch:** session variable `disable_topn_runtime_boundary`, with an environment twin, following
`disable_runtime_minmax_join_filter`.

**Correctness gates**, before any timing:

- **On/off oracle.** Q24, Q25 and Q27, plus synthetic tables, run with the feature armed and
  disabled. Compare the **sort-key columns of the result**, which are deterministic even when the
  row chosen among ties is not, and the full rows wherever the boundary value is unique.
- **Matrix:** ASC/DESC; explicit NULLS FIRST/LAST with NULLs in some row groups, all-NULL row
  groups, and n or more NULLs; row groups without statistics; ties at the boundary spanning row
  groups; LIMIT larger than the match count; a predicate matching nothing; UINT64 values at or
  above 2^63; INT32; DATE32; multi-key with the leading key tied across row groups.
- **Refusals asserted not armed:** FLOAT, TIMESTAMP, DECIMAL, string leading key, coerced column,
  a Filter node between scan and HeapSort. (OFFSET moved to the oracle set: it arms, see §6.)
- **Accounting:** a scan where every unit after the first block is skipped must finish,
  and must not hang or return early.
- `make q` green, and `tests/integration/test_runtime_minmax_join_filter.py` still green, because
  it shares the submit path.

**Performance:** baseline before the first edit. Use interleaved A/B with alternating arm
order, reporting Q24, Q25 and Q27 separately, plus the full ClickBench total as the regression
check for the tracker's cost on queries that arm and skip nothing.

---

## 8. Decisions

| # | Decision | Recommendation |
|---|---|---|
| **D1** | Proceed at all. The win is on three ClickBench queries and on real "latest N matching X" workloads, contingent on the data being clustered by the sort key. It is worth nothing on TPC-H and JOB. | Proceed. |
| **D2** | v1 scope. | **Parquet single-pass + parquet latmat** (Q24, Q25, and Q27 via D4), since the measured dataset is parquet. Skene single-pass and skene latmat as a follow-on with the same producer and a claim-time consumer. |
| **D3** | Draken seam for per-morsel ordinals: per-row `cxx_ordinal_c` + engine heap, or fused `cxx_ordinal_topn_c`. | **Fused** (§3.3). New draken C-ABI surface either way. |
| **D4** | Multi-key sorts: stamp the leading key on the scan independently of `single_physical_column_topn`. | **Yes**, needed for Q27. The latmat single-key spec stays as it is. |
| **D5** | Reorder row groups (fetch blocks) by leading-key min/max so the boundary tightens first. | **Defer.** On hits it adds 2 to 3 points (§5). It matters when listing order is unrelated to the key, and it touches IO locality and block coalescing. Measure separately. |
| **D6** | Type allow-list (§6). | Integers and DATE32 only in v1. *As built: UINT64 also refused (§6.5).* |
| **D7** | NULL policy: the precise per-row-group rule (§2.2), or `TopNManifestPruningStrategy`'s precedent of refusing any column with NULLs. | **Precise rule.** Footers carry `null_count`, a missing count keeps the row group, and NULLS LAST needs no count at all. |
| **D8** | Row-level use of the same boundary: drop rows worse than `B` in the scan or sink before buffering. | **Out of scope.** Separate change, separate measurement. The abandoned `FILTERING_HEAPSORT_DESIGN.md` is a warning that per-row savings here are easy to mispredict. |

---

## 9. Code map

| Piece | Where |
|---|---|
| Sort comparator, NULL placement | `draken/morsels/sort.hpp:26-28`, `:241` |
| Top-N sink (producer, single-pass) | `src/cpp/engine/native_sort.hpp:159` (`TopNSink`) |
| Latmat pass 1 (producer + consumer) | `src/cpp/engine/native_latmat_scan_source.hpp:279` (`run_pass1`) |
| Parquet submit loop (consumer) | `src/cpp/engine/native_parquet_scan_source.hpp:1013` (`get_morsel`) |
| Existing one-shot runtime pruning | same file, `:912` (`apply_runtime_bounds`) |
| Skene claim loop (consumer, follow-on) | `src/cpp/engine/native_skene_scan_source.hpp:1272` |
| Stat → ordinal | `src/cpp/engine/parquet_stat_ordinal.hpp` |
| Bound carrier (reuse or sibling) | `src/cpp/engine/runtime_bound.hpp` |
| Existing draken ordinal seam | `draken/morsels/cxx_ordinal.h` |
| HeapSort compile, sink wiring | `opteryx/managers/execution/compiler.py:2497` |
| Scan pipeline maps | `compiler.py:895-902`, `:4292`, `:4384` |
| Join-filter wiring and type gate (pattern to mirror) | `compiler.py:5802` (`_runtime_bound_type_ok`), `:5851` (`_wire_runtime_bounds`) |
| Top-N stamp on the scan | `opteryx/planner/optimizer/strategies/topn_scan_pushdown.py` |
| Single-key restriction | `opteryx/connectors/capabilities/topn_pushable.py:34` |
| Plan-time file pruning (unchanged, independent) | `opteryx/planner/optimizer/strategies/topn_manifest_pruning.py` |

---

## 10. What landed (v1, 2026-09-27)

### 10.1 The mechanism, end to end

| Piece | Where |
|---|---|
| Fused bounded selection over draken ordinals (D3) | `cxx_ordinal_topn_c`, `draken/draken_native.cpp`; declared in `draken/morsels/cxx_ordinal.h` |
| Boundary value (one relaxed atomic, CAS-tighten) + tracker | `src/cpp/engine/topn_boundary.hpp` (new) |
| The parquet row-group skip test (§2.2), shared by both consumers | `topn_excludes_row_group`, `src/cpp/engine/native_parquet_scan_source.hpp` |
| Producer, single-pass | `TopNSink::sink` → `TopNLocal::tracker`, `src/cpp/engine/native_sort.hpp` |
| Consumer, single-pass | `NativeParquetScanSource::get_morsel` submit loop + `submit_block` (skip flags, `topn_skipped` accounting) |
| Producer + consumer, two-pass | `LatmatScanSource::run_pass1`, `src/cpp/engine/native_latmat_scan_source.hpp` |
| Engine slots and wiring (fail loud on mismatch) | `Engine::new_topn_boundary` / `arm_topn_sink_boundary` / `add_parquet_topn_boundary` / `arm_latmat_topn_boundary`, `engine.hpp` |
| Bindings | `NativePlan.arm_parquet_topn_boundary` / `arm_latmat_topn_boundary` / `topn_boundary_skipped`, `opteryx/operators/_operators.pyx` |
| Leading-key stamp, any key count (D4) | `ScanStep.topn_boundary_key` (`plan_steps.pyx`), set by `TopNScanPushdownStrategy` |
| Eligibility (all of it) | `_Compiler._arm_topn_boundary` / `_topn_boundary_type_ok`, `compiler.py` |
| Telemetry | `row_groups_pruned_topn` per scan (absent when not armed), `topn_runtime_boundaries_armed` per plan — `_fold_skene_scan_facts`, `plan_telemetry.py` |
| Switch | `disable_topn_runtime_boundary` (USER / UNRESTRICTED) / `DISABLE_TOPN_RUNTIME_BOUNDARY` (env) |
| Gate | `tests/integration/test_topn_runtime_boundary.py` (53 cases) |

### 10.2 Corrections the build forced

- **UINT64 is refused** (§6.5): the widen asymmetry between `ordinalize_scalar_u64` and a
  narrower unsigned statistic.
- **OFFSET is covered** (§6.6): `HeapSort(l + o)` under a Limit.
- **`widen_types` is not a refusal signal** (§6.4): it is the declared width of every integer
  column. The first build refused every integer key because of this, and it was caught
  because the smoke test showed "armed" absent.
- **The single-pass lag is `window + dop`, not `window`.** Every execution worker claims a
  result before the first one returns, and each claim advances the submit frontier. So about
  18 units locally are in flight before any boundary can exist, which is the lag §5
  simulated. The two-pass producer runs on one thread, so its lag is the window alone.
- **Descending over ascending-clustered data skips nothing.** The answer is in the last row
  groups, and they are walked in file order. This is the D5 reordering case, deferred, not
  a defect. It is covered for correctness by the oracle tests.
- **A zero in-flight window now fails loud in pass 1.** The new drain loop ends when nothing
  is owed, which would silently return an empty pass 1. The old loop would have hung instead.

### 10.3 Correctness, as verified

- ClickBench Q24, Q25 and Q27 sort keys are checked against an independent pyarrow ground
  truth, and are identical with the boundary on and off. Q27's full rows are identical. Q25's
  rows differ only within the six-row tie at the 10th EventTime, which is unspecified.
- `test_topn_runtime_boundary.py`: 53 cases, stable over six repeated runs. They cover both
  paths; ASC and DESC; NULLS FIRST and LAST with scattered and whole-row-group NULLs; ties
  across row groups; LIMIT beyond the match count; zero matches; no statistics; INT16, INT32,
  INT64 negative, UINT32 above 2³¹, and DATE32; multi-key; OFFSET; refusals for FLOAT64,
  TIMESTAMP, VARCHAR, DECIMAL, UINT64, a computed key and the switch; and positive controls
  with skip counts and telemetry.
- `make q` green. Full serial suite with `VALIDATE_OPTIMIZER_PLANS=1`: 39 failures, every one
  in the 2026-09-26 pre-existing baseline categories, with no new failure.

### 10.4 Measured

Interleaved A/B on the same build, 5 rounds, arm order alternating, `hits_rugo_262k`, M5 Pro.
The per-query minimum is shown:

| Query | off | on | |
|---|---|---|---|
| Q24 | 556.8 ms | 90.2 ms | 0.16× |
| Q25 | 119.6 ms | 20.9 ms | 0.17× |
| Q27 | 125.8 ms | 19.6 ms | 0.16× |

⛔ **That run overlapped another session's `make c`** (load average 16). The three rows
above are 5× to 6× effects and survive that noise. The rest of the suite in that run showed
±47% arm-to-arm swings on queries that cannot arm, where both arms execute identical code.
Those swings are contamination, not a signal, so no suite-level cost figure is claimed from
that run. A quiet-machine rerun is recorded in §10.5.

### 10.5 Quiet-machine rerun

Same method, on a quiet machine: load average 1.0 at the start, and no compiler process
seen during the run (polled every 2 s). This build also contains another session's
concurrent changes, and the new tests and `make q` were re-run green on it first.

| Query | off | on | on / off | DuckDB (local calibration) |
|---|---|---|---|---|
| Q24 | 531.4 ms | 75.7 ms | **0.14×** | 147 ms |
| Q25 | 126.4 ms | 19.7 ms | **0.16×** | 42 ms |
| Q27 | 128.0 ms | 19.6 ms | **0.15×** | 33 ms |
| Q26 (cannot arm, string key) | 102.3 ms | 102.7 ms | 1.00× | 100 ms |
| **Suite, sum of minimums** | **9937.8 ms** | **9368.6 ms** | **0.943×** | 10654 ms |

- All three targets move from 2.8×–3.8× behind DuckDB to ahead of it: Q24 0.52×,
  Q25 0.47×, Q27 0.59×.
- **No measurable cost on queries that cannot arm.** For all 40 of them both arms execute
  identical code, since the switch is only read when a HeapSort sits on a parquet scan, and
  the arm-to-arm spread was −3% to +5%. That spread is the noise floor, not a cost. The
  tracker's per-row cost exists only on armed queries, and there it is inside a 6× saving.
- Row groups read, of 1584 (from the scan telemetry), against the §5 simulation at lag 18:
  Q24 170 read vs 165 simulated; Q25 153 vs 103; Q27 150 vs 103. The two-pass Q24, with
  one producer thread, matches. **Q25/Q27's gap is unattributed.** Two candidates, neither
  measured: (a) on the single-pass path the lag is the default window (workers + 2 = 18)
  *plus* one claim per worker, about 34 units rather than the 18 simulated, and the
  simulation at lag 64 reads 127; (b) each worker publishes its own local n-th best, from
  whichever row groups completed on that worker, which is looser than the simulation's
  global boundary until a worker sees the early row groups. Closing it is D5-adjacent
  (ordering and admission) work, not a correctness question.
