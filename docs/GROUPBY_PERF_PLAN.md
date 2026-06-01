# GROUP BY / Hashed Aggregation Performance Plan

Target: improve ClickBench GROUP BY query performance (q08–q36, 22 active queries).
Scope: `opteryx/operators/grouped_aggregate_hashed/` collectors + engine.
Benchmark: `python dev/groupby_phase_profile.py` (default: scratch.hits ~100M rows).

Status legend: ☐ not started · ◐ in progress · ☑ done · ✗ rejected

---

## Guiding rule

**Profile before committing.** The engine emits per-phase telemetry via
`dev/groupby_phase_profile.py`. Every item below is a *candidate the profile
must confirm*. No "optimization" lands without a before/after measurement on
`scratch.hits`. A change is not complete until `make q` passes.

Items marked **[arch]** are design-impacting — surface to the architect and get
sign-off **before** implementing (CLAUDE.md §3, §11).

---

## Phase 0 — Measurement harness  ☑

Harness: `dev/groupby_phase_profile.py`.
22/28 queries run; 6 skipped (pre-existing `EventDate` type issue in dataset,
unrelated to GROUP BY).

**Baseline (2026-06-01, scratch.hits ~100M rows, --warmup 1 --runs 2):**

| phase        | total ms | % of agg | dominant queries |
|--------------|----------|----------|-----------------|
| accumulate   |  21,936  |  37.4%   | Q29: 15486ms; Q09: 1132ms; Q10: 1681ms |
| lookup       |  19,263  |  32.8%   | Q16: 1200ms; Q17: 1917ms; Q33: 3675ms |
| slice_output |   6,566  |  11.2%   | Q17: 800ms; Q19: 909ms; Q34: 987ms |
| store_keys   |   5,527  |   9.4%   | Q19: 919ms; Q33: 1452ms |
| hash         |   2,238  |   3.8%   | Q17: 256ms; Q19: 365ms |
| reconstruct  |   1,332  |   2.3%   | Q19: 176ms; Q34: 452ms |
| eval_exprs   |   1,067  |   1.8%   | Q29: 746ms; Q19: 274ms |
| grow         |     772  |   1.3%   | Q14: 269ms; Q33: 246ms |

**Key findings:**
- **Q29 alone is 26% of all aggregate time** (15,486ms accumulate) —
  `MinObjectCollector` doing `to_pylist()` + Python comparison every morsel.
- **`lookup` at 33%** is the structural bottleneck for high-cardinality GROUP BY;
  CarcharIndex random probes into tables >> L3 cache at millions of groups.
- **`slice_output` at 11%** and **`store_keys` at 9%** were invisible on tiny;
  both are material at 100M rows for string keys with millions of distinct values.
- Q09/Q10 (COUNT DISTINCT RegionID, ~700 groups): 93–95% in accumulate —
  CarcharSet scatter with full drain scan, dense per-morsel coverage.
- Q14 (COUNT DISTINCT SearchPhrase, ~10M groups): 49% accumulate, 11% grow,
  17% lookup. The full-group drain scan is O(10M) per morsel — see item #1b.

---

## Phase 1 — No design risk

### 1a. Native string MIN/MAX collector  ☐
- **Rationale:** Q29 spends 15,486ms in `MinObjectCollector.accumulate` which
  calls `morsel.column().to_pylist()` then does Python-level min comparison.
  At 100M rows / 65k morsel size = 1526 morsels, that is 1526 full Python list
  materializations. This is 26% of all aggregate time across the suite.
- **No pre-existing shortcut:** `DrakenVector` has no min/max metadata fields
  by design — `buffers.h` line 17 explicitly reserves statistics as out-of-band.
  The `float_min` / `i64_min` ops exist but scan the whole vector; they are
  whole-column aggregates, not cached per-slot metadata. A proper per-group
  accumulator is needed.
- **Fix:** Implement `MinVarcharCollector` / `MaxVarcharCollector` holding one
  `DrakenStringSlot` per group plus a shared arena for long-string payloads.
  Factory resolves to these when the aggregated column is VARCHAR/NVARCHAR.
- **Comparison strategy** (from `string_slot.h`):
  - Short strings (len ≤ 12): payload is fully inline — two 64-bit compares
    (`raw.lo` then `raw.hi`), zero arena access.
  - Long strings (len > 12): `ext.prefix` is the first 4 bytes stored
    big-endian so `prefix_a < prefix_b` gives lexicographic order directly.
    Fast path: compare `raw.lo` (length + prefix) as a single 64-bit integer.
    Arena read (`memcmp`) only when length AND prefix both match — i.e. when the
    candidate string agrees on the first 4 bytes with the current group
    min/max. For URLs and search phrases this is the uncommon case.
  - `str_compare()` already exists in `string_slot.h` but goes straight to
    `str_data()` skipping the prefix fast-path. The collector's inner loop
    should compare `raw.lo` first and only call `str_compare()` on collision.
- Files: `_collectors_numeric.pxi` (new classes), `_factory.pxi` (type dispatch).
- **Measurable impact: Q29 only.** Q22 (agg 2ms) and Q23 (agg 10ms) also use
  string MIN but are filtered by `LIKE '%google%'` / `LIKE '%Google%'` before
  aggregation — IO-dominated, agg improvement would be noise. Q29 has no
  pre-aggregation filter and runs ~90M rows through `to_pylist()` every morsel.
- ☑ Implement · ☑ `make q` 137/137 · ☑ before/after on scratch.hits
- **Result: Q29 wall 18,437ms → 3,966ms (4.6×). Accumulate 15,486ms → 1,071ms (14.5×).**
  Q22 (agg 2.2ms→0.9ms) and Q23 (agg 9.6ms→2.4ms): minimal wall change as predicted.
  Remaining Q29 accumulate (1,071ms) is real slot-comparison work.
  Q29 next bottleneck: eval_exprs 735ms (REGEXP_REPLACE), lookup 515ms.

### 1b. `CountDistinctCollector` drain: post-scatter si_buf pass  ☑
- **Background:** First attempt (Phase 1, rejected) added `empty()` check per
  row in scatter loop → regression for dense-coverage queries (Q09 RegionID
  ~700 groups). The scatter-loop overhead exceeded drain savings.
- **Root cause of original approach:** Any per-row overhead in the scatter loop
  is O(n_rows × num_morsels) = O(100M) — too expensive even if individually
  cheap.
- **Redesigned approach:** Leave the scatter loop untouched. After scatter, make
  one *sequential* pass over `si_buf` (now 256KB with uint32 landed) to collect
  the set of touched group IDs. Then drain only those.
  - Cost added: O(n_rows) sequential si_buf read per morsel.
  - Cost saved: O(num_groups) random scan of `_scratch_per_group` per morsel.
  - Crossover: when `num_groups > n_rows` the si_buf pass wins. Gate with
    `if self._sets.size() > <size_t>n_rows:` — one comparison, no guesswork.
  - For Q09 (700 groups ≤ 65k rows): full scan path taken, no regression.
  - For Q14 (10M groups >> 65k rows): si_buf pass saves scanning 240MB of
    vector headers per morsel.
- ☑ Implement · ☑ `make q` 137/137 · ☑ before/after on scratch.hits
- **Result: Q14 accumulate 1096ms → 817ms (−25%). Wall 2820ms → 2769ms (−2%).**
  Wall barely moves — Q14's remaining bottlenecks are grow (307ms), lookup (430ms),
  slice_output (308ms). The drain was real but not the sole cost.
  Q09/Q10 unaffected (gate: 700 groups < 65536 rows → full-scan path kept).

### 2. `state_indices` buffer: `int64_t` → `uint32_t`  ☑
- Files: `_engine.pxi` (buffer + 4 write sites), `_collectors_base.pxi`,
  `_collectors_numeric.pxi`, `_collectors_distinct.pxi`,
  `_collectors_approx.pxi`, `_collectors_buffered.pxi` — all signatures.
- Local `state_idx` (from carchar int64 API) stays `int64_t`; cast on write.
- Neutral on dev (Apple Silicon 16MB L2); directionally correct for production
  x86 (GCP Cloud Run ~512KB L2).
- ✓ make q 137/137.

---

## Phase 2 — Investigation + design-impacting

### 3. `slice_output` — dense fast-path in `str_slice`  ☑
- **Root cause:** `draken/ops/string_gather.h::str_slice` compact path
  (`data_length > slice_length`) used `std::unordered_map` to deduplicate arena
  offsets across rows in the slice window. For dense vectors (`draken_is_dense`,
  data_length == length — what aggregation key reconstruction always produces)
  every code in the window is unique, so the map gave zero benefit: pure overhead.
- **Fix:** Added dense fast-path before the compact path using `draken_is_dense(&v)`.
  Single sequential scan: slots copied directly, long-string arena bytes appended
  sequentially with inline offset rebasing. No hash map, no two-pass scan.
- **Result:** slice_output −4–11% across Q13/Q15/Q17/Q18/Q19/Q34/Q35.
  Wall −3–9% on most; Q34 −20% (some run variance in reconstruct/lookup).
  Q35 flat. Remaining cost is arena byte scatter-reads (cache-miss-heavy) —
  arena offsets are in hash-table insertion order, not slice order.
- ✓ make q 137/137.

### 4. `store_keys` at high cardinality — single-pass fix  ☑ (partial)
- **Root cause:** `_ks_store_fixed_bulk_dict` used two passes over `row_indices`:
  Pass 1 scanned all n_new entries to detect any null (so the bitmap could be
  pre-allocated), then Pass 2 did the actual store. For non-nullable int64 keys
  (WatchID, ClientIP — the Q33 case), Pass 1 was 100% wasted work: n_new
  scattered index loads per column per morsel, finding nothing.
  Total wasted iterations: 2 × 38.7M (WatchID groups) × 2 columns = ~155M.
- **Fix:** Single-pass with lazy bitmap allocation on first null encountered.
  `_ks_alloc_all_valid_bitmap` initialises all bits to 1, so prior rows are
  automatically valid when the bitmap is lazily created on first null.
- **Result:** store_keys −3–8% across Q17/Q18/Q19/Q33/Q34/Q35. Q33: 1452ms →
  1380ms (−5%). Modest because the remaining cost is scattered random reads into
  `row_indices[]`, `codes[]`, `dict_data[]` — data access pattern, not algorithm.
- ✓ make q 137/137.

**Remaining store_keys opportunities (need architect sign-off, CLAUDE.md §11):**
- Dense int64 shape specialization: for dense vectors, `codes[i] == i` (identity
  selection) — the codes[] load is redundant and can be skipped. One fewer
  scattered load per iteration.
- `_ks_gs_store_bulk_dict` (string path used by Q19): same two-pass pattern,
  same fix applicable.

### 5. `CountDistinct.finalize` builds a Python list  ☑
- `alloc_fixed_buffer(DRAKEN_INT64, num_groups)` → write counts directly →
  `_consume_int64_buffer`. Zero Python objects. Same pattern as numeric collectors.
- **Result: Q14 wall 2769ms → 2482ms (−10%). Accumulate 817ms → 736ms (−10%).**
  Larger than expected — 10M-element list construction + GC pressure was real.
- ✓ make q 137/137.

### 6. Typed morsel + nogil accumulate loops  ☑
- **Problem:** `object morsel` in every `accumulate` signature violated CLAUDE.md §3
  (`object` is forbidden). `morsel.column(self.column_name)` was a Python dispatch
  on every accumulate call.
- **Fix (two parts):**
  1. `object morsel` → `Morsel morsel` across BaseCollector and all 15+ subclasses
     and engine ingest methods.
  2. `morsel.column(self.column_name)` → cached `_col_idx` + `morsel._get_column()`
     (`_get_column` is a typed `cdef` call; index resolved once per query via
     `_column_index_from_name`). `_col_idx` field moved to BaseCollector so all
     subclasses inherit it. Redundant per-class `_col_idx` in CountDistinctCollector
     removed.
  3. All pure-C inner loops wrapped in `with nogil:` (precondition for #7).
- CountStar, CountDistinct and Python-backed collectors (AnyValueObject, ArrayAgg)
  handled correctly as special cases.
- Wall time: no measurable change (column lookup is once-per-morsel regardless;
  `with nogil` is free single-threaded). No regressions.
- Precondition for #7 (parallel aggregation) is met.
- ✓ make q 137/137.

### 7. Partitioned parallel aggregation  ☐  [arch]
- **Rationale:** `lookup` at 33% is random access into a CarcharIndex that
  grows beyond L3 at millions of groups. Partition by hash high bits →
  independent per-partition tables on N threads → merge reduces working set
  per thread and allows parallel execution.
- Real design change. Depends on #6. Scope with architect before any code.
- ☐ Design doc · ☐ Architect sign-off · ☐ Implement

### 8. No-null fast path in numeric `accumulate`  ✗ REJECTED
- Attempted: hoist `if nulls == NULL:` outside all inner loops, providing a
  null-free path and a null-checking path.
- **Result: regression.** Q10 accumulate +37ms (+2%), Q33 accumulate +37ms (+8%),
  confirmed stable over 5 runs. No improvement anywhere.
- **Root cause:** the always-taken `_num_bitmap_valid(nulls, i)` branch (when
  nulls == NULL) is essentially free — branch predictor handles 100%-predictable
  branches in ~0 cycles after warmup. The hoist bought nothing in runtime but
  doubled every loop body, increasing code size and I-cache pressure. Net negative.
- Reverted. ✓ make q 137/137.

---

## Phase 3 — Lower confidence / cleanup

### 9. Collector-pass fusion  ☐
- Multi-agg queries scan `si_buf` once per collector. Fuse same-column
  aggregates into one pass.
- Low confidence on 100M numbers; profile first to confirm `si_buf` read
  cost is material relative to other phases.
- ☐ Confirm with profile · ☐ Surface

---

## Execution order

1. **1a** (native string MIN/MAX) — biggest single win, no risk.
2. **1b** (CountDistinct drain redesign) — no regression risk with threshold gate.
3. Re-measure on scratch.hits after 1a + 1b.
4. **3 + 4** (slice_output + store_keys investigations) — findings inform whether
   these need fixes or are acceptable.
5. **5** (CountDistinct.finalize native buffer) — clean-up, no risk.
6. **6 → 7** (nogil → parallel aggregation) — bring to architect once 1a/1b/3/4
   are resolved; `lookup` cost will be the remaining dominant phase by then.
7. **8 + 9** opportunistically.
