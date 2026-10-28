# ORC top-N late materialisation — `WHERE … ORDER BY … LIMIT n` over a wide projection

**Status:** BUILT 2026-10-28 (uncommitted) with D1–D5 as recommended; all suites green; NOT yet
benchmarked. What was built is in §7.
**Date:** 2026-10-28
**Trigger:** ClickBench Q23 on ORC, the largest single gap left after the ORC programme
(docs/ORC_READER_DESIGN.md §6.3–§6.7):

```sql
SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
```

| | ORC | Parquet (`hits_rugo_262k`, same rows) |
|---|---|---|
| Q23, min of 5 | 2,545 ms | 34 ms |
| Over the 2x target | 2.48 s of the 3.4 s still needed | |

## 1. What happens today (measured 2026-10-28, one run)

- Plan: `HeapSort ← Filter ← ORC Reader`. The ORC connector declines predicate pushdown, so
  the Filter stays in the plan, and `TopNScanPushdownStrategy` (HeapSort directly over a Scan)
  never stamps the scan. The compiler arms the ORC two-pass prefilter (§6.6), so the Filter's
  work runs in the scan.
- Pass 1 decodes `URL` for all 100M rows: 15,911 rows survive. Taken alone
  (`SELECT COUNT(*) … WHERE URL LIKE '%google%'`) that is ~0.5 s.
- Pass 2 decoded **35.2M rows × 104 columns** (`orc_pass2_rows`): survivors are scattered, and
  each 60,000-row range decodes the span from its first survivor's row group to its last.
- The HeapSort receives 15,911 full-width rows and keeps 10.

So ~1.85 s of Q23 is decoding 104 columns for rows that cannot be in the answer. Parquet
avoids exactly this with R3 (`fused_topn`, native_latmat_scan_source.hpp): pass 1 reads the
predicate columns plus the sort key, the survivors are cut to the top-n boundary across the
whole scan, and pass 2 reads the other columns for only those rows. Skene has the same shape
(native_skene_latmat_scan_source.hpp).

## 2. Proposal

Give the ORC prefilter path a **top-N mode**, armed at plan time when the shape is
`HeapSort(LIMIT n, no OFFSET) ← Filter ← OrcRead` and the prefilter is armed:

1. **Pass 1, every stripe, in parallel** (the decode pool, as today): decode the predicate
   columns **plus the ORDER BY key columns** (all of them, see D4); run the predicate; keep the
   survivors' pass-1 columns and their `(file, stripe, row)`. Emit nothing.
2. **Bounded as it goes:** each decode worker keeps only its running best `n` (plus ties): when
   its buffer passes `max(4n, 65,536)` rows it compacts it to the rows not strictly worse than
   its own n-th best. This is `TopNSink::compact`'s rule, with draken's `build_sort_keys` /
   `SortKeyCmp` — the comparator the HeapSort itself uses, so NULL and NaN ordering and ties are
   right by construction and no ordering rule is written here (D3).
3. **Barrier:** when every stripe's pass 1 is done, merge the workers' buffers and apply the
   same rule once more: keep every row not strictly worse than the n-th best. That is at most a
   few dozen rows (n plus ties).
4. **Pass 2, only the stripes holding a kept row:** for each, a `StripeCursor` over the other
   columns reads **the row group of each kept row** (a run per row group, seeking by the row
   index, §6.5), takes those rows (`cxx_take_c`), and zips them onto their pass-1 columns.
   These morsels are the scan's output.
5. The HeapSort stays in the plan and makes the final cut over ≤ n + ties rows; the scan's cut
   is a superset of the answer, never the answer. This is the same safety argument as R3: a row
   is dropped only if the predicate fails (the prefilter already owns that, §6.6) or it is
   strictly worse than the n-th best surviving key, which the HeapSort would have dropped.

Planning stays in Python and reads the plan above the scan, as `_skene_latmat_consumers` does —
no optimizer strategy changes, and the Filter is already absorbed by the prefilter.

## 3. Expected effect (estimates, not measurements)

- Pass 1: the `URL` decode + LIKE that Q20 already costs (480 ms on ORC), plus `EventTime`
  (~10% more).
- Barrier and reduction: 15,911 survivors in total, negligible.
- Pass 2: ≤ 10 row groups (≤ 100,000 rows) × 104 columns instead of 35.2M rows. Each seek
  decompresses about two 256 KiB chunks per column, so the worst case is ~500 MB of zstd spread
  over the pool: tens of milliseconds.
- **Q23 ≈ 0.55 s**, saving ~1.9 s. Summed ClickBench ≈ 10.4 s against Parquet's 4.46 s ⇒
  **~2.35x**. On its own this does not reach 2x; the rest is Q20–Q22's string decode, which this
  also helps (Q23's pass 1 is the same `URL LIKE` work).
- Q24/Q26 (narrow projections, ORDER BY EventTime) gain little: they decode nothing in pass 2.
  Their gap is pass 1 over 100M rows, which Parquet closes by skipping row groups on EventTime
  min/max as the boundary tightens (TOPN_RUNTIME_BOUNDARY_DESIGN). For ORC that needs
  timestamp statistics, excluded today (§6.7); see §5.

## 4. Where the code goes

| Piece | Where | Size |
|---|---|---|
| Shape detection, key columns added to pass 1, the spec | compiler `_arm_orc_topn` next to `_arm_two_pass_prefilter`; `OrcScanPlan.set_topn(...)` | ~80 lines Python |
| Top-N mode: per-worker buffers, compaction, barrier, pass-2 work list | `NativeOrcScanSource` | ~250 lines C++ |
| Pass 2 per kept row group | `StripeCursor::read` as is (a run per row group) | none |
| The boundary rule over a set of morsels | shared, see D2 | ~60 lines |
| Tests | `test_orc_prefilter.py` style, against pyarrow: ASC/DESC, NULLs first/last, ties at the boundary, NaN keys, multi-key, n larger than the survivors, no survivors, scattered and clustered survivors; telemetry `orc_topn_kept_rows` proving pass 2 read only the kept row groups | |

## 5. Not proposed here

- **A runtime boundary that skips pass-1 row groups** (Parquet's TOPN_RUNTIME_BOUNDARY): needs
  ORC timestamp statistics. Writers since ORC-135 record UTC min/max (`minimumUtc` /
  `maximumUtc`); the older local-time fields are what made timestamps unsafe. Accepting only the
  UTC fields would make it safe. It would help Q24/Q26 rather than Q23, so it is a separate piece.
- **Splitting pass 2 per row group for the ordinary prefilter** (the span cost in §1, for queries
  without a LIMIT): needs a morsel concat draken does not have.

## 6. Decisions for the architect

- **D1 — where it lives.** (a) a mode of `NativeOrcScanSource`, reusing its decode pool, stripe
  claiming, row-index seek and pass-1 programs; or (b) a separate `NativeOrcLatmatScanSource`,
  as Parquet and skene each have. **Recommend (a):** the ORC Source already is two-pass and
  seeks; a separate Source would duplicate both.
- **D2 — the boundary rule.** Parquet's `reduce_to_topn` and skene's are two copies of the same
  rule over draken's comparator. (a) factor the rule (keys over a set of morsels → keep mask)
  into one shared header and have all three call it — touches the Parquet and skene latmat
  Sources; or (b) a third copy for ORC only. **Recommend (a)**, done as its own step and
  verified with their existing suites (`test_wp_r3_latmat_scan.py`, the skene latmat tests)
  before ORC uses it.
- **D3 — bounding pass 1's memory.** (a) per-worker running compaction to n + ties (no gate, the
  same rule as the HeapSort, memory O(n × workers)); or (b) Parquet's plan-time selectivity gate
  (decline when the predicate keeps too much). **Recommend (a):** it needs no estimate and never
  declines, so a weak predicate (`WHERE SearchPhrase <> ''`, 13M survivors) still costs only
  pass 1 plus a small pass 2.
- **D4 — keys.** Every ORDER BY key must be a plain column the scan reads (any number of keys:
  the comparator takes the list), one LIMIT, no OFFSET. Anything else declines to today's
  prefilter. **Recommend as stated.**
- **D5 — width gate.** Engage only when the columns deferred to pass 2 are a material share of
  the projection (Parquet and skene gate on this)? With per-row-group pass 2 the worst case is
  small, so **recommend no gate**; a narrow projection simply has an empty pass 2.

## 7. What was built (2026-10-28)

- **D2 first:** `src/cpp/engine/topn_keep.hpp` (`topn_keep_mask`) is the one top-N cut; the
  parquet and skene latmat Sources now call it (their suites: 58 passed unchanged).
- **D1/D3:** `NativeOrcScanSource` top-N mode. Pass 1 (`decode_filtered` with a worker's
  `TopNBuffer`) pushes nothing; each worker cuts its buffer at `max(4n, 65,536)` rows; the last
  worker to reach the barrier (`topn_barrier`) cuts every buffer and lists one pass-2 item per
  range holding a kept row; `topn_pass2` decodes the other columns for each kept row's row group
  (seek; without a row index, the range) and emits one morsel per row group. Files are reopened in
  pass 2 (a remote file is fetched again — at most n + ties of them).
- **D4:** `compiler._orc_topn_spec`: `HeapSort ← [Projection that only selects columns] ← Filter
  ← OrcRead`, every key a plain emitted column, positive LIMIT. The keys join pass 1
  (`_arm_two_pass_prefilter(..., topn)`); `OrcScanPlan.set_topn`. An explicit SELECT list leaves a
  Projection between the HeapSort and the Filter; one that computes anything declines, since it
  could raise on a row the cut drops.
- **D5:** no width gate.
- Telemetry `orc_topn_kept_rows`. Tests: tests/unit/connectors/test_orc_topn.py (21: ASC/DESC,
  NULLS FIRST/LAST, ties at the boundary, multi-key, string keys, fewer survivors than the limit,
  none, a worker cutting its own buffer, a declined shape).
- ⛔ Found in the build: `vector.push_back(1 if x else 0)` in Cython binds a `__Pyx_FakeReference`
  to a temporary and pushes garbage (the sort flags arrived as 13 / 136). Typed locals fix it.
