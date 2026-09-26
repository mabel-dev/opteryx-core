# Late materialisation for filter + GROUP BY / aggregate shapes — assessment

**Verdict: NO-GO.** Do not extend the latmat Sources to filter + aggregate consumers.
The mechanism the extension would reuse — rugo's masked decode — is not cheaper
than a full decode, and the two-pass Source costs +41% on the one shape with a
meaningful deferred column set. The theoretical ceiling (perfectly free
deferred columns) is ~190ms summed across the whole ClickBench suite, and the
queries originally nominated (2, 8, 21, 22, 23, 37–43) are worth ≤ 20ms each
even at that ceiling.

All numbers below were measured on this checkout, 2026-09-26, Apple Silicon,
Python 3.14, after `make compile`. Datasets: `scratch/hits_rugo_262k`
(parquet, 99 files, 1584 row groups) and `scratch/hits_skene`. Wall times are
min of 3–4 in-process runs (first run discarded as cold). No historical
ClickBench results were used.

## 1. What late materialisation could save here

A filter + aggregate query reads two column sets:

- **P** — predicate columns (must be decoded for every row of every surviving row group);
- **D** — columns needed only after the filter (group keys / aggregate inputs not in P).

Only D can be deferred. For most candidates D is **empty**:

| Q | predicate | D (deferrable) |
|---|---|---|
| 2, 8 | `AdvEngineID <> 0` | — (key = predicate column) |
| 21 | `URL LIKE` | — |
| 22 | `URL LIKE`, `SearchPhrase <> ''` | — (MIN(URL), key SearchPhrase both in P) |
| 23 | `Title LIKE`, `URL NOT LIKE`, `SearchPhrase <> ''` | UserID |
| 37, 38 | CounterID/EventDate/flags + `URL`/`Title <> ''` | — |
| 39 | CounterID/EventDate/IsRefresh/IsLink/IsDownload | URL |
| 40 | CounterID/EventDate/IsRefresh | TraficSourceID, SearchEngineID, AdvEngineID, Referer, URL |
| 41–43 | CounterID/EventDate/… | URLHash / WindowClient* / EventTime |

The queries that actually carry a deferrable set are ones not on the original
list: 11, 12, 14, 15, 28, 31, 32. They are included below.

## 2. Selectivity and pruning (parquet)

| Q | row groups read / pruned | rows in → out | selectivity |
|---|---|---|---|
| 2, 8 | 1101 / 483 | 69.6M → 628k | 0.9% |
| 11, 12 | 1584 / 0 | 99.0M → 5.50M | 5.6% |
| 13–15, 31, 32 | 1584 / 0 | 99.0M → 13.09M | 13.2% |
| 21 | 1584 / 0 | 99.0M → 15.9k | 0.016% |
| 22 | 1584 / 0 | 99.0M → 1.0k | 0.001% |
| 23 | 1584 / 0 | 99.0M → 7.1k | 0.007% |
| 28 | 1584 / 0 | 99.0M → 98.9M | 99.9% (gate declines) |
| 37–43 | **15 / 1569** | 934k → 48k–723k | 5–77% within survivors |

Skene prunes 37–43 to 14 row groups. **The CounterID block is already won by
row-group pruning:** 37–43 run in 6–40ms total on either format, and the whole
deferred set of Q40 (the widest) costs 11ms wall on parquet and 6ms on skene.

## 3. Ceiling: what the deferred columns cost today

`A` = `SELECT COUNT(*) WHERE <pred>` (decodes P only). `B` = same plus
`MIN(d)` for every d in D. `B − A` is the full cost of D: decode, the
filter's gather, and a trivial aggregate. This is the most that *any*
late-materialisation scheme could remove, assuming masked decode were free.

| Q | full query (pq / skene) ms | B − A (pq / skene) ms | ceiling share (pq) |
|---|---|---|---|
| 11 | 86 / 60 | 17.5 / 11.5 | 20% |
| 12 | 99 / 69 | 29 / 19 | 29% |
| 14 | 250 / 209 | 23 / 14 | 9% |
| 15 | 209 / 167 | 17 / 10 | 8% |
| 23 | 538 / 396 | 20 / 13 | 4% |
| 28 | 322 / 109 | 25 / 27 | 8% (selectivity 99.9% — declined anyway) |
| 31 | 233 / 165 | 75 / 42 | 32% |
| 32 | 231 / 168 | 70 / 46 | 30% |
| 39 | 17 / 8 | 5 / 3 | — |
| 40 | 41 / 30 | 11 / 6 | — |

Sum of parquet ceilings ≈ 190ms over a suite of ~13s.

## 4. The mechanism does not deliver the ceiling

### 4.1 Masked decode is decode-then-compact

The existing latmat Source was used as a zero-code prototype: an
`ORDER BY <P column> LIMIT 20000000` (above the survivor count) makes
`TopNScanPushdownStrategy` fuse the top-n, so `_latmat_scan_plan` admits the
scan and pass 2 masked-decodes D for **every** survivor — exactly the
filter + aggregate work pattern. (`LIMIT 1000000000` does not get the hint;
20M does.) Admission was verified by instrumenting `_latmat_scan_plan`.
Selectivity gate lifted with `PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY=1.0`;
the single-pass arm used `FEATURE_PARQUET_LATE_MATERIALIZATION=0`.

Decode cost was taken from the execution trace (`TC_DECODE` spans, summed),
with pass 1 and pass 2 split at the barrier (a 6–21ms gap separates them).
Engine DOP was pinned to 1 (`MAX_EXECUTION_WORKERS=1`): at DOP 16 the
single-pass arm's decode spans are inflated by CPU contention with the engine
workers, while latmat pass 1 runs with the engine parked. That contention
made pass 1 look 35–40% cheaper than the same columns single-pass. Thread-ms,
min of 3:

| D set | selectivity | full decode of D (one-pass − P-only) | masked decode of D (pass 2) |
|---|---|---|---|
| Q12: MobilePhone, UserID | 5.6% | 344 | **404** |
| Q31: SearchEngineID, ClientIP, ResolutionWidth (+IsRefresh as key) | 13.2% | 570 | **599** |
| Q23: UserID | 0.007% | 162 over 1584 RGs | **163 over 812 RGs** |

Masked decode costs the same as or more than a full decode. rugo phase
telemetry (`rugo_native.get_cpp_telemetry`, Q12 column set, DOP 1, thread-s):

| phase | one-pass | two-pass |
|---|---|---|
| decompress | 0.085 | 0.100 |
| RLE | 0.173 | 0.209 |
| value expand | 0.302 | 0.248 |
| mask filter | 0 | **0.085** |

The decoder expands every value and then compacts to the mask (the
post-loop filter in `decode_column.cpp`). It is not selective. So deferring D
trades the engine's `cxx_take` for rugo's own compaction, and nothing is saved.

### 4.2 The two-pass Source is a net loss on this shape

Wall, interleaved A/B, 3 rounds × min of 3, Q31 column set, DOP 16:

| round | two-pass | one-pass |
|---|---|---|
| 1 | 1206 | 873 |
| 2 | 1222 | 856 |
| 3 | 1214 | 851 |

+41%. On top of the non-saving decode it pays the pass-1 barrier (the first worker runs pass 1 under the
global mutex, and every other worker blocks until it finishes), holds every pass-1 survivor across that
barrier, and runs a top-n reduction that a GROUP BY consumer does not need.

### 4.3 Skene has no row mask

`skene::read_morsel` takes no row mask. The skene latmat pass 2 re-decodes the
full projection for each surviving row group, then gathers. For filter +
aggregate, nearly every row group holds survivors, so skene would save nothing
without new reader support. I did not measure a skene masked decode because
none exists.

### 4.4 Predicate cascading (the only lever for 21–23) is decompress-bound

For Q22 and Q23 the only possible late-materialisation variant defers the
*expensive predicate* columns: evaluate `SearchPhrase <> ''` (13%) first, then
decode Title and URL only for its survivors. Phase telemetry for Q23's
predicate columns (DOP 1, thread-s): decompress 3.54, dict parse 0.99, RLE
0.11, value expand 0.14. Decompression is page-granular. With 13% of
survivors scattered across a row group, every page holds one, so no page can
be skipped. The LIKE evaluation itself is small: replacing both LIKEs with
`<> ''` moves the Q23-shape wall from 532 to 484ms. The ceiling is therefore
about 50ms (≤10%), and it needs a new in-decoder mechanism. Not worth it.

## 5. The carried key hash (E37)

- A *deferred* string key decoded under a mask keeps its seed. rugo builds the
  seed at slot build (`io_pipeline.hpp`, `want_seed`) from whichever rows it
  emits, so masked decode would seed only the survivors.
- A *predicate-column* key (SearchPhrase in 13–15, 22, 23) is compacted by the
  filter's `cxx_take`, which drops the seed (E37 §7.1). That happens today
  whether or not latmat exists, and it is fixed by the §7.1 take-carry, not by
  late materialisation. **I did not measure its cost here.**

## 6. What would have to change for this to become a GO

These are recorded so the question is not reopened without them. None is
proposed now.

1. **A selective decoder in rugo.** Value expansion only for masked-in rows:
   gather code → value for dict/RLE, and gather from the decompressed page for
   PLAIN. Measured upper bound: it removes the value-expand share (≈ 0.25–0.30s
   of Q12's ≈ 0.56s thread) and the compaction, while decompress and RLE
   remain. On Q31 that is roughly 20–25ms wall of 233. It helps no existing
   latmat query: Q24's pass 2 already touches only 7 row groups (82 thread-ms
   against 4,078 for pass 1).
2. **No barrier.** A filter + aggregate consumer needs no global boundary, so
   the right shape is per row group inside the rugo decode worker: decode P,
   run `pass1_run_predicate`, then decode D with the survivor mask, all on the
   same worker. This would not reuse `LatmatScanSource`, whose barrier and
   top-n reduction are what cost +41% in §4.2.
3. **Skene row masks** in `skene::read_morsel`, for the skene twin.

Even with all three, the realistic total across the suite is about 100ms,
concentrated in Q11, Q12, Q31 and Q32.

## 7. Side observations (not acted on)

- `LIMIT 1000000000` does not receive the fused top-n hint, while
  `LIMIT 20000000` does. I did not find where the cut-off is applied, and it
  did not affect this assessment.
- The skene scan's `records_in` for 37–43 reports 73.1M rows while
  `row_groups_read` is 14. It looks like a pre-pruning count on the telemetry
  row. Unverified.
- rugo `calls` for the Q23 predicate columns was 4,142, not 3 × 1,584 = 4,752,
  so about 610 chunks were skipped (presumably dictionary skip). Not
  investigated.
