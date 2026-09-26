# GROUP BY scaling limits — merge skew and flush lock (assessment, 2026-09-26)

Two suspected scaling limits in the native hash GROUP BY
(`src/cpp/engine/native_group_sinks.hpp`), measured on the current tree. Neither
turned out to be the limit on hardware we can measure. The measurement found a
third problem, which is: **the partition bits collapse carchar's SIMD tag.**

## Rig

- **Machine:** Apple M5 Pro, 18 cores = **6 performance + 12 efficiency**, 64 GB.
  Python 3.14 (GIL), `make compile` build, jemalloc preloaded as `make clickbench` does.
- **Dataset:** ClickBench `scratch.hits_rugo_262k`: 99 parquet files, 8.8 GB,
  99,997,497 rows.
- **Queries:** runner.py numbering 16–19, 32–35. Controls: 01 (no aggregate),
  08 (18 groups, parvi path), 05 (ungrouped `COUNT(DISTINCT)`), 13 (mid-card string).
- **Method:** every cell runs in a fresh process. Run 0 is discarded (cold) and run 1
  is recorded. Arms were interleaved across rounds, and the within-round order was
  alternated and rotated. The `.so` was swapped per cell. Min of 3 rounds.
- **Noise floor:** an accidental A/A/A/A run (four builds that turned out
  identical) gave min-of-3 ratios of **0.951–1.041**. Treat anything inside ±5%
  as nothing.
- **Correctness:** every arm returned bit-identical results on 8 checks, including
  full-group checksums over UserID, (WatchID, ClientIP), URL and
  (UserID, SearchPhrase).
- ⛔ **P/E cores:** above 6 threads, work lands on E-cores. Absolute counters are
  valid, but ratios across DOP above ~6 describe this CPU, not the engine.

## Findings

### A. Merge skew: no skew exists, and more partitions measured net negative

| | DOP 4 | DOP 8 | DOP 18 |
|---|---|---|---|
| partition entries max/mean (all 8 queries) | 1.004–1.007 | 1.002–1.007 | 1.002–1.006 |
| finalize over perfect split (fin − Σpart/nt), worst query | Q33 21 ms | Q33 23 ms | Q33 53 ms (5.8% of wall) |
| same, other queries | 5–21 ms | 5–20 ms | 7–25 ms (1.8–3.5% of wall) |

- Hash partitioning is uniform. With hot runs at merge width 1, the slowest
  partition is 1.05–1.10× the mean *time*. The code comment's "the residue is the
  largest SINGLE partition" does not hold on current code.
- Finalize efficiency is lost to **per-partition work inflation, not imbalance**.
  At DOP 8, forcing `OPTERYX_GB_MERGE_THREADS` 1→16 on identical data grows Σ
  per-partition merge time from 3,504 to 7,360 ms on Q33, and from 490 to 770 ms on
  Q16. At widths ≤ 8, finalize sits within 2–5% of Σ/nt. On this box that
  inflation is E-cores plus shared bandwidth, and it cannot be separated further.
- **256 partitions (prototype, A/B at DOP 16): +5% to +25% slower** on 7 of 8
  targets (Q16 1.25×, Q19 1.23×, Q17 1.13×).
  - Finalize improved only where it was already large (Q33 482→397 ms).
  - Each flush now queues 4× the tables: Q19 queues 191k tables instead of 48k.
    Total flush-lock wait went 250→1,220 ms, and hold went 81→345 ms.
  - Each queued `GBPartition` is **3,840 bytes inline**, 3,456 of which are the
    parvi and medius front maps it no longer uses. Q33 already queues ~51k of them,
    about 195 MB. 256 partitions would make that about 780 MB against an 8 GiB
    prod worker.
- **Thread spawn vs. engine pool:** measured fin_wall − busiest-thread time is a
  median of **~1 ms** (max 4 ms) at every DOP. The pool swap is worth ~1 ms/query.
- **Where granularity would bite:** with 64 uniform partitions under dynamic
  claiming, finalize ≈ ⌈64/N⌉·N/64 × perfect split. That gives 1.0 at N=16, 1.41
  at N=30, 1.5 at N=48 and 3.0 at N≈190. This is **arithmetic, not measured**: the
  only box wide enough is the c8g class, and all our c8g numbers predate the
  current engine.

### B. Flush lock: real but small; the hold time is a vector reallocation

Total wait on `g.mtx`, summed over workers, divided by DOP, as a share of wall:

| | DOP 4 | DOP 8 | DOP 18 |
|---|---|---|---|
| wait per worker / wall | 0.0–0.4% | 0.2–1.2% | 0.9–3.9% (Q16 highest) |

- Single waits reach 8–23 ms at DOP 18. That is the lock holder being descheduled
  on an oversubscribed box (18 workers plus the parquet decode pool on 18 cores),
  not queueing.
- **Promotions and reset do not dominate the hold time.** Moving all worker-local
  work outside the lock (arm FLUSH) left hold nearly unchanged. The time goes to
  `g.pending[p]` being a `std::vector<GBPartition>`: every doubling move-constructs
  every queued 3,840-byte table while `g.mtx` is held, about 250 MB of copying on
  Q33.
- A `std::deque` for `pending` (arm DEQ, DOP 18) cut total wait **2–8×** (Q33
  459→184 ms, and →106 ms with FLUSH as well; Q32 183→21 ms) and hold by 10–50%.
  **Wall stayed inside the noise band** (0.92–1.04, rounds disagree). This matches
  the 1–4% ceiling in the table above.

### C. Found on the way: partition bits collapse the SIMD tag filter

`gb_part = h >> 58` (bits 58–63). carchar's `key_tag` is `(h >> 57) & 0x7F` (bits
57–63), and parvi and medius use the same `key_tag`. Inside one partition, 6 of the
7 tag bits are therefore constant, so the tag has **1 bit of entropy**. The 16-way
tag scan then matches about half the occupied slots, and each match costs a full
hash compare against the slot array. This affects every sink table probe and every
finalize merge probe.

Prototype arm TAG takes the partition from bits 32–37 and changes nothing else.
Results (A/B, every round the same sign):

| query | DOP 4 | DOP 16 |
|---|---|---|
| 05 `COUNT(DISTINCT UserID)` | **0.71×** | **0.74×** |
| 16 `GROUP BY UserID` | **0.84×** | **0.86×** |
| 17 `GROUP BY UserID, SearchPhrase` + ORDER BY | **0.88×** | **0.91×** |
| 18 `GROUP BY UserID, SearchPhrase` | — | **0.92×** |
| 34 `GROUP BY URL` | 0.93× | 0.96× |
| 13, 19, 35 | — | 0.94–0.96× |
| 32, 33 (raw mode: sinks don't probe) | — | 0.98–0.99× (finalize Σ −6%) |
| 01, 08 controls | 1.01× | 0.94–1.00× (2 ms / 40 ms queries) |

The gain holds at DOP 4, which runs entirely on P-cores, so it is an engine
property, not an E-core artefact.

## Recommendations

| item | verdict |
|---|---|
| **C. Move partition bits off the tag** | **SHIPPED 2026-09-26** (ratified). Was: **do it.** Largest measured effect: 4–29% on 8 of 12 queries at both DOPs, neutral elsewhere. |
| A. More radix bits (256+ sink partitions) | **Don't.** Measured +5–25% at DOP 16, plus 4× the queued-table memory. |
| A. Split oversized partitions at finalize | **Needs a bigger machine to tell.** No skew exists at ≤18 threads. The arithmetic says granularity costs 40–50% of finalize at N=30–48. |
| A. Engine thread pool instead of `std::thread` | **Don't** (~1 ms/query). |
| B. Per-partition locks / lock-free append | **Don't.** Ceiling is 1–4% of wall at DOP 18. A 2–8× wait reduction produced no measurable wall change. |
| B. `pending` as deque (or queue a slim struct) | **Optional hygiene.** Cheap and correct, but no measurable wall gain here. Re-measure on the 32-core box. |

### Design for C (shipped 2026-09-26)

Confirmation A/B of the shipped build against the clean pre-change binary (DOP 16, min of 3,
interleaved; checksum battery identical at DOP 4 and 16; `make q` green): Q05 0.78×,
Q16 0.87×, Q17 0.90×, Q13 0.95×, Q34 0.95×, Q33 0.99× (raw mode), Q08 control 1.01×.


- Partition = hash bits **32–37**: `(h >> 32) & 63`. This bit budget must hold:
  - tag 57–63 (carchar, parvi, medius);
  - parvi group select 53–54;
  - radix-merge bucket bits 40–51 (`kGBMergeBucketShift` 40, ≤ 4096 buckets);
  - carchar slot bits from 0 up. Merge tables are capped at about 2¹⁷ capacity by
    `kGBMergeLeaf` 65,536 and the radix split, and sink-local tables are smaller.
- One `gb_part(h)` helper replaces all 33 `>> kGBPartShift` sites. That covers
  GroupBySink, the dict path (`dict_part`), ungrouped `COUNT(DISTINCT)` (`dparts`),
  DistinctSink and WindowTopK, all of which must agree. Add a `static_assert` that
  the partition, bucket, parvi-group and tag ranges are disjoint, so the layout
  cannot drift again.
- No other behaviour change: the partition count stays 64, and the flush and merge
  logic are untouched.
- The prototype used exactly this and passed the checksum battery. `make q`
  remains the gate for the real change.

### If the large-box finalize question is ever reopened (A)

- Do **not** raise `kGBParts`: its cost lands on the sink side (flush and queued
  tables), for every query.
- Instead, make the finalize work unit **(partition, radix bucket)**. Partitions
  over `kGBMergeLeaf` are already scattered into 2ᵏ buckets on bits 40+. Today one
  thread then merges all its partition's buckets serially. Publishing the buckets
  as claimable tasks after the scatter decouples task count from sink partition
  count and costs nothing when N ≤ 16.
- Measure it on homogeneous hardware at DOP ≥ 30 with this note's probes
  (per-partition time, fin − Σ/nt), not wall alone.

## Interactions

- **Dictionary path (`gb_rows_only`):** probes each distinct code through the same
  `find_or_insert_group`, so it gets the tag fix (Q16 is on it). `dict_part` is
  `uint8_t`, which caps any future partition count at 256.
- **Raw mode (`kGBRawSwitchRatio`):** raw sinks never probe, so they gain nothing
  at sink time. They do gain in finalize (Q33 Σ merge −6%), since the merge
  probes. Raw mode is also the heaviest flusher (Q33: 805 flushes), so it carries
  most of B's residue.
- **Routed prototype (`OPTERYX_GB_ROUTED`):** `owner(p) = p % nworkers` makes
  64-partition granularity a *build-time* imbalance there. At N=30, owners hold 2
  or 3 partitions; at N>64, workers own nothing. The tag fix applies unchanged.
  Not measured: routed covers only `COUNT(*)`.

## Reproduction

- The probes (per-partition entries and time, per-thread busy time, flush
  wait/hold) and the prototype switches (`OPTERYX_GB_PART_BITS/LO`,
  `OPTERYX_GB_FLUSH_OUTSIDE`, `OPTERYX_GB_PENDING_DEQUE`) were temporary and have
  been removed from the tree.
- ⛔ `CFLAGS=-D…` reaches only the C compiles, **not** the `clang++` compile of
  `_operators`. Pass defines through `CPPFLAGS`/`CXXFLAGS`, and verify them on the
  `clang++ … _operators.cpp` line of the build log, or better, in the binary's
  runtime output. The first A/B in this study built four identical binaries this
  way (which is where the A/A band above came from).
