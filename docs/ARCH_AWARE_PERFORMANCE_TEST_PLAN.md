# Architecture-Aware Performance Test Plan

Status: PROPOSAL — awaiting architect review. Nothing here is built.
Date: 2026-10-04

## Purpose

1. Decide, by measurement, which ideas from Pivot (pivotlake/pivot) are worth
   building in Opteryx.
2. Stop treating "faster on the Mac" as "faster". Every candidate is measured on
   ARM **and** x86, and a result that differs by architecture becomes an
   architecture-specific decision in the engine rather than a compromise.

The working hypothesis (architect, 2026-10-04): the next ~25% will not come
from one code path that is "the same for both". Techniques such as software
prefetch may help on x86 and hurt on ARM, and the engine should be able to act
on that.

## Rules this plan runs under (already ratified)

- Profile + Amdahl ceiling **before** building. A candidate whose profiled
  ceiling is under the ±5% noise band is dropped without being built.
- Baseline captured before the first edit.
- Interleaved A/B, arm order alternating within rounds, fresh subprocess per
  run, medians and per-round pairings. A/B only — no A/A arms; a doubted result
  gets more A/B rounds.
- Control queries the change provably cannot touch are read first. If they move,
  the harness is wrong, not the engine.
- Prove the knob moves (read back what the engine actually used) before any long
  or remote run.
- Multi-part work: build all parts, then leave-one-out ablation.
- Slower ⇒ deleted. Kill switches are removed once a result is banked.

## 1. Hardware matrix

D1 ruled 2026-10-04: the i5-8500 is the x86 reference. Only if a result there
is ambiguous or SKU-sensitive do we go elsewhere.

| Tier | Machine | ISA | Compiler | Role / notes |
|---|---|---|---|---|
| core | M5 Pro Mac | arm64, NEON | Apple clang | ARM dev. 6P + 12E cores: any DOP ratio above ~6 is a hardware artefact. Compare arms at fixed DOP; never quote scaling. |
| core | `opteryx-perf` (i5-8500) | x86_64, AVX2, no AVX-512 | gcc | x86 reference (prod ISA). 6 homogeneous cores, 15 GB RAM, Coffee Lake. Local fixtures only, never live data. |
| core | Raspberry Pi 5 (Cortex-A76) | aarch64, NEON | gcc | Second ARM site. **Separates ARM from Apple clang** (same compiler as x86 and the manylinux wheels) and from the M5's large caches. Also an aspirational target. |
| breadth | Orange Pi RV2 (SpacemiT K1, rv64gcv) | RISC-V, RVV 1.0 | gcc 14 | Third ISA. Catches latent portability bugs (last bring-up found unaligned-store SIGBUS and dead RVV intrinsics that ARM/x86 hid). A third answer for arch decisions. 3.7 GB RAM ⇒ reduced dataset tier only. |
| on demand | i3-14xxx (Windows, WSL2) | x86_64, AVX2 | gcc | Newer Intel microarchitecture. Used only when an x86 result looks SKU-sensitive (D3 option C). WSL2 is a VM: data must sit on the WSL ext4 disk, not `/mnt/c`, and IO-bound results are suspect. |

**Pi 5 vs M5 is the most useful pairing in the matrix:** two ARM machines
that differ in compiler and cache size. If they agree with each other and
disagree with the i5, it really is ARM vs x86.

**RISC-V caveats:** the box last ran free-threaded 3.14.5t (a different config
from the Mac/i5 GIL builds) and was kept at an old baseline. It needs a fresh
synced tree and ideally a GIL CPython build (~40 min from source). Performance
numbers there are evidence, not a veto. Prod is x86.

**Confound to manage:** today the arch split is also a compiler split (Apple
clang on ARM, gcc on x86) and a cache-hierarchy split (M5 L2 is MBs per
cluster; the i5 is 256 KB L2/core + 9 MB L3). A result that differs between
the Mac and the i5 is "arch, compiler, or cache size" until shown otherwise.
Every result records: CPU model, L1/L2/L3 sizes, compiler + version, `-march`
/`-mtune`, DOP, allocator, Python build, tree fingerprint.

⛔ Memory inconsistency to resolve before using the old x86 prefetch result:
the box is recorded as an Intel i5-8500, but the 2026-08-14 prefetch re-test
records "x86 (Zen, 6-core)". One of these is wrong.

## 2. Phase 0 — dual-arch harness (prerequisite)

Nothing in the tree runs a benchmark on the x86 box today (`dev/ab_bench.py`
compares two local trees; `make clickbench-interleave` is Opteryx vs DuckDB).

Build `dev/arch_ab.py` (or a Make target wrapping it):

1. Sync the tree to the box (`COPYFILE_DISABLE=1 tar`, excluding
   `scratch/ testdata/ .claude/worktrees/` and build artefacts; delete `._*`).
2. Build both arms on each machine (`make compile`, Python 3.14.0 GIL on the box,
   assert `opteryx.__file__` points at the synced tree — a stale wheel is
   installed there).
3. Run `dev/ab_bench.py`-style interleaved A/B **on each machine independently**
   (never compare times across machines — only the A/B ratio per machine).
4. Collect both CSVs and print one table: per query, B/A on ARM, B/A on x86,
   controls first.

The harness takes a host list, so the same campaign runs on every box in the
matrix.

Dataset tiers (local copies only on every remote box):
- **full** (Mac, i5, Pi 5 if RAM allows): `hits_rugo_262k`, canon `scratch/hits`
  (100 files), `tpch_10_skene`, `tpch_1_skene`, JOB skene mirror. Disk and 15 GB
  RAM on the i5 cap SF10-in-memory work.
- **reduced** (RISC-V): `tpch_1_skene`, the hits subset used at bring-up
  (`hits_41`), microbenches. A candidate's ratio on the reduced tier is compared
  with the same tier on the other boxes, never with the full tier.

Every campaign also runs `make q` on every box. Correctness on the third ISA is
part of the result, not a side effect.

Gate before any campaign: one knob run proving each arm's switch is live on
both machines (e.g. `RUGO_LOCAL_MMAP_CACHE` read back from telemetry).

## 3. Phase 1 — re-measure existing arch-sensitive decisions (no new code)

Cheapest wins first: decisions already in the tree that were measured on one
architecture only.

| Item | Where | Measured on | Test |
|---|---|---|---|
| AVX2 hash mixer kept | `draken/simd/simd_hash.cpp` | ARM only ("Unmeasured on x86") | scalar-x8 vs AVX2 on the box |
| `simd_mix_hash_from_dict` NEON kept | same | ARM | AVX2 vs scalar on the box |
| `RUGO_LOCAL_MMAP_CACHE` default 1 on x86, 0 elsewhere | `rugo/src/parquet/io_pipeline.hpp:70-80` | confirm both | env toggle, both arches |
| GROUP BY constants: `kGBFlushEntries`, `kGBMergeLeaf`, `kGBMergeMaxBuckets`, ParviMap/Medius thresholds, `kGBRawSwitchRatio` | `src/cpp/engine/native_group_sinks.hpp:1795-1878` | Mac (MB-scale L2) | sweep each on both; these are cache-size constants and the most likely to be wrong on a 256 KB-L2 part |
| Carchar load factor 0.80 / doubling | `third_party/mabel/carchar/carchar_index.hpp` | Mac | sweep |
| Tier-3 page-parallel decode gate (>2 pages, plain numeric) | `rugo/src/parquet/decode_column.cpp:1218-1302` | Mac | on/off both arches |
| `-mtune=generic` on x86, `-march=armv8-a+aes -mtune=generic` on Linux ARM | `build_common.py:426-449` | never | generic vs native tune on the i5 and the Pi 5, to size what tuning is worth before deciding wheel flags |
| Join build-side bloom prefilter (deleted; `src/cpp/bloom_filter_ops.hpp` kernel survives) | `native_join2.hpp` | Mac only (JOB +5.6% slower) | rerun `bench_join_csr_lookup.cpp` (miss-lookup cost `L`) on every box first; rebuild the join integration only if `L` is materially higher off the Mac |
| Software prefetch at GROUP BY probe sites | `native_group_sinks.hpp` | Mac + one x86 run (see the inconsistency above) | rerun the 2026-08-14 env-gated arm on the i5, Pi 5, RISC-V |

Output: a table of which existing constants/paths disagree between arches. If
none disagree, that is evidence against the hypothesis and should temper the
rest of this plan.

## 4. Phase 2 — Pivot-derived candidates

Current state comes from a code survey on 2026-10-04 (file:line references
there). Each candidate gets a profile + ceiling on **both** arches before any
code is written.

### C1. Sub-row-group decode claims  — highest expected value
- **Gap:** rugo `decode_row_group` (`io_pipeline.hpp:2710`) decodes every
  projected column of a row group sequentially on one thread; the parquet claim
  unit is the row group. Skene v3 claims a block and queues its row groups (D-4)
  but never splits below a row group.
- **Prior evidence (ARM only):** a 64k-row-group mirror cut SF1 execution −18–22%
  versus 262k; a post-decode split queue gave only ~2%. The imbalance is decode
  claims.
- **Cheap ceiling probe first (no engine code):** rerun the 262k vs 64k mirror
  comparison on the x86 box. If x86 shows the same gap, the decode-claim
  ceiling holds on both.
- **Build options:** (a) column-level decode tasks within a row group, (b)
  row-range decode tasks sharing the row group's dictionary (Pivot's
  "range cutter"). Build both, ABBA, keep faster per arch.
- **Metric:** TPC-H SF1 + SF10 exec wall, `barrier_idle`, ClickBench on 262k.

### C2. TopNSink batch rejection against the shared boundary
- **Gap:** `TopNBoundary` is shared across workers but only consumed by the
  scan to skip row groups (`topn_boundary.hpp`, `native_parquet_scan_source.hpp`).
  `TopNSink::sink` (`native_sort.hpp:252-268`) appends every morsel and
  compacts at `max(4n, 65536)`.
- **Mechanism:** one vectorized compare of the leading key against the boundary;
  drop a morsel with no survivors; cap the partial sort at the survivor count.
- **Target shape:** ClickBench Q25/Q27 (~13M rows into a sink for 10).
- **Arch angle:** the compare-and-compact is branch-heavy; measure both.

### C3. GROUP BY radix-scatter mode for high-cardinality integer keys
- **Gap:** we flush/queue partitions and rehash Carchar on growth; Pivot stops
  probing past 32k entries and scatters rows into radix buffers (no probe),
  aggregating in a cache-resident merge.
- **Overlap:** our raw mode (`kGBRawSwitchRatio`) and the routed prototype
  (`OPTERYX_GB_ROUTED`) cover part of this. Phase-1 constant sweeps come first;
  only build if the profile shows probe cost on high-NDV int keys (Q16/Q17/Q32/Q33
  shapes) after re-tuning.
- **Arch angle:** scatter-vs-probe crossover depends on L2 size — expect
  different thresholds per arch.

### C4. Filter: progressive conjuncts and post-filter coalescing
- **Gap:** `BC_DNF` evaluates every AND child on the full morsel, then folds;
  `BC_LAZY` narrows only operands that can raise. Guarding everything measured
  2.4x slower on Q06 (Mac). No coalescing of small post-filter morsels in the
  native pipeline.
- **Test:** (a) narrow after the first conjunct only when its measured
  selectivity is below a threshold; (b) coalesce sub-N morsels before
  pipeline breakers. Selection gather vs full-width compare is exactly the kind
  of trade that can flip between NEON and AVX2.

### C5. Software prefetch — join build scatter and pipelined join probe
- The ban stands for shipped code. **D2 (2026-10-04): re-testing any
  assumption is approved**, so this goes in as a gated experiment. Anything
  that doesn't win is deleted again.
- **What was tested:** GROUP BY probe sites, on ARM and once on x86: 8% slower
  cache-resident, flat to +2.8% high-NDV. **Not tested:** join build scatter
  and a hash-ahead/prefetch/drain probe ring, which is where Pivot uses it.
- **Order:** microbench first (`src/cpp/engine/bench_join_csr_lookup.cpp`
  extended with a prefetched arm) on both arches across L1/L2/L3/DRAM working
  sets; only if x86 shows a gain in the DRAM regime, a gated JOB + TPC-H SF10 run.
- If it wins on x86 and loses on ARM, it ships x86-only under the D3 mechanism.
  If it loses on both, the memory gets a third data point and it stays banned.

### C6. Dictionary check before data-page IO
- **Gap:** dict pruning exists (`decode_column.cpp:1006-1110`) but runs after
  the column chunk is fetched/mapped — it saves decode, not IO. It is off when a
  `row_mask` is present; one predicate per column.
- **Local benefit is small** (mmap). Value is remote (GCS/AIStor), so the test
  uses the `*-aistor` targets from the Mac. Lower priority; not an arch question.

### C7. In-process page cache across queries (Opteryx's own)
- **D4 (2026-10-04):** in scope as an improvement, kept only if it pays.
- **Gap:** only metadata is cached across queries (parsed footers, skene
  readers, manifests). The rugo mmap cache is per query. Pivot keeps compressed
  and decompressed pages in RAM, evicted with CLOCK, and when one copy of a page
  is evicted it protects the other.
- **Where the win can be:** for local files the OS page cache already holds the
  compressed bytes, so the local upside is the **decompressed** tier
  (decompression + decode skipped). For remote files (GCS/AIStor) the compressed
  tier saves the network round trip.
- **Ceiling first:** profile how much of a repeated query's time is decompression
  and decode, versus IO and operators, on every box. The ratio of decompression
  speed to memory bandwidth differs by architecture.
- **Workload:** a repeated-query mix, such as the same dashboard query run N
  times or overlapping column sets, plus remote AIStor runs. ClickBench "hot"
  alone would overstate it.
- **Isolation:** every other candidate is benchmarked with the cache off, so C7
  can't flatter their numbers.
- **Cloud Run question to answer alongside it:** how long instances live, and so
  how often a cache would ever be warm in production.

### Not pursued
- **Operator scheduling order** (downstream-first): our executor already pushes a
  morsel depth-first to the sink on one worker; no equivalent gap.
- **Indexer page-emission order:** tied to Pivot's page-at-a-time pipeline; rugo
  decodes by column chunk.

## 5. Phase 3 — the mechanism for architecture-specific decisions

D3 (2026-10-04): architecture-dependent decisions are expected. The mechanism
is designed alongside Phase 1, so the first divergent result has somewhere to
go. Which option(s) to use is decided by what Phase 1 shows. If the M5, Pi 5 and
i5 each want a different GROUP BY threshold, that points to B
(cache-derived) over A (per-ISA). Options:

| Option | Cost | Covers |
|---|---|---|
| **A. Compile-time, per-ISA** — one header (`draken/core/arch_tuning.h`) holding every tuned constant / path choice under `#if defined(__x86_64__)` / `__aarch64__`, with `static_assert`s | zero runtime; wheels are already per-arch | ARM vs x86 |
| **B. Startup cache-size derivation** — constants computed once from L1/L2/L3 sizes (`sysconf` / `sysctl hw.perflevel0.*`), stored in a const struct read at pipeline construction | one read per query, no hot-path branch | cache-sensitive constants (GROUP BY thresholds, partition counts) across x86 SKUs |
| **C. Runtime microarch selection** — pick among compiled specialisations (e.g. Zen vs Intel) once at pipeline build, never per morsel (§2: no dynamic dispatch in hot paths) | template-instantiation bloat; test matrix grows | only if measurement shows x86 SKUs disagree |

Rules regardless of option: the answer must be identical on every arch
(a divergent result is a bug); the uniform path stays the correctness
reference; every arch-specific choice records the measurement that justified
it next to the constant.

## 6. Decision rules per candidate

| ARM result | x86 result | Outcome |
|---|---|---|
| faster | faster | ship everywhere |
| neutral | faster | ship x86-only via D3 (prod is x86) |
| slower | faster | ship x86-only via D3 |
| faster | neutral/slower | ARM-only only if worth the dev-machine win; default delete (prod is x86) |
| neutral/slower | neutral/slower | delete; record the measurement in memory |

"Faster" = wins in the same direction in every paired round and the ranges
separate; magnitude claimed only when the ranges separate.

## 7. Order of work

1. Phase 0 harness + knob proof (blocks everything).
2. Phase 1 re-measurements (no new code; tests the hypothesis itself).
3. C1 ceiling probe (mirror comparison on x86) → build C1 if the ceiling holds.
4. C2 (small, self-contained).
5. C3 / C4 depending on Phase-1 GROUP BY and filter findings.
6. C5 only with D2 approval, microbench first.
7. C6 when remote work is next on the list.
8. D3 mechanism, the first time a result diverges by arch.

## 8. Rulings (2026-10-04)

- **D1 — hardware:** the i5-8500 is enough to show whether we need to look
  elsewhere. Matrix extended: Pi 5 (second ARM, gcc), RISC-V (third ISA),
  i3-14xxx under WSL2 on demand.
- **D2 — re-test assumptions:** approved for any assumption (prefetch, bloom,
  constants).
- **D3 — mechanism:** arch-dependent decisions are expected. The option is
  chosen from Phase 1 data.
- **D4 — page cache:** in scope as C7, kept only if it pays.

## 9. Progress log

**2026-10-04 (Mac + i5; Pi 5 and RISC-V offline, to follow)**
- i5 confirmed Intel Coffee Lake (lscpu: 256 KiB L2/core, 9 MiB L3), gcc 12.2
  **and clang 14 available** — so x86 can be built with both compilers, which
  separates compiler from ISA before the Pi 5 is back.
- Harness: `dev/ab_bench.py` extended — N arms from `--arms` JSON (tree + env +
  `.so` substitutes), within-round order alternates, untimed `--warmup` pass,
  `--controls` listed first, JSON output with machine fingerprint. Silent
  allocator-preload fallback (bare `except: pass`) replaced with a hard failure.
  `dev/arch_tree.sh` builds an isolated source-only bench tree (local or ssh)
  with datasets symlinked. `dev/build_so_variant.sh` builds one extension with
  `-D` defines, verifies each define reached a compile command, and restores the
  original. `dev/arch_compare.py` lays per-machine JSON results side by side.
- Datasets synced to the i5 under `~/arch-20261004/data` (hits_rugo_262k
  verified identical to the Mac copy; tpch_1/10_skene; job_skene). The box's
  older copies were a stale 99-file layout.
- Mac ClickBench baseline captured before any engine edit
  (`dev/bench_results/ab-clickbench-Justins-MacBook-Pro-20261004-191352.*`,
  3 rounds, ~9.7 s/suite).
- Temporary sweep switches `OPTERYX_SWEEP_GB_FLUSH_ENTRIES` /
  `OPTERYX_SWEEP_GB_MERGE_LEAF` in `native_group_sinks.hpp` — deleted
  2026-10-05 once banked, along with every other `OPTERYX_SWEEP_*` switch.
- C1 note: §9.4 of `docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md` measured 64k
  parquet row groups 4-9% SLOWER on ClickBench (Mac, fixed per-row-group cost),
  so a 64k-vs-262k mirror is not a clean ceiling for sub-row-group decode
  claims. Skene v3 mirrors are already 64k. C1's ceiling is taken from
  skew/barrier-idle counters on the parquet path instead.

### Phase 1 results so far (2026-10-04)

**Hash mixer kernels** (`dev/bench_hash_mix_arch.cpp`, min of 31, alternating,
byte-identical outputs). ns/value at 1 MiB (L2) / 64 MiB (DRAM):

| i5-8500 | mix | hash_i64 |
|---|---|---|
| true scalar (`-fno-tree-vectorize`) | 0.81 / 1.27 | 0.72 / 1.14 |
| hand AVX2 (shipped x86 slot) | 0.66-0.68 / 1.21 | 0.58-0.60 / 1.09-1.11 |
| plain unrolled loop, gcc auto-vectorized | **0.63 / 1.20** | **0.51 / 1.07** |
| plain unrolled loop, clang 14 auto-vectorized | 0.68 / 1.22 | 0.54 / 1.10 |

- The "scalar" arm is NOT scalar on x86: both gcc and clang emit `vpmuludq`
  for it under `-march=haswell`. The best x86 kernel is the compiler's, not
  the hand-written one. On ARM the plain loop is already the shipped choice,
  so the measured answer is "same source for both".
- M5 (clang, plain loop): mix 0.214-0.216, hash_i64 0.148-0.151.
- **Suite-level check (i5, ClickBench, 7 rounds):** `OPTERYX_SWEEP_HASH_PLAIN_LOOP`
  (plain loop in the x86 slot) = 0.9995 over rounds 2-7 (round 1 was a one-off
  68 s outlier on the variant), per-query median total 1.004, no query beyond
  ±5%. Switch verified to bind: the two `draken_native` builds carry different
  `simd_hash_i64` bodies, and integer-key hashing (`cxx_hash_c` →
  `draken/ops/hash.h`) calls it. ClickBench's large GROUP BYs hash string keys
  with XXH3, so this kernel is too small a share to show.
- **Verdict:** the plain loop is faster in isolation and tied at suite level.
  The hand AVX2 kernels are the loser by the "faster wins, loser deleted" rule.
  Deleting them changes shipped code outside this sweep, so it needs an
  architect ruling.

**`kGBFlushEntries`** (ClickBench, 4 arms, 5 rounds, ratio vs 65536):

| | 16384 | 32768 | 131072 |
|---|---|---|---|
| M5 suite | 1.040 | 1.020 | 1.019 |
| i5 suite | 1.023 | 1.022 | 1.005 |

Smaller loses on both (Q09/Q10/Q14/Q19 0/5 wins on both). **Not
architecture-sensitive; 65536 stays.**

**`kGBMergeLeaf`** (same method, ratio vs 65536):

| | 16384 | 32768 | 131072 |
|---|---|---|---|
| M5 suite | 1.020 | 1.002 | 1.005 |
| i5 suite | 0.998 | **0.992** | 1.002 |

- Q18 wants a larger leaf on both (131k: M5 0.943, i5 0.976, 5/5 both); Q13
  wants a smaller one on both (131k: +14% / +11%, 0/5 both).
- **Q33 diverges:** 32k is 2.8% faster on the i5 (5/5, ranges separate) and
  3.5% slower on the M5. First sign flip on a large query; consistent with the
  i5's 256 KiB L2.
- Suite effect ~0.8% on x86 only. No change. Evidence for D3 option B
  (cache-derived), since no single fixed value wins every query on either box.

**Harness observations.** The i5 shows occasional 2-5x single-round spikes on
sub-300 ms queries, so small queries need more rounds there. Two control flags
remain after requiring a consistent shift (Mac Q01 1.5 ms metadata count,
-0.08 ms in every round; i5 Q02 at flush 32k). Neither query can be reached by
the switches. Both look like arm-position effects in very short queries, and
both are inside the ±5% band.

### Prefetch and bloom re-tests (2026-10-04/05, D2)

**Microbench** (`src/cpp/engine/bench_join_csr_lookup.cpp --arms`, model of the
JoinCsr probe, min of 7, arms alternating, match counts checked):
- Pipelined prefetch (16 ahead) vs plain: ~10% SLOWER while the table is
  cache-resident on both; 0.49-0.70x on the M5 from 4M build rows (61 MiB) up;
  0.6-0.8x on the i5 from 100k rows (1.5 MiB) up. gcc and clang agree on x86.
- Bloom (word-64, k=2): misses 0.17-0.57x on both; hits 1.05-1.10x on the M5
  but **1.20-1.36x on the i5**; 50% misses 0.63-0.75x on the M5, 0.79-1.06x
  on the i5. The 2026-08-07 ruling (net negative) stands, more firmly on x86.

**Engine** (`OPTERYX_SWEEP_JOIN_PREFETCH=16`, all five JoinCsr probe loops):

| | M5 | i5 |
|---|---|---|
| JOB | 1.014, 1/7 rounds faster | 1.024, 0/5 |
| TPC-H SF10 | 0.946, 5/5 (Q04 0.58, Q21 0.82, Q22 0.84 — EXISTS paths; Q08 1.32) | 1.028, 0/5 |

- The microbench gain does not survive the engine probe (which also appends
  pairs and gathers rows). x86 loses every round of both suites. **Deleted**
  per the decision table (ARM-only TPC-H win, x86 loss). The ban stands, now
  measured at the join probe too.
- Join-free TPC-H Q06 moved 1.14x (M5) / 1.18x (i5) in the variant: a
  code-layout side effect of rebuilding `_operators`. Suite deltas under ~±3%
  in .so-variant A/Bs are partly layout, not the change.
- Mac JOB first run (2026-10-04 23:00) was void — both arms doubled mid-run
  under another session's load. Rerun gated on idle (load1 < 2, top process <
  30% for 3 consecutive minutes); per-round totals then stable 6.8-7.2 s.
- **GROUP BY probe prefetch** (`OPTERYX_SWEEP_GB_PREFETCH`, carchar control line
  8/16 rows ahead in pass B; ClickBench, 2026-10-05): M5 0.998 / 0.999 (7 rounds),
  i5 0.995 / 0.997 (5 rounds). Per-round totals within ~1% across arms; per-query
  moves (i5 Q13 0.905 at 16 only, Q36 0.954 at 8 only, Q18 +3-4% at both) are
  inconsistent between distances and inside the layout-noise band. Not faster
  on either architecture — **deleted**. The ban stands at the GROUP BY probe on
  both, under the full-suite harness.

### C1 ceiling — i5 (2026-10-05)

`dev/c1_skew_probe.py`: per query, median of 3 passes after a warm pass,
barrier idle summed over pipelines; ceiling = idle / DOP / wall, the wall share
a perfect distribution of the same work could remove (DOP 4 = cpu − 2 by
design on the 6-core box; mimalloc preload as the harness uses).

| dataset | suite wall | ceiling | worst queries |
|---|---|---|---|
| ClickBench, parquet 262k rgs | 61.7 s | **0.2%** | Q37-Q43 (34-206 ms, CounterID=62) 2-6% |
| TPC-H SF1, parquet | 3.1 s | **2.3%** | Q16 4.7%, Q02 4.1%, Q22 3.7% |
| TPC-H SF10, parquet | 33.8 s | **0.8%** | Q22 3.5%, Q02 2.5%, Q11 2.4% |
| TPC-H SF1, skene v3 (64k rgs) | 2.4 s | **2.9%** | Q10 11.1%, Q22 9.3%, Q02 7.0% |

- **Verdict on x86 at DOP 4: C1 has no ceiling worth building for.** Finer
  decode claims can only reclaim barrier idle, and that is ≤ 3% of every suite
  and 0.2% of ClickBench. ~378 row groups across 4 workers already balance.
- The 2026-09-23 SF1 imbalance finding (64k claims −18-22% exec) was at 14-18
  workers on the Mac with ~23 lineitem claims. The ceiling scales with workers
  per claim, so it is a high-DOP effect. The Mac re-measure (when idle) and
  the production vCPU count decide whether C1 matters anywhere. On a 4-8 vCPU
  Cloud Run instance the i5 picture applies.
- Not covered: idle INSIDE a pipeline, i.e. workers waiting on the decode pool
  mid-run. Process cores are 4.3-5.8 of 6 on parquet, so ≤ ~25% headroom on
  the small SF1 queries (Q02/Q11/Q16/Q22), which are latency-bound, not
  distribution-bound.

## 10. Still needed to start

- Pi 5: address, RAM, OS (64-bit Raspberry Pi OS / Debian?).
- RISC-V: confirm `pi@192.168.4.190` is current, and OK to sync a fresh tree
  and build a GIL CPython there.
- i3: exact model, and whether WSL2 is already installed (only needed on demand).
- i5: confirm Intel vs "Zen" (memory inconsistency above).
