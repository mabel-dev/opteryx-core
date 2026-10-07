# C7 — In-process cross-query page cache for the parquet scan

Status: DESIGN — partly ruled 2026-10-06 (§12). Only the A0 telemetry fix is built.
Date: 2026-10-06
Plan entry: `docs/ARCH_AWARE_PERFORMANCE_TEST_PLAN.md` §4 C7 (D4: in scope, kept only if it pays).

## 1. What the code does today

- **Decompression happens in exactly four places**, all in
  `rugo/src/parquet/decode_column.cpp`, all calling
  `rugo::compression::DecompressInto` into a scratch buffer that is reused and
  then decoded straight away:
  - `:735` dictionary page → local `dict_decompressed`-style buffer
  - `:1453` intra-chunk page pool (parallel page tasks) → `thread_local decomp_buf`
  - `:1729` DATA_PAGE v1, serial loop → `page_decompressed_data`
  - `:1857` DATA_PAGE_V2 values region, serial loop → `page_decompressed_data`
- ✅ **Telemetry gap (fixed 2026-10-06, A0):** `rugo_tel::decompress_ns` was recorded at `:735`, `:1729`
  and `:1857`, but **not** on the pool path at `:1453`. When the intra-chunk
  page pool is used, `decompress_s` under-reports. The perf-based i5 profile
  (35.4%) is not affected; telemetry-based ceilings are. This must be fixed
  before the ceiling phase relies on telemetry (§10, step A0).
- **Which .so:** the engine's `DecodeColumnFromChunk` is compiled into
  `opteryx.connectors.parquet_io.pool_reader` (setup.py:1463). `_operators.so`
  holds its own inline copy of `io_pipeline.hpp` and resolves
  `DecodeColumnFromChunk` from pool_reader (RTLD_GLOBAL load). `rugo.rugo_native`
  (build_common.py:840) compiles a **second** copy of `decode_column.cpp`. A
  `static` cache placed inside rugo's sources would therefore exist twice in
  one opteryx process — the SkeneReaderCache trap.
- **Cross-query state today:** parsed footers (`ParquetFooterMap`,
  `PARQUET_FOOTER_CACHE_BYTES`), skene readers (`SkeneReaderCache`, key path +
  size + mtime_ns), manifests. The rugo whole-file mmap cache
  (`io_pipeline.hpp:1893`, `MAP_SHARED`) lives for one query.
- **Existing structures, none usable as-is:**
  - `opteryx::MemoryPool` (`src/cpp/memory_pool.hpp`): per-plan, one mutex, no
    key index, no eviction, compaction moves memory (a pinned raw pointer would
    dangle).
  - `opteryx::LRU2` (`src/cpp/lru2.hpp`, behind `LRU_K`): `std::string` values,
    copies out on get, no lock, no pinning, heap-ordered — a Python-facing
    bytes cache.
  - Reusable ideas: MemoryPool's latch/unlatch pinning; `get_cgroup_memory_limit_bytes()`
    in `src/cpp/platform.cpp:147` (cgroup v2 `memory.max`, v1 fallback, 0 on macOS)
    already exists natively.
- **ClickBench harness:** `ClickBench/opteryx/benchmark.sh` sets
  `BENCH_RESTARTABLE=yes` — the driver stops the server, drops the OS caches and
  restarts it **before every query**, so try 1 is a true cold run and tries 2-3
  hit the same process. ClickBench rules allow source-data caches ("buffer
  pools") and require them to be empty for the cold run, which the restart
  already gives. So in the official run **a cache only ever helps tries 2-3 of
  the same query**; there is no cross-query reuse. The insert cost lands on
  try 1 (cold).

## 2. Tiers and their ceilings

| Tier | Holds | Saves on hit | Ceiling evidence today | Where it pays |
|---|---|---|---|---|
| **T0** compressed column-chunk bytes | raw bytes as fetched | IO (GET / pread / faults) | Local: ~0 — the OS page cache already holds them; minflt 10-45k on canon Q28. Remote: prod GET 110-150 ms each, round-trip shaped | remote only |
| **T1** decompressed page bytes | output of `DecompressInto` (+ parsed page header) | decompression | i5 canon Q28 (Snappy, post-stub): 35.4% CPU → **≤1.55×**. i5 rugo_262k Q28 (ZSTD): 72% → **≤3.5×**. Mac rugo mirror ~75% | local and remote, repeated queries |
| **T2** decoded column (DecodedColumn / Draken buffers) | decoded values, offsets, dict | decompression + decode (+ slot build if cached as vectors) | canon Q28: 35.4 + 19.1 = 54.5% → ≤2.2×; with slot build 9.2% → ≤2.8× | repeated identical projections |

Caveats on these ceilings:

1. **They are CPU shares, not wall.** Each is an upper bound from one query
   (Q28) on one box. The ceiling phase (§10) measures the whole suite and a
   repeated-query mix on every box.
2. **A hit is not free: it moves the cost to memory bandwidth.** Decompressing
   writes into a small, L2-resident scratch buffer that is decoded immediately. A
   T1 hit streams the decompressed page from DRAM instead. For the 100M-row
   dataset, URL is ~9 GB decompressed; reading 9 GB at an achievable ~25-30
   GB/s on the i5 (dual-channel DDR4) is ~0.3-0.4 s of floor before any
   decode. The same read on the Pi 5 (LPDDR4X, roughly half that) costs about
   twice as much, and on the M5 Pro much less. **The ratio of decompress speed
   to memory bandwidth decides the payoff, and it differs by architecture**,
   which is the plan's own warning. The bound for Snappy (fast decompressor) is
   therefore further below its CPU share than the bound for ZSTD.
3. **T2 conflicts with the work-skipping decode paths.** Decoded output depends
   on `row_mask`, `prefer_dict`, `skip_pred` (dict pruning), `PageJumpPlan`,
   `PageSearchOut` (raw-page LIKE), `length_only` (stub decode) and the external
   buffers. A T2 entry would be keyed by decode mode, or else exist only for
   full unfiltered decodes and be bypassed whenever selective decode, page
   search or length-only applies — exactly the paths that make the hot queries
   fast now. T2 also takes the most memory (strings: arena + offsets, plus the
   interned dict). T1 sits underneath all of these paths and does not change
   them.
4. **T0 for remote reopens an earlier ruling.** Data-file caching was rejected
   for the Cloud Run disk plan (2026-07-10: "explicitly NOT caching data
   files"; GCS cost was wasted transfer). That was a disk tier. An in-RAM T0 is a
   different object, but it is the same question, and prod's floor is GCS
   round trips, not decompression (§9). T1 alone does **not** skip the GET: the
   column chunk is fetched before decoding starts.

**Recommendation:** T1 only for the first build, local and remote alike (the
hook is below the IO layer). T2 measured as a ceiling only, not built. T0 is a
separate question for the architect (§11, D-C7-1).

## 3. Where the cache lives

Requirements: one instance per process; rugo stays opteryx-free and able to run
without Python; no dynamic dispatch in hot paths.

| Option | Description | Problem |
|---|---|---|
| A | `static` cache inside `decode_column.cpp` | two instances (pool_reader + rugo_native); the policy (budget, cgroup) ends up in rugo |
| B | abstract hook in rugo (virtual or fn-pointer), opteryx implements | dynamic dispatch per page (per page, not per row, but CLAUDE.md §2 rules on it) |
| **C** | **concrete `rugo::PageCache` class in `rugo/src/parquet/page_cache.hpp` (pure C++, no opteryx, no Python); rugo only takes a `PageCache*` (nullptr = no cache) through the decode call; opteryx owns the single instance** | — |

Option C detail:
- The **mechanism** (map, CLOCK, pinning, byte accounting) is in rugo, so it is
  statically dispatched and also usable by the standalone rugo wheel later, if
  that is ever wanted. Standalone rugo passes nullptr and nothing changes for it.
- The **instance and policy** (budget, cgroup read, telemetry export) are in
  opteryx: an out-of-line accessor `opteryx_page_cache()` in a `.cpp` compiled
  into **pool_reader.so only**, exported once. `_operators` gets the pointer the
  way the planner gets the SkeneReaderCache builder (`borrowed_address()`), or
  through the `ParquetIOPipeline` that pool_reader constructs. It is never a
  header-inline static.
- Plumbing: `DecodeColumnFromChunk` gains a `PageCache*` parameter (or a field in
  an existing options struct), threaded from `decode_row_group`
  (`io_pipeline.hpp`) where the pipeline holds the pointer. Python's only role
  is passing the budget once at startup (configuration, not execution).

## 4. Keying

- Entry = one page (dictionary or data). Key = `(file_id, page_header_offset)`.
  The page offset within an immutable file identifies the page exactly; the
  decompressed bytes depend on nothing else.
- `file_id` = a `uint32` from an interning table `path → id`, **not** a 64-bit
  hash of the path: a hash collision would return another file's bytes, a wrong
  answer with no error.
- File identity: files are immutable by standing ruling ("immutable = LAW,
  path-only cache keys correct"), so the key uses the path. One question only,
  not a recommendation to change it: does the ruling also cover local ad-hoc
  files that a test or user rewrites at the same path within one process?
  (`SkeneReaderCache` happens to key on `(path, size, mtime_ns)`.) If it does,
  a same-path rewrite inside one process is outside the contract (§11, D-C7-4).
- The entry also stores the parsed page-header fields the decoder needs
  (`num_values`, `encoding`, v2 level lengths, uncompressed size), so a hit
  does not re-parse the header. Re-reading headers from the mapped chunk is
  cheap locally, so this matters only if T0 is ever added.
- DATA_PAGE_V2: only the values region is compressed. The entry holds only the
  decompressed values region; the levels are read from the chunk as today.
- Uncompressed columns (`codec == 0`): never cached, there is nothing to save.

## 5. Eviction and admission

- **Base: CLOCK** (second chance) with byte accounting. The hand sweeps,
  clears reference bits, evicts unpinned unreferenced entries until under
  budget. O(1) amortised, no list surgery on hits (a hit sets one bit).
- **Cost-aware weight (Pivot-style), optional part:** GCLOCK, where the initial
  and refresh counter is weighted by reconstruction cost per byte by codec
  (ZSTD > GZIP > Snappy ≈ LZ4). Pivot's "protect the other copy" has no local
  equivalent: our T0 is the OS page cache, which we do not control. With T1
  only, the weight is between codecs, not between tiers.
- **Admission, the important one.** An undersized cache that admits
  everything thrashes: every page pays a fresh allocation (and first-touch
  faults) and is evicted before it is reused. That makes the query **slower**
  than no cache (Pivot's 8 GB box: hot 7.4 s vs cold 9.1 s, nearly nothing
  bought). Options:
  - admit on first miss: try 2 is hot; thrashes when the working set exceeds the budget
  - admit on second touch (doorkeeper / ghost keys): try 2 is still cold, try 3 hot; scan-resistant
  - admit while the query's projected working set ≤ remaining budget (computable from
    footer `total_uncompressed_size` per surviving column chunk at plan time); refuse
    the whole scan otherwise. Deterministic, no thrash, and the plan records the decision.
- Recommendation: CLOCK + working-set admission (third option). GCLOCK
  weighting built as a separate part and judged by leave-one-out (§10).
- Pinning: a lookup returns a handle with an atomic pin count. CLOCK skips
  pinned entries; the decoder unpins when the page has been decoded. A pinned
  entry is never freed or moved.

## 6. Memory budget

- Budget source, in order: an explicit config value (bytes; 0 = off) → else a
  fraction of `get_cgroup_memory_limit_bytes()` when non-zero (Linux container,
  Cloud Run) → else a fraction of physical RAM (`sysconf` / `sysctl hw.memsize`).
  CPU-count sizing is cgroup-blind today; memory has a native probe already.
- **Default value is an architect decision** (§11, D-C7-5). The cache competes
  with query memory (GROUP BY, joins). Query OOM is `std::terminate()` today,
  with no spill on most paths. A cache that does not yield memory can turn a
  query that used to succeed into a crash.
- Yield: either (a) a fixed budget, never shrinks (simple, but the budget must
  be conservative), or (b) the cache gives memory back when query memory is
  under pressure (hook from the existing agg/spill budget modules). (b) is more
  complex and needs its own design.
- Allocation: one heap allocation per entry (pages are ~1 MB uncompressed by
  default writers, so per-entry overhead is negligible). On a miss, decompress
  **directly into the new entry's buffer** instead of the thread-local scratch,
  so admission costs no extra memcpy. The cost of a miss that is admitted is the
  allocation plus first-touch page faults on fresh memory (~9 GB for URL ≈ 2.3M
  4 KiB faults). That cost lands on cold runs and must be measured (§10).
  Linux transparent huge pages (`MADV_HUGEPAGE`) on large entries would cut
  faults and TLB misses — a later, separately measured part.
- Double memory locally: the kernel keeps the compressed file in the page
  cache while we keep decompressed pages. Under cgroup v2 the page cache counts
  towards `memory.max` but is reclaimed first. Acceptable; noted.

## 7. Concurrency

- Callers: decode-pool threads and engine workers running decode inline
  (~15% of CPU on the i5 runs inline). Any thread can decode any page, so
  per-core caches would duplicate pages and share nothing. Not considered.
- Lookups happen once per page (~1 MB), not per row. A global mutex is probably
  fine at 6 cores and probably not at 30+.
- Recommendation: **lock striping** — N shards (e.g. 64) by key hash, each with
  its own mutex, map, CLOCK hand and budget/N. Insert and evict are per shard.
- Concurrent misses on the same page (two threads, or two concurrent queries):
  allow duplicate decompression; first insert wins and the loser frees its
  buffer. No single-flight in the first build (§11, D-C7-7).

## 8. IO interaction, direct I/O and io_uring

- Locally the compressed bytes come from `mmap(MAP_SHARED)` of the file (or
  pread for small chunks), backed by the OS page cache. With T1 only, nothing
  in the IO layer changes. A hit still maps the chunk and reads its page
  headers; that is cheap.
- **Direct I/O / io_uring are not needed for T1.** Pivot uses them so it can own
  the compressed tier and avoid double caching. They become relevant only if we
  own a local T0, which the OS page cache already provides. io_uring is
  Linux-only and cannot be tested on the macOS dev box (macOS has `F_NOCACHE`
  but no io_uring). Recommendation: out of C7; a separate candidate if ever.
  There is no recorded earlier io_uring result in the repo or memory.

## 9. Cloud Run: does a cache ever get warm?

Unknown, and it decides whether C7 matters in prod at all:
- Prod data is on GCS, so the OS page cache plays no part and every query pays
  the GETs (110-150 ms each, 2026-07-22 trace). T1 saves CPU on repeated reads
  but not the round trips that set prod's floor.
- What we need: instance lifetime distribution, queries per instance, and the
  **repeat rate** — the fraction of (file, column) chunks read that were read
  earlier by the same instance. Cloud Run `min-instances`, concurrency and
  memory size bound the answer.
- First step, without code: check what `benchmarks.telemetry` / prod telemetry
  already records (instance id, process start, file paths per scan). If it
  records no process identity, adding a process-start timestamp and instance id
  to telemetry is a small, separate change that needs a ruling.

## 10. Measurement plan (per the plan's rules)

Boxes: M5 Pro Mac (fixed DOP ≤ 6 P-cores), i5-8500 `pi@192.168.4.60`, Pi 5 when
available. The i5 is JCC-erratum-affected: a ±3-5% x86 result needs confirming
on an unaffected box before acting on it.

**A. Profile and ceiling, before any build**
- A0. Add decompress timing to the pool path at `decode_column.cpp:1453`
  (telemetry fix, separate small change), or use perf only.
- A1. **Footer census, no code:** per column, sum `total_uncompressed_size` over
  canon (100 files) and rugo_262k, read with rugo/manifest (not pyarrow). Gives
  the per-query working set (columns × surviving row groups) and the whole-suite
  total. That is the exact memory a full T1 hit needs, per query and per suite.
- A2. **Per-tier CPU share** per query, whole ClickBench suite + TPC-H SF10
  parquet + the repeated mix, on every box: decompress (T1), decompress +
  decode (T2), IO wait (T0, remote only).
- A3. **Wall ceiling for T1 with zero engine change:** a copy of the dataset
  with the same row groups, pages and encodings but codec = UNCOMPRESSED,
  served from a warm OS page cache. Decoding it is exactly a T1 hit minus the
  cache lookup, and it includes the DRAM-bandwidth cost from §2. Needs a
  page-preserving "decompress-only" rewrite tool in `dev/` (re-encoding through a
  writer would change page boundaries and dictionaries, a confound). Memory: the
  i5 has 15 GB, so use the 20-file canon subset or the projected columns only.
- A4. Memory bandwidth per box (a read-stream microbench) so the §2 bandwidth
  argument is measured, not estimated.
- **Gate:** if the A3 wall bound on the repeated mix is under the ±5% noise
  band on the i5 (prod ISA), C7 is dropped without being built.

**B. Baseline before the first edit**: ClickBench canon + rugo_262k, TPC-H SF10
parquet, the repeated mix, plus controls, on Mac and i5 with the current tree.

**C. Build all parts, then leave-one-out**: parts = T1 cache + CLOCK
(mandatory), working-set admission, GCLOCK codec weighting, (later) huge pages.

**D. A/B**
- Arms: cache OFF (no `PageCache*` passed — the code path provably never runs)
  vs ON. Interleaved, arm order alternating per round, fresh process per round.
  Inside a round, the process runs the workload N times so cold (run 1) and hot
  (runs 2..N) are reported **separately**.
- Workloads:
  1. ClickBench official protocol (restart per query; tries 2-3 hot). The plan
     says hot-only overstates, so it is never the sole number.
  2. Repeated mix: same dashboard query ×N; the full 43-query sequence in one
     process (cross-query column reuse).
  3. **Budget sweep:** 25 / 50 / 100 / 200% of the A1 working set. Shows the
     undersized cliff, and **must show ON ≥ OFF when thrashing** (the
     admission policy's job).
  4. Cold penalty: run 1 ON vs run 1 OFF (allocation + first-touch faults).
  5. Remote (AIStor targets) only if T0 is ruled in.
- Controls the change cannot touch, read first: queries answered from footers
  (`COUNT(*)` statistics-only), uncompressed-column-only queries, and the whole
  skene suite (parquet-only cache). If they move, the harness is wrong.
- Knob proof before any long or remote run: per-query cache telemetry (lookups,
  hits, misses, admitted bytes, refused bytes, evictions, resident bytes) read
  back. The OFF arm must report zero lookups.
- Correctness: answers identical ON/OFF on every query; unit tests for pin vs
  evict under concurrency and for the budget invariant; a rewrite-at-same-path
  test only if D-C7-4 says local same-path rewrites are in contract.
- Isolation: every other candidate's benchmarks run with the cache OFF, and
  their harness asserts zero lookups.
- Decision: §6 table of the plan. Slower ⇒ deleted (including a cold-run
  regression unless the architect rules cold runs out of the decision).

## 11. Decisions for the architect

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D-C7-1 | Tiers | T1 only / T1 + remote in-RAM T0 (reopens the 2026-07-10 "no data-file cache" ruling) / T2 | T1 only; T2 ceiling measured only; T0 a separate ruling |
| D-C7-2 | Location | A static in rugo / B virtual hook / C concrete `rugo::PageCache` class, instance owned by opteryx in pool_reader.so | C |
| D-C7-3 | Granularity | per page / per column chunk (one lookup per chunk, could skip IO, but breaks with page skipping) | per page |
| D-C7-4 | File identity | path only (standing immutability ruling) — confirm it also covers local ad-hoc files rewritten in-process | path only, per the ruling |
| D-C7-5 | Budget default | off unless configured / fraction of cgroup-or-RAM (which fraction?) | off unless configured until D-phase data exists; then rule a fraction |
| D-C7-6 | Memory pressure | fixed budget / yields to query memory | fixed budget first; yielding is its own design |
| D-C7-7 | Admission | first miss / second touch / working-set fits | working-set fits |
| D-C7-8 | Eviction | CLOCK / GCLOCK by codec cost | CLOCK + GCLOCK as an ablated part |
| D-C7-9 | Concurrency | global mutex / striped shards; single-flight or not | striped shards, no single-flight |
| D-C7-10 | Direct I/O / io_uring | in C7 / separate candidate / not pursued | not in C7 |
| D-C7-11 | Skene | parquet only / also cache skene decompressed sections | parquet only (skene is the control suite) |
| D-C7-12 | Config surface | budget is a permanent config value (0 = off), not a kill switch | permanent budget config; no separate on/off switch |
| D-C7-13 | Cold runs in the verdict | hot-only / hot and cold both count | both count; a cold regression needs an explicit ruling to accept |
| D-C7-14 | Ceiling tool | build the page-preserving decompress-only rewrite in `dev/` (A3) | yes — it is the only zero-engine-change wall bound |
| D-C7-15 | Cloud Run evidence | read existing telemetry only / add process identity to telemetry | read first; add only if absent, with a ruling |
| D-C7-16 | Telemetry fix A0 | fix the untimed pool-path decompress now / perf only | fix now (small, needed by every later step) |

Nothing proceeds past §10 step A without rulings on D-C7-1, -14 and -16 at minimum.

## 12. Rulings (2026-10-06)

- **D-C7-1 — tiers:** cache the decompressed pages (Snappy/ZSTD/etc. undone),
  for **local and remote** files. No compressed tier, no decoded tier. Note: for
  remote files a T1 hit saves decompression but the column chunk is still
  fetched (the GET happens before decode).
- **D-C7-4 — file identity:** files are immutable for the life of the process;
  the engine never updates or edits a data file. Key = path + page offset.
- **D-C7-5 — budget:** 20% of memory initially (cgroup limit when set, else
  physical RAM).
- **D-C7-14 — ceiling tool:** the decompress-only rewrite tool goes in
  `scratch/`, not `dev/`.
- **D-C7-16 — telemetry:** fix now. DONE: `decode_column.cpp` pool path now
  accumulates `decompress_ns`. Proof: a non-dict REQUIRED int64 Snappy file
  (only the pool path can run: `page_parallel_s` 1.5 ms) reports
  `decompress_s` 3.2 ms (was structurally 0). `make q` passes.
- **D-C7-13 — cold-run fill cost:** accepted. Separately: **the cache is OFF
  when its budget is under 1 GiB** (small installs would pay the fill cost and
  thrash). At 20%, that means hosts under ~5 GiB run without a cache.
- **D-C7-2 — location:** a performance question, decided by what is fastest
  across the benchmarks. Note for the measurement: options A and C compile to the
  same thing (a direct call to a concrete class); B adds one indirect call per
  page. The arms are therefore "direct" vs "indirect". A is still ruled out by
  the duplicate-instance trap (pool_reader.so + rugo_native.so), not by speed.
- **D-C7-6 — memory pressure:** the cache must release memory quickly when
  queries need it. The mechanism still needs a design (§13).
- **D-C7-7/8/9 — admission / eviction / concurrency:** reuse existing code
  (`opteryx::LRU2`, `src/cpp/lru2.hpp`) only where it saves effort. Each policy
  part (admission rule, eviction weighting, sharding) is kept only if
  leave-one-out shows the suite is faster with it; otherwise it is deleted.
- **D-C7-10 — direct I/O / io_uring:** not now; maybe later.
- **D-C7-11 — skene:** no (skene may be removed).
- **D-C7-12 — config:** a fixed percentage of the host's memory (20%), not a
  separate on/off switch.
- **D-C7-15 — Cloud Run:** no control over it; from experience an idle instance
  lives ~15 minutes before it is killed. So in prod a cache only helps queries
  that land on the same instance within ~15 minutes of each other.
- Still open: D-C7-3 (page vs column-chunk entries), and "host" for D-C7-12 (§13).

## 13. Notes on the open points

- **"Host" memory on Cloud Run.** Inside a container, physical RAM is the
  underlying machine's (often far more than the instance is allowed), so 20% of
  it can exceed the instance's own limit and get the instance killed. Proposed
  reading: "host" = the memory the process may use, i.e. the cgroup limit when
  one is set, else physical RAM. Needs confirming.
- **Why `LRU2` does not save effort as the store:** values are `std::string`,
  `get_into` copies the value out, there is no lock and no pinning. A hit would
  memcpy a ~1 MB page; that copy costs a large part of what the hit saves. Its
  *policy* (admit/promote on second access) is still a candidate admission
  rule, tested by leave-one-out like the others.
- **Fast release (D-C7-6), candidate mechanism:** whoever reserves query
  memory (the agg/spill budget modules) calls `PageCache::release(bytes)`
  before allocating when free memory is short; the cache evicts unpinned
  entries until that many bytes are freed (`free()` of large blocks returns them
  to the OS immediately). Pinned pages are only the few being decoded right now,
  so nearly all of the cache can be released at once. To be designed properly
  once the ceiling holds.

## 14. Rulings, round 3 (2026-10-06)

- **Budget:** 20% is the **default** and is configurable. The cache is off when
  the resulting budget is under 1 GiB.
- **Host = the container** (the instance doing the work): the cgroup memory
  limit when one is set (`get_cgroup_memory_limit_bytes()`), else physical RAM.
- **Eviction vs admission are separate.** LRU-style (LRU-2 / CLOCK) is the
  eviction policy. **Admission is cost-based: admit the most expensive pages
  first.** Inputs suggested by the architect (ideas, not settled): size, codec,
  compressed vs uncompressed bytes. Proposal to add to the list: the cost does
  not have to be predicted — on a miss the decompression has just been timed
  (the A0 telemetry), so the real ns spent per byte to be held is known at
  insert time. Candidate rule: admit while the budget has room; when full, admit
  only if the new entry's cost per byte beats the entries eviction would remove.
  Each input is a part judged by leave-one-out.
- **D-C7-3 — granularity: leaning per column chunk**, the unit we do IO on.
  Consequences to settle before building:
  1. **Remote IO can be skipped.** If a chunk's decompressed pages are all
     cached, the compressed bytes are never needed, so the GET can be skipped
     too, not only the decompression. That needs a cache lookup before the
     fetch-ahead stage issues the read (`io_pipeline.hpp`), not inside
     `DecodeColumnFromChunk`. This is the biggest prod win available (prod's
     floor is GETs), and it changes where the hook sits.
  2. **Partial chunks.** Several paths decode only some pages of a chunk
     (`PageJumpPlan`, dict pruning, raw-page LIKE search, row masks). Options:
     (a) a chunk entry is only inserted when every page was decompressed —
     partial decodes don't populate the cache; (b) the entry keeps per-page
     presence and fills over time — a hit is only "IO-free" when complete;
     (c) on a miss, decompress the skipped pages too so the entry is complete
     (extra work on the cold run).
  3. Entry contents: the decompressed pages + parsed page headers + the
     dictionary page, so a hit needs nothing from the file.
- **Partial chunks — ruled (a)** (2026-10-06): only fully decompressed chunks
  are cached; partial decodes add nothing. Raised by the architect: also keep
  the *compressed* bytes of pages that were fetched but skipped. Assessment in
  the reply: pointless locally (the OS page cache already holds them); for
  remote it saves a later GET but adds a second entry form (per-page state
  decompressed / compressed / absent, a hit that still decompresses, eviction
  weighing two forms — Pivot's model). Proposed: a later, separate part,
  measured on remote (AIStor) only, kept only if leave-one-out pays.

## 15. Ceiling phase — results (2026-10-06, in progress)

Tools (scratch, not production): `scratch/c7_decompress_rewrite.py` (census +
page-preserving decompress-only rewrite), `scratch/c7_ceiling_probe.py`
(answer check + interleaved A/B), `scratch/c7_ceiling_summary.py`.

**A0** done (§12). **A1 footer census** — bytes a full T1 cache must hold
(`total_uncompressed_size`, all row groups, no pruning):

| dataset | files | rows | compressed | decompressed | ratio |
|---|---|---|---|---|---|
| canon `scratch/hits` (Snappy) | 100 | 99,997,497 | 13.72 GiB | 33.18 GiB | 2.42 |
| `scratch/hits_rugo_262k` (ZSTD, some UNCOMPRESSED) | 8 | 99,997,497 | 7.73 GiB | 21.14 GiB | 2.74 |

Largest columns, canon: URL 7.74 GiB, Title 7.30, Referer 5.96, OriginalURL
4.92 — everything else under 1 GiB. rugo: URL 4.40 (dictionary encoded, so
smaller than canon's PLAIN 7.74), OriginalURL 3.84, Referer 3.31, Title 2.53.

Per-query working set, canon (referenced columns, no pruning; 1-based query
numbers as in the runner): most queries < 2 GiB; the URL/Title/Referer queries
are 6-16 GiB (Q21 7.74, Q22 8.49, Q23 16.07, Q24 8.33, Q28 7.74, Q29 5.96,
Q34/Q35 7.74, Q37-39 7.3-7.75, Q40 13.77). At the ruled 20% budget:
a 16 GiB container (3.2 GiB cache) holds **none** of the URL-family queries;
the whole canon suite needs ~33 GiB of cache, i.e. a ~166 GiB container.
c6a.4xlarge (32 GiB → 6.4 GiB cache) holds the < 6.4 GiB queries only.

**A3 rewrite verified:** decompress-only copies `scratch/hits_c7plain` (33 GiB)
and `scratch/hits_rugo_262k_c7plain` (22 GiB). OffsetIndex: none in either
dataset. All 43 answers identical on a canon file and on the full rugo
dataset; the queries that differ under LIMIT/OFFSET differ equally when a
dataset is compared with itself (ties), and their full results (LIMIT/OFFSET
removed) are identical.

**A3 wall ceiling — Mac M5 Pro** (full 100M-row datasets, default DOP, 5
interleaved rounds alternating arm order, fresh process per arm, 4 runs per
query with run 1 discarded — warm OS page cache for both arms; A = original,
B = decompress-only copy = a T1 hit without lookup cost):

| dataset | suite A | suite B | B/A | rounds B faster | Q28 B/A | biggest wins |
|---|---|---|---|---|---|---|
| canon (Snappy) | 8.39-8.93 s | 7.85-8.38 s | **0.916** | 5/5, ranges separate | 0.876 | Q21-23 LIKE 0.67-0.75, Q25 0.77, Q39 0.77 |
| rugo_262k (ZSTD) | 7.49-7.63 s | 6.26-6.63 s | **0.857** | 5/5, ranges separate | 0.622 | Q21-23 0.54-0.62, Q28 0.62, Q24/25/27 ~0.7, Q34/35 ~0.79 |

- Controls answered from footers (Q1, Q3/Q4/Q7 on rugo) and the
  CPU-bound GROUP BY queries without heavy decompression (Q16, Q33, Q36) are
  flat (0.97-1.03): the harness is not moving untouchable queries.
- Q28 on canon is 0.876, far below the 35.4%-of-CPU (≤1.55×) bound from the i5
  profile: on the Mac the page read from memory costs a large part of what the
  decompression cost (§2 caveat 2), and Q28 is no longer decompress-dominated
  after the length-only stub decode.
- Small CounterID=62 queries (Q37-43) gain 3-13% on canon with almost no
  decompression time recorded (Q39 0.77 with 14 ms decompress); not explained
  yet — candidates: page-search/pruning reads raw pages, fewer distinct mmaps.
- Note: the suite is a lower bound on what a cache would see across the whole
  ClickBench protocol only if the cache could hold the working set — at 20% of a
  64 GiB Mac (12.8 GiB) the 7-16 GiB URL-family working sets (§15 A1) only
  partly fit.

## 16. Pivot's cache budget (read from source, github.com/pivotlake/pivot HEAD, 2026-10-06)

- **Budget:** `bin/src/memory.rs` — `DEFAULT_MEMORY_PCT = 80` of **physical**
  memory, minus `OVERHEAD_RESERVE_BYTES = 4 GiB` (marked HACK). Configurable as
  bytes or %. The ClickBench entry sets none, so the default ran.
- **One pool for everything:** the budget is a single "ring" of 2 MB slots
  (`dispatch/src/memory/mod.rs`: "we're the only important process on the
  machine"), **faulted in whole at boot** (the server refuses to start if free
  memory can't back it). The compressed cache, the decompressed cache AND query
  working memory (vectors, hash tables, write buffers) all take slots from it.
  There is no separate cache budget: the cache is whatever the queries are not
  using, and a worker that needs a slot evicts one (CLOCK). NUMA-split per node.
- **Sizes:** c6a.4xlarge 32 GiB → 25.6 − 4 = **21.6 GiB** pool; canon URL
  decompressed (7.74 GiB) fits. c6a.xlarge 8 GiB → 6.4 − 4 = **2.4 GiB**; it
  does not → hot 7.4 s ≈ cold 9.1 s. Consistent with the published numbers.
- **Entry rules are loose, not strict:** everything read is admitted. CLOCK
  with a lives counter (+1 per touch, cap 16, `PIVOT_MAX_LIVES`), so one-shot
  inserts die first. Compressed tier target 30% of cached slots
  (`PIVOT_COMPRESSED_CACHE_PCT`). When one copy of some bytes is evicted, the
  surviving copy gets +6 per touch (`PIVOT_REINFORCE_BUMP`) so the tiers
  complement rather than die together.
- **Decompressed entries:** keyed by (file, page byte offset, span); a
  column-chunk `get_range` reports which pages are present and which are gaps.
  Pages are decompressed **directly into** a bump-allocated region of an
  already-faulted 2 MB slot — no per-entry malloc, no first-touch faults, so a
  fill costs a cold run almost nothing.

Implications for C7 (for the architect, not decided):
- The gap in budget is 80% − 4 GiB shared with queries vs our ruled 20%
  dedicated. On the ClickBench box that is 21.6 GiB vs 6.4 GiB.
- Their "fast release" (D-C7-6) is structural: query memory and cache are the
  same slots, so a query that needs memory takes it by eviction.
- Pre-faulted slab slots remove the cold-run fill cost (D-C7-13) we accepted.
- Per-page keys with a chunk-range lookup is a hybrid of D-C7-3's options.

**A3 wall ceiling — i5-8500 (x86, prod ISA)** — tree `~/c7-20261006/tree`
(current source incl. the A0 fix), same protocol, 5 rounds. RAM limits the
data to subsets: canon hits_0..19 (20M rows), rugo part-000/001 (25M rows).
Answers checked identical (Q1/3/21/28/34) on both.

| dataset | suite A | suite B | B/A | rounds B faster | Q28 B/A | biggest wins |
|---|---|---|---|---|---|---|
| canon 20 files (Snappy) | 13.16-13.28 s | 11.47-11.55 s | **0.868** | 5/5, ranges separate | 0.869 | Q21-23 0.49-0.60, Q39 0.64, Q24/25/27 ~0.72 |
| rugo 2 files (ZSTD) | 14.59-17.69 s | 13.38-13.47 s | **0.920** | 5/5 (one noisy A round) | 0.788 | Q21-23/24/25/27 0.69-0.75 |

- Controls (Q1, Q3/Q4/Q7 rugo, Q16, Q33, Q36) flat at 0.98-1.01.
- **The profile-based bound was wrong, on x86 too.** Canon Q28: telemetry
  decompress_s 1.40 of 2.72 CPU-s (51%), yet removing decompression saves only
  0.53 CPU-s (2.72 → 2.19) and 0.869× wall. "Decompress time" includes faulting
  and reading the compressed bytes, and the decoder then streams the
  decompressed bytes from DRAM instead of L2. **A T1 hit is worth roughly a
  third of the measured decompression time on these queries**, not all of it.
- Ceiling summary, both arches: suite 0.86-0.92 (8-14%); Q28 1.14-1.27×; LIKE
  queries up to 2×. It passes the 5% gate everywhere — but only when the
  working set fits, which at 20% it mostly does not (§15 A1).
- Correction to the Pivot comparison given in chat: "decompression-free, our
  CPU per Q28 ≈ Pivot's" is wrong. Decompress-free canon Q28 on the i5 is
  2.19 CPU-s per 20M rows (~11 CPU-s per 100M) vs Pivot ~4 vCPU-s per 100M on
  c6a — cross-hardware and rough, but a per-row gap of ~2-3× remains on top of
  the cache and scaling.

## 17. Mapped-file cache (was "tier M") — process-lifetime file-mapping cache (from the Q28/scaling hand-off, 2026-10-07)

Source: `scratch/C7_HANDOFF_mmap_tier.md` (Q28 / scaling session). Proposal only;
nothing built. That session owns the `io_pipeline.hpp` mmap / destructor lines
(option A, `MappingReaper`, ~1860-1910 and ~3880-3895) — C7 does not edit them.

**Finding it rests on (theirs, measured):** on x86, `RUGO_LOCAL_MMAP_CACHE_DEFAULT=1`
maps each local file whole once per pipeline, and `~ParquetIOPipeline()`
unmaps every file serially on the query's critical path: ~0.65 ms per file on
the i5, ~70 ms of a ~400 ms Q28 on the 32-core c7a. ARM unmaps per row group
inside the workers and doesn't show it. Turning the per-query mmap cache off
moves the cost into the workers rather than removing it.

**The tier:** a cache of whole-file `PROT_READ | MAP_SHARED` mappings that
outlives the query, keyed by path (+ size + mtime_ns in the hand-off; under the
D-C7-4 ruling path alone is enough).
- **It owns no memory.** Resident pages stay kernel page-cache pages (shared,
  reclaimable, not OOM-able); the cache holds address space and page-table
  entries (~8 B per touched 4 KiB page, ~5 MB for Q28's 2.6 GB read set).
  So the 20% budget, the < 1 GiB off-switch and the memory-release ruling
  (D-C7-6) do not apply to it. Its limits are `vm.max_map_count` (65,530 on
  the i5) and `RLIMIT_AS`.
- **Removes per query:** open + mmap per file, the teardown munmap, and the
  soft page faults on pages an earlier query already touched (page tables stay
  populated).
- **Needs refcounts or an epoch:** eviction must not unmap a mapping another
  query is still slicing. Evicted mappings go to option A's reaper
  (`submit(vector<pair<void*, size_t>>&&)`) so no query pays an unmap.
- **Single instance:** `io_pipeline.hpp` is header-only and compiled into both
  `pool_reader.so` and `_operators.so`; a `static` in rugo exists twice. The
  hand-off proposes the **draken `.so` behind a C bridge** (draken is one `.so`
  and ships in both wheels). That also answers D-C7-2 for T1 better than my
  `pool_reader.so` proposal (§3): one home for both tiers, valid in the
  standalone rugo wheel.
- **Per architecture:** ARM's per-row-group maps measured 1.02-1.08× slower
  with the whole-file cache (never explained). First cut x86-only via the D3
  mechanism, A/B on both arches.

**Why it matters for C7's verdict:** the i5 ceiling (§15) showed a T1 hit
recovers only ~1/3 of the measured decompress time, because that time includes
faulting in and reading the compressed bytes. Part of that fault cost is
exactly what tier M removes — with **no memory budget**, so it doesn't hit the
"working set doesn't fit at 20%" wall that sinks T1. Their teardown finding is
a further, separate saving on top.

**Ceiling for tier M before building (proposal):**
1. Teardown share: option A's reaper result measures it directly (theirs).
2. Fault share: per query, soft faults (`ru_minflt`) × measured cost per fault,
   back-to-back queries in one process; plus the mmap/open cost per file.
   On the i5 and the Mac, canon 20 files, Q28 + the LIKE queries + controls.
3. Gate as usual: build only if 1 + 2 clear the ±5% band on the i5.

**Decisions for the architect (tier M):**

| # | Decision | Options |
|---|---|---|
| D-C7-M1 | Does tier M join C7 (or replace T1 as C7's first build)? | M only / M then T1 / T1 only / close C7 |
| D-C7-M2 | Cap shape | files / mapped bytes / both; defaults; container-aware or not |
| D-C7-M3 | ARM | x86-only first cut / both from the start |
| D-C7-M4 | Home | draken `.so` behind a C bridge (hand-off) — and T1 there too? |
| D-C7-M5 | Reaper contention | `munmap` takes the process mmap lock for write; a reaper unmapping while another query faults can stall it — accept, or bound reaper work? |

**Rulings / answers (2026-10-07):**
- **Name and goal:** "tier M" is renamed the **mapped-file cache**. Functional
  goal: keep local data files open and mapped between queries, so a query that
  reads files an earlier query read does not pay again to open them, map them,
  fault their pages into the process, or unmap them at the end.
- **D-C7-M2 cap:** a file count, well below `vm.max_map_count` — ~4k files.
- **D-C7-M3:** x86 first (it looks mostly like an x86 issue).
- **D-C7-M4 home — advised: draken `.so`, behind C functions.** The deciding
  constraint: the caller is rugo's `io_pipeline.hpp`, which is compiled into
  several `.so`s, and rugo may depend only on draken (rugo is opteryx-free and
  ships standalone). draken is one `.so` per process and ships in both wheels,
  and the trace bridge already uses this pattern. An opteryx `.so`
  (pool_reader, thread_pool) would make rugo depend on opteryx. Interface
  sketch: `acquire(path) -> {base, len, handle}` (refcount +1, maps on miss),
  `release(handle)` (refcount −1); the cap is set once at init by the owner.
  An entry is evicted only at refcount 0 and is handed to the reaper.
- **D-C7-M5 unmap stalls:** yes — the deferred release (the other session's
  reaper, option A) is the answer: no query pays an unmap. What remains is
  that the kernel's `munmap` still takes the process mapping lock while the
  reaper runs, so a query faulting pages at that moment waits briefly. With the
  cache, unmaps only happen on eviction (rare under a 4k cap), so no extra
  mechanism unless a measurement shows a stall.

**Mapped-file cache ceiling — i5 (2026-10-07, idle box, tree `~/c7-20261006/tree`
= no reaper, canon 20 files, default DOP, 6 back-to-back runs, run 1 discarded;
`scratch/c7_mapped_file_probe.py`):**

| q | wall ms | CPU s | minflt (all kinds) | teardown ms (inline munmap) |
|---|---|---|---|---|
| 3 (control) | 94 | 0.48 | 20,035 | 0.4 |
| 16 | 254 | 1.09 | 10,710 | 2.3 |
| 21 | 261 | 1.36 | 5,824 | 14.3 |
| 22 | 311 | 1.63 | 6,710 | 16.3 |
| 23 | 740 | 3.95 | 19,165 | 36.4 |
| 28 | 515 | 2.81 | 40,891 | 14.4 |
| 34 | 983 | 5.27 | 34,510 | 14.3 |
| 36 | 238 | 1.01 | 554 | 1.7 |

Micro (same 20 files, page-cache warm): open+fstat+mmap **19.6 µs/file**;
soft fault **3.7 µs** each, mapping 16 pages per fault (Linux fault-around,
64 KiB); munmap+close of a fully touched file 5.8 ms.

What the cache would remove, Q28 (worst case for it among these):
- open+mmap: 20 × 19.6 µs = **0.4 ms**.
- file page faults: `minflt` counts every fault (heap, fresh anonymous memory
  too — Q3 reads almost nothing yet shows 20k). File faults are bounded by bytes
  mapped: ~0.5 GB of compressed URL / 64 KiB ≈ 7.6k faults × 3.7 µs ≈ **28 ms of
  CPU ≈ 1% of Q28's 2.81 CPU-s**; spread over the workers, < 1% of wall. Q21:
  even all 5.8k faults = 21 ms CPU, 1.6%.
- teardown munmap: 14.4 ms on the critical path — **already taken off it by the
  other session's reaper** (14.4 → 0.7-7.6 ms). The cache would also save the
  reaper's 0.7 ms/file of background CPU (~14 ms here).

**Verdict: beyond the reaper, the mapped-file cache's ceiling is ~1-2% on the
i5 — under the ±5% gate.** The real cost was the serial teardown, and the
reaper already removes it from wall time. Repeat faults are cheap on Linux
because fault-around maps 16 pages per fault. Recommend: not built; recorded as
measured.

## 18. Production memory check (2026-10-07, Cloud Monitoring, read-only)

Ruling under test: budget = 80% of the container − 4 GiB, off below 2 GiB.
Prod engine = Cloud Run `worker-opteryx-app-git` (project `mabeldev`, us-east1):
**16 GiB, 4 vCPU, concurrency 1, min instances 0**. Metric
`run.googleapis.com/container/memory/utilizations`, p99 per window, max across
instances.

| window | resolution | peak |
|---|---|---|
| last 24 h | 1 min | **20.9%** (3.3 GiB); hourly p99 never above 10% |
| last 7 days | 1 h | 17.2% |
| last 30 days | 1 min | **83.9% (13.4 GiB)** on 13 Sep; 82.9% 11 Sep; **81.9% 30 Sep**; 77.9% 27 Sep; 59% 1 Oct |

- Spikes are rare: 66 minutes above 4 GiB in 30 days, 31 above 7.2 GiB. The
  last 5 days stayed ≤ 31%.
- The top spikes sit at :26 past 00/06/12/18 h — a 6-hourly pattern (scheduled
  work?), not established.
- **A dedicated 8.8 GiB cache would have run the instance out of memory on at
  least 4 days of the last 30** (8.8 + 13.4 > 16 GiB). Sizing a dedicated cache
  to the 30-day peak leaves 16 − 13.4 = 2.6 GiB minus headroom → under the
  2 GiB rule → off.
- So on prod's 16 GiB the cache only works if it **gives memory back** when a
  query needs it (Pivot's shared-pool model / the "release quickly" ruling),
  not as a fixed reservation.

## 19. Rulings — budget and pressure (2026-10-07)

Supersedes the 20% default (§12/§14) and the 80% − 4 GiB proposal (§18).
- **Budget = 70% of the container − 4 GiB** by default. Both numbers (the
  percentage and the reserve) are **configurable and visible in VARIABLES**
  (`SHOW VARIABLES`). Cache off if the result is under 2 GiB. Container = cgroup
  limit when set, else physical RAM. Prod 16 GiB → 7.2 GiB.
- **Compaction:** a compaction statement flushes the cache before it runs and
  its reads are never admitted (planning decides, execution acts on a flag).
- **Give-way release as a safety net:** large query-memory reservations ask the
  cache to free that many bytes first (agg/spill budget sites are the candidate
  hook — not yet checked that every large allocation passes through them).
- **Residual risk accepted:** a spike that bypasses the release or outruns it
  (prod, 30 days: at most one minute, 14 Sep 10:55, 49%).

## 20. Give-way release — code read (2026-10-07)

- No central query-memory accounting exists. `engine/agg_budgets.hpp` /
  `engine/spill_budgets.hpp` are fixed per-operator ceilings (MEDIAN,
  ARRAY_AGG, CIDR_AGG, spill flush/ceiling) each sink checks against its own
  buffer.
- `draken/core/alloc.h` (`draken_malloc/calloc/realloc`) is one entry point for
  draken-owned buffers, plain `malloc` underneath, no accounting. Most large
  query memory bypasses it: `src/cpp/engine` has 81 `draken_*` alloc calls vs
  1,621 `std::vector`/resize/reserve sites; rugo parquet 1 vs 735; carchar 0 vs 95.
- Options: (1) reroute large allocations through draken and release from there
  (Pivot's model; large refactor); (2) RSS + cache check at native morsel
  boundaries (small; reactive, a single big resize can outrun it); (3) cgroup
  memory-pressure / PSI notification (reactive, Linux-only); (4) put decompressed
  pages in a file on ephemeral disk and read via the OS page cache, so the
  kernel reclaims them under pressure (needs a real disk; cold-run write cost;
  competes with spill space).
- Recommended: (2) as the safety net with the residual risk accepted; (4)
  ceiling-tested later; (1) is its own programme. Awaiting ruling.

## 21. Step 1 for give-way option 1a — where peak query memory sits (2026-10-07)

Tool (scratch): `scratch/c7_peakmem.c` — a malloc-interposing shim (LD_PRELOAD /
DYLD_INSERT_LIBRARIES) recording every allocation ≥ PEAK_MIN with its call stack
and snapshotting live bytes per call site at the process's peak;
`scratch/c7_peakmem_run.py` runs one query per process. Mac: ClickBench canon
100 files + TPC-H SF10. i5: debug-symbols build (`~/c7-20261006/tree-sym`),
canon 20 files + TPC-H SF10, sites resolved with addr2line.

Findings:
- **At ≥ 1 MiB the tracker misses most GROUP BY memory** (Mac CB33: 10.0 GiB max
  RSS, 0.85 GiB tracked). At ≥ 64 KiB it sees most of it (CB19 6.4 of 7.6 GiB,
  CB33 6.0 of 10.0), in ~130-150k allocations of 64-200 KiB per query. A charge
  threshold for 1a must be ~64 KiB, not 1 MiB (~150k atomic adds per query).
- **Where it is (i5, symbolised), in the GROUP BY sink, all from
  `GroupBySink::sink` (`native_group_sinks.hpp` ~3800-3972):**
  1. key store — `draken::AppendBuffer<uint8_t>::extend` (:3937/:3953) → already
     `draken_realloc`;
  2. per-group aggregate lanes — `std::vector<uint64_t/int64_t/__int128>` (:3803,
     :3935, :3968, :3972) → std allocator;
  3. `opteryx::carchar::CarcharIndex` slots (`carchar_index.hpp:162`,
     `std::vector<Slot>`) → std allocator;
  4. `GBPartition` vector (`maybe_flush`, :3582) → std allocator, small.
- **Outside GROUP BY:** the scan's `opteryx::MemoryPool` (created in
  `open_native_scan_plan`, 256 MiB-15 GiB **reserved**, mostly untouched — Mac
  CB24 15.2 GiB reserved vs 0.95 GiB max RSS); join build `gather_rows`
  (`sort.hpp:764`, `draken_malloc`); rugo decode buffers (`AppendBuffer` →
  draken) and `build_direct_string_dict` (`pool_sink_draken_alloc` → draken).
- TPC-H SF10 peaks are modest (≤ ~2 GiB max RSS on both boxes); the big peaks
  are high-cardinality GROUP BY (CB19, CB33, CB34/35).
- The remaining gap between tracked bytes and max RSS (e.g. i5 CB33 1.9 vs 1.38
  GiB) is allocations < 64 KiB, Python, and — on Linux — touched pages of the
  whole-file mmaps, which count in RSS but are page cache.

**Size of 1a:** "four, not forty". Most large query memory already goes through
the single `draken_malloc/realloc` entry point. What doesn't is the GROUP BY
lanes and the carchar index (std::vector) — switch those to a tracked
allocator — plus a decision on the scan `MemoryPool` reservation (charge what
is touched, or size it smaller). Join build and decode are already draken.

## 22. Scan MemoryPool — reserved vs used (2026-10-07; ruled: shrink it)

Telemetry added (built, `make q` passes): `PoolStats.peak_used_size`
(`src/cpp/memory_pool.{hpp,cpp}`, `memory_pool.{pxd,pyx}`, test in
`tests/unit/core/test_memory_pool.py`); `NativeScanPlan.diagnostics()` reports
`scan_pool_reserved_bytes / _peak_used_bytes / _commits / _failed_reservations`;
`compiler.py` sums them per query. Probe: `scratch/c7_scan_pool_probe.py`.

Sizing today (`pool_reader.pyx:3288`): largest projected row group
(uncompressed) × 2 × (in-flight window + 1), floor 256 MiB, one malloc.

Mac results:
- ClickBench canon, 43 queries: **75 GiB reserved in total, 0 bytes used**
  (every column direct-path).
- TPC-H SF10: pool used by DECIMAL columns — TQ01 246 of 324 MiB (76%), TQ06 111
  of 256 (43%), the rest ≤ 7%; TQ04/12/13/16/21 0. No failed reservations.
- List columns (positive control): `flat.unnest_bench` 46 MiB of 321 MiB,
  `array_types` 1.4 KB of 256 MiB, tweets 0.5 MiB of 813 MiB.

Implications: the 256 MiB floor (and any reservation for scans with no
pool-eligible column) is pure waste; the formula is not uniformly oversized
(TQ01 76%), and pool bytes per value exceed parquet's uncompressed width for
decimals, so a new formula must count pool-eligible columns at their SERIALIZED
width. Next: what a failed reservation does today (must fail loud).

**Failed reservation today (code read, 2026-10-07):** `reserve_for_write` →
no fit → compaction → still none → `failed_commits++`, `{-1, nullptr}` (scan
pool has auto_resize off) → decode worker sets `result.success=false`,
"MemoryPool exhausted serializing column: <name>" (`io_pipeline.hpp` ~3277) →
`NativeParquetScanSource` (`:1594-1608`) `err.code=1`, query FAILS LOUD. No
back-pressure: entries are released by the consumer after building the column
(`native_*_pool_decode.hpp`), so a pool smaller than the in-flight window's
need turns a transient shortage into a query failure. Pool-path choice is made
at decode time (`direct_kind_for`), so plan time sees only type-driven cases
(decimal, list, struct), not data-driven residual shapes.
Options: (1) lazy allocation — malloc on first reserve; zero risk, removes the
reservation from every scan that never uses the pool (all of ClickBench, 5 of
22 TPC-H); (2) type-based tighter sizing at serialized width + floor (risks new
exhaustion failures; needs A/B with failure telemetry); (3) back-pressure
(design change, deadlock risk). Recommended (1) first. Awaiting go-ahead.

## 23. Phase A built — process memory account (2026-10-07)

- `draken/core/mem_account.{h,cpp}`: one account per process, compiled into
  `draken_native` only (consumers resolve `draken_mem_*` at load; verified by
  `nm`: one `T`, every consumer `U`). Standalone native builds carry their own
  copy: skene/Makefile, and the Makefile's `DRAKEN_KERNEL_SRCS`, JSON bench,
  `decoded-column-reset-test`, `rle-dict-test`.
- `draken/core/alloc.h`: malloc/calloc/realloc/aligned/free charge ≥ 64 KiB by
  usable size. TEMPORARY A/B switch `DRAKEN_MEM_ACCOUNT=0` (remove when banked).
- `draken/core/tracked_allocator.h`: `TrackedAllocator` (value-init, std
  semantics) for GROUP BY partition/lane arrays + key validity;
  `TrackedUninitAllocator` for carchar `slots_` (was uninitialized_allocator).
- Scan `MemoryPool`: allocated lazily on first fit; used bytes charged.
- Telemetry: `memory_charged_peak_bytes`, `memory_charged_end_bytes`.
- Coverage (Mac, canon 100 files): charged peak / max RSS — CB19 4.95/7.13,
  CB33 5.43/9.56, CB17 1.85/3.62, CB34 7.08/5.86 (capacity > touched); end
  charge ≤ 0.15 GiB (pairs balance).
- Tests: make q, dt (3596), st (57), rt (2067 after the standalone-link fix) pass.
- A/B on vs off (Mac, 5 rounds, alternating): ClickBench median 9.16 → 9.22 s
  (on slower 4/5, ranges overlap), TPC-H SF10 neutral (on faster 3/5). Neutral
  within noise; possible ~0.5-1% on ClickBench from usable-size lookups on free.
Next: Phase B (the cache) on top.

## 24. Phase B built — the chunk cache (2026-10-07)

- `draken/core/chunk_cache.{h,cpp}` (draken_native only): one entry = one
  column chunk, every page decompressed into one buffer + an index by page
  offset within the chunk; key = (path, chunk's absolute first byte). 16 shards,
  CLOCK eviction, admission by measured decompression ns per byte (a new entry
  never evicts a resident worth more per byte). Pin count + EVICTED bit in one
  atomic word, so exactly one side frees an entry. Chunks a predicate decode
  left incomplete are remembered (no refill by predicate decodes).
- Give-way in `draken/core/mem_account.cpp`: the cache holds at most
  `min(B, C - R - charged)`; `draken_mem_charge` shrinks it on the charging
  thread. Config: `CHUNK_CACHE_MEMORY_PERCENT` (70) and
  `CHUNK_CACHE_RESERVE_BYTES` (4 GiB); container = cgroup limit else RAM;
  B < 2 GiB → off. Applied once at `import opteryx`. VARIABLES:
  `chunk_cache_memory_percent`, `chunk_cache_reserve_bytes`,
  `chunk_cache_budget_bytes` (SERVER), `chunk_cache_admit` (USER, session).
- rugo: `DecompressIntoRaw`; `DecodeColumnFromChunk(..., const ChunkCacheRequest*)`
  — all four decompression sites (dict page, parallel pages, serial v1, v2
  values) served from a hit or decompressed straight into the fill buffer;
  masked/jump-plan decodes never fill; `commit()` header-walks the chunk and
  inserts only when every compressed page has a slot. Byte-array dictionaries
  are copied into the output arena (never aliased). The pipeline passes the
  request at its three decode sites (mmap, remote, pread).
- `chunk_cache_admit = false`: `execute_native` flushes the cache and sets every
  scan plan to no-fill before the engine starts (for compaction jobs — the
  compaction caller must SET it; compaction runs outside this repo).
- Telemetry per query: `chunk_cache_{bytes,entries,hits,misses,inserts,refused,
  evictions,give_way_bytes,limit}` (process-lifetime counters).
- Tests: `tests/unit/core/test_chunk_cache.py` (repeat scan hits with identical
  answer; admit=false flushes and never fills; < 2 GiB budget = off + flush;
  config validation; VARIABLES rows). make q passes (SHOW VARIABLES now 32 rows).
- Functional (Mac, indicative only — train, not a benchmark): canon Q28 run 1
  ~630 ms vs ~540 ms cache off (fill cost), runs 2+ ~204 ms vs ~221-271 ms;
  rugo Q21 runs 2+ 128-159 ms, Q23 223-247 ms (from 445); answers identical.
  Give-way under a 5.5 GiB budget: cache 5.41 → 0.06 GiB while CB33 charged
  5.44 GiB; CB17/19/28 answers identical cache on vs off.

Outstanding (needs a stable machine; NOT done):
1. Cache on/off A/B and leave-one-out of the admission rule (Mac + x86).
2. Churn under pressure: with a tight limit, heavy GROUP BYs keep filling and
   giving away (15.8 GiB given away for 67 hits) — admission should not evict
   residents to admit while query memory is near the limit.
3. Remove the temporary `DRAKEN_MEM_ACCOUNT=0` switch once Phase A is banked.
4. The compaction job must `SET chunk_cache_admit = false` (outside this repo).

**Phase B follow-ups (2026-10-07):**
- Admission under pressure: when query memory holds the limit below the budget,
  an insert may only use free room (no evicting residents to admit). Kept, but
  it does not remove the churn seen in the tight-budget check — that churn is
  BETWEEN queries: query memory drops, the cache regains its budget and refills
  with the next query's chunks, and the following heavy query gives them away
  (6 alternating heavy queries on a 5.5 GiB budget: 3,735 inserts, 15.8 GiB
  given away, 56 hits). The working sets simply do not fit. Remedy on the table:
  admit a chunk only on its SECOND miss (a small set of recently missed keys) —
  costs one more cold run before a repeat query hits (ClickBench try 2, a
  dashboard's second refresh). Changes the cost-based admission ruling: needs
  the architect.
- Cache key for remote chunks: the fetch path up to the first '?' — a pre-signed
  URL's query string (credential, expiry) changes per query. Prod (Cloud Run)
  does not sign by default (`signs_urls` False), so prod keys were already stable.
- Linux (i5, gcc, glibc): Phase A and Phase B compile clean; make q 525 passed /
  5 failed, all 5 DatasetNotFoundError for tpcds_001 (the box's older testdata);
  test_chunk_cache + test_memory_pool 47/47.
- Mac suites: make q, dt 3596, rt 2067, st 57 pass; tests/unit/core +
  parquet_io + variable visibility: 10 failures, all on the 2026-09-26 known
  list (test_lruk set() arity ×7, date32 coercion ×3).
- Concurrency stress (Mac, correctness only): 4 threads × CB 3/13/21/22/28/34 ×3
  reps on a 3 GiB budget — 54 runs, 0 errors, every answer identical to a
  cache-off run; 5,092 inserts, 3,968 hits, 27.6 GiB given way (all eviction via
  give-way: under concurrent query memory the pressure rule refused rather than
  evicted).
- `reference/variables.json` is regenerated by the pre-commit hook (four new
  variables); not regenerated by hand here.

## 25. Cache on/off A/B — Mac (2026-10-07, stable power/network window)

ClickBench canon (100 files), all 43 queries; arms `CHUNK_CACHE_MEMORY_PERCENT`
0 (off) vs 70 (on, 40.8 GiB budget — the whole suite fits); 4 rounds, arm order
alternating, fresh process per arm, 3 runs per query, run 1 (fill) discarded,
best of runs 2-3 (`scratch/c7_ceiling_probe.py one`).

| | off | on | on/off | rounds on faster |
|---|---|---|---|---|
| suite | 7.75-8.16 s | 7.27-7.61 s | **0.912** | 4/4, ranges separate |

Per query (4/4 rounds, ranges separate): Q23 0.433, Q39 0.673, Q22 0.683, Q21
0.753, Q25 0.789, Q28 0.814, Q27 0.820, Q26 0.841, Q18 0.863, Q6 0.867, Q40
0.877, Q31 0.889, Q13 0.891, Q35 0.892. No query > 1.05.
The decompress-free ceiling (§15, Mac canon) was 0.916: the cache delivers it.
Hot path only — run-1 fill cost not in this table (indicative +16%, §24).
Still outstanding: x86 A/B, admission leave-one-out, removing the
DRAKEN_MEM_ACCOUNT switch.

## 26. Give-way to the OPERATING SYSTEM's memory — REVERTED 2026-10-07

**Reverted on the architect's order** (make clickbench-rugo ~1 s slower with it;
ruling: the engine does not try to read or game the OS's memory management). The
limit is back to `min(B, C - R - charged)`; no OS figures are read. The record
below is kept for the finding, not the mechanism.

### (as built, now removed)

Finding (Mac, train — counters, not timings): `make clickbench-canon` was ~1 s
slower with the cache. Not cache thrashing: two suite rounds in one process
showed 0 evictions, 0 refusals, 0 give-way, round 2 all hits (23.6 GiB held).
macOS's memory compressor was the thrasher: free memory went 37.5 → 0 GiB within
4 s of round 1, the compressor grew 12.7 → 32 GiB (kernel pressure level 2),
+36-46 GiB compressed in round 1 and +21-25 GiB decompressed in round 2 — the
cache's idle pages compressed by the kernel, every "hit" paying a kernel
decompression. Q34 (7.7 GiB URL) 781 → 1,632-2,314 ms. The budget rule only saw
our own query memory, not other processes or the OS file cache.

Built (`draken/core/mem_account.cpp`, native): a third term in the limit,
    limit = min(B, C - R - charged, cache + system_available - R)
refreshed at most every 100 ms (cache lookup/insert, every 256th charge), and
the cache shrinks on refresh when over it.
- Linux: `MemAvailable`; under a cgroup v2 limit also
  `memory.max - (memory.current - inactive_file)`, the smaller.
- macOS: free + speculative pages; 0 at `kern.memorystatus_vm_pressure_level`
  ≥ 2. File-backed pages are NOT counted — measured, the kernel compressed the
  cache while 6-12 GiB of file cache stayed resident.
- Stats/telemetry: `system_available` (`chunk_cache_system_available`).

Result (same two-round check): compressions +0.0 GiB in both rounds; Q34 984 /
878 ms; round 2 9.0 s (was 10.7 s). The cache now lives in 4-11 GiB on this
laptop and churns (40.6 GiB given way, 6,092 inserts in round 2): fills given
away again are wasted work. Remedies on the table: second-miss admission
(architect decision pending) or hysteresis (no admission after a give-way until
the OS figure recovers past reserve + margin).
Dedicated hosts (c6a.4xlarge, Cloud Run — no other processes, no compressor,
file cache reclaimed first) should rarely hit the new term.
Test caveat: tests/unit/core/test_chunk_cache.py expects the cache to fill; on a
machine with < 4 GiB actually available the cache correctly refuses and those
asserts would fail.
make q + cache/pool tests (47) pass.
