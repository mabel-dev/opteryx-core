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
