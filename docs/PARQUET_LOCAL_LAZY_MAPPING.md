# Local parquet scan: lazy mapping + process-lifetime file table

Status: BUILT 2026-10-25 (uncommitted), `rugo/src/parquet/io_pipeline.hpp`:
§2 lazy mapping, then §6 the process-lifetime file table (architect chose
option B of §5), then §8 the spin-then-block queue lock (proposal 1 of §7).
Scope: the per-row-group cost of the native local parquet scan on short
ClickBench queries (Q01 `COUNT(*) WHERE AdvEngineID <> 0`, Q07, Q36–Q42).

## 1. Problem (measured)

Mac M5 Pro, 16 workers, `scratch.hits_rg64k_b4` (100 files, 1,600 row groups),
samply + dSYM, per Q01 run (13.9 ms wall). Worker thread-ms:

| where | thread-ms / run |
|---|---|
| `local_file_mapping`: mutex wait 72, `open` 27, `mmap` 10, mutex drop 4, munmap/close/fstat 3 | **~116** |
| idle pool workers (`cvwait` in the pool) | 73 |
| `queue_mutex_` waits (claim / publish / result / consumer pop) | ~35 |
| `acquire_local_fd` / `release_local_fd` (open 10, mutex 6) | ~16 |
| engine user code (decode + kernels) | 24 |
| system malloc | 5 |

Every Q01 chunk (AdvEngineID, a few KB per row group) is below the H15
small-chunk pread threshold (256 KB), so **no byte of Q01 is read through the
whole-file mapping**. Yet the H6 per-pipeline cache opened, `fstat`ed and
`mmap`ed every file the query touched — and because row groups of one file are
claimed together, up to 16 workers raced the same first mapping (all but one
then `munmap`ed under the table mutex), plus ~0.65 ms/file of teardown munmap
on the `MappingReaper`.

x86 (i5-8500, 6 cores, mimalloc, `hits_rugo_262k`, `perf record` + `perf trace`):
a DIFFERENT shape. Q01 is ~21 ms and CPU-bound in decode
(`DecodeRLEBitPackedIndicesSelected`/`count_selected`, `DecodeColumnFromChunk`
≈ 46% of on-CPU samples), libc copies under `DecodeFromChunkImage` (≈ 15%), and
kernel syscall entry/exit (≈ 13%), part of it `telem_cpu_now_ns` →
`clock_gettime` (thread CPU clock, a real syscall, not vDSO) per morsel.
Only ~43 `openat` and ~26 `mmap` per run: `<> 0` prunes most row groups by
statistics on that dataset. The mapping cost is a Mac/many-row-group cost.

## 2. Change

`decode_row_group` resolves the file mapping on the FIRST chunk that reads
through it (`ensure_mapping`), instead of at row-group entry. A row group whose
chunks all take the small-chunk pread never maps its file. Both mapping modes
(the H6 whole-file cache and the `RUGO_LOCAL_MMAP_CACHE=0` per-row-group span)
go through the same lazy resolver; every read path, threshold and fallback is
unchanged. No ownership or lifetime change: mappings are still per pipeline and
released in the destructor.

## 3. Measurements (5 interleaved rounds, result digests identical)

Queries Q38–Q41 and Q21 return a different row set per run inside EACH arm
(`ORDER BY count LIMIT/OFFSET` over ties) — not a change effect.

Mac (`hits_rg64k_b4`), base vs lazy:

| query | base ms | lazy ms | ratio | wins | CPU ratio |
|---|---|---|---|---|---|
| Q01 | 13.84 | 9.64 | **0.696** | 5/5 | 0.581 |
| Q07 | 14.35 | 10.29 | **0.717** | 5/5 | 0.586 |
| `> 0` (scans on this dataset) | 14.17 | 10.56 | **0.745** | 5/5 | 0.648 |
| Q36 / Q37 / Q42 | | | 1.002 / 0.975 / 1.002 | | |
| Q20 LIKE (reads via the mapping) | | | 0.998 | | |
| ctl `SUM(AdvEngineID) GROUP BY OS` | | | 0.983 | | |
| ctl `>= 0` (stats-settled) | | | 1.026 (1/5) | | |

x86 (`hits_rugo_262k`), base vs lazy **with the base `_operators.so`**: Q01
0.994, Q07 0.997, `> 0` 0.968 (5/5), Q36–Q42 0.996–1.006, Q20 1.003, ctl
`SUM … GROUP BY OS` 1.004, `>= 0` 0.994. Flat-to-positive.

⛔ x86 measurement trap: an unmodified relink of `_operators.so` (it compiles
`io_pipeline.hpp` in for the consumer's inline-decode path) moved
`SUM … GROUP BY OS` by +3–5% (0/5) with IDENTICAL instruction counts
(4.66G vs 4.67G per run) and IPC 2.06 → 2.00 inside
`GroupBySink::da_sink` / `gb_apply_row_kind_rd`. Swapping the base
`_operators.so` into the candidate arm removed it (1.029 → 1.004). The x86
GROUP BY hot loop is layout-sensitive; any header change that relinks
`_operators` can show a phantom GROUP BY regression or win.

## 4. Rejected in this round (measured)

- **Single-flight first open/map per file** (one worker maps/opens, the rest
  wait on it). Leave-one-out on the Mac, lazy vs lazy+single-flight: Q01 1.025
  (CPU 1.038), Q07 0.994 (CPU 1.029), Q20 0.984 (5/5). The waiters' cv waits
  replace the racing opens; the targets gain nothing. Deleted.
- **macOS first-fit mutex policy** (`PTHREAD_MUTEX_DEFAULT_POLICY=3`): no change
  either arm; the remaining lock cost is not a fair-share hand-off convoy.

## 5. What remains (Mac Q01 after the change, ~10 ms)

Worker thread-ms/run: descriptor table ~36 (two `local_fd_mutex_` touches per
chunk + first-open waits), `queue_mutex_` ~40, decode ~20, idle ~69. Open
directions, both design-impacting (descriptor ownership), architect's call:

- **A — per-file handle carried on the WorkItem** (query lifetime): submitter
  creates one handle per file; workers do no table lookup per chunk; the
  descriptor closes with the file's last row group (bound = submission window,
  replacing the RLIMIT_NOFILE/16 LRU cap).
- **B — process-lifetime descriptor/mapping table keyed by path** (correct by
  the "files are immutable" ruling): no opens/maps at all in steady state, no
  per-query munmap; needs a process-wide descriptor budget + eviction, and
  GC-deleted files stay pinned until evicted.

x86's Q01 cost is decode/copy CPU and the per-morsel thread-CPU clock syscall —
separate work, not addressed here.

## 6. Option B: process-lifetime local file table (RULED, BUILT)

`LocalFileTable` (io_pipeline.hpp, after `MappingReaper`): one descriptor and,
on first need, one whole-file PROT_READ/MAP_SHARED mapping per local path,
shared by every scan of the process. Replaces the per-pipeline mapping cache,
the per-pipeline descriptor cache, `LocalFdLease` and their destructor cleanup.

- **Key: the path string alone.** Correct by the files-are-immutable ruling
  (2026-09-28); nothing is revalidated. Two spellings of one file (relative vs
  absolute) are two entries — harmless, a scan always names a file one way.
- **Lookup at submit, not per chunk.** `submit_block` takes the handle once per
  block (no IO, one table mutex) and every member WorkItem carries a
  `shared_ptr<LocalFileTable::File>`. Workers read `file->fd()` /
  `file->mapping()`: an acquire load in steady state; a per-file mutex only for
  the first open / first map. The page-index region read (`read_range`, local)
  uses the same descriptor instead of an open per read.
- **Failure semantics unchanged.** A failed open is not remembered (the next
  reader retries and raises "Cannot open file" as before); a failed fstat/mmap
  or an empty file is remembered and the reader preads (as the H6 cache did).
- **Bound.** At most RLIMIT_NOFILE/16 (soft) entries, now PROCESS-wide (it was
  per pipeline, so concurrent queries could each hold that many). On a miss at
  the cap, entries no work item holds are dropped least-recently-used first; a
  held handle is never closed under its reader — the table can exceed the cap by
  what is in flight and returns under it at its next miss. A dropped file's
  descriptor closes and its mapping goes to the `MappingReaper` when its last
  holder releases it.
- **One table per extension** (a function-local static does not merge across
  .so files). Every pipeline binds `files_ = &LocalFileTable::instance()` at
  construction, so this header's code compiled into `_operators` (the consumer's
  inline decode, `NativeParquetScanSource::submit_block`) uses the constructing
  extension's table. Scans construct in `pool_reader`; the vector-index builder
  (`src/cpp/engine/vector_index_build.hpp`) gets its own table.
- **Cost accepted:** a GC-deleted file stays open/mapped (its disk blocks
  pinned) until evicted; whole-file mappings hold address space (~14 GB for the
  ClickBench split set) but no memory.

Test: `tests/unit/connectors/parquet_io/test_local_file_table.py` — in a
subprocess, 12 files scanned per file and as one dataset, 3 rounds: answers
exact; with the default limit all 12 descriptors outlive their queries; with
soft limit 128 (cap 8) exactly 8 remain after a one-file-at-a-time pass. Fails
on the pre-B tree (0 held), passes on B.

Mac profile (Q01, after B): `open`, `mmap`, `acquire/release_local_fd` and the
descriptor mutex are gone; worker non-idle 125 → 108 thread-ms/run; the
contention moved to `queue_mutex_` (~60 thread-ms/run: `wait_and_get_result`,
`run_one_pending`, the result enqueue in `decode_row_group`, `publish_items`) —
the next lever.

A/B lazy (§2) → B, 5 interleaved rounds, digests same:

| query | Mac run 1 | Mac run 2 | x86 (hits_rugo_262k) |
|---|---|---|---|
| Q01 | 0.958 (5/5) | 0.952 (5/5, CPU 0.916) | 0.994 |
| Q07 | 0.938 (5/5) | 0.945 (5/5, CPU 0.907) | 0.994 |
| Q36 / Q37 / Q42 | 0.995 / 1.017 / 1.012 | 0.989 / 0.974 / 0.987 | 0.992 / 1.004 / 0.998 |
| Q20 / Q21 LIKE | 0.968 / 0.965 | 0.960 / 0.970 (5/5) | 0.996 / 0.996 |
| ctl SUM GROUP BY OS | 1.012 | 0.983 (5/5) | 0.995 |
| `> 0` | 1.065 (0/5, CPU 0.953) | 0.912 | 0.973 |
| `>= 0` | 1.072 | 1.029 | 1.004 |

`> 0` is bimodal only when it runs right after the GROUP BY control in one
process (order probe: B 10.34 / 9.04 / 8.97 ms vs lazy 9.99 / 10.10 / 10.48);
alone B wins it consistently (9.51 → 9.00 ms). Persistent mappings are not the
cause (gt0 timed before/after LIKE scans: unchanged in both arms).

## 7. `queue_mutex_` contention — attribution and proposal (2026-10-25, NOTHING BUILT in main)

All on the Mac, main == §6, Q01 on `hits_rg64k_b4`, probes in private trees only.

**Traffic per Q01 query** (counter probe): 1,114 row groups; 356 `publish_items`
calls; 1,114 pool tickets, ~250 of which find the queue empty (a consumer took
the item); ~864 decoded by tickets, ~250 inline by consumers; 1,114 results
enqueued and popped; ~10 consumer cv waits. ≈ **4,000 `queue_mutex_`
acquisitions per query** (~3.6 per row group), plus ~3 BS-pool mutex operations
per row group for the tickets.

**Where the wait sits** (samply, worker thread-ms/run): result enqueue in
`decode_row_group` 17.7 (+3.4 drop), `wait_and_get_result` entry 16.1 (+4.0),
`run_one_pending` claim 12.0 (+2.0), `publish_items` 4.6 (+1.0), relock after an
inline decode 2.9. CV waits ~0.3 and `notify_all` ~0: NOT a notify herd.

**Falsified** (no change in Q01 wall): decode-pool width 16/8/4/1 (1 → 92% inline,
SLOWER, 10.2 ms); submission window 18/36/72/144; first-fit pthread mutex policy
instead of fair-share.

**The mechanism** (hold-time probe): the lock is HELD only **1.2 ms per query**
(~0.3 µs per hold, ~27% of the 4.5 ms native window) while acquirers WAIT ~90
thread-ms (~23 µs per acquisition). The lock is mostly free; the cost is a waiter
that blocked in the kernel needing ~20 µs to be woken and scheduled. A worker's
per-row-group cycle is ~10 µs decode + ~3 such waits ≈ 70 µs → 16 workers ≈ the
measured ~250 row groups/ms. Width, window and hand-off order cannot move a
per-acquisition wake latency — which is why all three were null.

**Proof that it moves the wall** (spin-then-block probe — `try_lock` with an
`asm yield` between tries N times, then `lock()`; cv → `condition_variable_any`):

| spins | Q01 wall (2 passes) |
|---|---|
| 0 (main) | 8.70 / 8.76 ms |
| 64 | 8.68 / 8.74 |
| 256 | 8.04 / 8.09 |
| 1024 | 7.59 / 7.66 |
| 4096 | 7.39 / 7.49 |

At 1024: Q01 wall 8.82 → 7.56 ms (0.857), **CPU 82 → 59.5 ms (0.72)** — spinning is
cheaper than the kernel block/wake it replaces. `SUM … GROUP BY OS`, `URL LIKE`,
`SearchPhrase GROUP BY` flat on wall and CPU (2 passes each).

**Proposal (in order):**
1. **Spin-then-block queue lock** — internal to the pipeline, same protocol,
   same predicates. Spin bounded by TIME, not iterations: `pause` is ~140 cycles
   on Skylake-class x86 and `yield` ~1 cycle on ARM, so a fixed count means very
   different budgets per arch. Default: spin up to the measured knee (start at
   ~2 µs, sweep 0.5–8 µs per arch), then block. `queue_cv_` becomes
   `condition_variable_any`. Expected: Mac Q01 ~0.86; x86 unmeasured (glibc
   futex wake is cheaper, and x86 Q01 is decode-bound) — the box A/B decides.
2. **Fewer acquisitions per row group** (~3.6 → ~2): a ticket that enqueues its
   result claims the next pending item in the SAME critical section and loops
   while items remain; a consumer that decodes inline keeps its own result
   instead of a push + re-pop. Touches the `tickets_inflight_` teardown
   accounting — careful, separately measured (build all, then leave-one-out).
3. **Out of this lock, bigger:** of Q01's 8.5 ms wall only **4.66 ms is native
   execution** (trace); ~3.8 ms (45%) is planning/compile/setup/hand-off — the
   stats-settled `>= 0` is 2.3 ms in total. That slice is the largest single
   remaining piece of the short-query gap and needs its own profile.

## 8. Spin-then-block queue lock (proposal 1 of §7 — RULED, BUILT)

`spin_then_lock(std::mutex&)` (io_pipeline.hpp, namespace scope): `try_lock`;
on failure, while a TIME budget lasts, 16 × (spin hint, `try_lock`) per clock
check; then an ordinary blocking lock. Returns a `std::unique_lock<std::mutex>`
(adopted), so `queue_cv_` stays `std::condition_variable` — the probe's
`condition_variable_any` (an internal mutex on every notify) was NOT carried
over. Every `queue_mutex_` acquisition goes through it, including the relock
after an inline decode; condition-variable waits and their wake relock do not
spin. Budget is a compile-time constant per arch (`kQueueSpinBudget`); zero is a
plain blocking lock (the spin branch compiles away).

**Budget sweep** (2 interleaved passes):

| budget | Mac Q01 wall / CPU | x86 Q01 wall / CPU |
|---|---|---|
| main | 8.58–8.88 / 82 ms | 20.33–20.40 / 77.6 ms |
| 0 | 8.54–8.73 / 77 | 20.15–20.19 / 77.4 |
| 1 µs | 7.56–7.77 / 60 | 20.10–20.11 / 77.3 |
| 2 µs | 7.31–7.51 / 56.5 | 20.27–20.49 / 77.4 |
| 4 µs | 7.24–7.41 / 54.5 | 20.13–20.22 / 77.3 |
| 8 µs | 7.18–7.31 / 54 | 20.13–20.32 / 77.4 |
| 16 µs | 7.15–7.24 / 53.4 | 20.08–20.30 / 77.5 |

Mac controls: at 4 µs `SUM … GROUP BY OS` and `SearchPhrase GROUP BY` flat; at
16 µs `SearchPhrase` +1.7%/+2.4% (both passes). x86: no budget moves Q01 (glibc
futex wake is cheaper and x86 Q01 is decode-bound, §1).
**Chosen: arm64 4 µs, x86-64 0.**

**A/B** (5 interleaved rounds, digests same), main (§6) → spin lock:

Mac (`hits_rg64k_b4`): Q01 9.30 → 7.23 ms **0.778** (5/5, CPU 0.678), Q07 **0.779**
(5/5, CPU 0.652), Q36 0.901 (5/5), Q37 0.889 (5/5), Q40 0.907 (5/5), Q38/Q39/Q41/Q42
0.96–0.98, LIKE Q20/Q21 0.981/0.987, `> 0` 0.798, `>= 0` 0.831, `SUM … GROUP BY OS`
0.995. No loser.

x86 (`hits_rugo_262k`, warm): Q01 0.999, Q07 0.994, Q36 1.004, Q37 1.018 (0/5),
Q38 1.010 (0/5), rest 0.979–1.004. With main's `_operators.so` swapped in (valid
on x86, where the budget is 0 and both builds lock identically): Q37 1.003, Q38
0.999, all 0.981–1.007 — the 1–2% was the relink-layout effect (§3). Flat.

Not done (§7 items 2 and 3): fewer acquisitions per row group; the ~45% of Q01
wall outside native execution.

## 9. Outside native execution — attribution (2026-10-25, NOTHING BUILT)

Main (§8), Mac, `hits_rg64k_b4`, warm. Session telemetry medians over 30 runs (ms):

| query | total | planning | binder | binding metadata | optimizer | engine compile | native (generator) |
|---|---|---|---|---|---|---|---|
| `>= 0` (stats-settled) | 2.11 | 1.04 | 0.57 | 0.36 | 0.22 | 0.73 | 0.27 |
| Q01 | 6.77 | 1.32 | 0.70 | 0.46 | 0.28 | 1.16 | 4.22 |
| Q07 | 7.38 | 1.51 | 0.73 | 0.47 | 0.32 | 1.24 | 4.56 |

Q01: ~2.5 ms of 6.8 (37%) is outside native execution. cProfile (300 runs) +
the main thread of the samply profile (dSYM) attribute it:

1. **Engine compile → `pool_reader.open_native_scan_plan` ≈ 1.0 ms**, built per
   query from scratch, Cython over Python objects, scaling with files × row
   groups: `_prune_row_groups` per FILE (~0.46 ms for 100 files: predicate
   encode + a Python `keep` list + a native min/max test per row group); a
   Python `(path, rg)` tuple per work item (1,114) beside the C++ vector; a second
   loop that re-`encode`s the path and re-looks-up `footer_map` PER WORK ITEM;
   then `_count_remote_fetch_blocks_native(work_items, …)` and
   `_any_remote_path([...])` each walk all 1,114 items again; string
   conversions/mallocs ~0.1. Even `>= 0` (no page read) pays 0.73 ms here.
2. **Binder dataset metadata ≈ 0.36–0.47 ms**: `list_file_infos` (native walk +
   100 `FileInfoLike` objects) 0.34 ms, then 5 list comprehensions, 200
   `format_for_path`, a sorted (path, size, mtime) signature tuple — every query,
   to validate the manifest cache.
3. **Optimizer ≈ 0.22–0.32 ms** — the known flat cost of the strategy walk
   (TPC-DS planning profile 2026-10-22); not Q01-specific.
4. Rest of planning ≈ 0.3 ms (parse, logical plan, session).

**Proposals:**
- **P1 — native work-list build.** One C++ function over `footer_map` that, per
  file, runs the min/max test and the coverage verdict and emits the work items,
  `max_rg_bytes`, cache-fill bytes and the remote flags in ONE pass; Python passes
  the prepared predicates/coverage request once and keeps only what Python needs
  (no 1,114-tuple list, no per-item re-encode/lookup, no extra walks). The local
  bloom pread stays where it is (only for files with Eq/InList predicates).
  Expected: compile 0.73–1.24 → ~0.2 ms; Q01 ≈ −0.9 ms (−13%), `>= 0` ≈ −0.5 ms.
  Same answers by construction; same planning-phase ownership (it is plan setup,
  not execution). Not design-impacting beyond moving a loop to native.
- **P2 — cheaper dataset signature.** The listing must stay (files are added and
  GC-deleted; per-file immutability does not make the FILE SET immutable). Two
  steps: (a) build the signature and the filtered lists natively from the walk
  (no 100 Python objects, no 200 `format_for_path`) — mechanical, ~−0.25 ms;
  (b) gate the whole thing on the directories' mtimes (add/remove changes a
  directory's mtime; files are never rewritten in place by ruling) — ~−0.4 ms but
  a CHANGE TO A CACHE-VALIDITY RULE → architect's call.
- **P3 (existing, not this task):** optimizer walk cost; plan cache rulings
  (PLANCACHE, 4 pending) would remove most of all three for repeated queries.

## 10. P1 — native work-list build (RULED, BUILT)

`opteryx/connectors/parquet_io/pool_reader.pyx`:

- `_PrunePredicates` / `_prepare_prune_predicates`: a scan's pushed predicates
  (name bytes, operator code, value) prepared ONCE per scan; both callers
  (`open_native_scan_plan`, `open_ipc_source`) build it before their file loop.
- `_prune_row_groups` resolves each predicate's chunk index and statistic decode
  (`_stat_kind`) once per FILE from row group 0 (the parquet spec orders every row
  group's chunks by schema leaf). `_rg_passes_predicates_native` reads that chunk
  directly — no name scan over ~105 columns per row group — and raises if a row
  group's chunk at that index has another name. **The min/max test itself is
  unchanged and remains the single implementation**: it compares Python values
  (strings, Decimals, dates, the `TypeError` → keep rule), and a C++ twin would be
  a second implementation to keep identical (§9's "one C++ function runs the
  min/max test" was not built for that reason).
- `open_native_scan_plan`: one pass per file emits the C++ work items and sums the
  largest projected row group, the chunk-cache fill bytes, the remote flag and the
  remote items for the fetch-block count; path encoded once, footer looked up once,
  projected chunk indices resolved once per file. Gone: the Python `(path, rg)` list
  (1,114 tuples on Q01), the per-item re-encode + footer lookup, two extra walks.
  The two different remote tests the consumers used (`_is_local_path` for cache
  fill, the gs/http(s) prefix test for the scan flag and the fetch-block count) are
  kept exactly as they were.

**Equivalence:** the compiler's `open_native_scan_plan` wrapped in both trees; 56
queries (43 ClickBench + 13 predicate shapes over hits and TPC-H) → 52 scan plans,
every recorded fact identical (row groups scanned/pruned/covered/disjoint,
coverage seed, cache-fill bytes, surviving rows, fetch-ahead depth, memory
budget); result digests identical where deterministic.

**Measured** (phase telemetry, Mac): Q01 compile 1.23 → 0.77 ms, total 7.2–7.3 →
6.75–6.8; `_prune_row_groups` 0.46 → 0.11 ms. `>= 0` compile unchanged (0.71): its
compile is now the C++ statistics coverage (`parquet_cover_row_group` 0.34 ms),
which P1 did not touch.

A/B (5 interleaved rounds, digests same):
- Mac, two runs: Q01 0.967 / 0.942 (5/5), Q07 0.940 / 0.936 (5/5); Q39 1.033 (0/5)
  and Q40 1.016/1.015 in single runs but their phase timers show compile and total
  equal or lower under P1 (Q40 total 6.99/6.94/6.84 → 6.76/6.90/6.87) and Q39 did
  not reproduce — harness order effect, not the change.
- x86 (`hits_rugo_262k`): Q01 **0.923** (5/5), Q07 0.937 (5/5), `> 0` 0.948 (5/5),
  `>= 0` **0.928** (5/5, CPU 0.927), all others 0.983–1.005.

## 11. P2a — dataset listing: leaner native walk + one-pass signature (RULED, BUILT)

Measured first (Mac, 100-file dataset, per bind): the native walk
`list_files_info` 229 µs; Python `FileInfoLike` objects ~17 µs; Python filters +
format checks + signature ~51 µs. §9's estimate (~0.25 ms from moving the Python
work native) was wrong — the walk was the cost. Architect chose: leaner walk on
both OSes + `getattrlistbulk` on macOS + the Python one-pass.

`src/cpp/directories.cpp` `list_files_with_info`, via `read_directory_entries`:
- **macOS:** `getattrlistbulk` (name, type, mtime, data length for many entries
  per call). An entry with `ATTR_CMN_ERROR` is skipped (as a failed `lstat` was); a
  failing call stops that directory (as a `readdir` error did).
- **Elsewhere:** `opendir` + `fstatat` on the directory fd (`AT_SYMLINK_NOFOLLOW`
  only to classify an unknown `d_type`; `stat` semantics for a regular file's size
  and mtime).
- Same rules both ways: symlinks never followed or reported, the root must open, a
  subdirectory that cannot is skipped, path joined `dir + '/' + name` unless `dir`
  ends in `/`. Gone: a ~1 KB `dirent` copy and a path-string copy per entry, a
  `pathconf` per walk, and on macOS (no `_DIRENT_HAVE_D_TYPE`) an `lstat` AND a
  `stat` on the full path per file.
- ⛔ Kept unchanged, reported: with an EXTENSION FILTER the walk skips any entry
  whose name is shorter than the shortest extension BEFORE classifying it, so a
  short-named directory (`a`, `medium`) is never descended into. The production
  listing passes no extensions (`list_files_info(base, ())`), where it never
  applies. Fixing it is a separate decision.

`opteryx/connectors/filesystem_connector.py` `get_dataset_metadata`: one pass over
the listing — manifest test and `format_for_path` once per path (was twice) — and
`opteryx/models/dataset_format.py` `single_format(formats, dataset)`, the
mixed-format rule, which `dataset_format(paths)` now calls (one implementation of
the rule; the suffix rule stays in Python's `format_for_path` alone).

**Equivalence:** `list_files_info` old vs new over every directory under
`testdata` (depth ≤ 2), both hits datasets and an edge tree (symlink to file /
dir / loop, hidden files and dirs, a non-ASCII name, a FIFO, an empty dir, a file
as root, a missing root), each with no / one / two extensions: Mac 1,242 results,
7,562 entries; Linux 222 results, 1,662 entries — **identical** (name, type, size,
mtime ns, and error). Scan plans identical (52 plans / 56 queries).

**Measured:**

| | Mac walk | Mac binding metadata | x86 walk | x86 binding metadata |
|---|---|---|---|---|
| before | 219–228 µs | 0.58–0.65 ms | 31.1 µs | 1.01 ms |
| after | 48.5–49 µs | 0.40–0.41 ms | 23.8 µs | 0.90 ms |

A/B (5 interleaved rounds, digests same): Mac Q01 0.974, Q07 0.965, Q37 0.965,
Q38 0.960, Q41 0.936, Q42 0.972 (all 5/5), `>= 0` 0.950, rest 0.977–1.006. x86
(`hits_rugo_262k`) flat: 0.962–1.013, no consistent loser — Linux `stat` on a warm
dentry cache was already cheap (31 µs for the whole walk), so the −0.1 ms bind
saving is below the noise of a 20 ms query.

## 12. x86 re-profile on current main (2026-10-25, NOTHING BUILT)

Box (i5-8500, 6 cores, mimalloc, `hits_rugo_262k` warm), main as of §11, perf
with DWARF call graphs per query. Plain ms: Q01 18.9, Q07 19.8, Q36 51.6, Q37
18.8, Q38 30.0, Q39 118.9, Q40 19.5, Q41 17.5, Q42 18.8 — six short queries sit
on one ~18–20 ms floor.

Attribution (share of CPU samples):
- **Per-morsel thread-CPU clock** (`executor.hpp` `telem_cpu_now_ns`, bracketing
  every source pull / operator / sink call for `OpStats.cpu_ns`): ~6,500
  `CLOCK_THREAD_CPUTIME_ID` reads per Q01 (`perf trace`: 1,096,144 in ~168 runs).
  On this box a read is **800 ns** — a syscall (Linux has no vDSO path for the
  thread CPU clock) with PTI on every kernel entry; `CLOCK_MONOTONIC` is 18.7 ns.
  (The Apple Silicon figure in memory was 107 ns.)
  **Ceiling probe** (reads made free, private tree, 3 interleaved passes): Q01
  18.87 → 17.72 ms **0.939** (3/3, CPU 0.918), Q07 **0.961** (3/3), Q37 0.990, Q42
  0.993. The clock does not explain the Q37/Q42 floor.
- **Per-row work on RLE-decided rows (Q01)**: `DecodeRLEBitPackedIndicesSelected`
  12.4% (count_selected), `decode_codes_packed` 4.3%, verdict memset in
  `DecodeRLEBitPackedVerdicts` 3.4%, a `std::fill_n` byte fill 4.1% — ~25% of Q01
  CPU spent per row on 70.5M rows that runs already decided. Lever = count/settle
  from runs without per-row bytes (run-domain design work).
- **Chunk-image copies** under `DecodeColumnFromChunk` / `DecodeFromChunkImage`:
  ~4.6% Q01, ~4% Q42.
- **GROUP BY string keys**: `GBArena::put` copies 13.7% of Q39 (2.3% Q37);
  `AppendBuffer::append` + `build_direct_string_dict` another 5–10% on Q37–Q39.
- `libpython` 5–15% on the short queries (planning; the optimizer work is in
  another session).

Consumers of the per-operator `cpu_ns`: plan telemetry's per-node `cpu_time_ms`
and EXPLAIN ANALYZE's `cpu_ms` column (ruling D1, 2026-08-25 — this line first
said "plan telemetry only", corrected in §13). The per-PIPELINE CPU (process clock either side of `run_pipeline`, 2 reads
per pipeline) is separate and stays cheap.

⛔ x86 measurement trap: the box has 15 GB; `hits_rugo_262k` and
`hits_rg64k_b4` do not both fit in page cache. An A/B over both in one job read
the second from disk (kswapd busy, load 9–10 on 6 cores) and produced ratios
from 0.52 to 1.30 at 3/5 — discarded. Measure one dataset per job, warm it first.

## 13. Per-operator CPU clock armed for EXPLAIN ANALYZE only (RULED, BUILT)

Architect's choice (2026-10-25): stop reading the thread-CPU clock per morsel on
ordinary queries; keep per-operator `exec_ns` (wall) and the per-pipeline CPU;
measure per-operator CPU only for EXPLAIN ANALYZE, so its `cpu_ms` column (D1)
stays exact.

Change:
- `Pipeline::measure_cpu` (operator.hpp), set from `Engine::measure_operator_cpu_`
  in `run_step_`. `run_worker_body<bool kMeasureCpu>` (executor.hpp): the
  unmeasured body has no CPU clock read, no `cpu_ns` atomic add, no branch;
  `run_worker` picks the instantiation once per worker. The read-order contract
  for the measured body is unchanged.
- `NativePlan.set_measure_operator_cpu`; `collect_op_stats` emits `cpu_time` only
  when measured (an unmeasured 0 would read as "burned no CPU").
- `execute_native(..., measure_operator_cpu=False)`; only `serial_engine.explain`'s
  ANALYZE run passes True. `native_op_stats` and plan telemetry carry
  `cpu_time` / `cpu_time_ms` for those runs only — absent, not 0, otherwise.
  The profiling runners (tpch runner, `dev/tpch_profile.py`, ClickBench runner)
  already drive EXPLAIN ANALYZE, so they keep their CPU column.
- Test: `test_plain_query_does_not_measure_operator_cpu`; the D1 test
  (`cpu_ms > 0` under ANALYZE) still passes.

A/B (5 interleaved rounds, baseline = snapshot of main, candidate = the same
snapshot + this change; deterministic digests identical — Q38–Q41 and Q21 vary
within each arm, ties under OFFSET):
- **x86** (box, `hits_rugo_262k`, mimalloc): Q01 19.01 → 18.00 ms **0.947** (5/5,
  CPU 0.921), Q07 **0.968** (5/5, CPU 0.948), `> 0` **0.948** (5/5); Q36–Q42,
  LIKE and SUM GROUP BY 0.987–1.014, CPU 0.984–1.003. Close to the §12 ceiling
  probe (0.939).
- **Mac** (`hits_rg64k_b4`, 16 workers): flat as expected for a 107 ns clock —
  Q01 0.997, Q07 0.996, rest 0.98–1.01, no loser; the `> 0` control read 0.883
  (5/5, CPU 0.797) — not explained by a 107 ns clock and not chased.

