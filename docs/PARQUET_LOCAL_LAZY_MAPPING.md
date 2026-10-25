# Local parquet scan: lazy mapping + process-lifetime file table

Status: BUILT 2026-10-25 (uncommitted), `rugo/src/parquet/io_pipeline.hpp`:
§2 lazy mapping, then §6 the process-lifetime file table (architect chose
option B of §5).
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

⛔ x86 measurement trap: the box has 15 GB; `hits_rugo_262k` and
`hits_rg64k_b4` do not both fit in page cache. An A/B over both in one job read
the second from disk (kswapd busy, load 9–10 on 6 cores) and produced ratios
from 0.52 to 1.30 at 3/5 — discarded. Measure one dataset per job, warm it first.
