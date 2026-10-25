# Local parquet scan: lazy whole-file mapping

Status: BUILT 2026-10-25 (uncommitted), `rugo/src/parquet/io_pipeline.hpp`.
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
