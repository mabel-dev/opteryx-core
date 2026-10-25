# Slab pool for large transient buffers (as built, 2026-10-24)

Code: `draken/core/slab_pool.{h,cpp}` (single home: draken_native), tier routing in
`draken/core/alloc.h`, test `draken/core/test_slab_pool.cpp` (`make slab-pool-test`, in `make q`).

## Why
Peak query memory sits in ~10^5 allocations of 64 KiB and up (GROUP BY lanes, partitions, key
arenas; parquet decode output; the scan pool). The measured cost was never the containers: it was
fresh pages on every query - first-touch faults, zero-fill, mmap/munmap churn (40% of ClickBench
Q32 samples; ~70 us of kernel mmap/TLB work per row group on Q01). Pivot's numbers for the same
idea: 46 ns vs 249 ns per 2 MB acquire+release, 21% on TPC-H q21 from huge pages alone.

## Rulings (architect, 2026-10-24)
- D1 shape: reserve address space once (PROT_NONE, 256 GiB, MAP_NORESERVE), fault each 2 MiB slot
  the first time it is acquired, retain faulted slots for the process life up to a budget; a boot
  prefault size (config, default 0) for servers. Not a Pivot-style pool faulted whole at boot.
- D2 budget: faulted slots (used + free) are charged to the process memory account, so the chunk
  cache gives way to them like any query memory. Free slots above `SLAB_POOL_BUDGET_BYTES`
  (default 4 GiB - the knee of the 2026-10-25 sweep, see below; `SHOW VARIABLES slab_pool_budget_bytes`) give their pages back
  (MADV_DONTNEED; macOS MADV_FREE_REUSABLE + REUSE); the pool never munmaps.
- D3 entry point: inside `draken_malloc` / `draken_calloc` / `draken_realloc` /
  `draken_aligned_malloc` / `draken_free` for requests >= 64 KiB (`DRAKEN_SLAB_MIN`, the same line
  as the account's threshold). Ownership is one range compare against the reservation, so every
  large engine buffer - GB block pool, decode AppendBuffers, scan MemoryPool, joins, sort, carchar -
  is pool-served with no call-site changes.
- D4 carving: 64 KiB..1 MiB power-of-two classes carved from a slot (2..32 blocks, a free bitmap,
  a per-thread current slab: the fast path is one CAS, no lock); >= 2 MiB is a contiguous run of
  slots (first fit over coalesced free runs, grows/shrinks in place at the run's tail).

## Semantics that changed
- `draken_calloc` of a pool block memsets (recycled memory is not zero); below the threshold it is
  still `calloc`.
- `draken_realloc` across the threshold moves tier with one copy; within the pool a run extends over
  the free slots after it, else alloc + copy + free.
- `GBBlockPool` no longer caches classes >= 64 KiB (two layers of free lists over the same blocks
  retained twice); its < 64 KiB classes are unchanged.
- The scan `MemoryPool` block is a pool run charged whole when faulted; the per-segment
  charge/uncharge on commit/release was removed (it would have double-counted).
- A kernel without THP (`madvise` EINVAL) is recorded in `slab_pool_huge_pages = 0`, not hidden.
  macOS has no huge-page API on Apple Silicon: recycle only.

## Telemetry
Per query (`slab_pool_*`): faulted_bytes, in_use_bytes, free_faulted_bytes, class_slabs, acquires,
faults, give_backs, huge_pages. Config: `SLAB_POOL_BUDGET_BYTES`, `SLAB_POOL_PREFAULT_BYTES`.

## Measurement
See the memory entry / shared notes for the x86 (i5-8500, THP=madvise as on c6a) and Mac tables:
BASE vs FULL on ClickBench (hits_rg64k_b4, 5 interleaved rounds, process per query), the mimalloc
knob probe (ALLOW_LARGE_OS_PAGES, PURGE_DELAY=-1, plain glibc) on the baseline binary, the
leave-one-out arms (no prefault / no huge pages / no recycle), peak RSS, minor faults per run and
perf dTLB-load-misses.

## Budget sweep (2026-10-25, final pool, x86 i5 THP=madvise, hits_rg64k_b4, 5 rounds, hot sum vs pre-pool)
1 GiB 0.979, 2 GiB 0.975, 4 GiB 0.968, 8 GiB 0.966, 16 GiB 0.966; peak RSS 1.01 at every budget. Only Q32/Q33/Q34
(peaks ~12 GB) move: Q33 0.971/0.944/0.913/0.904/0.904, Q34 0.953/0.928/0.893/0.881/0.884. Knee at 4 GiB -> default.
Leave-one-out on the same box: no recycling 1.120, no huge pages 0.996 (dTLB 1.35x), prefault-at-acquire 0.977 vs
0.980 with it (Mac 0.964 vs 0.983, RSS 1.06 vs 1.16) -> deleted.
