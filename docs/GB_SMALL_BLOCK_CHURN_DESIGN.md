# GROUP BY small-block churn (ClickBench Q32) — design note

Status: RULED 2026-10-27 (§2). Nothing built.

## 1. Problem (measured)

x86 box (i5-8500, THP `always`), main snapshot 2026-10-27, `hits_rugo_262k`, process
per query, mimalloc preload as `make clickbench`, perf gated to the hot tries:

| Q32 hot try | value |
|---|---|
| wall | ~4.85 s |
| minor faults | ~1.15 M **every** hot try (does not settle) |
| system time | 2.5–3.3 s of ~21 s CPU |
| kernel share of cycles | 17% (fault entry/exit, TLB flush, `clear_page`) |
| GB block pool | ~557 k misses / 64 k hits; 8.47 GiB from the system, 8.47 GiB freed |
| GB pool retained | 1.0 GiB = `kRetainBytes` (cap reached) |
| slab pool | 0.57 GiB faulted, ~290 slot faults, 0 give-backs |

Where the faults land: `GBSegArray<8>/<16>::resize` zero-fill (45%), mimalloc
`mi_page_free_list_extend` (26%), `vector<GBLanes>::resize` (7%), the sink's
`put_fixed`/`push_back` (~16%).

Mechanism: `GBSegArray` segments start at 512 rows (`kGBSegFirstLog2 = 9`), i.e.
4–8 KiB for 8/16-byte lanes, and double. Q32 (~100 M groups) creates a very large
number of partition columns, so most blocks fall below `kSlabClass` (64 KiB). Those
are the system allocator's: `GBBlockPool` keeps up to `kRetainBytes` (1 GiB, marked
UNMEASURED in `gb_block_store.hpp`) and frees the rest, the allocator returns the
pages to the OS between tries, and the next try faults them again. The slab pool's
recycling, built for exactly this, never sees these blocks.

Q33/Q34 fault little (kernel 3.4%) and are out of scope.

Upper bound: kernel + fault-adjacent user time ≈ 15–20% of Q32 CPU. That is CPU, not
wall; the wall effect is unknown until measured.

## 2. Rulings (2026-10-27)

1. **One global slab pool.** The process already has a permanent pool — the slab pool
   (2 MiB slots, faulted once, recycled for process life, 4 GiB free-faulted budget,
   charged to the memory account). There is no second, operator-specific retention
   layer.
2. **Small classes for everything.** The slab pool gains classes below 64 KiB and
   `draken_malloc` serves them for every caller, not GROUP BY only.
3. **The GB-only pool is retired.** `GBBlockPool`'s small-block lists, `kRetainBytes`
   and its per-thread caches go; GB blocks of every size come from `draken_malloc`.
4. **A/B allocators:** x86 on both glibc (what the c6a entry runs — its scripts set no
   preload) and mimalloc (what `make` preloads on Linux); Mac on its system allocator
   (what `make` uses there).

Options not taken: raising `kRetainBytes` (a second, GB-only retention layer — ruled
out by 1); rounding small GB blocks up to 64 KiB (up to 16x memory per small segment,
the Q09 oversized-segment trap); a bigger first segment (reverses the 2026-10-13 L1
aliasing and Q09 RSS measurements, and only moves blocks to the slab sooner).

## 3. Design

### 3.1 Slab classes below 64 KiB

Today a slot is either a RUN (whole slots, > 1 MiB) or a CLASS SLAB: one slot cut into
equal blocks of one class, 64 KiB–1 MiB (32 × 64 KiB … 2 × 1 MiB), tracked by a 32-bit
`free_mask`, allocation = one CAS on the thread's current slab.

New classes 4, 8, 16, 32 KiB (64–512 blocks per slot). Changes:

- **Free bitmap.** 512 blocks do not fit a 32-bit mask. Proposed: a per-slab array of
  `uint64_t` words (8 words for 4 KiB), the fast path a CAS on the thread's current
  word, plus a per-slab count of free blocks so "slab full / slab empty" stays O(1).
  Classes ≥ 64 KiB keep their layout.
- **Threshold.** `DRAKEN_SLAB_MIN` becomes the smallest class (4 KiB proposed);
  everything below stays the system allocator's. `draken_slab_owns` (one range
  compare) and `draken_usable_size` (slot-table lookup + class) are unchanged in
  shape.
- **Zeroing.** `draken_calloc` already memsets pool blocks explicitly (`alloc.h`);
  small classes inherit that. The memset over an already-faulted block is the cost
  that replaces the fault.
- **Budget.** Unchanged: free faulted slots up to `slab_pool_budget_bytes` (4 GiB);
  a class slab whose blocks are all free returns its slot to the free runs and is
  counted against the budget like any slot.

### 3.2 GB blocks through `draken_malloc`

`GBBlockPool::acquire/release` reduce to `draken_malloc/draken_free` for every class
(`from_system` and `from_slab` merge); the small-block lists, `kRetainBytes`,
`kLocalBlocks`/`kLocalBytes` and the thread-exit spill are deleted. `gb_pool_tel`
keeps hits/misses only if they still mean something — otherwise deleted with the pool.

### 3.3 Allocations outside `draken_malloc` (separate step)

Part of Q32's faults are not `draken_malloc`: `vector<GBLanes>` uses `std::allocator`
(7% of faults), and the mimalloc frames (26%) are partly plain `malloc`/`new`. Moving
those onto a draken STL allocator (as `TrackedAllocator` does for accounting) is a
second step, measured on its own after 3.1–3.2.

### 3.4 Risks

- **Pinned slots.** One live 4 KiB block pins its 2 MiB slot; with 512 blocks per slot
  a slot is likelier to be held by stragglers. Measure: free-faulted bytes and slots
  held by partially-used class slabs at query end.
- **Contention.** Many more acquires go through the pool (Q32: ~600 k blocks per try).
  The 2026-10-12 lesson (one mutex per class = top frame on Q32) applies: the fast path
  must stay lock-free per thread.
- **Foreign memory.** Unchanged: the tier is decided by address range, so system-
  allocator pointers from foreign libraries are never mistaken for pool blocks.
- **Every draken caller moves.** Buffers of 4–64 KiB in scan, decode and kernels change
  tier at once; the A/B must cover the whole battery, not only GB queries.

## 4. A/B plan

Step 0 — counters: slab pool acquires/faults per class, partially-used class-slab
slots and their free bytes at query end, peak in-use bytes per query.

Arms (one changed build per arm, private tree on the box, never in main until
validated):

- base: main snapshot.
- B: 3.1 + 3.2 (small classes, GB pool retired).
- B+3.3 later, as its own A/B against B.

Allocators: x86 glibc (no preload) and x86 mimalloc (`make`'s preload,
`MIMALLOC_PURGE_DELAY=1000`); Mac system allocator. Each allocator is its own A/B
(base vs B on the same allocator); results are not compared across allocators.

Shape: official ClickBench (process per query, 1 fill + 2 hot), ≥ 9 interleaved
rounds, arm order alternating per round, `dev/ab_bench.py`.

Queries: whole ClickBench battery, then TPC-H SF1 (rugo parquet). Targets Q32, Q15,
Q16, Q18, Q35; guards Q09 (low-cardinality RSS trap), Q20, Q25. Q01 is not an
allocation control (it allocates lane segments too).

Record per try: wall, minflt, stime, peak RSS, `slab_pool_*`, chunk-cache bytes and
give-way.

Pass: Q32 hot wall faster with separated round ranges on every allocator; Q32 minflt
per hot try down by an order of magnitude; battery hot sum not slower; no guard > 1.02;
Q09 peak RSS within 5%. Then a c6a official-shape confirmation. Slower = deleted.

## 5. Open

1. Smallest class: 4 KiB proposed (one page). Lower means more blocks per slot and a
   larger bitmap.
2. Prod allocator: whether the prod container entrypoint sets the mimalloc
   `LD_PRELOAD` (`draken.preload_library_path()`) is unverified — it decides which
   x86 arm prod follows.
