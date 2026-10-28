# GROUP BY small-block churn (ClickBench Q32) — design note

Status: RULED 2026-10-27 (§2). Built in private trees and A/B'd 2026-10-28: FAILED
at the 4 GiB budget (§5) and at 8 GiB (§5.2), not landed.

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

## 5. Result (2026-10-28): FAILED — not landed

B (§3.1 + §3.2, 4 KiB smallest class, GB pool retired) was built in private trees
(Mac `~/opteryx-smallslab/{base,b}`, x86 `~/ss-20261027/{base,b}`) from one main
snapshot and run per §4: process per query, 1 fill + 2 hot, 9 interleaved rounds,
arm order alternating per round and query. B passed `make q`, the native tests under
ASan/UBSan (Mac + Linux) and TSan (Mac), and the memory-charge balance test. Main was
not touched.

| run | battery hot sum B/base | B faster rounds | Q32 | Q33 / Q34 |
|---|---|---|---|---|
| Mac ClickBench, system | 1.013 | 0/9 | 1.006 | 1.071 / 1.079 |
| Mac TPC-H SF1, system | 1.013 | 1/9 | — | — |
| x86 ClickBench, glibc | 1.022 | 0/9 | **1.080** | 1.064 / 1.062 |
| x86 ClickBench, mimalloc | 1.011 | 0/9 | **0.976** (9/9, separated) | 1.059 / 1.053 |
| x86 TPC-H SF1, glibc | 1.005 | 1/9 | — | — |
| x86 TPC-H SF1, mimalloc | 1.008 | 2/9 | — | — |

Guards: Q09 1.045 Mac (0/9), 1.013 x86 glibc (0/9) — over the 1.02 bar on the Mac.
Peak RSS +5–20% on TPC-H (Mac). Digest differences seen are LIMIT ties present in
both arms (several digests per arm; Q32 checked row by row: rows above the tie value
identical).

Why (slab counters, per process): with small blocks in the pool, peak slab demand
exceeds the 4 GiB free-faulted budget, so B gives slots back to the OS and faults
them again — x86 Q32 9,300 give-backs vs 0 in base, Q33 4,770 vs 800, Q18 4,450 vs
0; system time on Q32 glibc 0.72 → 1.51 s, Q33 0.48 → 1.05 s. The target effect
exists — x86 mimalloc Q32 minor faults 1.20 M → 0.31 M per hot try — but is bought
back by give-back churn.

Also found: x86 base Q32 on **glibc** takes 0.29 M faults per hot try, not 1.15 M;
the 1.15 M of §1 is mimalloc's purge (the preload `make` uses). The c6a entry runs
glibc, so §1's upper bound overstates what this lever could do on the board.

### 5.1 B with an 8 GiB slab budget (x86 ClickBench, 2026-10-28)

Same trees, same shape; B's arm ran with `SLAB_POOL_BUDGET_BYTES=8589934592`, base at
the default 4 GiB. Knob proven first: B Q32 give-backs 9,300 → 3,008, Q33 4,770 → 0.

| run | battery B/base | B faster rounds | Q32 | Q18 | Q33 / Q34 | > 1.02 |
|---|---|---|---|---|---|---|
| glibc | **0.994** | 9/9 | 1.006 (5/9) | 0.978 | 0.981 / 0.987 | Q04 1.028, Q05 1.022, Q24 1.053 (median 0.993), Q40 1.022 |
| mimalloc | **0.983** | 9/9 | **0.903** (9/9, separated) | 0.973 | 0.983 / 0.993 | Q05 1.027 |

Q32 mimalloc: minor faults 1.20 M → 0.24 M per hot try, system time 3.04 → 1.06 s.
Guard Q09 1.003 / 1.003. Q15 1.017–1.019 and Q35 1.013–1.015 slower on both (0/9).
Hot major faults on glibc rose 76 k → 217 k (B holds more memory on a 15 GB box;
page-cache displacement suspected, not proven). Max peak RSS unchanged (13.4 GiB).

### 5.2 §4 completed at 8 GiB (2026-10-28)

| run | battery B/base | B faster rounds | Q32 | Q09 (guard) | peak RSS B/base median (max) |
|---|---|---|---|---|---|
| Mac ClickBench, system | **0.987** | 9/9 | 0.976 (8/9) | **1.026** (1/9) | 1.016 (1.108) |
| Mac TPC-H SF1, system | 1.006 | 1/9 | — | 1.041 (TPC-H Q09) | **1.105** (1.188) |
| x86 ClickBench, glibc | **0.994** | 9/9 | 1.006 (5/9) | 1.003 | 1.039 (1.218) |
| x86 ClickBench, mimalloc | **0.983** | 9/9 | **0.903** (9/9) | 1.003 | 1.032 (1.169) |
| x86 TPC-H SF1, glibc | 1.004 | 0/9 | — | — | **1.151** (1.334) |
| x86 TPC-H SF1, mimalloc | 1.005 | 3/9 | — | — | **1.108** (1.203) |

Mac ClickBench at 8 GiB: Q33 0.957, Q34 0.964, Q18 0.961 (all 9/9); but the short
queries lose — Q41 1.063, Q06 1.056, Q37 1.042, Q40 1.029 (0–1/9). x86 TPC-H glibc:
Q01 1.030, Q18 1.033, Q22 1.026 (0/9). TPC-H SF1 never reaches the budget, so its
loss and its +10–15% peak RSS are the small classes' own cost, not the budget's.

**Verdict against §4: FAIL.** Q32 is not faster on glibc; TPC-H is slower on every
allocator; the Q09 guard is 1.026 on the Mac (bar 1.02); peak RSS is up 10–15%
(median) on TPC-H. The ClickBench win (0.983–0.994) needs the 8 GiB budget, which
changes a ruled value (4 GiB, 2026-10-24 sweep knee). Not landed.

## 6. Open

1. Smallest class: 4 KiB proposed (one page). Lower means more blocks per slot and a
   larger bitmap.
2. Prod allocator: whether the prod container entrypoint sets the mimalloc
   `LD_PRELOAD` (`draken.preload_library_path()`) is unverified — it decides which
   x86 arm prod follows.
