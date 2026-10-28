// Process-wide slab pool for the engine's large transient buffers.
//
// WHY. Peak query memory sits in ~10^5 allocations of 64 KiB and up (GROUP BY
// lanes, partitions and key arenas; parquet decode output; the scan pool), and
// the measured cost of that memory was never the containers: it was the kernel
// handing out fresh pages on every query - first-touch faults, zero-fill and
// munmap/mmap churn (40% of ClickBench Q32 samples; ~70 us of mmap/TLB work per
// row group on Q01; Pivot measures the same idea at 46 ns vs 249 ns per 2 MB
// acquire+release and 21% on TPC-H q21 from huge pages alone).
//
// WHAT. One reservation of address space per process, mmap'd once (PROT_NONE,
// 256 GiB - 64 GiB on riscv64, whose Sv39 address space cannot hold more - never
// touched by the kernel's commit accounting), cut into 2 MiB
// SLOTS. A slot is made writable the first time it is acquired and faulted by its
// first write - once; afterwards it is recycled through this pool for the life of
// the process, so a hot query writes into pages the last query already faulted.
// On Linux the reservation is MADV_HUGEPAGE: a slot is one TLB entry and one
// fault. (Touching every slot at acquire was built and MEASURED a loss on both
// architectures - see slab_pool.cpp - so the pool does not prefault at acquire;
// `prefault_bytes` is the one place it faults ahead, at boot.)
//
// Requests from DRAKEN_SLAB_MIN (64 KiB) to 1 MiB come from power-of-two
// CLASSES carved out of a slot (a class slab: 2..32 blocks, a free bitmap, a
// per-thread current slab so the allocation fast path is one CAS and no lock);
// larger requests are a contiguous RUN of whole slots (first fit over the free
// runs, coalesced on release). Every byte handed out is slot-aligned or
// block-aligned, so a pointer's size is a slot-table lookup, not a header.
//
// BUDGET. A faulted slot is memory the process holds: faulted bytes are charged
// to the process memory account (mem_account.h) when faulted and uncharged when
// given back, so the chunk cache gives way to pool memory exactly as it does to
// any query memory. Free slots stay faulted up to `budget_bytes` (default 4 GiB,
// SHOW VARIABLES slab_pool_budget_bytes); a release that would exceed it gives
// the slot's pages back to the OS (MADV_DONTNEED; the mapping is kept - the
// pool never munmaps), and the next acquire faults it again. `prefault_bytes`
// (default 0) faults that many bytes into the free runs at configure time, for
// a server that would rather pay at boot than on its first query.
//
// SINGLE HOME. Compiled into draken_native only (and into standalone native
// binaries, which have no draken_native); every other extension resolves these
// C symbols at load, like draken_mem_*. Two instances in one process would be
// two reservations - never add this file to a consumer's sources.
//
// Everything below DRAKEN_SLAB_MIN is the system allocator's, untouched.
#ifndef DRAKEN_CORE_SLAB_POOL_H
#define DRAKEN_CORE_SLAB_POOL_H

#include <stddef.h>
#include <stdint.h>

// Requests of at least this many bytes are pool-served (== DRAKEN_MEM_ACCOUNT_MIN:
// the account's "large" threshold and the pool's are one line).
#define DRAKEN_SLAB_MIN   ((size_t)65536)
// Slot size: the x86-64 / arm64 huge page.
#define DRAKEN_SLAB_SLOT  ((size_t)2 << 20)

#ifdef __cplusplus
extern "C" {
#endif

// Reservation bounds, for the inline ownership test in alloc.h. Both 0 until the
// pool's first use; set once and never changed.
extern uintptr_t draken_slab_base;
extern uintptr_t draken_slab_end;

// A block of at least `bytes` (>= DRAKEN_SLAB_MIN). Contents are unspecified (a
// recycled block holds what its last user left). NULL only when the reservation
// is exhausted or the OS refuses the pages - the same failure a malloc has.
void*  draken_slab_alloc(size_t bytes);
// Return a block from draken_slab_alloc / draken_slab_realloc.
void   draken_slab_free(void* p);
// The size of the block holding p (its class size or run size); aborts on a
// pointer the pool did not hand out.
size_t draken_slab_usable_size(const void* p);
// Resize a pool block to at least `bytes` (>= DRAKEN_SLAB_MIN), keeping its
// contents up to the smaller size. In place when the class or run allows, else
// a new block + copy + free. NULL on failure with p untouched.
void*  draken_slab_realloc(void* p, size_t bytes);

// Policy (process-wide, any time; the pool is built lazily on first use).
void    draken_slab_configure(int64_t budget_bytes, int64_t prefault_bytes);
int64_t draken_slab_budget_bytes(void);
int64_t draken_slab_prefault_bytes(void);

typedef struct {
    int64_t reserved_bytes;      // address space mapped at init
    int64_t mapped_bytes;        // slots made writable so far (the frontier)
    int64_t faulted_bytes;       // slots handed out or retained with their pages (== bytes charged)
    int64_t in_use_bytes;        // slots in runs or class slabs handed out
    int64_t free_faulted_bytes;  // faulted slots waiting in the free runs
    int64_t class_slabs;         // slots currently carved into class blocks
    int64_t acquires;            // slot runs / class slabs taken from the pool
    int64_t faults;              // slots handed out fresh or after a give-back (their pages fault on first write)
    int64_t give_backs;          // slots given back to the OS (over budget)
    int64_t huge_pages;          // 1 when the reservation is MADV_HUGEPAGE'd
} DrakenSlabStats;
DrakenSlabStats draken_slab_stats(void);

#ifdef __cplusplus
}
#endif

#endif  // DRAKEN_CORE_SLAB_POOL_H
