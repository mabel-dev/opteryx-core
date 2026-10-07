// Process-wide memory account for large heap allocations.
//
// One account per process. It lives in draken_native.so (mem_account.cpp is
// compiled into that extension only — never into a consumer, or the process
// would hold two accounts) and every other extension resolves these C symbols
// at load time, the same way it resolves draken_identity_sel: draken_native is
// loaded RTLD_GLOBAL by `import draken` before any consumer.
//
// What is charged: every allocation of at least DRAKEN_MEM_ACCOUNT_MIN bytes
// made through draken_malloc / draken_calloc / draken_realloc /
// draken_aligned_malloc (charged by the allocator's usable size, so the free
// side can uncharge without knowing the request), and every allocation of a
// draken::TrackedAllocator container (charged by the exact request). Smaller
// allocations, Python, and third-party libraries are not charged; the measured
// gap between charged bytes and RSS is what the container reserve covers.
//
// The account is a measurement in this phase: charging never refuses and never
// blocks. Peak query memory (docs/C7_PAGE_CACHE_DESIGN.md §21) sits in
// ~10^5 allocations of 64-200 KiB, hence the 64 KiB threshold.
#ifndef DRAKEN_CORE_MEM_ACCOUNT_H
#define DRAKEN_CORE_MEM_ACCOUNT_H

#include <stdint.h>

#define DRAKEN_MEM_ACCOUNT_MIN ((int64_t)65536)

#ifdef __cplusplus
extern "C" {
#endif

void    draken_mem_charge(int64_t bytes);
void    draken_mem_uncharge(int64_t bytes);
// Bytes currently charged, process-wide.
int64_t draken_mem_charged(void);
// High-water mark of draken_mem_charged() since the last reset.
int64_t draken_mem_peak(void);
// Restart the high-water mark at the current charge (e.g. at query start).
void    draken_mem_reset_peak(void);

// --- Cache give-way (C7) ---------------------------------------------------
// The account also knows how many bytes a cache holds (reported separately
// from query bytes) and the limits that bound it:
//   container C : the memory the process may use (cgroup limit, else RAM)
//   reserve   R : held back for unaccounted memory (Python, small allocations)
//   budget    B : the most the cache may hold
// The cache may hold at most
//     min(B,  C - R - charged,  cache + system_available - R)
// The third term is the OPERATING SYSTEM's view: memory it reports as really
// available (Linux MemAvailable and, under a cgroup v2 limit, limit minus
// non-reclaimable usage; macOS free + speculative + file-backed pages). Without
// it a host shared with other processes lets the cache grow into memory the OS
// then compresses or swaps — every "hit" paying a kernel decompression
// (measured, docs/C7_PAGE_CACHE_DESIGN.md §26). Read at most every 100 ms.
// When the limit falls below the cache's size, the registered shrinker is
// called (on the charging / refreshing thread) with the size to shrink to.
// With C == 0 (unconfigured) the second term is dropped.
void    draken_mem_configure(int64_t container_bytes, int64_t reserve_bytes, int64_t cache_budget_bytes);
int64_t draken_mem_container(void);
int64_t draken_mem_reserve(void);
int64_t draken_mem_cache_budget(void);
// Current cache limit: min(B, C - R - charged), floored at 0.
int64_t draken_mem_cache_limit(void);
void    draken_mem_cache_add(int64_t bytes);
void    draken_mem_cache_sub(int64_t bytes);
int64_t draken_mem_cache_bytes(void);
// The OS's available memory as of the last refresh (-1 = not readable here).
int64_t draken_mem_system_available(void);
// Re-read the OS figure if it is older than 100 ms, and shrink the cache if the
// limit fell below it. Cheap when fresh (one clock read).
void    draken_mem_refresh(void);
typedef void (*draken_mem_shrink_fn)(int64_t target_cache_bytes);
void    draken_mem_set_shrinker(draken_mem_shrink_fn fn);

#ifdef __cplusplus
}
#endif

#endif  // DRAKEN_CORE_MEM_ACCOUNT_H
