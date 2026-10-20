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
// draken::TrackedAllocator container (charged by the exact request), and
// whatever a draken::MemCharge (below) holds. Smaller allocations, Python, and
// third-party libraries are not charged; the measured gap between charged bytes
// and RSS is what the container reserve covers.
//
// MemCharge exists because the threshold left the largest query state
// unaccounted: H2O g10's group-by pool blocks are nearly all below 64 KiB and
// the join build holds std::vector / new[] arrays, so on the 15 GB x86 box the
// charged peak was 7.2 GiB against a 14.4 GiB RSS and the chunk cache never
// gave way (measured 2026-10-19). With them charged, g10 reads 13.6 GiB charged
// against 14.6 GiB RSS and the cache gives way as the query grows.
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
// The cache may hold at most  min(B, C - R - charged).  When a charge pushes
// query memory up so the cache is over that, the registered shrinker is
// called on the charging thread, before the allocation is used, with the
// cache size to shrink to. With C == 0 (unconfigured) the limit is B alone.
void    draken_mem_configure(int64_t container_bytes, int64_t reserve_bytes, int64_t cache_budget_bytes);
int64_t draken_mem_container(void);
int64_t draken_mem_reserve(void);
int64_t draken_mem_cache_budget(void);
// Current cache limit: min(B, C - R - charged), floored at 0, further clamped
// while the OS memory compressor is busy (macOS; mem_account.cpp): a sample above
// the compression-rate threshold clamps the limit to what the cache holds less
// what was just compressed, never below a floor share of B, and the clamp lifts
// a hold period after the last pressured sample.
int64_t draken_mem_cache_limit(void);
// Compressor pressure since draken_mem_reset_peak: the highest sampled
// compression rate (pages/s) and the number of samples over the threshold.
int64_t draken_mem_compress_rate_max(void);
int64_t draken_mem_pressure_samples(void);
void    draken_mem_cache_add(int64_t bytes);
void    draken_mem_cache_sub(int64_t bytes);
int64_t draken_mem_cache_bytes(void);
typedef void (*draken_mem_shrink_fn)(int64_t target_cache_bytes);
void    draken_mem_set_shrinker(draken_mem_shrink_fn fn);

#ifdef __cplusplus
}

namespace draken {

// A charge on the process account for memory no draken allocator charges -
// std::vector / new[] buffers, or blocks below DRAKEN_MEM_ACCOUNT_MIN that a
// pool holds in bulk. Move-only: set() moves the charge to a byte count, adopt()
// takes over another holder's charge with the memory it describes, and the
// destructor releases whatever is held, so the account balances on every path
// (including a query that fails part-way).
class MemCharge {
  public:
    MemCharge() = default;
    MemCharge(const MemCharge&) = delete;
    MemCharge& operator=(const MemCharge&) = delete;
    MemCharge(MemCharge&& o) noexcept : bytes_(o.bytes_) { o.bytes_ = 0; }
    MemCharge& operator=(MemCharge&& o) noexcept {
        if (this != &o) {
            release();
            bytes_ = o.bytes_;
            o.bytes_ = 0;
        }
        return *this;
    }
    ~MemCharge() { release(); }

    void set(int64_t bytes) {
        if (bytes > bytes_) draken_mem_charge(bytes - bytes_);
        else if (bytes < bytes_) draken_mem_uncharge(bytes_ - bytes);
        bytes_ = bytes;
    }
    void adopt(MemCharge& o) noexcept {
        bytes_ += o.bytes_;
        o.bytes_ = 0;
    }
    void release() noexcept {
        if (bytes_ != 0) {
            draken_mem_uncharge(bytes_);
            bytes_ = 0;
        }
    }
    int64_t bytes() const noexcept { return bytes_; }

  private:
    int64_t bytes_ = 0;
};

}  // namespace draken
#endif

#endif  // DRAKEN_CORE_MEM_ACCOUNT_H
