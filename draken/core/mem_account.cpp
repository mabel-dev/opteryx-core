// Process-wide memory account — see mem_account.h. Compiled into draken_native
// ONLY (build_common.py); standalone native builds (skene/Makefile) compile
// their own copy because they have no draken_native to resolve against.
#include "mem_account.h"

#include <atomic>
#include <chrono>
#include <cstdint>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

namespace {
std::atomic<int64_t> g_charged{0};
std::atomic<int64_t> g_peak{0};
std::atomic<int64_t> g_container{0};
std::atomic<int64_t> g_reserve{0};
std::atomic<int64_t> g_cache_budget{0};
std::atomic<int64_t> g_cache_bytes{0};
std::atomic<draken_mem_shrink_fn> g_shrinker{nullptr};

// ---- Compressor-pressure give-way (macOS) ------------------------------------
// The charged term above only sees memory draken charges. On SF100 Q18 the
// machine ran short with charged memory far below the container: the 33 GB cache
// sat idle while the macOS memory compressor compressed and decompressed GBs a
// second, and Q18 ran 2x slower. Free-page counts are no signal there - free
// stays ~0 under any file I/O, so a give-way keyed on them empties the cache for
// good - but compressor activity is: ~0 pages/s at rest, ~300K pages/s under that
// load. A sample above kCompressPagesPerSec clamps the cache to what it holds less
// what the compressor just compressed, never below kFloorPercent of the budget;
// the clamp lifts kHoldNs after the last pressured sample so the cache refills.
// The floor was swept on the SF100 suite (64 GB Mac): 25% gave Q18 1.9-2.1 s
// (from ~4.4) but cost Q21 ~17%; 60% sits near what the full cache holds, so the
// clamp often frees nothing and Q18 stayed at 4.2 s in one of two rounds; 40% gave
// Q18 1.92/1.96 s with the suite total 21.9/22.2 s against 22.3-24.6 s without it.
// Other platforms have no compressor signal here and keep the charged-only limit.
constexpr int64_t kSampleNs = 50'000'000;              // at most one sample per 50 ms
constexpr int64_t kCompressPagesPerSec = 10'000;       // ~160 MB/s at 16 KB pages
constexpr int64_t kFloorPercent = 40;                  // of the budget, never given below
constexpr int64_t kHoldNs = 2'000'000'000;             // clamp outlives the pressure by 2 s

std::atomic<int64_t> g_sampled_at{0};                  // steady_clock ns since boot
std::atomic<int64_t> g_compressions{-1};               // counter at the last sample
std::atomic<int64_t> g_clamp{-1};                      // -1 = no pressure clamp
std::atomic<int64_t> g_pressured_at{0};
std::atomic<int64_t> g_rate_max{0};                    // pages/s, since reset_peak
std::atomic<int64_t> g_pressure_samples{0};            // since reset_peak

int64_t steady_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// The compressor's cumulative compressions (pages), or -1 when not available.
int64_t read_compressions(int64_t* page_bytes) {
#if defined(__APPLE__)
    static const mach_port_t host = mach_host_self();
    vm_statistics64_data_t vm;
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    if (host_statistics64(host, HOST_VM_INFO64, reinterpret_cast<host_info64_t>(&vm), &count) != KERN_SUCCESS)
        return -1;
    *page_bytes = static_cast<int64_t>(vm_kernel_page_size);
    return static_cast<int64_t>(vm.compressions);
#else
    (void)page_bytes;
    return -1;
#endif
}

// Refresh the pressure clamp from a new sample when the last is older than
// kSampleNs (one caller wins the CAS; the rest read the clamp already there).
void sample_pressure() {
    const int64_t now = steady_ns();
    int64_t last = g_sampled_at.load(std::memory_order_relaxed);
    if (now - last < kSampleNs ||
        !g_sampled_at.compare_exchange_strong(last, now, std::memory_order_relaxed))
        return;
    int64_t page = 0;
    const int64_t c = read_compressions(&page);
    const int64_t prev = g_compressions.exchange(c, std::memory_order_relaxed);
    if (c < 0 || prev < 0 || last <= 0) return;
    const int64_t pages = c - prev;
    const int64_t rate = pages * 1'000'000'000 / (now - last > 0 ? now - last : 1);
    int64_t hi = g_rate_max.load(std::memory_order_relaxed);
    while (rate > hi && !g_rate_max.compare_exchange_weak(hi, rate, std::memory_order_relaxed)) {
    }
    if (rate > kCompressPagesPerSec) {
        g_pressure_samples.fetch_add(1, std::memory_order_relaxed);
        g_pressured_at.store(now, std::memory_order_relaxed);
        const int64_t floor = g_cache_budget.load(std::memory_order_relaxed) * kFloorPercent / 100;
        int64_t target = g_cache_bytes.load(std::memory_order_relaxed) - pages * page;
        if (target < floor) target = floor;
        const int64_t clamp = g_clamp.load(std::memory_order_relaxed);
        if (clamp < 0 || target < clamp) g_clamp.store(target, std::memory_order_relaxed);
    } else if (now - g_pressured_at.load(std::memory_order_relaxed) > kHoldNs) {
        g_clamp.store(-1, std::memory_order_relaxed);
    }
}

int64_t cache_limit_for(int64_t charged) {
    const int64_t budget = g_cache_budget.load(std::memory_order_relaxed);
    const int64_t container = g_container.load(std::memory_order_relaxed);
    if (container <= 0) return budget;
    int64_t room = container - g_reserve.load(std::memory_order_relaxed) - charged;
    if (room < 0) room = 0;
    int64_t limit = room < budget ? room : budget;
    sample_pressure();
    const int64_t clamp = g_clamp.load(std::memory_order_relaxed);
    if (clamp >= 0 && clamp < limit) limit = clamp;
    return limit;
}
}  // namespace

extern "C" {

void draken_mem_charge(int64_t bytes) {
    const int64_t now = g_charged.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    int64_t peak = g_peak.load(std::memory_order_relaxed);
    while (now > peak &&
           !g_peak.compare_exchange_weak(peak, now, std::memory_order_relaxed)) {
    }
    // Give-way: query memory grew past what leaves the cache its room.
    if (g_cache_bytes.load(std::memory_order_relaxed) > 0) {
        const int64_t limit = cache_limit_for(now);
        if (g_cache_bytes.load(std::memory_order_relaxed) > limit) {
            draken_mem_shrink_fn fn = g_shrinker.load(std::memory_order_acquire);
            if (fn != nullptr) fn(limit);
        }
    }
}

void draken_mem_uncharge(int64_t bytes) {
    g_charged.fetch_sub(bytes, std::memory_order_relaxed);
}

int64_t draken_mem_charged(void) { return g_charged.load(std::memory_order_relaxed); }

int64_t draken_mem_peak(void) { return g_peak.load(std::memory_order_relaxed); }

void draken_mem_reset_peak(void) {
    g_peak.store(g_charged.load(std::memory_order_relaxed), std::memory_order_relaxed);
    g_rate_max.store(0, std::memory_order_relaxed);
    g_pressure_samples.store(0, std::memory_order_relaxed);
}

int64_t draken_mem_compress_rate_max(void) { return g_rate_max.load(std::memory_order_relaxed); }
int64_t draken_mem_pressure_samples(void) { return g_pressure_samples.load(std::memory_order_relaxed); }

void draken_mem_configure(int64_t container_bytes, int64_t reserve_bytes, int64_t cache_budget_bytes) {
    g_container.store(container_bytes, std::memory_order_relaxed);
    g_reserve.store(reserve_bytes, std::memory_order_relaxed);
    g_cache_budget.store(cache_budget_bytes, std::memory_order_relaxed);
}

int64_t draken_mem_container(void) { return g_container.load(std::memory_order_relaxed); }
int64_t draken_mem_reserve(void) { return g_reserve.load(std::memory_order_relaxed); }
int64_t draken_mem_cache_budget(void) { return g_cache_budget.load(std::memory_order_relaxed); }

int64_t draken_mem_cache_limit(void) {
    return cache_limit_for(g_charged.load(std::memory_order_relaxed));
}

void draken_mem_cache_add(int64_t bytes) { g_cache_bytes.fetch_add(bytes, std::memory_order_relaxed); }
void draken_mem_cache_sub(int64_t bytes) { g_cache_bytes.fetch_sub(bytes, std::memory_order_relaxed); }
int64_t draken_mem_cache_bytes(void) { return g_cache_bytes.load(std::memory_order_relaxed); }

void draken_mem_set_shrinker(draken_mem_shrink_fn fn) {
    g_shrinker.store(fn, std::memory_order_release);
}

}  // extern "C"
