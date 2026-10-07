// Process-wide memory account — see mem_account.h. Compiled into draken_native
// ONLY (build_common.py); standalone native builds (skene/Makefile) compile
// their own copy because they have no draken_native to resolve against.
#include "mem_account.h"

#include <atomic>

namespace {
std::atomic<int64_t> g_charged{0};
std::atomic<int64_t> g_peak{0};
std::atomic<int64_t> g_container{0};
std::atomic<int64_t> g_reserve{0};
std::atomic<int64_t> g_cache_budget{0};
std::atomic<int64_t> g_cache_bytes{0};
std::atomic<draken_mem_shrink_fn> g_shrinker{nullptr};

int64_t cache_limit_for(int64_t charged) {
    const int64_t budget = g_cache_budget.load(std::memory_order_relaxed);
    const int64_t container = g_container.load(std::memory_order_relaxed);
    if (container <= 0) return budget;
    int64_t room = container - g_reserve.load(std::memory_order_relaxed) - charged;
    if (room < 0) room = 0;
    return room < budget ? room : budget;
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
}

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
