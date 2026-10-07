// Process-wide memory account — see mem_account.h. Compiled into draken_native
// ONLY (build_common.py); standalone native builds (skene/Makefile) compile
// their own copy because they have no draken_native to resolve against.
#include "mem_account.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>

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
std::atomic<int64_t> g_sys_avail{-1};       // bytes; -1 = unknown
std::atomic<int64_t> g_sys_read_ns{0};      // steady-clock ns of the last read
std::atomic<bool>    g_sys_reading{false};
constexpr int64_t kRefreshNs = 100'000'000;  // 100 ms

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

#if defined(__linux__)
// One "key: value kB" field of a /proc-style file, in bytes; -1 if absent.
int64_t read_kb_field(const char* path, const char* key) {
    FILE* f = std::fopen(path, "r");
    if (f == nullptr) return -1;
    char line[256];
    const size_t klen = std::strlen(key);
    int64_t out = -1;
    while (std::fgets(line, sizeof(line), f) != nullptr) {
        if (std::strncmp(line, key, klen) == 0 && line[klen] == ':') {
            long long kb = 0;
            if (std::sscanf(line + klen + 1, "%lld", &kb) == 1) out = static_cast<int64_t>(kb) * 1024;
            break;
        }
    }
    std::fclose(f);
    return out;
}

// One "key value" field of a cgroup v2 stat file, in bytes; -1 if absent.
int64_t read_stat_field(const char* path, const char* key) {
    FILE* f = std::fopen(path, "r");
    if (f == nullptr) return -1;
    char line[256];
    const size_t klen = std::strlen(key);
    int64_t out = -1;
    while (std::fgets(line, sizeof(line), f) != nullptr) {
        if (std::strncmp(line, key, klen) == 0 && line[klen] == ' ') {
            long long v = 0;
            if (std::sscanf(line + klen + 1, "%lld", &v) == 1) out = static_cast<int64_t>(v);
            break;
        }
    }
    std::fclose(f);
    return out;
}

// A single integer file (cgroup v2 memory.max / memory.current); -1 if absent
// or "max" (no limit).
int64_t read_int_file(const char* path) {
    FILE* f = std::fopen(path, "r");
    if (f == nullptr) return -1;
    char buf[64] = {0};
    const bool ok = std::fgets(buf, sizeof(buf), f) != nullptr;
    std::fclose(f);
    if (!ok || std::strncmp(buf, "max", 3) == 0) return -1;
    long long v = 0;
    return std::sscanf(buf, "%lld", &v) == 1 ? static_cast<int64_t>(v) : -1;
}

int64_t read_system_available() {
    int64_t avail = read_kb_field("/proc/meminfo", "MemAvailable");
    const int64_t max = read_int_file("/sys/fs/cgroup/memory.max");
    if (max > 0) {
        const int64_t cur = read_int_file("/sys/fs/cgroup/memory.current");
        const int64_t inactive_file = read_stat_field("/sys/fs/cgroup/memory.stat", "inactive_file");
        if (cur >= 0) {
            // Inactive file cache is reclaimable: it is not memory the cgroup is
            // short of.
            int64_t room = max - cur + (inactive_file > 0 ? inactive_file : 0);
            if (room < 0) room = 0;
            if (avail < 0 || room < avail) avail = room;
        }
    }
    return avail;
}
#elif defined(__APPLE__)
int64_t read_system_available() {
    vm_statistics64_data_t vm;
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    if (host_statistics64(mach_host_self(), HOST_VM_INFO64,
                          reinterpret_cast<host_info64_t>(&vm), &count) != KERN_SUCCESS)
        return -1;
    vm_size_t page = 0;
    if (host_page_size(mach_host_self(), &page) != KERN_SUCCESS) return -1;
    // Free, speculative and file-backed pages are what the kernel can hand out
    // without compressing or swapping anyone's memory.
    const uint64_t pages = static_cast<uint64_t>(vm.free_count) + vm.speculative_count +
                           vm.external_page_count;
    return static_cast<int64_t>(pages * page);
}
#else
int64_t read_system_available() { return -1; }
#endif

int64_t cache_limit_for(int64_t charged) {
    int64_t limit = g_cache_budget.load(std::memory_order_relaxed);
    const int64_t reserve = g_reserve.load(std::memory_order_relaxed);
    const int64_t container = g_container.load(std::memory_order_relaxed);
    if (container > 0) {
        int64_t room = container - reserve - charged;
        if (room < limit) limit = room;
    }
    const int64_t avail = g_sys_avail.load(std::memory_order_relaxed);
    if (avail >= 0) {
        const int64_t room = g_cache_bytes.load(std::memory_order_relaxed) + avail - reserve;
        if (room < limit) limit = room;
    }
    return limit < 0 ? 0 : limit;
}

void shrink_if_over(int64_t charged) {
    if (g_cache_bytes.load(std::memory_order_relaxed) <= 0) return;
    const int64_t limit = cache_limit_for(charged);
    if (g_cache_bytes.load(std::memory_order_relaxed) > limit) {
        draken_mem_shrink_fn fn = g_shrinker.load(std::memory_order_acquire);
        if (fn != nullptr) fn(limit);
    }
}

// Re-read the OS figure when stale; one thread reads, the rest keep going.
void refresh_if_stale() {
    const int64_t t = now_ns();
    if (t - g_sys_read_ns.load(std::memory_order_relaxed) < kRefreshNs) return;
    bool expected = false;
    if (!g_sys_reading.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return;
    g_sys_avail.store(read_system_available(), std::memory_order_relaxed);
    g_sys_read_ns.store(t, std::memory_order_relaxed);
    g_sys_reading.store(false, std::memory_order_release);
    shrink_if_over(g_charged.load(std::memory_order_relaxed));
}
}  // namespace

extern "C" {

void draken_mem_charge(int64_t bytes) {
    const int64_t now = g_charged.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    int64_t peak = g_peak.load(std::memory_order_relaxed);
    while (now > peak &&
           !g_peak.compare_exchange_weak(peak, now, std::memory_order_relaxed)) {
    }
    // Give-way: query memory grew past what leaves the cache its room. Every
    // 256th charge on a thread also refreshes the OS figure (stale > 100 ms).
    if (g_cache_bytes.load(std::memory_order_relaxed) > 0) {
        static thread_local uint32_t n = 0;
        if ((++n & 255u) == 0) refresh_if_stale();
        shrink_if_over(now);
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

int64_t draken_mem_system_available(void) { return g_sys_avail.load(std::memory_order_relaxed); }

void draken_mem_refresh(void) { refresh_if_stale(); }

}  // extern "C"
