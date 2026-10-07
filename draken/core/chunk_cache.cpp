// Process-wide decompressed column-chunk cache — see chunk_cache.h.
// Compiled into draken_native ONLY (build_common.py).
//
// Structure: 16 shards by key hash, each with its own mutex, map and CLOCK
// ring. Eviction (CLOCK, ref bit per entry) and admission (cost per byte)
// happen one shard lock at a time; no path holds two shard locks.
//
// Lifetime: an entry's `state` word holds its reader pin count plus an
// EVICTED bit. Eviction sets the bit; whoever observes pins == 0 with the bit
// set — the evictor, or the last unpin — frees the entry. One atomic word, so
// exactly one side frees it.
//
// Re-entrancy: the cache allocates with plain malloc / std containers, never
// through draken_malloc or a TrackedAllocator, so evicting (which can run
// inside draken_mem_charge on a query thread) never charges the account.
#include "chunk_cache.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mem_account.h"

struct DrakenChunkEntry {
    std::string path;
    int64_t offset = 0;
    uint8_t* buffer = nullptr;
    int64_t bytes = 0;
    std::vector<int64_t> index;   // (body_offset, start, length) triples, sorted by body_offset
    double cost_per_byte = 0.0;   // decompression ns per cached byte
    std::atomic<uint32_t> state{0};
    std::atomic<uint8_t> ref{0};
};

namespace {

constexpr uint32_t kEvicted = 0x80000000u;
constexpr uint32_t kPinMask = 0x7fffffffu;
constexpr size_t kShards = 16;

struct Key {
    std::string path;
    int64_t offset;
    bool operator==(const Key& o) const { return offset == o.offset && path == o.path; }
};

struct KeyHash {
    size_t operator()(const Key& k) const {
        return std::hash<std::string>{}(k.path) ^
               (static_cast<size_t>(k.offset) * 0x9e3779b97f4a7c15ull);
    }
};

// Bound on remembered incomplete chunks per shard; the set is cleared when full
// (forgetting only costs one more discarded fill per chunk).
constexpr size_t kIncompleteCap = 65536;

struct Shard {
    std::mutex mu;
    std::unordered_map<Key, DrakenChunkEntry*, KeyHash> map;
    std::unordered_set<Key, KeyHash> incomplete;
    std::vector<DrakenChunkEntry*> ring;
    size_t hand = 0;
};

Shard g_shards[kShards];
std::atomic<size_t> g_next_shard{0};

std::atomic<int64_t> g_entries{0}, g_hits{0}, g_misses{0}, g_inserts{0}, g_refused{0},
    g_evictions{0}, g_give_way{0};

void destroy(DrakenChunkEntry* e) {
    std::free(e->buffer);
    delete e;
}

// Under the shard lock: drop ring[i].
void evict_at(Shard& sh, size_t i) {
    DrakenChunkEntry* e = sh.ring[i];
    sh.map.erase(Key{e->path, e->offset});
    sh.ring[i] = sh.ring.back();
    sh.ring.pop_back();
    if (sh.hand >= sh.ring.size()) sh.hand = 0;
    draken_mem_cache_sub(e->bytes);
    g_entries.fetch_sub(1, std::memory_order_relaxed);
    const uint32_t prev = e->state.fetch_or(kEvicted, std::memory_order_acq_rel);
    if ((prev & kPinMask) == 0) destroy(e);
}

// Free one entry from one shard. `respect_cost`: refuse to evict an entry that
// is more expensive per byte than `incoming_cost` (admission). Returns
// 1 evicted, 0 nothing evictable here, -1 admission says keep the residents.
int evict_one(Shard& sh, bool respect_cost, double incoming_cost, int64_t* freed) {
    std::lock_guard<std::mutex> lk(sh.mu);
    const size_t n = sh.ring.size();
    for (size_t step = 0; step < 2 * n && !sh.ring.empty(); ++step) {
        if (sh.hand >= sh.ring.size()) sh.hand = 0;
        DrakenChunkEntry* e = sh.ring[sh.hand];
        if (e->ref.exchange(0, std::memory_order_relaxed) != 0) {
            ++sh.hand;
            continue;
        }
        if (respect_cost && e->cost_per_byte > incoming_cost) return -1;
        *freed = e->bytes;
        evict_at(sh, sh.hand);
        return 1;
    }
    return 0;
}

// Evict until the cache holds at most `target` bytes. With `respect_cost`,
// stop (return false) at the first resident worth more per byte than the
// incoming entry.
bool make_room(int64_t target, bool respect_cost, double incoming_cost, bool give_way) {
    size_t idle = 0;
    while (draken_mem_cache_bytes() > target) {
        Shard& sh = g_shards[g_next_shard.fetch_add(1, std::memory_order_relaxed) % kShards];
        int64_t freed = 0;
        const int r = evict_one(sh, respect_cost, incoming_cost, &freed);
        if (r < 0) return false;
        if (r == 0) {
            if (++idle >= kShards) return draken_mem_cache_bytes() <= target;
            continue;
        }
        idle = 0;
        if (give_way) g_give_way.fetch_add(freed, std::memory_order_relaxed);
        else g_evictions.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

void shrink_to(int64_t target) { make_room(target, false, 0.0, true); }

Shard& shard_for(const Key& k) { return g_shards[KeyHash{}(k) % kShards]; }

struct RegisterShrinker {
    RegisterShrinker() { draken_mem_set_shrinker(&shrink_to); }
} g_register_shrinker;

}  // namespace

extern "C" {

int draken_cc_enabled(void) { return draken_mem_cache_budget() > 0 ? 1 : 0; }

const DrakenChunkEntry* draken_cc_lookup(const char* path, size_t path_len, int64_t chunk_offset,
                                         int predicated, int* fill_ok) {
    draken_mem_refresh();   // before any shard lock: a refresh may shrink the cache
    Key k{std::string(path, path_len), chunk_offset};
    Shard& sh = shard_for(k);
    std::lock_guard<std::mutex> lk(sh.mu);
    auto it = sh.map.find(k);
    if (it == sh.map.end()) {
        g_misses.fetch_add(1, std::memory_order_relaxed);
        *fill_ok = (predicated && sh.incomplete.count(k) != 0) ? 0 : 1;
        return nullptr;
    }
    *fill_ok = 0;
    DrakenChunkEntry* e = it->second;
    e->state.fetch_add(1, std::memory_order_acq_rel);
    e->ref.store(1, std::memory_order_relaxed);
    g_hits.fetch_add(1, std::memory_order_relaxed);
    return e;
}

int draken_cc_page(const DrakenChunkEntry* entry, int64_t body_offset,
                   const uint8_t** data, int64_t* len) {
    const std::vector<int64_t>& ix = entry->index;
    size_t lo = 0, hi = ix.size() / 3;
    while (lo < hi) {
        const size_t mid = (lo + hi) / 2;
        if (ix[3 * mid] < body_offset) lo = mid + 1;
        else hi = mid;
    }
    if (lo >= ix.size() / 3 || ix[3 * lo] != body_offset) return 0;
    *data = entry->buffer + ix[3 * lo + 1];
    *len = ix[3 * lo + 2];
    return 1;
}

void draken_cc_unpin(const DrakenChunkEntry* entry) {
    DrakenChunkEntry* e = const_cast<DrakenChunkEntry*>(entry);
    const uint32_t prev = e->state.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == (kEvicted | 1u)) destroy(e);
}

void draken_cc_mark_incomplete(const char* path, size_t path_len, int64_t chunk_offset) {
    Key k{std::string(path, path_len), chunk_offset};
    Shard& sh = shard_for(k);
    std::lock_guard<std::mutex> lk(sh.mu);
    if (sh.incomplete.size() >= kIncompleteCap) sh.incomplete.clear();
    sh.incomplete.insert(std::move(k));
}

uint8_t* draken_cc_fill_alloc(int64_t bytes) {
    return static_cast<uint8_t*>(std::malloc(static_cast<size_t>(bytes > 0 ? bytes : 1)));
}

void draken_cc_fill_discard(uint8_t* buffer) { std::free(buffer); }

int draken_cc_insert(const char* path, size_t path_len, int64_t chunk_offset,
                     uint8_t* buffer, int64_t buffer_bytes,
                     const int64_t* page_index, int32_t npages, int64_t cost_ns) {
    draken_mem_refresh();   // before any shard lock: a refresh may shrink the cache
    const int64_t budget = draken_mem_cache_budget();
    if (budget <= 0 || buffer_bytes <= 0 || buffer_bytes > budget) {
        std::free(buffer);
        g_refused.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }
    const double cost = static_cast<double>(cost_ns) / static_cast<double>(buffer_bytes);
    const int64_t limit = draken_mem_cache_limit();
    // Under query-memory pressure (the limit is below the budget) an insert may
    // only use free room: evicting residents to admit would refill the cache the
    // running queries are making it give away — churn, not caching.
    const bool pressure = limit < budget;
    const bool fits_free = draken_mem_cache_bytes() + buffer_bytes <= limit;
    if (buffer_bytes > limit || (pressure && !fits_free) ||
        (!fits_free && !make_room(limit - buffer_bytes, true, cost, false))) {
        std::free(buffer);
        g_refused.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    auto* e = new DrakenChunkEntry();
    e->path.assign(path, path_len);
    e->offset = chunk_offset;
    e->buffer = buffer;
    e->bytes = buffer_bytes;
    e->cost_per_byte = cost;
    e->index.reserve(static_cast<size_t>(npages) * 3);
    std::vector<size_t> order(static_cast<size_t>(npages));
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(),
              [&](size_t a, size_t b) { return page_index[3 * a] < page_index[3 * b]; });
    for (size_t i : order) {
        e->index.push_back(page_index[3 * i]);
        e->index.push_back(page_index[3 * i + 1]);
        e->index.push_back(page_index[3 * i + 2]);
    }

    Key k{e->path, chunk_offset};
    Shard& sh = shard_for(k);
    {
        std::lock_guard<std::mutex> lk(sh.mu);
        if (sh.map.find(k) != sh.map.end()) {   // another reader filled it first
            destroy(e);
            g_refused.fetch_add(1, std::memory_order_relaxed);
            return 0;
        }
        sh.map.emplace(std::move(k), e);
        sh.ring.push_back(e);
        draken_mem_cache_add(buffer_bytes);
    }
    g_entries.fetch_add(1, std::memory_order_relaxed);
    g_inserts.fetch_add(1, std::memory_order_relaxed);
    return 1;
}

void draken_cc_flush(void) { make_room(0, false, 0.0, false); }

DrakenChunkCacheStats draken_cc_stats(void) {
    DrakenChunkCacheStats s;
    s.bytes = draken_mem_cache_bytes();
    s.entries = g_entries.load(std::memory_order_relaxed);
    s.hits = g_hits.load(std::memory_order_relaxed);
    s.misses = g_misses.load(std::memory_order_relaxed);
    s.inserts = g_inserts.load(std::memory_order_relaxed);
    s.refused = g_refused.load(std::memory_order_relaxed);
    s.evictions = g_evictions.load(std::memory_order_relaxed);
    s.give_way_bytes = g_give_way.load(std::memory_order_relaxed);
    return s;
}

}  // extern "C"
