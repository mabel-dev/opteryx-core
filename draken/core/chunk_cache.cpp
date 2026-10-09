// Process-wide decompressed column-chunk cache — see chunk_cache.h.
// Compiled into draken_native ONLY (build_common.py).
//
// Structure: 16 shards by key hash, each with its own mutex, map, PROBATION
// FIFO, MAIN CLOCK ring and a key-only memory of recent probation evictions.
// Every eviction happens one shard lock at a time; no path holds two.
//
// Policy (rulings 2026-10-08, docs/C7_PAGE_CACHE_DESIGN.md §27):
// - Admission is S3-FIFO's: a new chunk enters PROBATION. While the cache has
//   room nothing is evicted, so an empty cache fills completely. Once full,
//   probation is evicted first while it holds more than kProbationPercent of
//   the limit: a chunk hit while on probation moves to MAIN, one that was not
//   is evicted and its key remembered (the last kRemembered of them).
//   A remembered chunk that is filled again skips probation.
// - Eviction from MAIN is CLOCK, cost weighted: a hit gives the chunk
//   `weight` extra sweeps (0..kMaxWeight) on top of the ref bit, from its cost
//   to rebuild per byte, so an expensive chunk outlives a cheap one while both
//   are going cold. weight 0 is plain CLOCK.
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
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
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
    uint8_t weight = 0;           // extra CLOCK sweeps a hit buys (cost per byte)
    uint8_t lives = 0;            // extra sweeps left (MAIN only)
    bool main = false;            // false: on probation
    std::atomic<uint32_t> state{0};
    std::atomic<uint8_t> ref{0};
};

namespace {

constexpr uint32_t kEvicted = 0x80000000u;
constexpr uint32_t kPinMask = 0x7fffffffu;
constexpr size_t kShards = 16;

// Probation's share of the cache limit (S3-FIFO's small queue).
constexpr int64_t kProbationPercent = 10;
// Recent probation evictions remembered (keys only), split across the shards.
constexpr size_t kRemembered = 2048;
constexpr size_t kRememberedPerShard = kRemembered / kShards;
// Extra sweeps a hit can buy: 8 lives in all with the ref bit's own sweep.
constexpr uint8_t kMaxWeight = 7;

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
    std::deque<DrakenChunkEntry*> probation;    // FIFO, oldest at the front
    std::vector<DrakenChunkEntry*> ring;        // MAIN
    size_t hand = 0;
    std::deque<Key> remembered;                 // oldest at the front
    std::unordered_set<Key, KeyHash> remembered_set;
};

Shard g_shards[kShards];
std::atomic<size_t> g_next_shard{0};

std::atomic<int64_t> g_entries{0}, g_hits{0}, g_misses{0}, g_inserts{0}, g_refused{0},
    g_evictions{0}, g_give_way{0}, g_probation_bytes{0}, g_promotions{0},
    g_remembered_hits{0};

// TEMPORARY leave-one-out switch (C7 §27, ruling: every policy part is kept
// only if turning it off measures slower). DRAKEN_CC_ABLATE is a comma list of
// parts to turn OFF, read once per process:
//   probation  new chunks go straight to MAIN (no S3-FIFO admission)
//   remember   probation evictions are not remembered
//   cost       every chunk gets weight 0 (plain CLOCK)
// An unknown name aborts. Removed once the leave-one-out runs are banked.
struct Ablate {
    bool probation = false, remember = false, cost = false;
};

const Ablate& ablate() {
    static const Ablate a = []() {
        Ablate r;
        const char* v = std::getenv("DRAKEN_CC_ABLATE");
        std::string s = v ? v : "";
        size_t start = 0;
        while (start < s.size()) {
            size_t end = s.find(',', start);
            if (end == std::string::npos) end = s.size();
            const std::string part = s.substr(start, end - start);
            if (part == "probation") r.probation = true;
            else if (part == "remember") r.remember = true;
            else if (part == "cost") r.cost = true;
            else if (!part.empty()) {
                std::fprintf(stderr, "DRAKEN_CC_ABLATE: unknown part '%s' (probation, remember, cost)\n",
                             part.c_str());
                std::abort();
            }
            start = end + 1;
        }
        return r;
    }();
    return a;
}

// Why an eviction is happening. Only an ADMIT eviction from probation is a
// verdict on the chunk ("filled, not used again"), so only it is remembered.
enum class Why { kAdmit, kGiveWay, kFlush };

// Extra sweeps for a chunk costing `cost_ns` to rebuild: floor(log2(ns per
// byte)), clamped to [0, kMaxWeight]. Under 2 ns/byte (a local decompress) is
// plain CLOCK; a remote fetch's fixed latency makes small chunks heavy.
uint8_t weight_for(int64_t cost_ns, int64_t bytes) {
    if (ablate().cost) return 0;
    const double per_byte = static_cast<double>(cost_ns) / static_cast<double>(bytes);
    if (!(per_byte >= 2.0)) return 0;
    const double w = std::floor(std::log2(per_byte));
    return w >= kMaxWeight ? kMaxWeight : static_cast<uint8_t>(w);
}

void destroy(DrakenChunkEntry* e) {
    std::free(e->buffer);
    delete e;
}

void remember(Shard& sh, const Key& k) {
    if (ablate().remember) return;
    if (!sh.remembered_set.insert(k).second) return;
    sh.remembered.push_back(k);
    if (sh.remembered.size() > kRememberedPerShard) {
        sh.remembered_set.erase(sh.remembered.front());
        sh.remembered.pop_front();
    }
}

// Under the shard lock, after `e` has left its queue: drop it from the cache.
void drop(Shard& sh, DrakenChunkEntry* e) {
    sh.map.erase(Key{e->path, e->offset});
    if (!e->main) g_probation_bytes.fetch_sub(e->bytes, std::memory_order_relaxed);
    draken_mem_cache_sub(e->bytes);
    g_entries.fetch_sub(1, std::memory_order_relaxed);
    const uint32_t prev = e->state.fetch_or(kEvicted, std::memory_order_acq_rel);
    if ((prev & kPinMask) == 0) destroy(e);
}

// Free one entry from one shard. Returns 1 evicted, 0 nothing here. `patient`:
// sweep MAIN one lap only, so a shard whose residents all still have lives
// yields to the other shards before it spends them.
int evict_one(Shard& sh, Why why, bool patient, int64_t* freed) {
    std::lock_guard<std::mutex> lk(sh.mu);
    const int64_t probation_cap = draken_mem_cache_limit() * kProbationPercent / 100;
    // Probation first while it is over its share (or MAIN is empty).
    while (!sh.probation.empty() &&
           (why == Why::kFlush || sh.ring.empty() ||
            g_probation_bytes.load(std::memory_order_relaxed) > probation_cap)) {
        DrakenChunkEntry* e = sh.probation.front();
        sh.probation.pop_front();
        if (why != Why::kFlush && e->ref.exchange(0, std::memory_order_relaxed) != 0) {
            e->main = true;   // used while on probation: promote
            e->lives = e->weight;
            g_probation_bytes.fetch_sub(e->bytes, std::memory_order_relaxed);
            sh.ring.push_back(e);
            g_promotions.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        if (why == Why::kAdmit) remember(sh, Key{e->path, e->offset});
        *freed = e->bytes;
        drop(sh, e);
        return 1;
    }
    // Probation is over its share but this shard's part is gone: the next
    // victim is on another shard's probation, not in this shard's MAIN.
    if (why != Why::kFlush && !sh.ring.empty() &&
        g_probation_bytes.load(std::memory_order_relaxed) > probation_cap)
        return 0;
    // MAIN: CLOCK. A set ref bit becomes `weight` extra sweeps; each sweep
    // spends one. Bounded: every entry is evictable within kMaxWeight + 2 laps.
    const size_t n = sh.ring.size();
    const size_t laps = patient ? 1 : kMaxWeight + 2;
    for (size_t step = 0; step < laps * n && !sh.ring.empty(); ++step) {
        if (sh.hand >= sh.ring.size()) sh.hand = 0;
        DrakenChunkEntry* e = sh.ring[sh.hand];
        if (why != Why::kFlush) {
            if (e->ref.exchange(0, std::memory_order_relaxed) != 0) {
                e->lives = e->weight;
                ++sh.hand;
                continue;
            }
            if (e->lives > 0) {
                --e->lives;
                ++sh.hand;
                continue;
            }
        }
        sh.ring[sh.hand] = sh.ring.back();
        sh.ring.pop_back();
        if (sh.hand >= sh.ring.size()) sh.hand = 0;
        *freed = e->bytes;
        drop(sh, e);
        return 1;
    }
    return 0;
}

// Evict until the cache holds at most `target` bytes. False when the shards
// hold nothing more to evict (everything left is pinned or gone). Each victim
// is looked for patiently (one lap per shard) across every shard before any
// shard sweeps until something goes: a cold cheap chunk on one shard goes
// before an expensive one on another.
bool make_room(int64_t target, Why why) {
    size_t idle = 0;
    bool patient = true;
    while (draken_mem_cache_bytes() > target) {
        Shard& sh = g_shards[g_next_shard.fetch_add(1, std::memory_order_relaxed) % kShards];
        int64_t freed = 0;
        if (evict_one(sh, why, patient, &freed) == 0) {
            if (++idle < kShards) continue;
            if (!patient) return draken_mem_cache_bytes() <= target;
            patient = false;
            idle = 0;
            continue;
        }
        idle = 0;
        patient = true;
        if (why == Why::kGiveWay) g_give_way.fetch_add(freed, std::memory_order_relaxed);
        else if (why == Why::kAdmit) g_evictions.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

void shrink_to(int64_t target) { make_room(target, Why::kGiveWay); }

Shard& shard_for(const Key& k) { return g_shards[KeyHash{}(k) % kShards]; }

struct RegisterShrinker {
    RegisterShrinker() { draken_mem_set_shrinker(&shrink_to); }
} g_register_shrinker;

}  // namespace

extern "C" {

int draken_cc_enabled(void) { return draken_mem_cache_budget() > 0 ? 1 : 0; }

const DrakenChunkEntry* draken_cc_lookup(const char* path, size_t path_len, int64_t chunk_offset,
                                         int predicated, int* fill_ok) {
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
    const int64_t budget = draken_mem_cache_budget();
    if (budget <= 0 || buffer_bytes <= 0 || buffer_bytes > budget) {
        std::free(buffer);
        g_refused.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }
    const int64_t limit = draken_mem_cache_limit();
    // Under query-memory pressure (the limit is below the budget) an insert may
    // only use free room: evicting residents to admit would refill the cache the
    // running queries are making it give away — churn, not caching.
    const bool pressure = limit < budget;
    const bool fits_free = draken_mem_cache_bytes() + buffer_bytes <= limit;
    if (buffer_bytes > limit || (pressure && !fits_free) ||
        (!fits_free && !make_room(limit - buffer_bytes, Why::kAdmit))) {
        std::free(buffer);
        g_refused.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    auto* e = new DrakenChunkEntry();
    e->path.assign(path, path_len);
    e->offset = chunk_offset;
    e->buffer = buffer;
    e->bytes = buffer_bytes;
    e->weight = weight_for(cost_ns, buffer_bytes);
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
        if (ablate().probation) {
            e->main = true;
            e->lives = e->weight;
            sh.ring.push_back(e);
        } else if (sh.remembered_set.erase(k) != 0) {
            // Evicted from probation recently and wanted again: straight to MAIN.
            sh.remembered.erase(std::find(sh.remembered.begin(), sh.remembered.end(), k));
            e->main = true;
            e->lives = e->weight;
            sh.ring.push_back(e);
            g_remembered_hits.fetch_add(1, std::memory_order_relaxed);
        } else {
            sh.probation.push_back(e);
            g_probation_bytes.fetch_add(buffer_bytes, std::memory_order_relaxed);
        }
        sh.map.emplace(std::move(k), e);
        draken_mem_cache_add(buffer_bytes);
    }
    g_entries.fetch_add(1, std::memory_order_relaxed);
    g_inserts.fetch_add(1, std::memory_order_relaxed);
    return 1;
}

void draken_cc_flush(void) {
    make_room(0, Why::kFlush);
    for (Shard& sh : g_shards) {
        std::lock_guard<std::mutex> lk(sh.mu);
        sh.remembered.clear();
        sh.remembered_set.clear();
    }
}

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
    s.probation_bytes = g_probation_bytes.load(std::memory_order_relaxed);
    s.promotions = g_promotions.load(std::memory_order_relaxed);
    s.remembered_hits = g_remembered_hits.load(std::memory_order_relaxed);
    return s;
}

}  // extern "C"
