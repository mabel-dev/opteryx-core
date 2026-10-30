#ifndef OPTERYX_LRU2_HPP
#define OPTERYX_LRU2_HPP

#include <cstdint>
#include <string>
#include <unordered_map>
#include <queue>
#include <vector>
#include <utility>
#include <functional>
#include <stdexcept>

// LRU-K cache, K = 1 or 2 (default 2).
//
// K=1 is plain LRU: the entry whose most recent access is oldest is evicted. Every
// access goes to the mature heap at its last-access time; the young heap stays empty.
// The parquet footer caches use it: under K=2 a long-lived process fills them with
// footers read twice or more, and a new table's footers only evict each other.
//
// Eviction policy for K=2 (standard LRU-K):
//   - Entries with fewer than 2 accesses ("young") are always evicted before
//     entries with 2+ accesses ("mature").  Among young entries, the one with
//     the oldest first-access time is chosen (FIFO).
//   - Among mature entries, the one whose 2nd-most-recent access time is
//     oldest is chosen.
//
// Both heaps use lazy deletion: when an entry's generation changes (new
// access recorded), the old heap entry is left in place and skipped on pop.
// Generations come from one cache-wide counter, so an erased-then-reinserted key
// can never match a stale item; and the heaps are compacted once stale items
// outnumber live ones, so a cache that never evicts does not grow per hit.

namespace opteryx {

class LRU2 {
public:
    // Throws std::invalid_argument unless k is 1 or 2.
    LRU2(int64_t max_size = 0, int64_t max_memory = 0, int64_t k = 2);

    // Returns evicted (key, value); empty strings if no eviction occurred.
    std::pair<std::string, std::string> set(
        const char* key, int64_t key_len,
        const char* value, int64_t value_len,
        bool evict);

    // Returns pointer into internal storage, valid until next mutation.
    // Returns nullptr on miss.
    const std::string* get(const char* key, int64_t key_len);

    // Evict one entry.  If need_value is false, value in result is empty.
    std::pair<std::string, std::string> evict_one(bool need_value);

    bool erase(const char* key, int64_t key_len);
    void clear(bool reset_stats);

    bool contains(const char* key, int64_t key_len) const;

    // ── Cython-friendly output-parameter variants ─────────────────────────────
    // Returns true on hit; fills *out_data / *out_len with pointer and length
    // into internal storage (valid until next mutation).
    bool get_into(const char* key, int64_t key_len,
                  const char** out_data, int64_t* out_len);

    // Returns true if an entry was evicted; fills key_out / val_out.
    // If need_value is false, val_out is left empty.
    bool evict_one_into(bool need_value,
                        std::string& key_out, std::string& val_out);

    int64_t size()           const noexcept { return size_; }
    int64_t current_memory() const noexcept { return current_memory_; }
    int64_t hits()           const noexcept { return hits_; }
    int64_t misses()         const noexcept { return misses_; }
    int64_t evictions()      const noexcept { return evictions_; }
    int64_t inserts()        const noexcept { return inserts_; }
    // Heap items held, live + stale: bounded by ~2x size() + 64 (see compact_heaps).
    int64_t heap_items()     const noexcept { return (int64_t)(young_heap_.size() + mature_heap_.size()); }

private:
    struct Entry {
        std::string value;
        uint64_t    last_access;  // most recent clock tick
        uint64_t    prev_access;  // 2nd-most-recent; 0 means fewer than 2 accesses
        uint64_t    generation;   // fresh on each access; invalidates stale heap entries
    };

    struct HeapItem {
        uint64_t    priority;     // lower = evict first
        uint64_t    generation;   // generation at push time
        std::string key;
        bool operator>(const HeapItem& o) const noexcept { return priority > o.priority; }
    };

    using MinHeap = std::priority_queue<HeapItem,
                                        std::vector<HeapItem>,
                                        std::greater<HeapItem>>;

    std::unordered_map<std::string, Entry> cache_;
    MinHeap young_heap_;   // entries with prev_access == 0; priority = last_access
    MinHeap mature_heap_;  // entries with prev_access >  0; priority = prev_access

    int64_t max_size_;
    int64_t max_memory_;
    int64_t k_;
    int64_t size_;
    int64_t current_memory_;
    uint64_t clock_;
    uint64_t generation_;  // cache-wide; unique per access

    int64_t hits_;
    int64_t misses_;
    int64_t evictions_;
    int64_t inserts_;

    void record_access(Entry& entry, const std::string& key);
    void compact_heaps();
    bool should_evict() const noexcept;
    std::pair<std::string, std::string> evict_from_young(bool need_value);
    std::pair<std::string, std::string> evict_from_mature(bool need_value);
};

// ── Implementation ────────────────────────────────────────────────────────────

inline LRU2::LRU2(int64_t max_size, int64_t max_memory, int64_t k)
    : max_size_(max_size), max_memory_(max_memory), k_(k),
      size_(0), current_memory_(0), clock_(0), generation_(0),
      hits_(0), misses_(0), evictions_(0), inserts_(0) {
    if (k != 1 && k != 2)
        throw std::invalid_argument("LRU-K supports K = 1 or 2, got " + std::to_string(k));
}

inline void LRU2::record_access(Entry& entry, const std::string& key) {
    ++clock_;
    entry.generation = ++generation_;   // invalidates any older heap item for this key

    if (k_ == 1) {
        // Plain LRU: one heap, ordered by most recent access.
        entry.prev_access = entry.last_access;
        entry.last_access = clock_;
        mature_heap_.push({clock_, entry.generation, key});
    } else if (entry.last_access == 0) {
        // First access ever
        entry.last_access = clock_;
        young_heap_.push({clock_, entry.generation, key});
    } else {
        // Second or later access: (re)enters the mature heap at its 2nd-most-recent time
        entry.prev_access = entry.last_access;
        entry.last_access = clock_;
        mature_heap_.push({entry.prev_access, entry.generation, key});
    }
    // Each live entry has exactly one valid item; past 2x that, drop the stale ones.
    if (young_heap_.size() + mature_heap_.size() > 2 * cache_.size() + 64)
        compact_heaps();
}

inline void LRU2::compact_heaps() {
    auto keep_live = [this](MinHeap& heap) {
        std::vector<HeapItem> live;
        live.reserve(heap.size());
        while (!heap.empty()) {
            HeapItem item = heap.top();
            heap.pop();
            auto it = cache_.find(item.key);
            if (it != cache_.end() && it->second.generation == item.generation)
                live.push_back(std::move(item));
        }
        heap = MinHeap(std::greater<HeapItem>(), std::move(live));
    };
    keep_live(young_heap_);
    keep_live(mature_heap_);
}

inline bool LRU2::should_evict() const noexcept {
    if (max_size_   > 0 && size_           > max_size_)   return true;
    if (max_memory_ > 0 && current_memory_ > max_memory_) return true;
    return false;
}

inline std::pair<std::string, std::string>
LRU2::set(const char* key, int64_t key_len,
          const char* val, int64_t val_len,
          bool evict)
{
    ++inserts_;
    std::string k(key, key_len);
    int64_t item_mem = key_len + val_len;

    auto it = cache_.find(k);
    if (it != cache_.end()) {
        Entry& entry = it->second;
        current_memory_ -= (int64_t)(k.size() + entry.value.size());
        entry.value.assign(val, val_len);
        current_memory_ += item_mem;
        record_access(entry, k);
    } else {
        ++size_;
        current_memory_ += item_mem;
        Entry& entry = cache_[k];
        entry.value.assign(val, val_len);
        entry.last_access = 0;
        entry.prev_access = 0;
        record_access(entry, k);
    }

    if (evict) {
        while (should_evict()) {
            auto res = evict_one(true);
            if (res.first.empty()) break;
        }
    }
    return {"", ""};
}

inline const std::string* LRU2::get(const char* key, int64_t key_len) {
    auto it = cache_.find(std::string(key, key_len));
    if (it == cache_.end()) {
        ++misses_;
        return nullptr;
    }
    ++hits_;
    record_access(it->second, it->first);
    return &it->second.value;
}

inline std::pair<std::string, std::string> LRU2::evict_from_young(bool need_value) {
    while (!young_heap_.empty()) {
        HeapItem item = young_heap_.top();
        young_heap_.pop();

        auto it = cache_.find(item.key);
        if (it == cache_.end()) continue;          // already erased
        if (it->second.generation != item.generation) continue; // stale

        std::string evicted_key   = std::move(item.key);
        std::string evicted_value = need_value ? it->second.value : std::string{};
        current_memory_ -= (int64_t)(evicted_key.size() + it->second.value.size());
        cache_.erase(it);
        --size_;
        ++evictions_;
        return {std::move(evicted_key), std::move(evicted_value)};
    }
    return {"", ""};
}

inline std::pair<std::string, std::string> LRU2::evict_from_mature(bool need_value) {
    while (!mature_heap_.empty()) {
        HeapItem item = mature_heap_.top();
        mature_heap_.pop();

        auto it = cache_.find(item.key);
        if (it == cache_.end()) continue;
        if (it->second.generation != item.generation) continue;

        std::string evicted_key   = std::move(item.key);
        std::string evicted_value = need_value ? it->second.value : std::string{};
        current_memory_ -= (int64_t)(evicted_key.size() + it->second.value.size());
        cache_.erase(it);
        --size_;
        ++evictions_;
        return {std::move(evicted_key), std::move(evicted_value)};
    }
    return {"", ""};
}

inline std::pair<std::string, std::string> LRU2::evict_one(bool need_value) {
    // Always prefer evicting young (< 2 accesses) entries first.
    auto res = evict_from_young(need_value);
    if (!res.first.empty()) return res;
    return evict_from_mature(need_value);
}

inline bool LRU2::erase(const char* key, int64_t key_len) {
    auto it = cache_.find(std::string(key, key_len));
    if (it == cache_.end()) return false;
    current_memory_ -= (int64_t)(it->first.size() + it->second.value.size());
    cache_.erase(it);
    --size_;
    ++evictions_;
    return true;
}

inline void LRU2::clear(bool reset_stats) {
    cache_.clear();
    young_heap_  = MinHeap{};
    mature_heap_ = MinHeap{};
    size_           = 0;
    current_memory_ = 0;
    if (reset_stats) {
        hits_ = misses_ = evictions_ = inserts_ = 0;
    }
}

inline bool LRU2::contains(const char* key, int64_t key_len) const {
    return cache_.count(std::string(key, key_len)) > 0;
}

inline bool LRU2::get_into(const char* key, int64_t key_len,
                            const char** out_data, int64_t* out_len) {
    auto it = cache_.find(std::string(key, key_len));
    if (it == cache_.end()) {
        ++misses_;
        *out_data = nullptr;
        *out_len  = 0;
        return false;
    }
    ++hits_;
    record_access(it->second, it->first);
    *out_data = it->second.value.data();
    *out_len  = (int64_t)it->second.value.size();
    return true;
}

inline bool LRU2::evict_one_into(bool need_value,
                                  std::string& key_out, std::string& val_out) {
    auto res = evict_one(need_value);
    if (res.first.empty()) return false;
    key_out = std::move(res.first);
    val_out = std::move(res.second);
    return true;
}

} // namespace opteryx

#endif // OPTERYX_LRU2_HPP
