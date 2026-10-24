#pragma once
// Process-wide cache of REMOTE parquet bloom-filter bytes.
//
// The native scan's bloom round (src/cpp/engine/parquet_bloom_probe.hpp) reads
// each probed bloom filter (header + bitset) with a range GET. Files are
// immutable for the life of the process (the C7 ruling), so a bloom read once
// is the same bytes forever: the round looks each probe up here first and
// fetches only the misses, and the fetch ticket inserts every bloom it read
// (ParquetIOPipeline::fetch_ranges_async, `cache_slices`). A round whose blooms
// are all cached issues no GET and has landed before the scan walks its first
// unit, so it prunes before anything is submitted and leaves nothing in flight
// for teardown to wait on.
//
// Key: (object, offset, length) of one bloom filter, where the object is the
// path up to any '?' — the chunk cache's rule: a pre-signed fetch URL carries
// its credential and expiry in the query string, which changes per query.
// Bounded by kCapacityBytes,
// least-recently-used out. The bytes are NOT in the draken memory account: they
// are neither query bytes nor chunk-cache bytes (the chunk cache's give-way
// loop could not evict them), so the cap is the bound and `bytes()` is reported
// per scan in io_scan_diagnostics (`bloom_cache_bytes`).
//
// One instance per extension that instantiates it (a function-local static in a
// header). Lookups (start_bloom_round) and inserts (the fetch ticket inside
// fetch_ranges_async, instantiated by its only caller, start_bloom_round) are
// compiled into the same extension, so they always meet the same instance.
#include <algorithm>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace rugo {

class BloomBytesCache {
public:
    using Bytes = std::shared_ptr<const std::vector<uint8_t>>;

    // Remote blooms the ClickBench UserID point lookup reads after min/max
    // pruning are ~30 MB; 128 MiB holds several queries' working sets.
    static constexpr int64_t kCapacityBytes = int64_t{128} << 20;

    // The bloom's bytes, or nullptr. A hit becomes most recently used.
    Bytes lookup(const std::string& path, int64_t offset, int64_t length) {
        const std::string k = key(path, offset, length);
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(k);
        if (it == map_.end()) return nullptr;
        lru_.splice(lru_.begin(), lru_, it->second.pos);
        return it->second.bytes;
    }

    // Copy `length` bytes at `data` in as the bloom at (path, offset). A bloom
    // larger than the whole cache is not kept; an existing key is left as is
    // (same immutable file, same bytes).
    void insert(const std::string& path, int64_t offset, int64_t length, const uint8_t* data) {
        if (length <= 0 || length > kCapacityBytes) return;
        std::string k = key(path, offset, length);
        auto bytes = std::make_shared<const std::vector<uint8_t>>(data, data + length);
        std::lock_guard<std::mutex> lk(mtx_);
        if (map_.find(k) != map_.end()) return;
        while (bytes_ + length > kCapacityBytes) {
            auto victim = map_.find(lru_.back());
            bytes_ -= static_cast<int64_t>(victim->second.bytes->size());
            map_.erase(victim);
            lru_.pop_back();
        }
        lru_.push_front(k);
        map_.emplace(std::move(k), Entry{std::move(bytes), lru_.begin()});
        bytes_ += length;
    }

    int64_t bytes() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return bytes_;
    }

private:
    struct Entry {
        Bytes bytes;
        std::list<std::string>::iterator pos;
    };

    static std::string key(const std::string& path, int64_t offset, int64_t length) {
        const size_t object_len = std::min(path.find('?'), path.size());
        std::string k;
        k.reserve(object_len + 2 * sizeof(int64_t));
        k.append(path, 0, object_len);
        k.append(reinterpret_cast<const char*>(&offset), sizeof(offset));
        k.append(reinterpret_cast<const char*>(&length), sizeof(length));
        return k;
    }

    mutable std::mutex mtx_;
    std::list<std::string> lru_;   // front = most recently used
    std::unordered_map<std::string, Entry> map_;
    int64_t bytes_ = 0;
};

inline BloomBytesCache& bloom_bytes_cache() {
    static BloomBytesCache cache;
    return cache;
}

}  // namespace rugo
