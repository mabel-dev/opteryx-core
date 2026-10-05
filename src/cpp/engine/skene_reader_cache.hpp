#pragma once
// src/cpp/engine/skene_reader_cache.hpp — the process-wide cache of opened .skene
// readers, and the file primitives that open them.
//
// Moved out of native_skene_scan_source.hpp so planning can open a file the same
// way the scan does and leave the reader here for the scan to find
// (skene_footer_stats.hpp). The cache is a function-local static, so it is ONE
// instance per extension module: everything that must share it is compiled into
// the module that configures its budget (opteryx/operators/_operators.pyx).

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "skene/reader.h"

namespace opteryx::engine {

// RAII read-only mapping of a whole file. Fails loud (fd < 0 / addr == nullptr)
// rather than throwing — this runs on worker threads in a no-exception context,
// matching skene's own status-code posture.
class SkeneFileMapping {
  public:
    explicit SkeneFileMapping(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return;
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
            ::close(fd);
            return;
        }
        void* addr = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ,
                            MAP_SHARED, fd, 0);
        // The mapping keeps its own reference to the file; the descriptor is not
        // needed once mmap succeeds, and holding one per in-flight file would
        // burn descriptors on a wide scan.
        ::close(fd);
        if (addr == MAP_FAILED) return;
        data_ = addr;
        size_ = static_cast<size_t>(st.st_size);
    }

    ~SkeneFileMapping() {
        if (data_ != nullptr) ::munmap(data_, size_);
    }

    SkeneFileMapping(const SkeneFileMapping&) = delete;
    SkeneFileMapping& operator=(const SkeneFileMapping&) = delete;

    bool ok() const noexcept { return data_ != nullptr; }
    const void* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }

  private:
    void* data_ = nullptr;
    size_t size_ = 0;
};

// ─── Cross-query reader cache ────────────────────────────────────────────────
//
// Opening a .skene file — reading its suffix and parsing the footer — was paid by
// every scan, serially, inside the claim set's call_once while every other worker
// waited (measured ~3.8 ms of a 10 ms ClickBench query over 4 files). The engine
// keeps opened readers across queries instead. skene is not asked to know: the
// cache, its keying, its budget and its locking are all here.
//
// One entry is one opened file: the skene::FileReader (parsed footer, and every
// column directory any scan has attached), plus the whole-file mapping a v2
// reader borrows. Scans SHARE the entry through shared_ptr — a skene::FileReader
// copy would share its parse state anyway — so an entry evicted mid-query lives
// until the last scan holding it ends.
//
// Attaching directories mutates the shared reader. skene's attach writes only the
// nodes it attaches, and a scan only reads nodes it has attached, so the engine
// serialises attaches per entry (`attach_mtx`) and records which top-level columns
// are attached (`attached`, under the same lock). A scan never reads a node another
// scan is still attaching: it attaches (or finds attached) its own read set under
// the lock before planning a single read.
//
// Keyed by (path, size, mtime): a rewritten file is a different entry, never a
// stale hit. Budgeted in the ENCODED bytes an entry was built from — its footer
// plus every attached directory block — because the reader's parsed state is
// private to skene and reports no size.
struct SkeneCachedReader {
    uint16_t                           version = 0;
    std::unique_ptr<SkeneFileMapping>  mapping;   // v2: the reader borrows it
    skene::FileReader                  reader;
    std::mutex                         attach_mtx;
    std::unordered_set<std::string>    attached;  // top-level columns; under attach_mtx
};

class SkeneReaderCache {
  public:
    static SkeneReaderCache& instance() {
        static SkeneReaderCache cache;
        return cache;
    }

    // Set once from config at engine import (SKENE_FOOTER_CACHE_BYTES).
    void set_budget(int64_t budget_bytes) {
        std::lock_guard<std::mutex> lk(mtx_);
        budget_ = budget_bytes;
        evict_to(budget_);
    }

    static std::string key_for(const std::string& path, const struct stat& st) {
#if defined(__APPLE__)
        const int64_t mtime_ns = static_cast<int64_t>(st.st_mtimespec.tv_sec) * 1000000000LL +
                                 st.st_mtimespec.tv_nsec;
#else
        const int64_t mtime_ns = static_cast<int64_t>(st.st_mtim.tv_sec) * 1000000000LL +
                                 st.st_mtim.tv_nsec;
#endif
        return path + '\x1f' + std::to_string(static_cast<int64_t>(st.st_size)) + '\x1f' +
               std::to_string(mtime_ns);
    }

    // False when no budget was ever configured — the caller fails the scan.
    bool configured() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return budget_ > 0;
    }

    std::shared_ptr<SkeneCachedReader> get(const std::string& key) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it == map_.end()) return nullptr;
        lru_.splice(lru_.begin(), lru_, it->second.lru);
        return it->second.entry;
    }

    // Caches a freshly opened entry charged `bytes`. An entry larger than the whole
    // budget is not kept (the scan still uses it) and is counted in over_budget.
    void put(const std::string& key, const std::shared_ptr<SkeneCachedReader>& entry,
             int64_t bytes) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it != map_.end()) {           // a concurrent scan opened it too: replace
            resident_ -= it->second.bytes;
            lru_.erase(it->second.lru);
            map_.erase(it);
        }
        if (bytes > budget_) {
            ++over_budget_;
            return;
        }
        evict_to(budget_ - bytes);
        lru_.push_front(key);
        map_.emplace(key, Slot{entry, bytes, lru_.begin()});
        resident_ += bytes;
    }

    // Adds `bytes` (directories just attached) to `entry`'s charge while it is still
    // the cached entry for `key`, evicting others to stay within budget. An entry
    // the cache no longer holds is only its scans' memory, and is not charged.
    void charge(const std::string& key, const SkeneCachedReader* entry, int64_t bytes) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = map_.find(key);
        if (it == map_.end() || it->second.entry.get() != entry) return;
        it->second.bytes += bytes;
        resident_ += bytes;
        if (it->second.bytes > budget_) {
            resident_ -= it->second.bytes;
            lru_.erase(it->second.lru);
            map_.erase(it);
            ++over_budget_;
            return;
        }
        lru_.splice(lru_.begin(), lru_, it->second.lru);
        evict_to(budget_);
    }

    int64_t resident_bytes() const { std::lock_guard<std::mutex> lk(mtx_); return resident_; }
    int64_t budget_bytes() const { std::lock_guard<std::mutex> lk(mtx_); return budget_; }
    int64_t entries() const { std::lock_guard<std::mutex> lk(mtx_); return static_cast<int64_t>(map_.size()); }
    int64_t over_budget() const { std::lock_guard<std::mutex> lk(mtx_); return over_budget_; }

  private:
    struct Slot {
        std::shared_ptr<SkeneCachedReader> entry;
        int64_t                            bytes = 0;
        std::list<std::string>::iterator   lru;
    };

    // Evicts least-recently-used entries until resident <= target. Called under mtx_.
    // The most-recently-used entry is evicted last, so the entry being charged is
    // only ever evicted by the over-budget check in charge().
    void evict_to(int64_t target) {
        while (resident_ > target && !lru_.empty()) {
            auto it = map_.find(lru_.back());
            resident_ -= it->second.bytes;
            map_.erase(it);
            lru_.pop_back();
        }
    }

    mutable std::mutex                    mtx_;
    int64_t                               budget_ = 0;
    int64_t                               resident_ = 0;
    int64_t                               over_budget_ = 0;
    std::list<std::string>                lru_;   // front = most recently used
    std::unordered_map<std::string, Slot> map_;
};

// Bytes read from a file: allocated WITHOUT initialisation, because pread
// overwrites every one of them. (A std::vector<uint8_t> resize zero-fills first
// — measured in `sample` as bzero under skene_pread on every fetched byte.) The
// storage is heap-owned, so `data` is stable when the owner moves.
struct SkeneReadBuffer {
    std::unique_ptr<uint8_t[]> bytes;
    size_t                     length = 0;
    const uint8_t* data() const { return bytes.get(); }
    size_t size() const { return length; }
};

// Reads exactly [offset, offset + bytes) of `fd` into `out`. Fails loud on a
// short read: a range the plan named is a range the file must hold.
inline bool skene_pread(int fd, uint64_t offset, uint64_t bytes, SkeneReadBuffer* out,
                        const std::string& path, std::string& err_buf) {
    out->bytes.reset(new uint8_t[static_cast<size_t>(bytes)]);  // default-init: no fill
    out->length = static_cast<size_t>(bytes);
    uint8_t* dst = out->bytes.get();
    uint64_t done = 0;
    while (done < bytes) {
        const ssize_t n = ::pread(fd, dst + done, static_cast<size_t>(bytes - done),
                                  static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            err_buf = "skene scan: '" + path + "': read failed at " +
                      std::to_string(offset + done) + ": " + std::strerror(errno);
            return false;
        }
        if (n == 0) {
            err_buf = "skene scan: '" + path + "': file ended at " +
                      std::to_string(offset + done) + " inside a planned range ending at " +
                      std::to_string(offset + bytes);
            return false;
        }
        done += static_cast<uint64_t>(n);
    }
    return true;
}

}  // namespace opteryx::engine
