#include "memory_pool.hpp"

namespace opteryx {

MemoryPool::MemoryPool(int64_t size, std::string name, bool auto_resize, int64_t alignment)
    : size_(size), used_size_(0), alignment_(alignment), auto_resize_(auto_resize),
      name_(name), next_ref_id_(1), commits_(0), failed_commits_(0), reads_(0),
      read_locks_(0), compactions_(0), releases_(0), resizes_(0) {

    if (size <= 0) {
        throw std::invalid_argument("MemoryPool size must be a positive integer");
    }

    if (alignment != 1 && (alignment & (alignment - 1)) != 0) {
        throw std::invalid_argument("Alignment must be a power of two");
    }

    pool_ = static_cast<unsigned char*>(malloc(size));
    if (!pool_) {
        throw std::bad_alloc();
    }

    segments_[0] = {0, size, true};
}

MemoryPool::~MemoryPool() {
    if (pool_) {
        free(pool_);
        pool_ = nullptr;
    }
}

int64_t MemoryPool::align_size(int64_t size) {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

int64_t MemoryPool::find_best_fit_no_lock(int64_t size) {
    int64_t best_key   = -1;
    int64_t best_waste = size_ + 1;

    for (auto& [key, seg] : segments_) {
        if (seg.is_free && seg.length >= size) {
            int64_t waste = seg.length - size;
            if (waste < best_waste) {
                best_waste = waste;
                best_key   = key;
                if (waste == 0) break;
            }
        }
    }
    return best_key;
}

int64_t MemoryPool::find_best_fit_large_no_lock(int64_t size) {
    int64_t best_key   = -1;
    int64_t best_waste = size_ + 1;

    for (auto it = segments_.rbegin(); it != segments_.rend(); ++it) {
        if (it->second.is_free && it->second.length >= size) {
            int64_t waste = it->second.length - size;
            if (waste < best_waste) {
                best_waste = waste;
                best_key   = it->first;
                if (waste == 0) break;
            }
        }
    }
    return best_key;
}

void MemoryPool::compaction_no_lock() {
    if (segments_.size() <= 1) {
        return;
    }

    compactions_++;

    // Build reverse map: start offset → ref_id, so we can update metadata
    // start addresses as segments are physically moved.
    std::unordered_map<int64_t, int64_t> start_to_ref;
    start_to_ref.reserve(metadata_.size());
    for (auto& [ref_id, meta] : metadata_) {
        start_to_ref[meta.start] = ref_id;
    }

    std::map<int64_t, Segment> new_segments;
    int64_t current_pos = 0;

    for (auto& [key, seg] : segments_) {
        if (seg.is_free) continue;

        int64_t original_start = seg.start;

        // Look up latches from metadata (not from Segment, which no longer holds them).
        int64_t latches = 0;
        auto ref_it = start_to_ref.find(original_start);
        if (ref_it != start_to_ref.end()) {
            auto meta_it = metadata_.find(ref_it->second);
            if (meta_it != metadata_.end()) {
                latches = meta_it->second.latches;
            }
        }

        if (latches > 0) {
            // Pinned — cannot move. Leave a gap before it if current_pos drifted.
            if (current_pos != seg.start) {
                current_pos = seg.start + seg.length;
            } else {
                current_pos += seg.length;
            }
            new_segments[seg.start] = seg;

            if (ref_it != start_to_ref.end()) {
                auto meta_it = metadata_.find(ref_it->second);
                if (meta_it != metadata_.end()) {
                    meta_it->second.start = seg.start;
                }
            }
        } else {
            if (seg.start != current_pos) {
                std::memmove(pool_ + current_pos, pool_ + seg.start, seg.length);
            }
            Segment moved = {current_pos, seg.length, false};
            new_segments[current_pos] = moved;

            if (ref_it != start_to_ref.end()) {
                auto meta_it = metadata_.find(ref_it->second);
                if (meta_it != metadata_.end()) {
                    meta_it->second.start = current_pos;
                }
            }

            current_pos += seg.length;
        }
    }

    if (current_pos < size_) {
        new_segments[current_pos] = {current_pos, size_ - current_pos, true};
    }

    segments_ = std::move(new_segments);
}

bool MemoryPool::resize_pool_no_lock(int64_t new_size) {
    unsigned char* new_pool = static_cast<unsigned char*>(realloc(pool_, new_size));
    if (!new_pool) {
        return false;
    }

    pool_  = new_pool;
    size_  = new_size;
    resizes_++;
    return true;
}

int64_t MemoryPool::get_fragmentation_no_lock() {
    int64_t total_free  = 0;
    int64_t largest_free = 0;

    for (auto& [key, seg] : segments_) {
        if (seg.is_free) {
            total_free += seg.length;
            if (seg.length > largest_free) largest_free = seg.length;
        }
    }

    if (total_free == 0)    return 0;
    if (largest_free == 0)  return 100;
    return 100 - (largest_free * 100 / total_free);
}

PoolStats MemoryPool::get_stats_no_lock() {
    int64_t total_used   = 0;
    int64_t total_free   = 0;
    int64_t used_blocks  = 0;
    int64_t free_blocks  = 0;
    int64_t largest_free = 0;

    for (auto& [key, seg] : segments_) {
        if (seg.is_free) {
            total_free += seg.length;
            free_blocks++;
            if (seg.length > largest_free) largest_free = seg.length;
        } else {
            total_used += seg.length;
            used_blocks++;
        }
    }

    int64_t fragmentation = 0;
    if (total_free > 0 && largest_free > 0) {
        fragmentation = 100 - (largest_free * 100 / total_free);
    } else if (total_free > 0) {
        fragmentation = 100;
    }

    return {
        size_, total_used, total_free, used_blocks, free_blocks,
        largest_free, fragmentation,
        commits_, failed_commits_, reads_, releases_, compactions_, resizes_
    };
}

int64_t MemoryPool::commit(const void* data, int64_t length) {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t ref_id = next_ref_id_++;

    if (length == 0) {
        metadata_[ref_id] = {-1, 0, 0, 0};
        commits_++;
        return ref_id;
    }

    if (!data) {
        failed_commits_++;
        return -1;
    }

    int64_t aligned_size = align_size(length);
    bool    large        = aligned_size >= kLargeAllocThreshold;

    auto pick = [&]() -> int64_t {
        return large ? find_best_fit_large_no_lock(aligned_size)
                     : find_best_fit_no_lock(aligned_size);
    };

    int64_t seg_key = pick();
    if (seg_key == -1) {
        compaction_no_lock();
        seg_key = pick();

        if (seg_key == -1 && auto_resize_) {
            int64_t old_size = size_;
            int64_t new_size = std::max(size_ * 2, size_ + aligned_size * 2);

            if (resize_pool_no_lock(new_size)) {
                int64_t added = new_size - old_size;
                auto back = segments_.rbegin();
                if (back != segments_.rend() && back->second.is_free) {
                    back->second.length += added;
                } else {
                    segments_[old_size] = {old_size, added, true};
                }
                seg_key = pick();
            }
        }
    }

    if (seg_key == -1) {
        failed_commits_++;
        return -1;
    }

    int64_t seg_len   = segments_[seg_key].length;
    int64_t remainder = seg_len - aligned_size;
    int64_t block_len;

    if (remainder >= kMinSplitRemainder) {
        segments_[seg_key]              = {seg_key,              aligned_size, false};
        segments_[seg_key + aligned_size] = {seg_key + aligned_size, remainder,    true};
        block_len = aligned_size;
    } else {
        segments_[seg_key] = {seg_key, seg_len, false};
        block_len = seg_len;
    }

    std::memcpy(pool_ + seg_key, data, length);
    metadata_[ref_id] = {seg_key, block_len, 0, length};
    used_size_ += block_len;
    commits_++;

    return ref_id;
}

ReadResult MemoryPool::read(int64_t ref_id, bool latch) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) throw std::invalid_argument("Invalid reference ID.");

    Metadata& meta = it->second;
    if (meta.start == -1) return {nullptr, 0};

    reads_++;
    if (latch) {
        meta.latches++;
        read_locks_++;
    }

    return {pool_ + meta.start, meta.orig_length};
}

void MemoryPool::latch(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) throw std::invalid_argument("Invalid reference ID.");
    if (it->second.start == -1) return;

    it->second.latches++;
    read_locks_++;
}

void MemoryPool::unlatch(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) throw std::invalid_argument("Invalid reference ID.");
    if (it->second.start == -1) return;

    if (it->second.latches == 0) throw std::runtime_error("Segment was not latched.");
    it->second.latches--;
}

void MemoryPool::release(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto meta_it = metadata_.find(ref_id);
    if (meta_it == metadata_.end()) throw std::invalid_argument("Invalid reference ID.");

    releases_++;
    Metadata meta = meta_it->second;
    metadata_.erase(meta_it);

    if (meta.start == -1) return;

    auto it = segments_.find(meta.start);
    if (it == segments_.end()) throw std::invalid_argument("Invalid reference ID.");

    it->second.is_free = true;
    used_size_ -= it->second.length;

    // Coalesce right neighbor
    auto right = std::next(it);
    if (right != segments_.end() && right->second.is_free) {
        it->second.length += right->second.length;
        segments_.erase(right);
    }

    // Coalesce left neighbor
    if (it != segments_.begin()) {
        auto left = std::prev(it);
        if (left->second.is_free) {
            left->second.length += it->second.length;
            segments_.erase(it);
        }
    }
}

ReserveResult MemoryPool::reserve_for_write(int64_t size) {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t aligned_size = align_size(size);
    bool    large        = aligned_size >= kLargeAllocThreshold;

    auto pick = [&]() -> int64_t {
        return large ? find_best_fit_large_no_lock(aligned_size)
                     : find_best_fit_no_lock(aligned_size);
    };

    int64_t seg_key = pick();
    if (seg_key == -1) {
        compaction_no_lock();
        seg_key = pick();

        if (seg_key == -1 && auto_resize_) {
            int64_t old_size = size_;
            int64_t new_size = std::max(size_ * 2, size_ + aligned_size * 2);

            if (resize_pool_no_lock(new_size)) {
                int64_t added = new_size - old_size;
                auto back = segments_.rbegin();
                if (back != segments_.rend() && back->second.is_free) {
                    back->second.length += added;
                } else {
                    segments_[old_size] = {old_size, added, true};
                }
                seg_key = pick();
            }
        }
    }

    if (seg_key == -1) {
        failed_commits_++;
        return {-1, nullptr, 0};
    }

    int64_t seg_len   = segments_[seg_key].length;
    int64_t remainder = seg_len - aligned_size;
    int64_t block_len;

    if (remainder >= kMinSplitRemainder) {
        segments_[seg_key]               = {seg_key,               aligned_size, false};
        segments_[seg_key + aligned_size] = {seg_key + aligned_size, remainder,   true};
        block_len = aligned_size;
    } else {
        segments_[seg_key] = {seg_key, seg_len, false};
        block_len = seg_len;
    }

    int64_t ref_id = next_ref_id_++;
    metadata_[ref_id] = {seg_key, block_len, 1, 0};
    used_size_ += block_len;
    commits_++;
    read_locks_++;

    return {ref_id, pool_ + seg_key, block_len};
}

void MemoryPool::finalize_commit(int64_t ref_id, int64_t actual_length) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) throw std::invalid_argument("Invalid reference ID.");

    it->second.orig_length = actual_length;
    if (it->second.latches > 0) {
        it->second.latches--;
    }
}

void MemoryPool::clear() {
    std::lock_guard<std::mutex> lock(mu_);

    segments_.clear();
    metadata_.clear();
    used_size_    = 0;
    next_ref_id_  = 1;
    commits_      = 0;
    failed_commits_ = 0;
    reads_        = 0;
    read_locks_   = 0;
    compactions_  = 0;
    releases_     = 0;
    resizes_      = 0;

    segments_[0] = {0, size_, true};
}

int64_t MemoryPool::available_space() {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t total = 0;
    for (auto& [key, seg] : segments_) {
        if (seg.is_free) total += seg.length;
    }
    return total;
}

int64_t MemoryPool::get_fragmentation() {
    std::lock_guard<std::mutex> lock(mu_);
    return get_fragmentation_no_lock();
}

PoolStats MemoryPool::get_stats() {
    std::lock_guard<std::mutex> lock(mu_);
    return get_stats_no_lock();
}

void MemoryPool::compaction() {
    std::lock_guard<std::mutex> lock(mu_);
    compaction_no_lock();
}

std::vector<MetadataSnapshot> MemoryPool::snapshot_metadata() {
    std::lock_guard<std::mutex> lock(mu_);

    std::vector<MetadataSnapshot> result;
    result.reserve(metadata_.size());
    for (auto& [ref_id, meta] : metadata_) {
        result.push_back({ref_id, meta.start, meta.length, meta.latches, meta.orig_length});
    }
    return result;
}

std::vector<FreeSegmentSnapshot> MemoryPool::snapshot_free_segments() {
    std::lock_guard<std::mutex> lock(mu_);

    std::vector<FreeSegmentSnapshot> result;
    for (auto& [key, seg] : segments_) {
        if (seg.is_free) result.push_back({seg.start, seg.length});
    }
    return result;
}

} // namespace opteryx
