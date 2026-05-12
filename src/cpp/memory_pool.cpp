#include "memory_pool.hpp"

namespace opteryx {

MemoryPool::MemoryPool(int64_t size, std::string name, bool auto_resize, int64_t alignment)
    : size_(size), used_size_(0), alignment_(alignment), auto_resize_(auto_resize),
      name_(name), next_ref_id_(1), commits_(0), failed_commits_(0), reads_(0),
      read_locks_(0), l1_compactions_(0), l2_compactions_(0), releases_(0), resizes_(0) {

    if (size <= 0) {
        throw std::invalid_argument("MemoryPool size must be a positive integer");
    }

    if (alignment != 1 && (alignment & (alignment - 1)) != 0) {
        throw std::invalid_argument("Alignment must be a power of two");
    }

    pool_ = static_cast<unsigned char*>(std::malloc(size));
    if (!pool_) {
        throw std::bad_alloc();
    }

    Segment initial_segment;
    initial_segment.start = 0;
    initial_segment.length = size;
    initial_segment.latches = 0;
    initial_segment.is_free = true;
    segments_.push_back(initial_segment);
}

MemoryPool::~MemoryPool() {
    if (pool_) {
        std::free(pool_);
        pool_ = nullptr;
    }
}

int64_t MemoryPool::align_size(int64_t size) {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

int64_t MemoryPool::find_best_fit_no_lock(int64_t size) {
    int64_t best_index = -1;
    int64_t best_waste = size_ + 1;

    for (size_t i = 0; i < segments_.size(); i++) {
        if (segments_[i].is_free && segments_[i].length >= size) {
            int64_t waste = segments_[i].length - size;
            if (waste < best_waste) {
                best_waste = waste;
                best_index = static_cast<int64_t>(i);
                if (waste == 0) {
                    break;
                }
            }
        }
    }
    return best_index;
}

void MemoryPool::merge_adjacent_free_no_lock() {
    if (segments_.size() <= 1) {
        return;
    }

    l1_compactions_++;

    std::vector<Segment> new_segments;
    for (size_t i = 0; i < segments_.size(); i++) {
        if (new_segments.empty()) {
            new_segments.push_back(segments_[i]);
            continue;
        }

        Segment& last = new_segments.back();
        if (last.is_free && segments_[i].is_free &&
            last.start + last.length == segments_[i].start) {
            last.length += segments_[i].length;
        } else {
            new_segments.push_back(segments_[i]);
        }
    }

    segments_ = new_segments;
}

void MemoryPool::defragment_no_lock() {
    if (segments_.size() <= 1) {
        return;
    }

    l2_compactions_++;

    std::vector<Segment> new_segments;
    int64_t current_pos = 0;

    std::unordered_map<int64_t, int64_t> start_to_ref;
    for (auto& kv : metadata_) {
        start_to_ref[kv.second.start] = kv.first;
    }

    for (size_t i = 0; i < segments_.size(); ++i) {
        Segment seg = segments_[i];
        if (seg.is_free) {
            continue;
        }

        int64_t original_start = seg.start;

        if (seg.latches > 0) {
            if (current_pos != seg.start) {
                current_pos = seg.start + seg.length;
            } else {
                current_pos += seg.length;
            }

            new_segments.push_back(seg);

            auto it = start_to_ref.find(original_start);
            if (it != start_to_ref.end()) {
                int64_t ref_id = it->second;
                if (metadata_.find(ref_id) != metadata_.end()) {
                    metadata_[ref_id].start = seg.start;
                }
            }
        } else {
            if (seg.start != current_pos) {
                std::memmove(pool_ + current_pos, pool_ + seg.start, seg.length);
                seg.start = current_pos;
            }

            new_segments.push_back(seg);

            auto it = start_to_ref.find(original_start);
            if (it != start_to_ref.end()) {
                int64_t ref_id = it->second;
                if (metadata_.find(ref_id) != metadata_.end()) {
                    metadata_[ref_id].start = seg.start;
                }
            }

            current_pos += seg.length;
        }
    }

    if (current_pos < size_) {
        Segment free_segment;
        free_segment.start = current_pos;
        free_segment.length = size_ - current_pos;
        free_segment.latches = 0;
        free_segment.is_free = true;
        new_segments.push_back(free_segment);
    }

    segments_ = new_segments;
}

bool MemoryPool::resize_pool_no_lock(int64_t new_size) {
    unsigned char* new_pool = static_cast<unsigned char*>(std::realloc(pool_, new_size));
    if (!new_pool) {
        return false;
    }

    pool_ = new_pool;
    size_ = new_size;
    resizes_++;
    return true;
}

int64_t MemoryPool::find_segment_index_no_lock(int64_t start) {
    for (size_t i = 0; i < segments_.size(); i++) {
        if (segments_[i].start == start) {
            return static_cast<int64_t>(i);
        }
    }
    return -1;
}

int64_t MemoryPool::get_fragmentation_no_lock() {
    int64_t total_free = 0;
    int64_t largest_free = 0;

    for (const auto& seg : segments_) {
        if (seg.is_free) {
            total_free += seg.length;
            if (seg.length > largest_free) {
                largest_free = seg.length;
            }
        }
    }

    if (total_free == 0) {
        return 0;
    }
    if (largest_free == 0) {
        return 100;
    }

    return 100 - (largest_free * 100 / total_free);
}

PoolStats MemoryPool::get_stats_no_lock() {
    int64_t total_used = 0;
    int64_t total_free = 0;
    int64_t used_blocks = 0;
    int64_t free_blocks = 0;
    int64_t largest_free = 0;

    for (const auto& seg : segments_) {
        if (seg.is_free) {
            total_free += seg.length;
            free_blocks++;
            if (seg.length > largest_free) {
                largest_free = seg.length;
            }
        } else {
            total_used += seg.length;
            used_blocks++;
        }
    }

    int64_t fragmentation = 0;
    if (total_free == 0) {
        fragmentation = 0;
    } else if (largest_free == 0) {
        fragmentation = 100;
    } else {
        fragmentation = 100 - (largest_free * 100 / total_free);
    }

    return {
        size_,
        total_used,
        total_free,
        used_blocks,
        free_blocks,
        largest_free,
        fragmentation,
        commits_,
        failed_commits_,
        reads_,
        releases_,
        l1_compactions_,
        l2_compactions_,
        resizes_
    };
}

int64_t MemoryPool::commit(const void* data, int64_t length) {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t ref_id = next_ref_id_;
    next_ref_id_++;

    if (length == 0) {
        Metadata metadata;
        metadata.start = -1;
        metadata.length = 0;
        metadata.latches = 0;
        metadata.orig_length = 0;
        metadata_[ref_id] = metadata;
        commits_++;
        return ref_id;
    }

    if (!data) {
        failed_commits_++;
        return -1;
    }

    int64_t aligned_size = align_size(length);

    int64_t segment_index = find_best_fit_no_lock(aligned_size);

    if (segment_index == -1) {
        merge_adjacent_free_no_lock();
        segment_index = find_best_fit_no_lock(aligned_size);

        if (segment_index == -1) {
            defragment_no_lock();
            segment_index = find_best_fit_no_lock(aligned_size);

            if (segment_index == -1 && auto_resize_) {
                int64_t old_size = size_;
                int64_t new_size = size_ * 2;
                if (size_ + aligned_size * 2 > new_size) {
                    new_size = size_ + aligned_size * 2;
                }

                if (resize_pool_no_lock(new_size)) {
                    int64_t size_increase = new_size - old_size;
                    if (!segments_.empty() && segments_.back().is_free) {
                        segments_.back().length += size_increase;
                    } else {
                        Segment additional_space;
                        additional_space.start = old_size;
                        additional_space.length = size_increase;
                        additional_space.latches = 0;
                        additional_space.is_free = true;
                        segments_.push_back(additional_space);
                    }

                    segment_index = find_best_fit_no_lock(aligned_size);
                }
            }
        }
    }

    if (segment_index == -1) {
        failed_commits_++;
        return -1;
    }

    Segment segment = segments_[segment_index];

    Segment new_segment;
    new_segment.start = segment.start;
    new_segment.length = aligned_size;
    new_segment.latches = 0;
    new_segment.is_free = false;

    Metadata metadata;
    metadata.start = new_segment.start;
    metadata.length = new_segment.length;
    metadata.latches = 0;
    metadata.orig_length = length;

    if (segment.length > aligned_size) {
        segment.start += aligned_size;
        segment.length -= aligned_size;
        segments_[segment_index] = segment;
        segments_.insert(segments_.begin() + segment_index, new_segment);
        metadata_[ref_id] = metadata;
    } else {
        segments_[segment_index] = new_segment;
        metadata_[ref_id] = metadata;
    }

    std::memcpy(pool_ + new_segment.start, data, length);
    used_size_ += aligned_size;
    commits_++;

    return ref_id;
}

ReadResult MemoryPool::read(int64_t ref_id, bool latch) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Metadata metadata = it->second;

    if (metadata.start == -1) {
        return {nullptr, 0};
    }

    int64_t segment_index = find_segment_index_no_lock(metadata.start);
    if (segment_index == -1) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Segment& segment = segments_[segment_index];
    reads_++;

    if (latch) {
        segment.latches++;
        metadata.latches = segment.latches;
        metadata_[ref_id] = metadata;
        read_locks_++;
    }

    return {pool_ + segment.start, metadata.orig_length};
}

void MemoryPool::unlatch(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Metadata metadata = it->second;

    if (metadata.start == -1) {
        return;
    }

    int64_t segment_index = find_segment_index_no_lock(metadata.start);
    if (segment_index == -1) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Segment& segment = segments_[segment_index];
    if (segment.latches == 0) {
        throw std::runtime_error("Segment was not latched.");
    }

    segment.latches--;
    metadata.latches = segment.latches;
    metadata_[ref_id] = metadata;
}

void MemoryPool::latch(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Metadata metadata = it->second;

    if (metadata.start == -1) {
        return;
    }

    int64_t segment_index = find_segment_index_no_lock(metadata.start);
    if (segment_index == -1) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Segment& segment = segments_[segment_index];
    segment.latches++;
    metadata.latches = segment.latches;
    metadata_[ref_id] = metadata;
    read_locks_++;
}

void MemoryPool::release(int64_t ref_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    releases_++;
    Metadata metadata = it->second;
    metadata_.erase(ref_id);

    if (metadata.start == -1) {
        return;
    }

    int64_t segment_index = find_segment_index_no_lock(metadata.start);
    if (segment_index == -1) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Segment& segment = segments_[segment_index];

    if (segment.latches > 0) {
        segment.latches = 0;
    }

    segment.is_free = true;
    segment.latches = 0;
    used_size_ -= segment.length;
}

ReserveResult MemoryPool::reserve_for_write(int64_t size) {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t aligned_size = align_size(size);

    int64_t segment_index = find_best_fit_no_lock(aligned_size);

    if (segment_index == -1) {
        merge_adjacent_free_no_lock();
        segment_index = find_best_fit_no_lock(aligned_size);

        if (segment_index == -1) {
            defragment_no_lock();
            segment_index = find_best_fit_no_lock(aligned_size);

            if (segment_index == -1 && auto_resize_) {
                int64_t old_size = size_;
                int64_t new_size = size_ * 2;
                if (size_ + aligned_size * 2 > new_size) {
                    new_size = size_ + aligned_size * 2;
                }

                if (resize_pool_no_lock(new_size)) {
                    int64_t size_increase = new_size - old_size;
                    if (!segments_.empty() && segments_.back().is_free) {
                        segments_.back().length += size_increase;
                    } else {
                        Segment additional_space;
                        additional_space.start = old_size;
                        additional_space.length = size_increase;
                        additional_space.latches = 0;
                        additional_space.is_free = true;
                        segments_.push_back(additional_space);
                    }

                    segment_index = find_best_fit_no_lock(aligned_size);
                }
            }
        }
    }

    if (segment_index == -1) {
        failed_commits_++;
        return {-1, nullptr, 0};
    }

    Segment segment = segments_[segment_index];

    Segment new_segment;
    new_segment.start = segment.start;
    new_segment.length = aligned_size;
    new_segment.latches = 1;
    new_segment.is_free = false;

    Metadata metadata;
    metadata.start = new_segment.start;
    metadata.length = new_segment.length;
    metadata.latches = 1;
    metadata.orig_length = 0;

    if (segment.length > aligned_size) {
        segment.start += aligned_size;
        segment.length -= aligned_size;
        segments_[segment_index] = segment;
        segments_.insert(segments_.begin() + segment_index, new_segment);
        metadata_[next_ref_id_] = metadata;
    } else {
        segments_[segment_index] = new_segment;
        metadata_[next_ref_id_] = metadata;
    }

    int64_t ref_id = next_ref_id_;
    next_ref_id_++;

    used_size_ += aligned_size;
    commits_++;

    return {ref_id, pool_ + new_segment.start, new_segment.length};
}

void MemoryPool::finalize_commit(int64_t ref_id, int64_t actual_length) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = metadata_.find(ref_id);
    if (it == metadata_.end()) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Metadata metadata = it->second;
    metadata.orig_length = actual_length;

    if (metadata.start == -1) {
        metadata_[ref_id] = metadata;
        return;
    }

    int64_t segment_index = find_segment_index_no_lock(metadata.start);
    if (segment_index == -1) {
        throw std::invalid_argument("Invalid reference ID.");
    }

    Segment& segment = segments_[segment_index];
    if (segment.latches > 0) {
        segment.latches--;
        metadata.latches = segment.latches;
    }

    metadata_[ref_id] = metadata;
}

void MemoryPool::clear() {
    std::lock_guard<std::mutex> lock(mu_);

    segments_.clear();
    metadata_.clear();
    used_size_ = 0;
    next_ref_id_ = 0;
    commits_ = 0;
    failed_commits_ = 0;
    reads_ = 0;
    read_locks_ = 0;
    l1_compactions_ = 0;
    l2_compactions_ = 0;
    releases_ = 0;
    resizes_ = 0;

    Segment initial_segment;
    initial_segment.start = 0;
    initial_segment.length = size_;
    initial_segment.latches = 0;
    initial_segment.is_free = true;
    segments_.push_back(initial_segment);
}

int64_t MemoryPool::available_space() {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t total_free = 0;
    for (const auto& seg : segments_) {
        if (seg.is_free) {
            total_free += seg.length;
        }
    }
    return total_free;
}

int64_t MemoryPool::get_fragmentation() {
    std::lock_guard<std::mutex> lock(mu_);
    return get_fragmentation_no_lock();
}

PoolStats MemoryPool::get_stats() {
    std::lock_guard<std::mutex> lock(mu_);
    return get_stats_no_lock();
}

void MemoryPool::level1_compaction() {
    std::lock_guard<std::mutex> lock(mu_);
    merge_adjacent_free_no_lock();
}

void MemoryPool::level2_compaction() {
    std::lock_guard<std::mutex> lock(mu_);
    defragment_no_lock();
}

std::vector<MetadataSnapshot> MemoryPool::snapshot_metadata() {
    std::lock_guard<std::mutex> lock(mu_);

    std::vector<MetadataSnapshot> result;
    for (const auto& kv : metadata_) {
        MetadataSnapshot snap;
        snap.ref_id = kv.first;
        snap.start = kv.second.start;
        snap.length = kv.second.length;
        snap.latches = kv.second.latches;
        snap.orig_length = kv.second.orig_length;
        result.push_back(snap);
    }
    return result;
}

std::vector<FreeSegmentSnapshot> MemoryPool::snapshot_free_segments() {
    std::lock_guard<std::mutex> lock(mu_);

    std::vector<FreeSegmentSnapshot> result;
    for (const auto& seg : segments_) {
        if (seg.is_free) {
            FreeSegmentSnapshot snap;
            snap.start = seg.start;
            snap.length = seg.length;
            result.push_back(snap);
        }
    }
    return result;
}

} // namespace opteryx
