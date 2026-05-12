#ifndef OPTERYX_MEMORY_POOL_HPP
#define OPTERYX_MEMORY_POOL_HPP

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string>
#include <stdexcept>
#include <algorithm>

namespace opteryx {

struct PoolStats {
    int64_t total_size;
    int64_t used_size;
    int64_t free_size;
    int64_t used_blocks;
    int64_t free_blocks;
    int64_t largest_free_block;
    int64_t fragmentation;
    int64_t commits;
    int64_t failed_commits;
    int64_t reads;
    int64_t releases;
    int64_t l1_compactions;
    int64_t l2_compactions;
    int64_t resizes;
};

struct ReadResult {
    const void* ptr;
    int64_t length;
};

struct ReserveResult {
    int64_t ref_id;
    void* ptr;
    int64_t capacity;
};

struct MetadataSnapshot {
    int64_t ref_id;
    int64_t start;
    int64_t length;
    int64_t latches;
    int64_t orig_length;
};

struct FreeSegmentSnapshot {
    int64_t start;
    int64_t length;
};

class MemoryPool {
public:
    MemoryPool(int64_t size,
               std::string name = "Memory Pool",
               bool auto_resize = false,
               int64_t alignment = 1);
    ~MemoryPool();

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;
    MemoryPool(MemoryPool&&) = delete;
    MemoryPool& operator=(MemoryPool&&) = delete;

    int64_t commit(const void* data, int64_t length);
    ReadResult read(int64_t ref_id, bool latch);
    void unlatch(int64_t ref_id);
    void latch(int64_t ref_id);
    void release(int64_t ref_id);
    ReserveResult reserve_for_write(int64_t size);
    void finalize_commit(int64_t ref_id, int64_t actual_length);

    void clear();
    int64_t available_space();
    int64_t get_fragmentation();
    PoolStats get_stats();

    void level1_compaction();
    void level2_compaction();

    std::vector<MetadataSnapshot> snapshot_metadata();
    std::vector<FreeSegmentSnapshot> snapshot_free_segments();

private:
    struct Segment {
        int64_t start;
        int64_t length;
        int64_t latches;
        bool is_free;
    };

    struct Metadata {
        int64_t start;
        int64_t length;
        int64_t latches;
        int64_t orig_length;
    };

    std::mutex mu_;
    unsigned char* pool_;
    int64_t size_;
    int64_t used_size_;
    int64_t alignment_;
    bool auto_resize_;
    std::string name_;
    int64_t next_ref_id_;

    std::vector<Segment> segments_;
    std::unordered_map<int64_t, Metadata> metadata_;

    int64_t commits_;
    int64_t failed_commits_;
    int64_t reads_;
    int64_t read_locks_;
    int64_t l1_compactions_;
    int64_t l2_compactions_;
    int64_t releases_;
    int64_t resizes_;

    int64_t find_best_fit_no_lock(int64_t size);
    void merge_adjacent_free_no_lock();
    void defragment_no_lock();
    bool resize_pool_no_lock(int64_t new_size);
    int64_t find_segment_index_no_lock(int64_t start);
    int64_t get_fragmentation_no_lock();
    PoolStats get_stats_no_lock();
    int64_t align_size(int64_t size);
};

} // namespace opteryx

#endif // OPTERYX_MEMORY_POOL_HPP
