/**
 * Lock-free parquet IO pipeline using BS::thread_pool and moodycamel queue.
 *
 * Pure C++ IO:
 * - Local files: POSIX pread()
 * - HTTP / HTTPS: HttpClient::get() with Range header
 * - GCS gs://: rewritten to https://storage.googleapis.com/... then HTTP range
 *
 * Worker threads read + decode + IPC-serialize without the GIL.
 * Results dequeued via lock-free moodycamel queue.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <deque>
#include <exception>
#include <cstdint>
#include <utility>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <map>

#include "BS_thread_pool.hpp"
#include "http_client.hpp"
#include "decode.hpp"
#include "ipc_serialize.hpp"
#include "metadata.hpp"

namespace rugo {

struct MorselRef {
    std::string path;
    int rg_idx = -1;
    std::vector<std::string> column_names;
    std::vector<std::vector<uint8_t>> column_ipc_bytes;
    int64_t bytes_fetched = 0;
    uint64_t read_ns = 0;
    uint64_t decode_ns = 0;
    std::string error;
    bool success = false;
};

class ParquetIOPipeline {
 private:
    struct WorkItem {
        std::string path;
        int rg_idx;
        std::vector<std::string> column_names;
        std::vector<ColumnStats> column_stats;  // absolute file offsets
    };

    std::shared_ptr<BS::light_thread_pool> decode_pool_;
    // Multi-producer (4 decode workers) / single-consumer (Python-side caller)
    // queue. Lock contention is negligible vs the IO/decode cost per item.
    std::deque<MorselRef> result_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    size_t queue_capacity_;
    HttpClient http_client_;

    std::atomic<int> pending_work_{0};
    std::atomic<bool> shutdown_{false};

    // Diagnostic counters for queue-contention investigation.
    std::atomic<uint64_t> spin_iterations_{0};
    std::atomic<uint64_t> enqueue_count_{0};
    std::atomic<size_t>   queue_high_watermark_{0};

    /**
     * Convert gs://bucket/path to https://storage.googleapis.com/bucket/path.
     */
    static std::string gcs_to_https(const std::string& path) {
        // gs://bucket/object  →  https://storage.googleapis.com/bucket/object
        return "https://storage.googleapis.com/" + path.substr(5);
    }

    /**
     * Read a byte range from any supported path type.
     * Returns (bytes, elapsed_ns).
     */
    std::pair<std::vector<uint8_t>, uint64_t> read_range(
            const std::string& path, int64_t offset, int64_t size) {

        auto t0 = std::chrono::steady_clock::now();
        std::vector<uint8_t> bytes;

        if (path.substr(0, 5) == "gs://") {
            std::string url = gcs_to_https(path);
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = http_client_.get(url, {{"Range", range_hdr}});

        } else if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = http_client_.get(path, {{"Range", range_hdr}});

        } else {
            // Local file: POSIX pread
            bytes.resize(size);
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                throw std::runtime_error("Cannot open file: " + path);
            }
            ssize_t n = pread(fd, bytes.data(), size, offset);
            close(fd);
            if (n < 0) {
                throw std::runtime_error("Read error: " + path);
            }
            if (static_cast<size_t>(n) != static_cast<size_t>(size)) {
                throw std::runtime_error("Short read: " + path +
                    " (expected " + std::to_string(size) +
                    ", got " + std::to_string(n) + ")");
            }
        }

        uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();
        return {std::move(bytes), elapsed};
    }

    void decode_row_group(const WorkItem& item) {
        MorselRef result;
        result.path = item.path;
        result.rg_idx = item.rg_idx;
        result.column_names = item.column_names;
        result.success = true;

        uint64_t total_read_ns = 0;
        uint64_t total_decode_ns = 0;

        // For local files, mmap the full column-chunk extent of the row group
        // once rather than open/pread/close per column.  Eliminates per-column
        // heap allocation and gives the kernel a sequential-prefetch hint via
        // MADV_SEQUENTIAL.  Falls back to read_range() for HTTP/GCS.
        bool is_local = item.path.rfind("gs://",   0) != 0 &&
                        item.path.rfind("http://",  0) != 0 &&
                        item.path.rfind("https://", 0) != 0;

        void*   mmap_base   = MAP_FAILED;
        size_t  mmap_len    = 0;
        int64_t mmap_offset = 0;  // page-aligned file offset of the mapping

        if (is_local && !item.column_stats.empty()) {
            int64_t span_min = INT64_MAX, span_max = 0;
            for (const auto& cs : item.column_stats) {
                int64_t base = cs.data_page_offset;
                if (cs.dictionary_page_offset >= 0 && cs.dictionary_page_offset < base)
                    base = cs.dictionary_page_offset;
                int64_t end = base + cs.total_compressed_size;
                if (base < span_min) span_min = base;
                if (end   > span_max) span_max = end;
            }
            long page_size  = sysconf(_SC_PAGESIZE);
            mmap_offset     = (span_min / page_size) * page_size;
            mmap_len        = static_cast<size_t>(span_max - mmap_offset);

            auto t_map = std::chrono::steady_clock::now();
            int fd = open(item.path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd >= 0) {
                mmap_base = mmap(nullptr, mmap_len, PROT_READ, MAP_PRIVATE, fd, mmap_offset);
                close(fd);
                // No madvise: let the OS manage readahead.
                if (mmap_base == MAP_FAILED)
                    mmap_base = MAP_FAILED;
            }
            total_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - t_map).count();
        }

        try {
            for (size_t i = 0; i < item.column_stats.size(); ++i) {
                const auto& col_stats = item.column_stats[i];

                int64_t base_offset = col_stats.data_page_offset;
                if (col_stats.dictionary_page_offset >= 0 &&
                    col_stats.dictionary_page_offset < base_offset) {
                    base_offset = col_stats.dictionary_page_offset;
                }
                int64_t chunk_size = col_stats.total_compressed_size;

                ColumnStats adjusted = col_stats;
                adjusted.data_page_offset -= base_offset;
                if (adjusted.dictionary_page_offset >= 0)
                    adjusted.dictionary_page_offset -= base_offset;

                DecodedColumn decoded;
                if (mmap_base != MAP_FAILED) {
                    // Zero-copy: slice directly into the mmap — no heap allocation.
                    const uint8_t* chunk_ptr =
                        static_cast<const uint8_t*>(mmap_base) + (base_offset - mmap_offset);
                    auto t_dec = std::chrono::steady_clock::now();
                    decoded = DecodeColumnFromChunk(
                        chunk_ptr, static_cast<size_t>(chunk_size), &adjusted);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                    result.bytes_fetched += chunk_size;
                } else {
                    // Fallback: pread for HTTP/GCS or if mmap failed.
                    auto [raw_bytes, read_ns] = read_range(item.path, base_offset, chunk_size);
                    result.bytes_fetched += chunk_size;
                    total_read_ns += read_ns;
                    auto t_dec = std::chrono::steady_clock::now();
                    decoded = DecodeColumnFromChunk(
                        raw_bytes.data(), raw_bytes.size(), &adjusted);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                }

                if (!decoded.success) {
                    result.success = false;
                    result.error = "Decode failed for column: " + col_stats.name;
                    break;
                }

                std::vector<uint8_t> ipc_bytes;
                rugo::serialize_decoded_column(decoded, ipc_bytes);
                result.column_ipc_bytes.push_back(std::move(ipc_bytes));
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error = e.what();
        }

        if (mmap_base != MAP_FAILED)
            munmap(mmap_base, mmap_len);

        result.read_ns = total_read_ns;
        result.decode_ns = total_decode_ns;
        // Apply soft back-pressure: if the consumer is far behind, block
        // on the condition variable until it drains rather than spin-yielding.
        {
            std::unique_lock<std::mutex> lk(queue_mutex_);
            queue_cv_.wait(lk, [this]() {
                return result_queue_.size() < queue_capacity_ || shutdown_.load(std::memory_order_relaxed);
            });
            if (!shutdown_.load(std::memory_order_relaxed)) {
                result_queue_.push_back(std::move(result));
                size_t sz = result_queue_.size();
                enqueue_count_.fetch_add(1, std::memory_order_relaxed);
                size_t prev = queue_high_watermark_.load(std::memory_order_relaxed);
                while (sz > prev &&
                       !queue_high_watermark_.compare_exchange_weak(
                           prev, sz, std::memory_order_relaxed)) {}
            }
        }
        pending_work_--;
        queue_cv_.notify_one();
    }

 public:
    ParquetIOPipeline(int decode_workers = 4,
                      size_t result_queue_capacity = 256)
        : decode_pool_(std::make_shared<BS::light_thread_pool>(decode_workers)),
          queue_capacity_(result_queue_capacity),
          http_client_() {}

    ~ParquetIOPipeline() {
        wait_shutdown();
    }

    /**
     * Submit a row group for read + decode + serialize.
     * column_stats carry absolute file offsets — worker adjusts to buffer-relative.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats) {
        if (shutdown_) return;

        pending_work_++;

        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;

        decode_pool_->detach_task([this, item = std::move(item)]() {
            decode_row_group(item);
        });
    }

    bool try_get_result(MorselRef& out) {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        if (result_queue_.empty()) return false;
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        queue_cv_.notify_one();  // wake a blocked producer if queue was full
        return true;
    }

    /**
     * Block until a result is available or the pipeline is fully drained.
     * Returns true and populates `out` when a result is ready.
     * Returns false when the pipeline is shut down and the queue is empty.
     */
    bool wait_and_get_result(MorselRef& out) {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        queue_cv_.wait(lk, [this]() {
            return !result_queue_.empty() || shutdown_.load(std::memory_order_relaxed);
        });
        if (result_queue_.empty()) {
            return false;  // shutdown and nothing left
        }
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        queue_cv_.notify_one();  // wake a blocked producer if queue was full
        return true;
    }

    void wait_shutdown() {
        shutdown_ = true;
        queue_cv_.notify_all();
        if (decode_pool_) {
            decode_pool_->wait();
        }
    }

    int pending_work_count() const {
        return pending_work_.load(std::memory_order_relaxed);
    }

    uint64_t spin_iterations() const {
        return spin_iterations_.load(std::memory_order_relaxed);
    }
    uint64_t enqueue_count() const {
        return enqueue_count_.load(std::memory_order_relaxed);
    }
    size_t queue_high_watermark() const {
        return queue_high_watermark_.load(std::memory_order_relaxed);
    }
};

}  // namespace rugo
