/**
 * Lock-free parquet IO pipeline using BS::thread_pool and moodycamel queue.
 *
 * Pure C++ design:
 * - Read stage: Fetch column chunk bytes from filesystem (HTTP/local)
 * - Decode stage: Call DecodeColumnFromChunk() for each column
 * - Emit stage: Put decoded columns in lock-free result queue
 *
 * Results carry C++ structs (DecodedColumn), NOT serialized data.
 * Cython bridge converts DecodedColumn → Draken vectors → MemoryPool.
 *
 * Thread-safe: Read/decode happen in BS::thread_pool workers (no GIL).
 * Results dequeued via lock-free moodycamel queue.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <exception>
#include <cstdint>
#include <utility>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

#include "../../../../third_party/bshoshany/BS_thread_pool.hpp"
#include "../../../../third_party/moodycamel/readerwriterqueue.h"
#include "decode.hpp"
#include "metadata.hpp"

namespace rugo {

/**
 * Result from pipeline: decoded columns for a row group.
 */
struct MorselRef {
    std::string path;
    int rg_idx = -1;
    std::vector<std::string> column_names;
    std::vector<DecodedColumn> decoded_columns;
    int64_t bytes_fetched = 0;
    uint64_t read_ns = 0;
    uint64_t decode_ns = 0;
    std::string error;
    bool success = false;
};

/**
 * Parquet IO pipeline with integrated read/decode stages.
 *
 * Usage:
 *   ParquetIOPipeline pipeline(16, 4);  // 16 read workers, 4 decode workers
 *   pipeline.submit_row_group(path, rg_idx, column_names, column_stats_vec);
 *   MorselRef result;
 *   while (pipeline.try_get_result(result)) { ... }
 *   pipeline.wait_shutdown();
 */
class ParquetIOPipeline {
 private:
    struct WorkItem {
        std::string path;
        int rg_idx;
        std::vector<std::string> column_names;
        std::vector<ColumnStats> column_stats;
    };

    std::shared_ptr<BS::light_thread_pool> read_pool_;
    std::shared_ptr<BS::light_thread_pool> decode_pool_;
    moodycamel::ReaderWriterQueue<MorselRef> result_queue_;

    std::atomic<int> pending_work_{0};
    std::atomic<bool> shutdown_{false};

    /**
     * Read a range from a file or HTTP URL.
     * Path is detected as HTTP by "http://" or "https://" prefix.
     */
    std::pair<std::vector<uint8_t>, uint64_t> read_range(
        const std::string& path, int64_t offset, int64_t size) {

        auto t0 = std::chrono::steady_clock::now();
        std::vector<uint8_t> bytes(size);

        if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            // TODO: HTTP range read via HttpClient
            // For now, throw — HTTP support deferred to Phase 2
            throw std::runtime_error("HTTP paths not yet supported in C++ pipeline (Phase 2)");
        } else {
            // Local file: POSIX pread
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                throw std::runtime_error("Cannot open file: " + path);
            }

            ssize_t n = pread(fd, bytes.data(), size, offset);
            close(fd);

            if (n < 0) {
                throw std::runtime_error("Read error from file: " + path);
            }
            if (static_cast<size_t>(n) != size) {
                throw std::runtime_error("Short read from file: " + path +
                                       " (expected " + std::to_string(size) +
                                       ", got " + std::to_string(n) + ")");
            }
        }

        uint64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();

        return {std::move(bytes), elapsed_ns};
    }

    /**
     * Main worker that reads and decodes columns for a row group.
     */
    void read_and_decode_row_group(const WorkItem& item) {
        MorselRef result;
        result.path = item.path;
        result.rg_idx = item.rg_idx;
        result.column_names = item.column_names;
        result.success = true;

        uint64_t total_read_ns = 0;
        uint64_t total_decode_ns = 0;

        try {
            // For each requested column, read and decode
            for (size_t i = 0; i < item.column_stats.size(); ++i) {
                const auto& col_stats = item.column_stats[i];

                // Compute base offset: min of data_page_offset and dictionary_page_offset
                int64_t base_offset = col_stats.data_page_offset;
                if (col_stats.dictionary_page_offset >= 0 &&
                    col_stats.dictionary_page_offset < base_offset) {
                    base_offset = col_stats.dictionary_page_offset;
                }

                int64_t chunk_size = col_stats.total_compressed_size;

                // Read column chunk bytes from filesystem
                auto [raw_bytes, read_ns] = read_range(item.path, base_offset, chunk_size);
                result.bytes_fetched += chunk_size;
                total_read_ns += read_ns;

                // Adjust offsets to be relative to the start of the buffer
                // (DecodeColumnFromChunk expects relative offsets, not absolute file offsets)
                ColumnStats adjusted_stats = col_stats;
                adjusted_stats.data_page_offset -= base_offset;
                if (adjusted_stats.dictionary_page_offset >= 0) {
                    adjusted_stats.dictionary_page_offset -= base_offset;
                }

                // Decode the column chunk
                auto t_decode = std::chrono::steady_clock::now();
                DecodedColumn decoded = DecodeColumnFromChunk(
                    raw_bytes.data(), raw_bytes.size(), &adjusted_stats);
                uint64_t decode_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t_decode).count();
                total_decode_ns += decode_ns;

                if (!decoded.success) {
                    result.success = false;
                    result.error = "DecodeColumnFromChunk failed for column " + col_stats.name;
                    break;
                }

                result.decoded_columns.push_back(std::move(decoded));
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error = e.what();
        }

        result.read_ns = total_read_ns;
        result.decode_ns = total_decode_ns;

        // Enqueue result (fire and forget — consumer polls)
        result_queue_.try_enqueue(std::move(result));
        pending_work_--;
    }

 public:
    /**
     * Create the pipeline with specified worker counts.
     *
     * @param read_workers Number of worker threads for reading bytes
     * @param decode_workers Number of worker threads for decoding
     * @param result_queue_capacity Size of lock-free result queue
     */
    ParquetIOPipeline(int read_workers = 16,
                      int decode_workers = 4,
                      size_t result_queue_capacity = 256)
        : read_pool_(std::make_shared<BS::light_thread_pool>(read_workers)),
          decode_pool_(std::make_shared<BS::light_thread_pool>(decode_workers)),
          result_queue_(result_queue_capacity) {}

    /**
     * Destructor: Waits for all pending work to complete.
     */
    ~ParquetIOPipeline() {
        wait_shutdown();
    }

    /**
     * Submit a row group for processing.
     *
     * Non-blocking: queues work to be processed asynchronously.
     * Results are placed in the lock-free queue and consumed via try_get_result().
     *
     * @param path Parquet file path (local or http/https URL)
     * @param rg_idx Row group index
     * @param column_names Names of columns to decode
     * @param column_stats Metadata for each column (with absolute file offsets)
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats) {
        if (shutdown_) {
            return;
        }

        pending_work_++;

        // Create work item
        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;

        // Submit to decode pool (batches both read and decode together)
        // This simplifies synchronization: no intermediate queue needed
        decode_pool_->detach_task([this, item = std::move(item)]() {
            read_and_decode_row_group(item);
        });
    }

    /**
     * Non-blocking attempt to dequeue a result.
     *
     * @param out MorselRef to populate if result available
     * @return true if result was dequeued, false if queue is empty
     */
    bool try_get_result(MorselRef& out) {
        return result_queue_.try_dequeue(out);
    }

    /**
     * Wait for all pending work to complete, then shutdown pools.
     */
    void wait_shutdown() {
        shutdown_ = true;

        // Wait for all submitted tasks to finish
        if (read_pool_) {
            read_pool_->wait();
        }
        if (decode_pool_) {
            decode_pool_->wait();
        }
    }

    /**
     * Get approximate number of pending (unfinished) work items.
     */
    int pending_work_count() const {
        return pending_work_.load(std::memory_order_relaxed);
    }
};

}  // namespace rugo
