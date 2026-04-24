/**
 * Lock-free parquet IO pipeline using BS::thread_pool and moodycamel queue.
 *
 * Architecture:
 * 1. Read stage: Fetch column chunk ranges from filesystem
 * 2. Decode stage: Decompress, parse, assemble Morsel
 * 3. Emit stage: Serialize Morsel to MemoryPool, enqueue result ref
 *
 * The result queue (moodycamel) carries only metadata (ref_id, bytes_written).
 * Actual Morsel data lives in the provided MemoryPool.
 *
 * Thread-safe: Read/decode are fire-and-forget task submission; results
 * dequeued via lock-free moodycamel queue.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <exception>
#include <cstdint>

#include "../../../../third_party/bshoshany/BS_thread_pool.hpp"
#include "../../../../third_party/moodycamel/readerwriterqueue.h"

namespace rugo {

/**
 * Result metadata carried in the lock-free queue.
 * Actual Morsel data is in the MemoryPool at ref_id.
 */
struct MorselRef {
    int64_t ref_id = -1;
    int64_t bytes_written = 0;
    std::string error;
};

/**
 * Parquet IO pipeline with integrated read/decode/emit stages.
 *
 * Usage:
 *   ParquetIOPipeline pipeline(read_workers, decode_workers);
 *   pipeline.submit_row_group(path, rg_idx);
 *   MorselRef result;
 *   while (pipeline.try_get_result(result)) { ... }
 *   pipeline.wait_shutdown();
 */
class ParquetIOPipeline {
 private:
    struct WorkItem {
        std::string path;
        int rg_idx;
    };

    std::shared_ptr<BS::thread_pool> read_pool_;
    std::shared_ptr<BS::thread_pool> decode_pool_;
    moodycamel::ReaderWriterQueue<MorselRef> result_queue_;

    std::atomic<int> pending_work_{0};
    std::atomic<bool> shutdown_{false};

    // Work queues (passed between stages)
    // Note: For simplicity in this design, we use fire-and-forget semantics.
    // A more complex design could use explicit inter-stage queues, but this
    // minimizes contention and relies on thread pool's internal queue.

 public:
    /**
     * Create the pipeline with specified worker counts.
     *
     * @param read_workers Number of read worker threads (default: 16)
     * @param decode_workers Number of decode worker threads (default: 4)
     * @param result_queue_capacity Size of result queue (default: 256)
     */
    ParquetIOPipeline(int read_workers = 16,
                      int decode_workers = 4,
                      size_t result_queue_capacity = 256)
        : read_pool_(std::make_shared<BS::thread_pool>(read_workers)),
          decode_pool_(std::make_shared<BS::thread_pool>(decode_workers)),
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
     * Non-blocking. Queues work to read stage.
     * Results are enqueued to result_queue_ asynchronously.
     *
     * @param path Parquet file path
     * @param rg_idx Row group index
     */
    void submit_row_group(const std::string& path, int rg_idx) {
        if (shutdown_) {
            return;
        }

        pending_work_++;

        // Submit read task (stage 1)
        // The read task will submit decode, which submits emit.
        // This creates a pipeline with implicit flow.
        read_pool_->submit([this, path, rg_idx]() {
            try {
                _read_stage(path, rg_idx);
            } catch (const std::exception& e) {
                MorselRef error_ref;
                error_ref.ref_id = -1;
                error_ref.error = e.what();
                result_queue_.try_enqueue(error_ref);
                pending_work_--;
            }
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

        // Wait for thread pools to finish all submitted work
        if (read_pool_) {
            read_pool_->wait();
        }
        if (decode_pool_) {
            decode_pool_->wait();
        }

        // Optionally reset pool pointers for cleanup
        // (BS::thread_pool destructor will handle cleanup on shared_ptr::reset)
    }

    /**
     * Get approximate number of pending (unfinished) work items.
     */
    int pending_work_count() const {
        return pending_work_.load(std::memory_order_relaxed);
    }

 private:
    /**
     * Stage 1: Fetch column chunks from filesystem.
     *
     * Simplified for this design: assumes data is already fetched.
     * In a real implementation, this would:
     * - Open filesystem (GCS, local, HTTP, etc.)
     * - Fetch column chunk metadata
     * - Issue range reads
     * - Submit to decode_pool when chunks are ready
     *
     * For now, we just transition to decode directly.
     */
    void _read_stage(const std::string& path, int rg_idx) {
        // Submit decode task
        decode_pool_->submit([this, path, rg_idx]() {
            try {
                _decode_stage(path, rg_idx);
            } catch (const std::exception& e) {
                MorselRef error_ref;
                error_ref.ref_id = -1;
                error_ref.error = e.what();
                result_queue_.try_enqueue(error_ref);
                pending_work_--;
            }
        });
    }

    /**
     * Stage 2: Decode (decompress, parse) columns and assemble Morsel.
     *
     * Simplified: Would call rugo::DecodeColumnFromChunk() here
     * to decompress/parse each column chunk.
     *
     * For now, we transition directly to emit.
     */
    void _decode_stage(const std::string& path, int rg_idx) {
        // In real implementation:
        // - Decompress column chunks (LZ4, ZSTD, etc.)
        // - Parse values into Draken vectors
        // - Assemble into Morsel
        // - Call _emit_stage()

        _emit_stage(path, rg_idx);
    }

    /**
     * Stage 3: Serialize Morsel to MemoryPool and enqueue result ref.
     *
     * Simplified: Would call morsel_io::write_morsel() here.
     *
     * For now, we just decrement pending and enqueue a dummy ref.
     */
    void _emit_stage(const std::string& path, int rg_idx) {
        // In real implementation:
        // - Reserve segment in MemoryPool
        // - Serialize Morsel to reserved segment (write_morsel)
        // - Finalize commit
        // - Enqueue MorselRef to result_queue_

        MorselRef result;
        result.ref_id = 0;  // Placeholder
        result.bytes_written = 0;  // Placeholder
        result_queue_.try_enqueue(result);

        pending_work_--;
    }
};

}  // namespace rugo
