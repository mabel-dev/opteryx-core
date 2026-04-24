#include "../io_pipeline.hpp"

#include <iostream>
#include <cstring>
#include <stdexcept>

namespace rugo {

/* NOTE:
 * This translation unit implements the minimal ParquetIOPipeline methods that
 * interact with the C ABI MemoryPoolApi. The implementation is intentionally
 * small: it demonstrates constructor/destructor wiring and the emit helper that
 * performs reserve-for-write / finalize_commit and the commit_bytes fallback.
 *
 * The pipeline's read/decode logic is intentionally simplified: callers should
 * provide the serialized morsel payload to `submit_row_group` so the emit stage
 * can be exercised end-to-end with the MemoryPool.
 */

/* Constructor / Destructor
 *
 * The header provides the class definition. We implement the out-of-line
 * non-trivial methods here.
 */

ParquetIOPipeline::ParquetIOPipeline(MemoryPoolApi* mem_api, const ParquetIOPipeline::Config& cfg)
    : mem_api_(mem_api),
      cfg_(cfg),
      read_pool_(std::make_unique<BSThreadPoolWrapper>(cfg.read_workers, "io-read-pool")),
      decode_pool_(std::make_unique<BSThreadPoolWrapper>(cfg.decode_workers, "io-decode-pool")),
      result_queue_(cfg.result_queue_capacity),
      pending_work_(0),
      shutting_down_(false)
{
    if (mem_api_ == nullptr) {
        throw std::invalid_argument("MemoryPoolApi pointer must not be null");
    }
}

ParquetIOPipeline::~ParquetIOPipeline() {
    try {
        wait_shutdown();
    } catch (...) {
        // Destructor must not throw; log and continue.
        try {
            std::cerr << "warning: exception while shutting down ParquetIOPipeline\n";
        } catch (...) {
            // swallow
        }
    }
}

/* Submit a pre-serialized morsel for emit.
 *
 * For demonstration this schedules the emit on the decode pool. The real
 * pipeline would assemble the morsel after decode and then call the same
 * emit helper.
 */
void ParquetIOPipeline::submit_row_group(const std::string& path, int rg_idx, const std::string& serialized_morsel) {
    if (shutting_down_.load(std::memory_order_acquire)) {
        // pipeline is shutting down, reject work
        return;
    }

    pending_work_.fetch_add(1, std::memory_order_relaxed);

    // Schedule emit on decode pool to simulate read->decode->emit pipeline
    auto task = [this, path, rg_idx, serialized_morsel]() {
        MorselRef ref;
        try {
            bool ok = this->emit_morsel_to_pool(serialized_morsel, path, rg_idx, ref);
            if (!ok && ref.error.empty()) {
                ref.error = "emit_morsel_to_pool failed without error message";
                ref.ref_id = -1;
            }
        } catch (const std::exception& ex) {
            ref.ref_id = -1;
            ref.error = ex.what();
            ref.path = path;
            ref.rg_idx = rg_idx;
        } catch (...) {
            ref.ref_id = -1;
            ref.error = "unknown exception during emit";
            ref.path = path;
            ref.rg_idx = rg_idx;
        }

        // enqueue result (best-effort non-blocking)
        result_queue_.try_enqueue(ref);

        pending_work_.fetch_sub(1, std::memory_order_relaxed);
    };

    // BSThreadPoolWrapper::submit returns a future-like object; we don't need it here.
    read_pool_->submit(task);
}

/* Non-blocking attempt to pop a result */
bool ParquetIOPipeline::try_get_result(MorselRef& out) {
    return result_queue_.try_dequeue(out);
}

/* Wait for shutdown: block until pools finish work */
void ParquetIOPipeline::wait_shutdown() {
    // Mark shutting down so submitters are prevented from adding new work.
    shutting_down_.store(true, std::memory_order_release);

    // Wait for both pools to finish
    if (read_pool_) {
        read_pool_->wait();
    }
    if (decode_pool_) {
        decode_pool_->wait();
    }

    // At this point pending_work_ should be zero (best-effort). We leave any
    // remaining results in the result queue for the consumer.
}

/* emit_morsel_to_pool
 *
 * Attempt zero-copy reserve-for-write on the MemoryPool via the C ABI. If
 * reservation succeeds, copy payload into the provided pointer and finalize.
 * Otherwise fall back to commit_bytes which copies into the pool.
 *
 * Returns true on success (out contains ref_id and bytes_written), false on
 * failure (out.error set).
 */
bool ParquetIOPipeline::emit_morsel_to_pool(const std::string& serialized, const std::string& path, int rg_idx, MorselRef& out) {
    if (mem_api_ == nullptr) {
        out.ref_id = -1;
        out.bytes_written = 0;
        out.error = "MemoryPoolApi is null";
        out.path = path;
        out.rg_idx = rg_idx;
        return false;
    }

    const int64_t payload_len = static_cast<int64_t>(serialized.size());
    uintptr_t out_ptr = 0;
    int64_t out_capacity = 0;

    // Attempt reserve_for_write_ptr
    int64_t ref_id = -1;
    if (mem_api_->reserve_for_write_ptr) {
        ref_id = mem_api_->reserve_for_write_ptr(mem_api_->ctx, payload_len, &out_ptr, &out_capacity);
    } else {
        ref_id = -1;
    }

    if (ref_id != -1 && out_ptr != 0 && out_capacity >= payload_len) {
        // We have a writable region; copy the payload
        void* dst = reinterpret_cast<void*>(out_ptr);
        std::memcpy(dst, serialized.data(), static_cast<size_t>(payload_len));

        // finalize_commit returns int (0 success/non-zero failure)
        int finalize_rc = 0;
        if (mem_api_->finalize_commit) {
            finalize_rc = mem_api_->finalize_commit(mem_api_->ctx, ref_id, payload_len);
        }

        if (finalize_rc != 0) {
            // best-effort release
            if (mem_api_->release) {
                mem_api_->release(mem_api_->ctx, ref_id);
            }
            out.ref_id = -1;
            out.bytes_written = 0;
            out.error = "finalize_commit failed for reserved segment";
            out.path = path;
            out.rg_idx = rg_idx;
            return false;
        }

        out.ref_id = ref_id;
        out.bytes_written = payload_len;
        out.path = path;
        out.rg_idx = rg_idx;
        return true;
    }

    // Fallback path: commit_bytes
    if (mem_api_->commit_bytes) {
        int64_t commit_ref = mem_api_->commit_bytes(mem_api_->ctx, static_cast<const void*>(serialized.data()), payload_len);
        if (commit_ref == -1) {
            out.ref_id = -1;
            out.bytes_written = 0;
            out.error = "MemoryPool commit_bytes failed (pool exhausted?)";
            out.path = path;
            out.rg_idx = rg_idx;
            return false;
        }
        out.ref_id = commit_ref;
        out.bytes_written = payload_len;
        out.path = path;
        out.rg_idx = rg_idx;
        return true;
    }

    out.ref_id = -1;
    out.bytes_written = 0;
    out.error = "No commit API available on MemoryPoolApi";
    out.path = path;
    out.rg_idx = rg_idx;
    return false;
}

} // namespace rugo