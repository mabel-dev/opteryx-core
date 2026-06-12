// Adapter wiring an opteryx::MemoryPool into a rugo::PoolSink so the parquet
// IO pipeline's C++ worker threads can serialize decoded columns directly into
// pool-reserved memory (no intermediate heap buffer, no commit() copy).
//
// This lives on the opteryx side: opteryx depends on rugo, so it may include
// both headers. rugo's PoolSink stays pure-C and rugo never references opteryx.
//
// Thread-safety: reserve_for_write / finalize_commit are mutex-guarded inside
// MemoryPool, so concurrent calls from N decode workers are safe (serialized on
// the pool mutex — a tiny critical section vs the decode/serialize work).
#pragma once

#include "memory_pool.hpp"   // opteryx::MemoryPool  (src/cpp)
#include "io_pipeline.hpp"   // rugo::PoolSink, rugo::ParquetIOPipeline (rugo/src/parquet)
#include "core/alloc.h"      // draken_malloc / draken_free (draken/)

namespace opteryx {

// WP-6b direct path: the worker allocates non-nullable fixed-width column
// buffers with the Draken allocator so the consumer can transfer ownership to a
// Vector (freed by draken_free on GC). draken_malloc/free are extern "C" static
// inline; wrap them as plain function pointers for the C-ABI sink.
inline void* pool_sink_draken_alloc(size_t n) noexcept { return draken_malloc(n); }
inline void  pool_sink_draken_free(void* p) noexcept { draken_free(p); }

// noexcept: a throw here would cross the C-ABI function-pointer boundary, which
// is undefined. reserve_for_write never throws (returns a {-1,nullptr,0}
// sentinel on exhaustion, which the worker turns into an honest error);
// finalize_commit throws only on an invalid ref_id, which cannot happen for a
// ref_id we just reserved — so terminate-on-throw is the correct fail-loud.
inline int64_t pool_sink_reserve(void* ctx, int64_t size, void** out_ptr) noexcept {
    auto* pool = static_cast<MemoryPool*>(ctx);
    ReserveResult r = pool->reserve_for_write(size);
    *out_ptr = r.ptr;        // nullptr when ref_id == -1 (exhausted)
    return r.ref_id;
}

inline void pool_sink_finalize(void* ctx, int64_t ref_id, int64_t actual_len) noexcept {
    static_cast<MemoryPool*>(ctx)->finalize_commit(ref_id, actual_len);
}

// Build the sink and attach it to the pipeline. Called once at pipeline
// construction, before any row group is submitted.
inline void wire_pool_sink(rugo::ParquetIOPipeline* pipe, MemoryPool* pool) {
    rugo::PoolSink sink;
    sink.ctx          = static_cast<void*>(pool);
    sink.reserve      = &pool_sink_reserve;
    sink.finalize     = &pool_sink_finalize;
    sink.draken_alloc = &pool_sink_draken_alloc;
    sink.draken_free  = &pool_sink_draken_free;
    pipe->set_pool_sink(sink);
}

}  // namespace opteryx
