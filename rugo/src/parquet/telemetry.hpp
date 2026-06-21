#pragma once
// Process-wide timing accumulators for the rugo Parquet decoder.
//
// These MUST be global atomics, not thread_local: the decode hot path runs on
// the PageDecodePool / io_pipeline worker threads, so thread_local counters are
// incremented on the workers and never observed by get_cpp_telemetry() (which
// reads on the calling/Python thread). Atomic globals aggregate every worker's
// contribution. Accumulation is per-page / per-chunk, so the relaxed fetch_add
// is negligible against the work it measures.
//
// Usage (C++):
//   #include "telemetry.hpp"
//   RUGO_TEL_START(t0);
//   ... work ...
//   RUGO_TEL_ACCUM(rugo_tel::decompress_ns, t0);
//
// Usage (Cython):
//   import opteryx.compiled.rugo.parquet as rp
//   rp.reset_cpp_telemetry()
//   ... run workload ...
//   t = rp.get_cpp_telemetry()   # dict with keys below (seconds)

#include <atomic>
#include <chrono>

namespace rugo_tel {

// Accumulators in nanoseconds — zero them via reset()
inline std::atomic<long long> metadata_ns      {0};  // ReadParquetMetadataFromBuffer
inline std::atomic<long long> decompress_ns    {0};  // ZSTD/Snappy DecompressInto (dict + data pages)
inline std::atomic<long long> dict_parse_ns    {0};  // dict value parsing (type-specific loops)
inline std::atomic<long long> prescan_ns       {0};  // Tier 3B: page pre-scan metadata collection
inline std::atomic<long long> page_parallel_ns {0};  // Tier 3B: parallel page decoding
inline std::atomic<long long> rle_ns           {0};  // RLE/bit-packed index decode
inline std::atomic<long long> val_expand_ns    {0};  // index->value expansion
inline std::atomic<long long> mask_filter_ns   {0};  // post-loop row-mask filter
inline std::atomic<long long> validity_bmp_ns  {0};  // validity bitmap construction
inline std::atomic<long long> calls            {0};  // DecodeColumnFromChunk calls

inline void reset() {
    metadata_ns.store(0, std::memory_order_relaxed);
    decompress_ns.store(0, std::memory_order_relaxed);
    dict_parse_ns.store(0, std::memory_order_relaxed);
    prescan_ns.store(0, std::memory_order_relaxed);
    page_parallel_ns.store(0, std::memory_order_relaxed);
    rle_ns.store(0, std::memory_order_relaxed);
    val_expand_ns.store(0, std::memory_order_relaxed);
    mask_filter_ns.store(0, std::memory_order_relaxed);
    validity_bmp_ns.store(0, std::memory_order_relaxed);
    calls.store(0, std::memory_order_relaxed);
}

using Clock = std::chrono::steady_clock;
using TP    = std::chrono::time_point<Clock>;

inline TP now() { return Clock::now(); }

inline long long elapsed_ns(TP t0) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
}

// Seconds accessors for the Cython surface (ns -> s).
inline double metadata_s()      { return metadata_ns.load(std::memory_order_relaxed)      * 1e-9; }
inline double decompress_s()    { return decompress_ns.load(std::memory_order_relaxed)    * 1e-9; }
inline double dict_parse_s()    { return dict_parse_ns.load(std::memory_order_relaxed)    * 1e-9; }
inline double prescan_s()       { return prescan_ns.load(std::memory_order_relaxed)       * 1e-9; }
inline double page_parallel_s() { return page_parallel_ns.load(std::memory_order_relaxed) * 1e-9; }
inline double rle_s()           { return rle_ns.load(std::memory_order_relaxed)           * 1e-9; }
inline double val_expand_s()    { return val_expand_ns.load(std::memory_order_relaxed)    * 1e-9; }
inline double mask_filter_s()   { return mask_filter_ns.load(std::memory_order_relaxed)   * 1e-9; }
inline double validity_bmp_s()  { return validity_bmp_ns.load(std::memory_order_relaxed)  * 1e-9; }
inline long long calls_count()  { return calls.load(std::memory_order_relaxed); }

} // namespace rugo_tel

#define RUGO_TEL_START(name)       auto name = rugo_tel::now()
#define RUGO_TEL_ACCUM(acc, name)  (acc).fetch_add(rugo_tel::elapsed_ns(name), std::memory_order_relaxed)
