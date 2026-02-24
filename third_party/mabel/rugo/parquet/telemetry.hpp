#pragma once
// Lightweight per-thread timing accumulators for the rugo Parquet decoder.
// All state is thread_local and inline; no .cpp is required.
//
// Usage (C++):
//   #include "telemetry.hpp"
//   RUGO_TEL_START(t0);
//   ... work ...
//   RUGO_TEL_ACCUM(rugo_tel::decompress_s, t0);
//
// Usage (Cython):
//   import opteryx.rugo.parquet as rp
//   rp.reset_cpp_telemetry()
//   ... run workload ...
//   t = rp.get_cpp_telemetry()   # dict with keys below

#include <chrono>

namespace rugo_tel {

// Accumulators — zero them via reset()
inline thread_local double metadata_s    = 0.0;  // ReadParquetMetadataFromBuffer
inline thread_local double decompress_s  = 0.0;  // ZSTD DecompressData (dict + data pages)
inline thread_local double dict_parse_s  = 0.0;  // dict value parsing (type-specific loops)
inline thread_local double rle_s         = 0.0;  // RLE/bit-packed index decode
inline thread_local double val_expand_s  = 0.0;  // index→value expansion (push_back loops)
inline thread_local long long calls      = 0;    // DecodeColumnFromChunk calls

inline void reset() {
    metadata_s   = 0.0;
    decompress_s = 0.0;
    dict_parse_s = 0.0;
    rle_s        = 0.0;
    val_expand_s = 0.0;
    calls        = 0;
}

using Clock = std::chrono::steady_clock;
using TP    = std::chrono::time_point<Clock>;

inline TP now() { return Clock::now(); }

inline double elapsed(TP t0) {
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

} // namespace rugo_tel

#define RUGO_TEL_START(name)       auto name = rugo_tel::now()
#define RUGO_TEL_ACCUM(acc, name)  (acc) += rugo_tel::elapsed(name)
