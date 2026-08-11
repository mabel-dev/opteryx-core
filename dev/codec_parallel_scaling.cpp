// Parallel decompression scaling: does adding cores buy throughput, per codec?
//
// The question this answers: LZ4 decompresses at 79-94% of memcpy speed on ONE
// core (dev/codec_matrix_bench.cpp), which suggests it is already memory-
// bandwidth bound and has no headroom, while zstd at ~16% of memcpy is compute
// bound and should scale. That was an INFERENCE from single-core proximity to
// memcpy. This measures the curve instead.
//
// It matters because the engine decodes on a worker pool, and on the remote path
// there are idle cores while the pipe is the bottleneck. A codec that converts
// idle CPU into saved bytes is worth more there than a fast one that cannot use
// the cores it is given.
//
// Method, and why each part:
//   - Each thread owns a PRIVATE copy of the compressed input and a PRIVATE
//     output buffer. Sharing one input would let the compressed bytes sit in
//     shared cache and flatter every codec unequally.
//   - Per-thread working set is kChunkMB so the aggregate at high thread counts
//     comfortably exceeds any cache and the measurement reaches real memory.
//   - Threads rendezvous on a spin barrier before the timed region, so startup
//     skew is not counted as work.
//   - memcpy is measured on the same harness as the bandwidth CEILING. Speedup
//     is reported against each codec's own 1-thread number; "% ceiling" against
//     memcpy at the SAME thread count, which is what "saturated" actually means.
//
// Dev tooling only — never imported by production code (repo rules §5).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "lz4.h"
#include "zstd.h"

using Clock = std::chrono::steady_clock;

static constexpr size_t kChunkMB = 32;
static constexpr size_t kChunk = kChunkMB * 1024 * 1024;

struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
    uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
};

// Two shapes, chosen because they sit at opposite ends of the single-core
// result: "sequential" is where zstd's ratio advantage lives and where its decode
// is slowest; "random" is where LZ4 degenerates to a literal copy and runs at
// memcpy speed.
static std::vector<uint8_t> make_source(int kind) {
    std::vector<uint8_t> v(kChunk);
    Rng rng(0xBEEF);
    static const char kAlpha[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    const size_t width = 128;
    for (size_t i = 0; i * width < kChunk; ++i) {
        uint8_t* row = v.data() + i * width;
        const size_t left = std::min(width, kChunk - i * width);
        if (kind == 0) {
            char buf[192];
            std::snprintf(buf, sizeof(buf),
                          "CVE-2021-%06zu-vulnerability-record", i % 1000000);
            const size_t n = std::strlen(buf);
            std::memcpy(row, buf, std::min(n, left));
            if (n < left) std::memset(row + n, '.', left - n);
        } else {
            for (size_t b = 0; b < left; ++b)
                row[b] = static_cast<uint8_t>(kAlpha[rng.next() % (sizeof(kAlpha) - 1)]);
        }
    }
    return v;
}

enum class Kind { kMemcpy, kLz4, kZstd };

struct Run { int threads; double mbs; };

static double run_one(Kind kind, int nthreads,
                      const std::vector<uint8_t>& src,
                      const std::vector<uint8_t>& packed,
                      int iters) {
    // Private per-thread buffers.
    std::vector<std::vector<uint8_t>> ins(nthreads), outs(nthreads);
    for (int t = 0; t < nthreads; ++t) {
        ins[t] = packed;
        outs[t].assign(src.size(), 0);
    }

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> bad{false};
    std::vector<std::thread> pool;
    pool.reserve(nthreads);

    for (int t = 0; t < nthreads; ++t) {
        pool.emplace_back([&, t] {
            ZSTD_DCtx* dctx = (kind == Kind::kZstd) ? ZSTD_createDCtx() : nullptr;
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!go.load(std::memory_order_acquire)) { /* spin */ }

            for (int i = 0; i < iters; ++i) {
                if (kind == Kind::kMemcpy) {
                    std::memcpy(outs[t].data(), ins[t].data(), outs[t].size());
                } else if (kind == Kind::kLz4) {
                    int g = LZ4_decompress_safe(
                        reinterpret_cast<const char*>(ins[t].data()),
                        reinterpret_cast<char*>(outs[t].data()),
                        static_cast<int>(ins[t].size()),
                        static_cast<int>(outs[t].size()));
                    if (g != (int)outs[t].size()) bad.store(true);
                } else {
                    size_t g = ZSTD_decompressDCtx(dctx, outs[t].data(), outs[t].size(),
                                                   ins[t].data(), ins[t].size());
                    if (ZSTD_isError(g) || g != outs[t].size()) bad.store(true);
                }
            }
            if (dctx) ZSTD_freeDCtx(dctx);
        });
    }

    while (ready.load(std::memory_order_acquire) < nthreads) { /* spin */ }
    const auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (auto& th : pool) th.join();
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();

    if (bad.load()) { std::printf("  ** DECODE FAILED at %d threads **\n", nthreads); return 0.0; }
    const double total_mb = (double)src.size() * nthreads * iters / 1e6;
    return total_mb / (ms / 1e3);
}

int main() {
    const unsigned hw = std::thread::hardware_concurrency();
    std::printf("parallel decompression scaling — %u logical cores, %zu MB private "
                "buffer per thread\n\n", hw, kChunkMB);

    std::vector<int> thread_counts;
    for (int t : {1, 2, 4, 6, 8, 10, 12, 16, 18})
        if (t <= (int)hw) thread_counts.push_back(t);

    for (int shape = 0; shape < 2; ++shape) {
        const std::vector<uint8_t> src = make_source(shape);
        const char* shape_name = shape == 0 ? "str128 sequential (compressible)"
                                            : "str128 random (high entropy)";

        // Compress once per codec.
        std::vector<uint8_t> lz4_packed(LZ4_compressBound((int)src.size()));
        int ln = LZ4_compress_default(reinterpret_cast<const char*>(src.data()),
                                      reinterpret_cast<char*>(lz4_packed.data()),
                                      (int)src.size(), (int)lz4_packed.size());
        lz4_packed.resize(ln);

        std::vector<uint8_t> zstd_packed(ZSTD_compressBound(src.size()));
        size_t zn = ZSTD_compress(zstd_packed.data(), zstd_packed.size(),
                                  src.data(), src.size(), 4);
        zstd_packed.resize(zn);

        std::vector<uint8_t> raw_packed = src;  // memcpy source

        std::printf("%s\n", shape_name);
        std::printf("  lz4 %.2f MB (%.2fx)   zstd-4 %.2f MB (%.2fx)   raw %.2f MB\n",
                    ln / 1e6, (double)src.size() / ln,
                    zn / 1e6, (double)src.size() / zn, src.size() / 1e6);
        std::printf("  %-8s %14s %14s %14s %12s %12s\n",
                    "threads", "memcpy MB/s", "lz4 MB/s", "zstd-4 MB/s",
                    "lz4 speedup", "zstd speedup");

        double lz4_base = 0, zstd_base = 0;
        for (int n : thread_counts) {
            // Iterations sized so each configuration runs long enough to measure,
            // and so slower codecs are not measured over a shorter window.
            const int it_fast = 4, it_slow = 2;
            const double mc = run_one(Kind::kMemcpy, n, src, raw_packed, it_fast);
            const double l4 = run_one(Kind::kLz4,    n, src, lz4_packed, it_fast);
            const double zs = run_one(Kind::kZstd,   n, src, zstd_packed, it_slow);
            if (n == 1) { lz4_base = l4; zstd_base = zs; }
            std::printf("  %-8d %14.0f %14.0f %14.0f %11.2fx %11.2fx\n",
                        n, mc, l4, zs,
                        lz4_base > 0 ? l4 / lz4_base : 0.0,
                        zstd_base > 0 ? zs / zstd_base : 0.0);
        }
        std::printf("\n");
    }
    return 0;
}

// Build (from the repo root):
//
//   c++ -std=c++17 -O2 -DNDEBUG -DZSTD_DISABLE_ASM -o /tmp/codec_parallel_scaling \
//     dev/codec_parallel_scaling.cpp third_party/lz4/lz4.c \
//     $(find third_party/zstd -name "*.cpp") \
//     -I third_party/lz4 -I third_party/zstd -I third_party/zstd/common
//
//   /tmp/codec_parallel_scaling
//
// Dev tooling only — never imported by production code (repo rules §5).
