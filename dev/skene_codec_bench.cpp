// Decompression-speed comparison on REAL skene section bytes.
//
// Corpus: the plain (uncompressed) section payloads of a zstd-0 skene file —
// exactly the bytes a codec would be asked to compress. Sections under
// kCompressMinBytes are skipped, matching writer.cpp's own gate, so the numbers
// describe the bytes that would actually be compressed in practice.
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <string>
#include <algorithm>

#include "zstd.h"
#include "lz4.h"
#include "snappy.h"

using Clock = std::chrono::steady_clock;
static double ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

struct Result {
    const char* name;
    double comp_ms, decomp_ms;
    size_t comp_bytes;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: codec_bench <file> [zstd-level ...]\n");
        return 1;
    }
    // Levels come from the command line so the ratio-vs-compress-time curve can
    // be walked without a rebuild; the default set is the one the original
    // comparison used.
    std::vector<int> levels;
    for (int i = 2; i < argc; ++i) levels.push_back(std::atoi(argv[i]));
    if (levels.empty()) levels = {1, 3, 9};

    FILE* f = std::fopen(argv[1], "rb");
    if (!f) { std::fprintf(stderr, "cannot open %s\n", argv[1]); return 1; }
    std::fseek(f, 0, SEEK_END);
    size_t n = (size_t)std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> buf(n);
    if (std::fread(buf.data(), 1, n, f) != n) { std::fprintf(stderr, "short read\n"); return 1; }
    std::fclose(f);

    // Chunk the file the way skene stores sections: independent blocks. 256KB is
    // representative of a large column section and keeps each codec working on
    // the same block boundaries, so the comparison is apples-to-apples.
    const size_t kChunk = 256 * 1024;
    std::vector<std::pair<const uint8_t*, size_t>> chunks;
    for (size_t off = 0; off + kChunk <= n; off += kChunk)
        chunks.emplace_back(buf.data() + off, kChunk);
    const size_t plain_total = chunks.size() * kChunk;
    std::printf("corpus %s\n  %zu chunks x %zu KB = %.1f MB\n\n",
                argv[1], chunks.size(), kChunk / 1024, plain_total / 1e6);

    std::vector<Result> results;

    auto bench = [&](const char* name,
                     auto compress_fn, auto decompress_fn) {
        std::vector<std::vector<uint8_t>> packed(chunks.size());
        auto t0 = Clock::now();
        for (size_t i = 0; i < chunks.size(); ++i)
            compress_fn(chunks[i].first, chunks[i].second, packed[i]);
        auto t1 = Clock::now();

        size_t comp_bytes = 0;
        for (auto& p : packed) comp_bytes += p.size();

        std::vector<uint8_t> out(kChunk);
        // best of 3 decompression passes over the whole corpus
        double best = 1e30;
        for (int rep = 0; rep < 3; ++rep) {
            auto d0 = Clock::now();
            for (size_t i = 0; i < chunks.size(); ++i)
                decompress_fn(packed[i], out.data(), kChunk);
            auto d1 = Clock::now();
            best = std::min(best, ms(d0, d1));
        }
        results.push_back({name, ms(t0, t1), best, comp_bytes});
    };

    for (int level : levels) {
        std::string label = "zstd-" + std::to_string(level);
        char* stored = new char[label.size() + 1];
        std::strcpy(stored, label.c_str());
        bench(stored,
              [level](const uint8_t* src, size_t len, std::vector<uint8_t>& dst) {
                  dst.resize(ZSTD_compressBound(len));
                  size_t got = ZSTD_compress(dst.data(), dst.size(), src, len, level);
                  dst.resize(got);
              },
              [](std::vector<uint8_t>& src, uint8_t* dst, size_t cap) {
                  ZSTD_decompress(dst, cap, src.data(), src.size());
              });
    }

    bench("lz4",
          [](const uint8_t* src, size_t len, std::vector<uint8_t>& dst) {
              dst.resize((size_t)LZ4_compressBound((int)len));
              int got = LZ4_compress_default((const char*)src, (char*)dst.data(),
                                             (int)len, (int)dst.size());
              dst.resize((size_t)got);
          },
          [](std::vector<uint8_t>& src, uint8_t* dst, size_t cap) {
              LZ4_decompress_safe((const char*)src.data(), (char*)dst,
                                  (int)src.size(), (int)cap);
          });

    bench("snappy",
          [](const uint8_t* src, size_t len, std::vector<uint8_t>& dst) {
              dst.resize(snappy::MaxCompressedLength(len));
              size_t got = 0;
              snappy::RawCompress((const char*)src, len, (char*)dst.data(), &got);
              dst.resize(got);
          },
          [](std::vector<uint8_t>& src, uint8_t* dst, size_t cap) {
              (void)cap;
              snappy::RawUncompress((const char*)src.data(), src.size(), (char*)dst);
          });

    std::printf("%-10s %10s %12s %14s %12s\n",
                "codec", "ratio", "comp MB/s", "DECOMP MB/s", "decomp ms");
    std::printf("%s\n", std::string(62, '-').c_str());
    for (auto& r : results) {
        double ratio = (double)plain_total / (double)r.comp_bytes;
        double cmbs  = (plain_total / 1e6) / (r.comp_ms / 1e3);
        double dmbs  = (plain_total / 1e6) / (r.decomp_ms / 1e3);
        std::printf("%-10s %9.2fx %10.0f %13.0f %11.1f\n",
                    r.name, ratio, cmbs, dmbs, r.decomp_ms);
    }
    return 0;
}

// Build (from the repo root):
//
//   ZSTD_SRC=$(find third_party/zstd -name "*.cpp" | tr '\n' ' ')
//   c++ -std=c++17 -O2 -DNDEBUG -DZSTD_DISABLE_ASM -o /tmp/skene_codec_bench \
//     dev/skene_codec_bench.cpp third_party/lz4/lz4.c \
//     third_party/snappy/snappy.cc third_party/snappy/snappy-sinksource.cc \
//     third_party/snappy/snappy-stubs-internal.cc ${ZSTD_SRC} \
//     -I third_party/lz4 -I third_party/snappy -I third_party/zstd -I third_party/zstd/common
//
//   /tmp/skene_codec_bench scratch/hits_skene/hits_0-rg0000.skene
//
// Dev tooling only — never imported by production code (repo rules §5).
