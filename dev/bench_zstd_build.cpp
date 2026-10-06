// dev/bench_zstd_build.cpp — is the vendored zstd decompressing at full speed?
//
// ARCH-AWARE PLAN (docs/ARCH_AWARE_PERFORMANCE_TEST_PLAN.md §9): zstd decompress is
// 18.7% of ClickBench CPU and 37% of TPC-H SF10 parquet CPU on the i5. Before
// questioning the codec, check the build: compile this once against the vendored
// sources with the production flags and once against the system libzstd, run both
// on the same corpus, compare GB/s.
//
// The corpus is any file; it is cut into `frame` byte chunks (default 1 MiB, about
// a parquet page), each compressed at `level`, and every frame is decompressed with
// one reused DCtx (as rugo/src/parquet/compression.cpp does). Min of N passes.
//
//   vendored (x86, production flags):
//     g++ -O3 -std=c++20 -march=haswell -mtune=generic -DZSTD_STATIC_LINKING_ONLY \
//         -Ithird_party/zstd -Ithird_party/zstd/common dev/bench_zstd_build.cpp \
//         third_party/zstd/common/*.cpp third_party/zstd/decompress/*.cpp \
//         third_party/zstd/decompress/huf_decompress_amd64.S third_party/zstd/compress/*.cpp \
//         -o /tmp/bz_vendored
//   system:
//     g++ -O3 -std=c++20 -march=haswell -Ithird_party/zstd dev/bench_zstd_build.cpp \
//         -l:libzstd.so.1 -o /tmp/bz_system
//   run: /tmp/bz_xxx <corpus> [level=3] [frame_bytes=1048576] [passes=9]

#include <zstd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <corpus> [level] [frame_bytes] [passes]\n", argv[0]);
        return 2;
    }
    const int level = argc > 2 ? std::atoi(argv[2]) : 3;
    const size_t frame = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : (1u << 20);
    const int passes = argc > 4 ? std::atoi(argv[4]) : 9;

    FILE* f = std::fopen(argv[1], "rb");
    if (!f) { std::perror("open"); return 1; }
    std::vector<char> raw;
    char buf[1 << 16];
    size_t got;
    while ((got = std::fread(buf, 1, sizeof buf, f)) > 0) raw.insert(raw.end(), buf, buf + got);
    std::fclose(f);

    std::vector<std::vector<char>> comp;
    std::vector<size_t> sizes;
    size_t total_comp = 0;
    for (size_t off = 0; off < raw.size(); off += frame) {
        const size_t n = std::min(frame, raw.size() - off);
        std::vector<char> c(ZSTD_compressBound(n));
        const size_t r = ZSTD_compress(c.data(), c.size(), raw.data() + off, n, level);
        if (ZSTD_isError(r)) { std::fprintf(stderr, "compress: %s\n", ZSTD_getErrorName(r)); return 1; }
        c.resize(r);
        total_comp += r;
        comp.push_back(std::move(c));
        sizes.push_back(n);
    }

    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    std::vector<char> out(frame);
    double best = 1e300;
    for (int p = 0; p < passes; ++p) {
        const auto t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < comp.size(); ++i) {
            const size_t r = ZSTD_decompressDCtx(dctx, out.data(), sizes[i], comp[i].data(), comp[i].size());
            if (ZSTD_isError(r) || r != sizes[i]) { std::fprintf(stderr, "decompress failed\n"); return 1; }
        }
        const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        if (s < best) best = s;
    }
    ZSTD_freeDCtx(dctx);
    std::printf("zstd %s  level %d  frame %zu  corpus %.1f MiB  ratio %.2f  decompress %.0f MiB/s (min of %d)\n",
                ZSTD_versionString(), level, frame, raw.size() / 1048576.0,
                static_cast<double>(raw.size()) / total_comp, raw.size() / 1048576.0 / best, passes);
    return 0;
}
