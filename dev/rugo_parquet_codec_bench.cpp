// rugo_parquet_codec_bench — compare zstd levels on REAL rugo parquet page bodies.
//
// Takes a parquet file rugo wrote with compression="none", so every column
// chunk in it is exactly the byte sequence the writer hands to the codec
// (dictionary page + data page, already dict/RLE/bitpack encoded). Each column
// chunk is one compression unit, matching what the writer actually does.
//
// Both arms are measured with the SAME calls production uses:
//   compress   -> ZSTD_compress            (_parquet_writer.hpp::zstd_compress_block)
//   decompress -> ZSTD_decompressDCtx      (compression.cpp, reused thread DCtx)
//
// Levels are run INTERLEAVED (L_a, L_b, L_a, L_b, ...) within each round so a
// thermal ramp hits both arms equally; medians/bests are reported alongside
// means for the same reason.
//
// Dev tooling only — never imported by production code (repo rules §5).

#include "metadata.hpp"

#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

std::vector<uint8_t> read_file(const std::string &path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) throw std::runtime_error("cannot open " + path);
    std::streamsize n = f.tellg();
    f.seekg(0, std::ios::beg);
    std::vector<uint8_t> buf(static_cast<size_t>(n));
    if (!f.read(reinterpret_cast<char *>(buf.data()), n))
        throw std::runtime_error("short read on " + path);
    return buf;
}

struct Unit {          // one column chunk = one compression unit
    const uint8_t *ptr;
    size_t len;
};

struct RoundResult {
    double comp_ms;
    double decomp_ms;
    size_t comp_bytes;
};

double mean(const std::vector<double> &v) {
    return std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
}

double best(std::vector<double> v) {
    return *std::min_element(v.begin(), v.end());
}

double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 3) {
        std::fprintf(stderr,
                     "usage: %s <uncompressed.parquet> <column> [rounds] [level ...]\n",
                     argv[0]);
        return 2;
    }
    const std::string path = argv[1];
    const std::string column = argv[2];
    const int rounds = (argc > 3) ? std::atoi(argv[3]) : 3;

    std::vector<int> levels;
    for (int i = 4; i < argc; ++i) levels.push_back(std::atoi(argv[i]));
    if (levels.empty()) levels = {3, 9};

    std::vector<uint8_t> file = read_file(path);
    FileStats fs = ReadParquetMetadataFromBuffer(file.data(), file.size());

    // Census mode: column == "*" walks every column and reports chunk-size
    // distribution and compressibility, one round, at the given levels. Used to
    // size a per-column compression policy (which columns clear a byte
    // threshold, which respond to level at all).
    if (column == "*") {
        std::printf("%-26s %-12s %7s %11s %11s", "column", "type", "chunks",
                    "plain MB", "chunk KB");
        for (int lv : levels) std::printf("  L%-2d ratio  L%-2d ms", lv, lv);
        std::printf("\n");
        std::vector<std::string> seen;
        for (const auto &rg : fs.row_groups) {
            for (const auto &col : rg.columns) {
                if (std::find(seen.begin(), seen.end(), col.name) != seen.end()) continue;
                seen.push_back(col.name);
                std::vector<Unit> cu;
                size_t plain = 0;
                std::string ptype = col.physical_type;
                for (const auto &rg2 : fs.row_groups)
                    for (const auto &c2 : rg2.columns) {
                        if (c2.name != col.name) continue;
                        int64_t st = (c2.dictionary_page_offset > 0)
                                         ? c2.dictionary_page_offset
                                         : c2.data_page_offset;
                        cu.push_back({file.data() + st,
                                      static_cast<size_t>(c2.total_compressed_size)});
                        plain += static_cast<size_t>(c2.total_compressed_size);
                    }
                std::printf("%-26s %-12s %7zu %11.2f %11.0f", col.name.c_str(),
                            ptype.c_str(), cu.size(), plain / 1e6,
                            (plain / static_cast<double>(cu.size())) / 1e3);
                // Compress once per level, keeping every chunk so the decode
                // side is timed on real compressed bodies with the reused DCtx
                // production uses.
                ZSTD_DCtx *cdctx = ZSTD_createDCtx();
                std::vector<std::vector<uint8_t>> held(cu.size());
                std::vector<size_t> held_n(cu.size());
                size_t widest = 0;
                for (const auto &u : cu) widest = std::max(widest, u.len);
                std::vector<uint8_t> dst(widest);
                for (int lv : levels) {
                    size_t total = 0;
                    for (size_t i = 0; i < cu.size(); ++i)
                        held[i].resize(ZSTD_compressBound(cu[i].len));
                    auto t0 = Clock::now();
                    for (size_t i = 0; i < cu.size(); ++i) {
                        size_t n = ZSTD_compress(held[i].data(), held[i].size(),
                                                 cu[i].ptr, cu[i].len, lv);
                        if (ZSTD_isError(n)) throw std::runtime_error("compress failed");
                        held_n[i] = n;
                        total += n;
                    }
                    double ms = ms_since(t0);
                    auto t1 = Clock::now();
                    for (size_t i = 0; i < cu.size(); ++i) {
                        size_t n = ZSTD_decompressDCtx(cdctx, dst.data(), cu[i].len,
                                                       held[i].data(), held_n[i]);
                        if (ZSTD_isError(n) || n != cu[i].len)
                            throw std::runtime_error("decompress failed");
                    }
                    double dms = ms_since(t1);
                    // memcpy baseline: what an UNCOMPRESSED chunk costs the
                    // reader instead, so the threshold trade is like-for-like.
                    auto t2 = Clock::now();
                    for (const auto &u : cu) std::memcpy(dst.data(), u.ptr, u.len);
                    double mms = ms_since(t2);
                    std::printf("  %9.3fx %8.1f %8.2f %8.2f",
                                static_cast<double>(plain) / total, ms, dms, mms);
                }
                ZSTD_freeDCtx(cdctx);
                std::printf("\n");
            }
        }
        return 0;
    }

    // Collect the column's chunk regions, one per row group.
    std::vector<Unit> units;
    size_t plain_total = 0;
    for (const auto &rg : fs.row_groups) {
        for (const auto &col : rg.columns) {
            if (col.name != column) continue;
            if (col.codec != 0) {
                std::fprintf(stderr,
                             "column '%s' is stored with codec %d, not UNCOMPRESSED — "
                             "write the input with compression=\"none\"\n",
                             column.c_str(), col.codec);
                return 1;
            }
            int64_t start = (col.dictionary_page_offset > 0)
                                ? col.dictionary_page_offset
                                : col.data_page_offset;
            int64_t len = col.total_compressed_size;  // == uncompressed under codec 0
            if (start < 0 || len <= 0 ||
                static_cast<size_t>(start + len) > file.size())
                throw std::runtime_error("column chunk region out of file bounds");
            units.push_back({file.data() + start, static_cast<size_t>(len)});
            plain_total += static_cast<size_t>(len);
        }
    }
    if (units.empty()) throw std::runtime_error("column not found: " + column);

    std::printf("file      : %s\n", path.c_str());
    std::printf("column    : %s\n", column.c_str());
    std::printf("rows      : %lld\n", static_cast<long long>(fs.num_rows));
    std::printf("units     : %zu column chunks (one per row group)\n", units.size());
    std::printf("plain     : %.2f MB\n", plain_total / 1e6);
    std::printf("rounds    : %d, interleaved\n\n", rounds);

    // Pre-allocate destination buffers OUTSIDE the timed regions so allocation
    // noise cannot land on either arm.
    std::vector<std::vector<uint8_t>> comp_bufs(units.size());
    for (size_t i = 0; i < units.size(); ++i)
        comp_bufs[i].resize(ZSTD_compressBound(units[i].len));
    std::vector<size_t> comp_sizes(units.size());

    size_t max_plain = 0;
    for (const auto &u : units) max_plain = std::max(max_plain, u.len);
    std::vector<uint8_t> scratch(max_plain);

    ZSTD_DCtx *dctx = ZSTD_createDCtx();

    std::vector<std::vector<RoundResult>> results(levels.size());

    for (int r = 0; r < rounds; ++r) {
        for (size_t li = 0; li < levels.size(); ++li) {
            const int level = levels[li];

            auto t0 = Clock::now();
            for (size_t i = 0; i < units.size(); ++i) {
                size_t n = ZSTD_compress(comp_bufs[i].data(), comp_bufs[i].size(),
                                         units[i].ptr, units[i].len, level);
                if (ZSTD_isError(n))
                    throw std::runtime_error(std::string("compress failed: ") +
                                             ZSTD_getErrorName(n));
                comp_sizes[i] = n;
            }
            double comp_ms = ms_since(t0);

            size_t comp_bytes = 0;
            for (size_t n : comp_sizes) comp_bytes += n;

            auto t1 = Clock::now();
            for (size_t i = 0; i < units.size(); ++i) {
                size_t n = ZSTD_decompressDCtx(dctx, scratch.data(), units[i].len,
                                               comp_bufs[i].data(), comp_sizes[i]);
                if (ZSTD_isError(n) || n != units[i].len)
                    throw std::runtime_error("decompress failed / size mismatch");
            }
            double decomp_ms = ms_since(t1);

            results[li].push_back({comp_ms, decomp_ms, comp_bytes});
            std::printf("round %d  zstd-%-2d  compress %8.1f ms   decompress %7.1f ms   "
                        "%9.2f MB\n",
                        r + 1, level, comp_ms, decomp_ms, comp_bytes / 1e6);
        }
    }
    ZSTD_freeDCtx(dctx);

    std::printf("\n%-8s %8s %10s %10s %10s %10s %10s %10s\n", "level", "ratio",
                "bytes MB", "comp avg", "comp best", "dec avg", "dec med", "dec best");
    for (size_t li = 0; li < levels.size(); ++li) {
        std::vector<double> c, d;
        for (const auto &rr : results[li]) {
            c.push_back(rr.comp_ms);
            d.push_back(rr.decomp_ms);
        }
        size_t bytes = results[li][0].comp_bytes;
        for (const auto &rr : results[li])
            if (rr.comp_bytes != bytes)
                std::fprintf(stderr, "WARNING: compressed size varied across rounds\n");
        std::printf("zstd-%-3d %7.3fx %10.2f %10.1f %10.1f %10.1f %10.1f %10.1f\n",
                    levels[li], static_cast<double>(plain_total) / bytes, bytes / 1e6,
                    mean(c), best(c), mean(d), median(d), best(d));
    }

    std::printf("\n%-8s %14s %16s\n", "level", "comp MB/s", "decomp MB/s");
    for (size_t li = 0; li < levels.size(); ++li) {
        std::vector<double> c, d;
        for (const auto &rr : results[li]) {
            c.push_back(rr.comp_ms);
            d.push_back(rr.decomp_ms);
        }
        std::printf("zstd-%-3d %14.0f %16.0f\n", levels[li],
                    (plain_total / 1e6) / (best(c) / 1e3),
                    (plain_total / 1e6) / (best(d) / 1e3));
    }
    return 0;
}

// Build (from the repo root):
//
//   ZSTD_SRC=$(find third_party/zstd -name "*.cpp" | tr '\n' ' ')
//   c++ -std=c++17 -O2 -DNDEBUG -DZSTD_DISABLE_ASM -o /tmp/rugo_parquet_codec_bench \
//     dev/rugo_parquet_codec_bench.cpp rugo/src/parquet/metadata.cpp ${ZSTD_SRC} \
//     -I rugo/src/parquet -I third_party/zstd -I third_party/zstd/common
//
//   /tmp/rugo_parquet_codec_bench <url_none.parquet> URL 3 3 9
//
// Input must be written by rugo with compression="none".
