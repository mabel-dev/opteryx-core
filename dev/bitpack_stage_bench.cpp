// bitpack_stage_bench — how big a slice of a real Parquet column decode is the
// bit-unpacker actually worth?
//
// bitpack_ab_bench measures the unpack KERNEL. This measures the stage the
// kernel sits in — DecodeRLEBitPackedIndices, the production entry point — on a
// stream produced by the production WRITER (encode_dict_indices), so the run/
// literal mix is the one we actually put on disk, not a synthetic all-literal
// buffer.
//
// The output is a per-value nanosecond cost. Compare it against the per-value
// cost of a whole column read to get the CEILING on any unpacker optimisation:
// no change to the kernel can buy back more than the kernel's own share.
//
// BUILD:
//   clang++ -O3 -std=c++20 -I rugo/src/parquet -I draken/simd -I src/cpp \
//       dev/bitpack_stage_bench.cpp draken/simd/cpu_features.cpp \
//       draken/simd/simd_env.cpp -o /tmp/bitpack_stage_bench
//
// Dev tooling only — never imported by production code (repo rules §5).

#include "decode_encodings.cpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// --- lifted verbatim in BEHAVIOUR from _parquet_writer.hpp::encode_dict_indices.
// Reproduced here rather than included because that header drags in the whole
// writer (thrift, zstd, xxhash). The invariant that matters for this bench is
// the run/literal split, which is what is copied.
void put_varint(std::vector<uint8_t>& b, uint64_t v) {
    while (v >= 0x80) { b.push_back((uint8_t)v | 0x80); v >>= 7; }
    b.push_back((uint8_t)v);
}

std::vector<uint8_t> encode_indices(const std::vector<uint32_t>& codes, int bw) {
    std::vector<uint8_t> out;
    const int value_bytes = (bw + 7) / 8;
    const uint32_t mask = (bw >= 32) ? 0xFFFFFFFFu : ((1u << bw) - 1u);
    auto emit_rle = [&](uint32_t val, size_t run) {
        put_varint(out, (uint64_t)run << 1);
        uint32_t v = val & mask;
        for (int b = 0; b < value_bytes; b++) out.push_back((uint8_t)((v >> (8 * b)) & 0xFF));
    };
    auto emit_bp = [&](const uint32_t* base, size_t count) {
        size_t groups = (count + 7) / 8;
        put_varint(out, ((uint64_t)groups << 1) | 1u);
        uint64_t acc = 0; int nbits = 0;
        for (size_t k = 0; k < groups * 8; k++) {
            uint32_t v = (k < count) ? (base[k] & mask) : 0u;
            acc |= (uint64_t)v << nbits; nbits += bw;
            while (nbits >= 8) { out.push_back((uint8_t)(acc & 0xFF)); acc >>= 8; nbits -= 8; }
        }
    };
    size_t i = 0, n = codes.size();
    while (i < n) {
        size_t run = 1;
        while (i + run < n && codes[i + run] == codes[i]) run++;
        if (run >= 8) { emit_rle(codes[i], run); i += run; continue; }
        size_t lit_start = i, j = i;
        while (j < n) {
            size_t r = 1;
            while (j + r < n && codes[j + r] == codes[j]) r++;
            if (r >= 8) break;
            j += r;
        }
        size_t lit_n = j - lit_start, full = (lit_n / 8) * 8;
        if (full) emit_bp(codes.data() + lit_start, full);
        size_t left = lit_n - full;
        if (left) {
            if (j >= n) emit_bp(codes.data() + lit_start + full, left);
            else {
                size_t k = lit_start + full;
                while (k < lit_start + lit_n) {
                    size_t r = 1;
                    while (k + r < lit_start + lit_n && codes[k + r] == codes[k]) r++;
                    emit_rle(codes[k], r); k += r;
                }
            }
        }
        i = j;
    }
    return out;
}

double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    size_t m = v.size() / 2;
    return (v.size() % 2) ? v[m] : 0.5 * (v[m - 1] + v[m]);
}

// `run_len_mean` == 1 is a pure literal stream (worst case for the unpacker's
// share: every value goes through a bit-packed group). Larger means more RLE
// runs, which is what low-cardinality real columns look like.
std::vector<uint32_t> make_codes(int64_t n, int bw, double run_len_mean,
                                 std::mt19937& rng) {
    const uint32_t card = (bw >= 32) ? 0xFFFFFFFFu : (1u << bw);
    std::uniform_int_distribution<uint32_t> vd(0, card - 1);
    std::geometric_distribution<int> rd(1.0 / run_len_mean);
    std::vector<uint32_t> codes;
    codes.reserve(n);
    while ((int64_t)codes.size() < n) {
        uint32_t v = vd(rng);
        int r = 1 + rd(rng);
        for (int k = 0; k < r && (int64_t)codes.size() < n; k++) codes.push_back(v);
    }
    return codes;
}

} // namespace

int main(int argc, char** argv) {
    const int64_t N      = (argc > 1) ? std::atoll(argv[1]) : 8'000'000;
    const int     ROUNDS = (argc > 2) ? std::atoi(argv[2]) : 9;
    std::mt19937 rng(999);

    std::printf("bitpack_stage_bench  n=%lld values  rounds=%d (median)\n\n",
                (long long)N, ROUNDS);
    std::printf("%4s %8s %12s %12s %10s %12s %11s %10s\n", "bw", "runlen",
                "dense ns/v", "ToRuns ns/v", "unpack ns/v", "unpack%dense",
                "bitpacked%", "runs/val");

    for (int bw : {3, 6, 8, 12}) {
        for (double rl : {1.0, 4.0}) {
            auto codes = make_codes(N, bw, rl, rng);
            auto stream = encode_indices(codes, bw);
            stream.resize(stream.size() + 16, 0);   // slack, as a page body has

            // What fraction of values arrived in bit-packed groups (vs RLE runs)?
            // Only those pay the unpacker at all.
            int64_t bp_values = 0;
            {
                const uint8_t* p = stream.data();
                const uint8_t* e = stream.data() + stream.size();
                int64_t dec = 0;
                while (dec < N && p < e) {
                    uint32_t h = 0; int sh = 0;
                    while (p < e && sh < 32) { uint8_t b = *p++; h |= (uint32_t)(b & 0x7F) << sh; if (!(b & 0x80)) break; sh += 7; }
                    if (h & 1) {
                        int32_t g = (int32_t)(h >> 1), v = g * 8;
                        int32_t need = (v * bw + 7) / 8;
                        if (p + need > e) break;
                        bp_values += std::min<int64_t>(v, N - dec);
                        dec += v; p += need;
                    } else {
                        int32_t c = (int32_t)(h >> 1); int nb = (bw + 7) / 8;
                        p += nb; dec += c;
                    }
                }
            }

            std::vector<int32_t> idx, run_codes, run_counts;
            std::vector<double> t_stage, t_unpack, t_runs;
            std::vector<int32_t> scratch(N);
            size_t n_runs = 0;

            for (int r = 0; r < ROUNDS + 1; r++) {
                // Stage: the real production entry point. It expects a 4-byte
                // length prefix, so hand it one the way DecodeColumnFromChunk does.
                std::vector<uint8_t> prefixed(4 + stream.size());
                uint32_t len = (uint32_t)stream.size();
                std::memcpy(prefixed.data(), &len, 4);
                std::memcpy(prefixed.data() + 4, stream.data(), stream.size());

                auto t0 = Clock::now();
                int32_t got = DecodeRLEBitPackedIndices(prefixed.data(), prefixed.size(),
                                                        (int32_t)N, bw, idx);
                double a = ms_since(t0);
                if (got != (int32_t)N) { std::fprintf(stderr, "decode failed bw=%d\n", bw); return 1; }

                // Unpack only: the same number of bit-packed values the stage
                // just handled, through the same kernel.
                const int64_t bpg_vals = (bp_values / 8) * 8;
                t0 = Clock::now();
                if (bpg_vals > 0)
                    get_unpack_fn()(stream.data(), scratch.data(), (int)(bpg_vals / 8), bw);
                double b = ms_since(t0);

                // The OTHER production decoder: the run-oriented one that
                // byte_array dictionary columns actually take (decode_column.cpp
                // rle_path). Same stream, same values — a like-for-like arm.
                t0 = Clock::now();
                int32_t got2 = DecodeRLEBitPackedIndicesToRuns(
                    stream.data(), stream.size(), (int32_t)N, bw, run_codes, run_counts);
                double c = ms_since(t0);
                if (got2 != (int32_t)N) { std::fprintf(stderr, "ToRuns failed bw=%d\n", bw); return 1; }
                n_runs = run_codes.size();

                if (r > 0) { t_stage.push_back(a); t_unpack.push_back(b); t_runs.push_back(c); }
            }

            const double ms_stage = median(t_stage), ms_unpack = median(t_unpack),
                         ms_runs = median(t_runs);
            std::printf("%4d %8.1f %12.2f %12.2f %10.2f %11.1f%% %10.1f%% %10.3f\n", bw, rl,
                        ms_stage * 1e6 / (double)N,
                        ms_runs  * 1e6 / (double)N,
                        ms_unpack * 1e6 / (double)N,
                        100.0 * ms_unpack / ms_stage,
                        100.0 * (double)bp_values / (double)N,
                        (double)n_runs / (double)N);
        }
    }
    return 0;
}
