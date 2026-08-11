// Codec matrix: LZ4 / ZSTD / RAW across data patterns, for the "LZ4-or-raw at an
// 85% floor" policy decision.
//
// Measures, per (dataset, codec): compression ratio, compress throughput,
// decompress throughput. Decompressed output is VERIFIED against the input on
// the first pass — an unverified decode rate is just a memcpy rate.
//
// Buffers are compressed WHOLE, not chunked, because that is how both writers
// actually call a codec: skene compresses a section body, rugo's parquet writer
// compresses a column chunk (its default max_page_bytes is 0, one page per
// chunk). Chunking would measure a design neither of them has.
//
// ⚠ These are PLAIN buffers — the bytes a column has BEFORE the format's own
// encoding. Both formats bit-pack or dictionary-encode first where they can, so
// e.g. "ints 1..10" reaches a codec as 4-bit codes in skene, not as int64. Read
// this as "what a general codec does to this shape of data", not as a prediction
// of any particular column's on-disk size.
//
// ⛔ LZ4_HC is NOT vendored (third_party/lz4 has lz4.c only), so the HC levels
// are absent here. What plain lz4 offers instead is LZ4_compress_fast's
// ACCELERATION axis, which trades ratio for speed in the FASTER direction — the
// opposite of what HC would give. Vendoring lz4hc.c is an architect decision (§4).
//
// Dev tooling only — never imported by production code (repo rules §5).

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "lz4.h"
#include "zstd.h"

using Clock = std::chrono::steady_clock;
static double ms_between(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ─── Deterministic PRNG (no Date/rand seeding; runs must be reproducible) ────
struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
    uint64_t next() {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        return s;
    }
};

// ─── Datasets ───────────────────────────────────────────────────────────────

struct Dataset {
    std::string name;
    std::vector<uint8_t> bytes;
};

static constexpr size_t kRows = 1000000;

static Dataset ints(const char* name, int kind) {
    Dataset d;
    d.name = name;
    d.bytes.resize(kRows * sizeof(int64_t));
    int64_t* v = reinterpret_cast<int64_t*>(d.bytes.data());
    Rng rng(0xC0FFEE);
    for (size_t i = 0; i < kRows; ++i) {
        switch (kind) {
            case 0: v[i] = static_cast<int64_t>(rng.next()); break;          // random
            case 1: v[i] = static_cast<int64_t>(i); break;                   // sequential
            default: v[i] = static_cast<int64_t>(i % 10) + 1; break;         // 1..10
        }
    }
    return d;
}

// Fixed-width string column, packed back to back — the arena layout, without the
// slot table (which is a separate section and a separate compression decision).
static Dataset strings(const char* name, size_t width, int kind) {
    Dataset d;
    d.name = name;
    d.bytes.assign(kRows * width, 0);
    uint8_t* p = d.bytes.data();
    Rng rng(0xBEEF);
    static const char kAlpha[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    // "highly compressible" mirrors the ints' 1..10 case: a 10-value alphabet,
    // cycled. A single repeated value would compress ~arbitrarily well and say
    // nothing useful.
    std::vector<std::string> ten;
    for (int k = 0; k < 10; ++k) {
        std::string s(width, static_cast<char>('A' + k));
        std::snprintf(&s[0], width, "value-%d", k);
        s.resize(width, '.');
        ten.push_back(s);
    }

    for (size_t i = 0; i < kRows; ++i) {
        uint8_t* row = p + i * width;
        if (kind == 0) {                       // random
            for (size_t b = 0; b < width; ++b)
                row[b] = static_cast<uint8_t>(kAlpha[rng.next() % (sizeof(kAlpha) - 1)]);
        } else if (kind == 1) {                // sequential, CVE-style shared prefix
            char buf[160];
            if (width <= 16)
                std::snprintf(buf, sizeof(buf), "CV%06zu", i % 1000000);
            else
                std::snprintf(buf, sizeof(buf), "CVE-2021-%06zu-vulnerability-record", i % 1000000);
            const size_t n = std::strlen(buf);
            std::memcpy(row, buf, n < width ? n : width);
            if (n < width) std::memset(row + n, '.', width - n);
        } else {                               // highly compressible
            std::memcpy(row, ten[i % 10].data(), width);
        }
    }
    return d;
}

// ─── Codecs ─────────────────────────────────────────────────────────────────

struct Codec {
    std::string name;
    int zstd_level;   // >0 => zstd
    int lz4_accel;    // >0 => lz4 at this acceleration
    bool raw;
};

struct Result {
    std::string codec;
    size_t comp_bytes;
    double comp_ms, decomp_ms;
    bool verified;
};

int main() {
    std::vector<Dataset> datasets;
    datasets.push_back(ints("int64 random",      0));
    datasets.push_back(ints("int64 sequential",  1));
    datasets.push_back(ints("int64 1..10",       2));
    datasets.push_back(strings("str8 random",        8,   0));
    datasets.push_back(strings("str8 sequential",    8,   1));
    datasets.push_back(strings("str8 compressible",  8,   2));
    datasets.push_back(strings("str128 random",      128, 0));
    datasets.push_back(strings("str128 sequential",  128, 1));
    datasets.push_back(strings("str128 compressible",128, 2));

    // The OPERATIONAL band. zstd-19 is an archival setting, not a candidate here,
    // and it dominated the run time for a number nobody would ship (3 MB/s, 46.7s
    // for 128MB). The LZ4 acceleration axis was swept once and moved nothing —
    // ratio and both throughputs within noise at 1/4/8 across all nine datasets —
    // so it is not re-swept either.
    //
    // 4 and 7 are the live choice: rugo's zstd_level_for() currently gives
    // BYTE_ARRAY 4 (fast) / 7 (storage) and everything else 4.
    std::vector<Codec> codecs = {
        {"raw",      0,  0, true},
        {"lz4",      0,  1, false},
        {"zstd-1",   1,  0, false},
        {"zstd-3",   3,  0, false},
        {"zstd-4",   4,  0, false},
        {"zstd-7",   7,  0, false},
        {"zstd-9",   9,  0, false},
    };

    std::printf("codec matrix — %zu rows per dataset, whole-buffer (unchunked)\n\n",
                kRows);

    for (const Dataset& d : datasets) {
        const size_t n = d.bytes.size();
        std::printf("%s  (%.1f MB)\n", d.name.c_str(), n / 1e6);
        std::printf("  %-9s %9s %11s %13s %15s %12s\n",
                    "codec", "ratio", "comp MB", "comp MB/s", "decomp MB/s", "comp ms");

        std::vector<uint8_t> packed(std::max(ZSTD_compressBound(n),
                                             (size_t)LZ4_compressBound((int)n)) + 64);
        std::vector<uint8_t> out(n);

        for (const Codec& c : codecs) {
            Result r{c.name, 0, 1e30, 1e30, false};

            // FRESH contexts per codec. A reused CCtx across differing levels was
            // the first suspect for a non-monotonic ratio curve (a level-9 result
            // compressing WORSE than level-1 is not something zstd does), so the
            // variable is removed rather than reasoned about.
            ZSTD_CCtx* cctx = ZSTD_createCCtx();
            ZSTD_DCtx* dctx = ZSTD_createDCtx();

            // Repeat until at least kMinMeasureMs of work has accumulated, then
            // divide. A single pass over the 8 MB datasets finishes in ~0.1ms,
            // which is scheduling noise — measured that way, raw memcpy swung 2x
            // between runs (70640 vs 37982 MB/s) and the throughput column was
            // worthless. Heavy zstd levels already exceed the floor in one pass.
            constexpr double kMinMeasureMs = 200.0;

            // Probe pass to size the repeat count.
            int comp_reps = 1;
            {
                auto t0 = Clock::now();
                if (c.raw) std::memcpy(packed.data(), d.bytes.data(), n);
                else if (c.lz4_accel > 0)
                    LZ4_compress_fast(reinterpret_cast<const char*>(d.bytes.data()),
                                      reinterpret_cast<char*>(packed.data()),
                                      static_cast<int>(n), static_cast<int>(packed.size()),
                                      c.lz4_accel);
                else
                    ZSTD_compressCCtx(cctx, packed.data(), packed.size(),
                                      d.bytes.data(), n, c.zstd_level);
                const double one = ms_between(t0, Clock::now());
                if (one > 0.0 && one < kMinMeasureMs)
                    comp_reps = std::min(2000, (int)(kMinMeasureMs / one) + 1);
            }

            auto compress_once = [&](size_t* produced) {
                if (c.raw) {
                    std::memcpy(packed.data(), d.bytes.data(), n);
                    *produced = n;
                } else if (c.lz4_accel > 0) {
                    int got = LZ4_compress_fast(
                        reinterpret_cast<const char*>(d.bytes.data()),
                        reinterpret_cast<char*>(packed.data()),
                        static_cast<int>(n), static_cast<int>(packed.size()),
                        c.lz4_accel);
                    *produced = got > 0 ? static_cast<size_t>(got) : 0;
                } else {
                    size_t got = ZSTD_compressCCtx(cctx, packed.data(), packed.size(),
                                                   d.bytes.data(), n, c.zstd_level);
                    *produced = ZSTD_isError(got) ? 0 : got;
                }
            };

            {
                size_t produced = 0;
                auto t0 = Clock::now();
                for (int rep = 0; rep < comp_reps; ++rep) compress_once(&produced);
                r.comp_ms = ms_between(t0, Clock::now()) / comp_reps;
                r.comp_bytes = produced;
            }
            if (r.comp_bytes == 0) {
                std::printf("  %-9s  COMPRESS FAILED\n", c.name.c_str());
                ZSTD_freeCCtx(cctx); ZSTD_freeDCtx(dctx);
                continue;
            }

            auto decompress_once = [&](size_t* got) {
                if (c.raw) {
                    std::memcpy(out.data(), packed.data(), n);
                    *got = n;
                } else if (c.lz4_accel > 0) {
                    int g = LZ4_decompress_safe(
                        reinterpret_cast<const char*>(packed.data()),
                        reinterpret_cast<char*>(out.data()),
                        static_cast<int>(r.comp_bytes), static_cast<int>(n));
                    *got = g > 0 ? static_cast<size_t>(g) : 0;
                } else {
                    size_t g = ZSTD_decompressDCtx(dctx, out.data(), n,
                                                   packed.data(), r.comp_bytes);
                    *got = ZSTD_isError(g) ? 0 : g;
                }
            };

            int dec_reps = 1;
            {
                size_t got = 0;
                auto t0 = Clock::now();
                decompress_once(&got);
                const double one = ms_between(t0, Clock::now());
                if (one > 0.0 && one < kMinMeasureMs)
                    dec_reps = std::min(2000, (int)(kMinMeasureMs / one) + 1);
            }
            {
                size_t got = 0;
                auto t0 = Clock::now();
                for (int rep = 0; rep < dec_reps; ++rep) decompress_once(&got);
                r.decomp_ms = ms_between(t0, Clock::now()) / dec_reps;
                r.verified = (got == n) &&
                             (std::memcmp(out.data(), d.bytes.data(), n) == 0);
            }

            ZSTD_freeCCtx(cctx);
            ZSTD_freeDCtx(dctx);

            const double ratio  = (double)n / (double)r.comp_bytes;
            const double cmbs   = (n / 1e6) / (r.comp_ms / 1e3);
            const double dmbs   = (n / 1e6) / (r.decomp_ms / 1e3);
            std::printf("  %-9s %8.2fx %11.2f %13.0f %15.0f %12.1f%s\n",
                        c.name.c_str(), ratio, r.comp_bytes / 1e6, cmbs, dmbs, r.comp_ms,
                        r.verified ? "" : "   ** DECODE MISMATCH **");
        }
        std::printf("\n");
    }
    return 0;
}

// Build (from the repo root):
//
//   ZSTD_SRC=$(find third_party/zstd -name "*.c" | tr '\n' ' ')
//   c++ -std=c++17 -O2 -DNDEBUG -DZSTD_DISABLE_ASM -o /tmp/codec_matrix_bench \
//     dev/codec_matrix_bench.cpp third_party/lz4/lz4.c ${ZSTD_SRC} \
//     -I third_party/lz4 -I third_party/zstd -I third_party/zstd/common
//
//   /tmp/codec_matrix_bench
//
// Dev tooling only — never imported by production code (repo rules §5).
