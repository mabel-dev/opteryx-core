// bitpack_ab_bench — A/B the production Parquet bit-unpacker against an
// Impala-style compile-time-specialized unpacker.
//
// WHAT IS BEING TESTED
//   Impala's be/src/util/bit-packing.inline.h has NO SIMD. Its bet is that a
//   fully unrolled, batch-of-32 routine TEMPLATED on the bit width — every byte
//   offset, shift and mask a compile-time constant, one unaligned 64-bit load
//   per value — beats a runtime-parameterised SIMD kernel. Our production path
//   (rugo/src/parquet/decode_encodings.cpp) is the opposite: groups of 8, bit
//   width as a RUNTIME argument, NEON/AVX2 variable-shift vectors, and the
//   per-lane byte offsets rebuilt into a stack array on every group.
//
//   This bench decides that empirically, on our data shapes, on this machine.
//
// ARMS (all produce identical output; verified before timing)
//   A  prod      — get_unpack_fn(), i.e. exactly what DecodeRLEBitPackedIndices
//                  calls. NEON on Apple Silicon, AVX2 on x86.
//   B  impala    — Impala-style: template<int BW>, 32 values fully unrolled,
//                  scalar 64-bit loads, padded-scratch tail. No SIMD.
//   C  tmpl-neon — OUR idea informed by theirs: keep NEON, but template on the
//                  bit width so the lane offsets and shift vector are
//                  compile-time constants instead of a per-group stack array.
//                  (NEON targets only; falls back to B elsewhere.)
//
// METHOD
//   Arms run INTERLEAVED within each round (A,B,C,A,B,C,...) so a thermal ramp
//   hits all three equally, and the MEDIAN across rounds is reported, not the
//   mean — see the thermal-drift and arm-order traps. One warmup round is
//   discarded. Output is checksummed so nothing is optimised away, and every
//   arm's full output buffer is compared against A's before any timing runs.
//
// BUILD (matches build_common.py CPP_FLAGS for this platform exactly: -O3
// -std=c++20, plus -march=haswell -mtune=generic on x86):
//   clang++ -O3 -std=c++20 -I rugo/src/parquet -I draken/simd -I src/cpp \
//       dev/bitpack_ab_bench.cpp draken/simd/cpu_features.cpp \
//       draken/simd/simd_env.cpp -o /tmp/bitpack_ab_bench
//
// Dev tooling only — never imported by production code (repo rules §5).

// The production unpackers are `static` in decode_encodings.cpp, so they are
// not linkable. Including the .cpp pulls them into this TU, which is the only
// way to measure the REAL code rather than a copy of it that could drift.
#include "decode_encodings.cpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// ---------------------------------------------------------------------------
// Arm B — Impala-style compile-time specialisation
// ---------------------------------------------------------------------------
//
// One value, all offsets constant-folded. A value spans at most
// (7 + 32) = 39 bits, so a single unaligned 64-bit load starting at the value's
// first byte always covers it. That load reads up to 8 bytes past the value,
// which is why whole batches require the caller to guarantee slack and the tail
// goes through a padded scratch buffer (UnpackTail below) — Impala's rule.
template <int BW, int IDX>
static inline int32_t impala_unpack_value(const uint8_t* __restrict__ in) {
    constexpr int  FIRST_BIT = IDX * BW;
    constexpr int  BYTE_OFF  = FIRST_BIT / 8;
    constexpr int  SHIFT     = FIRST_BIT % 8;
    constexpr uint64_t MASK  = (BW == 64) ? ~0ULL : ((1ULL << BW) - 1ULL);
    uint64_t w;
    std::memcpy(&w, in + BYTE_OFF, sizeof(w));
    return (int32_t)((w >> SHIFT) & MASK);
}

template <int BW, int... IDX>
static inline void impala_unpack32_impl(const uint8_t* __restrict__ in,
                                        int32_t* __restrict__ out,
                                        std::integer_sequence<int, IDX...>) {
    ((out[IDX] = impala_unpack_value<BW, IDX>(in)), ...);
}

// 32 values, fully unrolled by pack expansion (Impala uses Boost.PP for this;
// a C++17 fold does the same job with no dependency — see §4).
template <int BW>
static inline void impala_unpack32(const uint8_t* __restrict__ in,
                                   int32_t* __restrict__ out) {
    impala_unpack32_impl<BW>(in, out, std::make_integer_sequence<int, 32>{});
}

// Tail: < 32 values. Copy the (few) live bytes into a zero-padded stack buffer
// so the overreading fast path stays in bounds, then unpack 32 and keep n.
// This is Impala's UnpackUpTo31Values, and it is what removes the "callers must
// route the last group through the safe variant" invariant our code carries.
template <int BW>
static inline void impala_unpack_tail(const uint8_t* __restrict__ in,
                                      int32_t* __restrict__ out, int n) {
    constexpr int FULL_BYTES = (32 * BW + 7) / 8;
    alignas(8) uint8_t scratch[FULL_BYTES + 8] = {};
    const int live = (n * BW + 7) / 8;
    std::memcpy(scratch, in, (size_t)live);
    int32_t tmp[32];
    impala_unpack32<BW>(scratch, tmp);
    std::memcpy(out, tmp, (size_t)n * sizeof(int32_t));
}

template <int BW>
static void impala_unpack_values(const uint8_t* __restrict__ in,
                                 int32_t* __restrict__ out, int64_t n) {
    constexpr int BYTES_PER_32 = (32 * BW) / 8 + ((32 * BW) % 8 != 0);
    int64_t i = 0;
    // All but the final batch: direct loads. The final batch also goes through
    // the padded path, so no read ever passes the end of `in`.
    for (; i + 32 <= n - 32; i += 32)
        impala_unpack32<BW>(in + (i * BW) / 8, out + i);
    for (; i < n; i += 32) {
        const int take = (int)std::min<int64_t>(32, n - i);
        impala_unpack_tail<BW>(in + (i * BW) / 8, out + i, take);
    }
    (void)sizeof(char[BYTES_PER_32 > 0 ? 1 : 1]);
}

// ---------------------------------------------------------------------------
// Arm C — NEON, but templated on the bit width
// ---------------------------------------------------------------------------
//
// Same vector op as production, one difference: BW is a compile-time constant,
// so the eight byte offsets and the shift vector fold to constants instead of
// being rebuilt in a stack array (`uint32_t w[8]; int32_t sh[8];`) per group.
// If production's NEON path is limited by that scalar gather rather than by the
// vector work, this is where it shows up.
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
template <int BW, int... IDX>
static inline uint32x4_t tneon_load4(const uint8_t* __restrict__ in, int base,
                                     std::integer_sequence<int, IDX...>) {
    uint32_t w[4];
    ((w[IDX] = [&] {
        uint32_t v;
        std::memcpy(&v, in + (((base + IDX) * BW) >> 3), sizeof(v));
        return v;
    }()), ...);
    return vld1q_u32(w);
}

template <int BW>
static inline void tneon_unpack8(const uint8_t* __restrict__ in,
                                 int32_t* __restrict__ out) {
    // Compile-time shift vectors (negative == right shift for vshlq_u32).
    const int32x4_t sh_lo = {-(int32_t)((0 * BW) & 7), -(int32_t)((1 * BW) & 7),
                             -(int32_t)((2 * BW) & 7), -(int32_t)((3 * BW) & 7)};
    const int32x4_t sh_hi = {-(int32_t)((4 * BW) & 7), -(int32_t)((5 * BW) & 7),
                             -(int32_t)((6 * BW) & 7), -(int32_t)((7 * BW) & 7)};
    const uint32x4_t mask = vdupq_n_u32(BW == 32 ? 0xFFFFFFFFu
                                                 : ((1u << BW) - 1u));
    auto seq = std::make_integer_sequence<int, 4>{};
    vst1q_s32(out, vreinterpretq_s32_u32(vandq_u32(
        vshlq_u32(tneon_load4<BW>(in, 0, seq), sh_lo), mask)));
    vst1q_s32(out + 4, vreinterpretq_s32_u32(vandq_u32(
        vshlq_u32(tneon_load4<BW>(in, 4, seq), sh_hi), mask)));
}

// BW > 16 needs more than 32 bits of window per lane (7 + 32 = 39), so the
// 32-bit-lane trick does not apply; those widths use the scalar template.
template <int BW>
static void tneon_unpack_values(const uint8_t* __restrict__ in,
                                int32_t* __restrict__ out, int64_t n) {
    if constexpr (BW > 16) {
        impala_unpack_values<BW>(in, out, n);
    } else {
        constexpr int BPG = BW; // bytes per group of 8, BW <= 8 ... see below
        (void)BPG;
        constexpr int BYTES_PER_GROUP = (8 * BW) / 8;
        int64_t i = 0;
        // Leave the last two groups to the padded scalar path: tneon_unpack8
        // overreads by up to 4 bytes per lane, exactly like production's
        // unpack_group_8_neon_wide.
        for (; i + 16 <= n; i += 8)
            tneon_unpack8<BW>(in + (i / 8) * BYTES_PER_GROUP, out + i);
        for (; i < n; i += 32) {
            const int take = (int)std::min<int64_t>(32, n - i);
            impala_unpack_tail<BW>(in + (i * BW) / 8, out + i, take);
        }
    }
}
#endif

// ---------------------------------------------------------------------------
// Dispatch: switch on the runtime bit width into the specialised template.
// This is the cost Impala pays once per call; production pays a function
// pointer once per RUN instead.
// ---------------------------------------------------------------------------

template <template <int> class FN>
static void dispatch_bw(int bw, const uint8_t* in, int32_t* out, int64_t n) {
    switch (bw) {
#define CASE(N) case N: FN<N>::run(in, out, n); return;
        CASE(1)  CASE(2)  CASE(3)  CASE(4)  CASE(5)  CASE(6)  CASE(7)  CASE(8)
        CASE(9)  CASE(10) CASE(11) CASE(12) CASE(13) CASE(14) CASE(15) CASE(16)
        CASE(17) CASE(18) CASE(19) CASE(20) CASE(21) CASE(22) CASE(23) CASE(24)
        CASE(25) CASE(26) CASE(27) CASE(28) CASE(29) CASE(30) CASE(31) CASE(32)
#undef CASE
        default: std::fprintf(stderr, "bad bit width %d\n", bw); std::abort();
    }
}

template <int BW> struct ImpalaFn {
    static void run(const uint8_t* in, int32_t* out, int64_t n) {
        impala_unpack_values<BW>(in, out, n);
    }
};

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
template <int BW> struct TneonFn {
    static void run(const uint8_t* in, int32_t* out, int64_t n) {
        tneon_unpack_values<BW>(in, out, n);
    }
};
#endif

// ---------------------------------------------------------------------------
// Arm A — production, called the way DecodeRLEBitPackedIndices calls it
// ---------------------------------------------------------------------------

static void prod_unpack_values(const uint8_t* in, int32_t* out, int64_t n,
                               int bw) {
    const int bpg = (bw <= 8) ? bw : (8 * bw + 7) / 8;
    const int64_t full_groups = n / 8;
    const int64_t remainder   = n - full_groups * 8;
    if (full_groups > 0)
        get_unpack_fn()(in, out, (int)full_groups, bw);
    if (remainder > 0) {
        int32_t tmp[8] = {};
        unpack_group_8_scalar(in + full_groups * bpg, tmp, bw);
        for (int i = 0; i < remainder; i++)
            out[full_groups * 8 + i] = tmp[i];
    }
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// LSB-first bit packing, the Parquet RLE/bit-packed hybrid layout.
static std::vector<uint8_t> pack(const std::vector<int32_t>& vals, int bw) {
    std::vector<uint8_t> out;
    uint64_t acc = 0;
    int nbits = 0;
    const uint32_t mask = (bw == 32) ? 0xFFFFFFFFu : ((1u << bw) - 1u);
    for (int32_t v : vals) {
        acc |= (uint64_t)((uint32_t)v & mask) << nbits;
        nbits += bw;
        while (nbits >= 8) {
            out.push_back((uint8_t)(acc & 0xFF));
            acc >>= 8;
            nbits -= 8;
        }
    }
    if (nbits > 0) out.push_back((uint8_t)(acc & 0xFF));
    out.resize(out.size() + 16, 0);   // slack for the overreading fast paths
    return out;
}

struct Case {
    int bw;
    std::vector<uint8_t> packed;
    std::vector<int32_t> expect;
};

static Case make_case(int bw, int64_t n, std::mt19937& rng) {
    Case c;
    c.bw = bw;
    c.expect.resize(n);
    const uint32_t hi = (bw == 32) ? 0xFFFFFFFFu : ((1u << bw) - 1u);
    std::uniform_int_distribution<uint32_t> d(0, hi);
    for (int64_t i = 0; i < n; i++) c.expect[i] = (int32_t)d(rng);
    c.packed = pack(c.expect, bw);
    return c;
}

static uint64_t checksum(const int32_t* p, int64_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (int64_t i = 0; i < n; i++) { h ^= (uint32_t)p[i]; h *= 1099511628211ULL; }
    return h;
}

struct ArmResult { std::string name; std::vector<double> ms; };

static double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    const size_t m = v.size() / 2;
    return (v.size() % 2) ? v[m] : 0.5 * (v[m - 1] + v[m]);
}

} // namespace

int main(int argc, char** argv) {
    const int64_t N      = (argc > 1) ? std::atoll(argv[1]) : (1 << 20);
    const int     ROUNDS = (argc > 2) ? std::atoi(argv[2]) : 9;

    std::printf("bitpack_ab_bench  n=%lld values/width  rounds=%d (median reported)\n",
                (long long)N, ROUNDS);
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    std::printf("arm A = production NEON path\n");
#elif defined(__AVX2__)
    std::printf("arm A = production AVX2 path\n");
#else
    std::printf("arm A = production scalar path\n");
#endif

    std::mt19937 rng(12345);
    std::vector<int32_t> out_a(N), out_b(N), out_c(N);

    std::printf("\n%4s %12s %12s %12s %10s %10s\n",
                "bw", "A prod ms", "B impala ms", "C tmpl-neon", "B/A", "C/A");

    double sum_ba = 0, sum_ca = 0;
    int counted = 0;

    for (int bw = 1; bw <= 32; bw++) {
        Case c = make_case(bw, N, rng);

        // ---- correctness gate: every arm must match the packed input exactly,
        // ---- before any timing. A fast wrong unpacker is worth nothing.
        prod_unpack_values(c.packed.data(), out_a.data(), N, bw);
        dispatch_bw<ImpalaFn>(bw, c.packed.data(), out_b.data(), N);
        bool ok_a = (std::memcmp(out_a.data(), c.expect.data(), N * 4) == 0);
        bool ok_b = (std::memcmp(out_b.data(), c.expect.data(), N * 4) == 0);
        bool ok_c = true;
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
        dispatch_bw<TneonFn>(bw, c.packed.data(), out_c.data(), N);
        ok_c = (std::memcmp(out_c.data(), c.expect.data(), N * 4) == 0);
#endif
        if (!ok_a || !ok_b || !ok_c) {
            std::printf("%4d  MISMATCH  A=%s B=%s C=%s  — arm excluded\n", bw,
                        ok_a ? "ok" : "BAD", ok_b ? "ok" : "BAD", ok_c ? "ok" : "BAD");
            if (!ok_a) { std::fprintf(stderr, "production path disagrees with the "
                                              "packer at bw=%d; bench invalid\n", bw); return 1; }
            continue;
        }

        std::vector<double> ta, tb, tc;
        uint64_t sink = 0;
        for (int r = 0; r < ROUNDS + 1; r++) {   // r == 0 is warmup, discarded
            Clock::time_point t0;

            t0 = Clock::now();
            prod_unpack_values(c.packed.data(), out_a.data(), N, bw);
            double a = ms_since(t0);
            sink ^= checksum(out_a.data(), N);

            t0 = Clock::now();
            dispatch_bw<ImpalaFn>(bw, c.packed.data(), out_b.data(), N);
            double b = ms_since(t0);
            sink ^= checksum(out_b.data(), N);

            double cc = 0;
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            t0 = Clock::now();
            dispatch_bw<TneonFn>(bw, c.packed.data(), out_c.data(), N);
            cc = ms_since(t0);
            sink ^= checksum(out_c.data(), N);
#endif
            if (r > 0) { ta.push_back(a); tb.push_back(b); tc.push_back(cc); }
        }
        if (sink == 0x1234) std::printf(" ");   // keep the checksums live

        const double ma = median(ta), mb = median(tb), mc = median(tc);
        std::printf("%4d %12.3f %12.3f %12.3f %10.2fx %10.2fx\n",
                    bw, ma, mb, mc, mb / ma, mc > 0 ? mc / ma : 0.0);
        sum_ba += mb / ma;
        if (mc > 0) sum_ca += mc / ma;
        counted++;
    }

    if (counted) {
        std::printf("\nmean ratio over %d widths:  B/A %.2fx   C/A %.2fx"
                    "   (<1.00 = faster than production)\n",
                    counted, sum_ba / counted, sum_ca / counted);
    }
    return 0;
}
