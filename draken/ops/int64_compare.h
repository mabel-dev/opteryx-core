#pragma once
// draken/ops/int64_compare.h — compare_scalar and compare_vector for int64 (Milestone C.3).
//
// Result is always a bit-packed DRAKEN_BOOL VecResult (1 bit/value, LSB-first within
// each byte). The result is always dense-identity (selection == identity permutation).
//
// NULL SEMANTICS (three-valued logic, SQL-correct):
//   compare_scalar: null input row → null output row (validity bit 0, result bit 0).
//     Output validity is a copy of the input validity; nullptr when input has no nulls.
//   compare_vector: output row is null if EITHER operand row is null.
//     Output validity is the AND of both input validities; nullptr when both are non-null.
//
//   compare_scalar copies validity to out_null and uses branchless AND. For null
//   rows the data is result=0 and the bitmap is validity=0.
//
// OP CODES:
//   0=eq  1=ne  2=gt  3=ge  4=lt  5=le
//
// ACCESS PATTERN: data[v.selection[i]] for i in [0, v.length).
// No shape discrimination — uniform access only (CLAUDE.md §11 contract).
//
// SIMD NOTE: the 8-compare byte-pack technique eliminates RAW dependencies on the
// output byte, giving the compiler a clean dependency graph for auto-vectorisation
// to NEON (ARM dev) and AVX2 (x86 prod). The 8-way unrolled inner loop is the
// intended vectorisation grain. Full hand-written intrinsics are a future refinement.
//
// BIT-BOUNDARY CORRECTNESS: alloc_bool_buf() zero-initialises the entire padded buffer.
// Partial tail bytes are written via scalar OR — they start at 0 and accumulate only
// actual row bits. SIMD padding bytes beyond ceil(n/8) remain 0. No read-past-end.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Compare operator structs — compile-time tags, no runtime dispatch in the loop.
// ---------------------------------------------------------------------------
struct CmpEq { static inline bool apply(int64_t a, int64_t b) noexcept { return a == b; } };
struct CmpNe { static inline bool apply(int64_t a, int64_t b) noexcept { return a != b; } };
struct CmpGt { static inline bool apply(int64_t a, int64_t b) noexcept { return a >  b; } };
struct CmpGe { static inline bool apply(int64_t a, int64_t b) noexcept { return a >= b; } };
struct CmpLt { static inline bool apply(int64_t a, int64_t b) noexcept { return a <  b; } };
struct CmpLe { static inline bool apply(int64_t a, int64_t b) noexcept { return a <= b; } };

// ---------------------------------------------------------------------------
// Internal allocation helpers — prefixed cmp_ to avoid ODR clash with
// identically-named helpers in int64_arithmetic.h (both headers land in the
// same TU via hash.h).
// ---------------------------------------------------------------------------

// Allocate a zero-initialised bit buffer for n rows, SIMD-padded to a multiple
// of 8 bytes (minimum 8 bytes even for n==0 so the pointer is always non-NULL
// and SIMD loads past the end are safe within the allocation).
static inline uint8_t* cmp_alloc_bool_buf(uint32_t n) {
    const uint32_t raw    = (n + 7u) >> 3;
    const uint32_t padded = (raw + 7u) & ~7u;
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    memset(p, 0, bytes);
    return p;
}

// Copy a validity bitmap for n rows from a non-null source. Masks the partial
// last byte so padding bits in the source are not propagated.
static inline uint8_t* cmp_copy_validity(const uint8_t* src, uint32_t n) {
    uint8_t* dst = cmp_alloc_bool_buf(n);   // zero-initialised
    const uint32_t nb = (n + 7u) >> 3;
    if (nb > 0) {
        memcpy(dst, src, nb);
        if ((n & 7u) != 0)
            dst[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }
    return dst;
}

// Compute the bitwise AND of two validity bitmaps (output row is valid iff both
// inputs are valid). Returns nullptr when both inputs are nullptr (no nulls).
// Normalises: if the AND is all-valid, frees and returns nullptr.
//
// SIMD: AND full bytes in wide SIMD registers; track min-byte to detect any
// byte < 0xFF (non-all-valid) without a per-byte branch.  The partial last
// byte (when n is not a multiple of 8) is handled scalar afterward.
static inline uint8_t* cmp_and_validity(
    const uint8_t* va, const uint8_t* vb, uint32_t n)
{
    if (va == nullptr && vb == nullptr) return nullptr;
    if (va == nullptr) return cmp_copy_validity(vb, n);
    if (vb == nullptr) return cmp_copy_validity(va, n);

    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* dst = cmp_alloc_bool_buf(n);

    // Separate full bytes (all expected to be 0xFF) from the partial tail byte.
    const uint32_t nb_full = (n & 7u) ? nb - 1u : nb;
    bool all_valid = true;
    uint32_t k = 0;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    {
        // Process 16 bytes per iteration; accumulate min to detect any < 0xFF.
        uint8x16_t min_acc = vdupq_n_u8(0xFFu);
        for (; k + 16u <= nb_full; k += 16u) {
            uint8x16_t r = vandq_u8(vld1q_u8(va + k), vld1q_u8(vb + k));
            vst1q_u8(dst + k, r);
            min_acc = vminq_u8(min_acc, r);
        }
        if (vminvq_u8(min_acc) < 0xFFu) all_valid = false;
    }

#elif defined(__AVX2__)
    {
        // Process 32 bytes per iteration; OR together any non-0xFF bytes to
        // detect invalidity: if OR(~r, 0) != 0 then some byte < 0xFF.
        __m256i not_acc = _mm256_setzero_si256();
        const __m256i ones = _mm256_set1_epi8(-1);
        for (; k + 32u <= nb_full; k += 32u) {
            __m256i r = _mm256_and_si256(
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(va + k)),
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(vb + k)));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + k), r);
            // Accumulate NOT(r): any non-zero bit after this loop means some byte < 0xFF
            not_acc = _mm256_or_si256(not_acc, _mm256_xor_si256(r, ones));
        }
        if (!_mm256_testz_si256(not_acc, not_acc)) all_valid = false;
    }

#elif defined(__riscv) && defined(__riscv_vector)
    {
        // Use vcpop on ~result to count non-0xFF bytes
        bool simd_all_valid = true;
        size_t sk = 0;
        while (sk < nb_full) {
            size_t vl = __riscv_vsetvl_e8m8(nb_full - sk);
            vuint8m8_t r = __riscv_vand_vv_u8m8(
                __riscv_vle8_v_u8m8(va + sk, vl),
                __riscv_vle8_v_u8m8(vb + sk, vl), vl);
            __riscv_vse8_v_u8m8(dst + sk, r, vl);
            // Any byte != 0xFF means not-all-valid
            vbool1_t bad = __riscv_vmsne_vx_u8m8_b1(r, (uint8_t)0xFFu, vl);
            if (__riscv_vcpop_m_b1(bad, vl) > 0) simd_all_valid = false;
            sk += vl;
        }
        k = (uint32_t)nb_full;  // skip the scalar loop below for full bytes
        if (!simd_all_valid) all_valid = false;
    }
#endif

    // Scalar tail: remaining full bytes the SIMD loop didn't reach
    for (; k < nb_full; ++k) {
        dst[k] = static_cast<uint8_t>(va[k] & vb[k]);
        if (dst[k] != 0xFFu) all_valid = false;
    }

    // Partial last byte (when n % 8 != 0): expected value is not 0xFF
    if (n & 7u) {
        const uint8_t exp = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        dst[nb - 1u] = static_cast<uint8_t>(va[nb - 1u] & vb[nb - 1u]);
        if (dst[nb - 1u] != exp) all_valid = false;
    }

    if (all_valid) {
        draken_free(dst);
        return nullptr;
    }
    return dst;
}

// ---------------------------------------------------------------------------
// cmp_constant_bool_result — architect-approved constant fast-path helper.
//
// Builds a dense DRAKEN_BOOL VecResult from a single comparison bit. The input
// vector has data_length==1, so one comparison replaces n; output fill is O(n/8).
//
// bit=true, no nulls : memset 0xFF (partial last byte masked).
// bit=true, nulls    : memcpy validity → data  (valid=1, null=0).
// bit=false          : dst is pre-zeroed by alloc — nothing extra needed.
// ---------------------------------------------------------------------------
static inline VecResult cmp_constant_bool_result(
    bool bit, const uint8_t* src_null, uint32_t n)
{
    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    if (bit) {
        const uint32_t nb = (n + 7u) >> 3;
        if (src_null == nullptr) {
            memset(dst, 0xFFu, nb);
            if (n & 7u) dst[nb - 1u] = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        } else {
            memcpy(dst, src_null, nb);
            if (n & 7u) dst[nb - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        }
    }
    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// cmp_dict_scatter — architect-approved dict fast-path scatter step.
//
// Maps each logical row i to dict_bytes[selection[i]] and packs into dst.
// dict_bytes has data_length entries: 1 = comparison true, 0 = false.
// dst must be pre-zeroed (cmp_alloc_bool_buf guarantees this).
// Used by cmp_dict_bool_result and str_dict_bool_result.
// ---------------------------------------------------------------------------
static inline void cmp_dict_scatter(
    const uint8_t*  dict_bytes,
    const uint32_t* selection,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (dict_bytes[selection[base+0]] << 0) |
                (dict_bytes[selection[base+1]] << 1) |
                (dict_bytes[selection[base+2]] << 2) |
                (dict_bytes[selection[base+3]] << 3) |
                (dict_bytes[selection[base+4]] << 4) |
                (dict_bytes[selection[base+5]] << 5) |
                (dict_bytes[selection[base+6]] << 6) |
                (dict_bytes[selection[base+7]] << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (dict_bytes[selection[i]])
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (dict_bytes[selection[base+0]] << 0) |
                (dict_bytes[selection[base+1]] << 1) |
                (dict_bytes[selection[base+2]] << 2) |
                (dict_bytes[selection[base+3]] << 3) |
                (dict_bytes[selection[base+4]] << 4) |
                (dict_bytes[selection[base+5]] << 5) |
                (dict_bytes[selection[base+6]] << 6) |
                (dict_bytes[selection[base+7]] << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((src_null[i >> 3] >> (i & 7)) & 1u)
                if (dict_bytes[selection[i]])
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

// cmp_dict_cross_scatter — both-dict vector scatter.
//
// cross[j * dl_b + k] = comparison result for a_data[j] OP b_data[k].
// For each logical row i: output bit = cross[a_sel[i]*dl_b + b_sel[i]].
static inline void cmp_dict_cross_scatter(
    const uint8_t*  cross,
    uint32_t        dl_b,
    const uint32_t* a_sel,
    const uint32_t* b_sel,
    const uint8_t*  comb_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (comb_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (cross[a_sel[base+0] * dl_b + b_sel[base+0]] << 0) |
                (cross[a_sel[base+1] * dl_b + b_sel[base+1]] << 1) |
                (cross[a_sel[base+2] * dl_b + b_sel[base+2]] << 2) |
                (cross[a_sel[base+3] * dl_b + b_sel[base+3]] << 3) |
                (cross[a_sel[base+4] * dl_b + b_sel[base+4]] << 4) |
                (cross[a_sel[base+5] * dl_b + b_sel[base+5]] << 5) |
                (cross[a_sel[base+6] * dl_b + b_sel[base+6]] << 6) |
                (cross[a_sel[base+7] * dl_b + b_sel[base+7]] << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (cross[a_sel[i] * dl_b + b_sel[i]])
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (cross[a_sel[base+0] * dl_b + b_sel[base+0]] << 0) |
                (cross[a_sel[base+1] * dl_b + b_sel[base+1]] << 1) |
                (cross[a_sel[base+2] * dl_b + b_sel[base+2]] << 2) |
                (cross[a_sel[base+3] * dl_b + b_sel[base+3]] << 3) |
                (cross[a_sel[base+4] * dl_b + b_sel[base+4]] << 4) |
                (cross[a_sel[base+5] * dl_b + b_sel[base+5]] << 5) |
                (cross[a_sel[base+6] * dl_b + b_sel[base+6]] << 6) |
                (cross[a_sel[base+7] * dl_b + b_sel[base+7]] << 7));
            dst[b] = static_cast<uint8_t>(m & comb_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((comb_null[i >> 3] >> (i & 7)) & 1u)
                if (cross[a_sel[i] * dl_b + b_sel[i]])
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

// Builds a dense DRAKEN_BOOL result from pre-computed dict_bytes + the source
// vector's selection, validity, and length.
static inline VecResult cmp_dict_bool_result(
    const uint8_t* dict_bytes, const DrakenVector& v)
{
    const uint32_t n        = v.length;
    const uint8_t* src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    cmp_dict_scatter(dict_bytes, v.selection, src_null, dst, n);

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// compare_scalar inner kernel (template on Op)
//
// Reads data[selection[i]] for each logical row i and compares against scalar.
// Packs 8 results into one byte (no RAW dependency on the output buffer →
// the compiler can vectorise the 8-way unroll with NEON/AVX2).
//
// Two paths selected at call time (no per-row branch):
//   src_null == nullptr: non-null — pure comparison, no validity gating.
//   src_null != nullptr: branchless AND — result byte & validity byte.
//
// dst must be pre-zeroed (alloc_bool_buf guarantees this).
// Tail (n % 8 != 0) is handled scalar; partial byte starts zeroed, only true
// bits are OR'd in — bit-boundary correctness is guaranteed.
// ---------------------------------------------------------------------------
// Identity == true: selection is the identity permutation, so index data[pos]
// directly — contiguous loads the compiler can auto-vectorise (NEON/AVX2 cannot
// vectorise the gather form). Identity == false: gather data[selection[pos]].
// Same answer either way (CLAUDE.md §11 hint-based dispatch). The caller selects
// the Identity specialisation from the input's DRAKEN_SEL_IDENTITY flag.
template<typename Op, bool Identity>
static inline void cmp_scalar_kernel(
    const int64_t*  data,
    const uint32_t* selection,
    int64_t         scalar,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    auto at = [&](uint32_t pos) -> int64_t {
        if constexpr (Identity) return data[pos];
        else                    return data[selection[pos]];
    };

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(at(base+0), scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(at(base+1), scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(at(base+2), scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(at(base+3), scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(at(base+4), scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(at(base+5), scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(at(base+6), scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(at(base+7), scalar)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (Op::apply(at(i), scalar))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        // Branchless: AND packed result with validity byte — null rows → bit 0.
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(at(base+0), scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(at(base+1), scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(at(base+2), scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(at(base+3), scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(at(base+4), scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(at(base+5), scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(at(base+6), scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(at(base+7), scalar)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                if (Op::apply(at(i), scalar))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

template<typename Op>
static inline VecResult compare_scalar_impl(const DrakenVector& v, int64_t scalar) {
    const uint32_t  n        = v.length;
    const int64_t*  data     = static_cast<const int64_t*>(v.data);
    const uint8_t*  src_null = v.validity;

    if (draken_is_constant(&v))
        return cmp_constant_bool_result(Op::apply(data[0], scalar), src_null, n);

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k)
            db[k] = Op::apply(data[k], scalar) ? 1u : 0u;
        VecResult r;
        try { r = cmp_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* dst = cmp_alloc_bool_buf(n);

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try {
            out_null = cmp_copy_validity(src_null, n);
        } catch (...) {
            draken_free(dst);
            throw;
        }
    }

    // Identity-gated: contiguous direct-index path when selection is identity
    // (auto-vectorisable), else the gather path. Same answer (hint-based, §11).
    if (v.flags & DRAKEN_SEL_IDENTITY)
        cmp_scalar_kernel<Op, true>(data, v.selection, scalar, src_null, dst, n);
    else
        cmp_scalar_kernel<Op, false>(data, v.selection, scalar, src_null, dst, n);

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult i64_compare_scalar(const DrakenVector& v, int64_t scalar, int op) {
    switch (op) {
        case 0: return compare_scalar_impl<CmpEq>(v, scalar);
        case 1: return compare_scalar_impl<CmpNe>(v, scalar);
        case 2: return compare_scalar_impl<CmpGt>(v, scalar);
        case 3: return compare_scalar_impl<CmpGe>(v, scalar);
        case 4: return compare_scalar_impl<CmpLt>(v, scalar);
        default: return compare_scalar_impl<CmpLe>(v, scalar);
    }
}

// ---------------------------------------------------------------------------
// compare_vector inner kernel (template on Op)
//
// Reads a[a.selection[i]] OP b[b.selection[i]] for each logical row i.
// comb_null is the pre-computed AND of both validities (nullptr when both
// inputs are non-null). Same 8-way byte-pack technique as scalar kernel.
// ---------------------------------------------------------------------------
template<typename Op>
static inline void cmp_vector_kernel(
    const int64_t*  a_data, const uint32_t* a_sel,
    const int64_t*  b_data, const uint32_t* b_sel,
    const uint8_t*  comb_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;

    if (comb_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+0]], b_data[b_sel[base+0]])) << 0) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+1]], b_data[b_sel[base+1]])) << 1) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+2]], b_data[b_sel[base+2]])) << 2) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+3]], b_data[b_sel[base+3]])) << 3) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+4]], b_data[b_sel[base+4]])) << 4) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+5]], b_data[b_sel[base+5]])) << 5) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+6]], b_data[b_sel[base+6]])) << 6) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+7]], b_data[b_sel[base+7]])) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (Op::apply(a_data[a_sel[i]], b_data[b_sel[i]]))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+0]], b_data[b_sel[base+0]])) << 0) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+1]], b_data[b_sel[base+1]])) << 1) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+2]], b_data[b_sel[base+2]])) << 2) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+3]], b_data[b_sel[base+3]])) << 3) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+4]], b_data[b_sel[base+4]])) << 4) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+5]], b_data[b_sel[base+5]])) << 5) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+6]], b_data[b_sel[base+6]])) << 6) |
                (static_cast<unsigned>(Op::apply(a_data[a_sel[base+7]], b_data[b_sel[base+7]])) << 7));
            dst[b] = static_cast<uint8_t>(m & comb_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((comb_null[i >> 3] >> (i & 7)) & 1u) {
                if (Op::apply(a_data[a_sel[i]], b_data[b_sel[i]]))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

template<typename Op>
static inline VecResult compare_vector_impl(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("compare_vector: operand lengths must match");

    const int64_t* a_data = static_cast<const int64_t*>(a.data);
    const int64_t* b_data = static_cast<const int64_t*>(b.data);

    if (draken_is_constant(&a) && draken_is_constant(&b)) {
        uint8_t* comb = cmp_and_validity(a.validity, b.validity, n);
        VecResult r;
        try { r = cmp_constant_bool_result(Op::apply(a_data[0], b_data[0]), comb, n); }
        catch (...) { if (comb) draken_free(comb); throw; }
        if (comb) draken_free(comb);
        return r;
    }

    if (draken_is_dict(&a) && draken_is_dict(&b) &&
        (uint64_t)a.data_length * b.data_length <= (uint64_t)n) {
        const uint32_t dl_a = a.data_length;
        const uint32_t dl_b = b.data_length;
        uint8_t* cross = static_cast<uint8_t*>(draken_malloc(dl_a * dl_b));
        if (!cross) throw std::bad_alloc();
        for (uint32_t j = 0; j < dl_a; ++j)
            for (uint32_t k = 0; k < dl_b; ++k)
                cross[j * dl_b + k] = Op::apply(a_data[j], b_data[k]) ? 1u : 0u;
        uint8_t* comb = nullptr;
        uint8_t* dst = nullptr;
        try {
            comb = cmp_and_validity(a.validity, b.validity, n);
            dst  = cmp_alloc_bool_buf(n);
        } catch (...) {
            draken_free(cross);
            if (comb) draken_free(comb);
            if (dst)  draken_free(dst);
            throw;
        }
        cmp_dict_cross_scatter(cross, dl_b, a.selection, b.selection, comb, dst, n);
        draken_free(cross);
        VecResult r;
        r.data = dst; r.validity = comb;
        r.selection = draken_identity_sel(n); r.owns_selection = false;
        r.data_length = n; r.length = n;
        r.type = DRAKEN_BOOL;
        r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return r;
    }

    // Combined validity (AND of both — nullptr if no nulls on either side).
    uint8_t* out_null = cmp_and_validity(a.validity, b.validity, n);

    uint8_t* dst = nullptr;
    try {
        dst = cmp_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }

    cmp_vector_kernel<Op>(
        a_data, a.selection, b_data, b.selection,
        out_null, dst, n);

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

static inline VecResult i64_compare_vector(const DrakenVector& a, const DrakenVector& b, int op) {
    switch (op) {
        case 0: return compare_vector_impl<CmpEq>(a, b);
        case 1: return compare_vector_impl<CmpNe>(a, b);
        case 2: return compare_vector_impl<CmpGt>(a, b);
        case 3: return compare_vector_impl<CmpGe>(a, b);
        case 4: return compare_vector_impl<CmpLt>(a, b);
        default: return compare_vector_impl<CmpLe>(a, b);
    }
}

}} // namespace draken::ops
