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
//   draken_old's _compare_scalar copies validity to out_null and uses branchless AND —
//   our implementation matches this for correct rows. For null rows, both produce
//   result=0 in the data and validity=0 in the bitmap. No known divergence from
//   draken_old's null behavior (the existing parity harness verified this).
//
// OP CODES (matching draken_old convention):
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
static inline uint8_t* cmp_and_validity(
    const uint8_t* va, const uint8_t* vb, uint32_t n)
{
    if (va == nullptr && vb == nullptr) return nullptr;
    if (va == nullptr) return cmp_copy_validity(vb, n);
    if (vb == nullptr) return cmp_copy_validity(va, n);

    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* dst = cmp_alloc_bool_buf(n);
    bool all_valid = true;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1u && (n & 7u) != 0)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        dst[k] = static_cast<uint8_t>(va[k] & vb[k]);
        if (dst[k] != expected) all_valid = false;
    }
    if (all_valid) {
        draken_free(dst);
        return nullptr;
    }
    return dst;
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
template<typename Op>
static inline void cmp_scalar_kernel(
    const int64_t*  data,
    const uint32_t* selection,
    int64_t         scalar,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(data[selection[base+0]], scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(data[selection[base+1]], scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(data[selection[base+2]], scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(data[selection[base+3]], scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(data[selection[base+4]], scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(data[selection[base+5]], scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(data[selection[base+6]], scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(data[selection[base+7]], scalar)) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (Op::apply(data[selection[i]], scalar))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        // Branchless: AND packed result with validity byte — null rows → bit 0.
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(Op::apply(data[selection[base+0]], scalar)) << 0) |
                (static_cast<unsigned>(Op::apply(data[selection[base+1]], scalar)) << 1) |
                (static_cast<unsigned>(Op::apply(data[selection[base+2]], scalar)) << 2) |
                (static_cast<unsigned>(Op::apply(data[selection[base+3]], scalar)) << 3) |
                (static_cast<unsigned>(Op::apply(data[selection[base+4]], scalar)) << 4) |
                (static_cast<unsigned>(Op::apply(data[selection[base+5]], scalar)) << 5) |
                (static_cast<unsigned>(Op::apply(data[selection[base+6]], scalar)) << 6) |
                (static_cast<unsigned>(Op::apply(data[selection[base+7]], scalar)) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                if (Op::apply(data[selection[i]], scalar))
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

    cmp_scalar_kernel<Op>(data, v.selection, scalar, src_null, dst, n);

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
