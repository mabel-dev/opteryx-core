#pragma once
// draken/ops/bool_logical.h — AND / OR / NOT over DRAKEN_BOOL vectors.
//
// Kleene three-valued logic (SQL NULL semantics):
//   AND: FALSE dominates — F∧N=F (valid!). T∧N=N, N∧N=N.
//   OR:  TRUE  dominates — T∨N=T (valid!). F∨N=N, N∨N=N.
//   NOT: ¬T=F, ¬F=T, ¬N=N (validity preserved; value bits flipped).
//
// Validity formulas (bitwise, letting av/bv = 0xFF when validity==nullptr):
//   AND valid = (av & bv) | (av & ~aval) | (bv & ~bval)
//   OR  valid = (av & bv) | (av &  aval) | (bv &  bval)
//   NOT valid = a.validity (unchanged); result value = ~a.data, tail-masked.
//
// Access is always data[selection[i]] for logical row i.
// Results are dense-identity DRAKEN_BOOL vectors (flags = SEL_IDENTITY | SEL_PERMUTATION).
// Callers must ensure both inputs have type == DRAKEN_BOOL and equal length.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Internal bit helpers
// ---------------------------------------------------------------------------

static inline uint32_t bool_get_val(const uint8_t* data, uint32_t sel) noexcept {
    return (data[sel >> 3] >> (sel & 7u)) & 1u;
}

static inline uint32_t bool_get_valid(const uint8_t* validity, uint32_t i) noexcept {
    // validity == nullptr ⟹ all-valid; caller uses this inline.
    return (validity[i >> 3] >> (i & 7u)) & 1u;
}

// Allocate a SIMD-padded byte buffer, zero-initialised. Returns owned ptr.
// alloc_bytes is the actual allocated size; padded = ((bm_bytes+7)&~7) capped to ≥8.
static inline uint8_t* bool_alloc_buf(uint32_t bm_bytes, size_t& alloc_bytes) {
    const uint32_t padded = ((bm_bytes + 7u) & ~7u);
    alloc_bytes = (padded > 0u) ? static_cast<size_t>(padded) : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(alloc_bytes));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, alloc_bytes);
    return p;
}

// Mask the partial last byte (bits n%8..7) to 0 in a bitmap.
// No-op when n is a multiple of 8.
static inline void bool_mask_tail(uint8_t* buf, uint32_t bm_bytes, uint32_t n) noexcept {
    const uint32_t tail = n & 7u;
    if (tail != 0u && bm_bytes > 0u)
        buf[bm_bytes - 1u] &= static_cast<uint8_t>((1u << tail) - 1u);
}

// Check if all bits in [0, n) are set in buf. Returns true if all-valid.
static inline bool bool_is_all_set(const uint8_t* buf, uint32_t n) noexcept {
    if (n == 0u) return true;
    const uint32_t bm = (n + 7u) >> 3;
    const uint32_t full = bm - ((n & 7u) != 0u ? 1u : 0u);
    for (uint32_t k = 0u; k < full; ++k)
        if (buf[k] != 0xFFu) return false;
    if ((n & 7u) != 0u) {
        const uint8_t mask = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        if ((buf[bm - 1u] & mask) != mask) return false;
    }
    return true;
}

// Build the output VecResult from allocated data + validity buffers.
// If vld_all_set is true, draken_free(out_vld) and set validity to nullptr.
static inline VecResult bool_make_result(
    uint8_t* out_val, uint8_t* out_vld,
    uint32_t n, bool vld_all_set) noexcept
{
    VecResult r;
    r.data           = out_val;
    r.validity       = vld_all_set ? nullptr : out_vld;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    if (vld_all_set && out_vld != nullptr) draken_free(out_vld);
    return r;
}

// ---------------------------------------------------------------------------
// AND — Kleene: FALSE dominates.
//   result_val  = aval & bval
//   result_vld  = (av & bv) | (av & ~aval) | (bv & ~bval)
// ---------------------------------------------------------------------------

static VecResult bool_and(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n       = a.length;
    const uint32_t bm      = (n + 7u) >> 3;
    const uint8_t* adata   = static_cast<const uint8_t*>(a.data);
    const uint8_t* bdata   = static_cast<const uint8_t*>(b.data);
    const uint8_t* av      = a.validity;   // nullptr ⟹ all-valid
    const uint8_t* bv      = b.validity;

    size_t val_alloc;
    uint8_t* out_val = bool_alloc_buf(bm, val_alloc);

    // Short-circuit: no nulls in either input → result is always all-valid.
    if (av == nullptr && bv == nullptr) {
        if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
            for (uint32_t k = 0u; k < bm; ++k)
                out_val[k] = adata[k] & bdata[k];
            bool_mask_tail(out_val, bm, n);
        } else {
            for (uint32_t i = 0u; i < n; ++i) {
                const uint8_t r = bool_get_val(adata, a.selection[i]) &
                                  bool_get_val(bdata, b.selection[i]);
                if (r) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            }
        }
        return bool_make_result(out_val, nullptr, n, true);
    }

    // General path: compute both value and validity bits.
    size_t vld_alloc;
    uint8_t* out_vld = bool_alloc_buf(bm, vld_alloc);

    if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
        // Byte-wise fast path for dense-identity inputs.
        for (uint32_t k = 0u; k < bm; ++k) {
            const uint8_t aval  = adata[k];
            const uint8_t bval  = bdata[k];
            const uint8_t av_b  = av ? av[k] : 0xFFu;
            const uint8_t bv_b  = bv ? bv[k] : 0xFFu;
            out_val[k] = aval & bval;
            out_vld[k] = (av_b & bv_b) | (av_b & static_cast<uint8_t>(~aval))
                                        | (bv_b & static_cast<uint8_t>(~bval));
        }
        bool_mask_tail(out_val, bm, n);
        bool_mask_tail(out_vld, bm, n);
    } else {
        for (uint32_t i = 0u; i < n; ++i) {
            const uint8_t aval  = static_cast<uint8_t>(bool_get_val(adata, a.selection[i]));
            const uint8_t bval  = static_cast<uint8_t>(bool_get_val(bdata, b.selection[i]));
            const uint8_t av_b  = static_cast<uint8_t>(av ? bool_get_valid(av, i) : 1u);
            const uint8_t bv_b  = static_cast<uint8_t>(bv ? bool_get_valid(bv, i) : 1u);
            const uint8_t rval  = aval & bval;
            // AND valid = (av & bv) | (av & ~aval) | (bv & ~bval)
            // For single bits ~aval is either 0 or 1, so use (1-aval).
            const uint8_t rvld  = (av_b & bv_b) | (av_b & (1u - aval)) | (bv_b & (1u - bval));
            if (rval) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            if (rvld) out_vld[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    const bool all_set = bool_is_all_set(out_vld, n);
    return bool_make_result(out_val, out_vld, n, all_set);
}

// ---------------------------------------------------------------------------
// OR — Kleene: TRUE dominates.
//   result_val  = aval | bval
//   result_vld  = (av & bv) | (av & aval) | (bv & bval)
// ---------------------------------------------------------------------------

static VecResult bool_or(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n       = a.length;
    const uint32_t bm      = (n + 7u) >> 3;
    const uint8_t* adata   = static_cast<const uint8_t*>(a.data);
    const uint8_t* bdata   = static_cast<const uint8_t*>(b.data);
    const uint8_t* av      = a.validity;
    const uint8_t* bv      = b.validity;

    size_t val_alloc;
    uint8_t* out_val = bool_alloc_buf(bm, val_alloc);

    if (av == nullptr && bv == nullptr) {
        if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
            for (uint32_t k = 0u; k < bm; ++k)
                out_val[k] = adata[k] | bdata[k];
            bool_mask_tail(out_val, bm, n);
        } else {
            for (uint32_t i = 0u; i < n; ++i) {
                const uint8_t r = bool_get_val(adata, a.selection[i]) |
                                  bool_get_val(bdata, b.selection[i]);
                if (r) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            }
        }
        return bool_make_result(out_val, nullptr, n, true);
    }

    size_t vld_alloc;
    uint8_t* out_vld = bool_alloc_buf(bm, vld_alloc);

    if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
        for (uint32_t k = 0u; k < bm; ++k) {
            const uint8_t aval  = adata[k];
            const uint8_t bval  = bdata[k];
            const uint8_t av_b  = av ? av[k] : 0xFFu;
            const uint8_t bv_b  = bv ? bv[k] : 0xFFu;
            out_val[k] = aval | bval;
            out_vld[k] = (av_b & bv_b) | (av_b & aval) | (bv_b & bval);
        }
        bool_mask_tail(out_val, bm, n);
        bool_mask_tail(out_vld, bm, n);
    } else {
        for (uint32_t i = 0u; i < n; ++i) {
            const uint8_t aval  = static_cast<uint8_t>(bool_get_val(adata, a.selection[i]));
            const uint8_t bval  = static_cast<uint8_t>(bool_get_val(bdata, b.selection[i]));
            const uint8_t av_b  = static_cast<uint8_t>(av ? bool_get_valid(av, i) : 1u);
            const uint8_t bv_b  = static_cast<uint8_t>(bv ? bool_get_valid(bv, i) : 1u);
            const uint8_t rval  = aval | bval;
            const uint8_t rvld  = (av_b & bv_b) | (av_b & aval) | (bv_b & bval);
            if (rval) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            if (rvld) out_vld[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    const bool all_set = bool_is_all_set(out_vld, n);
    return bool_make_result(out_val, out_vld, n, all_set);
}

// ---------------------------------------------------------------------------
// XOR — Kleene: no dominating value, so the result is known only when BOTH
// operands are known.
//   result_val  = aval ^ bval
//   result_vld  = av & bv        (N on either side ⟹ N)
// ---------------------------------------------------------------------------

static VecResult bool_xor(const DrakenVector& a, const DrakenVector& b) {
    const uint32_t n       = a.length;
    const uint32_t bm      = (n + 7u) >> 3;
    const uint8_t* adata   = static_cast<const uint8_t*>(a.data);
    const uint8_t* bdata   = static_cast<const uint8_t*>(b.data);
    const uint8_t* av      = a.validity;
    const uint8_t* bv      = b.validity;

    size_t val_alloc;
    uint8_t* out_val = bool_alloc_buf(bm, val_alloc);

    if (av == nullptr && bv == nullptr) {
        if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
            for (uint32_t k = 0u; k < bm; ++k)
                out_val[k] = adata[k] ^ bdata[k];
            bool_mask_tail(out_val, bm, n);
        } else {
            for (uint32_t i = 0u; i < n; ++i) {
                const uint8_t r = bool_get_val(adata, a.selection[i]) ^
                                  bool_get_val(bdata, b.selection[i]);
                if (r) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            }
        }
        return bool_make_result(out_val, nullptr, n, true);
    }

    size_t vld_alloc;
    uint8_t* out_vld = bool_alloc_buf(bm, vld_alloc);

    if ((a.flags & DRAKEN_SEL_IDENTITY) && (b.flags & DRAKEN_SEL_IDENTITY)) {
        for (uint32_t k = 0u; k < bm; ++k) {
            const uint8_t av_b = av ? av[k] : 0xFFu;
            const uint8_t bv_b = bv ? bv[k] : 0xFFu;
            out_val[k] = adata[k] ^ bdata[k];
            out_vld[k] = av_b & bv_b;
        }
        bool_mask_tail(out_val, bm, n);
        bool_mask_tail(out_vld, bm, n);
    } else {
        for (uint32_t i = 0u; i < n; ++i) {
            const uint8_t aval  = static_cast<uint8_t>(bool_get_val(adata, a.selection[i]));
            const uint8_t bval  = static_cast<uint8_t>(bool_get_val(bdata, b.selection[i]));
            const uint8_t av_b  = static_cast<uint8_t>(av ? bool_get_valid(av, i) : 1u);
            const uint8_t bv_b  = static_cast<uint8_t>(bv ? bool_get_valid(bv, i) : 1u);
            if (aval ^ bval) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            if (av_b & bv_b) out_vld[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    const bool all_set = bool_is_all_set(out_vld, n);
    return bool_make_result(out_val, out_vld, n, all_set);
}

// ---------------------------------------------------------------------------
// NOT — Kleene: ¬T=F, ¬F=T, ¬N=N (validity unchanged; value bits flipped).
// ---------------------------------------------------------------------------

static VecResult bool_not(const DrakenVector& a) {
    const uint32_t n     = a.length;
    const uint32_t bm    = (n + 7u) >> 3;
    const uint8_t* adata = static_cast<const uint8_t*>(a.data);
    const uint8_t* av    = a.validity;

    size_t val_alloc;
    uint8_t* out_val = bool_alloc_buf(bm, val_alloc);

    if (a.flags & DRAKEN_SEL_IDENTITY) {
        for (uint32_t k = 0u; k < bm; ++k)
            out_val[k] = static_cast<uint8_t>(~adata[k]);
        bool_mask_tail(out_val, bm, n);
    } else {
        for (uint32_t i = 0u; i < n; ++i) {
            const uint8_t flipped = static_cast<uint8_t>(
                1u - bool_get_val(adata, a.selection[i]));
            if (flipped) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    // Validity: copy the input's validity bitmap (nullptr stays nullptr).
    uint8_t* out_vld = nullptr;
    if (av != nullptr) {
        size_t vld_alloc;
        out_vld = bool_alloc_buf(bm, vld_alloc);
        std::memcpy(out_vld, av, static_cast<size_t>(bm));
        // Re-mask tail to match the copy (av's tail might have extra bits set).
        bool_mask_tail(out_vld, bm, n);
    }

    return bool_make_result(out_val, out_vld, n, out_vld == nullptr);
}

// ---------------------------------------------------------------------------
// IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE — never-null truth test.
//   op: 0=IS_TRUE 1=IS_FALSE 2=IS_NOT_TRUE 3=IS_NOT_FALSE
//   IS_TRUE      = data & validity
//   IS_FALSE     = ~data & validity
//   IS_NOT_TRUE  = ~data | ~validity
//   IS_NOT_FALSE = data | ~validity
// validity == nullptr is treated as an all-valid (0xFF) byte/bit throughout —
// the SAME av-fill idiom bool_and/bool_or use above.
// Result is ALWAYS all-valid (a truth test never yields NULL — SQL semantics,
// unlike bool_and/or/xor/not, which are Kleene NULL-preserving).
// ---------------------------------------------------------------------------

static VecResult bool_truth_test(const DrakenVector& a, int op) {
    const uint32_t n     = a.length;
    const uint32_t bm    = (n + 7u) >> 3;
    const uint8_t* adata = static_cast<const uint8_t*>(a.data);
    const uint8_t* av    = a.validity;

    size_t val_alloc;
    uint8_t* out_val = bool_alloc_buf(bm, val_alloc);

    if (a.flags & DRAKEN_SEL_IDENTITY) {
        for (uint32_t k = 0u; k < bm; ++k) {
            const uint8_t aval = adata[k];
            const uint8_t av_b = av ? av[k] : 0xFFu;
            uint8_t bit;
            switch (op) {
                case 0:  bit = aval & av_b; break;
                case 1:  bit = static_cast<uint8_t>(~aval) & av_b; break;
                case 2:  bit = static_cast<uint8_t>(~aval) | static_cast<uint8_t>(~av_b); break;
                default: bit = aval | static_cast<uint8_t>(~av_b); break;
            }
            out_val[k] = bit;
        }
        bool_mask_tail(out_val, bm, n);
    } else {
        for (uint32_t i = 0u; i < n; ++i) {
            const uint32_t val = bool_get_val(adata, a.selection[i]);
            const uint32_t vld = av ? bool_get_valid(av, i) : 1u;
            uint32_t bit;
            switch (op) {
                case 0:  bit = val & vld; break;
                case 1:  bit = (1u - val) & vld; break;
                case 2:  bit = (1u - val) | (1u - vld); break;
                default: bit = val | (1u - vld); break;
            }
            if (bit) out_val[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    return bool_make_result(out_val, nullptr, n, true);
}

}} // namespace draken::ops
