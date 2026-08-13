// draken/ops/kernels/function_kernels.cpp — Phase 9a-fn: scalar function kernels on
// the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// These are dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION
// C-native arm) — no Python, no nanobind, no GIL. Every kernel:
//   - reads inputs uniformly via data[selection[i]] (buffers.h contract; string
//     inputs resolve through the canonical DrakenStringArena header in `data`),
//   - returns an owned, dense VecResult (fixed buffers via draken_malloc, string
//     results via the consolidated-block helpers in result_helpers.h),
//   - fails LOUD with an error sentinel for anything outside its contract — never
//     a silent wrong answer.
//
// Case transforms accept VARCHAR and VARBINARY: both carry the exact same
// DrakenStringSlot/arena byte layout with no UTF-8 assumption, so an ASCII-range
// byte fold (non-ASCII bytes pass through unchanged) is correct for either, and
// preserves the input's tag. NVARCHAR needs full Unicode case mapping — that is
// a different feature; returning an error sentinel beats silently drifting from
// str.upper() semantics.

#include <algorithm>   // std::binary_search — draken_in_list
#include <vector>      // dict-shape per-unique staging (draken_length)
#include <cstdint>
#include <cstring>
#include <cmath>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/kernel_context.h"  // binary_op_ctx — carries the DECIMAL operand scale
#include "ops/kernels/error_handling.h"
#include "ops/kernels/glob_match.h"      // draken_glob::like_match — shared with draken_like_any
#include "ops/kernels/dfa_walk.h"        // draken_dfa::match — length-adaptive LIKE fast path
#include "ops/kernels/like_program.h"    // draken_like_prog::match — SIMD op-program LIKE matcher
#include "ops/kernels/utf8_ci_match.h"   // draken_utf8ci::* — Unicode casefold match for ci+NVARCHAR
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, same as every builder

namespace {

inline bool fk_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u);
}

inline bool fk_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// Shared ASCII case transform (VARCHAR/VARBINARY only — see file header).
//
// SHAPE-PRESERVING (the string-CAST pattern, cast_string.cpp): the fold is a pure
// per-value byte map that never changes a string's length, so it is computed ONCE
// per data_length PHYSICAL unique value (a dict's K entries, a constant's single
// value, or a dense column's `length` values), then kernel_preserve_shape carries
// the input's selection + per-logical-row validity onto the result. Dense stays
// dense, constant stays constant, dict stays dict — no force-expand to `length`.
// Validity is per-logical-row, so the K-slot physical block carries none; the
// preserved validity (copied from the input) is the sole null authority.
VecResult ascii_case_transform(const DrakenVector* v, bool to_upper, const char* who) {
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_VARBINARY) {
        return draken_error_sentinel_fmt(
            "%s: VARCHAR or VARBINARY (ASCII-range byte fold) input required — "
            "NVARCHAR case mapping is not implemented natively yet (fail loud, "
            "never wrong)", who);
    }
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // Pass 1: exact long-form byte total over the K physical values (lengths are
    // unchanged by casing).
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        uint32_t len = str_length(&sa->slots[j]);
        if (len > STR_INLINE_MAX) arena_len += len;
    }

    // K-slot physical string block, NO embedded validity — nulls live per-logical-
    // row and are supplied by kernel_preserve_shape below.
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel("ascii_case: allocation failed");

    uint8_t buf_inline[STR_INLINE_MAX];
    size_t arena_pos = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        uint32_t len = str_length(slot);
        const uint8_t* src = str_data(slot, sa->arena);
        if (len <= STR_INLINE_MAX) {
            for (uint32_t b = 0; b < len; ++b) {
                uint8_t c = src[b];
                buf_inline[b] = to_upper
                    ? ((c >= 'a' && c <= 'z') ? c - 32 : c)
                    : ((c >= 'A' && c <= 'Z') ? c + 32 : c);
            }
            str_init_inline(&slots[j], buf_inline, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            for (uint32_t b = 0; b < len; ++b) {
                uint8_t c = src[b];
                dst[b] = to_upper
                    ? ((c >= 'a' && c <= 'z') ? c - 32 : c)
                    : ((c >= 'A' && c <= 'Z') ? c + 32 : c);
            }
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }

    // vecresult_from_string_block hands back a dense K-shape (selection = non-owned
    // global identity over K, validity NULL); kernel_preserve_shape then overrides
    // selection/validity/length with the input's shape. The identity it replaces is
    // shared/non-owned, so the override does not leak. kernel_preserve_shape may
    // throw std::bad_alloc — catch it here (this kernel is not wrapped in
    // DRAKEN_KERNEL_TRY) so nothing escapes into the nogil VM, and free the block.
    VecResult r = vecresult_from_string_block(block, k, arena_len, /*has_validity=*/0, v->type);
    try {
        kernel_preserve_shape(r, v);   // r.validity = input copy (or NULL), non-embedded
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel("ascii_case: shape-carry allocation failed");
    }
    return r;
}

}  // namespace

extern "C" {

// LENGTH(string) -> INT64. VARCHAR/VARBINARY: byte length; NVARCHAR: UTF-8
// codepoint count (continuation bytes excluded) — the type family's own length
// semantics (string_family model).
VecResult draken_length(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_length: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fk_is_string(v->type)) {
        return draken_error_sentinel("draken_length: string input required "
                                     "(ARRAY length is a separate kernel)");
    }
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    uint32_t n = v->length;
    auto* out = static_cast<int64_t*>(
        draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("draken_length: allocation failed");
    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
        if (validity == nullptr) {
            draken_free(out);
            return draken_error_sentinel("draken_length: allocation failed");
        }
        std::memcpy(validity, v->validity, vb > 0 ? vb : 1);
    }
    if (v->data_length < v->length) {
        // dict/constant shape: compute each UNIQUE value's length once, then
        // gather — the NVARCHAR codepoint scan in particular is per-slot work
        // that must not repeat per row. Output stays DENSE (the VM's fixed-
        // result fold contract); values are identical to the per-row path.
        const uint32_t k = v->data_length;
        std::vector<int64_t> ulen(k > 0 ? k : 1, 0);
        for (uint32_t j = 0; j < k; ++j) {
            const DrakenStringSlot* slot = &sa->slots[j];
            uint32_t blen = str_length(slot);
            if (v->type == DRAKEN_NVARCHAR) {
                const uint8_t* p = str_data(slot, sa->arena);
                int64_t cp = 0;
                for (uint32_t b = 0; b < blen; ++b) {
                    if ((p[b] & 0xC0u) != 0x80u) ++cp;
                }
                ulen[j] = cp;
            } else {
                ulen[j] = blen;
            }
        }
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i) ? ulen[v->selection[i]] : 0;
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) { out[i] = 0; continue; }
            const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
            uint32_t blen = str_length(slot);
            if (v->type == DRAKEN_NVARCHAR) {
                const uint8_t* p = str_data(slot, sa->arena);
                int64_t cp = 0;
                for (uint32_t b = 0; b < blen; ++b) {
                    if ((p[b] & 0xC0u) != 0x80u) ++cp;
                }
                out[i] = cp;
            } else {
                out[i] = blen;
            }
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_INT64;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

VecResult draken_upper(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_upper: expected 1 argument");
    return ascii_case_transform(args[0], true, "draken_upper");
}

VecResult draken_lower(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_lower: expected 1 argument");
    return ascii_case_transform(args[0], false, "draken_lower");
}

}  // extern "C"

// ---- numeric scalar functions -----------------------------------------------------
// All accept INT8..64 / UINT8..64 / FLOAT32/64 / DECIMAL operands, read uniformly via
// data[selection[i]], and preserve the input's validity (copied).
//
// DECIMAL is an int64 unscaled value; the scale lives on the LogicalType descriptor,
// not on DrakenVector (core/buffers.h freezes that layout). A kernel whose ANSWER or
// whose RESULT DESCRIPTOR depends on the scale takes it from the bind-time
// binary_op_ctx (left_scale / result_scale / result_precision) and fails loud without
// one. A kernel that is scale-invariant end to end needs no ctx — SIGN is the only
// one: sign(unscaled) == sign(value), and it returns INTEGER.
//
// DECIMAL128 is int128-backed and is NOT admitted — it has no reader below.

namespace {

inline bool fk_is_signed_int(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            return true;
        default:
            return false;
    }
}

inline bool fk_is_unsigned_int(DrakenType t) {
    switch (t) {
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

inline bool fk_is_float(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

inline bool fk_is_numeric(DrakenType t) {
    return fk_is_signed_int(t) || fk_is_unsigned_int(t) || fk_is_float(t)
        || t == DRAKEN_DECIMAL;
}

// Each reader enumerates EXACTLY the types it accepts. There is deliberately no
// catch-all `default:` that casts v->data to the reader's own element type: the old
// `default: static_cast<const double*>(...)` reinterpreted an int64/uint32 bit
// pattern as an IEEE double for anything the entry guard admitted, which is a silent
// wrong answer rather than a crash. Callers gate on the fk_is_* predicates above.

inline double fk_read_double(const DrakenVector* v, uint32_t row) {
    uint32_t phys = v->selection[row];
    switch (v->type) {
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[phys];
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v->data)[phys]);
        case DRAKEN_UINT8:   return static_cast<const uint8_t*>(v->data)[phys];
        case DRAKEN_UINT16:  return static_cast<const uint16_t*>(v->data)[phys];
        case DRAKEN_UINT32:  return static_cast<const uint32_t*>(v->data)[phys];
        case DRAKEN_UINT64:  return static_cast<double>(static_cast<const uint64_t*>(v->data)[phys]);
        case DRAKEN_FLOAT32: return static_cast<const float*>(v->data)[phys];
        case DRAKEN_FLOAT64: return static_cast<const double*>(v->data)[phys];
        default:             return 0.0;   // unreachable: fk_is_* gates every caller
    }
}

// DECIMAL yields its raw int64 unscaled value. UINT64 is absent by design — it does
// not fit int64; unsigned callers use fk_read_uint64.
inline int64_t fk_read_int64(const DrakenVector* v, uint32_t row) {
    uint32_t phys = v->selection[row];
    switch (v->type) {
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[phys];
        case DRAKEN_UINT8:   return static_cast<const uint8_t*>(v->data)[phys];
        case DRAKEN_UINT16:  return static_cast<const uint16_t*>(v->data)[phys];
        case DRAKEN_UINT32:  return static_cast<const uint32_t*>(v->data)[phys];
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL: return static_cast<const int64_t*>(v->data)[phys];
        default:             return 0;     // unreachable: fk_is_* gates every caller
    }
}

inline uint64_t fk_read_uint64(const DrakenVector* v, uint32_t row) {
    uint32_t phys = v->selection[row];
    switch (v->type) {
        case DRAKEN_UINT8:   return static_cast<const uint8_t*>(v->data)[phys];
        case DRAKEN_UINT16:  return static_cast<const uint16_t*>(v->data)[phys];
        case DRAKEN_UINT32:  return static_cast<const uint32_t*>(v->data)[phys];
        case DRAKEN_UINT64:  return static_cast<const uint64_t*>(v->data)[phys];
        default:             return 0;     // unreachable: fk_is_unsigned_int gates it
    }
}

// Rejection for an operand fk_is_numeric does not admit. DECIMAL128 IS numeric, it
// simply has no int128 reader in this file, so saying "numeric input required" about
// it would send the reader hunting for a type error that isn't there.
inline VecResult fk_reject_operand(const DrakenVector* v, const char* who) {
    if (v->type == DRAKEN_DECIMAL128) {
        return draken_error_sentinel_fmt(
            "%s: DECIMAL128 operand (precision > 18) is not supported by this kernel", who);
    }
    return draken_error_sentinel_fmt("%s: numeric input required", who);
}

// Shared dense-numeric result assembly (validity copied from the operand).
template <typename T>
VecResult fk_numeric_result(const DrakenVector* src_validity_of, T* out, uint32_t n,
                            DrakenType t) {
    uint8_t* validity = nullptr;
    if (src_validity_of->validity != nullptr) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
        if (validity == nullptr) {
            draken_free(out);
            return draken_error_sentinel("function kernel: allocation failed");
        }
        std::memcpy(validity, src_validity_of->validity, vb > 0 ? vb : 1);
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = t;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// Dense same-type gather: out[i] = data[selection[i]]. Used by ABS over an unsigned
// operand, where |x| == x and the catalog's "same as `num`" return type must be kept
// exactly (widening UINT64 to INT64 would corrupt values above INT64_MAX).
template <typename T>
VecResult fk_gather_dense(const DrakenVector* v, uint32_t n, DrakenType t) {
    auto* out = static_cast<T*>(draken_malloc((n > 0 ? n : 1) * sizeof(T)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    const T* src = static_cast<const T*>(v->data);
    for (uint32_t i = 0; i < n; ++i) {
        out[i] = fk_row_valid(v, i) ? src[v->selection[i]] : static_cast<T>(0);
    }
    return fk_numeric_result(v, out, n, t);
}

// Unary numeric kernel body: INT-family -> INT64 via `ifn`; floats -> FLOAT64 via
// `ffn` (SIGN forces INT64 output for floats too via `force_int`).
//
// Callers must handle UNSIGNED and DECIMAL operands BEFORE delegating here, except
// where the int64 path is provably right for them: fk_read_int64 reads a DECIMAL as
// its unscaled int64, so SIGN(DECIMAL) is correct through this body. Unsigned never
// is — fk_read_int64 has no UINT64 arm.
template <typename IFn, typename FFn>
VecResult fk_unary_numeric(const DrakenVector* const* args, uint32_t nargs,
                           const char* who, IFn ifn, FFn ffn, bool force_int) {
    if (nargs != 1) return draken_error_sentinel_fmt("%s: expected 1 argument", who);
    const DrakenVector* v = args[0];
    if (!fk_is_numeric(v->type))
        return fk_reject_operand(v, who);
    uint32_t n = v->length;
    if (fk_is_float(v->type) && !force_int) {
        auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
        if (out == nullptr) return draken_error_sentinel("allocation failed");
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i) ? ffn(fk_read_double(v, i)) : 0.0;
        }
        return fk_numeric_result(v, out, n, DRAKEN_FLOAT64);
    }
    auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    if (fk_is_float(v->type)) {
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i)
                ? static_cast<int64_t>(ffn(fk_read_double(v, i))) : 0;
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i) ? ifn(fk_read_int64(v, i)) : 0;
        }
    }
    return fk_numeric_result(v, out, n, DRAKEN_INT64);
}

// digits operand for ROUND/FLOOR/CEILING's optional second argument (constant or
// per-row column, integer family).
inline bool fk_read_digits(const DrakenVector* v, uint32_t row, int64_t& out) {
    switch (v->type) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            out = fk_read_int64(v, row);
            return true;
        default:
            return false;
    }
}

// ROUND/FLOOR/CEILING(x[, digits]) -> FLOAT64 (catalog contract). `op`: 0=round
// (half-away-from-zero, the SQL convention), 1=floor, 2=ceil.
//
// DECIMAL operands round EXACTLY in the raw int64 domain (the double image of a
// decimal like 2.675 is 2.6749999…, so double-domain rounding gives the WRONG
// digit) — the operand's scale arrives via the bind-time binary_op_ctx
// (left_scale); a DECIMAL operand with no ctx fails loud.
VecResult fk_round_family(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                          const char* who, int op) {
    if (nargs < 1 || nargs > 2)
        return draken_error_sentinel_fmt("%s: expected 1 or 2 arguments", who);
    const DrakenVector* v = args[0];
    const DrakenVector* dg = (nargs == 2) ? args[1] : nullptr;
    uint32_t n = v->length;

    if (v->type == DRAKEN_DECIMAL) {
        if (ctx == nullptr) {
            return draken_error_sentinel_fmt(
                "%s: DECIMAL operand needs its bind-time scale context", who);
        }
        int s = static_cast<const binary_op_ctx*>(ctx)->left_scale;
        auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
        if (out == nullptr) return draken_error_sentinel("allocation failed");
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) { out[i] = 0.0; continue; }
            int64_t raw = fk_read_int64(v, i);
            int64_t d = 0;
            if (dg != nullptr) {
                if (!fk_row_valid(dg, i) || !fk_read_digits(dg, i, d)) {
                    draken_free(out);
                    return draken_error_sentinel_fmt("%s: integer digits required", who);
                }
            }
            if (d >= s) {
                // no fractional information beyond scale s — the value is exact
                out[i] = static_cast<double>(raw) / std::pow(10.0, static_cast<double>(s));
                continue;
            }
            int64_t steps = s - d;
            if (steps > 18) {
                draken_free(out);
                return draken_error_sentinel_fmt(
                    "%s: rounding step exceeds the int64 decimal domain", who);
            }
            int64_t p = 1;
            for (int64_t k = 0; k < steps; ++k) p *= 10;
            int64_t q = raw / p;
            int64_t r = raw % p;
            if (op == 0) {                       // half away from zero, exact
                int64_t ar = r < 0 ? -r : r;
                if (ar * 2 >= p) q += (raw > 0) - (raw < 0);
            } else if (op == 1) {                // floor
                if (r != 0 && raw < 0) q -= 1;
            } else {                             // ceil
                if (r != 0 && raw > 0) q += 1;
            }
            out[i] = static_cast<double>(q) * std::pow(10.0, -static_cast<double>(d));
        }
        return fk_numeric_result(v, out, n, DRAKEN_FLOAT64);
    }

    if (!fk_is_numeric(v->type))
        return fk_reject_operand(v, who);
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) { out[i] = 0.0; continue; }
        double x = fk_read_double(v, i);
        double scale = 1.0;
        if (dg != nullptr) {
            int64_t d = 0;
            if (!fk_row_valid(dg, i) || !fk_read_digits(dg, i, d)) {
                draken_free(out);
                return draken_error_sentinel_fmt("%s: integer digits required", who);
            }
            scale = std::pow(10.0, static_cast<double>(d));
        }
        double scaled = x * scale;
        double r = (op == 0) ? std::round(scaled)
                 : (op == 1) ? std::floor(scaled)
                             : std::ceil(scaled);
        out[i] = r / scale;
    }
    return fk_numeric_result(v, out, n, DRAKEN_FLOAT64);
}

}  // namespace

extern "C" {

// ABS(num) -> same as `num` (catalog contract).
//   unsigned : |x| == x, so gather at the operand's own width — never widen.
//   DECIMAL  : |unscaled| IS the unscaled |value|, so the arithmetic is scale-free,
//              but the result must be tagged DECIMAL(p, s). VecResult carries the
//              descriptor out; the kernel cannot read it off the input vector, so it
//              comes from the bind-time ctx and a DECIMAL operand without one fails.
VecResult draken_abs(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_abs: expected 1 argument");
    const DrakenVector* v = args[0];
    uint32_t n = v->length;

    if (fk_is_unsigned_int(v->type)) {
        switch (v->type) {
            case DRAKEN_UINT8:  return fk_gather_dense<uint8_t>(v, n, DRAKEN_UINT8);
            case DRAKEN_UINT16: return fk_gather_dense<uint16_t>(v, n, DRAKEN_UINT16);
            case DRAKEN_UINT32: return fk_gather_dense<uint32_t>(v, n, DRAKEN_UINT32);
            default:            return fk_gather_dense<uint64_t>(v, n, DRAKEN_UINT64);
        }
    }

    if (v->type == DRAKEN_DECIMAL) {
        if (ctx == nullptr) {
            return draken_error_sentinel(
                "draken_abs: DECIMAL operand needs its bind-time scale context");
        }
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
        if (out == nullptr) return draken_error_sentinel("allocation failed");
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) { out[i] = 0; continue; }
            int64_t raw = fk_read_int64(v, i);
            out[i] = raw < 0 ? -raw : raw;
        }
        VecResult r = fk_numeric_result(v, out, n, DRAKEN_DECIMAL);
        if (r.data == nullptr) return r;
        r.dec_precision = c->result_precision;
        r.dec_scale = c->result_scale;
        return r;
    }

    return fk_unary_numeric(args, nargs, "draken_abs",
                            [](int64_t x) { return x < 0 ? -x : x; },
                            [](double x) { return std::fabs(x); }, false);
}

// SIGN(num) -> INTEGER (catalog contract). Scale-invariant for DECIMAL, so no ctx:
// fk_read_int64 hands back the unscaled int64 and sign(unscaled) == sign(value).
// Unsigned must not reach fk_unary_numeric (no UINT64 arm in fk_read_int64, so a
// value above INT64_MAX would read negative and report -1).
VecResult draken_sign(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sign: expected 1 argument");
    const DrakenVector* v = args[0];

    if (fk_is_unsigned_int(v->type)) {
        uint32_t n = v->length;
        auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
        if (out == nullptr) return draken_error_sentinel("allocation failed");
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i) ? (fk_read_uint64(v, i) != 0u ? 1 : 0) : 0;
        }
        return fk_numeric_result(v, out, n, DRAKEN_INT64);
    }

    return fk_unary_numeric(args, nargs, "draken_sign",
                            [](int64_t x) -> int64_t { return (x > 0) - (x < 0); },
                            [](double x) -> double { return (x > 0.0) - (x < 0.0); },
                            true);
}

// SQRT(num) -> FLOAT (catalog contract). DECIMAL needs the ACTUAL value, so unlike
// SIGN it cannot work off the unscaled int64: sqrt(unscaled) != sqrt(value). The
// operand's scale arrives via the bind-time ctx; without one it fails loud.
VecResult draken_sqrt(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sqrt: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fk_is_numeric(v->type))
        return fk_reject_operand(v, "draken_sqrt");
    uint32_t n = v->length;

    double dec_unscale = 1.0;
    if (v->type == DRAKEN_DECIMAL) {
        if (ctx == nullptr) {
            return draken_error_sentinel(
                "draken_sqrt: DECIMAL operand needs its bind-time scale context");
        }
        dec_unscale = std::pow(
            10.0, -static_cast<double>(static_cast<const binary_op_ctx*>(ctx)->left_scale));
    }

    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    if (v->type == DRAKEN_DECIMAL) {
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i)
                ? std::sqrt(static_cast<double>(fk_read_int64(v, i)) * dec_unscale) : 0.0;
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            out[i] = fk_row_valid(v, i) ? std::sqrt(fk_read_double(v, i)) : 0.0;
        }
    }
    return fk_numeric_result(v, out, n, DRAKEN_FLOAT64);
}

VecResult draken_round(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return fk_round_family(ctx, args, nargs, "draken_round", 0);
}

VecResult draken_floor(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return fk_round_family(ctx, args, nargs, "draken_floor", 1);
}

VecResult draken_ceiling(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return fk_round_family(ctx, args, nargs, "draken_ceiling", 2);
}

}  // extern "C"

// ---------------------------------------------------------------------------
// EXTRACT(part FROM date/timestamp) — draken_date_part
// ---------------------------------------------------------------------------

namespace {

// Part ids — bind-time contract with compiled_expression.pyx (_EXTRACT_PARTS);
// carried in binary_op_ctx.op_code. The operand's TimestampUnit (0=s,1=ms,
// 2=us,3=ns) rides in left_unit; DATE32 operands ignore it (days).
enum {
    FK_PART_YEAR = 1, FK_PART_MONTH = 2, FK_PART_DAY = 3, FK_PART_QUARTER = 4,
    FK_PART_HOUR = 5, FK_PART_MINUTE = 6, FK_PART_SECOND = 7,
};

inline int64_t fk_floor_div(int64_t a, int64_t b) {
    int64_t q = a / b, r = a % b;
    return (r != 0 && ((r < 0) != (b < 0))) ? q - 1 : q;
}

// Howard Hinnant's civil-from-days (proleptic Gregorian, days since 1970-01-01).
struct fk_civil { int64_t y; unsigned m; unsigned d; };
inline fk_civil fk_civil_from_days(int64_t z) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t y = static_cast<int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned d = doy - (153 * mp + 2) / 5 + 1;
    const unsigned m = mp < 10 ? mp + 3 : mp - 9;
    return {y + (m <= 2), m, d};
}

// Inverse: days since 1970-01-01 for a proleptic-Gregorian y/m/d.
inline int64_t fk_days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

// TRUNC part ids (bind-time contract with compiled_expression.pyx _TRUNC_PARTS).
enum {
    FK_TR_SECOND = 1, FK_TR_MINUTE = 2, FK_TR_HOUR = 3, FK_TR_DAY = 4,
    FK_TR_WEEK = 5, FK_TR_MONTH = 6, FK_TR_QUARTER = 7, FK_TR_YEAR = 8,
};

}  // namespace

extern "C" {

VecResult draken_date_part(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1)
        return draken_error_sentinel("draken_date_part: expected 1 argument");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_date_part: missing bind-time ctx (part id)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    const int part = c->op_code;
    const DrakenVector* v = args[0];
    if (v->type != DRAKEN_DATE32 && v->type != DRAKEN_TIMESTAMP64)
        return draken_error_sentinel("draken_date_part: DATE/TIMESTAMP input required");
    if (v->type == DRAKEN_DATE32 &&
        (part == FK_PART_HOUR || part == FK_PART_MINUTE || part == FK_PART_SECOND))
        return draken_error_sentinel("draken_date_part: sub-day part of a DATE");

    static const int64_t unit_div[4] = {1, 1000, 1000000, 1000000000};
    const int64_t div = unit_div[c->left_unit & 3];

    uint32_t n = v->length;
    auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) { out[i] = 0; continue; }
        uint32_t phys = v->selection[i];
        int64_t days, sod = 0;   // sod = seconds-of-day
        if (v->type == DRAKEN_DATE32) {
            days = static_cast<const int32_t*>(v->data)[phys];
        } else {
            int64_t secs = fk_floor_div(static_cast<const int64_t*>(v->data)[phys], div);
            days = fk_floor_div(secs, 86400);
            sod = secs - days * 86400;
        }
        switch (part) {
            case FK_PART_YEAR:    out[i] = fk_civil_from_days(days).y; break;
            case FK_PART_MONTH:   out[i] = fk_civil_from_days(days).m; break;
            case FK_PART_DAY:     out[i] = fk_civil_from_days(days).d; break;
            case FK_PART_QUARTER: out[i] = (fk_civil_from_days(days).m - 1) / 3 + 1; break;
            case FK_PART_HOUR:    out[i] = sod / 3600; break;
            case FK_PART_MINUTE:  out[i] = (sod / 60) % 60; break;
            case FK_PART_SECOND:  out[i] = sod % 60; break;
            default:
                draken_free(out);
                return draken_error_sentinel("draken_date_part: unsupported part id");
        }
    }
    return fk_numeric_result(v, out, n, DRAKEN_INT64);
}

// TRUNC(ts, unit) — floor a TIMESTAMP64 to the unit boundary; result is the same
// TIMESTAMP64 (unit preserved). ctx=binary_op_ctx: op_code=trunc part id,
// left_unit=operand TimestampUnit (0=s,1=ms,2=us,3=ns). DATE32 operands rejected
// (a DATE has no sub-day resolution to truncate — use it directly).
VecResult draken_date_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_date_trunc: expected 1 argument");
    if (ctx == nullptr) return draken_error_sentinel("draken_date_trunc: missing ctx");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    const int part = c->op_code;
    const DrakenVector* v = args[0];
    if (v->type != DRAKEN_TIMESTAMP64)
        return draken_error_sentinel("draken_date_trunc: TIMESTAMP input required");
    static const int64_t unit_div[4] = {1, 1000, 1000000, 1000000000};
    const int64_t sub = unit_div[c->left_unit & 3];   // sub-second ticks per second

    uint32_t n = v->length;
    auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) { out[i] = 0; continue; }
        int64_t raw = static_cast<const int64_t*>(v->data)[v->selection[i]];
        int64_t secs = fk_floor_div(raw, sub);          // whole seconds since epoch
        int64_t sub_ticks = raw - secs * sub;            // preserved-then-dropped
        int64_t days = fk_floor_div(secs, 86400);
        int64_t sod = secs - days * 86400;               // seconds-of-day
        int64_t tsecs;                                   // truncated whole seconds
        switch (part) {
            case FK_TR_SECOND: tsecs = secs; break;
            case FK_TR_MINUTE: tsecs = secs - (sod % 60); break;
            case FK_TR_HOUR:   tsecs = secs - (sod % 3600); break;
            case FK_TR_DAY:    tsecs = days * 86400; break;
            case FK_TR_WEEK: {   // ISO week: back up to Monday (1970-01-01 = Thursday)
                int64_t dow = ((days % 7) + 7 + 3) % 7;  // 0 = Monday
                tsecs = (days - dow) * 86400; break;
            }
            case FK_TR_MONTH: {
                fk_civil cv = fk_civil_from_days(days);
                tsecs = fk_days_from_civil(cv.y, cv.m, 1) * 86400; break;
            }
            case FK_TR_QUARTER: {
                fk_civil cv = fk_civil_from_days(days);
                unsigned qm = ((cv.m - 1) / 3) * 3 + 1;
                tsecs = fk_days_from_civil(cv.y, qm, 1) * 86400; break;
            }
            case FK_TR_YEAR: {
                fk_civil cv = fk_civil_from_days(days);
                tsecs = fk_days_from_civil(cv.y, 1, 1) * 86400; break;
            }
            default:
                draken_free(out);
                return draken_error_sentinel("draken_date_trunc: unsupported unit");
        }
        (void)sub_ticks;
        out[i] = tsecs * sub;   // back into the operand's unit; sub-second dropped
    }
    VecResult r = fk_numeric_result(v, out, n, DRAKEN_TIMESTAMP64);
    // fk_numeric_result leaves ts_unit at its VecResult default (0xFF = unset),
    // which vecresult_to_owner() (draken_native.cpp) reads as "no LogicalType" —
    // the result would silently lose its logical_type_unit descriptor (a hard
    // error the next time anything reads TimestampUnit off it, e.g. a further
    // DATE_TRUNC/EXTRACT/DATEDIFF). The trunc preserves the operand's unit
    // (see the comment above: `out[i] = tsecs * sub` stays in that unit).
    if (r.data != nullptr) r.ts_unit = c->left_unit;
    return r;
}

}  // extern "C"

// ---------------------------------------------------------------------------
// CASE / IIF blend — draken_if_then_else(mask, then, else)
// ---------------------------------------------------------------------------

namespace {

// THE canonical width (core/buffers.h), not a private table. The private one
// this replaces listed only the SIGNED types, so every unsigned branch reported
// width 0 and the blend refused outright: CASE ... THEN CAST(x AS UINT32) died
// with "unsupported branch type" on the runtime path (and an IPv4 column, being
// UINT32, with it). The blend below is a type-agnostic memcpy of `es` bytes, so
// there is nothing signedness-specific about it — only the table was wrong.
// 0 still means "no flat per-element width" (bool and the string family are
// handled by their own blocks above) and is still refused.
inline size_t fk_fixed_elem_size(DrakenType t) {
    return draken_type_fixed_itemsize(t);
}

}  // namespace

extern "C" {

// A branch materialised as DRAKEN_NULL (a typed NULL literal — e.g. an implicit
// CASE ELSE, or a literal ELSE/THEN of NULL) has no `data`/`selection` to read:
// per its contract ("no data, no validity") every row is invalid. Treat it as
// invalid UNCONDITIONALLY, never call fk_row_valid or dereference its buffers.
inline bool fk_ite_branch_valid(const DrakenVector* v, bool is_null_branch, uint32_t row) {
    return !is_null_branch && fk_row_valid(v, row);
}

// SQL CASE semantics: a NULL condition row selects the else branch; the output
// row's validity is the CHOSEN branch's validity. Non-NULL branch types must
// already match (bind-time literal coercion in the plan compiler) — never
// coerced here. A DRAKEN_NULL branch (typed-NULL literal that lost its type —
// see `_materialise_constant_literal`'s comment: only VARCHAR family keeps a
// typed null) is the one exception: it pairs with ANY other branch type and
// contributes only invalidity, never data.
VecResult draken_if_then_else(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 3)
        return draken_error_sentinel("draken_if_then_else: expected 3 arguments");
    const DrakenVector* m = args[0];
    const DrakenVector* a = args[1];
    const DrakenVector* b = args[2];
    if (m->type != DRAKEN_BOOL)
        return draken_error_sentinel("draken_if_then_else: condition must be BOOLEAN");

    const bool a_isnull = (a->type == DRAKEN_NULL);
    const bool b_isnull = (b->type == DRAKEN_NULL);
    if (a_isnull && b_isnull) {
        // CASE WHEN ... THEN NULL ELSE NULL END (or equivalent) — every row is
        // NULL regardless of the condition, and neither branch has a real type
        // to borrow (SQL narrows an all-NULL expression's declared type at bind
        // time; this kernel never sees that type). `VecResult.data == nullptr`
        // is the ABI's ERROR sentinel (see _dv_function_kernel_c) — a genuine
        // all-NULL SUCCESS must never use it. Mirror the established idiom
        // (binary_op_arithmetic.cpp's DRAKEN_NULL-operand handling): allocate a
        // real dummy buffer of a concrete placeholder type (content is never
        // read — every row is marked invalid) instead of the DRAKEN_NULL tag.
        uint32_t n = m->length;
        auto* out_data = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
        if (out_data == nullptr) return draken_error_sentinel("allocation failed");
        const size_t nb = (static_cast<size_t>(n) + 7) / 8;
        auto* out_validity = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
        if (out_validity == nullptr) { draken_free(out_data); return draken_error_sentinel("allocation failed"); }
        std::memset(out_validity, 0, nb > 0 ? nb : 1);
        VecResult r{};
        r.data = out_data;
        r.validity = out_validity;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length = n;
        r.length = n;
        r.type = DRAKEN_INT64;
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    }
    // Dispatch (family selection, output-type decisions) always uses the REAL
    // side's type when the other is a null branch — the blend loops below never
    // read the null branch's data, only its (unconditional) invalidity.
    const DrakenType a_eff = a_isnull ? b->type : a->type;
    const DrakenType b_eff = b_isnull ? a->type : b->type;
    const bool force_validity = a_isnull || b_isnull;

    // STRING branches: blend into one canonical consolidated block. A mixed
    // {VARCHAR, NVARCHAR} pair widens to NVARCHAR (ASCII is valid UTF-8 —
    // exact); VARBINARY only pairs with VARBINARY (fail loud otherwise).
    if (fk_is_string(a_eff) && fk_is_string(b_eff)) {
        DrakenType s_t;
        if (a_eff == b_eff) {
            s_t = a_eff;
        } else if ((a_eff == DRAKEN_VARCHAR && b_eff == DRAKEN_NVARCHAR) ||
                   (a_eff == DRAKEN_NVARCHAR && b_eff == DRAKEN_VARCHAR)) {
            s_t = DRAKEN_NVARCHAR;
        } else {
            return draken_error_sentinel(
                "draken_if_then_else: VARBINARY branch paired with a text branch");
        }
        const auto* asa = a_isnull ? nullptr : static_cast<const DrakenStringArena*>(a->data);
        const auto* bsa = b_isnull ? nullptr : static_cast<const DrakenStringArena*>(b->data);
        const auto* smbits = static_cast<const uint8_t*>(m->data);
        uint32_t sn = m->length;
        const int want_validity =
            (force_validity || a->validity != nullptr || b->validity != nullptr) ? 1 : 0;
        // Pass 1: long-form byte total of the CHOSEN branch per row.
        size_t arena_len = 0;
        for (uint32_t i = 0; i < sn; ++i) {
            bool cond = false;
            if (fk_row_valid(m, i)) {
                uint32_t mp = m->selection[i];
                cond = (smbits[mp >> 3] >> (mp & 7)) & 1u;
            }
            const DrakenVector* src = cond ? a : b;
            if (!fk_ite_branch_valid(src, cond ? a_isnull : b_isnull, i)) continue;
            const auto* sa = cond ? asa : bsa;
            uint32_t len = str_length(&sa->slots[src->selection[i]]);
            if (len > STR_INLINE_MAX) arena_len += len;
        }
        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* svalidity;
        uint8_t* block = vecresult_string_block_alloc(sn, arena_len, want_validity,
                                                      &slots, &arena, &svalidity);
        if (block == nullptr)
            return draken_error_sentinel("draken_if_then_else: allocation failed");
        if (want_validity) {
            size_t vb = (static_cast<size_t>(sn) + 7) / 8;
            std::memset(svalidity, 0xFF, vb > 0 ? vb : 1);
        }
        size_t arena_pos = 0;
        for (uint32_t i = 0; i < sn; ++i) {
            bool cond = false;
            if (fk_row_valid(m, i)) {
                uint32_t mp = m->selection[i];
                cond = (smbits[mp >> 3] >> (mp & 7)) & 1u;
            }
            const DrakenVector* src = cond ? a : b;
            if (!fk_ite_branch_valid(src, cond ? a_isnull : b_isnull, i)) {
                std::memset(&slots[i], 0, sizeof(DrakenStringSlot));
                svalidity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            const auto* sa = cond ? asa : bsa;
            const DrakenStringSlot* sslot = &sa->slots[src->selection[i]];
            const uint8_t* bytes = str_data(sslot, sa->arena);
            uint32_t len = str_length(sslot);
            if (len <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], bytes, len);
            } else {
                uint8_t* dst = arena + arena_pos;
                std::memcpy(dst, bytes, len);
                str_init_extern(&slots[i], dst, len,
                                static_cast<uint32_t>(arena_pos));
                arena_pos += len;
            }
        }
        return vecresult_from_string_block(block, sn, arena_len, want_validity, s_t);
    }

    // BOOL branches: bit-packed, not a fixed-width element array — blend bit by
    // bit rather than through the memcpy path below (which assumes `es` bytes
    // per row). Mirrors the STRING block's per-row cond-select shape.
    if (a_eff == DRAKEN_BOOL && b_eff == DRAKEN_BOOL) {
        uint32_t bn = m->length;
        size_t bnb = (static_cast<size_t>(bn) + 7) / 8;
        auto* bout = static_cast<uint8_t*>(draken_malloc(bnb > 0 ? bnb : 1));
        if (bout == nullptr) return draken_error_sentinel("allocation failed");
        std::memset(bout, 0, bnb > 0 ? bnb : 1);
        uint8_t* bvalidity = nullptr;
        if (force_validity || a->validity != nullptr || b->validity != nullptr) {
            bvalidity = static_cast<uint8_t*>(draken_malloc(bnb > 0 ? bnb : 1));
            if (bvalidity == nullptr) { draken_free(bout); return draken_error_sentinel("allocation failed"); }
            std::memset(bvalidity, 0xFF, bnb > 0 ? bnb : 1);
        }
        const auto* mbits = static_cast<const uint8_t*>(m->data);
        const auto* abits = a_isnull ? nullptr : static_cast<const uint8_t*>(a->data);
        const auto* bbits = b_isnull ? nullptr : static_cast<const uint8_t*>(b->data);
        for (uint32_t i = 0; i < bn; ++i) {
            bool cond = false;
            if (fk_row_valid(m, i)) {
                uint32_t mp = m->selection[i];
                cond = (mbits[mp >> 3] >> (mp & 7)) & 1u;
            }
            const DrakenVector* src = cond ? a : b;
            if (!fk_ite_branch_valid(src, cond ? a_isnull : b_isnull, i)) {
                if (bvalidity != nullptr)
                    bvalidity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            const uint8_t* sbits = cond ? abits : bbits;
            uint32_t sp = src->selection[i];
            if ((sbits[sp >> 3] >> (sp & 7)) & 1u)
                bout[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
        VecResult br{};
        br.data = bout;
        br.validity = bvalidity;
        br.selection = draken_identity_sel(bn);
        br.owns_selection = false;
        br.data_length = bn;
        br.length = bn;
        br.type = DRAKEN_BOOL;
        br.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return br;
    }

    DrakenType out_t = a_eff;
    bool widen = false;   // DECIMAL (int64 tier) side sign-extends to int128
    if (a_eff != b_eff) {
        // DECIMAL literal branches cannot materialize at the int128 tier, so a
        // {DECIMAL, DECIMAL128} pair is legitimate — the plan compiler
        // guarantees equal scales (raw-domain widening is then exact).
        if ((a_eff == DRAKEN_DECIMAL && b_eff == DRAKEN_DECIMAL128) ||
            (a_eff == DRAKEN_DECIMAL128 && b_eff == DRAKEN_DECIMAL)) {
            out_t = DRAKEN_DECIMAL128;
            widen = true;
        } else {
            return draken_error_sentinel_fmt(
                "draken_if_then_else: branch types differ (%d vs %d) — bind-time "
                "coercion missing", (int)a_eff, (int)b_eff);
        }
    }
    size_t es = fk_fixed_elem_size(out_t);
    if (es == 0)
        return draken_error_sentinel("draken_if_then_else: unsupported branch type");

    uint32_t n = m->length;
    auto* out = static_cast<uint8_t*>(draken_malloc((n > 0 ? n : 1) * es));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    uint8_t* validity = nullptr;
    if (force_validity || a->validity != nullptr || b->validity != nullptr) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, vb > 0 ? vb : 1);
    }
    const auto* mbits = static_cast<const uint8_t*>(m->data);
    for (uint32_t i = 0; i < n; ++i) {
        bool cond = false;
        if (fk_row_valid(m, i)) {
            uint32_t mp = m->selection[i];
            cond = (mbits[mp >> 3] >> (mp & 7)) & 1u;
        }
        const DrakenVector* src = cond ? a : b;
        if (!fk_ite_branch_valid(src, cond ? a_isnull : b_isnull, i)) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            std::memset(out + static_cast<size_t>(i) * es, 0, es);
            continue;
        }
        if (widen && src->type == DRAKEN_DECIMAL) {
            __int128 w = static_cast<const int64_t*>(src->data)[src->selection[i]];
            std::memcpy(out + static_cast<size_t>(i) * es, &w, 16);
            continue;
        }
        std::memcpy(out + static_cast<size_t>(i) * es,
                    static_cast<const uint8_t*>(src->data)
                        + static_cast<size_t>(src->selection[i]) * es, es);
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = out_t;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // extern "C"

// ---------------------------------------------------------------------------
// LIKE — draken_like(value, pattern), ctx.op_code: 0=LIKE 1=NOT LIKE 2=ILIKE 3=NOT ILIKE
// ---------------------------------------------------------------------------

namespace {

// fk_ascii_lower / fk_like_match now live in the shared glob_match.h so
// draken_like and draken_like_any share ONE implementation (§2). Thin aliases
// keep the existing call sites in this file unchanged.
inline uint8_t fk_ascii_lower(uint8_t c) { return draken_glob::ascii_lower(c); }
inline bool fk_like_match(const uint8_t* s, uint32_t sn, const uint8_t* p, uint32_t pn, bool ci) {
    return draken_glob::like_match(s, sn, p, pn, ci);
}
// ci_utf8 = ci && NVARCHAR haystack — Unicode codepoint fold instead of the
// ASCII byte fold above (draken_like's NVARCHAR+'_' guard already rejects
// the one shape this can't express — see call sites).
inline bool fk_like_match_ci(const uint8_t* s, uint32_t sn, const uint8_t* p, uint32_t pn,
                             bool ci, bool ci_utf8) {
    if (ci_utf8) return draken_utf8ci::like_match(s, sn, p, pn);
    return draken_glob::like_match(s, sn, p, pn, ci);
}

}  // namespace

extern "C" {

// STARTS_WITH / ENDS_WITH — the optimizer rewrites `LIKE 'x%'` → _STARTS_WITH,
// `LIKE '%x'` → _ENDS_WITH (CI variants for ILIKE; negation wraps in a NOT node,
// so no negate flag here). ctx.op_code bit1 = case-insensitive: ASCII byte-fold
// on VARCHAR, Unicode codepoint-fold (draken_utf8ci) on NVARCHAR.
VecResult fk_affix(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                   bool suffix, const char* who) {
    if (nargs != 2) return draken_error_sentinel_fmt("%s: expected 2 arguments", who);
    const bool ci = (ctx != nullptr) && ((static_cast<const binary_op_ctx*>(ctx)->op_code & 2) != 0);
    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel_fmt("%s: string operands required", who);
    // VARBINARY is a legal case-insensitive subject: it folds ASCII, exactly as
    // VARCHAR does — only NVARCHAR takes the Unicode codepoint fold. Refusing it
    // here left ILIKE the odd one out of a family where LIKE, NOT LIKE, RLIKE and
    // NOT RLIKE all accept a VARBINARY subject, and where VARBINARY is already a
    // legal ILIKE *pattern*. See draken_like for the same lift.
    const bool ci_utf8 = ci && v->type == DRAKEN_NVARCHAR;
    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const auto* psa = static_cast<const DrakenStringArena*>(p->data);
    uint32_t n = v->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr || p->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb > 0 ? nb : 1);
    }
    // One affix verdict for one (haystack slot, affix) pair. Extracted so the
    // per-DISTINCT fast path below and the per-row path cannot drift apart — the
    // failure mode of duplicating it is a shape-dependent WRONG ANSWER, which is
    // exactly what §11 forbids of a fast path.
    auto affix_hit = [&](const DrakenStringSlot* vs, const DrakenStringSlot* ps) -> bool {
        const uint8_t* hay = str_data(vs, vsa->arena);
        const uint8_t* aff = str_data(ps, psa->arena);
        const uint32_t hlen = str_length(vs), alen = str_length(ps);
        if (alen > hlen) return false;
        if (ci_utf8) {
            // Codepoint-based: folded byte length isn't guaranteed equal to
            // the source, so this can't reuse the fixed byte-offset `base`
            // the ASCII/binary paths below rely on.
            return suffix ? draken_utf8ci::ends_with(hay, hlen, aff, alen)
                          : draken_utf8ci::starts_with(hay, hlen, aff, alen);
        }
        const uint8_t* base = suffix ? hay + (hlen - alen) : hay;
        if (!ci) return std::memcmp(base, aff, alen) == 0;
        for (uint32_t k = 0; k < alen; ++k)
            if (fk_ascii_lower(base[k]) != fk_ascii_lower(aff[k])) return false;
        return true;
    };

    if (v->data_length < v->length && p->data_length == 1 && fk_row_valid(p, 0)) {
        // Dict/constant haystack, constant affix: one verdict per DISTINCT slot,
        // then scatter — data_length probes instead of length. Same gate and
        // structure as draken_contains, which already had this; without it an
        // ANCHORED match cost more than an unanchored one on the same column,
        // which is backwards for a strictly cheaper test.
        const DrakenStringSlot* ps = &psa->slots[p->selection[0]];
        std::vector<uint8_t> uhit(v->data_length > 0 ? v->data_length : 1, 0);
        for (uint32_t j = 0; j < v->data_length; ++j)
            uhit[j] = affix_hit(&vsa->slots[j], ps) ? 1 : 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (uhit[v->selection[i]]) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i) || !fk_row_valid(p, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (affix_hit(&vsa->slots[v->selection[i]], &psa->slots[p->selection[i]]))
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out; r.validity = validity; r.selection = draken_identity_sel(n);
    r.owns_selection = false; r.data_length = n; r.length = n;
    r.type = DRAKEN_BOOL; r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

VecResult draken_starts_with(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return fk_affix(ctx, args, nargs, false, "draken_starts_with");
}
VecResult draken_ends_with(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return fk_affix(ctx, args, nargs, true, "draken_ends_with");
}

// Single haystack/needle substring verdict (memchr-anchored; ci = ASCII fold,
// or Unicode codepoint fold via draken_utf8ci::contains when ci_utf8).
static inline bool fk_contains_hit(const uint8_t* hay, uint32_t hlen,
                                   const uint8_t* ndl, uint32_t nlen, bool ci,
                                   bool ci_utf8) {
    if (nlen == 0) return true;
    if (nlen > hlen) return false;
    if (ci_utf8) return draken_utf8ci::contains(hay, hlen, ndl, nlen);
    if (!ci) {
        const uint8_t first = ndl[0];
        const uint8_t* cur = hay;
        const uint8_t* end = hay + hlen - nlen + 1;
        while (cur < end) {
            const uint8_t* found = static_cast<const uint8_t*>(
                std::memchr(cur, first, static_cast<size_t>(end - cur)));
            if (found == nullptr) return false;
            if (nlen == 1 || std::memcmp(found + 1, ndl + 1, nlen - 1) == 0)
                return true;
            cur = found + 1;
        }
        return false;
    }
    for (uint32_t s0 = 0; s0 + nlen <= hlen; ++s0) {
        uint32_t k = 0;
        while (k < nlen && fk_ascii_lower(hay[s0 + k]) == fk_ascii_lower(ndl[k])) ++k;
        if (k == nlen) return true;
    }
    return false;
}

// CONTAINS — draken_contains(value, needle): the optimizer's InStr family
// (`LIKE '%x%'` rewritten to a plain substring test, no wildcard semantics).
// ctx.op_code: bit0 = negate, bit1 = case-insensitive (same encoding as LIKE).
// Compressed haystack + constant needle: probe each DISTINCT slot once, then
// scatter the per-slot verdicts (§11 fast path — bit-identical to the per-row
// scan). Otherwise per-row memchr-anchored search; NULL row → NULL out.
VecResult draken_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_contains: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_contains: missing bind-time ctx (mode)");
    const int mode = static_cast<const binary_op_ctx*>(ctx)->op_code;
    const bool negate = (mode & 1) != 0;
    const bool ci = (mode & 2) != 0;
    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel("draken_contains: string operands required");
    // VARBINARY folds ASCII like VARCHAR — see draken_like for the ruling. This is
    // the `ILIKE '%x%'` rewrite target, so it has to lift with it or the binder
    // would accept a predicate the optimizer then rewrites into a refusal.
    const bool ci_utf8 = ci && v->type == DRAKEN_NVARCHAR;

    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const auto* psa = static_cast<const DrakenStringArena*>(p->data);
    uint32_t n = v->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr || p->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb > 0 ? nb : 1);
    }
    if (v->data_length < v->length && p->data_length == 1 && fk_row_valid(p, 0)) {
        // dict/constant haystack, constant needle: one verdict per DISTINCT slot.
        const DrakenStringSlot* ps = &psa->slots[p->selection[0]];
        const uint8_t* ndl = str_data(ps, psa->arena);
        const uint32_t nlen = str_length(ps);
        std::vector<uint8_t> uhit(v->data_length > 0 ? v->data_length : 1, 0);
        for (uint32_t j = 0; j < v->data_length; ++j) {
            const DrakenStringSlot* vs = &vsa->slots[j];
            bool hit = fk_contains_hit(str_data(vs, vsa->arena), str_length(vs),
                                       ndl, nlen, ci, ci_utf8);
            uhit[j] = (hit != negate) ? 1 : 0;
        }
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (uhit[v->selection[i]])
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i) || !fk_row_valid(p, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            const DrakenStringSlot* vs = &vsa->slots[v->selection[i]];
            const DrakenStringSlot* ps = &psa->slots[p->selection[i]];
            bool hit = fk_contains_hit(
                str_data(vs, vsa->arena), str_length(vs),
                str_data(ps, psa->arena), str_length(ps), ci, ci_utf8);
            if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

VecResult draken_like(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_like: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_like: missing bind-time ctx (mode)");
    const int mode = static_cast<const binary_op_ctx*>(ctx)->op_code;
    const bool negate = (mode & 1) != 0;
    const bool ci = (mode & 2) != 0;
    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel("draken_like: string operands required");
    // ILIKE accepts a VARBINARY subject. Case folding on VARBINARY is the SAME
    // ASCII byte fold VARCHAR gets (draken_glob::like_match's `ci` flag); only
    // NVARCHAR needs the Unicode codepoint fold, which is what ci_utf8 selects.
    // Refusing VARBINARY made ILIKE the only member of the LIKE family that would
    // not take a VARBINARY subject — LIKE, NOT LIKE, RLIKE and NOT RLIKE all do,
    // and a VARBINARY *pattern* was already legal (`name ILIKE b'%a%'`) — so the
    // rule was "bytes are text enough for wildcards and for regex, but not for
    // case", which is not a rule anyone could infer.
    const bool ci_utf8 = ci && v->type == DRAKEN_NVARCHAR;

    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const auto* psa = static_cast<const DrakenStringArena*>(p->data);
    uint32_t n = v->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr || p->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb > 0 ? nb : 1);
    }
    if (draken_is_compressed(v) && p->data_length == 1 && fk_row_valid(p, 0)) {
        // §11 fast path: compressed (dict/constant) haystack + constant pattern —
        // run the wildcard matcher once per DISTINCT slot, scatter the verdicts
        // (bit-identical to the per-row loop). draken_is_compressed is the
        // canonical shape predicate (buffers.h), not an open-coded compare.
        const DrakenStringSlot* ps = &psa->slots[p->selection[0]];
        const uint32_t plen = str_length(ps);
        const uint8_t* pdat =
            reinterpret_cast<const uint8_t*>(str_data(ps, psa->arena));
        if (v->type == DRAKEN_NVARCHAR) {
            for (uint32_t k = 0; k < plen; ++k) {
                if (pdat[k] == '_') {
                    draken_free(out);
                    if (validity != nullptr) draken_free(validity);
                    return draken_error_sentinel(
                        "draken_like: '_' against NVARCHAR (UTF-8) is not byte-safe "
                        "— not implemented natively (fail loud, never wrong)");
                }
            }
        }
        std::vector<uint8_t> uhit(v->data_length > 0 ? v->data_length : 1, 0);
        for (uint32_t j = 0; j < v->data_length; ++j) {
            const DrakenStringSlot* vs = &vsa->slots[j];
            bool hit = fk_like_match_ci(
                reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena)),
                str_length(vs), pdat, plen, ci, ci_utf8);
            uhit[j] = (hit != negate) ? 1 : 0;
        }
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (uhit[v->selection[i]])
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i) || !fk_row_valid(p, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            const DrakenStringSlot* vs = &vsa->slots[v->selection[i]];
            const DrakenStringSlot* ps = &psa->slots[p->selection[i]];
            uint32_t plen = str_length(ps);
            const uint8_t* pdat = reinterpret_cast<const uint8_t*>(str_data(ps, psa->arena));
            if (v->type == DRAKEN_NVARCHAR) {
                for (uint32_t k = 0; k < plen; ++k) {
                    if (pdat[k] == '_') {
                        draken_free(out);
                        if (validity != nullptr) draken_free(validity);
                        return draken_error_sentinel(
                            "draken_like: '_' against NVARCHAR (UTF-8) is not byte-safe "
                            "— not implemented natively (fail loud, never wrong)");
                    }
                }
            }
            bool hit = fk_like_match_ci(
                reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena)), str_length(vs),
                pdat, plen, ci, ci_utf8);
            if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// Length-adaptive LIKE — same answer as draken_like, chosen matcher varies by
// the column's average string length: a plan-time LIKE-DFA blob (ctx) on SHORT
// columns (the DFA table-walk wins ~2.2x there), the glob matcher on long ones
// (its early-exit backtrack wins). Only ever emitted for a constant, ASCII,
// case-sensitive pattern over a VARCHAR column whose glob DID NOT reduce to an
// affix/contains rewrite — so args[1] is the single glob pattern, ctx.blob the
// verified-equivalent DFA. Both paths dict-dedup + scatter identically; the
// length test can only change speed, never the result (§11).
VecResult draken_like_adaptive(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_like_adaptive: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_like_adaptive: missing bind-time ctx");
    const auto* c = static_cast<const like_dfa_ctx*>(ctx);
    const bool negate = (c->op_code & 1) != 0;
    const bool ci = (c->op_code & 2) != 0;
    const uint8_t* blob = like_dfa_ctx_blob(c);
    const size_t blob_len = c->blob_len;

    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel("draken_like_adaptive: string operands required");

    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const auto* psa = static_cast<const DrakenStringArena*>(p->data);
    const uint32_t n = v->length;
    const size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;

    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr || p->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
    }

    auto finish = [&]() -> VecResult {
        VecResult r{};
        r.data = out; r.validity = validity;
        r.selection = draken_identity_sel(n); r.owns_selection = false;
        r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    };

    // NULL pattern -> every row NULL (SQL comparison-with-NULL).
    if (p->validity != nullptr && !fk_row_valid(p, 0)) {
        std::memset(validity, 0x00, nb_alloc);
        return finish();
    }

    const uint32_t k = v->data_length;                 // distinct physical values
    // Average string length estimate (sampled) chooses the matcher.
    uint32_t sample = k < 256u ? k : 256u;
    uint64_t total = 0;
    for (uint32_t j = 0; j < sample; ++j) total += str_length(&vsa->slots[j]);
    const uint32_t avg = sample > 0u ? static_cast<uint32_t>(total / sample) : 0u;
    const bool use_dfa = (blob_len > 0u) && (avg < c->threshold);

    // The LIKE-DFA blob is plan-time-produced; validate its format ONCE (a -1
    // here means compiler/kernel drift), then trust it in the hot loop.
    if (use_dfa &&
        draken_dfa::match(blob, blob_len, reinterpret_cast<const uint8_t*>(""), 0) < 0) {
        draken_free(out); if (validity) draken_free(validity);
        return draken_error_sentinel("draken_like_adaptive: malformed LIKE-DFA blob");
    }

    const DrakenStringSlot* ps = &psa->slots[p->selection[0]];
    const uint8_t* pdat = reinterpret_cast<const uint8_t*>(str_data(ps, psa->arena));
    const uint32_t plen = str_length(ps);
    auto matches = [&](const DrakenStringSlot* vs) -> bool {
        const uint8_t* sd = reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena));
        const uint32_t sl = str_length(vs);
        return use_dfa ? (draken_dfa::match(blob, blob_len, sd, sl) != 0)
                       : fk_like_match(sd, sl, pdat, plen, ci);
    };

    // Dedup keyed on the canonical shape predicate draken_is_compressed(v) — NOT
    // an open-coded data_length compare (buffers.h §"Shape predicates"). A
    // compressed vector (dict/constant) matches each unique once and scatters;
    // a dense vector matches per row (no extra scatter pass).
    if (draken_is_compressed(v)) {
        std::vector<uint8_t> uhit(k > 0u ? k : 1u, 0);
        for (uint32_t j = 0; j < k; ++j)
            uhit[j] = (matches(&vsa->slots[j]) != negate) ? 1 : 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (uhit[v->selection[i]])
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (matches(&vsa->slots[v->selection[i]]) != negate)
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    return finish();
}

// SIMD op-program LIKE — draken_like_program(value, pattern). The optimizer
// compiles a decomposable ASCII glob (anchored fixed prefix + %-separated
// floating literals + optional anchored suffix) into a version-2 op-program
// blob (compile_like_program), which rides in like_dfa_ctx (threshold unused).
// draken_like_prog::match walks it with SIMD substring scans — O(segments),
// not O(bytes). Non-decomposable globs never reach here (the bind site keeps
// the transition-table draken_like_adaptive / glob path for those). Same
// dict-dedup + scatter, same NULL semantics as draken_like_adaptive; the op
// program can only change SPEED, never the answer (§11).
VecResult draken_like_program(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_like_program: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_like_program: missing bind-time ctx");
    const auto* c = static_cast<const like_dfa_ctx*>(ctx);
    const bool negate = (c->op_code & 1) != 0;
    const uint8_t* blob = like_dfa_ctx_blob(c);
    const size_t blob_len = c->blob_len;

    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel("draken_like_program: string operands required");

    const auto* vsa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t n = v->length;
    const size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;

    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr || p->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
    }

    auto finish = [&]() -> VecResult {
        VecResult r{};
        r.data = out; r.validity = validity;
        r.selection = draken_identity_sel(n); r.owns_selection = false;
        r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    };

    // NULL pattern -> every row NULL (SQL comparison-with-NULL).
    if (p->validity != nullptr && !fk_row_valid(p, 0)) {
        std::memset(validity, 0x00, nb_alloc);
        return finish();
    }

    // The op-program blob is plan-time-produced; decode its format ONCE (a
    // failure means compiler/kernel drift), then walk the decoded program per
    // row with no blob re-parsing.
    draken_like_prog::LikeProgram prog;
    if (!draken_like_prog::decode(blob, blob_len, &prog)) {
        draken_free(out); if (validity) draken_free(validity);
        return draken_error_sentinel("draken_like_program: malformed op-program blob");
    }

    const uint32_t k = v->data_length;
    auto matches = [&](const DrakenStringSlot* vs) -> bool {
        const uint8_t* sd = reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena));
        const uint32_t sl = str_length(vs);
        return draken_like_prog::match(&prog, sd, sl) != 0;
    };

    // Dict/constant: match each unique value once, then scatter (§11). Keyed on
    // the canonical shape predicate, identical to draken_like_adaptive.
    if (draken_is_compressed(v)) {
        std::vector<uint8_t> uhit(k > 0u ? k : 1u, 0);
        for (uint32_t j = 0; j < k; ++j)
            uhit[j] = (matches(&vsa->slots[j]) != negate) ? 1 : 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (uhit[v->selection[i]])
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            if (matches(&vsa->slots[v->selection[i]]) != negate)
                out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    return finish();
}

// ---------------------------------------------------------------------------
// IN-list membership — draken_in_list(value); the set rides in in_list_ctx.
// ---------------------------------------------------------------------------

// Mixed-type NUMERIC comparison — draken_compare_dv declines any type mismatch
// (DECIMAL vs FLOAT64, INT vs FLOAT, DECIMAL vs DECIMAL128, cross-scale DECIMAL).
// Two paths, chosen from the operand types at runtime:
//   • BOTH decimal-family → promote to int128, rescale to common scale — EXACT.
//   • otherwise (any float) → promote both to double (decimals divided by
//     10^scale) and compare — matches SQL's numeric-promotion semantics.
// ctx = binary_op_ctx: op_code (1=Eq 2=NotEq 3=Lt 4=Gt 5=LtEq 6=GtEq),
// left_scale, right_scale (0 for non-decimal).
//
// NULLS: a null on either side makes the comparison UNKNOWN, which is a cleared
// VALIDITY bit, not a definite false. Clearing only the data bit is enough for a
// WHERE filter (which drops the row either way) but it is a silent wrong answer
// for anything that reads the third value — `(a > b) IS NULL` answered FALSE for
// every row. Result validity is the AND of both operands', matching the
// contract draken_compare_dv's own paths obey (int64_compare.h): null row →
// validity bit 0 AND data bit 0.
inline bool fk_is_float_t(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// Read any integer-or-decimal operand as a RAW int128 (scale-0 for ints; the
// caller rescales by 10^(maxscale-own_scale) to a common scale — exact).
inline __int128 fk_read_dec(const DrakenVector* v, uint32_t phys) {
    switch (v->type) {
        case DRAKEN_DECIMAL128: return static_cast<const __int128*>(v->data)[phys];
        case DRAKEN_INT8:       return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:      return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:      return static_cast<const int32_t*>(v->data)[phys];
        default:                return static_cast<const int64_t*>(v->data)[phys];  // INT64/DECIMAL
    }
}

inline __int128 fk_pow10_i128(int e) {
    __int128 r = 1;
    for (int i = 0; i < e; ++i) r *= 10;
    return r;
}

inline double fk_read_num_double(const DrakenVector* v, uint32_t phys, int scale) {
    switch (v->type) {
        case DRAKEN_FLOAT32: return static_cast<const float*>(v->data)[phys];
        case DRAKEN_FLOAT64: return static_cast<const double*>(v->data)[phys];
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[phys];
        case DRAKEN_DECIMAL128: {
            double d = static_cast<double>(fk_read_dec(v, phys));
            for (int i = 0; i < scale; ++i) d /= 10.0;
            return d;
        }
        case DRAKEN_DECIMAL: {
            double d = static_cast<double>(static_cast<const int64_t*>(v->data)[phys]);
            for (int i = 0; i < scale; ++i) d /= 10.0;
            return d;
        }
        default:             return static_cast<double>(static_cast<const int64_t*>(v->data)[phys]);
    }
}

// op: 1=Eq 2=NotEq 3=Lt 4=Gt 5=LtEq 6=GtEq. Two non-template overloads — a
// template can't live in the extern "C" block this kernel is declared in.
inline bool fk_cmp_i128(int op, __int128 a, __int128 b) {
    switch (op) {
        case 1: return a == b; case 2: return a != b; case 3: return a <  b;
        case 4: return a >  b; case 5: return a <= b; default: return a >= b;
    }
}
inline bool fk_cmp_dbl(int op, double a, double b) {
    switch (op) {
        case 1: return a == b; case 2: return a != b; case 3: return a <  b;
        case 4: return a >  b; case 5: return a <= b; default: return a >= b;
    }
}

VecResult draken_numeric_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_numeric_cmp: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_numeric_cmp: missing ctx (op + scales)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    const int op = c->op_code;
    if (op < 1 || op > 6)
        return draken_error_sentinel("draken_numeric_cmp: bad op_code");
    const int ls = c->left_scale, rs = c->right_scale;
    const DrakenVector* L = args[0];
    const DrakenVector* R = args[1];
    // Exact int128 path whenever NO float is involved (ints are scale-0 decimals);
    // double promotion only when a float operand forces it (SQL numeric semantics).
    const bool any_float = fk_is_float_t(L->type) || fk_is_float_t(R->type);

    uint32_t n = L->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    // UNKNOWN rows need a cleared validity bit, not just a cleared data bit.
    // Allocated all-valid only when an operand can carry a null; bits are cleared
    // in the compare loop below, so the mask is exact for either shape of operand.
    uint8_t* validity = nullptr;
    if (L->validity != nullptr || R->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
        if ((n & 7u) != 0)
            validity[nb - 1] = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    if (!any_float) {
        const int maxs = ls > rs ? ls : rs;
        const __int128 lmul = fk_pow10_i128(maxs - ls);
        const __int128 rmul = fk_pow10_i128(maxs - rs);
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            __int128 a = fk_read_dec(L, L->selection[i]) * lmul;
            __int128 b = fk_read_dec(R, R->selection[i]) * rmul;
            if (fk_cmp_i128(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) {
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            double a = fk_read_num_double(L, L->selection[i], ls);
            double b = fk_read_num_double(R, R->selection[i], rs);
            if (fk_cmp_dbl(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// Read one temporal row and promote it to a common fine domain: NANOSECONDS,
// held in __int128 so no date (days×86.4e12) or ns TIMESTAMP ever overflows.
// DATE32 rows are days-since-epoch (unit-less — SQL promotes a DATE to the
// instant at midnight); TIMESTAMP64 rows carry their unit in ctx (0=s,1=ms,
// 2=us,3=ns). The vector's OWN type tag says which it is — the unit is only
// consulted for the TIMESTAMP64 side. Kept in ns (not us) so a nanosecond
// TIMESTAMP compares exactly rather than being truncated.
inline __int128 fk_temporal_to_ns(const DrakenVector* v, uint32_t phys,
                                  unsigned char unit) {
    if (v->type == DRAKEN_DATE32) {
        return static_cast<__int128>(static_cast<const int32_t*>(v->data)[phys])
               * static_cast<__int128>(86400000000000LL);   // days -> ns
    }
    static const int64_t unit_to_ns[4] = {1000000000LL, 1000000LL, 1000LL, 1LL};
    return static_cast<__int128>(static_cast<const int64_t*>(v->data)[phys])
           * static_cast<__int128>(unit_to_ns[unit & 3]);   // {s,ms,us,ns} -> ns
}

// Mixed-domain TEMPORAL comparison. draken_compare_dv's fast path declines any
// type mismatch (DATE32 vs TIMESTAMP64) and silently mis-compares two TIMESTAMP64
// operands carried at DIFFERENT units (raw-int ordering ignores the unit) — both
// leave a WHERE predicate wrong with no fallback on the native engine. The
// compiler routes here ONLY when the two temporal operands differ in physical
// type OR unit (a matched date/date or same-unit ts/ts pair stays on the fast
// draken_compare_dv). Both sides promote to ns in __int128, then compare.
//
// NULLS: same contract as draken_numeric_cmp above — a null on either side is
// UNKNOWN, which is a cleared VALIDITY bit, not a definite false. Result
// validity is the AND of both operands'.
VecResult draken_temporal_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_temporal_cmp: expected 2 arguments");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_temporal_cmp: missing ctx (op + units)");
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    const int op = c->op_code;
    if (op < 1 || op > 6)
        return draken_error_sentinel("draken_temporal_cmp: bad op_code");
    const DrakenVector* L = args[0];
    const DrakenVector* R = args[1];
    if ((L->type != DRAKEN_DATE32 && L->type != DRAKEN_TIMESTAMP64) ||
        (R->type != DRAKEN_DATE32 && R->type != DRAKEN_TIMESTAMP64))
        return draken_error_sentinel("draken_temporal_cmp: DATE/TIMESTAMP operands required");

    uint32_t n = L->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    uint8_t* validity = nullptr;
    if (L->validity != nullptr || R->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
        if ((n & 7u) != 0)
            validity[nb - 1] = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            continue;
        }
        __int128 a = fk_temporal_to_ns(L, L->selection[i], c->left_unit);
        __int128 b = fk_temporal_to_ns(R, R->selection[i], c->right_unit);
        if (fk_cmp_i128(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// col <> '' / col = '' lower to UNARY IsNotEmpty/IsEmpty.
//
// NULLS: a null operand makes the predicate UNKNOWN, which is a cleared VALIDITY
// bit — NOT a definite false with the validity left all-valid. That older
// spelling was enough for the WHERE filter this lowering was built for (the row
// is dropped either way) and wrong for everything that reads the third value:
// `(s <> '') IS NULL` answered FALSE for every row. Same contract as
// draken_numeric_cmp / draken_temporal_cmp above; one operand, so the result
// validity is a copy of the input's.
VecResult draken_string_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                              bool want_empty) {
    if (nargs != 1)
        return draken_error_sentinel("draken_string_empty: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fk_is_string(v->type))
        return draken_error_sentinel("draken_string_empty: string operand required");
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    uint32_t n = v->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
        if ((n & 7u) != 0)
            validity[nb - 1] = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));   // NULL → UNKNOWN
            continue;
        }
        bool empty = str_length(&sa->slots[v->selection[i]]) == 0;
        if (empty == want_empty) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// BITWISE NOT (~x) over the integer family — widened INT64 result (dense).
VecResult draken_bitwise_not(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_bitwise_not: expected 1 argument");
    const DrakenVector* v = args[0];
    switch (v->type) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            break;
        default:
            return draken_error_sentinel("draken_bitwise_not: integer operand required");
    }
    uint32_t n = v->length;
    auto* out = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) { out[i] = 0; continue; }
        uint32_t phys = v->selection[i];
        int64_t x;
        switch (v->type) {
            case DRAKEN_INT8:  x = static_cast<const int8_t*>(v->data)[phys]; break;
            case DRAKEN_INT16: x = static_cast<const int16_t*>(v->data)[phys]; break;
            case DRAKEN_INT32: x = static_cast<const int32_t*>(v->data)[phys]; break;
            default:           x = static_cast<const int64_t*>(v->data)[phys]; break;
        }
        out[i] = ~x;
    }
    return fk_numeric_result(v, out, n, DRAKEN_INT64);
}

VecResult draken_is_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return draken_string_empty(ctx, args, nargs, true);
}

VecResult draken_is_not_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    return draken_string_empty(ctx, args, nargs, false);
}

// SUBSTRING(str, start, count) — start 1-based (SQL); Python-slice semantics after
// `start -= 1` when start > 0. VARCHAR/VARBINARY byte-indexed; NVARCHAR codepoint-
// indexed (UTF-8 aware, matches Python str slicing). ctx = substring_ctx.
inline uint32_t fk_cp_to_byte(const uint8_t* data, uint32_t blen, long target) {
    long cnt = 0;
    uint32_t bb = 0;
    for (; bb < blen; ++bb) {
        if ((data[bb] & 0xC0u) != 0x80u) {
            if (cnt == target) return bb;
            ++cnt;
        }
    }
    return blen;
}

inline void fk_substr_range(const uint8_t* data, uint32_t blen, bool is_utf8,
                            long start0, bool has_count, long count,
                            uint32_t* out_off, uint32_t* out_len) {
    long unit_len;
    if (!is_utf8) {
        unit_len = blen;
    } else {
        unit_len = 0;
        for (uint32_t b = 0; b < blen; ++b) if ((data[b] & 0xC0u) != 0x80u) ++unit_len;
    }
    long s = start0;
    if (s < 0) { s += unit_len; if (s < 0) s = 0; } else if (s > unit_len) s = unit_len;
    long e;
    if (has_count) {
        e = count + start0;
        if (e < 0) { e += unit_len; if (e < 0) e = 0; } else if (e > unit_len) e = unit_len;
    } else {
        e = unit_len;
    }
    if (e < s) e = s;
    if (!is_utf8) {
        *out_off = static_cast<uint32_t>(s);
        *out_len = static_cast<uint32_t>(e - s);
    } else {
        uint32_t bs = fk_cp_to_byte(data, blen, s);
        uint32_t be = fk_cp_to_byte(data, blen, e);
        *out_off = bs;
        *out_len = be - bs;
    }
}

VecResult draken_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_substring: expected 1 argument");
    if (ctx == nullptr) return draken_error_sentinel("draken_substring: missing ctx");
    const auto* c = static_cast<const substring_ctx*>(ctx);
    const DrakenVector* v = args[0];
    if (!fk_is_string(v->type))
        return draken_error_sentinel("draken_substring: string operand required");
    const bool is_utf8 = (v->type == DRAKEN_NVARCHAR);
    const bool has_count = c->has_count != 0;
    long start0 = c->start;
    if (start0 > 0) start0 -= 1;

    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k = v->data_length;   // physical unique count

    // SHAPE-PRESERVING (see ascii_case_transform): the substring range is a pure
    // function of a physical value's bytes plus the bind-time start/count, so it is
    // computed ONCE per data_length physical value, then kernel_preserve_shape
    // carries the input's selection + per-logical-row validity onto the result.
    // Substring changes lengths per value, so the arena is sized over the K outputs.

    // Pass 1: total long-form bytes over the K physical substring outputs.
    size_t arena_len = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        uint32_t off, len;
        fk_substr_range(str_data(slot, sa->arena), str_length(slot), is_utf8,
                        start0, has_count, c->count, &off, &len);
        if (len > STR_INLINE_MAX) arena_len += len;
    }

    // K-slot physical block, NO embedded validity (per-logical-row nulls come from
    // kernel_preserve_shape).
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel("draken_substring: allocation failed");

    size_t arena_pos = 0;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        const uint8_t* src = str_data(slot, sa->arena);
        uint32_t off, len;
        fk_substr_range(src, str_length(slot), is_utf8, start0, has_count, c->count, &off, &len);
        const uint8_t* sub = src + off;
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], sub, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, sub, len);
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }

    // Carry the input's shape onto the dense K-block (see ascii_case_transform for
    // the identity-replacement and bad_alloc-containment rationale).
    VecResult r = vecresult_from_string_block(block, k, arena_len, /*has_validity=*/0, v->type);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel("draken_substring: shape-carry allocation failed");
    }
    return r;
}

// SUBSTRING(str, start_col, count_col) / LEFT(str, n_col) / RIGHT(str, n_col) — the
// column-valued sibling of draken_substring above. That kernel's ctx bakes start/count
// in at COMPILE time and reduces work over data_length PHYSICAL uniques (shape-
// preserving); here start/count come from vector OPERANDS and vary per LOGICAL row,
// so neither the ctx nor the physical-dict reduction applies — every row is resolved
// independently and the output is built dense over `length`, mirroring
// draken_random_string's two-pass (size-then-emit) pattern. `mode` distinguishes how
// the 1-2 non-string args map to (start, has_count, count), matching the three shapes
// draken_substring's literal callers already use:
//   SUBSTRING: args = [str, start, count?]  — start from args[1], optional count args[2]
//   LEFT:      args = [str, n]              — start fixed at 0 (SQL 1), count = n
//   RIGHT:     args = [str, n]              — start = -n, no count (runs to end)
// Three tiny extern "C" entry points (draken_substring_dynamic/_left_dynamic/
// _right_dynamic) share one implementation and carry the mode — no ctx needed.
enum FkSubstrDynMode : uint8_t {
    FK_SUBSTR_DYN_SUBSTRING = 0,
    FK_SUBSTR_DYN_LEFT = 1,
    FK_SUBSTR_DYN_RIGHT = 2,
};

// Resolve one logical row's (start0, has_count, count) per `mode` and compute its
// substring [off, len) via fk_substr_range. Returns false (row is NULL) when the
// string, start, or count operand is NULL at this row — TVL null propagation.
inline bool fk_substr_dyn_row(const DrakenVector* sv, const DrakenVector* a1,
                              const DrakenVector* a2, FkSubstrDynMode mode, bool is_utf8,
                              uint32_t row, uint32_t* out_off, uint32_t* out_len) {
    if (!fk_row_valid(sv, row)) return false;
    long start0;
    bool has_count;
    long count = 0;
    switch (mode) {
        case FK_SUBSTR_DYN_LEFT: {
            if (!fk_row_valid(a1, row)) return false;
            start0 = 0;   // SQL start=1 -> 0-based 0 (matches literal LEFT: start=1)
            has_count = true;
            count = static_cast<long>(fk_read_int64(a1, row));
            break;
        }
        case FK_SUBSTR_DYN_RIGHT: {
            if (!fk_row_valid(a1, row)) return false;
            const long n = static_cast<long>(fk_read_int64(a1, row));
            start0 = -n;   // matches literal RIGHT: start=-n, has_count=0 (runs to end)
            has_count = false;
            break;
        }
        default: {   // FK_SUBSTR_DYN_SUBSTRING
            if (!fk_row_valid(a1, row)) return false;
            const long start = static_cast<long>(fk_read_int64(a1, row));
            start0 = start > 0 ? start - 1 : start;
            has_count = (a2 != nullptr);
            if (has_count) {
                if (!fk_row_valid(a2, row)) return false;
                count = static_cast<long>(fk_read_int64(a2, row));
            }
            break;
        }
    }
    const uint32_t phys = sv->selection[row];
    const auto* sa = static_cast<const DrakenStringArena*>(sv->data);
    const DrakenStringSlot* slot = &sa->slots[phys];
    fk_substr_range(str_data(slot, sa->arena), str_length(slot), is_utf8,
                    start0, has_count, count, out_off, out_len);
    return true;
}

VecResult fk_substring_dynamic_impl(FkSubstrDynMode mode, const DrakenVector* const* args,
                                    uint32_t nargs) {
    if (mode == FK_SUBSTR_DYN_SUBSTRING) {
        if (nargs != 2 && nargs != 3)
            return draken_error_sentinel("draken_substring_dynamic: expected 2 or 3 arguments");
    } else if (nargs != 2) {
        return draken_error_sentinel("draken_substring_dynamic: expected 2 arguments");
    }
    const DrakenVector* sv = args[0];
    if (!fk_is_string(sv->type))
        return draken_error_sentinel("draken_substring_dynamic: string operand required");
    const DrakenVector* a1 = args[1];
    const DrakenVector* a2 = (mode == FK_SUBSTR_DYN_SUBSTRING && nargs == 3) ? args[2] : nullptr;
    const bool is_utf8 = (sv->type == DRAKEN_NVARCHAR);
    const uint32_t n = sv->length;

    try {
        std::vector<uint8_t> row_valid(n);
        std::vector<uint32_t> row_off(n), row_len(n);
        size_t arena_cap = 0;
        for (uint32_t i = 0; i < n; ++i) {
            uint32_t off = 0, len = 0;
            const bool valid = fk_substr_dyn_row(sv, a1, a2, mode, is_utf8, i, &off, &len);
            row_valid[i] = valid ? 1 : 0;
            row_off[i] = off;
            row_len[i] = len;
            if (valid && len > STR_INLINE_MAX) arena_cap += len;
        }

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* validity;
        uint8_t* block = vecresult_string_block_alloc(n, arena_cap, /*want_validity=*/1,
                                                       &slots, &arena, &validity);
        if (block == nullptr)
            return draken_error_sentinel("draken_substring_dynamic: allocation failed");

        const auto* sa = static_cast<const DrakenStringArena*>(sv->data);
        std::memset(validity, 0, (static_cast<size_t>(n) + 7) / 8);
        size_t arena_pos = 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (!row_valid[i]) { str_init_null(&slots[i]); continue; }
            validity[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            const uint32_t phys = sv->selection[i];
            const DrakenStringSlot* src_slot = &sa->slots[phys];
            const uint8_t* src = str_data(src_slot, sa->arena) + row_off[i];
            const uint32_t len = row_len[i];
            if (len <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], src, len);
            } else {
                uint8_t* dst = arena + arena_pos;
                std::memcpy(dst, src, len);
                str_init_extern(&slots[i], dst, len,
                                static_cast<uint32_t>(arena_pos));
                arena_pos += len;
            }
        }
        return vecresult_from_string_block(block, n, arena_cap, /*has_validity=*/1, sv->type);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_substring_dynamic: unknown error");
    }
}

VecResult draken_substring_dynamic(void* /*ctx*/, const DrakenVector* const* args,
                                   uint32_t nargs) {
    return fk_substring_dynamic_impl(FK_SUBSTR_DYN_SUBSTRING, args, nargs);
}

VecResult draken_left_dynamic(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return fk_substring_dynamic_impl(FK_SUBSTR_DYN_LEFT, args, nargs);
}

VecResult draken_right_dynamic(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return fk_substring_dynamic_impl(FK_SUBSTR_DYN_RIGHT, args, nargs);
}

VecResult draken_in_list(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1)
        return draken_error_sentinel("draken_in_list: expected 1 argument");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_in_list: missing bind-time ctx (value set)");
    const auto* c = static_cast<const in_list_ctx*>(ctx);
    const uint8_t* payload = reinterpret_cast<const uint8_t*>(c) + sizeof(in_list_ctx);
    const DrakenVector* v = args[0];
    const bool negate = c->negate != 0;

    uint32_t n = v->length;
    size_t nb = (static_cast<size_t>(n) + 7) / 8;
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);
    uint8_t* validity = nullptr;
    if (v->validity != nullptr) {
        validity = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memcpy(validity, v->validity, nb > 0 ? nb : 1);
    }

    if (c->kind == 0) {   // int64 raw, sorted ascending — binary search per row
        const auto* items = reinterpret_cast<const int64_t*>(payload);
        int64_t val;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) continue;
            uint32_t phys = v->selection[i];
            switch (v->type) {
                case DRAKEN_INT8:   val = static_cast<const int8_t*>(v->data)[phys]; break;
                case DRAKEN_INT16:  val = static_cast<const int16_t*>(v->data)[phys]; break;
                case DRAKEN_INT32:
                case DRAKEN_DATE32: val = static_cast<const int32_t*>(v->data)[phys]; break;
                case DRAKEN_INT64:
                case DRAKEN_DECIMAL:
                case DRAKEN_TIMESTAMP64:
                    val = static_cast<const int64_t*>(v->data)[phys]; break;
                default:
                    draken_free(out);
                    if (validity != nullptr) draken_free(validity);
                    return draken_error_sentinel(
                        "draken_in_list: integer-family operand required for kind-0 set");
            }
            bool hit = std::binary_search(items, items + c->count, val);
            if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else if (c->kind == 3) {   // uint64 raw, sorted ascending — binary search per row
        const auto* items = reinterpret_cast<const uint64_t*>(payload);
        uint64_t val;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) continue;
            uint32_t phys = v->selection[i];
            switch (v->type) {
                case DRAKEN_UINT8:  val = static_cast<const uint8_t*>(v->data)[phys]; break;
                case DRAKEN_UINT16: val = static_cast<const uint16_t*>(v->data)[phys]; break;
                case DRAKEN_UINT32: val = static_cast<const uint32_t*>(v->data)[phys]; break;
                case DRAKEN_UINT64: val = static_cast<const uint64_t*>(v->data)[phys]; break;
                default:
                    draken_free(out);
                    if (validity != nullptr) draken_free(validity);
                    return draken_error_sentinel(
                        "draken_in_list: unsigned-integer operand required for kind-3 set");
            }
            bool hit = std::binary_search(items, items + c->count, val);
            if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {              // string entries: (u32 len + bytes), linear scan
        if (!fk_is_string(v->type)) {
            draken_free(out);
            if (validity != nullptr) draken_free(validity);
            return draken_error_sentinel(
                "draken_in_list: string operand required for kind-1 set");
        }
        const auto* sa = static_cast<const DrakenStringArena*>(v->data);
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(v, i)) continue;
            const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
            uint32_t vlen = str_length(slot);
            const uint8_t* vdat = reinterpret_cast<const uint8_t*>(str_data(slot, sa->arena));
            bool hit = false;
            const uint8_t* p = payload;
            for (uint32_t e = 0; e < c->count; ++e) {
                uint32_t elen;
                std::memcpy(&elen, p, 4);
                p += 4;
                if (elen == vlen && std::memcmp(p, vdat, elen) == 0) { hit = true; break; }
                p += elen;
            }
            if (hit != negate) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // extern "C"
