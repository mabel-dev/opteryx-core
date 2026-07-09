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
#include "xxhash.h"                // XXH3_64bits — long-slot hash32, same as every builder

namespace {

inline bool fk_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u);
}

inline bool fk_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// Shared ASCII case transform (VARCHAR/VARBINARY only — see file header).
VecResult ascii_case_transform(const DrakenVector* v, bool to_upper, const char* who) {
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_VARBINARY) {
        return draken_error_sentinel_fmt(
            "%s: VARCHAR or VARBINARY (ASCII-range byte fold) input required — "
            "NVARCHAR case mapping is not implemented natively yet (fail loud, "
            "never wrong)", who);
    }
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    uint32_t n = v->length;

    // Pass 1: exact long-form byte total (lengths are unchanged by casing).
    size_t arena_len = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) continue;
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        uint32_t len = str_length(slot);
        if (len > STR_INLINE_MAX) arena_len += len;
    }
    int want_validity = (v->validity != nullptr) ? 1 : 0;
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity;
    uint8_t* block = vecresult_string_block_alloc(n, arena_len, want_validity,
                                                  &slots, &arena, &validity);
    if (block == nullptr) return draken_error_sentinel("ascii_case: allocation failed");
    if (want_validity) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        std::memcpy(validity, v->validity, vb > 0 ? vb : 1);
    }

    uint8_t buf_inline[STR_INLINE_MAX];
    size_t arena_pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) {
            std::memset(&slots[i], 0, sizeof(DrakenStringSlot));
            continue;
        }
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        uint32_t len = str_length(slot);
        const uint8_t* src = str_data(slot, sa->arena);
        if (len <= STR_INLINE_MAX) {
            for (uint32_t b = 0; b < len; ++b) {
                uint8_t c = src[b];
                buf_inline[b] = to_upper
                    ? ((c >= 'a' && c <= 'z') ? c - 32 : c)
                    : ((c >= 'A' && c <= 'Z') ? c + 32 : c);
            }
            str_init_inline(&slots[i], buf_inline, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            for (uint32_t b = 0; b < len; ++b) {
                uint8_t c = src[b];
                dst[b] = to_upper
                    ? ((c >= 'a' && c <= 'z') ? c - 32 : c)
                    : ((c >= 'A' && c <= 'Z') ? c + 32 : c);
            }
            str_init_extern(&slots[i], dst, len,
                            static_cast<uint32_t>(XXH3_64bits(dst, len)),
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }
    return vecresult_from_string_block(block, n, arena_len, want_validity, v->type);
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
// All accept INT8..64 / FLOAT32/64 operands, read uniformly via data[selection[i]],
// and preserve the input's validity (copied). DECIMAL operands fail loud — raw-scale
// math without the scale is a wrong answer, never a fallback.

namespace {

inline bool fk_is_numeric(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            return true;
        default:
            return false;
    }
}

inline bool fk_is_float(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

inline double fk_read_double(const DrakenVector* v, uint32_t row) {
    uint32_t phys = v->selection[row];
    switch (v->type) {
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[phys];
        case DRAKEN_INT64:   return static_cast<double>(static_cast<const int64_t*>(v->data)[phys]);
        case DRAKEN_FLOAT32: return static_cast<const float*>(v->data)[phys];
        default:             return static_cast<const double*>(v->data)[phys];
    }
}

inline int64_t fk_read_int64(const DrakenVector* v, uint32_t row) {
    uint32_t phys = v->selection[row];
    switch (v->type) {
        case DRAKEN_INT8:    return static_cast<const int8_t*>(v->data)[phys];
        case DRAKEN_INT16:   return static_cast<const int16_t*>(v->data)[phys];
        case DRAKEN_INT32:   return static_cast<const int32_t*>(v->data)[phys];
        default:             return static_cast<const int64_t*>(v->data)[phys];
    }
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

// Unary numeric kernel body: INT-family -> INT64 via `ifn`; floats -> FLOAT64 via
// `ffn` (SIGN forces INT64 output for floats too via `force_int`).
template <typename IFn, typename FFn>
VecResult fk_unary_numeric(const DrakenVector* const* args, uint32_t nargs,
                           const char* who, IFn ifn, FFn ffn, bool force_int) {
    if (nargs != 1) return draken_error_sentinel_fmt("%s: expected 1 argument", who);
    const DrakenVector* v = args[0];
    if (!fk_is_numeric(v->type))
        return draken_error_sentinel_fmt("%s: numeric input required", who);
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
        return draken_error_sentinel_fmt("%s: numeric input required", who);
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

VecResult draken_abs(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return fk_unary_numeric(args, nargs, "draken_abs",
                            [](int64_t x) { return x < 0 ? -x : x; },
                            [](double x) { return std::fabs(x); }, false);
}

VecResult draken_sign(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    return fk_unary_numeric(args, nargs, "draken_sign",
                            [](int64_t x) -> int64_t { return (x > 0) - (x < 0); },
                            [](double x) -> double { return (x > 0.0) - (x < 0.0); },
                            true);
}

VecResult draken_sqrt(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sqrt: expected 1 argument");
    const DrakenVector* v = args[0];
    if (!fk_is_numeric(v->type))
        return draken_error_sentinel("draken_sqrt: numeric input required");
    uint32_t n = v->length;
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        out[i] = fk_row_valid(v, i) ? std::sqrt(fk_read_double(v, i)) : 0.0;
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
    return fk_numeric_result(v, out, n, DRAKEN_TIMESTAMP64);
}

}  // extern "C"

// ---------------------------------------------------------------------------
// CASE / IIF blend — draken_if_then_else(mask, then, else)
// ---------------------------------------------------------------------------

namespace {

inline size_t fk_fixed_elem_size(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:                                             return 1;
        case DRAKEN_INT16:                                            return 2;
        case DRAKEN_INT32: case DRAKEN_FLOAT32:
        case DRAKEN_DATE32: case DRAKEN_TIME32:                       return 4;
        case DRAKEN_INT64: case DRAKEN_FLOAT64: case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64:                  return 8;
        case DRAKEN_DECIMAL128:                                       return 16;
        default:                                                      return 0;
    }
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
                                static_cast<uint32_t>(XXH3_64bits(dst, len)),
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

inline uint8_t fk_ascii_lower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<uint8_t>(c + 32) : c;
}

// Iterative glob match over bytes with %-backtracking. `_` matches ONE BYTE —
// correct for VARCHAR (ASCII); callers must reject `_` for NVARCHAR (UTF-8
// multibyte). `%` is byte-safe on UTF-8 (matches whole suffixes only).
bool fk_like_match(const uint8_t* s, uint32_t sn, const uint8_t* p, uint32_t pn, bool ci) {
    uint32_t si = 0, pi = 0, star_p = UINT32_MAX, star_s = 0;
    while (si < sn) {
        if (pi < pn && p[pi] == '%') { star_p = ++pi; star_s = si; continue; }
        if (pi < pn && (p[pi] == '_' ||
                        (ci ? fk_ascii_lower(p[pi]) == fk_ascii_lower(s[si])
                            : p[pi] == s[si]))) { ++pi; ++si; continue; }
        if (star_p != UINT32_MAX) { pi = star_p; si = ++star_s; continue; }
        return false;
    }
    while (pi < pn && p[pi] == '%') ++pi;
    return pi == pn;
}

}  // namespace

extern "C" {

// STARTS_WITH / ENDS_WITH — the optimizer rewrites `LIKE 'x%'` → _STARTS_WITH,
// `LIKE '%x'` → _ENDS_WITH (CI variants for ILIKE; negation wraps in a NOT node,
// so no negate flag here). ctx.op_code bit1 = case-insensitive (VARCHAR ASCII only).
VecResult fk_affix(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                   bool suffix, const char* who) {
    if (nargs != 2) return draken_error_sentinel_fmt("%s: expected 2 arguments", who);
    const bool ci = (ctx != nullptr) && ((static_cast<const binary_op_ctx*>(ctx)->op_code & 2) != 0);
    const DrakenVector* v = args[0];
    const DrakenVector* p = args[1];
    if (!fk_is_string(v->type) || !fk_is_string(p->type))
        return draken_error_sentinel_fmt("%s: string operands required", who);
    if (ci && v->type != DRAKEN_VARCHAR)
        return draken_error_sentinel_fmt("%s: case-insensitive needs VARCHAR (ASCII)", who);
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
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i) || !fk_row_valid(p, i)) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            continue;
        }
        const DrakenStringSlot* vs = &vsa->slots[v->selection[i]];
        const DrakenStringSlot* ps = &psa->slots[p->selection[i]];
        const uint8_t* hay = str_data(vs, vsa->arena);
        const uint8_t* aff = str_data(ps, psa->arena);
        uint32_t hlen = str_length(vs), alen = str_length(ps);
        bool hit = alen <= hlen;
        if (hit) {
            const uint8_t* base = suffix ? hay + (hlen - alen) : hay;
            if (!ci) {
                hit = std::memcmp(base, aff, alen) == 0;
            } else {
                for (uint32_t k = 0; k < alen; ++k)
                    if (fk_ascii_lower(base[k]) != fk_ascii_lower(aff[k])) { hit = false; break; }
            }
        }
        if (hit) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
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

// Single haystack/needle substring verdict (memchr-anchored; ci = ASCII fold).
static inline bool fk_contains_hit(const uint8_t* hay, uint32_t hlen,
                                   const uint8_t* ndl, uint32_t nlen, bool ci) {
    if (nlen == 0) return true;
    if (nlen > hlen) return false;
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
    if (ci && v->type != DRAKEN_VARCHAR)
        return draken_error_sentinel(
            "draken_contains: case-insensitive contains needs VARCHAR (ASCII) — "
            "NVARCHAR case folding is not implemented natively (fail loud)");

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
                                       ndl, nlen, ci);
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
                str_data(ps, psa->arena), str_length(ps), ci);
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
    if (ci && v->type != DRAKEN_VARCHAR)
        return draken_error_sentinel(
            "draken_like: ILIKE needs VARCHAR (ASCII) — NVARCHAR case folding is "
            "not implemented natively (fail loud, never wrong)");

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
        // §11 fast path: dict/constant haystack + constant pattern — run the
        // wildcard matcher once per DISTINCT slot, scatter the verdicts
        // (bit-identical to the per-row loop).
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
            bool hit = fk_like_match(
                reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena)),
                str_length(vs), pdat, plen, ci);
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
            bool hit = fk_like_match(
                reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena)), str_length(vs),
                pdat, plen, ci);
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
// left_scale, right_scale (0 for non-decimal). NULL operand → bit 0 (row dropped).
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
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);

    if (!any_float) {
        const int maxs = ls > rs ? ls : rs;
        const __int128 lmul = fk_pow10_i128(maxs - ls);
        const __int128 rmul = fk_pow10_i128(maxs - rs);
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) continue;
            __int128 a = fk_read_dec(L, L->selection[i]) * lmul;
            __int128 b = fk_read_dec(R, R->selection[i]) * rmul;
            if (fk_cmp_i128(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t i = 0; i < n; ++i) {
            if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) continue;
            double a = fk_read_num_double(L, L->selection[i], ls);
            double b = fk_read_num_double(R, R->selection[i], rs);
            if (fk_cmp_dbl(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
    VecResult r{};
    r.data = out;
    r.validity = nullptr;
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
// draken_compare_dv). Both sides promote to ns in __int128, then compare. Nulls
// (either side) yield bit 0 — SQL 3-valued: a NULL operand drops the row.
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
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);

    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(L, i) || !fk_row_valid(R, i)) continue;
        __int128 a = fk_temporal_to_ns(L, L->selection[i], c->left_unit);
        __int128 b = fk_temporal_to_ns(R, R->selection[i], c->right_unit);
        if (fk_cmp_i128(op, a, b)) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r{};
    r.data = out;
    r.validity = nullptr;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// col <> '' / col = '' lower to UNARY IsNotEmpty/IsEmpty. SQL 3-valued: a NULL
// operand makes the predicate NULL → the filter drops it, so NULL → result bit 0
// for BOTH (validity all-valid; the definite-false bit drops the row).
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
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb > 0 ? nb : 1);
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) continue;   // NULL → bit 0 (dropped)
        bool empty = str_length(&sa->slots[v->selection[i]]) == 0;
        if (empty == want_empty) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
    VecResult r{};
    r.data = out;
    r.validity = nullptr;
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
    uint32_t n = v->length;
    const int want_validity = (v->validity != nullptr) ? 1 : 0;

    // Pass 1: total long-form bytes (results with len > STR_INLINE_MAX).
    size_t arena_len = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) continue;
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        uint32_t off, len;
        fk_substr_range(str_data(slot, sa->arena), str_length(slot), is_utf8,
                        start0, has_count, c->count, &off, &len);
        if (len > STR_INLINE_MAX) arena_len += len;
    }

    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity;
    uint8_t* block = vecresult_string_block_alloc(n, arena_len, want_validity,
                                                  &slots, &arena, &validity);
    if (block == nullptr) return draken_error_sentinel("draken_substring: allocation failed");
    if (want_validity) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        std::memcpy(validity, v->validity, vb > 0 ? vb : 1);
    }

    size_t arena_pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!fk_row_valid(v, i)) { std::memset(&slots[i], 0, sizeof(DrakenStringSlot)); continue; }
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        const uint8_t* src = str_data(slot, sa->arena);
        uint32_t off, len;
        fk_substr_range(src, str_length(slot), is_utf8, start0, has_count, c->count, &off, &len);
        const uint8_t* sub = src + off;
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], sub, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, sub, len);
            str_init_extern(&slots[i], dst, len,
                            static_cast<uint32_t>(XXH3_64bits(dst, len)),
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }
    return vecresult_from_string_block(block, n, arena_len, want_validity, v->type);
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
