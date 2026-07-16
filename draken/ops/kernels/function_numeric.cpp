// draken/ops/kernels/function_numeric.cpp — Phase 9a-fn: numeric scalar function
// kernels (POWER, LOG, TRUNC, RANDOM, NORMAL) on the C ABI's func_fn_t shape:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL. Idiom matches function_kernels.cpp: read
// uniformly via data[selection[i]] (buffers.h contract), return an owned dense
// VecResult, fail LOUD with an error sentinel for anything outside contract.
//
// SHAPE: every operand is read through the uniform data[selection[i]] path, so a
// constant-shaped literal (data_length == 1, selection = global zero vector) needs
// no broadcast arm — it already reports length == the morsel's row count. This is
// the same contract draken_numeric_cmp relies on.
//
// DECIMAL on the FIRST operand (POWER's base, LOG's value, TRUNC's num) is supported
// via the bind-time binary_op_ctx compiled_expression.pyx allocates for this name
// list ("ROUND"/"FLOOR"/"CEILING"/"CEIL"/"SQRT"/"ABS"/"TRUNC"/"POWER"/"LOG") — the
// same vehicle the decimal binops use, carrying `left_scale` (a LogicalType detail
// the DrakenVector itself cannot carry). Without it, computing on the raw unscaled
// int64 would silently answer TRUNC(3.789) = 3789 — the exact off-scale wrong-answer
// class this codebase has been bitten by before, so a NULL ctx fails loud instead.
//
// DECIMAL on the SECOND operand (POWER's exponent, LOG's base, TRUNC's digits) is NOT
// supported: the ctx this codebase's binder builds carries only parameters[0]'s
// scale (`left_scale`) — there is no vehicle for a second operand's scale on this
// call path, so it always fails loud rather than guess.
//
// LOCAL HELPERS: function_kernels.cpp's fk_* readers are in an anonymous namespace
// and cannot be reached from another TU. The minimal readers below are duplicated
// from it on purpose (that file is under concurrent edit and must not be touched);
// extracting the fk_* family into a shared header is the right follow-up.

#include <cstdint>
#include <cstring>
#include <cmath>
#include <random>                          // std::random_device — per-thread RNG seeding

#include "pcg_random.hpp"                  // vendored third_party/pcg — RANDOM/NORMAL
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"            // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/kernel_context.h"   // binary_op_ctx — the (absent) DECIMAL scale vehicle
#include "ops/kernels/error_handling.h"

namespace {

inline bool fn_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u);
}

// RANDOM/NORMAL RNG. THREAD-SAFETY: the morsel scheduler runs operators across worker
// threads, so a shared engine would be a data race. Each thread gets its OWN pcg64,
// seeded independently from std::random_device on first use — lock-free, no shared
// mutable state. RANDOM/NORMAL are catalog-VOLATILE (deterministic=False), so
// independent per-thread streams are the correct design, not a compromise: there is no
// reproducibility contract to honour. (The old nanobind path used one process-global
// mt19937_64 guarded by the GIL — that serialization vanishes on the nogil engine, so a
// global here would corrupt under concurrency.)
inline pcg64& fn_thread_rng() {
    static thread_local pcg64 rng{pcg_extras::seed_seq_from<std::random_device>()};
    return rng;
}

// Uniform double in [0, 1): take the top 53 bits of a 64-bit draw (the exactly-
// representable mantissa width) and scale by 2^-53. Distribution-object-free, so
// nothing carries cross-call state that would need locking.
inline double fn_next_uniform(pcg64& rng) {
    return static_cast<double>(rng() >> 11) * 0x1.0p-53;
}

// The VM hands arity-0 RANDOM()/NORMAL() a synthetic length-only operand (see the
// evaluation.pyx arity-0 C-native arm); arity-1 RANDOM(n)/NORMAL(n) arrive with a real
// constant operand already broadcast to the morsel row count. Either way the OUTPUT is
// one value per row and the row count is args[0]->length. Any count VALUE is ignored:
// in a per-row projection there is no coherent "n values" meaning — the projection
// needs exactly one value per output row.
inline bool fn_rng_rowcount(const DrakenVector* const* args, uint32_t nargs,
                            const char* who, uint32_t& n_out) {
    if (nargs != 1) {
        return false;  // caller emits the error sentinel with `who`
    }
    n_out = args[0]->length;
    return true;
}

inline bool fn_is_signed_int(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            return true;
        default:
            return false;
    }
}

inline bool fn_is_unsigned_int(DrakenType t) {
    switch (t) {
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return true;
        default:
            return false;
    }
}

inline bool fn_is_float(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// Types these kernels can read a VALUE from with no bind-time context. DECIMAL is
// excluded on purpose (see the file header) — it is numeric, but not readable here.
inline bool fn_is_ctx_free_numeric(DrakenType t) {
    return fn_is_signed_int(t) || fn_is_unsigned_int(t) || fn_is_float(t);
}

// Each reader enumerates EXACTLY the types it accepts — no catch-all default that
// reinterprets one bit pattern as another element type (a silent wrong answer).
// Callers gate on fn_is_ctx_free_numeric above.
inline double fn_read_double(const DrakenVector* v, uint32_t row) {
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
        default:             return 0.0;   // unreachable: fn_is_ctx_free_numeric gates every caller
    }
}

// DECIMAL's raw storage IS int64_t (same width as DRAKEN_INT64) — the unscaled
// value. Callers unscale via the bind-time ctx's left_scale; this reader has no
// business knowing about scale, it just gathers the raw int64.
inline int64_t fn_read_decimal_raw(const DrakenVector* v, uint32_t row) {
    return static_cast<const int64_t*>(v->data)[v->selection[row]];
}

// Integer-family read for the `digits` operand of TRUNC.
inline bool fn_read_digits(const DrakenVector* v, uint32_t row, int64_t& out) {
    switch (v->type) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64: {
            uint32_t phys = v->selection[row];
            switch (v->type) {
                case DRAKEN_INT8:  out = static_cast<const int8_t*>(v->data)[phys];  break;
                case DRAKEN_INT16: out = static_cast<const int16_t*>(v->data)[phys]; break;
                case DRAKEN_INT32: out = static_cast<const int32_t*>(v->data)[phys]; break;
                default:           out = static_cast<const int64_t*>(v->data)[phys]; break;
            }
            return true;
        }
        default:
            return false;
    }
}

// Rejection for an operand these kernels cannot read. DECIMAL/DECIMAL128 are numeric
// — saying "numeric input required" about them would send the reader hunting a type
// error that isn't there — so each gets its own reason.
VecResult fn_reject_operand(const DrakenVector* v, const char* who) {
    if (v->type == DRAKEN_DECIMAL || v->type == DRAKEN_DECIMAL128) {
        return draken_error_sentinel_fmt(
            "%s: DECIMAL operand is not supported by this kernel — it needs the "
            "operand's bind-time scale, which the binder does not yet supply for "
            "this function (failing loud beats an off-scale wrong answer)", who);
    }
    return draken_error_sentinel_fmt("%s: numeric input required", who);
}

// Dense FLOAT64 result whose validity is the AND of `n_src` operands' validity
// (NULL propagates: a row is valid only where every operand row is valid).
VecResult fn_double_result(const DrakenVector* const* srcs, uint32_t n_src, double* out,
                           uint32_t n) {
    bool any_validity = false;
    for (uint32_t s = 0; s < n_src; ++s)
        if (srcs[s]->validity != nullptr) { any_validity = true; break; }

    uint8_t* validity = nullptr;
    if (any_validity) {
        size_t vb = (static_cast<size_t>(n) + 7) / 8;
        if (vb == 0) vb = 1;
        validity = static_cast<uint8_t*>(draken_malloc(vb));
        if (validity == nullptr) {
            draken_free(out);
            return draken_error_sentinel("function_numeric: allocation failed");
        }
        std::memset(validity, 0xFF, vb);
        for (uint32_t s = 0; s < n_src; ++s) {
            const uint8_t* sv = srcs[s]->validity;
            if (sv == nullptr) continue;   // all-valid operand contributes nothing
            for (size_t b = 0; b < vb; ++b) validity[b] &= sv[b];
        }
    }

    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_FLOAT64;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // namespace

extern "C" {

// POWER(num, exp) -> FLOAT64 (catalog contract). Element-wise pow.
//
// IEEE semantics, deliberately: an overflowing POWER (pow(1e300, 2)) yields +inf and
// pow(-1, 0.5) yields NaN rather than an error. This is NOT a silent degradation —
// it is the only self-consistent answer available. `SELECT POWER(1e300, 2)` over
// literals is CONSTANT-FOLDED at bind time and never reaches this kernel; folding
// already yields inf. Raising here would make the same expression error over a
// column and answer inf over literals — the identical query giving two different
// outcomes depending on foldability. Matching the folded path keeps one contract.
// (NaN sorts highest and -0.0 == 0.0 per the engine's float rules.)
VecResult draken_power(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_power: expected 2 arguments (base, exponent)");
    const DrakenVector* b = args[0];
    const DrakenVector* e = args[1];

    double b_dec_unscale = 1.0;
    bool b_is_decimal = (b->type == DRAKEN_DECIMAL);
    if (b_is_decimal) {
        if (ctx == nullptr) {
            return draken_error_sentinel(
                "draken_power: DECIMAL base needs its bind-time scale context");
        }
        b_dec_unscale = std::pow(
            10.0, -static_cast<double>(static_cast<const binary_op_ctx*>(ctx)->left_scale));
    } else if (!fn_is_ctx_free_numeric(b->type)) {
        return fn_reject_operand(b, "draken_power");
    }
    // The exponent has no scale vehicle (ctx carries only parameters[0]'s scale) — a
    // DECIMAL exponent always fails loud rather than silently reading the unscaled int64.
    if (!fn_is_ctx_free_numeric(e->type)) return fn_reject_operand(e, "draken_power");
    if (b->length != e->length)
        return draken_error_sentinel("draken_power: operand length mismatch");

    uint32_t n = b->length;
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("draken_power: allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fn_row_valid(b, i) || !fn_row_valid(e, i)) { out[i] = 0.0; continue; }
        double bv = b_is_decimal
            ? (static_cast<double>(fn_read_decimal_raw(b, i)) * b_dec_unscale)
            : fn_read_double(b, i);
        out[i] = std::pow(bv, fn_read_double(e, i));
    }
    return fn_double_result(args, 2, out, n);
}

// LOG(num, base) -> FLOAT64 (catalog contract): ln(v) / ln(base).
//
// IEEE semantics, deliberately — same reasoning as draken_power, and here the folded
// path is already OBSERVED to behave this way: `SELECT LOG(0, 10)` answers -inf and
// `SELECT LOG(-1, 10)` answers NaN today, via bind-time folding. vector_misc.cpp's
// vector_log documents that contract explicitly ("log(0)=-inf, log(-1)=NaN"). This
// kernel reproduces it so the column path and the folded path agree.
VecResult draken_log(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 2)
        return draken_error_sentinel("draken_log: expected 2 arguments (value, base)");
    const DrakenVector* v = args[0];
    const DrakenVector* b = args[1];

    double v_dec_unscale = 1.0;
    bool v_is_decimal = (v->type == DRAKEN_DECIMAL);
    if (v_is_decimal) {
        if (ctx == nullptr) {
            return draken_error_sentinel(
                "draken_log: DECIMAL value needs its bind-time scale context");
        }
        v_dec_unscale = std::pow(
            10.0, -static_cast<double>(static_cast<const binary_op_ctx*>(ctx)->left_scale));
    } else if (!fn_is_ctx_free_numeric(v->type)) {
        return fn_reject_operand(v, "draken_log");
    }
    // The base has no scale vehicle (ctx carries only parameters[0]'s scale) — a
    // DECIMAL base always fails loud rather than silently reading the unscaled int64.
    if (!fn_is_ctx_free_numeric(b->type)) return fn_reject_operand(b, "draken_log");
    if (v->length != b->length)
        return draken_error_sentinel("draken_log: operand length mismatch");

    uint32_t n = v->length;
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("draken_log: allocation failed");
    for (uint32_t i = 0; i < n; ++i) {
        if (!fn_row_valid(v, i) || !fn_row_valid(b, i)) { out[i] = 0.0; continue; }
        double vv = v_is_decimal
            ? (static_cast<double>(fn_read_decimal_raw(v, i)) * v_dec_unscale)
            : fn_read_double(v, i);
        out[i] = std::log(vv) / std::log(fn_read_double(b, i));
    }
    return fn_double_result(args, 2, out, n);
}

// TRUNC(num [, scale]) -> FLOAT64 (the TRUNC_numeric overload's contract).
// Truncates TOWARD ZERO at `scale` decimal places (scale defaults to 0).
//
// TEMPORAL OPERANDS FAIL LOUD (defense in depth). TRUNC is overloaded in the catalog —
// TRUNC_numeric (this kernel), TRUNC_date and TRUNC_timestamp (temporal-group kernels,
// a different unit-string contract) all share the name "TRUNC". compiled_expression.pyx
// gates the registry lookup on the bind-time-selected overload's kernel id ("numeric"
// only), so the temporal overloads never reach this function in practice — they stay
// on the Python callable_ref path, refused at PLAN time same as before this kernel
// existed. This in-kernel check is a backstop against a future binder change relaxing
// that gate, not the primary defense.
VecResult draken_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs < 1 || nargs > 2)
        return draken_error_sentinel("draken_trunc: expected 1 or 2 arguments");
    const DrakenVector* v = args[0];
    const DrakenVector* dg = (nargs == 2) ? args[1] : nullptr;

    if (v->type == DRAKEN_DATE32 || v->type == DRAKEN_TIMESTAMP64) {
        return draken_error_sentinel(
            "draken_trunc: temporal TRUNC(value, unit) is not implemented as a "
            "c-native kernel — only the numeric overload is");
    }
    if (dg != nullptr && dg->length != v->length)
        return draken_error_sentinel("draken_trunc: operand length mismatch");

    uint32_t n = v->length;
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("draken_trunc: allocation failed");

    if (v->type == DRAKEN_DECIMAL) {
        // Truncate EXACTLY in the raw int64 domain — the double image of a decimal
        // like 3.789 is not exact, so double-domain truncation can give the wrong
        // digit (the same reasoning fk_round_family documents for ROUND/FLOOR/CEILING).
        if (ctx == nullptr) {
            draken_free(out);
            return draken_error_sentinel(
                "draken_trunc: DECIMAL operand needs its bind-time scale context");
        }
        int s = static_cast<const binary_op_ctx*>(ctx)->left_scale;
        for (uint32_t i = 0; i < n; ++i) {
            if (!fn_row_valid(v, i) || (dg != nullptr && !fn_row_valid(dg, i))) {
                out[i] = 0.0;
                continue;
            }
            int64_t raw = fn_read_decimal_raw(v, i);
            int64_t d = 0;
            if (dg != nullptr && !fn_read_digits(dg, i, d)) {
                draken_free(out);
                return draken_error_sentinel("draken_trunc: integer scale required");
            }
            if (d >= s) {
                // target scale keeps at least as many decimal places as the value
                // has — nothing to truncate away.
                out[i] = static_cast<double>(raw) * std::pow(10.0, -static_cast<double>(s));
                continue;
            }
            int64_t steps = s - d;
            if (steps > 18) {
                draken_free(out);
                return draken_error_sentinel(
                    "draken_trunc: truncation step exceeds the int64 decimal domain");
            }
            int64_t p = 1;
            for (int64_t k = 0; k < steps; ++k) p *= 10;
            int64_t q = raw / p;   // C++ integer division truncates TOWARD ZERO
            out[i] = static_cast<double>(q) * std::pow(10.0, -static_cast<double>(d));
        }
    } else {
        if (!fn_is_ctx_free_numeric(v->type)) {
            draken_free(out);
            return fn_reject_operand(v, "draken_trunc");
        }
        for (uint32_t i = 0; i < n; ++i) {
            if (!fn_row_valid(v, i) || (dg != nullptr && !fn_row_valid(dg, i))) {
                out[i] = 0.0;
                continue;
            }
            double x = fn_read_double(v, i);
            if (dg == nullptr) {
                out[i] = std::trunc(x);
                continue;
            }
            int64_t d = 0;
            if (!fn_read_digits(dg, i, d)) {
                draken_free(out);
                return draken_error_sentinel("draken_trunc: integer scale required");
            }
            double scale = std::pow(10.0, static_cast<double>(d));
            out[i] = std::trunc(x * scale) / scale;
        }
    }
    const DrakenVector* srcs[2] = {v, dg};
    return fn_double_result(srcs, dg != nullptr ? 2u : 1u, out, n);
}

// Dense FLOAT64 result with NO validity — RANDOM/NORMAL never produce NULL, and the
// synthetic arity-0 operand carries none to propagate.
static VecResult fn_rng_result(double* out, uint32_t n) {
    VecResult r{};
    r.data = out;
    r.validity = nullptr;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_FLOAT64;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

// RANDOM([n]) -> FLOAT64: one uniform value in [0, 1) per output row.
VecResult draken_random(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    uint32_t n = 0;
    if (!fn_rng_rowcount(args, nargs, "draken_random", n))
        return draken_error_sentinel("draken_random: expected exactly one operand "
                                     "(the row-count carrier)");
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("draken_random: allocation failed");
    pcg64& rng = fn_thread_rng();
    for (uint32_t i = 0; i < n; ++i) out[i] = fn_next_uniform(rng);
    return fn_rng_result(out, n);
}

// NORMAL([n]) -> FLOAT64: one standard-normal value per output row, via Box-Muller
// (mean 0, stddev 1). Matches the semantics of the superseded nanobind vector_random_
// normal, minus its fixed seed — which existed only for reproducibility that a VOLATILE
// function does not promise (see fn_thread_rng).
VecResult draken_normal(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    uint32_t n = 0;
    if (!fn_rng_rowcount(args, nargs, "draken_normal", n))
        return draken_error_sentinel("draken_normal: expected exactly one operand "
                                     "(the row-count carrier)");
    auto* out = static_cast<double*>(draken_malloc((n > 0 ? n : 1) * sizeof(double)));
    if (out == nullptr) return draken_error_sentinel("draken_normal: allocation failed");
    pcg64& rng = fn_thread_rng();
    constexpr double TWO_PI = 6.283185307179586;
    constexpr double EPS    = 1e-300;   // keep u1 away from 0 so log(u1) is finite
    const uint32_t pairs = n >> 1;
    for (uint32_t i = 0; i < pairs; ++i) {
        double u1, u2;
        do { u1 = fn_next_uniform(rng); } while (u1 < EPS);
        u2 = fn_next_uniform(rng);
        double mag = std::sqrt(-2.0 * std::log(u1));
        out[2 * i]     = mag * std::cos(TWO_PI * u2);
        out[2 * i + 1] = mag * std::sin(TWO_PI * u2);
    }
    if (n & 1u) {
        double u1, u2;
        do { u1 = fn_next_uniform(rng); } while (u1 < EPS);
        u2 = fn_next_uniform(rng);
        out[n - 1] = std::sqrt(-2.0 * std::log(u1)) * std::cos(TWO_PI * u2);
    }
    return fn_rng_result(out, n);
}

}  // extern "C"
