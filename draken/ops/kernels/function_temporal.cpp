// draken/ops/kernels/function_temporal.cpp — Phase 9a-fn: temporal function kernels
// on the C ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (evaluation.pyx's BC_FUNCTION C-native
// arm) — no Python, no nanobind, no GIL.
//
// UNIT PROBLEM — how DATEDIFF/TIMEDIFF/FORMAT_TIMESTAMP/UNIXTIME/TIME_BUCKET solve it.
// All five need the *input* TIMESTAMP64 operand's TimestampUnit (s/ms/us/ns) to
// compute a correct answer, and that unit is a LogicalType descriptor that lives on
// the Python Vector object — NOT on the DrakenVector a C ABI kernel receives. A
// days-vs-micros mixup here is the exact class of bug this area has produced
// before. Each of the five is therefore lowered at bind time in
// compiled_expression.pyx, which reads the operand's `column_type.logical.unit`
// and stuffs it into a context struct — the same technique draken_date_part /
// draken_date_trunc already use (binary_op_ctx.left_unit/right_unit):
//   draken_datediff / draken_timediff — binary_op_ctx{op_code=diff_kind,
//     left_unit, right_unit}; both operands pushed (start, end).
//   draken_unixtime — binary_op_ctx{left_unit=ts_unit} (op_code unused); DATE32
//     operands pass left_unit=0 (unused — the kernel dispatches on the operand's
//     DrakenType directly). NOTE: cast_timestamp_ctx.unit uses a DIFFERENT
//     numbering (1=ns,2=us,3=ms,4=s,5=days, see cast_temporal.cpp) than
//     binary_op_ctx.left_unit's TimestampUnit (0=s,1=ms,2=us,3=ns) — reusing
//     cast_timestamp_ctx here would silently misinterpret the unit, so
//     binary_op_ctx is used for EVERY new kernel in this file, one convention.
//   draken_time_bucket — time_bucket_ctx{magnitude, unit_kind, ts_unit}
//     (kernel_context.h); magnitude/units are bind-time literals, consumed into
//     the ctx — only the `date` operand is pushed.
//   draken_date_format — format_ctx{ts_unit, fmt_len} + trailing pattern bytes
//     (kernel_context.h); the pattern LITERAL is consumed into the ctx — only
//     the `date` operand is pushed. Reuses the compiled token-program formatter
//     in draken/ops/temporal_format.h (shared with the nanobind FORMAT_TIMESTAMP path
//     — one formatter, not two).
//
// FROM_UNIXTIME is the exception needing no ctx: its operand is a plain NUMERIC
// (epoch seconds), which carries no unit descriptor, and its result unit is fixed
// by the catalog's declared return type — TIMESTAMP(MICROSECONDS), see
// registrar/temporal.pyx `_CT_TIMESTAMP()`. So it stamps ts_unit = 2 (us) directly.
//
// SHAPE: draken_from_unixtime is shape-preserving (cast_numeric.cpp pattern — a
// pure function of one physical value, computed once per data_length PHYSICAL
// value, kernel_preserve_shape carries selection+validity). The other five are
// NOT shape-specialized: DATEDIFF/TIMEDIFF are a function of TWO operands (no
// single physical value to preserve shape over), and FORMAT_TIMESTAMP/UNIXTIME/
// TIME_BUCKET follow the existing draken_date_part/draken_date_trunc precedent —
// dense over `length` via the uniform data[selection[i]] access pattern.

#include <cstdint>
#include <cmath>
#include <cstring>
#include <cstdio>
#include <string>
#include <new>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "ops/kernels/kernel_context.h"   // binary_op_ctx, cast_timestamp_ctx, time_bucket_ctx, format_ctx
#include "ops/temporal_arith.h"           // ta_floor_div, ta_ticks_per_second, date_diff_batch
#include "ops/temporal_format.h"          // shared compiled-token FORMAT_TIMESTAMP formatter
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"

namespace {

constexpr int64_t US_PER_SECOND = 1000000LL;

// TimestampUnit::MICROSECONDS (draken/logical_type.h) — the unit the catalog's
// declared FROM_UNIXTIME return type (_CT_TIMESTAMP()) pins.
constexpr uint8_t TS_UNIT_US = 2u;

// Largest |epoch second| whose microsecond tick still fits int64.
constexpr int64_t FU_MAX_SECONDS = INT64_MAX / US_PER_SECOND;   // ~year 294247

bool fu_is_signed_int(DrakenType t) noexcept {
    return t == DRAKEN_INT8 || t == DRAKEN_INT16 || t == DRAKEN_INT32 || t == DRAKEN_INT64;
}

bool fu_is_float(DrakenType t) noexcept {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// Read physical value j of a signed-int vector, widened to int64.
int64_t fu_load_int(const void* data, uint32_t j, DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:  return static_cast<const int8_t*>(data)[j];
        case DRAKEN_INT16: return static_cast<const int16_t*>(data)[j];
        case DRAKEN_INT32: return static_cast<const int32_t*>(data)[j];
        default:           return static_cast<const int64_t*>(data)[j];
    }
}

double fu_load_float(const void* data, uint32_t j, DrakenType t) noexcept {
    if (t == DRAKEN_FLOAT32) return static_cast<const float*>(data)[j];
    return static_cast<const double*>(data)[j];
}

// All-null TIMESTAMP64 result for a DRAKEN_NULL operand (the operand is typeless
// all-null; every output row is null regardless of what the value would have been).
VecResult fu_all_null(uint32_t n) {
    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) return draken_error_sentinel("draken_from_unixtime: allocation failed");
    std::memset(out, 0, data_sz);

    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!validity) {
        draken_free(out);
        return draken_error_sentinel("draken_from_unixtime: allocation failed");
    }
    std::memset(validity, 0x00u, vbytes);   // every row null

    VecResult r;
    r.data              = out;
    r.validity          = validity;
    r.selection         = draken_identity_sel(n);
    r.owns_selection    = false;
    r.data_length       = n;
    r.length            = n;
    r.type              = DRAKEN_TIMESTAMP64;
    r.flags             = DRAKEN_SEL_IDENTITY;
    r.validity_embedded = 0u;
    r.ts_unit           = TS_UNIT_US;
    return r;
}

}  // namespace

extern "C" {

// FROM_UNIXTIME(ts) -> TIMESTAMP(us). `ts` is epoch SECONDS (catalog type_family
// "numeric"). Integer operands scale exactly; float operands keep sub-second
// precision, rounded to the nearest microsecond.
//
// Range is validated only over LIVE rows (non-null, reached through `selection`).
// A null row's physical slot may hold arbitrary bytes, and shape-preserving kernels
// compute over every physical value — so validating the whole data block would fail
// a query on a value no row actually reads. The arithmetic itself is done in
// unsigned (well-defined wraparound) so an out-of-range dead slot cannot trip signed
// overflow UB before the live-row check runs.
VecResult draken_from_unixtime(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1)
            return draken_error_sentinel("draken_from_unixtime: expected 1 argument");
        const DrakenVector* v = args[0];
        if (!v) return draken_error_sentinel("draken_from_unixtime: input vector is null");

        if (v->type == DRAKEN_NULL) return fu_all_null(v->length);

        const bool is_int   = fu_is_signed_int(v->type);
        const bool is_float = fu_is_float(v->type);
        if (!is_int && !is_float) {
            // DECIMAL lands here on purpose: its unscaled int64 needs the bind-time
            // scale to mean anything, and this kernel is dispatched without a ctx.
            // Fail loud rather than silently reading the unscaled value as seconds.
            return draken_error_sentinel_fmt(
                "draken_from_unixtime: expected a signed integer or float operand, "
                "got DrakenType %d", static_cast<int>(v->type));
        }

        const uint32_t k = v->data_length;   // physical value count
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("draken_from_unixtime: allocation failed");

        if (is_int) {
            for (uint32_t j = 0u; j < k; ++j) {
                const uint64_t s = static_cast<uint64_t>(fu_load_int(v->data, j, v->type));
                out[j] = static_cast<int64_t>(s * static_cast<uint64_t>(US_PER_SECOND));
            }
        } else {
            for (uint32_t j = 0u; j < k; ++j) {
                const double s = fu_load_float(v->data, j, v->type);
                out[j] = std::isfinite(s)
                             ? static_cast<int64_t>(std::llround(s * static_cast<double>(US_PER_SECOND)))
                             : 0;
            }
        }

        // Live-row range validation (see the note above).
        for (uint32_t i = 0u; i < v->length; ++i) {
            if (kernel_row_is_null(v, i)) continue;
            const uint32_t j = v->selection[i];
            if (is_int) {
                const int64_t s = fu_load_int(v->data, j, v->type);
                if (s > FU_MAX_SECONDS || s < -FU_MAX_SECONDS) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "draken_from_unixtime: epoch second %lld is out of range for a "
                        "microsecond TIMESTAMP", static_cast<long long>(s));
                }
            } else {
                const double s = fu_load_float(v->data, j, v->type);
                if (!std::isfinite(s) ||
                    s > static_cast<double>(FU_MAX_SECONDS) ||
                    s < -static_cast<double>(FU_MAX_SECONDS)) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "draken_from_unixtime: epoch second %g is out of range for a "
                        "microsecond TIMESTAMP", s);
                }
            }
        }

        VecResult r;
        r.data              = out;
        r.type              = DRAKEN_TIMESTAMP64;
        r.validity_embedded = 0u;
        r.ts_unit           = TS_UNIT_US;
        kernel_preserve_shape(r, v);   // sets length/data_length/flags/selection/validity
        return r;
    });
}

}  // extern "C"

// ---------------------------------------------------------------------------
// Shared helpers for the dense (non-shape-preserving) temporal kernels below —
// same style as function_kernels.cpp's fk_row_valid/fk_numeric_result, local to
// this file (that helper is not exported across kernel .cpp TUs).
// ---------------------------------------------------------------------------

namespace {

inline bool ft_row_valid(const DrakenVector* v, uint32_t row) noexcept {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7u)) & 1u);
}

// Dense result assembly: `out` holds `n` (== length) physical values in logical-row
// order (uniform data[selection[i]] access already applied by the caller);
// validity is copied from `validity_of` (nullptr → no null bitmap allocated).
template <typename T>
VecResult ft_dense_result(const DrakenVector* validity_of, T* out, uint32_t n, DrakenType t) {
    uint8_t* validity = nullptr;
    if (validity_of != nullptr && validity_of->validity != nullptr) {
        const size_t vb = (static_cast<size_t>(n) + 7u) / 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vb > 0u ? vb : 1u));
        if (!validity) { draken_free(out); return draken_error_sentinel("draken_temporal: allocation failed"); }
        std::memcpy(validity, validity_of->validity, vb > 0u ? vb : 1u);
    }
    VecResult r{};
    r.data              = out;
    r.validity          = validity;
    r.selection         = draken_identity_sel(n);
    r.owns_selection    = false;
    r.data_length       = n;
    r.length            = n;
    r.type              = t;
    r.flags             = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    r.validity_embedded = 0u;
    r.ts_unit           = 0xFFu;
    return r;
}

// Row `i` is null in the two-operand result if either operand is null at row i.
// Returns nullptr (all-valid) only when NEITHER operand carries a validity bitmap.
uint8_t* ft_merge_validity(const DrakenVector* a, const DrakenVector* b, uint32_t n) {
    if (a->validity == nullptr && b->validity == nullptr) return nullptr;
    const size_t vb = (static_cast<size_t>(n) + 7u) / 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vb > 0u ? vb : 1u));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0xFF, vb > 0u ? vb : 1u);
    for (uint32_t i = 0u; i < n; ++i) {
        if (!ft_row_valid(a, i) || !ft_row_valid(b, i))
            out[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
    }
    return out;
}

// Gather one DATEDIFF/TIMEDIFF operand into a flat int64 tick array normalised so
// that date_diff_batch's unit_code is 2 (microseconds) for a DATE32 operand — it
// stores int32_t days, scaled here — and the operand's real TimestampUnit for a
// TIMESTAMP64 operand (ctx_unit, supplied at bind time; see the file header).
// Mirrors vector_temporal_arith.cpp's impl_date_diff gather exactly.
void ft_gather_diff_operand(const DrakenVector* v, uint32_t n, int ctx_unit,
                            int64_t* out_flat, int* out_unit_code, const char* who) {
    static constexpr int64_t US_PER_DAY = 86400000000LL;
    if (v->type == DRAKEN_DATE32) {
        const int32_t* d = static_cast<const int32_t*>(v->data);
        for (uint32_t i = 0u; i < n; ++i)
            out_flat[i] = ft_row_valid(v, i)
                ? static_cast<int64_t>(d[v->selection[i]]) * US_PER_DAY : 0;
        *out_unit_code = 2;
    } else if (v->type == DRAKEN_TIMESTAMP64) {
        const int64_t* d = static_cast<const int64_t*>(v->data);
        for (uint32_t i = 0u; i < n; ++i)
            out_flat[i] = ft_row_valid(v, i) ? d[v->selection[i]] : 0;
        *out_unit_code = ctx_unit;
    } else {
        char msg[128];
        std::snprintf(msg, sizeof(msg), "%s: operand must be DATE32 or TIMESTAMP64", who);
        throw std::invalid_argument(msg);
    }
}

// DATEDIFF/TIMEDIFF share one implementation — TIMEDIFF is DATEDIFF with a
// bind-time-fixed diff_kind (hours; parse_diff_kind("hours") == 4 in the nanobind
// reference), carried in the SAME binary_op_ctx the lowering arm builds.
VecResult ft_date_diff_impl(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                            const char* who) {
    if (nargs != 2)
        return draken_error_sentinel_fmt("%s: expected 2 arguments", who);
    if (ctx == nullptr)
        return draken_error_sentinel_fmt("%s: missing bind-time ctx (diff kind + units)", who);
    const auto* c = static_cast<const binary_op_ctx*>(ctx);
    const DrakenVector* start = args[0];
    const DrakenVector* end   = args[1];

    // DRAKEN_NULL means the operand is a typeless all-null literal — every output
    // row is null regardless of the (unknowable) part it would have used.
    if (start->type == DRAKEN_NULL || end->type == DRAKEN_NULL) {
        const uint32_t n = (start->type == DRAKEN_NULL) ? end->length : start->length;
        auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel_fmt("%s: allocation failed", who);
        std::memset(out, 0, (n > 0u ? n : 1u) * sizeof(int64_t));
        const size_t vb = (static_cast<size_t>(n) + 7u) / 8u;
        uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vb > 0u ? vb : 1u));
        if (!validity) { draken_free(out); return draken_error_sentinel_fmt("%s: allocation failed", who); }
        std::memset(validity, 0x00, vb > 0u ? vb : 1u);   // every row null
        VecResult r{};
        r.data = out; r.validity = validity;
        r.selection = draken_identity_sel(n); r.owns_selection = false;
        r.data_length = n; r.length = n; r.type = DRAKEN_INT64;
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        return r;
    }

    if (start->length != end->length)
        return draken_error_sentinel_fmt(
            "%s: start length %u != end length %u", who, start->length, end->length);

    const uint32_t n = start->length;
    const size_t flat_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    auto* s_flat = static_cast<int64_t*>(draken_malloc(flat_sz));
    auto* e_flat = static_cast<int64_t*>(draken_malloc(flat_sz));
    auto* out    = static_cast<int64_t*>(draken_malloc(flat_sz));
    if (!s_flat || !e_flat || !out) {
        draken_free(s_flat); draken_free(e_flat); draken_free(out);
        return draken_error_sentinel_fmt("%s: allocation failed", who);
    }

    // ft_gather_diff_operand throws on a non-DATE32/TIMESTAMP64 operand — guard
    // the three buffers so that path doesn't leak (DRAKEN_KERNEL_TRY at the call
    // site converts the exception to an error sentinel, but never runs our frees).
    struct Guard { int64_t* s; int64_t* e; int64_t* o;
        ~Guard() { if (s) draken_free(s); if (e) draken_free(e); if (o) draken_free(o); } } g{s_flat, e_flat, out};

    int ucs = 2, uce = 2;
    ft_gather_diff_operand(start, n, c->left_unit, s_flat, &ucs, who);
    ft_gather_diff_operand(end, n, c->right_unit, e_flat, &uce, who);

    date_diff_batch(s_flat, e_flat, n, ucs, uce, c->op_code, out);
    g.s = nullptr; g.e = nullptr;
    draken_free(s_flat);
    draken_free(e_flat);

    uint8_t* validity = ft_merge_validity(start, end, n);
    g.o = nullptr;
    VecResult r{};
    r.data = out; r.validity = validity;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n; r.type = DRAKEN_INT64;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    r.validity_embedded = 0u; r.ts_unit = 0xFFu;
    return r;
}

}  // namespace

extern "C" {

// DATEDIFF(part, start, end) / DATE_DIFF — signed difference (end − start) in the
// bind-time part. ctx = binary_op_ctx{op_code=diff_kind, left_unit, right_unit}.
VecResult draken_datediff(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({ return ft_date_diff_impl(ctx, args, nargs, "draken_datediff"); });
}

// TIMEDIFF(time1, time2) / TIME_DIFF — DATEDIFF with diff_kind hardcoded to hours
// at bind time (same ctx vehicle; see compiled_expression.pyx's lowering arm).
VecResult draken_timediff(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({ return ft_date_diff_impl(ctx, args, nargs, "draken_timediff"); });
}

// UNIXTIME(date) / TO_UNIXTIME — TIMESTAMP64|DATE32 -> INT64 unix seconds.
// ctx = binary_op_ctx{left_unit=ts_unit} (op_code unused): the operand's
// TimestampUnit for TIMESTAMP64 (unused for DATE32, which the kernel dispatches
// on directly).
VecResult draken_unixtime(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
  // Not DRAKEN_KERNEL_TRY: that macro splits its argument on any top-level comma
  // not inside (), and this body's brace-initializers ({0,1,...}, struct Guard
  // g{...}) trip that — a plain try/catch avoids the macro entirely.
  try {
        if (nargs != 1) return draken_error_sentinel("draken_unixtime: expected 1 argument");
        const DrakenVector* v = args[0];
        const uint32_t n = v->length;
        auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("draken_unixtime: allocation failed");

        if (v->type == DRAKEN_TIMESTAMP64) {
            if (ctx == nullptr) { draken_free(out); return draken_error_sentinel("draken_unixtime: missing ctx (unit)"); }
            const auto* c = static_cast<const binary_op_ctx*>(ctx);
            const int64_t tps = ta_ticks_per_second(c->left_unit);
            const int64_t* src = static_cast<const int64_t*>(v->data);
            for (uint32_t i = 0u; i < n; ++i)
                out[i] = ft_row_valid(v, i) ? ta_floor_div(src[v->selection[i]], tps) : 0;
        } else if (v->type == DRAKEN_DATE32) {
            const int32_t* src = static_cast<const int32_t*>(v->data);
            for (uint32_t i = 0u; i < n; ++i)
                out[i] = ft_row_valid(v, i) ? static_cast<int64_t>(src[v->selection[i]]) * 86400LL : 0;
        } else {
            draken_free(out);
            return draken_error_sentinel_fmt(
                "draken_unixtime: expected TIMESTAMP64 or DATE32, got DrakenType %d",
                static_cast<int>(v->type));
        }
        return ft_dense_result(v, out, n, DRAKEN_INT64);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_unixtime: unknown error");
    }
}

// TIME_BUCKET(magnitude, units, date) — floor a TIMESTAMP64 or DATE32 operand to a
// `magnitude`-wide bucket boundary. magnitude and units are bind-time literals,
// consumed into ctx = time_bucket_ctx{magnitude, unit_kind, ts_unit}
// (kernel_context.h); only `date` is pushed. Output is always TIMESTAMP64.
//
// unit_kind:
//   1=second 2=minute 3=hour 4=day  — fixed-width integer floors from the epoch.
//   5=week                          — 7-day floor anchored to the ISO Monday on or
//                                      before the epoch (1969-12-29, matching
//                                      date_trunc's week boundary).
//   6=month 7=quarter 8=year        — calendar buckets counted from 1970-01
//                                      (epoch-anchored): magnitude>1 groups whole
//                                      calendar units from the epoch; magnitude==1
//                                      reduces exactly to date_trunc.
//
// TIMESTAMP64 output preserves the operand's unit (the floor is a pure integer
// alignment, no rescale). A DATE32 operand carries no logical unit; it is promoted
// to microseconds and the result is TIMESTAMP64(microseconds) — the same DATE32
// convention date_trunc uses.
VecResult draken_time_bucket(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
  try {
        if (nargs != 1) return draken_error_sentinel("draken_time_bucket: expected 1 argument");
        if (ctx == nullptr) return draken_error_sentinel("draken_time_bucket: missing ctx");
        const auto* c = static_cast<const time_bucket_ctx*>(ctx);
        const DrakenVector* v = args[0];
        const bool is_date32 = (v->type == DRAKEN_DATE32);
        if (v->type != DRAKEN_TIMESTAMP64 && !is_date32)
            return draken_error_sentinel_fmt(
                "draken_time_bucket: expected TIMESTAMP64 or DATE32, got DrakenType %d",
                static_cast<int>(v->type));
        if (c->magnitude <= 0)
            return draken_error_sentinel_fmt(
                "draken_time_bucket: magnitude must be positive, got %lld",
                static_cast<long long>(c->magnitude));
        if (c->unit_kind < 1u || c->unit_kind > 8u)
            return draken_error_sentinel("draken_time_bucket: unsupported unit kind");

        const int64_t mag = c->magnitude;
        // DATE32 has no logical unit; work in microseconds and emit TIMESTAMP64(us).
        const int     out_unit = is_date32 ? 2 : static_cast<int>(c->ts_unit);
        const int64_t tps      = ta_ticks_per_second(out_unit);
        const int64_t tpd      = ta_ticks_per_day(out_unit);

        const uint32_t n = v->length;
        auto* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("draken_time_bucket: allocation failed");

        // Gather to flat int64 ticks in `out_unit`, promoting DATE32 days→ticks via
        // the uniform data[selection[i]] access; null rows produce 0 (validity is
        // carried by ft_dense_result, so the placeholder value is never read).
        if (is_date32) {
            const int32_t* src = static_cast<const int32_t*>(v->data);
            for (uint32_t i = 0u; i < n; ++i)
                out[i] = ft_row_valid(v, i) ? static_cast<int64_t>(src[v->selection[i]]) * tpd : 0;
        } else {
            const int64_t* src = static_cast<const int64_t*>(v->data);
            for (uint32_t i = 0u; i < n; ++i)
                out[i] = ft_row_valid(v, i) ? src[v->selection[i]] : 0;
        }

        // Bucket in place. The switch is hoisted out of the row loop so each unit
        // kind runs a branch-free loop.
        switch (c->unit_kind) {
            case 1: case 2: case 3: case 4: {   // second/minute/hour/day
                static const int64_t seconds_per_unit_kind[5] = {0, 1, 60, 3600, 86400};
                const int64_t period = seconds_per_unit_kind[c->unit_kind] * tps * mag;
                for (uint32_t i = 0u; i < n; ++i)
                    out[i] = ta_floor_div(out[i], period) * period;
                break;
            }
            case 5: {   // week — 7-day, anchored to the ISO Monday on/before epoch (day -3)
                for (uint32_t i = 0u; i < n; ++i) {
                    const int64_t days          = ta_floor_div(out[i], tpd);
                    const int64_t weeks_from_ref = ta_floor_div(days + 3LL, 7LL);
                    const int64_t bucket_week    = ta_floor_div(weeks_from_ref, mag) * mag;
                    out[i] = (-3LL + bucket_week * 7LL) * tpd;
                }
                break;
            }
            case 6: {   // month — epoch-anchored calendar months
                for (uint32_t i = 0u; i < n; ++i) {
                    const int64_t days = ta_floor_div(ta_floor_div(out[i], tps), 86400LL);
                    int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                    const int64_t mtot = (static_cast<int64_t>(yr) - 1970) * 12 + (mo - 1);
                    const int64_t mb   = ta_floor_div(mtot, mag) * mag;
                    const int64_t by   = 1970 + ta_floor_div(mb, 12);
                    const int     bm   = static_cast<int>(mb - ta_floor_div(mb, 12) * 12) + 1;
                    out[i] = ta_ymd_to_days(static_cast<int>(by), bm, 1) * tpd;
                }
                break;
            }
            case 7: {   // quarter — epoch-anchored calendar quarters (3-month blocks)
                for (uint32_t i = 0u; i < n; ++i) {
                    const int64_t days = ta_floor_div(ta_floor_div(out[i], tps), 86400LL);
                    int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                    const int64_t qtot = (static_cast<int64_t>(yr) - 1970) * 4 + (mo - 1) / 3;
                    const int64_t qb   = ta_floor_div(qtot, mag) * mag;
                    const int64_t by   = 1970 + ta_floor_div(qb, 4);
                    const int     bq   = static_cast<int>(qb - ta_floor_div(qb, 4) * 4);
                    out[i] = ta_ymd_to_days(static_cast<int>(by), bq * 3 + 1, 1) * tpd;
                }
                break;
            }
            case 8: {   // year — epoch-anchored calendar years
                for (uint32_t i = 0u; i < n; ++i) {
                    const int64_t days = ta_floor_div(ta_floor_div(out[i], tps), 86400LL);
                    int yr, mo, dy; ta_days_to_ymd(days, &yr, &mo, &dy);
                    const int64_t by = 1970 + ta_floor_div(static_cast<int64_t>(yr) - 1970, mag) * mag;
                    out[i] = ta_ymd_to_days(static_cast<int>(by), 1, 1) * tpd;
                }
                break;
            }
        }

        VecResult r = ft_dense_result(v, out, n, DRAKEN_TIMESTAMP64);
        if (r.data != nullptr) r.ts_unit = static_cast<uint8_t>(out_unit);
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_time_bucket: unknown error");
    }
}

// FORMAT_TIMESTAMP(pattern, date) — TIMESTAMP64|DATE32 -> VARCHAR via the shared
// compiled token-program formatter (draken/ops/temporal_format.h — the SAME
// formatter the nanobind FORMAT_TIMESTAMP path uses; not re-implemented here).
// ctx = format_ctx{ts_unit, fmt_len} + pattern bytes trailing the struct.
VecResult draken_date_format(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
  try {
        if (nargs != 1) return draken_error_sentinel("draken_date_format: expected 1 argument");
        if (ctx == nullptr) return draken_error_sentinel("draken_date_format: missing ctx (pattern)");
        const auto* c = static_cast<const format_ctx*>(ctx);
        const DrakenVector* v = args[0];
        if (v->type != DRAKEN_TIMESTAMP64 && v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt(
                "draken_date_format: expected TIMESTAMP64 or DATE32, got DrakenType %d",
                static_cast<int>(v->type));

        // Pattern bytes are NOT NUL-terminated (format_ctx doc) — tf_compile needs
        // a C string; copy once per kernel call (per morsel, not per row).
        const std::string fmt(format_ctx_fmt(c), static_cast<size_t>(c->fmt_len));
        char bad_spec = 0;
        if (!tf_validate(fmt.c_str(), &bad_spec)) {
            return draken_error_sentinel_fmt(
                "draken_date_format: unsupported format token '%%%c' (or trailing '%%')",
                bad_spec ? bad_spec : '?');
        }

        const bool is_date32 = (v->type == DRAKEN_DATE32);
        const int  unit_code = is_date32 ? 2 : c->ts_unit;
        const int64_t tps    = ta_ticks_per_second(unit_code);
        const uint32_t n     = v->length;

        size_t max_row_len = 0;
        const std::vector<TfToken> prog = tf_compile(fmt.c_str(), &max_row_len);
        std::vector<char> row_vec(max_row_len);
        char* const row_buf = row_vec.data();

        const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
        auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!slots) return draken_error_sentinel("draken_date_format: allocation failed");
        std::memset(slots, 0, slots_sz);

        size_t arena_cap = (n > 0u ? static_cast<size_t>(n) * 32u : 32u);
        auto* arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
        if (!arena) { draken_free(slots); return draken_error_sentinel("draken_date_format: allocation failed"); }

        struct Guard {
            DrakenStringSlot* s; uint8_t* a; uint8_t* v;
            ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
        } g{slots, arena, nullptr};

        g.v = kernel_copy_validity(v);
        size_t arena_used = 0u;

        for (uint32_t i = 0u; i < n; ++i) {
            if (!ft_row_valid(v, i)) { str_init_null(&slots[i]); continue; }

            int64_t days, secs_in_day;
            if (is_date32) {
                const int32_t* src = static_cast<const int32_t*>(v->data);
                days        = static_cast<int64_t>(src[v->selection[i]]);
                secs_in_day = 0;
            } else {
                const int64_t* src = static_cast<const int64_t*>(v->data);
                const int64_t  sec = ta_floor_div(src[v->selection[i]], tps);
                days        = ta_floor_div(sec, 86400LL);
                secs_in_day = sec - days * 86400LL;
            }

            const TfFields f = tf_decompose(days, secs_in_day);

            char* p = row_buf;
            for (const TfToken& tok : prog) {
                if (tok.spec == 0) {
                    std::memcpy(p, tok.lit, tok.lit_len);
                    p += tok.lit_len;
                } else {
                    p = tf_emit_spec(p, tok.spec, f);
                }
            }
            const uint32_t slen = static_cast<uint32_t>(p - row_buf);

            if (slen <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], reinterpret_cast<const uint8_t*>(row_buf), slen);
            } else {
                if (arena_used + slen > arena_cap) {
                    arena_cap = (arena_used + slen) * 2u;
                    auto* new_arena = static_cast<uint8_t*>(draken_malloc(arena_cap));
                    if (!new_arena) throw std::bad_alloc();
                    std::memcpy(new_arena, g.a, arena_used);
                    draken_free(g.a);
                    g.a = new_arena;
                }
                const uint32_t arena_off = static_cast<uint32_t>(arena_used);
                std::memcpy(g.a + arena_off, row_buf, slen);
                draken_build_string_slot(&slots[i], reinterpret_cast<const uint8_t*>(row_buf),
                                         slen, arena_off);
                arena_used += slen;
            }
        }

        uint8_t* validity         = g.v;
        DrakenStringSlot* out_slots = g.s;
        uint8_t*          out_arena = g.a;
        g.s = nullptr; g.a = nullptr; g.v = nullptr;

        return vecresult_from_string_buffers(out_slots, out_arena, arena_used, validity, n, DRAKEN_VARCHAR);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("draken_date_format: unknown error");
    }
}

}  // extern "C"
