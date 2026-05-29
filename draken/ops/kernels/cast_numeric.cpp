#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include <cstring>

/**
 * Cast kernels: numeric / boolean conversions (Phase 9c).
 *
 * Real VecResult-producing bodies extracted from the proven nanobind compute in
 * opteryx/compiled/nanobind/vector_casts.cpp. The nanobind functions now shim to
 * these kernels (core lives in draken; the binding layer calls across).
 *
 * Dense output: out[i] holds logical row i; selection is the global identity.
 * Null rows write a placeholder value and clear their validity bit (copied 1:1
 * from the input bitmap, which is also logical-row indexed).
 */

// File-scope string literals for bool→string. Defined outside any kernel body:
// brace-initializer commas would otherwise be parsed as DRAKEN_KERNEL_TRY macro
// argument separators.
static const uint8_t kBoolTrue[]  = { 't', 'r', 'u', 'e' };
static const uint8_t kBoolFalse[] = { 'f', 'a', 'l', 's', 'e' };

extern "C" {

// --- int → decimal ASCII (hand-rolled; matches the old .pyx fast path) -------

static inline int i64_to_ascii(int64_t value, char* buf) noexcept {
    if (value == 0) { buf[0] = '0'; return 1; }
    int i = 20;
    bool neg = value < 0;
    uint64_t uval = neg ? static_cast<uint64_t>(-value) : static_cast<uint64_t>(value);
    while (uval) { buf[--i] = '0' + static_cast<int>(uval % 10u); uval /= 10u; }
    if (neg) buf[--i] = '-';
    int len = 20 - i;
    if (i) std::memmove(buf, buf + i, static_cast<size_t>(len));
    return len;
}

static inline int u64_to_ascii(uint64_t value, char* buf) noexcept {
    if (value == 0) { buf[0] = '0'; return 1; }
    int i = 20;
    while (value) { buf[--i] = '0' + static_cast<int>(value % 10u); value /= 10u; }
    int len = 20 - i;
    if (i) std::memmove(buf, buf + i, static_cast<size_t>(len));
    return len;
}

// Shared int64/uint64 → VARCHAR core. Two passes: size the arena, then fill.
static VecResult int_to_string_core(const DrakenVector* v, bool treat_as_unsigned) {
    if (!v) return draken_error_sentinel("Input vector is null");
    if (v->type != DRAKEN_INT64)
        return draken_error_sentinel_fmt("cast-to-string: expected INT64, got %d", v->type);

    const int64_t* src = static_cast<const int64_t*>(v->data);
    const uint32_t n   = v->length;

    char tmp[21];
    size_t total_extern = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (kernel_row_is_null(v, i)) continue;
        const int64_t raw = src[v->selection[i]];
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(raw), tmp)
                                     : i64_to_ascii(raw, tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) total_extern += static_cast<size_t>(len);
    }

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) return draken_error_sentinel("Allocation failed");
    std::memset(slots, 0, slots_sz);

    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(total_extern > 0u ? total_extern : 1u));
    if (!arena) { draken_free(slots); return draken_error_sentinel("Allocation failed"); }

    uint8_t* validity = kernel_copy_validity(v);

    size_t arena_used = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (kernel_row_is_null(v, i)) { str_init_null(&slots[i]); continue; }
        const int64_t raw = src[v->selection[i]];
        int len = treat_as_unsigned ? u64_to_ascii(static_cast<uint64_t>(raw), tmp)
                                     : i64_to_ascii(raw, tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(arena + off, tmp, static_cast<size_t>(len));
            draken_build_string_slot(&slots[i], reinterpret_cast<const uint8_t*>(tmp),
                                     static_cast<uint32_t>(len), off);
            arena_used += static_cast<size_t>(len);
        } else {
            draken_build_string_slot(&slots[i], reinterpret_cast<const uint8_t*>(tmp),
                                     static_cast<uint32_t>(len), 0u);
        }
    }

    // Consumes slots/arena/validity; embeds validity in the consolidated block.
    return vecresult_from_string_buffers(slots, arena, arena_used, validity, n, DRAKEN_VARCHAR);
}

// --- Numeric primitive output helpers ----------------------------------------

// Allocate a dense output buffer + a copy of the validity bitmap, returning
// false (and an error sentinel via *err) on allocation failure.
static inline VecResult make_numeric_result(void* data, uint8_t* validity,
                                            uint32_t n, DrakenType type) {
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = type;
    r.flags          = 0;
    return r;
}

VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->float64: expected INT64, got %d", v->type);
        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        uint8_t* val = kernel_copy_validity(v);
        double* out = static_cast<double*>(draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        for (uint32_t i = 0u; i < n; ++i)
            out[i] = kernel_row_is_null(v, i) ? 0.0 : static_cast<double>(src[v->selection[i]]);
        return make_numeric_result(out, val, n, DRAKEN_FLOAT64);
    });
}

VecResult draken_cast_int64_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->bool: expected INT64, got %d", v->type);
        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        uint8_t* val = kernel_copy_validity(v);
        const size_t nbytes = (n > 0u ? (n + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        std::memset(out, 0, nbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) continue;
            if (src[v->selection[i]] != 0)
                out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
        }
        return make_numeric_result(out, val, n, DRAKEN_BOOL);
    });
}

VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({ return int_to_string_core(v, /*unsigned=*/false); });
}

VecResult draken_cast_bool_to_float64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->float64: expected BOOL, got %d", v->type);
        const uint32_t n = v->length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        uint8_t* val = kernel_copy_validity(v);
        double* out = static_cast<double*>(draken_malloc((n > 0u ? n : 1u) * sizeof(double)));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { out[i] = 0.0; continue; }
            const uint32_t b = v->selection[i];
            out[i] = static_cast<double>((src[b >> 3u] >> (b & 7u)) & 1u);
        }
        return make_numeric_result(out, val, n, DRAKEN_FLOAT64);
    });
}

VecResult draken_cast_bool_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->int64: expected BOOL, got %d", v->type);
        const uint32_t n = v->length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);
        uint8_t* val = kernel_copy_validity(v);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { out[i] = 0; continue; }
            const uint32_t b = v->selection[i];
            out[i] = static_cast<int64_t>((src[b >> 3u] >> (b & 7u)) & 1u);
        }
        return make_numeric_result(out, val, n, DRAKEN_INT64);
    });
}

VecResult draken_cast_bool_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_BOOL)
            return draken_error_sentinel_fmt("cast bool->string: expected BOOL, got %d", v->type);
        const uint32_t n = v->length;
        const uint8_t* src = static_cast<const uint8_t*>(v->data);

        const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
        DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
        if (!slots) return draken_error_sentinel("Allocation failed");
        std::memset(slots, 0, slots_sz);
        uint8_t* arena = static_cast<uint8_t*>(draken_malloc(1u));  // all inline
        if (!arena) { draken_free(slots); return draken_error_sentinel("Allocation failed"); }
        uint8_t* validity = kernel_copy_validity(v);

        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { str_init_null(&slots[i]); continue; }
            const uint32_t b = v->selection[i];
            const bool val = static_cast<bool>((src[b >> 3u] >> (b & 7u)) & 1u);
            draken_build_string_slot(&slots[i], val ? kBoolTrue : kBoolFalse, val ? 4u : 5u, 0u);
        }
        return vecresult_from_string_buffers(slots, arena, 0u, validity, n, DRAKEN_VARCHAR);
    });
}

VecResult draken_cast_float64_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->int64: expected FLOAT, got %d", v->type);
        const uint32_t n = v->length;
        uint8_t* val = kernel_copy_validity(v);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) { out[i] = 0; continue; }
            const uint32_t si = v->selection[i];
            const double d = (v->type == DRAKEN_FLOAT64)
                ? static_cast<const double*>(v->data)[si]
                : static_cast<double>(static_cast<const float*>(v->data)[si]);
            out[i] = static_cast<int64_t>(d);  // truncate toward zero
        }
        return make_numeric_result(out, val, n, DRAKEN_INT64);
    });
}

VecResult draken_cast_float64_to_bool(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_FLOAT64 && v->type != DRAKEN_FLOAT32)
            return draken_error_sentinel_fmt("cast float->bool: expected FLOAT, got %d", v->type);
        const uint32_t n = v->length;
        uint8_t* val = kernel_copy_validity(v);
        const size_t nbytes = (n > 0u ? (n + 7u) / 8u : 1u);
        uint8_t* out = static_cast<uint8_t*>(draken_malloc(nbytes));
        if (!out) { draken_free(val); return draken_error_sentinel("Allocation failed"); }
        std::memset(out, 0, nbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            if (kernel_row_is_null(v, i)) continue;
            const uint32_t si = v->selection[i];
            const double d = (v->type == DRAKEN_FLOAT64)
                ? static_cast<const double*>(v->data)[si]
                : static_cast<double>(static_cast<const float*>(v->data)[si]);
            if (d != 0.0) out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
        }
        return make_numeric_result(out, val, n, DRAKEN_BOOL);
    });
}

// FLOAT64 → VARCHAR: no extracted compute yet (no nanobind source). Remains a
// stub until a float-formatting kernel is written; not requested by the binder.
VecResult draken_cast_float64_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("cast float64->string not yet implemented"); });
}

}  // extern "C"
