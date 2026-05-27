// opteryx/compiled/nanobind/vector_casts.cpp — Milestone E.9, Phase 8, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions:
//
//   vector_cast_int64_to_string   — DRAKEN_INT64  → DRAKEN_VARCHAR (decimal ASCII, signed).
//   vector_cast_uint64_to_string  — DRAKEN_INT64  → DRAKEN_VARCHAR (decimal ASCII, unsigned bits).
//   vector_cast_string_to_int     — string family → DRAKEN_INT64.  Raises ValueError on invalid.
//   vector_cast_int64_to_timestamp — DRAKEN_INT64 → DRAKEN_TIMESTAMP64.  unit param mandatory.
//
// Null TVL: null input row → null output row; validity bitmap preserved.
// Fails loud on non-Vector input.
//
// Int↔string conversion: hand-rolled ASCII digit loops (matches and replaces the
// proven-faster hand-rolled code in the old .pyx files).  Run a bake-off against
// std::to_chars/std::from_chars if revisiting performance.
//
// Replaces: opteryx/compiled/vector_ops/vector_cast_int64_to_string.pyx
//           opteryx/compiled/vector_ops/vector_cast_uint64_to_string.pyx
//           opteryx/compiled/vector_ops/vector_cast_string_to_int.pyx
//           opteryx/compiled/vector_ops/vector_cast_int64_to_timestamp.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static inline bool row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return false;
    return !((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

// Deep-copy the validity bitmap from dv into a fresh draken_malloc'd buffer.
// Returns nullptr when dv->validity is nullptr (all-valid normalization invariant).
static uint8_t* copy_validity(const DrakenVector* dv) {
    if (!dv->validity) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}

static const DrakenVector* unwrap_int64(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_INT64)
        throw nb::type_error("expected an INT64 Vector");
    return dv;
}

static const DrakenVector* unwrap_string(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR &&
        dv->type != DRAKEN_VARBINARY)
        throw nb::type_error("expected a string-family Vector (VARCHAR/NVARCHAR/VARBINARY)");
    return dv;
}

// ---------------------------------------------------------------------------
// Hand-rolled integer → decimal ASCII conversion.
//
// Writes ASCII digits of value into buf[0..20] (21-byte scratch) and
// returns the byte length.  No NUL terminator.  buf[0] starts the string.
// ---------------------------------------------------------------------------

static inline int i64_to_ascii(int64_t value, char* buf) noexcept {
    if (value == 0) { buf[0] = '0'; return 1; }
    int i = 20;
    bool neg = value < 0;
    uint64_t uval = neg ? static_cast<uint64_t>(-value) : static_cast<uint64_t>(value);
    while (uval) { buf[--i] = '0' + static_cast<int>(uval % 10u); uval /= 10u; }
    if (neg) buf[--i] = '-';
    // Shift the result to buf[0..].
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

// ---------------------------------------------------------------------------
// vector_cast_int64_to_string / vector_cast_uint64_to_string
//
// Both follow the same shape; only the per-row conversion differs.
// Max output per row: 20 digits + sign = 21 bytes > STR_INLINE_MAX (12), so
// values ≥ 10^12 (or ≤ −10^11) spill to arena.  Two-pass approach:
//   Pass 1: compute total arena bytes needed.
//   Pass 2: fill slots.
// ---------------------------------------------------------------------------

static nb::object int_to_string_apply(nb::object obj, bool treat_as_unsigned) {
    const DrakenVector* dv = unwrap_int64(obj);
    const int64_t*  src = static_cast<const int64_t*>(dv->data);
    const uint32_t  n   = dv->length;

    // Pass 1: count arena bytes (strings longer than STR_INLINE_MAX).
    char tmp[21];
    size_t total_extern = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;
        const int64_t raw = src[dv->selection[i]];
        int len = treat_as_unsigned
            ? u64_to_ascii(static_cast<uint64_t>(raw), tmp)
            : i64_to_ascii(raw, tmp);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX)
            total_extern += static_cast<size_t>(len);
    }

    // Allocate output slots.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // Allocate arena (1-byte minimum so draken_vector_own_string always has a valid ptr).
    uint8_t* arena = static_cast<uint8_t*>(
        draken_malloc(total_extern > 0u ? total_extern : 1u));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    size_t arena_used = 0u;

    // Pass 2: fill slots.
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { str_init_null(&slots[i]); continue; }
        const int64_t raw = src[dv->selection[i]];
        int len = treat_as_unsigned
            ? u64_to_ascii(static_cast<uint64_t>(raw), tmp)
            : i64_to_ascii(raw, tmp);
        const uint32_t off = static_cast<uint32_t>(arena_used);
        if (static_cast<uint32_t>(len) > STR_INLINE_MAX) {
            std::memcpy(arena + off, tmp, static_cast<size_t>(len));
            draken_build_string_slot(&slots[i],
                reinterpret_cast<const uint8_t*>(tmp),
                static_cast<uint32_t>(len), off);
            arena_used += static_cast<size_t>(len);
        } else {
            draken_build_string_slot(&slots[i],
                reinterpret_cast<const uint8_t*>(tmp),
                static_cast<uint32_t>(len), 0u);
        }
    }

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_string(slots, arena, arena_used,
                                             out_validity, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_cast_string_to_int
//
// Parses each non-null slot as a signed decimal integer.  Raises ValueError on
// any non-digit character (matching the old .pyx parse_int64 behaviour).
// Null in → 0 stored in data, validity bit cleared (null out).
// ---------------------------------------------------------------------------

static nb::object string_to_int_apply(nb::object obj) {
    const DrakenVector*      dv = unwrap_string(obj);
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t           n  = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out_data = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out_data) throw std::bad_alloc();

    struct Guard {
        int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); }
    } g{out_data, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out_data[i] = 0; continue; }

        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint8_t*          sdata = str_data(slot, sa->arena);
        const uint32_t          slen  = str_length(slot);

        int64_t value = 0;
        int64_t sign  = 1;
        uint32_t k    = 0;

        if (slen > 0 && sdata[0] == '-') { sign = -1; k = 1; }

        for (; k < slen; ++k) {
            const uint8_t c = sdata[k];
            if (c < '0' || c > '9') {
                PyErr_SetString(PyExc_ValueError, "Invalid digit in integer literal");
                return nb::object();  // Guard frees buffers
            }
            value = value * 10 + (c - '0');
        }
        out_data[i] = sign * value;
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_raw(out_data, out_validity, n, DRAKEN_INT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_cast_int64_to_timestamp
//
// Zero-transformation: the int64 values are already in the correct physical
// unit (no multiply/divide needed in the new draken model).  We memcpy the
// data buffer and attach the mandatory LogicalType descriptor via
// draken_vector_own_timestamp.
// ---------------------------------------------------------------------------

static nb::object int64_to_timestamp_apply(nb::object obj, const char* unit_str) {
    const DrakenVector* dv = unwrap_int64(obj);
    const uint32_t      n  = dv->length;

    // Materialise a dense int64 array in logical-row order.
    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out_data = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out_data) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    const int64_t* src = static_cast<const int64_t*>(dv->data);
    for (uint32_t i = 0u; i < n; ++i)
        out_data[i] = src[dv->selection[i]];

    g.d = nullptr; g.v = nullptr;
    // draken_vector_own_timestamp consumes both buffers, attaches LogicalType.
    PyObject* out = draken_vector_own_timestamp(out_data, out_validity, n, unit_str);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Additional unwrap helpers for non-INT64/string types.
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap_float64(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_FLOAT64 && dv->type != DRAKEN_FLOAT32)
        throw nb::type_error("expected a FLOAT64 or FLOAT32 Vector");
    return dv;
}

static const DrakenVector* unwrap_bool(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_BOOL)
        throw nb::type_error("expected a BOOL Vector");
    return dv;
}

static const DrakenVector* unwrap_date32(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_DATE32)
        throw nb::type_error("expected a DATE32 Vector");
    return dv;
}

static const DrakenVector* unwrap_timestamp(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_TIMESTAMP64)
        throw nb::type_error("expected a TIMESTAMP64 Vector");
    return dv;
}

// ---------------------------------------------------------------------------
// Priority 1 — numeric/temporal widening and boolean expansion.
// ---------------------------------------------------------------------------

// INT64 → FLOAT64: widening cast, no precision loss for |v| < 2^53.
static nb::object int64_to_float64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_int64(obj);
    const int64_t* src = static_cast<const int64_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(double);
    double* out = static_cast<double*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { double* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0.0; continue; }
        out[i] = static_cast<double>(src[dv->selection[i]]);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_FLOAT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// BOOL → INT64: expand bit-packed bool (0 or 1 per row) to int64.
static nb::object bool_to_int64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_bool(obj);
    const uint8_t* src = static_cast<const uint8_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        const uint32_t bit_idx = dv->selection[i];
        out[i] = static_cast<int64_t>((src[bit_idx >> 3u] >> (bit_idx & 7u)) & 1u);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// INT64 → BOOL: 0 → false, non-zero → true.  Output is bit-packed DRAKEN_BOOL.
static nb::object int64_to_bool_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_int64(obj);
    const int64_t* src = static_cast<const int64_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? (n + 7u) / 8u : 1u);
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, data_sz);

    struct Guard { uint8_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;
        if (src[dv->selection[i]] != 0)
            out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_BOOL);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// FLOAT64/FLOAT32 → BOOL: 0.0 → false, non-zero → true.  Output is bit-packed.
static nb::object float64_to_bool_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_float64(obj);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? (n + 7u) / 8u : 1u);
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, data_sz);

    struct Guard { uint8_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;
        double val;
        if (dv->type == DRAKEN_FLOAT64)
            val = static_cast<const double*>(dv->data)[dv->selection[i]];
        else
            val = static_cast<double>(static_cast<const float*>(dv->data)[dv->selection[i]]);
        if (val != 0.0)
            out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_BOOL);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// DATE32 → INT64: widen int32 days-since-epoch to int64.
static nb::object date32_to_int64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_date32(obj);
    const int32_t* src = static_cast<const int32_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        out[i] = static_cast<int64_t>(src[dv->selection[i]]);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// TIMESTAMP64 → INT64: identity retype — microseconds-since-epoch stored as int64.
static nb::object timestamp_to_int64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_timestamp(obj);
    const int64_t* src = static_cast<const int64_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        out[i] = src[dv->selection[i]];
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// Priority 2 — string formatting.
// ---------------------------------------------------------------------------

// BOOL → VARCHAR: "true" or "false".  Both fit inline (≤ STR_INLINE_MAX=12).
static nb::object bool_to_string_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_bool(obj);
    const uint8_t* src = static_cast<const uint8_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // "true"/"false" are always inline; allocate 1-byte arena so own_string
    // always receives a valid non-null arena pointer.
    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(1u));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    static const uint8_t kTrue[]  = { 't', 'r', 'u', 'e' };
    static const uint8_t kFalse[] = { 'f', 'a', 'l', 's', 'e' };

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { str_init_null(&slots[i]); continue; }
        const uint32_t bit_idx = dv->selection[i];
        const bool val = static_cast<bool>((src[bit_idx >> 3u] >> (bit_idx & 7u)) & 1u);
        draken_build_string_slot(&slots[i], val ? kTrue : kFalse, val ? 4u : 5u, 0u);
    }

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_string(slots, arena, 0u, out_validity, n, DRAKEN_VARCHAR);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// BOOL → FLOAT64: false → 0.0, true → 1.0.
static nb::object bool_to_float64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_bool(obj);
    const uint8_t* src = static_cast<const uint8_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(double);
    double* out = static_cast<double*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { double* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0.0; continue; }
        const uint32_t bit_idx = dv->selection[i];
        out[i] = static_cast<double>((src[bit_idx >> 3u] >> (bit_idx & 7u)) & 1u);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_FLOAT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// Gregorian calendar: days-since-epoch (1970-01-01) → (year, month, day).
// Howard Hinnant's algorithm: https://howardhinnant.github.io/date_algorithms.html
static void days_to_ymd(int32_t days, int32_t& y, int32_t& m, int32_t& d) noexcept {
    const int32_t z   = days + 719468;
    const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int32_t doe = z - era * 146097;
    const int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    const int32_t yr  = yoe + era * 400;
    const int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    const int32_t mp  = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = yr + (m <= 2 ? 1 : 0);
}

// DATE32 → VARCHAR: "YYYY-MM-DD" (10 chars, always inline ≤ STR_INLINE_MAX=12).
static nb::object date_to_string_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_date32(obj);
    const int32_t* src = static_cast<const int32_t*>(dv->data);
    const uint32_t n = dv->length;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(1u));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    uint8_t buf[10];
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { str_init_null(&slots[i]); continue; }
        int32_t y, m, d;
        days_to_ymd(src[dv->selection[i]], y, m, d);
        buf[0] = static_cast<uint8_t>('0' + (y / 1000) % 10);
        buf[1] = static_cast<uint8_t>('0' + (y / 100)  % 10);
        buf[2] = static_cast<uint8_t>('0' + (y / 10)   % 10);
        buf[3] = static_cast<uint8_t>('0' + y % 10);
        buf[4] = '-';
        buf[5] = static_cast<uint8_t>('0' + m / 10);
        buf[6] = static_cast<uint8_t>('0' + m % 10);
        buf[7] = '-';
        buf[8] = static_cast<uint8_t>('0' + d / 10);
        buf[9] = static_cast<uint8_t>('0' + d % 10);
        draken_build_string_slot(&slots[i], buf, 10u, 0u);
    }

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_string(slots, arena, 0u, out_validity, n, DRAKEN_VARCHAR);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// TIMESTAMP64 → VARCHAR: "YYYY-MM-DD HH:MM:SS.ffffff+0000" (31 chars, always extern).
// Input is int64 microseconds-since-epoch (UTC); offset is always +0000.
static nb::object timestamp_to_string_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_timestamp(obj);
    const int64_t* src = static_cast<const int64_t*>(dv->data);
    const uint32_t n = dv->length;

    // Count non-null rows; each produces exactly 31 arena bytes.
    uint32_t valid_count = 0u;
    for (uint32_t i = 0u; i < n; ++i)
        if (!row_is_null(dv, i)) ++valid_count;
    const size_t total_arena = static_cast<size_t>(valid_count) * 31u;

    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(total_arena > 0u ? total_arena : 1u));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); if (v) draken_free(v); }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    size_t arena_used = 0u;
    uint8_t buf[31];
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { str_init_null(&slots[i]); continue; }

        int64_t us = src[dv->selection[i]];
        int64_t sec = us / 1000000LL;
        int32_t usec = static_cast<int32_t>(us % 1000000LL);
        if (usec < 0) { sec -= 1; usec += 1000000; }

        int64_t days64 = sec / 86400LL;
        int32_t tod = static_cast<int32_t>(sec % 86400LL);
        if (tod < 0) { days64 -= 1; tod += 86400; }

        int32_t y, m, d;
        days_to_ymd(static_cast<int32_t>(days64), y, m, d);

        const int32_t hh = tod / 3600;
        const int32_t mm = (tod % 3600) / 60;
        const int32_t ss = tod % 60;

        buf[0]  = static_cast<uint8_t>('0' + (y / 1000) % 10);
        buf[1]  = static_cast<uint8_t>('0' + (y / 100)  % 10);
        buf[2]  = static_cast<uint8_t>('0' + (y / 10)   % 10);
        buf[3]  = static_cast<uint8_t>('0' + y % 10);
        buf[4]  = '-';
        buf[5]  = static_cast<uint8_t>('0' + m / 10);
        buf[6]  = static_cast<uint8_t>('0' + m % 10);
        buf[7]  = '-';
        buf[8]  = static_cast<uint8_t>('0' + d / 10);
        buf[9]  = static_cast<uint8_t>('0' + d % 10);
        buf[10] = ' ';
        buf[11] = static_cast<uint8_t>('0' + hh / 10);
        buf[12] = static_cast<uint8_t>('0' + hh % 10);
        buf[13] = ':';
        buf[14] = static_cast<uint8_t>('0' + mm / 10);
        buf[15] = static_cast<uint8_t>('0' + mm % 10);
        buf[16] = ':';
        buf[17] = static_cast<uint8_t>('0' + ss / 10);
        buf[18] = static_cast<uint8_t>('0' + ss % 10);
        buf[19] = '.';
        buf[20] = static_cast<uint8_t>('0' + (usec / 100000) % 10);
        buf[21] = static_cast<uint8_t>('0' + (usec / 10000)  % 10);
        buf[22] = static_cast<uint8_t>('0' + (usec / 1000)   % 10);
        buf[23] = static_cast<uint8_t>('0' + (usec / 100)    % 10);
        buf[24] = static_cast<uint8_t>('0' + (usec / 10)     % 10);
        buf[25] = static_cast<uint8_t>('0' + usec % 10);
        buf[26] = '+'; buf[27] = '0'; buf[28] = '0'; buf[29] = '0'; buf[30] = '0';

        std::memcpy(arena + arena_used, buf, 31u);
        draken_build_string_slot(&slots[i], buf, 31u, static_cast<uint32_t>(arena_used));
        arena_used += 31u;
    }

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_string(slots, arena, arena_used, out_validity, n, DRAKEN_VARCHAR);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// Priority 3 — narrow integer widening and float↔int.
// ---------------------------------------------------------------------------

// unwrap_integer: accepts INT32 / INT16 / INT8.
static const DrakenVector* unwrap_integer(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_INT32 && dv->type != DRAKEN_INT16 && dv->type != DRAKEN_INT8)
        throw nb::type_error("expected an INTEGER Vector (INT32/INT16/INT8)");
    return dv;
}

// INTEGER (INT32/INT16/INT8) → FLOAT64: lossless widening.
static nb::object integer_to_float64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_integer(obj);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(double);
    double* out = static_cast<double*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { double* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0.0; continue; }
        const uint32_t si = dv->selection[i];
        if (dv->type == DRAKEN_INT32)
            out[i] = static_cast<double>(static_cast<const int32_t*>(dv->data)[si]);
        else if (dv->type == DRAKEN_INT16)
            out[i] = static_cast<double>(static_cast<const int16_t*>(dv->data)[si]);
        else  // DRAKEN_INT8
            out[i] = static_cast<double>(static_cast<const int8_t*>(dv->data)[si]);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_FLOAT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// INTEGER (INT32/INT16/INT8) → INT64: sign-extending widening.
static nb::object integer_to_int64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_integer(obj);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        const uint32_t si = dv->selection[i];
        if (dv->type == DRAKEN_INT32)
            out[i] = static_cast<int64_t>(static_cast<const int32_t*>(dv->data)[si]);
        else if (dv->type == DRAKEN_INT16)
            out[i] = static_cast<int64_t>(static_cast<const int16_t*>(dv->data)[si]);
        else  // DRAKEN_INT8
            out[i] = static_cast<int64_t>(static_cast<const int8_t*>(dv->data)[si]);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// FLOAT64/FLOAT32 → INT64: truncate toward zero (C static_cast semantics).
static nb::object float64_to_int64_apply(nb::object obj) {
    const DrakenVector* dv = unwrap_float64(obj);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        const uint32_t si = dv->selection[i];
        double val;
        if (dv->type == DRAKEN_FLOAT64)
            val = static_cast<const double*>(dv->data)[si];
        else
            val = static_cast<double>(static_cast<const float*>(dv->data)[si]);
        out[i] = static_cast<int64_t>(val);
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// STRING → BOOL: "true"/"false", "1"/"0", "yes"/"no", "on"/"off" (case-insensitive).
// Raises ValueError on unrecognized values.
static nb::object string_to_bool_apply(nb::object obj) {
    const DrakenVector*      dv = unwrap_string(obj);
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    const size_t data_sz = (n > 0u ? (n + 7u) / 8u : 1u);
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, data_sz);

    struct Guard { uint8_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;

        const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
        const uint8_t* sdata = str_data(slot, sa->arena);
        const uint32_t slen  = str_length(slot);

        bool val;
        if (slen == 4 &&
            ((sdata[0]|32u) == 't') && ((sdata[1]|32u) == 'r') &&
            ((sdata[2]|32u) == 'u') && ((sdata[3]|32u) == 'e')) {
            val = true;
        } else if (slen == 5 &&
            ((sdata[0]|32u) == 'f') && ((sdata[1]|32u) == 'a') &&
            ((sdata[2]|32u) == 'l') && ((sdata[3]|32u) == 's') &&
            ((sdata[4]|32u) == 'e')) {
            val = false;
        } else if (slen == 1 && sdata[0] == '1') {
            val = true;
        } else if (slen == 1 && sdata[0] == '0') {
            val = false;
        } else if (slen == 3 &&
            ((sdata[0]|32u) == 'y') && ((sdata[1]|32u) == 'e') &&
            ((sdata[2]|32u) == 's')) {
            val = true;
        } else if (slen == 2 &&
            ((sdata[0]|32u) == 'n') && ((sdata[1]|32u) == 'o')) {
            val = false;
        } else if (slen == 2 &&
            ((sdata[0]|32u) == 'o') && ((sdata[1]|32u) == 'n')) {
            val = true;
        } else if (slen == 3 &&
            ((sdata[0]|32u) == 'o') && ((sdata[1]|32u) == 'f') &&
            ((sdata[2]|32u) == 'f')) {
            val = false;
        } else {
            PyErr_SetString(PyExc_ValueError,
                "Cannot cast string to BOOL: expected true/false/1/0/yes/no/on/off");
            return nb::object();
        }

        if (val)
            out[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, out_validity, n, DRAKEN_BOOL);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// NB_MODULE — four functions, one module.
// ---------------------------------------------------------------------------

NB_MODULE(vector_casts, m) {

    m.def("vector_cast_int64_to_string",
        [](nb::object v) -> nb::object {
            return int_to_string_apply(v, false);
        },
        nb::arg("v"),
        "CAST(v AS VARCHAR): element-wise signed int64 → decimal ASCII DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_cast_uint64_to_string",
        [](nb::object v) -> nb::object {
            return int_to_string_apply(v, true);
        },
        nb::arg("v"),
        "CAST(v AS VARCHAR): element-wise uint64 (reinterpreted bits) → decimal ASCII DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_cast_string_to_int",
        [](nb::object v) -> nb::object {
            return string_to_int_apply(v);
        },
        nb::arg("v"),
        "CAST(v AS INT): element-wise decimal ASCII → int64. "
        "Raises ValueError on any non-digit character. Null rows propagate as null.");

    m.def("vector_cast_int64_to_timestamp",
        [](nb::object v, nb::object unit_obj) -> nb::object {
            const char* unit_str = "us";
            if (!unit_obj.is_none()) {
                // unit_obj is expected to be a Python str.
                PyObject* py_str = unit_obj.ptr();
                if (!PyUnicode_Check(py_str))
                    throw nb::type_error("unit must be a str or None");
                // Borrow UTF-8 bytes; valid while py_str is alive (unit_obj is on stack).
                unit_str = PyUnicode_AsUTF8(py_str);
                if (!unit_str) throw nb::python_error();
            }
            return int64_to_timestamp_apply(v, unit_str);
        },
        nb::arg("v"), nb::arg("unit") = nb::none(),
        "CAST(v AS TIMESTAMP[unit]): element-wise int64 → DRAKEN_TIMESTAMP64. "
        "unit: 's', 'ms', 'us' (default), 'ns', or 'days'. "
        "No data transformation for s/ms/us/ns; 'days' scales to microseconds. "
        "Null rows propagate as null.");

    // Priority 1 — numeric/temporal.
    m.def("vector_cast_int64_to_float64",
        [](nb::object v) -> nb::object { return int64_to_float64_apply(v); },
        nb::arg("v"),
        "CAST(v AS FLOAT64): element-wise INT64 widening to FLOAT64. Null rows propagate.");

    m.def("vector_cast_bool_to_int64",
        [](nb::object v) -> nb::object { return bool_to_int64_apply(v); },
        nb::arg("v"),
        "CAST(v AS INT64): expand bit-packed BOOL to INT64 (0 or 1). Null rows propagate.");

    m.def("vector_cast_int64_to_bool",
        [](nb::object v) -> nb::object { return int64_to_bool_apply(v); },
        nb::arg("v"),
        "CAST(v AS BOOL): 0→false, non-zero→true. Output is bit-packed DRAKEN_BOOL. Null rows propagate.");

    m.def("vector_cast_float64_to_bool",
        [](nb::object v) -> nb::object { return float64_to_bool_apply(v); },
        nb::arg("v"),
        "CAST(v AS BOOL): 0.0→false, non-zero→true. Accepts FLOAT32/FLOAT64. Output is bit-packed DRAKEN_BOOL. Null rows propagate.");

    m.def("vector_cast_date32_to_int64",
        [](nb::object v) -> nb::object { return date32_to_int64_apply(v); },
        nb::arg("v"),
        "CAST(v AS INT64): DATE32 days-since-epoch widened to INT64. Null rows propagate.");

    m.def("vector_cast_timestamp_to_int64",
        [](nb::object v) -> nb::object { return timestamp_to_int64_apply(v); },
        nb::arg("v"),
        "CAST(v AS INT64): TIMESTAMP64 microseconds-since-epoch retyped to INT64. Null rows propagate.");

    // Priority 2 — string formatting.
    m.def("vector_cast_bool_to_string",
        [](nb::object v) -> nb::object { return bool_to_string_apply(v); },
        nb::arg("v"),
        "CAST(v AS VARCHAR): BOOL → 'true'/'false'. Null rows propagate.");

    m.def("vector_cast_bool_to_float64",
        [](nb::object v) -> nb::object { return bool_to_float64_apply(v); },
        nb::arg("v"),
        "CAST(v AS FLOAT64): BOOL → 0.0/1.0. Null rows propagate.");

    m.def("vector_cast_date_to_string",
        [](nb::object v) -> nb::object { return date_to_string_apply(v); },
        nb::arg("v"),
        "CAST(v AS VARCHAR): DATE32 → 'YYYY-MM-DD' (10 chars, inline). Null rows propagate.");

    m.def("vector_cast_timestamp_to_string",
        [](nb::object v) -> nb::object { return timestamp_to_string_apply(v); },
        nb::arg("v"),
        "CAST(v AS VARCHAR): TIMESTAMP64 → 'YYYY-MM-DD HH:MM:SS.ffffff+0000'. Null rows propagate.");

    // Priority 3 — narrow integer widening and float↔int.
    m.def("vector_cast_integer_to_float64",
        [](nb::object v) -> nb::object { return integer_to_float64_apply(v); },
        nb::arg("v"),
        "CAST(v AS FLOAT64): INT32/INT16/INT8 lossless widening to FLOAT64. Null rows propagate.");

    m.def("vector_cast_integer_to_int64",
        [](nb::object v) -> nb::object { return integer_to_int64_apply(v); },
        nb::arg("v"),
        "CAST(v AS INT64): INT32/INT16/INT8 sign-extending widening to INT64. Null rows propagate.");

    m.def("vector_cast_float64_to_int64",
        [](nb::object v) -> nb::object { return float64_to_int64_apply(v); },
        nb::arg("v"),
        "CAST(v AS INT64): FLOAT64/FLOAT32 truncated toward zero to INT64. Null rows propagate.");

    m.def("vector_cast_string_to_bool",
        [](nb::object v) -> nb::object { return string_to_bool_apply(v); },
        nb::arg("v"),
        "CAST(v AS BOOL): 'true'/'false'/'1'/'0'/'yes'/'no'/'on'/'off' (case-insensitive). "
        "Raises ValueError on unrecognized values. Null rows propagate.");
}
