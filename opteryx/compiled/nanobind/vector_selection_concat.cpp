// opteryx/compiled/nanobind/vector_selection_concat.cpp — Milestone E.11, Phase 10, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, 3 functions:
//
//   vector_coalesce(*args)          — N-ary row selection: first non-null across N vectors.
//   vector_iif(mask, true_v, false_v) — ternary: bool mask selects between two value vectors.
//   vector_concat(*args)            — bytewise string concatenation across N string vectors.
//
// vector_coalesce:
//   Inputs: ≥2 Vectors of the same Draken type family (bool/string/fixed-width).
//   Output: same type. Per row: first non-null input value. All-null rows → null.
//   Cross-family type mismatch → TypeError.
//   String output type: VARCHAR unless any input is NVARCHAR (→ NVARCHAR) or VARBINARY (→ VARBINARY).
//
// vector_iif:
//   mask must be DRAKEN_BOOL. true_v and false_v must be same type family.
//   Per row: null/false mask → take false_v row; true mask → take true_v row.
//   Null in mask → treated as FALSE (SQL CASE WHEN NULL THEN x ELSE y = y).
//   Null in selected branch → null in output.
//
// vector_concat:
//   Inputs: ≥2 string-family Vectors (VARCHAR/NVARCHAR/VARBINARY/DICTIONARY/CONSTANT).
//   Output: bytewise concatenation per row.
//   Type promotion: VARCHAR + NVARCHAR → NVARCHAR; any VARBINARY → VARBINARY.
//   Null TVL: any null input at a row → null output row.
//   Length mismatch across inputs → ValueError.
//
// Fails loud on:
//   - Non-Vector arg → TypeError.
//   - Type mismatch within coalesce/iif branches → TypeError.
//   - coalesce/iif: bool-family mixed with other → TypeError.
//   - concat: non-string-family input → TypeError.
//   - Length mismatch → ValueError.
//   - Fewer than 2 inputs to coalesce/concat → ValueError.
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_coalesce.pyx
//   opteryx/compiled/vector_ops/vector_iif.pyx
//   opteryx/compiled/vector_ops/vector_concat.pyx  (vector_string_concat_binary only;
//     vector_concat_array / vector_concat_ws_array had no callers and are deleted)

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared type helpers
// ---------------------------------------------------------------------------

static inline bool is_bool_type(DrakenType t)   { return t == DRAKEN_BOOL; }

static inline bool is_string_type(DrakenType t) {
    return t == DRAKEN_VARCHAR  || t == DRAKEN_NVARCHAR ||
           t == DRAKEN_VARBINARY;
}

static inline bool is_fixed_type(DrakenType t) {
    return !is_bool_type(t) && !is_string_type(t) &&
           t != DRAKEN_ARRAY && t != DRAKEN_NON_NATIVE &&
           t != DRAKEN_NULL  && t != DRAKEN_VECTOR_FP16;
}

static size_t fixed_itemsize(DrakenType t) {
    switch (t) {
    case DRAKEN_INT8:        return 1u;
    case DRAKEN_INT16:       return 2u;
    case DRAKEN_INT32:       return 4u;
    case DRAKEN_INT64:       return 8u;
    case DRAKEN_DECIMAL:     return 8u;
    case DRAKEN_FLOAT32:     return 4u;
    case DRAKEN_FLOAT64:     return 8u;
    case DRAKEN_DATE32:      return 4u;
    case DRAKEN_TIMESTAMP64: return 8u;
    case DRAKEN_TIME32:      return 4u;
    case DRAKEN_TIME64:      return 8u;
    case DRAKEN_INTERVAL:    return 16u;
    default:                 return 0u;
    }
}

static inline bool is_int_type(DrakenType t) {
    return t == DRAKEN_INT8 || t == DRAKEN_INT16 ||
           t == DRAKEN_INT32 || t == DRAKEN_INT64;
}

static inline bool is_float_type(DrakenType t) {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// Promote two fixed-width types to a common output type for null-aware
// selection (iif/coalesce), mirroring the binder's find_compatible_type: any
// two integers widen to INT64; anything mixing a float widens to FLOAT64.
// Equal types pass through unchanged (no conversion). Returns DRAKEN_NULL when
// the pair cannot be promoted (e.g. mismatched DECIMAL/temporal) — the caller
// raises. DECIMAL/DATE/TIME/TIMESTAMP/INTERVAL only ever match same-type here;
// cross-family scaling is genuinely ambiguous and stays an error.
// Canonicalize a fixed-width result type to match the binder's declared type.
// The binder widens narrow ints (INT8/16/32) → INT64 via find_compatible_type
// for IIF/COALESCE/IFNULL results; the declared type drives C-native cast-kernel
// selection, so the produced vector must be INT64 too. DATE32/TIME32 (also 4-byte)
// and DECIMAL/temporal are distinct tags and pass through unchanged.
static inline DrakenType canon_fixed(DrakenType t) {
    if (t == DRAKEN_INT8 || t == DRAKEN_INT16 || t == DRAKEN_INT32) return DRAKEN_INT64;
    return t;
}

static inline DrakenType promote_fixed(DrakenType a, DrakenType b) {
    if (a == b) return canon_fixed(a);
    if (is_int_type(a) && is_int_type(b)) return DRAKEN_INT64;
    if ((is_int_type(a) || is_float_type(a)) &&
        (is_int_type(b) || is_float_type(b))) return DRAKEN_FLOAT64;
    return DRAKEN_NULL;  // incompatible
}

// Read a row's value from an integer-typed vector, sign-extending to int64.
// Only INT8/16/32/64 reach here (promote_fixed restricts the promoted path).
static inline int64_t read_int_row(const DrakenVector* dv, uint32_t row) {
    const uint8_t* p = static_cast<const uint8_t*>(dv->data)
        + static_cast<size_t>(dv->selection[row]) * fixed_itemsize(dv->type);
    switch (dv->type) {
    case DRAKEN_INT8:  return *reinterpret_cast<const int8_t*>(p);
    case DRAKEN_INT16: return *reinterpret_cast<const int16_t*>(p);
    case DRAKEN_INT32: return *reinterpret_cast<const int32_t*>(p);
    case DRAKEN_INT64: return *reinterpret_cast<const int64_t*>(p);
    default:           return 0;  // unreachable
    }
}

// Read a row's value as double, converting from FLOAT32/FLOAT64 or any integer.
static inline double read_double_row(const DrakenVector* dv, uint32_t row) {
    if (dv->type == DRAKEN_FLOAT32) {
        const uint8_t* p = static_cast<const uint8_t*>(dv->data)
            + static_cast<size_t>(dv->selection[row]) * 4u;
        return static_cast<double>(*reinterpret_cast<const float*>(p));
    }
    if (dv->type == DRAKEN_FLOAT64) {
        const uint8_t* p = static_cast<const uint8_t*>(dv->data)
            + static_cast<size_t>(dv->selection[row]) * 8u;
        return *reinterpret_cast<const double*>(p);
    }
    return static_cast<double>(read_int_row(dv, row));
}

// Write one source row into the output buffer at `out_type`, converting when
// the source's physical type differs (the promoted path). Same-type rows take
// a raw memcpy — zero overhead for the common uniform-type case.
static inline void write_fixed_row(uint8_t* out_data, uint32_t row,
                                    DrakenType out_type, const DrakenVector* src) {
    const size_t osz = fixed_itemsize(out_type);
    uint8_t* dst = out_data + static_cast<size_t>(row) * osz;
    if (src->type == out_type) {
        std::memcpy(dst,
            static_cast<const uint8_t*>(src->data)
                + static_cast<size_t>(src->selection[row]) * osz,
            osz);
        return;
    }
    if (out_type == DRAKEN_INT64) {
        const int64_t v = read_int_row(src, row);
        std::memcpy(dst, &v, sizeof(int64_t));
    } else {  // DRAKEN_FLOAT64
        const double v = read_double_row(src, row);
        std::memcpy(dst, &v, sizeof(double));
    }
}

// Canonical string type for promotion: DICTIONARY/CONSTANT → VARCHAR.
static inline DrakenType canon_string_type(DrakenType t) {
    if (t == DRAKEN_NVARCHAR)              return DRAKEN_NVARCHAR;
    if (t == DRAKEN_VARBINARY)             return DRAKEN_VARBINARY;
    return DRAKEN_VARCHAR;  // VARCHAR / DICTIONARY / CONSTANT
}

// Promote two canonical string types. VARBINARY beats NVARCHAR beats VARCHAR.
static inline DrakenType promote_string(DrakenType a, DrakenType b) {
    DrakenType ca = canon_string_type(a), cb = canon_string_type(b);
    if (ca == DRAKEN_VARBINARY || cb == DRAKEN_VARBINARY) return DRAKEN_VARBINARY;
    if (ca == DRAKEN_NVARCHAR  || cb == DRAKEN_NVARCHAR)  return DRAKEN_NVARCHAR;
    return DRAKEN_VARCHAR;
}

// ---------------------------------------------------------------------------
// Row-level access helpers (uniform access pattern: data[selection[i]])
// ---------------------------------------------------------------------------

static inline bool row_is_valid(const DrakenVector* dv, uint32_t row) {
    if (!dv->validity) return true;
    return ((dv->validity[row >> 3] >> (row & 7u)) & 1u) != 0u;
}

static inline void set_bit(uint8_t* bits, uint32_t i) {
    bits[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
}

static inline bool read_bool_row(const DrakenVector* dv, uint32_t row) {
    uint32_t code = dv->selection[row];
    return ((static_cast<const uint8_t*>(dv->data)[code >> 3u] >> (code & 7u)) & 1u) != 0u;
}

struct StrView { const uint8_t* data; uint32_t len; };

static inline StrView read_string_row(const DrakenVector* dv, uint32_t row) {
    const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(dv->data);
    const DrakenStringSlot*  slot = &sa->slots[dv->selection[row]];
    return { str_data(slot, sa->arena), str_length(slot) };
}

// Allocate a zero-filled validity bitmap (bit=1 valid). Returns nullptr when n==0.
static uint8_t* alloc_validity(uint32_t n) {
    if (n == 0u) return nullptr;
    const uint32_t bm     = (n + 7u) >> 3u;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(sz));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0, sz);  // 0 = null; set bits as valid rows are found
    return v;
}

// Unwrap a Python object to DrakenVector*. Raises TypeError on failure.
static const DrakenVector* unwrap(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set by unwrap
    return dv;
}

// Wrap hand-allocated buffers in Python Vector.
static nb::object own_raw(void* data, uint8_t* validity, uint32_t length, DrakenType type) {
    PyObject* out = draken_vector_own_raw(data, validity, length, type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static nb::object own_string(DrakenStringSlot* slots, uint8_t* arena, size_t arena_len,
                              uint8_t* validity, uint32_t length, DrakenType type) {
    PyObject* out = draken_vector_own_string(slots, arena, arena_len, validity, length, type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Logical-type descriptor propagation for fixed-width results.
//
// DECIMAL (scale/precision) and TIMESTAMP/TIME (unit) carry a logical descriptor
// on the Python-side VectorOwner, NOT on the DrakenVector the kernel sees. The
// generic own_raw wrap drops it, so a selection result (iif/coalesce) of those
// types renders as "missing its logical-type descriptor". We copy the descriptor
// from the input branch that supplies the value and wrap via the descriptor-aware
// own_* variants. (Mixed-scale DECIMAL branches are not rescaled — the descriptor
// is taken from the first branch; selection over differing scales is a separate
// concern. The NULLIF lowering and same-descriptor selections are exact.)
// kind: 0 none, 1 TIMESTAMP64, 2 TIME32/TIME64, 3 DECIMAL.
struct LogicalDesc {
    int     kind      = 0;
    char    unit[8]   = {0};   // "s" / "ms" / "us" / "ns"
    uint8_t precision = 0;
    uint8_t scale     = 0;
};

// Read the descriptor an output of out_type requires from a source Vector object.
// Returns kind=0 for types without a logical descriptor, or when the source
// unexpectedly lacks one (caller then falls back to a raw wrap, preserving the
// pre-existing behaviour for an already-malformed input).
static LogicalDesc read_logical_desc(nb::handle obj, DrakenType out_type) {
    LogicalDesc d;
    if (out_type == DRAKEN_TIMESTAMP64 || out_type == DRAKEN_TIME32 ||
        out_type == DRAKEN_TIME64) {
        PyObject* raw = PyObject_GetAttrString(obj.ptr(), "logical_type_unit");
        if (!raw) { PyErr_Clear(); return d; }
        nb::object u = nb::steal<nb::object>(raw);
        if (u.is_none()) return d;
        const char* s = PyUnicode_AsUTF8(u.ptr());
        if (!s) { PyErr_Clear(); return d; }
        std::strncpy(d.unit, s, sizeof(d.unit) - 1u);
        d.kind = (out_type == DRAKEN_TIMESTAMP64) ? 1 : 2;
        return d;
    }
    if (out_type == DRAKEN_DECIMAL) {
        PyObject* rp = PyObject_GetAttrString(obj.ptr(), "logical_type_precision");
        if (!rp) { PyErr_Clear(); return d; }
        nb::object p = nb::steal<nb::object>(rp);
        PyObject* rs = PyObject_GetAttrString(obj.ptr(), "logical_type_scale");
        if (!rs) { PyErr_Clear(); return d; }
        nb::object sc = nb::steal<nb::object>(rs);
        if (p.is_none() || sc.is_none()) return d;
        d.precision = static_cast<uint8_t>(nb::cast<int>(p));
        d.scale     = static_cast<uint8_t>(nb::cast<int>(sc));
        d.kind = 3;
    }
    return d;
}

// Wrap fixed-width result buffers, attaching the logical descriptor when present.
static nb::object own_fixed(void* data, uint8_t* validity, uint32_t n,
                            DrakenType type, const LogicalDesc& d) {
    PyObject* out;
    switch (d.kind) {
        case 1:  out = draken_vector_own_timestamp(data, validity, n, d.unit); break;
        case 2:  out = (type == DRAKEN_TIME32)
                           ? draken_vector_own_time32(data, validity, n, d.unit)
                           : draken_vector_own_time64(data, validity, n, d.unit); break;
        case 3:  out = draken_vector_own_decimal(data, validity, n, d.precision, d.scale); break;
        default: out = draken_vector_own_raw(data, validity, n, type); break;
    }
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// RAII guard for in-flight string output buffers
// ---------------------------------------------------------------------------

struct StrOutGuard {
    DrakenStringSlot* slots    = nullptr;
    uint8_t*          arena    = nullptr;
    uint8_t*          validity = nullptr;
    ~StrOutGuard() {
        if (slots)    draken_free(slots);
        if (arena)    draken_free(arena);
        if (validity) draken_free(validity);
    }
    void release() { slots = nullptr; arena = nullptr; validity = nullptr; }
};

// ---------------------------------------------------------------------------
// Bool coalesce / iif kernels
// ---------------------------------------------------------------------------

static nb::object coalesce_bool(const std::vector<const DrakenVector*>& vecs, uint32_t n) {
    const uint32_t padded = ((((n + 7u) >> 3u) + 7u) & ~7u);
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;

    uint8_t* out_bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!out_bits) throw std::bad_alloc();
    std::memset(out_bits, 0, sz);

    uint8_t* out_valid = alloc_validity(n);

    struct BG { uint8_t* d; uint8_t* v;
                ~BG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_bits, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            if (read_bool_row(dv, row)) set_bit(out_bits, row);
            if (out_valid)              set_bit(out_valid, row);
            found = true;
            break;
        }
        if (!found) any_null = true;
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_bits, out_valid, n, DRAKEN_BOOL);
}

static nb::object iif_bool(const DrakenVector* cond, const DrakenVector* true_v,
                            const DrakenVector* false_v, uint32_t n) {
    const uint32_t padded = ((((n + 7u) >> 3u) + 7u) & ~7u);
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;

    uint8_t* out_bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!out_bits) throw std::bad_alloc();
    std::memset(out_bits, 0, sz);

    uint8_t* out_valid = alloc_validity(n);

    struct BG { uint8_t* d; uint8_t* v;
                ~BG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_bits, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        // Null or false mask → choose false_v (SQL: CASE WHEN NULL = ELSE branch).
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) { any_null = true; continue; }
        if (read_bool_row(src, row)) set_bit(out_bits, row);
        if (out_valid)               set_bit(out_valid, row);
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_raw(out_bits, out_valid, n, DRAKEN_BOOL);
}

// ---------------------------------------------------------------------------
// Fixed-width coalesce / iif kernels
// ---------------------------------------------------------------------------

static nb::object coalesce_fixed(const std::vector<const DrakenVector*>& vecs,
                                  uint32_t n, DrakenType type, const LogicalDesc& desc) {
    const size_t isz    = fixed_itemsize(type);
    const size_t alloc  = n > 0u ? static_cast<size_t>(n) * isz : isz;
    uint8_t* out_data   = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc);

    uint8_t* out_valid = alloc_validity(n);

    struct FG { uint8_t* d; uint8_t* v;
                ~FG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            write_fixed_row(out_data, row, type, dv);
            if (out_valid) set_bit(out_valid, row);
            found = true;
            break;
        }
        if (!found) any_null = true;
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_fixed(out_data, out_valid, n, type, desc);
}

static nb::object iif_fixed(const DrakenVector* cond, const DrakenVector* true_v,
                              const DrakenVector* false_v, uint32_t n, DrakenType type,
                              const LogicalDesc& desc) {
    const size_t isz   = fixed_itemsize(type);
    const size_t alloc = n > 0u ? static_cast<size_t>(n) * isz : isz;
    uint8_t* out_data  = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc);

    uint8_t* out_valid = alloc_validity(n);

    struct FG { uint8_t* d; uint8_t* v;
                ~FG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) { any_null = true; continue; }
        write_fixed_row(out_data, row, type, src);
        if (out_valid) set_bit(out_valid, row);
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }

    g.d = nullptr; g.v = nullptr;
    return own_fixed(out_data, out_valid, n, type, desc);
}

// Power-of-ten for DECIMAL rescale; scale difference is bounded to [0, 18].
static inline int64_t pow10_i64(int e) {
    static const int64_t P[] = {
        1LL, 10LL, 100LL, 1000LL, 10000LL, 100000LL, 1000000LL, 10000000LL,
        100000000LL, 1000000000LL, 10000000000LL, 100000000000LL, 1000000000000LL,
        10000000000000LL, 100000000000000LL, 1000000000000000LL,
        10000000000000000LL, 100000000000000000LL, 1000000000000000000LL };
    return (e >= 0 && e <= 18) ? P[e] : 1LL;
}

// DECIMAL iif: int64 unscaled values. The two branches may carry different scales,
// so rescale each selected value up to out_scale (= max of the two) — matching the
// binder's declared scale — before storing. (DRAKEN_DECIMAL only; 8-byte int64.)
static nb::object iif_decimal(const DrakenVector* cond, const DrakenVector* true_v,
                              const DrakenVector* false_v, uint32_t n,
                              uint8_t out_prec, uint8_t out_scale,
                              uint8_t true_scale, uint8_t false_scale) {
    const int64_t mul_t = pow10_i64(static_cast<int>(out_scale) - static_cast<int>(true_scale));
    const int64_t mul_f = pow10_i64(static_cast<int>(out_scale) - static_cast<int>(false_scale));
    const size_t alloc = (n > 0u ? static_cast<size_t>(n) : 1u) * sizeof(int64_t);
    int64_t* out_data = static_cast<int64_t*>(draken_malloc(alloc));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc);
    uint8_t* out_valid = alloc_validity(n);
    struct DG { int64_t* d; uint8_t* v;
                ~DG() { if (d) draken_free(d); if (v) draken_free(v); } } g{out_data, out_valid};

    bool any_null = false;
    for (uint32_t row = 0u; row < n; ++row) {
        const bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) { any_null = true; continue; }
        const int64_t v = static_cast<const int64_t*>(src->data)[src->selection[row]];
        out_data[row] = v * (choose_true ? mul_t : mul_f);
        if (out_valid) set_bit(out_valid, row);
    }
    if (!any_null && out_valid) { draken_free(out_valid); out_valid = nullptr; g.v = nullptr; }
    g.d = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_decimal(out_data, out_valid, n, out_prec, out_scale);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// String coalesce / iif kernels (2-pass: budget → fill)
// ---------------------------------------------------------------------------

static nb::object coalesce_string(const std::vector<const DrakenVector*>& vecs,
                                   uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            StrView sv = read_string_row(dv, row);
            if (sv.len > STR_INLINE_MAX) total_bytes += sv.len;
            break;  // first non-null only
        }
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots    = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    bool     any_null  = false;
    size_t   arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        bool found = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) continue;
            StrView sv = read_string_row(dv, row);
            if (sv.len > STR_INLINE_MAX) {
                const uint32_t off = static_cast<uint32_t>(arena_pos);
                std::memcpy(g.arena + off, sv.data, sv.len);
                draken_build_string_slot(&g.slots[row], sv.data, sv.len, off);
                arena_pos += sv.len;
            } else {
                draken_build_string_slot(&g.slots[row], sv.data, sv.len, 0u);
            }
            if (g.validity) set_bit(g.validity, row);
            found = true;
            break;
        }
        if (!found) {
            str_init_null(&g.slots[row]);
            any_null = true;
        }
    }
    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

static nb::object iif_string(const DrakenVector* cond, const DrakenVector* true_v,
                               const DrakenVector* false_v, uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) continue;
        StrView sv = read_string_row(src, row);
        if (sv.len > STR_INLINE_MAX) total_bytes += sv.len;
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    bool   any_null  = false;
    size_t arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        bool choose_true = row_is_valid(cond, row) && read_bool_row(cond, row);
        const DrakenVector* src = choose_true ? true_v : false_v;
        if (!row_is_valid(src, row)) {
            str_init_null(&g.slots[row]);
            any_null = true;
            continue;
        }
        StrView sv = read_string_row(src, row);
        if (sv.len > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_pos);
            std::memcpy(g.arena + off, sv.data, sv.len);
            draken_build_string_slot(&g.slots[row], sv.data, sv.len, off);
            arena_pos += sv.len;
        } else {
            draken_build_string_slot(&g.slots[row], sv.data, sv.len, 0u);
        }
        if (g.validity) set_bit(g.validity, row);
    }
    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

// ---------------------------------------------------------------------------
// String concat kernel (N-ary bytewise, 2-pass)
// ---------------------------------------------------------------------------

static nb::object impl_concat(const std::vector<const DrakenVector*>& vecs,
                               uint32_t n, DrakenType out_type) {
    // Pass 1: total extern arena budget per row (sum of non-null row widths).
    //
    // The arena holds a row's bytes iff the *concatenated* length exceeds the
    // inline limit — that is the exact decision Pass 2 makes below. The budget
    // must use the same predicate on the same quantity (the per-row sum), not
    // the per-operand length: two individually-inline operands can concatenate
    // past STR_INLINE_MAX (e.g. 7 + 7 = 14), which still needs arena bytes. A
    // per-operand test under-budgets those rows and Pass 2 overruns the arena.
    size_t total_bytes = 0u;
    for (uint32_t row = 0u; row < n; ++row) {
        // If any input is null this row → null output; skip arena budget.
        bool row_null = false;
        size_t row_len = 0u;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) { row_null = true; break; }
            StrView sv = read_string_row(dv, row);
            row_len += sv.len;
        }
        if (!row_null && row_len > STR_INLINE_MAX) total_bytes += row_len;
    }

    const size_t slots_sz  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_sz  = total_bytes > 0u ? total_bytes : 1u;

    StrOutGuard g;
    g.slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!g.slots) throw std::bad_alloc();
    std::memset(g.slots, 0, slots_sz);

    g.arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!g.arena) throw std::bad_alloc();

    g.validity = alloc_validity(n);

    // Temp scratch buffer for assembling inline concats (or fitting small concats).
    // We grow it on demand; max inline is STR_INLINE_MAX so start at 64.
    size_t   scratch_cap = 64u;
    uint8_t* scratch     = static_cast<uint8_t*>(draken_malloc(scratch_cap));
    if (!scratch) throw std::bad_alloc();

    bool   any_null  = false;
    size_t arena_pos = 0u;

    // Pass 2: fill slots.
    for (uint32_t row = 0u; row < n; ++row) {
        // Check for null inputs first.
        bool row_null = false;
        for (const DrakenVector* dv : vecs) {
            if (!row_is_valid(dv, row)) { row_null = true; break; }
        }
        if (row_null) {
            str_init_null(&g.slots[row]);
            any_null = true;
            continue;
        }

        // Compute total byte length for this row.
        uint32_t row_len = 0u;
        for (const DrakenVector* dv : vecs) {
            StrView sv = read_string_row(dv, row);
            row_len += sv.len;
        }

        if (row_len == 0u) {
            draken_build_string_slot(&g.slots[row], nullptr, 0u, 0u);
            if (g.validity) set_bit(g.validity, row);
            continue;
        }

        // Ensure scratch is large enough.
        if (static_cast<size_t>(row_len) > scratch_cap) {
            draken_free(scratch);
            scratch_cap = static_cast<size_t>(row_len) + 64u;
            scratch = static_cast<uint8_t*>(draken_malloc(scratch_cap));
            if (!scratch) {
                draken_free(scratch); scratch = nullptr;
                throw std::bad_alloc();
            }
        }

        // Assemble concatenated bytes in scratch.
        uint32_t pos = 0u;
        for (const DrakenVector* dv : vecs) {
            StrView sv = read_string_row(dv, row);
            if (sv.len > 0u) {
                std::memcpy(scratch + pos, sv.data, sv.len);
                pos += sv.len;
            }
        }

        if (row_len > STR_INLINE_MAX) {
            const uint32_t off = static_cast<uint32_t>(arena_pos);
            std::memcpy(g.arena + off, scratch, row_len);
            draken_build_string_slot(&g.slots[row], scratch, row_len, off);
            arena_pos += row_len;
        } else {
            draken_build_string_slot(&g.slots[row], scratch, row_len, 0u);
        }
        if (g.validity) set_bit(g.validity, row);
    }
    draken_free(scratch);

    if (!any_null && g.validity) { draken_free(g.validity); g.validity = nullptr; }

    DrakenStringSlot* slots = g.slots; uint8_t* arena = g.arena; uint8_t* validity = g.validity;
    g.release();
    return own_string(slots, arena, arena_pos, validity, n, out_type);
}

// ---------------------------------------------------------------------------
// Public entry points (validation + dispatch)
// ---------------------------------------------------------------------------

static nb::object impl_coalesce(nb::args args) {
    const Py_ssize_t argc = static_cast<Py_ssize_t>(args.size());
    if (argc < 2)
        throw nb::value_error("vector_coalesce: requires at least 2 arguments");

    std::vector<const DrakenVector*> vecs;
    vecs.reserve(static_cast<size_t>(argc));
    for (Py_ssize_t i = 0; i < argc; ++i)
        vecs.push_back(unwrap(args[i], "vector_coalesce"));

    const uint32_t n = vecs[0]->length;
    for (Py_ssize_t i = 1; i < argc; ++i) {
        if (vecs[i]->length != n) {
            PyErr_Format(PyExc_ValueError,
                "vector_coalesce: argument %zd length %u does not match argument 0 length %u",
                i, vecs[i]->length, n);
            throw nb::python_error();
        }
    }

    const DrakenType t0 = vecs[0]->type;

    if (is_bool_type(t0)) {
        for (Py_ssize_t i = 1; i < argc; ++i) {
            if (!is_bool_type(vecs[i]->type)) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d is not BOOL (expected BOOL)",
                    i, static_cast<int>(vecs[i]->type));
                throw nb::python_error();
            }
        }
        return coalesce_bool(vecs, n);
    }

    if (is_string_type(t0)) {
        DrakenType out_type = canon_string_type(t0);
        for (Py_ssize_t i = 1; i < argc; ++i) {
            if (!is_string_type(vecs[i]->type)) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d is not string-family (expected string-family)",
                    i, static_cast<int>(vecs[i]->type));
                throw nb::python_error();
            }
            out_type = promote_string(out_type, vecs[i]->type);
        }
        return coalesce_string(vecs, n, out_type);
    }

    if (is_fixed_type(t0)) {
        if (fixed_itemsize(t0) == 0u) {
            PyErr_Format(PyExc_TypeError,
                "vector_coalesce: unsupported fixed-width type %d",
                static_cast<int>(t0));
            throw nb::python_error();
        }
        // Promote across branches: narrow ints widen to INT64, int/float mixes
        // to FLOAT64 (mirrors the binder's declared result type). Equal types
        // pass through unchanged. The promoted output is always 8 bytes.
        DrakenType out_type = t0;
        for (Py_ssize_t i = 1; i < argc; ++i) {
            const DrakenType ti = vecs[i]->type;
            DrakenType promoted = is_fixed_type(ti) && fixed_itemsize(ti) != 0u
                                      ? promote_fixed(out_type, ti)
                                      : DRAKEN_NULL;
            if (promoted == DRAKEN_NULL) {
                PyErr_Format(PyExc_TypeError,
                    "vector_coalesce: argument %zd type %d cannot be promoted with argument 0 type %d",
                    i, static_cast<int>(ti), static_cast<int>(t0));
                throw nb::python_error();
            }
            out_type = promoted;
        }
        return coalesce_fixed(vecs, n, out_type, read_logical_desc(args[0], out_type));
    }

    PyErr_Format(PyExc_TypeError,
        "vector_coalesce: unsupported type %d on argument 0",
        static_cast<int>(t0));
    throw nb::python_error();
}

static nb::object impl_iif(nb::object mask_obj, nb::object true_obj, nb::object false_obj) {
    const DrakenVector* cond    = unwrap(mask_obj,  "vector_iif");
    const DrakenVector* true_v  = unwrap(true_obj,  "vector_iif");
    const DrakenVector* false_v = unwrap(false_obj, "vector_iif");

    if (!is_bool_type(cond->type))
        throw nb::type_error("vector_iif: mask must be a BOOL Vector");

    const uint32_t n = cond->length;
    if (true_v->length != n || false_v->length != n) {
        PyErr_Format(PyExc_ValueError,
            "vector_iif: length mismatch: mask %u, true_v %u, false_v %u",
            n, true_v->length, false_v->length);
        throw nb::python_error();
    }

    const DrakenType tt = true_v->type, ft = false_v->type;

    // NULL-branch handling: a literal NULL (DRAKEN_NULL ⟹ every row null) on one
    // side adopts the OTHER branch's type and yields NULL wherever it is selected.
    // This is what SQL `IIF(c, NULL, x)` / `IIF(c, x, NULL)` mean, and the lowering
    // target for NULLIF (→ IIF(a = b, NULL, a)). We model the NULL side as an
    // all-invalid vector of the result type and reuse the existing same-family
    // kernels — they never read a row they deem null, so its data is untouched.
    if (tt == DRAKEN_NULL || ft == DRAKEN_NULL) {
        if (tt == DRAKEN_NULL && ft == DRAKEN_NULL)
            throw nb::type_error("vector_iif: both branches are NULL");

        const DrakenVector* present = (tt == DRAKEN_NULL) ? false_v : true_v;
        const DrakenType    pt      = present->type;

        DrakenType out_type;
        if (is_bool_type(pt))                                   out_type = DRAKEN_BOOL;
        else if (is_string_type(pt))                            out_type = canon_string_type(pt);
        else if (is_fixed_type(pt) && fixed_itemsize(pt) != 0u) out_type = canon_fixed(pt);
        else {
            PyErr_Format(PyExc_TypeError,
                "vector_iif: NULL branch paired with unsupported type %d",
                static_cast<int>(pt));
            throw nb::python_error();
        }

        // Synthetic all-invalid branch of out_type: validity all-zero ⟹ every row
        // null. data stays NULL (never dereferenced — the kernels guard on
        // row_is_valid); selection borrows the present branch's (length n, non-NULL)
        // purely to honour the "selection is never NULL" invariant.
        uint8_t* null_validity = alloc_validity(n);
        struct VG { uint8_t* v; ~VG() { if (v) draken_free(v); } } vg{null_validity};

        DrakenVector null_branch;
        std::memset(&null_branch, 0, sizeof(null_branch));
        null_branch.selection = present->selection;
        null_branch.length    = n;
        null_branch.validity  = null_validity;
        null_branch.type      = out_type;

        const DrakenVector* tv = (tt == DRAKEN_NULL) ? &null_branch : true_v;
        const DrakenVector* fv = (ft == DRAKEN_NULL) ? &null_branch : false_v;

        if (is_bool_type(out_type))   return iif_bool(cond, tv, fv, n);
        if (is_string_type(out_type)) return iif_string(cond, tv, fv, n, out_type);
        nb::object present_obj = (tt == DRAKEN_NULL) ? false_obj : true_obj;
        return iif_fixed(cond, tv, fv, n, out_type, read_logical_desc(present_obj, out_type));
    }

    if (is_bool_type(tt) && is_bool_type(ft))
        return iif_bool(cond, true_v, false_v, n);

    if (is_bool_type(tt) || is_bool_type(ft)) {
        PyErr_Format(PyExc_TypeError,
            "vector_iif: branch type mismatch: BOOL vs non-BOOL (true_v %d, false_v %d)",
            static_cast<int>(tt), static_cast<int>(ft));
        throw nb::python_error();
    }

    if (is_string_type(tt) && is_string_type(ft)) {
        DrakenType out_type = promote_string(tt, ft);
        return iif_string(cond, true_v, false_v, n, out_type);
    }

    if (is_string_type(tt) || is_string_type(ft)) {
        PyErr_Format(PyExc_TypeError,
            "vector_iif: branch type mismatch: string-family vs non-string (true_v %d, false_v %d)",
            static_cast<int>(tt), static_cast<int>(ft));
        throw nb::python_error();
    }

    if (is_fixed_type(tt) && is_fixed_type(ft)) {
        if (fixed_itemsize(tt) == 0u || fixed_itemsize(ft) == 0u) {
            PyErr_Format(PyExc_TypeError,
                "vector_iif: unsupported fixed-width type (true_v %d, false_v %d)",
                static_cast<int>(tt), static_cast<int>(ft));
            throw nb::python_error();
        }
        // Promote mismatched branches (narrow int → INT64, int/float → FLOAT64);
        // equal types pass through unchanged via the same-type fast path.
        const DrakenType out_type = promote_fixed(tt, ft);
        if (out_type == DRAKEN_NULL) {
            PyErr_Format(PyExc_TypeError,
                "vector_iif: fixed-width branch types cannot be promoted (true_v %d, false_v %d)",
                static_cast<int>(tt), static_cast<int>(ft));
            throw nb::python_error();
        }
        // DECIMAL: branches are int64 unscaled and may carry different scales.
        // Rescale to the common (max) scale so the result matches the binder's
        // declared scale (find_compatible_type widens to the larger scale).
        if (out_type == DRAKEN_DECIMAL) {
            const LogicalDesc dt = read_logical_desc(true_obj, DRAKEN_DECIMAL);
            const LogicalDesc df = read_logical_desc(false_obj, DRAKEN_DECIMAL);
            const uint8_t out_scale = dt.scale > df.scale ? dt.scale : df.scale;
            int p = dt.precision > df.precision ? dt.precision : df.precision;
            if (p < out_scale) p = out_scale;
            if (p > 18) p = 18;
            return iif_decimal(cond, true_v, false_v, n,
                               static_cast<uint8_t>(p), out_scale, dt.scale, df.scale);
        }
        return iif_fixed(cond, true_v, false_v, n, out_type,
                         read_logical_desc(true_obj, out_type));
    }

    PyErr_Format(PyExc_TypeError,
        "vector_iif: unsupported branch types (true_v %d, false_v %d)",
        static_cast<int>(tt), static_cast<int>(ft));
    throw nb::python_error();
}

// ---------------------------------------------------------------------------
// IFNULL / IFNOTNULL — null-aware selection, lowered onto vector_iif.
//
// IFNULL(value, default)    = IIF(value IS NULL, default, value)
// IFNOTNULL(value, result)  = IIF(value IS NULL, value,   result)
//
// The `value IS NULL` mask is built natively from `value`'s validity (no Python),
// then handed to impl_iif so all of its type/NULL-branch/descriptor/narrow-int
// handling is reused verbatim.
// ---------------------------------------------------------------------------

// Build isnull(v) as a fresh dense BOOL Vector (bit set = v is null at that row).
static nb::object isnull_bool_vector(const DrakenVector* v, uint32_t n) {
    const uint32_t padded = ((((n + 7u) >> 3u) + 7u) & ~7u);
    const size_t   sz     = padded ? static_cast<size_t>(padded) : 8u;
    uint8_t* bits = static_cast<uint8_t*>(draken_malloc(sz));
    if (!bits) throw std::bad_alloc();
    std::memset(bits, 0, sz);
    for (uint32_t row = 0u; row < n; ++row) {
        if (!row_is_valid(v, row)) set_bit(bits, row);
    }
    // validity NULL: the mask itself is never null. own_raw makes it dense.
    return own_raw(bits, nullptr, n, DRAKEN_BOOL);
}

static nb::object impl_ifnull(nb::object value_obj, nb::object default_obj) {
    const DrakenVector* v = unwrap(value_obj, "vector_ifnull");
    nb::object cond = isnull_bool_vector(v, v->length);
    return impl_iif(cond, default_obj, value_obj);
}

static nb::object impl_ifnotnull(nb::object value_obj, nb::object result_obj) {
    const DrakenVector* v = unwrap(value_obj, "vector_ifnotnull");
    nb::object cond = isnull_bool_vector(v, v->length);
    return impl_iif(cond, value_obj, result_obj);
}

static nb::object impl_vector_concat(nb::args args) {
    const Py_ssize_t argc = static_cast<Py_ssize_t>(args.size());
    if (argc < 2)
        throw nb::value_error("vector_concat: requires at least 2 arguments");

    std::vector<const DrakenVector*> vecs;
    vecs.reserve(static_cast<size_t>(argc));
    for (Py_ssize_t i = 0; i < argc; ++i) {
        const DrakenVector* dv = unwrap(args[i], "vector_concat");
        if (!is_string_type(dv->type)) {
            PyErr_Format(PyExc_TypeError,
                "vector_concat: argument %zd type %d is not string-family "
                "(expected VARCHAR/NVARCHAR/VARBINARY/DICTIONARY/CONSTANT)",
                i, static_cast<int>(dv->type));
            throw nb::python_error();
        }
        vecs.push_back(dv);
    }

    const uint32_t n = vecs[0]->length;
    for (Py_ssize_t i = 1; i < argc; ++i) {
        if (vecs[i]->length != n) {
            PyErr_Format(PyExc_ValueError,
                "vector_concat: argument %zd length %u does not match argument 0 length %u",
                i, vecs[i]->length, n);
            throw nb::python_error();
        }
    }

    // Compute output type via promotion.
    DrakenType out_type = canon_string_type(vecs[0]->type);
    for (Py_ssize_t i = 1; i < argc; ++i)
        out_type = promote_string(out_type, vecs[i]->type);

    return impl_concat(vecs, n, out_type);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_selection_concat(nb::module_ &m) {

    m.def("vector_coalesce",
        [](nb::args args) -> nb::object { return impl_coalesce(args); },
        "SQL COALESCE across ≥2 Vectors: first non-null per row. "
        "All inputs must be same type family (bool / string / fixed-width). "
        "Fixed-width: exact type match required. "
        "String: promotes VARCHAR+NVARCHAR→NVARCHAR, VARBINARY beats all. "
        "Fails loud on non-Vector, type mismatch, or length mismatch.");

    m.def("vector_iif",
        [](nb::object mask, nb::object tv, nb::object fv) -> nb::object {
            return impl_iif(mask, tv, fv);
        },
        nb::arg("mask"), nb::arg("when_true"), nb::arg("when_false"),
        "SQL IIF(mask, when_true, when_false). "
        "mask must be BOOL; branches must be same type family. "
        "Null or false mask → when_false (SQL CASE WHEN NULL = ELSE branch). "
        "Null in selected branch → null in output. "
        "Fails loud on non-Vector, type mismatch, or length mismatch.");

    m.def("vector_ifnull",
        [](nb::object values, nb::object default_) -> nb::object {
            return impl_ifnull(values, default_);
        },
        nb::arg("values"), nb::arg("default"),
        "SQL IFNULL(values, default) = IIF(values IS NULL, default, values). "
        "Native null-aware selection; reuses the vector_iif type machinery.");

    m.def("vector_ifnotnull",
        [](nb::object values, nb::object result) -> nb::object {
            return impl_ifnotnull(values, result);
        },
        nb::arg("values"), nb::arg("result"),
        "SQL IFNOTNULL(values, result) = IIF(values IS NULL, values, result). "
        "Native null-aware selection; reuses the vector_iif type machinery.");

    m.def("vector_concat",
        [](nb::args args) -> nb::object { return impl_vector_concat(args); },
        "Bytewise string concatenation across ≥2 string-family Vectors. "
        "Type promotion: VARCHAR+NVARCHAR→NVARCHAR, any VARBINARY→VARBINARY. "
        "Null TVL: any null input at a row → null output row. "
        "Fails loud on non-Vector, non-string-family input, or length mismatch.");
}
