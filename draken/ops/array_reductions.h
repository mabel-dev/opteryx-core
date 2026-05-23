#pragma once
// draken/ops/array_reductions.h — array element-reduction ops (Milestone E.5, Part A).
//
// 8 dispatching entry points for SQL ANY / ALL over array columns:
//   arr_any_eq, arr_all_eq, arr_any_ne, arr_all_ne,
//   arr_any_gt, arr_any_ge, arr_any_lt, arr_any_le
//
// Each entry point takes:
//   arr   — DRAKEN_ARRAY vector; vec.data = int32_t offsets[length+1].
//   child — flat child vector (DRAKEN_INT64 or DRAKEN_STRING; from draken_array_child_unwrap).
//   scalar — ArrScalar union holding the comparison value (caller builds from Python literal).
//
// Scalar building (consumer responsibility):
//   int64 child:  scalar.type = DRAKEN_INT64, scalar.i64 = nb::cast<int64_t>(lit)
//   string child: scalar.type = DRAKEN_STRING,
//                 str_init_inline / str_init_extern → scalar.str.slot,
//                 scalar.str.bytes = ptr to UTF-8 bytes (kept alive by caller)
//
// NULL SEMANTICS (ticket spec; differs from old .pyx allop semantics):
//   Null literal (scalar.i64 == 0 / str.slot == nullptr signalled by scalar.type==DRAKEN_NULL):
//     → all-False result, no validity bitmap.
//   Null ARRAY ROW → NULL in output (TVL); validity bit cleared for that row.
//     This applies to BOTH any and all ops.
//     NOTE: old allop .pyx emitted False (not NULL) for null rows and False for
//     empty rows.  This implementation uses correct SQL semantics per ticket spec.
//   Null CHILD ELEMENT (within a row):
//     any:  skip (non-matching) — as in old .pyx.
//     all:  treated as non-matching → all_match = false.
//   Empty row:
//     any → False (no match possible).
//     all → True  (vacuous truth — standard SQL; old .pyx emitted False).
//
// SUPPORTED CHILD TYPES: DRAKEN_INT64, DRAKEN_STRING.
//   Unsupported child type → std::invalid_argument thrown loud.
//
// ACCESS PATTERN: data[selection[i]] uniform — no shape discrimination (CLAUDE.md §11).
//
// RESULT: bit-packed DRAKEN_BOOL VecResult with validity bitmap when any row is null.

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ops/string_compare.h"  // str_eq_slots (§1 EXCEPTION — hash-only for long strings)
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Scalar carrier — consumer builds one before calling an entry point.
// ---------------------------------------------------------------------------
struct ArrScalar {
    DrakenType type;  // DRAKEN_INT64, DRAKEN_STRING, or DRAKEN_NULL (null literal)
    int64_t    i64;   // valid when type == DRAKEN_INT64
    struct {
        const DrakenStringSlot* slot;   // valid when type == DRAKEN_STRING
        const uint8_t*          bytes;  // UTF-8 bytes behind long slots (arena_offset==0)
    } str;
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static inline bool arr_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1u) != 0u;
}

static inline uint8_t* arr_alloc_bool_buf(uint32_t n) {
    const uint32_t raw    = (n + 7u) >> 3;
    const uint32_t padded = (raw + 7u) & ~7u;
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, bytes);
    return p;
}

// Allocate an all-valid bitmap for n rows and clear bits for null rows in arr.
static inline uint8_t* arr_build_validity(const DrakenVector& arr) {
    const uint32_t n      = arr.length;
    uint8_t*       val    = arr_alloc_bool_buf(n);  // zero-padded
    const uint32_t nb     = (n + 7u) >> 3;
    std::memset(val, 0xFFu, nb);
    // Mask tail bits.
    if (n & 7u) val[nb - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    // Clear bits for null rows.
    if (arr.validity != nullptr) {
        for (uint32_t i = 0; i < n; ++i) {
            if (!arr_row_valid(arr.validity, i))
                val[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7));
        }
    }
    return val;
}

static inline bool arr_has_null_rows(const DrakenVector& arr) noexcept {
    return arr.validity != nullptr;
}

// ---------------------------------------------------------------------------
// Int64 comparison tags — compile-time, no runtime dispatch in hot loop.
// ---------------------------------------------------------------------------
struct ArrI64Eq  { static bool cmp(int64_t a, int64_t b) noexcept { return a == b; } };
struct ArrI64Ne  { static bool cmp(int64_t a, int64_t b) noexcept { return a != b; } };
struct ArrI64Gt  { static bool cmp(int64_t a, int64_t b) noexcept { return a >  b; } };
struct ArrI64Ge  { static bool cmp(int64_t a, int64_t b) noexcept { return a >= b; } };
struct ArrI64Lt  { static bool cmp(int64_t a, int64_t b) noexcept { return a <  b; } };
struct ArrI64Le  { static bool cmp(int64_t a, int64_t b) noexcept { return a <= b; } };

// ---------------------------------------------------------------------------
// String comparison tags
// ---------------------------------------------------------------------------
struct ArrStrEq {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* /*arena_a*/,
                    const DrakenStringSlot* b, const uint8_t* /*arena_b*/) noexcept {
        return str_eq_slots(a, b) != 0;
    }
};
struct ArrStrNe {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* /*arena_a*/,
                    const DrakenStringSlot* b, const uint8_t* /*arena_b*/) noexcept {
        return str_eq_slots(a, b) == 0;
    }
};
struct ArrStrGt {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* arena_a,
                    const DrakenStringSlot* b, const uint8_t* arena_b) noexcept {
        return str_compare(a, arena_a, b, arena_b) > 0;
    }
};
struct ArrStrGe {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* arena_a,
                    const DrakenStringSlot* b, const uint8_t* arena_b) noexcept {
        return str_compare(a, arena_a, b, arena_b) >= 0;
    }
};
struct ArrStrLt {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* arena_a,
                    const DrakenStringSlot* b, const uint8_t* arena_b) noexcept {
        return str_compare(a, arena_a, b, arena_b) < 0;
    }
};
struct ArrStrLe {
    static bool cmp(const DrakenStringSlot* a, const uint8_t* arena_a,
                    const DrakenStringSlot* b, const uint8_t* arena_b) noexcept {
        return str_compare(a, arena_a, b, arena_b) <= 0;
    }
};

// ---------------------------------------------------------------------------
// Int64 inner kernels (any / all, templated on CmpOp)
// ---------------------------------------------------------------------------

template<typename CmpOp, bool reduce_all>
static VecResult arr_reduce_int64(const DrakenVector& arr,
                                   const DrakenVector& child,
                                   int64_t scalar)
{
    const uint32_t  n       = arr.length;
    const int32_t*  offsets = static_cast<const int32_t*>(arr.data);
    const int64_t*  cdata   = static_cast<const int64_t*>(child.data);
    const uint32_t* csel    = child.selection;
    const uint8_t*  cval    = child.validity;

    uint8_t* result   = arr_alloc_bool_buf(n);
    uint8_t* validity = arr_has_null_rows(arr) ? arr_build_validity(arr) : nullptr;

    for (uint32_t i = 0; i < n; ++i) {
        if (!arr_row_valid(arr.validity, i)) continue;  // null row → skip, validity bit already 0

        const uint32_t sel_i = arr.selection[i];
        const int32_t  start = offsets[sel_i];
        const int32_t  end   = offsets[sel_i + 1u];

        if (start == end) {
            // Empty row: any → False (stays 0); all → True (vacuous)
            if constexpr (reduce_all)
                result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            continue;
        }

        if constexpr (reduce_all) {
            bool all_ok = true;
            for (int32_t j = start; j < end; ++j) {
                if (!arr_row_valid(cval, static_cast<uint32_t>(j))) { all_ok = false; break; }
                if (!CmpOp::cmp(scalar, cdata[csel[j]])) { all_ok = false; break; }
            }
            if (all_ok) result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        } else {
            for (int32_t j = start; j < end; ++j) {
                if (!arr_row_valid(cval, static_cast<uint32_t>(j))) continue;
                if (CmpOp::cmp(scalar, cdata[csel[j]])) {
                    result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                    break;
                }
            }
        }
    }

    VecResult res;
    res.data          = result;
    res.validity      = validity;
    res.length        = n;
    res.type          = DRAKEN_BOOL;
    res.selection     = draken_identity_sel(n);
    res.data_length   = n;
    res.owns_selection = false;
    res.flags         = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return res;
}

// ---------------------------------------------------------------------------
// String inner kernels (any / all, templated on CmpOp)
// ---------------------------------------------------------------------------

template<typename CmpOp, bool reduce_all>
static VecResult arr_reduce_string(const DrakenVector& arr,
                                    const DrakenVector& child,
                                    const DrakenStringSlot* scalar_slot,
                                    const uint8_t*          scalar_bytes)
{
    const uint32_t       n      = arr.length;
    const int32_t*       offsets = static_cast<const int32_t*>(arr.data);
    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(child.data);
    const uint32_t*      csel   = child.selection;
    const uint8_t*       cval   = child.validity;

    uint8_t* result   = arr_alloc_bool_buf(n);
    uint8_t* validity = arr_has_null_rows(arr) ? arr_build_validity(arr) : nullptr;

    for (uint32_t i = 0; i < n; ++i) {
        if (!arr_row_valid(arr.validity, i)) continue;

        const uint32_t sel_i = arr.selection[i];
        const int32_t  start = offsets[sel_i];
        const int32_t  end   = offsets[sel_i + 1u];

        if (start == end) {
            if constexpr (reduce_all)
                result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            continue;
        }

        if constexpr (reduce_all) {
            bool all_ok = true;
            for (int32_t j = start; j < end; ++j) {
                if (!arr_row_valid(cval, static_cast<uint32_t>(j))) { all_ok = false; break; }
                const DrakenStringSlot* elem = &arena->slots[csel[j]];
                if (!CmpOp::cmp(scalar_slot, scalar_bytes, elem, arena->arena)) {
                    all_ok = false; break;
                }
            }
            if (all_ok) result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        } else {
            for (int32_t j = start; j < end; ++j) {
                if (!arr_row_valid(cval, static_cast<uint32_t>(j))) continue;
                const DrakenStringSlot* elem = &arena->slots[csel[j]];
                if (CmpOp::cmp(scalar_slot, scalar_bytes, elem, arena->arena)) {
                    result[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                    break;
                }
            }
        }
    }

    VecResult res;
    res.data          = result;
    res.validity      = validity;
    res.length        = n;
    res.type          = DRAKEN_BOOL;
    res.selection     = draken_identity_sel(n);
    res.data_length   = n;
    res.owns_selection = false;
    res.flags         = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return res;
}

// ---------------------------------------------------------------------------
// All-False result (null literal or unsupported null-literal path)
// ---------------------------------------------------------------------------

static inline VecResult arr_all_false(uint32_t n) {
    uint8_t* result = arr_alloc_bool_buf(n);
    VecResult res;
    res.data          = result;
    res.validity      = nullptr;
    res.length        = n;
    res.type          = DRAKEN_BOOL;
    res.selection     = draken_identity_sel(n);
    res.data_length   = n;
    res.owns_selection = false;
    res.flags         = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return res;
}

// ---------------------------------------------------------------------------
// Dispatching entry points — switch on child.type, call typed kernel.
// ---------------------------------------------------------------------------

#define ARR_DISPATCH(cmp_i64, cmp_str, reduce_all_flag)                             \
    if (scalar.type == DRAKEN_NULL) return arr_all_false(arr.length);               \
    if (child.type == DRAKEN_INT64) {                                               \
        return arr_reduce_int64<cmp_i64, reduce_all_flag>(arr, child, scalar.i64);  \
    }                                                                               \
    if (child.type == DRAKEN_STRING) {                                              \
        return arr_reduce_string<cmp_str, reduce_all_flag>(                         \
            arr, child, scalar.str.slot, scalar.str.bytes);                         \
    }                                                                               \
    throw std::invalid_argument(                                                    \
        "array_reductions: unsupported child element type (only INT64 and STRING supported)");

static inline VecResult arr_any_eq(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Eq, ArrStrEq, false)
}
static inline VecResult arr_all_eq(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Eq, ArrStrEq, true)
}
static inline VecResult arr_any_ne(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Ne, ArrStrNe, false)
}
static inline VecResult arr_all_ne(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Ne, ArrStrNe, true)
}
static inline VecResult arr_any_gt(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Gt, ArrStrGt, false)
}
static inline VecResult arr_any_ge(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Ge, ArrStrGe, false)
}
static inline VecResult arr_any_lt(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Lt, ArrStrLt, false)
}
static inline VecResult arr_any_le(const DrakenVector& arr, const DrakenVector& child, const ArrScalar& scalar) {
    ARR_DISPATCH(ArrI64Le, ArrStrLe, false)
}

#undef ARR_DISPATCH

}} // namespace draken::ops
