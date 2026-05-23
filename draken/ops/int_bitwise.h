#pragma once
// draken/ops/int_bitwise.h — integer bitwise kernels: AND / OR / XOR / NOT / SHL / SHR.
//
// Types: int8_t / int16_t / int32_t / int64_t (DRAKEN_INT8/16/32/64).
// Result type is always the same as the input type (bitwise ops never widen).
//
// Null TVL:
//   Binary ops (AND/OR/XOR/SHL/SHR): null in either operand → null output row.
//   Unary NOT:                         null in operand → null output row.
//
// SHL / SHR shift-count contract:
//   Both operands must be the same type; the shift count is the right operand.
//   For non-null rows: shift_count < 0 or shift_count >= sizeof(T)*8 throws
//   std::invalid_argument.  Null shift-count rows are skipped before the check.
//   Rationale: out-of-range shift is UB in C/C++; fail loud (CLAUDE.md §1).
//
// SHR on signed types: arithmetic (sign-extending, implementation-defined for
// negative values; deterministic on all target platforms — NEON, AVX2 x86).
//
// Dispatch entry points (lightweight switch — no OpsTable dependency here):
//   draken::ops::bitwise_and(a, b)
//   draken::ops::bitwise_or(a, b)
//   draken::ops::bitwise_xor(a, b)
//   draken::ops::bitwise_not(a)
//   draken::ops::bitwise_shl(a, b)
//   draken::ops::bitwise_shr(a, b)
//
// Per-type named instantiations (used by hash.h OpsTable wiring):
//   i8_bitwise_{and,or,xor,not,shl,shr}
//   i16_bitwise_{and,or,xor,not,shl,shr}
//   i32_bitwise_{and,or,xor,not,shl,shr}
//   i64_bitwise_{and,or,xor,not,shl,shr}

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdexcept>
#include <type_traits>
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Internal helpers (bi_ prefix — no ODR conflicts with other op headers)
// ---------------------------------------------------------------------------

static inline bool bi_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return (validity == nullptr) || ((validity[i >> 3] >> (i & 7)) & 1u);
}

// AND two validity bitmaps; returns nullptr when both inputs are all-valid.
static inline uint8_t* bi_combine_validity(
    const uint8_t* a, const uint8_t* b, uint32_t n)
{
    if (a == nullptr && b == nullptr) return nullptr;
    uint32_t nb = (n + 7u) >> 3;
    if (nb == 0) nb = 1;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();
    if (a != nullptr && b != nullptr) {
        for (uint32_t k = 0; k < nb; ++k) out[k] = a[k] & b[k];
    } else if (a != nullptr) {
        memcpy(out, a, nb);
    } else {
        memcpy(out, b, nb);
    }
    return out;
}

static inline uint8_t* bi_copy_validity(const uint8_t* src, uint32_t n) {
    if (src == nullptr) return nullptr;
    uint32_t nb = (n + 7u) >> 3;
    if (nb == 0) nb = 1;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(nb));
    if (!out) throw std::bad_alloc();
    memcpy(out, src, nb);
    return out;
}

template<typename T>
static inline T* bi_alloc(uint32_t n) {
    if (n == 0) n = 1;  // always non-null pointer
    T* p = static_cast<T*>(draken_malloc(n * sizeof(T)));
    if (!p) throw std::bad_alloc();
    return p;
}

template<typename T>
static inline VecResult bi_make_result(
    T* data, uint8_t* validity, uint32_t n, DrakenType tag)
{
    VecResult r;
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = tag;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// Binary kernels: AND / OR / XOR
//
// For null positions: placeholder 0 in data; validity bitmap marks them null.
// No range check needed — any bit pattern in the operands is valid.
// ---------------------------------------------------------------------------

#define BI_BINARY_KERNEL(fn_name, op)                                                  \
template<typename T, DrakenType TAG>                                                   \
static inline VecResult fn_name(const DrakenVector& a, const DrakenVector& b) {       \
    if (a.length != b.length)                                                          \
        throw std::invalid_argument(#fn_name ": length mismatch");                     \
    const uint32_t n = a.length;                                                       \
    const T* ad = static_cast<const T*>(a.data);                                       \
    const T* bd = static_cast<const T*>(b.data);                                       \
    T* dst = bi_alloc<T>(n);                                                           \
    for (uint32_t i = 0; i < n; ++i)                                                   \
        dst[i] = static_cast<T>(ad[a.selection[i]] op bd[b.selection[i]]);             \
    return bi_make_result(dst, bi_combine_validity(a.validity, b.validity, n), n, TAG);\
}

BI_BINARY_KERNEL(bi_and_tmpl, &)
BI_BINARY_KERNEL(bi_or_tmpl,  |)
BI_BINARY_KERNEL(bi_xor_tmpl, ^)

#undef BI_BINARY_KERNEL

// ---------------------------------------------------------------------------
// NOT — unary: inverts all bits.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static inline VecResult bi_not_tmpl(const DrakenVector& a) {
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    T* dst = bi_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<T>(~ad[a.selection[i]]);
    return bi_make_result(dst, bi_copy_validity(a.validity, n), n, TAG);
}

// ---------------------------------------------------------------------------
// SHL — left shift; throws on out-of-range shift count (for non-null rows).
//
// Casts to the unsigned counterpart before shifting to avoid signed-overflow UB.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static inline VecResult bi_shl_tmpl(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("bitwise_shl: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    static constexpr int width = static_cast<int>(sizeof(T) * 8);
    T* dst = bi_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!bi_row_valid(a.validity, i) || !bi_row_valid(b.validity, i)) {
            dst[i] = static_cast<T>(0);  // placeholder, masked by validity bitmap
            continue;
        }
        const int64_t shift = static_cast<int64_t>(bd[b.selection[i]]);
        if (shift < 0 || shift >= static_cast<int64_t>(width))
            throw std::invalid_argument("bitwise_shl: shift count out of range");
        typedef typename std::make_unsigned<T>::type UT;
        dst[i] = static_cast<T>(static_cast<UT>(ad[a.selection[i]]) << static_cast<unsigned>(shift));
    }
    return bi_make_result(dst, bi_combine_validity(a.validity, b.validity, n), n, TAG);
}

// ---------------------------------------------------------------------------
// SHR — arithmetic right shift; throws on out-of-range shift count.
//
// Signed right-shift of a negative value is implementation-defined in C++,
// but consistently arithmetic (sign-extending) on all target platforms.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static inline VecResult bi_shr_tmpl(const DrakenVector& a, const DrakenVector& b) {
    if (a.length != b.length)
        throw std::invalid_argument("bitwise_shr: length mismatch");
    const uint32_t n = a.length;
    const T* ad = static_cast<const T*>(a.data);
    const T* bd = static_cast<const T*>(b.data);
    static constexpr int width = static_cast<int>(sizeof(T) * 8);
    T* dst = bi_alloc<T>(n);
    for (uint32_t i = 0; i < n; ++i) {
        if (!bi_row_valid(a.validity, i) || !bi_row_valid(b.validity, i)) {
            dst[i] = static_cast<T>(0);  // placeholder, masked by validity bitmap
            continue;
        }
        const int64_t shift = static_cast<int64_t>(bd[b.selection[i]]);
        if (shift < 0 || shift >= static_cast<int64_t>(width))
            throw std::invalid_argument("bitwise_shr: shift count out of range");
        dst[i] = static_cast<T>(ad[a.selection[i]] >> static_cast<unsigned>(shift));
    }
    return bi_make_result(dst, bi_combine_validity(a.validity, b.validity, n), n, TAG);
}

// ---------------------------------------------------------------------------
// Named instantiations — one per integer type, one per op.
// These are the symbols wired into hash.h's OpsTable.
// ---------------------------------------------------------------------------

// INT8
static inline VecResult i8_bitwise_and(const DrakenVector& a, const DrakenVector& b) { return bi_and_tmpl<int8_t,  DRAKEN_INT8>(a, b); }
static inline VecResult i8_bitwise_or (const DrakenVector& a, const DrakenVector& b) { return bi_or_tmpl <int8_t,  DRAKEN_INT8>(a, b); }
static inline VecResult i8_bitwise_xor(const DrakenVector& a, const DrakenVector& b) { return bi_xor_tmpl<int8_t,  DRAKEN_INT8>(a, b); }
static inline VecResult i8_bitwise_not(const DrakenVector& a)                         { return bi_not_tmpl<int8_t,  DRAKEN_INT8>(a);    }
static inline VecResult i8_bitwise_shl(const DrakenVector& a, const DrakenVector& b) { return bi_shl_tmpl<int8_t,  DRAKEN_INT8>(a, b); }
static inline VecResult i8_bitwise_shr(const DrakenVector& a, const DrakenVector& b) { return bi_shr_tmpl<int8_t,  DRAKEN_INT8>(a, b); }

// INT16
static inline VecResult i16_bitwise_and(const DrakenVector& a, const DrakenVector& b) { return bi_and_tmpl<int16_t, DRAKEN_INT16>(a, b); }
static inline VecResult i16_bitwise_or (const DrakenVector& a, const DrakenVector& b) { return bi_or_tmpl <int16_t, DRAKEN_INT16>(a, b); }
static inline VecResult i16_bitwise_xor(const DrakenVector& a, const DrakenVector& b) { return bi_xor_tmpl<int16_t, DRAKEN_INT16>(a, b); }
static inline VecResult i16_bitwise_not(const DrakenVector& a)                         { return bi_not_tmpl<int16_t, DRAKEN_INT16>(a);    }
static inline VecResult i16_bitwise_shl(const DrakenVector& a, const DrakenVector& b) { return bi_shl_tmpl<int16_t, DRAKEN_INT16>(a, b); }
static inline VecResult i16_bitwise_shr(const DrakenVector& a, const DrakenVector& b) { return bi_shr_tmpl<int16_t, DRAKEN_INT16>(a, b); }

// INT32
static inline VecResult i32_bitwise_and(const DrakenVector& a, const DrakenVector& b) { return bi_and_tmpl<int32_t, DRAKEN_INT32>(a, b); }
static inline VecResult i32_bitwise_or (const DrakenVector& a, const DrakenVector& b) { return bi_or_tmpl <int32_t, DRAKEN_INT32>(a, b); }
static inline VecResult i32_bitwise_xor(const DrakenVector& a, const DrakenVector& b) { return bi_xor_tmpl<int32_t, DRAKEN_INT32>(a, b); }
static inline VecResult i32_bitwise_not(const DrakenVector& a)                         { return bi_not_tmpl<int32_t, DRAKEN_INT32>(a);    }
static inline VecResult i32_bitwise_shl(const DrakenVector& a, const DrakenVector& b) { return bi_shl_tmpl<int32_t, DRAKEN_INT32>(a, b); }
static inline VecResult i32_bitwise_shr(const DrakenVector& a, const DrakenVector& b) { return bi_shr_tmpl<int32_t, DRAKEN_INT32>(a, b); }

// INT64
static inline VecResult i64_bitwise_and(const DrakenVector& a, const DrakenVector& b) { return bi_and_tmpl<int64_t, DRAKEN_INT64>(a, b); }
static inline VecResult i64_bitwise_or (const DrakenVector& a, const DrakenVector& b) { return bi_or_tmpl <int64_t, DRAKEN_INT64>(a, b); }
static inline VecResult i64_bitwise_xor(const DrakenVector& a, const DrakenVector& b) { return bi_xor_tmpl<int64_t, DRAKEN_INT64>(a, b); }
static inline VecResult i64_bitwise_not(const DrakenVector& a)                         { return bi_not_tmpl<int64_t, DRAKEN_INT64>(a);    }
static inline VecResult i64_bitwise_shl(const DrakenVector& a, const DrakenVector& b) { return bi_shl_tmpl<int64_t, DRAKEN_INT64>(a, b); }
static inline VecResult i64_bitwise_shr(const DrakenVector& a, const DrakenVector& b) { return bi_shr_tmpl<int64_t, DRAKEN_INT64>(a, b); }

// ---------------------------------------------------------------------------
// Dispatch entry points — one per op, switch on v.type.
//
// No OpsTable dependency; this header is self-contained.  The OpsTable in
// hash.h also wires these kernels into its table (for consumers that already
// include hash.h), but this switch-dispatch is the primary path for consumers
// that include only int_bitwise.h.
//
// Type-mismatch between the two operands of binary ops is always an error.
// ---------------------------------------------------------------------------

#define BI_BINARY_DISPATCH(fn_name, kernel_stem)                                       \
static inline VecResult fn_name(const DrakenVector& a, const DrakenVector& b) {       \
    if (a.type != b.type)                                                               \
        throw std::invalid_argument(#fn_name ": type mismatch");                        \
    switch (a.type) {                                                                   \
        case DRAKEN_INT8:  return i8_##kernel_stem(a, b);                              \
        case DRAKEN_INT16: return i16_##kernel_stem(a, b);                             \
        case DRAKEN_INT32: return i32_##kernel_stem(a, b);                             \
        case DRAKEN_INT64: return i64_##kernel_stem(a, b);                             \
        default: throw std::invalid_argument(#fn_name ": unsupported type");           \
    }                                                                                   \
}

BI_BINARY_DISPATCH(bitwise_and, bitwise_and)
BI_BINARY_DISPATCH(bitwise_or,  bitwise_or)
BI_BINARY_DISPATCH(bitwise_xor, bitwise_xor)
BI_BINARY_DISPATCH(bitwise_shl, bitwise_shl)
BI_BINARY_DISPATCH(bitwise_shr, bitwise_shr)

#undef BI_BINARY_DISPATCH

static inline VecResult bitwise_not(const DrakenVector& a) {
    switch (a.type) {
        case DRAKEN_INT8:  return i8_bitwise_not(a);
        case DRAKEN_INT16: return i16_bitwise_not(a);
        case DRAKEN_INT32: return i32_bitwise_not(a);
        case DRAKEN_INT64: return i64_bitwise_not(a);
        default: throw std::invalid_argument("bitwise_not: unsupported type");
    }
}

}} // namespace draken::ops
