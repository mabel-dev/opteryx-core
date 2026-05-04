#pragma once
//
// Templated comparison kernels for IntegerVector (int8 / int16 / int32 widths).
//
// Same design as _int64_compare.hpp: one C++ template instantiated per
// operator so the inner loop reduces to a single direct compare and the
// compiler is free to auto-vectorise to NEON / AVX2.
//
// Three scalar variants and three vector-vector variants:
//   - _nonnull    : no input null bitmap
//   - _branchless : null bitmap present, low null density
//   - _branching  : null bitmap present, high null density (>~70%)
//
// Templates are parameterised on element type T (int8_t / int16_t / int32_t).
// Comparisons are widened to int64_t to match the int64_t scalar coming from
// the Cython layer.
//
// Concrete per-width dispatchers are exposed so that Cython `cdef extern`
// can bind them without needing template syntax.
//
// op codes match integer_vector.pyx convention:
//   0=eq  1=ne  2=gt  3=ge  4=lt  5=le
//

#include <stdint.h>
#include <stddef.h>
#include <string.h>

namespace draken { namespace integer_cmp {

// ---------------------------------------------------------------------------
// Comparison operator tags
// ---------------------------------------------------------------------------
struct Eq { static inline bool apply(int64_t a, int64_t b) { return a == b; } };
struct Ne { static inline bool apply(int64_t a, int64_t b) { return a != b; } };
struct Gt { static inline bool apply(int64_t a, int64_t b) { return a >  b; } };
struct Ge { static inline bool apply(int64_t a, int64_t b) { return a >= b; } };
struct Lt { static inline bool apply(int64_t a, int64_t b) { return a <  b; } };
struct Le { static inline bool apply(int64_t a, int64_t b) { return a <= b; } };

// ---------------------------------------------------------------------------
// bit_fill_range: set `count` bits in `dst` starting at bit offset `start`.
// (Shared with _int64_compare.hpp but copied here to keep the header
//  self-contained; the compiler will inline and deduplicate.)
// ---------------------------------------------------------------------------
static inline void bit_fill_range(uint8_t* dst, size_t start, size_t count) {
    if (count == 0) return;
    const size_t end        = start + count;
    const size_t first_byte = start >> 3;
    const size_t last_byte  = (end - 1) >> 3;

    if (first_byte == last_byte) {
        const uint8_t first_bit = static_cast<uint8_t>(start & 7);
        dst[first_byte] |= static_cast<uint8_t>(((1u << count) - 1u) << first_bit);
        return;
    }
    dst[first_byte] |= static_cast<uint8_t>(0xFFu << (start & 7));
    if (last_byte > first_byte + 1)
        memset(dst + first_byte + 1, 0xFF, last_byte - first_byte - 1);
    dst[last_byte] |= static_cast<uint8_t>(0xFFu >> (7u - ((end - 1u) & 7u)));
}

// ---------------------------------------------------------------------------
// dispatch_compare_once: single-value comparison (used for const / RLE paths)
// ---------------------------------------------------------------------------
static inline bool dispatch_compare_once(int op, int64_t a, int64_t b) {
    switch (op) {
        case 0: return Eq::apply(a, b);
        case 1: return Ne::apply(a, b);
        case 2: return Gt::apply(a, b);
        case 3: return Ge::apply(a, b);
        case 4: return Lt::apply(a, b);
        default: return Le::apply(a, b);
    }
}

// ---------------------------------------------------------------------------
// Scalar kernels: data[i] OP value
// ---------------------------------------------------------------------------

template <typename T, typename Op>
static inline void cmp_scalar_nonnull(
    const T* __restrict__ data,
    int64_t value,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t m = Op::apply(static_cast<int64_t>(data[i]), value) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>(m << (i & 7));
    }
}

template <typename T, typename Op>
static inline void cmp_scalar_branchless(
    const T* __restrict__ data,
    int64_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (src_null[i >> 3] >> (i & 7)) & 1u;
        const uint8_t m = Op::apply(static_cast<int64_t>(data[i]), value) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>((v & m) << (i & 7));
    }
}

template <typename T, typename Op>
static inline void cmp_scalar_branching(
    const T* __restrict__ data,
    int64_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        if ((src_null[i >> 3] >> (i & 7)) & 1u) {
            if (Op::apply(static_cast<int64_t>(data[i]), value)) {
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Vector-vector kernels: a[i] OP b[i] (widened to int64_t for comparison)
// ---------------------------------------------------------------------------

template <typename A, typename B, typename Op>
static inline void cmp_vector_nonnull(
    const A* __restrict__ a,
    const B* __restrict__ b,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t m = Op::apply(static_cast<int64_t>(a[i]), static_cast<int64_t>(b[i])) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>(m << (i & 7));
    }
}

template <typename A, typename B, typename Op>
static inline void cmp_vector_one_null(
    const A* __restrict__ a,
    const B* __restrict__ b,
    const uint8_t* __restrict__ null_side,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (null_side[i >> 3] >> (i & 7)) & 1u;
        const uint8_t m = Op::apply(static_cast<int64_t>(a[i]), static_cast<int64_t>(b[i])) ? 1u : 0u;
        const size_t  byte = i >> 3;
        const uint8_t bit  = static_cast<uint8_t>(i & 7);
        dst[byte]      |= static_cast<uint8_t>((v & m) << bit);
        out_null[byte] |= static_cast<uint8_t>(v        << bit);
    }
}

template <typename A, typename B, typename Op>
static inline void cmp_vector_both_null_branchless(
    const A* __restrict__ a,
    const B* __restrict__ b,
    const uint8_t* __restrict__ null_a,
    const uint8_t* __restrict__ null_b,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t va = (null_a[i >> 3] >> (i & 7)) & 1u;
        const uint8_t vb = (null_b[i >> 3] >> (i & 7)) & 1u;
        const uint8_t v  = static_cast<uint8_t>(va & vb);
        const uint8_t m  = Op::apply(static_cast<int64_t>(a[i]), static_cast<int64_t>(b[i])) ? 1u : 0u;
        const size_t  byte = i >> 3;
        const uint8_t bit  = static_cast<uint8_t>(i & 7);
        dst[byte]      |= static_cast<uint8_t>((v & m) << bit);
        out_null[byte] |= static_cast<uint8_t>(v       << bit);
    }
}

template <typename A, typename B, typename Op>
static inline void cmp_vector_both_null_branching(
    const A* __restrict__ a,
    const B* __restrict__ b,
    const uint8_t* __restrict__ null_a,
    const uint8_t* __restrict__ null_b,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t va = (null_a[i >> 3] >> (i & 7)) & 1u;
        const uint8_t vb = (null_b[i >> 3] >> (i & 7)) & 1u;
        if (va & vb) {
            const size_t  byte = i >> 3;
            const uint8_t bit  = static_cast<uint8_t>(1u << (i & 7));
            out_null[byte] |= bit;
            if (Op::apply(static_cast<int64_t>(a[i]), static_cast<int64_t>(b[i]))) {
                dst[byte] |= bit;
            }
        }
    }
}

template <typename A, typename B, typename Op>
static inline void cmp_vector_one_null_branching(
    const A* __restrict__ a,
    const B* __restrict__ b,
    const uint8_t* __restrict__ null_side,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        if ((null_side[i >> 3] >> (i & 7)) & 1u) {
            const size_t  byte = i >> 3;
            const uint8_t bit  = static_cast<uint8_t>(1u << (i & 7));
            out_null[byte] |= bit;
            if (Op::apply(static_cast<int64_t>(a[i]), static_cast<int64_t>(b[i]))) {
                dst[byte] |= bit;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Concrete scalar dispatchers — one set per element width.
// Cython cdef extern binds these by name.
// ---------------------------------------------------------------------------

#define MAKE_SCALAR_DISPATCHERS(SUFFIX, T)                                                      \
static inline void dispatch_scalar_nonnull_##SUFFIX(                                            \
    int op, const T* data, int64_t value, uint8_t* dst, size_t n)                              \
{                                                                                               \
    switch (op) {                                                                               \
        case 0: cmp_scalar_nonnull<T, Eq>(data, value, dst, n); break;                         \
        case 1: cmp_scalar_nonnull<T, Ne>(data, value, dst, n); break;                         \
        case 2: cmp_scalar_nonnull<T, Gt>(data, value, dst, n); break;                         \
        case 3: cmp_scalar_nonnull<T, Ge>(data, value, dst, n); break;                         \
        case 4: cmp_scalar_nonnull<T, Lt>(data, value, dst, n); break;                         \
        default: cmp_scalar_nonnull<T, Le>(data, value, dst, n); break;                        \
    }                                                                                           \
}                                                                                               \
static inline void dispatch_scalar_branchless_##SUFFIX(                                         \
    int op, const T* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)    \
{                                                                                               \
    switch (op) {                                                                               \
        case 0: cmp_scalar_branchless<T, Eq>(data, value, src_null, dst, n); break;            \
        case 1: cmp_scalar_branchless<T, Ne>(data, value, src_null, dst, n); break;            \
        case 2: cmp_scalar_branchless<T, Gt>(data, value, src_null, dst, n); break;            \
        case 3: cmp_scalar_branchless<T, Ge>(data, value, src_null, dst, n); break;            \
        case 4: cmp_scalar_branchless<T, Lt>(data, value, src_null, dst, n); break;            \
        default: cmp_scalar_branchless<T, Le>(data, value, src_null, dst, n); break;           \
    }                                                                                           \
}                                                                                               \
static inline void dispatch_scalar_branching_##SUFFIX(                                          \
    int op, const T* data, int64_t value, const uint8_t* src_null, uint8_t* dst, size_t n)    \
{                                                                                               \
    switch (op) {                                                                               \
        case 0: cmp_scalar_branching<T, Eq>(data, value, src_null, dst, n); break;             \
        case 1: cmp_scalar_branching<T, Ne>(data, value, src_null, dst, n); break;             \
        case 2: cmp_scalar_branching<T, Gt>(data, value, src_null, dst, n); break;             \
        case 3: cmp_scalar_branching<T, Ge>(data, value, src_null, dst, n); break;             \
        case 4: cmp_scalar_branching<T, Lt>(data, value, src_null, dst, n); break;             \
        default: cmp_scalar_branching<T, Le>(data, value, src_null, dst, n); break;            \
    }                                                                                           \
}

MAKE_SCALAR_DISPATCHERS(i8,  int8_t)
MAKE_SCALAR_DISPATCHERS(i16, int16_t)
MAKE_SCALAR_DISPATCHERS(i32, int32_t)

#undef MAKE_SCALAR_DISPATCHERS

// ---------------------------------------------------------------------------
// Concrete vector-vector dispatchers — same-type pairs (i8×i8, i16×i16, i32×i32).
// Mixed-width pairs widen through int64_t at comparison time.
// ---------------------------------------------------------------------------

#define MAKE_VECTOR_DISPATCHERS(SUFFIX, A, B)                                                                                                           \
static inline void dispatch_vector_nonnull_##SUFFIX(                                                                                                    \
    int op, const A* a, const B* b, uint8_t* dst, size_t n)                                                                                            \
{                                                                                                                                                       \
    switch (op) {                                                                                                                                       \
        case 0: cmp_vector_nonnull<A, B, Eq>(a, b, dst, n); break;                                                                                     \
        case 1: cmp_vector_nonnull<A, B, Ne>(a, b, dst, n); break;                                                                                     \
        case 2: cmp_vector_nonnull<A, B, Gt>(a, b, dst, n); break;                                                                                     \
        case 3: cmp_vector_nonnull<A, B, Ge>(a, b, dst, n); break;                                                                                     \
        case 4: cmp_vector_nonnull<A, B, Lt>(a, b, dst, n); break;                                                                                     \
        default: cmp_vector_nonnull<A, B, Le>(a, b, dst, n); break;                                                                                    \
    }                                                                                                                                                   \
}                                                                                                                                                       \
static inline void dispatch_vector_one_null_branchless_##SUFFIX(                                                                                        \
    int op, const A* a, const B* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)                                               \
{                                                                                                                                                       \
    switch (op) {                                                                                                                                       \
        case 0: cmp_vector_one_null<A, B, Eq>(a, b, null_side, dst, out_null, n); break;                                                               \
        case 1: cmp_vector_one_null<A, B, Ne>(a, b, null_side, dst, out_null, n); break;                                                               \
        case 2: cmp_vector_one_null<A, B, Gt>(a, b, null_side, dst, out_null, n); break;                                                               \
        case 3: cmp_vector_one_null<A, B, Ge>(a, b, null_side, dst, out_null, n); break;                                                               \
        case 4: cmp_vector_one_null<A, B, Lt>(a, b, null_side, dst, out_null, n); break;                                                               \
        default: cmp_vector_one_null<A, B, Le>(a, b, null_side, dst, out_null, n); break;                                                              \
    }                                                                                                                                                   \
}                                                                                                                                                       \
static inline void dispatch_vector_one_null_branching_##SUFFIX(                                                                                         \
    int op, const A* a, const B* b, const uint8_t* null_side, uint8_t* dst, uint8_t* out_null, size_t n)                                               \
{                                                                                                                                                       \
    switch (op) {                                                                                                                                       \
        case 0: cmp_vector_one_null_branching<A, B, Eq>(a, b, null_side, dst, out_null, n); break;                                                     \
        case 1: cmp_vector_one_null_branching<A, B, Ne>(a, b, null_side, dst, out_null, n); break;                                                     \
        case 2: cmp_vector_one_null_branching<A, B, Gt>(a, b, null_side, dst, out_null, n); break;                                                     \
        case 3: cmp_vector_one_null_branching<A, B, Ge>(a, b, null_side, dst, out_null, n); break;                                                     \
        case 4: cmp_vector_one_null_branching<A, B, Lt>(a, b, null_side, dst, out_null, n); break;                                                     \
        default: cmp_vector_one_null_branching<A, B, Le>(a, b, null_side, dst, out_null, n); break;                                                    \
    }                                                                                                                                                   \
}                                                                                                                                                       \
static inline void dispatch_vector_both_null_branchless_##SUFFIX(                                                                                       \
    int op, const A* a, const B* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)                          \
{                                                                                                                                                       \
    switch (op) {                                                                                                                                       \
        case 0: cmp_vector_both_null_branchless<A, B, Eq>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
        case 1: cmp_vector_both_null_branchless<A, B, Ne>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
        case 2: cmp_vector_both_null_branchless<A, B, Gt>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
        case 3: cmp_vector_both_null_branchless<A, B, Ge>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
        case 4: cmp_vector_both_null_branchless<A, B, Lt>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
        default: cmp_vector_both_null_branchless<A, B, Le>(a, b, null_a, null_b, dst, out_null, n); break;                                             \
    }                                                                                                                                                   \
}                                                                                                                                                       \
static inline void dispatch_vector_both_null_branching_##SUFFIX(                                                                                        \
    int op, const A* a, const B* b, const uint8_t* null_a, const uint8_t* null_b, uint8_t* dst, uint8_t* out_null, size_t n)                          \
{                                                                                                                                                       \
    switch (op) {                                                                                                                                       \
        case 0: cmp_vector_both_null_branching<A, B, Eq>(a, b, null_a, null_b, dst, out_null, n); break;                                               \
        case 1: cmp_vector_both_null_branching<A, B, Ne>(a, b, null_a, null_b, dst, out_null, n); break;                                               \
        case 2: cmp_vector_both_null_branching<A, B, Gt>(a, b, null_a, null_b, dst, out_null, n); break;                                               \
        case 3: cmp_vector_both_null_branching<A, B, Ge>(a, b, null_a, null_b, dst, out_null, n); break;                                               \
        case 4: cmp_vector_both_null_branching<A, B, Lt>(a, b, null_a, null_b, dst, out_null, n); break;                                               \
        default: cmp_vector_both_null_branching<A, B, Le>(a, b, null_a, null_b, dst, out_null, n); break;                                              \
    }                                                                                                                                                   \
}

// Same-type pairs
MAKE_VECTOR_DISPATCHERS(i8_i8,   int8_t,  int8_t)
MAKE_VECTOR_DISPATCHERS(i16_i16, int16_t, int16_t)
MAKE_VECTOR_DISPATCHERS(i32_i32, int32_t, int32_t)
// Mixed-width pairs (self narrower than other)
MAKE_VECTOR_DISPATCHERS(i8_i16,  int8_t,  int16_t)
MAKE_VECTOR_DISPATCHERS(i8_i32,  int8_t,  int32_t)
MAKE_VECTOR_DISPATCHERS(i16_i32, int16_t, int32_t)
// Mixed-width pairs (self wider than other)
MAKE_VECTOR_DISPATCHERS(i16_i8,  int16_t, int8_t)
MAKE_VECTOR_DISPATCHERS(i32_i8,  int32_t, int8_t)
MAKE_VECTOR_DISPATCHERS(i32_i16, int32_t, int16_t)

#undef MAKE_VECTOR_DISPATCHERS

}}  // namespace draken::integer_cmp
