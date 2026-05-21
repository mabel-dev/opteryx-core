#pragma once
//
// Cross-type comparison kernels: Int64Vector vs Float64Vector
//
// Compares int64_t values against double values. Int64 values are implicitly
// converted to double for comparison (lossless for the range of values
// typically encountered in SQL operations).
//
// Template instantiated per operator (Eq, Ne, Lt, Gt, Le, Ge). The op is a
// compile-time tag, so the inner loop body reduces to a single compare and
// the C++ compiler is free to auto-vectorise to NEON / AVX2. No dispatch,
// no function calls inside the hot loop.
//
// Three scalar variants and three vector-vector variants are provided:
//   - _nonnull         : no input null bitmap
//   - _branchless      : null bitmap present, low null density
//   - _branching       : null bitmap present, high null density (>~70%)
//
// The result `dst` is a packed bit array (one bit per row, LSB-first within
// each byte). Caller is responsible for zeroing `dst` before invocation and
// for sizing it to (n + 7) / 8 bytes.
//

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#include "draken/vectors/_compare_bitpack.hpp"

namespace draken { namespace int64_float64_cmp {

// Templated comparison operators: int64_t OP double
struct Eq { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) == b; } };
struct Ne { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) != b; } };
struct Gt { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) > b; } };
struct Ge { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) >= b; } };
struct Lt { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) < b; } };
struct Le { static inline bool apply(int64_t a, double b) { return static_cast<double>(a) <= b; } };

// Reverse operators: double OP int64_t
struct EqRev { static inline bool apply(double a, int64_t b) { return a == static_cast<double>(b); } };
struct NeRev { static inline bool apply(double a, int64_t b) { return a != static_cast<double>(b); } };
struct GtRev { static inline bool apply(double a, int64_t b) { return a > static_cast<double>(b); } };
struct GeRev { static inline bool apply(double a, int64_t b) { return a >= static_cast<double>(b); } };
struct LtRev { static inline bool apply(double a, int64_t b) { return a < static_cast<double>(b); } };
struct LeRev { static inline bool apply(double a, int64_t b) { return a <= static_cast<double>(b); } };

// ---------------------------------------------------------------------------
// Scalar compare: int64[i] OP double_value
// ---------------------------------------------------------------------------

template <typename Op>
static inline void cmp_int64_scalar_nonnull(
    const int64_t* __restrict__ data,
    double value,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data[b * 8 + j], value)) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = byte_result;
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        if (Op::apply(data[i], value)) {
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
}

template <typename Op>
static inline void cmp_int64_scalar_branchless(
    const int64_t* __restrict__ data,
    double value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data[b * 8 + j], value)) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = static_cast<uint8_t>(byte_result & src_null[b]);
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if (Op::apply(data[i], value) && (src_null[byte_idx] & bit_mask)) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

template <typename Op>
static inline void cmp_int64_scalar_branching(
    const int64_t* __restrict__ data,
    double value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if ((src_null[byte_idx] & bit_mask) && Op::apply(data[i], value)) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Vector compare: int64[i] OP float64[i]
// ---------------------------------------------------------------------------

template <typename Op>
static inline void cmp_int64_vector_nonnull(
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data_int[b * 8 + j], data_float[b * 8 + j])) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = byte_result;
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        if (Op::apply(data_int[i], data_float[i])) {
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
}

template <typename Op>
static inline void cmp_int64_vector_branchless(
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data_int[b * 8 + j], data_float[b * 8 + j])) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = static_cast<uint8_t>(byte_result & src_null[b]);
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if (Op::apply(data_int[i], data_float[i]) && (src_null[byte_idx] & bit_mask)) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

template <typename Op>
static inline void cmp_int64_vector_branching(
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if ((src_null[byte_idx] & bit_mask) && Op::apply(data_int[i], data_float[i])) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Vector compare: float64[i] OP int64[i] (reverse operand order)
// ---------------------------------------------------------------------------

template <typename Op>
static inline void cmp_float64_vector_nonnull(
    const double* __restrict__ data_float,
    const int64_t* __restrict__ data_int,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data_float[b * 8 + j], data_int[b * 8 + j])) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = byte_result;
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        if (Op::apply(data_float[i], data_int[i])) {
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }
}

template <typename Op>
static inline void cmp_float64_vector_branchless(
    const double* __restrict__ data_float,
    const int64_t* __restrict__ data_int,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    const size_t whole_bytes = n >> 3;
    for (size_t b = 0; b < whole_bytes; ++b) {
        uint8_t byte_result = 0;
        for (size_t j = 0; j < 8; ++j) {
            if (Op::apply(data_float[b * 8 + j], data_int[b * 8 + j])) {
                byte_result |= static_cast<uint8_t>(1u << j);
            }
        }
        dst[b] = static_cast<uint8_t>(byte_result & src_null[b]);
    }
    // Tail (< 8 rows)
    for (size_t i = whole_bytes << 3; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if (Op::apply(data_float[i], data_int[i]) && (src_null[byte_idx] & bit_mask)) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

template <typename Op>
static inline void cmp_float64_vector_branching(
    const double* __restrict__ data_float,
    const int64_t* __restrict__ data_int,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const size_t byte_idx = i >> 3;
        const uint8_t bit_mask = static_cast<uint8_t>(1u << (i & 7));
        if ((src_null[byte_idx] & bit_mask) && Op::apply(data_float[i], data_int[i])) {
            dst[byte_idx] |= bit_mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Per-call dispatchers for the int64-vs-float64 vector kernels. Select the op
// once, then run the templated loop. op codes match the .pyx convention:
//   0=eq  1=ne  2=gt  3=ge  4=lt  5=le
// ---------------------------------------------------------------------------

static inline void dispatch_i64_f64_vector_nonnull(
    int op,
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_int64_vector_nonnull<Eq>(data_int, data_float, dst, n); break;
        case 1: cmp_int64_vector_nonnull<Ne>(data_int, data_float, dst, n); break;
        case 2: cmp_int64_vector_nonnull<Gt>(data_int, data_float, dst, n); break;
        case 3: cmp_int64_vector_nonnull<Ge>(data_int, data_float, dst, n); break;
        case 4: cmp_int64_vector_nonnull<Lt>(data_int, data_float, dst, n); break;
        default: cmp_int64_vector_nonnull<Le>(data_int, data_float, dst, n); break;
    }
}

static inline void dispatch_i64_f64_vector_branchless(
    int op,
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_int64_vector_branchless<Eq>(data_int, data_float, src_null, dst, n); break;
        case 1: cmp_int64_vector_branchless<Ne>(data_int, data_float, src_null, dst, n); break;
        case 2: cmp_int64_vector_branchless<Gt>(data_int, data_float, src_null, dst, n); break;
        case 3: cmp_int64_vector_branchless<Ge>(data_int, data_float, src_null, dst, n); break;
        case 4: cmp_int64_vector_branchless<Lt>(data_int, data_float, src_null, dst, n); break;
        default: cmp_int64_vector_branchless<Le>(data_int, data_float, src_null, dst, n); break;
    }
}

static inline void dispatch_i64_f64_vector_branching(
    int op,
    const int64_t* __restrict__ data_int,
    const double* __restrict__ data_float,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_int64_vector_branching<Eq>(data_int, data_float, src_null, dst, n); break;
        case 1: cmp_int64_vector_branching<Ne>(data_int, data_float, src_null, dst, n); break;
        case 2: cmp_int64_vector_branching<Gt>(data_int, data_float, src_null, dst, n); break;
        case 3: cmp_int64_vector_branching<Ge>(data_int, data_float, src_null, dst, n); break;
        case 4: cmp_int64_vector_branching<Lt>(data_int, data_float, src_null, dst, n); break;
        default: cmp_int64_vector_branching<Le>(data_int, data_float, src_null, dst, n); break;
    }
}

}} // namespace draken::int64_float64_cmp
