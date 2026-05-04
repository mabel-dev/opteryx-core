#pragma once
//
// Templated comparison kernels for Date32Vector.
//
// One template instantiated per operator. The op is a compile-time tag, so
// the inner loop body reduces to a single direct compare and the C++
// compiler is free to auto-vectorise to NEON / AVX2. No dispatch, no
// function calls inside the hot loop.
//
// Three scalar variants and three vector-vector variants are provided to
// match the existing null-density gating in date32_vector.pyx:
//   - _nonnull         : no input null bitmap
//   - _branchless      : null bitmap present, low null density
//   - _branching       : null bitmap present, high null density (>~70%)
//
// The result `dst` is a packed bit array (one bit per row, LSB-first within
// each byte). Caller is responsible for zeroing `dst` before invocation and
// for sizing it to (n + 7) / 8 bytes.
//
// `out_null` (when present) follows the same layout. Caller pre-zeroes it.
//

#include <stdint.h>
#include <stddef.h>
#include <string.h>

namespace draken { namespace date32_cmp {

// Fill `count` bits in `dst` starting at bit offset `start` (LSB-first within each byte).
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
    // First partial byte: bits [start&7 .. 7]
    dst[first_byte] |= static_cast<uint8_t>(0xFFu << (start & 7));
    // Full middle bytes
    if (last_byte > first_byte + 1)
        memset(dst + first_byte + 1, 0xFF, last_byte - first_byte - 1);
    // Last partial byte: bits [0 .. (end-1)&7]
    dst[last_byte] |= static_cast<uint8_t>(0xFFu >> (7u - ((end - 1u) & 7u)));
}

struct Eq { static inline bool apply(int32_t a, int32_t b) { return a == b; } };
struct Ne { static inline bool apply(int32_t a, int32_t b) { return a != b; } };
struct Gt { static inline bool apply(int32_t a, int32_t b) { return a >  b; } };
struct Ge { static inline bool apply(int32_t a, int32_t b) { return a >= b; } };
struct Lt { static inline bool apply(int32_t a, int32_t b) { return a <  b; } };
struct Le { static inline bool apply(int32_t a, int32_t b) { return a <= b; } };

// ---------------------------------------------------------------------------
// Scalar compare: data[i] OP value
// ---------------------------------------------------------------------------

template <typename Op>
static inline void cmp_scalar_nonnull(
    const int32_t* __restrict__ data,
    int32_t value,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t m = Op::apply(data[i], value) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>(m << (i & 7));
    }
}

// Low null density: branchless. Reads the validity bit and ANDs it with the
// comparison result so mispredicted branches don't dominate.
template <typename Op>
static inline void cmp_scalar_branchless(
    const int32_t* __restrict__ data,
    int32_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (src_null[i >> 3] >> (i & 7)) & 1u;
        const uint8_t m = Op::apply(data[i], value) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>((v & m) << (i & 7));
    }
}

// High null density: branch on the validity bit so we skip the compare and
// the bit write entirely for null rows.
template <typename Op>
static inline void cmp_scalar_branching(
    const int32_t* __restrict__ data,
    int32_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        if ((src_null[i >> 3] >> (i & 7)) & 1u) {
            if (Op::apply(data[i], value)) {
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Vector-vector compare: a[i] OP b[i]
// ---------------------------------------------------------------------------

template <typename Op>
static inline void cmp_vector_nonnull(
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    uint8_t* __restrict__ dst,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t m = Op::apply(a[i], b[i]) ? 1u : 0u;
        dst[i >> 3] |= static_cast<uint8_t>(m << (i & 7));
    }
}

// One side has a null bitmap, the other does not. `null_side` points at the
// validity bitmap of whichever input owns one. The output null bitmap mirrors
// that input's validity directly.
template <typename Op>
static inline void cmp_vector_one_null(
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    const uint8_t* __restrict__ null_side,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        const uint8_t v = (null_side[i >> 3] >> (i & 7)) & 1u;
        const uint8_t m = Op::apply(a[i], b[i]) ? 1u : 0u;
        const size_t  byte = i >> 3;
        const uint8_t bit  = static_cast<uint8_t>(i & 7);
        dst[byte]       |= static_cast<uint8_t>((v & m) << bit);
        out_null[byte]  |= static_cast<uint8_t>(v        << bit);
    }
}

// Both sides have null bitmaps. Branchless variant — combined validity is
// AND of the two bits.
template <typename Op>
static inline void cmp_vector_both_null_branchless(
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
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
        const uint8_t m  = Op::apply(a[i], b[i]) ? 1u : 0u;
        const size_t  byte = i >> 3;
        const uint8_t bit  = static_cast<uint8_t>(i & 7);
        dst[byte]      |= static_cast<uint8_t>((v & m) << bit);
        out_null[byte] |= static_cast<uint8_t>(v       << bit);
    }
}

// Both sides have null bitmaps. High null density: branch.
template <typename Op>
static inline void cmp_vector_both_null_branching(
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
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
            if (Op::apply(a[i], b[i])) {
                dst[byte] |= bit;
            }
        }
    }
}

// High null density, one side has a null bitmap: branch per-row to skip nulls.
template <typename Op>
static inline void cmp_vector_one_null_branching(
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
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
            if (Op::apply(a[i], b[i])) {
                dst[byte] |= bit;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-call dispatchers: select the op once, then run a tight loop with no
// per-row branching. op codes match date32_vector.pyx convention:
//   0=eq  1=ne  2=gt  3=ge  4=lt  5=le
// ---------------------------------------------------------------------------

static inline bool dispatch_compare_once(int op, int32_t a, int32_t b) {
    switch (op) {
        case 0: return Eq::apply(a, b);
        case 1: return Ne::apply(a, b);
        case 2: return Gt::apply(a, b);
        case 3: return Ge::apply(a, b);
        case 4: return Lt::apply(a, b);
        default: return Le::apply(a, b);
    }
}

static inline void dispatch_scalar_nonnull(
    int op,
    const int32_t* __restrict__ data,
    int32_t value,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_scalar_nonnull<Eq>(data, value, dst, n); break;
        case 1: cmp_scalar_nonnull<Ne>(data, value, dst, n); break;
        case 2: cmp_scalar_nonnull<Gt>(data, value, dst, n); break;
        case 3: cmp_scalar_nonnull<Ge>(data, value, dst, n); break;
        case 4: cmp_scalar_nonnull<Lt>(data, value, dst, n); break;
        default: cmp_scalar_nonnull<Le>(data, value, dst, n); break;
    }
}

static inline void dispatch_scalar_branchless(
    int op,
    const int32_t* __restrict__ data,
    int32_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_scalar_branchless<Eq>(data, value, src_null, dst, n); break;
        case 1: cmp_scalar_branchless<Ne>(data, value, src_null, dst, n); break;
        case 2: cmp_scalar_branchless<Gt>(data, value, src_null, dst, n); break;
        case 3: cmp_scalar_branchless<Ge>(data, value, src_null, dst, n); break;
        case 4: cmp_scalar_branchless<Lt>(data, value, src_null, dst, n); break;
        default: cmp_scalar_branchless<Le>(data, value, src_null, dst, n); break;
    }
}

static inline void dispatch_scalar_branching(
    int op,
    const int32_t* __restrict__ data,
    int32_t value,
    const uint8_t* __restrict__ src_null,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_scalar_branching<Eq>(data, value, src_null, dst, n); break;
        case 1: cmp_scalar_branching<Ne>(data, value, src_null, dst, n); break;
        case 2: cmp_scalar_branching<Gt>(data, value, src_null, dst, n); break;
        case 3: cmp_scalar_branching<Ge>(data, value, src_null, dst, n); break;
        case 4: cmp_scalar_branching<Lt>(data, value, src_null, dst, n); break;
        default: cmp_scalar_branching<Le>(data, value, src_null, dst, n); break;
    }
}

static inline void dispatch_vector_nonnull(
    int op,
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    uint8_t* __restrict__ dst,
    size_t n)
{
    switch (op) {
        case 0: cmp_vector_nonnull<Eq>(a, b, dst, n); break;
        case 1: cmp_vector_nonnull<Ne>(a, b, dst, n); break;
        case 2: cmp_vector_nonnull<Gt>(a, b, dst, n); break;
        case 3: cmp_vector_nonnull<Ge>(a, b, dst, n); break;
        case 4: cmp_vector_nonnull<Lt>(a, b, dst, n); break;
        default: cmp_vector_nonnull<Le>(a, b, dst, n); break;
    }
}

static inline void dispatch_vector_one_null_branchless(
    int op,
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    const uint8_t* __restrict__ null_side,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    switch (op) {
        case 0: cmp_vector_one_null<Eq>(a, b, null_side, dst, out_null, n); break;
        case 1: cmp_vector_one_null<Ne>(a, b, null_side, dst, out_null, n); break;
        case 2: cmp_vector_one_null<Gt>(a, b, null_side, dst, out_null, n); break;
        case 3: cmp_vector_one_null<Ge>(a, b, null_side, dst, out_null, n); break;
        case 4: cmp_vector_one_null<Lt>(a, b, null_side, dst, out_null, n); break;
        default: cmp_vector_one_null<Le>(a, b, null_side, dst, out_null, n); break;
    }
}

static inline void dispatch_vector_one_null_branching(
    int op,
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    const uint8_t* __restrict__ null_side,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    switch (op) {
        case 0: cmp_vector_one_null_branching<Eq>(a, b, null_side, dst, out_null, n); break;
        case 1: cmp_vector_one_null_branching<Ne>(a, b, null_side, dst, out_null, n); break;
        case 2: cmp_vector_one_null_branching<Gt>(a, b, null_side, dst, out_null, n); break;
        case 3: cmp_vector_one_null_branching<Ge>(a, b, null_side, dst, out_null, n); break;
        case 4: cmp_vector_one_null_branching<Lt>(a, b, null_side, dst, out_null, n); break;
        default: cmp_vector_one_null_branching<Le>(a, b, null_side, dst, out_null, n); break;
    }
}

static inline void dispatch_vector_both_null_branchless(
    int op,
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    const uint8_t* __restrict__ null_a,
    const uint8_t* __restrict__ null_b,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    switch (op) {
        case 0: cmp_vector_both_null_branchless<Eq>(a, b, null_a, null_b, dst, out_null, n); break;
        case 1: cmp_vector_both_null_branchless<Ne>(a, b, null_a, null_b, dst, out_null, n); break;
        case 2: cmp_vector_both_null_branchless<Gt>(a, b, null_a, null_b, dst, out_null, n); break;
        case 3: cmp_vector_both_null_branchless<Ge>(a, b, null_a, null_b, dst, out_null, n); break;
        case 4: cmp_vector_both_null_branchless<Lt>(a, b, null_a, null_b, dst, out_null, n); break;
        default: cmp_vector_both_null_branchless<Le>(a, b, null_a, null_b, dst, out_null, n); break;
    }
}

static inline void dispatch_vector_both_null_branching(
    int op,
    const int32_t* __restrict__ a,
    const int32_t* __restrict__ b,
    const uint8_t* __restrict__ null_a,
    const uint8_t* __restrict__ null_b,
    uint8_t* __restrict__ dst,
    uint8_t* __restrict__ out_null,
    size_t n)
{
    switch (op) {
        case 0: cmp_vector_both_null_branching<Eq>(a, b, null_a, null_b, dst, out_null, n); break;
        case 1: cmp_vector_both_null_branching<Ne>(a, b, null_a, null_b, dst, out_null, n); break;
        case 2: cmp_vector_both_null_branching<Gt>(a, b, null_a, null_b, dst, out_null, n); break;
        case 3: cmp_vector_both_null_branching<Ge>(a, b, null_a, null_b, dst, out_null, n); break;
        case 4: cmp_vector_both_null_branching<Lt>(a, b, null_a, null_b, dst, out_null, n); break;
        default: cmp_vector_both_null_branching<Le>(a, b, null_a, null_b, dst, out_null, n); break;
    }
}

}}  // namespace draken::date32_cmp
