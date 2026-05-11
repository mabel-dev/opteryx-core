#pragma once
//
// Bit-packing macros shared by the per-type comparison kernels.
//
// The naive `dst[i>>3] |= m << (i&7)` pattern creates an inter-iteration RAW
// dependency on `dst` that defeats SIMD auto-vectorisation. These macros pack
// 8 independent compare results into one byte from local register values,
// then the caller writes the byte once. With `dst` pre-zeroed (the existing
// kernel contract), the write site uses `=` rather than `|=`.
//
// Two flavours:
//   - DRAKEN_PACK8_SCALAR(P, V) : 8 results of Op::apply(P[i], V) where V is
//                                 a scalar value.
//   - DRAKEN_PACK8_VECTOR(A, B) : 8 results of Op::apply(A[i], B[i]).
//
// Both expect `Op` to be in scope at the call site (i.e. inside a template
// instantiation). They are macros, not inline templates, so they textually
// inline the 8 compares into the caller's loop body.
//

#define DRAKEN_PACK8_SCALAR(P, V) \
    static_cast<uint8_t>( \
        (static_cast<unsigned>(Op::apply((P)[0], (V))) << 0) | \
        (static_cast<unsigned>(Op::apply((P)[1], (V))) << 1) | \
        (static_cast<unsigned>(Op::apply((P)[2], (V))) << 2) | \
        (static_cast<unsigned>(Op::apply((P)[3], (V))) << 3) | \
        (static_cast<unsigned>(Op::apply((P)[4], (V))) << 4) | \
        (static_cast<unsigned>(Op::apply((P)[5], (V))) << 5) | \
        (static_cast<unsigned>(Op::apply((P)[6], (V))) << 6) | \
        (static_cast<unsigned>(Op::apply((P)[7], (V))) << 7))

#define DRAKEN_PACK8_VECTOR(A, B) \
    static_cast<uint8_t>( \
        (static_cast<unsigned>(Op::apply((A)[0], (B)[0])) << 0) | \
        (static_cast<unsigned>(Op::apply((A)[1], (B)[1])) << 1) | \
        (static_cast<unsigned>(Op::apply((A)[2], (B)[2])) << 2) | \
        (static_cast<unsigned>(Op::apply((A)[3], (B)[3])) << 3) | \
        (static_cast<unsigned>(Op::apply((A)[4], (B)[4])) << 4) | \
        (static_cast<unsigned>(Op::apply((A)[5], (B)[5])) << 5) | \
        (static_cast<unsigned>(Op::apply((A)[6], (B)[6])) << 6) | \
        (static_cast<unsigned>(Op::apply((A)[7], (B)[7])) << 7))
