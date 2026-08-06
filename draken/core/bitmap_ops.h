#pragma once
#include <Python.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bitmap operations for the bytecode VM evaluator.
 *
 * All functions operate on bit-level bitmaps where bit i represents row i.
 * nbytes = (num_rows + 7) >> 3.
 *
 * NULL semantics:
 *   - null_bitmap = NULL means "no nulls in this vector"
 *   - null_bitmap != NULL means 1-bit-per-row validity bitmap (1=valid, 0=NULL)
 *   - null_bitmap is bitwise OR'd into the result's null bitmap (row is NULL if EITHER operand is NULL)
 */

/* Count set bits in a bitmap. */
size_t simd_popcount(const uint8_t* data, size_t nbytes);

/* In-place AND: dst &= src over nbytes bytes (word-wide). */
void c_bitmap_and_inplace(uint8_t* dst, const uint8_t* src, size_t nbytes);

/* Create a BoolVector from raw bitmap buffers.
 *
 * Parameters:
 *   bitmap       : uint8_t* pointing at the bit array (nbytes long)
 *   null_bitmap  : NULL if no nulls; otherwise uint8_t* validity bitmap
 *   num_rows     : logical row count
 *
 * Returns a new Python BoolVector object. On failure, returns NULL with
 * a Python exception set. The returned object owns the bitmaps (caller
 * must not free them after calling this function).
 */
PyObject* bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t num_rows);

/* AND two bitmaps: out = left & right.
 *
 * Parameters:
 *   out, out_null : output bitmap buffers (must be pre-allocated; assumed disjoint from inputs)
 *   left, left_null : left operand bitmap(s)
 *   right, right_null : right operand bitmap(s)
 *   nbytes : size of each bitmap in bytes
 *   num_rows : logical row count (for partial byte handling)
 *
 * Returns 1 if result has any nulls (from left_null | right_null), 0 otherwise.
 * out_null is filled with (left_null | right_null) regardless of AND result.
 */
int c_and_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
);

/* OR two bitmaps: out = left | right. */
int c_or_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
);

/* XOR two bitmaps: out = left ^ right. */
int c_xor_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
);

/* NOT a bitmap: out = ~src (within num_rows bits). */
int c_not_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* src, const uint8_t* src_null,
    size_t nbytes, uint32_t num_rows
);

/* Extract bitmap pointers from a DrakenVector.
 *
 * This is a utility for converting a DrakenVector (unified format) into
 * loose bitmap pointers. Currently a no-op stub; may be needed for future VM work.
 */
void c_get_bitmap_ptrs(void* draken_vector);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
// Kleene boolean ops for the bytecode VM — value-aware three-valued logic via
// draken::ops::bool_*. Unlike the c_*_bitmap family above (value-blind null
// merge), these produce correct SQL NULL semantics and safely accept a bare
// DRAKEN_NULL literal operand. op: 0 = AND, 1 = OR, 2 = XOR.
#include "ops/vec_result.h"
#include "buffers.h"
extern "C" {
VecResult draken_vm_bool_binop(int op, const DrakenVector* a, const DrakenVector* b,
                               uint32_t num_rows);
VecResult draken_vm_bool_not(const DrakenVector* a, uint32_t num_rows);

// IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE — never-null truth test
// (op: 0=IS_TRUE 1=IS_FALSE 2=IS_NOT_TRUE 3=IS_NOT_FALSE). See bool_logical.h
// bool_truth_test for the NULL-collapsing semantics (distinct from Kleene AND/OR/NOT).
VecResult draken_vm_bool_truth_test(int op, const DrakenVector* a, uint32_t num_rows);
}
#endif
