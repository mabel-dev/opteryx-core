// Bitmap operations for the bytecode VM evaluator.
#include "bitmap_ops.h"
#include <cstring>
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

/* Count set bits in a bitmap using std::popcount (C++20).
 *
 * Accumulates over nbytes bytes (each byte contains 8 bits).
 * For performance on large bitmaps, this could use SIMD intrinsics (POPCNT,
 * AVX2 _mm256_sad_epu8, etc.), but the simple byte-loop is correct and sufficient.
 */
size_t simd_popcount(const uint8_t* data, size_t nbytes) {
    size_t count = 0;
    for (size_t i = 0; i < nbytes; i++) {
        count += __builtin_popcount(data[i]);
    }
    return count;
}

/* AND two bitmaps: out = left & right, with NULL merging. */
int c_and_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    int has_null = 0;

    for (size_t i = 0; i < nbytes; i++) {
        // AND the data bits
        out[i] = left[i] & right[i];

        // NULL bitmap: OR the nulls (row is NULL if either is NULL)
        uint8_t null_byte = 0;
        if (left_null) {
            null_byte |= left_null[i];
        }
        if (right_null) {
            null_byte |= right_null[i];
        }
        out_null[i] = null_byte;

        if (null_byte) {
            has_null = 1;
        }
    }

    // Handle partial byte at the end (may have padding bits)
    if ((num_rows & 7) != 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return has_null;
}

/* OR two bitmaps: out = left | right, with NULL merging. */
int c_or_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    int has_null = 0;

    for (size_t i = 0; i < nbytes; i++) {
        // OR the data bits
        out[i] = left[i] | right[i];

        // NULL bitmap: OR the nulls
        uint8_t null_byte = 0;
        if (left_null) {
            null_byte |= left_null[i];
        }
        if (right_null) {
            null_byte |= right_null[i];
        }
        out_null[i] = null_byte;

        if (null_byte) {
            has_null = 1;
        }
    }

    // Handle partial byte at the end
    if ((num_rows & 7) != 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return has_null;
}

/* XOR two bitmaps: out = left ^ right, with NULL merging. */
int c_xor_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* left, const uint8_t* left_null,
    const uint8_t* right, const uint8_t* right_null,
    size_t nbytes, uint32_t num_rows
) {
    int has_null = 0;

    for (size_t i = 0; i < nbytes; i++) {
        // XOR the data bits
        out[i] = left[i] ^ right[i];

        // NULL bitmap: OR the nulls
        uint8_t null_byte = 0;
        if (left_null) {
            null_byte |= left_null[i];
        }
        if (right_null) {
            null_byte |= right_null[i];
        }
        out_null[i] = null_byte;

        if (null_byte) {
            has_null = 1;
        }
    }

    // Handle partial byte at the end
    if ((num_rows & 7) != 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return has_null;
}

/* NOT a bitmap: out = ~src (within num_rows bits), with NULL propagation. */
int c_not_bitmap(
    uint8_t* out, uint8_t* out_null,
    const uint8_t* src, const uint8_t* src_null,
    size_t nbytes, uint32_t num_rows
) {
    int has_null = 0;

    for (size_t i = 0; i < nbytes; i++) {
        // NOT the data bits
        out[i] = ~src[i];

        // NULL bitmap: propagate (NOT doesn't change NULL status)
        uint8_t null_byte = 0;
        if (src_null) {
            null_byte = src_null[i];
        }
        out_null[i] = null_byte;

        if (null_byte) {
            has_null = 1;
        }
    }

    // Handle partial byte at the end
    if ((num_rows & 7) != 0) {
        uint8_t mask = (1 << (num_rows & 7)) - 1;
        out[nbytes - 1] &= mask;
        out_null[nbytes - 1] &= mask;
    }

    return has_null;
}

/* Stub: extract bitmap pointers from a DrakenVector.
 *
 * Currently unused. Placeholder for future VM work.
 */
void c_get_bitmap_ptrs(void* draken_vector) {
    // No-op stub.
}

#ifdef __cplusplus
}
#endif
