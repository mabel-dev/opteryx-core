// draken/vectors/_bool_vector_bridge.cpp — CPython bridge for BoolVector
// construction. Moved verbatim out of draken/core/bitmap_ops.cpp so that
// core/bitmap_ops.h stays free of <Python.h> (CLAUDE.md §2/§5).
#include "vectors/_bool_vector_bridge.h"
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/draken_bridge.h"
#include <cstring>
#include <cstdint>

extern "C" {

/* bool_vector_from_bits — wrap a caller-owned bitmap into a Draken-owned
 * DRAKEN_BOOL Vector by COPYING into draken_malloc'd memory.
 *
 * The caller retains ownership of the input pointers (typically Cython
 * typed-memoryview / libc-malloc'd buffers) and may free them after this
 * call. The returned Vector owns its own (draken-allocated) bitmap copies.
 *
 * Why copy: callers (operator join inner loops, bytecode VM postpass) use
 * libc/Cython allocators for their working buffers; draken_vector_own_raw
 * requires draken_malloc-allocated memory because the Vector's destructor
 * will draken_free it. Mixing allocators is UB. The copy is the legitimate
 * bridge between the two ownership models.
 */
PyObject* bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t num_rows) {
    const uint32_t nbytes = (num_rows + 7u) >> 3;
    // SIMD-padded allocation: round up to 8-byte alignment, minimum 8 bytes
    // so even zero-row vectors get a valid buffer.
    const uint32_t padded = (nbytes + 7u) & ~7u;
    const size_t alloc = padded > 0u ? padded : 8u;

    uint8_t* draken_bitmap = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!draken_bitmap) {
        PyErr_NoMemory();
        return NULL;
    }
    std::memset(draken_bitmap, 0, alloc);
    if (nbytes > 0u) std::memcpy(draken_bitmap, bitmap, nbytes);

    uint8_t* draken_validity = NULL;
    if (null_bitmap != NULL) {
        draken_validity = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!draken_validity) {
            draken_free(draken_bitmap);
            PyErr_NoMemory();
            return NULL;
        }
        std::memset(draken_validity, 0, alloc);
        if (nbytes > 0u) std::memcpy(draken_validity, null_bitmap, nbytes);
    }

    PyObject* result = draken_vector_own_raw(
        draken_bitmap, draken_validity, num_rows, DRAKEN_BOOL);
    if (!result) {
        draken_free(draken_bitmap);
        if (draken_validity) draken_free(draken_validity);
        return NULL;  // exception already set by draken_vector_own_raw
    }
    return result;
}

}  // extern "C"
