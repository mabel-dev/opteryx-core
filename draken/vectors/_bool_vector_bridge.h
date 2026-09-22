#pragma once
// draken/vectors/_bool_vector_bridge.h — CPython bridge for BoolVector construction.
//
// This header is Python-facing BY DESIGN and is the reason it lives here rather
// than in draken/core/. core/bitmap_ops.h is the shared, Python-free bitmap
// surface (simd_popcount, c_*_bitmap, draken_vm_bool_*); every C++ consumer of
// those ops must be able to compile without CPython headers present, which is
// what CLAUDE.md §2/§5 ("Draken must be able to execute without Python")
// requires. bool_vector_from_bits returns a PyObject* and therefore belongs in
// the shim/bridge layer next to _bool_vector_shim.pyx, not in core.
#include <Python.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Create a BoolVector from raw bitmap buffers.
 *
 * Parameters:
 *   bitmap       : uint8_t* pointing at the bit array (nbytes long)
 *   null_bitmap  : NULL if no nulls; otherwise uint8_t* validity bitmap
 *   num_rows     : logical row count
 *
 * Returns a new Python BoolVector object. On failure, returns NULL with
 * a Python exception set. The input bitmaps are COPIED — the caller retains
 * ownership of them (see the implementation comment for why).
 */
PyObject* bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t num_rows);

#ifdef __cplusplus
}
#endif
