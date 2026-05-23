#pragma once
#include <stdint.h>
#include "core/buffers.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Lazy-grown global identity permutation. selection[i] == i for all i.
 * Backing buffer grows under a process-wide mutex; reads after the pointer
 * has been obtained are lock-free. Buffer is never freed (intentional —
 * other threads may still hold pointers; growth events are O(log N) in
 * the lifetime of the process).
 *
 * Each draken extension `.so` has its own copy of these globals. Owned-vs-
 * shared discrimination is tracked by the Cython typed wrapper class, not
 * by pointer comparison against these symbols. */
const uint32_t* draken_identity_sel(uint32_t length);

/* Lazy-grown global zero vector. selection[i] == 0 for all i. */
const uint32_t* draken_zero_sel(uint32_t length);

/* Lazy-grown global all-zero validity bitmap. bit[i] == 0 (null) for all i.
 * Buffer size is ceil(length/8) bytes, padded to a multiple of 8 for SIMD safety. */
const uint8_t* draken_zero_validity(uint32_t length);

/* The only sanctioned way to populate a DrakenVector.
 *
 * `validity` may be NULL (all valid). Ownership of `data` and `validity` is
 * NOT transferred — the caller continues to own those buffers. The selection
 * is taken from the global identity (no allocation, no ownership). */
DrakenVector draken_vector_from_dense(
    void* data, uint32_t length, DrakenType type, uint8_t* validity);

/* Constant: one row's worth of `data` (data_length == 1) broadcast to `length` rows.
 * Selection is the global zero vector (no allocation, no ownership). */
DrakenVector draken_vector_from_constant(
    void* data, uint32_t length, DrakenType type, uint8_t* validity);

/* Dictionary: `codes` is an array of length `length` containing uint32 indices
 * into `data`, where `data` holds `data_length` unique values.
 *
 * Ownership of `codes` is transferred to the vector. The caller's Cython
 * typed wrapper must record `_owns_selection = True` and `free` the codes
 * when the wrapper is deallocated. */
DrakenVector draken_vector_from_dict(
    void* data, uint32_t data_length,
    const uint32_t* codes, uint32_t length,
    DrakenType type, uint8_t* validity);

#ifdef __cplusplus
}
#endif
