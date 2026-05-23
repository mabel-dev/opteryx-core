#pragma once
// draken/ops/vec_result.h — Owned result of a vector-producing operation.
//
// VecResult carries ownership of the two heap-allocated buffers (data + validity)
// out of a kernel and into the nanobind binding layer, which wraps them in
// OwnedBuffer<> (unique_ptr + DrakenFree) to get RAII.
//
// Ownership contract:
//   data     — allocated via draken_malloc; caller must draken_free unless null.
//   validity — allocated via draken_malloc; caller must draken_free unless null.
//              nullptr means all-valid (normalization invariant: §00_data_model).
//   selection — NON-OWNING pointer to a global shared identity/zero array;
//              never draken_free'd.
//
// All fields must be set by the returning kernel. The caller converts this to
// a VectorOwner immediately and does not hold VecResult beyond that point.

#include <stdint.h>
#include "core/buffers.h"

struct VecResult {
    void*             data;          // owned (draken_malloc); at least 1 byte allocated
    uint8_t*          validity;      // owned or nullptr (all-valid)
    const uint32_t*   selection;     // points to global identity/zero OR owned codes
    bool              owns_selection;// if true: draken_free((void*)selection) on consume
    uint32_t          data_length;   // # unique/physical values in data
    uint32_t          length;        // logical row count
    DrakenType        type;
    uint8_t           flags;         // DRAKEN_SEL_IDENTITY / DRAKEN_SEL_PERMUTATION
};
