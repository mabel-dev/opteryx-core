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
//
// Phase 9c extensions (validity_embedded, ts_unit) carry result kinds the
// original two-buffer ownership model could not express:
//   - String columns store their null bitmap *inside* the single data block
//     (see draken_vector_own_string), so validity must NOT be freed as a
//     second buffer — validity_embedded = 1 signals that.
//   - TIMESTAMP64 needs a unit descriptor that lives on the VectorOwner's
//     LogicalType, not on DrakenVector — ts_unit carries it across the ABI.
// Both fields are appended and default-initialised so existing producers
// (which set fields individually and never touch these) get safe defaults.

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
    // --- appended; default-initialised so unaware producers stay correct ---
    uint8_t           validity_embedded = 0u;     // 1 = `validity` points INSIDE the
                                                  // `data` block; consumer must NOT free
                                                  // it separately (string-family output).
    uint8_t           ts_unit          = 0xFFu;   // TimestampUnit (0..3) for TIMESTAMP64
                                                  // output; 0xFF = no descriptor.
    uint8_t           dec_precision    = 0u;      // DECIMAL/DECIMAL128 result precision;
                                                  // 0 = no descriptor (carries scale too).
    uint8_t           dec_scale        = 0u;      // DECIMAL/DECIMAL128 result scale.
};
