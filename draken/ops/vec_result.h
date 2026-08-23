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
    uint16_t          vec_dimension    = 0u;      // VECTOR_FP16 result width; 0 = no
                                                  // descriptor. VECTOR_FP16 REQUIRES one
                                                  // (vecresult_to_owner rejects a fp16
                                                  // result without it), so a producer of
                                                  // that type must set this — the
                                                  // dimension lives on the VectorOwner's
                                                  // LogicalType, never on DrakenVector.
    // On error (data == nullptr), points at the SAME thread's error_handling.cpp
    // thread_local message buffer that draken_error_sentinel[_fmt] just formatted
    // into. NOT a caller-owned string: valid only until the next kernel call on
    // this thread. Read/copy it before that — never call draken_get_error_message()
    // to re-fetch it, since error_handling.cpp is compiled into more than one
    // extension (each with its own private thread_local buffer) and a caller that
    // doesn't already hold this VecResult may bind to a DIFFERENT copy than the one
    // the failing kernel actually wrote (silently returning an empty message). This
    // field is the explicit, ABI-carried fix for that — nullptr on success.
    const char*       error_msg        = nullptr;
    // --- ARRAY results ---
    // OWNED child element VecResult; non-null ONLY when type == DRAKEN_ARRAY.
    //
    // An ARRAY vector's elements do not live in `data` — `data` holds only the
    // int32_t offsets[length+1]. The elements hang off VectorOwner::child_owner
    // (vector_owner.h), which no DrakenVector/VecResult field could previously
    // reach, so an ARRAY was not an expressible kernel RESULT at all. This is
    // the mirror of that ownership edge: vecresult_to_owner consumes it
    // recursively into child_owner, so freeing the parent frees the subtree.
    //
    // Allocated with `new VecResult` and consumed (deleted) by vecresult_to_owner.
    // Nesting is arbitrary-depth (ARRAY<ARRAY<T>>) — the child may itself carry a
    // child. nullptr for every non-ARRAY result, which is why every existing
    // producer stays correct without touching this field.
    VecResult*        child            = nullptr;
    // Error CLASSIFICATION, meaningful only on an error sentinel (data == nullptr).
    // 0 = an internal fault (a null operand, a type the kernel does not accept, a
    // failed allocation) — the engine frames those with the operator name and the
    // failing opcode, because the reader cannot act on them and we need the
    // machine handle. 1 = a DATA error: the values themselves are the problem
    // (a string that is not a number, a value outside the target's range), the
    // message is complete, user-presentable text, and the engine raises it
    // verbatim as opteryx DataError with no engine framing at all. Set ONLY by
    // draken_data_error_sentinel[_fmt]; every other producer leaves it 0.
    uint8_t           data_error       = 0u;
};
