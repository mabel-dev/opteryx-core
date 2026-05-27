#pragma once
// draken/core/draken_bridge.h — Cython ↔ nanobind bridge surface.
//
// Functions bridging .pyx consumers to the nanobind VectorOwner handle.
// This is the only bridge surface.  Per-op specifics live in ops/*.h.
// Extension history: draken_vector_own_array added for E.16b (DRAKEN_ARRAY
// construction from raw C buffers; no Python intermediate).
//
// Lifetime contract for draken_vector_unwrap:
//   The returned pointer is BORROWED — it points inside the VectorOwner owned
//   by the Python `obj` handle. The caller MUST keep `obj` alive (hold a
//   Python reference) for as long as the pointer is in use. The pointer MUST
//   NOT be stored past the handle's lifetime.
//
// Ownership contract for draken_vector_own / draken_vector_own_raw:
//   Both CONSUME ownership of the data + validity buffers (draken_free'd by
//   the new Vector's destructor). The caller MUST NOT free those buffers after
//   calling these functions.
//
//   Allocator contract (important): any buffer that is transferred to a
//   draken_vector_own_* function MUST be allocated with the Draken allocator
//   (draken_malloc / draken_aligned_malloc) and will be freed with
//   draken_free by the Vector's destructor. Passing a libc-allocated buffer
//   (malloc / PyMem_Malloc / free) to any draken_vector_own_* function is
//   undefined behaviour and can corrupt the process heap or cause crashes.
//
//   If caller code cannot allocate via draken_malloc (for example when a
//   third-party C API or Cython typed-memoryview owns the buffer), the caller
//   MUST copy the data into a draken_malloc'd buffer before calling the
//   bridge (see draken/core/bitmap_ops.cpp::bool_vector_from_bits for a safe
//   example). Do NOT rely on implicit or accidental cross-allocator frees.

// USAGE: called from C++ nanobind glue (draken_native.cpp) ONLY.
//
// DO NOT declare these from .pyx via `cdef extern`. The pattern:
//   cdef extern from "core/draken_bridge.h":
//       const DrakenVector* draken_vector_unwrap(object vec)   ← BANNED
// puts `object` in .pyx — a CLAUDE.md §3 violation.
// nb::object is C++; it lives in nanobind glue, not Cython.
//
// Implementations live in draken/draken_native.cpp and are compiled into
// draken_native.so.

#include <Python.h>
#include <stdint.h>
#include "core/buffers.h"

#ifdef __cplusplus
#include "ops/vec_result.h"
extern "C" {
#endif

// draken_vector_unwrap — extract a borrowed DrakenVector* from a Python handle.
//
// obj must be an instance of draken.draken_native.Vector.
// Raises TypeError (never segfaults) if obj is not a Vector or is None.
// Returns a borrowed pointer valid ONLY while `obj` is kept alive.
const DrakenVector* draken_vector_unwrap(PyObject* obj);

// draken_array_child_unwrap — extract the child DrakenVector* of a DRAKEN_ARRAY Vector.
//
// obj must be an instance of draken.draken_native.Vector with type DRAKEN_ARRAY.
// Raises TypeError if obj is not a Vector, RuntimeError if type != DRAKEN_ARRAY or
// child is absent. Returns a borrowed pointer valid ONLY while `obj` is kept alive.
const DrakenVector* draken_array_child_unwrap(PyObject* obj);

// draken_vector_own_raw — wrap hand-allocated (draken_malloc) buffers in a new Vector.
//
// Creates a dense (identity-selection) Vector with `length` logical rows.
// data and validity must have been allocated with draken_malloc; ownership
// is transferred to the new Vector on success (draken_free'd on GC).
// validity may be NULL (all-valid normalization invariant).
// Returns a NEW reference to a Python Vector on success.
// Returns NULL with a Python exception set on allocation failure.
PyObject* draken_vector_own_raw(
    void* data, uint8_t* validity, uint32_t length, DrakenType type);

// draken_vector_own_dict_i64 — wrap hand-allocated dict-encoded int64 buffers in a new Vector.
//
// Creates a dict-encoded (selection = owned codes) int64 Vector.
// data:        draken_malloc'd int64_t[data_length] unique values (the dictionary).
// codes:       draken_malloc'd uint32_t[length] per-row codes.
// data_length: number of unique values.
// length:      logical row count.
// validity:    draken_malloc'd null bitmap (1-bit-per-row, Arrow convention), or NULL.
// All non-NULL buffers MUST be draken_malloc'd; ownership is transferred on call.
// Returns a NEW reference on success; NULL + exception on failure.
PyObject* draken_vector_own_dict_i64(
    void* data, uint32_t data_length,
    uint32_t* codes, uint32_t length,
    uint8_t* validity);

// draken_vector_own_string — wrap hand-allocated string buffers in a new string-family Vector.
//
// Canonical exit-point for C++ consumers that produce a new string column.
// All three buffers MUST have been allocated with draken_malloc. Ownership of all three
// is transferred unconditionally on call entry — the caller MUST NOT free them after
// calling this function, whether the call succeeds or fails.
//
// Parameters:
//   slots     — DrakenStringSlot[length] slot array. Consumer is responsible for
//               populating each slot in the correct format (see core/string_slot.h):
//                 short (len <= 12): str_init_inline / draken_build_string_slot.
//                 long  (len > 12):  str_init_extern / draken_build_string_slot +
//                                    bytes written to arena at the matching arena_offset.
//               hash32 MUST be the lower 32 bits of XXH3_64bits(bytes, len).
//               Null rows: slot must be zeroed (str_init_null); set validity bit to 0.
//   arena     — arena bytes backing long-form slots. May be NULL when all strings are
//               inline (arena_len == 0 implies arena may be NULL).
//   arena_len — number of valid bytes in arena (may be 0).
//   validity  — 1-bit-per-logical-row null bitmap (Arrow convention: bit set = valid).
//               May be NULL if all rows are valid (normalization invariant).
//   length    — logical row count.
//   type      — must be DRAKEN_VARCHAR, DRAKEN_NVARCHAR, or DRAKEN_VARBINARY.
//               Raises ValueError if any other type is passed.
//
// Slot format note: consumers MUST use draken_build_string_slot (or str_init_inline /
// str_init_extern) to populate slots. The prefix must be big-endian, hash32 must be
// XXH3-lower-32. Malformed slots produce incorrect equality/hash results downstream.
//
// Storage is identical across VARCHAR/NVARCHAR/VARBINARY (slot+arena layout). The type
// tag drives op semantics (e.g. LENGTH returns codepoints for NVARCHAR, bytes for the
// other two). No per-type storage specialisation.
//
// Creates a dense (identity-selection) Vector with the given type.
// flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION.
//
// Returns a NEW reference to a Python Vector on success.
// Returns NULL with a Python exception set on any failure.
PyObject* draken_vector_own_string(
    DrakenStringSlot* slots,
    uint8_t*          arena,
    size_t            arena_len,
    uint8_t*          validity,
    uint32_t          length,
    DrakenType        type);   // DRAKEN_VARCHAR | DRAKEN_NVARCHAR | DRAKEN_VARBINARY

// draken_vector_own_array — wrap hand-allocated buffers in a new DRAKEN_ARRAY[string] Vector.
//
// Constructs a DRAKEN_ARRAY whose child is a string-family Vector (VARCHAR, NVARCHAR,
// or VARBINARY).  Ownership of ALL five caller buffers is transferred unconditionally
// on call entry — the caller MUST NOT free them after calling this function.
//
// Parameters:
//   parent_offsets   — int32_t[length+1]: child index range for each parent row.
//                      parent_offsets[0] must be 0.  Allocated with draken_malloc.
//   child_slots      — DrakenStringSlot[child_length]: one slot per child element.
//                      Each slot populated with draken_build_string_slot (or zeroed for null).
//                      May be NULL only when child_length == 0.
//   child_arena      — arena bytes backing long-form child slots.  May be NULL when
//                      child_arena_len == 0 (all child strings are inline).
//   child_arena_len  — number of valid bytes in child_arena (may be 0).
//   child_length     — total number of child elements across all parent rows.
//   child_type       — DRAKEN_VARCHAR, DRAKEN_NVARCHAR, or DRAKEN_VARBINARY; ValueError otherwise.
//   parent_validity  — 1-bit-per-row null bitmap for the parent (Arrow convention: bit set = valid).
//                      May be NULL if all parent rows are valid.
//   length           — parent logical row count.
//
// Child elements are assumed fully valid (no per-element null bitmap).
// Parent null rows must have parent_offsets[i] == parent_offsets[i+1] (zero-length slice).
//
// Returns a NEW reference to a Python Vector on success.
// Returns NULL with a Python exception set on failure.
PyObject* draken_vector_own_array(
    int32_t*          parent_offsets,
    DrakenStringSlot* child_slots,
    uint8_t*          child_arena,
    size_t            child_arena_len,
    uint32_t          child_length,
    DrakenType        child_type,
    uint8_t*          parent_validity,
    uint32_t          length);

// draken_vector_own_timestamp — wrap a hand-allocated int64 buffer as a DRAKEN_TIMESTAMP64 Vector.
//
// Mandatory LogicalType descriptor is constructed from unit_str:
//   "s"    → SECONDS,  "ms"   → MILLISECONDS,
//   "us"   → MICROSECONDS (the common default),
//   "ns"   → NANOSECONDS,
//   "days" → data is scaled to MICROSECONDS (×86_400_000_000) before wrapping;
//            descriptor is set to MICROSECONDS.
// Any other unit_str raises ValueError.
//
// data must be a draken_malloc'd int64_t[length] buffer; validity may be NULL (all-valid).
// Ownership of both buffers is transferred unconditionally on call entry.
// For "days" inputs a new data buffer is allocated; the original is freed on success or failure.
//
// Returns a NEW reference to a Python Vector on success.
// Returns NULL with a Python exception set on failure.
PyObject* draken_vector_own_timestamp(
    void* data, uint8_t* validity, uint32_t length, const char* unit_str);

#ifdef __cplusplus
}  // extern "C"

// draken_vector_own — wrap a VecResult op result in a new Python Vector handle.
//
// C++ only — not callable from Cython C code. Cython kernels that need to
// return a new Vector from a VecResult should convert to raw buffers and use
// draken_vector_own_raw, or call draken_vector_own from C++ companion code.
//
// MOVES ownership from res:
//   res.data and res.validity are consumed (draken_free'd by new Vector on GC).
//   If res.owns_selection is true, res.selection is also draken_free'd.
// Returns a NEW reference to a Python Vector on success.
// Returns NULL with a Python exception set on failure.
PyObject* draken_vector_own(VecResult res);

#endif  // __cplusplus
