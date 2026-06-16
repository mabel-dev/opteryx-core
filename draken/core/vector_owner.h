#pragma once
// draken/core/vector_owner.h — VectorOwner ownership primitives (doc 01).
//
// Extracted from draken_native.cpp so C++ consumers OUTSIDE the nanobind module
// (the native scan path, the C++-first CxxMorsel — see docs/M4_CPP_MORSEL_DESIGN.md)
// can build and hold VectorOwners. The nanobind "Vector" binding wraps this type;
// the layout and semantics are unchanged by the extraction.

#include <cstdint>
#include <memory>

#include "core/buffers.h"   // DrakenVector
#include "core/alloc.h"     // draken_free

// Borrowed pointer member only — forward declaration suffices (definition in
// draken/logical_type.h).
struct LogicalType;

// ---------------------------------------------------------------------------
// Ownership primitives (doc 01)
// ---------------------------------------------------------------------------

// Stateless deleter. Empty type → unique_ptr<T, DrakenFree> stays one word (EBO).
struct DrakenFree {
    void operator()(void* p) const noexcept { draken_free(p); }
};

template <typename T>
using OwnedBuffer = std::unique_ptr<T, DrakenFree>;

// VectorOwner: the frozen 40-byte DrakenVector ABI struct plus owned buffers.
//
// Ownership map:
//   data_buf     — owns vec.data  (typed payload; draken_free on destruct)
//                  For DRAKEN_ARRAY: owns int32_t offsets[length+1].
//   validity_buf — owns vec.validity (null bitmap or empty → nullptr if all-valid)
//   codes_buf    — owns vec.selection for dict-encoded vectors (nullptr for
//                  identity/zero selections which point at shared globals)
//   logical_type — BORROWED pointer into the global LogicalType registry.
//                  Non-null for parameterized physical types (TIMESTAMP64, etc.).
//                  nullptr for simple scalar types (INT64, FLOAT64, BOOL, …).
//                  MANDATORY for DRAKEN_TIMESTAMP64: using a timestamp vector
//                  with logical_type==nullptr is a hard error (fail loud).
//   child_owner  — Non-null only for DRAKEN_ARRAY. Owns the child DrakenVector
//                  (and transitively its subtree). Destructor chains recursively,
//                  so freeing the parent frees the whole subtree. No back-pointers.
//
// RAII: all unique_ptrs call draken_free via DrakenFree on destruction.
// No owns_* flags anywhere — the unique_ptr itself IS the ownership record.
struct VectorOwner {
    DrakenVector         vec;
    OwnedBuffer<void>    data_buf;
    OwnedBuffer<uint8_t> validity_buf;
    OwnedBuffer<void>    codes_buf;   // non-null only for dict shapes
    const LogicalType*   logical_type = nullptr;  // borrowed; registry-interned
    std::unique_ptr<VectorOwner> child_owner;     // non-null only for DRAKEN_ARRAY

    VectorOwner(DrakenVector v,
                OwnedBuffer<void>    d,
                OwnedBuffer<uint8_t> val,
                OwnedBuffer<void>    codes = OwnedBuffer<void>(nullptr)) noexcept
        : vec(v), data_buf(std::move(d)), validity_buf(std::move(val)),
          codes_buf(std::move(codes)), logical_type(nullptr), child_owner(nullptr) {}

    VectorOwner(const VectorOwner&)            = delete;
    VectorOwner& operator=(const VectorOwner&) = delete;
    VectorOwner(VectorOwner&&)                 = default;
    VectorOwner& operator=(VectorOwner&&)      = default;
    ~VectorOwner()                             = default;
};
