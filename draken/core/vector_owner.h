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
//   arena_buf    — owns the long-string byte arena for DRAKEN_VARCHAR/
//                  NVARCHAR/VARBINARY vectors whose slots are not all inline
//                  (nullptr when every slot is inline, or for non-string
//                  types). Slots point into this arena via a byte OFFSET
//                  (str_data(slot, arena_buf.get())), never an absolute
//                  pointer — see draken/core/string_slot.h. This field is
//                  purely additive (default nullptr): every existing
//                  consumer that never sets it keeps working unchanged.
//   logical_type — BORROWED pointer into the global LogicalType registry.
//                  Non-null for parameterized physical types (TIMESTAMP64, etc.).
//                  nullptr for simple scalar types (INT64, FLOAT64, BOOL, …).
//                  MANDATORY for DRAKEN_TIMESTAMP64: using a timestamp vector
//                  with logical_type==nullptr is a hard error (fail loud).
//   child_owner  — Non-null only for DRAKEN_ARRAY. Owns the child DrakenVector
//                  (and transitively its subtree). Destructor chains recursively,
//                  so freeing the parent frees the whole subtree. No back-pointers.
//   keyhash_buf  — E37 carried key-hash. Non-null only when a producer (a string
//                  decoder) has pre-computed the per-data-element hash SEED
//                  (str_hash_seed) for this vector: one uint64_t per data-element
//                  (data_length entries), addressed as keyhash_buf[selection[i]]
//                  exactly like data. Lets the GROUP BY / JOIN / DISTINCT key hash
//                  skip re-seeding from the arena (see draken/docs/design/
//                  E37_carried_key_hash.md). Presence == validity: any op that does
//                  not explicitly propagate it yields nullptr, and the consumer
//                  falls back to recomputing str_hash_seed. Purely additive
//                  (default nullptr): every consumer that ignores it is unchanged.
//
// RAII: all unique_ptrs call draken_free via DrakenFree on destruction.
// No owns_* flags anywhere — the unique_ptr itself IS the ownership record.
struct VectorOwner {
    DrakenVector         vec;
    OwnedBuffer<void>    data_buf;
    OwnedBuffer<uint8_t> validity_buf;
    OwnedBuffer<void>    codes_buf;   // non-null only for dict shapes
    OwnedBuffer<uint8_t> arena_buf;   // non-null only for non-inline VARCHAR/NVARCHAR/VARBINARY
    const LogicalType*   logical_type = nullptr;  // borrowed; registry-interned
    std::unique_ptr<VectorOwner> child_owner;     // non-null only for DRAKEN_ARRAY
    OwnedBuffer<uint64_t> keyhash_buf;            // E37: non-null only when seed pre-computed

    VectorOwner(DrakenVector v,
                OwnedBuffer<void>    d,
                OwnedBuffer<uint8_t> val,
                OwnedBuffer<void>    codes = OwnedBuffer<void>(nullptr),
                OwnedBuffer<uint8_t> arena = OwnedBuffer<uint8_t>(nullptr)) noexcept
        : vec(v), data_buf(std::move(d)), validity_buf(std::move(val)),
          codes_buf(std::move(codes)), arena_buf(std::move(arena)),
          logical_type(nullptr), child_owner(nullptr) {}

    VectorOwner(const VectorOwner&)            = delete;
    VectorOwner& operator=(const VectorOwner&) = delete;
    VectorOwner(VectorOwner&&)                 = default;
    VectorOwner& operator=(VectorOwner&&)      = default;
    ~VectorOwner()                             = default;
};
