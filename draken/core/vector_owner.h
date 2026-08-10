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
    // data_source — non-null ONLY when `vec.data` is BORROWED from another
    // VectorOwner's payload instead of owned here, in which case `data_buf` (and,
    // for strings, `arena_buf`) stay null and this reference is what keeps those
    // bytes alive. The one producer today is a join emitting its build half as a
    // DICT over the consolidated build payload: many output morsels share one
    // physical block, each owning only its own `codes_buf`.
    //
    // Sharing the payload also shares its LIFETIME: the block outlives the operator
    // that produced it and dies with the last derived column, which is exactly why
    // this is a shared_ptr and not a raw pointer into sink state that the pipeline
    // may tear down first.
    //
    // NOT a second ownership path for the same bytes — data_buf and data_source are
    // mutually exclusive. Setting both would double-free.
    std::shared_ptr<VectorOwner> data_source;

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

// In-memory footprint (bytes) of a VectorOwner's payload, INCLUDING (for
// DRAKEN_ARRAY) the owned child subtree -- unlike draken_vector_nbytes alone,
// which only ever sees the bare DrakenVector and so cannot reach child_owner
// (see the KNOWN LIMITATION note on draken_vector_nbytes in buffers.h).
// Recurses through nested arrays (ARRAY<ARRAY<...>>); each level counts its
// own offsets/data/validity via draken_vector_nbytes, plus whatever its own
// child_owner contributes.
static inline size_t draken_vector_owner_nbytes(const VectorOwner* owner) noexcept {
    size_t total = 0u;
    while (owner != nullptr) {
        if (owner->data_source) {
            // BORROWED payload (see data_source): the data/arena bytes belong to
            // another owner and are counted THERE. Counting them here too would
            // multiply one shared block by the number of morsels sharing it — for a
            // fan-out join that is the whole build side counted once per output
            // morsel, which is precisely the over-report this field exists to avoid.
            // What IS owned here is the codes array and the per-row validity mask.
            total += static_cast<size_t>(owner->vec.length) * sizeof(uint32_t);
            if (owner->vec.validity != nullptr)
                total += (static_cast<size_t>(owner->vec.length) + 7u) / 8u;
        } else {
            total += draken_vector_nbytes(&owner->vec);
        }
        owner = owner->child_owner.get();
    }
    return total;
}
