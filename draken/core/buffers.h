#pragma once
#include <stdint.h>
#include <stddef.h>
#include "string_slot.h"
// =============================================================================
// Draken unified ABI — single source of truth (CLAUDE.md §11).
//
// This header is FROZEN for the duration of the C++-first rebuild. ~95 compiled
// `cimport draken.core.buffers` sites bind `DrakenVector`'s layout at compile
// time, so the layout must stay byte-identical (40 bytes on LP64). ABI drift (a
// silent field reorder or enum renumber) segfaults consumers instead of failing
// the build, so the layout is pinned by the static_asserts at the bottom of this
// file.
//
// Logical-type descriptor (06) and value statistics (05) are deliberately
// out-of-band, keyed by column — NOT fields on these structs. Adding either
// breaks the 40-byte freeze and the cimport ABI. Do not add them here.
// =============================================================================

// Portable compile-time assertion (C11 / C++11 / older-toolchain fallback).
#if defined(__cplusplus)
  #define DRAKEN_STATIC_ASSERT(cond, msg) static_assert(cond, msg)
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
  #define DRAKEN_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)
#else
  #define DRAKEN_STATIC_ASSERT_CAT_(a, b) a##b
  #define DRAKEN_STATIC_ASSERT_CAT(a, b) DRAKEN_STATIC_ASSERT_CAT_(a, b)
  #define DRAKEN_STATIC_ASSERT(cond, msg) \
      typedef char DRAKEN_STATIC_ASSERT_CAT(draken_static_assert_, __LINE__)[(cond) ? 1 : -1]
#endif

typedef enum {
    // Integer types: 1–19
    DRAKEN_INT8           = 1,
    DRAKEN_INT16          = 2,
    DRAKEN_INT32          = 3,
    DRAKEN_INT64          = 4,
    DRAKEN_DECIMAL        = 5,  // logical DECIMAL(p≤18,s); physical int64 unscaled value

    // Floating-point types: 20–29
    DRAKEN_FLOAT32        = 20,
    DRAKEN_FLOAT64        = 21,

    // Temporal types: 30–49
    DRAKEN_DATE32         = 30,
    DRAKEN_TIMESTAMP64    = 40,
    DRAKEN_TIME32         = 41,
    DRAKEN_TIME64         = 42,
    DRAKEN_INTERVAL       = 43,

    // Boolean: 50
    DRAKEN_BOOL           = 50,

    // String-like: 60–79
    DRAKEN_VARCHAR        = 60,  // default; ASCII semantics; byte-length ops
    // 61: reserved — permanently retired by E.30c (was DRAKEN_DICTIONARY, a shape-as-type violation; see CLAUDE.md §11)
    // 62: reserved — permanently retired by E.30c (was DRAKEN_CONSTANT,   a shape-as-type violation; see CLAUDE.md §11)
    DRAKEN_NVARCHAR       = 63,  // opt-in UTF-8; codepoint-length ops; Unicode-aware ops (future)
    DRAKEN_VARBINARY      = 64,  // opaque bytes; byte-length ops; character ops throw
    DRAKEN_VARIANT        = 65,  // polymorphic JSON value; German-string storage holds JSON text; renders to Python as str

    // Complex types: 80–99
    DRAKEN_ARRAY          = 80,

    // Catch-all
    DRAKEN_NON_NATIVE     = 100,  // Unoptimized or fallback-wrapped Arrow types

    // D.11 — new types (added at the end; do NOT renumber anything above)
    DRAKEN_NULL           = 101,  // Self-describing null: type==NULL ⟹ every row null; no data, no validity.
    DRAKEN_VECTOR_FP16    = 102,  // fp16 embedding vector; dimension via mandatory logical descriptor.

    // Decimal "do both" (doc 06): DRAKEN_DECIMAL (5) is the int64-backed fast path
    // (logical DECIMAL p≤18); DRAKEN_DECIMAL128 is the int128-backed correct-but-scalar
    // tier (logical DECIMAL p≤38). Physical `data` is a 16-byte __int128 unscaled value;
    // scale/precision live in the mandatory logical descriptor. No native SIMD for int128.
    DRAKEN_DECIMAL128     = 103,

    // Unsigned integers (E33): same-width kernel parity with the signed family;
    // mixed signed/unsigned binary ops promote to the next-wider signed type
    // (or DRAKEN_DECIMAL128 for UINT64+INT64). Appended at the tail, same as
    // DECIMAL128/VECTOR_FP16/NULL above — do NOT relocate into 1-19.
    DRAKEN_UINT8          = 104,
    DRAKEN_UINT16         = 105,
    DRAKEN_UINT32         = 106,
    DRAKEN_UINT64         = 107,
} DrakenType;

typedef struct {
    void* data;               // int64_t*, double*, etc.
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;
    size_t itemsize;
    DrakenType type;
} DrakenFixedBuffer;

typedef struct {
    uint8_t* data;            // UTF-8 bytes
    uint32_t* offsets;        // [N+1] entries — unsigned: addresses up to 4 GB
                              // of accumulated bytes (matches the German-string
                              // arena's own uint32 offset cap).
    uint8_t* null_bitmap;     // optional
    size_t length;
    DrakenType type;
} DrakenVarBuffer;

typedef struct {
    uint8_t* data;
    int32_t length;
} DrakenConstantStringPayload;

// German-string storage. Replaces DrakenVarBuffer for string values: an array
// of 16-byte DrakenStringSlot slots (length + inline-12 OR length + prefix + arena
// offset) plus a byte arena for long-form payloads (> 12 bytes). Used as the
// `data` payload of a string DrakenVector under the unified format.
//
// Lifetime: slots and arena are both owned by this struct when owns_buffers
// is non-zero. arena_used tracks bytes consumed during construction; arena_cap
// is the allocation size. Slots whose length <= 12 do not reference the arena.
typedef struct {
    DrakenStringSlot* slots;       // [length] slot array
    uint8_t*      arena;       // long-form byte arena (may be NULL when all rows inline)
    size_t        length;      // number of slots
    size_t        arena_used;  // bytes consumed in arena
    size_t        arena_cap;   // arena allocation size
    uint8_t*      null_bitmap; // optional, 1 bit per row
    uint8_t       owns_buffers;// free slots/arena/null_bitmap on free?
    DrakenType    type;        // DRAKEN_VARCHAR | DRAKEN_NVARCHAR | DRAKEN_VARBINARY
} DrakenStringArena;

typedef struct {
    int32_t* offsets;         // [length + 1] entries
    void* values;             // pointer to another column's data (DrakenFixedColumn*, DrakenVarColumn*, etc.)
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;            // number of array entries (rows)
    DrakenType value_type;    // type of the child values
} DrakenArrayBuffer;

// Unified vector view — one shape, one access pattern.
//
// Access is always: data[selection[i]]  for i in [0, length).
//
// `selection` is never NULL. For dense vectors it points at the lazy-grown
// global identity permutation; for constant vectors at the lazy-grown global
// zero vector; for dict-encoded vectors at owned uint32 codes. The choice of
// pointer is owned by the C constructors in vector_alloc.h.
//
// Default posture: uniform data[selection[i]] — no shape discrimination.
// Shape-specialized fast paths (constant, dict) require explicit architect
// approval (CLAUDE.md §11). Approved exceptions: compare kernels
// (int64_compare.h, fixed_int_ops.h, string_compare.h, float_ops.h), predicate
// kernels (int64_predicates.h, fixed_int_ops.h, string_predicates.h,
// float_ops.h), and arithmetic kernels (int64_arithmetic.h — constant-operand
// folding + dict/constant-preserving scalar/unary ops). float_ops.h compare/
// between gained constant/dict/identity paths; approved 2026-06-11.
//
// Memory-layout hints (informational only — never used in hot loops):
//   former-dense    => selection points at draken_identity_sel,  data_length == length
//   former-constant => selection points at draken_zero_sel,      data_length == 1
//   former-dict     => selection is owned codes,                 data_length <  length
typedef struct {
    void*             data;        // typed payload (cast at Cython typed-wrapper level)
    const uint32_t*   selection;   // always valid; indices into data
    uint32_t          data_length; // size of the data array (physical value count).
                                   // Uniqueness is guaranteed only by the *compress*
                                   // builders; other dict constructors may admit
                                   // duplicates, so do NOT assume data values are
                                   // distinct (kernels read data[selection[i]] regardless).
    uint32_t          length;      // logical row count
    uint8_t*          validity;    // 1-bit-per-logical-row null mask; NULL = all valid
    DrakenType        type;
    uint8_t           flags;       // Category-A layout hints (below); 0 = "don't know". Lands in tail padding → sizeof unchanged.
} DrakenVector;

// Category-A layout hint bits (00_data_model.md). Default 0 = conservative
// "don't know" → uniform data[selection[i]] path. A bit may be set ONLY when
// certain; a missed update loses a fast-path (slower, still correct), never
// changes the answer. Containment: IDENTITY ⟹ PERMUTATION ⟹ data_length==length.
#define DRAKEN_SEL_IDENTITY     (1u << 0)  // selection[i] == i (true dense)
#define DRAKEN_SEL_PERMUTATION  (1u << 1)  // bijection, data_length == length
#define DRAKEN_DICT_KEYS_SORTED (1u << 2)  // dict `data` ascending by engine order:
                                           // code_a < code_b ⟹ data[code_a] ≤ data[code_b].
                                           // Lets range/eq predicates collapse to a code
                                           // interval. Pure hint; set ONLY when certain.
#define DRAKEN_DICT_CODES_DENSE (1u << 3)  // every code in [0,data_length) is referenced by
                                           // at least one VALID row (no dead entries). Set by
                                           // the compacting take/mask. With KEYS_SORTED this
                                           // means data[0] / data[data_length-1] ARE the column
                                           // min / max (the "ends" shortcut). Pure hint.
// bits 4..7 reserved for future layout hints

// Shape predicates — canonical tests for the encoding shapes.
// Use these instead of open-coding data_length comparisons at call sites.
//
// Partition by data_length vs length:
//   is_dense      data_length == length   — every row its own value
//   is_compressed data_length <  length   — value array smaller than row count;
//                                            per-unique-value work is possible
// Within compressed:
//   is_constant   data_length == 1        — one value broadcast to all rows
//   is_dict       1 < data_length < length — true dictionary with repeats
static inline int draken_is_dense(const DrakenVector* v) {
    return v->data_length == v->length;
}
static inline int draken_is_compressed(const DrakenVector* v) {
    return v->data_length < v->length;
}
static inline int draken_is_constant(const DrakenVector* v) {
    return v->data_length == 1;
}
static inline int draken_is_dict(const DrakenVector* v) {
    return v->data_length > 1 && v->data_length < v->length;
}
// True iff this is a dict whose `data` is known-ascending (DRAKEN_DICT_KEYS_SORTED).
// A false result never means "unsorted" — only "not known sorted": fall back to
// the uniform path, which is always correct.
static inline int draken_dict_is_sorted(const DrakenVector* v) {
    return draken_is_dict(v) && (v->flags & DRAKEN_DICT_KEYS_SORTED);
}
// True iff this is a compressed (dict/constant) vector whose value array is both
// ascending AND fully referenced by valid rows — so data[0] is the column min and
// data[data_length-1] the column max (no dead entries can bracket the live ones).
static inline int draken_dict_sorted_dense(const DrakenVector* v) {
    return draken_is_compressed(v)
        && (v->flags & DRAKEN_DICT_KEYS_SORTED)
        && (v->flags & DRAKEN_DICT_CODES_DENSE);
}

// Physical width (bytes) of one element of a fixed-width type's `data` array.
// Returns 0 for the non-fixed families (bool is bit-packed; string/variant use a
// string arena; array/fp16/null/non-native have no flat per-element width) — those
// are handled out-of-band by draken_vector_nbytes below. Values mirror
// concat_fixed_itemsize in draken_native.cpp; keep the two in step if a type's
// physical width ever changes. INTERVAL is 16 (== sizeof(DrakenIntervalSlot),
// pinned by a static_assert in interval_slot.h — not included here to keep this
// header C-compatible and dependency-light).
static inline size_t draken_type_fixed_itemsize(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:
        case DRAKEN_UINT8:       return 1u;
        case DRAKEN_INT16:
        case DRAKEN_UINT16:      return 2u;
        case DRAKEN_INT32:
        case DRAKEN_UINT32:
        case DRAKEN_FLOAT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:      return 4u;
        case DRAKEN_INT64:
        case DRAKEN_UINT64:
        case DRAKEN_FLOAT64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:
        case DRAKEN_DECIMAL:     return 8u;
        case DRAKEN_DECIMAL128:
        case DRAKEN_INTERVAL:    return 16u;
        default:                 return 0u;
    }
}

// Approximate in-memory footprint (bytes) of ONE vector's owned payload: the
// data buffer (dedup-aware — sized by data_length, the physical value count, so
// dict/constant vectors are not counted at their logical row count), the string
// arena for the string family, and the validity bitmap. Intended as an honest
// memory-pressure signal (e.g. a result-buffer flush threshold), replacing the
// old flat rows×8 guess that grossly undercounted variable-length strings.
//
// The `selection` array is deliberately EXCLUDED: for dense/constant vectors it
// is a shared global buffer (draken_identity_sel / draken_zero_sel — not owned by
// the vector), so counting it would both over-count shared memory and require
// shape discrimination. Dict codes (owned, length×4) are therefore under-counted;
// the data/arena terms dominate, so this stays a close, deliberately-conservative
// estimate rather than an exact allocator figure.
//
// KNOWN LIMITATION — DRAKEN_ARRAY: an array's child values hang off the owning
// VectorOwner, not off this DrakenVector, so they are unreachable from this
// pointer alone and are NOT counted (only the validity bitmap is). Array-typed
// result columns are therefore under-counted; precise array accounting needs a
// VectorOwner-based API and is out of scope here.
static inline size_t draken_vector_nbytes(const DrakenVector* v) {
    if (v == NULL) return 0u;
    size_t bytes = 0u;
    const size_t n = (size_t)v->data_length;  // physical value count (dedup-aware)
    switch (v->type) {
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY:
        case DRAKEN_VARIANT: {
            // `data` is a DrakenStringArena: a slot array (16 bytes/slot) plus a
            // byte arena for long-form (>12 byte) payloads. Both are O(1) header
            // reads — no per-string walk.
            const DrakenStringArena* sa = (const DrakenStringArena*)v->data;
            if (sa != NULL)
                bytes += sa->length * sizeof(DrakenStringSlot) + sa->arena_used;
            break;
        }
        case DRAKEN_BOOL:
            bytes += (n + 7u) >> 3;  // 1 bit per physical value
            break;
        default:
            // Fixed-width families; 0 for array/fp16/null/non-native (see notes).
            bytes += n * draken_type_fixed_itemsize(v->type);
            break;
    }
    if (v->validity != NULL)
        bytes += ((size_t)v->length + 7u) >> 3;  // 1 bit per logical row
    return bytes;
}

// =============================================================================
// ABI guard — frozen layout (CLAUDE.md §11, 09_delivery.md risk #1).
// sizeof alone won't catch a field reorder, and a renumbered enum is as fatal
// as a layout shift, so every shared field offset and a few tag values are
// pinned here.
// =============================================================================
DRAKEN_STATIC_ASSERT(sizeof(DrakenVector) == 40, "DrakenVector must be 40 bytes on LP64");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, data)        == 0,  "DrakenVector.data offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, selection)   == 8,  "DrakenVector.selection offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, data_length) == 16, "DrakenVector.data_length offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, length)      == 20, "DrakenVector.length offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, validity)    == 24, "DrakenVector.validity offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, type)        == 32, "DrakenVector.type offset drift");
DRAKEN_STATIC_ASSERT(offsetof(DrakenVector, flags)       == 36, "DrakenVector.flags must land in tail padding at offset 36");

// Pin the DrakenType underlying integer width and representative tag values.
// A renumber silently breaks the runtime dispatch key for every cimport site.
DRAKEN_STATIC_ASSERT(sizeof(DrakenType) == 4, "DrakenType underlying type must be 4 bytes");
DRAKEN_STATIC_ASSERT(DRAKEN_INT64  == 4,   "DrakenType tag renumbered: DRAKEN_INT64");
DRAKEN_STATIC_ASSERT(DRAKEN_BOOL   == 50,  "DrakenType tag renumbered: DRAKEN_BOOL");
DRAKEN_STATIC_ASSERT(DRAKEN_VARCHAR == 60, "DrakenType tag renumbered: DRAKEN_VARCHAR");
DRAKEN_STATIC_ASSERT(DRAKEN_NON_NATIVE == 100, "DrakenType tag renumbered: DRAKEN_NON_NATIVE");
DRAKEN_STATIC_ASSERT(DRAKEN_DECIMAL128 == 103, "DrakenType tag renumbered: DRAKEN_DECIMAL128");
DRAKEN_STATIC_ASSERT(DRAKEN_UINT8  == 104, "DrakenType tag renumbered: DRAKEN_UINT8");
DRAKEN_STATIC_ASSERT(DRAKEN_UINT64 == 107, "DrakenType tag renumbered: DRAKEN_UINT64");
