#pragma once
// String slot — 16-byte fixed-width slot, two variants discriminated by length.
//
// Short (len ≤ 12):
//   [ uint32_t length ][ 12 inline bytes ]
//   Bytes beyond `length` MUST be zero so that slot comparison is deterministic.
//
// Long  (len > 12):
//   [ uint32_t length ][ uint32_t prefix ][ uint32_t hash32 ][ uint32_t arena_offset ]
//
//   prefix       = first 4 payload bytes stored as a big-endian uint32_t so that
//                  unsigned-integer comparison of prefix values gives lexicographic
//                  order: prefix_a < prefix_b ↔ first_4_bytes(a) < first_4_bytes(b).
//   hash32       = full-content XXH3 32-bit hash (lower 32 bits of XXH3_64bits);
//                  equal values → identical hash. Used by higher-level kernels for
//                  fast inequality rejection.
//   arena_offset = u32 byte offset into the enclosing arena; max 4 GB per vector.
//                  Overflow is a hard error — never wraps silently.
//
// Both forms expose the first 4 payload bytes at bytes 4..7 so the "lp_word"
// (bytes 0..7 = length + first-4-bytes) can short-circuit equality checks.
// For inline strings the first-4-bytes are raw (memcmp-correct).
// For long strings the first-4-bytes are stored big-endian (uint32-compare-correct).
//
// Access pattern (uniform): str_data(slot, arena_base) → const uint8_t*
// Length:                   str_length(slot) → uint32_t
// Slot size:                always 16 bytes.

#include <stdint.h>
#include <string.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STR_INLINE_MAX 12

#pragma pack(push, 1)
typedef union {
    struct {
        uint32_t length;
        uint8_t  data[STR_INLINE_MAX];
    } inl;
    struct {
        uint32_t length;
        uint32_t prefix;        // first 4 bytes as big-endian uint32 for lex comparison
        uint32_t hash32;        // full-content XXH3 hash (lower 32 bits)
        uint32_t arena_offset;  // byte offset into the enclosing arena (u32, max 4 GB)
    } ext;
    // Raw 16-byte view for bulk equality checks.
    struct {
        uint64_t lo;  // bytes 0-7: length || first-4-bytes
        uint64_t hi;  // bytes 8-15: inline data[4..11] OR hash32||arena_offset (long)
    } raw;
} DrakenStringSlot;
#pragma pack(pop)

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

static inline uint32_t str_length(const DrakenStringSlot* s) { return s->inl.length; }
static inline int      str_is_inline(const DrakenStringSlot* s) { return s->inl.length <= STR_INLINE_MAX; }

// ---------------------------------------------------------------------------
// Builder-side initializers
// ---------------------------------------------------------------------------

static inline void str_init_null(DrakenStringSlot* s) {
    // Deterministic zero state for null rows. Length=0, all bytes zero.
    memset(s, 0, sizeof(DrakenStringSlot));
}

static inline void str_init_inline(DrakenStringSlot* s, const uint8_t* src, uint32_t length) {
    // Precondition: length <= STR_INLINE_MAX. Zero-fill first so trailing bytes are
    // deterministically zero (required for raw.hi equality shortcut to work correctly).
    memset(s, 0, sizeof(DrakenStringSlot));
    s->inl.length = length;
    if (length > 0)
        memcpy(s->inl.data, src, length);
}

static inline void str_init_extern(DrakenStringSlot* s, const uint8_t* src,
                                   uint32_t length, uint32_t hash32, uint32_t arena_offset) {
    // Precondition: length > STR_INLINE_MAX, src has >= length bytes already written
    // to the arena at arena_offset. All four fields are set explicitly — no memset needed.
    s->ext.length = length;
    // Prefix: first 4 bytes stored big-endian so uint32 comparison gives lex order.
    s->ext.prefix = ((uint32_t)src[0] << 24) | ((uint32_t)src[1] << 16)
                  | ((uint32_t)src[2] << 8)  | ((uint32_t)src[3]);
    s->ext.hash32 = hash32;
    s->ext.arena_offset = arena_offset;
}

// ---------------------------------------------------------------------------
// Read helpers
// ---------------------------------------------------------------------------

// Payload pointer. For inline slots returns the inline bytes; for long slots
// returns arena_base + arena_offset. arena_base is not consulted for inline slots
// and may be NULL for callers that guarantee they only see inline slots.
static inline const uint8_t* str_data(const DrakenStringSlot* s, const uint8_t* arena_base) {
    return str_is_inline(s) ? s->inl.data : (arena_base + s->ext.arena_offset);
}

// Prefix as a big-endian uint32_t suitable for lex comparison: prefix_a < prefix_b
// ↔ first-4-bytes(a) < first-4-bytes(b) lexicographically. Works for both forms.
static inline uint32_t str_prefix4(const DrakenStringSlot* s) {
    if (!str_is_inline(s))
        return s->ext.prefix;  // already big-endian
    // Inline: construct big-endian from raw bytes (zero-padded if len < 4).
    const uint8_t* p = s->inl.data;
    const uint32_t n = s->inl.length;
    uint32_t r = 0;
    if (n > 0) r |= (uint32_t)p[0] << 24;
    if (n > 1) r |= (uint32_t)p[1] << 16;
    if (n > 2) r |= (uint32_t)p[2] << 8;
    if (n > 3) r |= (uint32_t)p[3];
    return r;
}

// lp_word: bytes 0..7 = length || first-4-bytes. Equal lp_words imply equal
// length and equal first-4-bytes. Used for fast equality rejection.
static inline uint64_t gs_lp_word(const DrakenStringSlot* s) { return s->raw.lo; }

// draken_build_string_slot — convenience builder for consumers producing new string Vectors.
//
// For short (len <= STR_INLINE_MAX): initialises an inline slot; arena_offset ignored.
// For long  (len >  STR_INLINE_MAX): initialises an extern slot with XXH3 hash;
//   bytes[0..len) MUST already be written to the enclosing arena at arena_offset
//   before this is called. The hash is computed from bytes, not from the arena copy.
//
// Caller is responsible for writing bytes into the arena and tracking arena_offset.
// This helper does NOT write to the arena — that is the caller's job.
static inline void draken_build_string_slot(
    DrakenStringSlot* slot, const uint8_t* bytes, uint32_t len, uint32_t arena_offset)
{
    if (len <= STR_INLINE_MAX) {
        str_init_inline(slot, bytes, len);
    } else {
        str_init_extern(slot, bytes, len,
                        (uint32_t)XXH3_64bits(bytes, len), arena_offset);
    }
}

// ---------------------------------------------------------------------------
// Equality and comparison
// ---------------------------------------------------------------------------

// Strict equality. Hot path: short-circuit on lp_word; for long strings also
// checks hash32 before falling to full byte compare.
static inline int str_equals(const DrakenStringSlot* a, const uint8_t* arena_a,
                             const DrakenStringSlot* b, const uint8_t* arena_b) {
    if (a->raw.lo != b->raw.lo) return 0;  // length or first-4-bytes differ
    const uint32_t len = a->inl.length;
    if (len <= STR_INLINE_MAX) {
        // Inline: raw.hi covers inline bytes 4..11 (zero-padded beyond length).
        return a->raw.hi == b->raw.hi;
    }
    // Long: same length + same prefix. Use hash32 for fast rejection.
    if (a->ext.hash32 != b->ext.hash32) return 0;
    // Same hash — full byte compare (only strings > 12 bytes reach here).
    return memcmp(arena_a + a->ext.arena_offset,
                  arena_b + b->ext.arena_offset, len) == 0;
}

// Lexicographic compare returning <0 / 0 / >0.
// Goes through str_data so it is always correct for both inline and long forms.
static inline int str_compare(const DrakenStringSlot* a, const uint8_t* arena_a,
                              const DrakenStringSlot* b, const uint8_t* arena_b) {
    const uint32_t la = a->inl.length, lb = b->inl.length;
    const uint32_t mn = la < lb ? la : lb;
    const uint8_t* pa = str_data(a, arena_a);
    const uint8_t* pb = str_data(b, arena_b);
    if (mn > 0) {
        const int c = memcmp(pa, pb, mn);
        if (c != 0) return c;
    }
    if (la == lb) return 0;
    return la < lb ? -1 : 1;
}

#ifdef __cplusplus
}
#endif
