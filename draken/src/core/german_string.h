#pragma once
// German string (a.k.a. "Umbra string"). 16-byte fixed-width slot, two
// variants discriminated by length.
//
//   Short (len <= 12):
//     [ uint32_t length ][ 12 inline bytes ]
//
//   Long  (len  > 12):
//     [ uint32_t length ][ 4-byte prefix ][ uint64_t arena_offset ]
//
// Both forms expose the first 4 payload bytes in the same physical position
// (bytes 4..7) so a single 8-byte load of `length||prefix` short-circuits most
// equality / ordering comparisons without touching the arena.
//
// Long-form slots reference payload bytes via a byte offset into the
// enclosing DrakenGermanArena's `arena` buffer — position-independent so slot
// arrays can be memcpy'd / IPC'd / persisted without pointer fixup. Helpers
// that read the payload (gs_data, gs_equals, gs_compare) therefore take an
// `arena_base` argument; lp_word / prefix-only helpers do not.
//
// Naming: Pavlo (CMU 15-721) calls this "German-style string storage".

#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GS_INLINE_MAX 12

#pragma pack(push, 1)
typedef union {
    struct {
        uint32_t length;
        uint8_t  data[GS_INLINE_MAX];
    } inl;
    struct {
        uint32_t length;
        uint8_t  prefix[4];
        uint64_t arena_offset;  // byte offset into the enclosing arena
    } ext;
    // Raw 16-byte view for bulk memcpy / comparison.
    struct {
        uint64_t lo;            // length | prefix[0..3]  (== length | inline[0..3])
        uint64_t hi;            // inline[4..11]  OR  arena_offset
    } raw;
} GermanString;
#pragma pack(pop)

static inline uint32_t gs_length(const GermanString* s) { return s->inl.length; }
static inline int      gs_is_inline(const GermanString* s) { return s->inl.length <= GS_INLINE_MAX; }

// Builder-side initializers. GermanString is opaque to Cython; field writes go
// through these inlines so the Cython side never needs to know the union layout.
//
// Every initializer zeroes the full 16-byte slot first. This is mandatory: for
// short strings whose length < 4, the unused trailing prefix bytes (positions
// 4..7) participate in lp_word comparisons, so they must be deterministically
// zero — uninitialised bytes would make equal logical strings compare unequal.

static inline void gs_init_null(GermanString* s) {
    // Default zero state. Length=0, all bytes zero. Used for null rows; the
    // enclosing null bitmap is the source of truth, but the slot value must
    // still be deterministic for any kernel that reads it before checking.
    memset(s, 0, sizeof(GermanString));
}

static inline void gs_init_inline(GermanString* s, const uint8_t* src, uint32_t length) {
    // Precondition: length <= GS_INLINE_MAX. Caller chose the inline form.
    memset(s, 0, sizeof(GermanString));
    s->inl.length = length;
    if (length > 0) {
        memcpy(s->inl.data, src, length);
    }
}

static inline void gs_init_extern(GermanString* s, const uint8_t* src,
                                   uint32_t length, uint64_t arena_offset) {
    // Precondition: length > GS_INLINE_MAX (so src has >= 13 bytes — the
    // 4-byte prefix memcpy below is in-bounds). The full payload bytes
    // (all `length` of them, including the first 4 which we also cache
    // in the prefix) must already have been written to the arena by the
    // caller at offset arena_offset.
    s->ext.length = length;
    memcpy(s->ext.prefix, src, 4);
    s->ext.arena_offset = arena_offset;
}

// First-4-byte prefix, valid for both forms. For values < 4 bytes, the unused
// trailing prefix bytes MUST be zero-padded by the builder so that prefix
// comparison remains memcmp-equivalent up to min(len_a, len_b, 4).
static inline uint32_t gs_prefix4(const GermanString* s) {
    uint32_t p;
    // bytes 4..7 are the inline payload OR the explicit prefix — same physical position
    memcpy(&p, ((const uint8_t*)s) + 4, 4);
    return p;
}

// Payload data pointer. For inline slots returns the inline bytes; for long
// slots returns arena_base + arena_offset. `arena_base` is only consulted in
// the long path — callers that know they are looking at a short slot may
// pass NULL safely.
static inline const uint8_t* gs_data(const GermanString* s, const uint8_t* arena_base) {
    return gs_is_inline(s) ? s->inl.data : (arena_base + s->ext.arena_offset);
}

// `length||prefix` as a single 8-byte word. Two slots with equal lo are
// either equal (both inline AND len<=12 — payload still needed to confirm
// inline bytes 4..11 match) or share length + first-4-bytes (long case —
// must still compare beyond the prefix to confirm).
static inline uint64_t gs_lp_word(const GermanString* s) { return s->raw.lo; }

// Strict equality. Hot path: short-circuit on lp_word; for long-equal-prefix
// case fall through to memcmp of the tail. Both slots' arenas must be passed;
// most callers use a single arena (a == b in `arena_a == arena_b`) but the
// kernel must be safe for the cross-arena case (e.g. predicate constant vs
// row value where the constant lives in a separate single-slot arena).
static inline int gs_equals(const GermanString* a, const uint8_t* arena_a,
                             const GermanString* b, const uint8_t* arena_b) {
    if (a->raw.lo != b->raw.lo) return 0;
    uint32_t len = a->inl.length;
    if (len <= GS_INLINE_MAX) {
        // both inline AND lp_word equal => first 4 inline bytes equal; tail still needed
        return memcmp(a->inl.data, b->inl.data, len) == 0;
    }
    // long: length and first 4 bytes equal; compare remaining (len - 4) bytes
    return memcmp(arena_a + a->ext.arena_offset + 4,
                  arena_b + b->ext.arena_offset + 4, len - 4) == 0;
}

// Lexicographic compare returning <0 / 0 / >0.
static inline int gs_compare(const GermanString* a, const uint8_t* arena_a,
                              const GermanString* b, const uint8_t* arena_b) {
    uint32_t la = a->inl.length, lb = b->inl.length;
    uint32_t mn = la < lb ? la : lb;
    uint32_t cmp_prefix = mn < 4 ? mn : 4;
    int c = memcmp(((const uint8_t*)a) + 4, ((const uint8_t*)b) + 4, cmp_prefix);
    if (c != 0) return c;
    if (mn <= 4) {
        if (la != lb) return (int32_t)la - (int32_t)lb;
        return 0;
    }
    const uint8_t* pa = gs_data(a, arena_a) + 4;
    const uint8_t* pb = gs_data(b, arena_b) + 4;
    c = memcmp(pa, pb, mn - 4);
    if (c != 0) return c;
    return (int32_t)la - (int32_t)lb;
}

#ifdef __cplusplus
}
#endif
