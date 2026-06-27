#pragma once
// draken/ops/string_compare.h — string compare_scalar / compare_vector (Milestone D.2).
//
// Result is always a bit-packed DRAKEN_BOOL VecResult (1 bit/logical row, LSB-first).
//
// OP CODES (matching draken convention): 0=eq  1=ne  2=gt  3=ge  4=lt  5=le
//
// EQ / NE:
//   Short (len ≤ 12): EXACT comparison using raw slot words (raw.lo + raw.hi).
//     Zero-padding is guaranteed by str_init_inline, so byte-identity is correct.
//   Long  (len > 12): length/prefix/hash32 are fast negative filters. If all
//     three match, arena bytes are fetched and compared exactly.
//
// GT / GE / LT / LE — NOT hash-only:
//   Uses str_compare (string_slot.h) which calls memcmp on actual bytes.
//   On a prefix tie for long strings, arena bytes are fetched.  This is correct:
//   hash-only ordering would silently misorder strings with equal prefixes.
//
// SCALAR:
//   compare_scalar(v, scalar_slot, scalar_bytes, op):
//     scalar_slot  — pre-built DrakenStringSlot via str_init_inline / str_init_extern.
//                    For long scalars, arena_offset MUST be 0 (so str_data(slot, ptr)==ptr).
//     scalar_bytes — pointer to the literal's UTF-8 bytes.  For short scalars this
//                    pointer is not consulted (str_data uses inline bytes); for long
//                    scalars str_compare reads arena_base+0 = scalar_bytes.
//                    Must remain valid for the duration of the call.
//     The caller MUST build scalar_slot using the same path as D.1 ingestion
//     (str_init_inline / str_init_extern + XXH3_64bits) so that eq against
//     a stored long string matches — determinism dependency honored.
//
// NULL SEMANTICS (three-valued logic, SQL-correct):
//   compare_scalar: null input row → null output row.
//   compare_vector: output row is null if EITHER operand row is null.
//
// ACCESS PATTERN: slots[v.selection[i]] for i in [0, v.length).
// No shape discrimination — uniform access; works for dict shape automatically.

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Allocation helpers (same logic as int64_compare.h; prefixed str_ to avoid
// ODR conflicts when both headers land in the same translation unit).
// ---------------------------------------------------------------------------

static inline uint8_t* str_alloc_bool_buf(uint32_t n) {
    const uint32_t raw    = (n + 7u) >> 3;
    const uint32_t padded = (raw + 7u) & ~7u;
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    memset(p, 0, bytes);
    return p;
}

static inline uint8_t* str_copy_validity(const uint8_t* src, uint32_t n) {
    uint8_t* dst = str_alloc_bool_buf(n);
    const uint32_t nb = (n + 7u) >> 3;
    if (nb > 0) {
        memcpy(dst, src, nb);
        if ((n & 7u) != 0)
            dst[nb - 1] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }
    return dst;
}

static inline uint8_t* str_and_validity(
    const uint8_t* va, const uint8_t* vb, uint32_t n)
{
    if (va == nullptr && vb == nullptr) return nullptr;
    if (va == nullptr) return str_copy_validity(vb, n);
    if (vb == nullptr) return str_copy_validity(va, n);

    const uint32_t nb = (n + 7u) >> 3;
    uint8_t* dst = str_alloc_bool_buf(n);
    bool all_valid = true;
    for (uint32_t k = 0; k < nb; ++k) {
        uint8_t expected = 0xFFu;
        if (k == nb - 1u && (n & 7u) != 0)
            expected = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        dst[k] = static_cast<uint8_t>(va[k] & vb[k]);
        if (dst[k] != expected) all_valid = false;
    }
    if (all_valid) {
        draken_free(dst);
        return nullptr;
    }
    return dst;
}

// ---------------------------------------------------------------------------
// str_eq_slots — exact equality for two slots.
//
// SHORT (≤12): exact — raw.lo (length||first4) and raw.hi (bytes 4-11, zero-
//   padded) cover all content; no arena needed.
// LONG (>12): fast-reject on length/prefix/hash32, then verify the candidate by
//   comparing arena bytes. raw.hi for long is [hash32 || arena_offset], so do
//   not compare raw.hi directly: arena_offset is not part of value identity.
// ---------------------------------------------------------------------------
static inline int str_eq_slots(const DrakenStringSlot* a,
                               const uint8_t* arena_a,
                               const DrakenStringSlot* b,
                               const uint8_t* arena_b) noexcept {
    if (a->raw.lo != b->raw.lo) return 0;  // length or first-4-bytes differ
    if (str_is_inline(a)) {
        // Short: full inline content in raw.hi (zero-padded beyond length).
        return a->raw.hi == b->raw.hi;
    }
    if (a->ext.hash32 != b->ext.hash32) return 0;
    return std::memcmp(arena_a + a->ext.arena_offset,
                       arena_b + b->ext.arena_offset,
                       a->ext.length) == 0;
}

// ---------------------------------------------------------------------------
// str_constant_bool_result — architect-approved constant fast-path helper.
//
// Mirrors cmp_constant_bool_result in int64_compare.h; uses str_ prefix helpers
// to stay ODR-clean when both headers land in the same translation unit.
// ---------------------------------------------------------------------------
static inline VecResult str_constant_bool_result(
    bool bit, const uint8_t* src_null, uint32_t n)
{
    uint8_t* dst = str_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = str_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    if (bit) {
        const uint32_t nb = (n + 7u) >> 3;
        if (src_null == nullptr) {
            memset(dst, 0xFFu, nb);
            if (n & 7u) dst[nb - 1u] = static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        } else {
            memcpy(dst, src_null, nb);
            if (n & 7u) dst[nb - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
        }
    }
    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// str_dict_scatter / str_dict_bool_result — architect-approved dict fast-path.
//
// Mirrors cmp_dict_scatter/cmp_dict_bool_result in int64_compare.h; str_ prefix
// avoids ODR clashes when both headers land in the same translation unit.
// ---------------------------------------------------------------------------
static inline void str_dict_scatter(
    const uint8_t*  dict_bytes,
    const uint32_t* selection,
    const uint8_t*  src_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (dict_bytes[selection[base+0]] << 0) |
                (dict_bytes[selection[base+1]] << 1) |
                (dict_bytes[selection[base+2]] << 2) |
                (dict_bytes[selection[base+3]] << 3) |
                (dict_bytes[selection[base+4]] << 4) |
                (dict_bytes[selection[base+5]] << 5) |
                (dict_bytes[selection[base+6]] << 6) |
                (dict_bytes[selection[base+7]] << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (dict_bytes[selection[i]])
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (dict_bytes[selection[base+0]] << 0) |
                (dict_bytes[selection[base+1]] << 1) |
                (dict_bytes[selection[base+2]] << 2) |
                (dict_bytes[selection[base+3]] << 3) |
                (dict_bytes[selection[base+4]] << 4) |
                (dict_bytes[selection[base+5]] << 5) |
                (dict_bytes[selection[base+6]] << 6) |
                (dict_bytes[selection[base+7]] << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((src_null[i >> 3] >> (i & 7)) & 1u)
                if (dict_bytes[selection[i]])
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

static inline VecResult str_dict_bool_result(
    const uint8_t* dict_bytes, const DrakenVector& v)
{
    const uint32_t n        = v.length;
    const uint8_t* src_null = v.validity;

    uint8_t* dst = str_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try { out_null = str_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }
    str_dict_scatter(dict_bytes, v.selection, src_null, dst, n);

    VecResult r;
    r.data = dst; r.validity = out_null;
    r.selection = draken_identity_sel(n); r.owns_selection = false;
    r.data_length = n; r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// str_dict_cross_scatter — both-dict vector scatter (str_ prefix for ODR safety).
// Mirrors cmp_dict_cross_scatter; body is identical.
static inline void str_dict_cross_scatter(
    const uint8_t*  cross,
    uint32_t        dl_b,
    const uint32_t* a_sel,
    const uint32_t* b_sel,
    const uint8_t*  comb_null,
    uint8_t*        dst,
    uint32_t        n)
{
    const uint32_t whole_bytes = n >> 3;
    if (comb_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (cross[a_sel[base+0] * dl_b + b_sel[base+0]] << 0) |
                (cross[a_sel[base+1] * dl_b + b_sel[base+1]] << 1) |
                (cross[a_sel[base+2] * dl_b + b_sel[base+2]] << 2) |
                (cross[a_sel[base+3] * dl_b + b_sel[base+3]] << 3) |
                (cross[a_sel[base+4] * dl_b + b_sel[base+4]] << 4) |
                (cross[a_sel[base+5] * dl_b + b_sel[base+5]] << 5) |
                (cross[a_sel[base+6] * dl_b + b_sel[base+6]] << 6) |
                (cross[a_sel[base+7] * dl_b + b_sel[base+7]] << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if (cross[a_sel[i] * dl_b + b_sel[i]])
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (cross[a_sel[base+0] * dl_b + b_sel[base+0]] << 0) |
                (cross[a_sel[base+1] * dl_b + b_sel[base+1]] << 1) |
                (cross[a_sel[base+2] * dl_b + b_sel[base+2]] << 2) |
                (cross[a_sel[base+3] * dl_b + b_sel[base+3]] << 3) |
                (cross[a_sel[base+4] * dl_b + b_sel[base+4]] << 4) |
                (cross[a_sel[base+5] * dl_b + b_sel[base+5]] << 5) |
                (cross[a_sel[base+6] * dl_b + b_sel[base+6]] << 6) |
                (cross[a_sel[base+7] * dl_b + b_sel[base+7]] << 7));
            dst[b] = static_cast<uint8_t>(m & comb_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i)
            if ((comb_null[i >> 3] >> (i & 7)) & 1u)
                if (cross[a_sel[i] * dl_b + b_sel[i]])
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
}

// ---------------------------------------------------------------------------
// compare_scalar inner kernels.
//
// Two specializations selected at call-site (no per-row branch on path):
//   EQ / NE path: exact; hash32 is a fast negative filter before arena compare.
//   ORDERING path: delegates to str_compare (reads arena on prefix tie for long).
//
// Equality results are packed 8 rows per output byte. dst must be pre-zeroed
// (str_alloc_bool_buf guarantees this).
// ---------------------------------------------------------------------------

// EQ scalar kernel.
static inline void str_cmp_scalar_eq(
    const DrakenStringSlot* slots,
    const uint32_t*         selection,
    const uint8_t*          arena,
    const DrakenStringSlot* scalar_slot,
    const uint8_t*          scalar_bytes,
    const uint8_t*          src_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;
#define DRAKEN_STR_SCALAR_EQ_BIT(BASE, BIT) \
    (static_cast<unsigned>(str_eq_slots(&slots[selection[(BASE) + (BIT)]], arena, scalar_slot, scalar_bytes)) << (BIT))

    for (uint32_t b = 0; b < whole_bytes; ++b) {
        const uint32_t base = b << 3;
        const uint8_t m = static_cast<uint8_t>(
            DRAKEN_STR_SCALAR_EQ_BIT(base, 0) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 1) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 2) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 3) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 4) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 5) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 6) |
            DRAKEN_STR_SCALAR_EQ_BIT(base, 7));
        dst[b] = (src_null == nullptr) ? m : static_cast<uint8_t>(m & src_null[b]);
    }
    for (uint32_t i = whole_bytes << 3; i < n; ++i) {
        if (src_null != nullptr && (((src_null[i >> 3] >> (i & 7)) & 1u) == 0u))
            continue;
        if (str_eq_slots(&slots[selection[i]], arena, scalar_slot, scalar_bytes))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
#undef DRAKEN_STR_SCALAR_EQ_BIT
}

// NE scalar kernel.
static inline void str_cmp_scalar_ne(
    const DrakenStringSlot* slots,
    const uint32_t*         selection,
    const uint8_t*          arena,
    const DrakenStringSlot* scalar_slot,
    const uint8_t*          scalar_bytes,
    const uint8_t*          src_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;
#define DRAKEN_STR_SCALAR_NE_BIT(BASE, BIT) \
    (static_cast<unsigned>(!str_eq_slots(&slots[selection[(BASE) + (BIT)]], arena, scalar_slot, scalar_bytes)) << (BIT))

    for (uint32_t b = 0; b < whole_bytes; ++b) {
        const uint32_t base = b << 3;
        const uint8_t m = static_cast<uint8_t>(
            DRAKEN_STR_SCALAR_NE_BIT(base, 0) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 1) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 2) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 3) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 4) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 5) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 6) |
            DRAKEN_STR_SCALAR_NE_BIT(base, 7));
        dst[b] = (src_null == nullptr) ? m : static_cast<uint8_t>(m & src_null[b]);
    }
    for (uint32_t i = whole_bytes << 3; i < n; ++i) {
        if (src_null != nullptr && (((src_null[i >> 3] >> (i & 7)) & 1u) == 0u))
            continue;
        if (!str_eq_slots(&slots[selection[i]], arena, scalar_slot, scalar_bytes))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
#undef DRAKEN_STR_SCALAR_NE_BIT
}

// Ordering scalar kernel (gt/ge/lt/le). Uses str_compare — fetches arena bytes
// for long strings on prefix tie. NOT hash-only.
// apply_ord(c) maps str_compare()'s <0/0/>0 return to a bool for the chosen op.
template<typename ApplyOrd>
static inline void str_cmp_scalar_ord(
    const DrakenStringSlot* slots,
    const uint32_t*         selection,
    const uint8_t*          arena,
    const DrakenStringSlot* scalar_slot,
    const uint8_t*          scalar_bytes, // "arena" base for scalar (offset==0)
    const uint8_t*          src_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;

    if (src_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+0]], arena, scalar_slot, scalar_bytes))) << 0) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+1]], arena, scalar_slot, scalar_bytes))) << 1) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+2]], arena, scalar_slot, scalar_bytes))) << 2) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+3]], arena, scalar_slot, scalar_bytes))) << 3) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+4]], arena, scalar_slot, scalar_bytes))) << 4) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+5]], arena, scalar_slot, scalar_bytes))) << 5) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+6]], arena, scalar_slot, scalar_bytes))) << 6) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+7]], arena, scalar_slot, scalar_bytes))) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (ApplyOrd::apply(str_compare(&slots[selection[i]], arena, scalar_slot, scalar_bytes)))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+0]], arena, scalar_slot, scalar_bytes))) << 0) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+1]], arena, scalar_slot, scalar_bytes))) << 1) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+2]], arena, scalar_slot, scalar_bytes))) << 2) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+3]], arena, scalar_slot, scalar_bytes))) << 3) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+4]], arena, scalar_slot, scalar_bytes))) << 4) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+5]], arena, scalar_slot, scalar_bytes))) << 5) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+6]], arena, scalar_slot, scalar_bytes))) << 6) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&slots[selection[base+7]], arena, scalar_slot, scalar_bytes))) << 7));
            dst[b] = static_cast<uint8_t>(m & src_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((src_null[i >> 3] >> (i & 7)) & 1u) {
                if (ApplyOrd::apply(str_compare(&slots[selection[i]], arena, scalar_slot, scalar_bytes)))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

// Ordering functor tags — compile-time, zero-cost dispatch.
struct StrOrdGt { static inline bool apply(int c) noexcept { return c >  0; } };
struct StrOrdGe { static inline bool apply(int c) noexcept { return c >= 0; } };
struct StrOrdLt { static inline bool apply(int c) noexcept { return c <  0; } };
struct StrOrdLe { static inline bool apply(int c) noexcept { return c <= 0; } };

// ---------------------------------------------------------------------------
// str_compare_scalar — public kernel for string vs literal.
//
// scalar_slot: pre-built slot (str_init_inline or str_init_extern with offset=0).
// scalar_bytes: literal's UTF-8 bytes (valid for duration of call).
// Builds validity, allocates bit-packed result, dispatches to inner kernel.
// ---------------------------------------------------------------------------
static inline VecResult str_compare_scalar(
    const DrakenVector&    v,
    const DrakenStringSlot& scalar_slot,
    const uint8_t*          scalar_bytes,
    int                     op)
{
    const uint32_t n     = v.length;
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot* slots = sa->slots;
    const uint8_t* arena = sa->arena;
    const uint8_t* src_null = v.validity;

    if (draken_is_constant(&v)) {
        bool bit;
        if (op == 0)      bit =  str_eq_slots(&slots[0], arena, &scalar_slot, scalar_bytes);
        else if (op == 1) bit = !str_eq_slots(&slots[0], arena, &scalar_slot, scalar_bytes);
        else if (op == 2) bit = str_compare(&slots[0], arena, &scalar_slot, scalar_bytes) >  0;
        else if (op == 3) bit = str_compare(&slots[0], arena, &scalar_slot, scalar_bytes) >= 0;
        else if (op == 4) bit = str_compare(&slots[0], arena, &scalar_slot, scalar_bytes) <  0;
        else              bit = str_compare(&slots[0], arena, &scalar_slot, scalar_bytes) <= 0;
        return str_constant_bool_result(bit, src_null, n);
    }

    if (draken_is_dict(&v)) {
        const uint32_t dl = v.data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(dl));
        if (!db) throw std::bad_alloc();
        for (uint32_t k = 0; k < dl; ++k) {
            bool bit;
            if (op == 0)      bit =  str_eq_slots(&slots[k], arena, &scalar_slot, scalar_bytes);
            else if (op == 1) bit = !str_eq_slots(&slots[k], arena, &scalar_slot, scalar_bytes);
            else if (op == 2) bit = str_compare(&slots[k], arena, &scalar_slot, scalar_bytes) >  0;
            else if (op == 3) bit = str_compare(&slots[k], arena, &scalar_slot, scalar_bytes) >= 0;
            else if (op == 4) bit = str_compare(&slots[k], arena, &scalar_slot, scalar_bytes) <  0;
            else              bit = str_compare(&slots[k], arena, &scalar_slot, scalar_bytes) <= 0;
            db[k] = bit ? 1u : 0u;
        }
        VecResult r;
        try { r = str_dict_bool_result(db, v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    }

    uint8_t* out_null = nullptr;
    if (src_null != nullptr) {
        try {
            out_null = str_copy_validity(src_null, n);
        } catch (...) { throw; }
    }

    uint8_t* dst = nullptr;
    try {
        dst = str_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }

    switch (op) {
        case 0:
            str_cmp_scalar_eq(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
        case 1:
            str_cmp_scalar_ne(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
        case 2:
            str_cmp_scalar_ord<StrOrdGt>(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
        case 3:
            str_cmp_scalar_ord<StrOrdGe>(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
        case 4:
            str_cmp_scalar_ord<StrOrdLt>(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
        default:
            str_cmp_scalar_ord<StrOrdLe>(slots, v.selection, arena, &scalar_slot, scalar_bytes, src_null, dst, n);
            break;
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// compare_vector inner kernels (string × string).
// EQ/NE: exact equality; length/prefix/hash32 fast-reject before arena compare.
// Ordering: str_compare with each vector's own arena.
// ---------------------------------------------------------------------------

static inline void str_cmp_vec_eq(
    const DrakenStringSlot* a_slots, const uint32_t* a_sel, const uint8_t* a_arena,
    const DrakenStringSlot* b_slots, const uint32_t* b_sel, const uint8_t* b_arena,
    const uint8_t*          comb_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;
#define DRAKEN_STR_VEC_EQ_BIT(BASE, BIT) \
    (static_cast<unsigned>(str_eq_slots(&a_slots[a_sel[(BASE) + (BIT)]], a_arena, &b_slots[b_sel[(BASE) + (BIT)]], b_arena)) << (BIT))

    for (uint32_t b = 0; b < whole_bytes; ++b) {
        const uint32_t base = b << 3;
        const uint8_t m = static_cast<uint8_t>(
            DRAKEN_STR_VEC_EQ_BIT(base, 0) |
            DRAKEN_STR_VEC_EQ_BIT(base, 1) |
            DRAKEN_STR_VEC_EQ_BIT(base, 2) |
            DRAKEN_STR_VEC_EQ_BIT(base, 3) |
            DRAKEN_STR_VEC_EQ_BIT(base, 4) |
            DRAKEN_STR_VEC_EQ_BIT(base, 5) |
            DRAKEN_STR_VEC_EQ_BIT(base, 6) |
            DRAKEN_STR_VEC_EQ_BIT(base, 7));
        dst[b] = (comb_null == nullptr) ? m : static_cast<uint8_t>(m & comb_null[b]);
    }
    for (uint32_t i = whole_bytes << 3; i < n; ++i) {
        if (comb_null != nullptr && (((comb_null[i >> 3] >> (i & 7)) & 1u) == 0u))
            continue;
        if (str_eq_slots(&a_slots[a_sel[i]], a_arena, &b_slots[b_sel[i]], b_arena))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
#undef DRAKEN_STR_VEC_EQ_BIT
}

static inline void str_cmp_vec_ne(
    const DrakenStringSlot* a_slots, const uint32_t* a_sel, const uint8_t* a_arena,
    const DrakenStringSlot* b_slots, const uint32_t* b_sel, const uint8_t* b_arena,
    const uint8_t*          comb_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;
#define DRAKEN_STR_VEC_NE_BIT(BASE, BIT) \
    (static_cast<unsigned>(!str_eq_slots(&a_slots[a_sel[(BASE) + (BIT)]], a_arena, &b_slots[b_sel[(BASE) + (BIT)]], b_arena)) << (BIT))

    for (uint32_t b = 0; b < whole_bytes; ++b) {
        const uint32_t base = b << 3;
        const uint8_t m = static_cast<uint8_t>(
            DRAKEN_STR_VEC_NE_BIT(base, 0) |
            DRAKEN_STR_VEC_NE_BIT(base, 1) |
            DRAKEN_STR_VEC_NE_BIT(base, 2) |
            DRAKEN_STR_VEC_NE_BIT(base, 3) |
            DRAKEN_STR_VEC_NE_BIT(base, 4) |
            DRAKEN_STR_VEC_NE_BIT(base, 5) |
            DRAKEN_STR_VEC_NE_BIT(base, 6) |
            DRAKEN_STR_VEC_NE_BIT(base, 7));
        dst[b] = (comb_null == nullptr) ? m : static_cast<uint8_t>(m & comb_null[b]);
    }
    for (uint32_t i = whole_bytes << 3; i < n; ++i) {
        if (comb_null != nullptr && (((comb_null[i >> 3] >> (i & 7)) & 1u) == 0u))
            continue;
        if (!str_eq_slots(&a_slots[a_sel[i]], a_arena, &b_slots[b_sel[i]], b_arena))
            dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
    }
#undef DRAKEN_STR_VEC_NE_BIT
}

template<typename ApplyOrd>
static inline void str_cmp_vec_ord(
    const DrakenStringSlot* a_slots, const uint32_t* a_sel, const uint8_t* a_arena,
    const DrakenStringSlot* b_slots, const uint32_t* b_sel, const uint8_t* b_arena,
    const uint8_t*          comb_null,
    uint8_t*                dst,
    uint32_t                n)
{
    const uint32_t whole_bytes = n >> 3;

    if (comb_null == nullptr) {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            dst[b] = static_cast<uint8_t>(
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+0]], a_arena, &b_slots[b_sel[base+0]], b_arena))) << 0) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+1]], a_arena, &b_slots[b_sel[base+1]], b_arena))) << 1) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+2]], a_arena, &b_slots[b_sel[base+2]], b_arena))) << 2) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+3]], a_arena, &b_slots[b_sel[base+3]], b_arena))) << 3) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+4]], a_arena, &b_slots[b_sel[base+4]], b_arena))) << 4) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+5]], a_arena, &b_slots[b_sel[base+5]], b_arena))) << 5) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+6]], a_arena, &b_slots[b_sel[base+6]], b_arena))) << 6) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+7]], a_arena, &b_slots[b_sel[base+7]], b_arena))) << 7));
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if (ApplyOrd::apply(str_compare(&a_slots[a_sel[i]], a_arena, &b_slots[b_sel[i]], b_arena)))
                dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        for (uint32_t b = 0; b < whole_bytes; ++b) {
            const uint32_t base = b << 3;
            const uint8_t m = static_cast<uint8_t>(
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+0]], a_arena, &b_slots[b_sel[base+0]], b_arena))) << 0) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+1]], a_arena, &b_slots[b_sel[base+1]], b_arena))) << 1) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+2]], a_arena, &b_slots[b_sel[base+2]], b_arena))) << 2) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+3]], a_arena, &b_slots[b_sel[base+3]], b_arena))) << 3) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+4]], a_arena, &b_slots[b_sel[base+4]], b_arena))) << 4) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+5]], a_arena, &b_slots[b_sel[base+5]], b_arena))) << 5) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+6]], a_arena, &b_slots[b_sel[base+6]], b_arena))) << 6) |
                (static_cast<unsigned>(ApplyOrd::apply(str_compare(&a_slots[a_sel[base+7]], a_arena, &b_slots[b_sel[base+7]], b_arena))) << 7));
            dst[b] = static_cast<uint8_t>(m & comb_null[b]);
        }
        for (uint32_t i = whole_bytes << 3; i < n; ++i) {
            if ((comb_null[i >> 3] >> (i & 7)) & 1u) {
                if (ApplyOrd::apply(str_compare(&a_slots[a_sel[i]], a_arena, &b_slots[b_sel[i]], b_arena)))
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// str_compare_vector — public kernel for string × string.
// Dispatch-table compatible: (DrakenVector&, DrakenVector&, int) → VecResult.
// ---------------------------------------------------------------------------
// Operand-swapped op code: `a OP b` == `b str_swap_op(OP) a`. Eq/Ne symmetric;
// Gt<->Lt, Ge<->Le. Lets a constant LEFT operand reduce to the scalar path.
static inline int str_swap_op(int op) {
    switch (op) { case 2: return 4; case 3: return 5; case 4: return 2; case 5: return 3; default: return op; }
}

static inline VecResult str_compare_vector(
    const DrakenVector& a, const DrakenVector& b, int op)
{
    const uint32_t n = a.length;
    if (b.length != n)
        throw std::invalid_argument("str_compare_vector: operand lengths must match");

    const DrakenStringArena* sa_a = static_cast<const DrakenStringArena*>(a.data);
    const DrakenStringArena* sa_b = static_cast<const DrakenStringArena*>(b.data);

    const DrakenStringSlot* a_slots = sa_a->slots;
    const DrakenStringSlot* b_slots = sa_b->slots;
    const uint8_t*          a_arena = sa_a->arena;
    const uint8_t*          b_arena = sa_b->arena;

    // constant ⇄ non-constant → reduce to the scalar path, which carries the
    // dict (compressed) and dense fast paths on the non-constant side. The
    // common WHERE str_col <op> 'literal' lands here: a dict-encoded string
    // column vs a constant literal compares only the unique values, not all n
    // rows. Skip when the constant is NULL (validity present) — left to the
    // validity-aware generic path.
    if (draken_is_constant(&b) && b.validity == nullptr)
        return str_compare_scalar(a, b_slots[0], b_arena, op);
    if (draken_is_constant(&a) && a.validity == nullptr)
        return str_compare_scalar(b, a_slots[0], a_arena, str_swap_op(op));

    if (draken_is_constant(&a) && draken_is_constant(&b)) {
        bool bit;
        if (op == 0)      bit =  str_eq_slots(&a_slots[0], a_arena, &b_slots[0], b_arena);
        else if (op == 1) bit = !str_eq_slots(&a_slots[0], a_arena, &b_slots[0], b_arena);
        else if (op == 2) bit = str_compare(&a_slots[0], a_arena, &b_slots[0], b_arena) >  0;
        else if (op == 3) bit = str_compare(&a_slots[0], a_arena, &b_slots[0], b_arena) >= 0;
        else if (op == 4) bit = str_compare(&a_slots[0], a_arena, &b_slots[0], b_arena) <  0;
        else              bit = str_compare(&a_slots[0], a_arena, &b_slots[0], b_arena) <= 0;
        uint8_t* comb = str_and_validity(a.validity, b.validity, n);
        VecResult r;
        try { r = str_constant_bool_result(bit, comb, n); }
        catch (...) { if (comb) draken_free(comb); throw; }
        if (comb) draken_free(comb);
        return r;
    }

    if (draken_is_dict(&a) && draken_is_dict(&b) &&
        (uint64_t)a.data_length * b.data_length <= (uint64_t)n) {
        const uint32_t dl_a = a.data_length;
        const uint32_t dl_b = b.data_length;
        uint8_t* cross = static_cast<uint8_t*>(draken_malloc(dl_a * dl_b));
        if (!cross) throw std::bad_alloc();
        for (uint32_t j = 0; j < dl_a; ++j) {
            for (uint32_t k = 0; k < dl_b; ++k) {
                bool bit;
                if (op == 0)      bit =  str_eq_slots(&a_slots[j], a_arena, &b_slots[k], b_arena);
                else if (op == 1) bit = !str_eq_slots(&a_slots[j], a_arena, &b_slots[k], b_arena);
                else if (op == 2) bit = str_compare(&a_slots[j], a_arena, &b_slots[k], b_arena) >  0;
                else if (op == 3) bit = str_compare(&a_slots[j], a_arena, &b_slots[k], b_arena) >= 0;
                else if (op == 4) bit = str_compare(&a_slots[j], a_arena, &b_slots[k], b_arena) <  0;
                else              bit = str_compare(&a_slots[j], a_arena, &b_slots[k], b_arena) <= 0;
                cross[j * dl_b + k] = bit ? 1u : 0u;
            }
        }
        uint8_t* comb = nullptr;
        uint8_t* dst = nullptr;
        try {
            comb = str_and_validity(a.validity, b.validity, n);
            dst  = str_alloc_bool_buf(n);
        } catch (...) {
            draken_free(cross);
            if (comb) draken_free(comb);
            if (dst)  draken_free(dst);
            throw;
        }
        str_dict_cross_scatter(cross, dl_b, a.selection, b.selection, comb, dst, n);
        draken_free(cross);
        VecResult r;
        r.data = dst; r.validity = comb;
        r.selection = draken_identity_sel(n); r.owns_selection = false;
        r.data_length = n; r.length = n;
        r.type = DRAKEN_BOOL;
        r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        return r;
    }

    uint8_t* out_null = str_and_validity(a.validity, b.validity, n);
    uint8_t* dst = nullptr;
    try {
        dst = str_alloc_bool_buf(n);
    } catch (...) {
        if (out_null) draken_free(out_null);
        throw;
    }

    switch (op) {
        case 0:
            str_cmp_vec_eq(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
        case 1:
            str_cmp_vec_ne(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
        case 2:
            str_cmp_vec_ord<StrOrdGt>(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
        case 3:
            str_cmp_vec_ord<StrOrdGe>(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
        case 4:
            str_cmp_vec_ord<StrOrdLt>(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
        default:
            str_cmp_vec_ord<StrOrdLe>(a_slots, a.selection, a_arena, b_slots, b.selection, b_arena, out_null, dst, n);
            break;
    }

    VecResult r;
    r.data           = dst;
    r.validity       = out_null;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

}} // namespace draken::ops
