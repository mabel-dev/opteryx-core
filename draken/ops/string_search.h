#pragma once
// draken/ops/string_search.h — bytewise string search ops (Milestone E.10, Part A).
//
// Implements starts_with / ends_with / contains — CS and ASCII-CI variants —
// for string-family vectors (VARCHAR / NVARCHAR / VARBINARY).
//
// BYTEWISE: the same implementation works for all three string types. Prefix /
// suffix / substring search operates on raw bytes. Valid UTF-8 guarantees that
// multi-byte character trailing bytes (0x80–0xBF) never appear as leading bytes
// of another character, so a byte-level search of a valid UTF-8 needle in a
// valid UTF-8 haystack always matches at codepoint boundaries.
//
// CI VARIANTS: ASCII-only (A–Z ↔ a–z, i.e. byte OR 0x20). No Unicode case
// folding. Callers supply a pre-lowercased needle; haystack folding is inline.
//
// EMPTY NEEDLE: SQL convention — every string starts with / ends with /
// contains the empty string. Returns True for all non-null rows when ndl_len==0.
//
// NULL SEMANTICS (TVL):
//   null input row → null output row (validity bit 0, result bit 0).
//   Output validity is a copy of input validity; nullptr when no nulls.
//
// ACCESS PATTERN: uniform slots[v.selection[i]] for all i in [0, v.length).
// No shape discrimination.
//
// OUTPUT: bit-packed DRAKEN_BOOL VecResult. Allocated via cmp_alloc_bool_buf /
// cmp_copy_validity from int64_compare.h (zero-init + masked validity copy).
//
// CONTAINS: uses Volnitsky algorithm (src/cpp/volnitsky.h) — same algorithm as
// the former vector_contains.pyx. The bigram table is built once per call, then
// probed per row. Null rows are skipped before the Volnitsky probe.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "ops/int64_compare.h"  // cmp_alloc_bool_buf, cmp_copy_validity
#include "ops/vec_result.h"
#include "volnitsky.h"          // VolnitskyTable, volnitsky_{alloc,free,build,contains_*}

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// ASCII fold — A–Z → a–z. Branchless: subtract 32 when in [65,90].
// ---------------------------------------------------------------------------
static inline uint8_t _ss_lower(uint8_t b) noexcept {
    return static_cast<uint8_t>((b >= 'A' && b <= 'Z') ? (b | 0x20u) : b);
}

// ---------------------------------------------------------------------------
// Type guard — string-family only. Raises std::invalid_argument on mismatch.
// ---------------------------------------------------------------------------
static inline void _ss_require_string(const DrakenVector& v, const char* op) {
    if (v.type != DRAKEN_VARCHAR  &&
        v.type != DRAKEN_NVARCHAR &&
        v.type != DRAKEN_VARBINARY) {
        throw std::invalid_argument(
            std::string(op) + ": expected a string Vector "
            "(VARCHAR, NVARCHAR, or VARBINARY)");
    }
}

// ---------------------------------------------------------------------------
// Build a VecResult from allocated bit-buffer + validity + length.
// ---------------------------------------------------------------------------
static inline VecResult _ss_make_result(uint8_t* bits, uint8_t* validity,
                                         uint32_t n) noexcept {
    VecResult r;
    r.data           = bits;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// ---------------------------------------------------------------------------
// All-valid test — true when the input carries no nulls (validity == nullptr,
// the normalized all-valid form, OR a bitmap with every logical bit set). The
// cpp-pipeline often attaches an all-ones validity bitmap rather than nullptr,
// so the cheap pointer check alone misses the common all-valid case. O(n/8).
// ---------------------------------------------------------------------------
static inline bool _ss_all_valid(const uint8_t* validity, uint32_t n) noexcept {
    if (validity == nullptr) return true;
    const uint32_t full = n >> 3;                 // whole bytes
    for (uint32_t b = 0; b < full; ++b)
        if (validity[b] != 0xFFu) return false;
    const uint32_t rem = n & 7u;                  // trailing bits
    if (rem != 0u) {
        const uint8_t mask = static_cast<uint8_t>((1u << rem) - 1u);
        if ((validity[full] & mask) != mask) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Byte-level match primitives (all handle ndl_len==0 → true).
// ---------------------------------------------------------------------------

static inline bool _ss_prefix(const uint8_t* hay, uint32_t hlen,
                               const uint8_t* ndl, uint32_t nlen) noexcept {
    if (nlen == 0) return true;
    if (hlen < nlen) return false;
    return memcmp(hay, ndl, nlen) == 0;
}

static inline bool _ss_suffix(const uint8_t* hay, uint32_t hlen,
                               const uint8_t* ndl, uint32_t nlen) noexcept {
    if (nlen == 0) return true;
    if (hlen < nlen) return false;
    return memcmp(hay + hlen - nlen, ndl, nlen) == 0;
}

static inline bool _ss_prefix_ci(const uint8_t* hay, uint32_t hlen,
                                   const uint8_t* ndl_lo, uint32_t nlen) noexcept {
    if (nlen == 0) return true;
    if (hlen < nlen) return false;
    for (uint32_t k = 0; k < nlen; ++k)
        if (_ss_lower(hay[k]) != ndl_lo[k]) return false;
    return true;
}

static inline bool _ss_suffix_ci(const uint8_t* hay, uint32_t hlen,
                                   const uint8_t* ndl_lo, uint32_t nlen) noexcept {
    if (nlen == 0) return true;
    if (hlen < nlen) return false;
    const uint8_t* tail = hay + hlen - nlen;
    for (uint32_t k = 0; k < nlen; ++k)
        if (_ss_lower(tail[k]) != ndl_lo[k]) return false;
    return true;
}

// ---------------------------------------------------------------------------
// str_starts_with — case-sensitive prefix test.
// ---------------------------------------------------------------------------
static inline VecResult str_starts_with(
    const DrakenVector& v, const uint8_t* needle, uint32_t ndl_len)
{
    _ss_require_string(v, "str_starts_with");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (_ss_prefix(str_data(slot, sa->arena), str_length(slot), needle, ndl_len))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return _ss_make_result(dst, out_null, n);
}

// ---------------------------------------------------------------------------
// str_starts_with_ci — ASCII-CI prefix test. needle_lower must be pre-lowercased.
// ---------------------------------------------------------------------------
static inline VecResult str_starts_with_ci(
    const DrakenVector& v, const uint8_t* needle_lower, uint32_t ndl_len)
{
    _ss_require_string(v, "str_starts_with_ci");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (_ss_prefix_ci(str_data(slot, sa->arena), str_length(slot), needle_lower, ndl_len))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return _ss_make_result(dst, out_null, n);
}

// ---------------------------------------------------------------------------
// str_ends_with — case-sensitive suffix test.
// ---------------------------------------------------------------------------
static inline VecResult str_ends_with(
    const DrakenVector& v, const uint8_t* needle, uint32_t ndl_len)
{
    _ss_require_string(v, "str_ends_with");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (_ss_suffix(str_data(slot, sa->arena), str_length(slot), needle, ndl_len))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return _ss_make_result(dst, out_null, n);
}

// ---------------------------------------------------------------------------
// str_ends_with_ci — ASCII-CI suffix test. needle_lower must be pre-lowercased.
// ---------------------------------------------------------------------------
static inline VecResult str_ends_with_ci(
    const DrakenVector& v, const uint8_t* needle_lower, uint32_t ndl_len)
{
    _ss_require_string(v, "str_ends_with_ci");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (_ss_suffix_ci(str_data(slot, sa->arena), str_length(slot), needle_lower, ndl_len))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return _ss_make_result(dst, out_null, n);
}

// ---------------------------------------------------------------------------
// str_contains — case-sensitive substring search via Volnitsky algorithm.
//
// Builds the bigram table once per call; probes each non-null row.
// Empty needle → True for all non-null rows (SQL convention, matches old .pyx).
// ---------------------------------------------------------------------------
static inline VecResult str_contains(
    const DrakenVector& v, const uint8_t* needle, uint32_t ndl_len)
{
    _ss_require_string(v, "str_contains");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    // Empty needle matches every non-null row (encoding-independent).
    if (ndl_len == 0) {
        uint8_t* dst = cmp_alloc_bool_buf(n);
        uint8_t* out_null = nullptr;
        if (src_null) {
            try { out_null = cmp_copy_validity(src_null, n); }
            catch (...) { draken_free(dst); throw; }
        }
        for (uint32_t i = 0; i < n; ++i) {
            if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
        }
        return _ss_make_result(dst, out_null, n);
    }

    VolnitskyTable* tbl = volnitsky_alloc();
    if (!tbl) throw std::bad_alloc();
    volnitsky_build(tbl, needle, ndl_len);

    // Compressed-shape fast path (dict / constant), all-valid input only.
    // Probe each DISTINCT slot once — not once per logical row — then return a
    // COMPRESSED bool vector that reuses the input's codes (copied: vectors own
    // their selection). The mask/take consumer reads data[selection[i]] via
    // row_bool, so the compressed result filters identically to the dense one.
    //
    // Gated on _ss_all_valid: a compressed bool's per-slot data bit cannot carry
    // a per-row null (a null row may share its slot with matching non-null rows,
    // so the bit is not clearable per row). Restricting to genuinely all-valid
    // input makes the result provably identical to the dense path with no
    // dependency on downstream validity handling; nullable inputs take the dense
    // path below. Falls through to dense on allocation failure.
    if (draken_is_compressed(&v) && _ss_all_valid(src_null, n)) {
        const uint32_t nd = v.data_length;
        uint8_t*  slot_bits = nullptr;
        uint32_t* codes     = nullptr;
        try {
            slot_bits = cmp_alloc_bool_buf(nd);   // zero-init, throws std::bad_alloc on OOM
            codes     = static_cast<uint32_t*>(
                            draken_malloc(static_cast<size_t>(n) * sizeof(uint32_t)));
        } catch (...) {
            draken_free(slot_bits);
            volnitsky_free(tbl);
            throw;
        }
        if (codes != nullptr) {
            for (uint32_t k = 0; k < nd; ++k) {
                const DrakenStringSlot* slot = &sa->slots[k];
                if (volnitsky_contains_cs(str_data(slot, sa->arena), str_length(slot),
                                          needle, ndl_len, tbl))
                    slot_bits[k >> 3u] |= static_cast<uint8_t>(1u << (k & 7u));
            }
            memcpy(codes, sel, static_cast<size_t>(n) * sizeof(uint32_t));
            volnitsky_free(tbl);

            VecResult r;
            r.data           = slot_bits;
            r.validity       = nullptr;        // all-valid (gated)
            r.selection      = codes;
            r.owns_selection = true;
            r.data_length    = nd;
            r.length         = n;
            r.type           = DRAKEN_BOOL;
            r.flags          = 0u;             // codes: neither identity nor permutation
            return r;
        }
        draken_free(slot_bits);                // codes alloc returned null → dense fallback
    }

    // Dense path: data_length == length, nullable input, or alloc fallback.
    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); volnitsky_free(tbl); throw; }
    }
    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (volnitsky_contains_cs(str_data(slot, sa->arena), str_length(slot),
                                   needle, ndl_len, tbl))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    volnitsky_free(tbl);
    return _ss_make_result(dst, out_null, n);
}

// ---------------------------------------------------------------------------
// str_contains_ci — ASCII-CI substring search via Volnitsky algorithm.
//
// needle_lower must be pre-lowercased by the caller (same contract as
// volnitsky_contains_ci). The bigram table is built from the lowercased needle.
// ---------------------------------------------------------------------------
static inline VecResult str_contains_ci(
    const DrakenVector& v, const uint8_t* needle_lower, uint32_t ndl_len)
{
    _ss_require_string(v, "str_contains_ci");
    const uint32_t           n        = v.length;
    const DrakenStringArena* sa       = static_cast<const DrakenStringArena*>(v.data);
    const uint32_t*          sel      = v.selection;
    const uint8_t*           src_null = v.validity;

    uint8_t* dst = cmp_alloc_bool_buf(n);
    uint8_t* out_null = nullptr;
    if (src_null) {
        try { out_null = cmp_copy_validity(src_null, n); }
        catch (...) { draken_free(dst); throw; }
    }

    if (ndl_len == 0) {
        for (uint32_t i = 0; i < n; ++i) {
            if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
        }
        return _ss_make_result(dst, out_null, n);
    }

    VolnitskyTable* tbl = volnitsky_alloc();
    if (!tbl) { draken_free(dst); draken_free(out_null); throw std::bad_alloc(); }
    volnitsky_build(tbl, needle_lower, ndl_len);

    for (uint32_t i = 0; i < n; ++i) {
        if (src_null && !((src_null[i >> 3] >> (i & 7u)) & 1u)) continue;
        const DrakenStringSlot* slot = &sa->slots[sel[i]];
        if (volnitsky_contains_ci(str_data(slot, sa->arena), str_length(slot),
                                   needle_lower, ndl_len, tbl))
            dst[i >> 3u] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    volnitsky_free(tbl);
    return _ss_make_result(dst, out_null, n);
}

}} // namespace draken::ops
