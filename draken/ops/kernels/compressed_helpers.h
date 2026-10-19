#pragma once
// draken/ops/kernels/compressed_helpers.h — compression-aware kernel helpers.
//
// A COMPRESSED operand (draken_is_compressed: a constant or a dictionary — the
// value array is smaller than the row count) is processed over its `data_length`
// PHYSICAL values, once per value instead of once per row, and the result keeps
// the operand's encoding (kernel_preserve_shape) wherever the output is a pure
// function of one input value. A DENSE operand keeps the kernel's own row loop.
// The uniform data[selection[i]] contract (CLAUDE.md §11) makes the two paths the
// same answer: the compressed path computes exactly the values the row loop would
// read, and the input's selection maps them back to rows.
//
// The shape test is ALWAYS draken_is_compressed (core/buffers.h) — never an
// open-coded data_length comparison.

#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "ops/vec_result.h"
#include "ops/int64_compare.h"           // draken::ops::cmp_constant_bool_result / cmp_dict_bool_result
#include "ops/kernels/error_handling.h"   // draken_error_sentinel
#include "ops/kernels/result_helpers.h"   // kernel_preserve_shape / kernel_live_slots

// ---------------------------------------------------------------------------
// kernel_map_compressed — fixed-width result, one value per PHYSICAL slot.
//
// out[j] = f(j) for j in [0, data_length); the result carries the input's
// selection and per-row validity (kernel_preserve_shape), so a constant stays
// constant and a dictionary stays a dictionary. `f` must not fail; a value that
// can fail (range check, domain error) needs kernel_map_compressed_live.
// Never throws: allocation failure returns the error sentinel.
// ---------------------------------------------------------------------------
template <typename T, typename F>
static inline VecResult kernel_map_compressed(const DrakenVector* v, DrakenType type, F&& f) noexcept {
    const uint32_t k = v->data_length;
    T* out = static_cast<T*>(draken_malloc((k > 0u ? k : 1u) * sizeof(T)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    for (uint32_t j = 0u; j < k; ++j) out[j] = f(j);
    VecResult r{};
    r.data = out; r.type = type; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
    try {
        kernel_preserve_shape(r, v);
    } catch (...) {
        draken_free(out);
        return draken_error_sentinel("allocation failed");
    }
    return r;
}

// Same, for values that can FAIL. Only slots a valid row reads are computed
// (kernel_live_slots — a dead slot may hold anything: a null row's placeholder,
// a dictionary entry the filter dropped) and must never raise. `f(j, &out)`
// returns nullptr on success or the error message; the first live failure is
// returned as the kernel's error, exactly as the row loop would raise it.
template <typename T, typename F>
static inline VecResult kernel_map_compressed_live(const DrakenVector* v, DrakenType type, F&& f) noexcept {
    const uint32_t k = v->data_length;
    T* out = static_cast<T*>(draken_malloc((k > 0u ? k : 1u) * sizeof(T)));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    VecResult r{};
    try {
        std::vector<uint8_t> live;
        const uint8_t* lv = kernel_live_slots(v, live);
        for (uint32_t j = 0u; j < k; ++j) {
            if (lv != nullptr && !lv[j]) { out[j] = T{}; continue; }
            const char* err = f(j, &out[j]);
            if (err != nullptr) {
                draken_free(out);
                return draken_error_sentinel(err);
            }
        }
        r.data = out; r.type = type; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
    } catch (...) {
        draken_free(out);
        return draken_error_sentinel("allocation failed");
    }
    return r;
}

// ---------------------------------------------------------------------------
// kernel_pred_compressed — BOOL predicate, one verdict per PHYSICAL slot.
//
// A constant operand answers with ONE verdict (cmp_constant_bool_result); a
// dictionary computes K verdicts and scatters them to rows (cmp_dict_bool_result).
// Output is the same dense BOOL the row loop builds: data bit = verdict for valid
// rows, 0 for null rows; validity = the operand's.
// ---------------------------------------------------------------------------
template <typename F>
static inline VecResult kernel_pred_compressed(const DrakenVector* v, F&& hit) noexcept {
    try {
        if (draken_is_constant(v))
            return draken::ops::cmp_constant_bool_result(hit(0u), v->validity, v->length);
        const uint32_t k = v->data_length;
        uint8_t* db = static_cast<uint8_t*>(draken_malloc(k > 0u ? k : 1u));
        if (db == nullptr) return draken_error_sentinel("allocation failed");
        for (uint32_t j = 0u; j < k; ++j) db[j] = hit(j) ? 1u : 0u;
        VecResult r;
        try { r = draken::ops::cmp_dict_bool_result(db, *v); }
        catch (...) { draken_free(db); throw; }
        draken_free(db);
        return r;
    } catch (...) {
        return draken_error_sentinel("allocation failed");
    }
}

// Same, for a verdict that can FAIL. Only slots a valid row reads are evaluated
// (kernel_live_slots); `f(j, &hit)` returns nullptr on success or the error
// message, raised exactly as the row loop would raise it.
template <typename F>
static inline VecResult kernel_pred_compressed_live(const DrakenVector* v, F&& f) noexcept {
    const uint32_t k = v->data_length;
    uint8_t* db = static_cast<uint8_t*>(draken_malloc(k > 0u ? k : 1u));
    if (db == nullptr) return draken_error_sentinel("allocation failed");
    try {
        std::vector<uint8_t> live;
        const uint8_t* lv = kernel_live_slots(v, live);
        for (uint32_t j = 0u; j < k; ++j) {
            db[j] = 0u;
            if (lv != nullptr && !lv[j]) continue;
            bool hit = false;
            const char* err = f(j, &hit);
            if (err != nullptr) {
                draken_free(db);
                return draken_error_sentinel(err);
            }
            db[j] = hit ? 1u : 0u;
        }
        VecResult r = draken_is_constant(v)
            ? draken::ops::cmp_constant_bool_result(db[0] != 0u, v->validity, v->length)
            : draken::ops::cmp_dict_bool_result(db, *v);
        draken_free(db);
        return r;
    } catch (...) {
        draken_free(db);
        return draken_error_sentinel("allocation failed");
    }
}

// ---------------------------------------------------------------------------
// Code-space blend — CASE / IIF / COALESCE family, STRING results (architect,
// 2026-10-19: "try it, see if there's a win for strings, and fixed width").
// Measured: strings KEEP (IIF-as-GROUP-BY-key 0.388, COALESCE 0.789, ClickBench
// Q39 0.973); fixed width DELETED (no win; SUM(CASE ... THEN 1 ELSE 0) 1.014 — a
// dense fixed-width blend is already a cheap copy and its consumers prefer dense).
//
// A blend row takes branch k's PHYSICAL value br[k]->selection[row]. When every
// branch is compressed (draken_is_compressed) or a typed NULL (DRAKEN_NULL: no
// values, never chosen as valid), the result is a dictionary over the branch value
// arrays laid end to end:  code[row] = base[k] + br[k]->selection[row].  Values are
// produced once per physical value instead of once per row, and the result stays
// compressed. Duplicate values across branches are legal (buffers.h: dict data is
// not distinct). Applies only when the concatenation is SHORTER than the row count —
// otherwise the result would not be a compressed vector. The chooser is the row
// blend's own, so the answer is the row blend's by construction (§11).
//
// `pick(row)` returns the chosen branch index in [0, nb), or -1 for a null row (no
// branch chosen, or the chosen branch is null at that row).
// ---------------------------------------------------------------------------

// Fills base[0..nb); returns the total value count, or 0 when the blend does not apply.
static inline uint32_t kernel_blend_bases(const DrakenVector* const* br, uint32_t nb,
                                          uint32_t n, uint32_t* base) noexcept {
    uint64_t total = 0;
    for (uint32_t k = 0; k < nb; ++k) {
        base[k] = static_cast<uint32_t>(total);
        if (br[k]->type == DRAKEN_NULL) continue;
        if (!draken_is_compressed(br[k])) return 0u;
        total += br[k]->data_length;
    }
    return (total > 0u && total < n) ? static_cast<uint32_t>(total) : 0u;
}

// Per-row codes + per-row validity (nullptr when every row is valid). A null row
// gets code 0 — any in-range code; its validity bit is 0. False on OOM.
template <typename Pick>
static inline bool kernel_blend_codes(const DrakenVector* const* br, uint32_t n,
                                      const uint32_t* base, Pick&& pick,
                                      uint32_t** out_codes, uint8_t** out_validity) noexcept {
    auto* codes = static_cast<uint32_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(uint32_t)));
    if (codes == nullptr) return false;
    const uint32_t bm = (n + 7u) >> 3;
    const size_t vbytes = ((bm + 7u) & ~7u) > 0u ? ((bm + 7u) & ~7u) : 8u;
    auto* validity = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (validity == nullptr) { draken_free(codes); return false; }
    std::memset(validity, 0, vbytes);
    bool any_null = false;
    for (uint32_t row = 0; row < n; ++row) {
        const int k = pick(row);
        if (k < 0) { codes[row] = 0u; any_null = true; continue; }
        codes[row] = base[k] + br[k]->selection[row];
        validity[row >> 3] |= static_cast<uint8_t>(1u << (row & 7u));
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }
    *out_codes = codes;
    *out_validity = validity;
    return true;
}

static inline void kernel_blend_dict_shape(VecResult& r, uint32_t* codes, uint8_t* validity,
                                           uint32_t total, uint32_t n) noexcept {
    r.validity          = validity;
    r.validity_embedded = 0u;
    r.selection         = codes;
    r.owns_selection    = true;
    r.data_length       = total;
    r.length            = n;
    r.flags             = 0u;   // no order / density hint survives a blend
}

// String values: every non-NULL branch is a string-family vector; the value block
// is `total` slots with its own arena and NO embedded validity (validity is per
// logical row).
template <typename Pick>
static inline VecResult kernel_blend_string(const DrakenVector* const* br, uint32_t nb, uint32_t n,
                                            const uint32_t* base, uint32_t total,
                                            DrakenType out_type, Pick&& pick,
                                            const char* who) noexcept {
    size_t arena_len = 0;
    for (uint32_t k = 0; k < nb; ++k) {
        if (br[k]->type == DRAKEN_NULL) continue;
        const auto* sa = static_cast<const DrakenStringArena*>(br[k]->data);
        for (uint32_t j = 0; j < br[k]->data_length; ++j) {
            const uint32_t len = str_length(&sa->slots[j]);
            if (len > STR_INLINE_MAX) arena_len += len;
        }
    }
    DrakenStringSlot* slots;
    uint8_t* arena;
    uint8_t* validity_unused;
    uint8_t* block = vecresult_string_block_alloc(total, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);
    size_t arena_pos = 0;
    for (uint32_t k = 0; k < nb; ++k) {
        if (br[k]->type == DRAKEN_NULL) continue;
        const auto* sa = static_cast<const DrakenStringArena*>(br[k]->data);
        for (uint32_t j = 0; j < br[k]->data_length; ++j) {
            const DrakenStringSlot* src = &sa->slots[j];
            const uint32_t len = str_length(src);
            DrakenStringSlot* dst = &slots[base[k] + j];
            if (len <= STR_INLINE_MAX) {
                str_init_inline(dst, str_data(src, sa->arena), len);
            } else {
                std::memcpy(arena + arena_pos, str_data(src, sa->arena), len);
                str_init_extern(dst, arena + arena_pos, len, static_cast<uint32_t>(arena_pos));
                arena_pos += len;
            }
        }
    }
    uint32_t* codes;
    uint8_t* validity;
    if (!kernel_blend_codes(br, n, base, pick, &codes, &validity)) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: allocation failed", who);
    }
    VecResult r = vecresult_from_string_block(block, total, arena_len, /*has_validity=*/0, out_type);
    kernel_blend_dict_shape(r, codes, validity, total, n);
    return r;
}
