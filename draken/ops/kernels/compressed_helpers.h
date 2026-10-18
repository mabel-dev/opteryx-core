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
