#pragma once
// draken/ops/string_result.h — component buffers of a string-family result column.
//
// Kernels that build a new string column (JSON extraction, character subscript)
// produce the same three buffers. Two consumers finalize them differently:
//
//   C ABI kernel   → vecresult_from_string_buffers (single consolidated block)
//   nanobind bind  → draken_vector_own_string      (PyObject Vector)
//
// Keeping the producer independent of the finalizer is what lets one row loop
// serve both, with no duplicated logic.

#include <cstdint>
#include <cstring>
#include <new>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"

namespace draken::ops {

// All non-null pointers are draken_malloc'd; ownership passes to the caller.
struct StringRows {
    DrakenStringSlot* slots     = nullptr;
    uint8_t*          arena     = nullptr;   // long-form bytes; null when all inline
    size_t            arena_len = 0u;
    uint8_t*          validity  = nullptr;   // null = all rows valid
    uint32_t          length    = 0u;
    DrakenType        type      = DRAKEN_VARCHAR;
};

// Padded null-bitmap width. MUST match string_block_layout() in result_helpers.cpp:
// vecresult_from_string_buffers memcpy's this many bytes out of `validity`, so a
// tighter allocation would be over-read.
static inline size_t sr_validity_bytes(uint32_t n) noexcept {
    const uint32_t bm     = (n + 7u) / 8u;
    const uint32_t padded = (bm + 7u) & ~7u;
    return padded > 0u ? padded : 8u;
}

static inline bool sr_row_is_valid(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

static inline void sr_free(StringRows& r) noexcept {
    draken_free(r.slots);
    draken_free(r.arena);
    draken_free(r.validity);
    r.slots = nullptr; r.arena = nullptr; r.validity = nullptr;
}

// Allocate `length` zeroed slots. Throws std::bad_alloc.
static inline DrakenStringSlot* sr_alloc_slots(uint32_t n) {
    const size_t sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* p = static_cast<DrakenStringSlot*>(draken_malloc(sz));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, sz);
    return p;
}

// Lazily allocate the padded all-valid bitmap, then mark row i null.
// Throws std::bad_alloc.
static inline void sr_mark_null(StringRows& r, uint32_t i) {
    if (!r.validity) {
        const size_t vbytes = sr_validity_bytes(r.length);
        r.validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!r.validity) throw std::bad_alloc();
        std::memset(r.validity, 0xFFu, vbytes);
    }
    r.validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
    str_init_null(&r.slots[i]);
}

} // namespace draken::ops
