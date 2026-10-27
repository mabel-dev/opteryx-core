#pragma once
// noexcept bridges from the nogil _DFA_EXTRACT kernel (vector_dfa_extract.pyx) to
// the C++ shape helpers in result_helpers.h, which signal OOM by throwing.
#include <cstdint>
#include <cstring>
#include <vector>

#include "core/alloc.h"
#include "ops/kernels/result_helpers.h"   // kernel_live_slots / kernel_preserve_shape

// kernel_live_slots as a draken_malloc'd mask. Returns 1 with *mask = nullptr when
// every physical slot is live, 1 with a data_length-byte mask the caller
// draken_frees, or 0 on allocation failure.
static inline int dfa_x_live_mask(const DrakenVector* v, uint8_t** mask) noexcept {
    *mask = nullptr;
    try {
        std::vector<uint8_t> live;
        const uint8_t* lv = kernel_live_slots(v, live);
        if (lv == nullptr) return 1;
        uint8_t* m = static_cast<uint8_t*>(draken_malloc(live.size()));
        if (m == nullptr) return 0;
        std::memcpy(m, lv, live.size());
        *mask = m;
        return 1;
    } catch (...) {
        return 0;
    }
}

// kernel_preserve_shape. Returns 0 on allocation failure, having freed any
// validity it copied (the caller still owns and frees r->data).
static inline int dfa_x_preserve_shape(VecResult* r, const DrakenVector* v) noexcept {
    try {
        kernel_preserve_shape(*r, v);
        return 1;
    } catch (...) {
        if (r->validity != nullptr) { draken_free(r->validity); r->validity = nullptr; }
        return 0;
    }
}
