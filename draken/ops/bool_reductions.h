#pragma once
// draken/ops/bool_reductions.h — any / all over a DRAKEN_BOOL vector.
//
// SQL three-valued-logic semantics:
//   any:  T if any valid row is True;
//         NULL if no True found but at least one null;
//         F otherwise (all valid+False, or empty).
//
//   all:  F if any valid row is False;
//         NULL if no False found but at least one null;
//         T otherwise (all valid+True, or empty).
//
// Return codes (int8_t): 1 = true, 0 = false, -1 = null.
//
// Access: data[selection[i]] for logical row i (uniform; no shape special-casing).

#include <cstdint>
#include "core/buffers.h"

namespace draken { namespace ops {

static inline int8_t bool_any(const DrakenVector& v) noexcept {
    const uint32_t  n        = v.length;
    const uint8_t*  data     = static_cast<const uint8_t*>(v.data);
    const uint8_t*  validity = v.validity;
    bool found_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        // Check validity for this logical row.
        if (validity != nullptr && !((validity[i >> 3] >> (i & 7u)) & 1u)) {
            found_null = true;
            continue;
        }
        // Valid row: check value bit.
        const uint32_t sel = v.selection[i];
        if ((data[sel >> 3] >> (sel & 7u)) & 1u)
            return 1;  // found True → any = True immediately
    }

    return found_null ? -1 : 0;
}

static inline int8_t bool_all(const DrakenVector& v) noexcept {
    const uint32_t  n        = v.length;
    const uint8_t*  data     = static_cast<const uint8_t*>(v.data);
    const uint8_t*  validity = v.validity;
    bool found_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (validity != nullptr && !((validity[i >> 3] >> (i & 7u)) & 1u)) {
            found_null = true;
            continue;
        }
        const uint32_t sel = v.selection[i];
        if (!((data[sel >> 3] >> (sel & 7u)) & 1u))
            return 0;  // found False → all = False immediately
    }

    return found_null ? -1 : 1;
}

}} // namespace draken::ops
