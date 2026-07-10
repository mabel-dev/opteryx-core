#pragma once
// draken/ops/string_subscript.h — character subscript on a string column.
//
// `str[i]` → a VARCHAR column of 1-byte slices. Single-char output always fits
// inline (STR_INLINE_MAX = 12), so the result never needs a long-form arena.
//
// Index semantics follow Python: negative counts from the end. Out-of-range and
// null input rows both yield SQL NULL.
//
// The SINGLE implementation, shared by:
//   * the C-ABI kernel  (draken/ops/kernels/extraction.cpp)
//   * the nanobind bind (opteryx/compiled/nanobind/vector_special.cpp)

#include <cstdint>

#include "ops/string_result.h"

namespace draken::ops {

// Throws std::bad_alloc, having freed every buffer it allocated. Callers running
// under the C ABI wrap this in DRAKEN_KERNEL_TRY, which converts the throw into
// an error sentinel.
static inline StringRows char_subscript_rows(const DrakenVector* dv, int64_t index) {
    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    StringRows out;
    out.length = n;
    out.type   = DRAKEN_VARCHAR;
    out.slots  = sr_alloc_slots(n);

    struct Guard {
        StringRows* o; bool released = false;
        ~Guard() { if (!released && o) sr_free(*o); }
    } guard{&out};

    bool any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!sr_row_is_valid(dv, i)) {
            sr_mark_null(out, i);
            any_null = true;
            continue;
        }
        const DrakenStringSlot* src_slot = &sa->slots[dv->selection[i]];
        const uint8_t* sdata = str_data(src_slot, sa->arena);
        const uint32_t slen  = str_length(src_slot);

        const int64_t pos = (index >= 0) ? index : static_cast<int64_t>(slen) + index;
        if (pos < 0 || pos >= static_cast<int64_t>(slen)) {
            sr_mark_null(out, i);
            any_null = true;
        } else {
            str_init_inline(&out.slots[i], sdata + static_cast<uint32_t>(pos), 1u);
        }
    }

    if (!any_null && out.validity) {
        draken_free(out.validity);
        out.validity = nullptr;
    }

    guard.released = true;
    return out;
}

} // namespace draken::ops
