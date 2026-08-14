#pragma once
// draken/ops/array_subscript.h — element subscript on an ARRAY column.
//
// `arr[i]` → a column of the ARRAY's ELEMENT type: one element per row, picked by
// a bind-time constant index.
//
// An ARRAY vector does not carry its elements. DrakenVector.data holds only the
// int32_t offsets[data_length+1]; the elements hang off VectorOwner::child_owner
// and are NOT reachable from a `const DrakenVector*`. So these loops take the
// parent and the child as SEPARATE vectors — the (parent, child) shape SORT,
// GREATEST/LEAST and the containment kernels already use, whose child the VM resolves
// per morsel from the column owner (BC_C_NATIVE_CHILD, cxx_column_child_vec).
//
// Access is the uniform one, twice over: the row's span is
// offsets[parent->selection[i]] .. offsets[parent->selection[i] + 1], and the
// chosen element is child->data[child->selection[j]]. No shape discrimination.
//
// Index semantics follow Python (and make_array_map_access, the nanobind impl
// this mirrors): 0-based, negative counts from the end. A row answers SQL NULL
// when the parent row is null, the index falls outside the row, or the selected
// ELEMENT is null.

#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"   // draken_identity_sel
#include "ops/string_result.h"
#include "ops/vec_result.h"

namespace draken::ops {

inline bool as_row_valid(const DrakenVector* v, uint32_t i) noexcept {
    return v->validity == nullptr || ((v->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// All-valid mask, padded to the width vecresult_from_string_buffers copies.
inline uint8_t* as_alloc_validity(uint32_t n) {
    const size_t bytes = sr_validity_bytes(n);
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0xFFu, bytes);
    return v;
}

// Resolve row `i`'s selected element to a PHYSICAL index into child->data, or -1
// when the row answers NULL. The one place the subscript's semantics live — every
// typed loop below just reads the index it returns.
inline long long as_pick(const DrakenVector* parent, const DrakenVector* child,
                         uint32_t i, int64_t index) noexcept {
    if (!as_row_valid(parent, i)) return -1;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const uint32_t sel_i   = parent->selection[i];
    const int32_t  start   = offsets[sel_i];
    const int64_t  row_len = static_cast<int64_t>(offsets[sel_i + 1u] - start);
    const int64_t  pos     = (index >= 0) ? index : row_len + index;
    if (pos < 0 || pos >= row_len) return -1;
    const uint32_t j = static_cast<uint32_t>(start + pos);
    if (!as_row_valid(child, j)) return -1;
    return static_cast<long long>(child->selection[j]);
}

// Fixed-width element types. Result is dense, of the child's own type.
template <typename T>
VecResult array_subscript_fixed(const DrakenVector* parent, const DrakenVector* child,
                                int64_t index, DrakenType out_type) {
    const uint32_t n = parent->length;
    const T* cdata = static_cast<const T*>(child->data);

    const size_t obytes = (n > 0u ? n : 1u) * sizeof(T);
    T* out = static_cast<T*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = as_alloc_validity(n);
    bool any_null = false;
    for (uint32_t i = 0u; i < n; ++i) {
        const long long phys = as_pick(parent, child, i, index);
        if (phys < 0) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
            any_null = true;
            continue;
        }
        out[i] = cdata[phys];
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }

    VecResult r{};
    r.data           = out;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);   // global; not owned
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = out_type;
    r.flags          = DRAKEN_SEL_IDENTITY;
    return r;
}

// BOOL elements are bit-packed on both sides.
inline VecResult array_subscript_bool(const DrakenVector* parent, const DrakenVector* child,
                                      int64_t index) {
    const uint32_t n = parent->length;
    const uint8_t* cbits = static_cast<const uint8_t*>(child->data);

    const size_t obytes = ((n + 7u) >> 3) > 0u ? ((n + 7u) >> 3) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = as_alloc_validity(n);
    bool any_null = false;
    for (uint32_t i = 0u; i < n; ++i) {
        const long long phys = as_pick(parent, child, i, index);
        if (phys < 0) {
            validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
            any_null = true;
            continue;
        }
        const uint32_t p = static_cast<uint32_t>(phys);
        if (((cbits[p >> 3] >> (p & 7u)) & 1u) != 0u)
            out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }

    VecResult r{};
    r.data           = out;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = DRAKEN_SEL_IDENTITY;
    return r;
}

// String-family elements. Output is a FRESH block of n rows: the child's arena
// belongs to the input vector and must not be aliased out of the span (same rule
// reduce_string_child / sort_string_child follow).
//
// Throws std::bad_alloc having freed what it allocated; the C-ABI caller converts
// the throw into an error sentinel.
inline StringRows array_subscript_rows(const DrakenVector* parent, const DrakenVector* child,
                                       int64_t index) {
    const uint32_t n = parent->length;
    const auto* sa = static_cast<const DrakenStringArena*>(child->data);

    StringRows out;
    out.length = n;
    out.type   = child->type;
    out.slots  = sr_alloc_slots(n);

    struct Guard {
        StringRows* o; bool released = false;
        ~Guard() { if (!released && o) sr_free(*o); }
    } guard{&out};

    std::vector<uint8_t> arena_buf;
    for (uint32_t i = 0u; i < n; ++i) {
        const long long phys = as_pick(parent, child, i, index);
        if (phys < 0) {
            sr_mark_null(out, i);
            continue;
        }
        const DrakenStringSlot* src = &sa->slots[static_cast<uint32_t>(phys)];
        const uint8_t* bytes = str_data(src, sa->arena);
        const uint32_t len   = str_length(src);
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&out.slots[i], bytes, len);
        } else {
            const uint32_t off = static_cast<uint32_t>(arena_buf.size());
            arena_buf.insert(arena_buf.end(), bytes, bytes + len);
            draken_build_string_slot(&out.slots[i], arena_buf.data() + off, len, off);
        }
    }

    out.arena_len = arena_buf.size();
    if (out.arena_len > 0u) {
        out.arena = static_cast<uint8_t*>(draken_malloc(out.arena_len));
        if (!out.arena) throw std::bad_alloc();
        std::memcpy(out.arena, arena_buf.data(), out.arena_len);
    }
    guard.released = true;
    return out;
}

}  // namespace draken::ops
