#pragma once
// draken/ops/array_membership.h — native array set-membership ops (E.10 follow-up).
//
// Two entry points, the multi-value cousins of array_reductions.h's arr_any_eq /
// arr_all_eq:
//
//   arr_contains_any(arr, child, items) — True where ANY element of the array row
//                                         is in `items`  (SQL @>, AtArrow).
//   arr_contains_all(arr, child, items) — True where ALL items appear in the array
//                                         row             (SQL @>>, contains-all).
//
// Layout (Arrow-list, identical to array_reductions.h):
//   arr   — DRAKEN_ARRAY vector; arr.data = int32_t offsets[length+1].
//   child — flat child vector (from draken_array_child_unwrap):
//           DRAKEN_INT64, DRAKEN_FLOAT64, or the string family
//           (DRAKEN_VARCHAR / DRAKEN_NVARCHAR / DRAKEN_VARBINARY).
//   items — MembershipItems: the query's item set, pre-converted to the child's
//           native element type by the binding edge (which holds the GIL). The
//           kernels themselves touch no Python and run with the GIL released.
//
// NULL / EMPTY SEMANTICS — deliberately matches the OLD Python path this replaces
// (opteryx/compiled/nanobind/vector_string_search.cpp impl_contains_any/all),
// NOT array_reductions.h's TVL semantics:
//   Null ARRAY ROW    → False. No output validity bitmap (validity == nullptr).
//   Null CHILD ELEM   → skipped (cannot match a real item).
//   contains_any:
//     empty items      → all False.
//     empty array row  → False.
//   contains_all:
//     empty items      → True for every non-null row (vacuous).
//     empty array row  → False when items non-empty (a required item is absent).
//     any item that cannot be represented in the child's element type
//                      → all False (that item can never appear).
//
// SUPPORTED CHILD TYPES: INT64, FLOAT64, VARCHAR/NVARCHAR/VARBINARY.
//   Anything else → std::invalid_argument, thrown loud (no Python fallback, §2).

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <new>        // std::bad_alloc / placement new — not reliably pulled in by <stdexcept> on stricter libc++
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "core/string_slot.h"
#include "ops/string_compare.h"    // str_eq_slots
#include "ops/float_ops.h"         // fp_total_eq
#include "ops/vec_result.h"

namespace draken { namespace ops {

// ---------------------------------------------------------------------------
// Item set — built once by the binding edge after inspecting child.type.
// Exactly one typed list is populated, matching the child element family.
// Pure C++: no Python handles cross into the kernel.
// ---------------------------------------------------------------------------
struct MembershipStrItem {
    DrakenStringSlot     slot;   // arena_offset is 0; bytes below back it
    std::vector<uint8_t> bytes;  // owned payload; &bytes[0] is the slot's arena base
};

struct MembershipItems {
    std::vector<int64_t>          i64;   // populated when child is DRAKEN_INT64
    std::vector<double>           f64;   // populated when child is DRAKEN_FLOAT64
    std::vector<MembershipStrItem> str;  // populated when child is string-family
    uint32_t requested_count   = 0u;     // total items the query asked for
    bool     has_unrepresentable = false;// an item that cannot exist in this child type
};

// ---------------------------------------------------------------------------
// Internal helpers (amb_ prefix → ODR-clean alongside array_reductions.h's arr_)
// ---------------------------------------------------------------------------
static inline bool amb_row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return validity == nullptr || ((validity[i >> 3] >> (i & 7)) & 1u) != 0u;
}

static inline uint8_t* amb_alloc_bool_buf(uint32_t n) {
    const uint32_t raw    = (n + 7u) >> 3;
    const uint32_t padded = (raw + 7u) & ~7u;
    const size_t   bytes  = padded > 0u ? padded : 8u;
    uint8_t* p = static_cast<uint8_t*>(draken_malloc(bytes));
    if (!p) throw std::bad_alloc();
    std::memset(p, 0, bytes);
    return p;
}

static inline void amb_set_bit(uint8_t* buf, uint32_t i) noexcept {
    buf[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
}

static inline VecResult amb_wrap_bool(uint8_t* result, uint32_t n) {
    VecResult res;
    res.data           = result;
    res.validity       = nullptr;          // null rows → False, never NULL (legacy semantics)
    res.selection      = draken_identity_sel(n);
    res.owns_selection = false;
    res.data_length    = n;
    res.length         = n;
    res.type           = DRAKEN_BOOL;
    res.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return res;
}

static inline VecResult amb_all_false(uint32_t n) {
    return amb_wrap_bool(amb_alloc_bool_buf(n), n);
}

// ---------------------------------------------------------------------------
// Per-element membership tests against the typed item list.
// ---------------------------------------------------------------------------
static inline bool amb_member_i64(const MembershipItems& items, int64_t v) noexcept {
    for (size_t k = 0; k < items.i64.size(); ++k)
        if (items.i64[k] == v) return true;
    return false;
}

static inline bool amb_member_f64(const MembershipItems& items, double v) noexcept {
    for (size_t k = 0; k < items.f64.size(); ++k)
        if (fp_total_eq<double>(items.f64[k], v)) return true;
    return false;
}

static inline bool amb_member_str(const MembershipItems& items,
                                  const DrakenStringSlot* elem,
                                  const uint8_t* elem_arena) noexcept {
    for (size_t k = 0; k < items.str.size(); ++k) {
        const MembershipStrItem& it = items.str[k];
        if (str_eq_slots(&it.slot, it.bytes.data(), elem, elem_arena))
            return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// contains_any — True where any non-null element of the row is in `items`.
// ---------------------------------------------------------------------------
template<DrakenType CHILD>
static VecResult arr_contains_any_typed(const DrakenVector& arr,
                                        const DrakenVector& child,
                                        const MembershipItems& items)
{
    const uint32_t  n       = arr.length;
    const int32_t*  offsets = static_cast<const int32_t*>(arr.data);
    const uint32_t* csel    = child.selection;
    const uint8_t*  cval    = child.validity;
    uint8_t*        result  = amb_alloc_bool_buf(n);

    // Child-typed buffer views (only the matching branch is read).
    const int64_t*           cdata_i64 = (CHILD == DRAKEN_INT64)
                                             ? static_cast<const int64_t*>(child.data) : nullptr;
    const double*            cdata_f64 = (CHILD == DRAKEN_FLOAT64)
                                             ? static_cast<const double*>(child.data) : nullptr;
    const DrakenStringArena* arena     = (CHILD != DRAKEN_INT64 && CHILD != DRAKEN_FLOAT64)
                                             ? static_cast<const DrakenStringArena*>(child.data) : nullptr;

    for (uint32_t i = 0; i < n; ++i) {
        if (!amb_row_valid(arr.validity, i)) continue;   // null row → False

        const uint32_t sel_i = arr.selection[i];
        const int32_t  start = offsets[sel_i];
        const int32_t  end   = offsets[sel_i + 1u];

        for (int32_t j = start; j < end; ++j) {
            if (!amb_row_valid(cval, static_cast<uint32_t>(j))) continue;  // null elem
            bool hit;
            if constexpr (CHILD == DRAKEN_INT64)
                hit = amb_member_i64(items, cdata_i64[csel[j]]);
            else if constexpr (CHILD == DRAKEN_FLOAT64)
                hit = amb_member_f64(items, cdata_f64[csel[j]]);
            else
                hit = amb_member_str(items, &arena->slots[csel[j]], arena->arena);
            if (hit) { amb_set_bit(result, i); break; }
        }
    }
    return amb_wrap_bool(result, n);
}

// ---------------------------------------------------------------------------
// contains_all — True where every requested item appears in the row.
// ---------------------------------------------------------------------------
template<DrakenType CHILD>
static VecResult arr_contains_all_typed(const DrakenVector& arr,
                                        const DrakenVector& child,
                                        const MembershipItems& items)
{
    const uint32_t  n       = arr.length;
    const int32_t*  offsets = static_cast<const int32_t*>(arr.data);
    const uint32_t* csel    = child.selection;
    const uint8_t*  cval    = child.validity;
    uint8_t*        result  = amb_alloc_bool_buf(n);

    const int64_t*           cdata_i64 = (CHILD == DRAKEN_INT64)
                                             ? static_cast<const int64_t*>(child.data) : nullptr;
    const double*            cdata_f64 = (CHILD == DRAKEN_FLOAT64)
                                             ? static_cast<const double*>(child.data) : nullptr;
    const DrakenStringArena* arena     = (CHILD != DRAKEN_INT64 && CHILD != DRAKEN_FLOAT64)
                                             ? static_cast<const DrakenStringArena*>(child.data) : nullptr;

    // The number of representable items that must each be found in the row.
    // (has_unrepresentable is short-circuited to all-False before this kernel.)
    const size_t need = (CHILD == DRAKEN_INT64) ? items.i64.size()
                      : (CHILD == DRAKEN_FLOAT64) ? items.f64.size()
                      : items.str.size();

    for (uint32_t i = 0; i < n; ++i) {
        if (!amb_row_valid(arr.validity, i)) continue;   // null row → False

        const uint32_t sel_i = arr.selection[i];
        const int32_t  start = offsets[sel_i];
        const int32_t  end   = offsets[sel_i + 1u];

        bool all_found = true;
        for (size_t k = 0; k < need && all_found; ++k) {
            bool found = false;
            for (int32_t j = start; j < end; ++j) {
                if (!amb_row_valid(cval, static_cast<uint32_t>(j))) continue;  // null elem
                if constexpr (CHILD == DRAKEN_INT64)
                    found = (cdata_i64[csel[j]] == items.i64[k]);
                else if constexpr (CHILD == DRAKEN_FLOAT64)
                    found = fp_total_eq<double>(cdata_f64[csel[j]], items.f64[k]);
                else
                    found = str_eq_slots(&items.str[k].slot, items.str[k].bytes.data(),
                                         &arena->slots[csel[j]], arena->arena) != 0;
                if (found) break;
            }
            if (!found) all_found = false;
        }
        if (all_found) amb_set_bit(result, i);  // need == 0 → vacuously True
    }
    return amb_wrap_bool(result, n);
}

// ---------------------------------------------------------------------------
// Dispatching entry points — switch on child.type, call the typed kernel.
// ---------------------------------------------------------------------------
static inline bool amb_is_string_child(DrakenType t) noexcept {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

static inline VecResult arr_contains_any(const DrakenVector& arr,
                                         const DrakenVector& child,
                                         const MembershipItems& items)
{
    if (child.type == DRAKEN_INT64)   return arr_contains_any_typed<DRAKEN_INT64>(arr, child, items);
    if (child.type == DRAKEN_FLOAT64) return arr_contains_any_typed<DRAKEN_FLOAT64>(arr, child, items);
    if (amb_is_string_child(child.type))
        return arr_contains_any_typed<DRAKEN_VARCHAR>(arr, child, items);
    throw std::invalid_argument(
        "array_membership: unsupported child element type "
        "(only INT64, FLOAT64, and the string family are supported)");
}

static inline VecResult arr_contains_all(const DrakenVector& arr,
                                         const DrakenVector& child,
                                         const MembershipItems& items)
{
    // A required item that cannot exist in this child type → no row can satisfy.
    if (items.has_unrepresentable) return amb_all_false(arr.length);

    if (child.type == DRAKEN_INT64)   return arr_contains_all_typed<DRAKEN_INT64>(arr, child, items);
    if (child.type == DRAKEN_FLOAT64) return arr_contains_all_typed<DRAKEN_FLOAT64>(arr, child, items);
    if (amb_is_string_child(child.type))
        return arr_contains_all_typed<DRAKEN_VARCHAR>(arr, child, items);
    throw std::invalid_argument(
        "array_membership: unsupported child element type "
        "(only INT64, FLOAT64, and the string family are supported)");
}

}}  // namespace draken::ops
