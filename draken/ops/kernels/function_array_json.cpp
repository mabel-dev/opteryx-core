// draken/ops/kernels/function_array_json.cpp — ARRAY & JSON scalar function kernels
// on the C ABI (func_fn_t):
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched directly from the nogil DV* VM — no Python, no GIL. Registration in
// kernel_registry.cpp is what activates a kernel: the binder probes for
// `draken_{function_name}` and flips BC_INSTR_C_NATIVE on a hit.
//
// ---------------------------------------------------------------------------
// The ARRAY problem, and how these kernels get around it
// ---------------------------------------------------------------------------
// An ARRAY vector does not carry its elements: DrakenVector.data holds only the
// int32_t offsets[length+1], and the element values hang off VectorOwner::
// child_owner (vector_owner.h) — not reachable from a `const DrakenVector*`.
// See the same constraint documented on draken_cast_array_to_varchar
// (cast_kernels.h), which is why THAT kernel has a bespoke (parent, child)
// signature plus BC_C_NATIVE_CHILD VM plumbing.
//
// RESULT half — JSONB_OBJECT_KEYS and SORT return an ARRAY. VecResult::child
// (vec_result.h) carries the element vector out; vecresult_to_owner consumes it
// into child_owner recursively. Before that field existed an ARRAY was not an
// expressible kernel result at all.
//
// READ half — SORT and ARRAY_CONTAINS_ANY/ALL take an ARRAY in. They reuse the
// ARRAY->VARCHAR cast's BC_C_NATIVE_CHILD mechanism, extended to BC_FUNCTION
// (compiled_expression.pyx, evaluation.pyx): the VM resolves the column's child
// via cxx_column_child_vec and appends it as a SYNTHETIC extra arg, so these
// keep the plain func_fn_t(ctx, args[], nargs) shape — args[0]=parent (offsets),
// args[1]=child (elements), nargs==2. That encoding carries exactly ONE
// column_identity, so the array must be a DIRECT column load; a computed array
// argument is not bind-time eligible and is refused at plan time (this engine
// has no Python fallback — an unregistered/ineligible function is a plan-time
// error, not a slow path).
//
// ARRAY_CONTAINS_ANY/ALL fit inside that one-child budget because their needle
// set is a LITERAL baked into an in_list_ctx blob at bind time (the same vehicle
// draken_in_list uses), not a second vector operand — so there is no second
// child to resolve.
//
// GREATEST/LEAST (array_reduce) READ their array via the same BC_C_NATIVE_CHILD
// child and return an element SCALAR — the mirror of the READ half without an
// ARRAY result. SPLIT does the opposite: it reads a VARCHAR and RETURNS an
// ARRAY<VARIANT>, so its child rides out on VecResult::child exactly like
// JSONB_OBJECT_KEYS.
//
// ARRAY_CONTAINS(arr, item) IS in this file (draken_array_contains, below): it
// is lowered at plan-build time to `item = ANY(arr)` (AnyOpEq), bypassing the
// compare admission gate entirely by dispatching as a BC_FUNCTION (same
// BC_C_NATIVE_CHILD path as ARRAY_CONTAINS_ANY/ALL) rather than a BC_COMPARE —
// so AnyOpEq itself stays refused, but ARRAY_CONTAINS never emits it. A literal
// array on the right (`x = ANY([1,2,3])`) lowers separately, to draken_in_list.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"            // draken_identity_sel
#include "ops/array_reductions.h"         // arr_reduce_int64/string — reference = ANY(arr) impl
#include "ops/float_ops.h"                // fp_total_lt — Draken's canonical NaN-highest order
#include "ops/string_result.h"            // StringRows + sr_* helpers
#include "ops/vec_result.h"
#include "ops/kernels/cast_kernels.h"     // draken_cast_to_array decl (impl lives here)
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"   // in_list_ctx; cast_array_ctx
#include "ops/kernels/result_helpers.h"   // vecresult_from_string_buffers
#include "yyjson.h"

namespace {

using draken::ops::StringRows;
using draken::ops::sr_alloc_slots;
using draken::ops::sr_free;
using draken::ops::sr_row_is_valid;

inline bool aj_is_string_family(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool aj_row_valid(const DrakenVector* v, uint32_t i) noexcept {
    return v->validity == nullptr || ((v->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// RAII for yyjson_doc* (mirrors draken::ops::JDocGuard; kept local so this TU does
// not pull in the whole extract_rows loop for one guard).
struct AjDocGuard {
    yyjson_doc* doc;
    explicit AjDocGuard(yyjson_doc* d) noexcept : doc(d) {}
    ~AjDocGuard() noexcept { if (doc) yyjson_doc_free(doc); }
    AjDocGuard(const AjDocGuard&)            = delete;
    AjDocGuard& operator=(const AjDocGuard&) = delete;
};

// Consolidate a flattened StringRows into the single owned block a string
// DrakenVector requires, boxed as an ARRAY result's child. CONSUMES `rows`
// (vecresult_from_string_buffers frees the component buffers either way).
//
// For JSONB_OBJECT_KEYS the rows are VARIANT — matching the bind-time return type
// ARRAY<VARIANT> declared in registrar/utility.pyx, and VARIANT renders to Python
// as str (buffers.h), the same surface as the Python impl's list-of-str. For SORT
// the caller passes the INPUT child's own tag straight through.
inline VecResult* finalize_child(StringRows& rows, const char* who) {
    VecResult child = vecresult_from_string_buffers(
        rows.slots, rows.arena, rows.arena_len, rows.validity, rows.length, rows.type);
    if (child.data == nullptr)
        throw std::runtime_error(std::string(who) + ": failed to build child element vector");
    // `new` here is the ABI contract: vecresult_to_owner deletes it after adopting.
    return new VecResult(child);
}

// ---------------------------------------------------------------------------
// SORT support
// ---------------------------------------------------------------------------
// Per-row ascending sort of the child's [start,end) span, NULLs last (ties among
// nulls stable — order doesn't matter, there's no value to distinguish them by).
// `less(a, b)` is the type's Draken-canonical strict ordering: plain numeric `<`
// for ints, fp_total_lt<T> (NaN highest, per draken_float_nan_semantics) for
// floats. Only fixed-width numeric/bool element types are supported — see the
// kernel's own comment for why string/other element types fail loud instead.

// One row's worth of (value, is_null) pairs, sorted then written to out_data/
// out_null starting at *out_pos (which the caller advances by the span length).
template <typename T, typename Less>
inline void sort_row_numeric(const DrakenVector* child, int32_t start, int32_t end,
                             const T* cdata, T* out_data, uint8_t* out_null,
                             uint32_t& out_pos, Less less,
                             std::vector<std::pair<T, bool>>& scratch) {
    scratch.clear();
    for (int32_t j = start; j < end; ++j) {
        const uint32_t jj = static_cast<uint32_t>(j);
        const bool nul = !aj_row_valid(child, jj);
        scratch.emplace_back(nul ? T{} : cdata[child->selection[jj]], nul);
    }
    std::stable_sort(scratch.begin(), scratch.end(),
        [&](const std::pair<T, bool>& a, const std::pair<T, bool>& b) {
            if (a.second != b.second) return b.second;      // valid sorts before null
            if (a.second) return false;                     // both null: stable, no order
            return less(a.first, b.first);
        });
    for (const auto& p : scratch) {
        out_data[out_pos] = p.first;
        out_null[out_pos] = p.second ? 1u : 0u;
        ++out_pos;
    }
}

// Materializes new_offsets (fresh, cumulative — row SPAN LENGTHS are invariant
// under sort, only element order within a span changes) and runs sort_row_numeric
// per row. Returns the flattened element count (new_offsets[n]).
inline uint32_t sort_build_offsets(const DrakenVector* parent, std::vector<int32_t>& new_offsets) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    new_offsets.assign(static_cast<size_t>(n) + 1, 0);
    for (uint32_t i = 0; i < n; ++i) {
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        new_offsets[i + 1] = new_offsets[i] + (end - start);
    }
    return static_cast<uint32_t>(new_offsets[n]);
}

template <typename T, typename Less>
VecResult* sort_numeric_child(const DrakenVector* parent, const DrakenVector* child,
                              uint32_t total, DrakenType out_type, Less less) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const T* cdata = static_cast<const T*>(child->data);

    T* out_data = static_cast<T*>(draken_malloc((total > 0u ? total : 1u) * sizeof(T)));
    if (!out_data) throw std::bad_alloc();
    std::vector<uint8_t> row_null(total > 0u ? total : 1u, 0u);

    std::vector<std::pair<T, bool>> scratch;
    uint32_t out_pos = 0;
    bool any_null = false;
    for (uint32_t i = 0; i < n; ++i) {
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        sort_row_numeric<T>(child, start, end, cdata, out_data, row_null.data(),
                            out_pos, less, scratch);
    }
    for (uint32_t k = 0; k < total; ++k) if (row_null[k]) { any_null = true; break; }

    uint8_t* validity = nullptr;
    if (any_null) {
        const uint32_t bm = (total + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
        if (!validity) { draken_free(out_data); throw std::bad_alloc(); }
        std::memset(validity, 0xFFu, padded > 0u ? padded : 8u);
        for (uint32_t k = 0; k < total; ++k)
            if (row_null[k]) validity[k >> 3] &= ~static_cast<uint8_t>(1u << (k & 7u));
    }

    VecResult child_r{};
    child_r.data           = out_data;
    child_r.validity       = validity;
    child_r.selection      = draken_identity_sel(total);
    child_r.owns_selection = false;
    child_r.data_length    = total;
    child_r.length         = total;
    child_r.type           = out_type;
    child_r.flags          = DRAKEN_SEL_IDENTITY;
    return new VecResult(child_r);
}

// BOOL is bit-packed (1 bit per PHYSICAL position, buffers.h) — cdata[phys] as a
// byte pointer would read the wrong bits, so this mirrors sort_numeric_child
// with a bit-aware accessor instead of reusing the T* template.
inline VecResult* sort_bool_child(const DrakenVector* parent, const DrakenVector* child,
                                  uint32_t total) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const uint8_t* cbits = static_cast<const uint8_t*>(child->data);

    uint8_t* out_bits = static_cast<uint8_t*>(
        draken_malloc(((total + 7u) >> 3) > 0u ? ((total + 7u) >> 3) : 1u));
    if (!out_bits) throw std::bad_alloc();
    std::memset(out_bits, 0, ((total + 7u) >> 3) > 0u ? ((total + 7u) >> 3) : 1u);
    std::vector<uint8_t> row_null(total > 0u ? total : 1u, 0u);

    std::vector<std::pair<bool, bool>> scratch;  // (value, is_null)
    uint32_t out_pos = 0;
    bool any_null = false;
    for (uint32_t i = 0; i < n; ++i) {
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        scratch.clear();
        for (int32_t j = start; j < end; ++j) {
            const uint32_t jj = static_cast<uint32_t>(j);
            const bool nul = !aj_row_valid(child, jj);
            bool v = false;
            if (!nul) {
                const uint32_t phys = child->selection[jj];
                v = ((cbits[phys >> 3] >> (phys & 7u)) & 1u) != 0u;
            }
            scratch.emplace_back(v, nul);
        }
        std::stable_sort(scratch.begin(), scratch.end(),
            [](const std::pair<bool, bool>& a, const std::pair<bool, bool>& b) {
                if (a.second != b.second) return b.second;   // valid before null
                if (a.second) return false;                  // both null: stable
                return (!a.first) && b.first;                // false < true
            });
        for (const auto& p : scratch) {
            if (p.first) out_bits[out_pos >> 3] |= static_cast<uint8_t>(1u << (out_pos & 7u));
            if (p.second) { row_null[out_pos] = 1u; any_null = true; }
            ++out_pos;
        }
    }

    uint8_t* validity = nullptr;
    if (any_null) {
        const uint32_t bm = (total + 7u) >> 3;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
        if (!validity) { draken_free(out_bits); throw std::bad_alloc(); }
        std::memset(validity, 0xFFu, padded > 0u ? padded : 8u);
        for (uint32_t k = 0; k < total; ++k)
            if (row_null[k]) validity[k >> 3] &= ~static_cast<uint8_t>(1u << (k & 7u));
    }

    VecResult child_r{};
    child_r.data           = out_bits;
    child_r.validity       = validity;
    child_r.selection      = draken_identity_sel(total);
    child_r.owns_selection = false;
    child_r.data_length    = total;
    child_r.length         = total;
    child_r.type           = DRAKEN_BOOL;
    child_r.flags          = DRAKEN_SEL_IDENTITY;
    return new VecResult(child_r);
}

// String-family elements. Ordering is str_compare (string_slot.h) — the SAME
// comparator draken's own GT/GE/LT/LE string kernels use (string_compare.h), so
// this is Draken's engine order, not a locally-invented one. Byte-wise memcmp is
// also codepoint order for NVARCHAR, since UTF-8 is order-preserving.
//
// Output is a fresh consolidated string block: the sorted slots are re-emitted
// (inline stays inline; long slots' bytes are copied into a new arena and
// re-hashed by draken_build_string_slot), because the input arena is not ours to
// alias — the frame arena is destroyed right after the span returns.
inline VecResult* sort_string_child(const DrakenVector* parent, const DrakenVector* child,
                                    uint32_t total) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const auto* sa = static_cast<const DrakenStringArena*>(child->data);

    StringRows rows;
    rows.length = total;
    rows.type   = child->type;          // preserve the input's string tag
    rows.slots  = sr_alloc_slots(total);
    struct RowsGuard {
        StringRows* r; bool released = false;
        ~RowsGuard() { if (!released && r) sr_free(*r); }
    } rg{&rows};

    std::vector<uint8_t> arena_buf;
    std::vector<std::pair<uint32_t, bool>> scratch;   // (physical slot index, is_null)
    uint32_t out_pos = 0;
    for (uint32_t i = 0; i < n; ++i) {
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        scratch.clear();
        for (int32_t j = start; j < end; ++j) {
            const uint32_t jj = static_cast<uint32_t>(j);
            const bool nul = !aj_row_valid(child, jj);
            scratch.emplace_back(nul ? 0u : child->selection[jj], nul);
        }
        std::stable_sort(scratch.begin(), scratch.end(),
            [&](const std::pair<uint32_t, bool>& a, const std::pair<uint32_t, bool>& b) {
                if (a.second != b.second) return b.second;   // valid sorts before null
                if (a.second) return false;                  // both null: stable
                return str_compare(&sa->slots[a.first], sa->arena,
                                   &sa->slots[b.first], sa->arena) < 0;
            });
        for (const auto& p : scratch) {
            if (p.second) {
                sr_mark_null(rows, out_pos);   // lazily allocs validity + str_init_null
            } else {
                const DrakenStringSlot* src = &sa->slots[p.first];
                const uint8_t* bytes = str_data(src, sa->arena);
                const uint32_t len   = str_length(src);
                if (len <= STR_INLINE_MAX) {
                    str_init_inline(&rows.slots[out_pos], bytes, len);
                } else {
                    const uint32_t off = static_cast<uint32_t>(arena_buf.size());
                    arena_buf.insert(arena_buf.end(), bytes, bytes + len);
                    // insert() may have reallocated — re-derive for the hash.
                    draken_build_string_slot(&rows.slots[out_pos],
                                             arena_buf.data() + off, len, off);
                }
            }
            ++out_pos;
        }
    }

    rows.arena_len = arena_buf.size();
    if (rows.arena_len > 0u) {
        rows.arena = static_cast<uint8_t*>(draken_malloc(rows.arena_len));
        if (!rows.arena) throw std::bad_alloc();
        std::memcpy(rows.arena, arena_buf.data(), rows.arena_len);
    }
    rg.released = true;                  // finalize_child consumes rows' buffers
    return finalize_child(rows, "sort");
}

// ---------------------------------------------------------------------------
// ARRAY_CONTAINS_ANY / ARRAY_CONTAINS_ALL support
// ---------------------------------------------------------------------------
// The needle set arrives as a bind-time in_list_ctx blob (kernel_context.h) —
// the SAME vehicle draken_in_list uses:
//   [u32 count][u8 kind][u8 negate][u16 pad][payload]
//   kind 0: count x int64, SORTED ASCENDING  -> std::binary_search
//   kind 1: count x (u32 len + bytes)        -> linear scan (mirrors draken_in_list)
// `negate` is unused here (always 0 from _build_array_membership_blob) — ANY vs
// ALL is the kernel identity, not a blob flag.

// Widen an integer-family element to int64, mirroring draken_in_list's kind-0
// accessor. Returns false when the element type is not integer-family — the
// caller fails loud (the blob kind was inferred from the LITERAL, so a mismatch
// means the query asked e.g. ARRAY_CONTAINS_ANY(string_array, (1,2))).
inline bool acm_elem_int64(const DrakenVector* v, uint32_t phys, int64_t& out) {
    switch (v->type) {
        case DRAKEN_INT8:   out = static_cast<const int8_t*>(v->data)[phys];  return true;
        case DRAKEN_INT16:  out = static_cast<const int16_t*>(v->data)[phys]; return true;
        case DRAKEN_INT32:
        case DRAKEN_DATE32: out = static_cast<const int32_t*>(v->data)[phys]; return true;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64:
            out = static_cast<const int64_t*>(v->data)[phys]; return true;
        default: return false;
    }
}

// Index of `phys`'s value in the kind-1 string payload, or -1 if absent.
inline int32_t acm_find_string(const uint8_t* payload, uint32_t count,
                               const DrakenStringArena* sa, uint32_t phys) {
    const DrakenStringSlot* slot = &sa->slots[phys];
    const uint32_t vlen  = str_length(slot);
    const uint8_t* vdat  = str_data(slot, sa->arena);
    const uint8_t* p = payload;
    for (uint32_t e = 0; e < count; ++e) {
        uint32_t elen;
        std::memcpy(&elen, p, 4);
        p += 4;
        if (elen == vlen && std::memcmp(p, vdat, elen) == 0) return static_cast<int32_t>(e);
        p += elen;
    }
    return -1;
}

// Shared driver for both kernels. want_all=false -> ANY (row ∩ needles ≠ ∅);
// want_all=true -> ALL (needles ⊆ row).
//
// NULL semantics deliberately match the Python implementation being replaced: a
// NULL array ROW yields FALSE, not NULL — the observable contract these kernels
// took over. (The catalog used to declare a null_policy="passthru" for these,
// contradicting that; the field described nothing and was read by nothing, and
// was removed outright — see KernelSpec in catalog.pyx. The kernel is now the
// single statement of its own null semantics.) NULL ELEMENTS inside a row simply
// never match: the needle set can never contain NULL (_membership_values rejects
// such literals), exactly as Python's set(row).intersection(needles) behaves.
VecResult acm_run(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                  bool want_all, const char* who) {
    if (!args || nargs != 2u || !args[0] || !args[1])
        return draken_error_sentinel_fmt(
            "%s: expects (array, child) — 2 arguments; a non-column array operand "
            "has no resolvable child on this kernel", who);
    if (!ctx)
        return draken_error_sentinel_fmt("%s: missing bind-time ctx (needle set)", who);

    const auto* c = static_cast<const in_list_ctx*>(ctx);
    const uint8_t* payload = reinterpret_cast<const uint8_t*>(c) + sizeof(in_list_ctx);
    const DrakenVector* parent = args[0];
    const DrakenVector* child  = args[1];
    if (parent->type != DRAKEN_ARRAY)
        return draken_error_sentinel_fmt("%s: operand must be ARRAY", who);

    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const size_t nb = (static_cast<size_t>(n) + 7u) / 8u;

    // Result is a never-null bit-packed BOOL (a NULL row answers false, not null).
    auto* out = static_cast<uint8_t*>(draken_malloc(nb > 0 ? nb : 1));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, nb > 0 ? nb : 1);

    const bool str_kind = (c->kind != 0);
    if (str_kind && !aj_is_string_family(child->type) && child->type != DRAKEN_VARIANT) {
        draken_free(out);
        return draken_error_sentinel_fmt(
            "%s: needle set is string-typed but the array's elements are type %d", who,
            (int)child->type);
    }
    const auto* sa = str_kind ? static_cast<const DrakenStringArena*>(child->data) : nullptr;
    const auto* items = str_kind ? nullptr : reinterpret_cast<const int64_t*>(payload);

    std::vector<uint8_t> seen;                 // ALL: which needles this row hit
    if (want_all) seen.resize(c->count);

    for (uint32_t i = 0; i < n; ++i) {
        if (!aj_row_valid(parent, i)) continue;          // NULL row -> false
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        if (want_all) std::fill(seen.begin(), seen.end(), 0u);

        bool hit_any = false;
        for (int32_t j = start; j < end; ++j) {
            const uint32_t jj = static_cast<uint32_t>(j);
            if (!aj_row_valid(child, jj)) continue;      // NULL element never matches
            const uint32_t phys = child->selection[jj];
            if (str_kind) {
                const int32_t idx = acm_find_string(payload, c->count, sa, phys);
                if (idx >= 0) {
                    if (!want_all) { hit_any = true; break; }
                    seen[static_cast<size_t>(idx)] = 1u;
                }
            } else {
                int64_t val;
                if (!acm_elem_int64(child, phys, val)) {
                    draken_free(out);
                    return draken_error_sentinel_fmt(
                        "%s: needle set is integer-typed but the array's elements are "
                        "type %d", who, (int)child->type);
                }
                const auto* f = std::lower_bound(items, items + c->count, val);
                if (f != items + c->count && *f == val) {
                    if (!want_all) { hit_any = true; break; }
                    seen[static_cast<size_t>(f - items)] = 1u;
                }
            }
        }

        bool result;
        if (want_all) {
            result = true;
            for (uint32_t e = 0; e < c->count; ++e)
                if (!seen[e]) { result = false; break; }
        } else {
            result = hit_any;
        }
        if (result) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    }

    VecResult r{};
    r.data           = out;
    r.validity       = nullptr;      // never null — a NULL row answered false
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = DRAKEN_BOOL;
    r.flags          = DRAKEN_SEL_IDENTITY;
    return r;
}

// ---------------------------------------------------------------------------
// GREATEST / LEAST support — per-row max/min reducer over an ARRAY column.
// ---------------------------------------------------------------------------
// Faithful port of make_array_greatest (draken_native.cpp — the nanobind
// vector_array_greatest/least these functions bound to as callable_ref):
//   * a NULL parent row               -> NULL result row;
//   * NULL elements are skipped;
//   * an empty / all-null array row   -> NULL result row (no element wins).
// Element order is that reducer's order, NOT draken_sort's fp_total_lt: for
// floats NaN is treated as SMALLEST for BOTH max and min (nanmax/nanmin) — a
// non-NaN always beats a NaN and a NaN never wins; an all-NaN row keeps the
// first NaN. This deliberately differs from SORT (where NaN sorts highest);
// GREATEST/LEAST match the reducer they replace. An array carries ONE element
// type, so there is no mixed-type ordering question at this layer.

// Allocate an all-valid, 8-byte-padded validity bitmap for `n` logical rows.
inline uint8_t* ar_alloc_validity(uint32_t n) {
    const uint32_t bm = (n + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0xFFu, padded > 0u ? padded : 8u);
    return v;
}

template <typename T>
VecResult reduce_numeric_child(const DrakenVector* parent, const DrakenVector* child,
                               DrakenType out_type, bool want_max) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const T* cdata = static_cast<const T*>(child->data);

    T* out_data = static_cast<T*>(draken_malloc((n > 0u ? n : 1u) * sizeof(T)));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, (n > 0u ? n : 1u) * sizeof(T));

    uint8_t* validity = ar_alloc_validity(n);
    bool any_null = false;

    for (uint32_t i = 0; i < n; ++i) {
        bool have = false;
        T best{};
        if (aj_row_valid(parent, i)) {
            const int32_t start = offsets[parent->selection[i]];
            const int32_t end   = offsets[parent->selection[i] + 1u];
            for (int32_t j = start; j < end; ++j) {
                const uint32_t jj = static_cast<uint32_t>(j);
                if (!aj_row_valid(child, jj)) continue;
                const T v = cdata[child->selection[jj]];
                if (!have) { best = v; have = true; continue; }
                bool v_wins;
                if constexpr (std::is_floating_point<T>::value) {
                    if (v != v)            v_wins = false;  // v is NaN — never wins
                    else if (best != best) v_wins = true;   // best is NaN — v beats it
                    else v_wins = want_max ? (v > best) : (v < best);
                } else {
                    v_wins = want_max ? (v > best) : (v < best);
                }
                if (v_wins) best = v;
            }
        }
        if (have) {
            out_data[i] = best;
        } else {
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            any_null = true;
        }
    }
    if (!any_null) { draken_free(validity); validity = nullptr; }

    VecResult r{};
    r.data           = out_data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length    = n;
    r.length         = n;
    r.type           = out_type;
    r.flags          = DRAKEN_SEL_IDENTITY;
    return r;
}

inline VecResult reduce_bool_child(const DrakenVector* parent, const DrakenVector* child,
                                   bool want_max) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const uint8_t* cbits = static_cast<const uint8_t*>(child->data);

    const uint32_t obytes = ((n + 7u) >> 3) > 0u ? ((n + 7u) >> 3) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = ar_alloc_validity(n);
    bool any_null = false;

    for (uint32_t i = 0; i < n; ++i) {
        bool have = false, best = false;
        if (aj_row_valid(parent, i)) {
            const int32_t start = offsets[parent->selection[i]];
            const int32_t end   = offsets[parent->selection[i] + 1u];
            for (int32_t j = start; j < end; ++j) {
                const uint32_t jj = static_cast<uint32_t>(j);
                if (!aj_row_valid(child, jj)) continue;
                const uint32_t phys = child->selection[jj];
                const bool v = ((cbits[phys >> 3] >> (phys & 7u)) & 1u) != 0u;
                if (!have) { best = v; have = true; continue; }
                if (want_max ? (v && !best) : (!v && best)) best = v;
            }
        }
        if (have) {
            if (best) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        } else {
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            any_null = true;
        }
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

// String-family reducer. Winner per row by str_compare (the engine's own GT/LT
// string order, string_compare.h). Output is a fresh consolidated string block
// of n rows (one winner or null per row) — the input arena cannot be aliased
// (the frame arena dies right after the span returns), mirroring
// sort_string_child.
inline VecResult reduce_string_child(const DrakenVector* parent, const DrakenVector* child,
                                     bool want_max) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const auto* sa = static_cast<const DrakenStringArena*>(child->data);

    StringRows rows;
    rows.length = n;
    rows.type   = child->type;
    rows.slots  = sr_alloc_slots(n);
    struct RowsGuard {
        StringRows* r; bool released = false;
        ~RowsGuard() { if (!released && r) sr_free(*r); }
    } rg{&rows};

    std::vector<uint8_t> arena_buf;
    for (uint32_t i = 0; i < n; ++i) {
        long long best = -1;   // physical slot index of the current winner
        if (aj_row_valid(parent, i)) {
            const int32_t start = offsets[parent->selection[i]];
            const int32_t end   = offsets[parent->selection[i] + 1u];
            for (int32_t j = start; j < end; ++j) {
                const uint32_t jj = static_cast<uint32_t>(j);
                if (!aj_row_valid(child, jj)) continue;
                const uint32_t phys = child->selection[jj];
                if (best < 0) { best = static_cast<long long>(phys); continue; }
                const int c = str_compare(&sa->slots[phys], sa->arena,
                                          &sa->slots[static_cast<uint32_t>(best)], sa->arena);
                if (want_max ? (c > 0) : (c < 0)) best = static_cast<long long>(phys);
            }
        }
        if (best < 0) {
            sr_mark_null(rows, i);
        } else {
            const DrakenStringSlot* src = &sa->slots[static_cast<uint32_t>(best)];
            const uint8_t* bytes = str_data(src, sa->arena);
            const uint32_t len   = str_length(src);
            if (len <= STR_INLINE_MAX) {
                str_init_inline(&rows.slots[i], bytes, len);
            } else {
                const uint32_t off = static_cast<uint32_t>(arena_buf.size());
                arena_buf.insert(arena_buf.end(), bytes, bytes + len);
                draken_build_string_slot(&rows.slots[i], arena_buf.data() + off, len, off);
            }
        }
    }

    rows.arena_len = arena_buf.size();
    if (rows.arena_len > 0u) {
        rows.arena = static_cast<uint8_t*>(draken_malloc(rows.arena_len));
        if (!rows.arena) throw std::bad_alloc();
        std::memcpy(rows.arena, arena_buf.data(), rows.arena_len);
    }
    rg.released = true;
    VecResult r = vecresult_from_string_buffers(
        rows.slots, rows.arena, rows.arena_len, rows.validity, rows.length, rows.type);
    if (r.data == nullptr)
        throw std::runtime_error("array reducer: failed to build string result");
    return r;
}

// ---------------------------------------------------------------------------
// ANY-EQ reducers for draken_array_contains (item = ANY(arr)). Same null-row/
// element semantics as reduce_numeric_child/reduce_bool_child above (NULL
// parent row -> NULL result row, NULL elements skipped) EXCEPT an empty or
// all-null row is FALSE, not NULL — that is the reference `= ANY` contract
// (array_reductions.h's arr_reduce_int64/arr_reduce_string), unlike GREATEST/
// LEAST where an empty row has no winner and must be NULL. Deliberately not
// unified with reduce_numeric_child: the "no NULL-when-empty" rule makes them
// different reducers, not a parameterization of the same one.
template <typename T>
VecResult contains_numeric_child(const DrakenVector* parent, const DrakenVector* child, T needle) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const T* cdata = static_cast<const T*>(child->data);

    const uint32_t obytes = ((n + 7u) >> 3) > 0u ? ((n + 7u) >> 3) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = ar_alloc_validity(n);
    bool any_null = false;

    for (uint32_t i = 0; i < n; ++i) {
        if (!aj_row_valid(parent, i)) {
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            any_null = true;
            continue;
        }
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        bool found = false;
        for (int32_t j = start; j < end && !found; ++j) {
            const uint32_t jj = static_cast<uint32_t>(j);
            if (!aj_row_valid(child, jj)) continue;
            if (cdata[child->selection[jj]] == needle) found = true;
        }
        if (found) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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

inline VecResult contains_bool_child(const DrakenVector* parent, const DrakenVector* child,
                                     bool needle) {
    const uint32_t n = parent->length;
    const int32_t* offsets = static_cast<const int32_t*>(parent->data);
    const uint8_t* cbits = static_cast<const uint8_t*>(child->data);

    const uint32_t obytes = ((n + 7u) >> 3) > 0u ? ((n + 7u) >> 3) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = ar_alloc_validity(n);
    bool any_null = false;

    for (uint32_t i = 0; i < n; ++i) {
        if (!aj_row_valid(parent, i)) {
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            any_null = true;
            continue;
        }
        const int32_t start = offsets[parent->selection[i]];
        const int32_t end   = offsets[parent->selection[i] + 1u];
        bool found = false;
        for (int32_t j = start; j < end && !found; ++j) {
            const uint32_t jj = static_cast<uint32_t>(j);
            if (!aj_row_valid(child, jj)) continue;
            const uint32_t phys = child->selection[jj];
            const bool v = ((cbits[phys >> 3] >> (phys & 7u)) & 1u) != 0u;
            if (v == needle) found = true;
        }
        if (found) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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

// A row where the item can provably never equal any element of the target
// integer width (out-of-range needle, e.g. item=9999 against a UINT8 array) —
// legitimately FALSE (not an error, not NULL) for every non-null row, same
// row-null propagation as contains_numeric_child.
inline VecResult contains_never_match(const DrakenVector* parent) {
    const uint32_t n = parent->length;
    const uint32_t obytes = ((n + 7u) >> 3) > 0u ? ((n + 7u) >> 3) : 1u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(obytes));
    if (!out) throw std::bad_alloc();
    std::memset(out, 0, obytes);

    uint8_t* validity = ar_alloc_validity(n);
    bool any_null = false;
    for (uint32_t i = 0; i < n; ++i) {
        if (!aj_row_valid(parent, i)) {
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            any_null = true;
        }
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

// Does int64 payload `v` fit exactly in T's range (no silent truncation)?
template <typename T>
inline bool numeric_item_fits(int64_t v) {
    if constexpr (std::is_same<T, int64_t>::value) {
        return true;
    } else if constexpr (std::is_unsigned<T>::value) {
        if (v < 0) return false;
        return static_cast<uint64_t>(v) <= static_cast<uint64_t>(std::numeric_limits<T>::max());
    } else {
        return v >= static_cast<int64_t>(std::numeric_limits<T>::min())
            && v <= static_cast<int64_t>(std::numeric_limits<T>::max());
    }
}

// Shared dispatch for GREATEST (want_max=true) / LEAST (want_max=false). ctx is
// a binary_op_ctx* carrying the element unit in left_unit, set ONLY for an
// ARRAY<TIMESTAMP> operand (compiled_expression.pyx) — a TIMESTAMP64 result with
// no unit descriptor is a hard error (vector_owner.h) and DrakenVector carries
// none, so the binder hands it over, exactly as draken_sort does.
VecResult array_reduce(void* ctx, const DrakenVector* const* args, uint32_t nargs,
                       bool want_max, const char* who) {
    if (!args || nargs != 2u || !args[0] || !args[1])
        return draken_error_sentinel_fmt(
            "%s: expects (parent, child) — 2 arguments; a computed array argument "
            "has no resolvable child on this kernel", who);
    const DrakenVector* parent = args[0];
    const DrakenVector* child  = args[1];
    if (parent->type != DRAKEN_ARRAY)
        return draken_error_sentinel_fmt("%s: operand must be ARRAY", who);

    switch (child->type) {
        case DRAKEN_INT8:
            return reduce_numeric_child<int8_t>(parent, child, DRAKEN_INT8, want_max);
        case DRAKEN_INT16:
            return reduce_numeric_child<int16_t>(parent, child, DRAKEN_INT16, want_max);
        case DRAKEN_INT32:
            return reduce_numeric_child<int32_t>(parent, child, DRAKEN_INT32, want_max);
        case DRAKEN_INT64:
            return reduce_numeric_child<int64_t>(parent, child, DRAKEN_INT64, want_max);
        case DRAKEN_UINT8:
            return reduce_numeric_child<uint8_t>(parent, child, DRAKEN_UINT8, want_max);
        case DRAKEN_UINT16:
            return reduce_numeric_child<uint16_t>(parent, child, DRAKEN_UINT16, want_max);
        case DRAKEN_UINT32:
            return reduce_numeric_child<uint32_t>(parent, child, DRAKEN_UINT32, want_max);
        case DRAKEN_UINT64:
            return reduce_numeric_child<uint64_t>(parent, child, DRAKEN_UINT64, want_max);
        case DRAKEN_FLOAT32:
            return reduce_numeric_child<float>(parent, child, DRAKEN_FLOAT32, want_max);
        case DRAKEN_FLOAT64:
            return reduce_numeric_child<double>(parent, child, DRAKEN_FLOAT64, want_max);
        case DRAKEN_BOOL:
            return reduce_bool_child(parent, child, want_max);
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY:
        case DRAKEN_VARIANT:
            return reduce_string_child(parent, child, want_max);
        case DRAKEN_TIMESTAMP64: {
            // Raw int64 instants share a unit within an array, so raw order is
            // chronological order — but the scalar result still needs the unit
            // descriptor, handed over in ctx (mirrors draken_sort).
            if (!ctx)
                return draken_error_sentinel_fmt(
                    "%s: ARRAY<TIMESTAMP> requires the bind-time unit ctx", who);
            VecResult r = reduce_numeric_child<int64_t>(
                parent, child, DRAKEN_TIMESTAMP64, want_max);
            if (r.data != nullptr)
                r.ts_unit = static_cast<const binary_op_ctx*>(ctx)->left_unit;
            return r;
        }
        default:
            // DECIMAL/DATE32/TIME/nested-ARRAY/INTERVAL are absent for the SAME
            // reasons draken_sort documents: unreachable as array children today
            // (rugo can't decode list<decimal>; only the timestamp leaf is
            // retagged), each would need its descriptor threaded through ctx, and
            // INTERVAL is not totally orderable. Fail loud rather than guess.
            return draken_error_sentinel_fmt(
                "%s: element type %d not supported (numeric / BOOL / string-family / "
                "TIMESTAMP only)", who, (int)child->type);
    }
}

// ---------------------------------------------------------------------------
// SPLIT support — VARCHAR -> ARRAY<VARIANT> by literal delimiter.
// ---------------------------------------------------------------------------
inline bool sp_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR ||
           t == DRAKEN_VARBINARY || t == DRAKEN_VARIANT;
}

// Split hay[0..hlen) by ndl[0..ndlen) with Python str.split(sep, maxsplit)
// semantics: separators are NOT collapsed, an empty input yields one empty part,
// and after `maxsplit` splits the remainder is a single trailing part. maxsplit
// < 0 means unlimited. Appends (ptr,len) spans (into hay) to `parts`.
inline void sp_split_one(const uint8_t* hay, uint32_t hlen,
                         const uint8_t* ndl, uint32_t ndlen, int64_t maxsplit,
                         std::vector<std::pair<const uint8_t*, uint32_t>>& parts) {
    uint32_t start = 0u, pos = 0u;
    int64_t done = 0;
    while (pos + ndlen <= hlen) {
        if (maxsplit >= 0 && done >= maxsplit) break;
        if (std::memcmp(hay + pos, ndl, ndlen) == 0) {
            parts.emplace_back(hay + start, pos - start);
            pos += ndlen;
            start = pos;
            ++done;
        } else {
            ++pos;
        }
    }
    parts.emplace_back(hay + start, hlen - start);
}

// ---------------------------------------------------------------------------
// CAST(json_text -> ARRAY<element_type>) support
// ---------------------------------------------------------------------------
// Element coercion is STRICT: a JSON element only satisfies the declared element
// type if it is already that kind of value. A number does not satisfy VARCHAR, a
// string does not satisfy INTEGER, a real with a fractional part does not satisfy
// an integer type, and an out-of-range integer does not satisfy a narrow width.
// Anything else fails the ROW (architect ruling: reject the whole row, never a
// per-element NULL) — which the caller then either raises on or nulls, per
// cast_array_ctx::safe.
//
// A JSON `null` element is NOT a coercion failure: it is an absent value, not a
// wrong-typed one, so it becomes a NULL element and the row survives. (Flagged
// as a judgement call, not architect-specified — the ruling was about mixed
// element TYPES. The alternative, failing the row, would make ARRAY<T> unusable
// against real-world JSON, where nulls inside arrays are routine.)

enum class CtaKind { SInt, UInt, Float, Bool, String, Unsupported };

inline CtaKind cta_kind_of(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
            return CtaKind::SInt;
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return CtaKind::UInt;
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            return CtaKind::Float;
        case DRAKEN_BOOL:
            return CtaKind::Bool;
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY: case DRAKEN_VARIANT:
            return CtaKind::String;
        default:
            return CtaKind::Unsupported;
    }
}

inline uint32_t cta_width_of(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:  case DRAKEN_UINT8:                       return 1u;
        case DRAKEN_INT16: case DRAKEN_UINT16:                      return 2u;
        case DRAKEN_INT32: case DRAKEN_UINT32: case DRAKEN_FLOAT32: return 4u;
        default:                                                    return 8u;
    }
}

// Signed-integer element -> raw little-endian bytes at the target width.
// Returns false (row fails) on a non-integral or out-of-range value.
inline bool cta_coerce_sint(yyjson_val* v, DrakenType t, std::vector<uint8_t>& out) {
    int64_t x;
    if (yyjson_is_int(v)) {
        x = yyjson_get_sint(v);
    } else if (yyjson_is_uint(v)) {
        const uint64_t u = yyjson_get_uint(v);
        if (u > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return false;
        x = static_cast<int64_t>(u);
    } else {
        return false;   // a real, string, bool, object or array is not an integer
    }
    switch (t) {
        case DRAKEN_INT8:
            if (x < INT8_MIN  || x > INT8_MAX)  return false; break;
        case DRAKEN_INT16:
            if (x < INT16_MIN || x > INT16_MAX) return false; break;
        case DRAKEN_INT32:
            if (x < INT32_MIN || x > INT32_MAX) return false; break;
        default: break;
    }
    const uint32_t w = cta_width_of(t);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&x);
    out.insert(out.end(), p, p + w);   // little-endian: low bytes first
    return true;
}

inline bool cta_coerce_uint(yyjson_val* v, DrakenType t, std::vector<uint8_t>& out) {
    uint64_t x;
    if (yyjson_is_uint(v)) {
        x = yyjson_get_uint(v);
    } else if (yyjson_is_int(v)) {
        const int64_t s = yyjson_get_sint(v);
        if (s < 0) return false;       // never wrap a negative into unsigned
        x = static_cast<uint64_t>(s);
    } else {
        return false;
    }
    switch (t) {
        case DRAKEN_UINT8:  if (x > UINT8_MAX)  return false; break;
        case DRAKEN_UINT16: if (x > UINT16_MAX) return false; break;
        case DRAKEN_UINT32: if (x > UINT32_MAX) return false; break;
        default: break;
    }
    const uint32_t w = cta_width_of(t);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&x);
    out.insert(out.end(), p, p + w);
    return true;
}

inline bool cta_coerce_float(yyjson_val* v, DrakenType t, std::vector<uint8_t>& out) {
    double d;
    if (yyjson_is_real(v))      d = yyjson_get_real(v);
    else if (yyjson_is_int(v))  d = static_cast<double>(yyjson_get_sint(v));
    else if (yyjson_is_uint(v)) d = static_cast<double>(yyjson_get_uint(v));
    else                        return false;
    if (t == DRAKEN_FLOAT32) {
        const float f = static_cast<float>(d);
        const uint8_t* p = reinterpret_cast<const uint8_t*>(&f);
        out.insert(out.end(), p, p + 4);
    } else {
        const uint8_t* p = reinterpret_cast<const uint8_t*>(&d);
        out.insert(out.end(), p, p + 8);
    }
    return true;
}

// String-family element. VARCHAR/NVARCHAR/VARBINARY accept ONLY a JSON string.
// VARIANT is the deliberate escape hatch — it accepts any element, holding a
// string's bytes verbatim (unquoted) and any other value as its JSON text. That
// matches draken_split, whose ARRAY<VARIANT> child likewise holds plain text.
inline bool cta_coerce_string(yyjson_val* v, DrakenType t, std::string& out) {
    if (yyjson_is_str(v)) {
        out.assign(yyjson_get_str(v), yyjson_get_len(v));
        return true;
    }
    if (t != DRAKEN_VARIANT) return false;
    size_t len = 0;
    char* txt = yyjson_val_write(v, 0, &len);
    if (!txt) return false;
    out.assign(txt, len);
    free(txt);          // yyjson_val_write allocates with the default allocator
    return true;
}

}  // namespace

extern "C" {

// JSONB_OBJECT_KEYS(json) -> ARRAY<VARIANT>
//
// One JSON document per row; emits that object's top-level keys, in document
// order, as an ARRAY row.
//
// Null TVL: a null input row yields a null output row (empty offset span) —
// consistent with the `->`/`->>` extract_rows contract.
//
// Fails LOUD (never a silent null / empty array) on: invalid JSON, or a document
// whose root is not an object. The Python impl it replaces would raise or produce
// garbage on those inputs rather than answer them, so failing is the honest
// mapping; a silent empty array would be a new wrong answer.
// NOTE: explicit try/catch rather than DRAKEN_KERNEL_TRY. That macro takes the whole
// body as ONE argument, and the preprocessor does not protect commas inside braces
// (only parentheses) — a brace-init like `pg{offsets, nullptr}` silently splits it
// into two macro arguments and fails to compile. Same semantics, no trap.
VecResult draken_jsonb_object_keys(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    (void)ctx;
    try {
        if (!args || nargs != 1u || !args[0])
            return draken_error_sentinel("draken_jsonb_object_keys: expects exactly 1 argument");

        const DrakenVector* dv = args[0];
        if (!aj_is_string_family(dv->type) && dv->type != DRAKEN_VARIANT)
            return draken_error_sentinel_fmt(
                "draken_jsonb_object_keys: operand must be a string-family or VARIANT "
                "vector (got type tag %d)", (int)dv->type);

        const uint32_t n  = dv->length;
        const auto*    sa = static_cast<const DrakenStringArena*>(dv->data);

        // Parent: offsets[n+1] into the flattened child.
        const size_t off_bytes = (static_cast<size_t>(n) + 1u) * sizeof(int32_t);
        int32_t* offsets = static_cast<int32_t*>(draken_malloc(off_bytes));
        if (!offsets) throw std::bad_alloc();
        // Parent buffers are raw until the VecResult below adopts them; anything
        // that throws between here and then must not leak them.
        struct ParentGuard {
            int32_t* off; uint8_t* val; bool released = false;
            ~ParentGuard() { if (!released) { draken_free(off); draken_free(val); } }
        } pg{offsets, nullptr};
        offsets[0] = 0;

        // Child keys accumulate as one flat StringRows; row spans are the offsets.
        // Two passes would mean parsing every document twice, so collect key
        // (ptr, len) first — pointers stay valid while their doc guard is alive,
        // so copy into `staged` immediately rather than holding docs open.
        std::vector<std::string> staged;
        staged.reserve(static_cast<size_t>(n) * 4u);

        bool any_null = false;
        std::vector<uint8_t> row_null;   // 1 = null row; sized only if needed
        row_null.assign(n, 0u);

        for (uint32_t i = 0u; i < n; ++i) {
            if (!sr_row_is_valid(dv, i)) {
                row_null[i]     = 1u;
                any_null        = true;
                offsets[i + 1u] = offsets[i];      // null row = empty span
                continue;
            }

            const DrakenStringSlot* slot = &sa->slots[dv->selection[i]];
            const uint8_t* json_bytes    = str_data(slot, sa->arena);
            const uint32_t json_len      = str_length(slot);

            yyjson_read_err perr;
            yyjson_doc* raw = yyjson_read_opts(
                const_cast<char*>(reinterpret_cast<const char*>(json_bytes)),
                static_cast<size_t>(json_len), 0u, nullptr, &perr);
            if (!raw)
                throw std::runtime_error(
                    "jsonb_object_keys: invalid JSON at row " + std::to_string(i) +
                    ": " + (perr.msg ? perr.msg : "parse error"));
            AjDocGuard guard{raw};

            yyjson_val* root = yyjson_doc_get_root(raw);
            if (!root || !yyjson_is_obj(root))
                throw std::runtime_error(
                    "jsonb_object_keys: row " + std::to_string(i) +
                    " is not a JSON object");

            yyjson_obj_iter it;
            yyjson_obj_iter_init(root, &it);
            yyjson_val* key = nullptr;
            int32_t emitted = 0;
            while ((key = yyjson_obj_iter_next(&it)) != nullptr) {
                staged.emplace_back(yyjson_get_str(key), yyjson_get_len(key));
                ++emitted;
            }
            offsets[i + 1u] = offsets[i] + emitted;
        }

        // Child StringRows over the flattened keys. Keys are never null (a JSON
        // object cannot have a null key), so the child carries no validity.
        const uint32_t total = static_cast<uint32_t>(staged.size());
        StringRows rows;
        rows.length = total;
        rows.type   = DRAKEN_VARIANT;
        rows.slots  = sr_alloc_slots(total);
        struct RowsGuard {
            StringRows* r; bool released = false;
            ~RowsGuard() { if (!released && r) sr_free(*r); }
        } rg{&rows};

        std::vector<uint8_t> arena_buf;
        for (uint32_t k = 0u; k < total; ++k) {
            const std::string& s = staged[k];
            const uint32_t len   = static_cast<uint32_t>(s.size());
            if (len <= STR_INLINE_MAX) {
                str_init_inline(&rows.slots[k],
                                reinterpret_cast<const uint8_t*>(s.data()), len);
            } else {
                const uint32_t off = static_cast<uint32_t>(arena_buf.size());
                arena_buf.insert(arena_buf.end(), s.data(), s.data() + len);
                // insert() may reallocate — re-derive the pointer for the hash.
                draken_build_string_slot(&rows.slots[k], arena_buf.data() + off, len, off);
            }
        }
        rows.arena_len = arena_buf.size();
        if (rows.arena_len > 0u) {
            rows.arena = static_cast<uint8_t*>(draken_malloc(rows.arena_len));
            if (!rows.arena) throw std::bad_alloc();
            std::memcpy(rows.arena, arena_buf.data(), rows.arena_len);
        }

        rg.released = true;              // finalize_child consumes rows' buffers
        VecResult* child = finalize_child(rows, "jsonb_object_keys");

        // Parent validity — logical-row indexed, bit set = valid.
        uint8_t* validity = nullptr;
        if (any_null) {
            const uint32_t bm     = (n + 7u) >> 3;
            const uint32_t padded = (bm + 7u) & ~7u;
            const size_t   vbytes = padded > 0u ? padded : 8u;
            validity = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!validity) { delete child; throw std::bad_alloc(); }
            std::memset(validity, 0xFFu, vbytes);
            for (uint32_t i = 0u; i < n; ++i)
                if (row_null[i]) validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            pg.val = validity;
        }

        VecResult r{};
        r.data           = offsets;
        r.validity       = validity;
        r.selection      = draken_identity_sel(n);   // global; not owned
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_ARRAY;
        r.flags          = DRAKEN_SEL_IDENTITY;
        r.child          = child;
        pg.released      = true;                     // r owns offsets + validity now
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_jsonb_object_keys");
    }
}

// LENGTH(arr) -> INT64: the element count of each ARRAY row.
//
// Alone among the ARRAY-reading kernels here, this one needs NO child: a row's
// element COUNT is fully described by the offsets in `data`
// (offsets[phys+1] - offsets[phys]). So it keeps the plain 1-arg shape and needs
// no BC_C_NATIVE_CHILD plumbing — which is why it composes over a COMPUTED array
// where the element-reading kernels (SORT, ARRAY_CONTAINS) cannot.
//
// Null TVL: a null row answers NULL, not 0 — an absent array has no length. The
// offsets of a null row are still in-bounds (an empty span), so the value loop
// stays branch-free and validity alone carries nullness.
VecResult draken_length_array(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    (void)ctx;
    try {
        if (!args || nargs != 1u || !args[0])
            return draken_error_sentinel("draken_length_array: expected 1 argument");

        const DrakenVector* v = args[0];
        if (v->type != DRAKEN_ARRAY)
            return draken_error_sentinel("draken_length_array: operand must be ARRAY");

        const uint32_t n = v->length;
        const int32_t* offsets = static_cast<const int32_t*>(v->data);

        auto* out = static_cast<int64_t*>(
            draken_malloc((n > 0 ? n : 1) * sizeof(int64_t)));
        if (!out) throw std::bad_alloc();

        uint8_t* validity = nullptr;
        if (v->validity != nullptr) {
            const size_t vb = (static_cast<size_t>(n) + 7u) / 8u;
            validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
            if (!validity) {
                draken_free(out);
                throw std::bad_alloc();
            }
            std::memcpy(validity, v->validity, vb > 0 ? vb : 1);
        }

        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t phys = v->selection[i];
            out[i] = static_cast<int64_t>(offsets[phys + 1u] - offsets[phys]);
        }

        VecResult r{};
        r.data           = out;
        r.validity       = validity;
        r.selection      = draken_identity_sel(n);   // global; not owned
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_INT64;
        r.flags          = DRAKEN_SEL_IDENTITY;
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_length_array");
    }
}

// SORT(arr) -> ARRAY, ascending, matching Draken's own engine order — NOT
// Python's sorted() (architect decision 2026-07-16): plain numeric order for
// ints, fp_total_lt (NaN sorts HIGHEST, -0.0 == 0.0) for floats, false < true
// for BOOL, str_compare (the engine's own GT/LT string comparator) for the
// string family. NULL elements sort LAST within their row (stable among
// themselves).
//
// nargs must be 2: args[0]=parent (ARRAY, offsets), args[1]=child (elements) —
// the synthetic BC_C_NATIVE_CHILD arg the VM appends (evaluation.pyx). Only a
// direct-column-load argument has a resolvable child (compiled_expression.pyx
// gates eligibility at bind time), so this kernel is never dispatched with
// nargs==1 for a computed array expression — that case stays on the Python path.
VecResult draken_sort(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    // ctx is a binary_op_ctx* carrying the element's TimestampUnit in left_unit —
    // set ONLY for an ARRAY<TIMESTAMP> operand (see compiled_expression.pyx); NULL
    // for every other element type, which needs no descriptor.
    try {
        if (!args || nargs != 2u || !args[0] || !args[1])
            return draken_error_sentinel(
                "draken_sort: expects (parent, child) — 2 arguments; a computed "
                "array argument has no resolvable child on this kernel");

        const DrakenVector* parent = args[0];
        const DrakenVector* child  = args[1];
        if (parent->type != DRAKEN_ARRAY)
            return draken_error_sentinel("draken_sort: operand must be ARRAY");

        std::vector<int32_t> new_offsets;
        const uint32_t total = sort_build_offsets(parent, new_offsets);
        const uint32_t n = parent->length;

        VecResult* child_r = nullptr;
        switch (child->type) {
            case DRAKEN_INT8:
                child_r = sort_numeric_child<int8_t>(parent, child, total, DRAKEN_INT8,
                    [](int8_t a, int8_t b) { return a < b; });
                break;
            case DRAKEN_INT16:
                child_r = sort_numeric_child<int16_t>(parent, child, total, DRAKEN_INT16,
                    [](int16_t a, int16_t b) { return a < b; });
                break;
            case DRAKEN_INT32:
                child_r = sort_numeric_child<int32_t>(parent, child, total, DRAKEN_INT32,
                    [](int32_t a, int32_t b) { return a < b; });
                break;
            case DRAKEN_INT64:
                child_r = sort_numeric_child<int64_t>(parent, child, total, DRAKEN_INT64,
                    [](int64_t a, int64_t b) { return a < b; });
                break;
            case DRAKEN_UINT8:
                child_r = sort_numeric_child<uint8_t>(parent, child, total, DRAKEN_UINT8,
                    [](uint8_t a, uint8_t b) { return a < b; });
                break;
            case DRAKEN_UINT16:
                child_r = sort_numeric_child<uint16_t>(parent, child, total, DRAKEN_UINT16,
                    [](uint16_t a, uint16_t b) { return a < b; });
                break;
            case DRAKEN_UINT32:
                child_r = sort_numeric_child<uint32_t>(parent, child, total, DRAKEN_UINT32,
                    [](uint32_t a, uint32_t b) { return a < b; });
                break;
            case DRAKEN_UINT64:
                child_r = sort_numeric_child<uint64_t>(parent, child, total, DRAKEN_UINT64,
                    [](uint64_t a, uint64_t b) { return a < b; });
                break;
            case DRAKEN_FLOAT32:
                child_r = sort_numeric_child<float>(parent, child, total, DRAKEN_FLOAT32,
                    [](float a, float b) { return draken::ops::fp_total_lt<float>(a, b); });
                break;
            case DRAKEN_FLOAT64:
                child_r = sort_numeric_child<double>(parent, child, total, DRAKEN_FLOAT64,
                    [](double a, double b) { return draken::ops::fp_total_lt<double>(a, b); });
                break;
            case DRAKEN_BOOL:
                child_r = sort_bool_child(parent, child, total);
                break;
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
            case DRAKEN_VARBINARY:
            case DRAKEN_VARIANT:
                child_r = sort_string_child(parent, child, total);
                break;
            case DRAKEN_TIMESTAMP64: {
                // Raw int64 instants: every element of one array shares a unit, so
                // raw order IS chronological order — no unit needed to COMPARE.
                // The unit IS needed on the RESULT: a TIMESTAMP64 vector with
                // logical_type == nullptr is a hard error (vector_owner.h), and
                // DrakenVector carries no descriptor, so the binder hands it over
                // in binary_op_ctx.left_unit (compiled_expression.pyx).
                if (!ctx)
                    return draken_error_sentinel(
                        "draken_sort: ARRAY<TIMESTAMP> requires the bind-time unit ctx");
                child_r = sort_numeric_child<int64_t>(parent, child, total, DRAKEN_TIMESTAMP64,
                    [](int64_t a, int64_t b) { return a < b; });
                child_r->ts_unit = static_cast<const binary_op_ctx*>(ctx)->left_unit;
                break;
            }
            default:
                // What's NOT here, and the real reason for each:
                //
                // DECIMAL / DECIMAL128 / DATE32 / TIME32/64 are ORDERABLE and need
                // no new comparator — every element of one array shares a
                // scale/unit, so raw int32/int64/int128 order IS value order (the
                // descriptor matters for the RESULT, not the compare; TIMESTAMP64
                // above is exactly that shape). They are absent because they are
                // unreachable, not unorderable: rugo cannot decode a
                // parquet list<decimal> at all, and only the timestamp leaf is
                // retagged on the way in (parquet_read.pyx) — a list<date>/
                // list<time> leaf still arrives as plain INT64 and is ordered
                // correctly by the int64 arm above. Adding arms for child types
                // nothing can produce would be unreachable code; each would also
                // need its descriptor (dec_precision/dec_scale, or the TIME unit)
                // threaded through ctx, as TIMESTAMP64's unit is.
                //
                // INTERVAL is genuinely not totally orderable: {int64 months,
                // int64 us} has no defined order between e.g. 1 month and 30 days.
                //
                // ARRAY (nested) needs a recursive element comparator — accepted as
                // a fair failure by the architect (2026-07-16).
                return draken_error_sentinel_fmt(
                    "draken_sort: element type %d not supported (numeric / BOOL / "
                    "string-family only)", (int)child->type);
        }

        // Parent: fresh offsets (row span LENGTHS unchanged by sort — the values
        // moved within each span, not across spans), validity copied verbatim
        // (whether a row is null/empty is a property of the ROW, untouched by
        // sorting its contents).
        int32_t* out_offsets = static_cast<int32_t*>(
            draken_malloc((static_cast<size_t>(n) + 1u) * sizeof(int32_t)));
        if (!out_offsets) { delete child_r; throw std::bad_alloc(); }
        std::memcpy(out_offsets, new_offsets.data(), (static_cast<size_t>(n) + 1u) * sizeof(int32_t));

        uint8_t* out_validity = nullptr;
        if (parent->validity != nullptr) {
            const uint32_t vbytes = (n + 7u) >> 3;
            const uint32_t padded = (vbytes + 7u) & ~7u;
            out_validity = static_cast<uint8_t*>(draken_malloc(padded > 0u ? padded : 8u));
            if (!out_validity) { draken_free(out_offsets); delete child_r; throw std::bad_alloc(); }
            std::memset(out_validity, 0xFFu, padded > 0u ? padded : 8u);
            for (uint32_t i = 0; i < n; ++i)
                if (!aj_row_valid(parent, i))
                    out_validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
        }

        VecResult r{};
        r.data           = out_offsets;
        r.validity       = out_validity;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_ARRAY;
        r.flags          = DRAKEN_SEL_IDENTITY;
        r.child          = child_r;
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_sort");
    }
}

// ARRAY_CONTAINS_ANY(arr, needles) -> BOOL. True iff the row shares at least one
// element with the bind-time needle set. See acm_run for null semantics.
VecResult draken_array_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    try {
        return acm_run(ctx, args, nargs, /*want_all=*/false, "draken_array_contains_any");
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_array_contains_any");
    }
}

// ARRAY_CONTAINS_ALL(arr, needles) -> BOOL. True iff EVERY needle appears in the
// row (needles ⊆ row) — note the direction: it is not "every element is a
// needle". An empty row is therefore false for any non-empty needle set, and the
// needle set is never empty (_membership_values rejects that).
VecResult draken_array_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    try {
        return acm_run(ctx, args, nargs, /*want_all=*/true, "draken_array_contains_all");
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_array_contains_all");
    }
}

// ARRAY_CONTAINS(arr, item) -> BOOL, i.e. `item = ANY(arr)`. The item is a
// bind-time LITERAL packed into a one-element in_list_ctx blob (the same vehicle
// ARRAY_CONTAINS_ANY uses), NOT a vector operand — a per-row item column is not
// supported (same as the GIL anyop_eq it replaces). args[0]=parent (offsets),
// args[1]=child (elements, via BC_C_NATIVE_CHILD), nargs==2; item via ctx.
//
// Semantics are the reference SQL `= ANY` (arr_reduce_int64/arr_reduce_string in
// array_reductions.h for kind 0/1; contains_numeric_child/contains_bool_child
// above match that SAME contract for the wider type set), which is THREE-VALUED
// and differs from ARRAY_CONTAINS_ANY (acm_run): a NULL array row -> NULL
// (validity cleared), an empty row -> FALSE, a NULL element is skipped, TRUE iff
// any non-null element equals the item.
//
// Item kinds (in_list_ctx.kind, kernel_context.h):
//   0 int64   -> INT8/16/32/64, UINT8/16/32/64 (range-checked — an
//                out-of-range item is a legitimate FALSE, not an error),
//                BOOL (0/1), TIMESTAMP64 (raw instant, PRE-QUANTIZED to the
//                array's own storage unit at bind time by
//                compiled_expression.pyx — the kernel does a plain int64
//                compare, no runtime unit ctx needed).
//   1 string  -> VARCHAR/NVARCHAR/VARBINARY/VARIANT, via arr_reduce_string
//                directly (bypassing arr_any_eq's VARCHAR-only dispatch) so
//                the whole string family works from one call, no duplicated
//                logic.
//   2 float64 -> FLOAT32/FLOAT64.
// DECIMAL and DATE32 array elements are absent: DECIMAL arrays are unreachable
// (rugo cannot decode list<decimal>, matching draken_sort/array_reduce's same
// finding); DATE32 arrays decode but the leaf is not yet retagged from raw
// INT32 (a pre-existing gap shared with GREATEST/LEAST/SORT, not something
// this kernel can paper over — see array_native_kernel_four_walls).
VecResult draken_array_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    try {
        if (!args || nargs != 2u || !args[0] || !args[1])
            return draken_error_sentinel(
                "draken_array_contains: expects (array, child) — 2 arguments; a "
                "non-column array operand has no resolvable child on this kernel");
        if (!ctx)
            return draken_error_sentinel("draken_array_contains: missing bind-time item ctx");
        const DrakenVector* parent = args[0];
        const DrakenVector* child  = args[1];
        if (parent->type != DRAKEN_ARRAY)
            return draken_error_sentinel("draken_array_contains: operand must be ARRAY");

        const auto* c = static_cast<const in_list_ctx*>(ctx);
        const uint8_t* payload = reinterpret_cast<const uint8_t*>(c) + sizeof(in_list_ctx);

        if (c->kind == 0) {
            int64_t v;
            std::memcpy(&v, payload, sizeof(int64_t));
            switch (child->type) {
                case DRAKEN_INT8:
                    return numeric_item_fits<int8_t>(v)
                        ? contains_numeric_child<int8_t>(parent, child, static_cast<int8_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_INT16:
                    return numeric_item_fits<int16_t>(v)
                        ? contains_numeric_child<int16_t>(parent, child, static_cast<int16_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_INT32:
                    return numeric_item_fits<int32_t>(v)
                        ? contains_numeric_child<int32_t>(parent, child, static_cast<int32_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_INT64:
                case DRAKEN_TIMESTAMP64:
                    return contains_numeric_child<int64_t>(parent, child, v);
                case DRAKEN_UINT8:
                    return numeric_item_fits<uint8_t>(v)
                        ? contains_numeric_child<uint8_t>(parent, child, static_cast<uint8_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_UINT16:
                    return numeric_item_fits<uint16_t>(v)
                        ? contains_numeric_child<uint16_t>(parent, child, static_cast<uint16_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_UINT32:
                    return numeric_item_fits<uint32_t>(v)
                        ? contains_numeric_child<uint32_t>(parent, child, static_cast<uint32_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_UINT64:
                    return numeric_item_fits<uint64_t>(v)
                        ? contains_numeric_child<uint64_t>(parent, child, static_cast<uint64_t>(v))
                        : contains_never_match(parent);
                case DRAKEN_BOOL:
                    return contains_bool_child(parent, child, v != 0);
                default:
                    return draken_error_sentinel_fmt(
                        "draken_array_contains: integer item but array elements are type %d "
                        "(supported: int/uint family, BOOL, TIMESTAMP)", (int)child->type);
            }
        }
        if (c->kind == 2) {
            double v;
            std::memcpy(&v, payload, sizeof(double));
            switch (child->type) {
                case DRAKEN_FLOAT32:
                    return contains_numeric_child<float>(parent, child, static_cast<float>(v));
                case DRAKEN_FLOAT64:
                    return contains_numeric_child<double>(parent, child, v);
                default:
                    return draken_error_sentinel_fmt(
                        "draken_array_contains: float item but array elements are type %d "
                        "(supported: FLOAT32/FLOAT64)", (int)child->type);
            }
        }
        // kind 1: string item — [u32 len][bytes]. Works for any string-family child.
        if (child->type != DRAKEN_VARCHAR && child->type != DRAKEN_NVARCHAR
                && child->type != DRAKEN_VARBINARY && child->type != DRAKEN_VARIANT)
            return draken_error_sentinel_fmt(
                "draken_array_contains: string item but array elements are type %d",
                (int)child->type);
        uint32_t len;
        std::memcpy(&len, payload, 4);
        const uint8_t* bytes = payload + 4u;
        // arena_offset == 0 → str_data(&slot, bytes) returns bytes directly, so the
        // item bytes double as the "arena" for the scalar slot (same trick the
        // nanobind build_scalar uses).
        DrakenStringSlot slot_storage;
        draken_build_string_slot(&slot_storage, bytes, len, 0);
        return draken::ops::arr_reduce_string<draken::ops::ArrStrEq, false>(
            *parent, *child, &slot_storage, bytes);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_array_contains");
    }
}

// GREATEST(arr) -> element scalar. Per-row MAX element of an ARRAY column. Unary
// ARRAY reducer (NOT a variadic scalar). args[0]=parent (offsets), args[1]=child
// (elements) — the synthetic BC_C_NATIVE_CHILD arg the VM appends. See
// array_reduce for null / empty-row / NaN semantics.
VecResult draken_greatest(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    try {
        return array_reduce(ctx, args, nargs, /*want_max=*/true, "draken_greatest");
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_greatest");
    }
}

// LEAST(arr) -> element scalar. Per-row MIN element of an ARRAY column.
VecResult draken_least(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    try {
        return array_reduce(ctx, args, nargs, /*want_max=*/false, "draken_least");
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_least");
    }
}

// SPLIT(string, delimiter[, limit]) -> ARRAY<VARIANT>. Splits each string by a
// scalar (literal) delimiter with Python str.split(sep, maxsplit) semantics:
// separators are not collapsed, an empty string yields one empty part, and after
// `limit` splits the remainder is one trailing part. A NULL string row (or a
// NULL delimiter) yields a NULL array row. args[1]/args[2] are scalar literals.
VecResult draken_split(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    (void)ctx;
    try {
        if (!args || (nargs != 2u && nargs != 3u) || !args[0] || !args[1])
            return draken_error_sentinel(
                "draken_split: expects (string, delimiter[, limit])");
        const DrakenVector* str   = args[0];
        const DrakenVector* delim = args[1];
        if (!sp_is_string(str->type))
            return draken_error_sentinel("draken_split: first operand must be a string");
        if (!sp_is_string(delim->type))
            return draken_error_sentinel("draken_split: delimiter must be a string");
        if (delim->data_length != 1u)
            return draken_error_sentinel(
                "draken_split: delimiter must be a scalar literal "
                "(per-row delimiter not supported natively)");

        const auto* sd = static_cast<const DrakenStringArena*>(delim->data);
        const bool delim_null = (delim->length == 0u) || !aj_row_valid(delim, 0u);
        const uint32_t dphys = delim_null ? 0u : delim->selection[0];
        const DrakenStringSlot* dslot = &sd->slots[dphys];
        const uint8_t* dbytes = str_data(dslot, sd->arena);
        const uint32_t dlen   = str_length(dslot);
        if (!delim_null && dlen == 0u)
            return draken_error_sentinel("draken_split: empty delimiter not supported");

        int64_t maxsplit = -1;
        if (nargs == 3u) {
            const DrakenVector* lim = args[2];
            if (lim->data_length != 1u)
                return draken_error_sentinel("draken_split: limit must be a scalar literal");
            if (lim->length > 0u && aj_row_valid(lim, 0u)) {
                int64_t lv;
                if (!acm_elem_int64(lim, lim->selection[0], lv))
                    return draken_error_sentinel("draken_split: limit must be an integer");
                if (lv < 1)
                    return draken_error_sentinel("draken_split: limit must be greater than 0");
                maxsplit = lv;
            }
            // NULL limit -> unlimited (maxsplit stays -1).
        }

        const uint32_t n = str->length;
        const auto* ss = static_cast<const DrakenStringArena*>(str->data);

        std::vector<int32_t> new_offsets(static_cast<size_t>(n) + 1u, 0);
        std::vector<std::pair<const uint8_t*, uint32_t>> parts;
        std::vector<uint8_t> row_null(n > 0u ? n : 1u, 0u);
        bool any_null = false;

        for (uint32_t i = 0; i < n; ++i) {
            if (delim_null || !aj_row_valid(str, i)) {
                new_offsets[i + 1] = new_offsets[i];
                row_null[i] = 1u;
                any_null = true;
                continue;
            }
            const uint32_t phys = str->selection[i];
            const DrakenStringSlot* slot = &ss->slots[phys];
            const uint8_t* hb = str_data(slot, ss->arena);
            const uint32_t hl = str_length(slot);
            const size_t before = parts.size();
            sp_split_one(hb, hl, dbytes, dlen, maxsplit, parts);
            new_offsets[i + 1] =
                new_offsets[i] + static_cast<int32_t>(parts.size() - before);
        }

        // Flattened child: every part, never null. The parts are substrings of the
        // input, so the element type is FIXED and known — it is the input's own
        // string type (VARCHAR/NVARCHAR/VARBINARY), never VARIANT. Tagging them
        // VARIANT threw away a type the kernel already had and left the child
        // untypable downstream (VARIANT has no gather/compare/cast path, so a
        // SPLIT result could not survive an ORDER BY, join, or GROUP BY).
        const uint32_t total = static_cast<uint32_t>(parts.size());
        StringRows rows;
        rows.length = total;
        rows.type   = str->type;
        rows.slots  = sr_alloc_slots(total);
        struct RowsGuard {
            StringRows* r; bool released = false;
            ~RowsGuard() { if (!released && r) sr_free(*r); }
        } rg{&rows};
        std::vector<uint8_t> arena_buf;
        for (uint32_t p = 0; p < total; ++p) {
            const uint8_t* bytes = parts[p].first;
            const uint32_t len   = parts[p].second;
            if (len <= STR_INLINE_MAX) {
                str_init_inline(&rows.slots[p], bytes, len);
            } else {
                const uint32_t off = static_cast<uint32_t>(arena_buf.size());
                arena_buf.insert(arena_buf.end(), bytes, bytes + len);
                draken_build_string_slot(&rows.slots[p], arena_buf.data() + off, len, off);
            }
        }
        rows.arena_len = arena_buf.size();
        if (rows.arena_len > 0u) {
            rows.arena = static_cast<uint8_t*>(draken_malloc(rows.arena_len));
            if (!rows.arena) throw std::bad_alloc();
            std::memcpy(rows.arena, arena_buf.data(), rows.arena_len);
        }
        rg.released = true;
        VecResult* child = finalize_child(rows, "split");   // owns rows' buffers now

        // Parent ARRAY: offsets (+ validity), child.
        int32_t* out_offsets = static_cast<int32_t*>(
            draken_malloc((static_cast<size_t>(n) + 1u) * sizeof(int32_t)));
        if (!out_offsets) { delete child; throw std::bad_alloc(); }
        std::memcpy(out_offsets, new_offsets.data(),
                    (static_cast<size_t>(n) + 1u) * sizeof(int32_t));

        uint8_t* validity = nullptr;
        if (any_null) {
            validity = ar_alloc_validity(n);
            for (uint32_t i = 0; i < n; ++i)
                if (row_null[i])
                    validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
        }

        VecResult r{};
        r.data           = out_offsets;
        r.validity       = validity;
        r.selection      = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_ARRAY;
        r.flags          = DRAKEN_SEL_IDENTITY;
        r.child          = child;
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_split");
    }
}

// CAST(<json text> AS ARRAY<element_type>) — VARCHAR/VARIANT -> ARRAY.
//
// Declared in cast_kernels.h; lives here rather than in cast_dispatch.cpp because
// every helper it needs (yyjson, StringRows staging, finalize_child) is already in
// this TU — the same reason draken_split does. It is the RESULT-half mirror of the
// ARRAY->VARCHAR cast: the elements ride out on VecResult::child, exactly as
// JSONB_OBJECT_KEYS and SPLIT do.
//
// Row dispositions:
//   NULL input row        -> NULL output row (empty span). Not a failure.
//   invalid JSON          -> row FAILS
//   root is not an array  -> row FAILS  (a JSON object or bare scalar is NOT
//                            silently wrapped into a 1-element array)
//   element won't coerce  -> row FAILS  (whole row, never a per-element NULL)
//   JSON null element     -> NULL element; the row survives
// A failed row raises under a plain `::` cast, or becomes a NULL row under
// TRY_CAST — cast_array_ctx::safe selects which, and nothing else differs.
//
// NOTE: explicit try/catch, not DRAKEN_KERNEL_TRY — that macro takes the body as
// ONE argument and the preprocessor does not protect commas inside braces, so a
// brace-init like `pg{offsets, nullptr}` would split it. Same trap as
// jsonb_object_keys above.
VecResult draken_cast_to_array(void* ctx, const DrakenVector* vector) {
    try {
        if (!ctx)
            return draken_error_sentinel("draken_cast_to_array: missing cast_array_ctx");
        if (!vector)
            return draken_error_sentinel("draken_cast_to_array: null operand");

        const cast_array_ctx* cc = static_cast<const cast_array_ctx*>(ctx);
        const DrakenType elem    = static_cast<DrakenType>(cc->element_type);
        const bool       safe    = cc->safe != 0;
        const CtaKind    kind    = cta_kind_of(elem);

        if (kind == CtaKind::Unsupported)
            return draken_error_sentinel_fmt(
                "CAST to ARRAY: unsupported element type (tag %d)", (int)elem);
        if (!aj_is_string_family(vector->type) && vector->type != DRAKEN_VARIANT)
            return draken_error_sentinel_fmt(
                "CAST to ARRAY: operand must be VARCHAR or VARIANT holding JSON array "
                "text (got type tag %d)", (int)vector->type);

        const uint32_t n  = vector->length;
        const auto*    sa = static_cast<const DrakenStringArena*>(vector->data);

        const size_t off_bytes = (static_cast<size_t>(n) + 1u) * sizeof(int32_t);
        int32_t* offsets = static_cast<int32_t*>(draken_malloc(off_bytes));
        if (!offsets) throw std::bad_alloc();
        struct ParentGuard {
            int32_t* off; uint8_t* val; bool released = false;
            ~ParentGuard() { if (!released) { draken_free(off); draken_free(val); } }
        } pg{offsets, nullptr};
        offsets[0] = 0;

        // Element staging. Fixed-width kinds accumulate raw target-width bytes;
        // BOOL accumulates one 0/1 byte per element (packed to bits at the end);
        // the string family stages owned copies. `elem_null` is one byte per
        // staged element, parallel to all three.
        std::vector<uint8_t>     fixed_buf;
        std::vector<uint8_t>     bool_buf;
        std::vector<std::string> staged;
        std::vector<uint8_t>     elem_null;
        bool any_elem_null = false;

        std::vector<uint8_t> row_null(n, 0u);
        bool any_row_null = false;

        for (uint32_t i = 0u; i < n; ++i) {
            if (!sr_row_is_valid(vector, i)) {
                row_null[i]     = 1u;
                any_row_null    = true;
                offsets[i + 1u] = offsets[i];
                continue;
            }

            // Rewind marks — a row that fails midway must leave no partial elements.
            const size_t mark_fixed = fixed_buf.size();
            const size_t mark_bool  = bool_buf.size();
            const size_t mark_str   = staged.size();
            const size_t mark_null  = elem_null.size();

            const DrakenStringSlot* slot = &sa->slots[vector->selection[i]];
            const uint8_t* json_bytes    = str_data(slot, sa->arena);
            const uint32_t json_len      = str_length(slot);

            const char* why = nullptr;   // non-null => this row failed
            int32_t emitted = 0;

            yyjson_read_err perr;
            yyjson_doc* raw = yyjson_read_opts(
                const_cast<char*>(reinterpret_cast<const char*>(json_bytes)),
                static_cast<size_t>(json_len), 0u, nullptr, &perr);
            AjDocGuard guard{raw};

            if (!raw) {
                why = "invalid JSON";
            } else {
                yyjson_val* root = yyjson_doc_get_root(raw);
                if (!root || !yyjson_is_arr(root)) {
                    why = "value is not a JSON array";
                } else {
                    yyjson_arr_iter it;
                    yyjson_arr_iter_init(root, &it);
                    yyjson_val* el = nullptr;
                    while ((el = yyjson_arr_iter_next(&it)) != nullptr) {
                        if (yyjson_is_null(el)) {
                            // Absent, not wrong-typed: a NULL element, row survives.
                            if (kind == CtaKind::String)     staged.emplace_back();
                            else if (kind == CtaKind::Bool)  bool_buf.push_back(0u);
                            else fixed_buf.insert(fixed_buf.end(), cta_width_of(elem), 0u);
                            elem_null.push_back(1u);
                            any_elem_null = true;
                            ++emitted;
                            continue;
                        }
                        bool ok;
                        switch (kind) {
                            case CtaKind::SInt:  ok = cta_coerce_sint(el, elem, fixed_buf);  break;
                            case CtaKind::UInt:  ok = cta_coerce_uint(el, elem, fixed_buf);  break;
                            case CtaKind::Float: ok = cta_coerce_float(el, elem, fixed_buf); break;
                            case CtaKind::Bool:
                                ok = yyjson_is_bool(el);
                                if (ok) bool_buf.push_back(yyjson_get_bool(el) ? 1u : 0u);
                                break;
                            default: {
                                std::string s;
                                ok = cta_coerce_string(el, elem, s);
                                if (ok) staged.emplace_back(std::move(s));
                                break;
                            }
                        }
                        if (!ok) { why = "element does not match the declared element type"; break; }
                        elem_null.push_back(0u);
                        ++emitted;
                    }
                }
            }

            if (why != nullptr) {
                if (!safe)
                    throw std::runtime_error(
                        "CAST to ARRAY: row " + std::to_string(i) + ": " + why +
                        " (use TRY_CAST to null such rows instead)");
                // TRY_CAST: discard anything this row staged, emit a NULL row.
                fixed_buf.resize(mark_fixed);
                bool_buf.resize(mark_bool);
                staged.resize(mark_str);
                elem_null.resize(mark_null);
                row_null[i]     = 1u;
                any_row_null    = true;
                offsets[i + 1u] = offsets[i];
                continue;
            }
            offsets[i + 1u] = offsets[i] + emitted;
        }

        const uint32_t total = static_cast<uint32_t>(elem_null.size());

        // Child validity — one bit per staged element, set = valid.
        uint8_t* child_validity = nullptr;
        if (any_elem_null) {
            const uint32_t bm     = (total + 7u) >> 3;
            const uint32_t padded = (bm + 7u) & ~7u;
            const size_t   vbytes = padded > 0u ? padded : 8u;
            child_validity = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!child_validity) throw std::bad_alloc();
            std::memset(child_validity, 0xFFu, vbytes);
            for (uint32_t k = 0u; k < total; ++k)
                if (elem_null[k])
                    child_validity[k >> 3] &= ~static_cast<uint8_t>(1u << (k & 7u));
        }

        VecResult* child = nullptr;
        if (kind == CtaKind::String) {
            StringRows rows;
            rows.length = total;
            rows.type   = elem;
            rows.slots  = sr_alloc_slots(total);
            struct RowsGuard {
                StringRows* r; bool released = false;
                ~RowsGuard() { if (!released && r) sr_free(*r); }
            } rg{&rows};

            std::vector<uint8_t> arena_buf;
            for (uint32_t k = 0u; k < total; ++k) {
                if (elem_null[k]) { sr_mark_null(rows, k); continue; }
                const std::string& s = staged[k];
                const uint32_t len   = static_cast<uint32_t>(s.size());
                if (len <= STR_INLINE_MAX) {
                    str_init_inline(&rows.slots[k],
                                    reinterpret_cast<const uint8_t*>(s.data()), len);
                } else {
                    const uint32_t off = static_cast<uint32_t>(arena_buf.size());
                    arena_buf.insert(arena_buf.end(), s.data(), s.data() + len);
                    // insert() may reallocate — re-derive the pointer for the hash.
                    draken_build_string_slot(&rows.slots[k], arena_buf.data() + off, len, off);
                }
            }
            rows.arena_len = arena_buf.size();
            if (rows.arena_len > 0u) {
                rows.arena = static_cast<uint8_t*>(draken_malloc(rows.arena_len));
                if (!rows.arena) { draken_free(child_validity); throw std::bad_alloc(); }
                std::memcpy(rows.arena, arena_buf.data(), rows.arena_len);
            }
            // sr_mark_null already built the child's validity into `rows`; the
            // standalone bitmap is redundant on this path.
            draken_free(child_validity);
            child_validity = nullptr;
            rg.released = true;              // finalize_child consumes rows' buffers
            child = finalize_child(rows, "cast_to_array");
        } else {
            const uint32_t w = (kind == CtaKind::Bool) ? 0u : cta_width_of(elem);
            void* cdata = nullptr;
            if (kind == CtaKind::Bool) {
                const uint32_t bm     = (total + 7u) >> 3;
                const uint32_t padded = (bm + 7u) & ~7u;
                const size_t   cbytes = padded > 0u ? padded : 8u;
                uint8_t* bits = static_cast<uint8_t*>(draken_malloc(cbytes));
                if (!bits) { draken_free(child_validity); throw std::bad_alloc(); }
                std::memset(bits, 0, cbytes);
                for (uint32_t k = 0u; k < total; ++k)
                    if (bool_buf[k])
                        bits[k >> 3] |= static_cast<uint8_t>(1u << (k & 7u));
                cdata = bits;
            } else {
                const size_t cbytes = static_cast<size_t>(total) * w;
                void* buf = draken_malloc(cbytes > 0u ? cbytes : 8u);
                if (!buf) { draken_free(child_validity); throw std::bad_alloc(); }
                if (cbytes > 0u) std::memcpy(buf, fixed_buf.data(), cbytes);
                cdata = buf;
            }
            VecResult cr{};
            cr.data           = cdata;
            cr.validity       = child_validity;
            cr.selection      = draken_identity_sel(total);
            cr.owns_selection = false;
            cr.data_length    = total;
            cr.length         = total;
            cr.type           = elem;
            cr.flags          = DRAKEN_SEL_IDENTITY;
            child             = new VecResult(cr);
            child_validity    = nullptr;      // the child owns it now
        }

        // Parent validity — logical-row indexed, bit set = valid.
        uint8_t* validity = nullptr;
        if (any_row_null) {
            const uint32_t bm     = (n + 7u) >> 3;
            const uint32_t padded = (bm + 7u) & ~7u;
            const size_t   vbytes = padded > 0u ? padded : 8u;
            validity = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!validity) { delete child; throw std::bad_alloc(); }
            std::memset(validity, 0xFFu, vbytes);
            for (uint32_t i = 0u; i < n; ++i)
                if (row_null[i]) validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            pg.val = validity;
        }

        VecResult r{};
        r.data           = offsets;
        r.validity       = validity;
        r.selection      = draken_identity_sel(n);   // global; not owned
        r.owns_selection = false;
        r.data_length    = n;
        r.length         = n;
        r.type           = DRAKEN_ARRAY;
        r.flags          = DRAKEN_SEL_IDENTITY;
        r.child          = child;
        pg.released      = true;                     // r owns offsets + validity now
        return r;
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("Unknown error in draken_cast_to_array");
    }
}

}  // extern "C"
