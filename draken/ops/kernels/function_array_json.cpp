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
// ARRAY_CONTAINS is not in this file by design — it is lowered at plan-build time
// to `item = ANY(arr)` (AnyOpEq, already native); its Python impl is a fail-loud
// guard for a bypassed rewrite. Registering draken_array_contains would silence
// that guard.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"            // draken_identity_sel
#include "ops/float_ops.h"                // fp_total_lt — Draken's canonical NaN-highest order
#include "ops/string_result.h"            // StringRows + sr_* helpers
#include "ops/vec_result.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"   // in_list_ctx — ARRAY_CONTAINS_ANY/ALL needles
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

}  // extern "C"
