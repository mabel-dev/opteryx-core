// draken/morsels/cxx_morsel_ops.cpp — the native morsel ops over CxxMorsel and
// their extern "C" ABI (morsels/cxx_morsel_c.h). Pure C++ (no <Python.h>, no
// nanobind): the native engine calls these per morsel and draken must execute
// without Python (CLAUDE.md §1/§2). Compiled into draken_native.so; the binding
// (draken_native.cpp) reaches the C++ surface through morsels/cxx_morsel_ops.h.

#include <algorithm>   // std::push_heap / std::pop_heap (cxx_ordinal_topn_c)
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>  // std::greater (cxx_ordinal_topn_c)
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner_ops.h"
#include "morsels/cxx_morsel_c.h"
#include "morsels/cxx_morsel_ops.h"
#include "ops/hash.h"                  // draken_hash(_carried_dense), draken_ordinalize, g_ops_table, simd_mix_hash
#include "ops/kernels/cast_kernels.h"  // WP-07: nogil join-key cast (to_float64/to_int64)

using namespace draken::owner_ops;

namespace draken::morsel_ops {

// S0: gather all columns of a CxxMorsel at the given row indices (nogil; reuses
// vector_take_impl — VectorOwner in/out, no PyObject). Returns a new CxxMorsel.
CxxMorsel cxx_take(const CxxMorsel& m, const int32_t* idx, uint32_t n) {
    CxxMorsel out;
    out.names = m.names;
    if (m.columns.empty()) { out.zero_col_rows = n; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_take_impl(*col.own, idx, n));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// CROSS JOIN UNNEST (nogil). Expand ARRAY column `array_idx` into one output row
// per element: every parent row is repeated by its array length and gains the
// flattened element under `target_name`.
//
// ─── THE UNNEST ROW-COUNT RULE — DRAKEN OWNS IT, THIS IS THE STATEMENT ───────
// A parent row contributes exactly as many output rows as its array has
// elements. A NULL array and an empty array both have zero elements, so both
// contribute ZERO output rows — the parent row does not survive the unnest.
// That is INNER semantics: unnest is a fan-out keyed on element count, and a
// count of zero is not a special case to be rescued with a NULL-padded row.
// An OUTER variant (parent survives with a NULL element) is a DIFFERENT
// operator and does not exist here; do not smuggle one in behind a flag.
//
// This is draken's rule to state because draken is where the expansion happens
// and draken must be correct without opteryx. Callers restate it only by
// reference — see src/cpp/engine/native_unnest.hpp and the compiler's
// _compile_unnest. If this paragraph and a caller disagree, this one is right
// and the caller is stale.
// ─────────────────────────────────────────────────────────────────────────────
//
// All parent
// columns (INCLUDING the source ARRAY, which downstream may still reference) are
// replicated via cxx_take; the flattened element column is built by take_child
// over the owner's child subtree. A parent row's array span is read through the
// uniform DrakenVector access (`offsets[selection[i]]`, validity keyed on the
// LOGICAL row) so it is correct for dense/constant/dict array shapes alike.
//
// When the expansion is empty (every row null/empty) the result carries 0 rows;
// the caller drops such a morsel (no output), so the placeholder NULL-typed
// target is never observed downstream. Caller owns the result (cxx_morsel_delete).
//
// `drop_source`: when true the consumed source ARRAY column is REPLACED in place by
// the flattened target (nothing above the unnest reads the raw array — and a
// replicated ARRAY cannot pass a downstream gather_rows join/sort). When false the
// target is APPENDED and the raw array survives, which `SELECT *` needs.
//
// `child_mask`: optional, one byte per LOGICAL child position (0 = drop the
// element, non-zero = keep). NULL applies no filter, which is the shape every
// caller had before pushed filters existed. This is a pushed WHERE on the
// unnested column, and it is deliberately a MASK rather than a predicate: draken
// stays predicate-agnostic, and the caller evaluates whatever it likes with
// whatever kernels it likes (opteryx runs the same compiled bytecode the
// standalone Filter would have run, so folding cannot change an answer).
//
// The mask is applied in pass 1, which is the entire point — a dropped element
// never enters `child_idx` or `parent_idx`, so it is never flattened by
// take_child and its parent row is never replicated for it. Filtering AFTER the
// expansion cannot recover that: the copies are already built. Because
// row-count semantics are element-count semantics (above), an element masked off
// is indistinguishable from an element that was never in the array, and a parent
// row whose every element is masked off contributes zero rows — the same INNER
// rule a NULL or empty array already follows.
static CxxMorsel cxx_unnest(const CxxMorsel& m, uint32_t array_idx,
                            const std::string& target_name, bool drop_source,
                            const uint8_t* child_mask = nullptr) {
    const CxxColumn& arrcol = m.columns[array_idx];
    const DrakenVector& av = arrcol.view;
    const int32_t* offsets = static_cast<const int32_t*>(av.data);
    const uint32_t nrows = av.length;

    // Pass 1: parent logical-row index (with repeats) + flat child index array.
    std::vector<int32_t> parent_idx;
    std::vector<int32_t> child_idx;
    for (uint32_t i = 0u; i < nrows; ++i) {
        if (!row_is_valid(av, i)) continue;              // null array row → no rows
        const uint32_t sel_i = av.selection[i];
        const int32_t start = offsets[sel_i];
        const int32_t end   = offsets[sel_i + 1u];
        for (int32_t j = start; j < end; ++j) {
            // Pushed WHERE on the unnested column: skip the element BEFORE it is
            // recorded, so neither it nor a copy of its parent row is ever built.
            if (child_mask != nullptr && child_mask[j] == 0u) continue;
            parent_idx.push_back(static_cast<int32_t>(i));
            child_idx.push_back(j);
        }
    }
    const uint32_t out_n = static_cast<uint32_t>(parent_idx.size());

    // Flatten the element column into the unnest target BEFORE replicating parents:
    // take_child reads the ORIGINAL child owner (child_idx are physical child
    // positions), independent of the parent gather.
    CxxColumn tc;
    if (out_n > 0u && arrcol.own && arrcol.own->child_owner) {
        tc.own = std::make_shared<VectorOwner>(take_child(*arrcol.own->child_owner, child_idx));
    } else {
        // No elements anywhere → empty target (morsel is dropped by the caller).
        tc.own = std::make_shared<VectorOwner>(make_null_vector(out_n));
    }
    tc.view = tc.own->vec;

    // Replicate the parent columns (ARRAY-aware) by the expanded parent index, then
    // either replace the consumed source array in place or append alongside it. The
    // compiler's _compile_unnest tracks the identical column layout either way.
    //
    // UNDER drop_source THE SOURCE ARRAY IS NEVER REPLICATED. It used to go through
    // the take with everything else and be overwritten on the very next line, which
    // made the operator QUADRATIC in the array length: a parent row holding an
    // N-element array expands to out_n == N rows, and replicating the array column
    // to N rows materialises N copies of the N elements — N² elements built and
    // immediately discarded. Measured on a 635K-block CIDR_AGG result that is a
    // 35 GB peak RSS and a SIGKILL where the un-unnested query needs 319 MB.
    // Skipping the column is not an optimisation of the old path; it is the same
    // answer with the discarded work never done — `tc` lands in exactly the slot
    // the overwrite used to fill, in the same position, under the same name.
    CxxMorsel out;
    out.names = m.names;
    out.columns.reserve(m.columns.size() + (drop_source ? 0u : 1u));
    for (uint32_t ci = 0u; ci < static_cast<uint32_t>(m.columns.size()); ++ci) {
        if (drop_source && ci == array_idx) {
            out.columns.push_back(std::move(tc));   // consumed source: target in place
            continue;
        }
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(
            vector_take_impl(*m.columns[ci].own, parent_idx.data(), out_n));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    // A zero-column parent cannot reach here (the operator rejects an out-of-range
    // array_idx first), so `columns` is never empty and num_rows() reads a real
    // column — but keep parity with cxx_take's row-count carry regardless.
    if (out.columns.empty()) out.zero_col_rows = out_n;
    // `names` is decorative mid-pipeline: the native engine addresses columns
    // POSITIONALLY and several producers leave the vector empty (the join2 probe
    // builds its output by pushing columns only; the sink stamps final_names). It
    // is copied from the input, so it can arrive short of — or empty against — the
    // column list. Restore the one-name-per-column invariant cxx_morsel.h declares
    // BEFORE indexing it, or `names[array_idx]` writes off the end of an empty
    // vector.
    if (out.names.size() != out.columns.size()) out.names.resize(out.columns.size());
    if (drop_source) {
        out.names[array_idx] = target_name;
    } else {
        out.columns.push_back(std::move(tc));
        out.names.push_back(target_name);
    }
    return out;
}

// CROSS JOIN UNNEST over a LITERAL array (nogil). `vals` is a plan-constant
// one-column morsel holding the literal's elements. Every parent row is repeated
// `k = vals.num_rows()` times and the literal is tiled across them, so the output
// is the cartesian product parent x literal — the semantics of
// `T CROSS JOIN UNNEST((a,b,c)) AS x`. Unlike the column form there is no source
// ARRAY column to consume, so the target column is APPENDED. A zero-length literal
// yields 0 rows (caller drops the morsel).
static CxxMorsel cxx_unnest_literal(const CxxMorsel& m, const CxxMorsel& vals,
                                    const std::string& target_name) {
    const uint32_t nrows = m.num_rows();
    const uint32_t k = vals.num_rows();

    std::vector<int32_t> parent_idx;
    std::vector<int32_t> child_idx;
    const size_t total = static_cast<size_t>(nrows) * static_cast<size_t>(k);
    parent_idx.reserve(total);
    child_idx.reserve(total);
    for (uint32_t i = 0u; i < nrows; ++i) {
        for (uint32_t j = 0u; j < k; ++j) {
            parent_idx.push_back(static_cast<int32_t>(i));
            child_idx.push_back(static_cast<int32_t>(j));
        }
    }
    const uint32_t out_n = static_cast<uint32_t>(total);

    CxxMorsel out   = cxx_take(m,    parent_idx.data(), out_n);
    CxxMorsel tiled = cxx_take(vals, child_idx.data(),  out_n);
    // Same invariant repair as cxx_unnest: `names` is decorative mid-pipeline and
    // producers (the join2 probe) leave it empty, so appending the target to an
    // unsized vector would leave one name against N+1 columns.
    if (out.names.size() != out.columns.size()) out.names.resize(out.columns.size());
    out.columns.push_back(std::move(tiled.columns[0]));
    out.names.push_back(target_name);
    return out;
}

// WP-07: cast ONE key column of a CxxMorsel to FLOAT64 (target==0) or INT64
// (target==1) via the phase-9c native cast dispatch kernels (nogil, no PyObject).
// Returns a NEW heap CxxMorsel sharing every other column's owner (shared_ptr
// copy — no data copy), with columns[col_idx] replaced by the cast result. This
// mirrors `_apply_join_key_casts`, which replaces the key column in-morsel so the
// cast value flows to BOTH the join-key hash AND the emitted output. Returns
// nullptr on a cast error (the kernel's VecResult data==nullptr sentinel) so the
// caller surfaces it via ErrCtx; nullptr too if col_idx is out of range.
static CxxMorsel* cxx_cast_column(const CxxMorsel& m, uint32_t col_idx, int target) {
    if (col_idx >= m.columns.size()) return nullptr;
    VecResult r = (target == 0)
        ? draken_cast_to_float64(nullptr, &m.columns[col_idx].view)
        : draken_cast_to_int64(nullptr, &m.columns[col_idx].view);
    if (r.data == nullptr) return nullptr;  // cast-error sentinel
    CxxMorsel* out = new CxxMorsel();
    out->names = m.names;
    out->state = m.state;
    out->zero_col_rows = m.zero_col_rows;
    out->columns.reserve(m.columns.size());
    for (uint32_t i = 0; i < static_cast<uint32_t>(m.columns.size()); ++i) {
        if (i == col_idx) {
            CxxColumn nc;
            nc.own  = std::make_shared<VectorOwner>(vecresult_to_owner(r));
            nc.view = nc.own->vec;
            out->columns.push_back(std::move(nc));
        } else {
            out->columns.push_back(m.columns[i]);  // shares owner, no copy
        }
    }
    return out;
}

// S0: slice a row window from all columns (nogil; reuses vector_slice_impl).
CxxMorsel cxx_slice(const CxxMorsel& m, uint32_t start, uint32_t length) {
    CxxMorsel out;
    out.names = m.names;
    if (m.columns.empty()) { out.zero_col_rows = length; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_slice_impl(*col.own, start, length));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// S1: filter every column by a DRAKEN_BOOL mask (keep rows valid AND true).
// Derives the surviving-row indices ONCE, then type-takes each column via the
// same vector_take_impl cxx_take uses. nogil — no PyObject, shared-owner result.
// Zero-column morsels carry the surviving row count (== mask count_true), which
// matches the PyObject filter_mask path.
CxxMorsel cxx_mask(const CxxMorsel& m, const DrakenVector& mask) {
    CxxMorsel out;
    out.names = m.names;
    std::vector<int32_t> idx_vec = mask_indices(mask);
    const uint32_t n = static_cast<uint32_t>(idx_vec.size());
    if (m.columns.empty()) { out.zero_col_rows = n; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_take_impl(*col.own, idx_vec.data(), n));
        // mask_indices() is strictly increasing (a filter never reorders
        // survivors) — carry ROW_SORTED through, same reasoning as
        // vector_slice_impl/vector_mask_impl above.
        if (col.own->vec.flags & DRAKEN_ROW_SORTED)
            nc.own->vec.flags |= (col.own->vec.flags &
                                  (DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// Clone a scalar literal's single value into a freshly OWNED constant vector of
// `length` rows.
//
// A const-broadcast column must own its value like every other column: the morsel
// owns its vectors and the vectors own their memory. The predicate program's
// literal buffers do not outlive the kernel call (see ExprFilterFn's out-param
// contract in src/cpp/engine/native_expression.hpp), so a column that merely
// POINTED at `scalar->data` dangled as soon as the stream advanced — reading a
// filtered morsel after the next pull segfaulted, for any equality predicate
// (`WHERE name = 'Earth'`). It went unnoticed because the cursor sliced every
// morsel before buffering it, and slicing copies.
//
// Returns false when the value cannot be cloned here — a null-carrying literal
// (the broadcast contract is a never-null literal, and a 1-row validity bitmap
// has no meaning spread over n rows) or a type with no flat width. The caller
// then gathers that column the ordinary way: always correct, only slower.
static bool clone_scalar_constant(const DrakenVector& scalar, uint32_t length,
                                  std::shared_ptr<VectorOwner>& out) {
    if (scalar.validity != nullptr || scalar.data == nullptr) return false;
    const uint32_t src_idx = (scalar.selection != nullptr) ? scalar.selection[0] : 0u;

    if (draken_type_is_string_storage(scalar.type)) {
        const DrakenStringArena* src_sa =
            static_cast<const DrakenStringArena*>(scalar.data);
        if (src_sa->slots == nullptr) return false;
        const DrakenStringSlot* src_slot = &src_sa->slots[src_idx];
        const bool inln = str_is_inline(src_slot) != 0;
        if (!inln && src_slot->ext.arena_offset == STR_ELIDED_PAYLOAD_OFFSET) return false;
        const size_t arena_ext = inln ? 0u : static_cast<size_t>(src_slot->ext.length);
        // Same single-block layout make_string_constant builds:
        // [DrakenStringArena | DrakenStringSlot[1] | payload]
        constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
        const size_t struct_end =
            (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
        const size_t arena_start = struct_end + sizeof(DrakenStringSlot);
        const size_t total = arena_start + arena_ext;
        uint8_t* block = static_cast<uint8_t*>(draken_malloc(total));
        if (block == nullptr) return false;
        std::memset(block, 0, total);
        OwnedBuffer<void> data_buf(block);
        DrakenStringArena* sa   = reinterpret_cast<DrakenStringArena*>(block);
        DrakenStringSlot*  slot = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
        uint8_t*           arena = (arena_ext > 0u) ? (block + arena_start) : nullptr;
        sa->slots           = slot;
        sa->arena           = arena;
        sa->length          = 1u;
        sa->arena_used      = arena_ext;
        sa->arena_cap       = arena_ext;
        sa->null_bitmap     = nullptr;
        sa->owns_buffers    = 0;
        sa->payloads_elided = 0;
        sa->type            = scalar.type;
        if (arena_ext > 0u) {
            if (src_sa->arena == nullptr) return false;
            std::memcpy(arena, src_sa->arena + src_slot->ext.arena_offset, arena_ext);
        }
        // Verbatim slot copy, rebased to offset 0 (our arena holds this one value).
        str_clone_with_offset(slot, src_slot, 0u);
        DrakenVector v = draken_vector_from_constant(sa, length, scalar.type, nullptr);
        out = std::make_shared<VectorOwner>(v, std::move(data_buf),
                                            OwnedBuffer<uint8_t>(nullptr));
        return true;
    }

    if (scalar.type == DRAKEN_BOOL) {
        // Bit-packed: the single value is one bit; the clone holds it at bit 0.
        uint8_t* b = static_cast<uint8_t*>(draken_malloc(1u));
        if (b == nullptr) return false;
        const uint8_t* src = static_cast<const uint8_t*>(scalar.data);
        b[0] = static_cast<uint8_t>((src[src_idx >> 3] >> (src_idx & 7u)) & 1u);
        DrakenVector v = draken_vector_from_constant(b, length, scalar.type, nullptr);
        out = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(b),
                                            OwnedBuffer<uint8_t>(nullptr));
        return true;
    }

    const size_t width = draken_type_fixed_itemsize(scalar.type);
    if (width == 0u) return false;   // array/fp16/null — gather instead
    uint8_t* buf = static_cast<uint8_t*>(draken_malloc(width));
    if (buf == nullptr) return false;
    std::memcpy(buf, static_cast<const uint8_t*>(scalar.data) +
                     static_cast<size_t>(src_idx) * width, width);
    DrakenVector v = draken_vector_from_constant(buf, length, scalar.type, nullptr);
    out = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(buf),
                                        OwnedBuffer<uint8_t>(nullptr));
    return true;
}

// S1 twin: like cxx_mask, but columns listed in const_col_idx are known (by the
// caller's static analysis of the predicate, e.g. `WHERE col = 7`) to be a single
// literal value on every surviving row. Those columns are broadcast in O(1) from a
// pre-resolved scalar DrakenVector* (data_length == 1, validity == nullptr — a
// never-null literal) instead of being gathered via vector_take_impl and then
// thrown away. The scalar's ONE value is CLONED into the output column's own
// buffer (clone_scalar_constant) — the caller's literal is not kept alive by the
// result, and a column that cannot be cloned falls back to the gather path.
static CxxMorsel cxx_mask_with_consts(const CxxMorsel& m, const DrakenVector& mask,
                                       const int32_t* const_col_idx,
                                       const DrakenVector* const* const_scalar_dv,
                                       uint32_t n_consts) {
    CxxMorsel out;
    out.names = m.names;
    std::vector<int32_t> idx_vec = mask_indices(mask);
    const uint32_t n = static_cast<uint32_t>(idx_vec.size());
    if (m.columns.empty()) { out.zero_col_rows = n; return out; }
    out.columns.reserve(m.columns.size());
    for (uint32_t ci = 0; ci < static_cast<uint32_t>(m.columns.size()); ++ci) {
        const DrakenVector* scalar = nullptr;
        for (uint32_t k = 0; k < n_consts; ++k) {
            if (static_cast<uint32_t>(const_col_idx[k]) == ci) { scalar = const_scalar_dv[k]; break; }
        }
        CxxColumn nc;
        std::shared_ptr<VectorOwner> cloned;
        if (scalar != nullptr && clone_scalar_constant(*scalar, n, cloned)) {
            nc.own = std::move(cloned);
        } else {
            nc.own = std::make_shared<VectorOwner>(
                vector_take_impl(*m.columns[ci].own, idx_vec.data(), n));
            // mask_indices() is strictly increasing — see cxx_mask above. Not
            // applicable to the const-broadcast branch (that column isn't
            // gathered from source order at all).
            if (m.columns[ci].own->vec.flags & DRAKEN_ROW_SORTED)
                nc.own->vec.flags |= (m.columns[ci].own->vec.flags &
                                      (DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));
        }
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// Rebuild every column of `m` as a fresh, plain-C++-owned VectorOwner (ordinary
// `delete`, never nanobind's `py_deleter`) via an identity take — the same
// vector_take_impl kernel cxx_mask uses, with idx[i]=i instead of a filtered
// mask. Exists solely to strip Python-object ownership from a morsel about to
// cross into a nogil-driven engine Source: cxx_from_vectors_list (below) wraps
// a fresh shared_ptr<VectorOwner> around a Python-owned buffer via nb::cast,
// and nanobind's shared_ptr caster attaches a py_deleter that does
// `gil_scoped_acquire; Py_DECREF` on last-ref-drop — on free-threaded builds
// gil_scoped_acquire attaches but does NOT serialize, so that decref can race a
// concurrent Python-side read on another thread (the StreamingScanSource
// trampoline's production SIGSEGV). Calling this once, right after a pulled
// morsel is assembled and before it is handed to the C++ Source, makes the
// morsel's lifetime pure-C++ regardless of how it was built (single-pass,
// LATMAT/two-pass, or the empty-manifest fallback all funnel through the same
// call site — see _scan_pull_run_inner in opteryx/operators/_operators.pyx).
static CxxMorsel cxx_morsel_materialize_native(const CxxMorsel& m) {
    CxxMorsel out;
    out.names = m.names;
    out.state = m.state;
    if (m.columns.empty()) { out.zero_col_rows = m.zero_col_rows; return out; }
    const uint32_t n = m.num_rows();
    std::vector<int32_t> idx_vec(n);
    for (uint32_t i = 0; i < n; ++i) idx_vec[i] = static_cast<int32_t>(i);
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_take_impl(*col.own, idx_vec.data(), n));
        // idx_vec is the identity permutation (i -> i) — this is ownership-only
        // reshuffling, not a reorder, so a row-sorted source stays row-sorted.
        // Without this, a morsel pulled through the older Python-trampoline scan
        // path (_scan_pull_run_inner in _operators.pyx, which always calls this
        // before the morsel crosses into the C++ engine) would silently lose the
        // flag even when the native scan producer set it.
        if (col.own->vec.flags & DRAKEN_ROW_SORTED)
            nc.own->vec.flags |= (col.own->vec.flags &
                                  (DRAKEN_ROW_SORTED | DRAKEN_ROW_SORTED_DESC));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// Keying hash over the group-key columns of a CxxMorsel → a 1-column CxxMorsel
// holding the INT64 hash vector. Mirrors Morsel.hash_keys EXACTLY: single key →
// shape-preserving (dict→dict) via hash_shaped_impl; multi key → dense mix
// (draken_hash per column + simd_mix_hash into a zeroed buffer). The 1-column
// wrapper lets the engine read columns[0].view and free via cxx_morsel_delete.
// GIL-free. Group keys are never DRAKEN_ARRAY (the binder rejects them).
CxxMorsel cxx_hash(const CxxMorsel& m, const int32_t* col_idxs, uint32_t n_cols) {
    std::shared_ptr<VectorOwner> sp;
    if (n_cols == 1) {
        sp = std::make_shared<VectorOwner>(hash_shaped_impl(*m.columns[col_idxs[0]].own));
    } else {
        const uint32_t n = m.num_rows();
        uint64_t* buf = static_cast<uint64_t*>(
            draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
        if (!buf) throw std::bad_alloc();
        OwnedBuffer<uint64_t> buf_owned(buf);
        std::memset(buf, 0, static_cast<size_t>(n) * sizeof(uint64_t));  // zeroed: required by simd_mix_hash
        if (n > 0u) {
            uint64_t* tmp = static_cast<uint64_t*>(draken_malloc(n * sizeof(uint64_t)));
            if (!tmp) throw std::bad_alloc();
            OwnedBuffer<uint64_t> tmp_owned(tmp);
            for (uint32_t c = 0u; c < n_cols; ++c) {
                const CxxColumn& kc = m.columns[col_idxs[c]];
                // E37: reuse the scan-carried seed for this key column if present.
                if (kc.own && kc.own->keyhash_buf)
                    draken_hash_carried_dense(kc.view, kc.own->keyhash_buf.get(), tmp, n);
                else
                    draken_hash(kc.view, tmp, n);
                simd_mix_hash(buf, tmp, static_cast<size_t>(n));
            }
        }
        VecResult r;
        r.data = buf_owned.release();
        r.validity = nullptr;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length = n;
        r.length = n;
        r.type = DRAKEN_INT64;
        r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        sp = std::make_shared<VectorOwner>(vecresult_to_owner(r));
    }
    CxxMorsel out;
    out.columns.push_back(CxxColumn{sp->vec, sp});
    out.names.push_back(std::string("$keyhash"));
    return out;
}

// Row-routing scatter — partition a morsel into W disjoint sub-morsels by
// hash(group-key) % W. Reuses cxx_hash (the SAME keying hash, so every
// occurrence of a key routes to one bin ⇒ the bins share no keys ⇒ a parallel
// grouped aggregate finalises by concatenation, never a merge) and cxx_take (so
// every column type, strings included, is materialised by the one tested take
// path). The only routing-specific work is the single bucketing pass. GIL-free.
// Group keys are never DRAKEN_ARRAY (the binder rejects them), same as cxx_hash.
//
// Uniform access only (§11): the routing key is read as h[selection[i]] with no
// shape discrimination — a dict-shaped single-key hash routes correctly because
// the read goes through `selection`, never the raw data array.
std::vector<CxxMorsel> cxx_scatter(
        const CxxMorsel& m, const int32_t* col_idxs, uint32_t n_cols, uint32_t W) {
    if (W == 0u) throw std::invalid_argument("cxx_scatter: W must be >= 1");
    if (n_cols == 0u) throw std::invalid_argument("cxx_scatter: no key columns");
    const uint32_t n = m.num_rows();
    std::vector<std::vector<int32_t>> bins(W);
    if (n > 0u) {
        CxxMorsel hashm = cxx_hash(m, col_idxs, n_cols);   // 1-col INT64 hash vector
        const DrakenVector& hv = hashm.columns[0].view;
        const uint64_t* h = static_cast<const uint64_t*>(hv.data);
        const uint32_t* sel = hv.selection;                // never NULL (§11)
        for (uint32_t i = 0u; i < n; ++i)
            bins[h[sel[i]] % W].push_back(static_cast<int32_t>(i));
    }
    std::vector<CxxMorsel> out;
    out.reserve(W);
    for (uint32_t b = 0u; b < W; ++b) {
        const uint32_t bn = static_cast<uint32_t>(bins[b].size());
        out.push_back(cxx_take(m, bn > 0u ? bins[b].data() : nullptr, bn));
    }
    return out;
}
}  // namespace draken::morsel_ops

using namespace draken::morsel_ops;

// ---------------------------------------------------------------------------
// S-B.0(a) — C-ABI transform surface.
//
// Thin `extern "C"` wrappers over the pure-C++ `cxx_*` ops (declared in
// morsels/cxx_morsel_c.h) so the native engine and the Cython operators call them
// at C level (nogil, no PyObject, no nanobind), resolved across the .so boundary
// through the RTLD_GLOBAL load of draken_native. Each returns a heap CxxMorsel
// the caller owns (free via cxx_morsel_delete); the result is move-constructed,
// so the columns' shared_ptrs are shared, not copied.
extern "C" CxxMorsel* cxx_take_c(const CxxMorsel* m, const int32_t* idx, uint32_t n) {
    return new CxxMorsel(cxx_take(*m, idx, n));
}
// CROSS JOIN UNNEST (see cxx_unnest). array_idx is the ARRAY column to expand;
// target_name is the identity (opaque bytes) of the flattened element column
// appended after every replicated parent column. `child_mask` is an optional
// pushed WHERE on the unnested column — one byte per logical child position,
// NULL for no filter. Caller owns the result and must check num_rows(): a 0-row
// result means the batch produced no unnested rows (which a mask that rejects
// everything now also produces, and which the caller already drops).
extern "C" CxxMorsel* cxx_unnest_c(const CxxMorsel* m, uint32_t array_idx,
                                   const char* target_name, uint32_t target_name_len,
                                   int drop_source, const uint8_t* child_mask) {
    return new CxxMorsel(cxx_unnest(*m, array_idx,
                                    std::string(target_name, target_name_len),
                                    drop_source != 0, child_mask));
}
// CROSS JOIN UNNEST over a literal array (see cxx_unnest_literal). `vals` is the
// plan-constant one-column morsel of literal elements. Caller owns the result.
extern "C" CxxMorsel* cxx_unnest_literal_c(const CxxMorsel* m, const CxxMorsel* vals,
                                           const char* target_name, uint32_t target_name_len) {
    if (vals == nullptr || vals->columns.size() != 1u) return nullptr;
    return new CxxMorsel(cxx_unnest_literal(*m, *vals,
                                            std::string(target_name, target_name_len)));
}
extern "C" CxxMorsel* cxx_slice_c(const CxxMorsel* m, uint32_t start, uint32_t length) {
    return new CxxMorsel(cxx_slice(*m, start, length));
}
// WP-07: nogil join-key cast (see cxx_cast_column). target 0=FLOAT64, 1=INT64.
// Returns nullptr on cast error / out-of-range col_idx. Caller owns the result.
extern "C" CxxMorsel* cxx_cast_column_c(const CxxMorsel* m, uint32_t col_idx, int target) {
    return cxx_cast_column(*m, col_idx, target);
}
extern "C" CxxMorsel* cxx_mask_c(const CxxMorsel* m, const DrakenVector* mask) {
    return new CxxMorsel(cxx_mask(*m, *mask));
}
extern "C" CxxMorsel* cxx_mask_with_consts_c(
        const CxxMorsel* m, const DrakenVector* mask,
        const int32_t* const_col_idx, const DrakenVector* const* const_scalar_dv,
        uint32_t n_consts) {
    return new CxxMorsel(cxx_mask_with_consts(*m, *mask, const_col_idx, const_scalar_dv, n_consts));
}
extern "C" CxxMorsel* cxx_morsel_materialize_native_c(const CxxMorsel* m) {
    return new CxxMorsel(cxx_morsel_materialize_native(*m));
}
// S-B.2: select/reorder columns by identity name (bytes → ptr+len arrays, since
// identity names are opaque bytes). Pure container op (shares owners, no copy).
extern "C" CxxMorsel* cxx_select_c(const CxxMorsel* m, const char** name_ptrs,
                                   const uint32_t* name_lens, uint32_t n) {
    std::vector<std::string> want;
    want.reserve(n);
    for (uint32_t i = 0; i < n; ++i)
        want.emplace_back(name_ptrs[i], name_lens[i]);
    return new CxxMorsel(cxx_select(*m, want));
}
extern "C" void cxx_morsel_delete(CxxMorsel* m) {
    delete m;
}

// --- TEMP INSTRUMENTATION (OPTERYX_HASH_TIMING) — measure key-hash share. ---
// Gated on the env var; zero cost when unset. Accumulates wall-ns / calls / rows
// across all cxx_hash_c callers (GROUP BY, DISTINCT, native JOIN) and dumps at
// exit. Remove once step-1 numbers are banked.
static std::atomic<uint64_t> g_hash_ns{0}, g_hash_calls{0}, g_hash_rows{0};
static const bool g_hash_timing = [](){
    if (std::getenv("OPTERYX_HASH_TIMING") == nullptr) return false;
    std::atexit([](){
        fprintf(stderr, "[HASH_TIMING] cxx_hash_c: %.3f ms over %llu calls, %llu rows"
                " | E37 carried-seed hits: %llu\n",
                g_hash_ns.load() / 1e6,
                (unsigned long long)g_hash_calls.load(),
                (unsigned long long)g_hash_rows.load(),
                (unsigned long long)g_e37_carried_hits.load());
    });
    return true;
}();

extern "C" CxxMorsel* cxx_hash_c(const CxxMorsel* m, const int32_t* col_idxs, uint32_t n_cols) {
    if (!g_hash_timing) return new CxxMorsel(cxx_hash(*m, col_idxs, n_cols));
    const auto t0 = std::chrono::steady_clock::now();
    CxxMorsel* out = new CxxMorsel(cxx_hash(*m, col_idxs, n_cols));
    const auto t1 = std::chrono::steady_clock::now();
    g_hash_ns.fetch_add((uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count(),
                        std::memory_order_relaxed);
    g_hash_calls.fetch_add(1, std::memory_order_relaxed);
    g_hash_rows.fetch_add(m->num_rows(), std::memory_order_relaxed);
    return out;
}

// ---------------------------------------------------------------------------
// cxx_ordinal_bounds_c — the ORDINAL min/max of one column, over its NON-NULL
// rows. The build-side capture for runtime min/max join filters
// (docs/RUNTIME_MINMAX_FILTER_DESIGN.md).
//
// This lives HERE, not in src/cpp/engine, for the same reason cxx_hash_c does:
// ops/hash.h's dispatch table is `static inline`, so including it in a second
// shared object would give that object its own copy. Routing through one
// extern "C" symbol (resolved via RTLD_GLOBAL, the pattern executor.hpp
// documents for the thread pool) keeps ONE ops table and ONE definition of what
// a value's ordinal is — the same one skene::compute_statistics writes file
// statistics with and Manifest._ordinalize_literal produces plan terms with.
//
// Returns 1 and writes *out_lo/*out_hi only when a bound genuinely exists.
// Returns 0 — meaning "no bound", i.e. PRUNE NOTHING, never "empty" — for:
//   * a type with no ordinalize kernel (DECIMAL128 deliberately has none),
//   * DRAKEN_ARRAY / DRAKEN_VECTOR_FP16 / DRAKEN_NULL,
//   * zero rows, or every row null.
// It never throws: the type check is made against the ops table before
// dispatch, so nothing can propagate across the C ABI.
//
// NULLS ARE EXCLUDED BY VALIDITY, NOT BY SENTINEL. draken_ordinalize writes
// ORDINAL_NULL (== INT64_MIN) for a null row, but INT64_MIN is also the honest
// ordinal of a real INT64 value, so filtering on the sentinel would drop that
// value out of the bound and OVER-PRUNE. The validity bitmap is re-read per row
// instead — the same choice skene/src/statistics.cpp makes, and for the same
// reason.
extern "C" int cxx_ordinal_bounds_c(const CxxMorsel* m, int32_t col_idx,
                                    int64_t* out_lo, int64_t* out_hi) {
    if (m == nullptr || out_lo == nullptr || out_hi == nullptr) return 0;
    if (col_idx < 0 || static_cast<size_t>(col_idx) >= m->columns.size()) return 0;
    const DrakenVector& v = m->columns[static_cast<size_t>(col_idx)].view;
    if (v.type == DRAKEN_ARRAY || v.type == DRAKEN_VECTOR_FP16 || v.type == DRAKEN_NULL)
        return 0;
    const unsigned type_idx = static_cast<unsigned>(v.type);
    if (type_idx >= OpsTable::kSize || g_ops_table().entries[type_idx].ordinalize == nullptr)
        return 0;
    const uint32_t n = v.length;
    if (n == 0u) return 0;

    std::vector<int64_t> ordinals(n);
    draken_ordinalize(v, ordinals.data(), n);

    const uint8_t* validity = v.validity;
    int64_t lo = 0;
    int64_t hi = 0;
    bool any = false;
    for (uint32_t i = 0u; i < n; ++i) {
        if (validity != nullptr && ((validity[i >> 3] >> (i & 7u)) & 1u) == 0u) continue;
        const int64_t o = ordinals[i];
        if (!any) { lo = o; hi = o; any = true; continue; }
        if (o < lo) lo = o;
        if (o > hi) hi = o;
    }
    if (!any) return 0;
    *out_lo = lo;
    *out_hi = hi;
    return 1;
}

// cxx_ordinal_topn_c — keep the n BEST non-null ordinals of one column, across
// calls. The producer half of the Top-N runtime boundary
// (docs/TOPN_RUNTIME_BOUNDARY_DESIGN.md §3): the caller owns the heap and feeds
// it morsel after morsel; once it holds n entries, heap[0] is the n-th best value
// seen so far — the boundary.
//
// Lives here for the same reason as cxx_ordinal_bounds_c above: one ops table,
// one definition of a value's ordinal, the one the file statistics are written in.
//
// State: `heap` is caller-owned with room for min(n, *heap_len + rows) entries
// (this call never writes past that); `*heap_len` is how many are valid, in
// heap order, on entry and on exit. `ascending != 0` keeps the n SMALLEST — a
// max-heap, so heap[0] is the worst kept (the largest). `ascending == 0` keeps
// the n LARGEST — a min-heap, heap[0] the smallest. Once full, a value is
// admitted only when STRICTLY better than heap[0]: an equal value would replace
// the boundary with itself.
//
// Returns 1 when the column has an ordinal (the heap may be unchanged), 0 for a
// type with no ordinalize kernel, ARRAY / VECTOR_FP16 / NULL, or a bad column
// index — with the heap untouched, which the caller treats as "no boundary".
//
// Uniform access only (§11): ordinals come from draken_ordinalize over the
// logical rows, whatever the shape. Nulls are excluded by VALIDITY, not by the
// ORDINAL_NULL sentinel, for the reason cxx_ordinal_bounds_c documents: the
// sentinel is also the honest ordinal of INT64_MIN. The per-thread scratch keeps
// the ordinal buffer from being reallocated on every morsel.
extern "C" int cxx_ordinal_topn_c(const CxxMorsel* m, int32_t col_idx, uint32_t n,
                                  int ascending, int64_t* heap, uint32_t* heap_len) {
    if (m == nullptr || heap == nullptr || heap_len == nullptr) return 0;
    if (col_idx < 0 || static_cast<size_t>(col_idx) >= m->columns.size()) return 0;
    const DrakenVector& v = m->columns[static_cast<size_t>(col_idx)].view;
    if (v.type == DRAKEN_ARRAY || v.type == DRAKEN_VECTOR_FP16 || v.type == DRAKEN_NULL)
        return 0;
    const unsigned type_idx = static_cast<unsigned>(v.type);
    if (type_idx >= OpsTable::kSize || g_ops_table().entries[type_idx].ordinalize == nullptr)
        return 0;
    const uint32_t rows = v.length;
    if (n == 0u || rows == 0u) return 1;

    thread_local std::vector<int64_t> scratch;
    if (scratch.size() < rows) scratch.resize(rows);
    draken_ordinalize(v, scratch.data(), rows);

    const uint8_t* validity = v.validity;
    uint32_t len = *heap_len;
    if (ascending != 0) {
        for (uint32_t i = 0u; i < rows; ++i) {
            if (validity != nullptr && ((validity[i >> 3] >> (i & 7u)) & 1u) == 0u) continue;
            const int64_t o = scratch[i];
            if (len < n) {
                heap[len++] = o;
                std::push_heap(heap, heap + len);
            } else if (o < heap[0]) {
                std::pop_heap(heap, heap + len);
                heap[len - 1u] = o;
                std::push_heap(heap, heap + len);
            }
        }
    } else {
        const std::greater<int64_t> worse_on_top{};
        for (uint32_t i = 0u; i < rows; ++i) {
            if (validity != nullptr && ((validity[i >> 3] >> (i & 7u)) & 1u) == 0u) continue;
            const int64_t o = scratch[i];
            if (len < n) {
                heap[len++] = o;
                std::push_heap(heap, heap + len, worse_on_top);
            } else if (o > heap[0]) {
                std::pop_heap(heap, heap + len, worse_on_top);
                heap[len - 1u] = o;
                std::push_heap(heap, heap + len, worse_on_top);
            }
        }
    }
    *heap_len = len;
    return 1;
}

// ---------------------------------------------------------------------------
// S-B.1a — boundary bridges (Morsel ⇄ shared_ptr<CxxMorsel>).
//
// `CxxMorsel` is move-only; a shallow copy duplicates the `columns` vector, which
// copies each `CxxColumn{view, shared_ptr<VectorOwner> own}` — sharing the column
// OWNERS (refcount++), NOT the bytes. So two CxxMorsels can reference the same
// buffers, kept alive independently of any Python handle. The shallow copy itself
// is cxx_morsel_shallow_into (morsels/cxx_morsel_ops.h), shared with the binding's
// cxx_morsel_to_handle.

extern "C" CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel* m) {
    CxxMorsel* out = new CxxMorsel();
    cxx_morsel_shallow_into(*out, *m);
    return out;
}
// S-B: the end-of-stream marker — a valid (empty) morsel carrying the EOS flag.
extern "C" CxxMorsel* cxx_morsel_new_eos() {
    CxxMorsel* out = new CxxMorsel();
    out->state = MorselState::END_OF_STREAM;
    return out;
}
