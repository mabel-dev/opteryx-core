#pragma once
// draken/morsels/cxx_morsel.h — the C++-first morsel/column types (S0).
//
// See docs/M4_CPP_MORSEL_DESIGN.md. This is the genuine C++ object that will flow
// through the operator chain once operators are converted (S1+); the Python
// Morsel/Vector become boundary-only shims. No PyObject lives here: columns own
// their bytes via shared_ptr<VectorOwner> (RAII, GIL-free free for natively-built
// morsels; shared so joins/exchange can fan one column into several outputs).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "core/buffers.h"        // DrakenVector
#include "core/vector_owner.h"   // VectorOwner
#include "logical_type.h"        // LogicalType / logical_type_intern

// One column: the 40-byte POD view (uniform data[selection[i]] access) plus a
// shared owner that keeps the underlying buffers alive. `view` mirrors `own->vec`;
// it is duplicated inline so the hot path reads the POD without chasing the
// shared_ptr. For a constant/zero-column morsel `own` may be null.
struct CxxColumn {
    DrakenVector                 view;
    std::shared_ptr<VectorOwner> own;
};

// Stream state a morsel carries through the operator chain (S-B). EOS is a valid
// morsel with a flag (no separate PyObject sentinel) so the chain can detect
// end-of-stream nogil. Extensible — add states here without changing the carrier.
enum class MorselState : uint8_t {
    DATA          = 0,   // normal data morsel
    END_OF_STREAM = 1,   // terminal marker; carries no rows
};

// Per-pipeline error context (S-B). nogil operator methods return a status code
// (0 == OK) and, on failure, set code + msg here; the gil boundary (drive loop /
// cursor) raises a Python exception once. This is the C-application error model
// validated by the spike — Cython cdef-class methods cannot propagate C++
// exceptions (`except +` is extern-only), so the chain uses status codes.
struct ErrCtx {
    int         code = 0;        // 0 == OK; non-zero == error
    const char* msg  = nullptr;  // static/owned-elsewhere message; valid at raise time
};

// A morsel: owned columns + names. Move-only (exclusive ownership of the column
// list; the bytes themselves are shared via each column's shared_ptr).
struct CxxMorsel {
    std::vector<CxxColumn>   columns;
    std::vector<std::string> names;       // column identities (bytes), one per column
    uint32_t                 zero_col_rows = 0;  // row count when columns.empty()
    MorselState              state = MorselState::DATA;  // S-B: EOS-as-flag; default DATA

    CxxMorsel() = default;
    CxxMorsel(CxxMorsel&&) noexcept = default;
    CxxMorsel& operator=(CxxMorsel&&) noexcept = default;
    CxxMorsel(const CxxMorsel&) = delete;
    CxxMorsel& operator=(const CxxMorsel&) = delete;

    uint32_t num_rows() const noexcept {
        return columns.empty() ? zero_col_rows : columns.front().view.length;
    }
    size_t num_columns() const noexcept { return columns.size(); }
};

// ARRAY child access for the expression VM: an ARRAY column's elements live in
// the VectorOwner's child_owner subtree, not in the 40-byte parent view. The
// VM's BC_C_NATIVE_CHILD cast path resolves the child per morsel through this
// accessor (the .pxd intentionally hides `own` from Cython). nullptr when the
// column is out of range, unowned, or has no child.
static inline const DrakenVector* cxx_column_child_vec(const CxxMorsel* m,
                                                       uint32_t idx) noexcept {
    if (m == nullptr || idx >= m->columns.size()) return nullptr;
    const CxxColumn& c = m->columns[idx];
    if (!c.own || !c.own->child_owner) return nullptr;
    return &c.own->child_owner->vec;
}

// Retag an INT64 column in place as TIMESTAMP64 with `unit`, the second
// sanctioned C-level reach into `own` (like cxx_column_child_vec above, which
// exists because `own` stays undeclared in the .pxd). The descriptor is
// MANDATORY and hangs off the owner — a TIMESTAMP64 vector with a nullptr
// logical_type is a hard error in draken (vector_owner.h) — so this cannot be
// done through `view` alone.
//
// Payload-preserving by construction: only the tag and the descriptor change.
// `data`, `selection`, `validity`, `length` and `data_length` are untouched, so
// the column keeps its shape (dense/constant/dict) and every row survives —
// INT64 and TIMESTAMP64 share the same 8-byte payload.
//
// Returns false (changing nothing) when the column is out of range, unowned, or
// not INT64: the caller decides whether that is an error, and nothing is left
// half-retagged either way.
static inline bool cxx_column_retag_timestamp64(CxxMorsel* m, uint32_t idx,
                                                uint8_t unit) noexcept {
    if (m == nullptr || idx >= m->columns.size()) return false;
    CxxColumn& c = m->columns[idx];
    if (!c.own || c.view.type != DRAKEN_INT64) return false;
    LogicalType lt;
    lt.kind = LogicalKind::TIMESTAMP;
    lt.unit = static_cast<TimestampUnit>(unit);
    lt.offset_minutes = 0;
    c.own->logical_type = logical_type_intern(lt);
    c.own->vec.type = DRAKEN_TIMESTAMP64;
    c.view.type = DRAKEN_TIMESTAMP64;
    return true;
}

// Approximate in-memory footprint (bytes) of a morsel: the sum of each column
// view's real owned payload (draken_vector_nbytes — offsets/fixed data, string
// arena, and validity), PLUS -- for DRAKEN_ARRAY columns -- the owned child
// subtree (draken_vector_owner_nbytes, recursing through nested arrays via
// c.own->child_owner; unreachable from c.view alone, see buffers.h). The
// C++-substrate twin of Morsel.nbytes; both now do the same array-aware
// accounting. nogil-safe: pure field reads, no allocation.
static inline size_t cxx_morsel_nbytes(const CxxMorsel* m) noexcept {
    if (m == nullptr) return 0u;
    size_t total = 0u;
    for (const CxxColumn& c : m->columns) {
        total += draken_vector_nbytes(&c.view);
        if (c.view.type == DRAKEN_ARRAY && c.own && c.own->child_owner)
            total += draken_vector_owner_nbytes(c.own->child_owner.get());
        // E37: carried key-hash seed buffer (one uint64 per data-element) is
        // owner-held, invisible to the view-only draken_vector_nbytes, so add it
        // here to keep the morsel footprint (and the OOM guard) honest.
        if (c.own && c.own->keyhash_buf)
            total += static_cast<size_t>(c.view.data_length) * sizeof(uint64_t);
    }
    return total;
}
