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

// One column: the 40-byte POD view (uniform data[selection[i]] access) plus a
// shared owner that keeps the underlying buffers alive. `view` mirrors `own->vec`;
// it is duplicated inline so the hot path reads the POD without chasing the
// shared_ptr. For a constant/zero-column morsel `own` may be null.
struct CxxColumn {
    DrakenVector                 view;
    std::shared_ptr<VectorOwner> own;
};

// A morsel: owned columns + names. Move-only (exclusive ownership of the column
// list; the bytes themselves are shared via each column's shared_ptr).
struct CxxMorsel {
    std::vector<CxxColumn>   columns;
    std::vector<std::string> names;       // column identities (bytes), one per column
    uint32_t                 zero_col_rows = 0;  // row count when columns.empty()

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
