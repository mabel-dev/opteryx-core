#pragma once
// draken/morsels/cxx_morsel_ops.h — nogil C++ morsel-op surface over CxxMorsel (S0).
//
// These are the operations the converted operators will call directly on a C++
// morsel, with NO PyObject and NO GIL. They read columns via the uniform
// data[selection[i]] path (CxxColumn.view), so dense/constant/dict all work, and
// build results as shared_ptr<VectorOwner> (RAII, GIL-free free).

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>

#include "morsels/cxx_morsel.h"
#include "core/vector_owner.h"

// Column subset by name, in the requested order (mirrors Morsel.select). Shares
// the underlying owners (no copy) — pure container op. A name not present is
// skipped. If the result has no columns, the row count is carried over.
static inline CxxMorsel cxx_select(const CxxMorsel& m, const std::vector<std::string>& want) {
    CxxMorsel out;
    out.columns.reserve(want.size());
    out.names.reserve(want.size());
    for (const std::string& w : want) {
        for (size_t i = 0; i < m.names.size(); ++i) {
            if (m.names[i] == w) {
                out.columns.push_back(CxxColumn{m.columns[i].view, m.columns[i].own});
                out.names.push_back(m.names[i]);
                break;
            }
        }
    }
    if (out.columns.empty()) out.zero_col_rows = m.num_rows();
    return out;
}
