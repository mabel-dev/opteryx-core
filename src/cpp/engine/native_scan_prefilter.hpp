#pragma once
// src/cpp/engine/native_scan_prefilter.hpp — the pieces a decoding scan Source uses to run
// the WHERE directly above it itself, two-pass: the predicate's columns are decoded first,
// the predicate runs over them plus a row-number column, and the survivors' rows say which
// rows of the other columns to decode. Shared by NativeAvroScanSource
// (docs/AVRO_READER_DESIGN.md §26) and NativeOrcScanSource (docs/ORC_READER_DESIGN.md §6.6).

#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "core/alloc.h"          // draken_malloc
#include "core/vector_alloc.h"   // draken_vector_from_dense
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "native_expression.hpp" // ExprProgram / ExprFilterFn / set_span_error

namespace opteryx::engine {

// Append the row-number column (INT32, 0..n-1, named `name`) the prefilter programs read
// last: after the predicate it says which rows of the morsel survived.
inline void add_prefilter_row_column(CxxMorsel& m, uint32_t n, const char* name) {
    auto* index = static_cast<int32_t*>(draken_malloc(static_cast<size_t>(n ? n : 1) * sizeof(int32_t)));
    if (index == nullptr) throw std::bad_alloc();
    for (uint32_t r = 0; r < n; ++r) index[r] = static_cast<int32_t>(r);
    CxxColumn col;
    col.own = std::make_shared<VectorOwner>(draken_vector_from_dense(index, n, DRAKEN_INT32, nullptr),
                                            OwnedBuffer<void>(index), OwnedBuffer<uint8_t>(nullptr));
    col.view = col.own->vec;
    m.columns.push_back(std::move(col));
    m.names.push_back(name);
}

// Run `prog` over `probe`. Null when no row survives, or on failure with `error` / `code`
// set exactly as the Filter would have reported it (set_span_error).
inline std::unique_ptr<CxxMorsel> run_prefilter(ExprFilterFn fn, const ExprProgram& prog, CxxMorsel& probe,
                                                std::string& error, int& code) {
    CxxMorsel* filtered = nullptr;
    int err_op = 0;
    const char* kernel_msg = nullptr;
    const int rc = fn(prog.instrs, prog.count, &probe, const_cast<int*>(prog.col_idx.data()),
                      const_cast<void**>(prog.lit_dv.data()), nullptr, nullptr, 0,
                      &filtered, &err_op, &kernel_msg);
    if (rc != 0) {
        ErrCtx span;
        set_span_error(span, "ExprFilterOperator: predicate evaluation failed", rc, err_op, kernel_msg);
        error = span.msg;
        code = span.code;
        return nullptr;
    }
    std::unique_ptr<CxxMorsel> kept(filtered);
    if (kept == nullptr || kept->num_rows() == 0) return nullptr;
    return kept;
}

// The surviving rows' numbers: the filtered morsel's last column (§11 access).
inline std::vector<uint32_t> prefilter_survivors(const CxxMorsel& kept) {
    const DrakenVector& iv = kept.columns.back().view;
    const auto* idata = static_cast<const int32_t*>(iv.data);
    std::vector<uint32_t> rows(iv.length);
    for (uint32_t i = 0; i < iv.length; ++i) rows[i] = static_cast<uint32_t>(idata[iv.selection[i]]);
    return rows;
}

}  // namespace opteryx::engine
