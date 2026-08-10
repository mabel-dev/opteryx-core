#pragma once
// src/cpp/engine/native_unnest.hpp — CROSS JOIN UNNEST operator.
//
// Single input, fan-out shape: expand ARRAY column `array_idx`, repeating every
// parent row by its element count and appending the flattened element under
// `target_name`.
//
// Row-count semantics (NULL/empty arrays, INNER vs OUTER) are DRAKEN's rule,
// stated in full above cxx_unnest in draken/draken_native.cpp. Not restated
// here — a second statement is a second thing to drift. The consequence this
// operator must handle: a batch can expand to zero rows, and such a morsel is
// dropped like any fully-filtered one.
//
// The array-aware work (replicate all parent columns incl. the source ARRAY, then
// flatten the child subtree by a raw index array) lives in draken's cxx_unnest_c,
// next to cxx_take_c, where the take/child machinery is. This operator is the thin
// engine-side driver that calls it and wraps the result on the carrier — mirroring
// how the Cython edge already calls cxx_take_c. Symbols resolve at load time from
// draken_native, the same dynamic-lookup path gather_rows' draken helpers use.

#include <memory>
#include <string>

#include "morsels/cxx_morsel.h"   // CxxMorsel, DRAKEN_ARRAY (via core/buffers.h)
#include "operator.hpp"

extern "C" CxxMorsel* cxx_unnest_c(const CxxMorsel* m, uint32_t array_idx,
                                   const char* target_name, uint32_t target_name_len,
                                   int drop_source);
extern "C" CxxMorsel* cxx_unnest_literal_c(const CxxMorsel* m, const CxxMorsel* vals,
                                           const char* target_name, uint32_t target_name_len);
extern "C" void cxx_morsel_delete(CxxMorsel* m);

namespace opteryx::engine {

// Expands one ARRAY column. A single input morsel yields at most one output morsel
// (the whole expansion), so no HAVE_MORE cursor is needed — the batch is already
// bounded by the source's morsel size.
struct UnnestOperator : Operator {
    uint32_t    array_idx;
    std::string target_name;   // identity (opaque bytes) of the flattened column
    // True when nothing above the unnest reads the raw source array: the array is
    // replaced in place by the target. Set by the compiler from projection-pushdown
    // liveness. False keeps the array (SELECT * needs it) — but a surviving ARRAY
    // column then fails loud in a downstream gather_rows join/sort.
    bool        drop_source;

    UnnestOperator(uint32_t idx, std::string name, bool drop)
        : array_idx(idx), target_name(std::move(name)), drop_source(drop) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }

    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        // EOS / empty batch: nothing to expand. Dropped (the pipeline terminates on
        // the source's FINISHED, not on an EOS morsel reaching the sink).
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        if (array_idx >= in->columns.size()) {
            err.code = 1;
            err.msg = "UnnestOperator: array column index out of range — the "
                      "compiler's layout tracking disagrees with the stream";
            return OpResult::NEED_INPUT;
        }
        if (in->columns[array_idx].view.type != DRAKEN_ARRAY) {
            err.code = 1;
            err.msg = "UnnestOperator: CROSS JOIN UNNEST source column is not an ARRAY";
            return OpResult::NEED_INPUT;
        }
        // draken owns the new()/delete() of the result (cxx_morsel_delete), so the
        // shared_ptr frees it in the TU that allocated it — no cross-.so heap mixing.
        MorselPtr result(cxx_unnest_c(in.get(), array_idx, target_name.data(),
                                      static_cast<uint32_t>(target_name.size()),
                                      drop_source ? 1 : 0),
                         cxx_morsel_delete);
        if (!result || result->num_rows() == 0) return OpResult::NEED_INPUT;
        result->state = in->state;
        out = std::move(result);
        return OpResult::EMIT;
    }
};

// CROSS JOIN UNNEST over a LITERAL array: `lit` is a plan-constant one-column
// morsel (materialized once at compile time, same legitimacy as a virtual
// dataset). Each input row is repeated len(lit) times with the literal tiled
// across them, and the target column is APPENDED (there is no source ARRAY column
// to consume). An empty literal yields no rows.
struct UnnestLiteralOperator : Operator {
    MorselPtr   lit;
    std::string target_name;

    UnnestLiteralOperator(MorselPtr literal, std::string name)
        : lit(std::move(literal)), target_name(std::move(name)) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }

    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        MorselPtr result(cxx_unnest_literal_c(in.get(), lit.get(), target_name.data(),
                                              static_cast<uint32_t>(target_name.size())),
                         cxx_morsel_delete);
        if (!result) {
            err.code = 1;
            err.msg = "UnnestLiteralOperator: literal array must be exactly one column";
            return OpResult::NEED_INPUT;
        }
        if (result->num_rows() == 0) return OpResult::NEED_INPUT;
        result->state = in->state;
        out = std::move(result);
        return OpResult::EMIT;
    }
};

}  // namespace opteryx::engine
