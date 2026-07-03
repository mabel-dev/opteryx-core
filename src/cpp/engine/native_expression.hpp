#pragma once
// src/cpp/engine/native_expression.hpp — general expression evaluation for the engine.
//
// An expression is lowered ONCE, at PLAN time (Python): the planner's Node tree ->
// CompiledBytecode (flat BytecodeInstr program whose compute instructions carry C
// kernel function pointers — the phase-9 C ABI), with LOAD_COL identities resolved
// to column indices against the compiler's tracked layout and LOAD_LIT_CONST
// literals resolved to their bind-time-materialized DrakenVector*. What crosses
// into these operators is plain C data: an instruction pointer + count, an index
// array, a literal-vector pointer array, and ONE C function pointer to the
// pure-nogil evaluation span (implemented next to the VM in evaluation.pyx —
// _dv_filter_span_cxx / _dv_eval_span_cxx; no PyObject is touched inside either).
//
// Only is_all_c_native programs are admitted — the compiler fails loud at plan
// time for anything else. There is no GIL, no fallback, no Python on this path.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "operator.hpp"
#include "core/vector_owner.h"
#include "logical_type.h"   // LogicalType / logical_type_intern — descriptor re-attachment

namespace opteryx::engine {

// Signatures of the Cython-side pure-nogil spans (passed in as C fn pointers, the
// same idiom as ScanPullFn — the engine never links against the Cython module).
typedef int (*ExprFilterFn)(void* instrs, int count, const CxxMorsel* m,
                            int* col_idx, void** lit_dv,
                            CxxMorsel** out_filtered, int* err_op);
typedef int (*ExprEvalFn)(void* instrs, int count, const CxxMorsel* m,
                          int* col_idx, void** lit_dv,
                          DrakenVector* out_vec, void** out_data,
                          uint8_t** out_validity, void** out_sel, int* err_op);

// One plan-resolved expression program. `instrs` is BORROWED — the NativePlan
// holds the CompiledBytecode (and thereby the instruction array and every literal
// vector) alive for the whole run.
struct ExprProgram {
    void* instrs = nullptr;
    int count = 0;
    std::vector<int> col_idx;    // per-instruction column index (-1 = not a load)
    std::vector<void*> lit_dv;   // per-instruction literal DrakenVector* (or null)
};

// ---- ExprFilterOperator: WHERE over an arbitrary c-native predicate --------------

struct ExprFilterOperator : Operator {
    ExprProgram prog;
    ExprFilterFn fn;

    ExprFilterOperator(ExprProgram p, ExprFilterFn f) : prog(std::move(p)), fn(f) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        CxxMorsel* filtered = nullptr;
        int err_op = 0;
        int rc = fn(prog.instrs, prog.count, in.get(), prog.col_idx.data(),
                    prog.lit_dv.data(), &filtered, &err_op);
        if (rc != 0) {
            err.code = 1;
            err.msg = "ExprFilterOperator: predicate evaluation failed (kernel error "
                      "or non-c-native program reached execution — see err_op)";
            return OpResult::NEED_INPUT;
        }
        std::shared_ptr<CxxMorsel> result(filtered);
        if (result == nullptr || result->num_rows() == 0) return OpResult::NEED_INPUT;
        out = std::move(result);
        return OpResult::EMIT;
    }
};

// ---- ExprMultiProjectOperator: append N computed columns in ONE operator -----------
// The compiler batches consecutive computed expressions here instead of chaining N
// ExprProjectOperators: a 90-projection query (clickbench Q30) paid O(N²)
// shared_ptr column-vector copies through the chain. Programs run in the given
// order over the GROWING morsel, so later programs can load earlier programs'
// outputs by index — the exact contract the chained form had.
struct ExprMultiProjectOperator : Operator {
    std::vector<ExprProgram> progs;
    ExprEvalFn fn;
    std::vector<std::string> out_names;               // one per program
    std::vector<const LogicalType*> out_logicals;     // one per program

    ExprMultiProjectOperator(std::vector<ExprProgram> ps, ExprEvalFn f,
                             std::vector<std::string> names,
                             std::vector<const LogicalType*> logicals)
        : progs(std::move(ps)), fn(f), out_names(std::move(names)),
          out_logicals(std::move(logicals)) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;   // same drop as single
        auto m = std::make_shared<CxxMorsel>();
        m->columns.reserve(in->columns.size() + progs.size());
        for (const CxxColumn& c : in->columns) m->columns.push_back(c);  // shared once
        m->names = in->names;
        m->zero_col_rows = in->num_rows();
        m->state = in->state;
        for (size_t k = 0; k < progs.size(); ++k) {
            DrakenVector v;
            void* data = nullptr;
            uint8_t* validity = nullptr;
            void* sel = nullptr;
            int err_op = 0;
            int rc = fn(progs[k].instrs, progs[k].count, m.get(),
                        progs[k].col_idx.data(), progs[k].lit_dv.data(),
                        &v, &data, &validity, &sel, &err_op);
            if (rc != 0) {
                err.code = 1;
                err.msg = (rc == 98)
                    ? "ExprMultiProjectOperator: expression result is not a "
                      "fixed-width type this span can materialize — fail loud"
                    : "ExprMultiProjectOperator: expression evaluation failed "
                      "(kernel error)";
                return OpResult::NEED_INPUT;
            }
            CxxColumn nc;
            nc.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                   OwnedBuffer<uint8_t>(validity),
                                                   OwnedBuffer<void>(sel));
            nc.own->logical_type = out_logicals[k];
            nc.view = nc.own->vec;
            m->columns.push_back(std::move(nc));
            m->names.push_back(out_names[k]);
        }
        out = std::move(m);
        return OpResult::EMIT;
    }
};

}  // namespace opteryx::engine
