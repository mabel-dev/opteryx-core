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
#include "core/draken_bridge.h"   // draken_vecresult_child_owner_new_c — ARRAY child adoption
#include "logical_type.h"   // LogicalType / logical_type_intern — descriptor re-attachment

namespace opteryx::engine {

// Signatures of the Cython-side pure-nogil spans (passed in as C fn pointers, the
// same idiom as ScanPullFn — the engine never links against the Cython module).
// `err_msg` (rc 4 only) is the failing kernel's VecResult::error_msg (see
// draken/ops/vec_result.h) — a pointer into that thread's error_handling.cpp
// buffer, valid only until the next kernel call on THIS thread. It is threaded
// through explicitly rather than re-fetched via draken_get_error_message():
// error_handling.cpp is compiled into more than one extension (draken_native,
// _kernel_registry), each with its own thread_local buffer, and this file's
// `-undefined dynamic_lookup` binding to that symbol is not guaranteed to land
// on the same copy the failing kernel actually wrote — confirmed empty in
// practice. Passing the pointer as data sidesteps that ambiguity entirely.
// `const_col_idx`/`const_scalar_dv` (length `n_consts`) name columns the compiler
// has proven hold a single literal value on every row surviving the predicate
// (an `IDENTIFIER = LITERAL` conjunct — see compiler.py's FilterNode branch). The
// span broadcasts those columns from the pre-resolved scalar DrakenVector* in O(1)
// instead of gathering them and discarding the result. n_consts == 0 (the common
// case) costs nothing extra — same as the old no-consts signature.
typedef int (*ExprFilterFn)(void* instrs, int count, const CxxMorsel* m,
                            int* col_idx, void** lit_dv,
                            int* const_col_idx, void** const_scalar_dv, int n_consts,
                            CxxMorsel** out_filtered, int* err_op,
                            const char** err_msg);
// `out_child` (VecResult**) is set to NULL, or to an OWNED VecResult* for an
// ARRAY result's element vector — non-null only when out_vec->type ==
// DRAKEN_ARRAY (see evaluation.pyx's _dv_eval_span_cxx). The span-side kernel
// draken_malloc'd it standalone (not arena-owned), matching the ownership
// contract draken_vecresult_child_owner_new_c (draken_bridge.h) expects.
typedef int (*ExprEvalFn)(void* instrs, int count, const CxxMorsel* m,
                          int* col_idx, void** lit_dv,
                          DrakenVector* out_vec, void** out_data,
                          uint8_t** out_validity, void** out_sel, int* err_op,
                          const char** err_msg, VecResult** out_child);

// One plan-resolved expression program. `instrs` is BORROWED — the NativePlan
// holds the CompiledBytecode (and thereby the instruction array and every literal
// vector) alive for the whole run.
struct ExprProgram {
    void* instrs = nullptr;
    int count = 0;
    std::vector<int> col_idx;    // per-instruction column index (-1 = not a load)
    std::vector<void*> lit_dv;   // per-instruction literal DrakenVector* (or null)
    // `IDENTIFIER = LITERAL` const-replacements (ExprFilterOperator only; empty for
    // ExprEvalFn programs). const_col_idx[k] holds a value equal to the scalar
    // DrakenVector* at const_scalar_dv[k] (data_length == 1) on every row surviving
    // the predicate — plan-time-proven by the compiler, resolved against the same
    // `layout` as col_idx/lit_dv (see compiler.py's FilterNode branch).
    std::vector<int> const_col_idx;
    std::vector<void*> const_scalar_dv;
};

// ErrCtx::msg is a bare `const char*` that must stay valid until the Cython raise
// site reads it (see _engine_plan_run in _operators.pyx). `kernel_msg` (from the
// ExprFilterFn/ExprEvalFn out-param) is only valid until the next kernel call on
// THIS thread — that hasn't happened yet (this operator's worker thread bails out
// immediately on error; see executor.hpp's `push`) — so it's still safe to read
// here, but not safe to stash as-is. Combine it into a thread_local std::string:
// thread_local (not a stack local) because `format_kernel_error`'s return value
// must outlive this call, thread_local instead of a member on the operator
// because the same Operator instance is shared and called concurrently by every
// worker thread (dop > 1) — a member would race.
inline const char* format_kernel_error(const char* op_name, int err_op,
                                       const char* kernel_msg) {
    static thread_local std::string buf;
    buf = std::string(op_name) + " (err_op=" + std::to_string(err_op) + "): " +
          (kernel_msg != nullptr ? kernel_msg : "");
    return buf.c_str();
}

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
        const char* kernel_msg = nullptr;
        int rc = fn(prog.instrs, prog.count, in.get(), prog.col_idx.data(),
                    prog.lit_dv.data(), prog.const_col_idx.data(), prog.const_scalar_dv.data(),
                    static_cast<int>(prog.const_col_idx.size()), &filtered, &err_op, &kernel_msg);
        if (rc != 0) {
            err.code = 1;
            err.msg = format_kernel_error("ExprFilterOperator: predicate evaluation failed",
                                          err_op, kernel_msg);
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
            const char* kernel_msg = nullptr;
            VecResult* child = nullptr;
            int rc = fn(progs[k].instrs, progs[k].count, m.get(),
                        progs[k].col_idx.data(), progs[k].lit_dv.data(),
                        &v, &data, &validity, &sel, &err_op, &kernel_msg, &child);
            if (rc != 0) {
                err.code = 1;
                err.msg = (rc == 98)
                    ? "ExprMultiProjectOperator: expression result is not a "
                      "fixed-width type this span can materialize — fail loud"
                    : format_kernel_error(
                          "ExprMultiProjectOperator: expression evaluation failed",
                          err_op, kernel_msg);
                return OpResult::NEED_INPUT;
            }
            CxxColumn nc;
            nc.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                   OwnedBuffer<uint8_t>(validity),
                                                   OwnedBuffer<void>(sel));
            // ARRAY result (v.type == DRAKEN_ARRAY): `child` is the owned element
            // vector the span could not carry any other way (see ExprEvalFn's
            // out_child doc above) — adopt it as this column's child_owner, the
            // SAME field make_array_from_sequence's ARRAY construction populates
            // (vector_owner.h), so every downstream ARRAY consumer sees an
            // identically-shaped owner regardless of which path built it.
            if (child != nullptr) {
                nc.own->child_owner.reset(draken_vecresult_child_owner_new_c(*child));
                delete child;
            }
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
