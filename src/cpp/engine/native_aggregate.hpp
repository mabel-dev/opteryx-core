#pragma once
// src/cpp/engine/native_aggregate.hpp — a genuinely native (zero-Python) UNGROUPED
// aggregate Sink: SUM/COUNT/AVG over either a raw column or a simple arithmetic
// expression of columns/literals (+, -, *, /), PLUS decimal-exact SUM over
// DECIMAL/DECIMAL128 expressions (native_decimal.hpp) for the TPC-H
// `SUM(price * discount)`-shaped money aggregates. No PyObject anywhere in this
// Sink's per-row loop.
//
// Scope (first landing, stated honestly, not silently extended):
//   - UNGROUPED only — no GROUP BY (native_groupby_aggregate.hpp, if/when built, is
//     the next slice).
//   - Numeric path: SUM / COUNT / AVG over fixed-width numeric operands
//     (INT8/16/32/64, FLOAT32/64) — no MIN/MAX/MEDIAN/COUNT(DISTINCT)/STDDEV.
//   - Decimal path: SUM only (no AVG-of-decimal, no decimal division yet — both
//     need additional precision-widening rules not implemented here). Values stay
//     exact __int128 unscaled integers end to end — NEVER converted through
//     `double` (that would silently drop DECIMAL's fixed-point rounding/precision
//     contract, a different and wrong numeric semantics, not an approximation).
//     Overflow fails loud (ErrCtx), unlike this codebase's existing int64-tier
//     DECIMAL sum which silently wraps on overflow (draken/ops/int64_reductions.h)
//     — this path does not repeat that.
//   - NULL semantics: SQL "the expression's value is NULL if any operand is NULL,
//     and a NULL expression value is excluded from SUM/COUNT/AVG" — implemented via
//     eval_expr_checked / eval_decimal_expr_checked's validity propagation.

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "executor.hpp"
#include "scan_filter_demo.hpp"  // NumericFilterOperator::is_valid / read_as_double (reused, not duplicated)
#include "native_decimal.hpp"    // DecimalExpr, DecimalValue, eval_decimal_expr_checked

namespace opteryx::engine {

enum class ExprKind : uint8_t { Column, Literal, Add, Sub, Mul, Div };

// A tiny arithmetic expression tree: Column(idx) | Literal(value) | BinaryOp(kind,
// left, right). Shared, not owned uniquely, so a single parsed expression can be
// reused across worker-thread-local evaluation without copying the tree.
struct NativeExpr {
    ExprKind kind;
    size_t col_idx = 0;    // Column
    double literal = 0.0;  // Literal
    std::shared_ptr<NativeExpr> left;
    std::shared_ptr<NativeExpr> right;

    static std::shared_ptr<NativeExpr> make_column(size_t idx) {
        auto e = std::make_shared<NativeExpr>();
        e->kind = ExprKind::Column;
        e->col_idx = idx;
        return e;
    }
    static std::shared_ptr<NativeExpr> make_literal(double v) {
        auto e = std::make_shared<NativeExpr>();
        e->kind = ExprKind::Literal;
        e->literal = v;
        return e;
    }
    static std::shared_ptr<NativeExpr> make_binary(ExprKind k, std::shared_ptr<NativeExpr> l,
                                                    std::shared_ptr<NativeExpr> r) {
        auto e = std::make_shared<NativeExpr>();
        e->kind = k;
        e->left = std::move(l);
        e->right = std::move(r);
        return e;
    }
};

// Evaluates `e` at row `row` of `m`. Returns false (an operand was NULL — SQL NULL
// propagation through arithmetic) without touching `out`; true with the computed
// value in `out` otherwise.
inline bool eval_expr_checked(const NativeExpr& e, const CxxMorsel& m, uint32_t row,
                              double& out) {
    switch (e.kind) {
        case ExprKind::Literal:
            out = e.literal;
            return true;
        case ExprKind::Column: {
            const DrakenVector& v = m.columns[e.col_idx].view;
            if (!NumericFilterOperator::is_valid(v, row)) return false;
            out = NumericFilterOperator::read_as_double(v, row);
            return true;
        }
        default: {
            double l, r;
            if (!eval_expr_checked(*e.left, m, row, l)) return false;
            if (!eval_expr_checked(*e.right, m, row, r)) return false;
            switch (e.kind) {
                case ExprKind::Add: out = l + r; return true;
                case ExprKind::Sub: out = l - r; return true;
                case ExprKind::Mul: out = l * r; return true;
                case ExprKind::Div: out = l / r; return true;
                default: return false;  // unreachable
            }
        }
    }
}

enum class AggFunc : uint8_t { Sum, Count, Avg };

struct AggregateSpec {
    AggFunc func;
    bool is_decimal = false;
    // Exactly one of these two is non-null, per `is_decimal`. Null `expr` (when
    // !is_decimal) means COUNT(*) (counts rows, not a column's non-null values).
    std::shared_ptr<NativeExpr> expr;
    std::shared_ptr<DecimalExpr> decimal_expr;  // is_decimal only; func must be Sum
};

struct NativeAggAccum {
    double sum = 0.0;
    int64_t count = 0;
    __int128 decimal_sum = 0;
    uint8_t decimal_scale = 0;
    bool decimal_overflow = false;
};

struct NativeAggregateLocal : LocalSinkState {
    std::vector<NativeAggAccum> accums;
    explicit NativeAggregateLocal(size_t n) : accums(n) {}
};
struct NativeAggregateGlobal : GlobalSinkState {
    std::mutex mtx;  // combine-only contact, not the per-row hot path
    std::vector<NativeAggAccum> accums;
    std::vector<double> result;         // finalized SUM/COUNT/AVG, non-decimal specs
    std::vector<int64_t> decimal_hi;    // high 64 bits of the unscaled __int128 sum
    std::vector<uint64_t> decimal_lo;   // low 64 bits (two's-complement split)
    std::vector<uint8_t> decimal_scale; // meaningful only for is_decimal specs
    explicit NativeAggregateGlobal(size_t n)
        : accums(n), result(n), decimal_hi(n), decimal_lo(n), decimal_scale(n) {}
};

struct NativeAggregateSink : Sink {
    std::vector<AggregateSpec> specs;
    explicit NativeAggregateSink(std::vector<AggregateSpec> s) : specs(std::move(s)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<NativeAggregateGlobal>(specs.size());
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<NativeAggregateLocal>(specs.size());
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<NativeAggregateLocal&>(ls);
        const uint32_t n = in->num_rows();
        for (size_t s = 0; s < specs.size(); ++s) {
            const AggregateSpec& spec = specs[s];
            NativeAggAccum& acc = l.accums[s];
            if (spec.is_decimal) {
                DecimalValue v;
                for (uint32_t i = 0; i < n; ++i) {
                    bool overflow = false;
                    if (!eval_decimal_expr_checked(*spec.decimal_expr, *in, i, v, overflow)) {
                        if (overflow) {
                            err.code = 1;
                            err.msg = "NativeAggregateSink: decimal expression overflowed __int128";
                            return SinkResult::CONTINUE;
                        }
                        continue;  // NULL operand — excluded, matching SQL SUM semantics
                    }
                    acc.decimal_scale = v.scale;
                    __int128 next = acc.decimal_sum + v.unscaled;
                    // Signed __int128 overflow check (sign-of-operands-vs-result idiom,
                    // matching draken_native.cpp's dec128_sum_reduce).
                    if (((acc.decimal_sum ^ next) & (v.unscaled ^ next)) < 0) {
                        err.code = 1;
                        err.msg = "NativeAggregateSink: decimal SUM accumulator overflowed __int128";
                        return SinkResult::CONTINUE;
                    }
                    acc.decimal_sum = next;
                }
                continue;
            }
            if (spec.func == AggFunc::Count && spec.expr == nullptr) {
                acc.count += n;  // COUNT(*) — every row counts, NULLs included
                continue;
            }
            double v;
            for (uint32_t i = 0; i < n; ++i) {
                if (eval_expr_checked(*spec.expr, *in, i, v)) {
                    acc.sum += v;
                    acc.count += 1;
                }
            }
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<NativeAggregateGlobal&>(gs);
        auto& l = static_cast<NativeAggregateLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (size_t s = 0; s < g.accums.size(); ++s) {
            g.accums[s].sum += l.accums[s].sum;
            g.accums[s].count += l.accums[s].count;
            if (specs[s].is_decimal) {
                __int128 a = g.accums[s].decimal_sum;
                __int128 b = l.accums[s].decimal_sum;
                __int128 next = a + b;
                if (((a ^ next) & (b ^ next)) < 0) {
                    err.code = 1;
                    err.msg = "NativeAggregateSink: decimal SUM combine overflowed __int128";
                    return;
                }
                g.accums[s].decimal_sum = next;
                g.accums[s].decimal_scale = l.accums[s].decimal_scale;
            }
        }
    }

    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<NativeAggregateGlobal&>(gs);
        for (size_t s = 0; s < specs.size(); ++s) {
            const NativeAggAccum& acc = g.accums[s];
            if (specs[s].is_decimal) {
                g.decimal_hi[s] = static_cast<int64_t>(acc.decimal_sum >> 64);
                g.decimal_lo[s] = static_cast<uint64_t>(
                    static_cast<unsigned __int128>(acc.decimal_sum) & 0xFFFFFFFFFFFFFFFFULL);
                g.decimal_scale[s] = acc.decimal_scale;
                continue;
            }
            switch (specs[s].func) {
                case AggFunc::Sum:   g.result[s] = acc.sum; break;
                case AggFunc::Count: g.result[s] = static_cast<double>(acc.count); break;
                case AggFunc::Avg:   g.result[s] = acc.count > 0 ? acc.sum / acc.count : 0.0; break;
            }
        }
    }
};

}  // namespace opteryx::engine
