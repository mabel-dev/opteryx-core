#pragma once
// src/cpp/engine/native_decimal.hpp — genuinely native (zero-Python) fixed-point
// DECIMAL arithmetic for SUM(expr) over DECIMAL/DECIMAL128 operands, matching
// TPC-H's `SUM(l_extendedprice * l_discount)` / `SUM(l_extendedprice * (1 -
// l_discount))` idiom.
//
// Why this exists as a SEPARATE path from native_aggregate.hpp's NativeExpr:
// DECIMAL's physical storage is an UNSCALED integer (int64 for DRAKEN_DECIMAL,
// __int128 for DRAKEN_DECIMAL128) — the scale (decimal places) is carried
// out-of-band on the column's LogicalType, not in the DrakenVector itself
// (draken/core/buffers.h). Converting a DECIMAL value through `double` (as
// NativeExpr does for plain numeric types) would silently drop DECIMAL's
// fixed-point rounding/precision contract — not an approximation, a different
// and wrong numeric semantics. This file keeps every value as an exact scaled
// integer end to end.
//
// Scale handling: the query planner's binder ALREADY computes the correct
// result scale for every subexpression (confirmed directly: for
// `l_extendedprice * (1 - l_discount)`, the binder's own schema_column carries
// LogicalType(scale=4) on the product, scale=2 on `l_extendedprice`, scale=2 on
// the nested `(1 - l_discount)`). So the scale of every node in the expression
// tree is a PLAN-TIME-KNOWN constant — parallel_engine.py reads it straight off
// each node's `schema_column.column_type.logical.scale` and bakes it into the
// DecimalExpr tree below. This file does NOT re-derive scale; it only aligns
// (rescales) operands whose STATIC scales differ before Add/Sub, and adds
// scales for Multiply (matching draken/ops/decimal_arith.h's dec_mul/dec128_mul
// convention: result_scale = sa + sb, no rescale needed for multiply).
//
// Scope (first landing, stated honestly): SUM only (no AVG-of-decimal, no
// decimal division — both need additional precision-widening rules this file
// does not implement yet). Overflow is checked and fails loud (ErrCtx), never
// silently wraps.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "morsels/cxx_morsel.h"  // CxxMorsel, DrakenVector, DRAKEN_DECIMAL, DRAKEN_DECIMAL128
#include "core/string_slot.h"    // DrakenStringSlot, str_length, str_data — Case/LIKE condition

namespace opteryx::engine {

// __int128 has no ostream/literal support portable across all lib versions used
// here; a small pow10 table (scale 0..38, DECIMAL's max) avoids repeated loops.
inline __int128 decimal_pow10(uint8_t n) {
    static const __int128 table[39] = {
        (__int128)1,
        (__int128)10,
        (__int128)100,
        (__int128)1000,
        (__int128)10000,
        (__int128)100000,
        (__int128)1000000,
        (__int128)10000000,
        (__int128)100000000,
        (__int128)1000000000,
        (__int128)10000000000LL,
        (__int128)100000000000LL,
        (__int128)1000000000000LL,
        (__int128)10000000000000LL,
        (__int128)100000000000000LL,
        (__int128)1000000000000000LL,
        (__int128)10000000000000000LL,
        (__int128)100000000000000000LL,
        (__int128)1000000000000000000LL,
        (__int128)1000000000000000000LL * 10,
        (__int128)1000000000000000000LL * 100,
        (__int128)1000000000000000000LL * 1000,
        (__int128)1000000000000000000LL * 10000,
        (__int128)1000000000000000000LL * 100000,
        (__int128)1000000000000000000LL * 1000000,
        (__int128)1000000000000000000LL * 10000000,
        (__int128)1000000000000000000LL * 100000000,
        (__int128)1000000000000000000LL * 1000000000LL,
        (__int128)1000000000000000000LL * 10000000000LL,
        (__int128)1000000000000000000LL * 100000000000LL,
        (__int128)1000000000000000000LL * 1000000000000LL,
        (__int128)1000000000000000000LL * 10000000000000LL,
        (__int128)1000000000000000000LL * 100000000000000LL,
        (__int128)1000000000000000000LL * 1000000000000000LL,
        (__int128)1000000000000000000LL * 10000000000000000LL,
        (__int128)1000000000000000000LL * 100000000000000000LL,
        (__int128)1000000000000000000LL * 100000000000000000LL * 10,
        (__int128)1000000000000000000LL * 100000000000000000LL * 100,
        (__int128)1000000000000000000LL * 100000000000000000LL * 1000,
    };
    return table[n];
}

struct DecimalValue {
    __int128 unscaled;
    uint8_t scale;
};

enum class DecimalExprKind : uint8_t { Column, Literal, Add, Sub, Mul, Case };

// Case: a SINGLE `WHEN <varchar_col> LIKE '<prefix>%' THEN <left> ELSE <right>
// END` shape — not general CASE (multi-branch) or general LIKE (wildcards
// anywhere but a single trailing '%', i.e. a plain prefix match). This exists
// specifically for TPC-H Q14's `CASE WHEN p_type LIKE 'PROMO%' THEN
// l_extendedprice*(1-l_discount) ELSE 0 END` — narrower is safer than
// guessing at a general CASE/LIKE engine this codebase doesn't have yet.
// `cond_col_idx` must be a DRAKEN_VARCHAR (inline-only) column; the prefix
// bytes are baked in at plan time (parallel_engine.py validates the pattern
// shape and strips the trailing '%' before calling make_case).
struct DecimalExpr {
    DecimalExprKind kind;
    size_t col_idx = 0;     // Column
    __int128 literal = 0;   // Literal — already at `scale` (see file header)
    uint8_t scale = 0;      // Literal's scale, or Column's schema scale
    std::shared_ptr<DecimalExpr> left;   // Add/Sub/Mul: left operand; Case: THEN
    std::shared_ptr<DecimalExpr> right;  // Add/Sub/Mul: right operand; Case: ELSE
    size_t cond_col_idx = 0;      // Case only
    std::string cond_prefix;      // Case only — the LIKE pattern's literal prefix

    static std::shared_ptr<DecimalExpr> make_column(size_t idx, uint8_t sc) {
        auto e = std::make_shared<DecimalExpr>();
        e->kind = DecimalExprKind::Column;
        e->col_idx = idx;
        e->scale = sc;
        return e;
    }
    // Takes int64_t, not __int128: Cython has no __int128 binding, and every
    // literal this path ever bakes (SQL constants like `1`, `0.06`, `25`) fits
    // comfortably in 64 bits once scaled — the accumulator (not the literal) is
    // what needs the full 128-bit range.
    static std::shared_ptr<DecimalExpr> make_literal(int64_t unscaled, uint8_t sc) {
        auto e = std::make_shared<DecimalExpr>();
        e->kind = DecimalExprKind::Literal;
        e->literal = static_cast<__int128>(unscaled);
        e->scale = sc;
        return e;
    }
    static std::shared_ptr<DecimalExpr> make_binary(DecimalExprKind k,
                                                    std::shared_ptr<DecimalExpr> l,
                                                    std::shared_ptr<DecimalExpr> r) {
        auto e = std::make_shared<DecimalExpr>();
        e->kind = k;
        e->left = std::move(l);
        e->right = std::move(r);
        return e;
    }
    static std::shared_ptr<DecimalExpr> make_case(size_t cond_idx, std::string prefix,
                                                  std::shared_ptr<DecimalExpr> then_expr,
                                                  std::shared_ptr<DecimalExpr> else_expr) {
        auto e = std::make_shared<DecimalExpr>();
        e->kind = DecimalExprKind::Case;
        e->cond_col_idx = cond_idx;
        e->cond_prefix = std::move(prefix);
        e->left = std::move(then_expr);
        e->right = std::move(else_expr);
        return e;
    }
};

// Read column `col_idx`'s raw unscaled value at `row`, for either DECIMAL
// (int64-backed) or DECIMAL128 (__int128-backed) physical storage, via the
// uniform data[selection[i]] access pattern (CLAUDE.md §11). Returns false
// (row excluded, NULL) if the column's validity bit is unset.
inline bool decimal_read_column(const DrakenVector& v, uint32_t row, __int128& out) {
    if (v.validity != nullptr && !((v.validity[row >> 3] >> (row & 7)) & 1u)) return false;
    uint32_t phys = v.selection[row];
    if (v.type == DRAKEN_DECIMAL) {
        out = static_cast<__int128>(static_cast<const int64_t*>(v.data)[phys]);
        return true;
    }
    if (v.type == DRAKEN_DECIMAL128) {
        out = static_cast<const __int128*>(v.data)[phys];
        return true;
    }
    return false;  // not a decimal column — caller's plan-time check should prevent this
}

// Align `v` (currently at `from_scale`) to `to_scale` (>= from_scale) by
// multiplying its unscaled value by 10^(to_scale - from_scale). Returns false
// on overflow (checked via division-back verification, since __int128 has no
// built-in overflow-detecting multiply).
inline bool decimal_rescale(__int128 v, uint8_t from_scale, uint8_t to_scale, __int128& out) {
    if (to_scale == from_scale) { out = v; return true; }
    __int128 factor = decimal_pow10(static_cast<uint8_t>(to_scale - from_scale));
    out = v * factor;
    if (factor != 0 && out / factor != v) return false;  // overflow
    return true;
}

// Evaluates `e` at row `row` of `m`, into `out` at `out.scale` (== the STATIC
// scale the plan-time tree already assigned this node — see file header).
// Returns false (row excluded — an operand was NULL, SQL NULL propagation
// through arithmetic) without touching `out.unscaled`; true otherwise.
// `err_overflow` is set true (never silently wrapped) if an intermediate
// computation overflows __int128.
inline bool eval_decimal_expr_checked(const DecimalExpr& e, const CxxMorsel& m, uint32_t row,
                                      DecimalValue& out, bool& err_overflow) {
    switch (e.kind) {
        case DecimalExprKind::Literal:
            out.unscaled = e.literal;
            out.scale = e.scale;
            return true;
        case DecimalExprKind::Column: {
            const DrakenVector& v = m.columns[e.col_idx].view;
            __int128 raw;
            if (!decimal_read_column(v, row, raw)) return false;
            out.unscaled = raw;
            out.scale = e.scale;
            return true;
        }
        case DecimalExprKind::Mul: {
            DecimalValue l, r;
            if (!eval_decimal_expr_checked(*e.left, m, row, l, err_overflow)) return false;
            if (!eval_decimal_expr_checked(*e.right, m, row, r, err_overflow)) return false;
            // result_scale = sa + sb (draken/ops/decimal_arith.h's dec_mul/dec128_mul
            // convention) — no rescale needed for multiply.
            out.unscaled = l.unscaled * r.unscaled;
            if (r.unscaled != 0 && out.unscaled / r.unscaled != l.unscaled) {
                err_overflow = true;
                return false;
            }
            out.scale = static_cast<uint8_t>(l.scale + r.scale);
            return true;
        }
        case DecimalExprKind::Add:
        case DecimalExprKind::Sub: {
            DecimalValue l, r;
            if (!eval_decimal_expr_checked(*e.left, m, row, l, err_overflow)) return false;
            if (!eval_decimal_expr_checked(*e.right, m, row, r, err_overflow)) return false;
            uint8_t target_scale = l.scale > r.scale ? l.scale : r.scale;
            __int128 la, ra;
            if (!decimal_rescale(l.unscaled, l.scale, target_scale, la) ||
                !decimal_rescale(r.unscaled, r.scale, target_scale, ra)) {
                err_overflow = true;
                return false;
            }
            out.unscaled = (e.kind == DecimalExprKind::Add) ? (la + ra) : (la - ra);
            out.scale = target_scale;
            return true;
        }
        case DecimalExprKind::Case: {
            const CxxColumn& cond_col = m.columns[e.cond_col_idx];
            const DrakenVector& cv = cond_col.view;
            if (cv.validity != nullptr && !((cv.validity[row >> 3] >> (row & 7)) & 1u)) {
                // NULL LIKE anything is NULL (3VL) — SQL CASE with a NULL
                // condition falls through to ELSE, same as a false condition
                // that never matched any WHEN.
                return eval_decimal_expr_checked(*e.right, m, row, out, err_overflow);
            }
            uint32_t phys = cv.selection[row];
            const auto* slot = static_cast<const DrakenStringSlot*>(cv.data) + phys;
            const uint8_t* arena_base = cond_col.own ? cond_col.own->arena_buf.get() : nullptr;
            uint32_t len = str_length(slot);
            bool matches = len >= e.cond_prefix.size() &&
                           std::memcmp(str_data(slot, arena_base), e.cond_prefix.data(),
                                      e.cond_prefix.size()) == 0;
            return eval_decimal_expr_checked(matches ? *e.left : *e.right, m, row, out, err_overflow);
        }
    }
    return false;
}

}  // namespace opteryx::engine
