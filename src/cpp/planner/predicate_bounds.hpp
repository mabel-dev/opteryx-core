// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/predicate_bounds.hpp — the terms a manifest prunes files on,
// derived natively from a query's predicates (native plan graph Q8, M-d2).
//
// The native port of the Python predicate_bounds.py (deleted with the Q8 façade), read from the
// query's expression arena (expr_arena.hpp) instead of Python expression objects.
// The bounds pruners read ONE shape - `column <op> literal` (and BETWEEN) - and
// this derives, from every other shape a user writes (IN, LIKE 'abc%', a
// same-column OR, a monotone transform wrapped around the column), an interval
// on the RAW STORED COLUMN that provably holds every row the predicate can match.
// See the Python module's docstring for each rule's reasoning; every rule here is
// the same rule. A derived interval may be WIDER than the true pre-image, never
// NARROWER, and every "decline" is "no information", never "false".
//
// Unlike the Python, a derived term is NOT minted as a new expression: it is a
// `BoundTerm` the pruners read directly, so pruning adds nothing to the arena.

#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "planner/bound_interval.hpp"
#include "planner/column_table.hpp"
#include "planner/column_type.hpp"
#include "planner/expr_arena.hpp"

namespace opteryx::planner {

enum TermOp : uint8_t { OP_EQ, OP_NOTEQ, OP_GT, OP_GTEQ, OP_LT, OP_LTEQ, OP_BETWEEN, OP_NONE };

inline TermOp term_op(const std::string& name) {
    if (name == "Eq") return OP_EQ;
    if (name == "NotEq") return OP_NOTEQ;
    if (name == "Gt") return OP_GT;
    if (name == "GtEq") return OP_GTEQ;
    if (name == "Lt") return OP_LT;
    if (name == "LtEq") return OP_LTEQ;
    return OP_NONE;
}

// One `column <op> value` (or `column BETWEEN value AND upper`) the pruners
// evaluate against file bounds.
struct BoundTerm {
    std::string column;               // the identifier's source column
    ExprId identifier = kNoExpr;      // for the bound schema column's type
    TermOp op = OP_NONE;
    BVal value;
    BVal upper;                       // BETWEEN only
    // The literal's declared type(s), for the temporal-domain guard; derived
    // terms never trip it (see derive_bound_terms) and carry none.
    ColumnTypeId literal_type = kNoColumnType;
    ColumnTypeId upper_type = kNoColumnType;
    bool derived = false;
};

// `IS NULL` (requires_null) / `IS NOT NULL` over a bare column.
struct NullTerm {
    std::string column;
    bool requires_null;
};

// Terms valid ONLY in a file where `fold` (LOWER / UPPER) is the identity on
// `column` - never mixed into the ordinary terms.
struct FoldTerm {
    std::string column;
    bool lower;                        // LOWER, else UPPER
    std::vector<BoundTerm> terms;
};

// What a derivation reads besides the arena: the column rows and the type table
// (for a column's type), and the manifest's LIVE schema - name to type - which
// is consulted first, as `Manifest._column_type` was.
struct DeriveInputs {
    const ExprTable* exprs = nullptr;
    const ColumnRows* columns = nullptr;
    const ColumnTypeTable* types = nullptr;
    const NodeKinds* kinds = nullptr;
    const std::unordered_map<std::string, ColumnTypeId>* live_types = nullptr;
};

namespace bounds_detail {

inline constexpr int64_t kTicksPerSecond[4] = {1, 1000, 1000000, 1000000000};

struct Deriver {
    const DeriveInputs& in;

    const ExprRow& row(ExprId id) const { return in.exprs->row(id); }

    ExprId unwrap(ExprId id) const {
        while (id != kNoExpr && row(id).kind == in.kinds->nested) id = row(id).centre;
        return id;
    }

    bool is(ExprId id, int32_t kind) const { return id != kNoExpr && row(id).kind == kind; }

    // `_column_type_of`: the live schema by source column, else the bound column.
    const ColumnTypeEntry* column_type_of(ExprId identifier) const {
        const ExprRow& r = row(identifier);
        if (!r.source_column.empty()) {
            auto found = in.live_types->find(r.source_column);
            if (found != in.live_types->end()) return &in.types->entry(found->second);
        }
        if (r.column_slot == kNoColumnSlot) return nullptr;
        const ColumnTypeId type_id = in.columns->row(r.column_slot).type_id;
        return type_id == kNoColumnType ? nullptr : &in.types->entry(type_id);
    }

    ColumnTypeId column_type_id_of(ExprId identifier) const {
        const ExprRow& r = row(identifier);
        if (!r.source_column.empty()) {
            auto found = in.live_types->find(r.source_column);
            if (found != in.live_types->end()) return found->second;
        }
        if (r.column_slot == kNoColumnSlot) return kNoColumnType;
        return in.columns->row(r.column_slot).type_id;
    }

    static bool temporal(const ColumnTypeEntry* t) {
        return t != nullptr && (t->physical == DRAKEN_DATE32 || t->physical == DRAKEN_TIMESTAMP64);
    }

    // Ticks per second for a TIMESTAMP column; 0 for anything else.
    static int64_t ticks_per_second(const ColumnTypeEntry* t) {
        if (t == nullptr || t->physical != DRAKEN_TIMESTAMP64) return 0;
        const int unit = t->has_logical ? static_cast<int>(t->logical.unit) : 2;
        return kTicksPerSecond[unit];
    }

    // The literal at `id` (after NESTED), or nullptr.
    const LiteralValue* literal_at(ExprId id) const {
        id = unwrap(id);
        if (!is(id, in.kinds->literal)) return nullptr;
        return &row(id).literal;
    }

    // An integer literal (not a bool) at `id`.
    bool int_literal(ExprId id, __int128& out) const {
        const LiteralValue* lit = literal_at(id);
        if (lit == nullptr) return false;
        if (lit->tag == LITERAL_INT64) { out = lit->i; return true; }
        if (lit->tag == LITERAL_UINT64) { out = static_cast<__int128>(static_cast<uint64_t>(lit->i)); return true; }
        return false;
    }

    // A string literal's text, lower-cased (units and date parts).
    bool text_literal(ExprId id, std::string& out) const {
        const LiteralValue* lit = literal_at(id);
        if (lit == nullptr || lit->tag != LITERAL_BYTES) return false;
        out = lit->bytes;
        for (char& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return true;
    }

    // --- temporal: moments as microseconds since the epoch --------------------

    static int64_t days_from_civil(int64_t y, int64_t m, int64_t d) {
        y -= m <= 2;
        const int64_t era = (y >= 0 ? y : y - 399) / 400;
        const int64_t yoe = y - era * 400;
        const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    static void civil_from_days(int64_t z, int64_t& y, int64_t& m, int64_t& d) {
        z += 719468;
        const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
        const int64_t doe = z - era * 146097;
        const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        y = yoe + era * 400;
        const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        const int64_t mp = (5 * doy + 2) / 153;
        d = doy - (153 * mp + 2) / 5 + 1;
        m = mp < 10 ? mp + 3 : mp - 9;
        if (m <= 2) ++y;
    }

    static constexpr int64_t kMicrosPerDay = 86400LL * 1000000LL;
    // datetime's range, 0001-01-01 .. 9999-12-31 23:59:59.999999
    static int64_t min_moment() { return days_from_civil(1, 1, 1) * kMicrosPerDay; }
    static int64_t max_moment() { return days_from_civil(10000, 1, 1) * kMicrosPerDay - 1; }

    static int64_t floor_div64(int64_t n, int64_t d) {
        int64_t q = n / d;
        if ((n % d != 0) && ((n < 0) != (d < 0))) --q;
        return q;
    }

    static int64_t year_of(int64_t moment) {
        int64_t y, m, d;
        civil_from_days(floor_div64(moment, kMicrosPerDay), y, m, d);
        return y;
    }

    // `_raw_to_datetime`: a raw stored temporal integer to a moment.
    static std::optional<int64_t> raw_to_moment(const BVal& value, const ColumnTypeEntry* t) {
        if (value.kind != BKind::INT || t == nullptr) return std::nullopt;
        if (t->physical == DRAKEN_DATE32) {
            if (!(value.i > -700000 && value.i < 2900000)) return std::nullopt;
            return static_cast<int64_t>(value.i) * kMicrosPerDay;
        }
        const int64_t ticks = ticks_per_second(t);
        if (ticks == 0) return std::nullopt;
        __int128 scaled;
        if (__builtin_mul_overflow(value.i, static_cast<__int128>(1000000), &scaled)) return std::nullopt;
        const __int128 micros = int_floordiv(scaled, ticks);
        if (!(micros > -60000000000000000LL && micros < 250000000000000000LL)) return std::nullopt;
        return static_cast<int64_t>(micros);
    }

    // `_datetime_to_raw` (predicate_rewriter._canonical_temporal_literal_value).
    static std::optional<BVal> moment_to_raw(int64_t moment, const ColumnTypeEntry* t) {
        if (t == nullptr) return std::nullopt;
        if (t->physical == DRAKEN_DATE32) return BVal::of_int(floor_div64(moment, kMicrosPerDay));
        const int64_t ticks = ticks_per_second(t);
        if (ticks == 0) return std::nullopt;
        const int64_t seconds = floor_div64(moment, 1000000);
        const int64_t micros = moment - seconds * 1000000;
        return BVal::of_int(static_cast<__int128>(seconds) * ticks + (static_cast<__int128>(micros) * ticks) / 1000000);
    }

    // `add_single_unit`; nullopt past datetime's range (where Python raised).
    static std::optional<int64_t> add_units(int64_t moment, const std::string& unit, int64_t n) {
        int64_t step = 0;
        if (unit == "second") step = 1000000;
        else if (unit == "minute") step = 60LL * 1000000;
        else if (unit == "hour") step = 3600LL * 1000000;
        else if (unit == "day") step = kMicrosPerDay;
        else if (unit == "week") step = 7 * kMicrosPerDay;
        int64_t out;
        if (step != 0) {
            if (__builtin_mul_overflow(step, n, &out) || __builtin_add_overflow(out, moment, &out)) return std::nullopt;
        } else {
            const int64_t months = unit == "month" ? n : unit == "quarter" ? n * 3 : n * 12;
            const int64_t day_number = floor_div64(moment, kMicrosPerDay);
            const int64_t time_of_day = moment - day_number * kMicrosPerDay;
            int64_t y, m, d;
            civil_from_days(day_number, y, m, d);
            const int64_t total = m - 1 + months;
            const int64_t new_year = y + floor_div64(total, 12);
            const int64_t new_month = total - floor_div64(total, 12) * 12 + 1;
            if (new_year < 1 || new_year > 9999) return std::nullopt;
            const int64_t next_first = new_month == 12 ? days_from_civil(new_year + 1, 1, 1)
                                                       : days_from_civil(new_year, new_month + 1, 1);
            const int64_t last_day = next_first - days_from_civil(new_year, new_month, 1);
            out = days_from_civil(new_year, new_month, std::min(d, last_day)) * kMicrosPerDay + time_of_day;
        }
        if (out < min_moment() || out > max_moment()) return std::nullopt;
        return out;
    }

    // --- pre-images ------------------------------------------------------------

    using Resolved = std::optional<std::pair<ExprId, Interval>>;

    // `_preimage`: the interval `expr`'s own argument column must lie in.
    Resolved preimage(ExprId expr, const Interval& iv) const {
        expr = unwrap(expr);
        if (expr == kNoExpr) return std::nullopt;
        const ExprRow& r = row(expr);
        if (r.kind == in.kinds->identifier) return std::make_pair(expr, iv);
        if (r.kind == in.kinds->binary) {
            auto folded = preimage_arithmetic(r, iv);
            if (!folded) return std::nullopt;
            return preimage(folded->first, folded->second);
        }
        if (r.kind == in.kinds->function) {
            auto folded = preimage_function(r, iv);
            if (!folded) return std::nullopt;
            return preimage(folded->first, folded->second);
        }
        return std::nullopt;
    }

    // `_literal_operand`: (expression, numeric literal, literal on the left).
    bool literal_operand(ExprId left, ExprId right, ExprId& expression, BVal& constant, bool& on_left) const {
        left = unwrap(left);
        right = unwrap(right);
        if (left == kNoExpr || right == kNoExpr) return false;
        const bool left_literal = is(left, in.kinds->literal), right_literal = is(right, in.kinds->literal);
        if (right_literal && !left_literal) {
            auto v = bval_of_literal(row(right).literal);
            if (!v || !v->number()) return false;
            expression = left; constant = *v; on_left = false;
            return true;
        }
        if (left_literal && !right_literal) {
            auto v = bval_of_literal(row(left).literal);
            if (!v || !v->number()) return false;
            expression = right; constant = *v; on_left = true;
            return true;
        }
        return false;
    }

    Resolved preimage_arithmetic(const ExprRow& r, const Interval& iv) const {
        const std::string& op = r.value;
        if (op != "Plus" && op != "Minus" && op != "Multiply") return std::nullopt;
        ExprId inner;
        BVal constant;
        bool on_left;
        if (!literal_operand(r.left, r.right, inner, constant, on_left)) return std::nullopt;
        if (!numeric_interval(iv)) return std::nullopt;
        std::optional<Interval> folded;
        if (op == "Plus") {
            folded = affine_preimage(iv, BVal::of_int(1), constant);
        } else if (op == "Minus") {
            if (on_left) {
                folded = affine_preimage(iv, BVal::of_int(-1), constant);
            } else {
                auto negated = negate(constant);
                if (!negated) return std::nullopt;
                folded = affine_preimage(iv, BVal::of_int(1), *negated);
            }
        } else {
            folded = affine_preimage(iv, constant, BVal::of_int(0));
        }
        if (!folded) return std::nullopt;
        return std::make_pair(inner, *folded);
    }

    // `_scale_argument`: 0 when absent; -1 when present but not a non-negative
    // integer literal.
    int64_t scale_argument(const std::vector<ExprId>& parameters, size_t position) const {
        if (parameters.size() <= position) return 0;
        __int128 v;
        if (!int_literal(parameters[position], v)) return -1;
        return v >= 0 ? static_cast<int64_t>(std::min<__int128>(v, INT64_MAX)) : -1;
    }

    // A positive integer width literal (`LEFT(x, n)`, `SUBSTRING(x, 1, n)`).
    bool width_argument(ExprId id, int64_t& width) const {
        __int128 v;
        if (!int_literal(id, v) || v < 1) return false;
        width = static_cast<int64_t>(std::min<__int128>(v, INT64_MAX));
        return true;
    }

    Resolved preimage_function(const ExprRow& r, const Interval& iv) const {
        const std::string& name = r.value;
        const std::vector<ExprId>& parameters = r.parameters;
        if (parameters.empty()) return std::nullopt;

        if (name == "FLOOR" || name == "CEILING") {
            if (scale_argument(parameters, 1) != 0 || !numeric_interval(iv)) return std::nullopt;
            auto folded = name == "FLOOR" ? floor_preimage(iv) : ceiling_preimage(iv);
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "ROUND" || name == "TRUNC") {
            // TRUNC's temporal overload carries a STRING unit, which is not a
            // non-negative integer scale, so it declines below.
            if (scale_argument(parameters, 1) < 0 || !numeric_interval(iv)) return std::nullopt;
            auto folded = widen(iv, BVal::of_int(1));
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "ABS") {
            if (!numeric_interval(iv)) return std::nullopt;
            auto folded = abs_preimage(iv);
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "SIGN") {
            if (!numeric_interval(iv)) return std::nullopt;
            auto folded = sign_preimage(iv);
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "LEFT") {
            int64_t width;
            if (parameters.size() < 2 || !width_argument(parameters[1], width)) return std::nullopt;
            auto folded = truncating_preimage(iv, width);
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "SUBSTRING") {
            // Only a prefix is a prefix: the start must be (a literal equal to) 1.
            if (parameters.size() < 2) return std::nullopt;
            const LiteralValue* start = literal_at(parameters[1]);
            if (start == nullptr) return std::nullopt;
            // `start != 1` in Python: any literal numerically equal to one
            bool is_one = (start->tag == LITERAL_INT64 && start->i == 1) ||
                          (start->tag == LITERAL_UINT64 && start->i == 1) ||
                          (start->tag == LITERAL_BOOL && start->i != 0) ||
                          (start->tag == LITERAL_DOUBLE && start->d == 1.0);
            if (start->tag == LITERAL_DECIMAL && start->k <= 0 && start->k >= -30) {
                const __int128 unscaled = (static_cast<__int128>(start->j) << 64) |
                                          static_cast<__int128>(static_cast<uint64_t>(start->i));
                __int128 one = 1;
                for (int32_t e = start->k; e < 0; ++e) one *= 10;
                is_one = unscaled == one;
            }
            if (!is_one) return std::nullopt;
            if (parameters.size() < 3) return std::make_pair(parameters[0], iv);
            int64_t width;
            if (!width_argument(parameters[2], width)) return std::nullopt;
            auto folded = truncating_preimage(iv, width);
            if (!folded) return std::nullopt;
            return std::make_pair(parameters[0], *folded);
        }
        if (name == "EXTRACT") {
            if (parameters.size() != 2) return std::nullopt;
            std::string part;
            if (!text_literal(parameters[0], part) || part != "year") return std::nullopt;
            const ExprId identifier = unwrap(parameters[1]);
            if (!is(identifier, in.kinds->identifier)) return std::nullopt;
            const ColumnTypeEntry* t = column_type_of(identifier);
            if (!temporal(t) || !numeric_interval(iv)) return std::nullopt;
            auto folded = year_preimage(iv, t);
            if (!folded) return std::nullopt;
            return std::make_pair(identifier, *folded);
        }
        if (name == "TIME_BUCKET") return time_bucket_preimage(parameters, iv);
        if (name == "UNIXTIME") {
            const ExprId identifier = unwrap(parameters[0]);
            if (!is(identifier, in.kinds->identifier)) return std::nullopt;
            const int64_t ticks = ticks_per_second(column_type_of(identifier));
            if (ticks == 0 || !numeric_interval(iv)) return std::nullopt;
            auto seconds = floor_preimage(iv);
            if (!seconds) return std::nullopt;
            Interval out;
            const BVal one = BVal::of_int(1), tick = BVal::of_int(ticks);
            if (seconds->lo) {
                auto v = sub(*seconds->lo, one);
                if (!v) return std::nullopt;
                out.lo = mul(*v, tick);
                if (!out.lo) return std::nullopt;
            }
            if (seconds->hi) {
                auto v = add(*seconds->hi, one);
                if (!v) return std::nullopt;
                out.hi = mul(*v, tick);
                if (!out.hi) return std::nullopt;
            }
            out.lo_closed = true;
            out.hi_closed = false;
            return std::make_pair(identifier, out);
        }
        if (name == "FROM_UNIXTIME") {
            const ExprId identifier = unwrap(parameters[0]);
            if (!is(identifier, in.kinds->identifier) || !numeric_interval(iv)) return std::nullopt;
            Interval out;
            const BVal million = BVal::of_int(1000000), one = BVal::of_int(1);
            auto truncated = [](const BVal& v) -> std::optional<BVal> {
                if (v.kind == BKind::INT) return v;
                if (!std::isfinite(v.d) || std::fabs(v.d) >= std::ldexp(1.0, 126)) return std::nullopt;
                return BVal::of_int(static_cast<__int128>(std::trunc(v.d)));
            };
            if (iv.lo) {
                auto t = truncated(*iv.lo);
                if (!t) return std::nullopt;
                auto q = floor_div(*t, million);
                if (!q) return std::nullopt;
                out.lo = sub(*q, one);
                if (!out.lo) return std::nullopt;
            }
            if (iv.hi) {
                auto t = truncated(*iv.hi);
                if (!t) return std::nullopt;
                auto q = ceil_div(*t, million);
                if (!q) return std::nullopt;
                out.hi = add(*q, one);
                if (!out.hi) return std::nullopt;
            }
            out.lo_closed = true;
            out.hi_closed = true;
            return std::make_pair(identifier, out);
        }
        return std::nullopt;
    }

    // EXTRACT(YEAR ...): the FLOOR rule, then each year's first instant.
    std::optional<Interval> year_preimage(const Interval& iv, const ColumnTypeEntry* t) const {
        auto years = floor_preimage(iv);
        if (!years) return std::nullopt;
        auto start_of_year = [&](const BVal& year) -> std::optional<BVal> {
            if (year.kind != BKind::INT || year.i < 1 || year.i > 9999) return std::nullopt;
            return moment_to_raw(days_from_civil(static_cast<int64_t>(year.i), 1, 1) * kMicrosPerDay, t);
        };
        Interval out;
        if (years->lo) {
            out.lo = start_of_year(*years->lo);
            if (!out.lo) return std::nullopt;
        }
        if (years->hi) {
            out.hi = start_of_year(*years->hi);
            if (!out.hi) return std::nullopt;
        }
        if (!out.lo && !out.hi) return std::nullopt;
        out.lo_closed = true;
        out.hi_closed = false;
        return out;
    }

    // TIME_BUCKET(magnitude, units, col): bounded without knowing the anchor.
    Resolved time_bucket_preimage(const std::vector<ExprId>& parameters, const Interval& iv) const {
        if (parameters.size() != 3) return std::nullopt;
        const LiteralValue* magnitude_literal = literal_at(parameters[0]);
        if (magnitude_literal == nullptr) return std::nullopt;
        auto magnitude = bval_of_literal(*magnitude_literal);
        if (!magnitude || !magnitude->number()) return std::nullopt;
        if (lt(*magnitude, BVal::of_int(1)) || lt(BVal::of_int(100000), *magnitude)) return std::nullopt;
        if (magnitude->kind == BKind::FLOAT && std::isnan(magnitude->d)) return std::nullopt;
        std::string unit;
        if (!text_literal(parameters[1], unit)) return std::nullopt;
        if (unit != "second" && unit != "minute" && unit != "hour" && unit != "day" && unit != "week" &&
            unit != "month" && unit != "quarter" && unit != "year") {
            return std::nullopt;
        }
        const ExprId identifier = unwrap(parameters[2]);
        if (!is(identifier, in.kinds->identifier)) return std::nullopt;
        const ColumnTypeEntry* t = column_type_of(identifier);
        if (!temporal(t)) return std::nullopt;

        Interval out;
        out.lo = iv.lo;
        out.lo_closed = iv.lo_closed;
        if (iv.hi) {
            auto moment = raw_to_moment(*iv.hi, t);
            if (!moment) return std::nullopt;
            if (year_of(*moment) > 9000) return std::nullopt;
            const int64_t n = (magnitude->kind == BKind::INT ? static_cast<int64_t>(magnitude->i)
                                                             : static_cast<int64_t>(std::trunc(magnitude->d))) + 1;
            auto edge = add_units(*moment, unit, n);
            if (!edge) return std::nullopt;
            out.hi = moment_to_raw(*edge, t);
            if (!out.hi) return std::nullopt;
        }
        if (!out.lo && !out.hi) return std::nullopt;
        out.hi_closed = false;
        return std::make_pair(identifier, out);
    }

    // --- predicate shapes -> one interval on one column --------------------------

    // `_identity_key`: the bound column's identity, else its source column name.
    std::string identity_key(ExprId identifier) const {
        const ExprRow& r = row(identifier);
        if (r.column_slot != kNoColumnSlot) return "I" + in.columns->row(r.column_slot).identity;
        if (r.source_column.empty()) return std::string();
        return "S" + r.source_column;
    }

    // `_in_list_interval`: the hull of an IN list of all numbers or all bytes.
    std::optional<Interval> in_list_interval(const LiteralValue& list) const {
        if (list.tag != LITERAL_ITEMS || list.items.empty()) return std::nullopt;
        std::vector<BVal> members;
        members.reserve(list.items.size());
        for (const LiteralValue& item : list.items) {
            auto v = bval_of_literal(item);
            if (!v) return std::nullopt;
            members.push_back(*v);
        }
        bool numbers = true, bytes = true;
        for (const BVal& m : members) {
            numbers = numbers && m.number();
            bytes = bytes && m.kind == BKind::BYTES;
        }
        if (!numbers && !bytes) return std::nullopt;
        const BVal* lo = &members[0];
        const BVal* hi = &members[0];
        for (const BVal& m : members) {
            if (lt(m, *lo)) lo = &m;
            if (lt(*hi, m)) hi = &m;
        }
        Interval out;
        out.lo = *lo;
        out.hi = *hi;
        return out;
    }

    // `_like_interval`: a prefix range, or an equality for a wildcard-free
    // pattern; ASCII patterns without backslashes only.
    static std::optional<Interval> like_interval(const std::string& pattern) {
        if (!is_ascii(pattern) || pattern.find('\\') != std::string::npos) return std::nullopt;
        size_t cut = pattern.size();
        for (size_t k = 0; k < pattern.size(); ++k) {
            if (pattern[k] == '%' || pattern[k] == '_') {
                cut = k;
                break;
            }
        }
        const std::string prefix = pattern.substr(0, cut);
        if (prefix.empty()) return std::nullopt;
        if (cut == pattern.size()) {
            Interval out;
            out.lo = BVal::of_bytes(prefix);
            out.hi = BVal::of_bytes(prefix);
            return out;
        }
        return prefix_interval(BVal::of_bytes(prefix));
    }

    // `_comparison_interval`: (expression, interval) for `expr <op> literal`
    // in either operand order.
    std::optional<std::pair<ExprId, Interval>> comparison_interval(const ExprRow& r) const {
        TermOp op = term_op(r.value);
        const ExprId left = unwrap(r.left), right = unwrap(r.right);
        if (left == kNoExpr || right == kNoExpr) return std::nullopt;
        ExprId expression, literal;
        const bool left_literal = is(left, in.kinds->literal), right_literal = is(right, in.kinds->literal);
        if (right_literal && !left_literal) {
            expression = left;
            literal = right;
        } else if (left_literal && !right_literal) {
            expression = right;
            literal = left;
            switch (op) {
                case OP_GT: op = OP_LT; break;
                case OP_GTEQ: op = OP_LTEQ; break;
                case OP_LT: op = OP_GT; break;
                case OP_LTEQ: op = OP_GTEQ; break;
                case OP_EQ: break;
                default: op = OP_NONE; break;
            }
        } else {
            return std::nullopt;
        }
        if (op != OP_EQ && op != OP_GT && op != OP_GTEQ && op != OP_LT && op != OP_LTEQ) return std::nullopt;
        auto v = bval_of_literal(row(literal).literal);
        if (!v) return std::nullopt;
        Interval iv;
        switch (op) {
            case OP_EQ: iv.lo = *v; iv.hi = *v; break;
            case OP_GT: iv.lo = *v; iv.lo_closed = false; break;
            case OP_GTEQ: iv.lo = *v; break;
            case OP_LT: iv.hi = *v; iv.hi_closed = false; break;
            default: iv.hi = *v; break;
        }
        return std::make_pair(expression, iv);
    }

    // `_conjunct_interval`: (identifier, interval) for a conjunct that confines
    // exactly ONE column.
    Resolved conjunct_interval(ExprId conjunct, int depth) const {
        conjunct = unwrap(conjunct);
        if (conjunct == kNoExpr || depth > 8) return std::nullopt;
        const ExprRow& r = row(conjunct);
        const NodeKinds& k = *in.kinds;

        if (r.kind == k.comparison) {
            if (r.value == "InList" || r.value == "Like") {
                const ExprId left = unwrap(r.left), right = unwrap(r.right);
                if (!is(left, k.identifier) || !is(right, k.literal)) return std::nullopt;
                const LiteralValue& lit = row(right).literal;
                std::optional<Interval> iv;
                if (r.value == "InList") {
                    iv = in_list_interval(lit);
                } else if (lit.tag == LITERAL_BYTES) {
                    iv = like_interval(lit.bytes);
                }
                if (!iv) return std::nullopt;
                return std::make_pair(left, *iv);
            }
            auto comparison = comparison_interval(r);
            if (!comparison) return std::nullopt;
            return preimage(comparison->first, comparison->second);
        }

        if (r.kind == k.between) {
            const ExprId left = unwrap(r.left), lower = unwrap(r.right), upper = unwrap(r.centre);
            if (left == kNoExpr || !is(lower, k.literal) || !is(upper, k.literal)) return std::nullopt;
            auto lo = bval_of_literal(row(lower).literal);
            auto hi = bval_of_literal(row(upper).literal);
            if (!lo || !hi) return std::nullopt;
            Interval iv;
            iv.lo = *lo;
            iv.hi = *hi;
            return preimage(left, iv);
        }

        if (r.kind == k.function) {
            // `_STARTS_WITH(col, b'abc')` - an anchored LIKE, lowered.
            if (r.value != "_STARTS_WITH" || r.parameters.size() != 2) return std::nullopt;
            const ExprId identifier = unwrap(r.parameters[0]);
            if (!is(identifier, k.identifier)) return std::nullopt;
            const LiteralValue* pattern = literal_at(r.parameters[1]);
            if (pattern == nullptr) return std::nullopt;
            auto value = bval_of_literal(*pattern);
            if (!value) return std::nullopt;
            auto iv = prefix_interval(*value);
            if (!iv) return std::nullopt;
            return std::make_pair(identifier, *iv);
        }

        if (r.kind == k.or_ || r.kind == k.cnf) {
            // Constrains a column only when EVERY arm constrains THAT column.
            std::vector<ExprId> arms;
            if (r.kind == k.cnf) {
                arms = r.parameters;
            } else {
                arms = {r.left, r.right};
            }
            if (arms.size() < 2) return std::nullopt;
            ExprId identifier = kNoExpr;
            std::string key;
            std::vector<Interval> intervals;
            for (ExprId arm : arms) {
                auto resolved = conjunct_interval(arm, depth + 1);
                if (!resolved) return std::nullopt;
                const std::string arm_key = identity_key(resolved->first);
                if (arm_key.empty()) return std::nullopt;
                if (identifier == kNoExpr) {
                    identifier = resolved->first;
                    key = arm_key;
                } else if (arm_key != key) {
                    return std::nullopt;
                }
                intervals.push_back(resolved->second);
            }
            bool any = false, numbers = true, bytes = true;
            for (const Interval& iv : intervals) {
                for (const std::optional<BVal>* bound : {&iv.lo, &iv.hi}) {
                    if (!bound->has_value()) continue;
                    any = true;
                    numbers = numbers && (*bound)->number();
                    bytes = bytes && (*bound)->kind == BKind::BYTES;
                }
            }
            if (any && !numbers && !bytes) return std::nullopt;
            return std::make_pair(identifier, hull(intervals));
        }
        return std::nullopt;
    }

    // `_is_canonical`: a shape the pruners read as it stands (no NESTED unwrap).
    bool is_canonical(const ExprRow& r) const {
        const NodeKinds& k = *in.kinds;
        if (r.kind == k.comparison) {
            return term_op(r.value) != OP_NONE && is(r.left, k.identifier) && is(r.right, k.literal);
        }
        if (r.kind == k.between) {
            return is(r.left, k.identifier) && is(r.right, k.literal) && is(r.centre, k.literal);
        }
        return false;
    }

    // `_emit`: canonical terms for `identifier`'s value lying in `iv`.
    void emit(ExprId identifier, const Interval& iv, std::vector<BoundTerm>& out) const {
        if (!iv.lo && !iv.hi) return;
        BoundTerm base;
        base.column = row(identifier).source_column;
        base.identifier = identifier;
        base.derived = true;
        if (iv.lo && iv.hi && iv.lo_closed && iv.hi_closed && eq(*iv.lo, *iv.hi)) {
            BoundTerm t = base;
            t.op = OP_EQ;
            t.value = *iv.lo;
            out.push_back(std::move(t));
            return;
        }
        if (iv.lo) {
            BoundTerm t = base;
            t.op = iv.lo_closed ? OP_GTEQ : OP_GT;
            t.value = *iv.lo;
            out.push_back(std::move(t));
        }
        if (iv.hi) {
            BoundTerm t = base;
            t.op = iv.hi_closed ? OP_LTEQ : OP_LT;
            t.value = *iv.hi;
            out.push_back(std::move(t));
        }
    }

    // `_inner_split`: the ANDed terms (binary AND and n-ary DNF, through NESTED);
    // never through an OR.
    void split(ExprId id, std::vector<ExprId>& out) const {
        id = unwrap(id);
        if (id == kNoExpr) return;
        const ExprRow& r = row(id);
        if (r.kind == in.kinds->dnf) {
            for (ExprId p : r.parameters) split(p, out);
            return;
        }
        if (r.kind != in.kinds->and_) {
            out.push_back(id);
            return;
        }
        split(r.left, out);
        split(r.right, out);
    }
};

}  // namespace bounds_detail

// The ANDed conjuncts of `predicates`.
inline std::vector<ExprId> split_conjuncts(const DeriveInputs& in, const std::vector<ExprId>& predicates) {
    bounds_detail::Deriver d{in};
    std::vector<ExprId> out;
    for (ExprId p : predicates) {
        if (p != kNoExpr) d.split(p, out);
    }
    return out;
}

// The canonical terms of `conjuncts` as they stand (`column <op> literal` with a
// comparison operator the pruners know, and `column BETWEEN literal AND
// literal`), in order, then a DERIVED term for every conjunct that confines a
// column in another shape (`derive_bound_conjuncts`).
//
// Derived terms never trip the temporal-domain guard: a temporal column's derived
// literal takes the column's own type, and any other column's literal is not
// temporal - so they carry no literal type.
inline std::vector<BoundTerm> derive_bound_terms(const DeriveInputs& in, const std::vector<ExprId>& conjuncts) {
    bounds_detail::Deriver d{in};
    const NodeKinds& k = *in.kinds;
    std::vector<BoundTerm> originals, derived;
    for (ExprId c : conjuncts) {
        const ExprRow& r = d.row(c);
        if (d.is_canonical(r)) {
            BoundTerm t;
            const ExprRow& left = d.row(r.left);
            t.column = left.source_column;
            t.identifier = r.left;
            if (r.kind == k.between) {
                auto lo = bval_of_literal(d.row(r.right).literal);
                auto hi = bval_of_literal(d.row(r.centre).literal);
                t.op = OP_BETWEEN;
                // a NULL end is carried as an OPAQUE value no bound compares with
                t.value = lo ? *lo : BVal{BKind::OPAQUE};
                t.upper = hi ? *hi : BVal{BKind::OPAQUE};
                t.value.literal = &d.row(r.right).literal;
                t.upper.literal = &d.row(r.centre).literal;
                t.literal_type = d.row(r.right).type_id;
                t.upper_type = d.row(r.centre).type_id;
            } else {
                auto v = bval_of_literal(d.row(r.right).literal);
                t.op = term_op(r.value);
                t.value = v ? *v : BVal{BKind::OPAQUE};
                t.value.literal = &d.row(r.right).literal;
                t.literal_type = d.row(r.right).type_id;
            }
            originals.push_back(std::move(t));
            continue;
        }
        auto resolved = d.conjunct_interval(c, 0);
        if (!resolved) continue;
        const ExprId identifier = resolved->first;
        const Interval& iv = resolved->second;
        if (d.row(identifier).source_column.empty()) continue;
        if (iv.lo && iv.hi) {
            if (!comparable_pair(*iv.lo, *iv.hi)) continue;
            if (lt(*iv.hi, *iv.lo)) continue;   // inverted: a derivation bug looks the same
        }
        d.emit(identifier, iv, derived);
    }
    originals.insert(originals.end(), std::make_move_iterator(derived.begin()), std::make_move_iterator(derived.end()));
    return originals;
}

// `derive_null_terms`: IS NULL / IS NOT NULL over a bare column.
inline std::vector<NullTerm> derive_null_terms(const DeriveInputs& in, const std::vector<ExprId>& conjuncts) {
    bounds_detail::Deriver d{in};
    std::vector<NullTerm> out;
    for (ExprId c : conjuncts) {
        c = d.unwrap(c);
        if (!d.is(c, in.kinds->unary)) continue;
        const ExprRow& r = d.row(c);
        if (r.value != "IsNull" && r.value != "IsNotNull") continue;
        const ExprId operand = d.unwrap(r.centre);
        if (!d.is(operand, in.kinds->identifier)) continue;
        const std::string& column = d.row(operand).source_column;
        if (column.empty()) continue;
        out.push_back(NullTerm{column, r.value == "IsNull"});
    }
    return out;
}

// `derive_case_fold_conjuncts`: bounds valid only where a case fold is the
// identity on the column.
inline std::vector<FoldTerm> derive_fold_terms(const DeriveInputs& in, const std::vector<ExprId>& conjuncts) {
    bounds_detail::Deriver d{in};
    const NodeKinds& k = *in.kinds;
    std::vector<FoldTerm> out;
    auto ascii_lower = [](std::string s) {
        for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return s;
    };
    for (ExprId c : conjuncts) {
        c = d.unwrap(c);
        if (c == kNoExpr) continue;
        const ExprRow& r = d.row(c);
        ExprId identifier = kNoExpr;
        bool lower = true;
        std::optional<Interval> iv;

        if (r.kind == k.comparison && r.value == "ILike") {
            const ExprId left = d.unwrap(r.left), right = d.unwrap(r.right);
            if (d.is(left, k.identifier) && d.is(right, k.literal)) {
                const LiteralValue& lit = d.row(right).literal;
                if (lit.tag == LITERAL_BYTES && is_ascii(lit.bytes)) {
                    identifier = left;
                    iv = bounds_detail::Deriver::like_interval(ascii_lower(lit.bytes));
                }
            }
        } else if (r.kind == k.comparison) {
            auto comparison = d.comparison_interval(r);
            if (comparison) {
                const ExprId expression = d.unwrap(comparison->first);
                if (d.is(expression, k.function)) {
                    const ExprRow& f = d.row(expression);
                    if ((f.value == "LOWER" || f.value == "UPPER") && f.parameters.size() == 1) {
                        const ExprId inner = d.unwrap(f.parameters[0]);
                        if (d.is(inner, k.identifier)) {
                            identifier = inner;
                            lower = f.value == "LOWER";
                            iv = comparison->second;
                        }
                    }
                }
            }
        } else if (r.kind == k.function && r.value == "_CI_STARTS_WITH" && r.parameters.size() == 2) {
            const ExprId inner = d.unwrap(r.parameters[0]);
            const LiteralValue* pattern = d.literal_at(r.parameters[1]);
            if (d.is(inner, k.identifier) && pattern != nullptr && pattern->tag == LITERAL_BYTES &&
                is_ascii(pattern->bytes)) {
                identifier = inner;
                iv = prefix_interval(BVal::of_bytes(ascii_lower(pattern->bytes)));
            }
        }

        if (identifier == kNoExpr || !iv || d.row(identifier).source_column.empty()) continue;
        if (iv->lo && iv->hi && (!comparable_pair(*iv->lo, *iv->hi) || lt(*iv->hi, *iv->lo))) continue;
        FoldTerm term;
        term.column = d.row(identifier).source_column;
        term.lower = lower;
        d.emit(identifier, *iv, term.terms);
        if (!term.terms.empty()) out.push_back(std::move(term));
    }
    return out;
}

}  // namespace opteryx::planner
