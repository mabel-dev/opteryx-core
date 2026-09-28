// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/selectivity.hpp — predicate selectivity and evaluation cost,
// read from the query's expression arena (native plan graph P5, S4).
//
// Ported from the Python cost_estimation/selectivity.py and predicate_cost.py
// (deleted at the P5 cut-over): a predicate (an ExprId) and a relation's
// statistics (stats_store.hpp) in, the fraction of rows matching it out, in
// [0, 1]. The port was verified bit-identical to the Python over every estimate
// of the planner corpus and test suites (py_numeric.hpp; compiled without FMA
// contraction). For each predicate kind:
//   1. the histogram, when the column has one;
//   2. what the column's NDV, value range, ordinal bounds or byte-class
//      statistics imply;
//   3. a textbook constant.
//
// Two departures from the Python, both where it had no defined answer:
//   - a _STARTS_WITH/_ENDS_WITH operand that is not a literal is unresolvable
//     (the Python used a non-literal's NAME as the needle);
//   - where the Python raised (ordinalizing a prefix for a numeric column), a
//     SelectivityRefused is thrown - the caller prices the predicate, the
//     estimator never silently does.

#pragma once

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"
#include "ops/ordinalize.h"
#include "planner/column_table.hpp"
#include "planner/column_type.hpp"
#include "planner/expr_arena.hpp"
#include "planner/join_estimator.hpp"
#include "planner/py_numeric.hpp"
#include "planner/stats_store.hpp"

namespace opteryx::planner {

// The no-statistics fallbacks (fallback_selectivity.py reads these).
inline constexpr double kRangeFallbackSelectivity = 0.25;
inline constexpr double kLikePrefixSelectivity = 0.25;
inline constexpr double kLikeInfixSelectivity = 0.1;
// An inequality between two columns: per-column statistics say nothing about
// their correlation (Selinger et al.).
inline constexpr double kColumnVsColumnRangeSelectivity = 1.0 / 3.0;
inline constexpr double kIsNullFallbackSelectivity = 0.05;
inline constexpr double kInListFallbackSelectivity = 0.1;   // per member, and for a non-list operand

// The estimator could not price the predicate (the Python raised).
struct SelectivityRefused : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// What an estimate reads besides the statistics: the arena, the column rows
// (a column's root slot and type) and the type table.
struct SelectivityInputs {
    const ExprTable* exprs = nullptr;
    const ColumnRows* columns = nullptr;
    const ColumnTypeTable* types = nullptr;
    const NodeKinds* kinds = nullptr;
};

namespace selectivity_detail {

// The 8 byte classes of the infix LIKE char-class estimator, in the order the
// native char_class_stats kernel counts them (ColumnStats::class_proportions).
enum CharClass : int { UPPER = 0, LOWER = 1, DIGIT = 2, WHITESPACE = 3, PUNCT_TEXT = 4, SEMANTIC = 5, EXTENDED = 6, CONTROL = 7 };

// Distinct byte values per class (a class proportion back to a per-byte
// probability, assuming uniformity within the class).
inline constexpr double kClassCardinality[kCharClasses] = {26, 26, 10, 6, 10, 22, 128, 28};

// Byte -> class for ASCII; tests/unit/compiled/test_char_class_stats_parity.py
// keeps the Python table, the native kernel's and scratch/like_selectivity's in
// step - this is the Python table's ASCII half (every byte >= 0x80, and every
// code point beyond, is EXTENDED).
inline constexpr uint8_t kAsciiClass[128] = {
    7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 3, 3, 3, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    3, 4, 4, 5, 5, 5, 5, 4, 4, 4, 5, 5, 4, 4, 4, 5,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 4, 5, 5, 5, 4,
    5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 5, 5, 5,
    5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 5, 5, 7,
};

inline constexpr int kExtendedClass = EXTENDED;

inline int classify(uint32_t code_point) {
    return code_point < 128 ? kAsciiClass[code_point] : EXTENDED;
}

inline double log_char_floor() {
    static const double floor = std::log(1e-6);
    return floor;
}

inline double clamp01(double value) {
    // NaN is "could not compute": no reduction, never "nothing matches".
    if (value != value) return 1.0;
    if (value < 0.0) return 0.0;
    if (value > 1.0) return 1.0;
    return value;
}

// Python truthiness of an optional integer statistic.
inline bool truthy(int64_t v) { return v != kNoStat && v != 0; }

// `float(value)` of a literal's value, as _to_float: false for None and for
// a value float() refuses.
inline bool to_float(const LiteralValue& v, double& out) {
    switch (v.tag) {
        case LITERAL_BOOL: out = v.i != 0 ? 1.0 : 0.0; return true;
        case LITERAL_INT64: out = static_cast<double>(v.i); return true;
        case LITERAL_UINT64: out = static_cast<double>(static_cast<uint64_t>(v.i)); return true;
        case LITERAL_DOUBLE: out = v.d; return true;
        case LITERAL_BYTES: return py::float_from_bytes(v.bytes, out);
        case LITERAL_DECIMAL: out = py::decimal_to_double(v.i, v.j, v.k); return true;
        default: return false;   // NULL, an INTERVAL or ARRAY (tuples), a draft
    }
}

inline bool bound_to_float(const StatBound& b, double& out) {
    if (!b.present()) return false;
    out = b.as_double();
    return true;
}

// One value's share of a bin-width probe's mass (see _bin_mass_point_share).
inline double point_share(double density, int64_t bin_count, int64_t ndv) {
    if (truthy(ndv) && ndv > bin_count && bin_count > 0) {
        return density * static_cast<double>(bin_count) / static_cast<double>(ndv);
    }
    return density;
}

// `1 - exp(-n_positions * p_pos)` over the candidate start offsets.
inline double containment(double p_pos, double avg_length, size_t needle_len) {
    const double n_positions = py::max(avg_length - static_cast<double>(needle_len) + 1.0, 0.0);
    if (n_positions <= 0 || p_pos <= 0) return 0.0;
    const double exponent = n_positions * p_pos;
    return clamp01(1.0 - std::exp(-exponent));
}

inline double plain_char_probability(uint32_t c, const ColumnStats& col) {
    const int cls = classify(c);
    return col.class_proportions[cls] / kClassCardinality[cls];
}

// Case-insensitive: an alphabetic byte matches either case, and upper and
// lower have the same cardinality.
inline double ci_char_probability(uint32_t c, const ColumnStats& col) {
    const int cls = classify(c);
    if (cls == UPPER || cls == LOWER) {
        return (col.class_proportions[UPPER] + col.class_proportions[LOWER]) / kClassCardinality[LOWER];
    }
    return col.class_proportions[cls] / kClassCardinality[cls];
}

inline double log_char(double p_char) { return p_char > 0 ? std::log(p_char) : log_char_floor(); }

// An infix needle: each position's log-probability discounted by decay**i.
inline double decayed_char_class(const std::vector<uint32_t>& needle, const ColumnStats& col, double decay) {
    if (needle.empty()) return 1.0;
    double log_p_pos = 0.0;
    for (size_t i = 0; i < needle.size(); ++i) {
        log_p_pos += std::pow(decay, static_cast<double>(i)) * log_char(plain_char_probability(needle[i], col));
    }
    return containment(std::exp(log_p_pos), col.avg_length, needle.size());
}

// A needle with one anchor (a suffix, or a case-insensitive prefix).
template <typename Probability>
inline double anchored_char_class(const std::vector<uint32_t>& needle, const ColumnStats& col, Probability probability) {
    if (needle.empty()) return 1.0;
    double log_p = 0.0;
    for (uint32_t c : needle) log_p += log_char(probability(c, col));
    const double p = std::exp(log_p);
    const double discount = py::min(1.0, col.avg_length / static_cast<double>(needle.size()));
    return clamp01(p * discount);
}

inline bool string_family(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY || t == DRAKEN_VARIANT;
}

// The types whose scalar ordinalize raised ValueError on a bytes prefix (the
// Python caught it: the flat prefix constant). Every other non-string type
// raised TypeError, which escaped the estimator.
inline bool ordinalize_value_error(DrakenType t) {
    switch (t) {
        case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64: case DRAKEN_TIME32: case DRAKEN_TIME64:
        case DRAKEN_INTERVAL: case DRAKEN_ARRAY: case DRAKEN_NULL: case DRAKEN_VECTOR_FP16:
        case DRAKEN_DECIMAL128:
            return true;
        default:
            return false;
    }
}

// predicate_cost's per-row comparison cost of a column's category.
inline double comparison_cost(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
            return 0.001;
        case DRAKEN_DECIMAL: case DRAKEN_DECIMAL128: return 1.533;
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64: return 0.002;
        case DRAKEN_DATE32: return 0.008;
        case DRAKEN_TIMESTAMP64: return 0.008;
        case DRAKEN_TIME32: case DRAKEN_TIME64: return 10.00;
        case DRAKEN_INTERVAL: return 10.00;
        case DRAKEN_BOOL: return 0.003;
        case DRAKEN_VARCHAR: return 0.231;
        case DRAKEN_NVARCHAR: return 10.00;
        case DRAKEN_VARBINARY: return 0.058;
        case DRAKEN_ARRAY: return 10.00;
        case DRAKEN_NULL: return 10.00;
        case DRAKEN_VARIANT: case DRAKEN_VECTOR_FP16: return 10.0;   // categories with no measured cost
        default: throw SelectivityRefused("predicate_cost: no dispatch category for the column's physical type");
    }
}

// Pattern-matching operators, whatever their operand types.
inline bool operation_cost(const std::string& op, double& out) {
    if (op == "InStr" || op == "IInStr" || op == "NotInStr" || op == "NotIInStr" || op == "Like" || op == "ILike" ||
        op == "NotLike" || op == "NotILike") {
        out = 2.5;
        return true;
    }
    if (op == "RLike" || op == "NotRLike") {
        out = 3.0;
        return true;
    }
    return false;
}

// A FUNCTION with no measured catalog cost is expensive, not free.
inline constexpr double kUnknownFunctionCost = 100.0;
inline constexpr double kUnknownComparisonCost = 10.0;

struct Estimator {
    const SelectivityInputs& in;
    const RelationStats* stats;   // null: the cost walk, which reads none

    const ExprRow& row(ExprId id) const { return in.exprs->row(id); }
    const NodeKinds& k() const { return *in.kinds; }

    // _identifier_identity: an IDENTIFIER's column, as its root slot.
    Slot identity(ExprId id) const {
        if (id == kNoExpr) return kNoSlot;
        const ExprRow& r = row(id);
        if (r.kind != k().identifier || r.column_slot == kNoColumnSlot) return kNoSlot;
        return in.columns->root(r.column_slot);
    }

    const ColumnStats* column(Slot root) const { return root == kNoSlot ? nullptr : stats->column(root); }

    // _physical_type: the bound column's physical type.
    bool physical(ExprId id, DrakenType& out) const {
        const ExprRow& r = row(id);
        if (r.column_slot == kNoColumnSlot) return false;
        const ColumnTypeId type_id = in.columns->row(r.column_slot).type_id;
        if (type_id == kNoColumnType) return false;
        out = in.types->entry(type_id).physical;
        return true;
    }

    // A needle longer than the column's longest observed value cannot match.
    // Not for NVARCHAR: its length bounds may be CHARACTER counts.
    static bool exceeds_max_length(size_t needle_len, const ColumnStats* col, bool has_physical, DrakenType physical) {
        if (has_physical && physical == DRAKEN_NVARCHAR) return false;
        if (col == nullptr || !col->has_length_bounds) return false;
        return static_cast<int64_t>(needle_len) > col->length_hi;
    }

    // ---- dispatch ----------------------------------------------------------

    double estimate(ExprId id) const {
        if (id == kNoExpr) return 1.0;
        const ExprRow& r = row(id);
        const int32_t kind = r.kind;
        if (kind == k().and_) return estimate(r.left) * estimate(r.right);
        if (kind == k().or_) {
            const double s1 = estimate(r.left);
            const double s2 = estimate(r.right);
            return 1.0 - (1.0 - s1) * (1.0 - s2);
        }
        // DNF / CNF: the n-ary AND / OR (PredicateOrdering builds them).
        if (kind == k().dnf) {
            double product = 1.0;
            for (ExprId term : r.parameters) product *= estimate(term);
            return product;
        }
        if (kind == k().cnf) {
            double complement = 1.0;
            for (ExprId term : r.parameters) complement *= 1.0 - estimate(term);
            return 1.0 - complement;
        }
        if (kind == k().not_) return 1.0 - estimate(r.centre);
        if (kind == k().unary) {
            const Slot identity_ = identity(r.centre);
            if (identity_ == kNoSlot) return 1.0;
            if (r.value == "IsNull") return is_null(identity_);
            if (r.value == "IsNotNull") return 1.0 - is_null(identity_);
            return 1.0;
        }
        if (kind == k().between) return between(r);
        if (kind == k().comparison) return comparison(r);
        if (kind == k().function) {
            if (r.value == "_STARTS_WITH") return starts_with(r);
            if (r.value == "_CI_STARTS_WITH") return ci_starts_with(r);
            if (r.value == "_ENDS_WITH") return ends_with(r, false);
            if (r.value == "_CI_ENDS_WITH") return ends_with(r, true);
            return 1.0;
        }
        return 1.0;
    }

    double comparison(const ExprRow& r) const {
        std::string op = r.value;
        const Slot left = identity(r.left);
        const Slot right = identity(r.right);
        if (left != kNoSlot && right != kNoSlot) return column_vs_column(op, left, right);

        Slot identity_ = left;
        ExprId literal = r.right;
        if (identity_ == kNoSlot) {
            identity_ = right;
            literal = r.left;
            if (op == "Lt") op = "Gt";
            else if (op == "LtEq") op = "GtEq";
            else if (op == "Gt") op = "Lt";
            else if (op == "GtEq") op = "LtEq";
        }
        if (identity_ == kNoSlot) return 1.0;
        if (literal == kNoExpr || row(literal).kind != k().literal) return 1.0;
        const LiteralValue& value = row(literal).literal;

        if (op == "Eq") return eq(identity_, value);
        if (op == "NotEq") return 1.0 - eq(identity_, value);
        if (op == "Lt" || op == "LtEq" || op == "Gt" || op == "GtEq") return range(identity_, op, value);
        if (op == "InList") return in_list(identity_, value);
        if (op == "NotInList") return 1.0 - in_list(identity_, value);
        if (op == "Like" || op == "ILike" || op == "RLike") return like(value);
        if (op == "NotLike" || op == "NotILike" || op == "NotRLike") return 1.0 - like(value);
        if (op == "InStr" || op == "IInStr") return instr(identity_, value, r);
        if (op == "NotInStr" || op == "NotIInStr") return 1.0 - instr(identity_, value, r);
        return 1.0;
    }

    // ---- per-predicate-kind estimators --------------------------------------

    double eq(Slot identity_, const LiteralValue& value) const {
        const ColumnStats* col = column(identity_);
        const maki_nage::Distogram* dgram = col != nullptr ? col->histogram.get() : nullptr;
        double lit = 0.0;
        const bool has_lit = to_float(value, lit);
        if (dgram != nullptr && has_lit) {
            const double total = static_cast<double>(dgram->count());
            if (total > 0) {
                const int64_t bins = dgram->bin_count();
                if (bins > 0) {
                    const double span = dgram->max() - dgram->min();
                    if (span > 0 && bins > 1) {
                        const double bin_width = span / static_cast<double>(bins);
                        const double below = dgram->count_up_to(lit - bin_width / 2.0);
                        const double above = dgram->count_up_to(lit + bin_width / 2.0);
                        const double density = (above - below) / total;
                        return clamp01(point_share(density, bins, col->distinct_count));
                    }
                    if (span == 0) return lit == dgram->min() ? 1.0 : 0.0;
                }
            }
        }
        const int64_t ndv = col != nullptr ? col->distinct_count : kNoStat;
        if (truthy(ndv) && ndv > 0) return 1.0 / static_cast<double>(ndv);
        return kEqUnknownNdvFallback;
    }

    double column_vs_column(const std::string& op, Slot left, Slot right) const {
        if (left == right) {
            // a column compared to itself: always / never true
            if (op == "Eq" || op == "LtEq" || op == "GtEq") return 1.0;
            if (op == "NotEq" || op == "Lt" || op == "Gt") return 0.0;
            return 1.0;
        }
        if (op == "Eq" || op == "NotEq") {
            const ColumnStats* l = column(left);
            const ColumnStats* r = column(right);
            const int64_t left_ndv = l != nullptr ? l->distinct_count : kNoStat;
            const int64_t right_ndv = r != nullptr ? r->distinct_count : kNoStat;
            double eq_selectivity = kEqUnknownNdvFallback;
            if (truthy(left_ndv) && truthy(right_ndv)) {
                eq_selectivity = 1.0 / static_cast<double>(left_ndv > right_ndv ? left_ndv : right_ndv);
            }
            return op == "Eq" ? eq_selectivity : 1.0 - eq_selectivity;
        }
        if (op == "Lt" || op == "LtEq" || op == "Gt" || op == "GtEq") return kColumnVsColumnRangeSelectivity;
        return 1.0;
    }

    double range(Slot identity_, const std::string& op, const LiteralValue& value) const {
        const ColumnStats* col = column(identity_);
        const maki_nage::Distogram* dgram = col != nullptr ? col->histogram.get() : nullptr;
        double lit = 0.0;
        const bool has_lit = to_float(value, lit);
        const bool below_op = op == "Lt" || op == "LtEq";
        if (dgram != nullptr && has_lit) {
            const double total = static_cast<double>(dgram->count());
            if (total > 0) {
                const double fraction_below = dgram->count_up_to(lit) / total;
                return below_op ? clamp01(fraction_below) : clamp01(1.0 - fraction_below);
            }
        }
        // No histogram: interpolate across the column's known value range.
        if (has_lit && col != nullptr) {
            double lower = 0.0;
            double upper = 0.0;
            if (bound_to_float(col->lower, lower) && bound_to_float(col->upper, upper) && upper > lower) {
                const double fraction_below = (lit - lower) / (upper - lower);
                return below_op ? clamp01(fraction_below) : clamp01(1.0 - fraction_below);
            }
        }
        return kRangeFallbackSelectivity;
    }

    double in_list(Slot identity_, const LiteralValue& value) const {
        // The members: an ARRAY's items (an INTERVAL is a (months, us) tuple
        // to the Python, so it counts as two).
        std::vector<LiteralValue> interval_members;
        const std::vector<LiteralValue>* members = nullptr;
        if (value.tag == LITERAL_ITEMS) {
            members = &value.items;
        } else if (value.tag == LITERAL_INTERVAL) {
            interval_members.resize(2);
            interval_members[0].tag = LITERAL_INT64;
            interval_members[0].i = value.i;
            interval_members[1].tag = LITERAL_INT64;
            interval_members[1].i = value.j;
            members = &interval_members;
        } else {
            return kInListFallbackSelectivity;
        }
        const int64_t n = static_cast<int64_t>(members->size());
        if (n == 0) return 0.0;

        const ColumnStats* col = column(identity_);
        const maki_nage::Distogram* dgram = col != nullptr ? col->histogram.get() : nullptr;
        if (dgram != nullptr) {
            const double total = static_cast<double>(dgram->count());
            if (total > 0 && dgram->bin_count() > 0) {
                const double span = dgram->max() - dgram->min();
                if (span > 0) {
                    const int64_t bins = dgram->bin_count();
                    const double bin_width = span / static_cast<double>(bins);
                    double accumulated = 0.0;
                    bool coerced_any = false;
                    for (const LiteralValue& member : *members) {
                        double f = 0.0;
                        if (!to_float(member, f)) continue;
                        coerced_any = true;
                        const double below = dgram->count_up_to(f - bin_width / 2.0);
                        const double above = dgram->count_up_to(f + bin_width / 2.0);
                        // each member is a point probe: `x IN (v)` agrees with `x = v`
                        accumulated += point_share((above - below) / total, bins, col->distinct_count);
                    }
                    if (coerced_any) return clamp01(accumulated);
                }
            }
        }
        const int64_t ndv = col != nullptr ? col->distinct_count : kNoStat;
        if (truthy(ndv) && ndv > 0) return py::min(1.0, py::true_divide(n, ndv));
        return py::min(1.0, static_cast<double>(n) * kInListFallbackSelectivity);
    }

    double between(const ExprRow& r) const {
        const Slot identity_ = identity(r.left);
        if (identity_ == kNoSlot) return 1.0;
        if (r.right == kNoExpr || r.centre == kNoExpr) return 1.0;
        if (row(r.right).kind != k().literal || row(r.centre).kind != k().literal) return 1.0;
        double a = 0.0;
        double b = 0.0;
        const bool has_a = to_float(row(r.right).literal, a);
        const bool has_b = to_float(row(r.centre).literal, b);

        const ColumnStats* col = column(identity_);
        const maki_nage::Distogram* dgram = col != nullptr ? col->histogram.get() : nullptr;
        if (dgram != nullptr && has_a && has_b) {
            const double total = static_cast<double>(dgram->count());
            if (total > 0) {
                double lo = a <= b ? a : b;
                double hi = a <= b ? b : a;
                // A histogram cannot resolve a window narrower than one bin:
                // widen the probe to one bin width about the window's centre.
                const int64_t bins = dgram->bin_count();
                const double span = dgram->max() - dgram->min();
                if (bins > 1 && span > 0) {
                    const double bin_width = span / static_cast<double>(bins);
                    if ((hi - lo) < bin_width) {
                        const double centre = (lo + hi) / 2.0;
                        lo = centre - bin_width / 2.0;
                        hi = centre + bin_width / 2.0;
                    }
                }
                return clamp01((dgram->count_up_to(hi) - dgram->count_up_to(lo)) / total);
            }
        }
        return kRangeFallbackSelectivity;
    }

    double is_null(Slot identity_) const {
        const ColumnStats* col = column(identity_);
        if (col == nullptr || !col->has_null_fraction) return kIsNullFallbackSelectivity;
        return clamp01(col->null_fraction);
    }

    static double like(const LiteralValue& value) {
        std::vector<uint32_t> pattern;
        if (value.tag != LITERAL_BYTES || !py::utf8_code_points(value.bytes, pattern)) return kLikeInfixSelectivity;
        const std::string& p = value.bytes;
        // only a trailing '%': a prefix pattern
        if (!p.empty() && p.back() == '%' && p.find('%') == p.size() - 1 && p.find('_') == std::string::npos) {
            return kLikePrefixSelectivity;
        }
        return kLikeInfixSelectivity;
    }

    // `x LIKE '%needle%'` (rewritten to InStr): the char-class model when the
    // column has byte-class statistics and the binder captured a decay.
    double instr(Slot identity_, const LiteralValue& value, const ExprRow& r) const {
        const ColumnStats* col = column(identity_);
        std::vector<uint32_t> needle;
        const bool has_needle = value.tag == LITERAL_BYTES && py::utf8_code_points(value.bytes, needle);
        if (has_needle) {
            const ExprId column_node = identity(r.left) == identity_ ? r.left : r.right;
            DrakenType physical_type = DRAKEN_NULL;
            const bool has_physical = physical(column_node, physical_type);
            if (exceeds_max_length(value.bytes.size(), col, has_physical, physical_type)) return 0.0;
        }
        if (col != nullptr && has_needle && col->has_char_class && col->avg_length != 0 && col->avg_length > 0 &&
            (r.flags & FLAG_HAS_LIKE_DECAY) != 0) {
            return decayed_char_class(needle, *col, r.like_selectivity_decay);
        }
        return kLikeInfixSelectivity;
    }

    // ---- prefix / suffix (the rewritten LIKE 'x%' / '%x') -------------------

    // `FUNCTION(column, literal)`: the column expression, its identity and the
    // literal's bytes; false when any part is missing.
    bool two_operands(const ExprRow& r, ExprId& column_node, Slot& identity_, const std::string*& literal) const {
        if (r.parameters.size() != 2) return false;
        column_node = r.parameters[0];
        identity_ = identity(column_node);
        if (identity_ == kNoSlot) return false;
        const ExprId literal_node = r.parameters[1];
        if (literal_node == kNoExpr) return false;
        const ExprRow& l = row(literal_node);
        if (l.kind != k().literal || l.literal.tag != LITERAL_BYTES) return false;
        literal = &l.literal.bytes;
        return true;
    }

    double starts_with(const ExprRow& r) const {
        ExprId column_node = kNoExpr;
        Slot identity_ = kNoSlot;
        const std::string* prefix = nullptr;
        if (!two_operands(r, column_node, identity_, prefix)) return kLikePrefixSelectivity;
        const ColumnStats* col = column(identity_);
        DrakenType physical_type = DRAKEN_NULL;
        const bool has_physical = physical(column_node, physical_type);
        if (col == nullptr || !has_physical) return kLikePrefixSelectivity;
        if (exceeds_max_length(prefix->size(), col, true, physical_type)) return 0.0;

        if (!string_family(physical_type)) {
            if (ordinalize_value_error(physical_type)) return kLikePrefixSelectivity;
            throw SelectivityRefused("a prefix cannot be ordinalized for a non-string column");
        }
        // The prefix's ordinal-key range: [ordinalize(prefix),
        // ordinalize(prefix 0xFF-padded to 8 bytes)]; 8+ bytes is a point.
        const auto* bytes = reinterpret_cast<const uint8_t*>(prefix->data());
        const int64_t lo_key =
            draken::ops::ordinalize_scalar_bytes8(bytes, static_cast<uint32_t>(prefix->size()));
        const size_t pad = 8 - (prefix->size() < 8 ? prefix->size() : 8);
        const bool has_hi = pad > 0;
        int64_t hi_key = 0;
        if (has_hi) {
            std::string padded = prefix->substr(0, 8);
            padded.append(pad, '\xff');
            hi_key = draken::ops::ordinalize_scalar_bytes8(reinterpret_cast<const uint8_t*>(padded.data()),
                                                           static_cast<uint32_t>(padded.size()));
        }

        // a point match past the 8 bytes the key sees is only a coincidence of
        // the visible prefix: discounted by how the needle compares to an
        // average value's length
        double point_discount = 1.0;
        if (!has_hi && col->has_char_class && col->avg_length != 0 && col->avg_length > 0) {
            point_discount = py::min(1.0, col->avg_length / static_cast<double>(prefix->size()));
        }

        const maki_nage::Distogram* dgram = col->histogram.get();
        if (dgram != nullptr) {
            const double total = static_cast<double>(dgram->count());
            if (total > 0) {
                const double lo_f = static_cast<double>(lo_key);
                if (!has_hi) {
                    const int64_t bins = dgram->bin_count();
                    const double span = dgram->max() - dgram->min();
                    if (bins > 1 && span > 0) {
                        const double bin_width = span / static_cast<double>(bins);
                        const double below = dgram->count_up_to(lo_f - bin_width / 2.0);
                        const double above = dgram->count_up_to(lo_f + bin_width / 2.0);
                        return clamp01((above - below) / total * point_discount);
                    }
                    // a degenerate histogram: the tiers below
                } else {
                    const double below = dgram->count_up_to(lo_f);
                    const double above = dgram->count_up_to(static_cast<double>(hi_key));
                    return clamp01((above - below) / total);
                }
            }
        }

        if (col->has_ordinal_bounds) {
            const __int128 bound_lo = col->ordinal_lo;
            const __int128 bound_hi = col->ordinal_hi;
            const __int128 query_lo = lo_key;
            const __int128 query_hi = has_hi ? hi_key : lo_key;
            if (query_hi < bound_lo || query_lo > bound_hi) return 0.0;
            const __int128 span = bound_hi - bound_lo;
            if (span <= 0) return !has_hi ? clamp01(1.0 * point_discount) : 1.0;
            if (!has_hi) {
                const int64_t ndv = col->distinct_count;
                if (truthy(ndv) && ndv > 0) return clamp01((1.0 / static_cast<double>(ndv)) * point_discount);
                return clamp01(kLikePrefixSelectivity * point_discount);
            }
            const __int128 overlap = (query_hi < bound_hi ? query_hi : bound_hi) - (query_lo > bound_lo ? query_lo : bound_lo);
            return clamp01(py::true_divide(overlap, span));
        }
        return !has_hi ? clamp01(kLikePrefixSelectivity * point_discount) : kLikePrefixSelectivity;
    }

    // The char-class tiers' guard: byte-class statistics and a usable average
    // length (`not avg_length or avg_length <= 0` refuses).
    static bool char_class_usable(const ColumnStats* col) {
        return col != nullptr && col->has_char_class && !(col->avg_length == 0 || col->avg_length <= 0);
    }

    // Case-insensitive prefix: case variants are disjoint in ordinal-key space,
    // so the char-class product anchored at position 0.
    double ci_starts_with(const ExprRow& r) const {
        ExprId column_node = kNoExpr;
        Slot identity_ = kNoSlot;
        const std::string* prefix = nullptr;
        if (!two_operands(r, column_node, identity_, prefix)) return kLikePrefixSelectivity;
        const ColumnStats* col = column(identity_);
        std::vector<uint32_t> needle;
        const bool has_needle = py::utf8_code_points(*prefix, needle);
        if (!has_needle || !char_class_usable(col)) return kLikePrefixSelectivity;
        if (needle.empty()) return 1.0;
        DrakenType physical_type = DRAKEN_NULL;
        const bool has_physical = physical(column_node, physical_type);
        if (exceeds_max_length(prefix->size(), col, has_physical, physical_type)) return 0.0;
        return anchored_char_class(needle, *col, ci_char_probability);
    }

    // A suffix has no ordinal-key range: the char-class product at its one anchor.
    double ends_with(const ExprRow& r, bool case_insensitive) const {
        ExprId column_node = kNoExpr;
        Slot identity_ = kNoSlot;
        const std::string* suffix = nullptr;
        if (!two_operands(r, column_node, identity_, suffix)) return kLikePrefixSelectivity;
        const ColumnStats* col = column(identity_);
        std::vector<uint32_t> needle;
        const bool has_needle = py::utf8_code_points(*suffix, needle);
        if (!has_needle || !char_class_usable(col)) return kLikePrefixSelectivity;
        if (needle.empty()) return 1.0;
        DrakenType physical_type = DRAKEN_NULL;
        const bool has_physical = physical(column_node, physical_type);
        if (exceeds_max_length(suffix->size(), col, has_physical, physical_type)) return 0.0;
        return case_insensitive ? anchored_char_class(needle, *col, ci_char_probability)
                                : anchored_char_class(needle, *col, plain_char_probability);
    }

    // ---- which estimator tier fires (telemetry) -----------------------------

    const char* tag(ExprId id) const {
        const ExprRow& r = row(id);
        if (r.kind == k().function) {
            ExprId column_node = kNoExpr;
            Slot identity_ = kNoSlot;
            const std::string* literal = nullptr;
            if (r.value == "_STARTS_WITH") {
                if (!two_operands(r, column_node, identity_, literal)) return "flat_fallback";
                const ColumnStats* col = column(identity_);
                DrakenType physical_type = DRAKEN_NULL;
                if (col == nullptr || !physical(column_node, physical_type)) return "flat_fallback";
                if (col->histogram) return "ordinal_range";
                if (col->has_ordinal_bounds) return "ordinal_bounds";
                return "flat_fallback";
            }
            if (r.value == "_CI_STARTS_WITH" || r.value == "_ENDS_WITH" || r.value == "_CI_ENDS_WITH") {
                if (!two_operands(r, column_node, identity_, literal)) return "flat_fallback";
                const ColumnStats* col = column(identity_);
                if (col != nullptr && col->has_char_class && col->avg_length != 0 && col->avg_length > 0) {
                    return r.value == "_CI_STARTS_WITH" ? "char_class_prefix" : "char_class_suffix";
                }
                return "flat_fallback";
            }
            return nullptr;
        }
        if (r.kind != k().comparison) return nullptr;
        const std::string& op = r.value;
        if (op != "InStr" && op != "IInStr" && op != "NotInStr" && op != "NotIInStr") return nullptr;
        Slot identity_ = identity(r.left);
        ExprId literal = r.right;
        if (identity_ == kNoSlot) {
            identity_ = identity(r.right);
            literal = r.left;
        }
        if (identity_ == kNoSlot || literal == kNoExpr || row(literal).kind != k().literal) return "flat_fallback";
        const ColumnStats* col = column(identity_);
        const LiteralValue& value = row(literal).literal;
        std::vector<uint32_t> needle;
        const bool has_needle = value.tag == LITERAL_BYTES && py::utf8_code_points(value.bytes, needle);
        if (col != nullptr && has_needle && col->has_char_class && col->avg_length != 0 && col->avg_length > 0 &&
            (r.flags & FLAG_HAS_LIKE_DECAY) != 0) {
            return "char_class_decay";
        }
        return "flat_fallback";
    }

    // ---- evaluation cost ----------------------------------------------------

    // Every FUNCTION in the tree, pre-order.
    void functions(ExprId id, std::vector<ExprId>& out) const {
        const ExprRow& r = row(id);
        if (r.kind == k().function) out.push_back(id);
        std::vector<ExprId> kids;
        expression_children(r, k(), kids);
        for (ExprId child : kids) {
            if (child == kNoExpr) throw SelectivityRefused("an expression list holds no expression");
            functions(child, out);
        }
    }

    // The name base_cost matched against the pattern operators.
    bool operator_name(const ExprRow& r, std::string& out) const {
        const int32_t kind = r.kind;
        if (kind == k().comparison || kind == k().binary || kind == k().unary || kind == k().extraction ||
            kind == k().and_ || kind == k().or_ || kind == k().xor_ || kind == k().cast || kind == k().function ||
            kind == k().aggregator) {
            out = r.value;
            return true;
        }
        if (kind == k().identifier) {   // a column reference's value is its current name
            out = ((r.flags & FLAG_HAS_ALIAS) != 0 && !r.alias.empty()) ? r.alias : r.source_column;
            return true;
        }
        return false;
    }

    double cost(ExprId id, const std::unordered_map<std::string, double>& function_costs) const {
        std::vector<ExprId> found;
        functions(id, found);
        if (!found.empty()) {
            double total = 0.0;
            for (ExprId f : found) {
                auto it = function_costs.find(row(f).value);
                const double c = it == function_costs.end() ? 0.0 : it->second;
                total += c > 0.0 ? c : kUnknownFunctionCost;
            }
            return total;
        }
        return base_cost(id);
    }

    // base_cost: the pattern operators' cost, else the comparison's column
    // category's.
    double base_cost(ExprId id) const {
        const ExprRow& r = row(id);
        std::string op;
        double op_cost = 0.0;
        if (operator_name(r, op) && operation_cost(op, op_cost)) return op_cost;
        const int32_t kind = r.kind;
        const bool has_left = kind == k().comparison || kind == k().binary || kind == k().extraction ||
                              kind == k().and_ || kind == k().or_ || kind == k().xor_ || kind == k().between ||
                              kind == k().cast;
        if (!has_left || r.left == kNoExpr) return kUnknownComparisonCost;
        DrakenType physical_type = DRAKEN_NULL;
        if (!physical(r.left, physical_type)) return kUnknownComparisonCost;
        return comparison_cost(physical_type);
    }
};

}  // namespace selectivity_detail

// The estimated fraction of `stats`' rows matching `predicate`, in [0, 1].
inline double estimate_selectivity(const SelectivityInputs& in, ExprId predicate, const RelationStats& stats) {
    const selectivity_detail::Estimator e{in, &stats};
    return selectivity_detail::clamp01(e.estimate(predicate));
}

// Which estimator tier a LIKE-family predicate uses (telemetry), or nullptr.
inline const char* predicate_estimator_tag(const SelectivityInputs& in, ExprId predicate, const RelationStats& stats) {
    const selectivity_detail::Estimator e{in, &stats};
    return e.tag(predicate);
}

// The relative per-row cost of a simple (function-free) comparison: its
// operator's, else its column category's.
inline double predicate_base_cost(const SelectivityInputs& in, ExprId predicate) {
    const selectivity_detail::Estimator e{in, nullptr};
    return e.base_cost(predicate);
}

// The relative per-row cost of evaluating `predicate`: the measured catalog
// cost of every function in it, else its comparison's.
inline double predicate_cost(const SelectivityInputs& in, ExprId predicate,
                             const std::unordered_map<std::string, double>& function_costs) {
    const selectivity_detail::Estimator e{in, nullptr};
    return e.cost(predicate, function_costs);
}

}  // namespace opteryx::planner
