// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/expr_arena.hpp — one query's expressions as native rows.
//
// Every expression of a query is a row here and its ExprId is its row number
// plus one (native plan graph P3, architect rulings 2026-09-27):
//   - the arena OWNS the expression: kind, operator or function name, children
//     (as ExprIds), naming, the bound column's slot, the relations it reads, its
//     flags and, for a literal, its type and its native value;
//   - one row per ExprId - a copy is a new row sharing the ORIGIN of the
//     expression as it was written;
//   - once the arena is sealed (end of binding) a placed row never changes.
//
// The Python planner reads each row through its expression object
// (opteryx/compiled/structures/expressions.pyx), which writes the row whenever
// it is built or written. What native code never reads - a resolved function
// reference, a subquery's plan, a window spec - stays on the Python object.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace opteryx::planner {

using ExprId = int64_t;
inline constexpr ExprId kNoExpr = 0;
inline constexpr uint32_t kNoColumnSlot = UINT32_MAX;
inline constexpr uint32_t kNoTypeId = UINT32_MAX;

// A literal's native value. The tag is chosen from the literal's declared type
// (a tuple is an ARRAY's elements or an INTERVAL's (months, microseconds)).
enum LiteralTag : uint8_t {
    LITERAL_NONE = 0,      // no value recorded yet (a draft mid-construction)
    LITERAL_NULL = 1,
    LITERAL_BOOL = 2,
    LITERAL_INT64 = 3,
    LITERAL_UINT64 = 4,
    LITERAL_DOUBLE = 5,
    LITERAL_BYTES = 6,
    LITERAL_DECIMAL = 7,   // unscaled value (hi:lo, two's complement) and exponent
    LITERAL_INTERVAL = 8,  // months, microseconds
    LITERAL_ITEMS = 9,     // an ARRAY / VECTOR: the elements
};

struct LiteralValue {
    LiteralTag tag = LITERAL_NONE;
    int64_t i = 0;          // BOOL, INT64, UINT64 (as bits), DECIMAL lo, INTERVAL months
    int64_t j = 0;          // DECIMAL hi, INTERVAL microseconds
    int32_t k = 0;          // DECIMAL exponent
    double d = 0.0;
    std::string bytes;
    std::vector<LiteralValue> items;
};

// opteryx.expression.NodeType values, handed over from Python once (never
// restated here, so the enum has one definition): see node_kinds() in
// opteryx/compiled/structures/expressions.pyx.
struct NodeKinds {
    int32_t and_ = 0, or_ = 0, xor_ = 0, not_ = 0, dnf = 0, cnf = 0;
    int32_t case_ = 0, comparison = 0, binary = 0, unary = 0, function = 0;
    int32_t identifier = 0, nested = 0, aggregator = 0, literal = 0, cast = 0;
    int32_t extraction = 0, between = 0;
};

// Row flags.
enum ExprFlag : uint16_t {
    FLAG_DRAFT = 1u << 0,
    FLAG_DO_NOT_CREATE_COLUMN = 1u << 1,
    FLAG_NEGATED = 1u << 2,
    FLAG_OUTER_REFERENCE = 1u << 3,
    FLAG_WILDCARD_ORDER_POSITION = 1u << 4,
    FLAG_RLIKE_COMPILED = 1u << 5,
    FLAG_LOWER_INCLUSIVE = 1u << 6,   // BETWEEN
    FLAG_UPPER_INCLUSIVE = 1u << 7,   // BETWEEN
    FLAG_HAS_ALIAS = 1u << 8,
    FLAG_HAS_QUERY_COLUMN = 1u << 9,
    FLAG_HAS_SPAN = 1u << 10,
    FLAG_HAS_LIKE_DECAY = 1u << 11,
    FLAG_HAS_MATCH_THRESHOLD = 1u << 12,
    FLAG_HAS_LIMIT = 1u << 13,
};

struct ExprRow {
    ExprId origin = kNoExpr;
    int32_t kind = 0;                  // opteryx.expression.NodeType value
    uint16_t flags = 0;

    std::string value;                 // operator / function / aggregate / cast name
    std::string alias;
    std::string query_column;
    uint32_t column_slot = kNoColumnSlot;   // the bound SchemaColumn's slot
    std::vector<uint32_t> relations;        // interned relation names

    // children, by declared field
    ExprId left = kNoExpr;
    ExprId right = kNoExpr;
    ExprId centre = kNoExpr;
    ExprId else_result = kNoExpr;
    ExprId format = kNoExpr;
    std::vector<ExprId> parameters;
    std::vector<ExprId> conditions;
    std::vector<ExprId> results;
    std::vector<std::pair<ExprId, bool>> order;   // aggregate ORDER BY (expr, ascending)

    // a column reference
    std::string source;
    std::string source_column;
    std::string outer_relation;
    // a function / aggregate
    std::string qualified_name;
    std::string duplicate_treatment;
    std::string null_treatment;
    int64_t limit = 0;
    double like_selectivity_decay = 0.0;
    double match_threshold = 0.0;
    int32_t span[4] = {0, 0, 0, 0};

    // a literal
    uint32_t type_id = kNoTypeId;      // interned ColumnType
    LiteralValue literal;
};

class ExprTable {
public:
    // A query mints hundreds to a few thousand expressions; reserving up front
    // keeps the (large) rows from being moved while the planner builds.
    ExprTable() { rows_.reserve(1024); }

    // A new row; its ExprId (row number + 1).
    ExprId add() {
        rows_.emplace_back();
        return static_cast<ExprId>(rows_.size());
    }

    ExprRow& row(ExprId id) {
        if (id <= 0 || static_cast<size_t>(id) > rows_.size()) {
            throw std::out_of_range("no expression with this id in the arena");
        }
        return rows_[static_cast<size_t>(id - 1)];
    }

    const ExprRow& row(ExprId id) const {
        if (id <= 0 || static_cast<size_t>(id) > rows_.size()) {
            throw std::out_of_range("no expression with this id in the arena");
        }
        return rows_[static_cast<size_t>(id - 1)];
    }

    size_t size() const { return rows_.size(); }

    // The interned id of a relation name.
    uint32_t relation(const std::string& name) {
        auto found = relation_ids_.find(name);
        if (found != relation_ids_.end()) {
            return found->second;
        }
        uint32_t id = static_cast<uint32_t>(relation_names_.size());
        relation_names_.push_back(name);
        relation_ids_.emplace(name, id);
        return id;
    }

    const std::string& relation_name(uint32_t id) const { return relation_names_.at(id); }

private:
    std::vector<ExprRow> rows_;
    std::vector<std::string> relation_names_;
    std::unordered_map<std::string, uint32_t> relation_ids_;
};

// An expression's children in their declared order (Expression.children): a
// field holding no expression is skipped; a LIST member holding none is kept
// as kNoExpr for the caller to refuse (the Python walks failed on one).
inline void expression_children(const ExprRow& r, const NodeKinds& k, std::vector<ExprId>& out) {
    out.clear();
    auto one = [&](ExprId id) {
        if (id != kNoExpr) out.push_back(id);
    };
    auto many = [&](const std::vector<ExprId>& ids) { out.insert(out.end(), ids.begin(), ids.end()); };
    const int32_t kind = r.kind;
    if (kind == k.comparison || kind == k.binary || kind == k.extraction || kind == k.and_ || kind == k.or_ ||
        kind == k.xor_) {
        one(r.left);
        one(r.right);
    } else if (kind == k.unary) {
        one(r.centre);
        many(r.parameters);
    } else if (kind == k.not_ || kind == k.nested) {
        one(r.centre);
    } else if (kind == k.dnf || kind == k.cnf || kind == k.function || kind == k.aggregator) {
        many(r.parameters);
    } else if (kind == k.between) {
        one(r.left);
        one(r.right);
        one(r.centre);
    } else if (kind == k.case_) {
        many(r.conditions);
        many(r.results);
        one(r.else_result);
    } else if (kind == k.cast) {
        one(r.left);
        many(r.parameters);
        one(r.format);
    }
}

}  // namespace opteryx::planner
