// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/step_row.hpp — a logical plan step's fields as native code
// reads them (native plan graph P5; architect rulings 2026-09-28).
//
// Every PlanStep owns one StepRow, rebuilt by the step whenever a field the
// row mirrors is written (its setters, its constructor, its copies) - so the
// row is never stale and nothing lowers a step at read time. The native
// statistics refresh reads plan steps ONLY through their rows: it collects the
// row pointers once per refresh (node id -> row) and runs without Python.
//
// What a row holds is what the refresh reads, per step kind:
//   every step    kind, output columns
//   Scan          relation, alias, pushed predicates, schema (column slots and
//                 declared row counts), manifest (+ the schema it was bound
//                 with), pushed LIMIT, pushed aggregate / groups / DISTINCT
//   Filter        condition
//   Join          join type, equi-key columns of each leg
//   Aggregate     group keys
//   Limit         limit, offset (HeapSort: limit)
//   CTE reference cte_key
// Expressions are held as ExprIds in `arena` (the query's expression table);
// a join key given as a column identity (bytes) is held as that identity.
// The manifest is BORROWED: the step holds the Python manifest that owns it,
// so it lives exactly as long as the step's own field.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "planner/expr_arena.hpp"
#include "planner/native_manifest.hpp"

namespace opteryx::planner {

inline constexpr int64_t kNoStepValue = INT64_MIN;   // an unset optional integer

// A join key or group key: an expression, or a column identity given directly.
struct StepKey {
    ExprId expr = kNoExpr;
    std::string identity;   // when `expr` is kNoExpr
};

struct StepRow {
    int32_t kind = -1;                       // LogicalPlanStepType value
    const ExprTable* arena = nullptr;        // the table every ExprId below is a row of
    bool arena_conflict = false;             // a field write met another arena: rebuild
    std::vector<ExprId> columns;

    // Scan
    std::string relation;
    std::string alias;
    std::vector<ExprId> predicates;
    bool has_schema = false;
    std::vector<uint32_t> schema_slots;      // the schema's columns, as ColumnTable slots
    int64_t schema_row_count_metric = kNoStepValue;
    int64_t schema_row_count_estimate = kNoStepValue;
    const NativeManifest* manifest = nullptr;
    std::vector<uint32_t> manifest_schema_slots;   // the schema the manifest was bound with
    int64_t limit = kNoStepValue;
    bool has_pushed_aggregates = false;
    std::vector<StepKey> pushed_groups;
    bool pushed_distinct = false;

    // Filter
    ExprId condition = kNoExpr;

    // Join
    std::string join_type;
    bool has_join_type = false;
    std::vector<StepKey> left_keys;
    std::vector<StepKey> right_keys;

    // AggregateAndGroup
    std::vector<StepKey> groups;

    // Limit / HeapSort
    int64_t offset = kNoStepValue;

    // MaterializedCteRef
    std::string cte_key;
};

}  // namespace opteryx::planner
