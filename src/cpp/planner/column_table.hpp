// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/column_table.hpp — one query's bound columns as native rows.
//
// Every bound column of a query is a row here, and its SLOT is its position
// (native plan graph P1, architect rulings 2026-09-27):
//   - the row holds EVERYTHING the column is: name, aliases, origin, identity,
//     nullability, catalog field id, type (an interned ColumnTypeId) and kind;
//   - a row is fixed once minted - a column that differs (renamed in another
//     scope, retyped, standing in for another) is a NEW row, and `alias_of`
//     names the row it derives from;
//   - an alias row keeps its root's identity, the stream key the data travels
//     under, so the engine's identity comparisons keep meaning "same data".
//
// The Python planner reads each row through one canonical façade object per
// slot (opteryx/compiled/planner/column_table.pyx); native consumers read the
// rows directly by slot.

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "planner/column_type.hpp"

namespace opteryx::planner {

using Slot = uint32_t;
inline constexpr Slot kNoSlot = UINT32_MAX;

// What a column IS, beyond its values: a plain column of a relation, a literal,
// or a computed column (a function call, or any other expression).
enum ColumnKind : uint8_t {
    COLUMN_PLAIN = 0,
    COLUMN_CONSTANT = 1,
    COLUMN_FUNCTION = 2,
    COLUMN_EXPRESSION = 3,
};

struct ColumnRow {
    std::string name;
    std::string identity;               // opaque bytes: the stream key
    std::vector<std::string> aliases;   // meaningful only when has_aliases
    std::vector<std::string> origin;    // meaningful only when has_origin
    bool has_aliases = false;
    bool has_origin = false;
    bool nullable = true;
    bool has_field_id = false;
    int64_t field_id = 0;               // catalog field id, when has_field_id
    ColumnTypeId type_id = kNoColumnType;  // kNoColumnType: not yet resolved
    uint8_t kind = COLUMN_PLAIN;
    Slot alias_of = kNoSlot;            // the row this one derives from; kNoSlot for a root
};

class ColumnRows {
public:
    Slot append(ColumnRow&& row) {
        rows_.push_back(std::move(row));
        return static_cast<Slot>(rows_.size() - 1);
    }

    const ColumnRow& row(Slot slot) const noexcept { return rows_[slot]; }
    size_t size() const noexcept { return rows_.size(); }

    // The root of `slot`'s derivation chain - the row that minted its identity.
    Slot root(Slot slot) const noexcept {
        while (rows_[slot].alias_of != kNoSlot) slot = rows_[slot].alias_of;
        return slot;
    }

private:
    std::vector<ColumnRow> rows_;
};

}  // namespace opteryx::planner
