// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// See the License at http://www.apache.org/licenses/LICENSE-2.0
// Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

// src/cpp/planner/column_type.hpp — the native ColumnType: Opteryx's one type
// vocabulary (CLAUDE.md §14) as process-wide interned type ids.
//
// A column type is a VALUE: a DrakenType physical tag, an optional draken
// LogicalType descriptor, and for ARRAY an element type. Each distinct value is
// interned once per process and named by a ColumnTypeId, so type equality is id
// equality and a plan column row carries its type as one uint32 (architect
// rulings 2026-09-27, native plan graph P1-c2). DrakenType and LogicalType are
// draken's and are included, never copied.
//
// Ownership: the table lives in the opteryx.compiled.planner.column_type
// extension. Other extensions reach it through that module's cimported API
// (column_type.pxd), never by including this header's table accessor themselves:
// a header-only function-local static is one object per linked module, and on
// macOS's two-level namespace two extensions would each get their own table.
//
// Threading: the table grows only while the GIL is held (every intern comes from
// planning). Readers hold the GIL too; a reader that releases it may only read
// ids interned before it did.

#pragma once

#include <cstdint>
#include <deque>
#include <unordered_map>

#include "logical_type.h"  // LogicalType, LogicalKind (and core/buffers.h: DrakenType)

namespace opteryx::planner {

using ColumnTypeId = uint32_t;
inline constexpr ColumnTypeId kNoColumnType = UINT32_MAX;

struct ColumnTypeEntry {
    DrakenType physical;
    bool has_logical;
    LogicalType logical;   // meaningful only when has_logical
    ColumnTypeId element;  // kNoColumnType unless physical is ARRAY
};

// Why a candidate type is not a type. The Python surface turns each into the
// ValueError the frozen-dataclass ColumnType raised, word for word.
enum class ColumnTypeCheck : uint8_t {
    OK = 0,
    PARAMETERIZED_NEEDS_LOGICAL = 1,
    PARAMETERIZED_WITH_ELEMENT = 2,
    ARRAY_NEEDS_ELEMENT = 3,
    ARRAY_WITH_LOGICAL = 4,
    REFINEMENT_NOT_PERMITTED = 5,
    REFINABLE_WITH_ELEMENT = 6,
    UNPARAMETERIZED_WITH_LOGICAL = 7,
    UNPARAMETERIZED_WITH_ELEMENT = 8,
};

// Physical types that REQUIRE a LogicalType descriptor: their tag is
// uninterpretable without it (see logical_type.h).
inline bool column_type_is_parameterized(DrakenType physical) noexcept {
    switch (physical) {
        case DRAKEN_DECIMAL:
        case DRAKEN_DECIMAL128:  // int128-backed; same (precision, scale) descriptor
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME32:
        case DRAKEN_TIME64:
        case DRAKEN_VECTOR_FP16:
            return true;
        default:
            return false;
    }
}

// Physical types that PERMIT a descriptor without requiring one - a refinement of
// an already-complete type. Kept deliberately tight: each entry needs the
// architect's agreement (UINT32 -> IPV4 is the only one). Disjoint from the
// parameterized set by construction.
inline bool column_type_is_refinable(DrakenType physical) noexcept {
    return physical == DRAKEN_UINT32;
}

inline bool column_type_refinement_permitted(DrakenType physical, LogicalKind kind) noexcept {
    return physical == DRAKEN_UINT32 && kind == LogicalKind::IPV4;
}

inline ColumnTypeCheck column_type_check(const ColumnTypeEntry& e) noexcept {
    const bool has_element = e.element != kNoColumnType;
    if (column_type_is_parameterized(e.physical)) {
        if (!e.has_logical) return ColumnTypeCheck::PARAMETERIZED_NEEDS_LOGICAL;
        if (has_element) return ColumnTypeCheck::PARAMETERIZED_WITH_ELEMENT;
        return ColumnTypeCheck::OK;
    }
    if (e.physical == DRAKEN_ARRAY) {
        if (!has_element) return ColumnTypeCheck::ARRAY_NEEDS_ELEMENT;
        if (e.has_logical) return ColumnTypeCheck::ARRAY_WITH_LOGICAL;
        return ColumnTypeCheck::OK;
    }
    if (column_type_is_refinable(e.physical) && e.has_logical) {
        if (!column_type_refinement_permitted(e.physical, e.logical.kind))
            return ColumnTypeCheck::REFINEMENT_NOT_PERMITTED;
        if (has_element) return ColumnTypeCheck::REFINABLE_WITH_ELEMENT;
        return ColumnTypeCheck::OK;
    }
    if (e.has_logical) return ColumnTypeCheck::UNPARAMETERIZED_WITH_LOGICAL;
    if (has_element) return ColumnTypeCheck::UNPARAMETERIZED_WITH_ELEMENT;
    return ColumnTypeCheck::OK;
}

// column_type_check as its numeric code, for the Cython surface (which formats the
// messages).
inline uint8_t column_type_check_code(const ColumnTypeEntry& e) noexcept {
    return static_cast<uint8_t>(column_type_check(e));
}

// The interning table. Entries are never moved or freed (std::deque push_back
// keeps element addresses), so an id names one value for the process lifetime.
class ColumnTypeTable {
public:
    // The id of `e`, interning it on first sight. `e` must have passed
    // column_type_check; `*inserted` reports whether it was new.
    ColumnTypeId intern(const ColumnTypeEntry& e, bool* inserted) {
        const Key key = key_of(e);
        auto found = index_.find(key);
        if (found != index_.end()) {
            *inserted = false;
            return found->second;
        }
        const ColumnTypeId id = static_cast<ColumnTypeId>(entries_.size());
        entries_.push_back(e);
        index_.emplace(key, id);
        *inserted = true;
        return id;
    }

    const ColumnTypeEntry& entry(ColumnTypeId id) const noexcept { return entries_[id]; }
    size_t size() const noexcept { return entries_.size(); }

private:
    // The whole value, packed: physical, descriptor presence and fields, element.
    struct Key {
        uint64_t head;     // physical | has_logical | kind | unit | precision | scale | offset
        uint64_t tail;     // dimension | element
        bool operator==(const Key& o) const noexcept { return head == o.head && tail == o.tail; }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            return static_cast<size_t>(k.head * 0x9E3779B97F4A7C15ull ^ (k.tail + 0x632BE59BD9B4E019ull));
        }
    };

    static Key key_of(const ColumnTypeEntry& e) noexcept {
        const LogicalType lt = e.has_logical ? e.logical : LogicalType{};
        const uint64_t head =
            static_cast<uint64_t>(static_cast<uint8_t>(e.physical)) |
            (static_cast<uint64_t>(e.has_logical ? 1u : 0u) << 8) |
            (static_cast<uint64_t>(static_cast<uint8_t>(lt.kind)) << 16) |
            (static_cast<uint64_t>(static_cast<uint8_t>(lt.unit)) << 24) |
            (static_cast<uint64_t>(lt.precision) << 32) |
            (static_cast<uint64_t>(lt.scale) << 40) |
            (static_cast<uint64_t>(static_cast<uint16_t>(lt.offset_minutes)) << 48);
        const uint64_t tail =
            static_cast<uint64_t>(lt.dimension) | (static_cast<uint64_t>(e.element) << 32);
        return Key{head, tail};
    }

    std::deque<ColumnTypeEntry> entries_;
    std::unordered_map<Key, ColumnTypeId, KeyHash> index_;
};

}  // namespace opteryx::planner
