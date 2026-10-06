#pragma once
// src/cpp/engine/null_constant_column.hpp — an all-NULL column of any type, `n` rows
// long, at no per-row cost. Shared by GROUP BY ROLLUP's masked keys
// (native_grouping_expand.hpp) and the parquet scan's schema-evolution fill
// (native_parquet_scan_source.hpp: a projected column a file does not hold).

#include <cstdint>
#include <memory>

#include "core/buffers.h"        // DrakenVector
#include "core/vector_alloc.h"   // draken_zero_sel, draken_zero_validity
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "morsels/cxx_morsel.h"  // CxxColumn

namespace opteryx::engine {

// A constant-shaped, all-NULL view of `src`, `n` rows long.
//
// The payload is BORROWED, never copied: `data_source` holds the source column's owner
// alive, which is the sanctioned borrowing path (see vector_owner.h — `data_buf` and
// `arena_buf` stay null exactly because the bytes live in the source). `selection` and
// `validity` point at draken's process-wide shared globals, which are never freed and
// so are not owned here either. The net cost is one VectorOwner allocation per column
// per call — no per-row work at all.
//
// `src` must hold at least one data slot: the view reads `data[0]` for every row.
//
// Type and logical type ride along from the source, so a TIMESTAMP64 keeps its
// MANDATORY descriptor (a timestamp vector with a null logical_type is a hard error
// in draken) and a DECIMAL keeps its precision/scale.
inline CxxColumn null_constant_column(const CxxColumn& src, uint32_t n) {
    DrakenVector v = src.view;
    v.selection   = draken_zero_sel(n);
    v.data_length = 1;
    v.length      = n;
    v.validity    = const_cast<uint8_t*>(draken_zero_validity(n));
    // No layout hints survive the reshape: this is neither the source's shape nor a
    // known-identity selection. 0 == "don't know", which is always safe.
    v.flags       = 0;

    auto owner = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(nullptr),
                                               OwnedBuffer<uint8_t>(nullptr));
    owner->logical_type = src.own ? src.own->logical_type : nullptr;
    // Null when the source column is itself unowned (a constant/zero-column morsel);
    // borrowing from nothing is the source's own lifetime story, not a new one.
    owner->data_source  = src.own;

    CxxColumn out;
    out.view = v;
    out.own  = std::move(owner);
    return out;
}

}  // namespace opteryx::engine
