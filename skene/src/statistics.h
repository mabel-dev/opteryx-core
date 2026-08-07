#pragma once
// Internal: per-column statistics (FORMAT.md §8).
//
// The file carries only what the catalog does not: MIN/MAX, null_count and SUM.
// No sketches, no histograms — those are dataset-level and live in the catalog.
//
// Every statistic is optional and absent means "NOT TRACKED", never "zero".
// Spill files carry none at all.

#include <cstdint>

#include "skene/format.h"
#include "skene/status.h"

#include "core/buffers.h"
#include "logical_type.h"

namespace skene {

// Computes whatever is meaningful for this column, setting only the StatFlag
// bits it actually filled. A type with no order gets no min/max; a type where a
// stored sum could disagree with a recomputed one gets no sum.
//
// Never fails on an unsupported type — it simply reports fewer statistics. The
// absent state is first-class, so "cannot compute" and "computed zero" are
// different answers and are represented differently.
// When `ordered_data` is non-null the column has been value-ordered, so that
// array is ascending, deduplicated, and contains only values referenced by a
// non-null row. min/max are then data[0] and data[data_length-1] — exact, and
// two ordinalisations instead of one per row.
//
// The ordered array is passed rather than re-derived because the caller has just
// built it; asking statistics to sort again would be a second answer to a
// question already settled.
Status compute_statistics(const DrakenVector& vector, const LogicalType* logical,
                          const char* column_name, ColumnStatistics* out,
                          const void* ordered_data = nullptr,
                          uint32_t ordered_length = 0);

// One value's ordinal key, indexing `vector.data` by `code` directly (NOT via
// the selection — the caller supplies the code it wants).
//
// Returns false when the type has no ordinal, or when a string column's payloads
// were elided so there is nothing to compare. Shared with the zone map so the
// file has exactly ONE definition of a value's ordinal.
bool column_ordinal_at(const DrakenVector& vector, const LogicalType* logical,
                       uint32_t code, int64_t* out);

// True when min/max ordinals are defined for `type`.
//
// Excluded: DECIMAL128 (draken deliberately has no ordinalize kernel for it —
// a lossy int64 proxy for a 128-bit type would be worse than absence), VARIANT
// (no collation), ARRAY (no whole-array comparison), VECTOR_FP16 and NULL.
bool type_has_min_max(DrakenType type);

// True when an exact SUM is defined for `type`: the integer widths and DECIMAL.
//
// FLOAT32/FLOAT64 are excluded deliberately. Floating-point addition is not
// associative, so a sum computed at write time differs in the low bits from one
// computed at read time, and a query would return different answers depending on
// whether the optimizer used the footer. That is answer instability, not an
// optimization.
bool type_has_sum(DrakenType type);

}  // namespace skene
