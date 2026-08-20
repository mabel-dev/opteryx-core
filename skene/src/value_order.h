#pragma once
// Internal: value ordering (FORMAT.md §7.6).
//
// Sorts a column's `data` array ascending and deduplicates it, rewriting
// `selection` so the logical rows are unchanged. This reorders VALUES, never
// ROWS — `data[selection[i]]` yields exactly what it did before.
//
// What it buys: a predicate resolves to a contiguous code interval by binary
// search, `data[0]`/`data[data_length-1]` are exactly the min and max, and
// `data_length` becomes the EXACT distinct count rather than an estimate.
//
// What it costs: a sort per column, and — on an all-distinct column — a real
// permutation where an identity selection used to be free.

#include <cstdint>
#include <vector>

#include "skene/status.h"

#include "core/buffers.h"
#include "core/vector_owner.h"
#include "logical_type.h"

namespace skene {

// The reordered physical layout of one column. When `applied` is false the
// column is not eligible and MUST be written as ValueOrder::kAsWritten — the
// caller must not silently claim ordering it did not perform.
struct OrderedColumn {
    bool applied = false;

    // Non-string: the sorted, deduplicated value array.
    // String family: the sorted, deduplicated SLOT array (16 bytes per slot).
    OwnedBuffer<uint8_t> data;

    // String family only: payload bytes, with slot offsets rebased into it.
    OwnedBuffer<uint8_t> arena;
    uint64_t             arena_used = 0;
    uint64_t             slot_count = 0;

    std::vector<uint32_t> codes;         // one per logical row
    uint32_t              data_length = 0;
    uint8_t               flags = 0;     // recomputed, NOT inherited

    // v2: distinct-count ESTIMATE from the KMV sketch, set ONLY on the
    // string-family decline path — the one place the sketch runs. 0 means "no
    // estimate was measured", never "zero distinct". When `applied` is true the
    // exact answer is `data_length` and this stays 0; the writer must prefer
    // the exact count and never write both spellings of the same fact.
    double ndv_estimate = 0.0;
};

// Returns OK with out->applied == false when the column is not eligible; that
// is a normal outcome, not an error. Ineligible:
//
//   - types with no defined order: VARIANT (no collation), ARRAY (no
//     whole-array comparison), VECTOR_FP16, NULL
//   - BOOL: bit-packed at 1 bit per row, so adding 4-byte codes inflates it ~32x
//   - length-only string columns: the payload bytes do not exist, so the values
//     cannot be compared at all
//   - columns with no non-null rows: there is nothing to order, and inventing a
//     value would break `data_length == exact distinct count`
//
// Ordering is by draken's engine order. Deduplication is by BIT PATTERN, never
// by engine equality — under draken's float order -0.0 == 0.0, so an
// equality-based dedup would collapse them and a column containing -0.0 would
// read back as 0.0.
Status order_column(const DrakenVector& vector, const LogicalType* logical,
                    const char* column_name, OrderedColumn* out);

// True when `type` can be value-ordered at all. Exposed so the writer can make
// the same decision without building anything.
bool type_is_orderable(DrakenType type);

}  // namespace skene
