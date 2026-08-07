#include "statistics.h"

#include <cstring>

#include "core/interval_slot.h"
#include "core/string_slot.h"
#include "ops/ordinalize.h"

namespace skene {
namespace {

inline bool row_is_valid(const DrakenVector& vector, uint32_t row) {
    if (vector.validity == nullptr) return true;
    return (vector.validity[row >> 3] & (1u << (row & 7u))) != 0;
}

// One value's ordinal key.
//
// These delegate to draken's own ordinalize_scalar_* helpers — the SAME
// functions draken's vector kernels call — so the file's min/max speak exactly
// the dialect the catalog manifest and plan-time pruning already use. Writing a
// second ordinal definition here would be a second answer to the same question.
bool ordinal_of_impl(const DrakenVector& vector, const LogicalType* logical,
                     uint32_t code, int64_t* out) {
    using namespace draken::ops;
    const void* data = vector.data;
    switch (vector.type) {
        case DRAKEN_INT8:
            *out = ordinalize_scalar_widen<int8_t>(static_cast<const int8_t*>(data)[code]);
            return true;
        case DRAKEN_INT16:
            *out = ordinalize_scalar_widen<int16_t>(static_cast<const int16_t*>(data)[code]);
            return true;
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:
            *out = ordinalize_scalar_widen<int32_t>(static_cast<const int32_t*>(data)[code]);
            return true;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64:
            *out = ordinalize_scalar_widen<int64_t>(static_cast<const int64_t*>(data)[code]);
            return true;
        case DRAKEN_UINT8:
            *out = ordinalize_scalar_widen<uint8_t>(static_cast<const uint8_t*>(data)[code]);
            return true;
        case DRAKEN_UINT16:
            *out = ordinalize_scalar_widen<uint16_t>(static_cast<const uint16_t*>(data)[code]);
            return true;
        case DRAKEN_UINT32:
            *out = ordinalize_scalar_widen<uint32_t>(static_cast<const uint32_t*>(data)[code]);
            return true;
        case DRAKEN_UINT64:
            // Not widen: a value above INT64_MAX would come back negative and
            // invert the ordering against every other unsigned value.
            *out = ordinalize_scalar_u64(static_cast<const uint64_t*>(data)[code]);
            return true;
        case DRAKEN_FLOAT32:
            *out = ordinalize_scalar_f32(static_cast<const float*>(data)[code]);
            return true;
        case DRAKEN_FLOAT64:
            *out = ordinalize_scalar_f64(static_cast<const double*>(data)[code]);
            return true;
        case DRAKEN_BOOL: {
            const uint8_t* bits = static_cast<const uint8_t*>(data);
            *out = ((bits[code >> 3] >> (code & 7u)) & 1u) ? 1 : 0;
            return true;
        }
        case DRAKEN_INTERVAL: {
            const DrakenIntervalSlot& slot =
                static_cast<const DrakenIntervalSlot*>(data)[code];
            *out = ordinalize_scalar_interval(slot.months, slot.us);
            return true;
        }
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY: {
            const DrakenStringArena* arena =
                static_cast<const DrakenStringArena*>(data);
            if (arena == nullptr || arena->payloads_elided) return false;
            *out = ordinalize_scalar_string_slot(&arena->slots[code], arena->arena);
            return true;
        }
        default:
            (void)logical;
            return false;
    }
}

// One value, widened to the 128-bit accumulator. Only exact types reach here.
bool value_as_int128(const DrakenVector& vector, uint32_t code, __int128* out) {
    const void* data = vector.data;
    switch (vector.type) {
        case DRAKEN_INT8:   *out = static_cast<const int8_t*>(data)[code];   return true;
        case DRAKEN_INT16:  *out = static_cast<const int16_t*>(data)[code];  return true;
        case DRAKEN_INT32:  *out = static_cast<const int32_t*>(data)[code];  return true;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL: *out = static_cast<const int64_t*>(data)[code]; return true;
        case DRAKEN_UINT8:  *out = static_cast<const uint8_t*>(data)[code];  return true;
        case DRAKEN_UINT16: *out = static_cast<const uint16_t*>(data)[code]; return true;
        case DRAKEN_UINT32: *out = static_cast<const uint32_t*>(data)[code]; return true;
        case DRAKEN_UINT64:
            *out = static_cast<__int128>(static_cast<const uint64_t*>(data)[code]);
            return true;
        default: return false;
    }
}

}  // namespace

bool column_ordinal_at(const DrakenVector& vector, const LogicalType* logical,
                       uint32_t code, int64_t* out) {
    return ordinal_of_impl(vector, logical, code, out);
}

bool type_has_min_max(DrakenType type) {
    switch (type) {
        case DRAKEN_INT8:  case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_DATE32: case DRAKEN_TIME32: case DRAKEN_TIME64:
        case DRAKEN_TIMESTAMP64: case DRAKEN_DECIMAL: case DRAKEN_INTERVAL:
        case DRAKEN_BOOL:
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY:
            return true;
        default:
            return false;
    }
}

bool type_has_sum(DrakenType type) {
    switch (type) {
        case DRAKEN_INT8:  case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
        case DRAKEN_DECIMAL:
            return true;
        default:
            return false;
    }
}

Status compute_statistics(const DrakenVector& vector, const LogicalType* logical,
                          const char* column_name, ColumnStatistics* out,
                          const void* ordered_data, uint32_t ordered_length) {
    (void)column_name;
    std::memset(out, 0, sizeof(*out));

    const uint32_t length = vector.length;

    // ── null_count ──
    uint64_t nulls = 0;
    if (vector.type == DRAKEN_NULL) {
        nulls = length;   // self-describing: every row is null
    } else if (vector.validity != nullptr) {
        for (uint32_t row = 0; row < length; ++row)
            if (!row_is_valid(vector, row)) ++nulls;
    }
    out->null_count = nulls;
    out->flags |= kStatNullCount;

    // ── Row-order sortedness, mirrored from the layout hints ──
    if (vector.flags & DRAKEN_ROW_SORTED) {
        out->flags |= kStatRowSorted;
        if (vector.flags & DRAKEN_ROW_SORTED_DESC) out->flags |= kStatRowSortedDescending;
    }

    // ── min/max and sum, over NON-NULL values only ──
    //
    // ORDINAL_NULL is INT64_MIN, so including a null row would make every
    // nullable column's min INT64_MIN and prune nothing. Nulls are excluded
    // rather than ordinalized.
    const bool want_min_max = type_has_min_max(vector.type);
    const bool want_sum     = type_has_sum(vector.type);
    if (!want_min_max && !want_sum) return Status::ok();

    int64_t  minimum = 0;
    int64_t  maximum = 0;
    __int128 total   = 0;
    bool     any     = false;
    bool     ordinals_available = want_min_max;

    // Value-ordered fast path: the ends of an ascending, deduplicated array ARE
    // the extremes, so this is exact rather than an approximation of the scan
    // below. Only the SUM still needs every row.
    bool min_max_done = false;
    if (want_min_max && ordered_data != nullptr && ordered_length > 0) {
        DrakenVector view = vector;
        view.data = const_cast<void*>(ordered_data);
        int64_t low = 0;
        int64_t high = 0;
        if (ordinal_of_impl(view, logical, 0, &low)
                && ordinal_of_impl(view, logical, ordered_length - 1u, &high)) {
            minimum = low;
            maximum = high;
            min_max_done = true;
            any = true;
        } else {
            ordinals_available = false;
        }
    }
    if (min_max_done) ordinals_available = false;   // nothing left for the scan

    if (!want_sum && min_max_done) {
        out->min_ordinal = minimum;
        out->max_ordinal = maximum;
        out->flags |= kStatMin | kStatMax;
        return Status::ok();
    }

    for (uint32_t row = 0; row < length; ++row) {
        if (!row_is_valid(vector, row)) continue;
        const uint32_t code = vector.selection[row];

        if (ordinals_available) {
            int64_t ordinal = 0;
            if (!ordinal_of_impl(vector, logical, code, &ordinal)) {
                // A length-only string column knows its lengths but not its
                // bytes, so no ordinal exists. Report no min/max rather than a
                // bound derived from bytes we do not have.
                ordinals_available = false;
            } else if (!any) {
                minimum = maximum = ordinal;
            } else {
                if (ordinal < minimum) minimum = ordinal;
                if (ordinal > maximum) maximum = ordinal;
            }
        }

        if (want_sum) {
            __int128 value = 0;
            if (value_as_int128(vector, code, &value)) total += value;
        }
        any = true;
    }

    if (any && want_min_max && (min_max_done || ordinals_available)) {
        out->min_ordinal = minimum;
        out->max_ordinal = maximum;
        out->flags |= kStatMin | kStatMax;
    }

    if (want_sum) {
        // A signed 128-bit accumulator cannot overflow at any row count this
        // format addresses: |INT64_MIN| * 2^32 == 2^95, far inside 2^127. So
        // there is no overflow flag and none is needed.
        out->sum_low  = static_cast<int64_t>(static_cast<uint64_t>(total));
        out->sum_high = static_cast<int64_t>(total >> 64);
        out->flags |= kStatSum;
    }

    return Status::ok();
}

}  // namespace skene
