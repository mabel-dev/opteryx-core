#pragma once
// draken/ops/exact_sum.h — the EXACT sum of a vector's non-null integer values,
// as every statistics producer records it (skene's per-row-group kStatSum,
// rugo's `rugo.sum` chunk statistic, the opteryx writer and ANALYZE; see
// docs/MANIFEST_SUM_STATISTIC_DESIGN.md).
//
// One definition, so a sum written by any producer means the same number:
//   - exact types only: the integer family at every width and signedness, and
//     DECIMAL (int64-backed; the sum is of the UNSCALED values). Floats never
//     get a sum — float addition is not associative, so a stored sum and a
//     recomputed one would disagree in the low bits;
//   - signed values sign-extend, unsigned values zero-extend (a UINT64 above
//     INT64_MAX is its value, never value - 2^64);
//   - NULL rows are skipped; the sum of zero valid values is 0 (the caller
//     tells "no values" from "sums to zero" by the valid count).
//
// The accumulator is __int128. One vector cannot overflow it: at most 2^32
// rows of magnitude at most 2^64 is 2^96, far inside 2^127. A caller folding
// many vectors (or files) checks its own additions.
//
// Access is the uniform data[selection[i]] path (buffers.h), correct for every
// encoding shape.

#include <stdint.h>

#include "core/buffers.h"

namespace draken { namespace ops {

// Does `type` have an exact sum?
inline bool exact_sum_type(DrakenType type) noexcept {
    switch (type) {
        case DRAKEN_INT8:  case DRAKEN_INT16:  case DRAKEN_INT32:  case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
        case DRAKEN_DECIMAL:
            return true;
        default:
            return false;
    }
}

// The value at physical position `code`, widened to the accumulator. False for
// a type with no exact sum.
inline bool exact_sum_value(const DrakenVector& vector, uint32_t code, __int128* out) noexcept {
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
        default:
            return false;
    }
}

namespace exact_sum_detail {

template <typename T>
inline void accumulate(const DrakenVector& v, __int128* sum, uint64_t* valid) noexcept {
    const T* data = static_cast<const T*>(v.data);
    const uint32_t* sel = v.selection;
    const uint32_t n = v.length;
    __int128 total = 0;
    if (v.validity == nullptr) {
        for (uint32_t i = 0; i < n; ++i) total += data[sel[i]];
        *valid = n;
    } else {
        uint64_t count = 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (((v.validity[i >> 3] >> (i & 7u)) & 1u) == 0) continue;
            total += data[sel[i]];
            ++count;
        }
        *valid = count;
    }
    *sum = total;
}

}  // namespace exact_sum_detail

// The exact sum and valid-row count of `vector`. False (outputs untouched) for
// a type with no exact sum; a DRAKEN_NULL vector is 0 valid rows summing to 0.
inline bool exact_sum(const DrakenVector& vector, __int128* sum, uint64_t* valid) noexcept {
    using exact_sum_detail::accumulate;
    switch (vector.type) {
        case DRAKEN_INT8:    accumulate<int8_t>(vector, sum, valid);   return true;
        case DRAKEN_INT16:   accumulate<int16_t>(vector, sum, valid);  return true;
        case DRAKEN_INT32:   accumulate<int32_t>(vector, sum, valid);  return true;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL: accumulate<int64_t>(vector, sum, valid);  return true;
        case DRAKEN_UINT8:   accumulate<uint8_t>(vector, sum, valid);  return true;
        case DRAKEN_UINT16:  accumulate<uint16_t>(vector, sum, valid); return true;
        case DRAKEN_UINT32:  accumulate<uint32_t>(vector, sum, valid); return true;
        case DRAKEN_UINT64:  accumulate<uint64_t>(vector, sum, valid); return true;
        case DRAKEN_NULL:    *sum = 0; *valid = 0;                     return true;
        default:             return false;
    }
}

}}  // namespace draken::ops
