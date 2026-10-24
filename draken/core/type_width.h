#pragma once
// Physical value width of a DrakenType, for code that must know whether two tags
// describe the same bytes (a re-tag) or different ones (a conversion).

#include <stddef.h>

#include "buffers.h"

#ifdef __cplusplus
extern "C" {
#endif

// Byte width of one fixed-width value; 0 = not a fixed-width type (the bit-packed
// BOOL, the string family, nested types).
static inline size_t draken_fixed_width(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:  case DRAKEN_UINT8:  return 1;
        case DRAKEN_INT16: case DRAKEN_UINT16: return 2;
        case DRAKEN_INT32: case DRAKEN_UINT32: case DRAKEN_FLOAT32:
        case DRAKEN_DATE32: case DRAKEN_TIME32: return 4;
        case DRAKEN_INT64: case DRAKEN_UINT64: case DRAKEN_FLOAT64: case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64: return 8;
        case DRAKEN_DECIMAL128: case DRAKEN_INTERVAL: case DRAKEN_UUID: return 16;
        default: return 0;
    }
}

// The variable-width types that share the string-arena layout.
static inline int draken_is_string_family(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY ||
           t == DRAKEN_VARIANT;
}

#ifdef __cplusplus
}  // extern "C"
#endif
