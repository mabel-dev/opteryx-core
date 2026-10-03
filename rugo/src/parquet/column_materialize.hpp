#pragma once
// rugo/src/parquet/column_materialize.hpp — DecodedColumn -> owned Draken buffers.
//
// Pure C++ (rugo runs without Python). Each materializer scatters a decoded
// parquet column into draken_malloc'd buffers plus a validity bitmap and hands
// them back in a MaterializedColumn; turning that into a Vector handle is the
// only CPython step, done by the Python edge (parquet/_parquet_column_wrap.hpp,
// rugo_native only). One call per column; no per-row objects of any kind.
//
// Errors THROW, on the error path only: std::invalid_argument for a file that
// contradicts itself (corrupt dictionary code, RLE runs past the row count, a
// value stream shorter than the valid rows, DECIMAL precision/scale out of
// range), std::overflow_error for a value that does not fit its declared width
// or precision, std::bad_alloc when an allocation fails. Cython's `except +`
// maps these to ValueError / OverflowError / MemoryError. Nothing leaks on a throw.

#include <cstddef>
#include <cstdint>

#include "decode.hpp"           // DecodedColumn
#include "core/buffers.h"       // DrakenType

namespace rugo::_parquet {

// Owned column buffers ready for wrapping. Ownership passes to the Vector built
// from them (draken_vector_own_* consumes every buffer).
struct MaterializedColumn {
    DrakenType type      = DRAKEN_INT64;
    uint32_t   length    = 0;
    void*      data      = nullptr;  // values; DrakenStringSlot[length] for VARCHAR/VARBINARY
    uint8_t*   validity  = nullptr;  // nullptr = all valid
    uint8_t*   arena     = nullptr;  // VARCHAR/VARBINARY long-string bytes; nullptr when none
    size_t     arena_len = 0;
    uint8_t    precision = 0;        // DECIMAL / DECIMAL128 only
    uint8_t    scale     = 0;
};

// DECIMAL (p<=18, int64) or DECIMAL128 (p>18, int128) from a decoded DECIMAL column.
MaterializedColumn materialize_decimal(const DecodedColumn& col, int32_t num_rows);

// Integer column at its DECLARED width and signedness (IntType annotation; an
// unannotated column takes its width from the physical type).
MaterializedColumn materialize_int(const DecodedColumn& col, int32_t num_rows, bool from_int32);

// FLOAT32 (parquet `float`) or FLOAT64 (parquet `double`), values canonicalised.
MaterializedColumn materialize_float(const DecodedColumn& col, int32_t num_rows, bool from_float32);

// Dense VARCHAR (is_text) or VARBINARY from a decoded BYTE_ARRAY column.
MaterializedColumn materialize_string(const DecodedColumn& col, int32_t num_rows, bool is_text);

// Bit-packed BOOL from a decoded BOOLEAN column.
MaterializedColumn materialize_bool(const DecodedColumn& col, int32_t num_rows);

}  // namespace rugo::_parquet
