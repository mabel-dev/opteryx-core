// IPC deserialiser for the rugo→Cython column handoff.
//
// Reads the binary IPC format defined in rugo/src/parquet/ipc_serialize.hpp
// from a raw `(ptr, length)` buffer (the caller holds a MemoryPool latch on
// the underlying segment) and emits malloc'd destination buffers (data,
// null_bitmap) that the Cython caller transfers ownership of into a Draken
// Vector.
//
// Scope: fixed-width tags 1..5 (int64, int32→int64 widening, float32,
// float64, bool). Dict/string tags (6..10) are reported with
// status=kStatusNotHandled so the Cython layer can fall back to its existing
// implementation.
//
// All operations are nogil-safe: no Python C API calls, no exceptions thrown
// across the boundary. Errors are signalled via status fields. Allocations
// use std::malloc so the Cython destructor's free() is correct.
//
// Pool lifecycle stays with the Cython caller, which mirrors the existing
// deserialize_column semantics (caller latches; caller unlatches; the
// row-group loop releases).

#ifndef OPTERYX_IPC_DESERIALIZE_HPP
#define OPTERYX_IPC_DESERIALIZE_HPP

#include <cstddef>
#include <cstdint>

namespace opteryx {

// IPC type tags — must match rugo/src/parquet/ipc_serialize.hpp.
enum IpcTag : uint8_t {
    kTagInt64       = 1,
    kTagInt32       = 2,
    kTagFloat32     = 3,
    kTagFloat64     = 4,
    kTagBool        = 5,
    kTagStrDict     = 6,
    kTagStrPlain    = 7,
    kTagInt64Dict   = 8,
    kTagFloat32Dict = 9,
    kTagFloat64Dict = 10,
    kTagArray       = 11,  // list columns; Cython handles via _build_array_vector
    kTagInt128      = 12,  // DECIMAL128 (FLBA width 9..16, precision > 18)
    // E33 — unsigned integer (exact declared width, never widened). Not handled
    // by this fast C++ path (see the switch in ipc_deserialize.cpp) — Cython's
    // column_deserializer.pyx parses them, mirroring tags 6-10.
    kTagUInt8       = 13,
    kTagUInt16      = 14,
    kTagUInt32      = 15,
    kTagUInt64      = 16,
    kTagUInt8Dict   = 17,
    kTagUInt16Dict  = 18,
    kTagUInt32Dict  = 19,
    kTagUInt64Dict  = 20,
};

// IpcKind describes the Vector shape produced for the caller.
// Int32 widens to Int64 here because the destination buffer is already a
// contiguous int64 array.
enum class IpcKind : uint8_t {
    Int64    = 1,
    Float32  = 2,
    Float64  = 3,
    Bool     = 4,
    Int128   = 5,   // DECIMAL128: data is __int128 positional buffer after scatter
};

// Status codes returned via DecodedFixedColumn::status. Zero is success.
enum DeserializeStatus : int {
    kStatusOk          = 0,
    kStatusTruncated   = 1,   // declared length exceeds the IPC payload
    kStatusOom         = 2,   // std::malloc returned NULL
    kStatusUnknownTag  = 3,   // tag byte not in the known range
    kStatusNotHandled  = 4,   // tag is a dict/string variant; Cython handles
};

// Decoded fixed-width column. Buffers are malloc'd; ownership transfers to
// the Cython caller. On non-Ok status the pointers are nullptr, so an
// unconditional free() in the caller's error path is safe.
struct DecodedFixedColumn {
    IpcKind  kind;
    uint32_t num_rows;
    void*    data;          // malloc'd primary buffer (int64*, double*, float*, uint8_t*)
    uint32_t data_len;      // bytes in the compact data buffer (K * element_size, K <= num_rows)
                            // data_len < num_rows*element_size iff there are nulls and the
                            // stream is compact (Parquet omits null rows from the value stream).
                            // _wrap_decoded_fixed scatters compact → positional at the Draken
                            // boundary so the Vector always holds num_rows positional slots.
    uint8_t* null_bitmap;   // malloc'd null bitmap, or nullptr if non-nullable
    int      status;        // kStatusOk on success; see DeserializeStatus otherwise
    uint8_t  tag;           // raw IPC tag byte; useful for kStatusNotHandled dispatch
    uint8_t  decimal_precision; // DECIMAL128 only (kind==Int128); precision 1..38
    uint8_t  decimal_scale;     // DECIMAL128 only (kind==Int128); scale 0..precision
};

// Deserialise a single fixed-width column from an already-latched buffer.
//
// The caller is responsible for: holding a MemoryPool latch on the segment
// referenced by `data`, then unlatching after this function returns. This
// function performs no pool operations and never throws.
//
// On success: out.kind/num_rows/data/null_bitmap populated; status == kStatusOk.
// On failure: out.data and out.null_bitmap are nullptr; status non-zero; the
// out.tag field always reflects what was read from the payload (or 0 if the
// payload was too short to contain a tag byte).
void deserialize_fixed_column(const uint8_t* data, int64_t length, DecodedFixedColumn& out) noexcept;

// Forward declaration so the header doesn't pull in memory_pool.hpp. The .cpp
// includes the full definition.
class MemoryPool;

// Batched per-row-group driver. Loops over `ref_ids` and for each one:
//   - pool.read(ref_id, latch=true)
//   - deserialize_fixed_column(ptr, length, out[i])
//   - pool.unlatch(ref_id)
// All in a single C++ frame so the Cython caller crosses the GIL boundary
// exactly twice per row group (one nogil entry, one nogil exit), regardless
// of column count.
//
// Dict/string tags surface as out[i].status == kStatusNotHandled with the
// tag byte populated; the segment has already been unlatched. The Cython
// caller is expected to re-enter the existing single-column path for those
// ref_ids (which re-latches under its own lock — safe under MemoryPool's
// internal mutex).
//
// `out` must point to a caller-owned array of `n_cols` DecodedFixedColumn
// slots; entries are written in-place. The function never throws.
void deserialize_row_group_fixed(
    MemoryPool& pool,
    const int64_t* ref_ids,
    size_t n_cols,
    DecodedFixedColumn* out
) noexcept;

}  // namespace opteryx

#endif  // OPTERYX_IPC_DESERIALIZE_HPP
