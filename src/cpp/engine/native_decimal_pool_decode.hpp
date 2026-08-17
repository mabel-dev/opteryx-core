#pragma once
// src/cpp/engine/native_decimal_pool_decode.hpp — genuinely native (zero-Python)
// decoder for int64-backed DECIMAL columns that rugo's ParquetIOPipeline routes
// through MemoryPool (the "pool path") rather than the "direct" fast path.
//
// WHY these columns land here at all: rugo/src/parquet/io_pipeline.hpp
// deliberately forces every int64-backed DECIMAL column (precision <= 18 —
// which is what TPC-H's l_extendedprice/l_discount/l_quantity/l_tax actually
// are) onto the pool path REGARDLESS of parquet-level encoding (plain or
// dictionary). That gate is a real, load-bearing design decision, not a
// technical limitation: a raw direct-INT64 reinterpret previously caused a
// genuine bug elsewhere in this codebase (tpch Q01 dec_mul type error) because
// that consumer took DK_INT64 at face value as plain INT64 and lost the scale
// semantics. This file does NOT touch that gate — it reads the SAME serialized
// bytes rugo already wrote into the MemoryPool (rugo/src/parquet/ipc_serialize.hpp)
// directly, entirely in C++, and builds a DrakenVector tagged DRAKEN_DECIMAL
// (not DRAKEN_INT64) itself, so the old failure mode cannot recur here — this
// consumer never mistakes the column for plain INT64.
//
// Scope (fail loud, not silently, outside it): only the three wire tags an
// int64-backed DECIMAL column can actually produce — TAG_INT64 (1, plain or
// RLE-widened), TAG_INT64_DICT (8, dictionary-encoded), and TAG_INT32 (2,
// rugo's unwidened plain encoding for int32-*physical* DECIMAL columns —
// e.g. TPC-DS's DECIMAL(7,2), too narrow-precision for parquet to store as
// INT64; see rugo/src/parquet/ipc_serialize.hpp's serialize_int32). Any other
// tag (string/array/int128/bool/etc.) reaching here means a non-decimal
// column was misrouted by the caller; this is treated as a hard error, never
// guessed at.
//
// Scale handling: NOT read from this buffer — int64-backed DECIMAL's wire
// format (ipc_serialize.hpp's serialize_int64) carries no precision/scale
// descriptor (unlike TAG_INT128's DECIMAL128 payload). Matching every other
// piece of this engine's decimal support (native_decimal.hpp), scale is
// plan-time-known and baked into the DecimalExpr tree by the caller — never
// re-derived here.

#include <cstdint>
#include <cstring>
#include <memory>

#include "operator.hpp"          // CxxColumn, ErrCtx
#include "memory_pool.hpp"       // opteryx::MemoryPool, ReadResult
#include "ipc_deserialize.hpp"   // opteryx::deserialize_fixed_column, DecodedFixedColumn, kTag*
#include "core/vector_alloc.h"   // draken_vector_from_dense / draken_vector_from_dict
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "core/alloc.h"          // draken_malloc / draken_free

namespace opteryx::engine {

inline const uint8_t* pool_dec_read_u32(const uint8_t* p, uint32_t* out) {
    std::memcpy(out, p, 4);
    return p + 4;
}

// Build a CxxColumn(type=DRAKEN_DECIMAL) directly from a MemoryPool-serialized
// int64-backed column. `ref_id` must reference a segment rugo's pool sink
// wrote (rugo::ColumnOut.ref_id for a DK_POOL column). Pool lifecycle:
// read(latch=true) -> parse -> unlatch -> release, all before returning,
// mirroring opteryx/compiled/structures/column_deserializer.pyx's
// deserialize_column/deserialize_row_group discipline exactly.
inline bool build_pool_decimal_column(MemoryPool* pool, int64_t ref_id, CxxColumn& out, ErrCtx& err) {
    ReadResult r = pool->read(ref_id, true);
    if (r.length == 0 || r.ptr == nullptr) {
        pool->unlatch(ref_id);
        err.code = 1;
        err.msg = "build_pool_decimal_column: MemoryPool read failed";
        return false;
    }
    const uint8_t* p = static_cast<const uint8_t*>(r.ptr);
    uint8_t tag = p[0];

    if (tag == kTagInt64 || tag == kTagInt32) {
        // deserialize_fixed_column already widens TAG_INT32's raw int32 payload
        // into the same int64-shaped DecodedFixedColumn::data as TAG_INT64
        // (ipc_deserialize.cpp's kTagInt32 case sets IpcKind::Int64) — this
        // branch doesn't need to know which tag it got past this point.
        DecodedFixedColumn dc;
        deserialize_fixed_column(p, r.length, dc);
        pool->unlatch(ref_id);
        pool->release(ref_id);
        if (dc.status != kStatusOk) {
            draken_free(dc.data);
            draken_free(dc.null_bitmap);
            err.code = 1;
            err.msg = "build_pool_decimal_column: fixed-column deserialize failed";
            return false;
        }
        // Scatter compact -> positional exactly as
        // column_deserializer.pyx's _wrap_decoded_fixed does for IpcKind_Int64.
        const uint32_t elem_size = 8;
        const uint32_t full_bytes = dc.num_rows * elem_size;
        void* pos_data;
        if (dc.null_bitmap != nullptr && dc.data_len < full_bytes && dc.num_rows > 0) {
            pos_data = draken_malloc(full_bytes);
            std::memset(pos_data, 0, full_bytes);
            const uint8_t* src = static_cast<const uint8_t*>(dc.data);
            uint8_t* dst = static_cast<uint8_t*>(pos_data);
            uint32_t compact_i = 0;
            for (uint32_t row = 0; row < dc.num_rows; ++row) {
                uint8_t bit = (dc.null_bitmap[row >> 3] >> (row & 7)) & 1u;
                if (bit) {
                    std::memcpy(dst + row * elem_size, src + compact_i * elem_size, elem_size);
                    ++compact_i;
                }
            }
            draken_free(dc.data);
        } else {
            pos_data = dc.data;
        }
        DrakenVector v = draken_vector_from_dense(pos_data, dc.num_rows, DRAKEN_DECIMAL, dc.null_bitmap);
        OwnedBuffer<void> data_buf(pos_data);
        OwnedBuffer<uint8_t> val_buf(dc.null_bitmap);
        out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf));
        out.view = out.own->vec;
        return true;
    }

    if (tag == kTagInt64Dict) {
        // Wire format (rugo/src/parquet/ipc_serialize.hpp: serialize_numeric_dict):
        //   tag(1) num_rows(4) null_bitmap_len(4)[+bytes] dict_size(4) code_width(1)
        //   is_sorted(1) codes_len(4)[+codes, code_width bytes each] values_len(4)[+int64 values]
        const uint8_t* q = p + 1;
        uint32_t num_rows;   q = pool_dec_read_u32(q, &num_rows);
        uint32_t nbmp_len;   q = pool_dec_read_u32(q, &nbmp_len);
        const uint8_t* nbmp_src = q; q += nbmp_len;
        uint32_t dict_size;  q = pool_dec_read_u32(q, &dict_size);
        uint8_t code_width = q[0]; q += 1;
        q += 1;  // is_sorted hint — irrelevant to SUM, skip
        uint32_t codes_len;  q = pool_dec_read_u32(q, &codes_len);
        const uint8_t* codes_ptr = q; q += codes_len;
        uint32_t values_len; q = pool_dec_read_u32(q, &values_len);
        (void)values_len;
        const uint8_t* dict_src = q;

        void* dict_vals = draken_malloc(static_cast<size_t>(dict_size) * sizeof(int64_t));
        std::memcpy(dict_vals, dict_src, static_cast<size_t>(dict_size) * sizeof(int64_t));

        uint32_t* codes_buf = static_cast<uint32_t*>(
            draken_malloc(static_cast<size_t>(num_rows) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < num_rows; ++i) {
            if (code_width == 1) {
                codes_buf[i] = static_cast<uint32_t>(codes_ptr[i]);
            } else if (code_width == 2) {
                uint16_t tmp16; std::memcpy(&tmp16, codes_ptr + i * 2, 2);
                codes_buf[i] = tmp16;
            } else {
                uint32_t tmp32; std::memcpy(&tmp32, codes_ptr + i * 4, 4);
                codes_buf[i] = tmp32;
            }
        }

        uint8_t* validity_buf = nullptr;
        if (nbmp_len > 0) {
            validity_buf = static_cast<uint8_t*>(draken_malloc(nbmp_len));
            std::memcpy(validity_buf, nbmp_src, nbmp_len);
        }

        pool->unlatch(ref_id);
        pool->release(ref_id);

        DrakenVector v = draken_vector_from_dict(dict_vals, dict_size, codes_buf, num_rows,
                                                 DRAKEN_DECIMAL, validity_buf);
        OwnedBuffer<void> data_buf(dict_vals);
        OwnedBuffer<uint8_t> val_buf(validity_buf);
        OwnedBuffer<void> codes_owned(codes_buf);
        out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf),
                                                 std::move(codes_owned));
        out.view = out.own->vec;
        return true;
    }

    // Any other tag reaching here means a non-decimal (or DECIMAL128) column
    // was misrouted into the decimal-only pool path — fail loud, never guess.
    pool->unlatch(ref_id);
    pool->release(ref_id);
    err.code = 1;
    err.msg = "build_pool_decimal_column: unsupported pool-path tag for a DECIMAL column";
    return false;
}

}  // namespace opteryx::engine
