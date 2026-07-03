#pragma once
// src/cpp/engine/native_varchar_pool_decode.hpp — genuinely native (zero-Python)
// decoder for VARCHAR columns that rugo's ParquetIOPipeline routes through
// MemoryPool as TAG_STR_DICT, specifically the RLE-encoded case.
//
// WHY these columns land here at all: TPC-H's l_returnflag/l_linestatus are
// non-nullable dictionary-encoded byte_array columns with only a handful of
// distinct values. Verified directly against the real parquet files: rugo's
// decoder takes the "RLE skip-dense" path for exactly this shape (non-nullable
// + dict-encoded), producing rle_str_lens/rle_run_lengths rather than the
// plain dict_indices/dict_codes_array layout — so
// rugo/src/parquet/io_pipeline.hpp's direct_kind_for() does NOT classify them
// as DK_VARCHAR_DICT (that requires dict_indices/dict_codes_array to be
// non-empty); they fall through to DK_POOL, serialized via
// ipc_serialize.hpp's serialize_rle_string_as_dict as TAG_STR_DICT (6) — same
// tag column_deserializer.pyx's _build_string_dict already parses in Cython.
// This file reads that SAME wire format directly in C++, no Python involved.
//
// Scope (fail loud, not silently, outside it): only TAG_STR_DICT (6). Long
// (>12 byte) dictionary entries ARE supported: their bytes are copied into a
// freshly draken_malloc'd, consolidated arena (the wire buffer itself is only
// valid while the MemoryPool segment is latched, so it can't be referenced
// directly), and each long slot's offset is rebased to point into that new
// arena — see draken/core/vector_owner.h's VectorOwner.arena_buf.

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "operator.hpp"          // CxxColumn, ErrCtx
#include "memory_pool.hpp"       // opteryx::MemoryPool, ReadResult
#include "core/vector_alloc.h"   // draken_vector_from_dict
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "core/alloc.h"          // draken_malloc / draken_free
#include "core/string_slot.h"    // DrakenStringSlot, draken_build_string_slot

namespace opteryx::engine {

inline const uint8_t* varchar_pool_read_u32(const uint8_t* p, uint32_t* out) {
    std::memcpy(out, p, 4);
    return p + 4;
}

inline bool build_pool_varchar_dict_column(MemoryPool* pool, int64_t ref_id, CxxColumn& out,
                                           ErrCtx& err) {
    ReadResult r = pool->read(ref_id, true);
    if (r.length == 0 || r.ptr == nullptr) {
        pool->unlatch(ref_id);
        err.code = 1;
        err.msg = "build_pool_varchar_dict_column: MemoryPool read failed";
        return false;
    }
    const uint8_t* p = static_cast<const uint8_t*>(r.ptr);
    uint8_t tag = p[0];
    if (tag != 6 /* TAG_STR_DICT */) {
        pool->unlatch(ref_id);
        pool->release(ref_id);
        err.code = 1;
        err.msg = "build_pool_varchar_dict_column: unsupported pool-path tag for a VARCHAR column";
        return false;
    }

    // Wire format (rugo/src/parquet/ipc_serialize.hpp: serialize_rle_string_as_dict
    // / the general TAG_STR_DICT shape column_deserializer.pyx's _build_string_dict
    // parses):
    //   tag(1) num_rows(4) null_bitmap_len(4)[+bytes] dict_size(4) code_width(1)
    //   is_sorted(1) codes_len(4)[+codes, code_width bytes each]
    //   offsets_count(4)[+ (dict_size+1) int32 offsets] arena[...]
    const uint8_t* q = p + 1;
    uint32_t num_rows;  q = varchar_pool_read_u32(q, &num_rows);
    uint32_t nbmp_len;  q = varchar_pool_read_u32(q, &nbmp_len);
    const uint8_t* nbmp_src = q; q += nbmp_len;
    uint32_t dict_size; q = varchar_pool_read_u32(q, &dict_size);
    uint8_t code_width = q[0]; q += 1;
    q += 1;  // is_sorted hint — irrelevant here
    uint32_t codes_len; q = varchar_pool_read_u32(q, &codes_len);
    const uint8_t* codes_ptr = q; q += codes_len;
    uint32_t offsets_count; q = varchar_pool_read_u32(q, &offsets_count);
    const uint8_t* offsets_src = q; q += offsets_count * 4;
    const uint8_t* arena_src = q;

    if (offsets_count != dict_size + 1) {
        pool->unlatch(ref_id);
        pool->release(ref_id);
        err.code = 1;
        err.msg = "build_pool_varchar_dict_column: malformed TAG_STR_DICT offsets";
        return false;
    }

    // Copy offsets to an aligned temp buffer (ARM SIGBUS if read unaligned).
    std::vector<uint32_t> offsets(offsets_count);
    std::memcpy(offsets.data(), offsets_src, static_cast<size_t>(offsets_count) * 4);

    // First pass: total bytes needed for the consolidated arena (long entries only).
    size_t total_arena = 0;
    for (uint32_t k = 0; k < dict_size; ++k) {
        uint32_t slen = offsets[k + 1] - offsets[k];
        if (slen > 12 /* STR_INLINE_MAX */) total_arena += slen;
    }
    uint8_t* arena_buf = nullptr;
    if (total_arena > 0) {
        arena_buf = static_cast<uint8_t*>(draken_malloc(total_arena));
    }

    void* slots = draken_malloc(static_cast<size_t>(dict_size) * sizeof(DrakenStringSlot));
    auto* slot_ptr = static_cast<DrakenStringSlot*>(slots);
    size_t arena_pos = 0;
    for (uint32_t k = 0; k < dict_size; ++k) {
        uint32_t s_off = offsets[k];
        uint32_t s_off_end = offsets[k + 1];
        uint32_t slen = s_off_end - s_off;
        if (slen <= 12 /* STR_INLINE_MAX */) {
            draken_build_string_slot(slot_ptr + k, arena_src + s_off, slen, 0);
        } else {
            std::memcpy(arena_buf + arena_pos, arena_src + s_off, slen);
            draken_build_string_slot(slot_ptr + k, arena_buf + arena_pos, slen,
                                     static_cast<uint32_t>(arena_pos));
            arena_pos += slen;
        }
    }

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

    DrakenVector v = draken_vector_from_dict(slots, dict_size, codes_buf, num_rows,
                                             DRAKEN_VARCHAR, validity_buf);
    OwnedBuffer<void> data_buf(slots);
    OwnedBuffer<uint8_t> val_buf(validity_buf);
    OwnedBuffer<void> codes_owned(codes_buf);
    OwnedBuffer<uint8_t> arena_owned(arena_buf);
    out.own = std::make_shared<VectorOwner>(v, std::move(data_buf), std::move(val_buf),
                                            std::move(codes_owned), std::move(arena_owned));
    out.view = out.own->vec;
    return true;
}

}  // namespace opteryx::engine
