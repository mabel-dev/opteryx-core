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
// Scope (fail loud, not silently, outside it): TAG_STR_DICT (6) and TAG_STR_PLAIN
// (7) — the exact two string tags column_deserializer.pyx's deserialize_column
// routes through _build_string_dict / _build_string_plain. A DK_POOL string
// column (rugo/src/parquet/io_pipeline.hpp direct_kind_for → DK_POOL for the
// RLE-skip-dense and non-positional cases) serialises via
// ipc_serialize.hpp's serialize_core as one of these two tags, so covering both
// keeps this native path a total function over every string shape the gate
// admits (the plain tag is reachable e.g. from an all-null string column). Long
// (>12 byte) dictionary/plain entries ARE supported: their bytes are copied into
// a freshly draken_malloc'd, consolidated arena (the wire buffer itself is only
// valid while the MemoryPool segment is latched, so it can't be referenced
// directly), and each long slot's offset is rebased to point into that new
// arena — see draken/core/vector_owner.h's VectorOwner.arena_buf.
//
// `want_type` is the schema's declared physical string type (DRAKEN_VARCHAR /
// DRAKEN_NVARCHAR / DRAKEN_VARBINARY). All three share the identical
// DrakenStringSlot/arena byte layout — it only sets the resulting vector's type
// tag, byte-identically to the Cython deserializer's `want_string_type` param.

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "operator.hpp"          // CxxColumn, ErrCtx
#include "memory_pool.hpp"       // opteryx::MemoryPool, ReadResult
#include "core/buffers.h"        // DrakenStringArena, DrakenVector
#include "core/vector_alloc.h"   // draken_vector_from_dense / draken_vector_from_dict
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "core/alloc.h"          // draken_malloc / draken_free
#include "core/string_slot.h"    // DrakenStringSlot, draken_build_string_slot

namespace opteryx::engine {

inline const uint8_t* varchar_pool_read_u32(const uint8_t* p, uint32_t* out) {
    std::memcpy(out, p, 4);
    return p + 4;
}

// ── Canonical string-vector emission ────────────────────────────────────────
// A string DrakenVector's `data` must point at ONE consolidated block laid out
// [ DrakenStringArena header | DrakenStringSlot[nslots] | arena_bytes ], exactly
// as draken_vector_own_string / native_group_sinks.hpp's emit_string_lane_column
// build it — the slot/arena kernels (draken/ops/string_gather.h str_slice/str_take)
// read `data` AS a DrakenStringArena*, never as a bare slot array. Building the
// vector with a raw slot pointer (as an earlier draft did) is a latent
// use-after-cast that only surfaces once the vector reaches a general string
// kernel (e.g. Morsel.slice). These helpers are the nogil, PyObject-free
// counterpart of draken_vector_own_string{,_dict}.
//
// `src_slots` (nslots entries) and `src_arena` (arena_len bytes) are copied
// verbatim into the block and then draken_free'd (ownership transferred, same as
// draken_vector_own_string). Slot arena offsets are relative to the arena base,
// so a verbatim copy needs no rebasing. `validity` (and `codes` for the dict
// variant) are retained by the VectorOwner.
inline uint8_t* consolidate_string_block(const DrakenStringSlot* src_slots, uint32_t nslots,
                                         const uint8_t* src_arena, size_t arena_len,
                                         DrakenType type, DrakenStringArena** out_sa,
                                         bool payloads_elided = false) {
    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes = (nslots > 0u ? static_cast<size_t>(nslots) : 1u) *
                               sizeof(DrakenStringSlot);
    const size_t arena_start = struct_end + slots_bytes;
    const size_t total = arena_start + arena_len;
    const size_t alloc_size = total > 0u ? total : sizeof(DrakenStringArena);

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc_size));
    std::memset(block, 0, alloc_size);
    DrakenStringArena* sa = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot* dslots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t* darena = (arena_len > 0u) ? (block + arena_start) : nullptr;
    if (nslots > 0u && src_slots)
        std::memcpy(dslots, src_slots, static_cast<size_t>(nslots) * sizeof(DrakenStringSlot));
    if (arena_len > 0u && src_arena)
        std::memcpy(darena, src_arena, arena_len);
    sa->slots = dslots;
    sa->arena = darena;
    sa->length = nslots;
    sa->arena_used = arena_len;
    sa->arena_cap = arena_len;
    // Carry the decoder's state across the block copy. Losing it here is how a
    // downstream gather ends up believing payloads exist while the slots carry
    // the trap offset.
    sa->payloads_elided = payloads_elided ? 1u : 0u;
    sa->null_bitmap = nullptr;   // validity tracked separately via VectorOwner
    sa->owns_buffers = 0;
    sa->type = type;
    *out_sa = sa;
    return block;
}

// Dense (positional) string column: nslots == length, selection = global identity.
inline void emit_dense_string_column(DrakenStringSlot* src_slots, uint32_t length,
                                     uint8_t* src_arena, size_t arena_len,
                                     uint8_t* validity, DrakenType type, CxxColumn& out,
                                     uint64_t* keyhash = nullptr,
                                     bool payloads_elided = false,
                                     bool row_sorted = false,
                                     bool row_sorted_descending = false) {
    DrakenStringArena* sa = nullptr;
    uint8_t* block = consolidate_string_block(src_slots, length, src_arena, arena_len, type, &sa,
                                              payloads_elided);
    draken_free(src_slots);
    draken_free(src_arena);
    DrakenVector v = draken_vector_from_dense(sa, length, type, validity);
    // Clustering hint (rugo sorting_columns, trust-gated in metadata.cpp). Direct
    // scan callers pass the real value; pool/IPC deserialize callers leave the
    // default (that wire format does not yet carry it — see the parquet writer
    // conversation's scoping note).
    if (row_sorted)
        v.flags |= DRAKEN_ROW_SORTED | (row_sorted_descending ? DRAKEN_ROW_SORTED_DESC : 0);
    out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(block),
                                            OwnedBuffer<uint8_t>(validity));
    // E37: attach the scan-carried seed (length entries), taking ownership.
    if (keyhash) out.own->keyhash_buf = OwnedBuffer<uint64_t>(keyhash);
    out.view = out.own->vec;
}

// Dict-shaped string column: value array of `data_length` unique slots + a per-row
// `codes` selection (retained by the VectorOwner). `sorted` carries the parquet
// dictionary's on-disk is_sorted hint (rugo ColumnOut.dict_sorted) through to
// DRAKEN_DICT_KEYS_SORTED.
inline void emit_dict_string_column(DrakenStringSlot* src_slots, uint32_t data_length,
                                    uint8_t* src_arena, size_t arena_len,
                                    uint32_t* codes, uint32_t length,
                                    uint8_t* validity, DrakenType type, CxxColumn& out,
                                    bool sorted = false, uint64_t* keyhash = nullptr,
                                    bool row_sorted = false,
                                    bool row_sorted_descending = false) {
    DrakenStringArena* sa = nullptr;
    uint8_t* block = consolidate_string_block(src_slots, data_length, src_arena, arena_len,
                                              type, &sa);
    draken_free(src_slots);
    draken_free(src_arena);
    DrakenVector v = draken_vector_from_dict(sa, data_length, codes, length, type, validity);
    if (sorted && draken_is_dict(&v))
        v.flags |= DRAKEN_DICT_KEYS_SORTED;
    // Clustering hint (rugo sorting_columns) — unlike DICT_KEYS_SORTED above this
    // is not gated on the dict shape (row order is meaningful regardless).
    if (row_sorted)
        v.flags |= DRAKEN_ROW_SORTED | (row_sorted_descending ? DRAKEN_ROW_SORTED_DESC : 0);
    out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(block),
                                            OwnedBuffer<uint8_t>(validity),
                                            OwnedBuffer<void>(codes));
    // E37: attach the scan-carried seed (data_length distinct entries), taking ownership.
    if (keyhash) out.own->keyhash_buf = OwnedBuffer<uint64_t>(keyhash);
    out.view = out.own->vec;
}

// TAG_STR_DICT (6): dict-shaped string vector. `p` points at the tag byte; the
// caller owns the pool latch and releases it after this returns. Byte-identical
// to column_deserializer.pyx's _build_string_dict.
inline bool build_pool_varchar_dict_body(const uint8_t* p, DrakenType want_type,
                                         CxxColumn& out, ErrCtx& err) {
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
        err.code = 1;
        err.msg = "build_pool_varchar_dict_body: malformed TAG_STR_DICT offsets";
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

    void* slots = draken_malloc(static_cast<size_t>(dict_size ? dict_size : 1u) *
                                sizeof(DrakenStringSlot));
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
        draken_malloc(static_cast<size_t>(num_rows ? num_rows : 1u) * sizeof(uint32_t)));
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

    emit_dict_string_column(slot_ptr, dict_size, arena_buf, total_arena,
                            codes_buf, num_rows, validity_buf, want_type, out);
    return true;
}

// TAG_STR_PLAIN (7): one length-prefixed string per row (dense positional slots).
// `p` points at the tag byte; caller owns the pool latch. Byte-identical to
// column_deserializer.pyx's _build_string_plain.
//   tag(1) num_rows(4) null_bitmap_len(4)[+bytes] num_strings(4)
//   [ len(4) bytes[len] ] * num_strings
inline bool build_pool_varchar_plain_body(const uint8_t* p, DrakenType want_type,
                                          CxxColumn& out, ErrCtx& err) {
    const uint8_t* q = p + 1;
    uint32_t num_rows;  q = varchar_pool_read_u32(q, &num_rows);
    uint32_t nbmp_len;  q = varchar_pool_read_u32(q, &nbmp_len);
    const uint8_t* nbmp_src = q; q += nbmp_len;
    uint32_t n;          q = varchar_pool_read_u32(q, &n);
    const uint8_t* body = q;  // first (len, bytes) record

    // The `n` records are COMPACT (present-only): Parquet omits null rows from the
    // value stream. The emitted string column is POSITIONAL — one slot per logical
    // row (row i at slot i), null rows an init-null slot masked by the validity
    // bitmap. So the vector length is num_rows, NOT n; we scatter the n present
    // records to their row positions via the null bitmap, byte-identically to the
    // Cython _build_string_plain. (Emitting length n dropped every null row:
    // all-null → 0 rows, and a partially-null plain column silently lost its nulls.)
    if (num_rows == 0) {
        // Empty column — hand back a length-0 string vector.
        void* slots0 = draken_malloc(sizeof(DrakenStringSlot));
        uint8_t* arena0 = static_cast<uint8_t*>(draken_malloc(1u));
        emit_dense_string_column(static_cast<DrakenStringSlot*>(slots0), 0,
                                 arena0, 0, nullptr, want_type, out);
        return true;
    }

    // First pass: total arena bytes over the n present records (only length gates
    // arena residence; long strings > STR_INLINE_MAX live in the arena).
    const uint8_t* scan = body;
    size_t total_arena = 0;
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t slen; scan = varchar_pool_read_u32(scan, &slen);
        if (slen > 12 /* STR_INLINE_MAX */) total_arena += slen;
        scan += slen;
    }

    void* slots = draken_malloc(static_cast<size_t>(num_rows) * sizeof(DrakenStringSlot));
    auto* slot_ptr = static_cast<DrakenStringSlot*>(slots);
    uint8_t* arena_buf = static_cast<uint8_t*>(draken_malloc(total_arena ? total_arena : 1u));
    uint8_t* validity_buf = nullptr;
    if (nbmp_len > 0) {
        validity_buf = static_cast<uint8_t*>(draken_malloc(nbmp_len));
        std::memcpy(validity_buf, nbmp_src, nbmp_len);
    }

    // Second pass: scatter present records into positional slots. Null rows get an
    // init-null slot and consume NO record from the (compact) stream; present rows
    // consume the next record. With no null bitmap the column is non-nullable, so
    // every row is present and n == num_rows (a straight positional copy).
    const uint8_t* r = body;
    uint32_t arena_pos = 0;
    for (uint32_t i = 0; i < num_rows; ++i) {
        DrakenStringSlot* slot = slot_ptr + i;
        if (nbmp_len > 0 && !((nbmp_src[i >> 3] >> (i & 7)) & 1)) {
            str_init_null(slot);  // null row: no bytes in the stream
            continue;
        }
        uint32_t slen; r = varchar_pool_read_u32(r, &slen);
        if (slen > 12 /* STR_INLINE_MAX */) {
            std::memcpy(arena_buf + arena_pos, r, slen);
            draken_build_string_slot(slot, r, slen, arena_pos);
            r += slen;
            arena_pos += slen;
        } else {
            draken_build_string_slot(slot, r, slen, arena_pos);  // inline; offset ignored
            r += slen;
        }
    }

    emit_dense_string_column(slot_ptr, num_rows, arena_buf, arena_pos, validity_buf, want_type, out);
    return true;
}

// Total function over the two string pool tags a gate-admitted string column can
// land on. `want_type` = the schema's declared DRAKEN_VARCHAR/NVARCHAR/VARBINARY.
inline bool build_pool_varchar_column(MemoryPool* pool, int64_t ref_id, DrakenType want_type,
                                      CxxColumn& out, ErrCtx& err) {
    ReadResult res = pool->read(ref_id, true);
    if (res.length == 0 || res.ptr == nullptr) {
        pool->unlatch(ref_id);
        err.code = 1;
        err.msg = "build_pool_varchar_column: MemoryPool read failed";
        return false;
    }
    const uint8_t* p = static_cast<const uint8_t*>(res.ptr);
    uint8_t tag = p[0];
    bool ok;
    if (tag == 6 /* TAG_STR_DICT */) {
        ok = build_pool_varchar_dict_body(p, want_type, out, err);
    } else if (tag == 7 /* TAG_STR_PLAIN */) {
        ok = build_pool_varchar_plain_body(p, want_type, out, err);
    } else {
        err.code = 1;
        err.msg = "build_pool_varchar_column: unsupported pool-path tag for a VARCHAR column";
        ok = false;
    }
    pool->unlatch(ref_id);
    pool->release(ref_id);
    return ok;
}

}  // namespace opteryx::engine
