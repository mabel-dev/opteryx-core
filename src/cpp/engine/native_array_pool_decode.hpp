#pragma once
// src/cpp/engine/native_array_pool_decode.hpp — genuinely native (zero-Python)
// decoder for ARRAY (parquet LIST) columns that rugo's ParquetIOPipeline routes
// through MemoryPool as TAG_ARRAY (11).
//
// WHY these columns land here at all: a list column carries repetition levels,
// and rugo/src/parquet/io_pipeline.hpp's direct_kind_for() classifies ANY column
// with non-empty rep_levels as DK_POOL — there is no "direct" list kind. The
// pipeline then serializes it via ipc_serialize.hpp's serialize_list_column into
// the recursive TAG_ARRAY wire format. Both scan paths (trampoline and native)
// share that producer side verbatim; only the CONSUMER differed — the trampoline
// parses TAG_ARRAY in Cython (opteryx/compiled/structures/column_deserializer.pyx,
// `_build_array_vector*`) and boxes the result through the PyObject-returning
// draken_vector_own_array* family. This file is the PyObject-free counterpart of
// exactly those three functions; it is a faithful port, not a reimplementation,
// because the bar is byte-identical output to the trampoline.
//
// Scope (fail loud, not silently, outside it):
//   - TAG_ARRAY (11) only. The caller (native_parquet_scan_source.hpp) routes a
//     DK_POOL column here when the plan flagged it ARRAY via `array_columns`;
//     any other pool tag on such a column is an error, never a guess.
//   - Child (element) tags, all of which serialize_list_column can emit:
//       CHILD_INT64(1) CHILD_INT32(2) CHILD_FLOAT32(3) CHILD_FLOAT64(4)
//       CHILD_BOOL(5)  CHILD_STRING(6) CHILD_UINT64(7)  CHILD_ARRAY(8, nested)
//       CHILD_INT8(9)  CHILD_INT16(10) CHILD_UINT8(11)  CHILD_UINT16(12)
//       CHILD_UINT32(13)
//     CHILD_ARRAY recurses, so list<list<...>> of arbitrary depth is handled the
//     same way the Cython `_build_array_vector_nested` handles it.
//   - Element types rugo CANNOT serialize (int96, fixed_len_byte_array/int128
//     decimal, struct/map children) never reach here: serialize_list_column
//     itself throws on them, on BOTH scan paths, and the plan-time footer gate
//     (native_scan_supported, pool_reader.pyx) refuses those physical types.
//
// SHAPE (CLAUDE.md §11): every vector built here is DENSE — the parent ARRAY and
// its child alike, exactly as draken_vector_own_array{,_numeric,_child} build
// them. There is no encoding-shape discriminant anywhere in this file, and no
// fast path keyed on one; the uniform `data[selection[i]]` contract holds for
// every vector it produces.
//
// LIFETIME: the parent VectorOwner owns `offsets` (data_buf) and the parent
// validity bitmap; its `child_owner` (draken/core/vector_owner.h) owns the child
// VectorOwner outright, whose destructor chains recursively. Nothing here aliases
// a child buffer into a second owner — the one sanctioned intra-morsel aliasing
// pattern (draken/morsels/sort.hpp:805, a non-owning shared_ptr aliased onto
// `own->child_owner.get()`) is a CONSUMER-side borrow of an already-built vector
// and has no counterpart in a decoder, which only ever constructs.

#include <cstdint>
#include <cstring>
#include <memory>

#include "operator.hpp"          // CxxColumn, ErrCtx
#include "memory_pool.hpp"       // opteryx::MemoryPool, ReadResult
#include "core/buffers.h"        // DrakenStringArena, DrakenVector, DrakenType
#include "core/vector_alloc.h"   // draken_vector_from_dense
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "core/alloc.h"          // draken_malloc / draken_free
#include "core/string_slot.h"    // DrakenStringSlot, draken_build_string_slot
#include "logical_type.h"        // LogicalType / logical_type_intern (ARRAY<TIMESTAMP> child)
#include "native_varchar_pool_decode.hpp"  // varchar_pool_read_u32, consolidate_string_block

namespace opteryx::engine {

// Child (element) wire tags — ipc_serialize.hpp's CHILD_* constants, mirrored by
// column_deserializer.pyx's DEF block. Kept as an enum rather than magic numbers
// so a producer-side addition shows up as an unhandled case, not a mis-parse.
enum : uint8_t {
    ARR_CHILD_INT64   = 1,
    ARR_CHILD_INT32   = 2,
    ARR_CHILD_FLOAT32 = 3,
    ARR_CHILD_FLOAT64 = 4,
    ARR_CHILD_BOOL    = 5,
    ARR_CHILD_STRING  = 6,
    ARR_CHILD_UINT64  = 7,
    ARR_CHILD_ARRAY   = 8,
    // Narrow integer leaves (physical int32 + INTEGER(bitWidth, isSigned)).
    // Appended; 1..8 never renumber.
    ARR_CHILD_INT8    = 9,
    ARR_CHILD_INT16   = 10,
    ARR_CHILD_UINT8   = 11,
    ARR_CHILD_UINT16  = 12,
    ARR_CHILD_UINT32  = 13,
};

// Cheap structural bound check. The Cython reference does none of this — it trusts
// the producer — but this decoder runs on a native worker thread with no Python
// exception to unwind into, so a malformed blob must surface as an ErrCtx rather
// than an out-of-bounds read.
inline bool array_pool_have(const uint8_t* p, size_t need, const uint8_t* end, ErrCtx& err) {
    if (p > end || static_cast<size_t>(end - p) < need) {
        err.code = 1;
        err.msg = "build_pool_array_column: truncated TAG_ARRAY payload";
        return false;
    }
    return true;
}

// Copy `len` bytes of validity bitmap into a freshly draken_malloc'd buffer, or
// return nullptr when the level carries none (== every entry valid).
inline uint8_t* array_pool_copy_validity(const uint8_t* src, uint32_t len) {
    if (len == 0u) return nullptr;
    uint8_t* buf = static_cast<uint8_t*>(draken_malloc(len));
    std::memcpy(buf, src, len);
    return buf;
}

// Build ONE nesting level's VectorOwner. `p` enters pointing at this level's
// child_type_tag byte (i.e. just past the generic IPC header for the outermost
// level, or just past the parent level's inner validity bitmap for a nested one)
// and leaves pointing just past this level's block. Returns nullptr with `err`
// set on any malformed or unsupported input.
//
// Mirrors column_deserializer.pyx's `_build_array_vector` dispatch plus its
// three builders (`_build_array_vector_nested` / `_string` / `_numeric`) and the
// draken_vector_own_array* functions they hand off to, with the nanobind boxing
// removed.
inline std::unique_ptr<VectorOwner> build_array_level(const uint8_t*& p, const uint8_t* end,
                                                      uint32_t num_rows,
                                                      const uint8_t* list_null_bmap,
                                                      uint32_t list_null_bmap_len,
                                                      ErrCtx& err) {
    if (!array_pool_have(p, 5u, end, err)) return nullptr;
    const uint8_t child_tag = p[0];
    p += 1;
    uint32_t child_count;
    p = varchar_pool_read_u32(p, &child_count);

    // Offsets (Arrow-style child start indices), one per row plus the terminal.
    // memcpy'd into an aligned owned buffer: the pool blob is byte-packed, and an
    // unaligned int32 read is a SIGBUS on ARM.
    const size_t offsets_bytes = (static_cast<size_t>(num_rows) + 1u) * sizeof(int32_t);
    if (!array_pool_have(p, offsets_bytes, end, err)) return nullptr;
    int32_t* offsets = static_cast<int32_t*>(draken_malloc(offsets_bytes));
    std::memcpy(offsets, p, offsets_bytes);
    p += offsets_bytes;
    OwnedBuffer<void> offsets_buf(offsets);

    std::unique_ptr<VectorOwner> child;

    if (child_tag == ARR_CHILD_ARRAY) {
        // This level's entries hold nested lists: the child level's own validity
        // bitmap, then its block (its num_rows == our child_count).
        if (!array_pool_have(p, 4u, end, err)) return nullptr;
        uint32_t inner_bmap_len;
        p = varchar_pool_read_u32(p, &inner_bmap_len);
        if (!array_pool_have(p, inner_bmap_len, end, err)) return nullptr;
        const uint8_t* inner_bmap = p;
        p += inner_bmap_len;
        child = build_array_level(p, end, child_count, inner_bmap, inner_bmap_len, err);
        if (!child) return nullptr;
    } else {
        // Leaf level: per-element validity bitmap, then the element body.
        if (!array_pool_have(p, 4u, end, err)) return nullptr;
        uint32_t child_bmap_len;
        p = varchar_pool_read_u32(p, &child_bmap_len);
        if (!array_pool_have(p, child_bmap_len, end, err)) return nullptr;
        const uint8_t* child_bmap_src = p;
        p += child_bmap_len;

        if (child_tag == ARR_CHILD_STRING) {
            // Body: child_count records of [u32 len][len bytes]. Null elements are
            // written as a zero-length record (they still occupy a slot) and are
            // masked by child_bmap.
            //
            // NOTE the arena sizing: this sums EVERY element's length, inline
            // (<= STR_INLINE_MAX) ones included, and advances arena_pos for all of
            // them — byte-for-byte what `_build_array_vector_string` does. Inline
            // slots ignore their recorded offset, so the extra bytes are dead
            // weight, not a correctness difference; matching the reference layout
            // exactly is worth more here than shaving them.
            const uint8_t* scan = p;
            size_t total_arena = 0;
            for (uint32_t i = 0; i < child_count; ++i) {
                if (!array_pool_have(scan, 4u, end, err)) return nullptr;
                uint32_t slen;
                scan = varchar_pool_read_u32(scan, &slen);
                if (!array_pool_have(scan, slen, end, err)) return nullptr;
                total_arena += slen;
                scan += slen;
            }

            DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
                draken_malloc(static_cast<size_t>(child_count ? child_count : 1u) *
                              sizeof(DrakenStringSlot)));
            uint8_t* arena = static_cast<uint8_t*>(draken_malloc(total_arena ? total_arena : 1u));
            uint32_t arena_pos = 0;
            for (uint32_t i = 0; i < child_count; ++i) {
                uint32_t slen;
                p = varchar_pool_read_u32(p, &slen);
                if (slen > 0u) std::memcpy(arena + arena_pos, p, slen);
                // draken_build_string_slot reads the bytes from `p` for the hash /
                // inline payload; `arena_pos` is the offset a long value resolves
                // against (ignored for an inline one).
                draken_build_string_slot(slots + i, p, slen, arena_pos);
                p += slen;
                arena_pos += slen;
            }

            uint8_t* child_validity = array_pool_copy_validity(child_bmap_src, child_bmap_len);
            // The canonical string-vector block: [DrakenStringArena | slots | arena].
            // A raw slot pointer is NOT a valid string vector `data` — the slot/arena
            // kernels read `data` AS a DrakenStringArena*.
            DrakenStringArena* sa = nullptr;
            uint8_t* block = consolidate_string_block(slots, child_count, arena, total_arena,
                                                      DRAKEN_VARCHAR, &sa);
            draken_free(slots);
            draken_free(arena);
            // draken_vector_own_array publishes the child validity on the arena
            // header as well as the vector (consolidate_string_block leaves it
            // null, which is right for a top-level string column but not for an
            // array child) — keep the two paths' output identical.
            sa->null_bitmap = child_validity;
            DrakenVector child_vec = draken_vector_from_dense(sa, child_count, DRAKEN_VARCHAR,
                                                             child_validity);
            child = std::make_unique<VectorOwner>(child_vec, OwnedBuffer<void>(block),
                                                  OwnedBuffer<uint8_t>(child_validity));
        } else {
            DrakenType child_type;
            uint32_t elem_size;
            switch (child_tag) {
                case ARR_CHILD_INT64:   child_type = DRAKEN_INT64;   elem_size = 8; break;
                case ARR_CHILD_INT32:   child_type = DRAKEN_INT32;   elem_size = 4; break;
                case ARR_CHILD_UINT64:  child_type = DRAKEN_UINT64;  elem_size = 8; break;
                case ARR_CHILD_FLOAT32: child_type = DRAKEN_FLOAT32; elem_size = 4; break;
                case ARR_CHILD_FLOAT64: child_type = DRAKEN_FLOAT64; elem_size = 8; break;
                case ARR_CHILD_BOOL:    child_type = DRAKEN_BOOL;    elem_size = 0; break;
                case ARR_CHILD_INT8:    child_type = DRAKEN_INT8;    elem_size = 1; break;
                case ARR_CHILD_INT16:   child_type = DRAKEN_INT16;   elem_size = 2; break;
                case ARR_CHILD_UINT8:   child_type = DRAKEN_UINT8;   elem_size = 1; break;
                case ARR_CHILD_UINT16:  child_type = DRAKEN_UINT16;  elem_size = 2; break;
                case ARR_CHILD_UINT32:  child_type = DRAKEN_UINT32;  elem_size = 4; break;
                default:
                    err.code = 1;
                    err.msg = "build_pool_array_column: unsupported TAG_ARRAY child type tag";
                    return nullptr;
            }
            // BOOL elements are bit-packed LSB-first (DRAKEN_BOOL's own in-memory
            // contract); everything else is one packed native-endian slot each.
            const size_t data_bytes = (child_tag == ARR_CHILD_BOOL)
                ? ((static_cast<size_t>(child_count) + 7u) / 8u)
                : (static_cast<size_t>(child_count) * elem_size);
            if (!array_pool_have(p, data_bytes, end, err)) return nullptr;
            void* child_data = nullptr;
            if (data_bytes > 0) {
                child_data = draken_malloc(data_bytes);
                std::memcpy(child_data, p, data_bytes);
            }
            p += data_bytes;

            uint8_t* child_validity = array_pool_copy_validity(child_bmap_src, child_bmap_len);
            DrakenVector child_vec = draken_vector_from_dense(child_data, child_count,
                                                             child_type, child_validity);
            child = std::make_unique<VectorOwner>(child_vec, OwnedBuffer<void>(child_data),
                                                  OwnedBuffer<uint8_t>(child_validity));
        }
    }

    uint8_t* parent_validity = array_pool_copy_validity(list_null_bmap, list_null_bmap_len);
    DrakenVector parent_vec = draken_vector_from_dense(offsets, num_rows, DRAKEN_ARRAY,
                                                       parent_validity);
    auto owner = std::make_unique<VectorOwner>(parent_vec, OwnedBuffer<void>(offsets_buf.release()),
                                               OwnedBuffer<uint8_t>(parent_validity));
    owner->child_owner = std::move(child);
    return owner;
}

// Parse a whole TAG_ARRAY blob (generic IPC header + the level blocks) into a
// CxxColumn. Split out from build_pool_array_column so the pool latch/release is
// one unconditional pair around a function with ordinary early returns.
inline bool build_array_column_body(const uint8_t* p, const uint8_t* end,
                                    int coerce_kind, int coerce_unit,
                                    CxxColumn& out, ErrCtx& err) {
    if (p[0] != 11 /* TAG_ARRAY */) {
        err.code = 1;
        err.msg = "build_pool_array_column: pool-path tag is not TAG_ARRAY for an ARRAY column";
        return false;
    }
    // Generic IPC header: tag(1) num_rows(4) null_bitmap_len(4) [+bitmap].
    p += 1;
    if (!array_pool_have(p, 8u, end, err)) return false;
    uint32_t num_rows, bmap_len;
    p = varchar_pool_read_u32(p, &num_rows);
    p = varchar_pool_read_u32(p, &bmap_len);
    if (!array_pool_have(p, bmap_len, end, err)) return false;
    const uint8_t* bmap = p;
    p += bmap_len;

    std::unique_ptr<VectorOwner> owner =
        build_array_level(p, end, num_rows, bmap, bmap_len, err);
    if (!owner) return false;

    if (coerce_kind != 0) {
        // The only ARRAY coercion: an INT64 leaf retagged to TIMESTAMP64 with the
        // schema's unit. INT64 is the only shape the list decoder produces for a
        // timestamp leaf — anything else means the schema and the data disagree,
        // so fail loud rather than mislabel real values (the same contract as
        // draken's vector_retag_array_child_as_timestamp64).
        if (!owner->child_owner || owner->child_owner->vec.type != DRAKEN_INT64) {
            err.code = 1;
            err.msg = "build_pool_array_column: ARRAY<TIMESTAMP> retag requires an INT64 child";
            return false;
        }
        owner->child_owner->vec.type = DRAKEN_TIMESTAMP64;
        LogicalType lt;
        lt.kind = LogicalKind::TIMESTAMP;
        lt.unit = static_cast<TimestampUnit>(coerce_unit);
        lt.offset_minutes = 0;
        // MANDATORY for TIMESTAMP64 (vector_owner.h): a timestamp vector with a
        // null descriptor is a hard error downstream.
        owner->child_owner->logical_type = logical_type_intern(lt);
    }
    out.own = std::shared_ptr<VectorOwner>(std::move(owner));
    out.view = out.own->vec;
    return true;
}

// Read one TAG_ARRAY blob out of the MemoryPool and build the CxxColumn.
// `coerce_kind` is non-zero only for LC_ARRAY_TIMESTAMP (see
// native_parquet_scan_source.hpp) — the retag the trampoline scan performs with
// `vector_retag_array_child_as_timestamp64`, because parquet's list<timestamp>
// leaf decodes as physical int64 and the IPC format carries no logical type.
inline bool build_pool_array_column(MemoryPool* pool, int64_t ref_id, int coerce_kind,
                                    int coerce_unit, CxxColumn& out, ErrCtx& err) {
    ReadResult res = pool->read(ref_id, true);
    if (res.length == 0 || res.ptr == nullptr) {
        pool->unlatch(ref_id);
        err.code = 1;
        err.msg = "build_pool_array_column: MemoryPool read failed";
        return false;
    }
    const uint8_t* p = static_cast<const uint8_t*>(res.ptr);
    const bool ok = build_array_column_body(p, p + res.length, coerce_kind, coerce_unit,
                                            out, err);
    pool->unlatch(ref_id);
    pool->release(ref_id);
    return ok;
}

}  // namespace opteryx::engine
