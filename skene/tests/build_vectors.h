#pragma once
// Test-only constructors for draken vectors and morsels.
//
// These build the SAME buffer shapes draken's own constructors produce, using
// draken's allocator, so a VectorOwner frees them correctly. Nothing here is a
// simplification of the real layout — a test that serialized a simplified vector
// would prove nothing about the real one.

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"
#include "morsels/cxx_morsel.h"

namespace skene_test {

// Validity bitmap: bit i set == row i is VALID. nullptr when every row is valid,
// which is what draken means by "no validity buffer".
inline uint8_t* make_validity(const std::vector<bool>& valid) {
    bool any_null = false;
    for (bool v : valid) if (!v) { any_null = true; break; }
    if (!any_null) return nullptr;

    const size_t bytes = (valid.size() + 7u) / 8u;
    uint8_t* bitmap = static_cast<uint8_t*>(draken_malloc(bytes));
    std::memset(bitmap, 0, bytes);
    for (size_t i = 0; i < valid.size(); ++i)
        if (valid[i]) bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    return bitmap;
}

template <typename T>
inline void* copy_values(const std::vector<T>& values) {
    const size_t bytes = values.size() * sizeof(T);
    void* buf = draken_malloc(bytes > 0 ? bytes : 1);
    if (bytes > 0) std::memcpy(buf, values.data(), bytes);
    return buf;
}

// Dense: one value per row, identity selection.
template <typename T>
inline CxxColumn dense_column(const std::vector<T>& values, DrakenType type,
                              const std::vector<bool>& valid = {},
                              const LogicalType* logical = nullptr) {
    void*    data     = copy_values(values);
    uint8_t* validity = valid.empty() ? nullptr : make_validity(valid);
    DrakenVector v = draken_vector_from_dense(
        data, static_cast<uint32_t>(values.size()), type, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity));
    column.own->logical_type = logical;
    return column;
}

// BOOL is bit-packed, so it cannot go through dense_column (which sizes the
// buffer as n * sizeof(T)). data_length is the ROW count and the data buffer is
// ceil(n/8) bytes — get that wrong and the written data section is 8x too big.
inline CxxColumn bool_column(const std::vector<bool>& bits,
                             const std::vector<bool>& valid = {}) {
    const size_t bytes = (bits.size() + 7u) / 8u;
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(bytes > 0 ? bytes : 1));
    std::memset(data, 0, bytes > 0 ? bytes : 1);
    for (size_t i = 0; i < bits.size(); ++i)
        if (bits[i]) data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));

    uint8_t* validity = valid.empty() ? nullptr : make_validity(valid);
    DrakenVector v = draken_vector_from_dense(
        data, static_cast<uint32_t>(bits.size()), DRAKEN_BOOL, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity));
    return column;
}

// Constant: one physical value broadcast to `length` rows, zero selection.
template <typename T>
inline CxxColumn constant_column(T value, uint32_t length, DrakenType type,
                                 const LogicalType* logical = nullptr) {
    std::vector<T> one{value};
    void* data = copy_values(one);
    DrakenVector v = draken_vector_from_constant(data, length, type, nullptr);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr));
    column.own->logical_type = logical;
    return column;
}

// Dictionary: `values` are the distinct physical values, `codes` index into them
// per row. This is the shape whose `selection` MUST survive verbatim — the
// reason Parquet was rejected is that it re-derives the encoding rather than
// restoring it.
template <typename T>
inline CxxColumn dict_column(const std::vector<T>& values,
                             const std::vector<uint32_t>& codes, DrakenType type,
                             const std::vector<bool>& valid = {}) {
    void*     data     = copy_values(values);
    uint8_t*  validity = valid.empty() ? nullptr : make_validity(valid);
    uint32_t* owned    = static_cast<uint32_t*>(
        draken_malloc(codes.size() * sizeof(uint32_t)));
    std::memcpy(owned, codes.data(), codes.size() * sizeof(uint32_t));

    DrakenVector v = draken_vector_from_dict(
        data, static_cast<uint32_t>(values.size()), owned,
        static_cast<uint32_t>(codes.size()), type, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity),
        OwnedBuffer<void>(owned));
    return column;
}

// German-string column, in the single-block layout draken itself uses:
//   [ DrakenStringArena | DrakenStringSlot[n] | arena bytes ]
//
// `elide_payloads` builds a LENGTH-ONLY column: no arena bytes, and every long
// slot stamped STR_ELIDED_PAYLOAD_OFFSET as a trap. That is the case where
// losing payloads_elided across a round trip turns a trap value into a 4 GB
// out-of-bounds read, so it has to be exercised for real.
inline CxxColumn string_column(const std::vector<std::string>& values,
                               DrakenType type = DRAKEN_VARCHAR,
                               const std::vector<bool>& valid = {},
                               bool elide_payloads = false) {
    const size_t n = values.size();

    size_t arena_bytes = 0;
    if (!elide_payloads)
        for (const std::string& s : values)
            if (s.size() > STR_INLINE_MAX) arena_bytes += s.size();

    const size_t struct_end  = sizeof(DrakenStringArena);
    const size_t slots_bytes = (n > 0 ? n : 1) * sizeof(DrakenStringSlot);
    const size_t total       = struct_end + slots_bytes + arena_bytes;

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(total));
    std::memset(block, 0, total);

    DrakenStringArena* sa   = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena = (arena_bytes > 0) ? (block + struct_end + slots_bytes)
                                                 : nullptr;

    size_t arena_used = 0;
    for (size_t i = 0; i < n; ++i) {
        const std::string& s = values[i];
        const uint32_t len = static_cast<uint32_t>(s.size());
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], reinterpret_cast<const uint8_t*>(s.data()), len);
        } else if (elide_payloads) {
            // Length recorded, payload deliberately never materialized.
            slots[i].ext.length       = len;
            slots[i].ext.prefix       = 0;
            slots[i].ext.hash32       = 0;
            slots[i].ext.arena_offset = STR_ELIDED_PAYLOAD_OFFSET;
        } else {
            std::memcpy(arena + arena_used, s.data(), len);
            str_init_extern(&slots[i], reinterpret_cast<const uint8_t*>(s.data()),
                            len, static_cast<uint32_t>(arena_used));
            arena_used += len;
        }
    }

    sa->slots           = slots;
    sa->arena           = arena;
    sa->length          = n;
    sa->arena_used      = arena_used;
    sa->arena_cap       = arena_bytes;
    sa->null_bitmap     = nullptr;  // validity lives on the DrakenVector
    sa->owns_buffers    = 0;        // the VectorOwner's unique_ptr IS the record
    sa->payloads_elided = elide_payloads ? 1 : 0;
    sa->type            = type;

    uint8_t* validity = valid.empty() ? nullptr : make_validity(valid);
    DrakenVector v = draken_vector_from_dense(block, static_cast<uint32_t>(n),
                                              type, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(block), OwnedBuffer<uint8_t>(validity));
    return column;
}

// VECTOR_FP16 cannot go through dense_column either, and for the opposite
// reason to BOOL: its data buffer is LARGER than one item per row. `data` is a
// flat uint16 array of rows * dimension, while `length` and `data_length` count
// ROWS — dense_column would set both from the half count and write a column
// `dimension` times too long. The dimension itself lives ONLY in the logical
// descriptor, which is why every one of these takes it.
inline CxxColumn fp16_column(const std::vector<uint16_t>& halves, uint32_t rows,
                             const LogicalType* logical,
                             const std::vector<bool>& valid = {}) {
    void*    data     = copy_values(halves);
    uint8_t* validity = valid.empty() ? nullptr : make_validity(valid);
    DrakenVector v = draken_vector_from_dense(data, rows, DRAKEN_VECTOR_FP16, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity));
    column.own->logical_type = logical;
    return column;
}

// Dictionary-encoded VECTOR_FP16: `halves` holds distinct * dimension values,
// `codes` one per row. dict_column cannot serve here — it sizes data from the
// value count and carries no logical type, and fp16 needs both corrected.
inline CxxColumn fp16_dict_column(const std::vector<uint16_t>& halves,
                                  uint32_t distinct,
                                  const std::vector<uint32_t>& codes,
                                  const LogicalType* logical) {
    void*     data  = copy_values(halves);
    uint32_t* owned = static_cast<uint32_t*>(
        draken_malloc(codes.size() * sizeof(uint32_t)));
    std::memcpy(owned, codes.data(), codes.size() * sizeof(uint32_t));

    DrakenVector v = draken_vector_from_dict(
        data, distinct, owned, static_cast<uint32_t>(codes.size()),
        DRAKEN_VECTOR_FP16, nullptr);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr),
        OwnedBuffer<void>(owned));
    column.own->logical_type = logical;
    return column;
}

// ARRAY: int32 offsets[length+1] in `data`, elements on child_owner.
inline CxxColumn array_column(const std::vector<std::vector<int64_t>>& rows,
                              const std::vector<bool>& valid = {}) {
    const size_t n = rows.size();

    std::vector<int64_t> flat;
    std::vector<int32_t> offsets(n + 1, 0);
    for (size_t i = 0; i < n; ++i) {
        for (int64_t value : rows[i]) flat.push_back(value);
        offsets[i + 1] = static_cast<int32_t>(flat.size());
    }

    void* offset_buf = draken_malloc(offsets.size() * sizeof(int32_t));
    std::memcpy(offset_buf, offsets.data(), offsets.size() * sizeof(int32_t));

    uint8_t* validity = valid.empty() ? nullptr : make_validity(valid);
    DrakenVector v = draken_vector_from_dense(
        offset_buf, static_cast<uint32_t>(n), DRAKEN_ARRAY, validity);

    CxxColumn column;
    column.view = v;
    column.own  = std::make_shared<VectorOwner>(
        v, OwnedBuffer<void>(offset_buf), OwnedBuffer<uint8_t>(validity));

    CxxColumn child = dense_column(flat, DRAKEN_INT64);
    column.own->child_owner = std::make_unique<VectorOwner>(std::move(*child.own));
    return column;
}

inline CxxMorsel morsel_of(std::vector<std::pair<std::string, CxxColumn>> columns) {
    CxxMorsel morsel;
    for (auto& entry : columns) {
        morsel.names.push_back(entry.first);
        morsel.columns.push_back(std::move(entry.second));
    }
    return morsel;
}

}  // namespace skene_test
