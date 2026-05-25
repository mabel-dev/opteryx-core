// Draken nanobind binding — Milestone B.1 / D.1.
//
// One module, one Python surface (doc 03). Exposes:
//   Vector     — Python handle wrapping VectorOwner (RAII; destructor frees via mimalloc).
//   Morsel     — dumb container grouping Vector handles; owns nothing in C++.
//   vector_from_sequence(list)        — int64 ingestion.
//   vector_from_string_sequence(list) — string ingestion (Milestone D.1).
//
// Edge marshalling (boxing/unboxing) lives ONLY in this file.
// No object in compiled paths between the edges.
// No import opteryx. No fallback to draken_old.

#include <Python.h>
#include <datetime.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "core/interval_slot.h"
#include "logical_type.h"
#include "fp16/fp16.h"   // fp16_ieee_from_fp32_value / fp16_ieee_to_fp32_value (D.11)
#include "ops/bool_logical.h"
#include "ops/bool_reductions.h"
#include "ops/hash.h"               // includes decimal_arith.h transitively (E.32)
#include "ops/int64_arithmetic.h"   // i64_neg (used by bridge round-trip test)
#include "ops/int64_reductions.h"   // i64_sum (used by bridge round-trip test)
#include "ops/string_gather.h"  // sg_eq_slots, str_hash_seed (for dict ingestion)
#include "core/draken_bridge.h"     // bridge surface declarations

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Ownership primitives (doc 01)
// ---------------------------------------------------------------------------

// Stateless deleter. Empty type → unique_ptr<T, DrakenFree> stays one word (EBO).
struct DrakenFree {
    void operator()(void* p) const noexcept { draken_free(p); }
};

template <typename T>
using OwnedBuffer = std::unique_ptr<T, DrakenFree>;

// VectorOwner: the frozen 40-byte DrakenVector ABI struct plus owned buffers.
//
// Ownership map:
//   data_buf     — owns vec.data  (typed payload; draken_free on destruct)
//                  For DRAKEN_ARRAY: owns int32_t offsets[length+1].
//   validity_buf — owns vec.validity (null bitmap or empty → nullptr if all-valid)
//   codes_buf    — owns vec.selection for dict-encoded vectors (nullptr for
//                  identity/zero selections which point at shared globals)
//   logical_type — BORROWED pointer into the global LogicalType registry.
//                  Non-null for parameterized physical types (TIMESTAMP64, etc.).
//                  nullptr for simple scalar types (INT64, FLOAT64, BOOL, …).
//                  MANDATORY for DRAKEN_TIMESTAMP64: using a timestamp vector
//                  with logical_type==nullptr is a hard error (fail loud).
//   child_owner  — Non-null only for DRAKEN_ARRAY. Owns the child DrakenVector
//                  (and transitively its subtree). Destructor chains recursively,
//                  so freeing the parent frees the whole subtree. No back-pointers.
//
// RAII: all unique_ptrs call draken_free via DrakenFree on destruction.
// No owns_* flags anywhere — the unique_ptr itself IS the ownership record.
struct VectorOwner {
    DrakenVector         vec;
    OwnedBuffer<void>    data_buf;
    OwnedBuffer<uint8_t> validity_buf;
    OwnedBuffer<void>    codes_buf;   // non-null only for dict shapes
    const LogicalType*   logical_type = nullptr;  // borrowed; registry-interned
    std::unique_ptr<VectorOwner> child_owner;     // non-null only for DRAKEN_ARRAY

    VectorOwner(DrakenVector v,
                OwnedBuffer<void>    d,
                OwnedBuffer<uint8_t> val,
                OwnedBuffer<void>    codes = OwnedBuffer<void>(nullptr)) noexcept
        : vec(v), data_buf(std::move(d)), validity_buf(std::move(val)),
          codes_buf(std::move(codes)), logical_type(nullptr), child_owner(nullptr) {}

    VectorOwner(const VectorOwner&)            = delete;
    VectorOwner& operator=(const VectorOwner&) = delete;
    VectorOwner(VectorOwner&&)                 = default;
    VectorOwner& operator=(VectorOwner&&)      = default;
    ~VectorOwner()                             = default;
};

// Morsel: dumb container grouping related Vector handles.
// Holds Python object references (refcount-based keep-alive); owns nothing in C++.
// When Morsel is collected, refcounts drop; if it was the last holder the Vector is freed.
struct Morsel {
    std::vector<nb::object> columns;

    void append_col(nb::object v) { columns.push_back(std::move(v)); }

    nb::object get_col(int64_t i) const {
        auto sz = static_cast<int64_t>(columns.size());
        if (i < 0) i += sz;
        if (i < 0 || i >= sz)
            throw nb::index_error("morsel column index out of range");
        return columns[static_cast<size_t>(i)];
    }

    size_t size() const noexcept { return columns.size(); }
};

// ---------------------------------------------------------------------------
// Int64 ingestion (the only type at this milestone)
// ---------------------------------------------------------------------------

static VectorOwner make_int64_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    // Allocate data buffer. Allocate at least 1 element so the pointer is non-NULL
    // even for empty sequences (DrakenVector.data must be interpretable as a typed ptr).
    const size_t data_bytes = (length > 0 ? length : 1u) * sizeof(int64_t);
    int64_t* data = static_cast<int64_t*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    // Fill data, detect nulls in one pass.
    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = 0;  // placeholder; validity bit will mark this null
            has_nulls = true;
        } else {
            data[i] = nb::cast<int64_t>(obj);
        }
    }

    // Validity bitmap: only allocated when nulls exist.
    // Arrow convention: bit set = valid, bit clear = null.
    // NULL validity pointer means all-valid (normalization invariant).
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (has_nulls) {
        // Allocate at least 8 bytes (SIMD-padded) even for small vectors.
        const uint32_t bitmap_bytes = (length + 7u) / 8u;
        const uint32_t padded = ((bitmap_bytes + 7u) & ~7u);
        const size_t alloc_bytes = (padded > 0) ? padded : 8u;

        validity = static_cast<uint8_t*>(draken_malloc(alloc_bytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);

        std::memset(validity, 0xFF, alloc_bytes);  // all valid; we clear nulls below

        for (uint32_t i = 0; i < length; ++i) {
            if (seq[i].is_none())
                validity[i / 8] &= static_cast<uint8_t>(~(1u << (i % 8)));
        }
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_INT64, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

// ---------------------------------------------------------------------------
// C.2 factory: constant-shape int64 vector (data_length == 1, length rows).
// value_obj may be None → all-null constant.
// ---------------------------------------------------------------------------

static VectorOwner make_int64_constant(nb::object value_obj, uint32_t length) {
    const bool is_null = value_obj.is_none();
    const int64_t scalar = is_null ? 0 : nb::cast<int64_t>(value_obj);

    int64_t* data = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t alloc_bytes = (padded > 0) ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(alloc_bytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, alloc_bytes);  // all null (bit 0 = null)
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_INT64, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

// ---------------------------------------------------------------------------
// C.2 factory: dict-encoded int64 vector.
// values_seq: Python list of int64 unique values (the dictionary).
// codes_seq:  Python list of int (uint32 codes), one per logical row.
// nullable_seq: optional Python list of bool (True=valid); if omitted, all valid.
// ---------------------------------------------------------------------------

static VectorOwner make_int64_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    // Allocate dict data.
    size_t dict_bytes = (dict_size > 0 ? dict_size : 1u) * sizeof(int64_t);
    int64_t* dict_data = static_cast<int64_t*>(draken_malloc(dict_bytes));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k)
        dict_data[k] = nb::cast<int64_t>(values_seq[k]);

    // Allocate codes.
    size_t codes_bytes = (length > 0 ? length : 1u) * sizeof(uint32_t);
    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(codes_bytes));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    // Optional validity bitmap.
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bitmap_bytes = (length + 7u) / 8u;
        const uint32_t padded = ((bitmap_bytes + 7u) & ~7u);
        const size_t alloc_bytes = (padded > 0) ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(alloc_bytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, alloc_bytes);
        for (uint32_t i = 0; i < length; ++i) {
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8] &= static_cast<uint8_t>(~(1u << (i % 8)));
        }
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, DRAKEN_INT64, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.1: string ingestion — Python list[str | None] → dense DRAKEN_VARCHAR vector.
//
// Ownership model: all storage (DrakenStringArena struct, slot array, arena
// bytes, validity bitmap) lives in one mimalloc block.  data_buf owns the
// block; freeing it releases everything.  validity_buf is always nullptr for
// strings because validity is embedded in the block.
//
// Slot format (see core/string_slot.h):
//   short (len ≤ 12): inline bytes, zero-padded.
//   long  (len > 12): big-endian prefix + XXH3 hash32 + u32 arena_offset.
// ---------------------------------------------------------------------------

static VectorOwner make_string_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    // --- Pass 1: collect UTF-8 views, count extern bytes, detect nulls -------
    std::vector<const char*> ptrs(length, nullptr);
    std::vector<Py_ssize_t>  lens(length, 0);
    size_t total_extern = 0;
    bool   has_nulls    = false;

    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            has_nulls = true;
        } else {
            PyObject* pystr = obj.ptr();
            if (!PyUnicode_Check(pystr))
                throw std::invalid_argument(
                    "vector_from_string_sequence: element is not str or None");
            Py_ssize_t slen = 0;
            const char* utf8 = PyUnicode_AsUTF8AndSize(pystr, &slen);
            if (!utf8) throw nb::python_error();
            ptrs[i] = utf8;
            lens[i] = slen;
            if (slen > STR_INLINE_MAX)
                total_extern += static_cast<size_t>(slen);
        }
    }

    // Guard: arena offsets are u32 → 4 GB cap per vector.
    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error(
            "vector_from_string_sequence: total arena bytes exceed 4 GB limit");

    // --- Compute single-block layout -----------------------------------------
    // Block: [DrakenStringArena | DrakenStringSlot[length] | arena_bytes | validity]
    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes  = (length > 0u ? length : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start  = struct_end + slots_bytes;

    size_t validity_start = arena_start + total_extern;
    size_t validity_bytes = 0u;
    if (has_nulls) {
        const uint32_t bm = (length + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity_bytes = padded > 0u ? padded : 8u;
    }
    const size_t total_alloc = validity_start + validity_bytes;

    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena)));
    if (!block) throw std::bad_alloc();
    // Zero the whole block: ensures inline slot padding and validity bits are clean.
    std::memset(block, 0, total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena));
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa     = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots  = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena  = (total_extern > 0u) ? (block + arena_start) : nullptr;
    uint8_t*           bitmap = has_nulls           ? (block + validity_start) : nullptr;

    sa->slots       = slots;
    sa->arena       = arena;
    sa->length      = length;
    sa->arena_used  = 0u;
    sa->arena_cap   = total_extern;
    sa->null_bitmap = nullptr;  // set below after bitmap is initialised
    sa->owns_buffers = 0;
    sa->type        = DRAKEN_VARCHAR;

    // All-valid starting state; null rows clear their bit in pass 2.
    if (has_nulls) {
        std::memset(bitmap, 0xFF, validity_bytes);
        sa->null_bitmap = bitmap;
    }

    // --- Pass 2: fill slots and arena ----------------------------------------
    for (uint32_t i = 0; i < length; ++i) {
        if (ptrs[i] == nullptr) {
            // null row — slot is already zeroed; clear validity bit.
            if (bitmap)
                bitmap[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        } else {
            const uint8_t* src  = reinterpret_cast<const uint8_t*>(ptrs[i]);
            const uint32_t slen = static_cast<uint32_t>(lens[i]);
            if (slen <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], src, slen);
            } else {
                // Hard check before casting to u32 (total already checked above,
                // but arena_used must stay representable as u32 per slot).
                if (sa->arena_used > static_cast<size_t>(UINT32_MAX))
                    throw std::overflow_error(
                        "vector_from_string_sequence: arena offset overflow");
                const uint32_t off = static_cast<uint32_t>(sa->arena_used);
                std::memcpy(arena + off, src, slen);
                str_init_extern(&slots[i], src, slen,
                                (uint32_t)XXH3_64bits(src, slen), off);
                sa->arena_used += slen;
            }
        }
    }

    DrakenVector v = draken_vector_from_dense(sa, length, DRAKEN_VARCHAR, bitmap);
    // validity_buf is nullptr: validity bitmap is embedded in data_buf's block.
    return VectorOwner(v, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
}

// ---------------------------------------------------------------------------
// D.3: string dict ingestion — Python list[str | None] → dict DRAKEN_VARCHAR vector.
//
// Deduplicates the input sequence into unique slots (in first-appearance order).
// Equal values share one slot; dedup uses exact sg_eq_slots semantics, with
// length/prefix/hash32 as fast negative filters before arena-byte verification.
//
// Ownership:
//   data_buf   = single block [DrakenStringArena | unique_slots | arena_bytes].
//   codes_buf  = owned uint32_t codes[length].
//   validity   = separate allocation (nullptr if all-valid).
//
// The resulting DrakenVector has:
//   data_length == # unique non-null values (≥ 1 for non-empty non-all-null input).
//   length      == len(seq).
//   selection   == owned codes (one per logical row).
//   validity    == nullptr (all-valid) or owned bitmap.
//
// All-null / empty sequences return a constant-shape vector (data_length=1).
// ---------------------------------------------------------------------------

static VectorOwner make_string_dict_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    // --- Pass 1: collect UTF-8 views and detect nulls -----------------------
    std::vector<const char*> ptrs(length, nullptr);
    std::vector<Py_ssize_t>  lens(length, 0);
    bool has_nulls = false;

    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            has_nulls = true;
        } else {
            PyObject* pystr = obj.ptr();
            if (!PyUnicode_Check(pystr))
                throw std::invalid_argument(
                    "vector_from_string_dict_sequence: element is not str or None");
            Py_ssize_t slen = 0;
            const char* utf8 = PyUnicode_AsUTF8AndSize(pystr, &slen);
            if (!utf8) throw nb::python_error();
            ptrs[i] = utf8;
            lens[i] = slen;
        }
    }

    // --- Pass 2: dedup non-null values using sg_eq_slots semantics ----------
    // Build temporary slots for dedup (arena_offset=0 for long; same as D.1).
    // Hash key = str_hash_seed; equality = sg_eq_slots exact verification.
    std::unordered_map<uint64_t, std::vector<uint32_t>> dedup_map;
    std::vector<DrakenStringSlot> uniq_slots;   // unique slot for each group
    std::vector<const char*>      uniq_ptrs;    // source UTF-8 pointer per unique
    std::vector<uint32_t>         uniq_lens_u;  // UTF-8 byte length per unique
    std::vector<uint32_t>         codes(length, 0u);

    for (uint32_t i = 0; i < length; ++i) {
        if (ptrs[i] == nullptr) continue;  // null row

        const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(ptrs[i]);
        const uint32_t ulen   = static_cast<uint32_t>(lens[i]);

        // Build temporary slot with arena_offset=0 (not yet placed in output arena).
        DrakenStringSlot tmp_slot;
        if (ulen <= STR_INLINE_MAX) {
            str_init_inline(&tmp_slot, ubytes, ulen);
        } else {
            str_init_extern(&tmp_slot, ubytes, ulen,
                            (uint32_t)XXH3_64bits(ubytes, ulen), 0u);
        }

        const uint64_t hseed = draken::ops::str_hash_seed(&tmp_slot, ubytes);

        bool found = false;
        auto it = dedup_map.find(hseed);
        if (it != dedup_map.end()) {
            for (uint32_t uidx : it->second) {
                // Long temporary slots use arena_offset=0; their source UTF-8
                // pointers are the arena bases for exact candidate verification.
                if (draken::ops::sg_eq_slots(
                        &uniq_slots[uidx],
                        reinterpret_cast<const uint8_t*>(uniq_ptrs[uidx]),
                        &tmp_slot,
                        ubytes)) {
                    codes[i] = uidx;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            const uint32_t new_idx = static_cast<uint32_t>(uniq_slots.size());
            uniq_slots.push_back(tmp_slot);
            uniq_ptrs.push_back(ptrs[i]);
            uniq_lens_u.push_back(ulen);
            codes[i] = new_idx;
            dedup_map[hseed].push_back(new_idx);
        }
    }

    const uint32_t dict_size = static_cast<uint32_t>(uniq_slots.size());

    // --- Compute output arena bytes for unique long strings ------------------
    size_t total_extern = 0u;
    std::vector<uint32_t> uniq_arena_off(dict_size, 0u);
    for (uint32_t k = 0; k < dict_size; ++k) {
        if (!str_is_inline(&uniq_slots[k])) {
            uniq_arena_off[k] = static_cast<uint32_t>(total_extern);
            total_extern     += uniq_lens_u[k];
        }
    }
    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error(
            "vector_from_string_dict_sequence: arena exceeds 4 GB");

    // --- Allocate data block, codes buffer, and optional validity ------------
    // Layout: [DrakenStringArena | unique_slots[dict_size_or_1] | arena_bytes].
    constexpr size_t kAlign = alignof(DrakenStringSlot);
    const uint32_t eff_dict = (dict_size > 0u) ? dict_size : 1u;
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kAlign - 1u) & ~(kAlign - 1u);
    const size_t slots_sz  = static_cast<size_t>(eff_dict) * sizeof(DrakenStringSlot);
    const size_t arena_off = struct_end + slots_sz;
    const size_t total     = arena_off + total_extern;
    const size_t alloc     = total > 0u ? total : sizeof(DrakenStringArena);

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, alloc);
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa    = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena_bytes =
        (total_extern > 0u) ? (block + arena_off) : nullptr;

    // Allocate codes buffer.
    const size_t codes_bytes = static_cast<size_t>(length > 0u ? length : 1u)
                               * sizeof(uint32_t);
    uint32_t* out_codes = static_cast<uint32_t*>(draken_malloc(codes_bytes));
    if (!out_codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_buf(out_codes);

    // Allocate validity (separate; may be nullptr if all-valid).
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls && length > 0u) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);  // all valid; nulls cleared below
        for (uint32_t i = 0; i < length; ++i) {
            if (ptrs[i] == nullptr)
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    // --- Fill unique slots + arena bytes ------------------------------------
    for (uint32_t k = 0; k < dict_size; ++k) {
        const DrakenStringSlot& ts = uniq_slots[k];
        if (str_is_inline(&ts)) {
            slots[k] = ts;
        } else {
            const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(uniq_ptrs[k]);
            slots[k].ext.length       = ts.ext.length;
            slots[k].ext.prefix       = ts.ext.prefix;
            slots[k].ext.hash32       = ts.ext.hash32;
            slots[k].ext.arena_offset = uniq_arena_off[k];
            if (arena_bytes != nullptr)
                std::memcpy(arena_bytes + uniq_arena_off[k], ubytes, uniq_lens_u[k]);
        }
    }

    // --- Fill codes array ---------------------------------------------------
    if (length > 0u)
        std::memcpy(out_codes, codes.data(), length * sizeof(uint32_t));

    // --- Initialize DrakenStringArena --------------------------------------
    sa->slots        = slots;
    sa->arena        = arena_bytes;
    sa->length       = eff_dict;
    sa->arena_used   = total_extern;
    sa->arena_cap    = total_extern;
    sa->null_bitmap  = validity;
    sa->owns_buffers = 0;
    sa->type         = DRAKEN_VARCHAR;

    // --- Build DrakenVector and VectorOwner ---------------------------------
    // All-null or empty: constant shape (data_length=1, zero selection).
    // Otherwise: dict shape (data_length=dict_size, owned codes).
    DrakenVector vec;
    if (dict_size == 0u && length > 0u) {
        // All-null: constant-shape with one dummy zero slot, all rows null.
        vec = draken_vector_from_constant(sa, length, DRAKEN_VARCHAR, validity);
    } else if (dict_size == 0u) {
        // Empty: dense-identity with data_length=0.
        vec = draken_vector_from_dense(sa, 0u, DRAKEN_VARCHAR, nullptr);
    } else {
        vec = draken_vector_from_dict(sa, dict_size, out_codes, length,
                                      DRAKEN_VARCHAR, validity);
    }

    return VectorOwner(vec, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_buf));
}

// ---------------------------------------------------------------------------
// D.5: bool ingestion — Python list[bool | None] → dense DRAKEN_BOOL vector.
//
// Data layout: bit-packed, 1 bit/row, LSB-first within each byte.
// Value bit at logical row i lives at byte i>>3, bit i&7.
// Null rows have their value bit set to 0 (don't-care).
// validity==nullptr when no nulls (normalization invariant).
// ---------------------------------------------------------------------------

static VectorOwner make_bool_from_sequence(nb::list seq) {
    const uint32_t n      = static_cast<uint32_t>(seq.size());
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   alloc  = (padded > 0u) ? static_cast<size_t>(padded) : 8u;

    uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, alloc);
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < n; ++i) {
        nb::object obj = seq[static_cast<Py_ssize_t>(i)];
        if (obj.is_none()) {
            has_nulls = true;
        } else {
            if (nb::cast<bool>(obj))
                data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (has_nulls) {
        validity = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, alloc);
        for (uint32_t i = 0u; i < n; ++i) {
            if (seq[static_cast<Py_ssize_t>(i)].is_none())
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
        }
        // Mask tail bits past n to 0 so they don't look valid.
        if ((n & 7u) != 0u && bm > 0u)
            validity[bm - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
    }

    DrakenVector v = draken_vector_from_dense(data, n, DRAKEN_BOOL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

// D.5: constant-shape bool vector.
// value_obj may be None → all-null constant.
static VectorOwner make_bool_constant(nb::object value_obj, uint32_t length) {
    const bool is_null   = value_obj.is_none();
    const bool bool_val  = is_null ? false : nb::cast<bool>(value_obj);

    // One byte of data; bit 0 holds the constant value (selection[i] == 0 always).
    const size_t data_alloc = 8u;  // SIMD-padded minimum
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(data_alloc));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, data_alloc);
    if (bool_val) data[0] = 0x01u;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (is_null) {
        const uint32_t bm     = (length + 7u) >> 3;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   alloc  = (padded > 0u) ? static_cast<size_t>(padded) : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, alloc);  // all null
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_BOOL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

// D.5: dict-encoded bool vector.
// values_seq: list of bool/None unique values (the dictionary, packed as bits).
// codes_seq:  list of int (uint32 code per logical row, indexing into values).
// nullable_seq: optional list of bool (True=valid) for the logical-row validity bitmap.
static VectorOwner make_bool_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    // Bit-pack the dictionary values. data_length == dict_size.
    const uint32_t dict_bm = (dict_size + 7u) >> 3;
    const size_t   data_alloc = (dict_bm > 0u) ? static_cast<size_t>(dict_bm) : 8u;
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(data_alloc));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, data_alloc);
    OwnedBuffer<void> data_buf(data);

    for (uint32_t k = 0u; k < dict_size; ++k) {
        nb::object obj = values_seq[static_cast<Py_ssize_t>(k)];
        if (!obj.is_none() && nb::cast<bool>(obj))
            data[k >> 3] |= static_cast<uint8_t>(1u << (k & 7u));
    }

    // Allocate codes buffer.
    const size_t codes_bytes = static_cast<size_t>(length > 0u ? length : 1u) * sizeof(uint32_t);
    uint32_t* codes = static_cast<uint32_t*>(draken_malloc(codes_bytes));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0u; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[static_cast<Py_ssize_t>(i)]);

    // Optional validity bitmap (logical-row granularity).
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;

    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) >> 3;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   alloc  = (padded > 0u) ? static_cast<size_t>(padded) : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(alloc));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, alloc);
        for (uint32_t i = 0u; i < length; ++i) {
            if (!nb::cast<bool>(null_list[static_cast<Py_ssize_t>(i)]))
                validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
        }
    }

    DrakenVector v = draken_vector_from_dict(
        data, dict_size, codes, length, DRAKEN_BOOL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.6: narrow integer ingestion — int8 / int16 / int32
// Fail loud (std::overflow_error) on any value outside the type's range.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static VectorOwner make_narrow_int_from_sequence(nb::list seq, const char* type_name) {
    const int64_t TMIN = static_cast<int64_t>(std::numeric_limits<T>::min());
    const int64_t TMAX = static_cast<int64_t>(std::numeric_limits<T>::max());
    const uint32_t length = static_cast<uint32_t>(seq.size());

    T* data = static_cast<T*>(draken_malloc((length > 0u ? length : 1u) * sizeof(T)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = T(0);
            has_nulls = true;
        } else {
            const int64_t val = nb::cast<int64_t>(obj);
            if (val < TMIN || val > TMAX)
                throw std::overflow_error(
                    std::string(type_name) + ": value out of range");
            data[i] = static_cast<T>(val);
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[i].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

template<typename T, DrakenType TAG>
static VectorOwner make_narrow_int_constant(nb::object value_obj, uint32_t length,
                                             const char* type_name) {
    const bool is_null = value_obj.is_none();
    T scalar = T(0);
    if (!is_null) {
        const int64_t val  = nb::cast<int64_t>(value_obj);
        const int64_t TMIN = static_cast<int64_t>(std::numeric_limits<T>::min());
        const int64_t TMAX = static_cast<int64_t>(std::numeric_limits<T>::max());
        if (val < TMIN || val > TMAX)
            throw std::overflow_error(std::string(type_name) + ": value out of range");
        scalar = static_cast<T>(val);
    }

    T* data = static_cast<T*>(draken_malloc(sizeof(T)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

template<typename T, DrakenType TAG>
static VectorOwner make_narrow_int_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq,
    const char* type_name)
{
    const int64_t TMIN    = static_cast<int64_t>(std::numeric_limits<T>::min());
    const int64_t TMAX    = static_cast<int64_t>(std::numeric_limits<T>::max());
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    T* dict_data = static_cast<T*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(T)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        const int64_t val = nb::cast<int64_t>(values_seq[k]);
        if (val < TMIN || val > TMAX)
            throw std::overflow_error(std::string(type_name) + ": dict value out of range");
        dict_data[k] = static_cast<T>(val);
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.7: float ingestion — Python list[float | None] → dense FLOAT32 / FLOAT64 vector.
//
// Canonicalization is applied at ingestion via fp_canon<T>:
//   -0.0 → +0.0;  any NaN bit-pattern → canonical quiet NaN.
// This ensures hash and equality are consistent with arithmetic results.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static VectorOwner make_float_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    T* data = static_cast<T*>(draken_malloc((length > 0u ? length : 1u) * sizeof(T)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = T(0);
            has_nulls = true;
        } else {
            data[i] = draken::ops::fp_canon(static_cast<T>(nb::cast<double>(obj)));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[i].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

template<typename T, DrakenType TAG>
static VectorOwner make_float_constant(nb::object value_obj, uint32_t length) {
    const bool is_null = value_obj.is_none();
    const T scalar = is_null ? T(0)
        : draken::ops::fp_canon(static_cast<T>(nb::cast<double>(value_obj)));

    T* data = static_cast<T*>(draken_malloc(sizeof(T)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

template<typename T, DrakenType TAG>
static VectorOwner make_float_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    T* dict_data = static_cast<T*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(T)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k)
        dict_data[k] = draken::ops::fp_canon(
            static_cast<T>(nb::cast<double>(values_seq[k])));

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, TAG, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.8: timestamp unit helpers
// ---------------------------------------------------------------------------

static inline const char* unit_to_str(TimestampUnit u) noexcept {
    switch (u) {
        case TimestampUnit::SECONDS:      return "s";
        case TimestampUnit::MILLISECONDS: return "ms";
        case TimestampUnit::MICROSECONDS: return "us";
        case TimestampUnit::NANOSECONDS:  return "ns";
    }
    return "us";
}

static inline TimestampUnit str_to_unit(const std::string& s) {
    if (s == "s")  return TimestampUnit::SECONDS;
    if (s == "ms") return TimestampUnit::MILLISECONDS;
    if (s == "us") return TimestampUnit::MICROSECONDS;
    if (s == "ns") return TimestampUnit::NANOSECONDS;
    throw std::invalid_argument(
        "timestamp unit must be \"s\", \"ms\", \"us\", or \"ns\"; got: " + s);
}

// Convert a Python datetime.datetime object to a UTC instant in the column's unit.
// Timezone-aware datetimes are converted to UTC by subtracting utcoffset().
// Timezone-naive datetimes are treated as UTC (no offset applied).
// Raises if obj is not a datetime.datetime instance.
static int64_t py_datetime_to_instant(nb::object obj, TimestampUnit unit) {
    PyObject* dt = obj.ptr();
    if (!PyDateTime_Check(dt))
        throw std::invalid_argument(
            "timestamp sequence: element must be datetime.datetime or None");

    int year   = PyDateTime_GET_YEAR(dt);
    int month  = PyDateTime_GET_MONTH(dt);
    int day    = PyDateTime_GET_DAY(dt);
    int hour   = PyDateTime_DATE_GET_HOUR(dt);
    int minute = PyDateTime_DATE_GET_MINUTE(dt);
    int second = PyDateTime_DATE_GET_SECOND(dt);
    int usec   = PyDateTime_DATE_GET_MICROSECOND(dt);

    int64_t us_epoch = parts_to_us_epoch(year, month, day, hour, minute, second, usec);

    // Subtract UTC offset if timezone-aware.
    PyObject* tzinfo = PyDateTime_DATE_GET_TZINFO(dt);  // borrowed reference
    if (tzinfo && tzinfo != Py_None) {
        nb::object off = obj.attr("utcoffset")();
        if (!off.is_none()) {
            // timedelta attributes: days (int), seconds (int), microseconds (int).
            int off_days  = nb::cast<int>(off.attr("days"));
            int off_secs  = nb::cast<int>(off.attr("seconds"));
            int off_usecs = nb::cast<int>(off.attr("microseconds"));
            int64_t off_us = static_cast<int64_t>(off_days)  * 86400000000LL
                           + static_cast<int64_t>(off_secs)  * 1000000LL
                           + static_cast<int64_t>(off_usecs);
            us_epoch -= off_us;  // convert local → UTC
        }
    }

    return us_to_ts(us_epoch, unit);
}

// Convert a UTC instant (raw int64 in the column's unit) back to a Python
// datetime.datetime with the timezone derived from the logical descriptor's
// offset_minutes.  offset_minutes == 0 → UTC.
// Raises if lt is nullptr (mandatory descriptor for TIMESTAMP64).
static nb::object instant_to_py_datetime(int64_t raw, const LogicalType* lt) {
    if (!lt)
        throw std::invalid_argument(
            "TIMESTAMP64 vector is missing its logical-type descriptor; "
            "this is a hard error — use the vector_timestamp_from_* factories");

    // Step 1: convert raw value → UTC microseconds since epoch.
    int64_t utc_us = ts_to_us(raw, lt->unit);

    // Step 2: apply stored offset → local microseconds for calendar decomposition.
    int64_t local_us = utc_us + static_cast<int64_t>(lt->offset_minutes) * 60LL * 1000000LL;

    // Step 3: decompose to calendar parts.
    int y, mo, d, h, mi, s, us;
    us_epoch_to_parts(local_us, y, mo, d, h, mi, s, us);

    // Step 4: build timezone object.
    PyObject* tz;
    PyObject* owned_tz = nullptr;  // only set (and freed) for non-UTC offsets
    if (lt->offset_minutes == 0) {
        tz = PyDateTime_TimeZone_UTC;  // borrowed singleton, not freed
    } else {
        // PyDelta_FromDSU normalises negative inputs correctly.
        int offset_secs = static_cast<int>(lt->offset_minutes) * 60;
        PyObject* offset_td = PyDelta_FromDSU(0, offset_secs, 0);
        if (!offset_td) throw nb::python_error();
        owned_tz = PyTimeZone_FromOffset(offset_td);
        Py_DECREF(offset_td);
        if (!owned_tz) throw nb::python_error();
        tz = owned_tz;
    }

    PyObject* result = PyDateTimeAPI->DateTime_FromDateAndTime(
        y, mo, d, h, mi, s, us, tz, PyDateTimeAPI->DateTimeType);

    if (owned_tz) Py_DECREF(owned_tz);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// D.8: timestamp ingestion factories
// ---------------------------------------------------------------------------

// Dense TIMESTAMP64 vector from a Python list[datetime | None].
static VectorOwner make_timestamp_from_sequence(
    nb::list seq, TimestampUnit unit, int16_t offset_minutes)
{
    const uint32_t length = static_cast<uint32_t>(seq.size());

    const size_t data_bytes = (length > 0 ? length : 1u) * sizeof(int64_t);
    int64_t* data = static_cast<int64_t*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = 0;
            has_nulls = true;
        } else {
            data[i] = py_datetime_to_instant(obj, unit);
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[i].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_TIMESTAMP64, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind = LogicalKind::TIMESTAMP;
    lt.unit = unit;
    lt.offset_minutes = offset_minutes;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// Constant-shape TIMESTAMP64 vector (single value, length rows).
static VectorOwner make_timestamp_constant(
    nb::object value_obj, uint32_t length,
    TimestampUnit unit, int16_t offset_minutes)
{
    const bool is_null = value_obj.is_none();
    const int64_t scalar = is_null ? 0LL : py_datetime_to_instant(value_obj, unit);

    int64_t* data = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3u) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_TIMESTAMP64, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind = LogicalKind::TIMESTAMP;
    lt.unit = unit;
    lt.offset_minutes = offset_minutes;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// Dict-encoded TIMESTAMP64 vector.
// values_seq: Python list[datetime | None] — unique dictionary entries.
// codes_seq:  Python list[int] — uint32 code per logical row.
// nullable_seq: optional list[bool] (True=valid); omit for all-valid.
static VectorOwner make_timestamp_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq,
    TimestampUnit unit, int16_t offset_minutes)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    int64_t* dict_data = static_cast<int64_t*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(int64_t)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        nb::object obj = values_seq[k];
        if (obj.is_none()) {
            dict_data[k] = 0;
        } else {
            dict_data[k] = py_datetime_to_instant(obj, unit);
        }
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, DRAKEN_TIMESTAMP64, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf),
                      std::move(codes_owned));

    LogicalType lt;
    lt.kind = LogicalKind::TIMESTAMP;
    lt.unit = unit;
    lt.offset_minutes = offset_minutes;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// ---------------------------------------------------------------------------
// D.9: date32 helpers — Python date ↔ int32 days since 1970-01-01
// ---------------------------------------------------------------------------

// Python date/datetime → int32 days since Unix epoch.
// Accepts datetime.date and datetime.datetime (subclass); truncates time part.
static inline int32_t py_date_to_days(PyObject* d) {
    if (!PyDate_Check(d))
        throw std::invalid_argument(
            "date32: element must be datetime.date or None");
    const int y   = PyDateTime_GET_YEAR(d);
    const int mo  = PyDateTime_GET_MONTH(d);
    const int day = PyDateTime_GET_DAY(d);
    // parts_to_us_epoch returns exact microseconds at midnight; divide out.
    const int64_t us = parts_to_us_epoch(y, mo, day, 0, 0, 0, 0);
    return static_cast<int32_t>(us / 86400000000LL);
}

// int32 days since Unix epoch → Python datetime.date.
static nb::object days_to_py_date(int32_t days) {
    int y, mo, d, h, mi, s, us;
    us_epoch_to_parts(static_cast<int64_t>(days) * 86400000000LL,
                      y, mo, d, h, mi, s, us);
    PyObject* result = PyDate_FromDate(y, mo, d);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// D.9: time helpers — Python time ↔ int32/int64 time-of-day in column unit
// ---------------------------------------------------------------------------

// Python datetime.time → raw integer in the given unit.
static inline int64_t py_time_to_raw(PyObject* t, TimestampUnit unit) {
    if (!PyTime_Check(t))
        throw std::invalid_argument(
            "time: element must be datetime.time or None");
    const int h  = PyDateTime_TIME_GET_HOUR(t);
    const int mi = PyDateTime_TIME_GET_MINUTE(t);
    const int s  = PyDateTime_TIME_GET_SECOND(t);
    const int us = PyDateTime_TIME_GET_MICROSECOND(t);
    const int64_t total_us = static_cast<int64_t>(h)  * 3600000000LL
                           + static_cast<int64_t>(mi) * 60000000LL
                           + static_cast<int64_t>(s)  * 1000000LL
                           + us;
    return us_to_ts(total_us, unit);
}

// Raw integer in the given unit → Python datetime.time (naive).
static nb::object raw_to_py_time(int64_t raw, TimestampUnit unit) {
    int64_t rem = ts_to_us(raw, unit);
    const int h  = static_cast<int>(rem / 3600000000LL); rem %= 3600000000LL;
    const int mi = static_cast<int>(rem / 60000000LL);   rem %= 60000000LL;
    const int s  = static_cast<int>(rem / 1000000LL);    rem %= 1000000LL;
    const int us = static_cast<int>(rem);
    PyObject* result = PyDateTimeAPI->Time_FromTime(
        h, mi, s, us, Py_None, PyDateTimeAPI->TimeType);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// D.9: date32 ingestion factories (no logical descriptor)
// ---------------------------------------------------------------------------

static VectorOwner make_date32_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());
    int32_t* data = static_cast<int32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(int32_t)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = 0;
            has_nulls = true;
        } else {
            data[i] = py_date_to_days(obj.ptr());
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[i].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_DATE32, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

static VectorOwner make_date32_constant(nb::object value_obj, uint32_t length) {
    const bool is_null = value_obj.is_none();
    const int32_t scalar = is_null ? 0 : py_date_to_days(value_obj.ptr());

    int32_t* data = static_cast<int32_t*>(draken_malloc(sizeof(int32_t)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_DATE32, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

static VectorOwner make_date32_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    int32_t* dict_data = static_cast<int32_t*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(int32_t)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        nb::object obj = values_seq[k];
        dict_data[k] = obj.is_none() ? 0 : py_date_to_days(obj.ptr());
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, DRAKEN_DATE32, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.9: time ingestion factories (mandatory logical descriptor, unit only)
// T = int32_t (TIME32) or int64_t (TIME64); TAG = corresponding DrakenType.
// ---------------------------------------------------------------------------

template<typename T, DrakenType TAG>
static VectorOwner make_time_from_sequence(nb::list seq, TimestampUnit unit) {
    const uint32_t length = static_cast<uint32_t>(seq.size());
    T* data = static_cast<T*>(draken_malloc((length > 0u ? length : 1u) * sizeof(T)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            data[i] = T(0);
            has_nulls = true;
        } else {
            data[i] = static_cast<T>(py_time_to_raw(obj.ptr(), unit));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[i].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, TAG, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));
    LogicalType lt; lt.kind = LogicalKind::TIME; lt.unit = unit; lt.offset_minutes = 0;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

template<typename T, DrakenType TAG>
static VectorOwner make_time_constant(nb::object value_obj, uint32_t length,
                                      TimestampUnit unit) {
    const bool is_null = value_obj.is_none();
    const T scalar = is_null ? T(0)
        : static_cast<T>(py_time_to_raw(value_obj.ptr(), unit));

    T* data = static_cast<T*>(draken_malloc(sizeof(T)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3u) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, TAG, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));
    LogicalType lt; lt.kind = LogicalKind::TIME; lt.unit = unit; lt.offset_minutes = 0;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

template<typename T, DrakenType TAG>
static VectorOwner make_time_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq,
    TimestampUnit unit)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    T* dict_data = static_cast<T*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(T)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        nb::object obj = values_seq[k];
        dict_data[k] = obj.is_none() ? T(0)
            : static_cast<T>(py_time_to_raw(obj.ptr(), unit));
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[i]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[i]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, TAG, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf),
                      std::move(codes_owned));
    LogicalType lt; lt.kind = LogicalKind::TIME; lt.unit = unit; lt.offset_minutes = 0;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// ---------------------------------------------------------------------------
// Row validity helper — defined here so D.11 helpers below can use it.
// (The readback block at ~line 1800 repeats a note but C++ resolves to this
// single definition.)
// ---------------------------------------------------------------------------
static inline bool row_is_valid(const DrakenVector& v, uint32_t i) noexcept {
    if (v.validity == nullptr) return true;
    return static_cast<bool>((v.validity[i / 8u] >> (i % 8u)) & 1u);
}

// ---------------------------------------------------------------------------
// D.11: null vector — self-describing, no data buffer, no validity buffer.
// All rows are null; type tag is the sole signal — short-circuit on type==NULL.
// ---------------------------------------------------------------------------
static VectorOwner make_null_vector(uint32_t length) {
    DrakenVector v;
    v.data        = nullptr;
    v.selection   = draken_zero_sel(length > 0u ? length : 1u);
    v.data_length = 0u;
    v.length      = length;
    v.validity    = nullptr;
    v.type        = DRAKEN_NULL;
    v.flags       = 0u;
    return VectorOwner(v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr));
}

// D.11: build an all-null DRAKEN_BOOL result vector (TVL: any comparison with null).
static VectorOwner make_all_null_bool(uint32_t n) {
    uint8_t* data_b = static_cast<uint8_t*>(draken_malloc(8u));
    if (!data_b) throw std::bad_alloc();
    std::memset(data_b, 0u, 8u);
    OwnedBuffer<void> data_buf(data_b);
    const uint32_t bm     = (n + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!validity) throw std::bad_alloc();
    std::memset(validity, 0x00u, vbytes);
    OwnedBuffer<uint8_t> validity_buf(validity);
    DrakenVector vr = draken_vector_from_dense(data_b, n, DRAKEN_BOOL, validity);
    return VectorOwner(vr, std::move(data_buf), std::move(validity_buf));
}

// ---------------------------------------------------------------------------
// D.11: fp16 descriptor helpers.
// ---------------------------------------------------------------------------
static void require_fp16_descriptor(const VectorOwner& v, const char* ctx) {
    if (!v.logical_type || v.logical_type->kind != LogicalKind::VECTOR
            || v.logical_type->dimension == 0u)
        throw std::invalid_argument(
            std::string(ctx) +
            ": VECTOR_FP16 requires a logical-type descriptor with dimension >= 1");
}

// FNV-1a seed over the 2*dim raw fp16 bytes for a single row.
// Passed through simd_hash_i64 at the call site for distribution consistency.
static inline uint64_t fp16_row_fnv_seed(const uint16_t* fp16_data, uint32_t dim) {
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(fp16_data);
    uint64_t h = 14695981039346656037ULL;
    const uint32_t nbytes = dim * 2u;
    for (uint32_t k = 0u; k < nbytes; ++k) {
        h ^= static_cast<uint64_t>(bytes[k]);
        h *= 1099511628211ULL;
    }
    return h;
}

// ---------------------------------------------------------------------------
// D.11: fp16 ingestion — Python list[list[float] | None] → dense VECTOR_FP16 vector.
// dimension: number of fp16 values per row (mandatory, ≥ 1).
// Each non-null row must have exactly dimension floats; raises loud on mismatch.
// None → null row. Conversion: float → fp16 via IEEE 754 round-to-nearest.
// ---------------------------------------------------------------------------
static VectorOwner make_fp16_from_sequence(nb::list seq, uint32_t dimension) {
    if (dimension == 0u)
        throw std::invalid_argument(
            "vector_fp16_from_sequence: dimension must be >= 1");

    const uint32_t length = static_cast<uint32_t>(seq.size());

    // Each row occupies `dimension` uint16_t values in the flat data array.
    const size_t data_bytes = static_cast<size_t>(length > 0u ? length : 1u)
                              * dimension * sizeof(uint16_t);
    uint16_t* data = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, data_bytes);
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < length; ++i) {
        nb::object row_obj = seq[static_cast<Py_ssize_t>(i)];
        if (row_obj.is_none()) {
            has_nulls = true;
            // data row already zeroed
        } else {
            nb::list row = nb::cast<nb::list>(row_obj);
            const uint32_t row_len = static_cast<uint32_t>(row.size());
            if (row_len != dimension)
                throw std::invalid_argument(
                    "vector_fp16_from_sequence: row length " +
                    std::to_string(row_len) + " != dimension " +
                    std::to_string(dimension));
            uint16_t* dst = data + static_cast<size_t>(i) * dimension;
            for (uint32_t k = 0u; k < dimension; ++k) {
                const float f = static_cast<float>(
                    nb::cast<double>(row[static_cast<Py_ssize_t>(k)]));
                dst[k] = fp16_ieee_from_fp32_value(f);
            }
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < length; ++i) {
            if (seq[static_cast<Py_ssize_t>(i)].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_VECTOR_FP16, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind      = LogicalKind::VECTOR;
    lt.dimension = dimension;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// D.11: fp16 take — gather rows by index list, producing a dense output vector.
static VectorOwner make_fp16_take(const VectorOwner& v,
                                  const int32_t* indices, uint32_t n) {
    require_fp16_descriptor(v, "take");
    const uint32_t dim = v.logical_type->dimension;
    const uint16_t* src = static_cast<const uint16_t*>(v.vec.data);

    const size_t data_bytes = static_cast<size_t>(n > 0u ? n : 1u)
                              * dim * sizeof(uint16_t);
    uint16_t* dst = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();
    std::memset(dst, 0, data_bytes);
    OwnedBuffer<void> data_buf(dst);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        const int32_t vlen = static_cast<int32_t>(v.vec.length);
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw nb::index_error("take: index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;  // data row zeroed by memset
        } else {
            std::memcpy(dst + static_cast<size_t>(i) * dim,
                        src + v.vec.selection[static_cast<uint32_t>(idx)]
                              * static_cast<size_t>(dim),
                        dim * sizeof(uint16_t));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            int32_t idx = indices[i];
            const int32_t vlen = static_cast<int32_t>(v.vec.length);
            if (idx < 0) idx += vlen;
            if (!row_is_valid(v.vec, static_cast<uint32_t>(idx)))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    DrakenVector vr = draken_vector_from_dense(dst, n, DRAKEN_VECTOR_FP16, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.logical_type = v.logical_type;
    return owner;
}

// D.11: fp16 materialize — expand selection to a dense identity-selection vector.
// For the dense encoding we produce at ingestion, this is a full copy.
static VectorOwner make_fp16_materialize(const VectorOwner& v) {
    require_fp16_descriptor(v, "materialize");
    const uint32_t dim    = v.logical_type->dimension;
    const uint32_t length = v.vec.length;
    const uint16_t* src   = static_cast<const uint16_t*>(v.vec.data);

    const size_t data_bytes = static_cast<size_t>(length > 0u ? length : 1u)
                              * dim * sizeof(uint16_t);
    uint16_t* dst = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();
    std::memset(dst, 0, data_bytes);
    OwnedBuffer<void> data_buf(dst);

    for (uint32_t i = 0u; i < length; ++i) {
        if (row_is_valid(v.vec, i)) {
            std::memcpy(dst + static_cast<size_t>(i) * dim,
                        src + v.vec.selection[i] * static_cast<size_t>(dim),
                        dim * sizeof(uint16_t));
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (v.vec.validity != nullptr) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memcpy(validity, v.vec.validity, vbytes);
    }

    DrakenVector vr = draken_vector_from_dense(dst, length, DRAKEN_VECTOR_FP16, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.logical_type = v.logical_type;
    return owner;
}

// D.11: fp16 compress — keep only valid rows, producing a dense all-valid output.
static VectorOwner make_fp16_compress(const VectorOwner& v) {
    require_fp16_descriptor(v, "compress");
    const uint32_t dim    = v.logical_type->dimension;
    const uint32_t length = v.vec.length;
    const uint16_t* src   = static_cast<const uint16_t*>(v.vec.data);

    uint32_t valid_count = 0u;
    if (v.vec.validity == nullptr) {
        valid_count = length;  // all valid
    } else {
        for (uint32_t i = 0u; i < length; ++i)
            if (row_is_valid(v.vec, i)) ++valid_count;
    }

    const size_t data_bytes = static_cast<size_t>(valid_count > 0u ? valid_count : 1u)
                              * dim * sizeof(uint16_t);
    uint16_t* dst = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!dst) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dst);

    uint32_t out_idx = 0u;
    for (uint32_t i = 0u; i < length; ++i) {
        if (row_is_valid(v.vec, i)) {
            std::memcpy(dst + static_cast<size_t>(out_idx) * dim,
                        src + v.vec.selection[i] * static_cast<size_t>(dim),
                        dim * sizeof(uint16_t));
            ++out_idx;
        }
    }

    DrakenVector vr = draken_vector_from_dense(dst, valid_count, DRAKEN_VECTOR_FP16, nullptr);
    VectorOwner owner(vr, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
    owner.logical_type = v.logical_type;
    return owner;
}

// ---------------------------------------------------------------------------
// C.2: convert a VecResult into a VectorOwner, transferring ownership.
// Consumes r — do not use r after this call.
// ---------------------------------------------------------------------------
static VectorOwner vecresult_to_owner(VecResult r) {
    DrakenVector v;
    v.data        = r.data;
    v.selection   = r.selection;
    v.data_length = r.data_length;
    v.length      = r.length;
    v.validity    = r.validity;
    v.type        = r.type;
    v.flags       = r.flags;

    OwnedBuffer<void>    data_buf(r.data);
    OwnedBuffer<uint8_t> val_buf(r.validity);
    OwnedBuffer<void>    codes_buf(r.owns_selection
                                    ? const_cast<void*>(static_cast<const void*>(r.selection))
                                    : nullptr);
    return VectorOwner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
}

// ---------------------------------------------------------------------------
// E.1 bridge surface — draken_bridge.h implementations.
//
// All three functions are compiled into draken_native.so and declared in
// draken/core/draken_bridge.h so .pyx consumers can bind them via
//   cdef extern from "core/draken_bridge.h": ...
// ---------------------------------------------------------------------------

// draken_vector_unwrap — borrowed DrakenVector* from a Python Vector handle.
//
// Type-check is mandatory: non-Vector (including None) raises TypeError rather
// than segfaulting. The returned pointer is borrowed (caller keeps `obj` alive).
extern "C" const DrakenVector* draken_vector_unwrap(PyObject* obj) {
    if (!obj || obj == Py_None) {
        PyErr_SetString(PyExc_TypeError,
            "draken_vector_unwrap: expected draken.draken_native.Vector, got None");
        return nullptr;
    }
    nb::handle h(obj);
    if (!nb::isinstance<VectorOwner>(h)) {
        PyErr_Format(PyExc_TypeError,
            "draken_vector_unwrap: expected draken.draken_native.Vector, got %.100s",
            Py_TYPE(obj)->tp_name);
        return nullptr;
    }
    return &nb::inst_ptr<VectorOwner>(h)->vec;
}

// draken_array_child_unwrap — borrowed child DrakenVector* from a DRAKEN_ARRAY Vector.
//
// Fails loud on any bad input: TypeError for non-Vector, RuntimeError for non-array
// or absent child.  Caller MUST keep the parent `obj` alive while using the pointer.
extern "C" const DrakenVector* draken_array_child_unwrap(PyObject* obj) {
    if (!obj || obj == Py_None) {
        PyErr_SetString(PyExc_TypeError,
            "draken_array_child_unwrap: expected DRAKEN_ARRAY Vector, got None");
        return nullptr;
    }
    nb::handle h(obj);
    if (!nb::isinstance<VectorOwner>(h)) {
        PyErr_Format(PyExc_TypeError,
            "draken_array_child_unwrap: expected DRAKEN_ARRAY Vector, got %.100s",
            Py_TYPE(obj)->tp_name);
        return nullptr;
    }
    const VectorOwner* owner = nb::inst_ptr<VectorOwner>(h);
    if (owner->vec.type != DRAKEN_ARRAY) {
        PyErr_SetString(PyExc_TypeError,
            "draken_array_child_unwrap: vector is not DRAKEN_ARRAY type");
        return nullptr;
    }
    if (!owner->child_owner) {
        PyErr_SetString(PyExc_RuntimeError,
            "draken_array_child_unwrap: DRAKEN_ARRAY vector has no child");
        return nullptr;
    }
    return &owner->child_owner->vec;
}

// draken_vector_own_raw — wrap hand-allocated (draken_malloc) buffers in a new Vector.
//
// Creates a dense (identity-selection) Vector. data and validity ownership
// is transferred to the new VectorOwner; caller must not free them after.
// validity may be NULL (all-valid normalization invariant).
// Returns a NEW reference on success; NULL + exception on failure.
extern "C" PyObject* draken_vector_own_raw(
    void* data, uint8_t* validity, uint32_t length, DrakenType type)
{
    try {
        DrakenVector v = draken_vector_from_dense(data, length, type, validity);
        OwnedBuffer<void>    data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
        // obj destructor Py_DECREF's; net effect: one new reference returned.
    } catch (nb::python_error& e) {
        e.restore();
        return nullptr;
    } catch (std::bad_alloc&) {
        PyErr_NoMemory();
        return nullptr;
    } catch (std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// draken_vector_own_string — wrap hand-allocated string buffers in a string-family Vector.
//
// Canonical exit-point for C++ consumers that produce a new string column. Ownership of
// all three caller buffers (slots, arena, validity) is transferred unconditionally on
// entry — caller must NOT free them after this call, success or failure.
//
// type must be DRAKEN_VARCHAR, DRAKEN_NVARCHAR, or DRAKEN_VARBINARY; ValueError otherwise.
// Storage is identical across all three types (slot+arena); the type tag drives op semantics.
//
// Implementation consolidates slots + arena into a single block matching the layout of
// make_string_from_sequence (DrakenStringArena header || slots[] || arena_bytes) so that
// _slot_fields determinism with vector_from_string_sequence holds automatically. Validity
// stays as a separate OwnedBuffer (same pattern as dict-encoded string vectors).
extern "C" PyObject* draken_vector_own_string(
    DrakenStringSlot* slots,
    uint8_t*          arena,
    size_t            arena_len,
    uint8_t*          validity,
    uint32_t          length,
    DrakenType        type)
{
    // Step 1: take ownership of all three caller buffers immediately via RAII.
    // If any allocation fails below, destructors free them. If we succeed, we
    // release and free them manually after copying into the consolidated block.
    OwnedBuffer<void>    slots_guard(static_cast<void*>(slots));
    OwnedBuffer<void>    arena_guard(static_cast<void*>(arena));  // safe for nullptr
    OwnedBuffer<uint8_t> validity_guard(validity);                // safe for nullptr

    try {
        if (type != DRAKEN_VARCHAR && type != DRAKEN_NVARCHAR && type != DRAKEN_VARBINARY) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string: type must be DRAKEN_VARCHAR, "
                "DRAKEN_NVARCHAR, or DRAKEN_VARBINARY");
            return nullptr;
        }
        if (arena_len > 0u && !arena) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string: arena_len > 0 but arena is NULL");
            return nullptr;
        }
        if (length > 0u && !slots) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string: length > 0 but slots is NULL");
            return nullptr;
        }

        // Step 2: allocate consolidated block.
        // Layout: [DrakenStringArena | DrakenStringSlot[length] | arena_bytes]
        constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
        const size_t struct_end =
            (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
        const size_t slots_bytes = (length > 0u ? (size_t)length : 1u) * sizeof(DrakenStringSlot);
        const size_t arena_start = struct_end + slots_bytes;
        const size_t total       = arena_start + arena_len;
        const size_t alloc_size  = total > 0u ? total : sizeof(DrakenStringArena);

        uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc_size));
        if (!block) throw std::bad_alloc();
        std::memset(block, 0, alloc_size);
        OwnedBuffer<void> data_buf(block);

        DrakenStringArena* sa     = reinterpret_cast<DrakenStringArena*>(block);
        DrakenStringSlot*  dslots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
        uint8_t*           darena = (arena_len > 0u) ? (block + arena_start) : nullptr;

        // Step 3: copy caller's slots and arena bytes into consolidated block.
        if (length > 0u && slots)
            std::memcpy(dslots, slots, (size_t)length * sizeof(DrakenStringSlot));
        if (arena_len > 0u && arena)
            std::memcpy(darena, arena, arena_len);

        // Step 4: release guards and free the caller's original buffers.
        // release() prevents the destructor from double-freeing. draken_free(nullptr) is safe.
        void*    raw_slots = slots_guard.release();
        void*    raw_arena = arena_guard.release();
        uint8_t* raw_valid = validity_guard.release();
        draken_free(raw_slots);
        draken_free(raw_arena);
        // validity is taken into val_buf (still owned, freed by VectorOwner on GC).
        OwnedBuffer<uint8_t> val_buf(raw_valid);

        // Step 5: initialise DrakenStringArena with the caller's type tag.
        sa->slots       = dslots;
        sa->arena       = darena;
        sa->length      = (size_t)length;
        sa->arena_used  = arena_len;
        sa->arena_cap   = arena_len;
        sa->null_bitmap = nullptr;  // validity is tracked separately via VectorOwner
        sa->owns_buffers = 0;
        sa->type        = type;

        // Step 6: construct DrakenVector (dense; flags set by draken_vector_from_dense).
        DrakenVector v = draken_vector_from_dense(sa, length, type, raw_valid);

        VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
        // obj destructor Py_DECREFs; net effect: one new reference returned.
    } catch (nb::python_error& e) {
        e.restore();
        return nullptr;
    } catch (std::bad_alloc&) {
        PyErr_NoMemory();
        return nullptr;
    } catch (std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// draken_vector_own_array — wrap hand-allocated buffers in a DRAKEN_ARRAY[string] Vector.
//
// See draken_bridge.h for the full contract. child_type must be VARCHAR/NVARCHAR/VARBINARY.
// All five caller buffers are transferred unconditionally on entry.
extern "C" PyObject* draken_vector_own_array(
    int32_t*          parent_offsets,
    DrakenStringSlot* child_slots,
    uint8_t*          child_arena,
    size_t            child_arena_len,
    uint32_t          child_length,
    DrakenType        child_type,
    uint8_t*          parent_validity,
    uint32_t          length)
{
    // Step 1: take ownership of all caller buffers immediately via RAII.
    OwnedBuffer<void>    off_guard(static_cast<void*>(parent_offsets));
    OwnedBuffer<void>    slots_guard(static_cast<void*>(child_slots));
    OwnedBuffer<void>    arena_guard(static_cast<void*>(child_arena));
    OwnedBuffer<uint8_t> pval_guard(parent_validity);

    try {
        if (child_type != DRAKEN_VARCHAR && child_type != DRAKEN_NVARCHAR &&
            child_type != DRAKEN_VARBINARY) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_array: child_type must be DRAKEN_VARCHAR, "
                "DRAKEN_NVARCHAR, or DRAKEN_VARBINARY");
            return nullptr;
        }
        if (child_arena_len > 0u && !child_arena) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_array: child_arena_len > 0 but child_arena is NULL");
            return nullptr;
        }
        if (child_length > 0u && !child_slots) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_array: child_length > 0 but child_slots is NULL");
            return nullptr;
        }
        if (!parent_offsets && length > 0u) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_array: parent_offsets is NULL but length > 0");
            return nullptr;
        }

        // Step 2: build child consolidated block.
        // Layout: [DrakenStringArena | DrakenStringSlot[child_length] | arena_bytes]
        constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
        const size_t struct_end =
            (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
        const size_t slots_bytes = (child_length > 0u ? (size_t)child_length : 1u)
                                   * sizeof(DrakenStringSlot);
        const size_t arena_start = struct_end + slots_bytes;
        const size_t total       = arena_start + child_arena_len;
        const size_t alloc_size  = total > 0u ? total : sizeof(DrakenStringArena);

        uint8_t* child_block = static_cast<uint8_t*>(draken_malloc(alloc_size));
        if (!child_block) throw std::bad_alloc();
        std::memset(child_block, 0, alloc_size);
        OwnedBuffer<void> child_data_buf(child_block);

        DrakenStringArena* sa     = reinterpret_cast<DrakenStringArena*>(child_block);
        DrakenStringSlot*  dslots = reinterpret_cast<DrakenStringSlot*>(child_block + struct_end);
        uint8_t*           darena = (child_arena_len > 0u) ? (child_block + arena_start) : nullptr;

        // Step 3: copy child slots and arena into consolidated block.
        if (child_length > 0u && child_slots)
            std::memcpy(dslots, child_slots, (size_t)child_length * sizeof(DrakenStringSlot));
        if (child_arena_len > 0u && child_arena)
            std::memcpy(darena, child_arena, child_arena_len);

        // Step 4: release and free the caller's original child buffers.
        void* raw_slots = slots_guard.release();
        void* raw_arena = arena_guard.release();
        draken_free(raw_slots);
        draken_free(raw_arena);

        // Step 5: populate DrakenStringArena.
        sa->slots       = dslots;
        sa->arena       = darena;
        sa->length      = (size_t)child_length;
        sa->arena_used  = child_arena_len;
        sa->arena_cap   = child_arena_len;
        sa->null_bitmap = nullptr;  // child elements are always valid
        sa->owns_buffers = 0;
        sa->type        = child_type;

        // Step 6: build child VectorOwner (dense, no validity — child elements always valid).
        DrakenVector child_vec = draken_vector_from_dense(
            sa, child_length, child_type, nullptr);
        VectorOwner child_owner(child_vec, std::move(child_data_buf),
                                OwnedBuffer<uint8_t>(nullptr));

        // Step 7: build parent VectorOwner (dense, DRAKEN_ARRAY, parent validity).
        void*    raw_off  = off_guard.release();
        uint8_t* raw_pval = pval_guard.release();
        OwnedBuffer<void>    parent_data_buf(raw_off);
        OwnedBuffer<uint8_t> parent_val_buf(raw_pval);

        DrakenVector parent_vec = draken_vector_from_dense(
            raw_off, length, DRAKEN_ARRAY, raw_pval);
        VectorOwner owner(parent_vec, std::move(parent_data_buf), std::move(parent_val_buf));
        owner.child_owner = std::make_unique<VectorOwner>(std::move(child_owner));

        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
    } catch (nb::python_error& e) {
        e.restore();
        return nullptr;
    } catch (std::bad_alloc&) {
        PyErr_NoMemory();
        return nullptr;
    } catch (std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// draken_vector_own_timestamp — wrap a hand-allocated int64 buffer as a DRAKEN_TIMESTAMP64 Vector.
//
// See draken_bridge.h for the contract. The "days" unit is special: the input data buffer
// is scaled to microseconds in a new allocation; the caller's original buffer is freed here.
extern "C" PyObject* draken_vector_own_timestamp(
    void* data, uint8_t* validity, uint32_t length, const char* unit_str)
{
    // Take ownership of caller buffers immediately so they are always freed.
    OwnedBuffer<void>    data_guard(data);
    OwnedBuffer<uint8_t> val_guard(validity);

    try {
        TimestampUnit unit;

        if      (std::strcmp(unit_str, "us")   == 0) unit = TimestampUnit::MICROSECONDS;
        else if (std::strcmp(unit_str, "ms")   == 0) unit = TimestampUnit::MILLISECONDS;
        else if (std::strcmp(unit_str, "s")    == 0) unit = TimestampUnit::SECONDS;
        else if (std::strcmp(unit_str, "ns")   == 0) unit = TimestampUnit::NANOSECONDS;
        else if (std::strcmp(unit_str, "days") == 0) unit = TimestampUnit::MICROSECONDS;
        else {
            PyErr_Format(PyExc_ValueError,
                "draken_vector_own_timestamp: unsupported unit '%s'; "
                "use 's', 'ms', 'us', 'ns', or 'days'", unit_str);
            return nullptr;
        }

        // "days": scale epoch-days → epoch-microseconds, swap out the data buffer.
        const bool scale_days = (std::strcmp(unit_str, "days") == 0);
        if (scale_days) {
            const size_t nbytes = (length > 0u ? length : 1u) * sizeof(int64_t);
            int64_t* scaled = static_cast<int64_t*>(draken_malloc(nbytes));
            if (!scaled) throw std::bad_alloc();
            const int64_t* src = static_cast<const int64_t*>(data);
            for (uint32_t i = 0u; i < length; ++i)
                scaled[i] = src[i] * 86'400'000'000LL;
            // data_guard frees the caller's original; scaled takes its place.
            data_guard.reset(scaled);
        }

        void* final_data = data_guard.release();
        DrakenVector v = draken_vector_from_dense(final_data, length, DRAKEN_TIMESTAMP64, validity);
        OwnedBuffer<void>    data_buf(final_data);
        OwnedBuffer<uint8_t> vbuf(val_guard.release());
        VectorOwner owner(v, std::move(data_buf), std::move(vbuf));

        LogicalType lt;
        lt.kind           = LogicalKind::TIMESTAMP;
        lt.unit           = unit;
        lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);

        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
    } catch (nb::python_error& e) {
        e.restore();
        return nullptr;
    } catch (std::bad_alloc&) {
        PyErr_NoMemory();
        return nullptr;
    } catch (std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// draken_vector_own — wrap a VecResult op result in a new Python Vector handle.
//
// C++ only (declared in bridge header under #ifdef __cplusplus).
// MOVES ownership: res.data and res.validity are consumed.
// If res.owns_selection is true, res.selection is also freed.
// Returns a NEW reference on success; NULL + exception on failure.
PyObject* draken_vector_own(VecResult res) {
    try {
        nb::object obj = nb::cast(vecresult_to_owner(res));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
    } catch (nb::python_error& e) {
        e.restore();
        return nullptr;
    } catch (std::bad_alloc&) {
        PyErr_NoMemory();
        return nullptr;
    } catch (std::exception& e) {
        PyErr_SetString(PyExc_RuntimeError, e.what());
        return nullptr;
    }
}

// ---------------------------------------------------------------------------
// Readback helpers (int64 only at this milestone)
// ---------------------------------------------------------------------------

// Uniform access: data[selection[i]] for logical row i.

static inline int64_t row_int64(const DrakenVector& v, uint32_t i) noexcept {
    const int64_t* data = static_cast<const int64_t*>(v.data);
    return data[v.selection[i]];
}

// Bool readback: bit-extract at position selection[i] in the bit-packed data buffer.
// Uniform access pattern: bit(data, selection[i]) — same as int64 but sub-byte element.
static inline bool row_bool(const DrakenVector& v, uint32_t i) noexcept {
    const uint32_t    bit_idx = v.selection[i];
    const uint8_t*    data    = static_cast<const uint8_t*>(v.data);
    return static_cast<bool>((data[bit_idx >> 3] >> (bit_idx & 7)) & 1u);
}

// Narrow integer readback: sign-extends to int64 for uniform Python boxing.
// Covers INT8/INT16/INT32; falls through to row_int64 for INT64.
static inline int64_t row_narrow_int(const DrakenVector& v, uint32_t i) noexcept {
    switch (v.type) {
        case DRAKEN_INT8:  return static_cast<int64_t>(
            static_cast<const int8_t* >(v.data)[v.selection[i]]);
        case DRAKEN_INT16: return static_cast<int64_t>(
            static_cast<const int16_t*>(v.data)[v.selection[i]]);
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:
            return static_cast<int64_t>(
                static_cast<const int32_t*>(v.data)[v.selection[i]]);
        default:           return row_int64(v, i);
    }
}

// Cross-width integer arithmetic helpers (D.6).
static inline bool is_integer_type(DrakenType t) noexcept {
    const unsigned u = static_cast<unsigned>(t);
    return u >= 1u && u <= 4u;  // DRAKEN_INT8=1 .. DRAKEN_INT64=4
}

// D.7 — float type predicate.
static inline bool is_float_type(DrakenType t) noexcept {
    return t == DRAKEN_FLOAT32 || t == DRAKEN_FLOAT64;
}

// D.7 — float scalar readback: returns canonical value as Python float (double).
static inline double row_float(const DrakenVector& v, uint32_t i) noexcept {
    if (v.type == DRAKEN_FLOAT32) {
        const float* data = static_cast<const float*>(v.data);
        return static_cast<double>(data[v.selection[i]]);
    }
    const double* data = static_cast<const double*>(v.data);
    return data[v.selection[i]];
}

static inline DrakenType wider_int_type(DrakenType a, DrakenType b) noexcept {
    return static_cast<DrakenType>(
        std::max(static_cast<int>(a), static_cast<int>(b)));
}

// Promote v to target if needed; returns nullptr when already the right type.
static std::unique_ptr<VectorOwner> maybe_promote(const DrakenVector& v, DrakenType target) {
    if (v.type == target) return nullptr;
    return std::make_unique<VectorOwner>(
        vecresult_to_owner(draken::ops::promote_narrow_int(v, target)));
}

// String-family type predicate: VARCHAR, NVARCHAR, or VARBINARY.
static inline bool is_varchar_family(DrakenType t) noexcept {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// String readback: decode the slot at logical row i to a Python str.
// Sole UTF-8 decode point; caller must have checked row_is_valid first.
static inline nb::object row_string(const DrakenVector& v, uint32_t i) {
    const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slot = &sa->slots[v.selection[i]];
    const uint32_t           len  = str_length(slot);
    const uint8_t*           bytes = str_data(slot, sa->arena);
    PyObject* pystr = PyUnicode_DecodeUTF8(
        reinterpret_cast<const char*>(bytes),
        static_cast<Py_ssize_t>(len),
        "strict");
    if (!pystr) throw nb::python_error();
    return nb::steal<nb::object>(pystr);
}

// Bytes readback: return the slot at logical row i as a Python bytes object.
// Used for DRAKEN_VARBINARY; caller must have checked row_is_valid first.
static inline nb::object row_bytes(const DrakenVector& v, uint32_t i) {
    const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(v.data);
    const DrakenStringSlot*  slot = &sa->slots[v.selection[i]];
    const uint32_t           len  = str_length(slot);
    const uint8_t*           bytes = str_data(slot, sa->arena);
    PyObject* pyb = PyBytes_FromStringAndSize(
        reinterpret_cast<const char*>(bytes),
        static_cast<Py_ssize_t>(len));
    if (!pyb) throw nb::python_error();
    return nb::steal<nb::object>(pyb);
}

// ---------------------------------------------------------------------------
// D.10 — decimal helpers (edge only; no Python objects in kernels)
// ---------------------------------------------------------------------------

// Lazily import and cache decimal.Decimal type.
// Called only from Python paths that hold the GIL.
static PyObject* get_decimal_type() {
    static PyObject* dec_t = nullptr;
    if (!dec_t) {
        PyObject* mod = PyImport_ImportModule("decimal");
        if (!mod) throw nb::python_error();
        dec_t = PyObject_GetAttrString(mod, "Decimal");
        Py_DECREF(mod);
        if (!dec_t) throw nb::python_error();
    }
    return dec_t;
}

// Convert Python decimal.Decimal → int64 unscaled value at column scale.
// Fails loud on:
//   - NaN / Inf
//   - sub-scale precision (value has more decimal places than scale)
//   - overflow of int64 range
//   - value exceeds declared precision
static int64_t decimal_to_unscaled(PyObject* d, uint8_t precision, uint8_t scale) {
    // Check for NaN / Inf before any arithmetic.
    PyObject* fin = PyObject_CallMethod(d, "is_finite", nullptr);
    if (!fin) throw nb::python_error();
    const bool finite = (PyObject_IsTrue(fin) == 1);
    Py_DECREF(fin);
    if (!finite)
        throw std::invalid_argument(
            "decimal: NaN and Inf cannot be stored as DECIMAL");

    PyObject* dec_type = get_decimal_type();

    // Build factor = Decimal(10**scale) for exact multiply.
    // scale ≤ 18, so 10**scale fits in int64; no overflow possible here.
    PyObject* factor_int = PyLong_FromLong(1);
    if (!factor_int) throw nb::python_error();
    for (int i = 0; i < static_cast<int>(scale); ++i) {
        PyObject* ten = PyLong_FromLong(10);
        if (!ten) { Py_DECREF(factor_int); throw nb::python_error(); }
        PyObject* nf = PyNumber_Multiply(factor_int, ten);
        Py_DECREF(ten); Py_DECREF(factor_int);
        if (!nf) throw nb::python_error();
        factor_int = nf;
    }
    PyObject* factor = PyObject_CallOneArg(dec_type, factor_int);
    Py_DECREF(factor_int);
    if (!factor) throw nb::python_error();

    // scaled = d * factor  (exact Decimal arithmetic, preserves all digits)
    PyObject* scaled = PyNumber_Multiply(d, factor);
    Py_DECREF(factor);
    if (!scaled) throw nb::python_error();

    // as_int = int(scaled)  (truncation toward 0 via __trunc__)
    PyObject* as_int = PyNumber_Long(scaled);
    if (!as_int) { Py_DECREF(scaled); throw nb::python_error(); }

    // Exact check: the value must be exactly an integer at this scale.
    // Reconstruct Decimal(as_int) and compare to scaled.
    // Python's decimal equality ignores trailing zeros (2.0 == 2), so
    // Decimal('2.0') at scale 0 passes; Decimal('1.505') at scale 2 fails.
    PyObject* as_dec_check = PyObject_CallOneArg(dec_type, as_int);
    if (!as_dec_check) { Py_DECREF(as_int); Py_DECREF(scaled); throw nb::python_error(); }

    PyObject* eq_obj = PyObject_RichCompare(scaled, as_dec_check, Py_EQ);
    Py_DECREF(as_dec_check); Py_DECREF(scaled);
    if (!eq_obj) { Py_DECREF(as_int); throw nb::python_error(); }

    const bool exact = (PyObject_IsTrue(eq_obj) == 1);
    Py_DECREF(eq_obj);
    if (!exact) {
        Py_DECREF(as_int);
        throw std::invalid_argument(
            "decimal: value has more decimal places than the declared scale");
    }

    // Check int64 range.
    int overflow_flag;
    const long long val = PyLong_AsLongLongAndOverflow(as_int, &overflow_flag);
    Py_DECREF(as_int);
    if (overflow_flag != 0)
        throw std::overflow_error(
            "decimal: unscaled value does not fit in int64 range");

    // Check precision: |unscaled| < 10^precision.
    // precision ≤ 18; 10^18 = 1e18 < INT64_MAX, so no overflow building limit.
    int64_t limit = 1;
    for (int i = 0; i < static_cast<int>(precision); ++i) limit *= 10;
    if (val >= static_cast<long long>(limit) || val <= -static_cast<long long>(limit))
        throw std::overflow_error(
            "decimal: value exceeds declared precision");

    return static_cast<int64_t>(val);
}

// Convert int64 unscaled value + scale → Python decimal.Decimal preserving scale.
// Decimal(unscaled).scaleb(-scale) keeps trailing zeros:
//   unscaled=150, scale=2  →  Decimal('1.50')
//   unscaled=0,   scale=3  →  Decimal('0.000')
static nb::object unscaled_to_py_decimal(int64_t unscaled, uint8_t scale) {
    PyObject* dec_type = get_decimal_type();

    PyObject* pyint = PyLong_FromLongLong(static_cast<long long>(unscaled));
    if (!pyint) throw nb::python_error();

    PyObject* d = PyObject_CallOneArg(dec_type, pyint);
    Py_DECREF(pyint);
    if (!d) throw nb::python_error();

    PyObject* neg_scale = PyLong_FromLong(-static_cast<long>(scale));
    if (!neg_scale) { Py_DECREF(d); throw nb::python_error(); }

    PyObject* result = PyObject_CallMethod(d, "scaleb", "O", neg_scale);
    Py_DECREF(neg_scale); Py_DECREF(d);
    if (!result) throw nb::python_error();

    return nb::steal<nb::object>(result);
}

// Validate that a DECIMAL vector has its mandatory logical-type descriptor.
static void require_decimal_descriptor(const VectorOwner& v, const char* ctx) {
    if (!v.logical_type)
        throw std::invalid_argument(
            std::string(ctx) +
            ": DECIMAL vector is missing its logical-type descriptor");
}

// ---------------------------------------------------------------------------
// D.10 — decimal ingestion factories (dense / constant / dict)
// ---------------------------------------------------------------------------

static VectorOwner make_decimal_from_sequence(
    nb::list seq, uint8_t precision, uint8_t scale)
{
    if (precision < 1 || precision > 18)
        throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
    if (scale > precision)
        throw std::invalid_argument("DECIMAL scale must be <= precision");

    const uint32_t length = static_cast<uint32_t>(seq.size());
    int64_t* data = static_cast<int64_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(int64_t)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[static_cast<Py_ssize_t>(i)];
        if (obj.is_none()) {
            data[i] = 0;
            has_nulls = true;
        } else {
            data[i] = decimal_to_unscaled(obj.ptr(), precision, scale);
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[static_cast<Py_ssize_t>(i)].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_DECIMAL, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind      = LogicalKind::DECIMAL;
    lt.precision = precision;
    lt.scale     = scale;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner make_decimal_constant(
    nb::object value_obj, uint32_t length, uint8_t precision, uint8_t scale)
{
    if (precision < 1 || precision > 18)
        throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
    if (scale > precision)
        throw std::invalid_argument("DECIMAL scale must be <= precision");

    const bool    is_null = value_obj.is_none();
    const int64_t scalar  = is_null ? 0LL
        : decimal_to_unscaled(value_obj.ptr(), precision, scale);

    int64_t* data = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_DECIMAL, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind      = LogicalKind::DECIMAL;
    lt.precision = precision;
    lt.scale     = scale;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner make_decimal_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq,
    uint8_t precision, uint8_t scale)
{
    if (precision < 1 || precision > 18)
        throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
    if (scale > precision)
        throw std::invalid_argument("DECIMAL scale must be <= precision");

    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    int64_t* dict_data = static_cast<int64_t*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(int64_t)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        nb::object obj = values_seq[static_cast<Py_ssize_t>(k)];
        dict_data[k] = obj.is_none() ? 0LL
            : decimal_to_unscaled(obj.ptr(), precision, scale);
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[static_cast<Py_ssize_t>(i)]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[static_cast<Py_ssize_t>(i)]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, DRAKEN_DECIMAL, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf),
                      std::move(codes_owned));

    LogicalType lt;
    lt.kind      = LogicalKind::DECIMAL;
    lt.precision = precision;
    lt.scale     = scale;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// ---------------------------------------------------------------------------
// D.12 — interval ingestion helpers
//
// Physical: DrakenIntervalSlot { int64_t months; int64_t ms; }, 16 bytes/row.
// Python API: (months: int, ms: int) tuple or None.
//
// Normalization overflow is checked at ingestion so stored data never overflows
// when the kernel normalizes unchecked at op entry.
//
// NOTE: (months, ms) tuples are used as the Python type for now. This is
// flagged for the consumer-rewrite as the engine may want a richer type.
// ---------------------------------------------------------------------------

static DrakenIntervalSlot py_to_interval_slot(nb::object obj) {
    if (!PyTuple_Check(obj.ptr()) || PyTuple_GET_SIZE(obj.ptr()) != 2)
        throw std::invalid_argument(
            "interval: element must be a (months, ms) tuple or None");
    // PyTuple_GET_ITEM returns a BORROWED reference — use PyLong_AsLongLong
    // directly to avoid ref-count manipulation on the borrowed pointer.
    int64_t months = PyLong_AsLongLong(PyTuple_GET_ITEM(obj.ptr(), 0));
    int64_t ms     = PyLong_AsLongLong(PyTuple_GET_ITEM(obj.ptr(), 1));
    if ((months == -1 || ms == -1) && PyErr_Occurred())
        throw nb::python_error();
    // Validate that normalization doesn't overflow.
    draken::ops::interval_normalize_checked(months, ms);
    return DrakenIntervalSlot{months, ms};
}

static nb::object interval_slot_to_py(const DrakenIntervalSlot& s) {
    PyObject* tup = PyTuple_New(2);
    if (!tup) throw nb::python_error();
    PyObject* mo = PyLong_FromLongLong(static_cast<long long>(s.months));
    PyObject* ms = PyLong_FromLongLong(static_cast<long long>(s.ms));
    if (!mo || !ms) {
        Py_XDECREF(mo); Py_XDECREF(ms); Py_DECREF(tup);
        throw nb::python_error();
    }
    PyTuple_SET_ITEM(tup, 0, mo);
    PyTuple_SET_ITEM(tup, 1, ms);
    return nb::steal<nb::object>(tup);
}

static VectorOwner make_interval_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    const size_t data_bytes = (length > 0u ? length : 1u) * sizeof(DrakenIntervalSlot);
    DrakenIntervalSlot* data = static_cast<DrakenIntervalSlot*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[static_cast<Py_ssize_t>(i)];
        if (obj.is_none()) {
            data[i] = {0, 0};
            has_nulls = true;
        } else {
            data[i] = py_to_interval_slot(obj);
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (seq[static_cast<Py_ssize_t>(i)].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_INTERVAL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

static VectorOwner make_interval_constant(nb::object value_obj, uint32_t length) {
    const bool is_null = value_obj.is_none();
    const DrakenIntervalSlot scalar = is_null ? DrakenIntervalSlot{0, 0}
                                              : py_to_interval_slot(value_obj);

    DrakenIntervalSlot* data = static_cast<DrakenIntervalSlot*>(
        draken_malloc(sizeof(DrakenIntervalSlot)));
    if (!data) throw std::bad_alloc();
    data[0] = scalar;
    OwnedBuffer<void> data_buf(data);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (is_null) {
        const uint32_t padded = ((((length + 7u) >> 3) + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        std::memset(validity, 0x00, vbytes);
        validity_buf.reset(validity);
    }

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_INTERVAL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

static VectorOwner make_interval_dict(
    nb::list values_seq, nb::list codes_seq, nb::object nullable_seq)
{
    const uint32_t dict_size = static_cast<uint32_t>(values_seq.size());
    const uint32_t length    = static_cast<uint32_t>(codes_seq.size());

    DrakenIntervalSlot* dict_data = static_cast<DrakenIntervalSlot*>(
        draken_malloc((dict_size > 0u ? dict_size : 1u) * sizeof(DrakenIntervalSlot)));
    if (!dict_data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dict_data);
    for (uint32_t k = 0; k < dict_size; ++k) {
        nb::object obj = values_seq[static_cast<Py_ssize_t>(k)];
        dict_data[k] = obj.is_none() ? DrakenIntervalSlot{0, 0} : py_to_interval_slot(obj);
    }

    uint32_t* codes = static_cast<uint32_t*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(uint32_t)));
    if (!codes) throw std::bad_alloc();
    OwnedBuffer<void> codes_owned(codes);
    for (uint32_t i = 0; i < length; ++i)
        codes[i] = nb::cast<uint32_t>(codes_seq[static_cast<Py_ssize_t>(i)]);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (!nullable_seq.is_none()) {
        nb::list null_list = nb::cast<nb::list>(nullable_seq);
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0; i < length; ++i)
            if (!nb::cast<bool>(null_list[static_cast<Py_ssize_t>(i)]))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dict(
        dict_data, dict_size, codes, length, DRAKEN_INTERVAL, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf),
                       std::move(codes_owned));
}

// ---------------------------------------------------------------------------
// D.13: DRAKEN_ARRAY — offsets + child, parent-owns-child RAII (doc 01).
//
// Physical layout:
//   vec.data      = int32_t offsets[length+1]  (owned by data_buf)
//   child_owner   = the child VectorOwner (all child buffers owned here; RAII chains)
//   vec.selection = identity (arrays are always stored dense in this implementation)
//   vec.validity  = row-level null bitmap (NULL = all rows valid)
//
// Access: logical row i → child[offsets[sel[i]] : offsets[sel[i]+1]].
// Empty sublist ([] ) ↔ offsets[i] == offsets[i+1], validity bit set (valid).
// Null row (None)    ↔ validity bit clear; offsets[i] == offsets[i+1].
//
// Forward declarations for mutual recursion (array-of-array).
static VectorOwner make_array_take(const VectorOwner& v,
                                   const int32_t* indices, uint32_t n);
static VectorOwner make_array_materialize(const VectorOwner& v);
static VectorOwner make_array_compress(const VectorOwner& v);

// ---------------------------------------------------------------------------
// Readback helper: decode logical row row_idx of an ARRAY vector to Python list.
// row_idx must already be in [0, length). Returns None for null rows.
// ---------------------------------------------------------------------------
static nb::object row_array_to_pylist(const VectorOwner& v, uint32_t row_idx);

static nb::object child_elem_to_py(const VectorOwner& child, uint32_t child_idx) {
    if (child.vec.type == DRAKEN_NULL || !row_is_valid(child.vec, child_idx))
        return nb::none();
    if (child.vec.type == DRAKEN_ARRAY)
        return row_array_to_pylist(child, child_idx);
    if (child.vec.type == DRAKEN_VARCHAR || child.vec.type == DRAKEN_NVARCHAR)
        return row_string(child.vec, child_idx);
    if (child.vec.type == DRAKEN_VARBINARY)
        return row_bytes(child.vec, child_idx);
    if (is_float_type(child.vec.type))
        return nb::cast(row_float(child.vec, child_idx));
    if (child.vec.type == DRAKEN_BOOL)
        return nb::cast(row_bool(child.vec, child_idx));
    return nb::cast(row_narrow_int(child.vec, child_idx));
}

static nb::object row_array_to_pylist(const VectorOwner& v, uint32_t row_idx) {
    if (!row_is_valid(v.vec, row_idx)) return nb::none();
    const int32_t* offsets = static_cast<const int32_t*>(v.vec.data);
    const uint32_t sel_i   = v.vec.selection[row_idx];
    const int32_t  start   = offsets[sel_i];
    const int32_t  end     = offsets[sel_i + 1u];
    nb::list result;
    if (v.child_owner && start < end) {
        const VectorOwner& child = *v.child_owner;
        for (int32_t j = start; j < end; ++j)
            result.append(child_elem_to_py(child, static_cast<uint32_t>(j)));
    }
    return result;
}

// ---------------------------------------------------------------------------
// Helper: take child elements by a flat index array (non-negative raw indices).
// Routes by child type; recursive for DRAKEN_ARRAY children.
// ---------------------------------------------------------------------------
static VectorOwner take_child(const VectorOwner& src_child,
                               const std::vector<int32_t>& cidx) {
    const uint32_t cn = static_cast<uint32_t>(cidx.size());
    if (src_child.vec.type == DRAKEN_ARRAY)
        return make_array_take(src_child, cidx.data(), cn);
    if (src_child.vec.type == DRAKEN_VECTOR_FP16)
        return make_fp16_take(src_child, cidx.data(), cn);
    auto result = vecresult_to_owner(draken_take(src_child.vec, cidx.data(), cn));
    result.vec.type     = src_child.vec.type;
    result.logical_type = src_child.logical_type;
    return result;
}

// ---------------------------------------------------------------------------
// D.13: array ingestion — Python list[list | None] → dense DRAKEN_ARRAY vector.
//
// Child type inferred from first non-null, non-empty element:
//   int   → DRAKEN_INT64
//   str   → DRAKEN_VARCHAR
//   list  → DRAKEN_ARRAY (recursive)
// Defaults to DRAKEN_INT64 if all rows are null or empty.
// ---------------------------------------------------------------------------
static VectorOwner make_array_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    enum ChildType { CT_UNKNOWN, CT_INT64, CT_STRING, CT_ARRAY };
    ChildType child_type = CT_UNKNOWN;

    // Pass 1: detect child type, compute offsets, collect flat child elements.
    std::vector<int32_t> offsets(length + 1u);
    offsets[0] = 0;
    nb::list flat_children;
    bool has_nulls = false;

    for (uint32_t i = 0u; i < length; ++i) {
        nb::object row = seq[static_cast<Py_ssize_t>(i)];
        if (row.is_none()) {
            offsets[i + 1u] = offsets[i];
            has_nulls = true;
            continue;
        }
        if (!PyList_Check(row.ptr()) && !PyTuple_Check(row.ptr()))
            throw std::invalid_argument(
                "vector_array_from_sequence: each non-null element must be a list");
        nb::list sub = nb::cast<nb::list>(row);
        const uint32_t sub_len = static_cast<uint32_t>(sub.size());

        if (child_type == CT_UNKNOWN && sub_len > 0u) {
            nb::object first = sub[0];
            if      (PyLong_Check(first.ptr()))                        child_type = CT_INT64;
            else if (PyUnicode_Check(first.ptr()))                     child_type = CT_STRING;
            else if (PyList_Check(first.ptr()) || PyTuple_Check(first.ptr())) child_type = CT_ARRAY;
            else
                throw std::invalid_argument(
                    "vector_array_from_sequence: unsupported child element type "
                    "(expected int, str, or list)");
        }
        for (uint32_t j = 0u; j < sub_len; ++j)
            flat_children.append(sub[static_cast<Py_ssize_t>(j)]);
        offsets[i + 1u] = offsets[i] + static_cast<int32_t>(sub_len);
    }
    if (child_type == CT_UNKNOWN) child_type = CT_INT64;

    // Build offsets buffer.
    const size_t off_bytes = (length + 1u) * sizeof(int32_t);
    int32_t* off_buf = static_cast<int32_t*>(draken_malloc(off_bytes));
    if (!off_buf) throw std::bad_alloc();
    std::memcpy(off_buf, offsets.data(), off_bytes);
    OwnedBuffer<void> data_buf(off_buf);

    // Build validity bitmap.
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < length; ++i) {
            if (seq[static_cast<Py_ssize_t>(i)].is_none())
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    // Build child VectorOwner.
    std::unique_ptr<VectorOwner> child;
    switch (child_type) {
        case CT_STRING:
            child = std::make_unique<VectorOwner>(make_string_from_sequence(flat_children));
            break;
        case CT_ARRAY:
            child = std::make_unique<VectorOwner>(make_array_from_sequence(flat_children));
            break;
        default:
            child = std::make_unique<VectorOwner>(make_int64_from_sequence(flat_children));
    }

    DrakenVector v = draken_vector_from_dense(off_buf, length, DRAKEN_ARRAY, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));
    owner.child_owner = std::move(child);
    return owner;
}

// ---------------------------------------------------------------------------
// D.13: array take — gather rows by index array → new owned DRAKEN_ARRAY.
// Indices are int32_t; negative values are resolved before calling this function.
// Result owns its own offsets + a new child (recursive RAII).
// ---------------------------------------------------------------------------
static VectorOwner make_array_take(const VectorOwner& v,
                                   const int32_t* indices, uint32_t n) {
    const int32_t* src_offsets = static_cast<const int32_t*>(v.vec.data);
    const int32_t  vlen        = static_cast<int32_t>(v.vec.length);

    // Build new offsets and child index list in one pass.
    const size_t off_bytes = (n + 1u) * sizeof(int32_t);
    int32_t* new_offsets = static_cast<int32_t*>(
        draken_malloc(off_bytes > 0u ? off_bytes : sizeof(int32_t)));
    if (!new_offsets) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(new_offsets);
    new_offsets[0] = 0;

    std::vector<int32_t> child_idx;
    bool has_nulls = false;

    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw nb::index_error("take: array index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;
            new_offsets[i + 1u] = new_offsets[i];
        } else {
            const uint32_t sel_i = v.vec.selection[static_cast<uint32_t>(idx)];
            const int32_t  start = src_offsets[sel_i];
            const int32_t  end   = src_offsets[sel_i + 1u];
            for (int32_t j = start; j < end; ++j)
                child_idx.push_back(j);
            new_offsets[i + 1u] = new_offsets[i] + (end - start);
        }
    }

    // Build validity bitmap for output rows.
    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm     = (n + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            int32_t idx = indices[i];
            if (idx < 0) idx += vlen;
            if (!row_is_valid(v.vec, static_cast<uint32_t>(idx)))
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    // Gather child elements (recursive RAII — result owns its child).
    std::unique_ptr<VectorOwner> new_child;
    if (v.child_owner)
        new_child = std::make_unique<VectorOwner>(take_child(*v.child_owner, child_idx));

    DrakenVector vr = draken_vector_from_dense(new_offsets, n, DRAKEN_ARRAY, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.child_owner = std::move(new_child);
    return owner;
}

// ---------------------------------------------------------------------------
// D.13: array materialize — expand selection to dense identity copy.
// Arrays are always stored dense in this implementation; materialize = full copy.
// ---------------------------------------------------------------------------
static VectorOwner make_array_materialize(const VectorOwner& v) {
    const uint32_t length      = v.vec.length;
    const int32_t* src_offsets = static_cast<const int32_t*>(v.vec.data);

    const size_t off_bytes = (length + 1u) * sizeof(int32_t);
    int32_t* new_offsets = static_cast<int32_t*>(
        draken_malloc(off_bytes > 0u ? off_bytes : sizeof(int32_t)));
    if (!new_offsets) throw std::bad_alloc();
    std::memcpy(new_offsets, src_offsets, off_bytes);
    OwnedBuffer<void> data_buf(new_offsets);

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (v.vec.validity != nullptr) {
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t   vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memcpy(validity, v.vec.validity, vbytes);
    }

    std::unique_ptr<VectorOwner> new_child;
    if (v.child_owner) {
        const VectorOwner& sc = *v.child_owner;
        if (sc.vec.type == DRAKEN_ARRAY) {
            new_child = std::make_unique<VectorOwner>(make_array_materialize(sc));
        } else if (sc.vec.type == DRAKEN_VECTOR_FP16) {
            new_child = std::make_unique<VectorOwner>(make_fp16_materialize(sc));
        } else {
            auto r = vecresult_to_owner(draken_materialize(sc.vec));
            r.vec.type     = sc.vec.type;
            r.logical_type = sc.logical_type;
            new_child = std::make_unique<VectorOwner>(std::move(r));
        }
    }

    DrakenVector vr = draken_vector_from_dense(new_offsets, length, DRAKEN_ARRAY, validity);
    VectorOwner owner(vr, std::move(data_buf), std::move(validity_buf));
    owner.child_owner = std::move(new_child);
    return owner;
}

// ---------------------------------------------------------------------------
// D.13: array compress — keep only valid rows (drop null rows).
// Result owns its own offsets + a compacted child.
// ---------------------------------------------------------------------------
static VectorOwner make_array_compress(const VectorOwner& v) {
    const uint32_t length      = v.vec.length;
    const int32_t* src_offsets = static_cast<const int32_t*>(v.vec.data);

    uint32_t valid_count = 0u;
    for (uint32_t i = 0u; i < length; ++i)
        if (row_is_valid(v.vec, i)) ++valid_count;

    const size_t off_bytes = (valid_count + 1u) * sizeof(int32_t);
    int32_t* new_offsets = static_cast<int32_t*>(
        draken_malloc(off_bytes > 0u ? off_bytes : sizeof(int32_t)));
    if (!new_offsets) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(new_offsets);
    new_offsets[0] = 0;

    std::vector<int32_t> child_idx;
    uint32_t out_row = 0u;
    for (uint32_t i = 0u; i < length; ++i) {
        if (!row_is_valid(v.vec, i)) continue;
        const uint32_t sel_i = v.vec.selection[i];
        const int32_t  start = src_offsets[sel_i];
        const int32_t  end   = src_offsets[sel_i + 1u];
        for (int32_t j = start; j < end; ++j)
            child_idx.push_back(j);
        new_offsets[out_row + 1u] = new_offsets[out_row] + (end - start);
        ++out_row;
    }

    std::unique_ptr<VectorOwner> new_child;
    if (v.child_owner)
        new_child = std::make_unique<VectorOwner>(take_child(*v.child_owner, child_idx));

    DrakenVector vr = draken_vector_from_dense(new_offsets, valid_count, DRAKEN_ARRAY, nullptr);
    VectorOwner owner(vr, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
    owner.child_owner = std::move(new_child);
    return owner;
}

// ---------------------------------------------------------------------------
// E.7 — NVARCHAR ingestion: same slot+arena storage as VARCHAR; type tag differs.
// Python list[str | None] → dense DRAKEN_NVARCHAR vector.
// LENGTH returns codepoint count; character ops are Unicode-aware (future).
// ---------------------------------------------------------------------------

static VectorOwner make_nvarchar_from_sequence(nb::list seq) {
    VectorOwner owner = make_string_from_sequence(seq);
    owner.vec.type = DRAKEN_NVARCHAR;
    if (owner.vec.data)
        static_cast<DrakenStringArena*>(owner.vec.data)->type = DRAKEN_NVARCHAR;
    return owner;
}

// ---------------------------------------------------------------------------
// E.7 — VARBINARY ingestion: Python list[bytes | None] → dense DRAKEN_VARBINARY vector.
// Opaque bytes; byte-length ops; character ops throw.
// Same slot+arena storage as VARCHAR; bytes extracted via PyBytes_AsStringAndSize.
// ---------------------------------------------------------------------------

static VectorOwner make_bytes_from_sequence(nb::list seq) {
    const uint32_t length = static_cast<uint32_t>(seq.size());

    std::vector<const char*> ptrs(length, nullptr);
    std::vector<Py_ssize_t>  lens(length, 0);
    size_t total_extern = 0;
    bool   has_nulls    = false;

    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[i];
        if (obj.is_none()) {
            has_nulls = true;
        } else {
            PyObject* pybytes = obj.ptr();
            if (!PyBytes_Check(pybytes))
                throw std::invalid_argument(
                    "vector_from_bytes_sequence: element is not bytes or None");
            Py_ssize_t slen = 0;
            char* buf = nullptr;
            if (PyBytes_AsStringAndSize(pybytes, &buf, &slen) < 0)
                throw nb::python_error();
            ptrs[i] = buf;
            lens[i] = slen;
            if (slen > STR_INLINE_MAX)
                total_extern += static_cast<size_t>(slen);
        }
    }

    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error(
            "vector_from_bytes_sequence: total arena bytes exceed 4 GB limit");

    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes  = (length > 0u ? length : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start  = struct_end + slots_bytes;

    size_t validity_start = arena_start + total_extern;
    size_t validity_bytes = 0u;
    if (has_nulls) {
        const uint32_t bm = (length + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity_bytes = padded > 0u ? padded : 8u;
    }
    const size_t total_alloc = validity_start + validity_bytes;

    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena)));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena));
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa     = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots  = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena  = (total_extern > 0u) ? (block + arena_start) : nullptr;
    uint8_t*           bitmap = has_nulls           ? (block + validity_start) : nullptr;

    sa->slots       = slots;
    sa->arena       = arena;
    sa->length      = length;
    sa->arena_used  = 0u;
    sa->arena_cap   = total_extern;
    sa->null_bitmap = nullptr;
    sa->owns_buffers = 0;
    sa->type        = DRAKEN_VARBINARY;

    if (has_nulls) {
        std::memset(bitmap, 0xFF, validity_bytes);
        sa->null_bitmap = bitmap;
    }

    for (uint32_t i = 0; i < length; ++i) {
        if (ptrs[i] == nullptr) {
            if (bitmap)
                bitmap[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        } else {
            const uint8_t* src  = reinterpret_cast<const uint8_t*>(ptrs[i]);
            const uint32_t slen = static_cast<uint32_t>(lens[i]);
            if (slen <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], src, slen);
            } else {
                if (sa->arena_used > static_cast<size_t>(UINT32_MAX))
                    throw std::overflow_error(
                        "vector_from_bytes_sequence: arena offset overflow");
                const uint32_t off = static_cast<uint32_t>(sa->arena_used);
                std::memcpy(arena + off, src, slen);
                str_init_extern(&slots[i], src, slen,
                                (uint32_t)XXH3_64bits(src, slen), off);
                sa->arena_used += slen;
            }
        }
    }

    DrakenVector v = draken_vector_from_dense(sa, length, DRAKEN_VARBINARY, bitmap);
    return VectorOwner(v, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
}

// ---------------------------------------------------------------------------
// E.32 — DECIMAL arithmetic nanobind dispatch helpers.
//
// These intercept DECIMAL × DECIMAL arithmetic before it reaches the OpsTable
// (whose arithmetic slots for DECIMAL are null; see hash.h D.10/E.32 comment).
// Each helper:
//   1. Validates both operands are DECIMAL with valid logical-type descriptors.
//   2. Extracts scales from the descriptors.
//   3. Calls the scale-aware kernel from decimal_arith.h.
//   4. Computes result precision/scale per PostgreSQL rules.
//   5. Interns and attaches the result LogicalType to the returned VectorOwner.
// ---------------------------------------------------------------------------

static VectorOwner decimal_add_dispatch(const VectorOwner& a, const VectorOwner& b) {
    if (a.vec.type != DRAKEN_DECIMAL || b.vec.type != DRAKEN_DECIMAL)
        throw std::invalid_argument("dec_add: both operands must be DRAKEN_DECIMAL");
    if (!a.logical_type || !b.logical_type)
        throw std::invalid_argument("dec_add: missing logical-type descriptor");
    const uint8_t sa = a.logical_type->scale,  pa = a.logical_type->precision;
    const uint8_t sb = b.logical_type->scale,  pb = b.logical_type->precision;
    // result_scale = max(sa, sb)
    // result_prec  = max(pa-sa, pb-sb) + max(sa,sb) + 1, capped at 18
    const uint8_t rs = (sa >= sb) ? sa : sb;
    const int int_a = (int)pa - (int)sa, int_b = (int)pb - (int)sb;
    const int rp_raw = (int_a >= int_b ? int_a : int_b) + (int)rs + 1;
    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = draken::ops::dec_add(a.vec, sa, b.vec, sb);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_sub_dispatch(const VectorOwner& a, const VectorOwner& b) {
    if (a.vec.type != DRAKEN_DECIMAL || b.vec.type != DRAKEN_DECIMAL)
        throw std::invalid_argument("dec_sub: both operands must be DRAKEN_DECIMAL");
    if (!a.logical_type || !b.logical_type)
        throw std::invalid_argument("dec_sub: missing logical-type descriptor");
    const uint8_t sa = a.logical_type->scale,  pa = a.logical_type->precision;
    const uint8_t sb = b.logical_type->scale,  pb = b.logical_type->precision;
    const uint8_t rs = (sa >= sb) ? sa : sb;
    const int int_a = (int)pa - (int)sa, int_b = (int)pb - (int)sb;
    const int rp_raw = (int_a >= int_b ? int_a : int_b) + (int)rs + 1;
    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = draken::ops::dec_sub(a.vec, sa, b.vec, sb);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_mul_dispatch(const VectorOwner& a, const VectorOwner& b) {
    if (a.vec.type != DRAKEN_DECIMAL || b.vec.type != DRAKEN_DECIMAL)
        throw std::invalid_argument("dec_mul: both operands must be DRAKEN_DECIMAL");
    if (!a.logical_type || !b.logical_type)
        throw std::invalid_argument("dec_mul: missing logical-type descriptor");
    const uint8_t sa = a.logical_type->scale,  pa = a.logical_type->precision;
    const uint8_t sb = b.logical_type->scale,  pb = b.logical_type->precision;
    // result_scale = sa + sb (kernel raises if > 18)
    // result_prec  = pa + pb, capped at 18
    const int rs_raw = (int)sa + (int)sb;
    const uint8_t rs = (rs_raw <= 18) ? (uint8_t)rs_raw : 18u;
    const int rp_raw = (int)pa + (int)pb;
    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = draken::ops::dec_mul(a.vec, sa, b.vec, sb);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_div_dispatch(const VectorOwner& a, const VectorOwner& b) {
    if (a.vec.type != DRAKEN_DECIMAL || b.vec.type != DRAKEN_DECIMAL)
        throw std::invalid_argument("dec_div: both operands must be DRAKEN_DECIMAL");
    if (!a.logical_type || !b.logical_type)
        throw std::invalid_argument("dec_div: missing logical-type descriptor");
    const uint8_t sa = a.logical_type->scale,  pa = a.logical_type->precision;
    const uint8_t sb = b.logical_type->scale;
    // result_scale = max(sa + 6, 6), capped at 18
    const int rs_raw = ((int)sa + 6 >= 6) ? (int)sa + 6 : 6;
    const uint8_t rs = (rs_raw <= 18) ? (uint8_t)rs_raw : 18u;
    // result_prec: approximated as pa + 6, capped at 18
    const int rp_raw = (int)pa + 6;
    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = draken::ops::dec_div(a.vec, sa, b.vec, sb, rs);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_mod_dispatch(const VectorOwner& a, const VectorOwner& b) {
    if (a.vec.type != DRAKEN_DECIMAL || b.vec.type != DRAKEN_DECIMAL)
        throw std::invalid_argument("dec_mod: both operands must be DRAKEN_DECIMAL");
    if (!a.logical_type || !b.logical_type)
        throw std::invalid_argument("dec_mod: missing logical-type descriptor");
    const uint8_t sa = a.logical_type->scale,  pa = a.logical_type->precision;
    const uint8_t sb = b.logical_type->scale;
    // result_scale = sa, result_prec = pa
    VecResult vr = draken::ops::dec_mod(a.vec, sa, b.vec, sb);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    owner.logical_type = a.logical_type;  // same scale/prec as dividend
    return owner;
}

// ---------------------------------------------------------------------------
// nanobind module
// ---------------------------------------------------------------------------

NB_MODULE(draken_native, m) {
    m.doc() = "Draken C++-first vector library — nanobind binding (Milestone B.1)";

    // D.8: initialise the datetime C API before any datetime ingestion/readback calls.
    // Must be the first thing done in the module init; sets the process-global
    // PyDateTimeAPI pointer used by all PyDateTime_* macros in this TU.
    PyDateTime_IMPORT;
    if (!PyDateTimeAPI)
        throw nb::python_error();

    // DrakenType enum exposed to Python. Integers match the frozen ABI values.
    nb::enum_<DrakenType>(m, "DrakenType")
        .value("INT8",         DRAKEN_INT8)
        .value("INT16",        DRAKEN_INT16)
        .value("INT32",        DRAKEN_INT32)
        .value("INT64",        DRAKEN_INT64)
        .value("DECIMAL",      DRAKEN_DECIMAL)
        .value("FLOAT32",      DRAKEN_FLOAT32)
        .value("FLOAT64",      DRAKEN_FLOAT64)
        .value("DATE32",       DRAKEN_DATE32)
        .value("TIMESTAMP64",  DRAKEN_TIMESTAMP64)
        .value("TIME32",       DRAKEN_TIME32)
        .value("TIME64",       DRAKEN_TIME64)
        .value("INTERVAL",     DRAKEN_INTERVAL)
        .value("BOOL",         DRAKEN_BOOL)
        .value("VARCHAR",      DRAKEN_VARCHAR)
        .value("NVARCHAR",     DRAKEN_NVARCHAR)
        .value("VARBINARY",    DRAKEN_VARBINARY)
        .value("ARRAY",        DRAKEN_ARRAY)
        .value("NON_NATIVE",   DRAKEN_NON_NATIVE)
        .value("NULL",         DRAKEN_NULL)
        .value("VECTOR_FP16",  DRAKEN_VECTOR_FP16)
        .export_values();

    // Vector: Python handle around VectorOwner. Destructor triggers RAII free.
    // Boxing (to Python objects) happens only in __getitem__ and to_pylist — nowhere else.
    nb::class_<VectorOwner>(m, "Vector")
        .def_prop_ro("type",   [](const VectorOwner& v) { return v.vec.type; })
        .def_prop_ro("length", [](const VectorOwner& v) { return v.vec.length; })
        .def("__len__", [](const VectorOwner& v) {
            return static_cast<size_t>(v.vec.length);
        })
        .def("__getitem__", [](const VectorOwner& v, int64_t i) -> nb::object {
            auto len = static_cast<int64_t>(v.vec.length);
            if (i < 0) i += len;
            if (i < 0 || i >= len)
                throw nb::index_error("vector index out of range");
            auto idx = static_cast<uint32_t>(i);
            // D.11: null type — every row is null by definition.
            if (v.vec.type == DRAKEN_NULL) return nb::none();
            if (!row_is_valid(v.vec, idx)) return nb::none();
            // D.13: array — readback as Python list[...] (or None for null rows).
            if (v.vec.type == DRAKEN_ARRAY) return row_array_to_pylist(v, idx);
            // D.11: fp16 vector — readback as Python list[float].
            if (v.vec.type == DRAKEN_VECTOR_FP16) {
                require_fp16_descriptor(v, "__getitem__");
                const uint32_t dim = v.logical_type->dimension;
                const uint16_t* row = static_cast<const uint16_t*>(v.vec.data)
                                      + v.vec.selection[idx] * static_cast<size_t>(dim);
                nb::list result;
                for (uint32_t k = 0u; k < dim; ++k)
                    result.append(nb::cast(
                        static_cast<double>(fp16_ieee_to_fp32_value(row[k]))));
                return result;
            }
            if (v.vec.type == DRAKEN_TIMESTAMP64)
                return instant_to_py_datetime(row_int64(v.vec, idx), v.logical_type);
            if (v.vec.type == DRAKEN_DATE32)
                return days_to_py_date(static_cast<int32_t>(row_narrow_int(v.vec, idx)));
            if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "TIME vector is missing its logical-type descriptor");
                const int64_t raw = (v.vec.type == DRAKEN_TIME64)
                    ? row_int64(v.vec, idx)
                    : static_cast<int64_t>(row_narrow_int(v.vec, idx));
                return raw_to_py_time(raw, v.logical_type->unit);
            }
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "__getitem__");
                return unscaled_to_py_decimal(row_int64(v.vec, idx),
                                              v.logical_type->scale);
            }
            if (v.vec.type == DRAKEN_INTERVAL) {
                const DrakenIntervalSlot* data =
                    static_cast<const DrakenIntervalSlot*>(v.vec.data);
                return interval_slot_to_py(data[v.vec.selection[idx]]);
            }
            if (v.vec.type == DRAKEN_BOOL)     return nb::cast(row_bool(v.vec, idx));
            if (v.vec.type == DRAKEN_VARCHAR)   return row_string(v.vec, idx);
            if (v.vec.type == DRAKEN_NVARCHAR)  return row_string(v.vec, idx);
            if (v.vec.type == DRAKEN_VARBINARY) return row_bytes(v.vec, idx);
            if (is_float_type(v.vec.type))      return nb::cast(row_float(v.vec, idx));
            return nb::cast(row_narrow_int(v.vec, idx));
        })
        .def("to_pylist", [](const VectorOwner& v) {
            nb::list out;
            // D.11: null type — every row is None.
            if (v.vec.type == DRAKEN_NULL) {
                for (uint32_t i = 0u; i < v.vec.length; ++i)
                    out.append(nb::none());
                return out;
            }
            // D.13: array — each row is a Python list (or None for null rows).
            if (v.vec.type == DRAKEN_ARRAY) {
                for (uint32_t i = 0u; i < v.vec.length; ++i)
                    out.append(row_array_to_pylist(v, i));
                return out;
            }
            // D.11: fp16 vector — each row is a Python list[float].
            if (v.vec.type == DRAKEN_VECTOR_FP16) {
                require_fp16_descriptor(v, "to_pylist");
                const uint32_t dim = v.logical_type->dimension;
                const uint16_t* data = static_cast<const uint16_t*>(v.vec.data);
                for (uint32_t i = 0u; i < v.vec.length; ++i) {
                    if (!row_is_valid(v.vec, i)) {
                        out.append(nb::none());
                    } else {
                        const uint16_t* row = data + v.vec.selection[i]
                                              * static_cast<size_t>(dim);
                        nb::list row_list;
                        for (uint32_t k = 0u; k < dim; ++k)
                            row_list.append(nb::cast(
                                static_cast<double>(fp16_ieee_to_fp32_value(row[k]))));
                        out.append(row_list);
                    }
                }
                return out;
            }
            // D.12: interval — each row is a (months, ms) tuple.
            if (v.vec.type == DRAKEN_INTERVAL) {
                const DrakenIntervalSlot* data =
                    static_cast<const DrakenIntervalSlot*>(v.vec.data);
                for (uint32_t i = 0; i < v.vec.length; ++i) {
                    if (!row_is_valid(v.vec, i)) {
                        out.append(nb::none());
                    } else {
                        out.append(interval_slot_to_py(data[v.vec.selection[i]]));
                    }
                }
                return out;
            }
            const bool is_ts       = (v.vec.type == DRAKEN_TIMESTAMP64);
            const bool is_date32   = (v.vec.type == DRAKEN_DATE32);
            const bool is_time     = (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64);
            const bool is_decimal  = (v.vec.type == DRAKEN_DECIMAL);
            const bool is_bool     = (v.vec.type == DRAKEN_BOOL);
            const bool is_varchar  = (v.vec.type == DRAKEN_VARCHAR || v.vec.type == DRAKEN_NVARCHAR);
            const bool is_binary   = (v.vec.type == DRAKEN_VARBINARY);
            const bool is_float    = is_float_type(v.vec.type);
            const bool is_time64   = (v.vec.type == DRAKEN_TIME64);
            if (is_time && !v.logical_type)
                throw std::invalid_argument(
                    "TIME vector is missing its logical-type descriptor");
            if (is_decimal)
                require_decimal_descriptor(v, "to_pylist");
            for (uint32_t i = 0; i < v.vec.length; ++i) {
                if (!row_is_valid(v.vec, i)) {
                    out.append(nb::none());
                } else if (is_ts) {
                    out.append(instant_to_py_datetime(row_int64(v.vec, i), v.logical_type));
                } else if (is_date32) {
                    out.append(days_to_py_date(static_cast<int32_t>(row_narrow_int(v.vec, i))));
                } else if (is_time) {
                    const int64_t raw = is_time64
                        ? row_int64(v.vec, i)
                        : static_cast<int64_t>(row_narrow_int(v.vec, i));
                    out.append(raw_to_py_time(raw, v.logical_type->unit));
                } else if (is_decimal) {
                    out.append(unscaled_to_py_decimal(row_int64(v.vec, i),
                                                      v.logical_type->scale));
                } else if (is_bool) {
                    out.append(nb::cast(row_bool(v.vec, i)));
                } else if (is_varchar) {
                    out.append(row_string(v.vec, i));
                } else if (is_binary) {
                    out.append(row_bytes(v.vec, i));
                } else if (is_float) {
                    out.append(nb::cast(row_float(v.vec, i)));
                } else {
                    out.append(nb::cast(row_narrow_int(v.vec, i)));
                }
            }
            return out;
        })
        // hash() — single-column hash. One uint64_t per logical row.
        // Null rows receive the NULL_HASH sentinel mixed through simd_hash_i64
        // (same convention as draken_old c_hash_single). Boxing at this edge only.
        .def("hash", [](const VectorOwner& v) {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("hash: not supported for DRAKEN_ARRAY");
            const uint32_t n = v.vec.length;
            const size_t alloc = (n > 0u ? n : 1u) * sizeof(uint64_t);
            uint64_t* out = static_cast<uint64_t*>(draken_malloc(alloc));
            if (!out) throw std::bad_alloc();
            OwnedBuffer<uint64_t> out_owned(out);
            // D.11: null — every row hashes as the NULL_HASH sentinel (same path as
            // null rows in other types: scratch=NULL_HASH → simd_hash_i64 → out[i]).
            if (v.vec.type == DRAKEN_NULL) {
                uint64_t scratch[1024];
                uint32_t i = 0u;
                while (i < n) {
                    const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
                    for (uint32_t j = 0u; j < block; ++j) scratch[j] = NULL_HASH;
                    simd_hash_i64(scratch, out + i, block);
                    i += block;
                }
            } else if (v.vec.type == DRAKEN_VECTOR_FP16) {
                // D.11: fp16 — hash canonical fp16 bit patterns per row.
                // Null rows receive the NULL_HASH sentinel (same convention as other types).
                require_fp16_descriptor(v, "hash");
                const uint32_t dim = v.logical_type->dimension;
                const uint16_t* data = static_cast<const uint16_t*>(v.vec.data);
                uint64_t scratch[1024];
                uint32_t i = 0u;
                while (i < n) {
                    const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
                    for (uint32_t j = 0u; j < block; ++j) {
                        if (!row_is_valid(v.vec, i + j)) {
                            scratch[j] = NULL_HASH;
                        } else {
                            scratch[j] = fp16_row_fnv_seed(
                                data + v.vec.selection[i + j]
                                     * static_cast<size_t>(dim), dim);
                        }
                    }
                    simd_hash_i64(scratch, out + i, block);
                    i += block;
                }
            } else {
                draken_hash(v.vec, out, n);
            }
            nb::list result;
            for (uint32_t i = 0u; i < n; ++i)
                result.append(nb::cast(out[i]));
            return result;
        })
        // ----------------------------------------------------------------
        // C.2 — reductions
        // sum(): empty or all-null → 0 (int) / 0.0 (float) / Decimal('0.00…') (decimal).
        // DECIMAL: accumulates unscaled int64 values then converts at the edge.
        .def("sum", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("sum: not supported for DRAKEN_ARRAY");
            if (v.vec.type == DRAKEN_VECTOR_FP16)
                throw std::invalid_argument("sum: not supported for VECTOR_FP16");
            // D.11: null — all-null sum = 0 per established contract.
            if (v.vec.type == DRAKEN_NULL) return nb::cast(static_cast<int64_t>(0));
            if (is_float_type(v.vec.type)) {
                double val = 0.0;
                draken_float_sum(v.vec, &val);
                return nb::cast(val);
            }
            int64_t val = 0;
            draken_sum(v.vec, &val);
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "sum");
                return unscaled_to_py_decimal(val, v.logical_type->scale);
            }
            return nb::cast(val);
        })
        // min(): empty or all-null → raises ValueError.
        // TIMESTAMP64: returns datetime; DATE32: returns date; TIME32/64: returns time.
        // DECIMAL: returns decimal.Decimal preserving scale.
        .def("min", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("min: not supported for DRAKEN_ARRAY");
            if (v.vec.type == DRAKEN_VECTOR_FP16)
                throw std::invalid_argument("min: not supported for VECTOR_FP16");
            if (v.vec.type == DRAKEN_NULL)
                throw std::invalid_argument("Cannot compute min of all-null column");
            if (v.vec.length == 0)
                throw std::invalid_argument("Cannot compute min of empty column");
            // D.12: INTERVAL — custom scan returns original (months, ms) of min slot.
            if (v.vec.type == DRAKEN_INTERVAL) {
                auto r = draken::ops::interval_find_min(v.vec);
                if (!r.found)
                    throw std::invalid_argument("Cannot compute min of all-null column");
                return interval_slot_to_py(DrakenIntervalSlot{r.months, r.ms});
            }
            if (is_float_type(v.vec.type)) {
                double val = 0.0;
                uint32_t count = draken_float_min(v.vec, &val);
                if (count == 0)
                    throw std::invalid_argument("Cannot compute min of all-null column");
                return nb::cast(val);
            }
            int64_t val = 0;
            uint32_t count = draken_min(v.vec, &val);
            if (count == 0)
                throw std::invalid_argument("Cannot compute min of all-null column");
            if (v.vec.type == DRAKEN_TIMESTAMP64)
                return instant_to_py_datetime(val, v.logical_type);
            if (v.vec.type == DRAKEN_DATE32)
                return days_to_py_date(static_cast<int32_t>(val));
            if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "TIME vector is missing its logical-type descriptor");
                return raw_to_py_time(val, v.logical_type->unit);
            }
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "min");
                return unscaled_to_py_decimal(val, v.logical_type->scale);
            }
            return nb::cast(val);
        })
        // max(): empty or all-null → raises ValueError.
        // TIMESTAMP64: returns datetime; DATE32: returns date; TIME32/64: returns time.
        // DECIMAL: returns decimal.Decimal preserving scale.
        // INTERVAL: returns (months, ms) tuple of the row with maximum normalized duration.
        .def("max", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("max: not supported for DRAKEN_ARRAY");
            if (v.vec.type == DRAKEN_VECTOR_FP16)
                throw std::invalid_argument("max: not supported for VECTOR_FP16");
            if (v.vec.type == DRAKEN_NULL)
                throw std::invalid_argument("Cannot compute max of all-null column");
            if (v.vec.length == 0)
                throw std::invalid_argument("Cannot compute max of empty column");
            // D.12: INTERVAL — custom scan returns original (months, ms) of max slot.
            if (v.vec.type == DRAKEN_INTERVAL) {
                auto r = draken::ops::interval_find_max(v.vec);
                if (!r.found)
                    throw std::invalid_argument("Cannot compute max of all-null column");
                return interval_slot_to_py(DrakenIntervalSlot{r.months, r.ms});
            }
            if (is_float_type(v.vec.type)) {
                double val = 0.0;
                uint32_t count = draken_float_max(v.vec, &val);
                if (count == 0)
                    throw std::invalid_argument("Cannot compute max of all-null column");
                return nb::cast(val);
            }
            int64_t val = 0;
            uint32_t count = draken_max(v.vec, &val);
            if (count == 0)
                throw std::invalid_argument("Cannot compute max of all-null column");
            if (v.vec.type == DRAKEN_TIMESTAMP64)
                return instant_to_py_datetime(val, v.logical_type);
            if (v.vec.type == DRAKEN_DATE32)
                return days_to_py_date(static_cast<int32_t>(val));
            if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "TIME vector is missing its logical-type descriptor");
                return raw_to_py_time(val, v.logical_type->unit);
            }
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "max");
                return unscaled_to_py_decimal(val, v.logical_type->scale);
            }
            return nb::cast(val);
        })
        // ----------------------------------------------------------------
        // C.2 — arithmetic (vector × vector or vector × scalar)
        // add / sub / mul / div / mod: dispatch on arg type at Python edge.
        // Cross-width promotion macro (D.6): if a and b are integer types but differ,
        // promote the narrower to match the wider before calling the kernel.
#define DRAKEN_BINOP_CROSS(fn, draken_fn, draken_fn_s, draken_float_fn_s, decimal_fn) \
        .def(#fn, [](const VectorOwner& self, nb::object other) -> VectorOwner {\
            if (self.vec.type == DRAKEN_ARRAY)                                 \
                throw std::invalid_argument(                                   \
                    std::string(#fn) + ": not supported for DRAKEN_ARRAY");    \
            if (nb::isinstance<VectorOwner>(other)) {                          \
                const VectorOwner& bo = nb::cast<const VectorOwner&>(other);   \
                const DrakenVector& a = self.vec;                              \
                const DrakenVector& b = bo.vec;                                \
                /* E.32: DECIMAL arithmetic intercepted before OpsTable. */    \
                if (a.type == DRAKEN_DECIMAL || b.type == DRAKEN_DECIMAL)     \
                    return decimal_fn(self, bo);                               \
                if (is_integer_type(a.type) && is_integer_type(b.type)        \
                        && a.type != b.type) {                                 \
                    DrakenType wt = wider_int_type(a.type, b.type);            \
                    auto pa = maybe_promote(a, wt);                            \
                    auto pb = maybe_promote(b, wt);                            \
                    return vecresult_to_owner(draken_fn(                       \
                        pa ? pa->vec : a, pb ? pb->vec : b));                  \
                }                                                              \
                if (a.type != b.type)                                          \
                    throw std::invalid_argument(                               \
                        "cross-type vector arithmetic not supported");         \
                return vecresult_to_owner(draken_fn(a, b));                    \
            }                                                                  \
            /* E.32: decimal × scalar not supported; promote scalar first. */  \
            if (self.vec.type == DRAKEN_DECIMAL)                               \
                throw std::invalid_argument(                                   \
                    std::string(#fn) + ": DECIMAL × scalar not supported; "   \
                    "promote scalar to DECIMAL first");                         \
            if (is_float_type(self.vec.type))                                  \
                return vecresult_to_owner(draken_float_fn_s(self.vec, nb::cast<double>(other))); \
            return vecresult_to_owner(draken_fn_s(self.vec, nb::cast<int64_t>(other))); \
        })
        DRAKEN_BINOP_CROSS(add, draken_add, draken_add_scalar, draken_float_add_scalar, decimal_add_dispatch)
        DRAKEN_BINOP_CROSS(sub, draken_sub, draken_sub_scalar, draken_float_sub_scalar, decimal_sub_dispatch)
        DRAKEN_BINOP_CROSS(mul, draken_mul, draken_mul_scalar, draken_float_mul_scalar, decimal_mul_dispatch)
        // div: integer truncation toward zero (div-by-zero → 0) for integers.
        // Float division is IEEE: 1.0/0.0 → +inf; 0.0/0.0 → NaN.
        // Decimal division: half-even rounding, div-by-zero raises (E.32).
        DRAKEN_BINOP_CROSS(div, draken_div, draken_div_scalar, draken_float_div_scalar, decimal_div_dispatch)
        DRAKEN_BINOP_CROSS(mod, draken_mod, draken_mod_scalar, draken_float_mod_scalar, decimal_mod_dispatch)
#undef DRAKEN_BINOP_CROSS
        // neg: unary negation; neg(INT64_MIN) wraps for integers (matches draken_old).
        // E.32: DECIMAL neg raises on INT64_MIN (financial data; no silent wrap).
        .def("neg", [](const VectorOwner& v) -> VectorOwner {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("neg: not supported for DRAKEN_ARRAY");
            if (v.vec.type == DRAKEN_DECIMAL) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "neg: DECIMAL requires a logical-type descriptor");
                VecResult vr = draken::ops::dec_neg(v.vec);
                VectorOwner owner = vecresult_to_owner(vr);
                owner.vec.type    = DRAKEN_DECIMAL;
                owner.logical_type = v.logical_type;  // scale/prec unchanged
                return owner;
            }
            return vecresult_to_owner(draken_neg(v.vec));
        })
        // ----------------------------------------------------------------
        // D.5 — bool logical ops (Kleene 3VL) + reductions.
        //
        // bool_and / bool_or: DRAKEN_BOOL × DRAKEN_BOOL → DRAKEN_BOOL.
        //   False dominates AND; True dominates OR.
        //   F∧N = F (valid), T∨N = T (valid); all other null cells → null.
        //
        // bool_not: DRAKEN_BOOL → DRAKEN_BOOL.
        //   ¬T=F, ¬F=T, ¬N=N (validity preserved).
        //
        // bool_any: T if any valid True; NULL if any null (no True); F otherwise.
        //   Empty → F.
        // bool_all: F if any valid False; NULL if any null (no False); T otherwise.
        //   Empty → T.
        //
        // All ops require DRAKEN_BOOL inputs; mismatched types throw invalid_argument.
        .def("bool_and", [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
            if (self.vec.type != DRAKEN_BOOL || other.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_and: both operands must be DRAKEN_BOOL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("bool_and: operands must have equal length");
            return vecresult_to_owner(draken::ops::bool_and(self.vec, other.vec));
        }, nb::arg("other"),
            "Kleene AND: BOOL × BOOL → BOOL. FALSE dominates (F∧N=F). T∧N=N.")
        .def("bool_or", [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
            if (self.vec.type != DRAKEN_BOOL || other.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_or: both operands must be DRAKEN_BOOL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("bool_or: operands must have equal length");
            return vecresult_to_owner(draken::ops::bool_or(self.vec, other.vec));
        }, nb::arg("other"),
            "Kleene OR: BOOL × BOOL → BOOL. TRUE dominates (T∨N=T). F∨N=N.")
        .def("bool_not", [](const VectorOwner& v) -> VectorOwner {
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_not: operand must be DRAKEN_BOOL");
            return vecresult_to_owner(draken::ops::bool_not(v.vec));
        },
            "Kleene NOT: ¬T=F, ¬F=T, ¬N=N (validity preserved).")
        .def("bool_any", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_any: operand must be DRAKEN_BOOL");
            const int8_t r = draken::ops::bool_any(v.vec);
            if (r < 0) return nb::none();
            return nb::cast(r == 1);
        },
            "SQL ANY (bool_or reduction). True/False/None. Empty → False.")
        .def("bool_all", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_all: operand must be DRAKEN_BOOL");
            const int8_t r = draken::ops::bool_all(v.vec);
            if (r < 0) return nb::none();
            return nb::cast(r == 1);
        },
            "SQL ALL (bool_and reduction). True/False/None. Empty → True.")
        // ----------------------------------------------------------------
        // C.2 — take / materialize / compress
        // logical_type is propagated: the output vector has the same unit/offset
        // as the input (the physical instant values are reordered, not reinterpreted).
        .def("take", [](const VectorOwner& v, nb::list indices) -> VectorOwner {
            const uint32_t n = static_cast<uint32_t>(indices.size());
            // D.11: null — taking from null always produces a null vector of length n.
            if (v.vec.type == DRAKEN_NULL) return make_null_vector(n);
            std::vector<int32_t> idx_vec(n);
            for (uint32_t i = 0; i < n; ++i)
                idx_vec[i] = nb::cast<int32_t>(indices[i]);
            // D.13: array — gather rows with owned child copy.
            if (v.vec.type == DRAKEN_ARRAY)
                return make_array_take(v, idx_vec.data(), n);
            // D.11: fp16 — gather rows by index.
            if (v.vec.type == DRAKEN_VECTOR_FP16)
                return make_fp16_take(v, idx_vec.data(), n);
            auto result = vecresult_to_owner(draken_take(v.vec, idx_vec.data(), n));
            // Typed kernels hardcode their own type tag in VecResult (e.g. i64_take
            // always emits DRAKEN_INT64).  Restore the original physical type so that
            // TIMESTAMP64 (and any future aliased type) stays correct after gather.
            result.vec.type     = v.vec.type;
            result.logical_type = v.logical_type;
            return result;
        })
        .def("materialize", [](const VectorOwner& v) -> VectorOwner {
            // D.11: null — materialize is a no-op (no encoding to expand).
            if (v.vec.type == DRAKEN_NULL) return make_null_vector(v.vec.length);
            // D.13: array — copy offsets + materialize child recursively.
            if (v.vec.type == DRAKEN_ARRAY) return make_array_materialize(v);
            // D.11: fp16 — expand selection to dense identity.
            if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_materialize(v);
            auto result = vecresult_to_owner(draken_materialize(v.vec));
            result.vec.type     = v.vec.type;
            result.logical_type = v.logical_type;
            return result;
        })
        .def("compress", [](const VectorOwner& v) -> VectorOwner {
            // D.11: null — all rows are null → 0 valid rows after compress.
            if (v.vec.type == DRAKEN_NULL) return make_null_vector(0u);
            // D.13: array — keep only valid rows, compacting child.
            if (v.vec.type == DRAKEN_ARRAY) return make_array_compress(v);
            // D.11: fp16 — keep only valid rows.
            if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_compress(v);
            auto result = vecresult_to_owner(draken_compress(v.vec));
            result.vec.type     = v.vec.type;
            result.logical_type = v.logical_type;
            return result;
        })
        // ----------------------------------------------------------------
        // C.3 — compare → DRAKEN_BOOL result vector.
        // op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
        //
        // compare_scalar: vector OP scalar → bool mask.
        //   INT64 vectors: scalar is int (int64_t).
        //   STRING vectors: scalar is str (Python unicode).
        //     The literal's slot is built at the edge using the same D.1 path
        //     (str_init_inline / str_init_extern + XXH3_64bits) so that
        //     equality can fast-reject on length/prefix/hash32 before exact
        //     arena-byte verification.
        //     For long literals arena_offset is set to 0; str_data(slot, bytes)
        //     returns bytes directly.
        //
        // compare_vector: vector OP vector (same type, same length) → bool mask.
        // Unsupported types throw std::invalid_argument.
        .def("compare_scalar", [](const VectorOwner& v, nb::object scalar, int op) -> VectorOwner {
            // D.13: array — whole-array comparison is unsupported (06, out of scope).
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument(
                    "compare_scalar: not supported for DRAKEN_ARRAY");
            // D.11: null — every row is null, so NULL OP anything = NULL (3VL).
            if (v.vec.type == DRAKEN_NULL)
                return make_all_null_bool(v.vec.length);
            // D.11: fp16 — ordering/equality not supported.
            if (v.vec.type == DRAKEN_VECTOR_FP16)
                throw std::invalid_argument(
                    "compare_scalar: ordering not supported for VECTOR_FP16");
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "compare_scalar");
                const int64_t unscaled = decimal_to_unscaled(
                    scalar.ptr(), v.logical_type->precision, v.logical_type->scale);
                return vecresult_to_owner(draken_compare_scalar(v.vec, unscaled, op));
            }
            if (v.vec.type == DRAKEN_TIMESTAMP64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "compare_scalar: TIMESTAMP64 requires a logical-type descriptor");
                const int64_t ts = py_datetime_to_instant(scalar, v.logical_type->unit);
                return vecresult_to_owner(draken_compare_scalar(v.vec, ts, op));
            }
            if (v.vec.type == DRAKEN_DATE32) {
                const int64_t days = static_cast<int64_t>(py_date_to_days(scalar.ptr()));
                return vecresult_to_owner(draken_compare_scalar(v.vec, days, op));
            }
            if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "compare_scalar: TIME vector requires a logical-type descriptor");
                const int64_t raw = py_time_to_raw(scalar.ptr(), v.logical_type->unit);
                return vecresult_to_owner(draken_compare_scalar(v.vec, raw, op));
            }
            // D.12: INTERVAL — scalar is (months, ms) tuple; normalize then dispatch.
            if (v.vec.type == DRAKEN_INTERVAL) {
                const DrakenIntervalSlot slot = py_to_interval_slot(scalar);
                const int64_t norm = draken::ops::interval_normalize_checked(
                    slot.months, slot.ms);
                return vecresult_to_owner(draken_compare_scalar(v.vec, norm, op));
            }
            if (v.vec.type == DRAKEN_VARCHAR) {
                // Build literal slot at the Python edge using the same ingestion
                // path as D.1 so equality against stored long strings is correct.
                PyObject* pystr = scalar.ptr();
                if (!PyUnicode_Check(pystr))
                    throw std::invalid_argument(
                        "compare_scalar: STRING vector requires str scalar");
                Py_ssize_t slen = 0;
                const char* utf8 = PyUnicode_AsUTF8AndSize(pystr, &slen);
                if (!utf8) throw nb::python_error();
                const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(utf8);
                const uint32_t ulen   = static_cast<uint32_t>(slen);
                DrakenStringSlot scalar_slot;
                if (ulen <= STR_INLINE_MAX) {
                    str_init_inline(&scalar_slot, ubytes, ulen);
                } else {
                    // arena_offset=0: str_data(&scalar_slot, ubytes) returns ubytes.
                    str_init_extern(&scalar_slot, ubytes, ulen,
                                    (uint32_t)XXH3_64bits(ubytes, ulen), 0u);
                }
                return vecresult_to_owner(
                    draken_str_compare_scalar(v.vec, scalar_slot, ubytes, op));
            }
            // FLOAT32/64: scalar is Python float (or int coerced to double).
            if (is_float_type(v.vec.type))
                return vecresult_to_owner(
                    draken_float_compare_scalar(v.vec, nb::cast<double>(scalar), op));
            // INT64 (and other types): expect int scalar.
            return vecresult_to_owner(draken_compare_scalar(v.vec, nb::cast<int64_t>(scalar), op));
        }, nb::arg("scalar"), nb::arg("op"),
            "Compare each row against scalar. op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le.\n"
            "INT64: scalar is int. STRING: scalar is str.\n"
            "Returns a DRAKEN_BOOL vector (bit-packed, 1 bit/row, LSB-first).")
        .def("compare_vector", [](const VectorOwner& self, const VectorOwner& other, int op) -> VectorOwner {
            const DrakenVector& a = self.vec;
            const DrakenVector& b = other.vec;
            // D.13: array — whole-array comparison unsupported.
            if (a.type == DRAKEN_ARRAY || b.type == DRAKEN_ARRAY)
                throw std::invalid_argument(
                    "compare_vector: not supported for DRAKEN_ARRAY");
            // D.11: null — NULL OP anything = NULL (3VL).
            if (a.type == DRAKEN_NULL || b.type == DRAKEN_NULL) {
                const uint32_t n = (a.type == DRAKEN_NULL) ? b.length : a.length;
                return make_all_null_bool(n);
            }
            // D.11: fp16 — ordering/equality not supported.
            if (a.type == DRAKEN_VECTOR_FP16 || b.type == DRAKEN_VECTOR_FP16)
                throw std::invalid_argument(
                    "compare_vector: ordering not supported for VECTOR_FP16");
            // DECIMAL: cross-scale comparison is a hard error — different scales
            // store different magnitudes; silently mis-comparing them produces wrong
            // answers.  Scale alignment (requires int128 rescale) is deferred to pt2.
            if (a.type == DRAKEN_DECIMAL || b.type == DRAKEN_DECIMAL) {
                if (a.type != DRAKEN_DECIMAL || b.type != DRAKEN_DECIMAL)
                    throw std::invalid_argument(
                        "compare_vector: cannot compare DECIMAL with a different type");
                if (!self.logical_type || !other.logical_type)
                    throw std::invalid_argument(
                        "compare_vector: DECIMAL requires a logical-type descriptor");
                if (self.logical_type->scale != other.logical_type->scale)
                    throw std::invalid_argument(
                        "compare_vector: cross-scale decimal comparison is not supported; "
                        "align scales before comparing");
                return vecresult_to_owner(draken_compare_vector(a, b, op));
            }
            // TIMESTAMP64: cross-unit comparison is a hard error — different units
            // store different magnitudes; silently mis-comparing them produces wrong
            // answers.  Unit alignment is deferred to a later decision; fail loud now.
            if (a.type == DRAKEN_TIMESTAMP64 || b.type == DRAKEN_TIMESTAMP64) {
                if (a.type != DRAKEN_TIMESTAMP64 || b.type != DRAKEN_TIMESTAMP64)
                    throw std::invalid_argument(
                        "compare_vector: cannot compare TIMESTAMP64 with a different type");
                if (!self.logical_type || !other.logical_type)
                    throw std::invalid_argument(
                        "compare_vector: TIMESTAMP64 requires a logical-type descriptor");
                if (self.logical_type->unit != other.logical_type->unit)
                    throw std::invalid_argument(
                        "compare_vector: cross-unit timestamp comparison is not supported; "
                        "align units before comparing");
            }
            // TIME32/TIME64: cross-unit compare is a hard error (same rationale as
            // TIMESTAMP64). Different physical types (TIME32 vs TIME64) also throw.
            if (a.type == DRAKEN_TIME32 || a.type == DRAKEN_TIME64 ||
                b.type == DRAKEN_TIME32 || b.type == DRAKEN_TIME64) {
                if (a.type != b.type)
                    throw std::invalid_argument(
                        "compare_vector: cannot compare TIME32 with TIME64 or a different type");
                if (!self.logical_type || !other.logical_type)
                    throw std::invalid_argument(
                        "compare_vector: TIME vector requires a logical-type descriptor");
                if (self.logical_type->unit != other.logical_type->unit)
                    throw std::invalid_argument(
                        "compare_vector: cross-unit time comparison is not supported; "
                        "align units before comparing");
            }
            // D.12: INTERVAL — cross-type comparison is a hard error.
            if (a.type == DRAKEN_INTERVAL || b.type == DRAKEN_INTERVAL) {
                if (a.type != DRAKEN_INTERVAL || b.type != DRAKEN_INTERVAL)
                    throw std::invalid_argument(
                        "compare_vector: cannot compare INTERVAL with a different type");
                return vecresult_to_owner(draken_compare_vector(a, b, op));
            }
            if (is_integer_type(a.type) && is_integer_type(b.type) && a.type != b.type) {
                DrakenType wt = wider_int_type(a.type, b.type);
                auto pa = maybe_promote(a, wt);
                auto pb = maybe_promote(b, wt);
                return vecresult_to_owner(draken_compare_vector(
                    pa ? pa->vec : a, pb ? pb->vec : b, op));
            }
            return vecresult_to_owner(draken_compare_vector(a, b, op));
        }, nb::arg("other"), nb::arg("op"),
            "Compare row-wise against another vector of the same type and length.\n"
            "Cross-width integers are promoted to the wider type before comparison.\n"
            "op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le.\n"
            "Returns a DRAKEN_BOOL vector (bit-packed, 1 bit/row, LSB-first).")
        // ----------------------------------------------------------------
        // C.4 — between and in_list → DRAKEN_BOOL result vector.
        //
        // between: single fused range pass (lo ≤/< v ≤/< hi).
        //   lo_inclusive / hi_inclusive default to True (SQL BETWEEN semantics).
        //   Null input row → null output row (TVL).
        //   Unsupported types throw std::invalid_argument.
        .def("between",
            [](const VectorOwner& v, nb::object lo, nb::object hi,
               bool lo_inclusive, bool hi_inclusive) -> VectorOwner {
                // D.13: array — unsupported.
                if (v.vec.type == DRAKEN_ARRAY)
                    throw std::invalid_argument("between: not supported for DRAKEN_ARRAY");
                // D.11: null — NULL BETWEEN anything = NULL (3VL).
                if (v.vec.type == DRAKEN_NULL)
                    return make_all_null_bool(v.vec.length);
                // D.11: fp16 — ordering not supported.
                if (v.vec.type == DRAKEN_VECTOR_FP16)
                    throw std::invalid_argument(
                        "between: ordering not supported for VECTOR_FP16");
                if (v.vec.type == DRAKEN_DECIMAL) {
                    require_decimal_descriptor(v, "between");
                    const int64_t lo_u = decimal_to_unscaled(
                        lo.ptr(), v.logical_type->precision, v.logical_type->scale);
                    const int64_t hi_u = decimal_to_unscaled(
                        hi.ptr(), v.logical_type->precision, v.logical_type->scale);
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_u, hi_u, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_TIMESTAMP64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "between: TIMESTAMP64 requires a logical-type descriptor");
                    const int64_t lo_i = py_datetime_to_instant(lo, v.logical_type->unit);
                    const int64_t hi_i = py_datetime_to_instant(hi, v.logical_type->unit);
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_DATE32) {
                    const int64_t lo_i = static_cast<int64_t>(py_date_to_days(lo.ptr()));
                    const int64_t hi_i = static_cast<int64_t>(py_date_to_days(hi.ptr()));
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "between: TIME vector requires a logical-type descriptor");
                    const int64_t lo_i = py_time_to_raw(lo.ptr(), v.logical_type->unit);
                    const int64_t hi_i = py_time_to_raw(hi.ptr(), v.logical_type->unit);
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                // D.12: INTERVAL — bounds are (months, ms) tuples; normalize.
                if (v.vec.type == DRAKEN_INTERVAL) {
                    const DrakenIntervalSlot lo_s = py_to_interval_slot(lo);
                    const DrakenIntervalSlot hi_s = py_to_interval_slot(hi);
                    const int64_t lo_ms = draken::ops::interval_normalize_checked(
                        lo_s.months, lo_s.ms);
                    const int64_t hi_ms = draken::ops::interval_normalize_checked(
                        hi_s.months, hi_s.ms);
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_ms, hi_ms, lo_inclusive, hi_inclusive));
                }
                if (is_float_type(v.vec.type)) {
                    return vecresult_to_owner(draken_float_between(
                        v.vec, nb::cast<double>(lo), nb::cast<double>(hi),
                        lo_inclusive, hi_inclusive));
                }
                return vecresult_to_owner(
                    draken_between(v.vec, nb::cast<int64_t>(lo), nb::cast<int64_t>(hi),
                                   lo_inclusive, hi_inclusive));
            },
            nb::arg("lo"), nb::arg("hi"),
            nb::arg("lo_inclusive") = true, nb::arg("hi_inclusive") = true,
            "Range membership: lo OP v OP hi (fused single pass).\n"
            "lo_inclusive / hi_inclusive control whether bounds are closed.\n"
            "FLOAT32/64 vectors accept float bounds; integer vectors accept int bounds.\n"
            "Null input row → null output row. Returns a DRAKEN_BOOL vector.")
        // in_list: hash-only membership via CarcharSet.
        //
        // CarcharSet stores 64-bit hashes only — no key verification. A hash
        // collision can admit a wrong row.
        //
        // Hash path: simd_hash_i64 on raw uint64 cast of each int64 value.
        // This is the SINGLE SHARED HASH PATH used by the hash op and joins;
        // routing through it guarantees set-build and probe hashes match.
        //
        // The CarcharSet is constructed here at the Python edge and passed by
        // const reference to the kernel — no buffer crosses allocator
        // boundaries (CarcharSet owns its std::vector storage throughout).
        .def("in_list",
            [](const VectorOwner& v, nb::list values) -> VectorOwner {
                // D.13: array — unsupported.
                if (v.vec.type == DRAKEN_ARRAY)
                    throw std::invalid_argument("in_list: not supported for DRAKEN_ARRAY");
                // D.11: null — NULL IN LIST = NULL (3VL).
                if (v.vec.type == DRAKEN_NULL)
                    return make_all_null_bool(v.vec.length);
                // D.11: fp16 — similarity search not supported here (usearch's domain).
                if (v.vec.type == DRAKEN_VECTOR_FP16)
                    throw std::invalid_argument(
                        "in_list: not supported for VECTOR_FP16 (use usearch for similarity)");
                // Build the hash set at the Python edge.  Hash path depends on type.
                const size_t n = static_cast<size_t>(values.size());
                opteryx::carchar::CarcharSet set(n > 0 ? n : 16u);
                if (v.vec.type == DRAKEN_DECIMAL) {
                    require_decimal_descriptor(v, "in_list");
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        const int64_t u = decimal_to_unscaled(
                            obj.ptr(),
                            v.logical_type->precision,
                            v.logical_type->scale);
                        uint64_t raw = static_cast<uint64_t>(u);
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    return vecresult_to_owner(draken_in_list(v.vec, set));
                }
                if (v.vec.type == DRAKEN_TIMESTAMP64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "in_list: TIMESTAMP64 requires a logical-type descriptor");
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        const int64_t ts = py_datetime_to_instant(obj, v.logical_type->unit);
                        uint64_t raw = static_cast<uint64_t>(ts);
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    return vecresult_to_owner(draken_in_list(v.vec, set));
                }
                if (v.vec.type == DRAKEN_DATE32) {
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        uint64_t raw = static_cast<uint64_t>(
                            static_cast<int64_t>(py_date_to_days(obj.ptr())));
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    return vecresult_to_owner(draken_in_list(v.vec, set));
                }
                if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "in_list: TIME vector requires a logical-type descriptor");
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        uint64_t raw = static_cast<uint64_t>(
                            py_time_to_raw(obj.ptr(), v.logical_type->unit));
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    return vecresult_to_owner(draken_in_list(v.vec, set));
                }
                // D.12: INTERVAL — normalize each value to total_ms, hash.
                if (v.vec.type == DRAKEN_INTERVAL) {
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        if (obj.is_none()) continue;  // null values never match non-null rows
                        const DrakenIntervalSlot slot = py_to_interval_slot(obj);
                        const int64_t norm = draken::ops::interval_normalize_checked(
                            slot.months, slot.ms);
                        uint64_t raw = static_cast<uint64_t>(norm);
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    return vecresult_to_owner(draken_in_list(v.vec, set));
                }
                if (v.vec.type == DRAKEN_VARCHAR) {
                    // Hash-only membership via CarcharSet; same str_hash_seed →
                    // simd_hash_i64 path as str_in_list and hash_string.  Any
                    // deviation here causes present values to miss.
                    for (size_t k = 0; k < n; ++k) {
                        PyObject* pystr = values[static_cast<Py_ssize_t>(k)].ptr();
                        if (!PyUnicode_Check(pystr))
                            throw std::invalid_argument(
                                "in_list: STRING vector requires str values");
                        Py_ssize_t slen = 0;
                        const char* utf8 = PyUnicode_AsUTF8AndSize(pystr, &slen);
                        if (!utf8) throw nb::python_error();
                        const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(utf8);
                        const uint32_t ulen   = static_cast<uint32_t>(slen);
                        DrakenStringSlot slot;
                        if (ulen <= STR_INLINE_MAX) {
                            str_init_inline(&slot, ubytes, ulen);
                        } else {
                            str_init_extern(&slot, ubytes, ulen,
                                            (uint32_t)XXH3_64bits(ubytes, ulen), 0u);
                        }
                        uint64_t seed = draken::ops::str_hash_seed(&slot, ubytes);
                        uint64_t h;
                        simd_hash_i64(&seed, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                } else if (is_float_type(v.vec.type)) {
                    // FLOAT32/64: canonicalize → fp_bits64 → simd_hash_i64.
                    // Same hash path as float_hash kernel: NaN and -0.0 canonical bits.
                    // FLOAT32 uses fp_bits64(float) → 32-bit bits zero-extended to 64.
                    // FLOAT64 uses fp_bits64(double) → 64-bit bits.
                    // Must narrow to float for FLOAT32 to match float_hash kernel.
                    const bool is_f32 = (v.vec.type == DRAKEN_FLOAT32);
                    for (size_t k = 0; k < n; ++k) {
                        const double d64 = nb::cast<double>(values[static_cast<Py_ssize_t>(k)]);
                        uint64_t raw;
                        if (is_f32) {
                            const float  f32v = draken::ops::fp_canon(static_cast<float>(d64));
                            raw = draken::ops::fp_bits64(f32v);
                        } else {
                            const double f64v = draken::ops::fp_canon(d64);
                            raw = draken::ops::fp_bits64(f64v);
                        }
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                } else {
                    // INT64 (and other types): values are Python ints.
                    for (size_t k = 0; k < n; ++k) {
                        uint64_t raw = static_cast<uint64_t>(
                            nb::cast<int64_t>(values[static_cast<Py_ssize_t>(k)]));
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                }
                return vecresult_to_owner(draken_in_list(v.vec, set));
            },
            nb::arg("values"),
            "Set membership: returns True for rows whose hash is found in values.\n"
            "Hash-only probe (§1 exception — no key verify; accepted at our volumes).\n"
            "STRING: each value is str; INT64: each value is int.\n"
            "Null input row → null output row. Returns a DRAKEN_BOOL vector.")
        // ----------------------------------------------------------------
        // D.13 — array per-row accessors.
        //
        // array_length(i): cardinality of sublist at logical row i.
        //   Null row → None. Empty sublist → 0.
        // array_get(i, j): element j of sublist at logical row i.
        //   Null row → None. j out of range → IndexError.
        // array_child_type: DrakenType of the child vector, or None if no child.
        .def("array_length", [](const VectorOwner& v, int64_t i) -> nb::object {
            if (v.vec.type != DRAKEN_ARRAY)
                throw std::invalid_argument("array_length: requires a DRAKEN_ARRAY vector");
            const int64_t len = static_cast<int64_t>(v.vec.length);
            if (i < 0) i += len;
            if (i < 0 || i >= len)
                throw nb::index_error("array_length: row index out of range");
            const uint32_t idx = static_cast<uint32_t>(i);
            if (!row_is_valid(v.vec, idx)) return nb::none();
            const int32_t* offsets = static_cast<const int32_t*>(v.vec.data);
            const uint32_t sel_i   = v.vec.selection[idx];
            return nb::cast(offsets[sel_i + 1u] - offsets[sel_i]);
        }, nb::arg("i"),
            "Return the length of the sublist at row i, or None for null rows.")
        .def("array_get", [](const VectorOwner& v, int64_t i, int64_t j) -> nb::object {
            if (v.vec.type != DRAKEN_ARRAY)
                throw std::invalid_argument("array_get: requires a DRAKEN_ARRAY vector");
            const int64_t len = static_cast<int64_t>(v.vec.length);
            if (i < 0) i += len;
            if (i < 0 || i >= len)
                throw nb::index_error("array_get: row index out of range");
            const uint32_t idx = static_cast<uint32_t>(i);
            if (!row_is_valid(v.vec, idx)) return nb::none();
            const int32_t* offsets = static_cast<const int32_t*>(v.vec.data);
            const uint32_t sel_i   = v.vec.selection[idx];
            const int32_t  start   = offsets[sel_i];
            const int32_t  end     = offsets[sel_i + 1u];
            const int32_t  sub_len = end - start;
            if (j < 0) j += static_cast<int64_t>(sub_len);
            if (j < 0 || j >= static_cast<int64_t>(sub_len))
                throw nb::index_error("array_get: element index out of range");
            if (!v.child_owner)
                throw std::invalid_argument("array_get: missing child owner");
            return child_elem_to_py(*v.child_owner,
                                    static_cast<uint32_t>(start + j));
        }, nb::arg("i"), nb::arg("j"),
            "Return element j of the sublist at row i.\n"
            "Null row → None. Negative indices supported. Raises IndexError if out of range.")
        .def_prop_ro("array_child_type", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_ARRAY || !v.child_owner) return nb::none();
            return nb::cast(v.child_owner->vec.type);
        },  "DrakenType of the child vector, or None for non-array or empty vectors.")
        // ----------------------------------------------------------------
        // is_dict / is_constant / is_dense — layout introspection for tests only.
        .def_prop_ro("is_dict", [](const VectorOwner& v) {
            return v.vec.data_length < v.vec.length;
        })
        .def_prop_ro("is_constant", [](const VectorOwner& v) {
            return v.vec.data_length == 1 && v.vec.length > 1;
        })
        .def_prop_ro("data_length", [](const VectorOwner& v) {
            return v.vec.data_length;
        })
        // D.8 — logical-type introspection (primarily for tests and consumers).
        // Returns None for types that carry no logical descriptor.
        .def_prop_ro("logical_type_unit", [](const VectorOwner& v) -> nb::object {
            if (!v.logical_type) return nb::none();
            return nb::cast(std::string(unit_to_str(v.logical_type->unit)));
        })
        .def_prop_ro("logical_type_offset_minutes", [](const VectorOwner& v) -> nb::object {
            if (!v.logical_type) return nb::none();
            return nb::cast(static_cast<int>(v.logical_type->offset_minutes));
        })
        // D.10 — decimal descriptor introspection.
        .def_prop_ro("logical_type_precision", [](const VectorOwner& v) -> nb::object {
            if (!v.logical_type) return nb::none();
            if (v.logical_type->kind != LogicalKind::DECIMAL) return nb::none();
            return nb::cast(static_cast<int>(v.logical_type->precision));
        })
        .def_prop_ro("logical_type_scale", [](const VectorOwner& v) -> nb::object {
            if (!v.logical_type) return nb::none();
            if (v.logical_type->kind != LogicalKind::DECIMAL) return nb::none();
            return nb::cast(static_cast<int>(v.logical_type->scale));
        })
        // D.11 — fp16 dimension introspection.
        .def_prop_ro("logical_type_dimension", [](const VectorOwner& v) -> nb::object {
            if (!v.logical_type) return nb::none();
            if (v.logical_type->kind != LogicalKind::VECTOR) return nb::none();
            return nb::cast(static_cast<int>(v.logical_type->dimension));
        })
        // _slot_fields(i) — test-only slot inspector for string-family vectors.
        // Short (len ≤ 12): returns (length, inline_bytes) where inline_bytes is
        //   all 12 inline bytes including zero-padding beyond the string content.
        // Long  (len > 12): returns (length, prefix, hash32).
        //   arena_offset is excluded — it need not be equal across vectors for the
        //   same string value, and is not part of the determinism contract.
        // Null row: returns None.
        // Raises ValueError for non-string-family vectors; IndexError if i out of range.
        .def("_slot_fields", [](const VectorOwner& v, int64_t i) -> nb::object {
            if (!is_varchar_family(v.vec.type))
                throw std::invalid_argument("_slot_fields: requires a VARCHAR/NVARCHAR/VARBINARY vector");
            auto len = static_cast<int64_t>(v.vec.length);
            if (i < 0) i += len;
            if (i < 0 || i >= len)
                throw nb::index_error("vector index out of range");
            const uint32_t idx = static_cast<uint32_t>(i);
            if (!row_is_valid(v.vec, idx)) return nb::none();
            const DrakenStringArena* sa   = static_cast<const DrakenStringArena*>(v.vec.data);
            const DrakenStringSlot*  slot = &sa->slots[v.vec.selection[idx]];
            if (str_is_inline(slot)) {
                PyObject* plen   = PyLong_FromUnsignedLong(slot->inl.length);
                PyObject* pbytes = PyBytes_FromStringAndSize(
                    reinterpret_cast<const char*>(slot->inl.data),
                    static_cast<Py_ssize_t>(STR_INLINE_MAX));
                if (!plen || !pbytes) {
                    Py_XDECREF(plen);
                    Py_XDECREF(pbytes);
                    throw nb::python_error();
                }
                PyObject* tup = PyTuple_Pack(2, plen, pbytes);
                Py_DECREF(plen);
                Py_DECREF(pbytes);
                if (!tup) throw nb::python_error();
                return nb::steal<nb::object>(tup);
            } else {
                PyObject* a0 = PyLong_FromUnsignedLong(slot->ext.length);
                PyObject* a1 = PyLong_FromUnsignedLong(slot->ext.prefix);
                PyObject* a2 = PyLong_FromUnsignedLong(slot->ext.hash32);
                if (!a0 || !a1 || !a2) {
                    Py_XDECREF(a0); Py_XDECREF(a1); Py_XDECREF(a2);
                    throw nb::python_error();
                }
                PyObject* tup = PyTuple_Pack(3, a0, a1, a2);
                Py_DECREF(a0); Py_DECREF(a1); Py_DECREF(a2);
                if (!tup) throw nb::python_error();
                return nb::steal<nb::object>(tup);
            }
        }, nb::arg("i"),
            "Test-only slot inspector for STRING vectors.\n"
            "Short (len<=12): (length, inline_bytes[12]). Long: (length, prefix, hash32).\n"
            "Null row -> None. Not part of the public Vector API.");

    // Morsel: dumb column container; owns nothing in C++.
    nb::class_<Morsel>(m, "Morsel")
        .def(nb::init<>())
        .def("append", &Morsel::append_col, nb::arg("vector"))
        .def("__getitem__", &Morsel::get_col, nb::arg("i"))
        .def("__len__",     &Morsel::size);

    // Factory: Python list → dense int64 Vector.
    m.def("vector_from_sequence", &make_int64_from_sequence, nb::arg("sequence"),
        "Build a dense int64 Vector from a Python sequence. None elements become nulls.\n"
        "All-valid input leaves validity==NULL (normalization invariant).");

    // C.2 factories — constant and dict shapes for testing take/materialize/compress.
    m.def("vector_from_constant",
        [](nb::object value, uint32_t length) {
            return make_int64_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape int64 Vector. value may be None (→ all-null constant).");

    m.def("vector_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_int64_dict(values, codes, nullable);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded int64 Vector.\n"
        "values: list of unique int64 (the dictionary).\n"
        "codes:  list of int (uint32 index per logical row).\n"
        "nullable: optional list of bool (True=valid); omit for all-valid.");

    // D.1 — VARCHAR ingestion (default string type; unchanged behavior).
    m.def("vector_from_string_sequence",
        [](nb::list seq) { return make_string_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense VARCHAR Vector from a Python list[str | None].\n"
        "Elements are UTF-8 encoded at the Python boundary.\n"
        "None elements become null rows.\n"
        "All-valid input leaves validity==NULL (normalization invariant).\n"
        "Raises OverflowError if total arena bytes exceed 4 GB.");

    // E.7 — NVARCHAR ingestion (opt-in UTF-8; codepoint-length ops).
    m.def("vector_from_nvarchar_sequence",
        [](nb::list seq) { return make_nvarchar_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense NVARCHAR Vector from a Python list[str | None].\n"
        "Same storage as VARCHAR (slot+arena). Type tag drives codepoint-length ops.\n"
        "LENGTH returns UTF-8 codepoint count, not byte count.\n"
        "None elements become null rows.");

    // E.7 — VARBINARY ingestion (opaque bytes).
    m.def("vector_from_bytes_sequence",
        [](nb::list seq) { return make_bytes_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense VARBINARY Vector from a Python list[bytes | None].\n"
        "Elements are opaque byte strings; to_pylist() returns Python bytes objects.\n"
        "LENGTH returns byte count. Character ops raise on VARBINARY.\n"
        "None elements become null rows.");

    // D.3 — dict-encoded string ingestion.
    m.def("vector_from_string_dict_sequence",
        [](nb::list seq) { return make_string_dict_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dict-encoded STRING Vector from a Python list[str | None].\n"
        "Deduplicates values: equal strings share one slot; long strings use\n"
        "length/prefix/hash32 fast-reject before exact byte verification.\n"
        "None elements become null rows.\n"
        "is_dict == True; data_length == # unique non-null values.\n"
        "All-null input returns a constant-shape vector (data_length=1).");

    // D.5 — bool ingestion.
    m.def("vector_from_bool_sequence",
        [](nb::list seq) { return make_bool_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense BOOL Vector from a Python list[bool | None].\n"
        "Data is bit-packed (1 bit/row, LSB-first). None elements become null rows.\n"
        "All-valid input leaves validity==NULL (normalization invariant).");

    m.def("vector_from_bool_constant",
        [](nb::object value, uint32_t length) {
            return make_bool_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape BOOL Vector. value may be None (→ all-null constant).");

    m.def("vector_from_bool_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_bool_dict(values, codes, nullable);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded BOOL Vector.\n"
        "values: list of unique bool (the dictionary, bit-packed).\n"
        "codes:  list of int (uint32 code per logical row).\n"
        "nullable: optional list of bool (True=valid); omit for all-valid.");

    // D.6 — int8 ingestion.
    m.def("vector_int8_from_sequence",
        [](nb::list seq) {
            return make_narrow_int_from_sequence<int8_t, DRAKEN_INT8>(seq, "int8");
        },
        nb::arg("sequence"),
        "Build a dense INT8 Vector from a Python list[int | None].\n"
        "Raises OverflowError if any value is outside [-128, 127].\n"
        "None elements become null rows.");
    m.def("vector_int8_from_constant",
        [](nb::object value, uint32_t length) {
            return make_narrow_int_constant<int8_t, DRAKEN_INT8>(value, length, "int8");
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape INT8 Vector. value may be None (→ all-null constant).");
    m.def("vector_int8_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_narrow_int_dict<int8_t, DRAKEN_INT8>(values, codes, nullable, "int8");
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded INT8 Vector.");

    // D.6 — int16 ingestion.
    m.def("vector_int16_from_sequence",
        [](nb::list seq) {
            return make_narrow_int_from_sequence<int16_t, DRAKEN_INT16>(seq, "int16");
        },
        nb::arg("sequence"),
        "Build a dense INT16 Vector from a Python list[int | None].\n"
        "Raises OverflowError if any value is outside [-32768, 32767].\n"
        "None elements become null rows.");
    m.def("vector_int16_from_constant",
        [](nb::object value, uint32_t length) {
            return make_narrow_int_constant<int16_t, DRAKEN_INT16>(value, length, "int16");
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape INT16 Vector. value may be None (→ all-null constant).");
    m.def("vector_int16_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_narrow_int_dict<int16_t, DRAKEN_INT16>(values, codes, nullable, "int16");
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded INT16 Vector.");

    // D.6 — int32 ingestion.
    m.def("vector_int32_from_sequence",
        [](nb::list seq) {
            return make_narrow_int_from_sequence<int32_t, DRAKEN_INT32>(seq, "int32");
        },
        nb::arg("sequence"),
        "Build a dense INT32 Vector from a Python list[int | None].\n"
        "Raises OverflowError if any value is outside [-2147483648, 2147483647].\n"
        "None elements become null rows.");
    m.def("vector_int32_from_constant",
        [](nb::object value, uint32_t length) {
            return make_narrow_int_constant<int32_t, DRAKEN_INT32>(value, length, "int32");
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape INT32 Vector. value may be None (→ all-null constant).");
    m.def("vector_int32_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_narrow_int_dict<int32_t, DRAKEN_INT32>(values, codes, nullable, "int32");
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded INT32 Vector.");

    // D.7 — float32 ingestion.
    m.def("vector_float32_from_sequence",
        [](nb::list seq) {
            return make_float_from_sequence<float, DRAKEN_FLOAT32>(seq);
        },
        nb::arg("sequence"),
        "Build a dense FLOAT32 Vector from a Python list[float | None].\n"
        "Values are canonicalized at ingestion: NaN → quiet NaN, -0.0 → +0.0.\n"
        "None elements become null rows.");
    m.def("vector_float32_from_constant",
        [](nb::object value, uint32_t length) {
            return make_float_constant<float, DRAKEN_FLOAT32>(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape FLOAT32 Vector. value may be None (→ all-null constant).");
    m.def("vector_float32_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_float_dict<float, DRAKEN_FLOAT32>(values, codes, nullable);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded FLOAT32 Vector.");

    // D.7 — float64 ingestion.
    m.def("vector_float64_from_sequence",
        [](nb::list seq) {
            return make_float_from_sequence<double, DRAKEN_FLOAT64>(seq);
        },
        nb::arg("sequence"),
        "Build a dense FLOAT64 Vector from a Python list[float | None].\n"
        "Values are canonicalized at ingestion: NaN → quiet NaN, -0.0 → +0.0.\n"
        "None elements become null rows.");
    m.def("vector_float64_from_constant",
        [](nb::object value, uint32_t length) {
            return make_float_constant<double, DRAKEN_FLOAT64>(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape FLOAT64 Vector. value may be None (→ all-null constant).");
    m.def("vector_float64_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_float_dict<double, DRAKEN_FLOAT64>(values, codes, nullable);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded FLOAT64 Vector.");

    // D.8 — TIMESTAMP64 ingestion.
    //
    // unit:           "s" / "ms" / "us" / "ns" — storage resolution.
    // offset_minutes: fixed UTC offset in minutes (e.g., 60 for +01:00, -330 for
    //                 -05:30).  Pass 0 for UTC.  Determines the timezone embedded in
    //                 datetime objects produced by readback.
    //
    // Ingestion semantics:
    //   - timezone-aware datetimes: UTC offset is subtracted to produce a UTC instant.
    //   - timezone-naive datetimes: treated as UTC (no offset applied).
    //   - None:                     becomes a null row.
    //
    // Readback semantics (to_pylist / __getitem__):
    //   UTC instant + offset_minutes → timezone-aware datetime with the stored offset.
    //
    // A TIMESTAMP64 vector without a logical-type descriptor is a HARD ERROR —
    // these factories always create one; use only these factories.
    m.def("vector_timestamp_from_sequence",
        [](nb::list seq, std::string unit_str, int offset_minutes) {
            if (offset_minutes < -1439 || offset_minutes > 1439)
                throw std::invalid_argument(
                    "offset_minutes must be in [-1439, +1439]");
            return make_timestamp_from_sequence(
                seq, str_to_unit(unit_str), static_cast<int16_t>(offset_minutes));
        },
        nb::arg("sequence"), nb::arg("unit") = "us", nb::arg("offset_minutes") = 0,
        "Build a dense TIMESTAMP64 Vector from a Python list[datetime | None].\n"
        "unit: storage resolution (\"s\"/\"ms\"/\"us\"/\"ns\").\n"
        "offset_minutes: fixed UTC offset in minutes (0 = UTC).\n"
        "Timezone-aware datetimes are converted to UTC; naive datetimes treated as UTC.\n"
        "None elements become null rows.\n"
        "Raises if any element is neither datetime nor None.");

    m.def("vector_timestamp_from_constant",
        [](nb::object value, uint32_t length, std::string unit_str, int offset_minutes) {
            if (offset_minutes < -1439 || offset_minutes > 1439)
                throw std::invalid_argument(
                    "offset_minutes must be in [-1439, +1439]");
            return make_timestamp_constant(
                value, length, str_to_unit(unit_str),
                static_cast<int16_t>(offset_minutes));
        },
        nb::arg("value").none(true), nb::arg("length"),
        nb::arg("unit") = "us", nb::arg("offset_minutes") = 0,
        "Build a constant-shape TIMESTAMP64 Vector.\n"
        "value may be None (→ all-null constant).");

    m.def("vector_timestamp_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable,
           std::string unit_str, int offset_minutes) {
            if (offset_minutes < -1439 || offset_minutes > 1439)
                throw std::invalid_argument(
                    "offset_minutes must be in [-1439, +1439]");
            return make_timestamp_dict(
                values, codes, nullable, str_to_unit(unit_str),
                static_cast<int16_t>(offset_minutes));
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        nb::arg("unit") = "us", nb::arg("offset_minutes") = 0,
        "Build a dict-encoded TIMESTAMP64 Vector.\n"
        "values: list of datetime (unique dictionary entries); None becomes a null slot.\n"
        "codes:  list of int (uint32 code per logical row).\n"
        "nullable: optional list of bool (True=valid); omit for all-valid.");

    // D.9 — DATE32 ingestion.
    // No logical descriptor (not parameterized); physical int32 = days since 1970-01-01.
    m.def("vector_date32_from_sequence",
        [](nb::list seq) { return make_date32_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense DATE32 Vector from a Python list[date | None].\n"
        "Values are stored as int32 days since 1970-01-01.\n"
        "None elements become null rows.\n"
        "Accepts datetime.date and datetime.datetime (date part extracted).");

    m.def("vector_date32_from_constant",
        [](nb::object value, uint32_t length) {
            return make_date32_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape DATE32 Vector. value may be None (→ all-null constant).");

    m.def("vector_date32_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable) {
            return make_date32_dict(values, codes, nullable);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        "Build a dict-encoded DATE32 Vector.\n"
        "values: list of unique date (the dictionary); None becomes a null slot.\n"
        "codes:  list of int (uint32 code per logical row).\n"
        "nullable: optional list of bool (True=valid); omit for all-valid.");

    // D.9 — TIME32 ingestion (unit ∈ {"s", "ms"}; mandatory logical descriptor).
    m.def("vector_time32_from_sequence",
        [](nb::list seq, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::SECONDS && u != TimestampUnit::MILLISECONDS)
                throw std::invalid_argument(
                    "TIME32 unit must be \"s\" or \"ms\"");
            return make_time_from_sequence<int32_t, DRAKEN_TIME32>(seq, u);
        },
        nb::arg("sequence"), nb::arg("unit") = "s",
        "Build a dense TIME32 Vector from a Python list[time | None].\n"
        "unit: \"s\" or \"ms\" — storage resolution.\n"
        "Values are stored as int32 time-of-day in the given unit.\n"
        "None elements become null rows. Mandatory unit descriptor attached.");

    m.def("vector_time32_from_constant",
        [](nb::object value, uint32_t length, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::SECONDS && u != TimestampUnit::MILLISECONDS)
                throw std::invalid_argument("TIME32 unit must be \"s\" or \"ms\"");
            return make_time_constant<int32_t, DRAKEN_TIME32>(value, length, u);
        },
        nb::arg("value").none(true), nb::arg("length"), nb::arg("unit") = "s",
        "Build a constant-shape TIME32 Vector. value may be None (→ all-null constant).");

    m.def("vector_time32_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::SECONDS && u != TimestampUnit::MILLISECONDS)
                throw std::invalid_argument("TIME32 unit must be \"s\" or \"ms\"");
            return make_time_dict<int32_t, DRAKEN_TIME32>(values, codes, nullable, u);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        nb::arg("unit") = "s",
        "Build a dict-encoded TIME32 Vector.");

    // D.9 — TIME64 ingestion (unit ∈ {"us", "ns"}; mandatory logical descriptor).
    m.def("vector_time64_from_sequence",
        [](nb::list seq, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::MICROSECONDS && u != TimestampUnit::NANOSECONDS)
                throw std::invalid_argument(
                    "TIME64 unit must be \"us\" or \"ns\"");
            return make_time_from_sequence<int64_t, DRAKEN_TIME64>(seq, u);
        },
        nb::arg("sequence"), nb::arg("unit") = "us",
        "Build a dense TIME64 Vector from a Python list[time | None].\n"
        "unit: \"us\" or \"ns\" — storage resolution.\n"
        "Values are stored as int64 time-of-day in the given unit.\n"
        "None elements become null rows. Mandatory unit descriptor attached.");

    m.def("vector_time64_from_constant",
        [](nb::object value, uint32_t length, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::MICROSECONDS && u != TimestampUnit::NANOSECONDS)
                throw std::invalid_argument("TIME64 unit must be \"us\" or \"ns\"");
            return make_time_constant<int64_t, DRAKEN_TIME64>(value, length, u);
        },
        nb::arg("value").none(true), nb::arg("length"), nb::arg("unit") = "us",
        "Build a constant-shape TIME64 Vector. value may be None (→ all-null constant).");

    m.def("vector_time64_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable, std::string unit_str) {
            TimestampUnit u = str_to_unit(unit_str);
            if (u != TimestampUnit::MICROSECONDS && u != TimestampUnit::NANOSECONDS)
                throw std::invalid_argument("TIME64 unit must be \"us\" or \"ns\"");
            return make_time_dict<int64_t, DRAKEN_TIME64>(values, codes, nullable, u);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        nb::arg("unit") = "us",
        "Build a dict-encoded TIME64 Vector.");

    // D.10 — DECIMAL ingestion.
    // Logical DECIMAL(precision, scale): physical int64 unscaled value.
    // precision: 1..18 (fits unscaled value in int64; pt2 adds int128 for p>18).
    // scale:     0..precision (digits to the right of the decimal point).
    // Mandatory logical descriptor attached to all three shapes.
    // Ingestion accepts decimal.Decimal values.  Fails loud on:
    //   NaN/Inf, sub-scale precision (more decimal places than scale),
    //   value exceeding declared precision, or overflow of int64 range.
    m.def("vector_decimal_from_sequence",
        [](nb::list seq, int precision, int scale) {
            if (precision < 1 || precision > 18)
                throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL scale must be in [0, precision]");
            return make_decimal_from_sequence(
                seq, static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
        },
        nb::arg("sequence"), nb::arg("precision"), nb::arg("scale"),
        "Build a dense DECIMAL Vector from a Python list[Decimal | None].\n"
        "precision: total significant digits (1..18).\n"
        "scale: digits right of decimal point (0..precision).\n"
        "None elements become null rows.\n"
        "Raises on NaN/Inf, sub-scale precision, exceeded precision, or int64 overflow.\n"
        "All-valid input leaves validity==NULL (normalization invariant).");

    m.def("vector_decimal_from_constant",
        [](nb::object value, uint32_t length, int precision, int scale) {
            if (precision < 1 || precision > 18)
                throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL scale must be in [0, precision]");
            return make_decimal_constant(
                value, length,
                static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
        },
        nb::arg("value").none(true), nb::arg("length"),
        nb::arg("precision"), nb::arg("scale"),
        "Build a constant-shape DECIMAL Vector. value may be None (→ all-null constant).");

    m.def("vector_decimal_from_dict",
        [](nb::list values, nb::list codes, nb::object nullable,
           int precision, int scale) {
            if (precision < 1 || precision > 18)
                throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL scale must be in [0, precision]");
            return make_decimal_dict(
                values, codes, nullable,
                static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nullable") = nb::none(),
        nb::arg("precision"), nb::arg("scale"),
        "Build a dict-encoded DECIMAL Vector.\n"
        "values: list of unique Decimal (the dictionary); None becomes a null slot.\n"
        "codes:  list of int (uint32 code per logical row).\n"
        "nullable: optional list of bool (True=valid); omit for all-valid.");

    // D.11 — NULL vector factory.
    // A null vector carries no data and no validity buffer; type tag alone is the signal.
    // Every row is null. Build from a row count or equivalently from [None]*n.
    m.def("vector_null_from_length",
        [](uint32_t length) { return make_null_vector(length); },
        nb::arg("length"),
        "Build a NULL Vector of the given length. All rows are null.\n"
        "No data or validity buffers are allocated; type==NULL is self-describing.\n"
        "Equivalent to ingesting [None]*length but without allocating any storage.");

    // D.11 — fp16 embedding vector factory.
    // Physical: uint16_t[length * dimension] — dimension fp16 values per row.
    // dimension is mandatory and carried in the logical-type descriptor.
    m.def("vector_fp16_from_sequence",
        [](nb::list seq, int dimension) {
            if (dimension < 1)
                throw std::invalid_argument(
                    "vector_fp16_from_sequence: dimension must be >= 1");
            return make_fp16_from_sequence(seq, static_cast<uint32_t>(dimension));
        },
        nb::arg("sequence"), nb::arg("dimension"),
        "Build a dense VECTOR_FP16 Vector from a Python list[list[float] | None].\n"
        "dimension: number of fp16 values per row (mandatory, >= 1).\n"
        "Each non-null row must be a list of exactly dimension floats.\n"
        "Raises ValueError if any row's length != dimension.\n"
        "None elements become null rows.\n"
        "Conversion: float -> fp16 via IEEE 754 round-to-nearest (lossy).\n"
        "Unsupported ops: ordering, arithmetic, similarity (throw).");

    // D.12 — INTERVAL ingestion.
    m.def("vector_interval_from_sequence",
        [](nb::list seq) {
            return make_interval_from_sequence(seq);
        },
        nb::arg("sequence"),
        "Build a dense INTERVAL Vector from a Python list[(months: int, ms: int) | None].\n"
        "Each non-null element must be a 2-tuple (months: int, ms: int).\n"
        "None elements become null rows.\n"
        "Raises OverflowError if months × 2_592_000_000 + ms overflows int64.\n"
        "Normalized comparison: total_ms = months × 2_592_000_000 + ms (30-day month).\n"
        "Arithmetic: component-wise (months and ms independently, not normalized).");

    m.def("vector_interval_from_constant",
        [](nb::object value, uint32_t length) {
            return make_interval_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant INTERVAL Vector from a single (months, ms) tuple or None.\n"
        "None produces a length-row all-null vector.\n"
        "Raises OverflowError if normalization overflows int64.");

    m.def("vector_interval_from_dict",
        [](nb::list values, nb::list codes, nb::object nulls) {
            return make_interval_dict(values, codes, nulls);
        },
        nb::arg("values"), nb::arg("codes"), nb::arg("nulls") = nb::none(),
        "Build a dict-encoded INTERVAL Vector.\n"
        "values: list of distinct (months, ms) tuples (the dictionary).\n"
        "codes: list of uint32 indices into values.\n"
        "nulls: optional bytes validity bitmap (1 bit per logical row, LSB first); "
        "None = all valid.\n"
        "Raises OverflowError if any slot's normalization overflows int64.");

    // D.13 — DRAKEN_ARRAY ingestion.
    //
    // Physical: int32 offsets[length+1] + owned child DrakenVector (RAII chains).
    // Child type is inferred from the first non-null, non-empty element:
    //   int   → DRAKEN_INT64
    //   str   → DRAKEN_VARCHAR
    //   list  → DRAKEN_ARRAY  (recursive: array-of-array)
    //
    // None rows → null (validity bit cleared).
    // [] rows   → empty sublist (valid, zero-length slice in child).
    //
    // Ownership: parent owns child; freeing parent frees entire subtree.
    m.def("vector_array_from_sequence",
        [](nb::list seq) { return make_array_from_sequence(seq); },
        nb::arg("sequence"),
        "Build a dense DRAKEN_ARRAY Vector from a Python list[list | None].\n"
        "Each non-null element must be a list (or tuple) of homogeneous elements.\n"
        "Child type inferred: int → INT64, str → STRING, list → ARRAY (recursive).\n"
        "None elements become null rows (validity bit cleared).\n"
        "[] elements become valid empty sublists (distinct from null).\n"
        "Parent owns child; RAII destructor frees the entire nested subtree.\n"
        "Unsupported ops: hash, compare, between, in_list, sum/min/max, arithmetic.");

    // -------------------------------------------------------------------------
    // E.1 bridge test helpers (not production API; prefixed _bridge_test_).
    //
    // These exercise draken_vector_unwrap / draken_vector_own_raw / draken_vector_own
    // via Python-visible entry points so draken/tests/native/test_bridge.py can
    // validate correctness without requiring a separate Cython extension build.
    // -------------------------------------------------------------------------

    // _bridge_test_unwrap_sum(vec) → int
    // Unwraps vec, calls i64_sum nogil, returns the sum as a Python int.
    // Raises TypeError if vec is not a Vector.
    m.def("_bridge_test_unwrap_sum",
        [](nb::object vec) -> nb::object {
            const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
            if (!dv) throw nb::python_error();
            int64_t sum_val = 0;
            uint32_t nonnull;
            {
                nb::gil_scoped_release _;
                nonnull = draken::ops::i64_sum(*dv, &sum_val);
            }
            (void)nonnull;
            return nb::cast(sum_val);
        },
        nb::arg("vec"),
        "Bridge test: unwrap Vector, call i64_sum nogil, return sum as int.");

    // _bridge_test_neg_via_own(vec) → Vector
    // Unwraps vec, calls i64_neg (returns VecResult), wraps via draken_vector_own.
    // Exercises the full round-trip: Python Vector → unwrap → nogil op → own → Vector.
    m.def("_bridge_test_neg_via_own",
        [](nb::object vec) -> nb::object {
            const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
            if (!dv) throw nb::python_error();
            VecResult r;
            {
                nb::gil_scoped_release _;
                r = draken::ops::i64_neg(*dv);
            }
            PyObject* result = draken_vector_own(r);
            if (!result) throw nb::python_error();
            return nb::steal<nb::object>(result);
        },
        nb::arg("vec"),
        "Bridge test: unwrap int64 Vector, negate via i64_neg, wrap via draken_vector_own.");

    // _bridge_test_own_raw(values) → Vector
    // Allocates an int64 buffer with draken_malloc, fills it, wraps via draken_vector_own_raw.
    // Exercises the hand-allocation + own_raw path.
    m.def("_bridge_test_own_raw",
        [](nb::list values) -> nb::object {
            const uint32_t n = static_cast<uint32_t>(values.size());
            int64_t* data = static_cast<int64_t*>(draken_malloc(
                (n > 0u ? n : 1u) * sizeof(int64_t)));
            if (!data) throw std::bad_alloc();

            bool has_nulls = false;
            for (uint32_t i = 0; i < n; ++i) {
                nb::object obj = values[i];
                if (obj.is_none()) {
                    data[i] = 0;
                    has_nulls = true;
                } else {
                    data[i] = nb::cast<int64_t>(obj);
                }
            }

            uint8_t* validity = nullptr;
            if (has_nulls) {
                const uint32_t bm     = (n + 7u) / 8u;
                const uint32_t padded = ((bm + 7u) & ~7u);
                const size_t   vbytes = padded > 0u ? padded : 8u;
                validity = static_cast<uint8_t*>(draken_malloc(vbytes));
                if (!validity) { draken_free(data); throw std::bad_alloc(); }
                std::memset(validity, 0xFF, vbytes);
                for (uint32_t i = 0; i < n; ++i) {
                    if (values[i].is_none())
                        validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
                }
            }

            PyObject* result = draken_vector_own_raw(data, validity, n, DRAKEN_INT64);
            if (!result) throw nb::python_error();
            return nb::steal<nb::object>(result);
        },
        nb::arg("values"),
        "Bridge test: allocate int64 buffer via draken_malloc, wrap via draken_vector_own_raw.");

    // _bridge_test_type_error() — verify draken_vector_unwrap raises TypeError on non-Vector.
    m.def("_bridge_test_type_error",
        []() {
            PyObject* not_a_vector = PyLong_FromLong(42);
            const DrakenVector* dv = draken_vector_unwrap(not_a_vector);
            Py_DECREF(not_a_vector);
            if (dv != nullptr) {
                PyErr_SetString(PyExc_AssertionError,
                    "draken_vector_unwrap should have returned nullptr for non-Vector");
                throw nb::python_error();
            }
            // TypeError is set by draken_vector_unwrap; re-raise it.
            throw nb::python_error();
        },
        "Bridge test: draken_vector_unwrap on non-Vector must raise TypeError.");
}
