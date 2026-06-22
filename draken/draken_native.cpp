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
// No import opteryx. No fallback to a legacy implementation.

#include <Python.h>
#include <datetime.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/shared_ptr.h>   // S0: shared_ptr<VectorOwner> seam (CxxMorsel)

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "fast_float.h"
#include "ryu.h"
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "core/interval_slot.h"
#include "utf8.h"                    // E.26: UTF-8 validation (utf8nvalid) for NVARCHAR
#include "logical_type.h"
#include "fp16/fp16.h"   // fp16_ieee_from_fp32_value / fp16_ieee_to_fp32_value (D.11)
#include "ops/bool_logical.h"
#include "ops/bool_reductions.h"
#include "ops/hash.h"               // includes decimal_arith.h transitively (E.32)
#include "ops/int64_arithmetic.h"   // i64_neg (used by bridge round-trip test)
#include "ops/int64_reductions.h"   // i64_sum (used by bridge round-trip test)
#include "ops/float_ops.h"          // fp_total_lt (used by compare_at row ordering)
#include "ops/string_gather.h"  // sg_eq_slots, str_hash_seed (for dict ingestion)
#include "core/draken_bridge.h"     // bridge surface declarations
#include "core/frame_arena.h"       // per-frame allocator
#include "ops/compare_dv.h"         // arena-backed compare entry point
#include "ops/arithmetic_dv.h"      // arena-backed arithmetic entry point

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Ownership primitives (doc 01) — DrakenFree / OwnedBuffer / VectorOwner now live
// in core/vector_owner.h so the native scan + C++-first CxxMorsel can share them.
// ---------------------------------------------------------------------------
#include "core/vector_owner.h"
#include "morsels/cxx_morsel.h"       // S0: C++-first CxxMorsel / CxxColumn
#include "morsels/cxx_morsel_ops.h"   // S0: nogil morsel-op surface (cxx_hash, ...)

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
            // VARCHAR carries raw bytes verbatim. A Python str must never reach
            // this edge — callers encode to bytes (CLAUDE.md §1). Reject str.
            PyObject* pybytes = obj.ptr();
            if (!PyBytes_Check(pybytes))
                throw std::invalid_argument(
                    "vector_from_string_sequence: element is not bytes or None "
                    "(str must be encoded to bytes by the caller)");
            char* bptr = nullptr;
            Py_ssize_t slen = 0;
            if (PyBytes_AsStringAndSize(pybytes, &bptr, &slen) < 0)
                throw nb::python_error();
            ptrs[i] = bptr;
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
    // Zero struct + slots (clean inline-slot padding / null slots) and, separately,
    // the validity region. The arena [arena_start, validity_start) is fully written
    // by the payload memcpys below for valid long strings; its unused tail beyond
    // arena_used is never read, so it does not need zeroing.
    std::memset(block, 0, arena_start);
    if (has_nulls)
        std::memset(block + validity_start, 0, validity_bytes);
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
// D.1c: constant-shape VARCHAR vector (data_length == 1, length rows).
//
// value_obj may be None → all-null constant (data_length=1, all rows null).
// Non-null: one slot in the arena, selection = global zero vector.
//
// Ownership model mirrors make_string_from_sequence: all storage lives in one
// mimalloc block [DrakenStringArena | DrakenStringSlot[1] | arena_bytes | validity].
// data_buf owns the block; validity is embedded in the block.
// ---------------------------------------------------------------------------

// Unified constant-shape string builder for the VARCHAR / NVARCHAR / VARBINARY
// family. value_obj is bytes (stored verbatim) or None (→ all-null constant).
// A Python str must NEVER reach this edge — string literals are encoded to bytes
// at the binder/planner (CLAUDE.md §1).
//
// validate_utf8 (NVARCHAR only): the bytes are checked with utf8nvalid before
// storage; invalid UTF-8 fails loud. VARCHAR/VARBINARY store bytes unvalidated.
static VectorOwner make_string_constant(nb::object value_obj, uint32_t length,
                                        DrakenType type, bool validate_utf8) {
    const bool is_null = value_obj.is_none();

    const char*  bytes_ptr = nullptr;
    Py_ssize_t   bytes_len = 0;
    size_t       arena_ext = 0u;  // bytes needed in arena (> 0 only if long form)

    if (!is_null) {
        PyObject* pyobj = value_obj.ptr();
        if (!PyBytes_Check(pyobj)) {
            throw std::invalid_argument(
                "string constant: value must be bytes or None "
                "(str must be encoded to bytes at the binder)");
        }
        if (PyBytes_AsStringAndSize(pyobj, const_cast<char**>(&bytes_ptr), &bytes_len) < 0)
            throw nb::python_error();
        if (validate_utf8 && bytes_len > 0 &&
            utf8nvalid(reinterpret_cast<const utf8_int8_t*>(bytes_ptr),
                       static_cast<size_t>(bytes_len)) != nullptr) {
            throw std::invalid_argument("nvarchar constant: value is not valid UTF-8");
        }
        if (bytes_len > STR_INLINE_MAX)
            arena_ext = static_cast<size_t>(bytes_len);
    }

    // Single-block layout: [DrakenStringArena | DrakenStringSlot[1] | arena_bytes | validity]
    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes    = sizeof(DrakenStringSlot);   // exactly 1 slot
    const size_t arena_start    = struct_end + slots_bytes;
    const size_t validity_start = arena_start + arena_ext;

    size_t validity_bytes = 0u;
    if (is_null) {
        // All rows null: need a validity bitmap (all bits 0).
        const uint32_t bm     = (length + 7u) / 8u;
        const uint32_t padded = ((bm + 7u) & ~7u);
        validity_bytes = (padded > 0u) ? padded : 8u;
    }

    const size_t total_alloc = validity_start + validity_bytes;
    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena)));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena));
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa    = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slot  = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena = (arena_ext > 0u) ? (block + arena_start) : nullptr;
    uint8_t*           bitmap = is_null ? (block + validity_start) : nullptr;

    sa->slots        = slot;
    sa->arena        = arena;
    sa->length       = 1u;             // one unique value in the dict
    sa->arena_used   = arena_ext;
    sa->arena_cap    = arena_ext;
    sa->null_bitmap  = nullptr;        // validity is on the DrakenVector, not the arena
    sa->owns_buffers = 0;
    sa->type         = type;

    if (!is_null) {
        const uint8_t* src  = reinterpret_cast<const uint8_t*>(bytes_ptr);
        const uint32_t slen = static_cast<uint32_t>(bytes_len);
        if (slen <= STR_INLINE_MAX) {
            str_init_inline(slot, src, slen);
        } else {
            // Write bytes to arena at offset 0 (this is the only slot).
            std::memcpy(arena, src, slen);
            str_init_extern(slot, src, slen,
                            static_cast<uint32_t>(XXH3_64bits(src, slen)), 0u);
        }
    }
    // is_null: slot is already zeroed (null sentinel: length == 0, all bytes zero).

    // Constant shape: one data slot broadcast over `length` rows.
    DrakenVector v = draken_vector_from_constant(sa, length, type, bitmap);
    return VectorOwner(v, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
}

static VectorOwner make_varchar_constant(nb::object value_obj, uint32_t length) {
    return make_string_constant(value_obj, length, DRAKEN_VARCHAR, /*validate_utf8=*/false);
}

static VectorOwner make_nvarchar_constant(nb::object value_obj, uint32_t length) {
    return make_string_constant(value_obj, length, DRAKEN_NVARCHAR, /*validate_utf8=*/true);
}

// ---------------------------------------------------------------------------
// E.7b — VARBINARY constant (bytes or None → constant-shape DRAKEN_VARBINARY vector).
// Mirrors make_varchar_constant but accepts Python bytes and tags the result VARBINARY.
// ---------------------------------------------------------------------------
static VectorOwner make_varbinary_constant(nb::object value_obj, uint32_t length) {
    return make_string_constant(value_obj, length, DRAKEN_VARBINARY, /*validate_utf8=*/false);
}

// ---------------------------------------------------------------------------
// Constant length-adjust view — borrow an existing constant-shape Vector's single
// data slot and present it with a new logical length. Zero-copy: the returned
// Vector owns nothing (data/validity/codes buffers are null); the source's data
// is kept alive via nb::keep_alive on the binding. selection/validity are the
// shared global zero buffers sized for `length`.
//
// Used by the bytecode executor's cold path (_slot_to_pyobj): the hot path
// re-stamps the cached bind-time constant straight into its DV stack (no Python
// object), and only when a Python-fallback kernel needs a real Vector do we
// materialise this borrowed view at the correct logical length.
// ---------------------------------------------------------------------------
static VectorOwner make_constant_view(const VectorOwner& src, uint32_t length) {
    if (src.vec.data_length != 1u)
        throw std::invalid_argument(
            "vector_constant_view: source must be a constant-shape vector (data_length==1)");
    DrakenVector v = src.vec;                 // borrow the single-slot data pointer
    v.length    = length;
    v.selection = draken_zero_sel(length);
    if (v.validity != nullptr)
        v.validity = const_cast<uint8_t*>(draken_zero_validity(length));
    VectorOwner o(v, OwnedBuffer<void>(nullptr), OwnedBuffer<uint8_t>(nullptr));
    o.logical_type = src.logical_type;        // preserve TIMESTAMP/DECIMAL descriptors
    return o;
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
            // VARCHAR carries raw bytes verbatim; reject str (encode at caller).
            PyObject* pybytes = obj.ptr();
            if (!PyBytes_Check(pybytes))
                throw std::invalid_argument(
                    "vector_from_string_dict_sequence: element is not bytes or None "
                    "(str must be encoded to bytes by the caller)");
            char* bptr = nullptr;
            Py_ssize_t slen = 0;
            if (PyBytes_AsStringAndSize(pybytes, &bptr, &slen) < 0)
                throw nb::python_error();
            ptrs[i] = bptr;
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

// Compare two logical rows WITHIN the same vector. Returns -1/0/1.
// Used by ORDER BY (heap_sort) for stable ordering. Null handling is the
// caller's responsibility (heap_sort checks is_null_at first); this
// function compares values only and assumes both rows are non-null.
//
// Both rows share the same vector → same logical type, same scale (for
// decimal), same string arena. Comparison is the natural value order,
// with float using total-order (NaN highest) for sort stability.
static int draken_vector_compare_at(const DrakenVector& v,
                                    uint32_t li, uint32_t lj) {
    const uint32_t a = v.selection[li];
    const uint32_t b = v.selection[lj];

    switch (v.type) {
        case DRAKEN_INT8: {
            const int8_t* d = static_cast<const int8_t*>(v.data);
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }
        case DRAKEN_INT16: {
            const int16_t* d = static_cast<const int16_t*>(v.data);
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32: {
            const int32_t* d = static_cast<const int32_t*>(v.data);
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:        // same-vector → same scale; unscaled order == value order
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64: {
            const int64_t* d = static_cast<const int64_t*>(v.data);
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }
        case DRAKEN_DECIMAL128: {   // same-vector → same scale; unscaled int128 order == value order
            const __int128* d = static_cast<const __int128*>(v.data);
            return (d[a] < d[b]) ? -1 : (d[a] > d[b]) ? 1 : 0;
        }
        case DRAKEN_FLOAT32: {
            const float* d = static_cast<const float*>(v.data);
            // Total order: NaN sorts highest, -0.0 == 0.0 (canonicalised at ingest).
            if (draken::ops::fp_total_lt(d[a], d[b])) return -1;
            if (draken::ops::fp_total_lt(d[b], d[a])) return 1;
            return 0;
        }
        case DRAKEN_FLOAT64: {
            const double* d = static_cast<const double*>(v.data);
            if (draken::ops::fp_total_lt(d[a], d[b])) return -1;
            if (draken::ops::fp_total_lt(d[b], d[a])) return 1;
            return 0;
        }
        case DRAKEN_BOOL: {
            const uint8_t* d = static_cast<const uint8_t*>(v.data);
            const int va = (d[a / 8u] >> (a % 8u)) & 1u;
            const int vb = (d[b / 8u] >> (b % 8u)) & 1u;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY: {
            const DrakenStringArena* sa =
                static_cast<const DrakenStringArena*>(v.data);
            const int c = str_compare(&sa->slots[a], sa->arena,
                                      &sa->slots[b], sa->arena);
            return (c < 0) ? -1 : (c > 0) ? 1 : 0;
        }
        case DRAKEN_INTERVAL: {
            const DrakenIntervalSlot* s =
                static_cast<const DrakenIntervalSlot*>(v.data);
            // Total µs ordering (months normalised at the documented 30-day rate).
            const int64_t ta = s[a].months * INTERVAL_MONTH_US + s[a].us;
            const int64_t tb = s[b].months * INTERVAL_MONTH_US + s[b].us;
            return (ta < tb) ? -1 : (ta > tb) ? 1 : 0;
        }
        default:
            // ARRAY / VECTOR_FP16 / NULL / NON_NATIVE — no natural sort order.
            throw std::invalid_argument(
                "compare_at: ordering not supported for this vector type");
    }
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

// D.11: fp16 zeros — allocate a fresh dense VECTOR_FP16 Vector of (length, dimension)
// with all values zero and no nulls. Used by callers that build embedding matrices
// row-by-row (e.g. opteryx/vectors/vector_math.pyx new_matrix). The returned Vector's
// data buffer is mutable through the unified() pointer, so callers can write rows
// in place after construction.
static VectorOwner make_fp16_zeros(uint32_t length, uint32_t dimension) {
    if (dimension == 0u)
        throw std::invalid_argument("vector_fp16_zeros: dimension must be >= 1");

    const size_t data_bytes = static_cast<size_t>(length > 0u ? length : 1u)
                              * dimension * sizeof(uint16_t);
    uint16_t* data = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, data_bytes);
    OwnedBuffer<void> data_buf(data);

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_VECTOR_FP16, nullptr);
    VectorOwner owner(v, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));

    LogicalType lt;
    lt.kind      = LogicalKind::VECTOR;
    lt.dimension = dimension;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// D.11: fp16 with-nulls — allocate a fresh dense VECTOR_FP16 Vector of (length, dimension)
// with all rows initially null. Arrow validity convention: bit=1 = valid, bit=0 = null.
// Initial validity bitmap is memset to 0 (all null); callers SET bits in
// vec.unified()->validity to mark rows present, then write into vec.unified()->data.
// Companion to make_fp16_zeros.
static VectorOwner make_fp16_with_nulls(uint32_t length, uint32_t dimension) {
    if (dimension == 0u)
        throw std::invalid_argument("vector_fp16_with_nulls: dimension must be >= 1");

    const size_t data_bytes = static_cast<size_t>(length > 0u ? length : 1u)
                              * dimension * sizeof(uint16_t);
    uint16_t* data = static_cast<uint16_t*>(draken_malloc(data_bytes));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, data_bytes);
    OwnedBuffer<void> data_buf(data);

    // All-null validity bitmap: bit=0 means null per Arrow/Draken convention.
    const uint32_t bm     = (length + 7u) / 8u;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* validity = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!validity) throw std::bad_alloc();
    std::memset(validity, 0, vbytes);
    OwnedBuffer<uint8_t> validity_buf(validity);

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

// D.12: bool take — gather logical rows of a bit-packed DRAKEN_BOOL vector by index.
// Uniform access pattern: data[selection[i]] at bit level.
static VectorOwner make_bool_take(const VectorOwner& v,
                                   const int32_t* indices, uint32_t n) {
    if (v.vec.type != DRAKEN_BOOL)
        throw std::invalid_argument("make_bool_take: expected DRAKEN_BOOL");

    const uint8_t* src_data = static_cast<const uint8_t*>(v.vec.data);

    const uint32_t bm_out    = (n + 7u) >> 3;
    const size_t   alloc_out = (bm_out > 0u) ? static_cast<size_t>((bm_out + 7u) & ~7u) : 8u;
    uint8_t* out_data = static_cast<uint8_t*>(draken_malloc(alloc_out));
    if (!out_data) throw std::bad_alloc();
    std::memset(out_data, 0, alloc_out);
    OwnedBuffer<void> data_buf(out_data);

    bool has_nulls = false;
    for (uint32_t i = 0u; i < n; ++i) {
        int32_t idx = indices[i];
        const int32_t vlen = static_cast<int32_t>(v.vec.length);
        if (idx < 0) idx += vlen;
        if (idx < 0 || idx >= vlen)
            throw nb::index_error("take: index out of range");
        if (!row_is_valid(v.vec, static_cast<uint32_t>(idx))) {
            has_nulls = true;
        } else {
            const uint32_t code = v.vec.selection[static_cast<uint32_t>(idx)];
            const uint32_t bit  = (src_data[code >> 3] >> (code & 7u)) & 1u;
            if (bit)
                out_data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
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

    DrakenVector vr = draken_vector_from_dense(out_data, n, DRAKEN_BOOL, validity);
    return VectorOwner(vr, std::move(data_buf), std::move(validity_buf));
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
    // Phase 9c: when validity is embedded in the data block (string-family
    // output), it is freed with the block — do NOT own it as a second buffer.
    OwnedBuffer<uint8_t> val_buf(r.validity_embedded ? nullptr : r.validity);
    OwnedBuffer<void>    codes_buf(r.owns_selection
                                    ? const_cast<void*>(static_cast<const void*>(r.selection))
                                    : nullptr);
    VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));

    // Phase 9c: attach the timestamp unit descriptor when the kernel set one.
    // DrakenVector/VecResult carry no LogicalType; it lives on the VectorOwner.
    if (r.type == DRAKEN_TIMESTAMP64 && r.ts_unit != 0xFFu) {
        LogicalType lt;
        lt.kind           = LogicalKind::TIMESTAMP;
        lt.unit           = static_cast<TimestampUnit>(r.ts_unit);
        lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);
    }
    // S-A.2: attach the DECIMAL precision/scale descriptor when the kernel set one
    // (dec_precision > 0). Mirrors the timestamp block; the arena DV*/VecResult
    // carry no LogicalType, so DECIMAL results would otherwise fail to_pylist.
    if ((r.type == DRAKEN_DECIMAL || r.type == DRAKEN_DECIMAL128) && r.dec_precision > 0u) {
        LogicalType lt;
        lt.kind      = LogicalKind::DECIMAL;
        lt.precision = r.dec_precision;
        lt.scale     = r.dec_scale;
        owner.logical_type = logical_type_intern(lt);
    }
    return owner;
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

// draken_vector_own_dict_i64 — wrap hand-allocated dict-encoded int64 buffers in a new Vector.
//
// data: draken_malloc'd int64_t[data_length] unique values (dictionary).
// codes: draken_malloc'd uint32_t[length] per-row selection codes.
// Ownership of all non-NULL buffers is transferred.
extern "C" PyObject* draken_vector_own_dict_i64(
    void* data, uint32_t data_length,
    uint32_t* codes, uint32_t length,
    uint8_t* validity)
{
    try {
        DrakenVector v = draken_vector_from_dict(data, data_length,
                                                  codes, length,
                                                  DRAKEN_INT64, validity);
        OwnedBuffer<void>    data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void>    codes_buf(static_cast<void*>(codes));
        VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
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

// draken_vector_own_dict_f64 / _f32 — like _i64 but for float dictionaries.
// data is draken_malloc'd T[data_length] unique values; codes are uint32[length].
extern "C" PyObject* draken_vector_own_dict_f64(
    void* data, uint32_t data_length, uint32_t* codes, uint32_t length, uint8_t* validity)
{
    try {
        DrakenVector v = draken_vector_from_dict(data, data_length, codes, length,
                                                  DRAKEN_FLOAT64, validity);
        OwnedBuffer<void>    data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void>    codes_buf(static_cast<void*>(codes));
        VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
    } catch (nb::python_error& e) { e.restore(); return nullptr; }
    catch (std::bad_alloc&) { PyErr_NoMemory(); return nullptr; }
    catch (std::exception& e) { PyErr_SetString(PyExc_RuntimeError, e.what()); return nullptr; }
}

extern "C" PyObject* draken_vector_own_dict_f32(
    void* data, uint32_t data_length, uint32_t* codes, uint32_t length, uint8_t* validity)
{
    try {
        DrakenVector v = draken_vector_from_dict(data, data_length, codes, length,
                                                  DRAKEN_FLOAT32, validity);
        OwnedBuffer<void>    data_buf(data);
        OwnedBuffer<uint8_t> val_buf(validity);
        OwnedBuffer<void>    codes_buf(static_cast<void*>(codes));
        VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr();
        Py_INCREF(result);
        return result;
    } catch (nb::python_error& e) { e.restore(); return nullptr; }
    catch (std::bad_alloc&) { PyErr_NoMemory(); return nullptr; }
    catch (std::exception& e) { PyErr_SetString(PyExc_RuntimeError, e.what()); return nullptr; }
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
        if (type != DRAKEN_VARCHAR && type != DRAKEN_NVARCHAR &&
            type != DRAKEN_VARBINARY && type != DRAKEN_VARIANT) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string: type must be DRAKEN_VARCHAR, "
                "DRAKEN_NVARCHAR, DRAKEN_VARBINARY, or DRAKEN_VARIANT");
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

// draken_arrow_varlen_to_string_block — Arrow varlen (data+offsets+nulls) →
// German-string block + separate validity. Pure buffer work; no Python, no
// decode, raw bytes preserved, type tag carried. See draken_bridge.h.
extern "C" void* draken_arrow_varlen_to_string_block(
    const uint8_t* data, const uint32_t* offsets, const uint8_t* nulls,
    uint32_t length, DrakenType type, uint8_t** out_validity)
{
    *out_validity = nullptr;

    // Pass 1: total extern bytes + null presence.
    // The source buffer uses int32 offsets; a monotonicity break means the
    // accumulated key bytes exceeded INT32_MAX (2 GB) and the offsets wrapped.
    // Fail cleanly with an actionable error rather than reading a wrapped offset.
    size_t total_extern = 0u;
    bool   has_nulls = false;
    for (uint32_t i = 0u; i < length; ++i) {
        const bool valid = (nulls == nullptr) ||
                           ((nulls[i >> 3] >> (i & 7u)) & 1u);
        if (!valid) { has_nulls = true; continue; }
        if (offsets[i + 1] < offsets[i]) {
            PyErr_SetString(PyExc_OverflowError,
                "draken_arrow_varlen_to_string_block: source int32 offsets overflowed "
                "(>2 GB of string keys in one buffer); key-store offset width is the limit");
            return nullptr;
        }
        const uint32_t slen = static_cast<uint32_t>(offsets[i + 1] - offsets[i]);
        if (slen > STR_INLINE_MAX) total_extern += slen;
    }
    if (total_extern > static_cast<size_t>(UINT32_MAX)) {
        PyErr_SetString(PyExc_OverflowError,
            "draken_arrow_varlen_to_string_block: arena exceeds 4 GB");
        return nullptr;
    }

    // Consolidated block (NO embedded validity): [DrakenStringArena | slots | arena]
    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes = (length > 0u ? length : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start = struct_end + slots_bytes;
    const size_t total_alloc = arena_start + total_extern;

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(total_alloc));
    if (!block) return nullptr;
    std::memset(block, 0, total_alloc);

    uint8_t* bitmap = nullptr;
    if (has_nulls) {
        const uint32_t bm = (length + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t vbytes = padded > 0u ? padded : 8u;
        bitmap = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!bitmap) { draken_free(block); return nullptr; }
        std::memset(bitmap, 0xFF, vbytes);
    }

    DrakenStringArena* sa    = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t*           arena = (total_extern > 0u) ? (block + arena_start) : nullptr;

    sa->slots = slots; sa->arena = arena; sa->length = length;
    sa->arena_used = 0u; sa->arena_cap = total_extern;
    sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = type;

    for (uint32_t i = 0u; i < length; ++i) {
        const bool valid = (nulls == nullptr) ||
                           ((nulls[i >> 3] >> (i & 7u)) & 1u);
        if (!valid) {
            bitmap[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
            continue;  // slot stays zeroed (null sentinel)
        }
        const uint8_t* src  = data + offsets[i];
        const uint32_t slen = static_cast<uint32_t>(offsets[i + 1] - offsets[i]);
        if (slen <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], src, slen);
        } else {
            const uint32_t off = static_cast<uint32_t>(sa->arena_used);
            std::memcpy(arena + off, src, slen);
            str_init_extern(&slots[i], src, slen,
                            static_cast<uint32_t>(XXH3_64bits(src, slen)), off);
            sa->arena_used += slen;
        }
    }

    *out_validity = bitmap;
    return block;
}

// draken_vector_own_string_dict — wrap a value array + caller selection in a string Vector.
//
// Identical consolidated-block layout to draken_vector_own_string, except the value
// array holds data_length unique slots and the caller supplies a codes selection of
// length entries; the result is built with draken_vector_from_dict instead of
// draken_vector_from_dense. There is no separate "dictionary" type — the unified
// value_array[selection[i]] access path is unchanged; data_length < length is just an
// observable property. All four caller buffers (slots, arena, codes, validity) are
// transferred unconditionally on entry. See draken_bridge.h for the full contract.
extern "C" PyObject* draken_vector_own_string_dict(
    DrakenStringSlot* slots,
    uint8_t*          arena,
    size_t            arena_len,
    uint32_t*         codes,
    uint32_t          data_length,
    uint8_t*          validity,
    uint32_t          length,
    DrakenType        type)
{
    // Take ownership of all four caller buffers immediately via RAII.
    OwnedBuffer<void>    slots_guard(static_cast<void*>(slots));
    OwnedBuffer<void>    arena_guard(static_cast<void*>(arena));   // safe for nullptr
    OwnedBuffer<void>    codes_guard(static_cast<void*>(codes));   // safe for nullptr
    OwnedBuffer<uint8_t> validity_guard(validity);                 // safe for nullptr

    try {
        if (type != DRAKEN_VARCHAR && type != DRAKEN_NVARCHAR && type != DRAKEN_VARBINARY) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string_dict: type must be DRAKEN_VARCHAR, "
                "DRAKEN_NVARCHAR, or DRAKEN_VARBINARY");
            return nullptr;
        }
        if (arena_len > 0u && !arena) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string_dict: arena_len > 0 but arena is NULL");
            return nullptr;
        }
        if (data_length > 0u && !slots) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string_dict: data_length > 0 but slots is NULL");
            return nullptr;
        }
        if (length > 0u && !codes) {
            PyErr_SetString(PyExc_ValueError,
                "draken_vector_own_string_dict: length > 0 but codes is NULL");
            return nullptr;
        }

        // Allocate consolidated block holding the VALUE array (data_length slots).
        // Layout: [DrakenStringArena | DrakenStringSlot[data_length] | arena_bytes]
        constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
        const size_t struct_end =
            (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
        const size_t slots_bytes =
            (data_length > 0u ? (size_t)data_length : 1u) * sizeof(DrakenStringSlot);
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

        // Copy caller's unique slots and arena bytes into the consolidated block.
        if (data_length > 0u && slots)
            std::memcpy(dslots, slots, (size_t)data_length * sizeof(DrakenStringSlot));
        if (arena_len > 0u && arena)
            std::memcpy(darena, arena, arena_len);

        // Release the slots/arena guards and free the caller's originals. codes and
        // validity are retained (freed by VectorOwner on GC, mirroring own_dict_i64).
        draken_free(slots_guard.release());
        draken_free(arena_guard.release());
        uint32_t* raw_codes = static_cast<uint32_t*>(codes_guard.release());
        uint8_t*  raw_valid = validity_guard.release();
        OwnedBuffer<void>    codes_buf(static_cast<void*>(raw_codes));
        OwnedBuffer<uint8_t> val_buf(raw_valid);

        // Initialise DrakenStringArena. length here is the VALUE-array length (K).
        sa->slots        = dslots;
        sa->arena        = darena;
        sa->length       = (size_t)data_length;
        sa->arena_used   = arena_len;
        sa->arena_cap    = arena_len;
        sa->null_bitmap  = nullptr;  // validity tracked separately via VectorOwner
        sa->owns_buffers = 0;
        sa->type         = type;

        // Build the unified vector: selection = caller codes, value array = sa.
        DrakenVector v = draken_vector_from_dict(sa, data_length, raw_codes, length,
                                                 type, raw_valid);

        VectorOwner owner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
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

// draken_vector_own_time32 / _own_time64 — wrap a hand-allocated time buffer as a
// DRAKEN_TIME32 (int32 data) / DRAKEN_TIME64 (int64 data) Vector tagged with `unit`.
// Mirrors draken_vector_own_timestamp; lets the grouped-agg collectors build typed
// TIME results off the Python edge (no nanobind dispatch, no boxing). Ownership of
// data + validity transfers; both are freed on failure.
extern "C" PyObject* draken_vector_own_time32(
    void* data, uint8_t* validity, uint32_t length, const char* unit_str)
{
    OwnedBuffer<void>    data_guard(data);
    OwnedBuffer<uint8_t> val_guard(validity);
    try {
        const std::string u(unit_str);
        TimestampUnit unit = str_to_unit(u);
        void* final_data = data_guard.release();
        DrakenVector v = draken_vector_from_dense(final_data, length, DRAKEN_TIME32, validity);
        OwnedBuffer<void>    data_buf(final_data);
        OwnedBuffer<uint8_t> vbuf(val_guard.release());
        VectorOwner owner(v, std::move(data_buf), std::move(vbuf));
        LogicalType lt; lt.kind = LogicalKind::TIME; lt.unit = unit; lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr(); Py_INCREF(result); return result;
    } catch (nb::python_error& e) { e.restore(); return nullptr; }
      catch (std::bad_alloc&) { PyErr_NoMemory(); return nullptr; }
      catch (std::exception& e) { PyErr_SetString(PyExc_RuntimeError, e.what()); return nullptr; }
}

extern "C" PyObject* draken_vector_own_time64(
    void* data, uint8_t* validity, uint32_t length, const char* unit_str)
{
    OwnedBuffer<void>    data_guard(data);
    OwnedBuffer<uint8_t> val_guard(validity);
    try {
        const std::string u(unit_str);
        TimestampUnit unit = str_to_unit(u);
        void* final_data = data_guard.release();
        DrakenVector v = draken_vector_from_dense(final_data, length, DRAKEN_TIME64, validity);
        OwnedBuffer<void>    data_buf(final_data);
        OwnedBuffer<uint8_t> vbuf(val_guard.release());
        VectorOwner owner(v, std::move(data_buf), std::move(vbuf));
        LogicalType lt; lt.kind = LogicalKind::TIME; lt.unit = unit; lt.offset_minutes = 0;
        owner.logical_type = logical_type_intern(lt);
        nb::object obj = nb::cast(std::move(owner));
        PyObject* result = obj.ptr(); Py_INCREF(result); return result;
    } catch (nb::python_error& e) { e.restore(); return nullptr; }
      catch (std::bad_alloc&) { PyErr_NoMemory(); return nullptr; }
      catch (std::exception& e) { PyErr_SetString(PyExc_RuntimeError, e.what()); return nullptr; }
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

// draken_vecresult_own_c — C-linkage trampoline over draken_vector_own.
//
// Phase 9c: exposes draken_vector_own with C linkage (declared in the bridge
// header inside extern "C") so the expression executor can cimport a stable,
// unmangled symbol across the .so boundary. Behaviour is identical.
extern "C" PyObject* draken_vecresult_own_c(VecResult res) {
    return draken_vector_own(res);
}

// ---------------------------------------------------------------------------
// Readback helpers (int64 only at this milestone)
// ---------------------------------------------------------------------------

// Uniform access: data[selection[i]] for logical row i.

static inline int64_t row_int64(const DrakenVector& v, uint32_t i) noexcept {
    const int64_t* data = static_cast<const int64_t*>(v.data);
    return data[v.selection[i]];
}

// int128 unscaled readback for DRAKEN_DECIMAL128 (16-byte storage).
static inline __int128 row_int128(const DrakenVector& v, uint32_t i) noexcept {
    const __int128* data = static_cast<const __int128*>(v.data);
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

static inline bool is_ascii_space(uint8_t c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

static VectorOwner make_float64_from_string_vector(const VectorOwner& src) {
    if (!is_varchar_family(src.vec.type))
        throw std::invalid_argument(
            "vector_cast_string_to_float64: expected a string-family Vector");

    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(src.vec.data);
    const uint32_t n = src.vec.length;
    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(double);
    double* out = static_cast<double*>(draken_malloc(data_bytes));
    if (!out) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(out);

    std::vector<uint8_t> valid(n, 1u);
    bool has_nulls = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(src.vec, i)) {
            out[i] = 0.0;
            valid[i] = 0u;
            has_nulls = true;
            continue;
        }

        const DrakenStringSlot* slot = &sa->slots[src.vec.selection[i]];
        const uint8_t* bytes = str_data(slot, sa->arena);
        uint32_t len = str_length(slot);
        uint32_t start = 0u;
        uint32_t end = len;
        while (start < end && is_ascii_space(bytes[start])) ++start;
        while (end > start && is_ascii_space(bytes[end - 1u])) --end;
        if (start < end && bytes[start] == '+') ++start;

        double value = 0.0;
        const char* first = reinterpret_cast<const char*>(bytes + start);
        const char* last = reinterpret_cast<const char*>(bytes + end);
        fast_float::from_chars_result res = fast_float::from_chars(first, last, value);
        if (first == last || res.ec != std::errc() || res.ptr != last) {
            out[i] = 0.0;
            valid[i] = 0u;
            has_nulls = true;
        } else {
            out[i] = draken::ops::fp_canon(value);
        }
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm = (n + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i) {
            if (!valid[i])
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
        }
    }

    DrakenVector v = draken_vector_from_dense(out, n, DRAKEN_FLOAT64, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

// Convert a numeric Vector (DECIMAL / INT64 / FLOAT32 / FLOAT64) to a dense
// FLOAT64 Vector. DECIMAL values are divided by 10^scale to recover the real
// value. Null rows are preserved. Used by DECIMAL-vs-FLOAT64 comparison and by
// explicit float casts.
static VectorOwner make_float64_from_numeric_vector(const VectorOwner& src) {
    const DrakenType t = src.vec.type;
    if (t != DRAKEN_DECIMAL && t != DRAKEN_DECIMAL128 && t != DRAKEN_INT64 &&
        t != DRAKEN_FLOAT64 && t != DRAKEN_FLOAT32)
        throw std::invalid_argument(
            "to_float64: expected DECIMAL, DECIMAL128, INT64, FLOAT32 or FLOAT64 Vector");

    const uint32_t n = src.vec.length;
    const size_t data_bytes = (n > 0u ? n : 1u) * sizeof(double);
    double* out = static_cast<double*>(draken_malloc(data_bytes));
    if (!out) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(out);

    double scale_div = 1.0;
    if (t == DRAKEN_DECIMAL || t == DRAKEN_DECIMAL128) {
        if (!src.logical_type)
            throw std::invalid_argument("to_float64: DECIMAL requires a logical-type descriptor");
        scale_div = static_cast<double>(draken::ops::kDecPow10[src.logical_type->scale]);
    }

    const int64_t*  idata = (t == DRAKEN_DECIMAL || t == DRAKEN_INT64)
        ? static_cast<const int64_t*>(src.vec.data) : nullptr;
    const __int128* i128data = (t == DRAKEN_DECIMAL128)
        ? static_cast<const __int128*>(src.vec.data) : nullptr;
    const double*   fdata = (t == DRAKEN_FLOAT64) ? static_cast<const double*>(src.vec.data) : nullptr;
    const float*    sdata = (t == DRAKEN_FLOAT32) ? static_cast<const float*>(src.vec.data) : nullptr;

    bool has_nulls = false;
    std::vector<uint8_t> valid(n, 1u);
    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(src.vec, i)) {
            out[i] = 0.0; valid[i] = 0u; has_nulls = true; continue;
        }
        const uint32_t s = src.vec.selection[i];
        if (t == DRAKEN_DECIMAL)
            out[i] = static_cast<double>(idata[s]) / scale_div;
        else if (t == DRAKEN_DECIMAL128)
            out[i] = static_cast<double>(i128data[s]) / scale_div;
        else if (t == DRAKEN_INT64)
            out[i] = static_cast<double>(idata[s]);
        else if (t == DRAKEN_FLOAT64)
            out[i] = fdata[s];
        else  // FLOAT32
            out[i] = static_cast<double>(sdata[s]);
    }

    OwnedBuffer<uint8_t> validity_buf;
    uint8_t* validity = nullptr;
    if (has_nulls) {
        const uint32_t bm = (n + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        const size_t vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        validity_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
        for (uint32_t i = 0u; i < n; ++i)
            if (!valid[i])
                validity[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
    }

    DrakenVector v = draken_vector_from_dense(out, n, DRAKEN_FLOAT64, validity);
    return VectorOwner(v, std::move(data_buf), std::move(validity_buf));
}

static inline size_t ryu_format_double(char* buf, double d, uint32_t precision) {
    if (!std::isfinite(d)) {
        if (std::isnan(d)) {
            std::memcpy(buf, "NaN", 3u);
            return 3u;
        }
        if (d > 0.0) {
            std::memcpy(buf, "Infinity", 8u);
            return 8u;
        }
        std::memcpy(buf, "-Infinity", 9u);
        return 9u;
    }

    if (d >= 9.9e24 || d <= -9.9e24) {
        const int n = std::snprintf(buf, 32u, "%.17g", d);
        if (n < 0) throw std::runtime_error("vector_cast_float64_to_string: snprintf failed");
        return static_cast<size_t>(n);
    }

    int len = d2fixed_buffered_n(d, precision, buf);
    while (len > 0 && buf[len - 1] == '0') --len;
    if (len > 0 && buf[len - 1] == '.') {
        buf[len++] = '0';
    }
    return static_cast<size_t>(len);
}

static VectorOwner make_string_from_float_vector(const VectorOwner& src, uint32_t precision) {
    if (src.vec.type != DRAKEN_FLOAT64 && src.vec.type != DRAKEN_FLOAT32)
        throw std::invalid_argument(
            "vector_cast_float64_to_string: expected a FLOAT64/FLOAT32 Vector");

    const uint32_t n = src.vec.length;
    std::vector<std::string> formatted(n);
    size_t total_extern = 0u;
    bool has_nulls = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(src.vec, i)) {
            has_nulls = true;
            continue;
        }
        char buf[32];
        const double value = row_float(src.vec, i);
        const size_t len = ryu_format_double(buf, value, precision);
        formatted[i].assign(buf, len);
        if (len > STR_INLINE_MAX)
            total_extern += len;
    }

    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error(
            "vector_cast_float64_to_string: total arena bytes exceed 4 GB limit");

    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start = struct_end + slots_bytes;

    size_t validity_start = arena_start + total_extern;
    size_t validity_bytes = 0u;
    if (has_nulls) {
        const uint32_t bm = (n + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity_bytes = padded > 0u ? padded : 8u;
    }
    const size_t total_alloc = validity_start + validity_bytes;

    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena)));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena));
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot* slots = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t* arena = (total_extern > 0u) ? (block + arena_start) : nullptr;
    uint8_t* bitmap = has_nulls ? (block + validity_start) : nullptr;

    sa->slots = slots;
    sa->arena = arena;
    sa->length = n;
    sa->arena_used = 0u;
    sa->arena_cap = total_extern;
    sa->null_bitmap = nullptr;
    sa->owns_buffers = 0;
    sa->type = DRAKEN_VARCHAR;

    if (has_nulls) {
        std::memset(bitmap, 0xFF, validity_bytes);
        sa->null_bitmap = bitmap;
    }

    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(src.vec, i)) {
            if (bitmap)
                bitmap[i / 8u] &= static_cast<uint8_t>(~(1u << (i % 8u)));
            continue;
        }

        const std::string& s = formatted[i];
        const uint32_t len = static_cast<uint32_t>(s.size());
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(s.data());
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], bytes, len);
        } else {
            if (sa->arena_used > static_cast<size_t>(UINT32_MAX))
                throw std::overflow_error(
                    "vector_cast_float64_to_string: arena offset overflow");
            const uint32_t off = static_cast<uint32_t>(sa->arena_used);
            std::memcpy(arena + off, bytes, len);
            str_init_extern(&slots[i], bytes, len,
                            static_cast<uint32_t>(XXH3_64bits(bytes, len)), off);
            sa->arena_used += len;
        }
    }

    DrakenVector v = draken_vector_from_dense(sa, n, DRAKEN_VARCHAR, bitmap);
    return VectorOwner(v, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
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

// Convert a Python numeric scalar (Decimal / int / float) to its OWN
// (unscaled, scale) int128 representation — used for scale-aware decimal
// comparison, which (unlike storage) does not require the literal to be
// representable at the column's scale. A float is routed through str() so
// `0.05` becomes Decimal('0.05') (scale 2) rather than its binary expansion.
static void py_scalar_to_unscaled_scale(PyObject* obj, __int128& unscaled, uint8_t& scale) {
    PyObject* dec_type = get_decimal_type();

    // Materialise a Decimal we can introspect with as_tuple().
    PyObject* d = nullptr;
    if (PyObject_IsInstance(obj, dec_type) == 1) {
        Py_INCREF(obj);
        d = obj;
    } else if (PyFloat_Check(obj)) {
        PyObject* s = PyObject_Str(obj);          // shortest round-trip repr
        if (!s) throw nb::python_error();
        d = PyObject_CallOneArg(dec_type, s);
        Py_DECREF(s);
        if (!d) throw nb::python_error();
    } else {
        d = PyObject_CallOneArg(dec_type, obj);   // int (or anything Decimal accepts)
        if (!d) throw nb::python_error();
    }

    // Reject NaN / Inf before reading the digit tuple.
    PyObject* fin = PyObject_CallMethod(d, "is_finite", nullptr);
    if (!fin) { Py_DECREF(d); throw nb::python_error(); }
    const bool finite = (PyObject_IsTrue(fin) == 1);
    Py_DECREF(fin);
    if (!finite) {
        Py_DECREF(d);
        throw std::invalid_argument("decimal compare: NaN and Inf are not comparable");
    }

    // as_tuple() → (sign, (digit, digit, ...), exponent)
    PyObject* t = PyObject_CallMethod(d, "as_tuple", nullptr);
    Py_DECREF(d);
    if (!t) throw nb::python_error();
    PyObject* sign_o   = PyTuple_GetItem(t, 0);   // borrowed
    PyObject* digits_o = PyTuple_GetItem(t, 1);   // borrowed
    PyObject* exp_o    = PyTuple_GetItem(t, 2);   // borrowed
    if (!sign_o || !digits_o || !exp_o) { Py_DECREF(t); throw nb::python_error(); }

    __int128 mag = 0;
    const Py_ssize_t ndig = PyTuple_Size(digits_o);
    for (Py_ssize_t i = 0; i < ndig; ++i) {
        const long dig = PyLong_AsLong(PyTuple_GetItem(digits_o, i));
        mag = mag * 10 + (__int128)dig;
    }
    const long sign = PyLong_AsLong(sign_o);
    const long exp  = PyLong_AsLong(exp_o);
    Py_DECREF(t);

    if (sign != 0) mag = -mag;

    if (exp >= 0) {
        // Integer-valued at a coarser-than-unit scale (e.g. 1E+2): fold the
        // exponent into the magnitude and treat as scale 0.
        for (long k = 0; k < exp; ++k) mag *= 10;
        unscaled = mag;
        scale    = 0;
    } else {
        unscaled = mag;
        scale    = static_cast<uint8_t>(-exp);
    }
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
// DECIMAL128 (int128-backed) — conversion helpers, mirroring the int64 path.
// ---------------------------------------------------------------------------

// 10^e as __int128 for e in [0, 38] (10^38 < INT128_MAX ≈ 1.7e38).
static inline __int128 i128_pow10(int e) {
    __int128 r = 1;
    for (int k = 0; k < e; ++k) r *= 10;
    return r;
}

// Build a Python int from an __int128 via its 64-bit halves (no private CPython API).
static PyObject* pylong_from_i128(__int128 v) {
    const bool neg = v < 0;
    unsigned __int128 mag = neg ? (unsigned __int128)(-(v + 1)) + 1u  // avoid INT128_MIN UB
                                : (unsigned __int128)v;
    PyObject* hi = PyLong_FromUnsignedLongLong((unsigned long long)(mag >> 64));
    if (!hi) throw nb::python_error();
    PyObject* sixtyfour = PyLong_FromLong(64);
    if (!sixtyfour) { Py_DECREF(hi); throw nb::python_error(); }
    PyObject* hi_shifted = PyNumber_Lshift(hi, sixtyfour);
    Py_DECREF(hi); Py_DECREF(sixtyfour);
    if (!hi_shifted) throw nb::python_error();
    PyObject* lo = PyLong_FromUnsignedLongLong((unsigned long long)(uint64_t)mag);
    if (!lo) { Py_DECREF(hi_shifted); throw nb::python_error(); }
    PyObject* full = PyNumber_Add(hi_shifted, lo);
    Py_DECREF(hi_shifted); Py_DECREF(lo);
    if (!full) throw nb::python_error();
    if (neg) {
        PyObject* negated = PyNumber_Negative(full);
        Py_DECREF(full);
        if (!negated) throw nb::python_error();
        return negated;
    }
    return full;
}

// Python decimal.Decimal → int128 unscaled value at the declared (precision, scale).
// Fails loud on NaN/Inf, sub-scale precision, precision overflow (digit-tuple based,
// so it handles values wider than int64). Mirrors decimal_to_unscaled.
static __int128 decimal_to_unscaled128(PyObject* obj, uint8_t precision, uint8_t scale) {
    PyObject* dec_type = get_decimal_type();
    PyObject* d = (PyObject_IsInstance(obj, dec_type) == 1)
        ? (Py_INCREF(obj), obj)
        : PyObject_CallOneArg(dec_type, obj);
    if (!d) throw nb::python_error();

    PyObject* fin = PyObject_CallMethod(d, "is_finite", nullptr);
    if (!fin) { Py_DECREF(d); throw nb::python_error(); }
    const bool finite = (PyObject_IsTrue(fin) == 1);
    Py_DECREF(fin);
    if (!finite) { Py_DECREF(d); throw std::invalid_argument("decimal: NaN/Inf cannot be stored as DECIMAL"); }

    PyObject* t = PyObject_CallMethod(d, "as_tuple", nullptr);
    Py_DECREF(d);
    if (!t) throw nb::python_error();
    const long sign = PyLong_AsLong(PyTuple_GetItem(t, 0));
    PyObject* digits_o = PyTuple_GetItem(t, 1);              // borrowed
    const long exp = PyLong_AsLong(PyTuple_GetItem(t, 2));
    __int128 mag = 0;
    const Py_ssize_t ndig = PyTuple_Size(digits_o);
    for (Py_ssize_t i = 0; i < ndig; ++i)
        mag = mag * 10 + (__int128)PyLong_AsLong(PyTuple_GetItem(digits_o, i));
    Py_DECREF(t);

    // own_scale: digits represent value * 10^exp. exp>=0 folds into magnitude (scale 0).
    int own_scale;
    if (exp >= 0) { mag *= i128_pow10((int)exp); own_scale = 0; }
    else          { own_scale = (int)(-exp); }

    // Rescale own_scale → declared scale.
    if (own_scale > (int)scale) {
        const __int128 div = i128_pow10(own_scale - (int)scale);
        if (mag % div != 0)
            throw std::invalid_argument("decimal: value has more decimal places than the declared scale");
        mag /= div;
    } else if (own_scale < (int)scale) {
        mag *= i128_pow10((int)scale - own_scale);
    }
    if (sign != 0) mag = -mag;

    const __int128 limit = i128_pow10((int)precision);
    if (mag >= limit || mag <= -limit)
        throw std::overflow_error("decimal: value exceeds declared precision");
    return mag;
}

// int128 unscaled value + scale → Python decimal.Decimal (preserves trailing zeros).
static nb::object unscaled128_to_py_decimal(__int128 unscaled, uint8_t scale) {
    PyObject* dec_type = get_decimal_type();
    PyObject* pyint = pylong_from_i128(unscaled);
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

// Dense DECIMAL128 vector from a Python list (int128 storage, precision up to 38).
static VectorOwner make_decimal128_from_sequence(
    nb::list seq, uint8_t precision, uint8_t scale)
{
    if (precision < 1 || precision > 38)
        throw std::invalid_argument("DECIMAL128 precision must be in [1, 38]");
    if (scale > precision)
        throw std::invalid_argument("DECIMAL128 scale must be <= precision");

    const uint32_t length = static_cast<uint32_t>(seq.size());
    __int128* data = static_cast<__int128*>(
        draken_malloc((length > 0u ? length : 1u) * sizeof(__int128)));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    bool has_nulls = false;
    for (uint32_t i = 0; i < length; ++i) {
        nb::object obj = seq[static_cast<Py_ssize_t>(i)];
        if (obj.is_none()) { data[i] = 0; has_nulls = true; }
        else { data[i] = decimal_to_unscaled128(obj.ptr(), precision, scale); }
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

    DrakenVector v = draken_vector_from_dense(data, length, DRAKEN_DECIMAL128, validity);
    VectorOwner owner(v, std::move(data_buf), std::move(validity_buf));

    LogicalType lt;
    lt.kind      = LogicalKind::DECIMAL;
    lt.precision = precision;
    lt.scale     = scale;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// DECIMAL128 reductions (int128). All rows in a column share one scale, so unscaled
// order == value order — min/max compare raw int128 and sum accumulates raw int128.
// Each returns the count of valid (non-null) rows.
static uint32_t dec128_sum_reduce(const DrakenVector& v, __int128& out) {
    __int128 acc = 0; uint32_t cnt = 0;
    for (uint32_t i = 0; i < v.length; ++i) {
        if (!row_is_valid(v, i)) continue;
        const __int128 x = row_int128(v, i);
        const __int128 nx = acc + x;
        if (((acc ^ nx) & (x ^ nx)) < 0)  // signed add overflow
            throw std::overflow_error("sum: DECIMAL128 accumulator overflowed int128");
        acc = nx; ++cnt;
    }
    out = acc; return cnt;
}
static uint32_t dec128_min_reduce(const DrakenVector& v, __int128& out) {
    bool seen = false; __int128 best = 0; uint32_t cnt = 0;
    for (uint32_t i = 0; i < v.length; ++i) {
        if (!row_is_valid(v, i)) continue;
        const __int128 x = row_int128(v, i);
        if (!seen || x < best) { best = x; seen = true; }
        ++cnt;
    }
    out = best; return cnt;
}
static uint32_t dec128_max_reduce(const DrakenVector& v, __int128& out) {
    bool seen = false; __int128 best = 0; uint32_t cnt = 0;
    for (uint32_t i = 0; i < v.length; ++i) {
        if (!row_is_valid(v, i)) continue;
        const __int128 x = row_int128(v, i);
        if (!seen || x > best) { best = x; seen = true; }
        ++cnt;
    }
    out = best; return cnt;
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

// int128-backed (DECIMAL128) constant — the p>18 sibling of make_decimal_constant.
static VectorOwner make_decimal128_constant(
    nb::object value_obj, uint32_t length, uint8_t precision, uint8_t scale)
{
    if (precision < 1 || precision > 38)
        throw std::invalid_argument("DECIMAL128 precision must be in [1, 38]");
    if (scale > precision)
        throw std::invalid_argument("DECIMAL128 scale must be <= precision");

    const bool is_null = value_obj.is_none();
    __int128* data = static_cast<__int128*>(draken_malloc(sizeof(__int128)));
    if (!data) throw std::bad_alloc();
    data[0] = is_null ? (__int128)0 : decimal_to_unscaled128(value_obj.ptr(), precision, scale);
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

    DrakenVector v = draken_vector_from_constant(data, length, DRAKEN_DECIMAL128, validity);
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
// Physical: DrakenIntervalSlot { int64_t months; int64_t us; }, 16 bytes/row.
// Python API: (months: int, us: int) tuple or None — the second element is
// MICROSECONDS (the canonical engine unit; stored verbatim, no conversion).
//
// Normalization overflow is checked at ingestion so stored data never overflows
// when the kernel normalizes unchecked at op entry.
//
// NOTE: (months, us) tuples are used as the Python type for now. This is
// flagged for the consumer-rewrite as the engine may want a richer type.
// ---------------------------------------------------------------------------

static DrakenIntervalSlot py_to_interval_slot(nb::object obj) {
    if (!PyTuple_Check(obj.ptr()) || PyTuple_GET_SIZE(obj.ptr()) != 2)
        throw std::invalid_argument(
            "interval: element must be a (months, us) tuple or None");
    // PyTuple_GET_ITEM returns a BORROWED reference — use PyLong_AsLongLong
    // directly to avoid ref-count manipulation on the borrowed pointer.
    int64_t months = PyLong_AsLongLong(PyTuple_GET_ITEM(obj.ptr(), 0));
    int64_t us     = PyLong_AsLongLong(PyTuple_GET_ITEM(obj.ptr(), 1));
    if ((months == -1 || us == -1) && PyErr_Occurred())
        throw nb::python_error();
    // Validate that normalization doesn't overflow.
    draken::ops::interval_normalize_checked(months, us);
    return DrakenIntervalSlot{months, us};
}

static nb::object interval_slot_to_py(const DrakenIntervalSlot& s) {
    PyObject* tup = PyTuple_New(2);
    if (!tup) throw nb::python_error();
    PyObject* mo = PyLong_FromLongLong(static_cast<long long>(s.months));
    PyObject* us = PyLong_FromLongLong(static_cast<long long>(s.us));
    if (!mo || !us) {
        Py_XDECREF(mo); Py_XDECREF(us); Py_DECREF(tup);
        throw nb::python_error();
    }
    PyTuple_SET_ITEM(tup, 0, mo);
    PyTuple_SET_ITEM(tup, 1, us);
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
        case CT_STRING: {
            // make_string_from_sequence is bytes-only; encode any str children to
            // bytes here so this dev/test array-ingestion helper stays usable
            // while no str reaches the string edge.
            nb::list encoded;
            for (auto item : flat_children) {
                PyObject* p = item.ptr();
                if (item.is_none() || PyBytes_Check(p)) {
                    encoded.append(item);
                } else if (PyUnicode_Check(p)) {
                    Py_ssize_t sl = 0;
                    const char* u = PyUnicode_AsUTF8AndSize(p, &sl);
                    if (!u) throw nb::python_error();
                    encoded.append(nb::steal(PyBytes_FromStringAndSize(u, sl)));
                } else {
                    throw std::invalid_argument(
                        "vector_array_from_sequence: string child element must be str/bytes/None");
                }
            }
            child = std::make_unique<VectorOwner>(make_string_from_sequence(encoded));
            break;
        }
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
// D.13: array element subscript — vec[index] per logical row.
//
// For each logical row i of a DRAKEN_ARRAY vector:
//   sublist range  = child[offsets[sel[i]] : offsets[sel[i]+1]]
//   row_len        = end - start
//   pos            = (index >= 0) ? index : row_len + index   (Python semantics)
//   output[i]      = child[start + pos]   when 0 <= pos < row_len
//                    null                  otherwise (out-of-bounds or null parent)
//
// The output Vector has the same element type as the child vector. Result is
// dense (selection is identity). Validity is the merge of parent-row validity,
// in-bounds check, and the child element's own validity at the taken position.
// ---------------------------------------------------------------------------
static VectorOwner make_array_map_access(const VectorOwner& v, int64_t index) {
    if (v.vec.type != DRAKEN_ARRAY)
        throw std::invalid_argument(
            "vector_array_map_access: not a DRAKEN_ARRAY vector");
    if (!v.child_owner)
        throw std::invalid_argument(
            "vector_array_map_access: DRAKEN_ARRAY vector has no child");

    const int32_t*  offsets = static_cast<const int32_t*>(v.vec.data);
    const uint32_t* sel     = v.vec.selection;
    const uint32_t  n       = v.vec.length;

    // Gather child indices and per-row "is in bounds and parent valid" mask.
    // cidx[i] = 0 is a safe placeholder for null output rows (child element 0
    // must exist if any sublist is non-empty; if child is empty, no row can be
    // valid anyway and the take is a no-op for null rows).
    std::vector<int32_t> cidx(n, 0);

    // Always allocate the local null mask; we merge it into the result below.
    const uint32_t bm     = (n + 7u) / 8u;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   vbytes = padded > 0u ? padded : 8u;
    std::vector<uint8_t> local_validity(vbytes, 0xFFu);
    bool any_invalid = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!row_is_valid(v.vec, i)) {
            local_validity[i / 8u] &=
                static_cast<uint8_t>(~(1u << (i % 8u)));
            any_invalid = true;
            continue;
        }
        const uint32_t sel_i   = sel[i];
        const int32_t  start   = offsets[sel_i];
        const int32_t  end     = offsets[sel_i + 1u];
        const int32_t  row_len = end - start;
        int64_t pos = (index >= 0)
            ? index
            : static_cast<int64_t>(row_len) + index;
        if (pos < 0 || pos >= static_cast<int64_t>(row_len)) {
            local_validity[i / 8u] &=
                static_cast<uint8_t>(~(1u << (i % 8u)));
            any_invalid = true;
        } else {
            cidx[i] = start + static_cast<int32_t>(pos);
        }
    }

    // Empty input: skip the take, return a zero-length null vector of child type.
    // take_child requires at least one index path; for n == 0 the result is
    // trivially empty regardless of type.
    VectorOwner result = take_child(*v.child_owner, cidx);

    // Merge local null mask into the result's validity.
    //
    // After take_child, result.vec.validity reflects nulls inherited from the
    // child at the gathered positions (e.g. a null element child[start+pos]).
    // We need:  output_valid[i] = parent_in_bounds[i] AND child_valid_at_cidx[i].
    //
    // If we have no parent/OOB nulls, the child's validity is already correct.
    // Otherwise we either AND our mask into the existing validity, or attach
    // our mask as the new validity when the child has none.
    if (any_invalid) {
        if (result.vec.validity == nullptr) {
            // Result has no validity buffer yet — allocate one from our mask.
            uint8_t* dst = static_cast<uint8_t*>(draken_malloc(vbytes));
            if (!dst) throw std::bad_alloc();
            std::memcpy(dst, local_validity.data(), vbytes);
            result.validity_buf.reset(dst);
            result.vec.validity = dst;
        } else {
            // AND our mask into existing validity in place (we own the buffer).
            uint8_t* dst = result.vec.validity;
            for (uint32_t i = 0u; i < bm; ++i)
                dst[i] &= local_validity[i];
        }
    }

    return result;
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

// Build a string-family Vector from list[bytes|None] with an explicit type tag
// (no decode). Storage is identical across VARCHAR/NVARCHAR/VARBINARY; only the
// type tag differs. Used where bytes data must carry a known source type
// (e.g. MIN/MAX of a VARCHAR column preserving VARCHAR).
static VectorOwner make_bytes_from_sequence(nb::list seq);  // defined below
static VectorOwner make_bytes_from_sequence_typed(nb::list seq, DrakenType type) {
    VectorOwner owner = make_bytes_from_sequence(seq);  // produces DRAKEN_VARBINARY
    if (type != DRAKEN_VARBINARY) {
        owner.vec.type = type;
        if (owner.vec.data)
            static_cast<DrakenStringArena*>(owner.vec.data)->type = type;
    }
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
// CONCAT — vertical concatenation of N same-type vectors into one dense vector.
//
// Buffer-level; no Python objects, no decode, no factory map. The result type
// tag and logical_type are taken from the first input (combine requires a
// uniform schema). Each input is read via the uniform data[selection[i]]
// access so dense/dict/constant shapes all concatenate correctly.
// ---------------------------------------------------------------------------

static inline size_t concat_fixed_itemsize(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:                       return 1u;
        case DRAKEN_INT16:                      return 2u;
        case DRAKEN_INT32:
        case DRAKEN_FLOAT32:
        case DRAKEN_DATE32:                     return 4u;
        case DRAKEN_INT64:
        case DRAKEN_FLOAT64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_DECIMAL:                    return 8u;
        case DRAKEN_DECIMAL128:                 return 16u;  // int128 unscaled storage
        case DRAKEN_INTERVAL:                   return sizeof(DrakenIntervalSlot);
        default:                                return 0u;
    }
}

static VectorOwner concat_fixed(const std::vector<const VectorOwner*>& parts,
                                size_t itemsize, DrakenType type,
                                const LogicalType* lt) {
    uint64_t total = 0u;
    bool any_null = false;
    for (const VectorOwner* p : parts) {
        total += p->vec.length;
        if (p->vec.validity != nullptr) any_null = true;
    }
    if (total > static_cast<uint64_t>(UINT32_MAX))
        throw std::overflow_error("concat: total length exceeds u32");
    const uint32_t n = static_cast<uint32_t>(total);

    uint8_t* data = static_cast<uint8_t*>(draken_malloc((n > 0u ? n : 1u) * itemsize));
    if (!data) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(data);

    uint8_t* validity = nullptr;
    OwnedBuffer<uint8_t> val_buf;
    if (any_null) {
        const uint32_t padded = ((((n + 7u) >> 3) + 7u) & ~7u);
        const size_t vbytes = padded > 0u ? padded : 8u;
        validity = static_cast<uint8_t*>(draken_malloc(vbytes));
        if (!validity) throw std::bad_alloc();
        val_buf.reset(validity);
        std::memset(validity, 0xFF, vbytes);
    }

    uint32_t out = 0u;
    for (const VectorOwner* p : parts) {
        const DrakenVector& v = p->vec;
        const uint8_t* src = static_cast<const uint8_t*>(v.data);
        const uint32_t len = v.length;
        if ((v.flags & DRAKEN_SEL_IDENTITY) && v.data_length == v.length) {
            std::memcpy(data + static_cast<size_t>(out) * itemsize,
                        src, static_cast<size_t>(len) * itemsize);
        } else {
            for (uint32_t i = 0u; i < len; ++i)
                std::memcpy(data + static_cast<size_t>(out + i) * itemsize,
                            src + static_cast<size_t>(v.selection[i]) * itemsize,
                            itemsize);
        }
        if (validity) {
            for (uint32_t i = 0u; i < len; ++i)
                if (!row_is_valid(v, i))
                    validity[(out + i) >> 3] &=
                        static_cast<uint8_t>(~(1u << ((out + i) & 7u)));
        }
        out += len;
    }

    DrakenVector rv = draken_vector_from_dense(data, n, type, validity);
    VectorOwner owner(rv, std::move(data_buf), std::move(val_buf));
    owner.logical_type = lt;
    return owner;
}

static VectorOwner concat_bool(const std::vector<const VectorOwner*>& parts) {
    uint64_t total = 0u;
    bool any_null = false;
    for (const VectorOwner* p : parts) {
        total += p->vec.length;
        if (p->vec.validity != nullptr) any_null = true;
    }
    if (total > static_cast<uint64_t>(UINT32_MAX))
        throw std::overflow_error("concat: total length exceeds u32");
    const uint32_t n = static_cast<uint32_t>(total);

    const uint32_t bm = (n + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t alloc_out = padded > 0u ? padded : 8u;
    uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc_out));
    if (!data) throw std::bad_alloc();
    std::memset(data, 0, alloc_out);
    OwnedBuffer<void> data_buf(data);

    uint8_t* validity = nullptr;
    OwnedBuffer<uint8_t> val_buf;
    if (any_null) {
        validity = static_cast<uint8_t*>(draken_malloc(alloc_out));
        if (!validity) throw std::bad_alloc();
        val_buf.reset(validity);
        std::memset(validity, 0xFF, alloc_out);
    }

    uint32_t out = 0u;
    for (const VectorOwner* p : parts) {
        const DrakenVector& v = p->vec;
        const uint32_t len = v.length;
        for (uint32_t i = 0u; i < len; ++i) {
            if (row_is_valid(v, i)) {
                if (row_bool(v, i))
                    data[(out + i) >> 3] |= static_cast<uint8_t>(1u << ((out + i) & 7u));
            } else if (validity) {
                validity[(out + i) >> 3] &=
                    static_cast<uint8_t>(~(1u << ((out + i) & 7u)));
            }
        }
        out += len;
    }

    DrakenVector rv = draken_vector_from_dense(data, n, DRAKEN_BOOL, validity);
    return VectorOwner(rv, std::move(data_buf), std::move(val_buf));
}

static VectorOwner concat_string(const std::vector<const VectorOwner*>& parts,
                                 DrakenType type) {
    uint64_t total = 0u;
    size_t   total_extern = 0u;
    bool     any_null = false;
    for (const VectorOwner* p : parts) {
        const DrakenVector& v = p->vec;
        total += v.length;
        if (v.validity != nullptr) any_null = true;
        const DrakenStringArena* sa =
            static_cast<const DrakenStringArena*>(v.data);
        for (uint32_t i = 0u; i < v.length; ++i) {
            if (!row_is_valid(v, i)) continue;
            const DrakenStringSlot* s = &sa->slots[v.selection[i]];
            if (!str_is_inline(s)) total_extern += s->ext.length;
        }
    }
    if (total > static_cast<uint64_t>(UINT32_MAX))
        throw std::overflow_error("concat: total length exceeds u32");
    if (total_extern > static_cast<size_t>(UINT32_MAX))
        throw std::overflow_error("concat: total arena bytes exceed 4 GB");
    const uint32_t n = static_cast<uint32_t>(total);

    constexpr size_t kSlotAlign = alignof(DrakenStringSlot);
    const size_t struct_end =
        (sizeof(DrakenStringArena) + kSlotAlign - 1u) & ~(kSlotAlign - 1u);
    const size_t slots_bytes  = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t arena_start  = struct_end + slots_bytes;
    const size_t validity_start = arena_start + total_extern;

    size_t validity_bytes = 0u;
    if (any_null) {
        const uint32_t bm = (n + 7u) / 8u;
        const uint32_t padded = (bm + 7u) & ~7u;
        validity_bytes = padded > 0u ? padded : 8u;
    }
    const size_t total_alloc = validity_start + validity_bytes;

    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena)));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, total_alloc > 0u ? total_alloc : sizeof(DrakenStringArena));
    OwnedBuffer<void> data_buf(block);

    DrakenStringArena* sa_out = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  slots  = reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    uint8_t* arena  = (total_extern > 0u) ? (block + arena_start) : nullptr;
    uint8_t* bitmap = any_null ? (block + validity_start) : nullptr;

    sa_out->slots = slots; sa_out->arena = arena;
    sa_out->length = n; sa_out->arena_used = 0u; sa_out->arena_cap = total_extern;
    sa_out->null_bitmap = nullptr; sa_out->owns_buffers = 0;
    sa_out->type = type;
    if (any_null) { std::memset(bitmap, 0xFF, validity_bytes); sa_out->null_bitmap = bitmap; }

    uint32_t out = 0u;
    for (const VectorOwner* p : parts) {
        const DrakenVector& v = p->vec;
        const DrakenStringArena* sa_in =
            static_cast<const DrakenStringArena*>(v.data);
        for (uint32_t i = 0u; i < v.length; ++i, ++out) {
            if (!row_is_valid(v, i)) {
                if (bitmap)
                    bitmap[out >> 3] &= static_cast<uint8_t>(~(1u << (out & 7u)));
                continue;
            }
            const DrakenStringSlot* src = &sa_in->slots[v.selection[i]];
            if (str_is_inline(src)) {
                slots[out] = *src;
            } else {
                const uint32_t slen = src->ext.length;
                const uint32_t off  = static_cast<uint32_t>(sa_out->arena_used);
                std::memcpy(arena + off, str_data(src, sa_in->arena), slen);
                slots[out].ext.length       = slen;
                slots[out].ext.prefix       = src->ext.prefix;
                slots[out].ext.hash32       = src->ext.hash32;
                slots[out].ext.arena_offset = off;
                sa_out->arena_used += slen;
            }
        }
    }

    DrakenVector rv = draken_vector_from_dense(sa_out, n, type, bitmap);
    return VectorOwner(rv, std::move(data_buf), OwnedBuffer<uint8_t>(nullptr));
}

static VectorOwner concat_owners(const std::vector<const VectorOwner*>& parts) {
    if (parts.empty())
        throw std::invalid_argument("concat: empty input");
    const DrakenType type = parts[0]->vec.type;
    const LogicalType* lt = parts[0]->logical_type;
    for (const VectorOwner* p : parts)
        if (p->vec.type != type)
            throw std::invalid_argument("concat: all inputs must share one type");

    if (is_varchar_family(type))
        return concat_string(parts, type);
    if (type == DRAKEN_BOOL)
        return concat_bool(parts);
    const size_t itemsize = concat_fixed_itemsize(type);
    if (itemsize == 0u)
        throw std::invalid_argument("concat: unsupported type");
    return concat_fixed(parts, itemsize, type, lt);
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

// Resolve an arithmetic operand to (scale, precision) for the decimal kernels.
// DECIMAL/DECIMAL128 uses its logical-type descriptor. An INTEGER operand is a
// scale-0 decimal — integer N is exactly DECIMAL(digits, 0) — and its precision is
// the type's max decimal digit count (INT8→3, INT16→5, INT32→10, INT64→19), matching
// the binder's `type_unification._INT_DIGITS`. Using the actual width (not a flat 18)
// keeps the runtime's derived result tier (DECIMAL int64 vs DECIMAL128) in lock-step
// with the bound schema, so no downstream op reads the wrong physical width.
// Narrow ints (INT8/16/32) are laid out at sub-int64 stride, so the caller widens them
// to INT64 before the kernels run (see maybe_widen_narrow_int_to_i64); this function
// only reports (scale, precision) from the ORIGINAL width.
static inline void decimal_operand_scale_prec(
    const VectorOwner& v, const char* op, uint8_t& scale, uint8_t& prec) {
    if (v.vec.type == DRAKEN_DECIMAL || v.vec.type == DRAKEN_DECIMAL128) {
        if (!v.logical_type)
            throw std::invalid_argument(
                std::string(op) + ": missing logical-type descriptor");
        scale = v.logical_type->scale;
        prec  = v.logical_type->precision;
    } else if (is_integer_type(v.vec.type)) {
        scale = 0;
        switch (v.vec.type) {
            case DRAKEN_INT8:  prec = 3;  break;
            case DRAKEN_INT16: prec = 5;  break;
            case DRAKEN_INT32: prec = 10; break;
            default:           prec = 19; break;  // DRAKEN_INT64
        }
    } else {
        throw std::invalid_argument(
            std::string(op) + ": operands must be DRAKEN_DECIMAL, DRAKEN_DECIMAL128, "
            "or an integer type");
    }
}

// Widen an int64-backed DECIMAL (or INT64) operand to a dense int128 DECIMAL128
// vector — resolving any selection, borrowing the (per-logical-row) validity bitmap.
// Temporary operand for the int128 kernels; freed by the caller after the kernel runs.
static VectorOwner widen_decimal_to_i128(
    const VectorOwner& src, uint8_t scale, uint8_t precision) {
    const uint32_t n = src.vec.length;
    __int128* dst = static_cast<__int128*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(__int128)));
    if (!dst) throw std::bad_alloc();
    OwnedBuffer<void> data_buf(dst);
    const int64_t* sd = static_cast<const int64_t*>(src.vec.data);
    for (uint32_t i = 0; i < n; ++i)
        dst[i] = static_cast<__int128>(sd[src.vec.selection[i]]);
    // validity is per-logical-row, so it carries over unchanged; borrow it (not owned).
    DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_DECIMAL128, src.vec.validity);
    VectorOwner owner(v, std::move(data_buf), OwnedBuffer<uint8_t>{});
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = precision; lt.scale = scale;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

// Widen a narrow-int operand (INT8/16/32) to a dense INT64 VectorOwner. The int64
// decimal kernels (dec_add/…/dec_mul) and widen_decimal_to_i128 read operands at int64
// stride; a narrow int must therefore be promoted first or they would misread its data.
// Returns nullptr for INT64 / DECIMAL / DECIMAL128 (already int64-stride). promote_narrow_int
// resolves the selection and carries per-logical-row validity (§11).
static std::unique_ptr<VectorOwner> maybe_widen_narrow_int_to_i64(const VectorOwner& v) {
    const DrakenType t = v.vec.type;
    if (t == DRAKEN_INT8 || t == DRAKEN_INT16 || t == DRAKEN_INT32)
        return std::make_unique<VectorOwner>(
            vecresult_to_owner(draken::ops::promote_narrow_int(v.vec, DRAKEN_INT64)));
    return nullptr;
}

// Shared decimal binary-op dispatch with int64↔int128 PROMOTION (I-5).
// Tier is int128 when either operand is already int128, OR the result needs more than
// 18 digits of precision/scale (so it can't fit the int64 fast path). int64-backed
// operands are widened to int128 before the int128 kernel runs. Otherwise the int64
// fast path is used.
typedef VecResult (*DecKernel)(const DrakenVector&, uint8_t, const DrakenVector&, uint8_t);
static VectorOwner decimal_binop_promote(
    const VectorOwner& a_in, uint8_t sa, uint8_t pa,
    const VectorOwner& b_in, uint8_t sb, uint8_t pb,
    uint8_t rs, int rp_raw, int rs_raw,
    DecKernel k64, DecKernel k128) {
    // Widen narrow ints to INT64 once (scale/prec were already taken from the ORIGINAL
    // widths by the caller, so the result tier still matches the binder). After this,
    // both the int64 kernels and widen_decimal_to_i128 read every operand at int64 stride.
    std::unique_ptr<VectorOwner> aw_int = maybe_widen_narrow_int_to_i64(a_in);
    std::unique_ptr<VectorOwner> bw_int = maybe_widen_narrow_int_to_i64(b_in);
    const VectorOwner& a = aw_int ? *aw_int : a_in;
    const VectorOwner& b = bw_int ? *bw_int : b_in;
    const bool a128 = (a.vec.type == DRAKEN_DECIMAL128);
    const bool b128 = (b.vec.type == DRAKEN_DECIMAL128);
    // Promote to DECIMAL128 when either operand is already int128 (mixed-tier), OR
    // when the result precision/scale exceeds the int64 cap of 18 (type-driven overflow).
    // The downstream DECIMAL128 matrix (gather, compare, hash, aggregates, group-by,
    // key-store, parquet ingestion) is now complete (I-4..I-6), so promotion is safe.
    // Previously this was mixed-only to avoid routing normal int64 decimals into DECIMAL128
    // before the downstream matrix existed (that broke TPC-H q15 in the first attempt).
    const bool need128 = a128 || b128 || rp_raw > 18 || rs_raw > 18;

    if (need128) {
        std::unique_ptr<VectorOwner> aw, bw;
        const DrakenVector* av = &a.vec;
        const DrakenVector* bv = &b.vec;
        if (!a128) { aw = std::make_unique<VectorOwner>(widen_decimal_to_i128(a, sa, pa)); av = &aw->vec; }
        if (!b128) { bw = std::make_unique<VectorOwner>(widen_decimal_to_i128(b, sb, pb)); bv = &bw->vec; }
        const uint8_t rp = (rp_raw <= 38) ? (uint8_t)rp_raw : 38u;
        VecResult vr = k128(*av, sa, *bv, sb);
        VectorOwner owner = vecresult_to_owner(vr);
        owner.vec.type = DRAKEN_DECIMAL128;
        LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
        owner.logical_type = logical_type_intern(lt);
        return owner;
    }

    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = k64(a.vec, sa, b.vec, sb);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_add_dispatch(const VectorOwner& a, const VectorOwner& b) {
    uint8_t sa, pa, sb, pb;
    decimal_operand_scale_prec(a, "dec_add", sa, pa);
    decimal_operand_scale_prec(b, "dec_add", sb, pb);
    const uint8_t rs = (sa >= sb) ? sa : sb;
    const int int_a = (int)pa - (int)sa, int_b = (int)pb - (int)sb;
    const int rp_raw = (int_a >= int_b ? int_a : int_b) + (int)rs + 1;
    return decimal_binop_promote(a, sa, pa, b, sb, pb, rs, rp_raw, (int)rs,
                                 draken::ops::dec_add, draken::ops::dec128_add);
}

static VectorOwner decimal_sub_dispatch(const VectorOwner& a, const VectorOwner& b) {
    uint8_t sa, pa, sb, pb;
    decimal_operand_scale_prec(a, "dec_sub", sa, pa);
    decimal_operand_scale_prec(b, "dec_sub", sb, pb);
    const uint8_t rs = (sa >= sb) ? sa : sb;
    const int int_a = (int)pa - (int)sa, int_b = (int)pb - (int)sb;
    const int rp_raw = (int_a >= int_b ? int_a : int_b) + (int)rs + 1;
    return decimal_binop_promote(a, sa, pa, b, sb, pb, rs, rp_raw, (int)rs,
                                 draken::ops::dec_sub, draken::ops::dec128_sub);
}

static VectorOwner decimal_mul_dispatch(const VectorOwner& a, const VectorOwner& b) {
    uint8_t sa, pa, sb, pb;
    decimal_operand_scale_prec(a, "dec_mul", sa, pa);
    decimal_operand_scale_prec(b, "dec_mul", sb, pb);
    const int rs_raw = (int)sa + (int)sb;            // result scale = sa + sb
    const int rp_raw = (int)pa + (int)pb;            // result precision = pa + pb
    const uint8_t rs = (rs_raw <= 38) ? (uint8_t)rs_raw : 38u;
    return decimal_binop_promote(a, sa, pa, b, sb, pb, rs, rp_raw, rs_raw,
                                 draken::ops::dec_mul, draken::ops::dec128_mul);
}

static VectorOwner decimal_div_dispatch(const VectorOwner& a_in, const VectorOwner& b_in) {
    uint8_t sa, pa, sb, pb;
    decimal_operand_scale_prec(a_in, "dec_div", sa, pa);
    decimal_operand_scale_prec(b_in, "dec_div", sb, pb);
    // result_scale = max(sa + 6, 6); result_prec ≈ pa + 6 (both capped at the tier max).
    // DIV has a 5-arg kernel signature, so it can't go through decimal_binop_promote
    // (which is typed for 4-arg add/sub/mul/mod) — the int128 promotion is inlined here.
    const int rs_raw = ((int)sa + 6 >= 6) ? (int)sa + 6 : 6;
    const int rp_raw = (int)pa + 6;
    // Widen narrow ints to INT64 (scale/prec already taken from the original widths).
    std::unique_ptr<VectorOwner> aw_int = maybe_widen_narrow_int_to_i64(a_in);
    std::unique_ptr<VectorOwner> bw_int = maybe_widen_narrow_int_to_i64(b_in);
    const VectorOwner& a = aw_int ? *aw_int : a_in;
    const VectorOwner& b = bw_int ? *bw_int : b_in;
    const bool a128 = (a.vec.type == DRAKEN_DECIMAL128);
    const bool b128 = (b.vec.type == DRAKEN_DECIMAL128);
    // Promote when either operand is int128, OR the result precision/scale exceeds the
    // int64 cap of 18 (type-driven overflow) — same posture as decimal_binop_promote, so
    // the runtime tier matches the bound schema for divides whose result is DECIMAL128.
    const bool need128 = a128 || b128 || rp_raw > 18 || rs_raw > 18;

    if (need128) {
        const uint8_t rs = (rs_raw <= 38) ? (uint8_t)rs_raw : 38u;
        const uint8_t rp = (rp_raw <= 38) ? (uint8_t)rp_raw : 38u;
        std::unique_ptr<VectorOwner> aw, bw;
        const DrakenVector* av = &a.vec;
        const DrakenVector* bv = &b.vec;
        if (!a128) { aw = std::make_unique<VectorOwner>(widen_decimal_to_i128(a, sa, pa)); av = &aw->vec; }
        if (!b128) { bw = std::make_unique<VectorOwner>(widen_decimal_to_i128(b, sb, pb)); bv = &bw->vec; }
        VecResult vr = draken::ops::dec128_div(*av, sa, *bv, sb, rs);
        VectorOwner owner = vecresult_to_owner(vr);
        owner.vec.type = DRAKEN_DECIMAL128;
        LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
        owner.logical_type = logical_type_intern(lt);
        return owner;
    }

    const uint8_t rs = (rs_raw <= 18) ? (uint8_t)rs_raw : 18u;
    const uint8_t rp = (rp_raw <= 18) ? (uint8_t)rp_raw : 18u;
    VecResult vr = draken::ops::dec_div(a.vec, sa, b.vec, sb, rs);
    VectorOwner owner = vecresult_to_owner(vr);
    owner.vec.type = DRAKEN_DECIMAL;
    LogicalType lt{}; lt.kind = LogicalKind::DECIMAL; lt.precision = rp; lt.scale = rs;
    owner.logical_type = logical_type_intern(lt);
    return owner;
}

static VectorOwner decimal_mod_dispatch(const VectorOwner& a, const VectorOwner& b) {
    uint8_t sa, pa, sb, pb;
    decimal_operand_scale_prec(a, "dec_mod", sa, pa);
    decimal_operand_scale_prec(b, "dec_mod", sb, pb);
    // result_scale = sa, result_prec = pa (same as the dividend). MOD fits the 4-arg
    // DecKernel signature, so it rides the shared int64↔int128 promotion path.
    return decimal_binop_promote(a, sa, pa, b, sb, pb, /*rs=*/sa, /*rp_raw=*/pa, /*rs_raw=*/sa,
                                 draken::ops::dec_mod, draken::ops::dec128_mod);
}

// ---------------------------------------------------------------------------
// take — shared dispatch over a raw int32 index buffer. The nb::list `take`
// binding boxes into a std::vector and calls this; the C bridge
// (draken_vector_take_buffer) passes a typed memoryview pointer directly, so
// hot-path Cython callers (Morsel.take / _take_inplace / align_tables) avoid
// per-row PyObject boxing entirely.
// ---------------------------------------------------------------------------
static VectorOwner vector_take_impl(const VectorOwner& v, const int32_t* idx, uint32_t n) {
    // D.11: null — taking from null always produces a null vector of length n.
    if (v.vec.type == DRAKEN_NULL) return make_null_vector(n);
    // D.13: array — gather rows with owned child copy.
    if (v.vec.type == DRAKEN_ARRAY) return make_array_take(v, idx, n);
    // D.11: fp16 — gather rows by index.
    if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_take(v, idx, n);
    // D.12: bool — bit-packed gather.
    if (v.vec.type == DRAKEN_BOOL) return make_bool_take(v, idx, n);
    auto result = vecresult_to_owner(draken_take(v.vec, idx, n));
    // Typed kernels hardcode their own type tag in VecResult (e.g. i64_take
    // always emits DRAKEN_INT64). Restore the original physical type so that
    // TIMESTAMP64 (and any future aliased type) stays correct after gather.
    result.vec.type     = v.vec.type;
    result.logical_type = v.logical_type;
    return result;
}

// take_with_null — gather where index < 0 yields a NULL output row (outer-join
// unmatched rows). Negative indices are gathered with a safe substitute (0) and
// then forced null in the result's validity bitmap. Type-uniform: works for every
// type vector_take_impl handles, including ARRAY / INTERVAL / VARBINARY that the
// old to_pylist round-trip could not.
static VectorOwner vector_take_with_null_impl(const VectorOwner& v, const int32_t* idx, uint32_t n) {
    std::vector<int32_t> safe(n > 0u ? n : 1u);
    bool any_neg = false;
    for (uint32_t i = 0; i < n; ++i) {
        if (idx[i] < 0) { safe[i] = 0; any_neg = true; }
        else            { safe[i] = idx[i]; }
    }
    VectorOwner result = vector_take_impl(v, n > 0u ? safe.data() : nullptr, n);
    // DRAKEN_NULL is already all-null; nothing to force.
    if (!any_neg || result.vec.type == DRAKEN_NULL) return result;

    uint8_t* val = result.vec.validity;
    if (val == nullptr) {
        const uint32_t nb = (n + 7u) >> 3;
        val = static_cast<uint8_t*>(draken_malloc(nb > 0u ? nb : 1u));
        if (!val) throw std::bad_alloc();
        std::memset(val, 0xFF, nb > 0u ? nb : 1u);
        result.vec.validity = val;
        result.validity_buf.reset(val);   // VectorOwner now frees it on GC
    }
    for (uint32_t i = 0; i < n; ++i) {
        if (idx[i] < 0)
            val[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
    }
    return result;
}

// S0: slice/mask compute extracted from the nanobind Vector.slice/.mask lambdas so
// cxx_slice/cxx_mask share ONE body (no duplication). Pure C++ over DrakenVector
// structs — the callers (the lambdas, and the nogil cxx_* ops) manage the GIL.
static VectorOwner vector_slice_impl(const VectorOwner& v, uint32_t start, uint32_t length) {
    if (static_cast<uint64_t>(start) + length > v.vec.length)
        throw std::out_of_range("Vector.slice: start + length exceeds vector length");
    if (v.vec.type == DRAKEN_NULL) return make_null_vector(length);
    if (v.vec.type == DRAKEN_ARRAY || v.vec.type == DRAKEN_VECTOR_FP16 ||
        v.vec.type == DRAKEN_BOOL) {
        std::vector<int32_t> idx_vec(length);
        for (uint32_t i = 0; i < length; ++i)
            idx_vec[i] = static_cast<int32_t>(start + i);
        if (v.vec.type == DRAKEN_ARRAY)       return make_array_take(v, idx_vec.data(), length);
        if (v.vec.type == DRAKEN_VECTOR_FP16) return make_fp16_take(v, idx_vec.data(), length);
        return make_bool_take(v, idx_vec.data(), length);
    }
    auto result = vecresult_to_owner(draken_slice(v.vec, start, length));
    result.vec.type     = v.vec.type;
    result.logical_type = v.logical_type;
    return result;
}

// Derive the surviving-row indices (valid AND true) from a DRAKEN_BOOL mask.
// Shared by the single-column vector_mask_impl and the whole-morsel cxx_mask so
// the index list is built ONCE per mask, not re-scanned per column.
static std::vector<int32_t> mask_indices(const DrakenVector& m) {
    if (m.type != DRAKEN_BOOL)
        throw std::invalid_argument("mask: expected a DRAKEN_BOOL mask vector");
    const uint32_t mn = m.length;
    std::vector<int32_t> idx_vec;
    idx_vec.reserve(mn);
    for (uint32_t i = 0; i < mn; ++i)
        if (row_is_valid(m, i) && row_bool(m, i))
            idx_vec.push_back(static_cast<int32_t>(i));
    return idx_vec;
}

static VectorOwner vector_mask_impl(const VectorOwner& v, const VectorOwner& m) {
    std::vector<int32_t> idx_vec = mask_indices(m.vec);
    // vector_take_impl's type switch is identical to the old inline body.
    return vector_take_impl(v, idx_vec.data(), static_cast<uint32_t>(idx_vec.size()));
}

// draken_vector_take_buffer — C-bridge take over a raw int32 index buffer.
//
// vec_obj must be a draken.draken_native.Vector. `indices` is a caller-owned
// int32_t[n] buffer (e.g. a Cython typed memoryview); it is only read, never
// retained. Returns a NEW reference to a Python Vector, or NULL + exception.
extern "C" PyObject* draken_vector_take_buffer(
    PyObject* vec_obj, const int32_t* indices, uint32_t n)
{
    if (!vec_obj || vec_obj == Py_None) {
        PyErr_SetString(PyExc_TypeError,
            "draken_vector_take_buffer: expected Vector, got None");
        return nullptr;
    }
    nb::handle h(vec_obj);
    if (!nb::isinstance<VectorOwner>(h)) {
        PyErr_Format(PyExc_TypeError,
            "draken_vector_take_buffer: expected Vector, got %.100s",
            Py_TYPE(vec_obj)->tp_name);
        return nullptr;
    }
    try {
        const VectorOwner& v = *nb::inst_ptr<VectorOwner>(h);
        nb::object obj = nb::cast(vector_take_impl(v, indices, n));
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

// draken_vector_take_with_null_buffer — like draken_vector_take_buffer, but
// index < 0 produces a NULL output row (outer-join unmatched rows). Returns a
// NEW reference to a Python Vector, or NULL + exception.
extern "C" PyObject* draken_vector_take_with_null_buffer(
    PyObject* vec_obj, const int32_t* indices, uint32_t n)
{
    if (!vec_obj || vec_obj == Py_None) {
        PyErr_SetString(PyExc_TypeError,
            "draken_vector_take_with_null_buffer: expected Vector, got None");
        return nullptr;
    }
    nb::handle h(vec_obj);
    if (!nb::isinstance<VectorOwner>(h)) {
        PyErr_Format(PyExc_TypeError,
            "draken_vector_take_with_null_buffer: expected Vector, got %.100s",
            Py_TYPE(vec_obj)->tp_name);
        return nullptr;
    }
    try {
        const VectorOwner& v = *nb::inst_ptr<VectorOwner>(h);
        nb::object obj = nb::cast(vector_take_with_null_impl(v, indices, n));
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
// CxxMorsel helpers used by the wired path (the CxxMorsel nanobind methods +
// the from_cxx_vectors factory). cast<shared_ptr<VectorOwner>>(handle) yields an
// aliasing shared_ptr (Py keep-alive deleter); cast(shared_ptr) yields a Vector
// wrapper referencing the same instance — both zero-copy.
// ---------------------------------------------------------------------------

// S0: gather all columns of a CxxMorsel at the given row indices (nogil; reuses
// vector_take_impl — VectorOwner in/out, no PyObject). Returns a new CxxMorsel.
static CxxMorsel cxx_take(const CxxMorsel& m, const int32_t* idx, uint32_t n) {
    CxxMorsel out;
    out.names = m.names;
    if (m.columns.empty()) { out.zero_col_rows = n; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_take_impl(*col.own, idx, n));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// S0: slice a row window from all columns (nogil; reuses vector_slice_impl).
static CxxMorsel cxx_slice(const CxxMorsel& m, uint32_t start, uint32_t length) {
    CxxMorsel out;
    out.names = m.names;
    if (m.columns.empty()) { out.zero_col_rows = length; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_slice_impl(*col.own, start, length));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

// S1: filter every column by a DRAKEN_BOOL mask (keep rows valid AND true).
// Derives the surviving-row indices ONCE, then type-takes each column via the
// same vector_take_impl cxx_take uses. nogil — no PyObject, shared-owner result.
// Zero-column morsels carry the surviving row count (== mask count_true), which
// matches the PyObject filter_mask path.
static CxxMorsel cxx_mask(const CxxMorsel& m, const DrakenVector& mask) {
    CxxMorsel out;
    out.names = m.names;
    std::vector<int32_t> idx_vec = mask_indices(mask);
    const uint32_t n = static_cast<uint32_t>(idx_vec.size());
    if (m.columns.empty()) { out.zero_col_rows = n; return out; }
    out.columns.reserve(m.columns.size());
    for (const CxxColumn& col : m.columns) {
        CxxColumn nc;
        nc.own  = std::make_shared<VectorOwner>(vector_take_impl(*col.own, idx_vec.data(), n));
        nc.view = nc.own->vec;
        out.columns.push_back(std::move(nc));
    }
    return out;
}

static CxxMorsel cxx_from_vectors_list(nb::list vectors) {
    CxxMorsel m;
    const size_t n = nb::len(vectors);
    m.columns.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        CxxColumn col;
        col.own  = nb::cast<std::shared_ptr<VectorOwner>>(vectors[i]);
        col.view = col.own->vec;
        m.columns.push_back(std::move(col));
    }
    return m;
}
static nb::list cxx_columns_to_list(const CxxMorsel& m) {
    nb::list out;
    for (const CxxColumn& col : m.columns) out.append(nb::cast(col.own));
    return out;
}

static std::string nb_bytes_to_std(nb::handle h) {
    char* data; Py_ssize_t len;
    if (PyBytes_AsStringAndSize(h.ptr(), &data, &len) != 0)
        throw std::invalid_argument("cxx_select: column name must be bytes");
    return std::string(data, static_cast<size_t>(len));
}

// Boundary bridge: hand the converted (Cython) operators the raw C++ CxxMorsel
// pointer behind a nanobind handle, so the relational hot path reads columns
// (columns[i].view → DrakenVector*) at C level — NO PyObject, NO nanobind, GIL
// releasable — instead of dispatching a Python method per column access. Called
// ONCE when a morsel becomes Cxx-backed (GIL held); the handle owns the C++
// object, so the pointer stays valid for the handle's lifetime.
extern "C" const CxxMorsel* cxx_morsel_raw_ptr(PyObject* handle) {
    return nb::cast<CxxMorsel*>(nb::handle(handle));
}

// ---------------------------------------------------------------------------
// S-B.0(a) — C-ABI transform surface.
//
// Thin `extern "C"` wrappers over the pure-C++ `cxx_*` ops so the Cython operator
// chain can call them at C level (nogil, no PyObject, no nanobind), resolved across
// the .so boundary via dynamic_lookup (same mechanism as cxx_morsel_raw_ptr). Each
// returns a heap CxxMorsel the caller owns (free via cxx_morsel_delete); the result
// is move-constructed, so the columns' shared_ptrs are shared, not copied. These
// are GIL-free: no Python/nanobind in the body. NOT yet called (S-B.1 wires them).
extern "C" CxxMorsel* cxx_take_c(const CxxMorsel* m, const int32_t* idx, uint32_t n) {
    return new CxxMorsel(cxx_take(*m, idx, n));
}
extern "C" CxxMorsel* cxx_slice_c(const CxxMorsel* m, uint32_t start, uint32_t length) {
    return new CxxMorsel(cxx_slice(*m, start, length));
}
extern "C" CxxMorsel* cxx_mask_c(const CxxMorsel* m, const DrakenVector* mask) {
    return new CxxMorsel(cxx_mask(*m, *mask));
}
// S-B.2: select/reorder columns by identity name (bytes → ptr+len arrays, since
// identity names are opaque bytes). Pure container op (shares owners, no copy).
extern "C" CxxMorsel* cxx_select_c(const CxxMorsel* m, const char** name_ptrs,
                                   const uint32_t* name_lens, uint32_t n) {
    std::vector<std::string> want;
    want.reserve(n);
    for (uint32_t i = 0; i < n; ++i)
        want.emplace_back(name_ptrs[i], name_lens[i]);
    return new CxxMorsel(cxx_select(*m, want));
}
extern "C" void cxx_morsel_delete(CxxMorsel* m) {
    delete m;
}

// ── Shape-preserving keying hash — ONE implementation shared by the
//    Vector.hash_shaped binding and the nogil C-ABI cxx_hash_c. Pure C++,
//    GIL-free (no PyObject/nanobind). Mirrors the old hash_shaped lambda body.
static VectorOwner hash_shaped_impl(const VectorOwner& v) {
    if (v.vec.type == DRAKEN_ARRAY)
        throw std::invalid_argument("hash_shaped: not supported for DRAKEN_ARRAY");
    const uint32_t n = v.vec.length;
    if (v.vec.type != DRAKEN_NULL && v.vec.type != DRAKEN_VECTOR_FP16
            && v.vec.type != DRAKEN_DECIMAL128) {
        return vecresult_to_owner(draken_hash_shaped(v.vec));
    }
    // Dense fallback for NULL / FP16 / DECIMAL128: materialise n row hashes.
    // DECIMAL128 has no OpsTable hash slot (its hash is boundary-only); the
    // per-row hash here is the SAME cross-tier-consistent logic as .hash(), so a
    // DECIMAL128 key collides with the int64-decimal of the same value.
    uint64_t* out = static_cast<uint64_t*>(
        draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
    if (!out) throw std::bad_alloc();
    OwnedBuffer<uint64_t> out_owned(out);
    uint64_t scratch[1024];
    uint32_t i = 0u;
    if (v.vec.type == DRAKEN_NULL) {
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) scratch[j] = NULL_HASH;
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    } else if (v.vec.type == DRAKEN_DECIMAL128) {
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) {
                if (!row_is_valid(v.vec, i + j)) {
                    scratch[j] = NULL_HASH;
                } else {
                    const __int128 x = row_int128(v.vec, i + j);
                    const uint64_t lo = static_cast<uint64_t>(x);
                    const uint64_t hi = static_cast<uint64_t>(x >> 64);
                    scratch[j] = (hi == static_cast<uint64_t>(static_cast<int64_t>(lo) >> 63))
                        ? lo
                        : (lo ^ (hi * 0x9E3779B97F4A7C15ULL));
                }
            }
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    } else {  // FP16
        require_fp16_descriptor(v, "hash_shaped");
        const uint32_t dim = v.logical_type->dimension;
        const uint16_t* data = static_cast<const uint16_t*>(v.vec.data);
        while (i < n) {
            const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
            for (uint32_t j = 0u; j < block; ++j) {
                scratch[j] = row_is_valid(v.vec, i + j)
                    ? fp16_row_fnv_seed(data + v.vec.selection[i + j]
                                             * static_cast<size_t>(dim), dim)
                    : NULL_HASH;
            }
            simd_hash_i64(scratch, out + i, block);
            i += block;
        }
    }
    VecResult r;
    r.data = out_owned.release();
    r.validity = nullptr;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_INT64;
    r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return vecresult_to_owner(r);
}

// Keying hash over the group-key columns of a CxxMorsel → a 1-column CxxMorsel
// holding the INT64 hash vector. Mirrors Morsel.hash_keys EXACTLY: single key →
// shape-preserving (dict→dict) via hash_shaped_impl; multi key → dense mix
// (draken_hash per column + simd_mix_hash into a zeroed buffer). The 1-column
// wrapper lets the engine read columns[0].view and free via cxx_morsel_delete.
// GIL-free. Group keys are never DRAKEN_ARRAY (the binder rejects them).
static CxxMorsel cxx_hash(const CxxMorsel& m, const int32_t* col_idxs, uint32_t n_cols) {
    std::shared_ptr<VectorOwner> sp;
    if (n_cols == 1) {
        sp = std::make_shared<VectorOwner>(hash_shaped_impl(*m.columns[col_idxs[0]].own));
    } else {
        const uint32_t n = m.num_rows();
        uint64_t* buf = static_cast<uint64_t*>(
            draken_malloc((n > 0u ? n : 1u) * sizeof(uint64_t)));
        if (!buf) throw std::bad_alloc();
        OwnedBuffer<uint64_t> buf_owned(buf);
        std::memset(buf, 0, static_cast<size_t>(n) * sizeof(uint64_t));  // zeroed: required by simd_mix_hash
        if (n > 0u) {
            uint64_t* tmp = static_cast<uint64_t*>(draken_malloc(n * sizeof(uint64_t)));
            if (!tmp) throw std::bad_alloc();
            OwnedBuffer<uint64_t> tmp_owned(tmp);
            for (uint32_t c = 0u; c < n_cols; ++c) {
                draken_hash(m.columns[col_idxs[c]].view, tmp, n);
                simd_mix_hash(buf, tmp, static_cast<size_t>(n));
            }
        }
        VecResult r;
        r.data = buf_owned.release();
        r.validity = nullptr;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.data_length = n;
        r.length = n;
        r.type = DRAKEN_INT64;
        r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
        sp = std::make_shared<VectorOwner>(vecresult_to_owner(r));
    }
    CxxMorsel out;
    out.columns.push_back(CxxColumn{sp->vec, sp});
    out.names.push_back(std::string("$keyhash"));
    return out;
}
extern "C" CxxMorsel* cxx_hash_c(const CxxMorsel* m, const int32_t* col_idxs, uint32_t n_cols) {
    return new CxxMorsel(cxx_hash(*m, col_idxs, n_cols));
}

// Row-routing scatter — partition a morsel into W disjoint sub-morsels by
// hash(group-key) % W. Reuses cxx_hash (the SAME keying hash, so every
// occurrence of a key routes to one bin ⇒ the bins share no keys ⇒ a parallel
// grouped aggregate finalises by concatenation, never a merge) and cxx_take (so
// every column type, strings included, is materialised by the one tested take
// path). The only routing-specific work is the single bucketing pass. GIL-free.
// Group keys are never DRAKEN_ARRAY (the binder rejects them), same as cxx_hash.
//
// Uniform access only (§11): the routing key is read as h[selection[i]] with no
// shape discrimination — a dict-shaped single-key hash routes correctly because
// the read goes through `selection`, never the raw data array.
static std::vector<CxxMorsel> cxx_scatter(
        const CxxMorsel& m, const int32_t* col_idxs, uint32_t n_cols, uint32_t W) {
    if (W == 0u) throw std::invalid_argument("cxx_scatter: W must be >= 1");
    if (n_cols == 0u) throw std::invalid_argument("cxx_scatter: no key columns");
    const uint32_t n = m.num_rows();
    std::vector<std::vector<int32_t>> bins(W);
    if (n > 0u) {
        CxxMorsel hashm = cxx_hash(m, col_idxs, n_cols);   // 1-col INT64 hash vector
        const DrakenVector& hv = hashm.columns[0].view;
        const uint64_t* h = static_cast<const uint64_t*>(hv.data);
        const uint32_t* sel = hv.selection;                // never NULL (§11)
        for (uint32_t i = 0u; i < n; ++i)
            bins[h[sel[i]] % W].push_back(static_cast<int32_t>(i));
    }
    std::vector<CxxMorsel> out;
    out.reserve(W);
    for (uint32_t b = 0u; b < W; ++b) {
        const uint32_t bn = static_cast<uint32_t>(bins[b].size());
        out.push_back(cxx_take(m, bn > 0u ? bins[b].data() : nullptr, bn));
    }
    return out;
}

// ---------------------------------------------------------------------------
// S-B.1a — boundary bridges (Morsel ⇄ shared_ptr<CxxMorsel>).
//
// `CxxMorsel` is move-only; a shallow copy duplicates the `columns` vector, which
// copies each `CxxColumn{view, shared_ptr<VectorOwner> own}` — sharing the column
// OWNERS (refcount++), NOT the bytes. So two CxxMorsels can reference the same
// buffers, kept alive independently of any Python handle. Used by `morsel_to_cxx`
// (PyObject Morsel → owned heap CxxMorsel) and `cxx_to_morsel` (heap CxxMorsel →
// new-ref nanobind handle → Morsel). GIL-free except the cast (boundary only).
static inline void cxx_morsel_shallow_into(CxxMorsel& out, const CxxMorsel& m) {
    out.columns       = m.columns;        // copies vector<CxxColumn> → shares owners
    out.names         = m.names;
    out.zero_col_rows = m.zero_col_rows;
    out.state         = m.state;
}
extern "C" CxxMorsel* cxx_morsel_shallow_copy(const CxxMorsel* m) {
    CxxMorsel* out = new CxxMorsel();
    cxx_morsel_shallow_into(*out, *m);
    return out;
}
// S-B: the end-of-stream marker — a valid (empty) morsel carrying the EOS flag.
extern "C" CxxMorsel* cxx_morsel_new_eos() {
    CxxMorsel* out = new CxxMorsel();
    out->state = MorselState::END_OF_STREAM;
    return out;
}
// Wrap a CxxMorsel (shallow copy) into a NEW-reference nanobind handle (boundary out).
extern "C" PyObject* cxx_morsel_to_handle(const CxxMorsel* m) {
    CxxMorsel copy;
    cxx_morsel_shallow_into(copy, *m);
    nb::object obj = nb::cast(std::move(copy));
    obj.inc_ref();
    return obj.ptr();
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
        .value("VARIANT",      DRAKEN_VARIANT)
        .value("ARRAY",        DRAKEN_ARRAY)
        .value("NON_NATIVE",   DRAKEN_NON_NATIVE)
        .value("NULL",         DRAKEN_NULL)
        .value("VECTOR_FP16",  DRAKEN_VECTOR_FP16)
        .value("DECIMAL128",   DRAKEN_DECIMAL128)
        .export_values();

    // ------------------------------------------------------------------
    // Logical-type surface exposed to Python (D.8 / type-unification C-i).
    //
    // The binder/planner needs plan-time logical-type objects BEFORE any
    // vector exists. These bindings expose the existing C++ LogicalType
    // (logical_type.h) and its two enums faithfully, by value. The struct
    // carries logical PARAMETERS ONLY (kind, unit, offset, precision, scale,
    // dimension) — the physical type (DrakenType) is paired at the vector
    // level, not stored here (see doc 06 "physical vs logical type split").
    //
    // Value semantics: nanobind copies the small struct; equality/hash are by
    // value so a LogicalType is usable as a dict key (operator-map / schema).
    // Interning stays a C++ vector-side concern; Python does not hold borrowed
    // registry pointers.
    // ------------------------------------------------------------------
    nb::enum_<LogicalKind>(m, "LogicalKind")
        .value("NONE",      LogicalKind::NONE)
        .value("TIMESTAMP", LogicalKind::TIMESTAMP)
        .value("TIME",      LogicalKind::TIME)
        .value("DECIMAL",   LogicalKind::DECIMAL)
        .value("VECTOR",    LogicalKind::VECTOR)
        .export_values();

    nb::enum_<TimestampUnit>(m, "TimestampUnit")
        .value("SECONDS",      TimestampUnit::SECONDS)
        .value("MILLISECONDS", TimestampUnit::MILLISECONDS)
        .value("MICROSECONDS", TimestampUnit::MICROSECONDS)
        .value("NANOSECONDS",  TimestampUnit::NANOSECONDS)
        .export_values();

    nb::class_<LogicalType>(m, "LogicalType")
        .def("__init__",
             [](LogicalType* self, LogicalKind kind, TimestampUnit unit,
                int16_t offset_minutes, uint8_t precision, uint8_t scale,
                uint32_t dimension) {
                 new (self) LogicalType{kind, unit, offset_minutes,
                                        precision, scale, dimension};
             },
             nb::arg("kind") = LogicalKind::NONE,
             nb::arg("unit") = TimestampUnit::MICROSECONDS,
             nb::arg("offset_minutes") = static_cast<int16_t>(0),
             nb::arg("precision") = static_cast<uint8_t>(0),
             nb::arg("scale") = static_cast<uint8_t>(0),
             nb::arg("dimension") = static_cast<uint32_t>(0))
        .def_ro("kind",           &LogicalType::kind)
        .def_ro("unit",           &LogicalType::unit)
        .def_ro("offset_minutes", &LogicalType::offset_minutes)
        .def_ro("precision",      &LogicalType::precision)
        .def_ro("scale",          &LogicalType::scale)
        .def_ro("dimension",      &LogicalType::dimension)
        .def("__eq__", [](const LogicalType& a, nb::handle o) -> bool {
            if (!nb::isinstance<LogicalType>(o)) return false;
            return a == nb::cast<LogicalType>(o);
        })
        .def("__ne__", [](const LogicalType& a, nb::handle o) -> bool {
            if (!nb::isinstance<LogicalType>(o)) return true;
            return a != nb::cast<LogicalType>(o);
        })
        .def("__hash__", [](const LogicalType& a) -> Py_hash_t {
            size_t h = static_cast<size_t>(a.kind);
            h = h * 131u + static_cast<size_t>(a.unit);
            h = h * 131u + static_cast<size_t>(static_cast<uint16_t>(a.offset_minutes));
            h = h * 131u + static_cast<size_t>(a.precision);
            h = h * 131u + static_cast<size_t>(a.scale);
            h = h * 131u + static_cast<size_t>(a.dimension);
            Py_hash_t hv = static_cast<Py_hash_t>(h);
            return hv == static_cast<Py_hash_t>(-1) ? 0 : hv;
        })
        .def("__repr__", [](const LogicalType& a) {
            std::string r = "LogicalType(kind=";
            switch (a.kind) {
                case LogicalKind::NONE:      r += "NONE";      break;
                case LogicalKind::TIMESTAMP: r += "TIMESTAMP"; break;
                case LogicalKind::TIME:      r += "TIME";      break;
                case LogicalKind::DECIMAL:   r += "DECIMAL";   break;
                case LogicalKind::VECTOR:    r += "VECTOR";    break;
            }
            if (a.kind == LogicalKind::DECIMAL) {
                r += ", precision=" + std::to_string(a.precision)
                   + ", scale=" + std::to_string(a.scale);
            } else if (a.kind == LogicalKind::TIMESTAMP || a.kind == LogicalKind::TIME) {
                r += ", unit=" + std::string(unit_to_str(a.unit));
                if (a.kind == LogicalKind::TIMESTAMP)
                    r += ", offset_minutes=" + std::to_string(a.offset_minutes);
            } else if (a.kind == LogicalKind::VECTOR) {
                r += ", dimension=" + std::to_string(a.dimension);
            }
            r += ")";
            return r;
        })
        // copy/deepcopy support: LogicalType is a value-equal, hashable, immutable
        // descriptor (the C++ side interns them via logical_type_intern). copy/deepcopy
        // can correctly return self — there is no aliasing hazard. Required so that
        // FlatColumn instances (which may carry a ColumnType with a LogicalType) can
        // be deepcopied (the binder's merge_schemas does this per schema). Without it
        // copy.deepcopy raises `TypeError: cannot pickle 'LogicalType' object`.
        .def("__copy__",
             [](const LogicalType& self) -> LogicalType { return self; })
        .def("__deepcopy__",
             [](const LogicalType& self, nb::handle /*memo*/) -> LogicalType {
                 return self;
             });

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
            if (v.vec.type == DRAKEN_DECIMAL128) {
                require_decimal_descriptor(v, "__getitem__");
                return unscaled128_to_py_decimal(row_int128(v.vec, idx),
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
            if (v.vec.type == DRAKEN_VARIANT)   return row_string(v.vec, idx);
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
            const bool is_decimal128 = (v.vec.type == DRAKEN_DECIMAL128);
            const bool is_bool     = (v.vec.type == DRAKEN_BOOL);
            const bool is_varchar  = (v.vec.type == DRAKEN_VARCHAR || v.vec.type == DRAKEN_NVARCHAR || v.vec.type == DRAKEN_VARIANT);
            const bool is_binary   = (v.vec.type == DRAKEN_VARBINARY);
            const bool is_float    = is_float_type(v.vec.type);
            const bool is_time64   = (v.vec.type == DRAKEN_TIME64);
            if (is_time && !v.logical_type)
                throw std::invalid_argument(
                    "TIME vector is missing its logical-type descriptor");
            if (is_decimal || is_decimal128)
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
                } else if (is_decimal128) {
                    out.append(unscaled128_to_py_decimal(row_int128(v.vec, i),
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
        // (NULL_HASH-sentinel convention). Boxing at this edge only.
        .def("hash", [](const VectorOwner& v) {
            if (v.vec.type == DRAKEN_ARRAY)
                throw std::invalid_argument("hash: not supported for DRAKEN_ARRAY");
            const uint32_t n = v.vec.length;
            const size_t alloc = (n > 0u ? n : 1u) * sizeof(uint64_t);
            uint64_t* out = static_cast<uint64_t*>(draken_malloc(alloc));
            if (!out) throw std::bad_alloc();
            OwnedBuffer<uint64_t> out_owned(out);
            // The hash computation is pure C++ over the column; release the GIL
            // for it and re-acquire only to box the result list below.
            {
            nb::gil_scoped_release _gil;
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
            } else if (v.vec.type == DRAKEN_DECIMAL128) {
                // Hash the int128 unscaled value. A value that fits int64 hashes
                // IDENTICALLY to the int64-decimal of the same value (seed = low 64
                // bits, matching the int64 hash kernel) — so mixed-tier equal keys
                // collide correctly. Wider values mix both halves deterministically.
                uint64_t scratch[1024];
                uint32_t i = 0u;
                while (i < n) {
                    const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
                    for (uint32_t j = 0u; j < block; ++j) {
                        if (!row_is_valid(v.vec, i + j)) {
                            scratch[j] = NULL_HASH;
                        } else {
                            const __int128 x = row_int128(v.vec, i + j);
                            const uint64_t lo = static_cast<uint64_t>(x);
                            const uint64_t hi = static_cast<uint64_t>(x >> 64);
                            // sign-extension of lo ⇒ value fits int64 ⇒ seed = lo.
                            scratch[j] = (hi == static_cast<uint64_t>(static_cast<int64_t>(lo) >> 63))
                                ? lo
                                : (lo ^ (hi * 0x9E3779B97F4A7C15ULL));
                        }
                    }
                    simd_hash_i64(scratch, out + i, block);
                    i += block;
                }
            } else {
                draken_hash(v.vec, out, n);
            }
            }  // re-acquire GIL
            nb::list result;
            for (uint32_t i = 0u; i < n; ++i)
                result.append(nb::cast(out[i]));
            return result;
        })
        // ----------------------------------------------------------------
        // hash_shaped — shape-preserving hash vector (the keying surface for
        // group-by / join / distinct). Returns an INT64 Vector obeying the
        // draken_hash_shaped invariant: compressed key → dict-shaped hash
        // (k distinct hashes + codes); dense → n hashes. Null keys baked to
        // NULL_HASH, source validity carried as a passenger mask. NULL/FP16
        // key types fall back to a dense INT64 hash vector.
        .def("hash_shaped", [](const VectorOwner& v) -> VectorOwner {
            // Shape-preserving keying hash. Pure C++ in hash_shaped_impl (shared
            // with the nogil C-ABI cxx_hash_c) — release the GIL for the body.
            nb::gil_scoped_release _gil;
            return hash_shaped_impl(v);
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
                { nb::gil_scoped_release _gil; draken_float_sum(v.vec, &val); }
                return nb::cast(val);
            }
            if (v.vec.type == DRAKEN_DECIMAL128) {
                require_decimal_descriptor(v, "sum");
                __int128 acc = 0;
                { nb::gil_scoped_release _gil; dec128_sum_reduce(v.vec, acc); }
                return unscaled128_to_py_decimal(acc, v.logical_type->scale);
            }
            int64_t val = 0;
            { nb::gil_scoped_release _gil; draken_sum(v.vec, &val); }
            if (v.vec.type == DRAKEN_DECIMAL) {
                require_decimal_descriptor(v, "sum");
                return unscaled_to_py_decimal(val, v.logical_type->scale);
            }
            return nb::cast(val);
        })
        // min(): empty or all-null → raises ValueError.
        // TIMESTAMP64: returns datetime; DATE32: returns date; TIME32/64: returns time.
        // is_null_at(idx) — is logical row idx null? Used by ORDER BY
        // (heap_sort) which checks nullness before comparing values.
        .def("is_null_at", [](const VectorOwner& v, int64_t idx) -> bool {
            if (idx < 0 || static_cast<uint32_t>(idx) >= v.vec.length)
                throw std::out_of_range("is_null_at: index out of range");
            // DRAKEN_NULL: every row is null by definition.
            if (v.vec.type == DRAKEN_NULL) return true;
            return !row_is_valid(v.vec, static_cast<uint32_t>(idx));
        }, nb::arg("idx"),
           "True if logical row `idx` is null. For ORDER BY / heap-sort.")
        // compare_at(i, j) — compare two logical rows within this vector.
        // Returns -1/0/1 (value order; float uses total-order, NaN highest).
        // Null handling is the caller's responsibility (check is_null_at first).
        .def("compare_at", [](const VectorOwner& v, int64_t i, int64_t j) -> int {
            if (i < 0 || j < 0 ||
                static_cast<uint32_t>(i) >= v.vec.length ||
                static_cast<uint32_t>(j) >= v.vec.length)
                throw std::out_of_range("compare_at: index out of range");
            return draken_vector_compare_at(
                v.vec, static_cast<uint32_t>(i), static_cast<uint32_t>(j));
        }, nb::arg("i"), nb::arg("j"),
           "Compare two logical rows within this vector; returns -1/0/1. "
           "Float uses total-order (NaN highest). Caller checks is_null_at first.")
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
            // D.12: INTERVAL — custom scan returns original (months, us) of min slot.
            if (v.vec.type == DRAKEN_INTERVAL) {
                draken::ops::IntervalMinMaxResult r;
                { nb::gil_scoped_release _gil; r = draken::ops::interval_find_min(v.vec); }
                if (!r.found)
                    throw std::invalid_argument("Cannot compute min of all-null column");
                return interval_slot_to_py(DrakenIntervalSlot{r.months, r.us});
            }
            if (is_float_type(v.vec.type)) {
                double val = 0.0;
                uint32_t count;
                { nb::gil_scoped_release _gil; count = draken_float_min(v.vec, &val); }
                if (count == 0)
                    throw std::invalid_argument("Cannot compute min of all-null column");
                return nb::cast(val);
            }
            if (v.vec.type == DRAKEN_DECIMAL128) {
                require_decimal_descriptor(v, "min");
                __int128 best = 0; uint32_t cnt;
                { nb::gil_scoped_release _gil; cnt = dec128_min_reduce(v.vec, best); }
                if (cnt == 0)
                    throw std::invalid_argument("Cannot compute min of all-null column");
                return unscaled128_to_py_decimal(best, v.logical_type->scale);
            }
            int64_t val = 0;
            uint32_t count;
            { nb::gil_scoped_release _gil; count = draken_min(v.vec, &val); }
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
            // D.12: INTERVAL — custom scan returns original (months, us) of max slot.
            if (v.vec.type == DRAKEN_INTERVAL) {
                draken::ops::IntervalMinMaxResult r;
                { nb::gil_scoped_release _gil; r = draken::ops::interval_find_max(v.vec); }
                if (!r.found)
                    throw std::invalid_argument("Cannot compute max of all-null column");
                return interval_slot_to_py(DrakenIntervalSlot{r.months, r.us});
            }
            if (is_float_type(v.vec.type)) {
                double val = 0.0;
                uint32_t count;
                { nb::gil_scoped_release _gil; count = draken_float_max(v.vec, &val); }
                if (count == 0)
                    throw std::invalid_argument("Cannot compute max of all-null column");
                return nb::cast(val);
            }
            if (v.vec.type == DRAKEN_DECIMAL128) {
                require_decimal_descriptor(v, "max");
                __int128 best = 0; uint32_t cnt;
                { nb::gil_scoped_release _gil; cnt = dec128_max_reduce(v.vec, best); }
                if (cnt == 0)
                    throw std::invalid_argument("Cannot compute max of all-null column");
                return unscaled128_to_py_decimal(best, v.logical_type->scale);
            }
            int64_t val = 0;
            uint32_t count;
            { nb::gil_scoped_release _gil; count = draken_max(v.vec, &val); }
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
                if (a.type == DRAKEN_DECIMAL || b.type == DRAKEN_DECIMAL ||   \
                    a.type == DRAKEN_DECIMAL128 || b.type == DRAKEN_DECIMAL128)\
                    { nb::gil_scoped_release _gil; return decimal_fn(self, bo); } \
                if (is_integer_type(a.type) && is_integer_type(b.type)        \
                        && a.type != b.type) {                                 \
                    DrakenType wt = wider_int_type(a.type, b.type);            \
                    auto pa = maybe_promote(a, wt);                            \
                    auto pb = maybe_promote(b, wt);                            \
                    nb::gil_scoped_release _gil;                               \
                    return vecresult_to_owner(draken_fn(                       \
                        pa ? pa->vec : a, pb ? pb->vec : b));                  \
                }                                                              \
                /* Mixed-width float arithmetic: dispatch is on a.type, so a  */\
                /* FLOAT32 vs FLOAT64 pair would read one operand through the */\
                /* wrong-width kernel (garbage). Widen the FLOAT32 operand to */\
                /* FLOAT64 — the numeric result type of float32 op float64 is */\
                /* float64 — before dispatch. (float32_col + 5.0 reaches here */\
                /* because the literal is a FLOAT64 constant vector.)         */\
                if (is_float_type(a.type) && is_float_type(b.type)            \
                        && a.type != b.type) {                                 \
                    std::unique_ptr<VectorOwner> pa, pb;                       \
                    const DrakenVector* av = &a;                              \
                    const DrakenVector* bv = &b;                              \
                    if (a.type == DRAKEN_FLOAT32) {                           \
                        pa = std::make_unique<VectorOwner>(                    \
                            make_float64_from_numeric_vector(self));          \
                        av = &pa->vec;                                        \
                    }                                                          \
                    if (b.type == DRAKEN_FLOAT32) {                           \
                        pb = std::make_unique<VectorOwner>(                    \
                            make_float64_from_numeric_vector(bo));            \
                        bv = &pb->vec;                                        \
                    }                                                          \
                    nb::gil_scoped_release _gil;                               \
                    return vecresult_to_owner(draken_fn(*av, *bv));           \
                }                                                              \
                if (a.type != b.type)                                          \
                    throw std::invalid_argument(                               \
                        "cross-type vector arithmetic not supported");         \
                nb::gil_scoped_release _gil;                                   \
                return vecresult_to_owner(draken_fn(a, b));                    \
            }                                                                  \
            /* E.32: decimal × scalar not supported; promote scalar first. */  \
            if (self.vec.type == DRAKEN_DECIMAL)                               \
                throw std::invalid_argument(                                   \
                    std::string(#fn) + ": DECIMAL × scalar not supported; "   \
                    "promote scalar to DECIMAL first");                         \
            if (is_float_type(self.vec.type)) {                               \
                const double _s = nb::cast<double>(other);                     \
                nb::gil_scoped_release _gil;                                   \
                return vecresult_to_owner(draken_float_fn_s(self.vec, _s));    \
            }                                                                  \
            const int64_t _si = nb::cast<int64_t>(other);                     \
            nb::gil_scoped_release _gil;                                       \
            return vecresult_to_owner(draken_fn_s(self.vec, _si));            \
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
        // neg: unary negation; neg(INT64_MIN) wraps for integers.
        // E.32: DECIMAL neg raises on INT64_MIN (financial data; no silent wrap).
        .def("neg", [](const VectorOwner& v) -> VectorOwner {
            // Pure C++ on DrakenVector — release the GIL for the body.
            nb::gil_scoped_release _gil;
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
        // to_float64: numeric Vector (DECIMAL/INT64/FLOAT32/FLOAT64) → FLOAT64.
        // DECIMAL is divided by 10^scale to recover the real value.
        .def("to_float64", [](const VectorOwner& v) -> VectorOwner {
            return make_float64_from_numeric_vector(v);
        }, "Convert a numeric Vector to a dense FLOAT64 Vector (null-preserving).")
        // ----------------------------------------------------------------
        // DRAKEN_INTERVAL arithmetic — restored temporal binary ops.
        //
        // The generic add/sub/mul/div path (DRAKEN_BINOP_CROSS) routes through
        // the OpsTable, whose arithmetic slots for INTERVAL are null (component-
        // wise interval math doesn't fit the scalar arithmetic ABI). These named
        // methods call the dedicated interval kernels directly.
        // ----------------------------------------------------------------
        // interval_add / interval_sub: INTERVAL × INTERVAL → INTERVAL (component-wise).
        .def("interval_add", [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
            nb::gil_scoped_release _gil;
            if (self.vec.type != DRAKEN_INTERVAL || other.vec.type != DRAKEN_INTERVAL)
                throw std::invalid_argument("interval_add: both operands must be DRAKEN_INTERVAL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("interval_add: operands must have equal length");
            return vecresult_to_owner(draken::ops::interval_add(self.vec, other.vec));
        }, nb::arg("other"), "INTERVAL + INTERVAL → INTERVAL (component-wise months/µs).")
        .def("interval_sub", [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
            nb::gil_scoped_release _gil;
            if (self.vec.type != DRAKEN_INTERVAL || other.vec.type != DRAKEN_INTERVAL)
                throw std::invalid_argument("interval_sub: both operands must be DRAKEN_INTERVAL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("interval_sub: operands must have equal length");
            return vecresult_to_owner(draken::ops::interval_sub(self.vec, other.vec));
        }, nb::arg("other"), "INTERVAL - INTERVAL → INTERVAL (component-wise months/µs).")
        // apply_to_temporal: (DATE32 | TIMESTAMP64) ± INTERVAL → TIMESTAMP64 (µs).
        // `self` is the temporal vector; `interval` the interval vector; signum
        // +1 for Plus, -1 for Minus. SQL calendar semantics with day-clamping.
        .def("apply_to_temporal",
            [](const VectorOwner& self, const VectorOwner& interval, int signum) -> VectorOwner {
                const bool is_date = (self.vec.type == DRAKEN_DATE32);
                if (!is_date && self.vec.type != DRAKEN_TIMESTAMP64)
                    throw std::invalid_argument(
                        "apply_to_temporal: temporal operand must be DATE32 or TIMESTAMP64");
                if (interval.vec.type != DRAKEN_INTERVAL)
                    throw std::invalid_argument(
                        "apply_to_temporal: second operand must be DRAKEN_INTERVAL");
                if (self.vec.length != interval.vec.length)
                    throw std::invalid_argument(
                        "apply_to_temporal: operands must have equal length");
                int src_unit = static_cast<int>(TimestampUnit::MICROSECONDS);
                if (!is_date && self.logical_type)
                    src_unit = static_cast<int>(self.logical_type->unit);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken::ops::interval_apply_to_temporal(
                    self.vec, interval.vec, is_date, src_unit, signum));
            }, nb::arg("interval"), nb::arg("signum"),
            "(DATE32|TIMESTAMP64) ± INTERVAL → TIMESTAMP64 (µs); calendar month day-clamping.")
        // temporal_minus_temporal: (DATE32|TIMESTAMP64) - (DATE32|TIMESTAMP64) → INTERVAL.
        .def("temporal_minus_temporal",
            [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
                const bool a_date = (self.vec.type == DRAKEN_DATE32);
                const bool b_date = (other.vec.type == DRAKEN_DATE32);
                if ((!a_date && self.vec.type != DRAKEN_TIMESTAMP64) ||
                    (!b_date && other.vec.type != DRAKEN_TIMESTAMP64))
                    throw std::invalid_argument(
                        "temporal_minus_temporal: operands must be DATE32 or TIMESTAMP64");
                if (self.vec.length != other.vec.length)
                    throw std::invalid_argument(
                        "temporal_minus_temporal: operands must have equal length");
                int a_unit = static_cast<int>(TimestampUnit::MICROSECONDS);
                int b_unit = static_cast<int>(TimestampUnit::MICROSECONDS);
                if (!a_date && self.logical_type)  a_unit = static_cast<int>(self.logical_type->unit);
                if (!b_date && other.logical_type) b_unit = static_cast<int>(other.logical_type->unit);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken::ops::temporal_minus_temporal(
                    self.vec, other.vec, a_date, a_unit, b_date, b_unit));
            }, nb::arg("other"),
            "(DATE32|TIMESTAMP64) - (DATE32|TIMESTAMP64) → INTERVAL (µs delta, months=0).")
        // set_decimal_descriptor: attach a DECIMAL (precision, scale) logical-type
        // descriptor in place. Used when a DECIMAL-typed buffer is assembled from
        // raw int64 storage (e.g. CASE WHEN result scatter) and the scale/precision
        // must be carried from the source vectors.
        .def("set_decimal_descriptor", [](VectorOwner& v, int precision, int scale) {
            if (v.vec.type != DRAKEN_DECIMAL && v.vec.type != DRAKEN_DECIMAL128)
                throw std::invalid_argument("set_decimal_descriptor: not a DECIMAL vector");
            LogicalType lt{};
            lt.kind = LogicalKind::DECIMAL;
            lt.precision = static_cast<uint8_t>(precision);
            lt.scale     = static_cast<uint8_t>(scale);
            v.logical_type = logical_type_intern(lt);
        }, nb::arg("precision"), nb::arg("scale"),
            "Attach a DECIMAL (precision, scale) descriptor to this Vector in place.")
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
            // Pure C++ Kleene op on DrakenVector — release the GIL for the body.
            nb::gil_scoped_release _gil;
            if (self.vec.type != DRAKEN_BOOL || other.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_and: both operands must be DRAKEN_BOOL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("bool_and: operands must have equal length");
            return vecresult_to_owner(draken::ops::bool_and(self.vec, other.vec));
        }, nb::arg("other"),
            "Kleene AND: BOOL × BOOL → BOOL. FALSE dominates (F∧N=F). T∧N=N.")
        .def("bool_or", [](const VectorOwner& self, const VectorOwner& other) -> VectorOwner {
            // Pure C++ Kleene op on DrakenVector — release the GIL for the body.
            nb::gil_scoped_release _gil;
            if (self.vec.type != DRAKEN_BOOL || other.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_or: both operands must be DRAKEN_BOOL");
            if (self.vec.length != other.vec.length)
                throw std::invalid_argument("bool_or: operands must have equal length");
            return vecresult_to_owner(draken::ops::bool_or(self.vec, other.vec));
        }, nb::arg("other"),
            "Kleene OR: BOOL × BOOL → BOOL. TRUE dominates (T∨N=T). F∨N=N.")
        .def("bool_not", [](const VectorOwner& v) -> VectorOwner {
            // Pure C++ Kleene op on DrakenVector — release the GIL for the body.
            nb::gil_scoped_release _gil;
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_not: operand must be DRAKEN_BOOL");
            return vecresult_to_owner(draken::ops::bool_not(v.vec));
        },
            "Kleene NOT: ¬T=F, ¬F=T, ¬N=N (validity preserved).")
        .def("bool_any", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_any: operand must be DRAKEN_BOOL");
            int8_t r;
            { nb::gil_scoped_release _gil; r = draken::ops::bool_any(v.vec); }
            if (r < 0) return nb::none();
            return nb::cast(r == 1);
        },
            "SQL ANY (bool_or reduction). True/False/None. Empty → False.")
        .def("bool_all", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("bool_all: operand must be DRAKEN_BOOL");
            int8_t r;
            { nb::gil_scoped_release _gil; r = draken::ops::bool_all(v.vec); }
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
            std::vector<int32_t> idx_vec(n);
            for (uint32_t i = 0; i < n; ++i)
                idx_vec[i] = nb::cast<int32_t>(indices[i]);
            // The index list is materialised under the GIL above; the gather
            // itself is pure C++ on DrakenVector, so run it off-GIL.
            nb::gil_scoped_release _gil;
            return vector_take_impl(v, idx_vec.data(), n);
        })
        // slice(start, length) — contiguous subrange. No Python index list; direct
        // memcpy for dense vectors. Equivalent to take(range(start, start+length))
        // but without materialising an index array at any level.
        .def("slice", [](const VectorOwner& v, uint32_t start, uint32_t length) -> VectorOwner {
            nb::gil_scoped_release _gil;  // pure C++ body; GIL not needed (CLAUDE.md §2)
            return vector_slice_impl(v, start, length);
        })
        // mask(bool_vector) — keep rows where the mask is valid AND true, gather
        // natively. The surviving-row indices are derived from the mask's bitmap
        // via the unified row_is_valid/row_bool accessors (correct for dense,
        // dict, and constant shapes); the gather then reuses the same typed take
        // dispatch as .take(). No Python, no boxed index list.
        .def("mask", [](const VectorOwner& v, const VectorOwner& m) -> VectorOwner {
            // Dominant cost of Filter's filter_mask gather; pure C++, GIL released.
            nb::gil_scoped_release _gil;
            return vector_mask_impl(v, m);
        })
        // count_true() — number of rows that are valid AND true. Used to size a
        // zero-column filtered morsel (filter on a projected-away column).
        .def("count_true", [](const VectorOwner& m) -> int64_t {
            // Pure C++ scan; int64_t return boxed after the lambda. Release GIL.
            nb::gil_scoped_release _gil;
            if (m.vec.type != DRAKEN_BOOL)
                throw std::invalid_argument("count_true: expected a DRAKEN_BOOL vector");
            const uint32_t n = m.vec.length;
            int64_t c = 0;
            for (uint32_t i = 0; i < n; ++i)
                if (row_is_valid(m.vec, i) && row_bool(m.vec, i)) ++c;
            return c;
        })
        .def("materialize", [](const VectorOwner& v) -> VectorOwner {
            // Pure C++ on DrakenVector structs — release the GIL for the body.
            nb::gil_scoped_release _gil;
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
            // Pure C++ on DrakenVector structs — release the GIL for the body.
            nb::gil_scoped_release _gil;
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
                // Scale-aware: convert the literal at its OWN scale and let the
                // kernel align magnitudes in int128. Unlike storage, comparison
                // does not require the literal to be exact at the column scale.
                __int128 b_unscaled; uint8_t sb;
                py_scalar_to_unscaled_scale(scalar.ptr(), b_unscaled, sb);
                const uint8_t sc = v.logical_type->scale;
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken::ops::dec_compare_scalar(
                    v.vec, sc, b_unscaled, sb, op));
            }
            if (v.vec.type == DRAKEN_DECIMAL128) {
                require_decimal_descriptor(v, "compare_scalar");
                __int128 b_unscaled; uint8_t sb;
                py_scalar_to_unscaled_scale(scalar.ptr(), b_unscaled, sb);
                const uint8_t sc = v.logical_type->scale;
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken::ops::dec128_compare_scalar(
                    v.vec, sc, b_unscaled, sb, op));
            }
            if (v.vec.type == DRAKEN_TIMESTAMP64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "compare_scalar: TIMESTAMP64 requires a logical-type descriptor");
                // Accept either a Python int (already-coerced microseconds-since-epoch)
                // or a datetime.datetime (converted via py_datetime_to_instant).
                int64_t ts;
                if (PyLong_Check(scalar.ptr()))
                    ts = nb::cast<int64_t>(scalar);
                else
                    ts = py_datetime_to_instant(scalar, v.logical_type->unit);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken_compare_scalar(v.vec, ts, op));
            }
            if (v.vec.type == DRAKEN_DATE32) {
                // Accept either a Python int (already-coerced days-since-epoch,
                // as produced by _coerce_date32) or a datetime.date — mirroring
                // the TIMESTAMP64 branch above.
                int64_t days;
                if (PyLong_Check(scalar.ptr()))
                    days = nb::cast<int64_t>(scalar);
                else
                    days = static_cast<int64_t>(py_date_to_days(scalar.ptr()));
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken_compare_scalar(v.vec, days, op));
            }
            if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                if (!v.logical_type)
                    throw std::invalid_argument(
                        "compare_scalar: TIME vector requires a logical-type descriptor");
                const int64_t raw = py_time_to_raw(scalar.ptr(), v.logical_type->unit);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken_compare_scalar(v.vec, raw, op));
            }
            // D.12: INTERVAL — scalar is (months, us) tuple; normalize then dispatch.
            if (v.vec.type == DRAKEN_INTERVAL) {
                const DrakenIntervalSlot slot = py_to_interval_slot(scalar);
                const int64_t norm = draken::ops::interval_normalize_checked(
                    slot.months, slot.us);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(draken_compare_scalar(v.vec, norm, op));
            }
            if (v.vec.type == DRAKEN_VARCHAR || v.vec.type == DRAKEN_NVARCHAR
                    || v.vec.type == DRAKEN_VARBINARY) {
                // Bytewise comparison for all string subtypes. The scalar must be
                // bytes (str is encoded at the binder; CLAUDE.md §1). Copy the
                // bytes into an owned buffer so NO Python-owned memory is read in
                // the released-GIL window (structural off-GIL fix, not a pre-copy
                // band-aid on a borrowed Python buffer).
                PyObject* pybytes = scalar.ptr();
                if (!PyBytes_Check(pybytes))
                    throw std::invalid_argument(
                        "compare_scalar: string vector requires a bytes scalar");
                char* bsrc = nullptr;
                Py_ssize_t blen = 0;
                if (PyBytes_AsStringAndSize(pybytes, &bsrc, &blen) < 0)
                    throw nb::python_error();
                std::string owned(bsrc, static_cast<size_t>(blen));
                const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(owned.data());
                const uint32_t ulen   = static_cast<uint32_t>(blen);
                DrakenStringSlot scalar_slot;
                if (ulen <= STR_INLINE_MAX) {
                    str_init_inline(&scalar_slot, ubytes, ulen);
                } else {
                    // arena_offset=0: str_data(&scalar_slot, ubytes) returns ubytes.
                    str_init_extern(&scalar_slot, ubytes, ulen,
                                    (uint32_t)XXH3_64bits(ubytes, ulen), 0u);
                }
                // `owned` (C++ storage, not Python memory) backs ubytes across
                // the released-GIL window and lives to the end of this scope.
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(
                    draken_str_compare_scalar(v.vec, scalar_slot, ubytes, op));
            }
            // FLOAT32/64: scalar is Python float (or int coerced to double).
            if (is_float_type(v.vec.type)) {
                const double s = nb::cast<double>(scalar);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(
                    draken_float_compare_scalar(v.vec, s, op));
            }
            // INT64 (and other types): expect int scalar.
            const int64_t si = nb::cast<int64_t>(scalar);
            nb::gil_scoped_release _gil;
            return vecresult_to_owner(draken_compare_scalar(v.vec, si, op));
        }, nb::arg("scalar"), nb::arg("op"),
            "Compare each row against scalar. op: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le.\n"
            "INT64: scalar is int. STRING: scalar is str.\n"
            "Returns a DRAKEN_BOOL vector (bit-packed, 1 bit/row, LSB-first).")
        .def("compare_vector", [](const VectorOwner& self, const VectorOwner& other, int op) -> VectorOwner {
            // Both operands are pre-unwrapped VectorOwner refs; the body touches
            // only DrakenVector/logical_type (C++) and returns a VectorOwner
            // converted after the lambda. Release the GIL for the whole body.
            nb::gil_scoped_release _gil;
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
            // DECIMAL128: any int128-decimal operand. Widen an int64-decimal / INT64
            // operand to int128, then compare scale-aware in int128. Checked BEFORE the
            // int64 DECIMAL branch so a (DECIMAL128, DECIMAL) pair isn't misread as int64.
            if (a.type == DRAKEN_DECIMAL128 || b.type == DRAKEN_DECIMAL128) {
                uint8_t sa, sb;
                if (a.type == DRAKEN_DECIMAL128 || a.type == DRAKEN_DECIMAL) {
                    if (!self.logical_type)
                        throw std::invalid_argument("compare_vector: DECIMAL requires a logical-type descriptor");
                    sa = self.logical_type->scale;
                } else if (a.type == DRAKEN_INT64) {
                    sa = 0;
                } else {
                    throw std::invalid_argument("compare_vector: cannot compare DECIMAL128 with this type (only DECIMAL/DECIMAL128/INT64)");
                }
                if (b.type == DRAKEN_DECIMAL128 || b.type == DRAKEN_DECIMAL) {
                    if (!other.logical_type)
                        throw std::invalid_argument("compare_vector: DECIMAL requires a logical-type descriptor");
                    sb = other.logical_type->scale;
                } else if (b.type == DRAKEN_INT64) {
                    sb = 0;
                } else {
                    throw std::invalid_argument("compare_vector: cannot compare DECIMAL128 with this type (only DECIMAL/DECIMAL128/INT64)");
                }
                std::unique_ptr<VectorOwner> aw, bw;
                const DrakenVector* av = &a;
                const DrakenVector* bv = &b;
                if (a.type != DRAKEN_DECIMAL128) { aw = std::make_unique<VectorOwner>(widen_decimal_to_i128(self, sa, 38)); av = &aw->vec; }
                if (b.type != DRAKEN_DECIMAL128) { bw = std::make_unique<VectorOwner>(widen_decimal_to_i128(other, sb, 38)); bv = &bw->vec; }
                return vecresult_to_owner(draken::ops::dec128_compare_vector(*av, sa, *bv, sb, op));
            }
            // DECIMAL: cross-scale comparison is a hard error — different scales
            // store different magnitudes; silently mis-comparing them produces wrong
            // answers.  Scale alignment (requires int128 rescale) is deferred to pt2.
            if (a.type == DRAKEN_DECIMAL || b.type == DRAKEN_DECIMAL) {
                // Scale-aware comparison: operands may differ in scale, and an
                // INT64 operand is a scale-0 decimal (int64 payload read as-is).
                // Magnitudes are aligned in int128 by dec_compare_vector.
                uint8_t sa, sb;
                if (a.type == DRAKEN_DECIMAL) {
                    if (!self.logical_type)
                        throw std::invalid_argument(
                            "compare_vector: DECIMAL requires a logical-type descriptor");
                    sa = self.logical_type->scale;
                } else if (a.type == DRAKEN_INT64) {
                    sa = 0;
                } else {
                    throw std::invalid_argument(
                        "compare_vector: cannot compare DECIMAL with this type "
                        "(only DECIMAL or INT64)");
                }
                if (b.type == DRAKEN_DECIMAL) {
                    if (!other.logical_type)
                        throw std::invalid_argument(
                            "compare_vector: DECIMAL requires a logical-type descriptor");
                    sb = other.logical_type->scale;
                } else if (b.type == DRAKEN_INT64) {
                    sb = 0;
                } else {
                    throw std::invalid_argument(
                        "compare_vector: cannot compare DECIMAL with this type "
                        "(only DECIMAL or INT64)");
                }
                return vecresult_to_owner(draken::ops::dec_compare_vector(a, sa, b, sb, op));
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
            // Mixed-width float comparison: dispatch is on a.type, so a FLOAT32
            // vs FLOAT64 pair would read one operand's bytes through the wrong
            // kernel (a 4-byte float* over an 8-byte double buffer, or vice
            // versa) and compare against garbage. Widen the FLOAT32 operand to
            // FLOAT64 so both are read at a common width. Only the FLOAT32 side
            // is materialized, so a FLOAT64 constant literal keeps its constant
            // shape and the kernel's scalar fast-path still fires.
            if (is_float_type(a.type) && is_float_type(b.type) && a.type != b.type) {
                std::unique_ptr<VectorOwner> pa, pb;
                const DrakenVector* av = &a;
                const DrakenVector* bv = &b;
                if (a.type == DRAKEN_FLOAT32) {
                    pa = std::make_unique<VectorOwner>(make_float64_from_numeric_vector(self));
                    av = &pa->vec;
                }
                if (b.type == DRAKEN_FLOAT32) {
                    pb = std::make_unique<VectorOwner>(make_float64_from_numeric_vector(other));
                    bv = &pb->vec;
                }
                return vecresult_to_owner(draken_compare_vector(*av, *bv, op));
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
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_u, hi_u, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_TIMESTAMP64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "between: TIMESTAMP64 requires a logical-type descriptor");
                    const int64_t lo_i = py_datetime_to_instant(lo, v.logical_type->unit);
                    const int64_t hi_i = py_datetime_to_instant(hi, v.logical_type->unit);
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_DATE32) {
                    // Accept Python ints (already-coerced days-since-epoch, as
                    // produced by _coerce_date32) or datetime.date bounds.
                    const int64_t lo_i = PyLong_Check(lo.ptr())
                        ? nb::cast<int64_t>(lo)
                        : static_cast<int64_t>(py_date_to_days(lo.ptr()));
                    const int64_t hi_i = PyLong_Check(hi.ptr())
                        ? nb::cast<int64_t>(hi)
                        : static_cast<int64_t>(py_date_to_days(hi.ptr()));
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_TIME32 || v.vec.type == DRAKEN_TIME64) {
                    if (!v.logical_type)
                        throw std::invalid_argument(
                            "between: TIME vector requires a logical-type descriptor");
                    const int64_t lo_i = py_time_to_raw(lo.ptr(), v.logical_type->unit);
                    const int64_t hi_i = py_time_to_raw(hi.ptr(), v.logical_type->unit);
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_i, hi_i, lo_inclusive, hi_inclusive));
                }
                // D.12: INTERVAL — bounds are (months, us) tuples; normalize.
                if (v.vec.type == DRAKEN_INTERVAL) {
                    const DrakenIntervalSlot lo_s = py_to_interval_slot(lo);
                    const DrakenIntervalSlot hi_s = py_to_interval_slot(hi);
                    const int64_t lo_us = draken::ops::interval_normalize_checked(
                        lo_s.months, lo_s.us);
                    const int64_t hi_us = draken::ops::interval_normalize_checked(
                        hi_s.months, hi_s.us);
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(
                        draken_between(v.vec, lo_us, hi_us, lo_inclusive, hi_inclusive));
                }
                if (is_float_type(v.vec.type)) {
                    const double lo_d = nb::cast<double>(lo);
                    const double hi_d = nb::cast<double>(hi);
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(draken_float_between(
                        v.vec, lo_d, hi_d, lo_inclusive, hi_inclusive));
                }
                if (v.vec.type == DRAKEN_VARCHAR || v.vec.type == DRAKEN_NVARCHAR
                        || v.vec.type == DRAKEN_VARBINARY) {
                    // Bounds must be bytes (str encoded at binder; CLAUDE.md §1).
                    // Copy into owned C++ storage so NO Python memory is read in
                    // the released-GIL window (structural off-GIL fix).
                    auto to_owned = [](nb::object pyobj, const char* which) -> std::string {
                        PyObject* pybytes = pyobj.ptr();
                        if (!PyBytes_Check(pybytes))
                            throw std::invalid_argument(
                                std::string("between: string vector requires a bytes bound for ") + which);
                        char* p = nullptr; Py_ssize_t n = 0;
                        if (PyBytes_AsStringAndSize(pybytes, &p, &n) < 0) throw nb::python_error();
                        return std::string(p, static_cast<size_t>(n));
                    };
                    auto make_slot = [](const std::string& s) -> DrakenStringSlot {
                        const uint8_t* u  = reinterpret_cast<const uint8_t*>(s.data());
                        const uint32_t ul = static_cast<uint32_t>(s.size());
                        DrakenStringSlot slot;
                        if (ul <= STR_INLINE_MAX)
                            str_init_inline(&slot, u, ul);
                        else
                            str_init_extern(&slot, u, ul, (uint32_t)XXH3_64bits(u, ul), 0u);
                        return slot;
                    };
                    std::string lo_owned = to_owned(lo, "lo");
                    std::string hi_owned = to_owned(hi, "hi");
                    DrakenStringSlot lo_slot = make_slot(lo_owned);
                    DrakenStringSlot hi_slot = make_slot(hi_owned);
                    const uint8_t* lo_bytes = reinterpret_cast<const uint8_t*>(lo_owned.data());
                    const uint8_t* hi_bytes = reinterpret_cast<const uint8_t*>(hi_owned.data());
                    // lo_owned/hi_owned (C++ storage) back the byte pointers across
                    // the released-GIL window; they live to the end of this scope.
                    nb::gil_scoped_release _gil;
                    return vecresult_to_owner(draken_str_between(
                        v.vec,
                        lo_slot, lo_bytes,
                        hi_slot, hi_bytes,
                        lo_inclusive, hi_inclusive));
                }
                const int64_t lo_l = nb::cast<int64_t>(lo);
                const int64_t hi_l = nb::cast<int64_t>(hi);
                nb::gil_scoped_release _gil;
                return vecresult_to_owner(
                    draken_between(v.vec, lo_l, hi_l, lo_inclusive, hi_inclusive));
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
                    { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
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
                    { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
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
                    { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
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
                    { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
                }
                // D.12: INTERVAL — normalize each value to total_ms, hash.
                if (v.vec.type == DRAKEN_INTERVAL) {
                    for (size_t k = 0; k < n; ++k) {
                        nb::object obj = values[static_cast<Py_ssize_t>(k)];
                        if (obj.is_none()) continue;  // null values never match non-null rows
                        const DrakenIntervalSlot slot = py_to_interval_slot(obj);
                        const int64_t norm = draken::ops::interval_normalize_checked(
                            slot.months, slot.us);
                        uint64_t raw = static_cast<uint64_t>(norm);
                        uint64_t h;
                        simd_hash_i64(&raw, &h, 1u);
                        set.insert_or_ignore(h);
                    }
                    { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
                }
                if (v.vec.type == DRAKEN_VARCHAR || v.vec.type == DRAKEN_NVARCHAR
                        || v.vec.type == DRAKEN_VARBINARY) {
                    // Hash-only membership via CarcharSet; same str_hash_seed →
                    // simd_hash_i64 path as str_in_list and hash_string.  Any
                    // deviation here causes present values to miss. Values must be
                    // bytes (str encoded at binder; CLAUDE.md §1). Hashing runs
                    // GIL-held, so no off-GIL borrowed-buffer concern here.
                    for (size_t k = 0; k < n; ++k) {
                        PyObject* pybytes = values[static_cast<Py_ssize_t>(k)].ptr();
                        if (!PyBytes_Check(pybytes))
                            throw std::invalid_argument(
                                "in_list: string vector requires bytes values");
                        char* bptr = nullptr;
                        Py_ssize_t slen = 0;
                        if (PyBytes_AsStringAndSize(pybytes, &bptr, &slen) < 0)
                            throw nb::python_error();
                        const uint8_t* ubytes = reinterpret_cast<const uint8_t*>(bptr);
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
                { nb::gil_scoped_release _gil; return vecresult_to_owner(draken_in_list(v.vec, set)); }
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
        .def_prop_ro("array_child", [](const VectorOwner& v) -> nb::object {
            if (v.vec.type != DRAKEN_ARRAY)
                throw std::invalid_argument("array_child: vector is not DRAKEN_ARRAY type");
            if (!v.child_owner)
                throw std::runtime_error("array_child: DRAKEN_ARRAY vector has no child");
            // Return an independently-owned copy of the child via identity take.
            const uint32_t cn = v.child_owner->vec.length;
            std::vector<int32_t> all_idx(cn);
            for (uint32_t i = 0; i < cn; ++i) all_idx[i] = static_cast<int32_t>(i);
            return nb::cast(take_child(*v.child_owner, all_idx));
        }, "Child Vector of a DRAKEN_ARRAY vector as a new independently-owned Vector. Raises for non-array vectors.")
        // ----------------------------------------------------------------
        // Shape predicates — canonical via draken_is_* in buffers.h.
        //   is_dense      data_length == length
        //   is_compressed data_length <  length   (== is_dict || is_constant)
        //   is_constant   data_length == 1
        //   is_dict       1 < data_length < length
        .def_prop_ro("is_dense", [](const VectorOwner& v) {
            return draken_is_dense(&v.vec) != 0;
        })
        .def_prop_ro("is_compressed", [](const VectorOwner& v) {
            return draken_is_compressed(&v.vec) != 0;
        })
        .def_prop_ro("is_dict", [](const VectorOwner& v) {
            return draken_is_dict(&v.vec) != 0;
        })
        .def_prop_ro("is_constant", [](const VectorOwner& v) {
            return draken_is_constant(&v.vec) != 0;
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
            // max_display_width() — return the maximum rendered byte length across all
            // logical rows when the vector is formatted for display (e.g. Morsel.__str__).
            //
            // For string-family types (VARCHAR/NVARCHAR/VARBINARY/VARIANT) the length is
            // read directly from the slot header — zero deserialization, zero Python objects.
            // For dict-encoded string vectors only the data_length unique slots are scanned
            // (not the full selection array), so dict vectors are O(distinct) not O(rows).
            //
            // For all other types the value is formatted into a stack buffer using the same
            // path as to_pylist, and the byte length of that formatting is measured.
            // Null rows contribute 4 bytes ("null").  Returns 0 for empty or all-null vectors.
            .def("max_display_width", [](const VectorOwner& v) -> uint32_t {
                const uint32_t n = v.vec.length;
                if (n == 0u) return 0u;

                const uint32_t null_len = 4u;  // len("null")
                uint32_t best = 0u;

                // --- String family: read lengths directly from the arena slots ----------
                // Iterate over data_length unique slots — works for dense, constant, and
                // dict shapes uniformly without touching the selection array at all.
                if (is_varchar_family(v.vec.type) || v.vec.type == DRAKEN_VARIANT) {
                    // Check whether any rows are null so we account for "null" width.
                    bool any_null = false;
                    for (uint32_t i = 0u; i < n && !any_null; ++i)
                        if (!row_is_valid(v.vec, i)) any_null = true;
                    if (any_null && best < null_len) best = null_len;

                    const DrakenStringArena* sa =
                        static_cast<const DrakenStringArena*>(v.vec.data);
                    if (sa) {
                        for (uint32_t k = 0u; k < v.vec.data_length; ++k) {
                            const uint32_t len = str_length(&sa->slots[k]);
                            if (len > best) best = len;
                        }
                    }
                    return best;
                }

                // --- NULL vector: every row is null -----------------------------------
                if (v.vec.type == DRAKEN_NULL) return null_len;

                // --- All other types: format unique data values into a stack buffer ---
                // For dict/constant shapes scan only data_length unique values; for dense
                // shapes data_length == length so this is still O(rows).
                // A char[64] stack buffer is large enough for any scalar we render:
                //   int64: max 20 chars; float64: ≤25 chars (ryu); bool: 5; timestamps: 26.
                char buf[64];
                const uint32_t nd = v.vec.data_length;

                // Helper: measure a null-row contribution once.
                bool any_null = false;
                for (uint32_t i = 0u; i < n && !any_null; ++i)
                    if (!row_is_valid(v.vec, i)) any_null = true;
                if (any_null && best < null_len) best = null_len;

                // Iterate over the unique data slots (index k into data[], not selection[]).
                // We build a synthetic per-unique-slot DrakenVector view that reuses the
                // existing row_* helpers without allocating anything.
                for (uint32_t k = 0u; k < nd; ++k) {
                    size_t len = 0u;
                    switch (v.vec.type) {
                        case DRAKEN_INT64:
                        case DRAKEN_INT8:
                        case DRAKEN_INT16:
                        case DRAKEN_INT32: {
                            // row_narrow_int reads data[selection[i]]; for our synthetic
                            // dense view selection[k]==k, so pass k directly as the index.
                            const int64_t val = (v.vec.type == DRAKEN_INT64)
                                ? static_cast<const int64_t*>(v.vec.data)[k]
                                : static_cast<int64_t>(([&]() -> int64_t {
                                    switch (v.vec.type) {
                                        case DRAKEN_INT8:  return static_cast<const int8_t* >(v.vec.data)[k];
                                        case DRAKEN_INT16: return static_cast<const int16_t*>(v.vec.data)[k];
                                        default:           return static_cast<const int32_t*>(v.vec.data)[k];
                                    }
                                  })());
                            len = static_cast<size_t>(
                                std::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(val)));
                            break;
                        }
                        case DRAKEN_FLOAT32:
                        case DRAKEN_FLOAT64: {
                            const double val = (v.vec.type == DRAKEN_FLOAT32)
                                ? static_cast<double>(static_cast<const float* >(v.vec.data)[k])
                                : static_cast<const double*>(v.vec.data)[k];
                            len = ryu_format_double(buf, val, 10u);
                            break;
                        }
                        case DRAKEN_BOOL: {
                            const uint32_t bit_idx = k;
                            const uint8_t* bdata = static_cast<const uint8_t*>(v.vec.data);
                            const bool val = static_cast<bool>(
                                (bdata[bit_idx >> 3] >> (bit_idx & 7)) & 1u);
                            len = val ? 4u : 5u;  // "true" / "false"
                            break;
                        }
                        case DRAKEN_DATE32: {
                            // ISO date: YYYY-MM-DD = 10 chars always.
                            len = 10u;
                            break;
                        }
                        case DRAKEN_TIMESTAMP64: {
                            // ISO datetime: "YYYY-MM-DD HH:MM:SS" = 19 chars; with microseconds ≤26.
                            len = 26u;
                            break;
                        }
                        case DRAKEN_TIME32:
                        case DRAKEN_TIME64: {
                            // "HH:MM:SS" = 8 chars; with sub-second ≤15.
                            len = 15u;
                            break;
                        }
                        case DRAKEN_INTERVAL: {
                            // "Xmo Yd Zh Wm Vs" — conservatively 20 chars.
                            len = 20u;
                            break;
                        }
                        case DRAKEN_DECIMAL:
                        case DRAKEN_DECIMAL128: {
                            // precision digits + sign + decimal point ≤ 42.
                            len = 42u;
                            break;
                        }
                        default: {
                            // Unknown type: fall back to a safe estimate.
                            len = 10u;
                            break;
                        }
                    }
                    if (static_cast<uint32_t>(len) > best)
                        best = static_cast<uint32_t>(len);
                }
                return best;
            },
            "Return the maximum rendered display width (in bytes) across all logical rows.\n"
            "For string-family vectors, reads slot lengths directly (O(distinct)).\n"
            "For other types, measures the formatted representation of each unique data value.\n"
            "Null rows contribute 4 bytes (\"null\"). Returns 0 for empty vectors.")

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

    // D.1c — constant-shape VARCHAR vector.
    m.def("vector_varchar_from_constant",
        [](nb::object value, uint32_t length) {
            return make_varchar_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape VARCHAR Vector (data_length==1, selection=zero-vector).\n"
        "value must be bytes (stored verbatim) or None (→ all-null constant).\n"
        "Raises if value is not bytes or None — str must be encoded to bytes at the binder.");
    m.def("vector_varbinary_from_constant",
        [](nb::object value, uint32_t length) {
            return make_varbinary_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape VARBINARY Vector (data_length==1, selection=zero-vector).\n"
        "value may be None (→ all-null constant).\n"
        "Raises if value is not bytes or None.");
    m.def("vector_nvarchar_from_constant",
        [](nb::object value, uint32_t length) {
            return make_nvarchar_constant(value, length);
        },
        nb::arg("value").none(true), nb::arg("length"),
        "Build a constant-shape NVARCHAR Vector (data_length==1, selection=zero-vector).\n"
        "value must be bytes (validated as UTF-8) or None (→ all-null constant).\n"
        "Raises if value is not bytes/None or the bytes are not valid UTF-8.");

    // Zero-copy length-adjust view of a constant-shape Vector (executor cold path).
    m.def("vector_constant_view",
        [](const VectorOwner& src, uint32_t length) {
            return make_constant_view(src, length);
        },
        nb::arg("source"), nb::arg("length"),
        nb::keep_alive<0, 1>(),
        "Borrow a constant-shape Vector's single value as a length-N constant view.\n"
        "Zero-copy; the source Vector is kept alive for the view's lifetime.\n"
        "Raises if source is not constant-shape (data_length==1).");

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

    m.def("vector_string_family_from_bytes",
        [](nb::list seq, int type_int) {
            return make_bytes_from_sequence_typed(seq, static_cast<DrakenType>(type_int));
        },
        nb::arg("sequence"), nb::arg("type"),
        "Build a string-family Vector from list[bytes | None] with an explicit type\n"
        "tag (DRAKEN_VARCHAR / NVARCHAR / VARBINARY). Raw bytes, no decode — used to\n"
        "carry bytes data under a known source type (e.g. MIN/MAX of VARCHAR).");


    // S1: CxxMorsel as a Python handle — the dual-representation Morsel's C++ backing
    // (one PyObject carrier vs N Vector PyObjects). The cdef Morsel holds this handle
    // in `_cxx`; materialization (`to_vectors`) builds the Vector handles lazily.
    nb::class_<CxxMorsel>(m, "CxxMorsel")
        .def_prop_ro("num_rows",    [](const CxxMorsel& cm) { return static_cast<int64_t>(cm.num_rows()); })
        .def_prop_ro("num_columns", [](const CxxMorsel& cm) { return static_cast<int64_t>(cm.num_columns()); })
        .def("names", [](const CxxMorsel& cm) {
            nb::list out;
            for (const std::string& s : cm.names)
                out.append(nb::bytes(s.data(), s.size()));
            return out;
        })
        .def("to_vectors", [](const CxxMorsel& cm) { return cxx_columns_to_list(cm); })
        // Read one column (by identity/name) from the substrate as a Vector handle —
        // for operators reading the CxxMorsel without materializing the whole morsel.
        .def("column", [](const CxxMorsel& cm, nb::handle identity) -> nb::object {
            const std::string want = nb_bytes_to_std(identity);
            for (size_t i = 0; i < cm.names.size(); ++i)
                if (cm.names[i] == want) return nb::cast(cm.columns[i].own);
            throw nb::key_error("CxxMorsel.column: not found");
        })
        // Cxx-native ops — return a new CxxMorsel (stay off-PyObject; no materialization).
        .def("select", [](const CxxMorsel& cm, nb::list want) -> CxxMorsel {
            std::vector<std::string> w;
            w.reserve(nb::len(want));
            for (size_t i = 0; i < nb::len(want); ++i) w.push_back(nb_bytes_to_std(want[i]));
            return cxx_select(cm, w);
        })
        .def("take", [](const CxxMorsel& cm, nb::list indices) -> CxxMorsel {
            const uint32_t n = static_cast<uint32_t>(nb::len(indices));
            std::vector<int32_t> idx(n > 0u ? n : 1u);
            for (uint32_t i = 0; i < n; ++i) idx[i] = nb::cast<int32_t>(indices[i]);
            return cxx_take(cm, n > 0u ? idx.data() : nullptr, n);
        })
        .def("slice", [](const CxxMorsel& cm, uint32_t start, uint32_t length) -> CxxMorsel {
            return cxx_slice(cm, start, length);
        })
        // Row-routing scatter → list of W disjoint sub-morsels by hash(key) % W
        // (parallel grouped-agg producer side; also the test surface for the kernel).
        .def("scatter", [](const CxxMorsel& cm, nb::list key_cols, uint32_t W) -> nb::list {
            const uint32_t nc = static_cast<uint32_t>(nb::len(key_cols));
            std::vector<int32_t> cols(nc > 0u ? nc : 1u);
            for (uint32_t i = 0; i < nc; ++i) cols[i] = nb::cast<int32_t>(key_cols[i]);
            std::vector<CxxMorsel> bins =
                cxx_scatter(cm, nc > 0u ? cols.data() : nullptr, nc, W);
            nb::list out;
            for (CxxMorsel& b : bins) out.append(nb::cast(std::move(b)));
            return out;
        })
        .def("mask", [](const CxxMorsel& cm, const VectorOwner& mask) -> CxxMorsel {
            return cxx_mask(cm, mask.vec);
        })
        .def("rename", [](const CxxMorsel& cm, nb::list new_names) -> CxxMorsel {
            CxxMorsel out;
            out.columns.reserve(cm.columns.size());
            for (const CxxColumn& c : cm.columns) out.columns.push_back(c);  // shared_ptr copy
            out.zero_col_rows = cm.zero_col_rows;
            out.names.reserve(nb::len(new_names));
            for (size_t i = 0; i < nb::len(new_names); ++i)
                out.names.push_back(nb_bytes_to_std(new_names[i]));
            return out;
        });

    m.def("cxx_morsel_from_vectors", [](nb::list vectors, nb::list names) -> CxxMorsel {
        CxxMorsel cm = cxx_from_vectors_list(vectors);
        cm.names.reserve(nb::len(names));
        for (size_t i = 0; i < nb::len(names); ++i)
            cm.names.push_back(nb_bytes_to_std(names[i]));
        return cm;
    }, "S1: build a CxxMorsel (handle) from Vector handles + bytes names.");

    m.def("vector_concat",
        [](nb::list vectors) -> VectorOwner {
            const size_t n = vectors.size();
            if (n == 0u)
                throw std::invalid_argument("vector_concat: empty list");
            std::vector<const VectorOwner*> parts;
            parts.reserve(n);
            for (size_t i = 0u; i < n; ++i)
                parts.push_back(&nb::cast<const VectorOwner&>(vectors[i]));
            return concat_owners(parts);
        },
        nb::arg("vectors"),
        "Vertically concatenate N same-type Vectors into one dense Vector.\n"
        "Buffer-level: no Python objects, no decode. Result type and logical_type\n"
        "are taken from the first input. All inputs must share one type.");

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
    m.def("vector_cast_string_to_float64",
        [](const VectorOwner& v) {
            return make_float64_from_string_vector(v);
        },
        nb::arg("v"),
        "CAST(v AS FLOAT64): element-wise string-family Vector parse using fast_float. "
        "Invalid or null rows become null output rows.");
    m.def("vector_cast_float64_to_string",
        [](const VectorOwner& v, uint32_t precision) {
            return make_string_from_float_vector(v, precision);
        },
        nb::arg("v"), nb::arg("precision") = 6u,
        "CAST(v AS VARCHAR): element-wise FLOAT64/FLOAT32 formatting using Ryu d2fixed. "
        "Null rows remain null.");

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

    m.def("vector_decimal128_from_sequence",
        [](nb::list seq, int precision, int scale) {
            if (precision < 1 || precision > 38)
                throw std::invalid_argument("DECIMAL128 precision must be in [1, 38]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL128 scale must be in [0, precision]");
            return make_decimal128_from_sequence(
                seq, static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
        },
        nb::arg("sequence"), nb::arg("precision"), nb::arg("scale"),
        "Build a dense int128-backed DECIMAL128 Vector from list[Decimal | None].\n"
        "precision: total significant digits (1..38); scale: 0..precision.\n"
        "The correct-but-scalar decimal tier (doc 06); None elements become nulls.");

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

    m.def("vector_decimal128_from_constant",
        [](nb::object value, uint32_t length, int precision, int scale) {
            if (precision < 1 || precision > 38)
                throw std::invalid_argument("DECIMAL128 precision must be in [1, 38]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL128 scale must be in [0, precision]");
            return make_decimal128_constant(
                value, length,
                static_cast<uint8_t>(precision), static_cast<uint8_t>(scale));
        },
        nb::arg("value").none(true), nb::arg("length"),
        nb::arg("precision"), nb::arg("scale"),
        "Build a constant-shape int128-backed DECIMAL128 Vector (p>18). "
        "value may be None (→ all-null constant).");

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

    m.def("vector_fp16_zeros",
        [](int64_t length, int dimension) {
            if (length < 0)
                throw std::invalid_argument("vector_fp16_zeros: length must be >= 0");
            if (dimension < 1)
                throw std::invalid_argument(
                    "vector_fp16_zeros: dimension must be >= 1");
            return make_fp16_zeros(static_cast<uint32_t>(length),
                                   static_cast<uint32_t>(dimension));
        },
        nb::arg("length"), nb::arg("dimension"),
        "Allocate a fresh VECTOR_FP16 Vector of shape (length, dimension) with all\n"
        "values zero and no nulls. The data buffer is mutable through unified().data\n"
        "so callers can write rows in place after construction.");

    m.def("vector_fp16_with_nulls",
        [](int64_t length, int dimension) {
            if (length < 0)
                throw std::invalid_argument("vector_fp16_with_nulls: length must be >= 0");
            if (dimension < 1)
                throw std::invalid_argument(
                    "vector_fp16_with_nulls: dimension must be >= 1");
            return make_fp16_with_nulls(static_cast<uint32_t>(length),
                                        static_cast<uint32_t>(dimension));
        },
        nb::arg("length"), nb::arg("dimension"),
        "Allocate a fresh VECTOR_FP16 Vector of shape (length, dimension) with all\n"
        "rows initially null. Arrow validity convention: bit=1 = valid, bit=0 = null.\n"
        "Callers SET bits in unified().validity to mark rows present, then write into\n"
        "unified().data. Companion to vector_fp16_zeros.");

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
    // vector_array_map_access — native array-element subscript.
    //
    // Replaces the legacy opteryx.vector_special.vector_map_access_array kernel,
    // which drove the Python C API row-by-row and returned a Python list. This
    // version walks the DRAKEN_ARRAY offsets natively and produces a Vector of
    // the child element type directly. No Python objects in the inner loop.
    // -------------------------------------------------------------------------
    m.def("vector_array_map_access",
        [](nb::object vec_obj, int64_t index) -> nb::object {
            nb::handle h(vec_obj);
            if (!nb::isinstance<VectorOwner>(h))
                throw nb::type_error(
                    "vector_array_map_access: expected draken_native.Vector");
            const VectorOwner& v = *nb::inst_ptr<VectorOwner>(h);
            return nb::cast(make_array_map_access(v, index));
        },
        nb::arg("vec"), nb::arg("index"),
        "Element subscript on a DRAKEN_ARRAY Vector: vec[index] per row.\n"
        "Returns a dense Vector of the child element type. Negative indices\n"
        "are Python-style (relative to each row's length). Out-of-bounds or\n"
        "null-parent rows produce null output rows.");

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

    // -------------------------------------------------------------------------
    // Type coercion helpers — reinterpret physical INT64 data as TIMESTAMP64
    // or DATE32. Used by the parquet reader to fix up logical types after IPC
    // deserialization (which tags DATE/TIMESTAMP as INT64).
    // -------------------------------------------------------------------------

    // vector_reinterpret_as_timestamp64 — reinterpret an INT64 vector's data as TIMESTAMP64.
    // The underlying int64 values are treated as raw counts in `unit` unchanged
    // (default "us" preserves the pre-existing microseconds behaviour for callers
    // that omit it; the grouped-agg MIN/MAX finalize passes the source unit).
    m.def("vector_reinterpret_as_timestamp64",
        [](nb::object obj, const std::string& unit) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_INT64)
                throw std::invalid_argument("vector_reinterpret_as_timestamp64: requires INT64 vector");
            // Identity take with type override: materialize all rows into a new owned TIMESTAMP64.
            const uint32_t n = src->length;
            int64_t* dst = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1u) * sizeof(int64_t)));
            if (!dst) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dst);
            OwnedBuffer<uint8_t> val_buf(nullptr);
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = static_cast<const int64_t*>(src->data)[src->selection[i]];
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_TIMESTAMP64, validity);
            VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
            LogicalType lt;
            lt.kind = LogicalKind::TIMESTAMP;
            lt.unit = str_to_unit(unit);
            lt.offset_minutes = 0;
            owner.logical_type = logical_type_intern(lt);
            return owner;
        },
        nb::arg("vec"), nb::arg("unit") = std::string("us"),
        "Reinterpret INT64 vector data as TIMESTAMP64 with the given unit "
        "(\"s\"/\"ms\"/\"us\"/\"ns\"; default \"us\"). Returns new Vector.");

    // vector_retag_int64_as_timestamp64 — ZERO-COPY retag of an INT64 vector to
    // TIMESTAMP64. Where vector_reinterpret_as_timestamp64 materialises a fresh
    // dense copy, this MOVES the source's owned buffers into the new TIMESTAMP64
    // owner — no data movement, and the source's shape (identity/constant/dict
    // selection) is preserved verbatim. The source Vector is consequently
    // emptied and MUST NOT be used afterwards; the caller must hold the SOLE
    // reference. Used by the parquet reader's logical-type coercion, where the
    // just-decoded int64 column is exclusively owned and immediately replaced.
    m.def("vector_retag_int64_as_timestamp64",
        [](nb::object obj, const std::string& unit) -> VectorOwner {
            if (obj.is_none() || !nb::isinstance<VectorOwner>(obj))
                throw std::invalid_argument(
                    "vector_retag_int64_as_timestamp64: expected draken Vector");
            VectorOwner* src = nb::inst_ptr<VectorOwner>(obj);
            if (src->vec.type != DRAKEN_INT64)
                throw std::invalid_argument(
                    "vector_retag_int64_as_timestamp64: requires INT64 vector");
            // Same payload, retagged type — selection/validity/data unchanged.
            DrakenVector v = src->vec;
            v.type = DRAKEN_TIMESTAMP64;
            // Transfer buffer ownership. data_buf/validity_buf/codes_buf now
            // belong to the new owner; v's pointers still address them (valid).
            VectorOwner owner(v,
                              std::move(src->data_buf),
                              std::move(src->validity_buf),
                              std::move(src->codes_buf));
            // Empty the husk: its unique_ptrs are already null from the move (so
            // it frees nothing), but null the borrowed pointers and lengths too
            // so a stray read of the moved-from source sees an empty vector
            // rather than buffers now owned elsewhere.
            src->vec.data = nullptr;
            src->vec.validity = nullptr;
            src->vec.length = 0;
            src->vec.data_length = 0;
            LogicalType lt;
            lt.kind = LogicalKind::TIMESTAMP;
            lt.unit = str_to_unit(unit);
            lt.offset_minutes = 0;
            owner.logical_type = logical_type_intern(lt);
            return owner;
        },
        nb::arg("vec"), nb::arg("unit") = std::string("us"),
        "Zero-copy retag of an INT64 Vector to TIMESTAMP64 with the given unit "
        "(\"s\"/\"ms\"/\"us\"/\"ns\"). MOVES the source's buffers — the source "
        "Vector is emptied and must not be used afterwards; caller must hold the "
        "sole reference.");

    // vector_reinterpret_as_time32 — INT64 vector → TIME32 (int32, unit-tagged).
    // Values are cast to int32 (counts-since-midnight in `unit`).
    m.def("vector_reinterpret_as_time32",
        [](nb::object obj, const std::string& unit) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_INT64)
                throw std::invalid_argument("vector_reinterpret_as_time32: requires INT64 vector");
            const uint32_t n = src->length;
            int32_t* dst = static_cast<int32_t*>(draken_malloc((n > 0 ? n : 1u) * sizeof(int32_t)));
            if (!dst) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dst);
            OwnedBuffer<uint8_t> val_buf(nullptr);
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = static_cast<int32_t>(static_cast<const int64_t*>(src->data)[src->selection[i]]);
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_TIME32, validity);
            VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
            LogicalType lt;
            lt.kind = LogicalKind::TIME;
            lt.unit = str_to_unit(unit);
            lt.offset_minutes = 0;
            owner.logical_type = logical_type_intern(lt);
            return owner;
        },
        nb::arg("vec"), nb::arg("unit") = std::string("ms"),
        "Reinterpret INT64 vector data as TIME32 (cast to int32) with the given unit. Returns new Vector.");

    // vector_reinterpret_as_time64 — INT64 vector → TIME64 (int64, unit-tagged).
    m.def("vector_reinterpret_as_time64",
        [](nb::object obj, const std::string& unit) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_INT64)
                throw std::invalid_argument("vector_reinterpret_as_time64: requires INT64 vector");
            const uint32_t n = src->length;
            int64_t* dst = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1u) * sizeof(int64_t)));
            if (!dst) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dst);
            OwnedBuffer<uint8_t> val_buf(nullptr);
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = static_cast<const int64_t*>(src->data)[src->selection[i]];
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_TIME64, validity);
            VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
            LogicalType lt;
            lt.kind = LogicalKind::TIME;
            lt.unit = str_to_unit(unit);
            lt.offset_minutes = 0;
            owner.logical_type = logical_type_intern(lt);
            return owner;
        },
        nb::arg("vec"), nb::arg("unit") = std::string("us"),
        "Reinterpret INT64 vector data as TIME64 with the given unit. Returns new Vector.");

    // vector_reinterpret_as_float32 — narrow a FLOAT64 vector's data to FLOAT32.
    // Used by grouped MIN/MAX(FLOAT32) finalize: min/max is computed in double,
    // then narrowed back so the result emerges as FLOAT32, not FLOAT64.
    m.def("vector_reinterpret_as_float32",
        [](nb::object obj) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_FLOAT64)
                throw std::invalid_argument("vector_reinterpret_as_float32: requires FLOAT64 vector");
            const uint32_t n = src->length;
            float* dst = static_cast<float*>(draken_malloc((n > 0 ? n : 1u) * sizeof(float)));
            if (!dst) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dst);
            OwnedBuffer<uint8_t> val_buf(nullptr);
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = static_cast<float>(static_cast<const double*>(src->data)[src->selection[i]]);
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_FLOAT32, validity);
            return VectorOwner(v, std::move(data_buf), std::move(val_buf));
        },
        nb::arg("vec"),
        "Narrow a FLOAT64 vector to FLOAT32 (cast each value to float). Returns new Vector.");

    // vector_reinterpret_as_date32 — reinterpret an INT64 vector's data as DATE32.
    // INT64 values are cast to int32 (days-since-epoch). SHAPE-PRESERVING: only the
    // data buffer (data_length values) is converted; dense stays dense, constant
    // stays constant, and a Dict-shaped vector keeps its codes (copied) so a
    // dict-encoded date column survives coercion compressed.
    m.def("vector_reinterpret_as_date32",
        [](nb::object obj) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_INT64)
                throw std::invalid_argument("vector_reinterpret_as_date32: requires INT64 vector");
            const uint32_t n  = src->length;
            const uint32_t dl = src->data_length;
            const int64_t* src_data = static_cast<const int64_t*>(src->data);
            // Convert only the data buffer (dl values), preserving shape.
            int32_t* dd = static_cast<int32_t*>(draken_malloc((dl > 0 ? dl : 1u) * sizeof(int32_t)));
            if (!dd) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dd);
            for (uint32_t k = 0; k < dl; ++k)
                dd[k] = static_cast<int32_t>(src_data[k]);
            // Validity is 1-bit-per-logical-row for every shape: copy n bits.
            OwnedBuffer<uint8_t> val_buf(nullptr);
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            if (draken_is_dict(src)) {
                // Own a copy of the per-row codes (src keeps its own).
                uint32_t* codes = static_cast<uint32_t*>(draken_malloc((n > 0 ? n : 1u) * sizeof(uint32_t)));
                if (!codes) throw std::bad_alloc();
                OwnedBuffer<void> codes_buf(static_cast<void*>(codes));
                std::memcpy(codes, src->selection, static_cast<size_t>(n) * sizeof(uint32_t));
                DrakenVector v = draken_vector_from_dict(dd, dl, codes, n, DRAKEN_DATE32, validity);
                return VectorOwner(v, std::move(data_buf), std::move(val_buf), std::move(codes_buf));
            }
            if (draken_is_constant(src)) {
                DrakenVector v = draken_vector_from_constant(dd, n, DRAKEN_DATE32, validity);
                return VectorOwner(v, std::move(data_buf), std::move(val_buf));
            }
            // Dense (data_length == length, identity selection).
            DrakenVector v = draken_vector_from_dense(dd, n, DRAKEN_DATE32, validity);
            return VectorOwner(v, std::move(data_buf), std::move(val_buf));
        },
        nb::arg("vec"),
        "Reinterpret INT64 vector data as DATE32 (days-since-epoch, cast to int32). "
        "Shape-preserving (dense/constant/dict). Returns new Vector.");

    // vector_reinterpret_as_decimal — reinterpret INT64 vector as DECIMAL with given precision/scale.
    // Used by parquet reader to fix up logical type post-IPC deserialization.
    m.def("vector_reinterpret_as_decimal",
        [](nb::object obj, int precision, int scale) -> VectorOwner {
            const DrakenVector* src = draken_vector_unwrap(obj.ptr());
            if (!src)
                throw nb::python_error();
            if (src->type != DRAKEN_INT64)
                throw std::invalid_argument("vector_reinterpret_as_decimal: requires INT64 vector");
            if (precision < 1 || precision > 18)
                throw std::invalid_argument("DECIMAL precision must be in [1, 18]");
            if (scale < 0 || scale > precision)
                throw std::invalid_argument("DECIMAL scale must be in [0, precision]");
            const uint32_t n = src->length;
            int64_t* dst = static_cast<int64_t*>(draken_malloc((n > 0 ? n : 1u) * sizeof(int64_t)));
            if (!dst) throw std::bad_alloc();
            OwnedBuffer<void> data_buf(dst);
            OwnedBuffer<uint8_t> val_buf(nullptr);
            const int64_t* src_data = static_cast<const int64_t*>(src->data);
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = src_data[src->selection[i]];
            uint8_t* validity = nullptr;
            if (src->validity) {
                const uint32_t nbytes = (n + 7u) / 8u;
                const uint32_t padded = ((nbytes + 7u) & ~7u);
                validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8u));
                if (!validity) throw std::bad_alloc();
                val_buf.reset(validity);
                std::memcpy(validity, src->validity, nbytes);
            }
            DrakenVector v = draken_vector_from_dense(dst, n, DRAKEN_DECIMAL, validity);
            VectorOwner owner(v, std::move(data_buf), std::move(val_buf));
            LogicalType lt;
            lt.kind      = LogicalKind::DECIMAL;
            lt.precision = static_cast<uint8_t>(precision);
            lt.scale     = static_cast<uint8_t>(scale);
            owner.logical_type = logical_type_intern(lt);
            return owner;
        },
        nb::arg("vec"), nb::arg("precision"), nb::arg("scale"),
        "Reinterpret INT64 vector data as DECIMAL with given precision/scale. Returns new Vector.");

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

    // _frame_arena_smoke_test — exercise the frame_arena lifecycle in C++ and
    // report per-step results. Returns a dict of {step_name: bool} so the
    // Python test can assert each independently. The void* API doesn't lend
    // itself to Python-level testing directly; this wrapper drives it from
    // C++ where the pointers are first-class.
    m.def("_frame_arena_smoke_test",
        []() -> nb::dict {
            nb::dict r;

            // 1. create → returns non-null, size == 0
            DrakenFrameArena* a = draken_frame_arena_create();
            r["create_returns_non_null"] = (a != nullptr);
            r["initial_size_zero"]       = (draken_frame_arena_size(a) == 0);

            // 2. alloc twice → size == 2, both pointers non-null
            void* p1 = draken_frame_arena_alloc(a, 64);
            void* p2 = draken_frame_arena_alloc(a, 128);
            r["alloc1_non_null"] = (p1 != nullptr);
            r["alloc2_non_null"] = (p2 != nullptr);
            r["size_after_two_allocs"] = (draken_frame_arena_size(a) == 2);

            // 3. write to the buffers to verify they're usable memory
            std::memset(p1, 0xAB, 64);
            std::memset(p2, 0xCD, 128);
            r["buffers_writable"] =
                (static_cast<uint8_t*>(p1)[0] == 0xAB) &&
                (static_cast<uint8_t*>(p2)[127] == 0xCD);

            // 4. release p1 → size == 1, p1 still valid (we own it now)
            draken_frame_arena_release(a, p1);
            r["size_after_release"] = (draken_frame_arena_size(a) == 1);

            // We own p1 now; verify by writing again (would be UAF if arena
            // had freed it on release).
            std::memset(p1, 0xEF, 64);
            r["released_ptr_still_writable"] =
                (static_cast<uint8_t*>(p1)[0] == 0xEF);

            // 5. release of NULL and of an untracked ptr are no-ops
            draken_frame_arena_release(a, nullptr);
            int sentinel;
            draken_frame_arena_release(a, &sentinel);
            r["size_unchanged_after_noop_releases"] =
                (draken_frame_arena_size(a) == 1);

            // 6. destroy → frees p2 (still tracked), leaves p1 alone (we own it)
            draken_frame_arena_destroy(a);
            // We can't directly verify p2 was freed without poking at
            // allocator internals; we verify by destroying twice (second is
            // a no-op when a == nullptr-equivalent), and by freeing p1
            // ourselves to confirm the OWNS semantics.
            draken_free(p1);                       // we own p1; this must not UAF
            draken_frame_arena_destroy(nullptr);   // null no-op
            r["destroy_null_is_noop"] = true;
            r["caller_can_free_released"] = true;  // reached without crash

            // 7. alloc after create-with-zero
            DrakenFrameArena* a2 = draken_frame_arena_create();
            void* p3 = draken_frame_arena_alloc(a2, 0);
            // size==0 alloc behaviour: implementation-defined; we just verify
            // it doesn't crash and is tracked if non-null.
            r["zero_alloc_did_not_crash"] = true;
            if (p3 != nullptr) {
                r["zero_alloc_tracked"] = (draken_frame_arena_size(a2) == 1);
            } else {
                r["zero_alloc_tracked"] = true;  // OOM path is also valid
            }
            draken_frame_arena_destroy(a2);

            // 8. adopt: ingest a draken_malloc'd buffer into the arena.
            DrakenFrameArena* a3 = draken_frame_arena_create();
            void* externally_alloc = draken_malloc(32);
            draken_frame_arena_adopt(a3, externally_alloc);
            r["adopt_increments_size"] = (draken_frame_arena_size(a3) == 1);
            // destroy frees the adopted pointer (no need for caller to free).
            draken_frame_arena_destroy(a3);
            r["adopt_freed_on_destroy"] = true;  // reached without crash

            // 9. adopt + release of the adopted pointer: caller owns it back.
            DrakenFrameArena* a4 = draken_frame_arena_create();
            void* externally_alloc2 = draken_malloc(32);
            draken_frame_arena_adopt(a4, externally_alloc2);
            draken_frame_arena_release(a4, externally_alloc2);
            r["adopt_then_release_size_zero"] = (draken_frame_arena_size(a4) == 0);
            draken_frame_arena_destroy(a4);
            draken_free(externally_alloc2);   // caller owns; must not UAF
            r["adopt_then_release_caller_owns"] = true;

            // 10. adopt of NULL is a no-op.
            DrakenFrameArena* a5 = draken_frame_arena_create();
            draken_frame_arena_adopt(a5, nullptr);
            r["adopt_null_is_noop"] = (draken_frame_arena_size(a5) == 0);
            draken_frame_arena_destroy(a5);

            return r;
        },
        "Frame-arena smoke test (C++-side lifecycle). Returns dict of "
        "{step_name: passed}. Used by draken/tests/native/test_frame_arena.py.");

    // _compare_dv_smoke_test — exercise draken_compare_dv end-to-end against
    // INT64 and FLOAT64 inputs, verifying that:
    //   * result type is DRAKEN_BOOL with correct length
    //   * bitmap contents match expected per-row results
    //   * unsupported types return NULL (caller's fallback signal)
    //   * cross-type operands return NULL
    //   * length mismatch returns NULL
    //   * arena destroy cleans up without UAF
    //
    // Returns dict of {step_name: bool} for the Python test to assert on.
    m.def("_compare_dv_smoke_test",
        []() -> nb::dict {
            nb::dict r;
            DrakenFrameArena* arena = draken_frame_arena_create();
            if (arena == nullptr) {
                r["arena_create"] = false;
                return r;
            }
            r["arena_create"] = true;

            // ---- INT64 EQ ----
            // left  = [1, 2, 3, 4]
            // right = [1, 5, 3, 9]
            // EQ    = [T, F, T, F]  →  bitmap = 0b0101 = 0x05
            const uint32_t n = 4;
            int64_t* ldata = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            int64_t* rdata = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            ldata[0] = 1; ldata[1] = 2; ldata[2] = 3; ldata[3] = 4;
            rdata[0] = 1; rdata[1] = 5; rdata[2] = 3; rdata[3] = 9;
            DrakenVector lv = draken_vector_from_dense(ldata, n, DRAKEN_INT64, nullptr);
            DrakenVector rv = draken_vector_from_dense(rdata, n, DRAKEN_INT64, nullptr);

            DrakenVector* res = draken_compare_dv(0, &lv, &rv, 0, 0, n, arena);
            r["int64_eq_returns_non_null"] = (res != nullptr);
            if (res != nullptr) {
                r["int64_eq_result_is_bool"] = (res->type == DRAKEN_BOOL);
                r["int64_eq_result_length"] = (res->length == n);
                const uint8_t* bits = static_cast<const uint8_t*>(res->data);
                // Bit i = (left[sel[i]] == right[sel[i]]).
                uint8_t got = 0u;
                for (uint32_t i = 0; i < n; ++i) {
                    if ((bits[i >> 3] >> (i & 7)) & 1u) got |= static_cast<uint8_t>(1u << i);
                }
                r["int64_eq_bitmap"] = (got == 0x05u);  // 0b0101: rows 0 and 2
            }
            draken_free(ldata);
            draken_free(rdata);

            // ---- FLOAT64 LT ----
            // left  = [1.0, 2.0, 3.0]
            // right = [2.0, 2.0, 1.0]
            // LT    = [T, F, F] → bitmap = 0b001 = 0x01
            const uint32_t fn = 3;
            double* fldata = static_cast<double*>(draken_malloc(fn * sizeof(double)));
            double* frdata = static_cast<double*>(draken_malloc(fn * sizeof(double)));
            fldata[0] = 1.0; fldata[1] = 2.0; fldata[2] = 3.0;
            frdata[0] = 2.0; frdata[1] = 2.0; frdata[2] = 1.0;
            DrakenVector flv = draken_vector_from_dense(fldata, fn, DRAKEN_FLOAT64, nullptr);
            DrakenVector frv = draken_vector_from_dense(frdata, fn, DRAKEN_FLOAT64, nullptr);

            DrakenVector* fres = draken_compare_dv(4, &flv, &frv, 0, 0, fn, arena);  // op 4 = LT
            r["float64_lt_returns_non_null"] = (fres != nullptr);
            if (fres != nullptr) {
                r["float64_lt_result_is_bool"] = (fres->type == DRAKEN_BOOL);
                const uint8_t* fbits = static_cast<const uint8_t*>(fres->data);
                uint8_t fgot = 0u;
                for (uint32_t i = 0; i < fn; ++i) {
                    if ((fbits[i >> 3] >> (i & 7)) & 1u) fgot |= static_cast<uint8_t>(1u << i);
                }
                r["float64_lt_bitmap"] = (fgot == 0x01u);  // 0b001: row 0 only
            }
            draken_free(fldata);
            draken_free(frdata);

            // ---- Unsupported type: BOOL on either side → NULL ----
            const uint32_t bn = 2;
            uint8_t* bldata = static_cast<uint8_t*>(draken_malloc(8));
            uint8_t* brdata = static_cast<uint8_t*>(draken_malloc(8));
            std::memset(bldata, 0, 8); std::memset(brdata, 0, 8);
            DrakenVector blv = draken_vector_from_dense(bldata, bn, DRAKEN_BOOL, nullptr);
            DrakenVector brv = draken_vector_from_dense(brdata, bn, DRAKEN_BOOL, nullptr);
            DrakenVector* bres = draken_compare_dv(0, &blv, &brv, 0, 0, bn, arena);
            r["unsupported_type_returns_null"] = (bres == nullptr);
            draken_free(bldata);
            draken_free(brdata);

            // ---- Cross-type (INT64 vs FLOAT64) → NULL ----
            int64_t* cldata = static_cast<int64_t*>(draken_malloc(8));
            double*  crdata = static_cast<double*>(draken_malloc(8));
            cldata[0] = 1;
            crdata[0] = 1.0;
            DrakenVector clv = draken_vector_from_dense(cldata, 1, DRAKEN_INT64, nullptr);
            DrakenVector crv = draken_vector_from_dense(crdata, 1, DRAKEN_FLOAT64, nullptr);
            DrakenVector* cres = draken_compare_dv(0, &clv, &crv, 0, 0, 1, arena);
            r["cross_type_returns_null"] = (cres == nullptr);
            draken_free(cldata);
            draken_free(crdata);

            // ---- Length mismatch → NULL ----
            int64_t* mldata = static_cast<int64_t*>(draken_malloc(16));
            int64_t* mrdata = static_cast<int64_t*>(draken_malloc(8));
            mldata[0] = 1; mldata[1] = 2;
            mrdata[0] = 1;
            DrakenVector mlv = draken_vector_from_dense(mldata, 2, DRAKEN_INT64, nullptr);
            DrakenVector mrv = draken_vector_from_dense(mrdata, 1, DRAKEN_INT64, nullptr);
            DrakenVector* mres = draken_compare_dv(0, &mlv, &mrv, 0, 0, 2, arena);
            r["length_mismatch_returns_null"] = (mres == nullptr);
            draken_free(mldata);
            draken_free(mrdata);

            // ---- Out-of-range op_code → NULL ----
            int64_t* oldata = static_cast<int64_t*>(draken_malloc(8));
            int64_t* ordata = static_cast<int64_t*>(draken_malloc(8));
            oldata[0] = 1; ordata[0] = 1;
            DrakenVector olv = draken_vector_from_dense(oldata, 1, DRAKEN_INT64, nullptr);
            DrakenVector orv = draken_vector_from_dense(ordata, 1, DRAKEN_INT64, nullptr);
            DrakenVector* ores = draken_compare_dv(99, &olv, &orv, 0, 0, 1, arena);
            r["bad_op_code_returns_null"] = (ores == nullptr);
            draken_free(oldata);
            draken_free(ordata);

            // ---- NULL inputs → NULL ----
            DrakenVector* nres = draken_compare_dv(0, nullptr, &olv, 0, 0, 1, arena);
            r["null_input_returns_null"] = (nres == nullptr);

            // ---- DATE32 EQ (Stage C) ----
            // left  = [100, 200, 300, 400]
            // right = [100, 999, 300, 999]
            // EQ    = [T, F, T, F]  →  bitmap = 0b0101 = 0x05
            int32_t* dldata = static_cast<int32_t*>(draken_malloc(n * sizeof(int32_t)));
            int32_t* drdata = static_cast<int32_t*>(draken_malloc(n * sizeof(int32_t)));
            dldata[0] = 100; dldata[1] = 200; dldata[2] = 300; dldata[3] = 400;
            drdata[0] = 100; drdata[1] = 999; drdata[2] = 300; drdata[3] = 999;
            DrakenVector dlv = draken_vector_from_dense(dldata, n, DRAKEN_DATE32, nullptr);
            DrakenVector drv = draken_vector_from_dense(drdata, n, DRAKEN_DATE32, nullptr);
            DrakenVector* dres = draken_compare_dv(0, &dlv, &drv, 0, 0, n, arena);
            r["date32_eq_returns_non_null"] = (dres != nullptr);
            if (dres != nullptr) {
                r["date32_eq_result_is_bool"] = (dres->type == DRAKEN_BOOL);
                const uint8_t* dbits = static_cast<const uint8_t*>(dres->data);
                uint8_t dgot = 0u;
                for (uint32_t i = 0; i < n; ++i) {
                    if ((dbits[i >> 3] >> (i & 7)) & 1u) dgot |= static_cast<uint8_t>(1u << i);
                }
                r["date32_eq_bitmap"] = (dgot == 0x05u);
            }
            draken_free(dldata);
            draken_free(drdata);

            // ---- TIMESTAMP64 LT (Stage C) ----
            // left  = [1000, 2000, 3000]
            // right = [2000, 2000, 1000]
            // LT    = [T, F, F] → 0b001
            int64_t* tldata = static_cast<int64_t*>(draken_malloc(fn * sizeof(int64_t)));
            int64_t* trdata = static_cast<int64_t*>(draken_malloc(fn * sizeof(int64_t)));
            tldata[0] = 1000; tldata[1] = 2000; tldata[2] = 3000;
            trdata[0] = 2000; trdata[1] = 2000; trdata[2] = 1000;
            DrakenVector tlv = draken_vector_from_dense(tldata, fn, DRAKEN_TIMESTAMP64, nullptr);
            DrakenVector trv = draken_vector_from_dense(trdata, fn, DRAKEN_TIMESTAMP64, nullptr);
            DrakenVector* tres = draken_compare_dv(4, &tlv, &trv, 0, 0, fn, arena);
            r["timestamp64_lt_returns_non_null"] = (tres != nullptr);
            if (tres != nullptr) {
                r["timestamp64_lt_result_is_bool"] = (tres->type == DRAKEN_BOOL);
                const uint8_t* tbits = static_cast<const uint8_t*>(tres->data);
                uint8_t tgot = 0u;
                for (uint32_t i = 0; i < fn; ++i) {
                    if ((tbits[i >> 3] >> (i & 7)) & 1u) tgot |= static_cast<uint8_t>(1u << i);
                }
                r["timestamp64_lt_bitmap"] = (tgot == 0x01u);
            }
            draken_free(tldata);
            draken_free(trdata);

            // ---- VARCHAR EQ — NOT exercised here in raw C++ smoke; ----
            // str_compare_vector consumes DrakenStringArena slot+arena
            // structures that are non-trivial to build outside the
            // `make_string_from_sequence` nanobind producer. Coverage for
            // VARCHAR compare via draken_compare_dv is asserted from
            // Python in test_compare_dv.py (Python-built string vectors
            // routed through this entry point). Marking placeholder so
            // the expected-steps set matches; the assertion is "the
            // VARCHAR branch is present in compare_dv.cpp", verified by
            // file inspection.
            r["varchar_smoke_skipped"] = true;

            // ---- DECIMAL returns NULL (descriptor-on-DrakenVector limitation) ----
            int64_t* qldata = static_cast<int64_t*>(draken_malloc(8));
            int64_t* qrdata = static_cast<int64_t*>(draken_malloc(8));
            qldata[0] = 150; qrdata[0] = 150;  // unscaled values
            DrakenVector qlv = draken_vector_from_dense(qldata, 1, DRAKEN_DECIMAL, nullptr);
            DrakenVector qrv = draken_vector_from_dense(qrdata, 1, DRAKEN_DECIMAL, nullptr);
            DrakenVector* qres = draken_compare_dv(0, &qlv, &qrv, 0, 0, 1, arena);
            r["decimal_returns_null_pending_descriptor"] = (qres == nullptr);
            draken_free(qldata);
            draken_free(qrdata);

            // ---- Destroy frees the result vector + adopted buffers ----
            draken_frame_arena_destroy(arena);
            r["destroy_no_crash"] = true;
            return r;
        },
        "Compare-dv smoke test (C++-side end-to-end). Returns dict of "
        "{step_name: passed}. Used by draken/tests/native/test_compare_dv.py.");

    // _arithmetic_dv_smoke_test — exercise draken_arithmetic_dv end-to-end.
    // Mirrors the compare_dv smoke test pattern.
    m.def("_arithmetic_dv_smoke_test",
        []() -> nb::dict {
            nb::dict r;
            DrakenFrameArena* arena = draken_frame_arena_create();
            r["arena_create"] = (arena != nullptr);
            if (arena == nullptr) return r;

            // ---- INT64 PLUS ----
            // a = [10, 20, 30], b = [1, 2, 3], a+b = [11, 22, 33]
            const uint32_t n = 3;
            int64_t* ldata = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            int64_t* rdata = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            ldata[0] = 10; ldata[1] = 20; ldata[2] = 30;
            rdata[0] = 1;  rdata[1] = 2;  rdata[2] = 3;
            DrakenVector lv = draken_vector_from_dense(ldata, n, DRAKEN_INT64, nullptr);
            DrakenVector rv = draken_vector_from_dense(rdata, n, DRAKEN_INT64, nullptr);

            DrakenVector* res = draken_arithmetic_dv(1, &lv, &rv, n, arena);  // PLUS
            r["int64_plus_returns_non_null"] = (res != nullptr);
            if (res != nullptr) {
                r["int64_plus_result_is_int64"] = (res->type == DRAKEN_INT64);
                r["int64_plus_length"] = (res->length == n);
                const int64_t* d = static_cast<const int64_t*>(res->data);
                r["int64_plus_values"] =
                    (d[res->selection[0]] == 11) &&
                    (d[res->selection[1]] == 22) &&
                    (d[res->selection[2]] == 33);
            }
            draken_free(ldata);
            draken_free(rdata);

            // ---- FLOAT64 MULTIPLY ----
            // a = [1.5, 2.5], b = [2.0, 4.0], a*b = [3.0, 10.0]
            const uint32_t fn = 2;
            double* fldata = static_cast<double*>(draken_malloc(fn * sizeof(double)));
            double* frdata = static_cast<double*>(draken_malloc(fn * sizeof(double)));
            fldata[0] = 1.5; fldata[1] = 2.5;
            frdata[0] = 2.0; frdata[1] = 4.0;
            DrakenVector flv = draken_vector_from_dense(fldata, fn, DRAKEN_FLOAT64, nullptr);
            DrakenVector frv = draken_vector_from_dense(frdata, fn, DRAKEN_FLOAT64, nullptr);

            DrakenVector* fres = draken_arithmetic_dv(3, &flv, &frv, fn, arena);  // MULTIPLY
            r["float64_mul_returns_non_null"] = (fres != nullptr);
            if (fres != nullptr) {
                r["float64_mul_result_is_float64"] = (fres->type == DRAKEN_FLOAT64);
                const double* fd = static_cast<const double*>(fres->data);
                r["float64_mul_values"] =
                    (fd[fres->selection[0]] == 3.0) &&
                    (fd[fres->selection[1]] == 10.0);
            }
            draken_free(fldata);
            draken_free(frdata);

            // ---- Cross-type returns NULL ----
            int64_t* cldata = static_cast<int64_t*>(draken_malloc(8));  cldata[0] = 1;
            double*  crdata = static_cast<double*>(draken_malloc(8));   crdata[0] = 1.0;
            DrakenVector clv = draken_vector_from_dense(cldata, 1, DRAKEN_INT64, nullptr);
            DrakenVector crv = draken_vector_from_dense(crdata, 1, DRAKEN_FLOAT64, nullptr);
            DrakenVector* cres = draken_arithmetic_dv(1, &clv, &crv, 1, arena);
            r["cross_type_returns_null"] = (cres == nullptr);
            draken_free(cldata);
            draken_free(crdata);

            // ---- Out-of-range op_code (e.g. BOP_STRING_CONCAT=7) → NULL ----
            int64_t* sldata = static_cast<int64_t*>(draken_malloc(8));  sldata[0] = 1;
            int64_t* srdata = static_cast<int64_t*>(draken_malloc(8));  srdata[0] = 1;
            DrakenVector slv = draken_vector_from_dense(sldata, 1, DRAKEN_INT64, nullptr);
            DrakenVector srv = draken_vector_from_dense(srdata, 1, DRAKEN_INT64, nullptr);
            DrakenVector* sres = draken_arithmetic_dv(7, &slv, &srv, 1, arena);
            r["bad_op_returns_null"] = (sres == nullptr);
            draken_free(sldata);
            draken_free(srdata);

            // ---- Unsupported type (BOOL) → NULL ----
            uint8_t* bldata = static_cast<uint8_t*>(draken_malloc(8));
            uint8_t* brdata = static_cast<uint8_t*>(draken_malloc(8));
            std::memset(bldata, 0, 8); std::memset(brdata, 0, 8);
            DrakenVector blv = draken_vector_from_dense(bldata, 2, DRAKEN_BOOL, nullptr);
            DrakenVector brv = draken_vector_from_dense(brdata, 2, DRAKEN_BOOL, nullptr);
            DrakenVector* bres = draken_arithmetic_dv(1, &blv, &brv, 2, arena);
            r["unsupported_type_returns_null"] = (bres == nullptr);
            draken_free(bldata);
            draken_free(brdata);

            // ---- Length mismatch → NULL ----
            int64_t* mldata = static_cast<int64_t*>(draken_malloc(16));
            int64_t* mrdata = static_cast<int64_t*>(draken_malloc(8));
            mldata[0] = 1; mldata[1] = 2; mrdata[0] = 1;
            DrakenVector mlv = draken_vector_from_dense(mldata, 2, DRAKEN_INT64, nullptr);
            DrakenVector mrv = draken_vector_from_dense(mrdata, 1, DRAKEN_INT64, nullptr);
            DrakenVector* mres = draken_arithmetic_dv(1, &mlv, &mrv, 2, arena);
            r["length_mismatch_returns_null"] = (mres == nullptr);
            draken_free(mldata);
            draken_free(mrdata);

            draken_frame_arena_destroy(arena);
            r["destroy_no_crash"] = true;
            return r;
        },
        "Arithmetic-dv smoke test. Returns {step_name: passed}.");
}
