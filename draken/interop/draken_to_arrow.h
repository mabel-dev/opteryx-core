#pragma once
// draken/interop/draken_to_arrow.h
//
// Export a DrakenVector to the Arrow C Data Interface (ArrowSchema + ArrowArray).
//
// Usage from Python/Cython:
//   ArrowArray  arr;
//   ArrowSchema schema;
//   if (draken_export_to_arrow(dv, &arr, &schema))
//       pa.Array._import_from_c(<uintptr_t>&arr, <uintptr_t>&schema)
//   else
//       // fall back to to_pylist()
//
// Supported types (dense identity selection):
//   INT8/16/32/64, FLOAT32/64, DATE32, TIMESTAMP64 (µs), BOOL,
//   VARCHAR/NVARCHAR/VARIANT (→ utf8), VARBINARY (→ binary), INTERVAL,
//   NULL, DECIMAL (→ int64 unscaled value; precision/scale lost)
//
// Dict-encoded and constant vectors return false — caller falls back.
// TIME32/TIME64 return false — units not recoverable at this level.
// DECIMAL128, ARRAY, NON_NATIVE, FP16 return false.
//
// Non-identity permutations (data_length == length, DRAKEN_SEL_IDENTITY not set)
// are gathered into a fresh buffer before export.
//
// Memory model: all buffers stored in ArrowArray are heap-allocated copies of
// the Draken data.  The release callback frees them.  This avoids coupling the
// Arrow array lifetime to the DrakenVector lifetime.

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "interop/arrow_c_data_interface.h"
#include "core/buffers.h"
#include "core/string_slot.h"

// ---------------------------------------------------------------------------
// Private release state — stored in ArrowArray.private_data
// ---------------------------------------------------------------------------

struct DrakenArrowPrivate {
    void* owned[3];  // up to 3 owned heap allocations (validity, offsets/data, string-data)
};

static void draken_array_release(struct ArrowArray* arr) {
    if (!arr->release) return;
    if (arr->private_data) {
        auto* p = static_cast<DrakenArrowPrivate*>(arr->private_data);
        for (int i = 0; i < 3; ++i)
            std::free(p->owned[i]);
        std::free(p);
        arr->private_data = nullptr;
    }
    if (arr->buffers) {
        std::free(const_cast<void**>(arr->buffers));
        arr->buffers = nullptr;
    }
    arr->release = nullptr;
}

static void draken_schema_release(struct ArrowSchema* schema) {
    // All format strings are compile-time literals — nothing to free.
    schema->release = nullptr;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static inline DrakenArrowPrivate* init_array(struct ArrowArray* arr,
                                              int64_t length,
                                              int64_t null_count,
                                              int n_buffers) {
    std::memset(arr, 0, sizeof(*arr));
    arr->length     = length;
    arr->null_count = null_count;
    arr->n_buffers  = n_buffers;
    arr->release    = draken_array_release;
    arr->buffers    = static_cast<const void**>(
        std::calloc(static_cast<size_t>(n_buffers), sizeof(void*)));
    if (!arr->buffers) throw std::bad_alloc();
    arr->private_data = std::calloc(1, sizeof(DrakenArrowPrivate));
    if (!arr->private_data) { std::free(const_cast<void**>(arr->buffers)); throw std::bad_alloc(); }
    return static_cast<DrakenArrowPrivate*>(arr->private_data);
}

static inline void init_schema(struct ArrowSchema* schema, const char* format) {
    std::memset(schema, 0, sizeof(*schema));
    schema->format  = format;
    schema->name    = "";
    schema->flags   = ARROW_FLAG_NULLABLE;
    schema->release = draken_schema_release;
}

// Copy src into a new malloc buffer; store in priv->owned[slot] and return it.
static inline void* own_copy(DrakenArrowPrivate* priv, int slot,
                              const void* src, size_t bytes) {
    priv->owned[slot] = std::malloc(bytes > 0 ? bytes : 1);
    if (!priv->owned[slot]) throw std::bad_alloc();
    if (bytes > 0) std::memcpy(priv->owned[slot], src, bytes);
    return priv->owned[slot];
}

// Gather selected rows from a fixed-width data array into a fresh buffer.
// Used when selection is not identity (permutation case).
static inline void* gather_fixed(DrakenArrowPrivate* priv, int slot,
                                  const void* data, const uint32_t* sel,
                                  uint32_t n, size_t itemsize) {
    const size_t bytes = static_cast<size_t>(n) * itemsize;
    priv->owned[slot]  = std::malloc(bytes > 0 ? bytes : 1);
    if (!priv->owned[slot]) throw std::bad_alloc();
    auto* dst = static_cast<uint8_t*>(priv->owned[slot]);
    auto* src = static_cast<const uint8_t*>(data);
    for (uint32_t i = 0; i < n; ++i)
        std::memcpy(dst + i * itemsize, src + sel[i] * itemsize, itemsize);
    return priv->owned[slot];
}

static inline int64_t count_nulls(const uint8_t* validity, uint32_t n) {
    if (!validity) return 0;
    int64_t valid = 0;
    const uint32_t full_bytes = n / 8;
    for (uint32_t i = 0; i < full_bytes; ++i)
        valid += __builtin_popcount(validity[i]);
    if (n % 8) {
        uint8_t last = validity[full_bytes] & static_cast<uint8_t>((1u << (n % 8)) - 1u);
        valid += __builtin_popcount(last);
    }
    return static_cast<int64_t>(n) - valid;
}

// Build Arrow variable-length string buffers (int32 offsets + payload bytes)
// by walking German string slots.  Writes into priv->owned[1] and priv->owned[2].
static bool build_string_buffers(const DrakenVector* dv,
                                  DrakenArrowPrivate* priv,
                                  struct ArrowArray* arr) {
    const auto* sa = static_cast<const DrakenStringArena*>(dv->data);
    if (!sa) return false;

    const uint32_t n = dv->length;

    // Pass 1: total payload bytes.
    size_t total = 0;
    for (uint32_t i = 0; i < n; ++i)
        total += str_length(&sa->slots[dv->selection[i]]);

    // Allocate.
    const size_t off_bytes = static_cast<size_t>(n + 1) * sizeof(int32_t);
    priv->owned[1] = std::malloc(off_bytes);
    if (!priv->owned[1]) return false;
    priv->owned[2] = std::malloc(total > 0 ? total : 1);
    if (!priv->owned[2]) return false;

    auto* offsets = static_cast<int32_t*>(priv->owned[1]);
    auto* payload = static_cast<uint8_t*>(priv->owned[2]);

    // Pass 2: fill.
    int32_t off = 0;
    for (uint32_t i = 0; i < n; ++i) {
        offsets[i] = off;
        const uint32_t si   = dv->selection[i];
        const uint32_t slen = str_length(&sa->slots[si]);
        if (slen > 0) {
            const uint8_t* src = str_data(&sa->slots[si], sa->arena);
            std::memcpy(payload + off, src, slen);
            off += static_cast<int32_t>(slen);
        }
    }
    offsets[n] = off;

    arr->buffers[1] = priv->owned[1];
    arr->buffers[2] = priv->owned[2];
    return true;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Export dv to ArrowSchema + ArrowArray via the Arrow C Data Interface.
// Returns true on success; false if the type is not supported.
// On success, both out_schema and out_array are initialised and the caller
// should pass their addresses to pa.Array._import_from_c() — PyArrow then
// owns the structs and will call the release callbacks when done.
static bool draken_export_to_arrow(const DrakenVector* dv,
                                    struct ArrowArray*  out_array,
                                    struct ArrowSchema* out_schema) {
    if (!dv) return false;

    const uint32_t   n    = dv->length;
    const DrakenType dt   = dv->type;
    const bool is_dict    = dv->data_length > 1 && dv->data_length < dv->length;
    const bool is_const   = dv->data_length == 1 && n > 1;
    const bool is_identity = (dv->flags & DRAKEN_SEL_IDENTITY) != 0;

    // Dict and constant: fall back to Python path.
    if (is_dict || is_const) return false;

    // NULL
    if (dt == DRAKEN_NULL) {
        init_schema(out_schema, "n");
        auto* priv = init_array(out_array, n, n, 0);
        (void)priv;
        return true;
    }

    const int64_t null_count = count_nulls(dv->validity, n);

    // Determine format string and itemsize for fixed-width types.
    const char* fmt   = nullptr;
    size_t      isize = 0;

    switch (dt) {
        case DRAKEN_INT8:        fmt = "c";    isize = 1; break;
        case DRAKEN_INT16:       fmt = "s";    isize = 2; break;
        case DRAKEN_INT32:       fmt = "i";    isize = 4; break;
        case DRAKEN_INT64:       fmt = "l";    isize = 8; break;
        case DRAKEN_DECIMAL:     fmt = "l";    isize = 8; break;  // unscaled int64
        case DRAKEN_FLOAT32:     fmt = "f";    isize = 4; break;
        case DRAKEN_FLOAT64:     fmt = "g";    isize = 8; break;
        case DRAKEN_DATE32:      fmt = "tdD";  isize = 4; break;
        case DRAKEN_TIMESTAMP64: fmt = "tsu:"; isize = 8; break;  // µs UTC
        case DRAKEN_BOOL:        fmt = "b";    isize = 0; break;  // bit-packed special case
        default: break;
    }

    // Fixed-width (including BOOL)
    if (fmt) {
        init_schema(out_schema, fmt);
        auto* priv = init_array(out_array, n, null_count, 2);

        // Buffer 0: validity bitmap copy (nullptr if all valid)
        if (dv->validity)
            out_array->buffers[0] = own_copy(priv, 0, dv->validity, (n + 7) / 8);

        // Buffer 1: data — copy directly if identity, gather if permutation.
        if (dt == DRAKEN_BOOL) {
            // 1-bit packed: data covers ceil(n/8) bytes.
            out_array->buffers[1] = own_copy(priv, 1, dv->data, (n + 7) / 8);
        } else if (is_identity) {
            out_array->buffers[1] = own_copy(priv, 1, dv->data,
                                             static_cast<size_t>(n) * isize);
        } else {
            out_array->buffers[1] = gather_fixed(priv, 1, dv->data,
                                                 dv->selection, n, isize);
        }
        return true;
    }

    // String / binary
    if (dt == DRAKEN_VARCHAR || dt == DRAKEN_NVARCHAR || dt == DRAKEN_VARIANT) {
        init_schema(out_schema, "u");  // utf8, int32 offsets
        auto* priv = init_array(out_array, n, null_count, 3);
        if (dv->validity)
            out_array->buffers[0] = own_copy(priv, 0, dv->validity, (n + 7) / 8);
        return build_string_buffers(dv, priv, out_array);
    }
    if (dt == DRAKEN_VARBINARY) {
        init_schema(out_schema, "z");  // binary, int32 offsets
        auto* priv = init_array(out_array, n, null_count, 3);
        if (dv->validity)
            out_array->buffers[0] = own_copy(priv, 0, dv->validity, (n + 7) / 8);
        return build_string_buffers(dv, priv, out_array);
    }

    // INTERVAL: Draken [months:int64][us:int64] → Arrow month_day_nano [months:int32][days:int32][ns:int64]
    if (dt == DRAKEN_INTERVAL) {
        init_schema(out_schema, "tin");  // month_day_nano
        auto* priv = init_array(out_array, n, null_count, 2);
        if (dv->validity)
            out_array->buffers[0] = own_copy(priv, 0, dv->validity, (n + 7) / 8);

        const size_t buf_bytes = static_cast<size_t>(n) * 16;
        priv->owned[1] = std::malloc(buf_bytes > 0 ? buf_bytes : 1);
        if (!priv->owned[1]) return false;

        const auto* src = static_cast<const int64_t*>(dv->data);
        auto*       dst = static_cast<uint8_t*>(priv->owned[1]);
        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t si = dv->selection[i];
            const int64_t months = src[si * 2];
            const int64_t us     = src[si * 2 + 1];
            *reinterpret_cast<int32_t*>(dst + i * 16)     = static_cast<int32_t>(months);
            *reinterpret_cast<int32_t*>(dst + i * 16 + 4) = 0;
            *reinterpret_cast<int64_t*>(dst + i * 16 + 8) = us * 1000LL;
        }
        out_array->buffers[1] = priv->owned[1];
        return true;
    }

    // TIME32, TIME64, DECIMAL128, ARRAY, NON_NATIVE, FP16 — not supported here.
    return false;
}
