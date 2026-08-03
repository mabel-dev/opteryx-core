// opteryx/compiled/nanobind/vector_codec.cpp — Milestone E.4, Phase 3, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions.
//
// Each function:
//   1. Receives nb::object operand (expected: DRAKEN_VARCHAR Vector).
//   2. Calls draken_vector_unwrap; raises TypeError on non-DRAKEN_VARCHAR.
//   3. Iterates logical rows, encoding/decoding each non-null slot.
//   4. Builds a dense output DrakenStringArena (single-block allocation).
//   5. Copies validity bitmap from input; null rows → null output slots.
//   6. Wraps output via draken_vector_own_raw → returns new DRAKEN_VARCHAR Vector.
//
// Output is always DENSE (identity selection).  Dict-preserving output would
// require a new extern "C" bridge function (Part A).  Semantics are identical;
// only memory layout differs for highly-repeated inputs.
//
// Null TVL: null input row → null output row (validity bitmap preserved).
// Empty string: encoded/decoded as "" (length 0, inline slot, all valid).
// Multibyte UTF-8: opaque byte sequences — codec operates on raw bytes, not
//   code points, so any encoding that round-trips raw bytes is safe.
//
// Replaces: opteryx/compiled/vector_ops/vector_base64.pyx
//           opteryx/compiled/vector_ops/vector_base85.pyx  (deleted in E.4)

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstring>
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

// Mabel C encoders (vendored C; include dirs wired in setup.py E.4 block).
extern "C" {
#include "_base64.h"
#include "_base85.h"
}

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap_string(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set
    // Codecs operate on raw bytes — accept the whole string family (matching
    // the hex codec): VARCHAR/NVARCHAR text and the VARBINARY produced by the
    // *_ENCODE counterparts.
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR &&
        dv->type != DRAKEN_VARBINARY)
        throw nb::type_error("expected a string-family Vector (VARCHAR/NVARCHAR/VARBINARY)");
    return dv;
}

// Build output validity bitmap as a copy of the input's validity.
// Returns nullptr (all-valid) when dv->validity is nullptr.
// Caller owns the returned buffer (draken_free on error / consumed by own_raw).
static uint8_t* copy_validity(const DrakenVector* dv) {
    if (dv->validity == nullptr) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = ((bm + 7u) & ~7u);
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}

// Returns true when logical row i is null.
static inline bool row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (dv->validity == nullptr) return false;
    return !((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

// Core codec: apply encode_fn to each non-null logical row and build a dense
// DRAKEN_VARCHAR output vector.
//
// max_out_fn:  upper bound on output byte count given input byte count.
// encode_fn:   encode_fn(dest, src, src_len) writes output to dest and returns
//              a pointer past the last byte written.
static nb::object codec_apply(
    nb::object obj,
    size_t   (*max_out_fn)(size_t),
    void*    (*encode_fn)(void*, const void*, size_t),
    const char* codec_name)
{
    const DrakenVector*      dv    = unwrap_string(obj);
    const DrakenStringArena* in_sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t           n     = dv->length;

    // --- Pass 1: compute total arena bytes for long-form output slots. --------
    size_t total_extern = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;
        const DrakenStringSlot* slot = &in_sa->slots[dv->selection[i]];
        const size_t out_max = max_out_fn(str_length(slot));
        if (out_max > STR_INLINE_MAX)
            total_extern += out_max;
    }

    // --- Allocate output block: [DrakenStringArena | slots[n] | arena]. ------
    constexpr size_t kAlign   = alignof(DrakenStringSlot);
    const size_t     hdr_end  = (sizeof(DrakenStringArena) + kAlign - 1u) & ~(kAlign - 1u);
    const size_t     slot_sz  = static_cast<size_t>(n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    const size_t     arena_at = hdr_end + slot_sz;
    const size_t     total    = arena_at + total_extern;
    const size_t     alloc    = total > 0u ? total : sizeof(DrakenStringArena);

    uint8_t* block = static_cast<uint8_t*>(draken_malloc(alloc));
    if (!block) throw std::bad_alloc();
    std::memset(block, 0, alloc);

    DrakenStringArena* out_sa    = reinterpret_cast<DrakenStringArena*>(block);
    DrakenStringSlot*  out_slots = reinterpret_cast<DrakenStringSlot*>(block + hdr_end);
    uint8_t*           out_arena = total_extern > 0u ? block + arena_at : nullptr;

    out_sa->slots        = out_slots;
    out_sa->arena        = out_arena;
    out_sa->length       = n;
    out_sa->arena_used   = 0u;
    out_sa->arena_cap    = total_extern;
    out_sa->null_bitmap  = nullptr;
    out_sa->owns_buffers = 0;
    out_sa->payloads_elided = 0;
    out_sa->type         = DRAKEN_VARCHAR;

    // Validity: copy from input (null rows are left as null slots = zero).
    uint8_t* out_validity = nullptr;
    // Guard: copy_validity can throw; on any error below we free block+validity.
    struct Guard {
        uint8_t* block; uint8_t* validity;
        ~Guard() { if (block) draken_free(block); if (validity) draken_free(validity); }
    } g{block, nullptr};

    out_validity = copy_validity(dv);
    g.validity   = out_validity;

    // --- Pass 2: encode each logical row. -------------------------------------
    // One reusable heap buffer per codec_apply call (grows as needed).
    //
    // Scratch headroom: the mabel codecs can WRITE past the length they RETURN,
    // so max_out_fn() alone is not a safe capacity. Measured worst-case excess
    // over max_out_fn(n) for every codec routed through here (n = 1..300, this
    // build's SIMD paths included):
    //     hex encode +1 (trailing NUL)   hex decode    0
    //     base64 encode 0                base64 decode 0
    //     base85 encode +3               base85 decode 0
    // base85 encode is the binding case: bintob85 pads its final partial group
    // to 4 bytes and writes all 5 output chars, then advances the cursor by
    // only (remaining + 1). 8 covers the measured 3 with margin.
    constexpr size_t kCodecScratchHeadroom = 8u;

    size_t   tmp_cap = 0u;
    uint8_t* tmp_buf = nullptr;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) {
            // Slot already zeroed (str_init_null semantics); skip.
            continue;
        }

        const DrakenStringSlot* in_slot = &in_sa->slots[dv->selection[i]];
        const uint8_t*          in_data = str_data(in_slot, in_sa->arena);
        const uint32_t          in_len  = str_length(in_slot);

        const size_t out_max = max_out_fn(in_len);

        // Empty input string: encode/decode of "" → ""; stays inline, zero length.
        // This is distinct from out_max == 0 (which can happen for short base85
        // inputs where b85_decoded_size(n<5) = 0 but actual output is non-empty).
        if (in_len == 0u) {
            str_init_inline(&out_slots[i], nullptr, 0u);
            continue;
        }

        // Grow temp buffer if needed. The guard and the allocation MUST use the
        // same headroom: sizing with +8 while testing with +1 let a long value
        // following a short one pass the guard without the buffer actually
        // carrying its own headroom (in_len 8 -> tmp_cap 18, then in_len 13
        // writes 20 bytes = 2-byte overflow).
        if (out_max + kCodecScratchHeadroom > tmp_cap) {
            if (tmp_buf) draken_free(tmp_buf);
            tmp_cap = out_max + kCodecScratchHeadroom;
            tmp_buf = static_cast<uint8_t*>(draken_malloc(tmp_cap));
            if (!tmp_buf) throw std::bad_alloc();
        }

        // Run codec.
        void* end = encode_fn(tmp_buf, in_data, in_len);
        // Decoders return NULL on malformed input (bad chars / wrong length).
        // Fail loud — pointer math on NULL underflows actual_len and segfaults.
        if (end == nullptr) {
            draken_free(tmp_buf);
            throw nb::value_error(
                (std::string(codec_name) + ": malformed input string").c_str());
        }
        const uint32_t actual_len = static_cast<uint32_t>(
            static_cast<uint8_t*>(end) - tmp_buf);

        if (actual_len <= STR_INLINE_MAX) {
            str_init_inline(&out_slots[i], tmp_buf, actual_len);
        } else {
            const uint32_t off = static_cast<uint32_t>(out_sa->arena_used);
            std::memcpy(out_arena + off, tmp_buf, actual_len);
            str_init_extern(&out_slots[i], tmp_buf, actual_len, off);
            out_sa->arena_used += actual_len;
        }
    }

    if (tmp_buf) draken_free(tmp_buf);

    // Transfer ownership to Python Vector.
    g.block    = nullptr;  // consumed by own_raw
    g.validity = nullptr;

    PyObject* out = draken_vector_own_raw(block, out_validity, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// Adapter functions: map mabel signatures to codec_apply's signature.
// mabel encode: (char* dest, const void* src, size_t size) → char*
// mabel decode: (void* dest, const char* src, size_t len)  → void*
// codec_apply: (void* dest, const void* src, size_t len) → void*
// Both need to match: void* (*)(void*, const void*, size_t).
// ---------------------------------------------------------------------------

static void* b64_encode_adapter(void* dest, const void* src, size_t len) {
    return static_cast<void*>(bintob64(static_cast<char*>(dest), src, len));
}

static void* b64_decode_adapter(void* dest, const void* src, size_t len) {
    return b64tobin_len(dest, static_cast<const char*>(src), len);
}

static void* b85_encode_adapter(void* dest, const void* src, size_t len) {
    return static_cast<void*>(bintob85(static_cast<char*>(dest), src, len));
}

static void* b85_decode_adapter(void* dest, const void* src, size_t len) {
    return b85tobin_len(dest, static_cast<const char*>(src), len);
}

// Max-size wrappers: mabel returns size_t, codec_apply uses size_t — direct.
static size_t b64_encoded_size_wrap(size_t n) { return b64_encoded_size(n); }
static size_t b64_decoded_size_wrap(size_t n) { return b64_decoded_size(n); }
static size_t b85_encoded_size_wrap(size_t n) { return b85_encoded_size(n); }
// b85_decoded_size(L) = (L/5)*4 ignores partial-group tail bytes.
// True decoded size = full*4 + max(0, rem-1) where rem = L%5.
// Example: L=17 → b85_decoded_size=12 but actual=13 (1 partial byte).
// Under-allocation causes actual_len > out_max in Pass 2, taking the extern
// branch with out_arena=nullptr → SEGFAULT.  Return the exact upper bound.
static size_t b85_decoded_size_wrap(size_t n) {
    const size_t full = (n / 5u) * 4u;
    const size_t rem  = n % 5u;
    return full + (rem >= 2u ? rem - 1u : 0u);
}

// ---------------------------------------------------------------------------
// NB_MODULE — four functions, one module.
// ---------------------------------------------------------------------------

void register_vector_codec(nb::module_ &m) {

    m.def("vector_base64_encode",
        [](nb::object v) -> nb::object {
            return codec_apply(v, b64_encoded_size_wrap, b64_encode_adapter, "BASE64_ENCODE");
        },
        nb::arg("v"),
        "BASE64(v): element-wise base64 encoding of a DRAKEN_VARCHAR Vector. "
        "Null rows propagate as null. Empty string → empty string.");

    m.def("vector_base64_decode",
        [](nb::object v) -> nb::object {
            return codec_apply(v, b64_decoded_size_wrap, b64_decode_adapter, "BASE64_DECODE");
        },
        nb::arg("v"),
        "UNBASE64(v): element-wise base64 decoding of a DRAKEN_VARCHAR Vector. "
        "Null rows propagate as null. Malformed base64 input raises ValueError.");

    m.def("vector_base85_encode",
        [](nb::object v) -> nb::object {
            return codec_apply(v, b85_encoded_size_wrap, b85_encode_adapter, "BASE85_ENCODE");
        },
        nb::arg("v"),
        "BASE85(v): element-wise base85 (Mercurial alphabet) encoding of a DRAKEN_VARCHAR Vector. "
        "Null rows propagate as null. Empty string → empty string.");

    m.def("vector_base85_decode",
        [](nb::object v) -> nb::object {
            return codec_apply(v, b85_decoded_size_wrap, b85_decode_adapter, "BASE85_DECODE");
        },
        nb::arg("v"),
        "UNBASE85(v): element-wise base85 decoding of a DRAKEN_VARCHAR Vector. "
        "Null rows propagate as null. Malformed base85 input raises ValueError.");
}
