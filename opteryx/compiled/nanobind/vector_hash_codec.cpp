// opteryx/compiled/nanobind/vector_hash_codec.cpp — Milestone E.8, Phase 7, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, six functions:
//
//   vector_hex_encode   — DRAKEN_VARCHAR → DRAKEN_VARCHAR (2× length, UPPERCASE hex).
//   vector_hex_decode   — DRAKEN_VARCHAR → DRAKEN_VARCHAR (½ length, raw bytes).
//   vector_md5          — DRAKEN_VARCHAR → DRAKEN_VARCHAR (32 lowercase hex chars).
//   vector_sha1         — DRAKEN_VARCHAR → DRAKEN_VARCHAR (40 lowercase hex chars).
//   vector_sha256       — DRAKEN_VARCHAR → DRAKEN_VARCHAR (64 lowercase hex chars).
//   vector_sha512       — DRAKEN_VARCHAR → DRAKEN_VARCHAR (128 lowercase hex chars).
//
// Uses draken_vector_own_string (Phase-6 bridge) for all outputs.
// Null TVL: null input row → null output slot; validity bitmap preserved.
// Fails loud on non-Vector or non-string-family input.
//
// Hash output case:
//   hex encode: UPPERCASE via mabel bintob16 (matches old vector_hex.pyx).
//   MD5/SHA:    lowercase via nibble table (matches old _hash_helpers.pyx _to_hex).
//
// Replaces: opteryx/compiled/vector_ops/vector_hex.pyx
//           opteryx/compiled/vector_ops/vector_md5.pyx
//           opteryx/compiled/vector_ops/vector_sha.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstring>
#include <stdexcept>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

// _base16.h uses C99 `restrict` keyword which is not valid in C++ even inside
// extern "C".  Map it to the GCC/Clang extension `__restrict__` before including.
#ifndef restrict
#define restrict __restrict__
#define DRAKEN_UNDEF_RESTRICT
#endif

extern "C" {
#include "_base16.h"
#include "md5.h"
#include "sha1.h"
#include "sha2.h"
}

#ifdef DRAKEN_UNDEF_RESTRICT
#undef restrict
#undef DRAKEN_UNDEF_RESTRICT
#endif

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap_string_vec(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();  // TypeError already set by unwrap
    if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR &&
        dv->type != DRAKEN_VARBINARY)
        throw nb::type_error("expected a string-family Vector (VARCHAR/NVARCHAR/VARBINARY)");
    return dv;
}

static inline bool row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return false;
    return !((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

static uint8_t* copy_validity(const DrakenVector* dv) {
    if (!dv->validity) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}

// Lowercase nibble table for MD5/SHA digest → hex.
static const char kHexLc[16] = {
    '0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'
};

static inline void bytes_to_hex_lc(const uint8_t* digest, uint32_t dlen,
                                   uint8_t* out) noexcept {
    for (uint32_t i = 0u; i < dlen; ++i) {
        const unsigned b  = digest[i];
        out[2u * i]       = static_cast<uint8_t>(kHexLc[b >> 4]);
        out[2u * i + 1u]  = static_cast<uint8_t>(kHexLc[b & 0xFu]);
    }
}

// ---------------------------------------------------------------------------
// Fixed-length digest apply (MD5 / SHA-1 / SHA-256 / SHA-512).
//
// All digest outputs are ≥ 32 chars > STR_INLINE_MAX, so every output slot
// is long-form (extern).  Arena is pre-allocated at n × hex_len bytes (worst
// case: all rows non-null).  Null rows get a zeroed slot; their arena bytes
// are not written, so arena_used < n × hex_len when nulls are present.
// ---------------------------------------------------------------------------

using DigestFn = void (*)(const uint8_t* data, uint32_t len, unsigned char* out);

static nb::object digest_apply(nb::object obj, DigestFn fn, uint32_t digest_bytes) {
    const uint32_t hex_len = digest_bytes * 2u;  // always > STR_INLINE_MAX

    const DrakenVector*      dv    = unwrap_string_vec(obj);
    const DrakenStringArena* in_sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t           n     = dv->length;

    // Allocate output slots (one per logical row).
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // Arena: worst-case all n rows non-null × hex_len bytes each.
    const size_t arena_sz = (n > 0u ? static_cast<size_t>(n) * hex_len : 1u);
    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() {
            if (s) draken_free(s);
            if (a) draken_free(a);
            if (v) draken_free(v);
        }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    unsigned char digest[64];  // enough for SHA-512
    size_t        arena_used = 0u;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) {
            str_init_null(&slots[i]);
            continue;
        }
        const DrakenStringSlot* in_slot = &in_sa->slots[dv->selection[i]];
        const uint8_t*          in_data = str_data(in_slot, in_sa->arena);
        const uint32_t          in_len  = str_length(in_slot);

        fn(in_data, in_len, digest);

        const uint32_t off  = static_cast<uint32_t>(arena_used);
        uint8_t*       dest = arena + off;
        bytes_to_hex_lc(digest, digest_bytes, dest);
        draken_build_string_slot(&slots[i], dest, hex_len, off);
        arena_used += hex_len;
    }

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_string(slots, arena, arena_used,
                                             out_validity, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// Digest function wrappers matching DigestFn signature.

static void do_md5(const uint8_t* data, uint32_t len, unsigned char* out) {
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, data, len);
    MD5_Final(out, &ctx);
}

static void do_sha1(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA_CTX ctx;
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, data, len);
    SHA1_Final(out, &ctx);
}

static void do_sha256(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}

static void do_sha512(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA512_CTX ctx;
    SHA512_Init(&ctx);
    SHA512_Update(&ctx, data, len);
    SHA512_Final(out, &ctx);
}

// ---------------------------------------------------------------------------
// Variable-length codec apply (hex encode / hex decode).
//
// Output may be inline (≤ STR_INLINE_MAX) or extern depending on input length.
// Pass 1 computes total extern arena bytes; Pass 2 encodes and fills slots.
// Uses a reusable tmp_buf grown to the largest output seen.
// ---------------------------------------------------------------------------

static nb::object vhex_apply(
    nb::object obj,
    size_t   (*max_out_fn)(size_t),
    void*    (*codec_fn)(void*, const void*, size_t))
{
    const DrakenVector*      dv    = unwrap_string_vec(obj);
    const DrakenStringArena* in_sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t           n     = dv->length;

    // Pass 1: compute total extern arena bytes needed.
    size_t total_extern = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) continue;
        const DrakenStringSlot* slot = &in_sa->slots[dv->selection[i]];
        const size_t out_max = max_out_fn(str_length(slot));
        if (out_max > STR_INLINE_MAX) total_extern += out_max;
    }

    // Allocate output slots.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // Allocate arena (1-byte minimum so draken_vector_own_string can always free).
    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(total_extern > 0u ? total_extern : 1u));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint8_t* v;
        ~Guard() {
            if (s) draken_free(s);
            if (a) draken_free(a);
            if (v) draken_free(v);
        }
    } g{slots, arena, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    // Reusable temp buffer for codec output (grown as needed).
    size_t   tmp_cap = 0u;
    uint8_t* tmp_buf = nullptr;
    size_t   arena_used = 0u;

    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) {
            str_init_null(&slots[i]);
            continue;
        }
        const DrakenStringSlot* in_slot = &in_sa->slots[dv->selection[i]];
        const uint8_t*          in_data = str_data(in_slot, in_sa->arena);
        const uint32_t          in_len  = str_length(in_slot);

        if (in_len == 0u) {
            str_init_inline(&slots[i], nullptr, 0u);
            continue;
        }

        const size_t out_max = max_out_fn(in_len);

        if (out_max + 1u > tmp_cap) {
            if (tmp_buf) draken_free(tmp_buf);
            tmp_cap = out_max + 8u;
            tmp_buf = static_cast<uint8_t*>(draken_malloc(tmp_cap));
            if (!tmp_buf) throw std::bad_alloc();
        }

        void*          end        = codec_fn(tmp_buf, in_data, in_len);
        const uint32_t actual_len = static_cast<uint32_t>(
            static_cast<uint8_t*>(end) - tmp_buf);

        if (actual_len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], tmp_buf, actual_len);
        } else {
            const uint32_t off = static_cast<uint32_t>(arena_used);
            std::memcpy(arena + off, tmp_buf, actual_len);
            draken_build_string_slot(&slots[i], tmp_buf, actual_len, off);
            arena_used += actual_len;
        }
    }

    if (tmp_buf) draken_free(tmp_buf);

    g.s = nullptr; g.a = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_string(slots, arena, arena_used,
                                             out_validity, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// Codec adapter wrappers matching vhex_apply signature.

static void* b16_encode_adapter(void* dest, const void* src, size_t len) {
    return static_cast<void*>(bintob16(static_cast<char*>(dest), src, len));
}

static void* b16_decode_adapter(void* dest, const void* src, size_t len) {
    return b16tobin_len(dest, static_cast<const char*>(src), len);
}

static size_t b16_encoded_size_wrap(size_t n) { return b16_encoded_size(n); }
static size_t b16_decoded_size_wrap(size_t n) { return b16_decoded_size(n); }

// ---------------------------------------------------------------------------
// NB_MODULE — six functions, one module.
// ---------------------------------------------------------------------------

NB_MODULE(vector_hash_codec, m) {

    m.def("vector_hex_encode",
        [](nb::object v) -> nb::object {
            return vhex_apply(v, b16_encoded_size_wrap, b16_encode_adapter);
        },
        nb::arg("v"),
        "HEX_ENCODE(v): element-wise base16 (hex) encoding of a DRAKEN_VARCHAR Vector. "
        "Output is UPPERCASE ASCII. Null rows propagate as null. Empty string → empty string.");

    m.def("vector_hex_decode",
        [](nb::object v) -> nb::object {
            return vhex_apply(v, b16_decoded_size_wrap, b16_decode_adapter);
        },
        nb::arg("v"),
        "HEX_DECODE(v): element-wise base16 decoding of a DRAKEN_VARCHAR Vector. "
        "Null rows propagate as null. Input must have even length.");

    m.def("vector_md5",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_md5, 16u);
        },
        nb::arg("v"),
        "MD5(v): element-wise MD5 hash → 32-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha1",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha1, 20u);
        },
        nb::arg("v"),
        "SHA1(v): element-wise SHA-1 hash → 40-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha256",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha256, 32u);
        },
        nb::arg("v"),
        "SHA256(v): element-wise SHA-256 hash → 64-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha512",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha512, 64u);
        },
        nb::arg("v"),
        "SHA512(v): element-wise SHA-512 hash → 128-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");
}
