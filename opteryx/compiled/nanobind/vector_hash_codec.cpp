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
#include <string>

#if defined(__aarch64__)
#include <arm_neon.h>  // NEON-vectorized digest→hex (bytes_to_hex_lc)
#endif

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

// Digest → lowercase hex. The scalar byte-at-a-time loop (two table lookups +
// two stores per byte) dominates the per-row build cost once the hash core is
// hardware-accelerated (~18 ns/row vs ~0.3 ns for the slot's XXH3). On aarch64
// we vectorize it: 16 digest bytes → 32 hex bytes via a NEON table lookup over
// the 16-entry alphabet plus an interleaved store. AdvSIMD is baseline on
// aarch64 (no target attribute / no runtime check needed); the <16-byte tail
// and other arches take the scalar path.
static inline void bytes_to_hex_lc(const uint8_t* digest, uint32_t dlen,
                                   uint8_t* out) noexcept {
    uint32_t i = 0u;
#if defined(__aarch64__)
    const uint8x16_t lut    = vld1q_u8(reinterpret_cast<const uint8_t*>(kHexLc));
    const uint8x16_t lomask = vdupq_n_u8(0x0F);
    for (; i + 16u <= dlen; i += 16u) {
        const uint8x16_t v  = vld1q_u8(digest + i);
        uint8x16x2_t     z;
        z.val[0] = vqtbl1q_u8(lut, vshrq_n_u8(v, 4));   // high nibble → hex char
        z.val[1] = vqtbl1q_u8(lut, vandq_u8(v, lomask)); // low nibble  → hex char
        vst2q_u8(out + 2u * i, z);                       // interleave hi,lo per byte
    }
#endif
    for (; i < dlen; ++i) {
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

// Compression-aware path: when the input carries fewer unique values than rows
// (dict / constant / RLE-sourced — draken_is_compressed), hash each UNIQUE value
// ONCE and reuse the input selection. The SHA core is ≈95% of the per-row cost,
// so this turns N hashes into data_length hashes; the output stays compressed
// (value array of digests + the same codes), preserving the uniform
// value[selection[i]] access contract — the answer is identical to the dense path.
static nb::object digest_apply_compressed(
    const DrakenVector* dv, const DrakenStringArena* in_sa,
    DigestFn fn, uint32_t digest_bytes, uint32_t hex_len)
{
    const uint32_t n = dv->length;
    const uint32_t k = dv->data_length;  // unique values (value-array length)

    // Value array: k slots, all extern (hex_len > STR_INLINE_MAX).
    const size_t slots_sz = (k > 0u ? k : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    const size_t arena_sz = (k > 0u ? static_cast<size_t>(k) * hex_len : 1u);
    uint8_t* arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
    if (!arena) { draken_free(slots); throw std::bad_alloc(); }

    const size_t codes_sz = (n > 0u ? static_cast<size_t>(n) : 1u) * sizeof(uint32_t);
    auto* codes = static_cast<uint32_t*>(draken_malloc(codes_sz));
    if (!codes) { draken_free(slots); draken_free(arena); throw std::bad_alloc(); }

    struct Guard {
        DrakenStringSlot* s; uint8_t* a; uint32_t* c; uint8_t* v;
        ~Guard() {
            if (s) draken_free(s);
            if (a) draken_free(a);
            if (c) draken_free(c);
            if (v) draken_free(v);
        }
    } g{slots, arena, codes, nullptr};

    uint8_t* out_validity = copy_validity(dv);
    g.v = out_validity;

    unsigned char digest[64];  // enough for SHA-512
    for (uint32_t s = 0u; s < k; ++s) {
        const DrakenStringSlot* in_slot = &in_sa->slots[s];
        const uint8_t*          in_data = str_data(in_slot, in_sa->arena);
        const uint32_t          in_len  = str_length(in_slot);

        fn(in_data, in_len, digest);

        const uint32_t off  = s * hex_len;
        uint8_t*       dest = arena + off;
        bytes_to_hex_lc(digest, digest_bytes, dest);
        draken_build_string_slot(&slots[s], dest, hex_len, off);
    }
    // Null rows keep their (valid) code; validity masks them. Codes are copied
    // verbatim — every selection entry is a valid [0,k) index by invariant.
    std::memcpy(codes, dv->selection, static_cast<size_t>(n) * sizeof(uint32_t));

    g.s = nullptr; g.a = nullptr; g.c = nullptr; g.v = nullptr;
    PyObject* out = draken_vector_own_string_dict(
        slots, arena, static_cast<size_t>(k) * hex_len, codes, k,
        out_validity, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

static nb::object digest_apply(nb::object obj, DigestFn fn, uint32_t digest_bytes) {
    const uint32_t hex_len = digest_bytes * 2u;  // always > STR_INLINE_MAX

    const DrakenVector*      dv    = unwrap_string_vec(obj);
    const DrakenStringArena* in_sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t           n     = dv->length;

    if (draken_is_compressed(dv))
        return digest_apply_compressed(dv, in_sa, fn, digest_bytes, hex_len);

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

static void do_sha224(const uint8_t* data, uint32_t len, unsigned char* out) {
    // SHA-224 is the vendored SHA-256 core with a distinct IV (FIPS 180-4 §5.3.2),
    // truncated to 28 bytes. We reuse the unmodified vendored SHA256_* functions and
    // only swap the initial state here; Final writes 32 bytes into the 64-byte digest
    // buffer and digest_apply hex-encodes the first 28.
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    ctx.state[0]=0xc1059ed8; ctx.state[1]=0x367cd507; ctx.state[2]=0x3070dd17; ctx.state[3]=0xf70e5939;
    ctx.state[4]=0xffc00b31; ctx.state[5]=0x68581511; ctx.state[6]=0x64f98fa7; ctx.state[7]=0xbefa4fa4;
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}

static void do_sha384(const uint8_t* data, uint32_t len, unsigned char* out) {
    // SHA-384 is the vendored SHA-512 core with a distinct IV (FIPS 180-4 §5.3.4),
    // truncated to 48 bytes. Same approach as SHA-224: reuse the unmodified vendored
    // SHA512_* functions and swap the initial state; Final writes 64 bytes and
    // digest_apply hex-encodes the first 48.
    SHA512_CTX ctx;
    SHA512_Init(&ctx);
    ctx.state[0]=0xcbbb9d5dc1059ed8ULL; ctx.state[1]=0x629a292a367cd507ULL; ctx.state[2]=0x9159015a3070dd17ULL; ctx.state[3]=0x152fecd8f70e5939ULL;
    ctx.state[4]=0x67332667ffc00b31ULL; ctx.state[5]=0x8eb44a8768581511ULL; ctx.state[6]=0xdb0c2e0d64f98fa7ULL; ctx.state[7]=0x47b5481dbefa4fa4ULL;
    SHA512_Update(&ctx, data, len);
    SHA512_Final(out, &ctx);
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
    void*    (*codec_fn)(void*, const void*, size_t),
    const char* codec_name)
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

        void* end = codec_fn(tmp_buf, in_data, in_len);
        // b16tobin returns NULL on malformed input (odd length / non-hex byte).
        // Fail loud — pointer math on NULL underflows actual_len and segfaults.
        if (end == nullptr) {
            draken_free(tmp_buf);
            throw nb::value_error(
                (std::string(codec_name) + ": malformed input string").c_str());
        }
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

void register_vector_hash_codec(nb::module_ &m) {

    m.def("vector_hex_encode",
        [](nb::object v) -> nb::object {
            return vhex_apply(v, b16_encoded_size_wrap, b16_encode_adapter, "HEX_ENCODE");
        },
        nb::arg("v"),
        "HEX_ENCODE(v): element-wise base16 (hex) encoding of a DRAKEN_VARCHAR Vector. "
        "Output is UPPERCASE ASCII. Null rows propagate as null. Empty string → empty string.");

    m.def("vector_hex_decode",
        [](nb::object v) -> nb::object {
            return vhex_apply(v, b16_decoded_size_wrap, b16_decode_adapter, "HEX_DECODE");
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

    m.def("vector_sha224",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha224, 28u);
        },
        nb::arg("v"),
        "SHA224(v): element-wise SHA-224 hash → 56-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha256",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha256, 32u);
        },
        nb::arg("v"),
        "SHA256(v): element-wise SHA-256 hash → 64-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha384",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha384, 48u);
        },
        nb::arg("v"),
        "SHA384(v): element-wise SHA-384 hash → 96-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");

    m.def("vector_sha512",
        [](nb::object v) -> nb::object {
            return digest_apply(v, do_sha512, 64u);
        },
        nb::arg("v"),
        "SHA512(v): element-wise SHA-512 hash → 128-char lowercase hex DRAKEN_VARCHAR. "
        "Null rows propagate as null.");
}
