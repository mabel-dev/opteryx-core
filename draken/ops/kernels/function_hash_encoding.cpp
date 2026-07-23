// draken/ops/kernels/function_hash_encoding.cpp — Phase 9a-fn: cryptographic
// digest kernels (MD5 / SHA-1 / SHA-224 / SHA-256 / SHA-384 / SHA-512) on the C
// ABI. Signature is the design's func_fn_t:
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (compiled_expression.pyx resolves
// draken_{name} at bind time and sets BC_INSTR_C_NATIVE) — no Python, no
// nanobind, no GIL. Each kernel maps a string-family operand to a lowercase-hex
// DRAKEN_VARCHAR digest.
//
// Semantics are pinned to the pre-existing nanobind entry points in
// opteryx/compiled/nanobind/vector_hash_codec.cpp (vector_md5 / vector_sha*),
// which remain as the registrar's callable_ref and as the Python-visible API:
//   - operand must be VARCHAR / NVARCHAR / VARBINARY; anything else fails loud.
//   - output is DRAKEN_VARCHAR, lowercase hex, 2 chars per digest byte.
//   - digests hash the raw slot bytes; a null row stays null.
//   - SHA-224 / SHA-384 reuse the unmodified vendored SHA-256 / SHA-512 cores
//     with the FIPS 180-4 IV swapped in, and hex only the leading 28 / 48 bytes.
//
// SHAPE-PRESERVING (the string_trim.cpp / draken_substring pattern): a digest is
// a pure function of a physical value's bytes, so it is computed ONCE per
// data_length PHYSICAL unique value and kernel_preserve_shape then carries the
// input's selection + per-logical-row validity onto the result. Dense stays
// dense, constant stays constant, dict stays dict. This is the uniform
// data[selection[i]] contract, not shape dispatch — there is no branch on
// encoding shape here, and the answer is identical for all three shapes. The
// digest core is ~95% of the per-row cost, so hashing K uniques rather than N
// rows is where the win is.
//
// Every digest is >= 16 bytes => hex_len >= 32 > STR_INLINE_MAX (12), so every
// output slot is long-form and the arena is exactly k * hex_len bytes — no
// sizing pass is needed.

#include <cstdint>
#include <cstdio>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"  // XXH3_64bits — long-slot hash32, via draken_build_string_slot

// _base16.h uses C99 `restrict`, invalid in C++ even inside extern "C". Map it
// to the GCC/Clang extension `__restrict__` before including (same shim as
// opteryx/compiled/nanobind/vector_hash_codec.cpp).
#ifndef restrict
#define restrict __restrict__
#define DRAKEN_UNDEF_RESTRICT
#endif

// Vendored mabel base16 (repo-root third_party/mabel/base16, opteryx-free so this
// draken kernel — which also ships in the standalone rugo wheel — can use it).
// bintob16_lower is the SIMD-dispatched (NEON/AVX2/RVV/scalar) lowercase hex
// encoder shared with HEX_ENCODE's bintob16 (same cores, different LUT) — one
// hex-encode implementation, not a second hand-rolled one living here.
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

namespace {

inline bool hash_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

using DigestFn = void (*)(const uint8_t* data, uint32_t len, unsigned char* out);

void do_md5(const uint8_t* data, uint32_t len, unsigned char* out) {
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, data, len);
    MD5_Final(out, &ctx);
}

void do_sha1(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA_CTX ctx;
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, data, len);
    SHA1_Final(out, &ctx);
}

void do_sha256(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}

void do_sha224(const uint8_t* data, uint32_t len, unsigned char* out) {
    // SHA-224 is the vendored SHA-256 core with a distinct IV (FIPS 180-4 §5.3.2),
    // truncated to 28 bytes. Reuse the unmodified vendored SHA256_* functions and
    // only swap the initial state; Final writes 32 bytes into the 64-byte digest
    // buffer and the caller hex-encodes the first 28.
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    ctx.state[0]=0xc1059ed8; ctx.state[1]=0x367cd507; ctx.state[2]=0x3070dd17; ctx.state[3]=0xf70e5939;
    ctx.state[4]=0xffc00b31; ctx.state[5]=0x68581511; ctx.state[6]=0x64f98fa7; ctx.state[7]=0xbefa4fa4;
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}

void do_sha384(const uint8_t* data, uint32_t len, unsigned char* out) {
    // SHA-384 is the vendored SHA-512 core with a distinct IV (FIPS 180-4 §5.3.4),
    // truncated to 48 bytes. Same approach as SHA-224.
    SHA512_CTX ctx;
    SHA512_Init(&ctx);
    ctx.state[0]=0xcbbb9d5dc1059ed8ULL; ctx.state[1]=0x629a292a367cd507ULL;
    ctx.state[2]=0x9159015a3070dd17ULL; ctx.state[3]=0x152fecd8f70e5939ULL;
    ctx.state[4]=0x67332667ffc00b31ULL; ctx.state[5]=0x8eb44a8768581511ULL;
    ctx.state[6]=0xdb0c2e0d64f98fa7ULL; ctx.state[7]=0x47b5481dbefa4fa4ULL;
    SHA512_Update(&ctx, data, len);
    SHA512_Final(out, &ctx);
}

void do_sha512(const uint8_t* data, uint32_t len, unsigned char* out) {
    SHA512_CTX ctx;
    SHA512_Init(&ctx);
    SHA512_Update(&ctx, data, len);
    SHA512_Final(out, &ctx);
}

VecResult digest_kernel(const DrakenVector* v, DigestFn fn,
                        uint32_t digest_bytes, const char* who) {
    if (!hash_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);

    const auto*    sa      = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k       = v->data_length;      // physical unique count
    const uint32_t hex_len = digest_bytes * 2u;   // >= 32, always long-form

    // Every output slot is extern, so the arena size is exact — no sizing pass.
    const size_t arena_len = static_cast<size_t>(k) * hex_len;

    // K-slot physical block, NO embedded validity (per-logical-row nulls come
    // from kernel_preserve_shape).
    DrakenStringSlot* slots;
    uint8_t*          arena;
    uint8_t*          validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_len, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);

    unsigned char digest[64];        // enough for SHA-512
    char          hex_tmp[64 * 2 + 1];  // + trailing NUL bintob16_lower writes past the end
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        fn(str_data(slot, sa->arena), str_length(slot), digest);

        bintob16_lower(hex_tmp, digest, digest_bytes);

        const uint32_t off  = j * hex_len;
        uint8_t*       dest = arena + off;
        std::memcpy(dest, hex_tmp, hex_len);
        draken_build_string_slot(&slots[j], dest, hex_len, off);
    }

    // Carry the input's shape onto the dense K-block (see string_trim.cpp for the
    // bad_alloc-containment rationale). Digests never introduce nulls, so the
    // input's validity maps 1:1 onto the result.
    VecResult r = vecresult_from_string_block(block, k, arena_len, /*has_validity=*/0,
                                              DRAKEN_VARCHAR);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who);
    }
    return r;
}

// ---------------------------------------------------------------------------
// HASH — XXH3-64 of the raw slot bytes, rendered as minimal-width lowercase hex.
// ---------------------------------------------------------------------------
//
// Semantics are pinned to the Python kernel this replaces (registrar's
// hash_encoding.pyx::_hash_kernel):
//     hex(hash_bytes(str(x).encode()))[2:].encode()   -> VARBINARY
// where hash_bytes is XXH3_64bits (opteryx/third_party/cyan4973/xxhash.pyx).
//
// The `str(x)` there looks like it demands CPython repr semantics for arbitrary
// types — but HASH is string-family-only (VARCHAR/NVARCHAR/VARBINARY; every
// other type fails loud, matching the digest kernels), and for a string `str(x)`
// is the identity. So the native form is exactly XXH3_64bits over the slot's
// raw bytes: no Python formatting to reproduce.
//
// Rendering: printf "%llx" is byte-identical to Python's hex(v)[2:] — lowercase,
// minimal digits, leading zeros stripped, and "0" for zero. So output is
// unchanged from the Python kernel (1..16 chars, hence variable-width: values
// <= STR_INLINE_MAX go inline, longer ones to the arena).
//
// Output type is DRAKEN_VARBINARY, matching BOTH the Python kernel's
// vector_from_sequence(dtype=VARBINARY) and the registrar's declared
// _CT_VARBINARY return spec.
VecResult hash_kernel(const DrakenVector* v, const char* who) {
    if (!hash_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);

    const auto*    sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k  = v->data_length;   // physical unique count

    // A 64-bit value renders to at most 16 hex chars. Size the arena at that
    // worst case; pass 2 packs the ACTUAL bytes and finalizes with the real
    // cursor, so an over-allocated capacity tail is simply unused (the arena's
    // start offset depends only on k, never on arena_len — see
    // result_helpers.cpp::string_block_layout).
    const size_t arena_cap = static_cast<size_t>(k) * 16u;

    DrakenStringSlot* slots;
    uint8_t*          arena;
    uint8_t*          validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_cap, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);

    char   buf[24];   // 16 hex digits + NUL, rounded up
    size_t arena_pos = 0u;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        const uint64_t h = XXH3_64bits(str_data(slot, sa->arena), str_length(slot));
        const int written = std::snprintf(buf, sizeof(buf), "%llx",
                                          static_cast<unsigned long long>(h));
        // snprintf's own count; 1..16 for any uint64 under %llx, so it cannot
        // truncate into buf — but trust the return value rather than assume.
        if (written < 0 || static_cast<size_t>(written) >= sizeof(buf)) {
            draken_free(block);
            return draken_error_sentinel_fmt("%s: hex formatting failed", who);
        }
        const uint32_t len = static_cast<uint32_t>(written);
        const uint8_t* src = reinterpret_cast<const uint8_t*>(buf);

        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], src, len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, src, len);
            str_init_extern(&slots[j], dst, len,
                            static_cast<uint32_t>(arena_pos));
            arena_pos += len;
        }
    }

    VecResult r = vecresult_from_string_block(block, k, arena_pos, /*has_validity=*/0,
                                              DRAKEN_VARBINARY);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who);
    }
    return r;
}

}  // namespace

extern "C" {

VecResult draken_hash(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_hash: expected 1 argument");
    return hash_kernel(args[0], "draken_hash");
}

VecResult draken_md5(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_md5: expected 1 argument");
    return digest_kernel(args[0], do_md5, 16u, "draken_md5");
}

VecResult draken_sha1(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sha1: expected 1 argument");
    return digest_kernel(args[0], do_sha1, 20u, "draken_sha1");
}

VecResult draken_sha224(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sha224: expected 1 argument");
    return digest_kernel(args[0], do_sha224, 28u, "draken_sha224");
}

VecResult draken_sha256(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sha256: expected 1 argument");
    return digest_kernel(args[0], do_sha256, 32u, "draken_sha256");
}

VecResult draken_sha384(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sha384: expected 1 argument");
    return digest_kernel(args[0], do_sha384, 48u, "draken_sha384");
}

VecResult draken_sha512(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_sha512: expected 1 argument");
    return digest_kernel(args[0], do_sha512, 64u, "draken_sha512");
}

}  // extern "C"
