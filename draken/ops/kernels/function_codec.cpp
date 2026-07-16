// draken/ops/kernels/function_codec.cpp — Phase 9a-fn: HEX/BASE64/BASE85
// encode+decode kernels on the C ABI (func_fn_t shape):
//
//     VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
//
// Dispatched DIRECTLY from the nogil DV* VM (compiled_expression.pyx resolves
// draken_{name} at bind time and sets BC_INSTR_C_NATIVE) — no Python, no
// nanobind, no GIL.
//
// All six wrap the vendored mabel codecs (repo-root third_party/mabel/base16 /
// base64 / base85 — the same base16 library the MD5/SHA digest kernels use for
// hex, see function_hash_encoding.cpp) — no duplicated codec logic. Operand
// must be VARCHAR / NVARCHAR / VARBINARY; anything else fails loud (same rule
// as the digest kernels — codecs operate on bytes, not arbitrary SQL types).
//
// Result type mirrors what the value actually IS, and matches the registrar's
// declared return spec (hash_encoding.pyx):
//   *_ENCODE -> DRAKEN_VARCHAR    (hex/base64/base85 output is ASCII text)
//   *_DECODE -> DRAKEN_VARBINARY  (decoded output is arbitrary bytes; calling it
//                                  text would be a lie — it need not be UTF-8)
//
// SHAPE-PRESERVING (the string_trim.cpp / draken_substring pattern): a codec
// transform is a pure function of a physical value's bytes, computed ONCE per
// data_length PHYSICAL unique value, then kernel_preserve_shape carries the
// input's selection + per-logical-row validity onto the result. Dense stays
// dense, constant stays constant, dict stays dict — the uniform
// data[selection[i]] contract, not shape dispatch.
//
// Output length is NOT always a pure function of input length — BASE64/BASE85
// decode depend on padding / the trailing partial group's content, which the
// b64_decoded_size/b85_decoded_size formulas can only bound, not predict
// exactly (see the pre-existing opteryx/compiled/nanobind/vector_codec.cpp
// vhex_apply/codec_apply, which hit exactly this and solved it the same way):
// every value is encoded/decoded into a reusable scratch buffer first, and the
// codec's own returned end pointer gives the ACTUAL byte count, which is what
// gets packed into the arena. Pass 1 only computes an upper bound (for scratch
// sizing and worst-case arena capacity); pass 2 packs actual bytes back to
// back, so the arena is never over-allocated by more than an unused capacity
// tail — harmless, since every slot's offset is assigned from the running
// actual-bytes cursor, never from the upper-bound capacity.
//
// Fails LOUD (error sentinel) on: non-string operand, and — decode only — a
// malformed input string (the underlying mabel decoder returning NULL).
// Empty input string -> empty output string (inline), matching the
// pre-existing nanobind entry points' contract.

#include <cstdint>
#include <cstring>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "xxhash.h"  // XXH3_64bits — long-slot hash32, via str_init_extern

// _base16.h uses C99 `restrict`, invalid in C++ even inside extern "C". Map it
// to the GCC/Clang extension `__restrict__` before including (base64/base85
// headers already self-guard for C++; see function_hash_encoding.cpp for the
// same shim).
#ifndef restrict
#define restrict __restrict__
#define DRAKEN_UNDEF_RESTRICT
#endif

extern "C" {
#include "_base16.h"
#include "_base64.h"
#include "_base85.h"
}

#ifdef DRAKEN_UNDEF_RESTRICT
#undef restrict
#undef DRAKEN_UNDEF_RESTRICT
#endif

namespace {

inline bool codec_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

using MaxOutFn = size_t (*)(size_t);
using CodecFn  = void*  (*)(void*, const void*, size_t);

// Generic shape-preserving codec kernel — see file header for the scratch/pass
// design. `who` names the kernel in error messages. `out_type` is VARCHAR for the
// encoders (hex/base64/base85 output is ASCII text) and VARBINARY for the
// decoders (decoded output is arbitrary bytes, not necessarily valid text) —
// matching the registrar's declared return types.
VecResult codec_kernel(const DrakenVector* v, MaxOutFn max_out_fn, CodecFn codec_fn,
                       DrakenType out_type, const char* who) {
    if (!codec_is_string(v->type))
        return draken_error_sentinel_fmt("%s: string operand required", who);

    const auto*    sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t k  = v->data_length;   // physical unique count

    // Pass 1: upper-bound scratch size + worst-case arena capacity. Empty
    // values need neither (they short-circuit to an empty slot in pass 2).
    size_t max_out   = 0u;
    size_t arena_cap = 0u;
    for (uint32_t j = 0; j < k; ++j) {
        const uint32_t in_len = str_length(&sa->slots[j]);
        if (in_len == 0u) continue;
        const size_t out_max = max_out_fn(in_len);
        if (out_max > max_out) max_out = out_max;
        if (out_max > STR_INLINE_MAX) arena_cap += out_max;
    }

    DrakenStringSlot* slots;
    uint8_t*          arena;
    uint8_t*          validity_unused;
    uint8_t* block = vecresult_string_block_alloc(k, arena_cap, /*want_validity=*/0,
                                                  &slots, &arena, &validity_unused);
    if (block == nullptr) return draken_error_sentinel_fmt("%s: allocation failed", who);

    // Reusable scratch buffer: every mabel encode function (bintob16/64/85)
    // writes at most out_max bytes of real content — the encode-direction
    // ones additionally write one trailing NUL past the returned end pointer,
    // hence the +8 headroom, matching the pre-existing nanobind codec's margin.
    uint8_t* tmp = nullptr;
    if (max_out > 0u) {
        tmp = static_cast<uint8_t*>(draken_malloc(max_out + 8u));
        if (!tmp) {
            draken_free(block);
            return draken_error_sentinel_fmt("%s: allocation failed", who);
        }
    }

    size_t arena_pos = 0u;
    for (uint32_t j = 0; j < k; ++j) {
        const DrakenStringSlot* slot   = &sa->slots[j];
        const uint8_t*          src    = str_data(slot, sa->arena);
        const uint32_t          in_len = str_length(slot);

        if (in_len == 0u) {
            str_init_inline(&slots[j], nullptr, 0u);
            continue;
        }

        void* end = codec_fn(tmp, src, in_len);
        // Decoders return NULL on malformed input (bad chars / wrong length).
        // Fail loud — pointer math on NULL underflows actual_len and segfaults.
        if (end == nullptr) {
            if (tmp) draken_free(tmp);
            draken_free(block);
            return draken_error_sentinel_fmt("%s: malformed input string", who);
        }
        const uint32_t actual_len = static_cast<uint32_t>(
            static_cast<uint8_t*>(end) - tmp);

        if (actual_len <= STR_INLINE_MAX) {
            str_init_inline(&slots[j], tmp, actual_len);
        } else {
            uint8_t* dst = arena + arena_pos;
            std::memcpy(dst, tmp, actual_len);
            str_init_extern(&slots[j], dst, actual_len,
                            static_cast<uint32_t>(XXH3_64bits(dst, actual_len)),
                            static_cast<uint32_t>(arena_pos));
            arena_pos += actual_len;
        }
    }
    if (tmp) draken_free(tmp);

    // Finalize with the ACTUAL bytes packed (arena_pos), not the upper-bound
    // capacity (arena_cap) — safe because the arena's start offset depends
    // only on `k` (slot count), never on the arena_len value, so alloc's
    // capacity and finalize's actual-used figure are free to differ.
    VecResult r = vecresult_from_string_block(block, k, arena_pos,
                                              /*has_validity=*/0, out_type);
    try {
        kernel_preserve_shape(r, v);
    } catch (const std::exception&) {
        draken_free(block);
        return draken_error_sentinel_fmt("%s: shape-carry allocation failed", who);
    }
    return r;
}

// Adapters binding each mabel codec to the uniform CodecFn / MaxOutFn shape.

size_t hex_encoded_size(size_t n) { return b16_encoded_size(n); }
size_t hex_decoded_size(size_t n) { return b16_decoded_size(n); }
void* hex_encode_fn(void* dest, const void* src, size_t n) {
    return bintob16(static_cast<char*>(dest), src, n);
}
void* hex_decode_fn(void* dest, const void* src, size_t n) {
    return b16tobin_len(dest, static_cast<const char*>(src), n);
}

size_t b64_encoded_size_fn(size_t n) { return b64_encoded_size(n); }
size_t b64_decoded_size_fn(size_t n) { return b64_decoded_size(n); }
void* b64_encode_fn(void* dest, const void* src, size_t n) {
    return bintob64(static_cast<char*>(dest), src, n);
}
void* b64_decode_fn(void* dest, const void* src, size_t n) {
    return b64tobin_len(dest, static_cast<const char*>(src), n);
}

size_t b85_encoded_size_fn(size_t n) { return b85_encoded_size(n); }
// b85_decoded_size(L) = L/5*4 ignores the partial trailing group: mabel's
// b85tobin_len decodes a remainder of r in [2,4] chars into r-1 extra bytes
// (third_party/mabel/base85/_base85.c). Exact upper bound: full groups' 4
// bytes each, plus max(0, remainder-1) for a partial tail — matches
// opteryx/compiled/nanobind/vector_codec.cpp's b85_decoded_size_wrap.
size_t b85_decoded_size_fn(size_t n) {
    const size_t full = (n / 5u) * 4u;
    const size_t rem  = n % 5u;
    return full + (rem >= 2u ? rem - 1u : 0u);
}
void* b85_encode_fn(void* dest, const void* src, size_t n) {
    return bintob85(static_cast<char*>(dest), src, n);
}
void* b85_decode_fn(void* dest, const void* src, size_t n) {
    return b85tobin_len(dest, static_cast<const char*>(src), n);
}

}  // namespace

extern "C" {

VecResult draken_hex_encode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_hex_encode: expected 1 argument");
    return codec_kernel(args[0], hex_encoded_size, hex_encode_fn,
                        DRAKEN_VARCHAR, "draken_hex_encode");
}

VecResult draken_hex_decode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_hex_decode: expected 1 argument");
    return codec_kernel(args[0], hex_decoded_size, hex_decode_fn,
                        DRAKEN_VARBINARY, "draken_hex_decode");
}

VecResult draken_base64_encode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_base64_encode: expected 1 argument");
    return codec_kernel(args[0], b64_encoded_size_fn, b64_encode_fn,
                        DRAKEN_VARCHAR, "draken_base64_encode");
}

VecResult draken_base64_decode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_base64_decode: expected 1 argument");
    return codec_kernel(args[0], b64_decoded_size_fn, b64_decode_fn,
                        DRAKEN_VARBINARY, "draken_base64_decode");
}

VecResult draken_base85_encode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_base85_encode: expected 1 argument");
    return codec_kernel(args[0], b85_encoded_size_fn, b85_encode_fn,
                        DRAKEN_VARCHAR, "draken_base85_encode");
}

VecResult draken_base85_decode(void* /*ctx*/, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs != 1) return draken_error_sentinel("draken_base85_decode: expected 1 argument");
    return codec_kernel(args[0], b85_decoded_size_fn, b85_decode_fn,
                        DRAKEN_VARBINARY, "draken_base85_decode");
}

}  // extern "C"
