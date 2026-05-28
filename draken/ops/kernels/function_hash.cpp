#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: hashing and encoding functions.
 * Phase 8f of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ hash/codec function implementations
extern "C" VecResult vector_md5_impl(const DrakenVector* v);
extern "C" VecResult vector_sha1_impl(const DrakenVector* v);
extern "C" VecResult vector_sha256_impl(const DrakenVector* v);
extern "C" VecResult vector_sha512_impl(const DrakenVector* v);
extern "C" VecResult vector_base64_encode_impl(const DrakenVector* v);
extern "C" VecResult vector_base64_decode_impl(const DrakenVector* v);
extern "C" VecResult vector_hex_encode_impl(const DrakenVector* v);
extern "C" VecResult vector_hex_decode_impl(const DrakenVector* v);
extern "C" VecResult vector_base85_encode_impl(const DrakenVector* v);
extern "C" VecResult vector_base85_decode_impl(const DrakenVector* v);

/**
 * MD5(value): MD5 hash
 */
VecResult vector_md5(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("MD5 expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_md5_impl(args[0]);
    });
}

/**
 * SHA1(value): SHA1 hash
 */
VecResult vector_sha1(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SHA1 expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_sha1_impl(args[0]);
    });
}

/**
 * SHA256(value): SHA256 hash
 */
VecResult vector_sha256(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SHA256 expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_sha256_impl(args[0]);
    });
}

/**
 * SHA512(value): SHA512 hash
 */
VecResult vector_sha512(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SHA512 expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_sha512_impl(args[0]);
    });
}

/**
 * BASE64_ENCODE(value): base64 encode
 */
VecResult vector_base64_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("BASE64_ENCODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_base64_encode_impl(args[0]);
    });
}

/**
 * BASE64_DECODE(value): base64 decode
 */
VecResult vector_base64_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("BASE64_DECODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_base64_decode_impl(args[0]);
    });
}

/**
 * HEX_ENCODE(value): hex encode
 */
VecResult vector_hex_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("HEX_ENCODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_hex_encode_impl(args[0]);
    });
}

/**
 * HEX_DECODE(value): hex decode
 */
VecResult vector_hex_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("HEX_DECODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_hex_decode_impl(args[0]);
    });
}

/**
 * BASE85_ENCODE(value): base85 encode
 */
VecResult vector_base85_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("BASE85_ENCODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_base85_encode_impl(args[0]);
    });
}

/**
 * BASE85_DECODE(value): base85 decode
 */
VecResult vector_base85_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("BASE85_DECODE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_base85_decode_impl(args[0]);
    });
}

}  // extern "C"
