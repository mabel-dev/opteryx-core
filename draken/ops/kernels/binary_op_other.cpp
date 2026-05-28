#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/error_handling.h"
#include <cstring>

/**
 * Binary bitwise and string concatenation operations for Phase 9a.
 *
 * Bitwise operations work on INTEGER types only.
 * String concatenation coerces both operands to VARCHAR and concatenates.
 */

extern "C" {

// Phase 9a: Bitwise and string operations are stubbed; implementations deferred to 9f

VecResult draken_bitwise_or(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_and(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_xor(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise operations not yet implemented");
    });
}

VecResult draken_bitwise_shift_left(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise shift operations not yet implemented");
    });
}

VecResult draken_bitwise_shift_right(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Bitwise shift operations not yet implemented");
    });
}

VecResult draken_string_concat(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("String concatenation not yet implemented");
    });
}

VecResult draken_ip_in_cidr(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("IP CIDR operations not yet implemented");
    });
}

}  // extern "C"
