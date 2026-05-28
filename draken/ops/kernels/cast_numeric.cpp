#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * Cast kernel stubs: numeric types.
 * Phase 9a: Stubbed - C++ nanobind implementations not yet exposed as extern "C".
 * Full implementations deferred to Phase 9f.
 */

extern "C" {

// Numeric casts
VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_int64_to_bool(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_bool_to_float64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_bool_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_bool_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_float64_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_float64_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_float64_to_bool(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

}  // extern "C"
