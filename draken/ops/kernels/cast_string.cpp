#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * Cast kernel stubs: string conversions.
 * Phase 9a: Stubbed - C++ nanobind implementations not yet exposed as extern "C".
 * Full implementations deferred to Phase 9f.
 */

extern "C" {

// String casts
VecResult draken_cast_string_to_float64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

}  // extern "C"
