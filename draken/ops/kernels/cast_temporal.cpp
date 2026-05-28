#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * Cast kernel stubs: temporal conversions.
 * Phase 9a: Stubbed - C++ nanobind implementations not yet exposed as extern "C".
 * Full implementations deferred to Phase 9f.
 */

extern "C" {

// Temporal casts
VecResult draken_cast_int64_to_timestamp(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_date32_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_date32_to_timestamp(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

VecResult draken_cast_timestamp_to_date32(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Cast operations not yet implemented"); });
}

}  // extern "C"
