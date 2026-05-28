#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_EXTRACTION kernel stubs for Phase 9a.
 * Phase 9a: Stubbed - C++ nanobind implementations not yet exposed as extern "C".
 * Full implementations deferred to Phase 9f.
 */

extern "C" {

// Extraction operations
VecResult draken_map_access_string(void* ctx, const DrakenVector* map_vec, const DrakenVector* key_vec) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Extraction operations not yet implemented"); });
}

VecResult draken_array_map_access(void* ctx, const DrakenVector* array_vec, const DrakenVector* index_vec) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Extraction operations not yet implemented"); });
}

VecResult draken_json_extract(void* ctx, const DrakenVector* json_vec, const DrakenVector* path_vec) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Extraction operations not yet implemented"); });
}

VecResult draken_pointer_extract(void* ctx, const DrakenVector* ptr_vec, const DrakenVector* key_vec) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("Extraction operations not yet implemented"); });
}

}  // extern "C"
