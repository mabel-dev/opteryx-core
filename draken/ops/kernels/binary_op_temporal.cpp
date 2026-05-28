#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"

/**
 * Temporal binary operations for Phase 9a.
 *
 * Handles:
 * - DATE/TIMESTAMP ± INTERVAL
 * - DATE - DATE → INTERVAL
 * - INTERVAL ± INTERVAL
 *
 * Note: Full implementation deferred. This phase provides error stubs
 * until Draken temporal arithmetic is exposed as C functions.
 */

extern "C" {

// Phase 9a: Temporal operations are stubbed; implementations deferred to 9f
// These functions return "not implemented" errors for now.

/**
 * Temporal operation dispatcher: DATE/TIMESTAMP ± INTERVAL
 * Phase 9a stub: not yet implemented.
 */
VecResult draken_temporal_interval_op(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Temporal interval operations not yet implemented");
    });
}

/**
 * Date - Date: compute interval between dates.
 * Phase 9a stub: not yet implemented.
 */
VecResult draken_date_minus_date(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Date difference operations not yet implemented");
    });
}

/**
 * Interval ± Interval: add or subtract intervals.
 * Phase 9a stub: not yet implemented.
 */
VecResult draken_interval_interval_op(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel("Interval arithmetic operations not yet implemented");
    });
}

}  // extern "C"
