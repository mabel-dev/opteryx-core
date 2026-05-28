#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: temporal functions.
 * Phase 8c of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ temporal function implementations
extern "C" VecResult vector_unixtime_impl(const DrakenVector* v);
extern "C" VecResult vector_unixtime_unit_impl(const DrakenVector* v, const DrakenVector* unit);
extern "C" VecResult vector_date_trunc_impl(const DrakenVector* v, const DrakenVector* unit);
extern "C" VecResult vector_date_format_impl(const DrakenVector* v, const DrakenVector* format);
extern "C" VecResult vector_date_part_impl(const DrakenVector* v, const DrakenVector* part);
extern "C" VecResult vector_date_diff_impl(const DrakenVector* unit, const DrakenVector* start_date, const DrakenVector* end_date);
extern "C" VecResult vector_time_diff_impl(const DrakenVector* start_time, const DrakenVector* end_time);
extern "C" VecResult vector_floor_temporal_impl(const DrakenVector* v, const DrakenVector* unit);
extern "C" VecResult vector_date32_to_timestamp_impl(const DrakenVector* v);
extern "C" VecResult vector_timestamp_to_date32_impl(const DrakenVector* v);

/**
 * UNIXTIME(datetime) or UNIXTIME(datetime, unit)
 * Converts to Unix timestamp (seconds since epoch by default)
 */
VecResult vector_unixtime(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("UNIXTIME expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("First argument is null");

        if (nargs == 1) {
            return vector_unixtime_impl(args[0]);
        } else {
            return vector_unixtime_unit_impl(args[0], args[1]);
        }
    });
}

/**
 * DATE_TRUNC(datetime, unit): truncate to specified unit
 * unit: 'year', 'month', 'day', 'hour', 'minute', 'second'
 */
VecResult vector_date_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("DATE_TRUNC expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_date_trunc_impl(args[0], args[1]);
    });
}

/**
 * DATE_FORMAT(datetime, format): format temporal as string
 */
VecResult vector_date_format(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("DATE_FORMAT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_date_format_impl(args[0], args[1]);
    });
}

/**
 * DATE_PART(datetime, part): extract date component
 * part: 'year', 'month', 'day', 'hour', 'minute', 'second', 'dow', 'doy', 'quarter'
 */
VecResult vector_date_part(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("DATE_PART expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_date_part_impl(args[0], args[1]);
    });
}

/**
 * DATE_DIFF(unit, start_date, end_date): difference between dates
 * Returns difference in specified unit (year, month, day, hour, minute, second)
 */
VecResult vector_date_diff(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 3) return draken_error_sentinel("DATE_DIFF expects 3 arguments");
        if (!args || !args[0] || !args[1] || !args[2]) return draken_error_sentinel("Arguments are null");
        return vector_date_diff_impl(args[0], args[1], args[2]);
    });
}

/**
 * TIME_DIFF(start_time, end_time): difference between times
 * Returns difference in seconds
 */
VecResult vector_time_diff(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("TIME_DIFF expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_time_diff_impl(args[0], args[1]);
    });
}

/**
 * FLOOR_TEMPORAL(datetime, unit): floor to specified unit
 */
VecResult vector_floor_temporal(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("FLOOR_TEMPORAL expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_floor_temporal_impl(args[0], args[1]);
    });
}

/**
 * DATE32 to TIMESTAMP conversion
 */
VecResult vector_date32_to_timestamp(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("DATE32_TO_TIMESTAMP expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_date32_to_timestamp_impl(args[0]);
    });
}

/**
 * TIMESTAMP to DATE32 conversion
 */
VecResult vector_timestamp_to_date32(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("TIMESTAMP_TO_DATE32 expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_timestamp_to_date32_impl(args[0]);
    });
}

}  // extern "C"
