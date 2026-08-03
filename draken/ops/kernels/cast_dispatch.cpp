#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"

/**
 * Dispatch helpers for cast operations.
 * These replace the Python row-loop closures in opteryx/expression/casts.pyx.
 *
 * Each dispatcher examines the input vector's type and calls the appropriate
 * native cast kernel.
 */

extern "C" {

// Forward declarations of the cast kernel functions
// (implemented in separate files or existing nanobind)
extern VecResult draken_cast_int64_to_float64(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_bool_to_float64(void* ctx, const DrakenVector* vector);


extern VecResult draken_cast_string_to_int64(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_bool_to_int64(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_date32_to_int64(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_float64_to_int64(void* ctx, const DrakenVector* vector);

extern VecResult draken_cast_int64_to_bool(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_float64_to_bool(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_string_to_bool(void* ctx, const DrakenVector* vector);

extern VecResult draken_cast_int64_to_string(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_integer_to_string(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_bool_to_string(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* vector);
extern VecResult draken_cast_float64_to_string(void* ctx, const DrakenVector* vector);

extern VecResult draken_cast_identity(void* ctx, const DrakenVector* vector);

/**
 * Cast any numeric/string type to FLOAT64.
 * Dispatches to the appropriate cast_*_to_float64 kernel based on input type.
 */
VecResult draken_cast_to_float64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        DrakenType input_type = vector->type;
        switch (input_type) {
            case DRAKEN_FLOAT64:
                // Already FLOAT64
                return draken_cast_identity(ctx, vector);
            case DRAKEN_INT64:
                return draken_cast_int64_to_float64(ctx, vector);
            case DRAKEN_BOOL:
                return draken_cast_bool_to_float64(ctx, vector);
            default:
                return draken_error_sentinel_fmt(
                    "Cannot cast type %d to FLOAT64", input_type);
        }
    });
}

/**
 * Cast any numeric/string type to INT64.
 * Dispatches to the appropriate cast_*_to_int64 kernel based on input type.
 */
VecResult draken_cast_to_int64(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        DrakenType input_type = vector->type;
        switch (input_type) {
            case DRAKEN_INT64:
                // Already INT64
                return draken_cast_identity(ctx, vector);
            case DRAKEN_FLOAT64:
                return draken_cast_float64_to_int64(ctx, vector);
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
                return draken_cast_string_to_int64(ctx, vector);
            case DRAKEN_BOOL:
                return draken_cast_bool_to_int64(ctx, vector);
            case DRAKEN_TIMESTAMP64:
                return draken_cast_timestamp_to_int64(ctx, vector);
            case DRAKEN_DATE32:
                return draken_cast_date32_to_int64(ctx, vector);
            default:
                return draken_error_sentinel_fmt(
                    "Cannot cast type %d to INT64", input_type);
        }
    });
}

/**
 * Cast any numeric/string/array type to VARCHAR.
 * Dispatches based on input type. Handles array → JSON string conversion.
 */
VecResult draken_cast_to_varchar(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        DrakenType input_type = vector->type;
        switch (input_type) {
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
                // Already VARCHAR
                return draken_cast_identity(ctx, vector);
            case DRAKEN_INT64:
                return draken_cast_int64_to_string(ctx, vector);
            case DRAKEN_INT8:
            case DRAKEN_INT16:
            case DRAKEN_INT32:
                return draken_cast_integer_to_string(ctx, vector);
            case DRAKEN_BOOL:
                return draken_cast_bool_to_string(ctx, vector);
            case DRAKEN_FLOAT64:
                return draken_cast_float64_to_string(ctx, vector);
            case DRAKEN_TIMESTAMP64:
                return draken_cast_timestamp_to_string(ctx, vector);
            case DRAKEN_DATE32:
                return draken_cast_date_to_string(ctx, vector);
            case DRAKEN_ARRAY:
                // Array → JSON string (requires special handling)
                // TODO: Call vector_array_from_sequence with JSON-encoded array elements
                return draken_error_sentinel("Array to VARCHAR not yet implemented");
            default:
                return draken_error_sentinel_fmt(
                    "Cannot cast type %d to VARCHAR", input_type);
        }
    });
}

/**
 * Cast any numeric/string type to BOOL.
 * Dispatches based on input type.
 */
VecResult draken_cast_to_bool(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        DrakenType input_type = vector->type;
        switch (input_type) {
            case DRAKEN_BOOL:
                // Already BOOL
                return draken_cast_identity(ctx, vector);
            case DRAKEN_INT64:
                return draken_cast_int64_to_bool(ctx, vector);
            case DRAKEN_FLOAT64:
                return draken_cast_float64_to_bool(ctx, vector);
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
                return draken_cast_string_to_bool(ctx, vector);
            default:
                return draken_error_sentinel_fmt(
                    "Cannot cast type %d to BOOL", input_type);
        }
    });
}

/**
 * Cast any numeric/string/temporal type to DATE32.
 * Dispatches based on input type.
 */
VecResult draken_cast_to_date(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        DrakenType input_type = vector->type;
        switch (input_type) {
            case DRAKEN_DATE32:
                // Already DATE32
                return draken_cast_identity(ctx, vector);
            case DRAKEN_TIMESTAMP64:
                // TODO: Implement draken_cast_timestamp_to_date32
                return draken_error_sentinel("Timestamp to DATE32 not yet wired");
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
                // TODO: Implement string to DATE32 parsing
                return draken_error_sentinel("String to DATE32 not yet implemented");
            case DRAKEN_INT64:
                // TODO: Implement int to DATE32 (Unix days)
                return draken_error_sentinel("Int to DATE32 not yet implemented");
            default:
                return draken_error_sentinel_fmt(
                    "Cannot cast type %d to DATE32", input_type);
        }
    });
}

/**
 * No-op cast: source type == target type.
 * Returns the input vector unchanged (no copy).
 */
VecResult draken_cast_identity(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({
        if (!vector) return draken_error_sentinel("Input vector is null");

        // Return the input vector as-is (caller will wrap in VectorOwner)
        VecResult result;
        result.data = vector->data;
        result.validity = vector->validity;
        result.selection = vector->selection;
        result.owns_selection = false;  // Don't own the selection; input owns it
        result.data_length = vector->data_length;
        result.length = vector->length;
        result.type = vector->type;
        result.flags = 0;

        return result;
    });
}

/**
 * Cast to DECIMAL(precision, scale).
 * ctx → cast_decimal_ctx with precision and scale.
 * (Deferred: Likely requires row-loop implementation in 9f)
 */
VecResult draken_cast_to_decimal(void* ctx, const DrakenVector* vector) {
    return draken_error_sentinel("DECIMAL cast not yet implemented");
}

/* draken_cast_to_array is IMPLEMENTED IN function_array_json.cpp — it needs that
 * TU's yyjson + StringRows staging + finalize_child helpers, the same reason
 * draken_split lives there. Declared in cast_kernels.h alongside the rest. */

/**
 * Cast to VECTOR (FP16 embedding).
 * (Deferred: Requires FP16 quantization)
 */
VecResult draken_cast_to_vector(void* ctx, const DrakenVector* vector) {
    return draken_error_sentinel("VECTOR cast not yet implemented");
}

/**
 * Cast to VARCHAR(length).
 * ctx → cast_varchar_ctx with max_length.
 * (Deferred: Requires length validation)
 */
VecResult draken_cast_to_varchar_with_length(void* ctx, const DrakenVector* vector) {
    return draken_error_sentinel("VARCHAR(length) cast not yet implemented");
}

}  // extern "C"
