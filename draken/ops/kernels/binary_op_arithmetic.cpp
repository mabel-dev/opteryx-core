#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include <cstring>
#include <cmath>

/**
 * Binary arithmetic operations for Phase 9a C kernel ABI.
 *
 * These implement element-wise arithmetic on DrakenVectors, returning VecResult.
 * Allocation via draken_malloc (not frame arena).
 *
 * Supported: INT64, FLOAT64, and mixed combinations.
 * Falls back to error for unsupported types (DECIMAL, temporal, etc. deferred to 9f).
 */

extern "C" {

// Op codes (match BCBinaryOpCode in compiled_expression.pxd)
#define OP_PLUS     1
#define OP_MINUS    2
#define OP_MULTIPLY 3
#define OP_DIVIDE   4
#define OP_MODULO   5

/**
 * Arithmetic ADD: left + right
 */
VecResult draken_add(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("Input vectors are null");
        if (left->length != right->length) {
            return draken_error_sentinel_fmt(
                "Vector length mismatch: left=%u, right=%u",
                left->length, right->length);
        }

        // Handle DRAKEN_NULL (all rows null) — result is also all NULL
        if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
            uint32_t n = left->length;
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");
            const uint32_t nbytes = (n + 7) >> 3;
            auto* out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            memset(out_validity, 0, nbytes);

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = draken_identity_sel(left->length);
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        }

        // Dispatch based on types
        if (left->type == DRAKEN_INT64 && right->type == DRAKEN_INT64) {
            // INT64 + INT64 → INT64
            uint32_t n = left->length;
            auto* left_data = static_cast<const int64_t*>(left->data);
            auto* right_data = static_cast<const int64_t*>(right->data);

            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                out_data[i] = left_data[left->selection[i]] +
                              right_data[right->selection[i]];
            }

            // Merge validity bitmaps from inputs: result valid iff both inputs valid
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = draken_identity_sel(n);  // Use global identity selector
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;

            return result;
        } else if ((left->type == DRAKEN_INT64 || left->type == DRAKEN_FLOAT64) &&
                   (right->type == DRAKEN_INT64 || right->type == DRAKEN_FLOAT64)) {
            // Numeric + Numeric → FLOAT64
            uint32_t n = left->length;
            auto* out_data = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                double lval = (left->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(left->data)[left->selection[i]]) :
                    static_cast<const double*>(left->data)[left->selection[i]];
                double rval = (right->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(right->data)[right->selection[i]]) :
                    static_cast<const double*>(right->data)[right->selection[i]];

                out_data[i] = lval + rval;
            }

            // Merge validity bitmaps
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_FLOAT64;
            result.flags = 0;

            return result;
        } else {
            return draken_error_sentinel_fmt(
                "Unsupported types for addition: left=%d, right=%d",
                left->type, right->type);
        }
    });
}

/**
 * Arithmetic SUBTRACT: left - right
 */
VecResult draken_subtract(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("Input vectors are null");
        if (left->length != right->length) {
            return draken_error_sentinel("Vector length mismatch");
        }

        // Handle DRAKEN_NULL (all rows null) — result is also all NULL
        if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
            uint32_t n = left->length;
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");
            const uint32_t nbytes = (n + 7) >> 3;
            auto* out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            memset(out_validity, 0, nbytes);

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        }

        if (left->type == DRAKEN_INT64 && right->type == DRAKEN_INT64) {
            uint32_t n = left->length;
            auto* left_data = static_cast<const int64_t*>(left->data);
            auto* right_data = static_cast<const int64_t*>(right->data);
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                out_data[i] = left_data[left->selection[i]] -
                              right_data[right->selection[i]];
            }

            // Merge validity bitmaps
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        } else {
            // Mixed or FLOAT64 → FLOAT64
            uint32_t n = left->length;
            auto* out_data = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                double lval = (left->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(left->data)[left->selection[i]]) :
                    static_cast<const double*>(left->data)[left->selection[i]];
                double rval = (right->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(right->data)[right->selection[i]]) :
                    static_cast<const double*>(right->data)[right->selection[i]];
                out_data[i] = lval - rval;
            }

            // Merge validity bitmaps
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_FLOAT64;
            result.flags = 0;
            return result;
        }
    });
}

/**
 * Arithmetic MULTIPLY: left * right
 */
VecResult draken_multiply(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("Input vectors are null");
        if (left->length != right->length) {
            return draken_error_sentinel("Vector length mismatch");
        }

        // Handle DRAKEN_NULL (all rows null) — result is also all NULL
        if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
            uint32_t n = left->length;
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");
            const uint32_t nbytes = (n + 7) >> 3;
            auto* out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            memset(out_validity, 0, nbytes);

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        }

        if (left->type == DRAKEN_INT64 && right->type == DRAKEN_INT64) {
            uint32_t n = left->length;
            auto* left_data = static_cast<const int64_t*>(left->data);
            auto* right_data = static_cast<const int64_t*>(right->data);
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                out_data[i] = left_data[left->selection[i]] *
                              right_data[right->selection[i]];
            }

            // Merge validity bitmaps
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        } else {
            uint32_t n = left->length;
            auto* out_data = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!out_data) return draken_error_sentinel("Allocation failed");

            for (uint32_t i = 0; i < n; ++i) {
                double lval = (left->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(left->data)[left->selection[i]]) :
                    static_cast<const double*>(left->data)[left->selection[i]];
                double rval = (right->type == DRAKEN_INT64) ?
                    static_cast<double>(static_cast<const int64_t*>(right->data)[right->selection[i]]) :
                    static_cast<const double*>(right->data)[right->selection[i]];
                out_data[i] = lval * rval;
            }

            // Merge validity bitmaps
            uint8_t* out_validity = nullptr;
            if (left->validity || right->validity) {
                const uint32_t nbytes = (n + 7) >> 3;
                out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
                if (!out_validity) {
                    draken_free(out_data);
                    return draken_error_sentinel("Allocation failed");
                }
                for (uint32_t i = 0; i < nbytes; ++i) {
                    uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                    uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                    out_validity[i] = left_valid & right_valid;
                }
            }

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_FLOAT64;
            result.flags = 0;
            return result;
        }
    });
}

/**
 * Arithmetic DIVIDE: left / right
 * Result is always FLOAT64 (even for INT64 / INT64).
 */
VecResult draken_divide(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("Input vectors are null");
        if (left->length != right->length) {
            return draken_error_sentinel("Vector length mismatch");
        }

        // Handle DRAKEN_NULL (all rows null) — result is also all NULL
        if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
            uint32_t n = left->length;
            auto* out_data = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!out_data) return draken_error_sentinel("Allocation failed");
            const uint32_t nbytes = (n + 7) >> 3;
            auto* out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            memset(out_validity, 0, nbytes);

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_FLOAT64;
            result.flags = 0;
            return result;
        }

        uint32_t n = left->length;
        auto* out_data = static_cast<double*>(draken_malloc(n * sizeof(double)));
        if (!out_data) return draken_error_sentinel("Allocation failed");

        for (uint32_t i = 0; i < n; ++i) {
            double lval = (left->type == DRAKEN_INT64) ?
                static_cast<double>(static_cast<const int64_t*>(left->data)[left->selection[i]]) :
                static_cast<const double*>(left->data)[left->selection[i]];
            double rval = (right->type == DRAKEN_INT64) ?
                static_cast<double>(static_cast<const int64_t*>(right->data)[right->selection[i]]) :
                static_cast<const double*>(right->data)[right->selection[i]];

            if (rval == 0.0) {
                out_data[i] = std::nan("");  // Division by zero → NaN
            } else {
                out_data[i] = lval / rval;
            }
        }

        // Merge validity bitmaps
        uint8_t* out_validity = nullptr;
        if (left->validity || right->validity) {
            const uint32_t nbytes = (n + 7) >> 3;
            out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            for (uint32_t i = 0; i < nbytes; ++i) {
                uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                out_validity[i] = left_valid & right_valid;
            }
        }

        VecResult result;
        result.data = out_data;
        result.validity = out_validity;
        result.selection = left->selection;
        result.owns_selection = false;
        result.data_length = n;
        result.length = n;
        result.type = DRAKEN_FLOAT64;
        result.flags = 0;
        return result;
    });
}

/**
 * Arithmetic MODULO: left % right
 * Only defined for INT64.
 */
VecResult draken_modulo(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!left || !right) return draken_error_sentinel("Input vectors are null");
        if (left->length != right->length) {
            return draken_error_sentinel("Vector length mismatch");
        }

        // Handle DRAKEN_NULL (all rows null) — result is also all NULL
        if (left->type == DRAKEN_NULL || right->type == DRAKEN_NULL) {
            uint32_t n = left->length;
            auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
            if (!out_data) return draken_error_sentinel("Allocation failed");
            const uint32_t nbytes = (n + 7) >> 3;
            auto* out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            memset(out_validity, 0, nbytes);

            VecResult result;
            result.data = out_data;
            result.validity = out_validity;
            result.selection = left->selection;
            result.owns_selection = false;
            result.data_length = n;
            result.length = n;
            result.type = DRAKEN_INT64;
            result.flags = 0;
            return result;
        }

        if (left->type != DRAKEN_INT64 || right->type != DRAKEN_INT64) {
            return draken_error_sentinel("Modulo requires INT64 operands");
        }

        uint32_t n = left->length;
        auto* left_data = static_cast<const int64_t*>(left->data);
        auto* right_data = static_cast<const int64_t*>(right->data);
        auto* out_data = static_cast<int64_t*>(draken_malloc(n * sizeof(int64_t)));
        if (!out_data) return draken_error_sentinel("Allocation failed");

        for (uint32_t i = 0; i < n; ++i) {
            int64_t lval = left_data[left->selection[i]];
            int64_t rval = right_data[right->selection[i]];

            if (rval == 0) {
                // Modulo by zero: could return error or NULL
                // For now, set to NULL (TODO: define error behavior)
                out_data[i] = 0;
            } else {
                out_data[i] = lval % rval;
            }
        }

        // Merge validity bitmaps
        uint8_t* out_validity = nullptr;
        if (left->validity || right->validity) {
            const uint32_t nbytes = (n + 7) >> 3;
            out_validity = static_cast<uint8_t*>(draken_malloc(nbytes));
            if (!out_validity) {
                draken_free(out_data);
                return draken_error_sentinel("Allocation failed");
            }
            for (uint32_t i = 0; i < nbytes; ++i) {
                uint8_t left_valid = left->validity ? left->validity[i] : 0xff;
                uint8_t right_valid = right->validity ? right->validity[i] : 0xff;
                out_validity[i] = left_valid & right_valid;
            }
        }

        VecResult result;
        result.data = out_data;
        result.validity = out_validity;
        result.selection = left->selection;
        result.owns_selection = false;
        result.data_length = n;
        result.length = n;
        result.type = DRAKEN_INT64;
        result.flags = 0;
        return result;
    });
}

/**
 * Binary arithmetic dispatcher: dispatches based on op_code in context.
 * ctx → binary_op_ctx with op_code (OP_PLUS, OP_MINUS, OP_MULTIPLY, OP_DIVIDE, OP_MODULO).
 */
VecResult draken_binary_arith(void* ctx, const DrakenVector* left, const DrakenVector* right) {
    DRAKEN_KERNEL_TRY({
        if (!ctx) return draken_error_sentinel("Context is null for binary_arith");

        auto* ctx_typed = static_cast<const binary_op_ctx*>(ctx);
        switch (ctx_typed->op_code) {
            case OP_PLUS:
                return draken_add(nullptr, left, right);
            case OP_MINUS:
                return draken_subtract(nullptr, left, right);
            case OP_MULTIPLY:
                return draken_multiply(nullptr, left, right);
            case OP_DIVIDE:
                return draken_divide(nullptr, left, right);
            case OP_MODULO:
                return draken_modulo(nullptr, left, right);
            default:
                return draken_error_sentinel_fmt(
                    "Invalid op_code for binary_arith: %d", ctx_typed->op_code);
        }
    });
}

}  // extern "C"
