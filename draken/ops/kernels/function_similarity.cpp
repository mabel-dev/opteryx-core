#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: similarity and distance functions.
 * Phase 8g of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ similarity/distance function implementations
extern "C" VecResult vector_cosine_similarity_impl(const DrakenVector* vec1, const DrakenVector* vec2);
extern "C" VecResult vector_cosine_distance_impl(const DrakenVector* vec1, const DrakenVector* vec2);
extern "C" VecResult vector_euclidean_distance_impl(const DrakenVector* vec1, const DrakenVector* vec2);
extern "C" VecResult vector_manhattan_distance_impl(const DrakenVector* vec1, const DrakenVector* vec2);
extern "C" VecResult vector_hamming_distance_impl(const DrakenVector* vec1, const DrakenVector* vec2);

/**
 * COSINE_SIMILARITY(vec1, vec2): cosine similarity between vectors
 * Returns value between -1 and 1 (1 = identical direction)
 */
VecResult vector_cosine_similarity(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("COSINE_SIMILARITY expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_cosine_similarity_impl(args[0], args[1]);
    });
}

/**
 * COSINE_DISTANCE(vec1, vec2): cosine distance between vectors
 * Returns value between 0 and 2 (0 = identical direction)
 */
VecResult vector_cosine_distance(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("COSINE_DISTANCE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_cosine_distance_impl(args[0], args[1]);
    });
}

/**
 * EUCLIDEAN_DISTANCE(vec1, vec2): Euclidean distance between vectors
 */
VecResult vector_euclidean_distance(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("EUCLIDEAN_DISTANCE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_euclidean_distance_impl(args[0], args[1]);
    });
}

/**
 * MANHATTAN_DISTANCE(vec1, vec2): Manhattan distance between vectors (L1 norm)
 */
VecResult vector_manhattan_distance(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("MANHATTAN_DISTANCE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_manhattan_distance_impl(args[0], args[1]);
    });
}

/**
 * HAMMING_DISTANCE(str1, str2): Hamming distance between strings
 * Count of positions where characters differ
 */
VecResult vector_hamming_distance(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("HAMMING_DISTANCE expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_hamming_distance_impl(args[0], args[1]);
    });
}

}  // extern "C"
