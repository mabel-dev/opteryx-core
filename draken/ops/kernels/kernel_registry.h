/**
 * Kernel Registry Header — maps kernel names to C function pointers.
 * Phase 9b: Registry for bytecode builder to resolve C kernels at bind time.
 *
 * This header provides:
 * - Typedef for kernel function signatures
 * - Exported registry function to lookup kernels by name
 * - Context struct types for parameterized kernels
 *
 * The registry is populated by function declarations in individual phase files.
 * Bytecode builder calls kernel_registry_lookup(name, out_fn, out_ctx) to
 * resolve kernel names at bind time.
 */

#ifndef DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_
#define DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_

#include "ops/kernels/c_kernel_abi.h"
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Kernel function signature: all C kernels follow this pattern.
 * ctx: optional context struct (cast_timestamp_ctx, binary_op_ctx, etc.) or NULL
 * args: pointer array of DrakenVector pointers
 * nargs: argument count
 * Returns: VecResult with data/validity/selection/type/length set
 */
typedef VecResult (*kernel_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs);

/**
 * Lookup kernel by name.
 * Returns: true if found, false otherwise
 * out_fn: filled with kernel function pointer
 * out_ctx: filled with context struct pointer (NULL if no context needed)
 *
 * Name format: "FUNCTION_NAME" or "BC_OPERATION_NAME" (uppercase)
 * Examples: "ABS", "LENGTH", "ADD", "MAP_ACCESS_STRING", "CAST_INT64_TO_FLOAT64"
 *
 * Caller owns the returned context struct (if non-NULL) for the lifetime of bytecode execution.
 */
bool kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx);
// Runtime registration for kernels compiled in OTHER modules (e.g. the bespoke
// DFA runner in opteryx's vector_ops) - called once at that module's import.
void kernel_registry_register(const char* name, kernel_fn_t fn);

/**
 * Allocate context struct for parameterized kernels.
 * Used by bytecode builder to create context structs (cast_timestamp_ctx with unit, etc.).
 * Context is stored in the CompiledBytecode._held_refs list for lifetime management.
 *
 * Context structs are defined in kernel_context.h
 */

cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit);
binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code,
                                          unsigned char left_scale,
                                          unsigned char right_scale,
                                          unsigned char result_scale,
                                          unsigned char result_precision,
                                          unsigned char left_unit,
                                          unsigned char right_unit);
// Allocate a BC_EXTRACTION context. `nav` (nav_len bytes, may be null) is the raw
// path/key from the binder; for JSON sub-ops it is converted to an RFC 6901 pointer
// here and the converted form is stored. `index` is the subscript for the MAP_*
// sub-ops. Throws std::invalid_argument on a malformed JSON path.
extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code, const char* nav,
                                            size_t nav_len, int64_t index);

// Allocate context for draken_time_bucket (magnitude/unit_kind/ts_unit — see
// kernel_context.h). Allocate context for draken_date_format (ts_unit + the
// pattern LITERAL's bytes, trailing the struct — see kernel_context.h).
time_bucket_ctx* kernel_alloc_time_bucket_ctx(int64_t magnitude, unsigned char unit_kind,
                                              unsigned char ts_unit);
format_ctx* kernel_alloc_format_ctx(unsigned char ts_unit, const char* fmt, size_t fmt_len);

// Vector/distance kernels (function_vector_distance.cpp). The _vector/_text suffixes are
// catalog OVERLOAD ids: compiled_expression.pyx probes draken_{overload_id} before the
// bare draken_{name}, so COSINE_SIMILARITY's two overloads reach two kernels. The bare
// names are intentionally absent from the registry.
VecResult draken_embed(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_cosine_similarity_vector(void* ctx, const DrakenVector* const* args,
                                          uint32_t nargs);
VecResult draken_cosine_distance_vector(void* ctx, const DrakenVector* const* args,
                                        uint32_t nargs);
VecResult draken_cosine_similarity_text(void* ctx, const DrakenVector* const* args,
                                        uint32_t nargs);
VecResult draken_cosine_distance_text(void* ctx, const DrakenVector* const* args,
                                      uint32_t nargs);
// MATCH (col) AGAINST (str) -> BOOL. `COSINE_SIMILARITY(col, str) >= ctx->threshold`,
// running the text cosine body itself so the two cannot disagree. ctx is a match_ctx.
VecResult draken__match_against_2(void* ctx, const DrakenVector* const* args,
                                  uint32_t nargs);
// CAST(array AS VECTOR(n)). Two-vector (parent offsets + child elements) shape, like
// draken_cast_array_to_varchar — dispatched via BC_C_NATIVE_CHILD. Width via ctx.
VecResult draken_cast_array_to_vector(void* ctx, const DrakenVector* parent,
                                      const DrakenVector* child);
// Allocate context for the fp16 cosine kernels (the operands' VECTOR width).
vector_dim_ctx* kernel_alloc_vector_dim_ctx(uint32_t dimension);
// Allocate context for the TEXT cosine overloads (width + the resolved EMBED kernel
// they delegate both operands to).
cosine_text_ctx* kernel_alloc_cosine_text_ctx(uint32_t dimension, void* embed_fn);

// Allocate context for the length-adaptive LIKE kernel (draken_like_adaptive):
// op mode + avg-length threshold + a plan-time LIKE-DFA blob (copied inline).
like_dfa_ctx* kernel_alloc_like_dfa_ctx(uint16_t op_code, uint16_t threshold,
                                        const uint8_t* blob, size_t blob_len);

// Allocate context for draken_like_any (LIKE ANY / ILIKE ANY). The matcher blob
// (opteryx.compiled.vector_ops.compile_like_any) is copied behind a u32 length
// prefix: [u32 blob_len][blob bytes]. Freed by the generic kernel_free_context.
void* kernel_alloc_like_any_ctx(const uint8_t* blob, size_t blob_len);

// Free allocated context (called during bytecode cleanup)
void kernel_free_context(void* ctx);

#ifdef __cplusplus
}
#endif

#endif  // DRAKEN_OPS_KERNELS_KERNEL_REGISTRY_H_
