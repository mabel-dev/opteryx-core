/**
 * Kernel Registry Implementation — lookup and allocation for C kernels.
 * Phase 9b: Central registry for bytecode builder to resolve C kernels at bind time.
 *
 * Maintains a map from kernel name (string) to function pointer + context allocator.
 * Built at module load time via forward declarations + explicit registration.
 */

#include "ops/kernels/kernel_registry.h"
#include "ops/kernels/error_handling.h"
#include "ops/json_path.h"   // dotpath_to_jsonptr / tokenize_jsonptr — bind-time path resolution

// The C-ABI token struct and the C++ one it is copied from must agree byte for
// byte: kernel_alloc_extraction_ctx memcpy's a std::vector<JsonPtrToken> straight
// into the ctx tail, which the kernel then reads back as json_ptr_token.
static_assert(sizeof(json_ptr_token) == sizeof(draken::ops::JsonPtrToken),
              "json_ptr_token must match draken::ops::JsonPtrToken");
static_assert(offsetof(json_ptr_token, off) == offsetof(draken::ops::JsonPtrToken, off) &&
              offsetof(json_ptr_token, len) == offsetof(draken::ops::JsonPtrToken, len) &&
              offsetof(json_ptr_token, index) == offsetof(draken::ops::JsonPtrToken, index),
              "json_ptr_token field order must match draken::ops::JsonPtrToken");
static_assert(JSON_PTR_NOT_INDEX == draken::ops::kJsonPtrNotIndex,
              "JSON_PTR_NOT_INDEX must match draken::ops::kJsonPtrNotIndex");
#include <cstring>
#include <cstdlib>
#include <map>
#include <functional>
#include <string>

// ---------------------------------------------------------------------------
// Forward declarations of C kernel functions (Phase 9a)
// ---------------------------------------------------------------------------
//
// Phase 9a implements:
// - Cast kernels (cast_numeric, cast_string, cast_temporal, cast_dispatch)
// - Binary operation kernels (binary_op_arithmetic, binary_op_other, binary_op_temporal)
// - Extraction kernels (placeholder)
//
// Function kernels (Phase 8a-8i) are deferred to Phase 9a-fn; they require
// nanobind wrapper functions to be ported to extern "C" signatures.

#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/binop_kernels.h"   // P9.1: unified draken_binop (canonical)
#include "ops/kernels/extraction_kernels.h"

// Phase 9a-fn: scalar function kernels (function_kernels.cpp), func_fn_t shape:
//   VecResult fn(void* ctx, const DrakenVector* const* args, uint32_t nargs)
extern "C" {
VecResult draken_length(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_upper(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_lower(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_abs(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sign(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sqrt(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_round(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_floor(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ceiling(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_date_part(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_if_then_else(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_like(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// function_kernels.cpp — length-adaptive LIKE: walks a plan-time LIKE-DFA blob
// on short-string columns, the glob matcher on long ones (both verified equal).
VecResult draken_like_adaptive(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// function_kernels.cpp — SIMD op-program LIKE: walks a plan-time version-2
// op-program blob (compile_like_program) with SIMD substring scans. Replaces
// the transition-table path for decomposable globs; non-decomposable globs
// stay on draken_like_adaptive.
VecResult draken_like_program(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// function_rlike.cpp — pattern operand is always a pre-compiled DFA blob
// (opteryx.compiled.vector_ops.compile_rlike_dfa, plan-time only); this
// kernel has zero RE2 dependency and is a genuine draken-native kernel.
VecResult draken_rlike(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// function_like_any.cpp — LIKE ANY / ILIKE ANY (+ NOT). Pattern set is a
// pre-compiled matcher blob (opteryx.compiled.vector_ops.compile_like_any) in
// ctx; scalar-string or ARRAY<string> subject. Zero RE2, zero Python.
VecResult draken_like_any(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_in_list(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_is_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_is_not_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_numeric_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_temporal_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Column-valued SUBSTRING/LEFT/RIGHT (function_kernels.cpp) — the vector-operand
// sibling of draken_substring above; start/count are read per-row from DrakenVector
// args instead of a bind-time ctx. Three thin entry points sharing one impl, keyed
// by mode (see the file's comment above fk_substring_dynamic_impl).
VecResult draken_substring_dynamic(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_left_dynamic(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_right_dynamic(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_starts_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ends_with(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_date_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_bitwise_not(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: TRIM/LTRIM/RTRIM (string_trim.cpp)
VecResult draken_trim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ltrim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_rtrim(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: REVERSE/INITCAP (string_reverse_initcap.cpp)
VecResult draken_reverse(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_initcap(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: LPAD/RPAD (string_pad.cpp)
VecResult draken_lpad(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_rpad(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: REPLACE/SOUNDEX (string_replace_soundex.cpp)
VecResult draken_replace(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_soundex(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: HUMANIZE (string_humanize.cpp)
VecResult draken_humanize(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: MD5/SHA-1/SHA-224/SHA-256/SHA-384/SHA-512 (function_hash_encoding.cpp)
VecResult draken_hash(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_md5(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sha1(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sha224(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sha256(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sha384(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sha512(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: HEX/BASE64/BASE85 ENCODE/DECODE (function_codec.cpp)
VecResult draken_hex_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_hex_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_base64_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_base64_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_base85_encode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_base85_decode(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: FROM_UNIXTIME (function_temporal.cpp)
VecResult draken_from_unixtime(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: POWER/LOG/TRUNC/RANDOM/NORMAL (function_numeric.cpp). RANDOM/NORMAL are
// nullary in SQL; the VM's arity-0 C-native arm hands them a synthetic length-only
// operand carrying num_rows (the func_fn_t ABI has no other row-count channel).
VecResult draken_power(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_log(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_random(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_normal(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: OCTET_LENGTH/POSITION/LEVENSHTEIN/TO_ASCII + TO_CHAR/RANDOM_STRING
// (function_string_extra.cpp). CONCAT/CONCAT_WS/REGEXP_REPLACE/MATCH are absent by
// design — see that file's header. SPLIT returns an ARRAY, so it lives with the
// ARRAY kernels below (function_array_json.cpp), not here.
VecResult draken_octet_length(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_position(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_levenshtein(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_to_ascii(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_to_char(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_random_string(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: COALESCE/IFNULL/IFNOTNULL/IIF (function_null_conditional.cpp).
// NULLIF is absent by design — the logical planner lowers it to IIF(a = b, NULL, a)
// before binding, so a draken_nullif entry would be unreachable. (GREATEST/LEAST are
// ARRAY reducers and live with the ARRAY kernels below, function_array_json.cpp.)
VecResult draken_coalesce(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ifnull(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ifnotnull(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_iif(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// ARRAY & JSON kernels (function_array_json.cpp)
VecResult draken_jsonb_object_keys(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_json_path_exists(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_length_array(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sort(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_array_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_array_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_greatest(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_least(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_split(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_generate_series(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_array_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: DATEDIFF/TIMEDIFF/FORMAT_TIMESTAMP/UNIXTIME/TIME_BUCKET (function_temporal.cpp)
VecResult draken_datediff(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_timediff(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_date_format(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_unixtime(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_time_bucket(void* ctx, const DrakenVector* const* args, uint32_t nargs);
}

// ---------------------------------------------------------------------------
// Kernel Registry Table (Phase 9a)
// ---------------------------------------------------------------------------
//
// Phase 9a: Cast and binary op kernels.
// Function kernels (Phase 8a-8i) deferred to Phase 9a-fn.
//
// Key naming scheme:
// - Cast kernels: Directly use function name (e.g., "draken_cast_int64_to_float64")
// - Binary op kernels: Use function name (e.g., "draken_add", "draken_bitwise_or", "draken_binary_arith")
// - Extraction kernels: Use function name (e.g., "draken_map_access_string")
//
// At bind time (Phase 9b), resolvers will construct these keys from operation types
// and operand types, then look them up in this registry.
//
static std::map<std::string, kernel_fn_t> _kernel_registry = {
    // ========================================================================
    // Per-overload kernel names (draken_{overload_id})
    // ------------------------------------------------------------------------
    // The binder probes draken_{overload_id} first and only falls back to the
    // bare draken_{name} for a function with exactly ONE overload, where the
    // bare name cannot be ambiguous. A function with MORE than one overload
    // must name its kernel per overload HERE — otherwise the binder refuses to
    // bind it C-native rather than guessing.
    //
    // That rule exists because the old unconditional bare-name fallback bound
    // LENGTH(array) to the string-only draken_length, which then failed at
    // RUNTIME ("string input required"). Silently binding a kernel that refuses
    // the overload's own operand types is exactly the hidden behaviour §1
    // forbids; the fix is to make the choice explicit, not smarter.
    //
    // The entries below are the multi-overload functions whose overloads
    // GENUINELY share one kernel (arity variants, and TRUNC's type variants —
    // each kernel already dispatches internally). They are aliases, not new
    // kernels. LENGTH is the one function whose overloads truly differ.
    // ========================================================================
    {"draken_length_string", (kernel_fn_t)&draken_length},
    {"draken_length_array", (kernel_fn_t)&draken_length_array},
    {"draken_round_1", (kernel_fn_t)&draken_round},
    {"draken_round_2", (kernel_fn_t)&draken_round},
    {"draken_split_2", (kernel_fn_t)&draken_split},
    {"draken_split_3", (kernel_fn_t)&draken_split},
    {"draken_generate_series_1", (kernel_fn_t)&draken_generate_series},
    {"draken_generate_series_2", (kernel_fn_t)&draken_generate_series},
    {"draken_generate_series_3", (kernel_fn_t)&draken_generate_series},
    {"draken_substring_2", (kernel_fn_t)&draken_substring},
    {"draken_substring_3", (kernel_fn_t)&draken_substring},
    {"draken_random_default", (kernel_fn_t)&draken_random},
    {"draken_random_0", (kernel_fn_t)&draken_random},
    {"draken_normal_default", (kernel_fn_t)&draken_normal},
    {"draken_normal_0", (kernel_fn_t)&draken_normal},
    {"draken_trunc_numeric", (kernel_fn_t)&draken_trunc},
    {"draken_trunc_date", (kernel_fn_t)&draken_trunc},
    {"draken_trunc_timestamp", (kernel_fn_t)&draken_trunc},

    // ========================================================================
    // Scalar function kernels (Phase 9a-fn, function_kernels.cpp)
    // ========================================================================
    {"draken_length", (kernel_fn_t)&draken_length},
    {"draken_upper", (kernel_fn_t)&draken_upper},
    {"draken_lower", (kernel_fn_t)&draken_lower},
    {"draken_abs", (kernel_fn_t)&draken_abs},
    {"draken_sign", (kernel_fn_t)&draken_sign},
    {"draken_sqrt", (kernel_fn_t)&draken_sqrt},
    {"draken_round", (kernel_fn_t)&draken_round},
    {"draken_floor", (kernel_fn_t)&draken_floor},
    {"draken_ceiling", (kernel_fn_t)&draken_ceiling},
    {"draken_date_part", (kernel_fn_t)&draken_date_part},
    {"draken_if_then_else", (kernel_fn_t)&draken_if_then_else},
    {"draken_like", (kernel_fn_t)&draken_like},
    {"draken_like_adaptive", (kernel_fn_t)&draken_like_adaptive},
    {"draken_like_program", (kernel_fn_t)&draken_like_program},
    {"draken_rlike", (kernel_fn_t)&draken_rlike},
    {"draken_like_any", (kernel_fn_t)&draken_like_any},
    {"draken_in_list", (kernel_fn_t)&draken_in_list},
    {"draken_is_empty", (kernel_fn_t)&draken_is_empty},
    {"draken_is_not_empty", (kernel_fn_t)&draken_is_not_empty},
    {"draken_numeric_cmp", (kernel_fn_t)&draken_numeric_cmp},
    {"draken_temporal_cmp", (kernel_fn_t)&draken_temporal_cmp},
    {"draken_substring", (kernel_fn_t)&draken_substring},
    {"draken_substring_dynamic", (kernel_fn_t)&draken_substring_dynamic},
    {"draken_left_dynamic", (kernel_fn_t)&draken_left_dynamic},
    {"draken_right_dynamic", (kernel_fn_t)&draken_right_dynamic},
    {"draken_contains", (kernel_fn_t)&draken_contains},
    {"draken_starts_with", (kernel_fn_t)&draken_starts_with},
    {"draken_ends_with", (kernel_fn_t)&draken_ends_with},
    {"draken_date_trunc", (kernel_fn_t)&draken_date_trunc},
    {"draken_bitwise_not", (kernel_fn_t)&draken_bitwise_not},
    {"draken_trim", (kernel_fn_t)&draken_trim},
    {"draken_ltrim", (kernel_fn_t)&draken_ltrim},
    {"draken_rtrim", (kernel_fn_t)&draken_rtrim},
    {"draken_reverse", (kernel_fn_t)&draken_reverse},
    {"draken_initcap", (kernel_fn_t)&draken_initcap},
    {"draken_lpad", (kernel_fn_t)&draken_lpad},
    {"draken_rpad", (kernel_fn_t)&draken_rpad},
    {"draken_replace", (kernel_fn_t)&draken_replace},
    {"draken_soundex", (kernel_fn_t)&draken_soundex},
    {"draken_humanize", (kernel_fn_t)&draken_humanize},

    // Cryptographic digests (function_hash_encoding.cpp)
    {"draken_hash", (kernel_fn_t)&draken_hash},
    {"draken_md5", (kernel_fn_t)&draken_md5},
    {"draken_sha1", (kernel_fn_t)&draken_sha1},
    {"draken_sha224", (kernel_fn_t)&draken_sha224},
    {"draken_sha256", (kernel_fn_t)&draken_sha256},
    {"draken_sha384", (kernel_fn_t)&draken_sha384},
    {"draken_sha512", (kernel_fn_t)&draken_sha512},

    // HEX/BASE64/BASE85 encode+decode (function_codec.cpp)
    {"draken_hex_encode", (kernel_fn_t)&draken_hex_encode},
    {"draken_hex_decode", (kernel_fn_t)&draken_hex_decode},
    {"draken_base64_encode", (kernel_fn_t)&draken_base64_encode},
    {"draken_base64_decode", (kernel_fn_t)&draken_base64_decode},
    {"draken_base85_encode", (kernel_fn_t)&draken_base85_encode},
    {"draken_base85_decode", (kernel_fn_t)&draken_base85_decode},

    // ========================================================================
    // Cast kernels (43 total)
    // ========================================================================

    // Numeric type-specific casts (cast_numeric.cpp)
    {"draken_cast_int64_to_float64", (kernel_fn_t)&draken_cast_int64_to_float64},
    {"draken_cast_int64_to_string", (kernel_fn_t)&draken_cast_int64_to_string},
    {"draken_cast_int64_to_bool", (kernel_fn_t)&draken_cast_int64_to_bool},
    {"draken_cast_bool_to_float64", (kernel_fn_t)&draken_cast_bool_to_float64},
    {"draken_cast_bool_to_string", (kernel_fn_t)&draken_cast_bool_to_string},
    {"draken_cast_bool_to_int64", (kernel_fn_t)&draken_cast_bool_to_int64},
    {"draken_cast_float64_to_int64", (kernel_fn_t)&draken_cast_float64_to_int64},
    {"draken_cast_float64_to_string", (kernel_fn_t)&draken_cast_float64_to_string},
    {"draken_cast_float64_to_bool", (kernel_fn_t)&draken_cast_float64_to_bool},
    {"draken_cast_decimal_to_string", (kernel_fn_t)&draken_cast_decimal_to_string},
    {"draken_cast_decimal128_to_string", (kernel_fn_t)&draken_cast_decimal128_to_string},

    // → VARBINARY (BLOB) retag wrappers — same bytes as the _to_string twin,
    // result retagged VARBINARY (see cast_kernels.h doc comment).
    {"draken_cast_int64_to_blob", (kernel_fn_t)&draken_cast_int64_to_blob},
    {"draken_cast_integer_to_blob", (kernel_fn_t)&draken_cast_integer_to_blob},
    {"draken_cast_float64_to_blob", (kernel_fn_t)&draken_cast_float64_to_blob},
    {"draken_cast_bool_to_blob", (kernel_fn_t)&draken_cast_bool_to_blob},
    {"draken_cast_decimal_to_blob", (kernel_fn_t)&draken_cast_decimal_to_blob},
    {"draken_cast_decimal128_to_blob", (kernel_fn_t)&draken_cast_decimal128_to_blob},

    // Narrow-integer widening (INT32/INT16/INT8 → FLOAT64 / INT64) + direct → string
    {"draken_cast_integer_to_float64", (kernel_fn_t)&draken_cast_integer_to_float64},
    {"draken_cast_integer_to_int64", (kernel_fn_t)&draken_cast_integer_to_int64},
    {"draken_cast_integer_to_string", (kernel_fn_t)&draken_cast_integer_to_string},

    // Unsigned source (UINT8/16/32/64) → VARCHAR / VARBINARY.
    {"draken_cast_uint_to_string", (kernel_fn_t)&draken_cast_uint_to_string},
    {"draken_cast_uint_to_blob", (kernel_fn_t)&draken_cast_uint_to_blob},

    // E33 — any signed integer source (INT8/16/32/64) → the named unsigned
    // target, range-checked (fail loud on negative/out-of-range, never wraps).
    {"draken_cast_integer_to_uint8", (kernel_fn_t)&draken_cast_integer_to_uint8},
    {"draken_cast_integer_to_uint16", (kernel_fn_t)&draken_cast_integer_to_uint16},
    {"draken_cast_integer_to_uint32", (kernel_fn_t)&draken_cast_integer_to_uint32},
    {"draken_cast_integer_to_uint64", (kernel_fn_t)&draken_cast_integer_to_uint64},
    {"draken_cast_uint_to_int64", (kernel_fn_t)&draken_cast_uint_to_int64},
    {"draken_cast_uint_to_uint8", (kernel_fn_t)&draken_cast_uint_to_uint8},
    {"draken_cast_uint_to_uint16", (kernel_fn_t)&draken_cast_uint_to_uint16},
    {"draken_cast_uint_to_uint32", (kernel_fn_t)&draken_cast_uint_to_uint32},
    {"draken_cast_uint_to_uint64", (kernel_fn_t)&draken_cast_uint_to_uint64},
    {"draken_cast_uint_to_float64", (kernel_fn_t)&draken_cast_uint_to_float64},

    // E33 — FLOAT64/FLOAT32 → the named unsigned target, range-checked.
    {"draken_cast_float_to_uint8", (kernel_fn_t)&draken_cast_float_to_uint8},
    {"draken_cast_float_to_uint16", (kernel_fn_t)&draken_cast_float_to_uint16},
    {"draken_cast_float_to_uint32", (kernel_fn_t)&draken_cast_float_to_uint32},
    {"draken_cast_float_to_uint64", (kernel_fn_t)&draken_cast_float_to_uint64},

    // E33 — BOOL → the named unsigned target (always 0/1, no range check).
    {"draken_cast_bool_to_uint8", (kernel_fn_t)&draken_cast_bool_to_uint8},
    {"draken_cast_bool_to_uint16", (kernel_fn_t)&draken_cast_bool_to_uint16},
    {"draken_cast_bool_to_uint32", (kernel_fn_t)&draken_cast_bool_to_uint32},
    {"draken_cast_bool_to_uint64", (kernel_fn_t)&draken_cast_bool_to_uint64},

    // String type-specific casts (cast_string.cpp)
    {"draken_cast_string_to_int64", (kernel_fn_t)&draken_cast_string_to_int64},
    {"draken_cast_string_to_bool", (kernel_fn_t)&draken_cast_string_to_bool},
    {"draken_cast_string_to_date32", (kernel_fn_t)&draken_cast_string_to_date32},
    {"draken_cast_string_to_ipv4", (kernel_fn_t)&draken_cast_string_to_ipv4},
    // Reverse direction. Only ever selected when the bound SOURCE ColumnType
    // carries LogicalKind::IPV4 — a descriptor-less UINT32 goes to
    // draken_cast_uint_to_string instead (both are physically UINT32).
    {"draken_cast_ipv4_to_string", (kernel_fn_t)&draken_cast_ipv4_to_string},
    {"draken_cast_ipv4_to_blob", (kernel_fn_t)&draken_cast_ipv4_to_blob},
    {"draken_ipv4_in_cidr", (kernel_fn_t)&draken_ipv4_in_cidr},
    {"draken_ip_trunc", (kernel_fn_t)&draken_ip_trunc},
    {"draken_cast_string_to_float64", (kernel_fn_t)&draken_cast_string_to_float64},

    // String-family retag (byte-identical copy, new type tag only) — see the
    // doc comment on string_retag_core in cast_string.cpp.
    {"draken_cast_string_to_varchar", (kernel_fn_t)&draken_cast_string_to_varchar},
    {"draken_cast_string_to_blob", (kernel_fn_t)&draken_cast_string_to_blob},
    {"draken_cast_string_to_nvarchar", (kernel_fn_t)&draken_cast_string_to_nvarchar},

    // E33 — VARCHAR/NVARCHAR/VARBINARY → the named unsigned target (parse +
    // range-check via composition with draken_cast_string_to_int64).
    {"draken_cast_string_to_uint8", (kernel_fn_t)&draken_cast_string_to_uint8},
    {"draken_cast_string_to_uint16", (kernel_fn_t)&draken_cast_string_to_uint16},
    {"draken_cast_string_to_uint32", (kernel_fn_t)&draken_cast_string_to_uint32},
    {"draken_cast_string_to_uint64", (kernel_fn_t)&draken_cast_string_to_uint64},

    // Temporal type-specific casts (cast_temporal.cpp)
    // DATE32 → VARCHAR ("YYYY-MM-DD"); registered under its real name (the header's
    // draken_cast_date32_to_string is an alias that is not separately defined).
    {"draken_cast_date_to_string", (kernel_fn_t)&draken_cast_date_to_string},
    {"draken_cast_int64_to_timestamp", (kernel_fn_t)&draken_cast_int64_to_timestamp},
    {"draken_cast_date32_to_timestamp", (kernel_fn_t)&draken_cast_date32_to_timestamp},
    {"draken_cast_timestamp_rescale", (kernel_fn_t)&draken_cast_timestamp_rescale},
    {"draken_cast_float_to_decimal", (kernel_fn_t)&draken_cast_float_to_decimal},
    // DECIMAL → DECIMAL rescale, keyed on the SOURCE tier; the target tier rides in
    // the ctx precision. Both scales come from the ctx (the vector carries neither).
    {"draken_cast_decimal_to_decimal", (kernel_fn_t)&draken_cast_decimal_to_decimal},
    {"draken_cast_decimal128_to_decimal", (kernel_fn_t)&draken_cast_decimal128_to_decimal},
    // INTEGER → DECIMAL, keyed on the source width; target tier from the ctx precision.
    {"draken_cast_int64_to_decimal", (kernel_fn_t)&draken_cast_int64_to_decimal},
    {"draken_cast_integer_to_decimal", (kernel_fn_t)&draken_cast_integer_to_decimal},
    {"draken_cast_uint_to_decimal", (kernel_fn_t)&draken_cast_uint_to_decimal},
    // BOOL / STRING → DECIMAL — the two remaining holes in the → DECIMAL column.
    // Same ctx (target precision/scale) as the integer arms above.
    {"draken_cast_bool_to_decimal", (kernel_fn_t)&draken_cast_bool_to_decimal},
    {"draken_cast_string_to_decimal", (kernel_fn_t)&draken_cast_string_to_decimal},
    // DECIMAL → INT64 / FLOAT64, keyed on the source tier; the SOURCE scale comes
    // from the ctx (left_scale) — the vector does not carry it.
    {"draken_cast_decimal_to_int64", (kernel_fn_t)&draken_cast_decimal_to_int64},
    {"draken_cast_decimal128_to_int64", (kernel_fn_t)&draken_cast_decimal128_to_int64},
    {"draken_cast_decimal_to_float64", (kernel_fn_t)&draken_cast_decimal_to_float64},
    {"draken_cast_decimal128_to_float64", (kernel_fn_t)&draken_cast_decimal128_to_float64},
    {"draken_cast_decimal_to_uint8", (kernel_fn_t)&draken_cast_decimal_to_uint8},
    {"draken_cast_decimal128_to_uint8", (kernel_fn_t)&draken_cast_decimal128_to_uint8},
    {"draken_cast_decimal_to_uint16", (kernel_fn_t)&draken_cast_decimal_to_uint16},
    {"draken_cast_decimal128_to_uint16", (kernel_fn_t)&draken_cast_decimal128_to_uint16},
    {"draken_cast_decimal_to_uint32", (kernel_fn_t)&draken_cast_decimal_to_uint32},
    {"draken_cast_decimal128_to_uint32", (kernel_fn_t)&draken_cast_decimal128_to_uint32},
    {"draken_cast_decimal_to_uint64", (kernel_fn_t)&draken_cast_decimal_to_uint64},
    {"draken_cast_decimal128_to_uint64", (kernel_fn_t)&draken_cast_decimal128_to_uint64},

    // Narrow signed (INT8/16/32) and FLOAT32 targets.
    {"draken_cast_integer_to_int8", (kernel_fn_t)&draken_cast_integer_to_int8},
    {"draken_cast_uint_to_int8", (kernel_fn_t)&draken_cast_uint_to_int8},
    {"draken_cast_float_to_int8", (kernel_fn_t)&draken_cast_float_to_int8},
    {"draken_cast_bool_to_int8", (kernel_fn_t)&draken_cast_bool_to_int8},
    {"draken_cast_string_to_int8", (kernel_fn_t)&draken_cast_string_to_int8},
    {"draken_cast_decimal_to_int8", (kernel_fn_t)&draken_cast_decimal_to_int8},
    {"draken_cast_decimal128_to_int8", (kernel_fn_t)&draken_cast_decimal128_to_int8},
    {"draken_cast_integer_to_int16", (kernel_fn_t)&draken_cast_integer_to_int16},
    {"draken_cast_uint_to_int16", (kernel_fn_t)&draken_cast_uint_to_int16},
    {"draken_cast_float_to_int16", (kernel_fn_t)&draken_cast_float_to_int16},
    {"draken_cast_bool_to_int16", (kernel_fn_t)&draken_cast_bool_to_int16},
    {"draken_cast_string_to_int16", (kernel_fn_t)&draken_cast_string_to_int16},
    {"draken_cast_decimal_to_int16", (kernel_fn_t)&draken_cast_decimal_to_int16},
    {"draken_cast_decimal128_to_int16", (kernel_fn_t)&draken_cast_decimal128_to_int16},
    {"draken_cast_integer_to_int32", (kernel_fn_t)&draken_cast_integer_to_int32},
    {"draken_cast_uint_to_int32", (kernel_fn_t)&draken_cast_uint_to_int32},
    {"draken_cast_integer_to_date32", (kernel_fn_t)&draken_cast_integer_to_date32},
    {"draken_cast_uint_to_date32", (kernel_fn_t)&draken_cast_uint_to_date32},
    {"draken_cast_float_to_int32", (kernel_fn_t)&draken_cast_float_to_int32},
    {"draken_cast_bool_to_int32", (kernel_fn_t)&draken_cast_bool_to_int32},
    {"draken_cast_string_to_int32", (kernel_fn_t)&draken_cast_string_to_int32},
    {"draken_cast_decimal_to_int32", (kernel_fn_t)&draken_cast_decimal_to_int32},
    {"draken_cast_decimal128_to_int32", (kernel_fn_t)&draken_cast_decimal128_to_int32},
    {"draken_cast_float_to_float64", (kernel_fn_t)&draken_cast_float_to_float64},
    {"draken_cast_integer_to_float32", (kernel_fn_t)&draken_cast_integer_to_float32},
    {"draken_cast_uint_to_float32", (kernel_fn_t)&draken_cast_uint_to_float32},
    {"draken_cast_float_to_float32", (kernel_fn_t)&draken_cast_float_to_float32},
    {"draken_cast_bool_to_float32", (kernel_fn_t)&draken_cast_bool_to_float32},
    {"draken_cast_string_to_float32", (kernel_fn_t)&draken_cast_string_to_float32},
    {"draken_cast_decimal_to_float32", (kernel_fn_t)&draken_cast_decimal_to_float32},
    {"draken_cast_decimal128_to_float32", (kernel_fn_t)&draken_cast_decimal128_to_float32},
    {"draken_cast_date32_to_int64", (kernel_fn_t)&draken_cast_date32_to_int64},
    {"draken_cast_timestamp_to_int64", (kernel_fn_t)&draken_cast_timestamp_to_int64},
    {"draken_cast_timestamp_to_string", (kernel_fn_t)&draken_cast_timestamp_to_string},
    {"draken_cast_string_to_time64", (kernel_fn_t)&draken_cast_string_to_time64},
    {"draken_cast_time_to_string", (kernel_fn_t)&draken_cast_time_to_string},
    // VARCHAR -> TIMESTAMP64 (default ISO-8601 or CAST ... FORMAT).
    {"draken_cast_string_to_timestamp", (kernel_fn_t)&draken_cast_string_to_timestamp},
    // INTERVAL -> VARCHAR (default ISO-8601 duration or CAST ... FORMAT).
    {"draken_cast_interval_to_string", (kernel_fn_t)&draken_cast_interval_to_string},

    // → VARBINARY (BLOB) retag wrappers for DATE32/TIMESTAMP64/INTERVAL sources.
    {"draken_cast_date_to_blob", (kernel_fn_t)&draken_cast_date_to_blob},
    {"draken_cast_timestamp_to_blob", (kernel_fn_t)&draken_cast_timestamp_to_blob},
    {"draken_cast_interval_to_blob", (kernel_fn_t)&draken_cast_interval_to_blob},
    // TIMESTAMP64 -> DATE32 (truncates the time component). Real, nogil.
    {"draken_cast_timestamp_to_date32", (kernel_fn_t)&draken_cast_timestamp_to_date32},

    // Dispatch helpers (any → target type)
    {"draken_cast_to_float64", (kernel_fn_t)&draken_cast_to_float64},
    {"draken_cast_to_int64", (kernel_fn_t)&draken_cast_to_int64},
    {"draken_cast_to_varchar", (kernel_fn_t)&draken_cast_to_varchar},
    // TWO-vector signature (parent + child): dispatched ONLY via the VM's
    // BC_C_NATIVE_CHILD cast path, never through the one-vector cast table.
    {"draken_cast_array_to_varchar", (kernel_fn_t)&draken_cast_array_to_varchar},
    {"draken_cast_to_bool", (kernel_fn_t)&draken_cast_to_bool},
    {"draken_cast_to_date", (kernel_fn_t)&draken_cast_to_date},

    // Parameterized casts
    {"draken_cast_to_decimal", (kernel_fn_t)&draken_cast_to_decimal},
    {"draken_cast_to_array", (kernel_fn_t)&draken_cast_to_array},
    {"draken_cast_to_vector", (kernel_fn_t)&draken_cast_to_vector},
    {"draken_cast_to_varchar_with_length", (kernel_fn_t)&draken_cast_to_varchar_with_length},

    // Identity/passthrough cast
    {"draken_cast_identity", (kernel_fn_t)&draken_cast_identity},

    // ========================================================================
    // Binary operation kernels — REAL only
    // ========================================================================

    // P9.1: unified single-dispatch binop kernel (canonical, architect 2026-06-17).
    // The binder resolves every C-native binop family to this one symbol with
    // op_code (+ decimal scales) in binary_op_ctx. The per-op 9a kernels below are
    // superseded and will be retired once the flip is proven.
    {"draken_binop", (kernel_fn_t)&draken_binop},

    // Arithmetic dispatch and individual operations (binary_op_arithmetic.cpp — real).
    {"draken_binary_arith", (kernel_fn_t)&draken_binary_arith},
    {"draken_add", (kernel_fn_t)&draken_add},
    {"draken_subtract", (kernel_fn_t)&draken_subtract},
    {"draken_multiply", (kernel_fn_t)&draken_multiply},
    {"draken_divide", (kernel_fn_t)&draken_divide},
    {"draken_modulo", (kernel_fn_t)&draken_modulo},

    // P9.0: bitwise (×5), string_concat, IP (binary_op_other.cpp) and temporal
    // (binary_op_temporal.cpp) binary kernels removed — ALL STUBS ("not yet
    // implemented"). The registry holds only real kernels. These were also DEAD:
    // the BC_BINARY_OP path dispatches arithmetic via draken_arithmetic_dv (a separate
    // C entry point) and everything else via the resolve_binary_op closure — nothing
    // looked these names up. Per architect decision (2026-06-16) the binary-op path
    // will later UNIFY onto this registry's kernel_fn ABI, but only as a complete,
    // null-correct replacement (no beside-fallback); these get re-added real then.

    // ========================================================================
    // Extraction kernels (3). All REAL, all dispatched by the nogil VM straight
    // from kernel_fn: draken_json_extract (`->`/`->>`, sub-op in ctx),
    // draken_map_access_string (str[i]) and draken_array_map_access (arr[i]).
    //
    // draken_array_map_access reaches the ARRAY's child vector — which hangs off
    // the VectorOwner, not off DrakenVector — through the ABI's otherwise-unused
    // key slot: the binder flags BC_EXTR_MAP_ARRAY with BC_C_NATIVE_CHILD when
    // the operand lowers to BC_LOAD_COL, and the VM resolves the child per morsel
    // from the column owner. Same plumbing the ARRAY→VARCHAR cast uses.
    //
    // draken_pointer_extract (top-level key via yyjson_obj_getn) was removed
    // 2026-08-05: no sub-op could ever reach it. The dialect's only string-keyed
    // JSON navigation is `->`/`->>`, which draken_json_extract serves, and
    // MapAccess is INTEGER-keyed everywhere in the operator map — subscripting a
    // VARIANT is refused outright as ambiguous (operator_map.py). The nanobind
    // vector_map_access binding still exercises that mode of extract_rows.
    // ========================================================================

    {"draken_map_access_string", (kernel_fn_t)&draken_map_access_string},
    {"draken_array_map_access", (kernel_fn_t)&draken_array_map_access},
    {"draken_json_extract", (kernel_fn_t)&draken_json_extract},

    // ========================================================================
    // Temporal function kernels (Phase 9a-fn, function_temporal.cpp)
    // draken_from_unixtime is REAL and verified (native column path matches the
    // Python impl for DATE/TIMESTAMP/NULL/negative inputs). The rest of the
    // family needs the input operand's TimestampUnit, which DrakenVector does not
    // carry — see the scope note at the top of function_temporal.cpp.
    {"draken_from_unixtime", (kernel_fn_t)&draken_from_unixtime},

    // ========================================================================
    // Numeric function kernels (Phase 9a-fn, function_numeric.cpp)
    // POWER/LOG/TRUNC. DECIMAL on the FIRST operand (base/value/num) works via the
    // bind-time binary_op_ctx compiled_expression.pyx allocates for this name list.
    // DECIMAL on the SECOND operand (exponent/base/digits) fails loud — no scale
    // vehicle exists for it on this call path. See function_numeric.cpp's header.
    //
    // RANDOM/NORMAL are nullary in SQL. The C ABI hands a kernel only operand
    // vectors, so the VM's arity-0 C-native arm synthesizes a length-only operand
    // carrying num_rows (evaluation.pyx); the kernel reads that .length. PCG-backed,
    // thread_local engine per worker (see function_numeric.cpp's fn_thread_rng).
    // ========================================================================
    {"draken_power", (kernel_fn_t)&draken_power},
    {"draken_log", (kernel_fn_t)&draken_log},
    {"draken_trunc", (kernel_fn_t)&draken_trunc},
    {"draken_random", (kernel_fn_t)&draken_random},
    {"draken_normal", (kernel_fn_t)&draken_normal},

    // ========================================================================
    // String function kernels (Phase 9a-fn, function_string_extra.cpp)
    //
    // draken_to_char / draken_random_string produce STRING results (VARCHAR /
    // VARBINARY), dense per-logical-row; the other four produce dense INT64.
    // draken_random_string honours the architect's 2026-07-17 ruling: n random
    // BYTES per row as VARBINARY. It is VOLATILE and excluded from constant
    // folding (constant_folding.py). The rest of the string group is absent
    // DELIBERATELY, not pending:
    //   CONCAT / CONCAT_WS — the optimizer rewrites both to `||` (StringConcat)
    //       chains, which are already native. A kernel here would be shadowed.
    //       (CONCAT_WS's 2-arg form is now rewritten too — predicate_rewriter.py.)
    //   SPLIT          — returns ARRAY; VecResult has no array-ownership contract.
    //   REGEXP_REPLACE — needs RE2, which is not compiled into draken/rugo.
    //   MATCH          — LANDED, but as draken__match_against_2 on the vector/distance
    //                    path (function_vector_distance.cpp), not here: it is embedding
    //                    cosine similarity, not string manipulation.
    // Each is explained in function_string_extra.cpp's header and was raised with
    // the architect rather than guessed at.
    // ========================================================================
    {"draken_octet_length", (kernel_fn_t)&draken_octet_length},
    {"draken_position", (kernel_fn_t)&draken_position},
    {"draken_levenshtein", (kernel_fn_t)&draken_levenshtein},
    {"draken_to_ascii", (kernel_fn_t)&draken_to_ascii},
    {"draken_to_char", (kernel_fn_t)&draken_to_char},
    {"draken_random_string", (kernel_fn_t)&draken_random_string},

    // ========================================================================
    // Null & conditional kernels (function_null_conditional.cpp)
    //
    // Before these entries existed the plan compiler REFUSED every query using
    // COALESCE/IFNULL/IFNOTNULL/IIF ("outside the c-native kernel set") — they had
    // no C kernel, and the nanobind bindings they nominally bound to were
    // unreachable from the engine. Registering them is what admits the functions.
    //
    // Absent by design:
    //   NULLIF          — lowered to IIF(a = b, NULL, a) by the logical planner
    //                     before binding; an entry here would never be dispatched.
    // (GREATEST/LEAST are ARRAY reducers and now live in the ARRAY & JSON block
    //  below, using the BC_C_NATIVE_CHILD plumbing to reach the array child.)
    // ========================================================================
    {"draken_coalesce", (kernel_fn_t)&draken_coalesce},
    {"draken_ifnull", (kernel_fn_t)&draken_ifnull},
    {"draken_ifnotnull", (kernel_fn_t)&draken_ifnotnull},
    {"draken_iif", (kernel_fn_t)&draken_iif},

    // ========================================================================
    // ARRAY & JSON kernels (function_array_json.cpp)
    //
    // JSONB_OBJECT_KEYS is the first kernel to return a DRAKEN_ARRAY: its
    // elements ride out on VecResult::child, which vecresult_to_owner adopts
    // into child_owner. Before that field existed an ARRAY result was not
    // expressible on this ABI at all.
    //
    // SORT and the `@>` / `@>>` containment kernels all READ an ARRAY. They
    // reuse the ARRAY->VARCHAR cast's BC_C_NATIVE_CHILD mechanism, extended to
    // BC_FUNCTION (compiled_expression.pyx / evaluation.pyx): the VM appends the
    // column-resolved child element vector as a SYNTHETIC extra arg, so each still
    // fits the plain func_fn_t(ctx, args[], nargs) shape (nargs==2). That encoding
    // carries exactly ONE column_identity, so all three take their array from a
    // DIRECT column load; a computed array argument is not bind-time eligible and
    // is refused at plan time (this engine has no Python fallback).
    //
    // The containment kernels fit that one-child budget because their needle set is
    // a LITERAL, baked into an in_list_ctx blob at bind time (the same vehicle
    // draken_in_list uses) rather than passed as a second vector operand — so
    // there is no second child to resolve. The blob's kind is inferred from the
    // literal (ARRAY columns are untyped at bind time) and verified against the
    // real element type at run time, failing loud on a mismatch.
    //
    // SORT's order is Draken's own engine order — architect decision 2026-07-16,
    // NOT Python's sorted(): fp_total_lt for floats (NaN highest), str_compare for
    // strings (the engine's own GT/LT comparator), false<true for BOOL.
    //
    // GREATEST/LEAST are unary ARRAY reducers (per-row max/min of an ARRAY column,
    // NOT variadic scalars). They READ their array via the same BC_C_NATIVE_CHILD
    // one-child mechanism as SORT and return an element SCALAR. Their order matches
    // the reducer they replace (make_array_greatest, nanmax/nanmin — NaN smallest),
    // NOT SORT's NaN-highest order — see array_reduce in function_array_json.cpp.
    //
    // SPLIT reads a VARCHAR and RETURNS an ARRAY<VARIANT> (child rides VecResult::
    // child, like JSONB_OBJECT_KEYS); its scalar delimiter/limit are literal args.
    //
    // `item = ANY(arr)` (AnyOpEq). Architect ruling 2026-07-17: make the AnyOp
    // form native (Fix B). The old comments claiming it was "already native via
    // AnyOpEq" were WRONG — the compare admission gate refuses AnyOpEq, so it was
    // unrunnable. Rather than widen that hot gate, the compare arm lowers
    // AnyOpEq-over-an-ARRAY-column to this BC_FUNCTION (the same
    // BC_C_NATIVE_CHILD + WRAP_AS_BOOL path `@>` uses). The item is a
    // one-element in_list_ctx blob; semantics are the reference SQL `= ANY`
    // (three-valued: null row -> NULL), distinct from `@>`'s
    // null-row -> false. A per-row item column or a computed array stays refused
    // at plan time (no fallback).
    // ========================================================================
    {"draken_jsonb_object_keys", (kernel_fn_t)&draken_jsonb_object_keys},
    // `doc @? path` — arity 1, path in an extraction_ctx (the SAME ctx `->` binds,
    // allocated by kernel_alloc_extraction_ctx with BC_EXTR_JSON_PTR), so the two
    // operators cannot disagree about what a path means. Existence, not extraction:
    // a JSON `null` at the path is TRUE here and NULL through `->`.
    {"draken_json_path_exists", (kernel_fn_t)&draken_json_path_exists},
    {"draken_sort", (kernel_fn_t)&draken_sort},
    {"draken_array_contains_any", (kernel_fn_t)&draken_array_contains_any},
    {"draken_array_contains_all", (kernel_fn_t)&draken_array_contains_all},
    {"draken_array_contains", (kernel_fn_t)&draken_array_contains},
    {"draken_greatest", (kernel_fn_t)&draken_greatest},
    {"draken_least", (kernel_fn_t)&draken_least},
    {"draken_split", (kernel_fn_t)&draken_split},
    {"draken_generate_series", (kernel_fn_t)&draken_generate_series},

    // DATEDIFF/TIMEDIFF/FORMAT_TIMESTAMP/UNIXTIME/TIME_BUCKET (function_temporal.cpp)
    {"draken_datediff", (kernel_fn_t)&draken_datediff},
    {"draken_timediff", (kernel_fn_t)&draken_timediff},
    {"draken_date_format", (kernel_fn_t)&draken_date_format},
    {"draken_unixtime", (kernel_fn_t)&draken_unixtime},
    {"draken_time_bucket", (kernel_fn_t)&draken_time_bucket},

    // EMBED / COSINE_SIMILARITY / COSINE_DISTANCE (function_vector_distance.cpp).
    // Keyed by catalog OVERLOAD id, not by function name — see kernel_registry.h.
    {"draken_embed", (kernel_fn_t)&draken_embed},
    {"draken_cosine_similarity_vector", (kernel_fn_t)&draken_cosine_similarity_vector},
    {"draken_cosine_distance_vector", (kernel_fn_t)&draken_cosine_distance_vector},
    {"draken_cosine_similarity_text", (kernel_fn_t)&draken_cosine_similarity_text},
    {"draken_cosine_distance_text", (kernel_fn_t)&draken_cosine_distance_text},
    // MATCH (col) AGAINST (str) — the overload id _MATCH_AGAINST_2 lowercased, hence the
    // doubled underscore. Runs the text cosine body and thresholds it (match_ctx).
    {"draken__match_against_2", (kernel_fn_t)&draken__match_against_2},
    // Two-vector cast (parent+child); cast dispatch casts the fn ptr to its own
    // signature, exactly as draken_cast_array_to_varchar is registered.
    {"draken_cast_array_to_vector", (kernel_fn_t)&draken_cast_array_to_vector},
};

// ---------------------------------------------------------------------------
// Registry Lookup Implementation
// ---------------------------------------------------------------------------

bool kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx) {
    if (!name || !out_fn || !out_ctx) {
        return false;
    }

    auto it = _kernel_registry.find(name);
    if (it == _kernel_registry.end()) {
        return false;
    }

    *out_fn = it->second;
    *out_ctx = nullptr;  // No context for simple kernels; caller can allocate if needed
    return true;
}

// ---------------------------------------------------------------------------
// Context Allocators
// ---------------------------------------------------------------------------

cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit) {
    auto* ctx = static_cast<cast_timestamp_ctx*>(malloc(sizeof(cast_timestamp_ctx)));
    if (ctx) {
        ctx->unit = unit;
    }
    return ctx;
}

cast_array_ctx* kernel_alloc_cast_array_ctx(int element_type, int safe) {
    auto* ctx = static_cast<cast_array_ctx*>(malloc(sizeof(cast_array_ctx)));
    if (ctx) {
        ctx->element_type = element_type;
        ctx->safe = safe;
    }
    return ctx;
}

binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code,
                                          unsigned char left_scale,
                                          unsigned char right_scale,
                                          unsigned char result_scale,
                                          unsigned char result_precision,
                                          unsigned char left_unit,
                                          unsigned char right_unit,
                                          unsigned char safe) {
    auto* ctx = static_cast<binary_op_ctx*>(malloc(sizeof(binary_op_ctx)));
    if (ctx) {
        ctx->op_code = op_code;
        ctx->left_scale = left_scale;
        ctx->right_scale = right_scale;
        ctx->result_scale = result_scale;
        ctx->result_precision = result_precision;
        ctx->left_unit = left_unit;
        ctx->right_unit = right_unit;
        ctx->safe = safe;
    }
    return ctx;
}

extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code, const char* nav,
                                            size_t nav_len, int64_t index) {
    const bool is_json = (sub_op_code == 3 /* BC_EXTR_JSON_PTR */ ||
                          sub_op_code == 4 /* BC_EXTR_JSON_KEY */);
    if (nav == nullptr) nav_len = 0u;

    // JSON sub-ops resolve the ENTIRE path here, once per bind: dot-notation to an
    // RFC 6901 pointer, the pointer split into tokens, ~0/~1 escapes applied, array
    // indices parsed. None of it depends on the document, so the per-morsel kernel
    // call is left with parse + container lookups + emit and no path work at all.
    // The pointer string itself is then dead and is not stored.
    //
    // Non-JSON sub-ops navigate by `index` and keep their key bytes verbatim.
    draken::ops::JsonPtrPath path;
    if (is_json) {
        const std::string converted = draken::ops::dotpath_to_jsonptr(nav, nav_len);
        path    = draken::ops::tokenize_jsonptr(converted.data(), converted.size());
        nav_len = 0u;
    }

    const size_t blob_len   = path.blob.size();
    const size_t ntokens    = path.tokens.size();
    const size_t tokens_off = (sizeof(extraction_ctx) + nav_len + blob_len + 3u) & ~(size_t)3u;
    const size_t total      = tokens_off + ntokens * sizeof(json_ptr_token);

    auto* ctx = static_cast<extraction_ctx*>(malloc(total));
    if (!ctx) return nullptr;
    ctx->sub_op_code = static_cast<int32_t>(sub_op_code);
    ctx->nav_len     = static_cast<int32_t>(nav_len);
    ctx->index       = index;
    ctx->ntokens     = static_cast<uint32_t>(ntokens);
    ctx->blob_len    = static_cast<uint32_t>(blob_len);

    auto* base = reinterpret_cast<unsigned char*>(ctx);
    if (nav_len > 0u)
        memcpy(base + sizeof(extraction_ctx), nav, nav_len);
    if (blob_len > 0u)
        memcpy(base + sizeof(extraction_ctx) + nav_len, path.blob.data(), blob_len);
    if (ntokens > 0u)
        memcpy(base + tokens_off, path.tokens.data(), ntokens * sizeof(json_ptr_token));
    return ctx;
}

in_list_ctx* kernel_alloc_in_list_ctx(const uint8_t* blob, size_t blob_len) {
    // The blob's first bytes ARE the header (built by the binder); one copy.
    if (blob == nullptr || blob_len < sizeof(in_list_ctx)) return nullptr;
    auto* ctx = static_cast<in_list_ctx*>(malloc(blob_len));
    if (ctx) memcpy(ctx, blob, blob_len);
    return ctx;
}

like_dfa_ctx* kernel_alloc_like_dfa_ctx(uint16_t op_code, uint16_t threshold,
                                        const uint8_t* blob, size_t blob_len) {
    auto* ctx = static_cast<like_dfa_ctx*>(malloc(sizeof(like_dfa_ctx) + blob_len));
    if (!ctx) return nullptr;
    ctx->op_code = op_code;
    ctx->threshold = threshold;
    ctx->blob_len = static_cast<uint32_t>(blob_len);
    if (blob_len > 0u && blob != nullptr)
        memcpy(reinterpret_cast<unsigned char*>(ctx) + sizeof(like_dfa_ctx), blob, blob_len);
    return ctx;
}

void* kernel_alloc_like_any_ctx(const uint8_t* blob, size_t blob_len) {
    // [u32 blob_len][blob bytes] — one copy; the kernel reads the length prefix.
    if (blob == nullptr) return nullptr;
    auto* ctx = static_cast<uint8_t*>(malloc(4 + blob_len));
    if (!ctx) return nullptr;
    ctx[0] = static_cast<uint8_t>(blob_len & 0xFF);
    ctx[1] = static_cast<uint8_t>((blob_len >> 8) & 0xFF);
    ctx[2] = static_cast<uint8_t>((blob_len >> 16) & 0xFF);
    ctx[3] = static_cast<uint8_t>((blob_len >> 24) & 0xFF);
    memcpy(ctx + 4, blob, blob_len);
    return ctx;
}

substring_ctx* kernel_alloc_substring_ctx(int32_t start, int32_t count, uint8_t has_count) {
    auto* ctx = static_cast<substring_ctx*>(malloc(sizeof(substring_ctx)));
    if (ctx) { ctx->start = start; ctx->count = count; ctx->has_count = has_count; }
    return ctx;
}

time_bucket_ctx* kernel_alloc_time_bucket_ctx(int64_t magnitude, unsigned char unit_kind,
                                              unsigned char ts_unit) {
    auto* ctx = static_cast<time_bucket_ctx*>(malloc(sizeof(time_bucket_ctx)));
    if (ctx) { ctx->magnitude = magnitude; ctx->unit_kind = unit_kind; ctx->ts_unit = ts_unit; }
    return ctx;
}

format_ctx* kernel_alloc_format_ctx(unsigned char ts_unit, const char* fmt, size_t fmt_len,
                                    unsigned char safe) {
    if (fmt == nullptr) fmt_len = 0u;
    auto* ctx = static_cast<format_ctx*>(malloc(sizeof(format_ctx) + fmt_len));
    if (!ctx) return nullptr;
    ctx->ts_unit = ts_unit;
    ctx->safe = safe;
    ctx->fmt_len = static_cast<int32_t>(fmt_len);
    if (fmt_len > 0u)
        memcpy(reinterpret_cast<unsigned char*>(ctx) + sizeof(format_ctx), fmt, fmt_len);
    return ctx;
}

void kernel_registry_register(const char* name, kernel_fn_t fn) {
    if (name != nullptr && fn != nullptr) _kernel_registry[std::string(name)] = fn;
}

void kernel_free_context(void* ctx) {
    free(ctx);
}
