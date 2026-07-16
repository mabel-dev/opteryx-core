/**
 * Kernel Registry Implementation — lookup and allocation for C kernels.
 * Phase 9b: Central registry for bytecode builder to resolve C kernels at bind time.
 *
 * Maintains a map from kernel name (string) to function pointer + context allocator.
 * Built at module load time via forward declarations + explicit registration.
 */

#include "ops/kernels/kernel_registry.h"
#include "ops/kernels/error_handling.h"
#include "ops/json_path.h"   // dotpath_to_jsonptr — bind-time path normalization
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
VecResult draken_in_list(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_is_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_is_not_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_numeric_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_temporal_cmp(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs);
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
// Phase 9a-fn: OCTET_LENGTH/POSITION/LEVENSHTEIN/TO_ASCII (function_string_extra.cpp).
// CONCAT/CONCAT_WS/SPLIT/REGEXP_REPLACE/MATCH/RANDOM_STRING are absent by design —
// see that file's header and the report to the architect.
VecResult draken_octet_length(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_position(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_levenshtein(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_to_ascii(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: COALESCE/IFNULL/IFNOTNULL/IIF (function_null_conditional.cpp).
// NULLIF is absent by design — the logical planner lowers it to IIF(a = b, NULL, a)
// before binding, so a draken_nullif entry would be unreachable. GREATEST/LEAST are
// absent because they reduce over an ARRAY, whose child hangs off VectorOwner and is
// unreachable on this signature — see that file's header and the report to the architect.
VecResult draken_coalesce(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ifnull(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_ifnotnull(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_iif(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// ARRAY & JSON kernels (function_array_json.cpp)
VecResult draken_jsonb_object_keys(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_sort(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_array_contains_any(void* ctx, const DrakenVector* const* args, uint32_t nargs);
VecResult draken_array_contains_all(void* ctx, const DrakenVector* const* args, uint32_t nargs);
// Phase 9a-fn: DATEDIFF/TIMEDIFF/DATE_FORMAT/UNIXTIME/TIME_BUCKET (function_temporal.cpp)
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
    {"draken_in_list", (kernel_fn_t)&draken_in_list},
    {"draken_is_empty", (kernel_fn_t)&draken_is_empty},
    {"draken_is_not_empty", (kernel_fn_t)&draken_is_not_empty},
    {"draken_numeric_cmp", (kernel_fn_t)&draken_numeric_cmp},
    {"draken_temporal_cmp", (kernel_fn_t)&draken_temporal_cmp},
    {"draken_substring", (kernel_fn_t)&draken_substring},
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

    // Narrow-integer widening (INT32/INT16/INT8 → FLOAT64 / INT64) + direct → string
    {"draken_cast_integer_to_float64", (kernel_fn_t)&draken_cast_integer_to_float64},
    {"draken_cast_integer_to_int64", (kernel_fn_t)&draken_cast_integer_to_int64},
    {"draken_cast_integer_to_string", (kernel_fn_t)&draken_cast_integer_to_string},

    // E33 — any signed integer source (INT8/16/32/64) → the named unsigned
    // target, range-checked (fail loud on negative/out-of-range, never wraps).
    {"draken_cast_integer_to_uint8", (kernel_fn_t)&draken_cast_integer_to_uint8},
    {"draken_cast_integer_to_uint16", (kernel_fn_t)&draken_cast_integer_to_uint16},
    {"draken_cast_integer_to_uint32", (kernel_fn_t)&draken_cast_integer_to_uint32},
    {"draken_cast_integer_to_uint64", (kernel_fn_t)&draken_cast_integer_to_uint64},
    {"draken_cast_uint_to_int64", (kernel_fn_t)&draken_cast_uint_to_int64},

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
    {"draken_cast_string_to_float64", (kernel_fn_t)&draken_cast_string_to_float64},

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
    {"draken_cast_date32_to_int64", (kernel_fn_t)&draken_cast_date32_to_int64},
    {"draken_cast_timestamp_to_int64", (kernel_fn_t)&draken_cast_timestamp_to_int64},
    {"draken_cast_timestamp_to_string", (kernel_fn_t)&draken_cast_timestamp_to_string},
    // P9.0: draken_cast_timestamp_to_date32 stays unregistered — it is still a STUB
    // (cast_temporal.cpp returns "not yet implemented"). The registry holds ONLY real,
    // nogil, byte-identical kernels; a registered stub is a trap (the binder would mark
    // it BC_INSTR_C_NATIVE and dispatch an error sentinel). Re-add when implemented.
    // Its sibling draken_cast_date32_to_timestamp is now real and registered above.

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
    // Extraction kernels (4). draken_json_extract (`->`/`->>`, sub-op in ctx),
    // draken_map_access_string (str[i]) and draken_pointer_extract are REAL and
    // dispatched by the nogil VM straight from kernel_fn.
    //
    // draken_array_map_access remains a stub and is deliberately unreachable: the
    // ARRAY child vector hangs off the VectorOwner, not off DrakenVector, so this
    // signature cannot reach it. The binder does not flag BC_EXTR_MAP_ARRAY as
    // C-native, so it routes to the GIL VM. Making it real needs the
    // BC_C_NATIVE_CHILD plumbing the ARRAY→VARCHAR cast uses.
    // ========================================================================

    {"draken_map_access_string", (kernel_fn_t)&draken_map_access_string},
    {"draken_array_map_access", (kernel_fn_t)&draken_array_map_access},
    {"draken_json_extract", (kernel_fn_t)&draken_json_extract},
    {"draken_pointer_extract", (kernel_fn_t)&draken_pointer_extract},

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
    // Fixed-result string function kernels (Phase 9a-fn, function_string_extra.cpp)
    //
    // The rest of the string group is absent DELIBERATELY, not pending:
    //   CONCAT / CONCAT_WS — the optimizer rewrites both to `||` (StringConcat)
    //       chains, which are already native. A kernel here would be shadowed.
    //   SPLIT          — returns ARRAY; VecResult has no array-ownership contract.
    //   REGEXP_REPLACE — needs RE2, which is not compiled into draken/rugo.
    //   MATCH          — `_MATCH_AGAINST` raises NotImplementedError; no semantics
    //                    to port.
    //   RANDOM_STRING  — volatile, and its declared arity does not match its
    //                    callable; semantics undefined.
    // Each is explained in function_string_extra.cpp's header and was raised with
    // the architect rather than guessed at.
    // ========================================================================
    {"draken_octet_length", (kernel_fn_t)&draken_octet_length},
    {"draken_position", (kernel_fn_t)&draken_position},
    {"draken_levenshtein", (kernel_fn_t)&draken_levenshtein},
    {"draken_to_ascii", (kernel_fn_t)&draken_to_ascii},

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
    //   GREATEST/LEAST  — reduce over an ARRAY, whose child vector hangs off
    //                     VectorOwner rather than DrakenVector and so cannot be
    //                     reached on this signature (the same wall extraction.cpp's
    //                     `arr[i]` documents). Needs the BC_C_NATIVE_CHILD plumbing;
    //                     raised with the architect rather than guessed at.
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
    // SORT, ARRAY_CONTAINS_ANY and ARRAY_CONTAINS_ALL all READ an ARRAY. They
    // reuse the ARRAY->VARCHAR cast's BC_C_NATIVE_CHILD mechanism, extended to
    // BC_FUNCTION (compiled_expression.pyx / evaluation.pyx): the VM appends the
    // column-resolved child element vector as a SYNTHETIC extra arg, so each still
    // fits the plain func_fn_t(ctx, args[], nargs) shape (nargs==2). That encoding
    // carries exactly ONE column_identity, so all three take their array from a
    // DIRECT column load; a computed array argument is not bind-time eligible and
    // is refused at plan time (this engine has no Python fallback).
    //
    // ARRAY_CONTAINS_ANY/ALL fit that one-child budget because their needle set is
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
    // Absent by design:
    //   GREATEST/LEAST  — see the null-conditional block above; they reduce over an
    //                     ARRAY and could now plausibly use this same mechanism,
    //                     but that is a separate port, not assumed here.
    //   ARRAY_CONTAINS  — lowered to `item = ANY(arr)` (AnyOpEq) at plan-build
    //                     time; already native. Its Python impl is a fail-loud
    //                     guard for a bypassed rewrite, so an entry here would
    //                     silence that guard, not accelerate anything.
    // ========================================================================
    {"draken_jsonb_object_keys", (kernel_fn_t)&draken_jsonb_object_keys},
    {"draken_sort", (kernel_fn_t)&draken_sort},
    {"draken_array_contains_any", (kernel_fn_t)&draken_array_contains_any},
    {"draken_array_contains_all", (kernel_fn_t)&draken_array_contains_all},

    // DATEDIFF/TIMEDIFF/DATE_FORMAT/UNIXTIME/TIME_BUCKET (function_temporal.cpp)
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

binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code,
                                          unsigned char left_scale,
                                          unsigned char right_scale,
                                          unsigned char result_scale,
                                          unsigned char result_precision,
                                          unsigned char left_unit,
                                          unsigned char right_unit) {
    auto* ctx = static_cast<binary_op_ctx*>(malloc(sizeof(binary_op_ctx)));
    if (ctx) {
        ctx->op_code = op_code;
        ctx->left_scale = left_scale;
        ctx->right_scale = right_scale;
        ctx->result_scale = result_scale;
        ctx->result_precision = result_precision;
        ctx->left_unit = left_unit;
        ctx->right_unit = right_unit;
    }
    return ctx;
}

extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code, const char* nav,
                                            size_t nav_len, int64_t index) {
    // JSON sub-ops navigate by RFC 6901 pointer. Convert dot-notation ONCE here,
    // at bind time, so the per-morsel kernel call is parse + navigate + serialise
    // with no path work. Non-JSON sub-ops store the key bytes verbatim.
    std::string converted;
    if (sub_op_code == 3 /* BC_EXTR_JSON_PTR */ || sub_op_code == 4 /* BC_EXTR_JSON_KEY */) {
        if (nav == nullptr) nav_len = 0u;
        converted = draken::ops::dotpath_to_jsonptr(nav, nav_len);
        nav = converted.data();
        nav_len = converted.size();
    }
    if (nav == nullptr) nav_len = 0u;

    auto* ctx = static_cast<extraction_ctx*>(malloc(sizeof(extraction_ctx) + nav_len));
    if (!ctx) return nullptr;
    ctx->sub_op_code = static_cast<int32_t>(sub_op_code);
    ctx->nav_len     = static_cast<int32_t>(nav_len);
    ctx->index       = index;
    if (nav_len > 0u)
        memcpy(reinterpret_cast<unsigned char*>(ctx) + sizeof(extraction_ctx), nav, nav_len);
    return ctx;
}

in_list_ctx* kernel_alloc_in_list_ctx(const uint8_t* blob, size_t blob_len) {
    // The blob's first bytes ARE the header (built by the binder); one copy.
    if (blob == nullptr || blob_len < sizeof(in_list_ctx)) return nullptr;
    auto* ctx = static_cast<in_list_ctx*>(malloc(blob_len));
    if (ctx) memcpy(ctx, blob, blob_len);
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

format_ctx* kernel_alloc_format_ctx(unsigned char ts_unit, const char* fmt, size_t fmt_len) {
    if (fmt == nullptr) fmt_len = 0u;
    auto* ctx = static_cast<format_ctx*>(malloc(sizeof(format_ctx) + fmt_len));
    if (!ctx) return nullptr;
    ctx->ts_unit = ts_unit;
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
