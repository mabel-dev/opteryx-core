#include "ops/kernels/function_kernels.h"
#include "ops/kernels/error_handling.h"

/**
 * BC_FUNCTION kernel implementations: string functions.
 * Phase 8b of C kernel ABI.
 *
 * Wraps existing C++ nanobind function implementations.
 * All signatures: VecResult (*)(void* ctx, const DrakenVector* const* args, uint32_t nargs)
 */

extern "C" {

// Forward declarations of C++ string function implementations
extern "C" VecResult vector_length_impl(const DrakenVector* v);
extern "C" VecResult vector_string_substring_impl(const DrakenVector* str, const DrakenVector* start);
extern "C" VecResult vector_string_substring_len_impl(const DrakenVector* str, const DrakenVector* start, const DrakenVector* length);
extern "C" VecResult vector_string_slice_left_impl(const DrakenVector* str, const DrakenVector* n);
extern "C" VecResult vector_string_slice_right_impl(const DrakenVector* str, const DrakenVector* n);
extern "C" VecResult vector_trim_impl(const DrakenVector* v);
extern "C" VecResult vector_ltrim_impl(const DrakenVector* v);
extern "C" VecResult vector_rtrim_impl(const DrakenVector* v);
extern "C" VecResult vector_trim_chars_impl(const DrakenVector* v, const DrakenVector* chars);
extern "C" VecResult vector_ltrim_chars_impl(const DrakenVector* v, const DrakenVector* chars);
extern "C" VecResult vector_rtrim_chars_impl(const DrakenVector* v, const DrakenVector* chars);
extern "C" VecResult vector_lowercase_impl(const DrakenVector* v);
extern "C" VecResult vector_uppercase_impl(const DrakenVector* v);
extern "C" VecResult vector_initcap_impl(const DrakenVector* v);
extern "C" VecResult vector_reverse_impl(const DrakenVector* v);
extern "C" VecResult vector_replace_impl(const DrakenVector* str, const DrakenVector* from, const DrakenVector* to);
extern "C" VecResult vector_position_impl(const DrakenVector* substr, const DrakenVector* str);
extern "C" VecResult vector_contains_impl(const DrakenVector* str, const DrakenVector* substr);
extern "C" VecResult vector_starts_with_impl(const DrakenVector* str, const DrakenVector* prefix);
extern "C" VecResult vector_ends_with_impl(const DrakenVector* str, const DrakenVector* suffix);
extern "C" VecResult vector_ci_starts_with_impl(const DrakenVector* str, const DrakenVector* prefix);
extern "C" VecResult vector_ci_ends_with_impl(const DrakenVector* str, const DrakenVector* suffix);
extern "C" VecResult vector_regex_replace_impl(const DrakenVector* str, const DrakenVector* pattern, const DrakenVector* replacement);
extern "C" VecResult vector_levenshtein_impl(const DrakenVector* str1, const DrakenVector* str2);
extern "C" VecResult vector_soundex_impl(const DrakenVector* v);
extern "C" VecResult vector_string_is_empty_impl(const DrakenVector* v);
extern "C" VecResult vector_string_is_not_empty_impl(const DrakenVector* v);
extern "C" VecResult vector_split_impl(const DrakenVector* str, const DrakenVector* delimiter);

/**
 * LENGTH(str): string length (character count, UTF-8 aware)
 */
VecResult vector_length(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("LENGTH expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_length_impl(args[0]);
    });
}

/**
 * SUBSTRING(str, start) or SUBSTRING(str, start, length)
 */
VecResult vector_substring(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 2 || nargs > 3) return draken_error_sentinel("SUBSTRING expects 2 or 3 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");

        if (nargs == 2) {
            return vector_string_substring_impl(args[0], args[1]);
        } else {
            return vector_string_substring_len_impl(args[0], args[1], args[2]);
        }
    });
}

/**
 * LEFT(str, n): leftmost n characters
 */
VecResult vector_left(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("LEFT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_string_slice_left_impl(args[0], args[1]);
    });
}

/**
 * RIGHT(str, n): rightmost n characters
 */
VecResult vector_right(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("RIGHT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_string_slice_right_impl(args[0], args[1]);
    });
}

/**
 * TRIM(str) or TRIM(str, chars)
 */
VecResult vector_trim(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("TRIM expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("String argument is null");

        if (nargs == 1) {
            return vector_trim_impl(args[0]);
        } else {
            return vector_trim_chars_impl(args[0], args[1]);
        }
    });
}

/**
 * LTRIM(str) or LTRIM(str, chars)
 */
VecResult vector_ltrim(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("LTRIM expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("String argument is null");

        if (nargs == 1) {
            return vector_ltrim_impl(args[0]);
        } else {
            return vector_ltrim_chars_impl(args[0], args[1]);
        }
    });
}

/**
 * RTRIM(str) or RTRIM(str, chars)
 */
VecResult vector_rtrim(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs < 1 || nargs > 2) return draken_error_sentinel("RTRIM expects 1 or 2 arguments");
        if (!args || !args[0]) return draken_error_sentinel("String argument is null");

        if (nargs == 1) {
            return vector_rtrim_impl(args[0]);
        } else {
            return vector_rtrim_chars_impl(args[0], args[1]);
        }
    });
}

/**
 * LOWER(str): lowercase
 */
VecResult vector_lower(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("LOWER expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_lowercase_impl(args[0]);
    });
}

/**
 * UPPER(str): uppercase
 */
VecResult vector_upper(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("UPPER expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_uppercase_impl(args[0]);
    });
}

/**
 * INITCAP(str): title case
 */
VecResult vector_initcap(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("INITCAP expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_initcap_impl(args[0]);
    });
}

/**
 * REVERSE(str): reverse string
 */
VecResult vector_reverse(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("REVERSE expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_reverse_impl(args[0]);
    });
}

/**
 * REPLACE(str, from, to)
 */
VecResult vector_replace(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 3) return draken_error_sentinel("REPLACE expects 3 arguments");
        if (!args || !args[0] || !args[1] || !args[2]) return draken_error_sentinel("Arguments are null");
        return vector_replace_impl(args[0], args[1], args[2]);
    });
}

/**
 * POSITION(substr IN str) or STRPOS(str, substr)
 */
VecResult vector_position(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("POSITION expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_position_impl(args[0], args[1]);
    });
}

/**
 * CONTAINS(str, substr): check if str contains substr
 */
VecResult vector_contains(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("CONTAINS expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_contains_impl(args[0], args[1]);
    });
}

/**
 * STARTS_WITH(str, prefix)
 */
VecResult vector_starts_with(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("STARTS_WITH expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_starts_with_impl(args[0], args[1]);
    });
}

/**
 * ENDS_WITH(str, suffix)
 */
VecResult vector_ends_with(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ENDS_WITH expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_ends_with_impl(args[0], args[1]);
    });
}

/**
 * STARTS_WITH_CI(str, prefix): case-insensitive
 */
VecResult vector_starts_with_ci(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("STARTS_WITH_CI expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_ci_starts_with_impl(args[0], args[1]);
    });
}

/**
 * ENDS_WITH_CI(str, suffix): case-insensitive
 */
VecResult vector_ends_with_ci(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("ENDS_WITH_CI expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_ci_ends_with_impl(args[0], args[1]);
    });
}

/**
 * REGEX_REPLACE(str, pattern, replacement)
 */
VecResult vector_regex_replace(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 3) return draken_error_sentinel("REGEX_REPLACE expects 3 arguments");
        if (!args || !args[0] || !args[1] || !args[2]) return draken_error_sentinel("Arguments are null");
        return vector_regex_replace_impl(args[0], args[1], args[2]);
    });
}

/**
 * LEVENSHTEIN(str1, str2): Levenshtein distance
 */
VecResult vector_levenshtein(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("LEVENSHTEIN expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_levenshtein_impl(args[0], args[1]);
    });
}

/**
 * SOUNDEX(str): Soundex encoding
 */
VecResult vector_soundex(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("SOUNDEX expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_soundex_impl(args[0]);
    });
}

/**
 * IS_EMPTY(str): check if string is empty
 */
VecResult vector_is_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("IS_EMPTY expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_string_is_empty_impl(args[0]);
    });
}

/**
 * IS_NOT_EMPTY(str): check if string is not empty
 */
VecResult vector_is_not_empty(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 1) return draken_error_sentinel("IS_NOT_EMPTY expects 1 argument");
        if (!args || !args[0]) return draken_error_sentinel("Argument is null");
        return vector_string_is_not_empty_impl(args[0]);
    });
}

/**
 * SPLIT(str, delimiter): split string into array
 */
VecResult vector_split(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    DRAKEN_KERNEL_TRY({
        if (nargs != 2) return draken_error_sentinel("SPLIT expects 2 arguments");
        if (!args || !args[0] || !args[1]) return draken_error_sentinel("Arguments are null");
        return vector_split_impl(args[0], args[1]);
    });
}

}  // extern "C"
