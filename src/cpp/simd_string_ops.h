#ifndef SIMD_STRING_OPS_HPP
#define SIMD_STRING_OPS_HPP

#include <cstddef>

#ifdef __cplusplus
extern "C++" {
#endif

/**
 * Convert ASCII characters to uppercase in-place using SIMD.
 * Non-ASCII bytes (>127) are left unchanged.
 * 
 * On AVX2: processes 32 bytes per iteration
 * Fallback: scalar byte-by-byte conversion
 * 
 * @param data Pointer to character data (modified in-place)
 * @param length Number of bytes to process
 */
void simd_to_upper(char* data, size_t length);

/**
 * Convert ASCII characters to lowercase in-place using SIMD.
 * Non-ASCII bytes (>127) are left unchanged.
 * 
 * On AVX2: processes 32 bytes per iteration
 * Fallback: scalar byte-by-byte conversion
 * 
 * @param data Pointer to character data (modified in-place)
 * @param length Number of bytes to process
 */
void simd_to_lower(char* data, size_t length);

/**
 * Compare two byte sequences for ASCII case-insensitive equality using SIMD.
 * Returns true if strings are equal ignoring ASCII case, false otherwise.
 * Non-ASCII bytes (>127) are compared as-is (no case conversion).
 * 
 * On AVX2: processes 32 bytes per iteration with inline case folding
 * Fallback: scalar byte-by-byte comparison
 * 
 * This is faster than simd_to_lower + memcmp because it avoids:
 * - Memory allocation for temporary buffer
 * - Memory copy operation
 * - Separate comparison pass
 * 
 * @param a First string to compare
 * @param b Second string to compare (typically pre-lowercased search term)
 * @param length Number of bytes to compare (must be equal for both strings)
 * @return true if strings are equal (case-insensitive), false otherwise
 */
bool simd_equals_ci(const char* a, const char* b, size_t length);

#ifdef __cplusplus
}
#endif

#endif // SIMD_STRING_OPS_HPP