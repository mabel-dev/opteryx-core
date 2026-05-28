#pragma once

/**
 * Error handling for C kernel ABI — Phase 9a
 *
 * C++ exceptions cannot cross the extern "C" boundary. All C kernels must:
 * 1. Catch any C++ exceptions from underlying implementations.
 * 2. Return a sentinel VecResult (data == nullptr).
 * 3. Set a thread-local error message for the executor to read.
 *
 * The executor (9c) checks for data == nullptr and raises a Python exception
 * at the GIL boundary using the stored error message.
 *
 * Pattern:
 *
 *   extern "C" VecResult draken_some_kernel(void* ctx, const DrakenVector* v) {
 *       try {
 *           // C++ implementation
 *           return result;
 *       } catch (const std::exception& e) {
 *           return draken_error_sentinel(e.what());
 *       }
 *   }
 */

#include <stdint.h>
#include <cstring>
#include <exception>
#include <cstdio>
#include <cstdarg>
#include "core/buffers.h"
#include "ops/vec_result.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Maximum length of thread-local error message (including null terminator).
 */
#define DRAKEN_ERROR_MSG_LEN 256

/**
 * Get the thread-local error message buffer.
 * Caller writes to this buffer to set an error message.
 * Returns a pointer to a 256-byte buffer (thread-safe).
 */
char* draken_error_message_slot(void);

/**
 * Clear the thread-local error message.
 * Executor calls this at the start of each kernel call to reset state.
 */
void draken_error_message_clear(void);

/**
 * Return a sentinel VecResult indicating an error.
 * Sets the thread-local error message.
 * All fields except 'type' are zeroed; type is set to DRAKEN_NULL (sentinel).
 *
 * Usage:
 *   try { ... } catch (const std::exception& e) { return draken_error_sentinel(e.what()); }
 */
VecResult draken_error_sentinel(const char* error_message);

/**
 * Helper: set the thread-local error message and return a sentinel.
 * Formats a printf-style error message.
 *
 * Usage:
 *   return draken_error_sentinel_fmt("Invalid index: %d", idx);
 *
 * Limited to DRAKEN_ERROR_MSG_LEN characters (including null terminator).
 */
VecResult draken_error_sentinel_fmt(const char* format, ...);

/**
 * Check if the last operation resulted in an error.
 * Called by the executor to detect sentinel VecResults.
 *
 * Returns true if draken_error_message_slot()[0] != '\0'.
 */
bool draken_has_error(void);

/**
 * Get the current thread-local error message (read-only).
 * Returns pointer to error message or empty string if no error.
 */
const char* draken_get_error_message(void);

#ifdef __cplusplus
}  // extern "C"
#endif

/* ============================================================================
 * C++ convenience wrapper macro for try-catch boilerplate
 *
 * Usage in kernel implementation:
 *
 *   extern "C" VecResult draken_kernel(void* ctx, const DrakenVector* v) {
 *       DRAKEN_KERNEL_TRY({
 *           // C++ code here
 *           return result;
 *       });
 *   }
 * ========================================================================== */

#ifdef __cplusplus
#define DRAKEN_KERNEL_TRY(code) \
    try { \
        code \
    } catch (const std::exception& e) { \
        return draken_error_sentinel(e.what()); \
    } catch (...) { \
        return draken_error_sentinel("Unknown error in kernel"); \
    }
#endif
