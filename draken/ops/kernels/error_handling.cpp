#include "ops/kernels/error_handling.h"
#include <cstring>
#include <cstdio>
#include <thread>
#include <map>

/**
 * Thread-local error message storage.
 * Each thread has its own 256-byte buffer for error messages.
 */
static thread_local char g_error_message[DRAKEN_ERROR_MSG_LEN] = {0};

char* draken_error_message_slot(void) {
    return g_error_message;
}

void draken_error_message_clear(void) {
    g_error_message[0] = '\0';
}

VecResult draken_error_sentinel(const char* error_message) {
    // Set the thread-local error message
    if (error_message) {
        std::strncpy(g_error_message, error_message, DRAKEN_ERROR_MSG_LEN - 1);
        g_error_message[DRAKEN_ERROR_MSG_LEN - 1] = '\0';
    } else {
        std::strcpy(g_error_message, "Unknown error");
    }

    // Return a sentinel VecResult with data == nullptr
    VecResult result;
    result.data = nullptr;
    result.validity = nullptr;
    result.selection = nullptr;
    result.owns_selection = false;
    result.data_length = 0;
    result.length = 0;
    result.type = DRAKEN_NULL;  // Sentinel type
    result.flags = 0;

    return result;
}

VecResult draken_error_sentinel_fmt(const char* format, ...) {
    if (!format) {
        return draken_error_sentinel("Format string is null");
    }

    va_list args;
    va_start(args, format);
    int written = std::vsnprintf(g_error_message, DRAKEN_ERROR_MSG_LEN, format, args);
    va_end(args);

    if (written < 0 || written >= DRAKEN_ERROR_MSG_LEN) {
        // Truncation or error; ensure null-termination
        g_error_message[DRAKEN_ERROR_MSG_LEN - 1] = '\0';
    }

    // Return sentinel
    VecResult result;
    result.data = nullptr;
    result.validity = nullptr;
    result.selection = nullptr;
    result.owns_selection = false;
    result.data_length = 0;
    result.length = 0;
    result.type = DRAKEN_NULL;
    result.flags = 0;

    return result;
}

bool draken_has_error(void) {
    return g_error_message[0] != '\0';
}

const char* draken_get_error_message(void) {
    return draken_has_error() ? g_error_message : "";
}
