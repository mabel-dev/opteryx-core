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

// The sentinel VecResult itself — one definition for all four entry points, so
// the internal-fault and data-error twins cannot drift in anything but the
// classification they pass in. `data == nullptr` is what makes it a sentinel;
// `data_error` is what tells the engine whether to frame the message or raise it
// verbatim (see ops/vec_result.h).
static VecResult sentinel_result(uint8_t data_error) {
    VecResult result;
    result.data = nullptr;
    result.validity = nullptr;
    result.selection = nullptr;
    result.owns_selection = false;
    result.data_length = 0;
    result.length = 0;
    result.type = DRAKEN_NULL;  // Sentinel type
    result.flags = 0;
    result.error_msg = g_error_message;
    result.data_error = data_error;

    return result;
}

static void set_error_message(const char* error_message) {
    if (error_message) {
        std::strncpy(g_error_message, error_message, DRAKEN_ERROR_MSG_LEN - 1);
        g_error_message[DRAKEN_ERROR_MSG_LEN - 1] = '\0';
    } else {
        std::strcpy(g_error_message, "Unknown error");
    }
}

static void set_error_message_v(const char* format, va_list args) {
    const int written = std::vsnprintf(g_error_message, DRAKEN_ERROR_MSG_LEN, format, args);
    if (written < 0 || written >= DRAKEN_ERROR_MSG_LEN) {
        // Truncation or error; ensure null-termination
        g_error_message[DRAKEN_ERROR_MSG_LEN - 1] = '\0';
    }
}

VecResult draken_error_sentinel(const char* error_message) {
    set_error_message(error_message);
    return sentinel_result(0u);
}

VecResult draken_data_error_sentinel(const char* error_message) {
    set_error_message(error_message);
    return sentinel_result(1u);
}

VecResult draken_error_sentinel_fmt(const char* format, ...) {
    if (!format) {
        return draken_error_sentinel("Format string is null");
    }

    va_list args;
    va_start(args, format);
    set_error_message_v(format, args);
    va_end(args);

    return sentinel_result(0u);
}

VecResult draken_data_error_sentinel_fmt(const char* format, ...) {
    if (!format) {
        // A missing format string is an ENGINE fault, not a data error, however
        // the caller meant to classify what it was about to say.
        return draken_error_sentinel("Format string is null");
    }

    va_list args;
    va_start(args, format);
    set_error_message_v(format, args);
    va_end(args);

    return sentinel_result(1u);
}

bool draken_has_error(void) {
    return g_error_message[0] != '\0';
}

const char* draken_get_error_message(void) {
    return draken_has_error() ? g_error_message : "";
}
