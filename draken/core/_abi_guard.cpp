// Minimal compile target whose only job is to force the buffers.h ABI
// static_asserts (sizeof / per-field offsets / DrakenType tag pins) to run at
// build time on the dev platform. If the frozen layout drifts, this translation
// unit fails to compile — turning silent ABI drift (a consumer segfault) into a
// loud build break (09_delivery.md risk #1).
#include "core/buffers.h"

extern "C" int draken_abi_guard(void) {
    return (int)sizeof(DrakenVector);  // 40 on LP64; asserts above pin the rest.
}
