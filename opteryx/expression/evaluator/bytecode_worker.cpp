/*
 * bytecode_worker.cpp — global storage for the nogil bytecode VM trampoline.
 *
 * Provides the definition of opteryx_worker_fn (declared extern in the header)
 * and the opteryx_set_worker_fn() setter called once during module init.
 */

#include "bytecode_worker.h"

opteryx_worker_fn_t opteryx_worker_fn = nullptr;

void opteryx_set_worker_fn(opteryx_worker_fn_t fn) {
    opteryx_worker_fn = fn;
}
