// opteryx/compiled/thread_pool_bridge.cpp — the ONE compiled home for
// bs_pool_bridge_c.h's cross-.so entry points. Compiled ONLY into
// thread_pool.cpython-*.so (see setup.py's `opteryx.compiled.thread_pool`
// Extension sources) — see bs_pool_bridge_c.h for why that matters.

#include <Python.h>
#include "bs_pool_bridge_c.h"
#include "bs_pool_bridge.hpp"

extern "C" void bs_pool_submit_native(void* pool, void (*fn)(void*), void* arg) {
    static_cast<BSThreadPoolBridge*>(pool)->submit_native(fn, arg);
}

extern "C" void bs_pool_wait_native(void* pool) {
    static_cast<BSThreadPoolBridge*>(pool)->wait_native();
}
