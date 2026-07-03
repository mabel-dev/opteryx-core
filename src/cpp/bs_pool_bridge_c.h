#pragma once
// src/cpp/bs_pool_bridge_c.h — extern "C" bridge to BSThreadPoolBridge, callable
// from OUTSIDE the .so that constructs the pool.
//
// BSThreadPoolBridge (bs_pool_bridge.hpp) is a plain, non-virtual C++ class with no
// separate .cpp — every .so that #includes it gets its OWN independently compiled
// copy. Cython's cdef-class vtable dispatch (CppThreadPool.submit_native, used by
// every existing Python-facing caller) always runs through thread_pool.so's own
// compiled code against its own object, so it never noticed this. Pure C++ engine
// code (src/cpp/engine/executor.hpp) that calls BSThreadPoolBridge methods directly
// on the raw pointer does NOT go through that vtable — it uses whichever .so's own
// copy of the class it was compiled with. `thread_pool.so` builds with -std=c++17
// (no -DNB_FREE_THREADED); `_operators.so` builds with -std=c++20 -DNB_FREE_THREADED.
// That divergence changes BS::move_only_function's internal layout (its C++23
// feature-test branch differs) — the two .so's disagree about the object's layout,
// and calling detach_task() from the "foreign" .so corrupts the pool's task queue
// (reproduced as a real SIGSEGV inside BS::thread_pool's internal deque).
//
// The fix: implementations of these two functions live in EXACTLY ONE place —
// opteryx/compiled/thread_pool_bridge.cpp, compiled only into thread_pool.so — and
// every other .so only DECLARES them here (no body, so no second copy gets
// compiled). `opteryx/compiled/__init__.py` loads thread_pool.so with RTLD_GLOBAL
// so these symbols resolve at import time from consumer extensions linked with
// `-undefined dynamic_lookup` / `--allow-shlib-undefined` (the same mechanism
// draken_native.so already uses for draken_vector_unwrap et al. — see
// draken/core/draken_bridge.h and draken/__init__.py).
//
// `pool` is an opaque BSThreadPoolBridge* — callers on the far side of this bridge
// must never dereference it themselves, only pass it through.

#ifdef __cplusplus
extern "C" {
#endif

void bs_pool_submit_native(void* pool, void (*fn)(void*), void* arg);
void bs_pool_wait_native(void* pool);

#ifdef __cplusplus
}
#endif
