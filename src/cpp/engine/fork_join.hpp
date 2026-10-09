// src/cpp/engine/fork_join.hpp — the one exception-safe fork-join for native code
// that raises its own threads.
//
// A sink's finalize() runs once, on the executor's driver thread, with every
// pipeline worker already retired, so its parallel phases raise their own threads;
// so do other one-shot native phases (vector index build/search). An exception
// thrown on one of those threads used to call std::terminate (it escaped a
// std::thread body), and one thrown on the calling thread's own share destroyed a
// vector of still-joinable threads — std::terminate again. Neither ever reached
// the query as an error.
//
// fork_join catches each thread's exception, ALWAYS joins every thread it started,
// then rethrows the first one on the calling thread, where the caller's own error
// channel takes it: run_pipeline_impl (executor.hpp) converts an exception out of
// finalize() into the pipeline's ErrCtx; the vector index functions convert it to
// their `*err`.
#pragma once

#include <exception>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace opteryx::engine {

// Runs `fn(0..nt-1)`, the calling thread taking tid 0, and joins. A template, not a
// std::function: the bodies are per-bucket / per-chunk sweeps and must inline.
template <typename F>
inline void fork_join(unsigned nt, F&& fn) {
    std::exception_ptr first;
    std::mutex first_mu;
    auto record = [&](std::exception_ptr ex) {
        std::lock_guard<std::mutex> lock(first_mu);
        if (!first) first = std::move(ex);
    };
    auto guarded = [&](unsigned t) {
        try {
            fn(t);
        } catch (...) {
            record(std::current_exception());
        }
    };

    std::vector<std::thread> th;
    bool spawned_all = true;
    try {
        th.reserve(nt > 1 ? nt - 1 : 0);
        for (unsigned t = 1; t < nt; ++t) th.emplace_back(guarded, t);
    } catch (...) {
        // Could not raise every thread (std::system_error / bad_alloc). A caller may
        // partition work by tid, where a missing tid is a missing result: do not run
        // tid 0 as though the phase were whole — join what started and fail.
        record(std::current_exception());
        spawned_all = false;
    }
    if (spawned_all) guarded(0);
    for (std::thread& x : th) x.join();
    if (first) std::rethrow_exception(first);
}

}  // namespace opteryx::engine
