#pragma once
// src/cpp/engine/streaming_scan_source.hpp — the REAL (non-demo) streaming Source: pulls
// morsels from an existing native scan ON DEMAND, from worker threads, calling back into
// Cython only when a worker actually needs the next morsel. Unlike the slice 5a-d demos
// (which pre-pull every morsel into a vector before the C++ run — fine for a proof, not
// memory-bounded for production), this is genuinely streaming: memory use stays bounded
// by in-flight morsels, not the whole scan.
//
// Bridge shape: a C function pointer + an opaque `void*` (the scan's borrowed PyObject*),
// matching the `native_task_fn` idiom already used for the native worker fan-outs
// (thread_pool.pxd). The Cython side owns ONE trampoline function (GIL-bridged: nogil
// entry, `with gil` body calling the scan's existing `next_morsel()`); this header never
// touches Python/Cython machinery directly — it only calls the function pointer it's
// given. Multiple worker threads call `get_morsel()` concurrently; thread-safety is the
// SAME contract `is_concurrent_pull_safe()`/`_scan_mtx` already provide on the scan side
// (proven correct under genuine concurrency by this session's production hang fix).

#include <memory>
#include <mutex>

#include "executor.hpp"

namespace opteryx::engine {

// Pulls the next morsel from the scan at `scan_ptr` (a borrowed PyObject*, kept alive by
// the caller for the run's duration). Sets `*out` and `*finished=0` on success; sets
// `*finished=1` (out untouched) on exhaustion. `*err_code` is set non-zero (matching
// ErrCtx semantics) if the underlying pull raised — the trampoline stashes the Python
// exception on the scan object itself (existing `_take_exc`/`_cxx_push_exc` contract);
// the caller surfaces it at the GIL boundary same as the rest of the native drive path.
typedef void (*ScanPullFn)(void* scan_ptr, std::shared_ptr<CxxMorsel>* out,
                           int* finished, int* err_code);

struct StreamingScanSourceGlobal : GlobalSourceState {};
struct StreamingScanSourceLocal : LocalSourceState {};

struct StreamingScanSource : Source {
    void* scan_ptr;       // borrowed PyObject* — the caller owns its lifetime
    ScanPullFn pull_fn;   // the Cython trampoline
    bool serialize_pull;  // true when the scan is NOT concurrent-pull safe (two-pass
                          // latmat / fallback generator): pulls are strictly one at a
                          // time under `pull_mtx`, the operators/sink stay parallel.
    std::mutex pull_mtx;

    StreamingScanSource(void* s, ScanPullFn fn, bool serialize = false)
        : scan_ptr(s), pull_fn(fn), serialize_pull(serialize) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<StreamingScanSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<StreamingScanSourceLocal>();
    }
    SourceResult get_morsel(GlobalSourceState&, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        std::shared_ptr<CxxMorsel> result;
        int finished = 0;
        int err_code = 0;
        if (serialize_pull) {
            // Workers block here DETACHED from Python (run_worker threads only
            // attach inside the trampoline), so this contended wait cannot stall
            // a free-threaded stop-the-world.
            std::lock_guard<std::mutex> lk(pull_mtx);
            pull_fn(scan_ptr, &result, &finished, &err_code);
        } else {
            pull_fn(scan_ptr, &result, &finished, &err_code);
        }
        if (err_code != 0) {
            err.code = err_code;
            err.msg = "StreamingScanSource: scan pull raised — see the stashed Python "
                      "exception on the scan object";
            return SourceResult::FINISHED;
        }
        if (finished) {
            return SourceResult::FINISHED;
        }
        out = std::move(result);
        return SourceResult::HAVE_MORE;
    }
};

}  // namespace opteryx::engine
