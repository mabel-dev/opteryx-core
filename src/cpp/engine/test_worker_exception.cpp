// src/cpp/engine/test_worker_exception.cpp — a C++ exception thrown inside a pipeline
// must surface as the pipeline's ErrCtx, never as a silently empty result.
//
// BS::thread_pool's worker loop runs every detached task inside
// `try { task(); } catch (...) {}`, so an exception escaping run_worker on the
// production backend vanished: the worker's ErrCtx stayed 0, the driver saw no error
// and GROUP BY returned 0 rows with no exception (proven 2026-10-09 with a throw in
// GBPartition::promote_* reached from GroupBySink::combine). Live throw sites on that
// path today: CarcharIndex::find_slot ("Carchar probe exhausted table capacity") and
// insert_new ("key already exists").
//
// This drives a sink that throws from sink(), combine() or finalize() through the REAL
// production bridge (BSThreadPoolBridge via bs_pool_submit_native — this TU compiles
// opteryx/compiled/thread_pool_bridge.cpp, exactly as thread_pool.so does) and through
// the std::thread backend, at dop 1..8, and requires err.code == 1 with the
// exception's text in the message. Without run_worker's catch, every pool case
// reports code 0 (the bug), and the std::thread cases std::terminate.
//
// finalize()'s own fan-out (fork_join — sort, window, join build, GROUP BY) is covered
// too: a throw on a spawned thread, or on the calling thread's tid 0, must be joined
// and rethrown into finalize, never std::terminate.
//
// Separately, a native task that lets an exception ESCAPE must not reach that
// swallowing catch at all: BSThreadPoolBridge::submit_native terminates the process
// instead. Checked in a forked child, which must die by SIGABRT.
//
// `make worker-exception-test` (also run by `make test`) builds and runs it under ASan.

#include <atomic>
#include <csignal>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "executor.hpp"
#include "fork_join.hpp"
#include "bs_pool_bridge.hpp"

using namespace opteryx::engine;

// ---- SOURCE: N single-row zero-column morsels, claimed atomically. -----------------
struct NSourceGlobal : GlobalSourceState {
    std::atomic<int> next{0};
};
struct NSource : Source {
    int n;
    explicit NSource(int n_) : n(n_) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<NSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        if (static_cast<NSourceGlobal&>(gs).next.fetch_add(1) >= n)
            return SourceResult::FINISHED;
        auto m = std::make_shared<CxxMorsel>();
        m->zero_col_rows = 1;
        out = std::move(m);
        return SourceResult::HAVE_MORE;
    }
};

// Longer than any std::string small-buffer, so ASan tracks every copy of it.
static const char* kThrowMsg = "Carchar probe exhausted table capacity (forced by test_worker_exception)";

enum class ThrowAt { SINK, COMBINE, FINALIZE, FANOUT_SPAWNED, FANOUT_CALLER };

// ---- SINK: throws std::runtime_error from exactly one of its phases. ---------------
struct ThrowingSink : Sink {
    ThrowAt at;
    explicit ThrowingSink(ThrowAt a) : at(a) {}
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GlobalSinkState>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<LocalSinkState>();
    }
    SinkResult sink(const MorselPtr&, GlobalSinkState&, LocalSinkState&, ErrCtx&) override {
        if (at == ThrowAt::SINK) throw std::runtime_error(kThrowMsg);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) override {
        if (at == ThrowAt::COMBINE) throw std::runtime_error(kThrowMsg);
    }
    void finalize(GlobalSinkState&, ErrCtx&) override {
        if (at == ThrowAt::FINALIZE) throw std::runtime_error(kThrowMsg);
        if (at == ThrowAt::FANOUT_SPAWNED || at == ThrowAt::FANOUT_CALLER) {
            const unsigned thrower = at == ThrowAt::FANOUT_CALLER ? 0u : 3u;
            fork_join(4, [thrower](unsigned t) {
                if (t == thrower) throw std::runtime_error(kThrowMsg);
            });
        }
    }
};

static bool run_case(const char* name, ThrowAt at, BSThreadPoolBridge* pool) {
    const std::string expected =
        std::string(at != ThrowAt::SINK && at != ThrowAt::COMBINE
                        ? "unhandled C++ exception in pipeline finalize: "
                        : "unhandled C++ exception in pipeline worker: ") + kThrowMsg;
    bool ok = true;
    for (int dop : {1, 2, 4, 8}) {
        NSource src(64);
        ThrowingSink snk(at);
        Pipeline p;
        p.source = &src;
        p.sink = &snk;

        ErrCtx err;
        if (pool != nullptr) run_pipeline(p, dop, dop, err, pool);
        else                 run_pipeline(p, dop, err);

        // Copy exactly as _engine_plan_run does, after run_pipeline has returned.
        std::string got = err.msg != nullptr ? std::string(err.msg) : std::string("<null>");
        bool pass = err.code == 1 && got == expected;
        std::printf("%-9s %-8s dop=%d %s\n", name, pool != nullptr ? "pool" : "thread", dop,
                    pass ? "OK" : "*** FAIL ***");
        if (!pass)
            std::printf("  expected: code=1 %s\n  got:      code=%d %s\n", expected.c_str(),
                        err.code, got.c_str());
        ok = ok && pass;
    }
    return ok;
}

static void escaping_task(void*) { throw std::runtime_error(kThrowMsg); }

// Runs BEFORE main's pool exists, so the child forks from a single-threaded parent.
static bool escaping_task_terminates() {
    std::fflush(stdout);
    pid_t pid = fork();
    if (pid == 0) {
        BSThreadPoolBridge child_pool(2, "test_worker_exception_child");
        child_pool.submit_native(&escaping_task, nullptr);
        child_pool.wait_native();
        _exit(0);   // reached only if the exception was swallowed
    }
    int status = 0;
    waitpid(pid, &status, 0);
    bool pass = WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
    std::printf("escaping  pool     %s\n", pass ? "OK (child terminated)" : "*** FAIL ***");
    if (!pass)
        std::printf("  expected: child killed by SIGABRT\n  got:      %s %d\n",
                    WIFSIGNALED(status) ? "signal" : "exit",
                    WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status));
    return pass;
}

int main() {
    bool escaping_ok = escaping_task_terminates();
    BSThreadPoolBridge pool(8, "test_worker_exception");
    bool ok = escaping_ok;
    for (BSThreadPoolBridge* backend : {&pool, static_cast<BSThreadPoolBridge*>(nullptr)}) {
        ok = run_case("sink", ThrowAt::SINK, backend) && ok;
        ok = run_case("combine", ThrowAt::COMBINE, backend) && ok;
        ok = run_case("finalize", ThrowAt::FINALIZE, backend) && ok;
        ok = run_case("fan-spawn", ThrowAt::FANOUT_SPAWNED, backend) && ok;
        ok = run_case("fan-tid0", ThrowAt::FANOUT_CALLER, backend) && ok;
    }
    pool.shutdown();
    std::printf("%s\n", ok ? "WORKER EXCEPTION PASS" : "WORKER EXCEPTION FAIL");
    return ok ? 0 : 1;
}
