// src/cpp/engine/test_err_latch.cpp — ErrCtx::msg must still be readable after the
// pipeline returns, whoever lent the text.
//
// ErrCtx::msg is a BORROWED const char*. Two lenders die before the driver reads it:
//
//   1. set_span_error / format_kernel_error (native_expression.hpp) point it at a
//      thread_local std::string on the WORKER thread. That string is freed when the
//      thread exits (this test's std::thread backend) and when a later failure on the
//      same pool thread move-assigns it (the production BSThreadPoolBridge backend) —
//      observed as `UnicodeDecodeError` over random bytes on `1 = ANY(float_array)`.
//   2. An operator that keeps its message in its own OperatorState: op_states are
//      locals of run_worker, destroyed as it returns.
//
// run_worker's WorkerErrLatch copies the text into WorkerCtx::err_text, on the worker
// thread, before either lender dies. Each case here fails every worker at dop 1..8 and
// checks the message the driver ends up with, byte for byte.
//
// Reading freed memory only reliably shows up under AddressSanitizer — without it the
// freed bytes may happen to be intact — so build it with ASan. With WorkerErrLatch
// neutralised, every case fails its value check and ASan reports a heap-use-after-free
// in latch_err_msg on memory format_kernel_error freed on the worker thread.
//
// The Python include path is there only because native_expression.hpp includes
// core/draken_bridge.h, which includes Python.h; nothing here links libpython.
// trace_bridge.cpp supplies draken_trace_enabled, which the executor's trace spans call.
//
// `make err-latch-test` (also run by `make test`) builds and runs it; by hand:
//
//   c++ -std=c++20 -O1 -g -fsanitize=address -fno-omit-frame-pointer -pthread \
//       -I. -Idraken -Idraken/core -Isrc/cpp -Isrc/cpp/engine -Ithird_party/cyan4973 \
//       $(python3-config --includes) \
//       src/cpp/engine/test_err_latch.cpp draken/core/trace_bridge.cpp \
//       -o /tmp/test_err_latch && /tmp/test_err_latch

#include <atomic>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "executor.hpp"
#include "native_expression.hpp"   // set_span_error — the real lender, not a stand-in

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

// Longer than any std::string small-buffer, so every copy of it is a heap block that
// ASan tracks — an SSO-sized message would sit inside the string object itself.
static const char* kKernelMsg =
    "draken_array_contains: integer item but array elements are type 21 "
    "(supported: int/uint family, BOOL, TIMESTAMP, DATE, DECIMAL)";

// ---- OPERATOR, lender 1: fails through the real set_span_error. -------------------
struct SpanErrorOp : Operator {
    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr&, OperatorState&, MorselPtr&, ErrCtx& err) override {
        set_span_error(err, "ExprMultiProjectOperator: expression evaluation failed",
                       1, 15, kKernelMsg);
        return OpResult::NEED_INPUT;
    }
};

// ---- OPERATOR, lender 2: message owned by its own OperatorState. ------------------
struct OwnedMsgState : OperatorState {
    std::string msg;
};
struct StateErrorOp : Operator {
    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OwnedMsgState>();
    }
    OpResult execute(const MorselPtr&, OperatorState& st, MorselPtr&, ErrCtx& err) override {
        auto& s = static_cast<OwnedMsgState&>(st);
        s.msg = std::string("StateErrorOp: ") + kKernelMsg;
        err.code = 1;
        err.msg = s.msg.c_str();
        return OpResult::NEED_INPUT;
    }
};

// ---- SINK: never reached (every morsel fails first). -------------------------------
struct NullSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GlobalSinkState>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<LocalSinkState>();
    }
    SinkResult sink(const MorselPtr&, GlobalSinkState&, LocalSinkState&, ErrCtx&) override {
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) override {}
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

static bool run_case(const char* name, Operator& op, const std::string& expected) {
    bool ok = true;
    for (int dop : {1, 2, 4, 8}) {
        NSource src(64);
        NullSink snk;
        Pipeline p;
        p.source = &src;
        p.operators = {&op};
        p.sink = &snk;

        ErrCtx err;
        run_pipeline(p, dop, err);   // std::thread per worker: joined (and exited) here

        // Copy exactly as _engine_plan_run does, after run_pipeline has returned.
        std::string got = err.msg != nullptr ? std::string(err.msg) : std::string("<null>");
        bool pass = err.code == 1 && got == expected;
        std::printf("%-12s dop=%d %s\n", name, dop, pass ? "OK" : "*** FAIL ***");
        if (!pass) std::printf("  expected: %s\n  got:      %s\n", expected.c_str(), got.c_str());
        ok = ok && pass;
    }
    return ok;
}

int main() {
    SpanErrorOp span_op;
    StateErrorOp state_op;
    bool ok = true;
    ok = run_case("span_error", span_op,
                  std::string("ExprMultiProjectOperator: expression evaluation failed "
                              "(err_op=15): ") + kKernelMsg) && ok;
    ok = run_case("state_owned", state_op, std::string("StateErrorOp: ") + kKernelMsg) && ok;
    std::printf("%s\n", ok ? "ERR LATCH PASS" : "ERR LATCH FAIL");
    return ok ? 0 : 1;
}
