// src/cpp/engine/test_slice1.cpp — slice 1 proof: the morsel-driven parallel pipeline
// executor + the Source/Operator/Sink API, end to end, pure C++, no Python.
//
// Pipeline: CountSource (N morsels) -> HalfFilter (keeps half the rows) -> SumSink.
// Verifies the result is correct AND identical across dop=1,2,4,8 (deterministic under
// parallelism — the morsel-driven local-then-combine contract).
//
// Build (from repo root):
//   c++ -std=c++17 -O2 -pthread -Idraken -Isrc/cpp/engine \
//       src/cpp/engine/test_slice1.cpp -o /tmp/test_slice1 && /tmp/test_slice1

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>

#include "executor.hpp"

using namespace opteryx::engine;

// ---- Toy SOURCE: hands out N zero-column morsels of `rows_per` rows each. Parallel via
//      an atomic claim on the global (dynamic assignment). -------------------------------
struct CountSourceGlobal : GlobalSourceState {
    std::atomic<int> next{0};
    int n = 0;
    uint32_t rows_per = 0;
};
struct CountSourceLocal : LocalSourceState {};
struct CountSource : Source {
    int n;
    uint32_t rows_per;
    CountSource(int n_, uint32_t r) : n(n_), rows_per(r) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<CountSourceGlobal>();
        g->n = n;
        g->rows_per = rows_per;
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<CountSourceLocal>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        auto& g = static_cast<CountSourceGlobal&>(gs);
        int idx = g.next.fetch_add(1);
        if (idx >= g.n) return SourceResult::FINISHED;
        auto m = std::make_shared<CxxMorsel>();
        m->zero_col_rows = g.rows_per;  // zero-column morsel (COUNT(*)-shape), real CxxMorsel
        out = std::move(m);
        return SourceResult::HAVE_MORE;
    }
};

// ---- Toy OPERATOR: "filter" keeping half the rows. ------------------------------------
struct HalfFilter : Operator {
    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out, ErrCtx&) override {
        auto m = std::make_shared<CxxMorsel>();
        m->zero_col_rows = in->num_rows() / 2;
        out = std::move(m);
        return OpResult::EMIT;
    }
};

// ---- Toy SINK: sum rows. Local counter per worker; combine -> global; finalize. -------
struct SumSinkGlobal : GlobalSinkState {
    std::atomic<long long> total{0};
    long long result = -1;
};
struct SumSinkLocal : LocalSinkState {
    long long local = 0;
};
struct SumSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<SumSinkGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<SumSinkLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        static_cast<SumSinkLocal&>(ls).local += in->num_rows();
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        static_cast<SumSinkGlobal&>(gs).total +=
            static_cast<SumSinkLocal&>(ls).local;
    }
    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<SumSinkGlobal&>(gs);
        g.result = g.total.load();
    }
};

int main() {
    const int N = 4096;
    const uint32_t ROWS = 1000;
    const long long expected = static_cast<long long>(N) * ROWS / 2;  // halved by the filter

    bool ok = true;
    long long first = -1;
    for (int dop : {1, 2, 4, 8}) {
        CountSource src(N, ROWS);
        HalfFilter filt;
        SumSink snk;
        Pipeline p;
        p.source = &src;
        p.operators = {&filt};
        p.sink = &snk;

        ErrCtx err;
        auto gsink = run_pipeline(p, dop, err);
        long long result = static_cast<SumSinkGlobal*>(gsink.get())->result;

        bool pass = (err.code == 0) && (result == expected);
        if (dop == 1) first = result;
        if (result != first) pass = false;  // determinism across dop
        std::printf("dop=%d result=%lld expected=%lld %s\n", dop, result, expected,
                    pass ? "OK" : "*** FAIL ***");
        ok = ok && pass;
    }
    std::printf("%s\n", ok ? "SLICE 1 PASS" : "SLICE 1 FAIL");
    return ok ? 0 : 1;
}
