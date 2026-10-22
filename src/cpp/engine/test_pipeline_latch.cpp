// src/cpp/engine/test_pipeline_latch.cpp — a pipeline waits for ITS OWN workers, not for
// the whole pool, so pipelines on different driver threads can share the pool.
//
// UNION legs run their pipelines concurrently (Engine::run_span_), each leg driven from
// its own thread against the one worker pool. run_pipeline used to end with
// bs_pool_wait_native(pool) - "block until every task on the pool is done" - which is only
// the pipeline's own tasks while pipelines run one at a time. With a sibling leg's tasks
// on the pool it also waited for those, so a leg could not return from run_pipeline, run
// its finalize, or start its next pipeline until the slowest sibling's current pipeline
// had finished: the legs would run in lock-step instead of overlapping.
//
// This holds pipeline B open - its source refuses to finish - until pipeline A's
// run_pipeline has RETURNED on another thread. Under a per-pipeline latch A returns as
// soon as its own worker is done and B is then released. Under the pool-wide wait A cannot
// return while B's task is on the pool, B never gets released, and the 10 s guard fires.
//
// Also checked: with drain_pool (the serial path's behaviour, kept) a pipeline still
// returns only when the pool is idle, and the latch is exact for dop > 1.
//
// And ConcatBufferSource, the read side of a union whose legs run concurrently: each leg
// fills a buffer of its own, in whatever order the legs finish, and the source must hand
// them out buffer 0 first, then buffer 1, then buffer 2 (the order the legs had when they
// ran one after another), a leg's own morsels in the order they were appended.
//
// `make pipeline-latch-test` builds and runs it under ASan.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "executor.hpp"
#include "pipeline_buffers.hpp"
#include "bs_pool_bridge.hpp"

using namespace opteryx::engine;

struct GateGlobal : GlobalSourceState {
    std::atomic<int> next{0};
};

// N single-row morsels; after the last, it holds the pipeline open while `hold` is true.
struct GateSource : Source {
    int n;
    const std::atomic<bool>* hold;       // null = never hold
    std::atomic<bool>* started;          // set when the first worker pulls
    std::atomic<bool>* timed_out;
    GateSource(int n_, const std::atomic<bool>* h, std::atomic<bool>* s, std::atomic<bool>* t)
        : n(n_), hold(h), started(s), timed_out(t) {}
    std::unique_ptr<GlobalSourceState> make_global() override { return std::make_unique<GateGlobal>(); }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out, ErrCtx&) override {
        if (started != nullptr) started->store(true);
        if (static_cast<GateGlobal&>(gs).next.fetch_add(1) >= n) {
            if (hold != nullptr) {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (hold->load() && std::chrono::steady_clock::now() < deadline)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                if (hold->load()) timed_out->store(true);
            }
            return SourceResult::FINISHED;
        }
        auto m = std::make_shared<CxxMorsel>();
        m->zero_col_rows = 1;
        out = std::move(m);
        return SourceResult::HAVE_MORE;
    }
};

struct CountSink : Sink {
    std::atomic<int>* rows;
    explicit CountSink(std::atomic<int>* r) : rows(r) {}
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<GlobalSinkState>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<LocalSinkState>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState&, ErrCtx&) override {
        rows->fetch_add(static_cast<int>(in->num_rows()));
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState&, LocalSinkState&, ErrCtx&) override {}
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

static bool a_returns_while_b_is_held(BSThreadPoolBridge* pool) {
    std::atomic<bool> hold_b{true}, b_started{false}, timed_out{false}, a_started{false};
    std::atomic<int> rows_a{0}, rows_b{0};
    GateSource src_a(32, nullptr, &a_started, &timed_out);
    GateSource src_b(32, &hold_b, &b_started, &timed_out);
    CountSink snk_a(&rows_a), snk_b(&rows_b);
    Pipeline pa, pb;
    pa.source = &src_a; pa.sink = &snk_a;
    pb.source = &src_b; pb.sink = &snk_b;

    ErrCtx err_a, err_b;
    std::thread tb([&] { run_pipeline(pb, 1, 1, err_b, pool, nullptr, /*drain_pool=*/false); });
    // B is on the pool and parked inside its source before A starts.
    while (!b_started.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    run_pipeline(pa, 2, 2, err_a, pool, nullptr, /*drain_pool=*/false);
    hold_b.store(false);   // A's run_pipeline has RETURNED: only now may B finish
    tb.join();

    const bool pass = !timed_out.load() && err_a.code == 0 && err_b.code == 0 &&
                      rows_a.load() == 32 && rows_b.load() == 32;
    std::printf("A returns while B holds the pool   %s\n", pass ? "OK" : "*** FAIL ***");
    if (!pass)
        std::printf("  timed_out=%d err_a=%d err_b=%d rows_a=%d rows_b=%d\n", timed_out.load(),
                    err_a.code, err_b.code, rows_a.load(), rows_b.load());
    return pass;
}

static bool exact_for_every_dop(BSThreadPoolBridge* pool, bool drain) {
    bool ok = true;
    for (int dop : {1, 2, 3, 8}) {
        std::atomic<bool> timed_out{false};
        std::atomic<int> rows{0};
        GateSource src(200, nullptr, nullptr, &timed_out);
        CountSink snk(&rows);
        Pipeline p;
        p.source = &src; p.sink = &snk;
        ErrCtx err;
        run_pipeline(p, dop, dop, err, pool, nullptr, drain);
        const bool pass = err.code == 0 && rows.load() == 200;
        if (!pass) std::printf("  dop=%d rows=%d err=%d\n", dop, rows.load(), err.code);
        ok = ok && pass;
    }
    std::printf("every row seen at dop 1..8 (drain_pool=%d)   %s\n", drain ? 1 : 0, ok ? "OK" : "*** FAIL ***");
    return ok;
}

// The few operations ConcatSource needs from a buffer, held in memory.
struct FakeBuffer {
    std::vector<MorselPtr> morsels;
    std::string err = "fake buffer error";
    void append(MorselPtr m) { morsels.push_back(std::move(m)); }
    bool seal() { return true; }
    size_t claim_count() { return morsels.size(); }
    bool get(size_t i, MorselPtr& out) { out = morsels[i]; return true; }
    const std::string& error() const { return err; }
};
using TestConcat = ConcatSource<FakeBuffer>;

static MorselPtr rows_morsel(uint32_t rows) {
    auto m = std::make_shared<CxxMorsel>();
    m->zero_col_rows = rows;
    return m;
}

// Fill the three buffers in an order unrelated to their numbering; read at one worker.
static bool concat_source_keeps_buffer_order() {
    FakeBuffer b0, b1, b2;
    b2.append(rows_morsel(100));
    b1.append(rows_morsel(10));
    b0.append(rows_morsel(1));
    b1.append(rows_morsel(20));
    b0.append(rows_morsel(2));
    TestConcat src({&b0, &b1, &b2});
    auto g = src.make_global();
    auto l = src.make_local(*g);
    std::vector<uint32_t> seen;
    MorselPtr out;
    ErrCtx err;
    while (src.get_morsel(*g, *l, out, err) == SourceResult::HAVE_MORE) seen.push_back(out->num_rows());
    const std::vector<uint32_t> expected{1, 2, 10, 20, 100};
    const bool pass = err.code == 0 && seen == expected;
    std::printf("concat source: buffer order, then append order   %s\n", pass ? "OK" : "*** FAIL ***");
    if (!pass) {
        std::printf("  got:");
        for (uint32_t n : seen) std::printf(" %u", n);
        std::printf("\n");
    }
    return pass;
}

static bool concat_source_with_an_empty_leg_and_many_workers() {
    FakeBuffer b0, b1, b2;
    for (uint32_t i = 1; i <= 50; ++i) b0.append(rows_morsel(i));
    for (uint32_t i = 1; i <= 30; ++i) b2.append(rows_morsel(1000 + i));
    TestConcat src({&b0, &b1, &b2});   // b1 empty
    auto g = src.make_global();
    std::atomic<int> count{0};
    std::atomic<uint64_t> total{0};
    std::vector<std::thread> workers;
    for (int w = 0; w < 6; ++w) {
        workers.emplace_back([&] {
            auto l = src.make_local(*g);
            MorselPtr out;
            ErrCtx err;
            while (src.get_morsel(*g, *l, out, err) == SourceResult::HAVE_MORE) {
                count.fetch_add(1);
                total.fetch_add(out->num_rows());
            }
        });
    }
    for (auto& t : workers) t.join();
    uint64_t want = 0;
    for (uint32_t i = 1; i <= 50; ++i) want += i;
    for (uint32_t i = 1; i <= 30; ++i) want += 1000 + i;
    const bool pass = count.load() == 80 && total.load() == want;
    std::printf("concat source: empty leg, 6 workers claim each once   %s\n", pass ? "OK" : "*** FAIL ***");
    return pass;
}

int main() {
    BSThreadPoolBridge pool(8, "test_pipeline_latch");
    bool ok = true;
    for (int rep = 0; rep < 20; ++rep) ok = a_returns_while_b_is_held(&pool) && ok;
    ok = exact_for_every_dop(&pool, true) && ok;
    ok = exact_for_every_dop(&pool, false) && ok;
    ok = concat_source_keeps_buffer_order() && ok;
    ok = concat_source_with_an_empty_leg_and_many_workers() && ok;
    pool.shutdown();
    std::printf("%s\n", ok ? "PIPELINE LATCH PASS" : "PIPELINE LATCH FAIL");
    return ok ? 0 : 1;
}
