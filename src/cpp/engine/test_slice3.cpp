// src/cpp/engine/test_slice3.cpp — slice 3 proof: a PIPELINE GRAPH with a dependency.
// A breaker's FINALIZED result becomes the SOURCE of the next pipeline (the structure
// behind GROUP BY -> ORDER BY, build -> probe, breaker chains).
//
//   P1: KeySource (int32 keys) -> GroupedCountSink            [materialises {key,count}]
//   P2: BufferSource (P1's result morsels) -> SumColumnSink   [sums the count column]
//
// The engine runs P1 to completion (parallel), hands its materialised morsels to P2's
// source, runs P2 (parallel). P2's total == P1's total rows (each group's count summed).
// Verified correct + identical across dop=1,2,4,8. Pure C++, real CxxMorsel columns.
//
// Build: same flags as slice 1/2 (c++ -std=c++17 -O2 -pthread -Idraken -Isrc/cpp/engine
//   -Ithird_party/cyan4973 -Ithird_party/utf8h -Ithird_party/fastfloat
//   -Ithird_party/fastfloat/fast_float src/cpp/engine/test_slice3.cpp -o /tmp/test_slice3).

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "executor.hpp"

using namespace opteryx::engine;

// ---- Column construction (test convenience; real columns carry a VectorOwner). --------
struct RawBufs {
    std::vector<void*> bufs;
    ~RawBufs() { for (void* p : bufs) std::free(p); }
    void* keep(void* p) { bufs.push_back(p); return p; }
};

template <typename T>
static CxxColumn make_dense_col(const std::vector<T>& vals, DrakenType type, RawBufs& bufs) {
    uint32_t n = static_cast<uint32_t>(vals.size());
    T* data = static_cast<T*>(bufs.keep(std::malloc(n * sizeof(T))));
    uint32_t* sel = static_cast<uint32_t*>(bufs.keep(std::malloc(n * sizeof(uint32_t))));
    for (uint32_t i = 0; i < n; ++i) { data[i] = vals[i]; sel[i] = i; }
    CxxColumn col;
    col.view.data = data;
    col.view.selection = sel;
    col.view.data_length = n;
    col.view.length = n;
    col.view.validity = nullptr;
    col.view.type = type;
    col.view.flags = DRAKEN_SEL_IDENTITY;
    col.own = nullptr;
    return col;
}

// ---- SOURCE 1: pre-built int32-key morsels, atomic hand-out. ---------------------------
struct AtomicSourceGlobal : GlobalSourceState { std::atomic<size_t> next{0}; };
struct VecSource : Source {
    std::vector<MorselPtr> morsels;
    explicit VecSource(std::vector<MorselPtr> m) : morsels(std::move(m)) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<AtomicSourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        size_t idx = static_cast<AtomicSourceGlobal&>(gs).next.fetch_add(1);
        if (idx >= morsels.size()) return SourceResult::FINISHED;
        out = morsels[idx];
        return SourceResult::HAVE_MORE;
    }
};

// ---- SINK 1: grouped COUNT on col 0; finalize MATERIALISES {key:int32, count:int64}. ---
struct GCLocal : LocalSinkState { std::unordered_map<int32_t, long long> m; };
struct GCGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int32_t, long long> result;
    std::vector<MorselPtr> materialised;  // produced by finalize
    RawBufs bufs;
};
struct GroupedCountSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<GCGlobal>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override { return std::make_unique<GCLocal>(); }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx&) override {
        const CxxColumn& col = in->columns[0];
        const int32_t* data = static_cast<const int32_t*>(col.view.data);
        const uint32_t* sel = col.view.selection;
        auto& m = static_cast<GCLocal&>(ls).m;
        for (uint32_t i = 0; i < col.view.length; ++i) m[data[sel[i]]] += 1;
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<GCGlobal&>(gs);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& kv : static_cast<GCLocal&>(ls).m) g.result[kv.first] += kv.second;
    }
    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<GCGlobal&>(gs);
        std::vector<int32_t> keys;
        std::vector<long long> counts;
        keys.reserve(g.result.size());
        counts.reserve(g.result.size());
        for (auto& kv : g.result) { keys.push_back(kv.first); counts.push_back(kv.second); }
        auto m = std::make_shared<CxxMorsel>();
        m->columns.push_back(make_dense_col<int32_t>(keys, DRAKEN_INT32, g.bufs));
        m->columns.push_back(make_dense_col<long long>(counts, DRAKEN_INT64, g.bufs));
        m->names = {"k", "c"};
        g.materialised.push_back(std::move(m));
    }
};

// ---- SINK 2: sum an int64 column (col index `which`). ----------------------------------
struct SumLocal : LocalSinkState { long long s = 0; };
struct SumGlobal : GlobalSinkState { std::atomic<long long> total{0}; long long result = -1; };
struct SumColumnSink : Sink {
    size_t which;
    explicit SumColumnSink(size_t w) : which(w) {}
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<SumGlobal>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override { return std::make_unique<SumLocal>(); }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx&) override {
        const CxxColumn& col = in->columns[which];
        const long long* data = static_cast<const long long*>(col.view.data);
        const uint32_t* sel = col.view.selection;
        long long s = 0;
        for (uint32_t i = 0; i < col.view.length; ++i) s += data[sel[i]];
        static_cast<SumLocal&>(ls).s += s;
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        static_cast<SumGlobal&>(gs).total += static_cast<SumLocal&>(ls).s;
    }
    void finalize(GlobalSinkState& gs, ErrCtx&) override {
        auto& g = static_cast<SumGlobal&>(gs);
        g.result = g.total.load();
    }
};

int main() {
    const int N = 2000, R = 1000, K = 37;
    RawBufs src_bufs;
    std::vector<MorselPtr> morsels;
    long long total_rows = 0;
    for (int mi = 0; mi < N; ++mi) {
        std::vector<int32_t> keys(R);
        for (int r = 0; r < R; ++r) {
            keys[r] = static_cast<int32_t>((static_cast<long long>(mi) * R + r) % K);
            ++total_rows;
        }
        auto m = std::make_shared<CxxMorsel>();
        m->columns.push_back(make_dense_col<int32_t>(keys, DRAKEN_INT32, src_bufs));
        m->names = {"k"};
        morsels.push_back(std::move(m));
    }

    bool ok = true;
    for (int dop : {1, 2, 4, 8}) {
        // ---- Pipeline 1: KeySource -> GroupedCountSink (the breaker) ----
        VecSource src1(morsels);
        GroupedCountSink agg;
        Pipeline p1; p1.source = &src1; p1.sink = &agg;
        ErrCtx e1;
        auto g1 = run_pipeline(p1, dop, e1);

        // ---- Dependency hand-off: P1's materialised result IS P2's source ----
        auto& gg = *static_cast<GCGlobal*>(g1.get());

        // ---- Pipeline 2: BufferSource(P1 result) -> SumColumnSink(count col) ----
        VecSource src2(gg.materialised);
        SumColumnSink sum(1);  // col 1 == count
        Pipeline p2; p2.source = &src2; p2.sink = &sum;
        ErrCtx e2;
        auto g2 = run_pipeline(p2, dop, e2);
        long long result = static_cast<SumGlobal*>(g2.get())->result;

        bool pass = (e1.code == 0) && (e2.code == 0) && (gg.result.size() == (size_t)K)
                    && (result == total_rows);
        std::printf("dop=%d P1.groups=%zu P2.sum=%lld expected=%lld %s\n",
                    dop, gg.result.size(), result, total_rows, pass ? "OK" : "*** FAIL ***");
        ok = ok && pass;
    }
    std::printf("%s\n", ok ? "SLICE 3 PASS" : "SLICE 3 FAIL");
    return ok ? 0 : 1;
}
