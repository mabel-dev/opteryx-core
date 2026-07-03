// src/cpp/engine/test_slice2.cpp — slice 2 proof: the AGGREGATE SINK (the first breaker).
//
// Pipeline: KeySource (N morsels, each an int32 key column) -> GroupedCountSink.
// The morsel-driven breaker contract: each worker accumulates into a thread-LOCAL hash
// map (lock-free), then combine() merges its local into the shared global (one lock per
// worker), then finalize(). Verifies the grouped counts are CORRECT and IDENTICAL across
// dop=1,2,4,8 (deterministic — count is associative/commutative so merge order is
// irrelevant). Pure C++, no Python. Real CxxMorsel columns (int32 DrakenVector).
//
// Build (from repo root):
//   c++ -std=c++17 -O2 -pthread -Idraken -Isrc/cpp/engine -Ithird_party/cyan4973 \
//       -Ithird_party/utf8h -Ithird_party/fastfloat -Ithird_party/fastfloat/fast_float \
//       src/cpp/engine/test_slice2.cpp -o /tmp/test_slice2 && /tmp/test_slice2

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

// ---- Build a dense int32 column morsel (real CxxMorsel/DrakenVector). The buffers are
//      owned by the source (freed in its dtor); CxxColumn.own == nullptr (no RAII here —
//      a test convenience; real columns carry a VectorOwner). -----------------------------
struct RawBufs {
    std::vector<int32_t*> datas;
    std::vector<uint32_t*> sels;
    ~RawBufs() {
        for (auto* d : datas) std::free(d);
        for (auto* s : sels) std::free(s);
    }
};

static MorselPtr make_int32_morsel(const std::vector<int32_t>& keys, RawBufs& bufs) {
    uint32_t n = static_cast<uint32_t>(keys.size());
    auto* data = static_cast<int32_t*>(std::malloc(n * sizeof(int32_t)));
    auto* sel = static_cast<uint32_t*>(std::malloc(n * sizeof(uint32_t)));
    for (uint32_t i = 0; i < n; ++i) { data[i] = keys[i]; sel[i] = i; }
    bufs.datas.push_back(data);
    bufs.sels.push_back(sel);

    auto m = std::make_shared<CxxMorsel>();
    CxxColumn col;
    col.view.data = data;
    col.view.selection = sel;
    col.view.data_length = n;
    col.view.length = n;
    col.view.validity = nullptr;            // all valid
    col.view.type = DRAKEN_INT32;
    col.view.flags = DRAKEN_SEL_IDENTITY;   // dense
    col.own = nullptr;                      // test owns the bytes (RawBufs)
    m->columns.push_back(std::move(col));
    m->names.push_back("k");
    return m;
}

// ---- SOURCE: pre-built int32-key morsels, handed out by atomic claim. ------------------
struct KeySourceGlobal : GlobalSourceState { std::atomic<size_t> next{0}; };
struct KeySource : Source {
    std::vector<MorselPtr> morsels;
    explicit KeySource(std::vector<MorselPtr> m) : morsels(std::move(m)) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<KeySourceGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        size_t idx = static_cast<KeySourceGlobal&>(gs).next.fetch_add(1);
        if (idx >= morsels.size()) return SourceResult::FINISHED;
        out = morsels[idx];
        return SourceResult::HAVE_MORE;
    }
};

// ---- SINK: grouped COUNT(*) on column 0. Local map per worker -> combine -> finalize. --
struct GCLocal : LocalSinkState { std::unordered_map<int32_t, long long> m; };
struct GCGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int32_t, long long> result;
};
struct GroupedCountSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GCGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<GCLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        const CxxColumn& col = in->columns[0];
        const int32_t* data = static_cast<const int32_t*>(col.view.data);
        const uint32_t* sel = col.view.selection;
        uint32_t n = col.view.length;
        auto& m = static_cast<GCLocal&>(ls).m;
        for (uint32_t i = 0; i < n; ++i) m[data[sel[i]]] += 1;  // data[selection[i]]
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<GCGlobal&>(gs);
        auto& lm = static_cast<GCLocal&>(ls).m;
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& kv : lm) g.result[kv.first] += kv.second;
    }
    void finalize(GlobalSinkState&, ErrCtx&) override { /* result already merged */ }
};

int main() {
    const int N = 2000;          // morsels
    const int R = 1000;          // rows per morsel
    const int K = 37;            // distinct keys

    RawBufs bufs;
    std::vector<MorselPtr> morsels;
    std::map<int32_t, long long> reference;  // ordered, for deterministic compare
    long long total_rows = 0;
    for (int mi = 0; mi < N; ++mi) {
        std::vector<int32_t> keys(R);
        for (int r = 0; r < R; ++r) {
            int32_t k = static_cast<int32_t>((static_cast<long long>(mi) * R + r) % K);
            keys[r] = k;
            reference[k] += 1;
            ++total_rows;
        }
        morsels.push_back(make_int32_morsel(keys, bufs));
    }

    bool ok = true;
    for (int dop : {1, 2, 4, 8}) {
        KeySource src(morsels);
        GroupedCountSink snk;
        Pipeline p;
        p.source = &src;
        p.sink = &snk;

        ErrCtx err;
        auto gsink = run_pipeline(p, dop, err);
        auto& res = static_cast<GCGlobal*>(gsink.get())->result;

        // compare against the serial reference
        bool pass = (err.code == 0) && (res.size() == reference.size());
        long long sum = 0;
        for (auto& kv : reference) {
            auto it = res.find(kv.first);
            if (it == res.end() || it->second != kv.second) { pass = false; break; }
        }
        for (auto& kv : res) sum += kv.second;
        if (sum != total_rows) pass = false;
        std::printf("dop=%d groups=%zu total=%lld expected_total=%lld %s\n",
                    dop, res.size(), sum, total_rows, pass ? "OK" : "*** FAIL ***");
        ok = ok && pass;
    }
    std::printf("%s\n", ok ? "SLICE 2 PASS" : "SLICE 2 FAIL");
    return ok ? 0 : 1;
}
