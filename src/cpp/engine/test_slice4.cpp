// src/cpp/engine/test_slice4.cpp — slice 4 proof: HASH JOIN.
// Build pipeline's Sink builds a shared hash table (finalize publishes it, read-only);
// probe pipeline's Operator looks up each probe row and emits matched (build_val,
// probe_val) pairs. Inner equi-join, one build key may match multiple probe rows and
// vice versa (fan-out both ways) -> exercises OpResult::HAVE_MORE (an operator producing
// MULTIPLE outputs from ONE input).
//
//   P1 (build): BuildSource(int32 key, int32 val)   -> HashBuildSink   [finalize: hash map]
//   P2 (probe): ProbeSource(int32 key, int32 pval)  -> JoinOperator -> MatchCountSink
//
// Verified: match count + checksum identical across dop=1,2,4,8, and equal to a serial
// nested-loop reference. Pure C++, real CxxMorsel columns.
//
// Build: same flags as slice 1-3, src/cpp/engine/test_slice4.cpp -o /tmp/test_slice4.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "executor.hpp"

using namespace opteryx::engine;

// ---- Column construction helper (shared shape with slice 3). ---------------------------
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
    col.view.data = data; col.view.selection = sel;
    col.view.data_length = n; col.view.length = n;
    col.view.validity = nullptr; col.view.type = type; col.view.flags = DRAKEN_SEL_IDENTITY;
    col.own = nullptr;
    return col;
}

// ---- Generic vector-backed source (shared by build + probe sides). ---------------------
struct AtomicSourceGlobal : GlobalSourceState { std::atomic<size_t> next{0}; };
struct VecSource : Source {
    std::vector<MorselPtr> morsels;
    explicit VecSource(std::vector<MorselPtr> m) : morsels(std::move(m)) {}
    std::unique_ptr<GlobalSourceState> make_global() override { return std::make_unique<AtomicSourceGlobal>(); }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override { return std::make_unique<LocalSourceState>(); }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out, ErrCtx&) override {
        size_t idx = static_cast<AtomicSourceGlobal&>(gs).next.fetch_add(1);
        if (idx >= morsels.size()) return SourceResult::FINISHED;
        out = morsels[idx];
        return SourceResult::HAVE_MORE;
    }
};

// ---- BUILD SINK: accumulates (key -> vector<val>) thread-locally, merges into a shared
//      multimap at combine, no-op finalize (the map IS the result — probe reads it
//      read-only once the build pipeline has fully run + joined). --------------------------
struct BuildLocal : LocalSinkState {
    std::unordered_map<int32_t, std::vector<int32_t>> m;
};
struct BuildGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int32_t, std::vector<int32_t>> table;  // key -> [build_val...]
};
struct HashBuildSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<BuildGlobal>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override { return std::make_unique<BuildLocal>(); }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx&) override {
        const CxxColumn& kcol = in->columns[0];
        const CxxColumn& vcol = in->columns[1];
        const int32_t* kd = static_cast<const int32_t*>(kcol.view.data);
        const int32_t* vd = static_cast<const int32_t*>(vcol.view.data);
        const uint32_t* ksel = kcol.view.selection;
        const uint32_t* vsel = vcol.view.selection;
        auto& m = static_cast<BuildLocal&>(ls).m;
        for (uint32_t i = 0; i < kcol.view.length; ++i)
            m[kd[ksel[i]]].push_back(vd[vsel[i]]);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<BuildGlobal&>(gs);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (auto& kv : static_cast<BuildLocal&>(ls).m) {
            auto& dst = g.table[kv.first];
            dst.insert(dst.end(), kv.second.begin(), kv.second.end());
        }
    }
    void finalize(GlobalSinkState&, ErrCtx&) override { /* table is the result */ }
};

// ---- JOIN OPERATOR: probes the shared (read-only) hash table. Fan-out via HAVE_MORE — one
//      probe row with M build matches emits M morsels (one match per call), the engine
//      re-driving execute() with the SAME input until EMIT/NEED_INPUT. ---------------------
struct JoinOpState : OperatorState {
    MorselPtr pending_in;   // the probe morsel currently being expanded
    uint32_t row = 0;       // next probe row to expand
    size_t match_idx = 0;   // next match within the current row's build-vals
};
struct JoinOperator : Operator {
    const std::unordered_map<int32_t, std::vector<int32_t>>* table;  // shared, read-only
    explicit JoinOperator(const std::unordered_map<int32_t, std::vector<int32_t>>* t) : table(t) {}

    std::unique_ptr<OperatorState> make_state() override { return std::make_unique<JoinOpState>(); }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out, ErrCtx&) override {
        auto& st = static_cast<JoinOpState&>(st_);
        // New input morsel: reset cursor.
        if (st.pending_in != in) { st.pending_in = in; st.row = 0; st.match_idx = 0; }

        const CxxColumn& kcol = in->columns[0];   // probe key
        const CxxColumn& vcol = in->columns[1];   // probe val
        const int32_t* kd = static_cast<const int32_t*>(kcol.view.data);
        const int32_t* vd = static_cast<const int32_t*>(vcol.view.data);
        const uint32_t* ksel = kcol.view.selection;
        const uint32_t* vsel = vcol.view.selection;
        uint32_t n = kcol.view.length;

        std::vector<int32_t> out_build, out_probe;
        out_build.reserve(4);
        out_probe.reserve(4);

        while (st.row < n) {
            int32_t key = kd[ksel[st.row]];
            int32_t pval = vd[vsel[st.row]];
            auto it = table->find(key);
            if (it == table->end()) { ++st.row; st.match_idx = 0; continue; }
            const std::vector<int32_t>& matches = it->second;
            while (st.match_idx < matches.size()) {
                out_build.push_back(matches[st.match_idx]);
                out_probe.push_back(pval);
                ++st.match_idx;
                // Cap a single emitted morsel's size so HAVE_MORE genuinely re-enters
                // (proves the fan-out path, not just one giant morsel).
                if (out_build.size() >= 8) {
                    out = std::make_shared<CxxMorsel>();
                    out->columns.push_back(make_owned_col(out_build));
                    out->columns.push_back(make_owned_col(out_probe));
                    out->names = {"bv", "pv"};
                    return OpResult::HAVE_MORE;
                }
            }
            st.match_idx = 0;
            ++st.row;
        }
        if (!out_build.empty()) {
            out = std::make_shared<CxxMorsel>();
            out->columns.push_back(make_owned_col(out_build));
            out->columns.push_back(make_owned_col(out_probe));
            out->names = {"bv", "pv"};
            return OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }

    // Test-only allocation: malloc'd buffers, intentionally never freed (process-lifetime
    // test binary). Real Operators build output columns via VectorOwner (RAII, freed when
    // the morsel's shared_ptr refcount drops).
    static CxxColumn make_owned_col(const std::vector<int32_t>& vals) {
        uint32_t n = static_cast<uint32_t>(vals.size());
        int32_t* data = static_cast<int32_t*>(std::malloc(n * sizeof(int32_t)));
        uint32_t* sel = static_cast<uint32_t*>(std::malloc(n * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) { data[i] = vals[i]; sel[i] = i; }
        CxxColumn col;
        col.view.data = data; col.view.selection = sel;
        col.view.data_length = n; col.view.length = n;
        col.view.validity = nullptr; col.view.type = DRAKEN_INT32; col.view.flags = DRAKEN_SEL_IDENTITY;
        col.own = nullptr;
        return col;
    }
};

// ---- MATCH-COUNT SINK: counts matched rows + checksums (build_val*1000003 + probe_val). -
struct MCLocal : LocalSinkState { long long count = 0; long long checksum = 0; };
struct MCGlobal : GlobalSinkState { std::atomic<long long> count{0}; std::atomic<long long> checksum{0}; };
struct MatchCountSink : Sink {
    std::unique_ptr<GlobalSinkState> make_global() override { return std::make_unique<MCGlobal>(); }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override { return std::make_unique<MCLocal>(); }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx&) override {
        const CxxColumn& bcol = in->columns[0];
        const CxxColumn& pcol = in->columns[1];
        const int32_t* bd = static_cast<const int32_t*>(bcol.view.data);
        const int32_t* pd = static_cast<const int32_t*>(pcol.view.data);
        const uint32_t* bsel = bcol.view.selection;
        const uint32_t* psel = pcol.view.selection;
        auto& l = static_cast<MCLocal&>(ls);
        for (uint32_t i = 0; i < bcol.view.length; ++i) {
            l.count += 1;
            l.checksum += static_cast<long long>(bd[bsel[i]]) * 1000003LL + pd[psel[i]];
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<MCGlobal&>(gs);
        auto& l = static_cast<MCLocal&>(ls);
        g.count += l.count;
        g.checksum += l.checksum;
    }
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

int main() {
    const int BUILD_N = 500, BUILD_R = 200;   // build rows = 100000, keys in [0,BK)
    const int BK = 50;
    const int PROBE_N = 800, PROBE_R = 250;   // probe rows = 200000, keys in [0,BK*2) (half miss)

    RawBufs bbufs, pbufs;
    std::vector<MorselPtr> build_morsels, probe_morsels;

    // Build side: key = i % BK, val = i (unique per row).
    long long bi = 0;
    for (int m = 0; m < BUILD_N; ++m) {
        std::vector<int32_t> keys(BUILD_R), vals(BUILD_R);
        for (int r = 0; r < BUILD_R; ++r) { keys[r] = static_cast<int32_t>(bi % BK); vals[r] = static_cast<int32_t>(bi); ++bi; }
        auto mm = std::make_shared<CxxMorsel>();
        mm->columns.push_back(make_dense_col<int32_t>(keys, DRAKEN_INT32, bbufs));
        mm->columns.push_back(make_dense_col<int32_t>(vals, DRAKEN_INT32, bbufs));
        mm->names = {"k", "v"};
        build_morsels.push_back(std::move(mm));
    }
    // Probe side: key = i % (BK*2) (half the keys never match), val = i.
    long long pi = 0;
    for (int m = 0; m < PROBE_N; ++m) {
        std::vector<int32_t> keys(PROBE_R), vals(PROBE_R);
        for (int r = 0; r < PROBE_R; ++r) { keys[r] = static_cast<int32_t>(pi % (BK * 2)); vals[r] = static_cast<int32_t>(pi); ++pi; }
        auto mm = std::make_shared<CxxMorsel>();
        mm->columns.push_back(make_dense_col<int32_t>(keys, DRAKEN_INT32, pbufs));
        mm->columns.push_back(make_dense_col<int32_t>(vals, DRAKEN_INT32, pbufs));
        mm->names = {"k", "v"};
        probe_morsels.push_back(std::move(mm));
    }

    // ---- Serial reference (nested-loop equivalent via plain maps, no engine). ----
    std::unordered_map<int32_t, std::vector<int32_t>> ref_table;
    for (long long i = 0; i < bi; ++i) ref_table[static_cast<int32_t>(i % BK)].push_back(static_cast<int32_t>(i));
    long long ref_count = 0, ref_checksum = 0;
    for (long long i = 0; i < pi; ++i) {
        int32_t key = static_cast<int32_t>(i % (BK * 2));
        auto it = ref_table.find(key);
        if (it == ref_table.end()) continue;
        for (int32_t bv : it->second) {
            ref_count += 1;
            ref_checksum += static_cast<long long>(bv) * 1000003LL + static_cast<int32_t>(i);
        }
    }

    bool ok = true;
    for (int dop : {1, 2, 4, 8}) {
        // ---- Build pipeline ----
        VecSource bsrc(build_morsels);
        HashBuildSink bsink;
        Pipeline bp; bp.source = &bsrc; bp.sink = &bsink;
        ErrCtx be;
        auto bg = run_pipeline(bp, dop, be);
        auto& table = static_cast<BuildGlobal*>(bg.get())->table;

        // ---- Probe pipeline (join Operator reads the finalized build table) ----
        VecSource psrc(probe_morsels);
        JoinOperator join(&table);
        MatchCountSink msink;
        Pipeline pp; pp.source = &psrc; pp.operators = {&join}; pp.sink = &msink;
        ErrCtx pe;
        auto pg = run_pipeline(pp, dop, pe);
        auto& mg = *static_cast<MCGlobal*>(pg.get());

        bool pass = (be.code == 0) && (pe.code == 0)
                    && (mg.count.load() == ref_count) && (mg.checksum.load() == ref_checksum);
        std::printf("dop=%d matches=%lld expected=%lld checksum_ok=%s %s\n",
                    dop, (long long)mg.count.load(), ref_count,
                    (mg.checksum.load() == ref_checksum) ? "yes" : "no",
                    pass ? "OK" : "*** FAIL ***");
        ok = ok && pass;
    }
    std::printf("%s\n", ok ? "SLICE 4 PASS" : "SLICE 4 FAIL");
    return ok ? 0 : 1;
}
