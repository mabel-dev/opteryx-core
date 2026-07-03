#pragma once
// src/cpp/engine/pipeline_buffers.hpp — breaker -> dependent-pipeline hand-off.
//
// A breaker's sink finalize() (single-threaded, after all combines) materializes its
// result morsels into a MorselBuffer; the dependent pipeline's BufferSource hands
// them out to workers by atomic claim (the same dynamic assignment as every other
// Source). Split out of engine.hpp so breaker sinks (native_sort.hpp) can write into
// buffers without a circular include.

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "operator.hpp"

namespace opteryx::engine {

struct MorselBuffer {
    std::vector<MorselPtr> morsels;   // written only in finalize (single-threaded)
    std::atomic<size_t> cursor{0};    // read-side claim
};

// Append-through sink: streams every morsel into a (possibly SHARED) MorselBuffer.
// UNION ALL is exactly this — each leg's pipeline appends into one buffer, the
// dependent pipeline reads the union. Local accumulate is lock-free; the one mutex
// touch is per worker at combine. Order across legs is unspecified (UNION ALL
// semantics; an ORDER BY above restores determinism).
struct BufferAppendGlobal : GlobalSinkState {
    std::mutex mtx;
};
struct BufferAppendSink : Sink {
    MorselBuffer* out;
    explicit BufferAppendSink(MorselBuffer* b) : out(b) {}
    struct Local : LocalSinkState { std::vector<MorselPtr> morsels; };
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<BufferAppendGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<Local>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<Local&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<BufferAppendGlobal&>(gs);
        auto& l = static_cast<Local&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) out->morsels.push_back(std::move(m));
    }
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

struct BufferSource : Source {
    MorselBuffer* buf;
    explicit BufferSource(MorselBuffer* b) : buf(b) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<GlobalSourceState>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState&, LocalSourceState&, MorselPtr& out,
                            ErrCtx&) override {
        size_t idx = buf->cursor.fetch_add(1);
        if (idx >= buf->morsels.size()) return SourceResult::FINISHED;
        out = buf->morsels[idx];
        return SourceResult::HAVE_MORE;
    }
};

}  // namespace opteryx::engine
