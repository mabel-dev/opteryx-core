#pragma once
// src/cpp/engine/native_range_source.hpp — GENERATE_SERIES over integers, streamed.
//
// The series is a PLAN CONSTANT described by three numbers (start, step, count); no
// row of it exists until a worker asks for it. Each get_morsel() claims the next
// kRangeMorselRows-row slice by atomic and fills one dense INT64 column for it, so a
// billion-row series holds one morsel per worker, never the series. The planner works
// out `count` once (opteryx/utils/series.py: integer_series_extent) and has already
// proved every value fits INT64; nothing here re-decides it.
//
// Slices are claimed, not assigned, so with more than one worker the slices reach the
// sink in no fixed order — the same unordered-morsel-stream contract every other source
// has. Within a slice the values ascend/descend in series order.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "core/alloc.h"          // draken_malloc
#include "core/buffers.h"        // DrakenVector
#include "core/vector_alloc.h"   // draken_vector_from_dense
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "morsels/cxx_morsel.h"  // CxxMorsel, CxxColumn
#include "operator.hpp"

namespace opteryx::engine {

inline constexpr uint32_t kRangeMorselRows = 65536;

struct RangeSource : Source {
    int64_t     start;
    int64_t     step;
    uint64_t    count;   // total rows in the series
    std::string name;    // the one column's identity

    struct Global : GlobalSourceState {
        std::atomic<uint64_t> next_slice{0};
    };

    RangeSource(int64_t start_, int64_t step_, uint64_t count_, std::string name_)
        : start(start_), step(step_), count(count_), name(std::move(name_)) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<Global>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<Global&>(gs);
        const uint64_t first = g.next_slice.fetch_add(1) * uint64_t{kRangeMorselRows};
        if (first >= count) return SourceResult::FINISHED;
        const uint64_t left = count - first;
        const uint32_t n = left < kRangeMorselRows ? static_cast<uint32_t>(left)
                                                   : kRangeMorselRows;

        OwnedBuffer<void> buf(draken_malloc(static_cast<size_t>(n) * sizeof(int64_t)));
        if (!buf) {
            err.code = 1;
            err.msg = "RangeSource: out of memory allocating a GENERATE_SERIES morsel";
            return SourceResult::FINISHED;
        }
        // Unsigned arithmetic: the planner proved every value fits INT64, and two's
        // complement wrap makes a negative step correct without signed-overflow UB.
        int64_t* data = static_cast<int64_t*>(buf.get());
        const uint64_t ustep = static_cast<uint64_t>(step);
        uint64_t v = static_cast<uint64_t>(start) + first * ustep;
        for (uint32_t i = 0; i < n; ++i) {
            data[i] = static_cast<int64_t>(v);
            v += ustep;
        }

        DrakenVector dv = draken_vector_from_dense(buf.get(), n, DRAKEN_INT64, nullptr);
        auto owner = std::make_shared<VectorOwner>(dv, std::move(buf), OwnedBuffer<uint8_t>(nullptr));

        auto m = std::make_shared<CxxMorsel>();
        CxxColumn col;
        col.view = owner->vec;
        col.own  = std::move(owner);
        m->columns.push_back(std::move(col));
        m->names.push_back(name);
        m->zero_col_rows = n;
        out = std::move(m);
        return SourceResult::HAVE_MORE;
    }
};

}  // namespace opteryx::engine
