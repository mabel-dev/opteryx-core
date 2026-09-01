#pragma once
// src/cpp/engine/runtime_bound.hpp — a run-time-discovered ORDINAL range bound,
// captured on a join's build side and consumed by a probe-side scan's row-group
// zone map. See docs/RUNTIME_MINMAX_FILTER_DESIGN.md.
//
// This type is deliberately NEUTRAL: the join writes it, the scan reads it, and
// neither header includes the other. Engine::run() owns the slots and performs
// the one copy, at the same point it publishes the build table (the Join2Ref
// handoff) — which is the point at which the build pipeline has completed and the
// probe pipeline's GlobalSourceState has not yet been constructed.
//
// THE ORDINAL SPACE IS DRAKEN'S, NOT A NEW ONE. `lo`/`hi` are
// draken_ordinalize keys (draken/ops/hash.h), which is the SAME space
// skene::compute_statistics writes its min_ordinal/max_ordinal in
// (skene/src/statistics.cpp) and the same space Manifest._ordinalize_literal
// produces plan-time zone terms in. A second "value -> int64" mapping here would
// be the exact failure ordinal_zone_map_terms warns about.
//
// `valid == 0` is the honest default and it means PRUNES NOTHING, never "prunes
// everything". A build side that produced no non-null keys, a key type with no
// ordinal, or a bound that never got filled all land here, and all of them cost a
// read rather than an answer. (An empty build side does make an INNER join emit
// nothing, but that is an emptiness optimisation with its own soundness argument
// and it belongs in the join, not in the scan.)

#include <cstdint>
#include <string>
#include <vector>

namespace opteryx::engine {

struct RuntimeKeyBound {
    int64_t lo = 0;
    int64_t hi = 0;
    uint8_t valid = 0;   // 0 = unusable -> prunes nothing
};

// The per-scan collection of bounds, appended at PLAN time (after the Source was
// constructed — the probe scan is compiled before the join that supplies the
// bound has finished wiring) and read at RUN time in the scan's make_global /
// first-worker init, by which point Engine::run() has published the values.
//
// Lives here rather than in either scan's header because both consumers need the
// identical shape: two copies would be free to drift in what "parallel" means.
// Owned by the Source (not borrowed): a handful of strings and pointers, empty
// for every scan the compiler did not find eligible, which is the overwhelming
// majority.
struct RuntimeBoundSet {
    std::vector<std::string>            columns;   // physical (in-file) names
    std::vector<const RuntimeKeyBound*> bounds;    // parallel; engine-owned

    bool empty() const { return columns.empty(); }
    size_t size() const { return columns.size(); }
};

}  // namespace opteryx::engine
