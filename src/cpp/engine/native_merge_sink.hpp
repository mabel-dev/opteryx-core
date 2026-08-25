#pragma once
// src/cpp/engine/native_merge_sink.hpp — MERGE INTO's per-row work.
//
// MERGE's classification and blending already happened natively in the
// projection above this (see opteryx/planner/logical_planner/merge_desugar.py).
// What remains per row is narrow and mechanical:
//
//   * every MATCHED row's address is recorded, and a second act on the same
//     address is the SQL cardinality violation;
//   * rows whose action retires the old version contribute a delete position;
//   * rows whose action writes a new version are gathered for the writer.
//
// It lives here rather than in the Cython sink because the sink receives
// Cxx-backed morsels, whose values are only readable through the C++ substrate
// (`columns[i].view`, uniform data[selection[i]]). Reading them as PyObjects is
// refused by design — see Morsel._ensure_pyobject. Doing the row loop here also
// keeps the whole per-row path GIL-free; Python is crossed ONCE, at end of
// stream, to hand the accumulated addresses to the catalog commit.
//
// Addresses are (file index, file-local ordinal). The file index refers to the
// scan's own ordered file list; the sink maps it back to a path.

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"
#include "engine/native_roaring32.hpp"
#include "morsels/cxx_morsel.h"

namespace opteryx::engine {

// Action codes, kept in step with merge_desugar.py's MERGE_* constants. A
// mismatch between the two would silently mis-route rows, so they are named
// here rather than open-coded at the comparison sites.
enum : int64_t {
    kMergeNoop   = 0,
    kMergeInsert = 1,
    kMergeUpdate = 2,
    kMergeDelete = 3,
};

// MERGE's address sets charge their OWN budget, not the one CIDR_AGG's per-group
// sets charge. The two are unrelated workloads that can run in the same process,
// and a shared counter would make a merge fail for reasons in someone else's
// query — with a message that could not honestly say why.
using MergeRoaring = opteryx::roaring32::Roaring32T<opteryx::roaring32::MergeAddressBudget>;

// What one MERGE statement accumulates. Lives for the statement, not the morsel.
//
// The address sets are ROARING BITMAPS, not hash sets, and that choice is what
// lets MERGE run without a row cap. Ordinals are dense row positions within a
// file — the case roaring is built for. A hash set costs ~40-56 bytes per
// matched row, so it was the statement's real ceiling and the row cap existed
// to keep it reachable. A Roaring32 is bounded BY CONSTRUCTION at ~512MB per
// set however many rows feed it, charges the shared set budget on growth, and
// latches `overflowed` rather than truncating — so the limit is now a memory
// bound that fails loud, which is the honest thing to bound.
struct MergeAddressState {
    // Every matched target row, whatever its arm decided. Tracking MATCHED
    // rather than merely retired rows is what makes the cardinality check
    // complete: a target row matched twice where both arms yielded NOOP is
    // still a violation, and contributes no delete position to notice it by.
    std::unordered_map<int64_t, MergeRoaring> matched;
    // The subset whose old version is retired — UPDATE and DELETE alike.
    std::unordered_map<int64_t, MergeRoaring> retired;

    int64_t rows_inserted = 0;
    int64_t rows_updated  = 0;
    int64_t rows_deleted  = 0;

    // Set when split() reports a cardinality violation, so the caller can name
    // the row in the error without re-scanning for it.
    int64_t violation_file    = -1;
    int64_t violation_ordinal = -1;
};

// Read a fixed-width integer column as int64 through the uniform access path.
// MERGE's three control columns are INT64 by construction (the action is a
// small literal, both address halves are declared INT64 in the Scan's schema),
// so a narrower physical width means the plan drifted from the schema rather
// than a case to accommodate.
inline bool merge_read_i64(const DrakenVector& v, uint32_t row, int64_t& out) {
    if (v.type != DRAKEN_INT64) return false;
    out = static_cast<const int64_t*>(v.data)[v.selection[row]];
    return true;
}

inline bool merge_row_is_valid(const DrakenVector& v, uint32_t row) {
    if (v.validity == nullptr) return true;  // absent bitmap => all valid
    return (v.validity[row >> 3] >> (row & 7)) & 1u;
}

// Status codes. Returned rather than thrown: Cython cdef methods cannot
// propagate C++ exceptions, so the chain uses the status-code model the rest of
// the engine uses (see cxx_morsel.h's ErrCtx note).
enum : int {
    kMergeOk              = 0,
    kMergeCardinality     = 1,  // a target row was matched more than once
    kMergeBadColumnType   = 2,  // a control column is not INT64
    kMergeMissingOrdinal  = 3,  // a matched row carried a file but no ordinal
    kMergeAddressTooLarge = 4,  // an ordinal outside the addressable range
    kMergeBudget          = 5,  // the address set breached the shared set budget
};

// Roaring addresses values as uint32. A data file holding more than 4.29 billion
// rows is not something this engine produces or reads, so an ordinal past that
// is a corrupt address rather than a size to accommodate — refuse it instead of
// truncating it into a different row's coordinate.
constexpr int64_t kMaxAddressableOrdinal = 0xFFFFFFFFLL;

// Split ONE morsel. Appends the row indices that must be written to
// `write_rows` (not cleared here — the caller owns it) and folds every address
// into `st`.
//
// A row is matched iff its `$file` is non-NULL: the outer join leaves both
// address halves NULL for an unmatched source row, and that is the only
// discriminator that survives a NULL join key on either side.
inline int merge_split_morsel(const CxxMorsel& m,
                              int32_t action_idx,
                              int32_t file_idx,
                              int32_t ordinal_idx,
                              MergeAddressState& st,
                              std::vector<int32_t>& write_rows) {
    const DrakenVector& actions  = m.columns[static_cast<size_t>(action_idx)].view;
    const DrakenVector& files    = m.columns[static_cast<size_t>(file_idx)].view;
    const DrakenVector& ordinals = m.columns[static_cast<size_t>(ordinal_idx)].view;

    const uint32_t n = actions.length;
    for (uint32_t i = 0; i < n; ++i) {
        int64_t action = kMergeNoop;
        if (!merge_read_i64(actions, i, action)) return kMergeBadColumnType;

        const bool matched = merge_row_is_valid(files, i);
        if (matched) {
            int64_t file = 0;
            int64_t ordinal = 0;
            if (!merge_read_i64(files, i, file)) return kMergeBadColumnType;
            if (!merge_row_is_valid(ordinals, i)) return kMergeMissingOrdinal;
            if (!merge_read_i64(ordinals, i, ordinal)) return kMergeBadColumnType;

            if (ordinal < 0 || ordinal > kMaxAddressableOrdinal) {
                st.violation_file = file;
                st.violation_ordinal = ordinal;
                return kMergeAddressTooLarge;
            }
            const uint32_t addr = static_cast<uint32_t>(ordinal);

            // `add` reports only budget refusal, so novelty is read off the
            // cardinality: it moves iff the value was not already present.
            auto& seen = st.matched[file];
            const uint64_t before = seen.cardinality();
            if (!seen.add(addr)) return kMergeBudget;
            if (seen.cardinality() == before) {
                st.violation_file = file;
                st.violation_ordinal = ordinal;
                return kMergeCardinality;
            }
            if (action == kMergeUpdate || action == kMergeDelete) {
                if (!st.retired[file].add(addr)) return kMergeBudget;
            }
        }

        switch (action) {
            case kMergeInsert:
                ++st.rows_inserted;
                write_rows.push_back(static_cast<int32_t>(i));
                break;
            case kMergeUpdate:
                ++st.rows_updated;
                write_rows.push_back(static_cast<int32_t>(i));
                break;
            case kMergeDelete:
                ++st.rows_deleted;
                break;
            default:
                break;  // NOOP costs nothing: no address retired, no row written
        }
    }
    return kMergeOk;
}

// ---- EOS readout -----------------------------------------------------------
// The ONE crossing into Python: two flat vectors instead of a per-row callback,
// so the address set stays native for the whole statement and becomes Python
// exactly once, when the commit needs it.

inline std::vector<int64_t> merge_retired_files(const MergeAddressState& st) {
    std::vector<int64_t> out;
    out.reserve(st.retired.size());
    for (const auto& kv : st.retired) out.push_back(kv.first);
    return out;
}

// Sorted: `merge_commit` merges these into a file's existing delete vector, and
// a sorted list keeps that merge (and the sidecar it writes) in order without
// the caller sorting a Python list per file.
// Ascending by construction: roaring stores values in key order within sorted
// /16 containers, so the emit walk is already sorted and the caller never sorts
// a per-file list. `merge_commit` merges these into a file's existing delete
// vector, which wants them in order.
inline std::vector<int64_t> merge_retired_ordinals(const MergeAddressState& st,
                                                   int64_t file) {
    std::vector<int64_t> out;
    auto it = st.retired.find(file);
    if (it == st.retired.end()) return out;
    const auto& bm = it->second;
    out.reserve(static_cast<size_t>(bm.cardinality()));
    // `keys` is kept sorted and each container's own lane is ordered, so the
    // walk emits ascending without a sort. Mirrors the CIDR emit pass.
    for (size_t k = 0; k < bm.keys.size(); ++k) {
        const int64_t high = static_cast<int64_t>(bm.keys[k]) << 16;
        const opteryx::roaring32::Container& c = bm.conts[bm.slots[k]];
        if (c.bitmap) {
            for (uint32_t w = 0; w < opteryx::roaring32::kBitmapWords; ++w) {
                uint64_t word = c.words[w];
                while (word) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(word));
                    out.push_back(high + (static_cast<int64_t>(w) << 6) + bit);
                    word &= word - 1;
                }
            }
        } else {
            for (uint16_t v : c.arr) out.push_back(high + v);
        }
    }
    return out;
}

}  // namespace opteryx::engine
