#pragma once
// src/cpp/engine/native_sort.hpp — ORDER BY breaker SINKS for the engine.
//
// The sort itself is NOT here. It lives in draken/morsels/sort.hpp — one
// implementation (vergesort prepass -> comparison-sort fallback over normalized
// keys), shared with rugo, which has no query engine at all. This file is only the
// engine-side wrapper: the Sink/state classes that plug that sort into the
// morsel-driven pipeline.
//
// SortSink: accumulate all input (per-worker, lock-free) -> combine (append under
// one mutex per worker) -> finalize: sort every buffered row and gather them into
// fresh dense morsels (chunked) in sorted order.
// TopNSink: the ORDER BY + LIMIT fusion (HeapSortNode) — same ordering, but each
// worker keeps only a bounded candidate set (periodic compaction to the top N), so
// memory stays O(N), never O(input).
// WindowSink: ROW_NUMBER / RANK / DENSE_RANK over PARTITION BY + ORDER BY.
//
// The `using` block below re-exports draken's sort names into opteryx::engine so
// every engine header that reaches them through this one — native_key_hash.hpp,
// native_join2.hpp, native_group_sinks.hpp, native_unnest.hpp, engine.hpp, and
// _operators.pyx's `cdef extern ... namespace "opteryx::engine"` — keeps compiling
// unchanged. These are re-exports, NOT redefinitions: there is exactly one
// definition of each, in draken.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "operator.hpp"
#include "pipeline_buffers.hpp"
#include "morsels/sort.hpp"      // THE sort (build: -Idraken)

namespace opteryx::engine {

// ---- re-exports from draken/morsels/sort.hpp --------------------------------------
// Key building / comparison
using ::SortKeySpec;
using ::SortKeyColumn;
using ::SortKeyCmp;
using ::build_sort_keys;
using ::sort_num_key;
using ::sort_row_valid;
using ::sort_type_is_string;
using ::sort_key_type_supported;
using ::string_arena_of;
// Ordering
using ::sort_perm;
using ::parallel_stable_sort_perm;
using ::sort_morsels;
// Row materialization (also used by joins, UNNEST and LIMIT's partial slice)
using ::gather_rows;
using ::gather_elem_size;
using ::flatten_rows;
using ::make_empty_col;
using ::EmptyColElem;
using ::kGatherNullRow;

// Sort `ms` and append the fully sorted rows, chunked, into `out`. The engine's
// MorselBuffer hand-off around draken's engine-agnostic sort_morsels().
//
// `emit_cols` (nullptr = every column) is the sink's EMIT set — the columns still
// wanted ABOVE the sort. It is disjoint in purpose from `spec`, the READ set: the
// ORDER BY key is read here and, unless something above also selects it, is never
// copied into an output row. Only ever valid on a TERMINAL sort: an intermediate
// one (TopNSink::compact) must keep the key, because the next round sorts on it
// again.
inline void sort_and_emit(const std::vector<MorselPtr>& ms,
                          const std::vector<SortKeySpec>& spec,
                          size_t take_first,          // SIZE_MAX = all rows
                          size_t chunk_rows,
                          MorselBuffer* out, ErrCtx& err,
                          const std::vector<uint32_t>* emit_cols = nullptr) {
    std::vector<MorselPtr> sorted;
    if (!sort_morsels(ms, spec, take_first, chunk_rows, sorted, err, emit_cols)) return;
    for (MorselPtr& m : sorted) {
        if (!out->append(m)) {
            err.code = 1;
            err.msg = out->error().c_str();
            return;
        }
    }
}

// ---- SortSink ---------------------------------------------------------------------

struct SortLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct SortGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
};

// An emit subset is a genuine three-state: "no subset given" (emit everything) is
// NOT the same as "the subset is empty" (emit a zero-column morsel — what a
// COUNT(*) over an ordered subquery legitimately wants). An empty vector cannot
// carry both, so the two are separated: `emit_prune` is the switch, `emit_cols` the
// value. `emit_ptr()` folds them back into the nullptr-or-subset argument draken's
// sort_morsels/gather_rows take.
struct EmitSubset {
    bool emit_prune = false;
    std::vector<uint32_t> emit_cols;
    const std::vector<uint32_t>* emit_ptr() const {
        return emit_prune ? &emit_cols : nullptr;
    }
};

struct SortSink : Sink, EmitSubset {
    std::vector<SortKeySpec> spec;
    MorselBuffer* out;
    size_t chunk_rows;

    SortSink(std::vector<SortKeySpec> s, MorselBuffer* b, size_t chunk = 131072,
             bool prune = false, std::vector<uint32_t> emit = {})
        : spec(std::move(s)), out(b), chunk_rows(chunk) {
        emit_prune = prune;
        emit_cols = std::move(emit);
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<SortGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<SortLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<SortLocal&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<SortGlobal&>(gs);
        auto& l = static_cast<SortLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<SortGlobal&>(gs);
        sort_and_emit(g.morsels, spec, SIZE_MAX, chunk_rows, out, err, emit_ptr());
    }
};

// ---- TopNSink (ORDER BY + LIMIT fused — HeapSortNode) -------------------------------

struct TopNLocal : LocalSinkState {
    std::vector<MorselPtr> morsels;
    size_t rows = 0;
};
struct TopNGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> candidates;
};

struct TopNSink : Sink, EmitSubset {
    std::vector<SortKeySpec> spec;
    size_t n_limit;
    MorselBuffer* out;
    size_t compact_threshold;

    TopNSink(std::vector<SortKeySpec> s, size_t n, MorselBuffer* b,
             bool prune = false, std::vector<uint32_t> emit = {})
        : spec(std::move(s)), n_limit(n), out(b),
          compact_threshold(std::max<size_t>(4 * n, 65536)) {
        emit_prune = prune;
        emit_cols = std::move(emit);
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<TopNGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<TopNLocal>();
    }

    // Reduce the worker's candidate set to its top N (bounds memory to O(N)).
    // NO emit subset here, deliberately: this is an INTERMEDIATE sort whose output
    // is sorted again on the next round and once more in finalize. Dropping the
    // ORDER BY key here would leave nothing to sort by.
    void compact(TopNLocal& l, ErrCtx& err) {
        MorselBuffer tmp;
        sort_and_emit(l.morsels, spec, n_limit, n_limit == 0 ? 1 : n_limit, &tmp, err);
        if (err.code != 0) return;
        l.morsels = tmp.take_resident();
        l.rows = 0;
        for (const MorselPtr& m : l.morsels) l.rows += m->num_rows();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<TopNLocal&>(ls);
        if (in->num_rows() == 0) return SinkResult::CONTINUE;
        l.morsels.push_back(in);
        l.rows += in->num_rows();
        if (l.rows > compact_threshold) compact(l, err);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<TopNGlobal&>(gs);
        auto& l = static_cast<TopNLocal&>(ls);
        if (l.rows > n_limit) compact(l, err);
        if (err.code != 0) return;
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.candidates.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<TopNGlobal&>(gs);
        sort_and_emit(g.candidates, spec, n_limit, n_limit == 0 ? 1 : n_limit, out, err,
                      emit_ptr());
    }
};

// ---- WindowSink (the ranking / navigation / value window functions) ---------------
// ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST, LAG, LEAD,
// FIRST_VALUE, LAST_VALUE, NTH_VALUE.
//
// OVER (PARTITION BY p... ORDER BY o...). Breaker: buffer all input, sort by
// (partition keys ASC, order keys with their asc), one pass over the sorted rows
// computes every function's per-row value, appends them as columns, emits in
// sorted order. Sort-key equality (win_keys_equal) defines partition boundaries
// and order-ties EXACTLY (value compare, not a hash).
//
// That pass walks PARTITION at a time and, inside a partition, PEER GROUP at a
// time (a run of rows equal on the ORDER BY keys). Both are contiguous because
// the sort made them so, so the walk is still O(n) — and it gives every function
// the four numbers it needs as a closed form: the partition's start and end and
// the peer group's start and end. A row-at-a-time pass cannot serve NTILE,
// PERCENT_RANK or CUME_DIST (they need the partition's SIZE) nor LAST_VALUE /
// NTH_VALUE (they need its END), none of which is known until it closes.
//
// The gather kinds — LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE — are a
// PERMUTATION GATHER, not a kernel: the value for output row i (in sorted order)
// is the argument column's value at some other sorted position within row i's
// partition, or NULL when that position falls outside it. LAG/LEAD locate it
// RELATIVE to i (i ∓ offset); FIRST_VALUE/LAST_VALUE/NTH_VALUE locate it against
// the PARTITION (its first row, its last row, its nth row). gather_rows already
// supports both halves of that — kGatherNullRow rows and a single-column emit
// subset — so these outputs reuse the canonical row gather and inherit every
// type it supports, string arenas and ARRAY children included.
//
// FIRST_VALUE/LAST_VALUE/NTH_VALUE are computed over the WHOLE ordered partition.
// See VALUE_FUNCTIONS in opteryx/operators/window/helpers.py for why that, and
// not the SQL standard's default frame, is the reading here.
//
// The mirror of these codes is WINDOW_FUNCTIONS in
// opteryx/operators/window/helpers.py — change one, change both.

enum class WinFn : uint8_t {
    RowNumber = 0, Rank = 1, DenseRank = 2, Lag = 3, Lead = 4,
    Ntile = 5, PercentRank = 6, CumeDist = 7,
    FirstValue = 8, LastValue = 9, NthValue = 10,
};

// The three output shapes. Every kind is in exactly one of them, and the
// switch statements below are exhaustive over each — a new kind that is added
// to the enum without being classified here lands in no shape and produces no
// column, so classify it in ALL THREE predicates, not just the one that looks
// relevant.
//
// GATHER: the output is a VALUE read from another row of the partition. The
// per-row result is a SOURCE ROW ID and the value is produced by the canonical
// row gather, so the output takes the ARGUMENT's type, whatever it is.
inline bool win_fn_is_gather(WinFn k) {
    return k == WinFn::Lag || k == WinFn::Lead || k == WinFn::FirstValue ||
           k == WinFn::LastValue || k == WinFn::NthValue;
}
// FLOAT: the output is a FRACTION of the partition — FLOAT64.
inline bool win_fn_is_float(WinFn k) {
    return k == WinFn::PercentRank || k == WinFn::CumeDist;
}
// INT: everything else — a count or an ordinal, INT64.
//
// win_fn_is_rank_valued is NARROWER than win_fn_is_int: NTILE is an INT64
// ordinal but is not a rank, so a fused `output <= K` top-K filter must not be
// applied to it (its bucket boundaries depend on the partition size). The
// mirror of this set is RANK_VALUED in opteryx/operators/window/helpers.py.
inline bool win_fn_is_rank_valued(WinFn k) {
    return k == WinFn::RowNumber || k == WinFn::Rank || k == WinFn::DenseRank;
}

// arg_col / offset are only meaningful for SOME kinds:
//   arg_col — the INPUT column the value is read from; the gather kinds only,
//             -1 for every other kind.
//   offset  — the kind's single constant integer parameter:
//               Lag/Lead  the row shift (>= 0)
//               Ntile     the bucket count (>= 1)
//               NthValue  the 1-based position within the partition (>= 1)
//             unused (0) for every other kind.
struct WindowFnSpec { WinFn kind; std::string name; int arg_col = -1; int64_t offset = 0; };

struct WindowLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct WindowGlobal : GlobalSinkState { std::mutex mtx; std::vector<MorselPtr> morsels; };

inline bool win_keys_equal(const std::vector<SortKeyColumn>& keys, uint32_t a,
                           uint32_t b, size_t kb, size_t ke) {
    for (size_t k = kb; k < ke; ++k) {
        const SortKeyColumn& c = keys[k];
        uint8_t va = c.valid[a], vb = c.valid[b];
        if (va != vb) return false;
        if (!va) continue;                       // both NULL → equal on this key
        if (c.is_str) {
            if (c.slen[a] != c.slen[b]) return false;
            if (c.slen[a] && std::memcmp(c.sptr[a], c.sptr[b], c.slen[a]) != 0)
                return false;
        } else if (c.is_i128) {
            if (c.num128[a] != c.num128[b]) return false;
        } else {
            if (c.num[a] != c.num[b]) return false;
        }
    }
    return true;
}

struct WindowSink : Sink, EmitSubset {
    std::vector<SortKeySpec> sort_spec;   // [partition keys asc..., order keys...]
    size_t n_part;                        // # partition keys at the front of sort_spec
    std::vector<WindowFnSpec> funcs;
    MorselBuffer* out;
    int64_t top_k;                        // WindowTopKFusionStrategy hint; <0 = none
    size_t chunk_rows;

    WindowSink(std::vector<SortKeySpec> s, size_t np, std::vector<WindowFnSpec> f,
               MorselBuffer* b, int64_t topk = -1, size_t chunk = 131072,
               bool prune = false, std::vector<uint32_t> emit = {})
        : sort_spec(std::move(s)), n_part(np), funcs(std::move(f)), out(b),
          top_k(topk), chunk_rows(chunk) {
        emit_prune = prune;
        emit_cols = std::move(emit);
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<WindowGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<WindowLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<WindowLocal&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<WindowGlobal&>(gs);
        auto& l = static_cast<WindowLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<WindowGlobal&>(gs);
        std::vector<MorselPtr> src;
        for (const MorselPtr& m : g.morsels) if (m->num_rows() > 0) src.push_back(m);
        if (src.empty()) return;

        std::vector<uint32_t> row_m, row_r;
        size_t n = flatten_rows(src, row_m, row_r);
        std::vector<SortKeyColumn> keys;
        if (!build_sort_keys(src, sort_spec, n, keys, err)) return;
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        sort_perm(keys, perm);

        // Rank numbers in perm order (gather_rows emits rows in perm order too).
        // Navigation functions (LAG/LEAD) get no rank: their per-row value is a
        // SOURCE ROW ID — the global row id of the row `offset` sorted positions
        // away, or kGatherNullRow when that position falls outside the partition.
        // Same-partition is decided by partition-key equality, which is exact: the
        // sort groups each partition contiguously, so the row at i±offset is in row
        // i's partition iff their partition keys compare equal (win_keys_equal).
        size_t nf = funcs.size();
        if (top_k >= 0 && nf > 0 && !win_fn_is_rank_valued(funcs[0].kind)) {
            // WindowTopKFusionStrategy only fuses rank-valued outputs. Fusing
            // any other kind would filter on its values as if they were ranks —
            // a NTILE bucket, a CUME_DIST fraction, or a LAG'd value.
            err.code = 1;
            err.msg = "WindowSink: top_k fused onto a window function that is not "
                      "rank-valued — fail loud, never a silent wrong answer";
            return;
        }
        for (size_t f = 0; f < nf; ++f) {
            if (funcs[f].kind == WinFn::Ntile && funcs[f].offset < 1) {
                err.code = 1;
                err.msg = "WindowSink: NTILE bucket count must be >= 1";
                return;
            }
            if (funcs[f].kind == WinFn::NthValue && funcs[f].offset < 1) {
                err.code = 1;
                err.msg = "WindowSink: NTH_VALUE position must be >= 1";
                return;
            }
        }
        std::vector<std::vector<int64_t>> ranks(nf);
        std::vector<std::vector<double>> fracs(nf);
        std::vector<std::vector<uint32_t>> nav_order(nf);
        for (size_t f = 0; f < nf; ++f) {
            if (win_fn_is_gather(funcs[f].kind)) nav_order[f].resize(n);
            else if (win_fn_is_float(funcs[f].kind)) fracs[f].resize(n);
            else ranks[f].resize(n);
        }

        // Partition at a time, and within a partition peer group at a time.
        //
        // The previous single forward pass could not serve the whole set: NTILE,
        // PERCENT_RANK and CUME_DIST need the partition's SIZE, and FIRST_VALUE/
        // LAST_VALUE/NTH_VALUE need its BOUNDS — none of which is known until the
        // partition closes. Walking the extents first costs the same O(n) (the
        // sort already grouped both partitions and peer groups contiguously) and
        // makes every per-row value a closed-form expression of
        // (partition start, partition end, peer-group start, peer-group end)
        // rather than of the previous row.
        //
        // A peer group is a run equal on the ORDER BY keys. With no ORDER BY (the
        // internal INTERSECT/EXCEPT ALL ROW_NUMBER) the key range is empty,
        // win_keys_equal is vacuously true, and the whole partition is one peer
        // group — which is the right reading: with no ordering every row ties.
        size_t ps = 0;
        while (ps < n) {
            size_t pe = ps + 1;
            while (pe < n && win_keys_equal(keys, perm[pe], perm[ps], 0, n_part)) ++pe;
            const int64_t pn = static_cast<int64_t>(pe - ps);

            size_t gs = ps;
            int64_t dense = 0;
            while (gs < pe) {
                size_t ge = gs + 1;
                while (ge < pe &&
                       win_keys_equal(keys, perm[ge], perm[gs], n_part, sort_spec.size()))
                    ++ge;
                ++dense;
                // RANK is the peer group's FIRST position; CUME_DIST counts through
                // its LAST. Both are properties of the group, not of the row.
                const int64_t rank = static_cast<int64_t>(gs - ps) + 1;
                const int64_t through_last_peer = static_cast<int64_t>(ge - ps);

                for (size_t i = gs; i < ge; ++i) {
                    const int64_t idx = static_cast<int64_t>(i - ps);  // 0-based
                    for (size_t f = 0; f < nf; ++f) {
                        const WindowFnSpec& fn = funcs[f];
                        if (win_fn_is_gather(fn.kind)) {
                            // Every gather kind resolves to one absolute position
                            // within [ps, pe); anything outside is NULL.
                            int64_t j;
                            switch (fn.kind) {
                                case WinFn::Lag:
                                    j = static_cast<int64_t>(i) - fn.offset; break;
                                case WinFn::Lead:
                                    j = static_cast<int64_t>(i) + fn.offset; break;
                                case WinFn::FirstValue:
                                    j = static_cast<int64_t>(ps); break;
                                case WinFn::LastValue:
                                    j = static_cast<int64_t>(pe) - 1; break;
                                default:  // NthValue, 1-based within the partition
                                    j = static_cast<int64_t>(ps) + fn.offset - 1; break;
                            }
                            bool in_part = j >= static_cast<int64_t>(ps) &&
                                           j < static_cast<int64_t>(pe);
                            nav_order[f][i] =
                                in_part ? perm[static_cast<size_t>(j)] : kGatherNullRow;
                            continue;
                        }
                        if (win_fn_is_float(fn.kind)) {
                            double val;
                            if (fn.kind == WinFn::PercentRank) {
                                // (rank - 1) / (rows - 1); a one-row partition has
                                // no spread to be a fraction of, and the standard
                                // fixes that degenerate case at 0.
                                val = pn > 1
                                    ? static_cast<double>(rank - 1) /
                                      static_cast<double>(pn - 1)
                                    : 0.0;
                            } else {  // CumeDist — always in (0, 1]
                                val = static_cast<double>(through_last_peer) /
                                      static_cast<double>(pn);
                            }
                            fracs[f][i] = val;
                            continue;
                        }
                        int64_t val;
                        switch (fn.kind) {
                            case WinFn::RowNumber: val = idx + 1; break;
                            case WinFn::Rank:      val = rank; break;
                            case WinFn::DenseRank: val = dense; break;
                            default: {  // Ntile
                                // pn rows into k buckets: the first (pn % k) buckets
                                // take one row more than the rest. When k > pn the
                                // quotient is 0, every one of the first pn buckets
                                // takes exactly one row, and `big` spans the whole
                                // partition — so the `/ q` branch is unreachable and
                                // cannot divide by zero.
                                const int64_t k = fn.offset;
                                const int64_t q = pn / k;
                                const int64_t r = pn % k;
                                const int64_t big = r * (q + 1);
                                val = idx < big ? idx / (q + 1) + 1
                                                : r + (idx - big) / q + 1;
                                break;
                            }
                        }
                        ranks[f][i] = val;
                    }
                }
                gs = ge;
            }
            ps = pe;
        }

        // WindowTopKFusionStrategy's fused `WHERE <rank> <= K` filter (top_k >= 0):
        // every row still got an exact rank above — RANK/DENSE_RANK ties can only be
        // resolved once every row in the partition is known — but only the surviving
        // prefix needs to be gathered and emitted. Compacts `perm`/`ranks` down to the
        // kept rows once (O(n) pass, no per-element branch in the gather loop below),
        // so a query keeping 200k of 10M ranked rows doesn't materialize, copy, then
        // immediately filter back out the other 9.8M via a separate downstream Filter.
        std::vector<uint32_t> kept_perm;
        std::vector<std::vector<int64_t>> filtered_ranks;
        std::vector<std::vector<double>> filtered_fracs;
        std::vector<std::vector<uint32_t>> filtered_nav;
        const std::vector<uint32_t>* gather_order = &perm;
        const std::vector<std::vector<int64_t>>* ranks_src = &ranks;
        const std::vector<std::vector<double>>* fracs_src = &fracs;
        const std::vector<std::vector<uint32_t>>* nav_src = &nav_order;
        size_t total = n;
        if (top_k >= 0 && nf > 0) {
            std::vector<uint32_t> kept;
            kept.reserve(n);
            for (size_t i = 0; i < n; ++i) if (ranks[0][i] <= top_k) kept.push_back(static_cast<uint32_t>(i));
            total = kept.size();
            kept_perm.resize(total);
            for (size_t j = 0; j < total; ++j) kept_perm[j] = perm[kept[j]];
            // Nav entries are GLOBAL row ids (or kGatherNullRow), so compacting the
            // OUTPUT rows never invalidates them — a source row outside the kept set
            // is still gathered from `src` by id.
            //
            // funcs[0] is rank-valued (guarded above), but the OTHER functions on the
            // same window are not restricted: `ROW_NUMBER() OVER w, CUME_DIST() OVER w
            // ... WHERE rn <= 5` compacts a float column alongside the rank. Every
            // per-row array is compacted, whatever its shape.
            filtered_ranks.assign(nf, {});
            filtered_fracs.assign(nf, {});
            filtered_nav.assign(nf, {});
            for (size_t f = 0; f < nf; ++f) {
                if (!ranks[f].empty()) {
                    filtered_ranks[f].resize(total);
                    for (size_t j = 0; j < total; ++j) filtered_ranks[f][j] = ranks[f][kept[j]];
                }
                if (!fracs[f].empty()) {
                    filtered_fracs[f].resize(total);
                    for (size_t j = 0; j < total; ++j) filtered_fracs[f][j] = fracs[f][kept[j]];
                }
                if (!nav_order[f].empty()) {
                    filtered_nav[f].resize(total);
                    for (size_t j = 0; j < total; ++j) filtered_nav[f][j] = nav_order[f][kept[j]];
                }
            }
            gather_order = &kept_perm;
            ranks_src = &filtered_ranks;
            fracs_src = &filtered_fracs;
            nav_src = &filtered_nav;
        }

        // Chunked gather: each chunk builds one independent output morsel (its own
        // gather_rows call + its own rank columns) — no cross-chunk state, so chunks
        // are dispatched to a one-shot thread pool-let (same idiom as
        // parallel_stable_sort_perm / GroupBySink::finalize above) and written into
        // pre-sized slots so `out->morsels` still ends up in sorted-chunk order,
        // which downstream relies on (WindowNode's pipeline runs at dop 1 to
        // preserve it).
        const std::vector<std::string>& names = src.front()->names;
        size_t num_chunks = (total + chunk_rows - 1) / chunk_rows;
        std::vector<MorselPtr> chunk_out(num_chunks);

        unsigned hw = std::thread::hardware_concurrency();
        unsigned nt = hw > 2 ? static_cast<unsigned>(hw - 2) : 1u;
        if (nt > 16) nt = 16;
        if (nt > num_chunks) nt = static_cast<unsigned>(num_chunks);
        if (total < 200000) nt = 1;
        if (nt < 1) nt = 1;

        std::vector<ErrCtx> errs(nt);
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            for (;;) {
                size_t ci = next_chunk.fetch_add(1);
                if (ci >= num_chunks) break;
                size_t start = ci * chunk_rows;
                size_t count = std::min(chunk_rows, total - start);
                // The PARTITION BY / ORDER BY keys were consumed by build_sort_keys
                // and win_keys_equal above — the ranks are already computed — so a
                // key nothing above this window reads dies here rather than being
                // gathered into every output row.
                MorselPtr m = gather_rows(src, *gather_order, start, count, row_m, row_r,
                                          names, errs[tid], emit_ptr());
                if (errs[tid].code != 0) return;
                uint32_t cn = static_cast<uint32_t>(count);
                for (size_t f = 0; f < nf; ++f) {
                    if (win_fn_is_gather(funcs[f].kind)) {
                        // LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE: gather the
                        // argument column by the source ids computed above.
                        // gather_rows handles the NULL rows (kGatherNullRow) and
                        // every supported type; the emit subset narrows it to the
                        // one argument column.
                        size_t ncols_in = src.front()->columns.size();
                        if (funcs[f].arg_col < 0 ||
                            static_cast<size_t>(funcs[f].arg_col) >= ncols_in) {
                            errs[tid].code = 1;
                            errs[tid].msg = "WindowSink: window function "
                                            "argument column index out of range";
                            return;
                        }
                        std::vector<uint32_t> one_col{
                            static_cast<uint32_t>(funcs[f].arg_col)};
                        MorselPtr nav = gather_rows(src, (*nav_src)[f], start, count,
                                                    row_m, row_r, names, errs[tid],
                                                    &one_col);
                        if (nav == nullptr || errs[tid].code != 0) return;
                        m->columns.push_back(std::move(nav->columns[0]));
                        m->names.push_back(funcs[f].name);
                        continue;
                    }
                    // A computed output column: INT64 counts/ordinals, or the
                    // FLOAT64 fractions PERCENT_RANK/CUME_DIST produce. Same
                    // dense-identity vector either way, so the two differ only in
                    // element width, source array, and DrakenType.
                    const bool is_float = win_fn_is_float(funcs[f].kind);
                    const size_t width = is_float ? sizeof(double) : sizeof(int64_t);
                    void* data = draken_malloc((cn == 0 ? 1 : cn) * width);
                    if (data == nullptr) {
                        errs[tid].code = 1;
                        errs[tid].msg = "WindowSink: out of memory allocating a "
                                        "window function output column";
                        return;
                    }
                    if (is_float) {
                        double* out_d = static_cast<double*>(data);
                        for (uint32_t j = 0; j < cn; ++j) out_d[j] = (*fracs_src)[f][start + j];
                    } else {
                        int64_t* out_i = static_cast<int64_t*>(data);
                        for (uint32_t j = 0; j < cn; ++j) out_i[j] = (*ranks_src)[f][start + j];
                    }
                    uint32_t* sel = static_cast<uint32_t*>(
                        draken_malloc((cn == 0 ? 1 : cn) * sizeof(uint32_t)));
                    if (sel == nullptr) {
                        draken_free(data);
                        errs[tid].code = 1;
                        errs[tid].msg = "WindowSink: out of memory allocating a "
                                        "window function selection vector";
                        return;
                    }
                    for (uint32_t j = 0; j < cn; ++j) sel[j] = j;
                    DrakenVector v;
                    v.data = data; v.selection = sel; v.data_length = cn; v.length = cn;
                    v.validity = nullptr;
                    v.type = is_float ? DRAKEN_FLOAT64 : DRAKEN_INT64;
                    v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
                    CxxColumn c;
                    c.own = std::make_shared<VectorOwner>(
                        v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr),
                        OwnedBuffer<void>(sel));
                    c.own->logical_type = nullptr;
                    c.view = c.own->vec;
                    m->columns.push_back(std::move(c));
                    m->names.push_back(funcs[f].name);
                }
                chunk_out[ci] = std::move(m);
            }
        };
        std::vector<std::thread> threads;
        threads.reserve(nt > 0 ? nt - 1 : 0);
        for (unsigned t = 1; t < nt; ++t) threads.emplace_back(worker, t);
        worker(0);
        for (std::thread& t : threads) t.join();
        for (ErrCtx& e : errs) {
            if (e.code != 0) { err = e; return; }
        }
        for (MorselPtr& m : chunk_out) {
            if (!out->append(m)) {
                err.code = 1;
                err.msg = out->error().c_str();
                return;
            }
        }
    }
};

}  // namespace opteryx::engine
