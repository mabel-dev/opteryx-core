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
using ::kGatherNullRow;

// Sort `ms` and append the fully sorted rows, chunked, into `out`. The engine's
// MorselBuffer hand-off around draken's engine-agnostic sort_morsels().
inline void sort_and_emit(const std::vector<MorselPtr>& ms,
                          const std::vector<SortKeySpec>& spec,
                          size_t take_first,          // SIZE_MAX = all rows
                          size_t chunk_rows,
                          MorselBuffer* out, ErrCtx& err) {
    std::vector<MorselPtr> sorted;
    if (!sort_morsels(ms, spec, take_first, chunk_rows, sorted, err)) return;
    for (MorselPtr& m : sorted) out->morsels.push_back(std::move(m));
}

// ---- SortSink ---------------------------------------------------------------------

struct SortLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct SortGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
};

struct SortSink : Sink {
    std::vector<SortKeySpec> spec;
    MorselBuffer* out;
    size_t chunk_rows;

    SortSink(std::vector<SortKeySpec> s, MorselBuffer* b, size_t chunk = 131072)
        : spec(std::move(s)), out(b), chunk_rows(chunk) {}

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
        sort_and_emit(g.morsels, spec, SIZE_MAX, chunk_rows, out, err);
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

struct TopNSink : Sink {
    std::vector<SortKeySpec> spec;
    size_t n_limit;
    MorselBuffer* out;
    size_t compact_threshold;

    TopNSink(std::vector<SortKeySpec> s, size_t n, MorselBuffer* b)
        : spec(std::move(s)), n_limit(n), out(b),
          compact_threshold(std::max<size_t>(4 * n, 65536)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<TopNGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<TopNLocal>();
    }

    // Reduce the worker's candidate set to its top N (bounds memory to O(N)).
    void compact(TopNLocal& l, ErrCtx& err) {
        MorselBuffer tmp;
        sort_and_emit(l.morsels, spec, n_limit, n_limit == 0 ? 1 : n_limit, &tmp, err);
        if (err.code != 0) return;
        l.morsels = std::move(tmp.morsels);
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
        sort_and_emit(g.candidates, spec, n_limit, n_limit == 0 ? 1 : n_limit, out, err);
    }
};

// ---- WindowSink (ROW_NUMBER / RANK / DENSE_RANK) -----------------------------------
// OVER (PARTITION BY p... ORDER BY o...). Breaker: buffer all input, sort by
// (partition keys ASC, order keys with their asc), one pass assigns the rank per
// partition, appends them as INT64 columns, emits in sorted order. Sort-key equality
// (win_keys_equal) defines partition boundaries and order-ties EXACTLY (value
// compare, not a hash).

enum class WinFn : uint8_t { RowNumber = 0, Rank = 1, DenseRank = 2 };
struct WindowFnSpec { WinFn kind; std::string name; };

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

struct WindowSink : Sink {
    std::vector<SortKeySpec> sort_spec;   // [partition keys asc..., order keys...]
    size_t n_part;                        // # partition keys at the front of sort_spec
    std::vector<WindowFnSpec> funcs;
    MorselBuffer* out;
    int64_t top_k;                        // WindowTopKFusionStrategy hint; <0 = none
    size_t chunk_rows;

    WindowSink(std::vector<SortKeySpec> s, size_t np, std::vector<WindowFnSpec> f,
               MorselBuffer* b, int64_t topk = -1, size_t chunk = 131072)
        : sort_spec(std::move(s)), n_part(np), funcs(std::move(f)), out(b),
          top_k(topk), chunk_rows(chunk) {}

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
        size_t nf = funcs.size();
        std::vector<std::vector<int64_t>> ranks(nf, std::vector<int64_t>(n));
        std::vector<int64_t> prev(nf, 0);
        size_t part_start = 0;
        for (size_t i = 0; i < n; ++i) {
            bool new_part = (i == 0) ||
                !win_keys_equal(keys, perm[i], perm[i - 1], 0, n_part);
            if (new_part) part_start = i;
            bool same_order = !new_part &&
                win_keys_equal(keys, perm[i], perm[i - 1], n_part, sort_spec.size());
            int64_t pos = static_cast<int64_t>(i - part_start) + 1;
            for (size_t f = 0; f < nf; ++f) {
                int64_t val;
                switch (funcs[f].kind) {
                    case WinFn::RowNumber: val = pos; break;
                    case WinFn::Rank:
                        val = new_part ? 1 : (same_order ? prev[f] : pos); break;
                    default:  // DenseRank
                        val = new_part ? 1 : (same_order ? prev[f] : prev[f] + 1); break;
                }
                ranks[f][i] = val;
                prev[f] = val;
            }
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
        const std::vector<uint32_t>* gather_order = &perm;
        const std::vector<std::vector<int64_t>>* ranks_src = &ranks;
        size_t total = n;
        if (top_k >= 0 && nf > 0) {
            std::vector<uint32_t> kept;
            kept.reserve(n);
            for (size_t i = 0; i < n; ++i) if (ranks[0][i] <= top_k) kept.push_back(static_cast<uint32_t>(i));
            total = kept.size();
            kept_perm.resize(total);
            for (size_t j = 0; j < total; ++j) kept_perm[j] = perm[kept[j]];
            filtered_ranks.assign(nf, std::vector<int64_t>(total));
            for (size_t f = 0; f < nf; ++f)
                for (size_t j = 0; j < total; ++j) filtered_ranks[f][j] = ranks[f][kept[j]];
            gather_order = &kept_perm;
            ranks_src = &filtered_ranks;
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
                MorselPtr m = gather_rows(src, *gather_order, start, count, row_m, row_r,
                                          names, errs[tid]);
                if (errs[tid].code != 0) return;
                uint32_t cn = static_cast<uint32_t>(count);
                for (size_t f = 0; f < nf; ++f) {
                    int64_t* data = static_cast<int64_t*>(
                        draken_malloc((cn == 0 ? 1 : cn) * sizeof(int64_t)));
                    for (uint32_t j = 0; j < cn; ++j) data[j] = (*ranks_src)[f][start + j];
                    uint32_t* sel = static_cast<uint32_t*>(
                        draken_malloc((cn == 0 ? 1 : cn) * sizeof(uint32_t)));
                    for (uint32_t j = 0; j < cn; ++j) sel[j] = j;
                    DrakenVector v;
                    v.data = data; v.selection = sel; v.data_length = cn; v.length = cn;
                    v.validity = nullptr; v.type = DRAKEN_INT64;
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
        for (MorselPtr& m : chunk_out) out->morsels.push_back(std::move(m));
    }
};

}  // namespace opteryx::engine
