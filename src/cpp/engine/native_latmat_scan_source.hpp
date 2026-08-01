#pragma once
// src/cpp/engine/native_latmat_scan_source.hpp — R3 (`fused_topn`): the zero-Python
// two-pass late-materialization parquet scan Source.
//
// The shape this exists for is `SELECT <wide> FROM t WHERE <pred> ORDER BY <col>
// LIMIT n` (ClickBench Q24). Decoding every projected column of every row and then
// throwing away all but n of them is the wrong amount of work by two orders of
// magnitude, so the scan runs in two passes:
//
//   pass 1  decode ONLY the predicate columns + the sort key, for the whole table;
//           evaluate the predicate per row group into a survivor bitmap; keep the
//           surviving rows' pass-1 columns.
//   reduce  across ALL row groups, find the LIMIT boundary in the sort key and drop
//           every survivor that is strictly worse than it (n rows plus any ties at
//           the boundary). Row groups left with no candidate are never read again.
//   pass 2  decode the REMAINING projected columns, masked, for just those rows,
//           and zip them back onto their pass-1 columns.
//
// This is the native twin of the trampoline scan's `_run_pass1` / `_apply_topn` /
// `_combine_pass1_pass2_row_group` (opteryx/operators/parquet_read/parquet_read.pyx),
// which drove exactly this algorithm from Python — it was the last per-morsel Python
// touch on the SELECT path. Every heavy piece was ALREADY native and is reused, not
// rebuilt:
//   * decode + masked decode ....... rugo::ParquetIOPipeline::submit_row_group(..., row_mask)
//   * pass-1 predicate on the decode workers ... rugo Pass1Pred / pass1_run_predicate
//   * the same predicate as a fallback ......... opteryx_pass1_predicate_eval's C ABI
//   * column materialization ................... NativeScanColumnBuilder (shared with
//                                                NativeParquetScanSource)
//   * row gather ............................... draken gather_rows
//
// ── The one genuinely new piece: the boundary reduction ───────────────────────────
// `reduce_to_topn` below. It does NOT reimplement an ordering. It builds draken's own
// normalized sort keys (`build_sort_keys`) over the pass-1 survivors and uses draken's
// own comparator (`SortKeyCmp`) — the SAME definition the downstream TopNSink sorts
// with — then keeps every row that is not strictly worse than the n-th best:
//
//     nth_element(idx, idx + n - 1, cmp);  b = idx[n-1];
//     keep[r] = !cmp(b, r);          // r is not strictly worse than the boundary
//
// Because the comparator is shared, this is correct BY CONSTRUCTION for every key
// type, for ties (a tied row compares neither-before-nor-after `b`, so `!cmp(b,r)` is
// true and it is kept), and for NULLs — no separate null rule is written here at all.
//
// ⚠ This deliberately DIFFERS from the trampoline's `_apply_topn`, which hard-codes
// "NULLs sort last" in both directions. draken orders NULL BELOW every value
// (SortKeyCmp: `cmp = va ? 1 : -1`), i.e. NULLs come FIRST ascending and LAST
// descending — so `_apply_topn` drops NULL survivors that belong in the answer for
// `ORDER BY <nullable> ASC LIMIT n`, and the trampoline returns rows the un-pushed
// plan does not. Verified directly (latmat on vs off over a 3-NULL fixture: on gave
// `[1003..1012]`, off gave `[NULL,NULL,NULL,1003..1009]`). This Source matches the
// UN-PUSHED plan, which is the actual contract; the trampoline bug is untouched and
// still open.
//
// ── Threading ─────────────────────────────────────────────────────────────────────
// Pass 1 is a barrier: it must see every row group's sort key before any boundary
// exists. The first worker into `get_morsel` runs it to completion under the global
// mutex (rugo's own decode workers still parallelise the actual decode + predicate);
// every other worker blocks there, then all of them stream pass 2 concurrently,
// claiming work items exactly the way NativeParquetScanSource does.
//
// Planning — footers, pruning, pool sizing, and the pass-1/pass-2 column split —
// stays in Python (two NativeScanPlans, see compiler.py::_latmat_scan_plan). Nothing
// here touches a PyObject.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "native_parquet_scan_source.hpp"   // NativeScanColumnBuilder, LC_* packing
#include "native_sort.hpp"                  // build_sort_keys / SortKeyCmp / gather_rows
#include "operator.hpp"
#include "trace.hpp"                        // TC_IO_WAIT

namespace opteryx::engine {

// The pass-1 predicate, as the rugo decode workers already receive it
// (rugo::Pass1PredFn): `int fn(void* ctx, DrakenVector** cols, int ncols,
// uint32_t num_rows, uint8_t* out_mask)`. `ctx` is opteryx's Pass1PredCtx*, resolved
// once on the planning thread by Pass1PredResolver (evaluation.pyx) and kept alive by
// the NativePlan. Only draken's DrakenVector and this pointer cross the boundary.
using LatmatPredFn = rugo::Pass1PredFn;

// One row group that survived pass 1.
struct LatmatRowGroup {
    std::string           path;
    int                   rg_idx = -1;
    uint32_t              num_rows = 0;      // rows in the row group, pre-filter
    std::vector<uint32_t> positions;         // ascending ORIGINAL row indices of survivors
    MorselPtr             p1;                // pass-1 columns, compacted to `positions`
};

// A row group that also survived the top-n reduction, with its pass-2 work.
struct LatmatPass2Item {
    std::string          path;
    int                  rg_idx = -1;
    std::vector<uint8_t> row_mask;   // one byte per row group row (1 = decode) — the
                                     // shape rugo's decoder consumes (decode_column.cpp
                                     // indexes it by row offset, not by bit)
    MorselPtr            p1;         // pass-1 columns for exactly the masked rows, in
                                     // the same ascending row order pass 2 emits
};

struct LatmatScanGlobal : GlobalSourceState {
    std::mutex mtx;
    bool  pass1_done = false;
    int   pass1_err = 0;             // sticky: a pass-1 failure must fail every worker
    std::vector<LatmatPass2Item> work;
    // "<path>#<rg_idx>" -> index into `work`. Results come back in COMPLETION order,
    // so each one has to be matched to its pass-1 partner by key; a linear scan would
    // make the whole pass-2 drain quadratic in the work-item count (fine for a
    // 10-row-group top-n, not for a large LIMIT that keeps thousands). Written once
    // during pass 1, read-only afterwards.
    std::unordered_map<std::string, size_t> work_index;
    int   next_to_submit = 0;
    int   results_received = 0;
};

struct LatmatScanSource : Source {
    // ── pass 1 (predicate columns + sort key) ──────────────────────────────────────
    rugo::ParquetIOPipeline* p1_pipeline;
    const std::unordered_map<std::string, FileStats>* footer_map;
    const std::vector<std::pair<std::string, int>>* work_items;
    const std::vector<std::string>* p1_column_names;
    NativeScanColumnBuilder p1_build;
    int in_flight_limit;

    // ── pass 2 (the remaining projected columns) ───────────────────────────────────
    rugo::ParquetIOPipeline* p2_pipeline;
    const std::vector<std::string>* p2_column_names;
    NativeScanColumnBuilder p2_build;

    // ── the pushed predicate ───────────────────────────────────────────────────────
    LatmatPredFn pred_fn;
    void*        pred_ctx;
    // pred_col_to_p1[k] = index into p1_column_names of the predicate's k-th column,
    // in the order Pass1PredCtx's col_idx expects. Resolved at plan time.
    const std::vector<int>* pred_col_to_p1;

    // ── the top-n spec ─────────────────────────────────────────────────────────────
    int     sort_p1_index;     // sort key's position within the pass-1 columns
    bool    sort_ascending;
    int64_t topn_limit;

    // ── output assembly ────────────────────────────────────────────────────────────
    // For output column j exactly one of these is >= 0: the source column's index in
    // the pass-1 or pass-2 layout. Built at plan time from the scan's projection.
    const std::vector<int>* out_from_p1;
    const std::vector<int>* out_from_p2;
    const std::vector<std::string>* out_names;

    LatmatScanSource() = default;

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<LatmatScanGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    // Build the (names, stats) parallel arrays for one row group and hand them to a
    // pipeline. Same lockstep contract as NativeParquetScanSource::submit_one — a
    // column missing from this row group's stats means schema evolution, which
    // neither native scan path supports, so it fails loud instead of NULL-filling.
    bool submit(rugo::ParquetIOPipeline* pipeline,
                const std::vector<std::string>& want_names,
                const std::string& path, int rg_idx,
                const std::vector<uint8_t>* row_mask, ErrCtx& err) {
        auto fit = footer_map->find(path);
        if (fit == footer_map->end()) {
            err.code = 1;
            err.msg = "LatmatScanSource: work item path missing from footer_map";
            return false;
        }
        const RowGroupStats& rg = fit->second.row_groups[static_cast<size_t>(rg_idx)];
        std::vector<std::string> col_names_vec;
        std::vector<ColumnStats> col_stats_vec;
        col_names_vec.reserve(want_names.size());
        col_stats_vec.reserve(want_names.size());
        for (const std::string& want : want_names) {
            for (const ColumnStats& cs : rg.columns) {
                if (cs.name == want) {
                    col_names_vec.push_back(want);
                    col_stats_vec.push_back(cs);
                    break;
                }
            }
        }
        if (col_names_vec.size() != want_names.size()) {
            err.code = 1;
            err.msg = "LatmatScanSource: row group is missing a projected column "
                      "(schema evolution is not supported on this path)";
            return false;
        }
        if (row_mask == nullptr)
            pipeline->submit_row_group(path, rg_idx, col_names_vec, col_stats_vec);
        else
            pipeline->submit_row_group(path, rg_idx, col_names_vec, col_stats_vec, *row_mask);
        return true;
    }

    // Materialize a decoded row group into a morsel, taking ownership of its buffers.
    MorselPtr build_morsel(NativeScanColumnBuilder& builder, rugo::MorselRef& result,
                           const std::vector<std::string>& names, ErrCtx& err) {
        auto m = std::make_shared<CxxMorsel>();
        m->names = names;
        m->columns.reserve(result.columns.size());
        for (size_t i = 0; i < result.columns.size(); ++i) {
            CxxColumn col;
            if (!builder.build_column(result, i, col, err)) {
                if (err.code == 0) {
                    err.code = 1;
                    err.msg = "LatmatScanSource: unsupported column encoding in a "
                              "late-materialized scan";
                }
                return nullptr;
            }
            m->columns.push_back(std::move(col));
        }
        return m;
    }

    // Gather `order` (row indices into `src`) into a fresh dense morsel.
    static MorselPtr take_rows(const MorselPtr& src, const std::vector<uint32_t>& order,
                               ErrCtx& err) {
        std::vector<MorselPtr> ms{src};
        std::vector<uint32_t> row_m(src->num_rows(), 0);
        std::vector<uint32_t> row_r(src->num_rows());
        for (uint32_t i = 0; i < src->num_rows(); ++i) row_r[i] = i;
        return gather_rows(ms, order, 0, order.size(), row_m, row_r, src->names, err);
    }

    // ── PASS 1 ────────────────────────────────────────────────────────────────────
    // Drain every pruned-surviving row group through the pass-1 pipeline, evaluate the
    // predicate, and collect the survivors. Runs once, on whichever worker gets here
    // first, holding g.mtx.
    void run_pass1(std::vector<LatmatRowGroup>& out, ErrCtx& err) {
        const int n_items = static_cast<int>(work_items->size());
        int submitted = 0, received = 0;
        while (received < n_items) {
            while (submitted < n_items && (submitted - received) < in_flight_limit) {
                if (!submit(p1_pipeline, *p1_column_names,
                            (*work_items)[submitted].first,
                            (*work_items)[submitted].second, nullptr, err))
                    return;
                submitted += 1;
            }
            rugo::MorselRef result;
            const auto _tr_idx = BS::this_thread::get_index();
            const uint16_t _tr_worker =
                _tr_idx.has_value() ? static_cast<uint16_t>(*_tr_idx) : 0xFFFFu;
            TraceHandle _tr_wait = trace_begin(TC_IO_WAIT, p1_pipeline->trace_node_id(), 0,
                                               0xFFFFFFFFu, _tr_worker);
            bool got = p1_pipeline->wait_and_get_result(result);
            trace_end(_tr_wait, 0, 0);
            if (!got) {
                err.code = 1;
                err.msg = "LatmatScanSource: pass-1 pipeline drained with result(s) missing";
                return;
            }
            received += 1;
            if (!result.success) {
                err.code = 1;
                static thread_local std::string p1_err_buf;
                p1_err_buf = "LatmatScanSource (pass 1): " +
                             (result.error.empty() ? std::string("parquet decode error")
                                                   : result.error);
                err.msg = p1_err_buf.c_str();
                return;
            }
            // Phase-2 dictionary decode-skip proved this row group empty — no
            // survivors, and pass 2 must never read it.
            if (result.empty_filtered) continue;
            if (result.columns.size() != p1_column_names->size()) {
                err.code = 1;
                err.msg = "LatmatScanSource: pass-1 decoded column count does not match "
                          "the pass-1 projection (schema evolution is not supported)";
                return;
            }
            const uint32_t nrows = result.columns[0].length;
            if (nrows == 0) continue;

            // The decode worker may already have run the predicate for us (rugo's
            // Pass1Pred path, taken when every predicate column is a shape it can view
            // without a copy). Capture the bitmap BEFORE build_morsel takes the buffers.
            std::vector<uint8_t> worker_mask = std::move(result.survivor_mask);

            LatmatRowGroup rg;
            rg.path = result.path;
            rg.rg_idx = result.rg_idx;
            rg.num_rows = nrows;
            MorselPtr p1m = build_morsel(p1_build, result, *p1_column_names, err);
            if (err.code != 0) return;

            const size_t nbytes = (static_cast<size_t>(nrows) + 7u) >> 3;
            std::vector<uint8_t> mask;
            if (worker_mask.size() >= nbytes) {
                mask = std::move(worker_mask);
            } else {
                // Fallback: the same predicate, same C ABI, evaluated here over the
                // built columns. Reached whenever rugo declined the worker-side view
                // (pass1_build_dv_view supports plain DK_VARCHAR only), so every other
                // column shape still gets its predicate — never a silent unfiltered
                // pass. The columns are already materialized, so this reads the
                // DrakenVector views directly.
                mask.assign(nbytes, 0);
                std::vector<DrakenVector*> cols;
                cols.reserve(pred_col_to_p1->size());
                for (int ci : *pred_col_to_p1) {
                    if (ci < 0 || static_cast<size_t>(ci) >= p1m->columns.size()) {
                        err.code = 1;
                        err.msg = "LatmatScanSource: pass-1 predicate column index out "
                                  "of range for the pass-1 layout";
                        return;
                    }
                    cols.push_back(&p1m->columns[static_cast<size_t>(ci)].view);
                }
                const int rc = pred_fn(pred_ctx, cols.data(),
                                       static_cast<int>(cols.size()), nrows, mask.data());
                if (rc != 0) {
                    err.code = 1;
                    err.msg = "LatmatScanSource: pass-1 predicate evaluation failed";
                    return;
                }
            }

            rg.positions.reserve(64);
            for (uint32_t r = 0; r < nrows; ++r) {
                if ((mask[r >> 3] >> (r & 7)) & 1u) rg.positions.push_back(r);
            }
            if (rg.positions.empty()) continue;
            rg.p1 = take_rows(p1m, rg.positions, err);
            if (err.code != 0) return;
            out.push_back(std::move(rg));
        }
    }

    // ── THE REDUCTION ─────────────────────────────────────────────────────────────
    // Shrink the pass-1 survivors to the rows that can still be in the top-n, and turn
    // what is left into pass-2 work. See this file's header for why the ordering is
    // draken's own comparator rather than a rule written here.
    static std::string work_key(const std::string& path, int rg_idx) {
        return path + "#" + std::to_string(rg_idx);
    }

    void reduce_to_topn(std::vector<LatmatRowGroup>& rgs,
                        std::vector<LatmatPass2Item>& work,
                        std::unordered_map<std::string, size_t>& work_index,
                        ErrCtx& err) {
        std::vector<MorselPtr> ms;
        ms.reserve(rgs.size());
        size_t total = 0;
        for (LatmatRowGroup& rg : rgs) {
            ms.push_back(rg.p1);
            total += rg.p1->num_rows();
        }
        if (total == 0) return;

        // `keep[g]` over the CONCATENATED survivor row space (row-group order), which
        // is exactly the order build_sort_keys walks `ms` in.
        std::vector<uint8_t> keep(total, 1u);
        const size_t n = static_cast<size_t>(topn_limit);
        if (n < total) {
            std::vector<SortKeySpec> spec{
                SortKeySpec{static_cast<size_t>(sort_p1_index), sort_ascending}};
            std::vector<SortKeyColumn> keys;
            if (!build_sort_keys(ms, spec, total, keys, err)) return;
            SortKeyCmp cmp{keys};
            std::vector<uint32_t> idx(total);
            std::iota(idx.begin(), idx.end(), 0u);
            std::nth_element(idx.begin(), idx.begin() + static_cast<ptrdiff_t>(n - 1),
                             idx.end(), cmp);
            const uint32_t boundary = idx[n - 1];
            // Keep every row the boundary row does NOT strictly precede: the n best
            // plus every row tied with the n-th. Dropping only strictly-worse rows is
            // what makes the downstream TopNSink's answer identical to the un-pushed
            // plan's.
            for (size_t g = 0; g < total; ++g)
                keep[g] = cmp(boundary, static_cast<uint32_t>(g)) ? 0u : 1u;
        }

        size_t base = 0;
        for (LatmatRowGroup& rg : rgs) {
            const uint32_t surv = rg.p1->num_rows();
            std::vector<uint32_t> winners;
            for (uint32_t s = 0; s < surv; ++s) {
                if (keep[base + s]) winners.push_back(s);
            }
            base += surv;
            if (winners.empty()) continue;   // this row group is never read again
            LatmatPass2Item item;
            item.path = rg.path;
            item.rg_idx = rg.rg_idx;
            // One byte per row group row — rugo's decoder indexes the mask by row
            // offset. `winners` is ascending, so the surviving rows keep their
            // original relative order and line up positionally with pass 2's output.
            item.row_mask.assign(rg.num_rows, 0u);
            for (uint32_t w : winners) item.row_mask[rg.positions[w]] = 1u;
            item.p1 = (winners.size() == surv) ? rg.p1 : take_rows(rg.p1, winners, err);
            if (err.code != 0) return;
            work_index.emplace(work_key(item.path, item.rg_idx), work.size());
            work.push_back(std::move(item));
        }
    }

    // ── PASS 2 + assembly ─────────────────────────────────────────────────────────
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<LatmatScanGlobal&>(gs);
        while (true) {
            int submit_start, submit_end;
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                if (!g.pass1_done) {
                    // The barrier. Every other worker parks on g.mtx (detached from
                    // Python — there is no Python here) until the boundary exists.
                    std::vector<LatmatRowGroup> rgs;
                    ErrCtx p1err;
                    run_pass1(rgs, p1err);
                    if (p1err.code == 0)
                        reduce_to_topn(rgs, g.work, g.work_index, p1err);
                    g.pass1_done = true;
                    if (p1err.code != 0) {
                        g.pass1_err = p1err.code;
                        err = p1err;
                        return SourceResult::FINISHED;
                    }
                }
                if (g.pass1_err != 0) {
                    err.code = g.pass1_err;
                    err.msg = "LatmatScanSource: pass 1 failed on another worker";
                    return SourceResult::FINISHED;
                }
                const int n_items = static_cast<int>(g.work.size());
                submit_start = g.next_to_submit;
                submit_end = submit_start;
                while (submit_end < n_items &&
                       (submit_end - g.results_received) < in_flight_limit) {
                    submit_end += 1;
                }
                g.next_to_submit = submit_end;
                if (g.results_received >= g.next_to_submit) return SourceResult::FINISHED;
                g.results_received += 1;
            }

            for (int i = submit_start; i < submit_end; ++i) {
                const LatmatPass2Item& it = g.work[static_cast<size_t>(i)];
                if (!submit(p2_pipeline, *p2_column_names, it.path, it.rg_idx,
                            &it.row_mask, err))
                    return SourceResult::FINISHED;
            }

            rugo::MorselRef result;
            const auto _tr_idx = BS::this_thread::get_index();
            const uint16_t _tr_worker =
                _tr_idx.has_value() ? static_cast<uint16_t>(*_tr_idx) : 0xFFFFu;
            TraceHandle _tr_wait = trace_begin(TC_IO_WAIT, p2_pipeline->trace_node_id(), 0,
                                               0xFFFFFFFFu, _tr_worker);
            bool got = p2_pipeline->wait_and_get_result(result);
            trace_end(_tr_wait, 0, 0);
            if (!got) {
                err.code = 1;
                err.msg = "LatmatScanSource: pass-2 pipeline drained with result(s) missing";
                return SourceResult::FINISHED;
            }
            if (!result.success) {
                err.code = 1;
                static thread_local std::string p2_err_buf;
                p2_err_buf = "LatmatScanSource (pass 2): " +
                             (result.error.empty() ? std::string("parquet decode error")
                                                   : result.error);
                err.msg = p2_err_buf.c_str();
                return SourceResult::FINISHED;
            }
            if (result.empty_filtered) {
                // rugo's dictionary decode-skip is disabled under a row_mask
                // (io_pipeline.hpp checks `item.row_mask.empty()` before consulting
                // dict_preds_), so a masked submit cannot legitimately come back
                // empty. If it ever does, the pass-1 rows for this row group would be
                // silently dropped from the answer — fail loud, never quietly.
                err.code = 1;
                err.msg = "LatmatScanSource: masked pass-2 row group came back "
                          "empty_filtered — its pass-1 survivors would be lost";
                return SourceResult::FINISHED;
            }
            if (result.columns.size() != p2_column_names->size()) {
                err.code = 1;
                err.msg = "LatmatScanSource: pass-2 decoded column count does not match "
                          "the pass-2 projection (schema evolution is not supported)";
                return SourceResult::FINISHED;
            }

            // Find this result's pass-1 partner. Results come back in completion
            // order, so (path, rg_idx) is the key — the same pairing the trampoline's
            // `_combine_pass1_pass2_row_group` does with its p1 cache.
            auto wit = g.work_index.find(work_key(result.path, result.rg_idx));
            if (wit == g.work_index.end()) {
                err.code = 1;
                err.msg = "LatmatScanSource: pass-2 result has no matching pass-1 row group";
                return SourceResult::FINISHED;
            }
            const LatmatPass2Item* item = &g.work[wit->second];
            MorselPtr p2m = build_morsel(p2_build, result, *p2_column_names, err);
            if (err.code != 0) return SourceResult::FINISHED;
            if (p2m->num_rows() != item->p1->num_rows()) {
                // The masked decode must yield exactly the rows the mask selected. A
                // mismatch means the two halves would be zipped out of alignment,
                // silently pairing one row's key with another row's payload.
                err.code = 1;
                err.msg = "LatmatScanSource: masked pass-2 row count does not match the "
                          "pass-1 survivor count for this row group";
                return SourceResult::FINISHED;
            }

            auto m = std::make_shared<CxxMorsel>();
            m->names = *out_names;
            m->columns.reserve(out_names->size());
            for (size_t j = 0; j < out_names->size(); ++j) {
                const int i1 = (*out_from_p1)[j];
                const int i2 = (*out_from_p2)[j];
                if (i1 >= 0) {
                    m->columns.push_back(item->p1->columns[static_cast<size_t>(i1)]);
                } else if (i2 >= 0) {
                    m->columns.push_back(p2m->columns[static_cast<size_t>(i2)]);
                } else {
                    err.code = 1;
                    err.msg = "LatmatScanSource: output column maps to neither pass";
                    return SourceResult::FINISHED;
                }
            }
            out = std::move(m);
            return SourceResult::HAVE_MORE;
        }
    }
};

}  // namespace opteryx::engine
