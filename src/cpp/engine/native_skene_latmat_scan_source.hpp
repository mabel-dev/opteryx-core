#pragma once
// src/cpp/engine/native_skene_latmat_scan_source.hpp — the zero-Python two-pass
// late-materialization skene scan Source. The skene twin of
// native_latmat_scan_source.hpp (R3 / parquet), and the same idea:
//
//   `SELECT <wide> FROM t WHERE <pred> ORDER BY <col> LIMIT n` (ClickBench Q24)
//   decodes every projected column of every row and then throws all but n away.
//   For a 105-column `SELECT *` over 99M rows that is the wrong amount of work by
//   two orders of magnitude, and it is the whole of skene's ClickBench deficit
//   (measured 2026-08-08: Q24 was 7755ms of skene's 21083ms suite, against 787ms
//   on the same data in parquet, which already has this path).
//
//   pass 1  decode ONLY the predicate columns + the sort key, for every file;
//           evaluate the predicate per file; keep the SURVIVORS' SORT KEY and
//           their row positions — and nothing else.
//   reduce  across ALL files, find the LIMIT boundary in the sort key and drop
//           every survivor strictly worse than it (n rows plus any ties at the
//           boundary). A file left with no candidate is never opened again.
//   pass 2  decode the FULL projection for the files that still hold a candidate,
//           and gather just those rows.
//
// ── Relationship to the single-pass Source's reader-side filter ────────────────────
// They save different things and they compose. The single-pass Source
// (native_skene_scan_source.hpp) applies the pushed predicate to save the engine
// FILTER's work; this Source uses the same predicate to save DECODE work — the 104
// columns Q24 never looks at, for the 99M rows it discards.
//
// Since the 2026-08-21 ruling, both take the predicate from the same place.
// FileSystemTable::can_push ACCEPTS for skene, so the pushdown strategy consumes the
// Filter node and the predicate arrives on `scan.predicates`;
// compiler.py::_skene_latmat_scan_plan composes those pushed conjuncts with any
// residual Filter still above the scan (a conjunct the connector declined) into ONE
// pass-1 program.
//
// What changed for THIS Source is the safety argument, and it got stricter. It used
// to lean on the Filter above re-checking every candidate; there is now usually no
// Filter above. Pass 1 drops a row only if (a) it fails the predicate or (b) it is
// strictly worse than the n-th best surviving sort key, which the downstream TopNSink
// would have dropped. (b) is unchanged. (a) is now load-bearing on its own: a pushed
// conjunct pass 1 fails to evaluate is applied by NOTHING. Which is why the compiler
// DECLINES this Source (falling through to the single-pass Source, which applies the
// predicate itself) for any predicate it cannot lower for pass 1, rather than
// planning a pass 1 that quietly evaluates a subset.
//
// ── Differences from the parquet twin, all forced by the format ────────────────────
//   * The pass-1/pass-2 unit is a ROW GROUP, and a .skene file holds up to 16 of
//     them. Both passes claim from the same flat (file, row group) list, so a
//     surviving row group is re-decoded on its own rather than dragging its
//     fifteen neighbours back in with it. That shared list is also where ROW-GROUP
//     ZONE-MAP pruning happens (SkeneClaimSet::build), so a row group the footer
//     statistics prove empty is opened by NEITHER pass — pass 1 does not claim it,
//     and pass 2 only revisits what pass 1 kept.
//   * skene::read_morsel has no row-mask parameter, so pass 2 decodes a surviving
//     file's projected columns in full and gathers the winners afterwards. The win is
//     therefore entirely in the FILES not opened, which is the dominant term: a
//     LIMIT 10 over a full ClickBench scan leaves a handful of row groups standing.
//   * Pass 1 keeps only the SORT KEY of each survivor, never the predicate columns.
//     Pass 2 re-decodes the predicate columns as part of the projection anyway (they
//     are projected — the Filter above reads them), so carrying them across the
//     barrier would buy nothing and would hold, for ClickBench Q24, every matching
//     URL string in memory until the boundary was known. The parquet twin keeps its
//     pass-1 columns because its masked pass 2 deliberately does NOT re-read them.
//   * Pass 1 is genuinely parallel here (workers claim row groups off an atomic counter,
//     exactly as NativeSkeneScanSource does), where the parquet twin runs pass 1 on
//     one worker and leans on rugo's decode pool for parallelism. There is no such
//     pool for skene — skene::read_morsel is a pure function a worker calls inline —
//     so parallelism has to be here or nowhere.
//
// ── The reduction ─────────────────────────────────────────────────────────────────
// `reduce_to_topn` does NOT reimplement an ordering. It builds draken's own
// normalized sort keys (`build_sort_keys`) over the pass-1 survivors and uses
// draken's own comparator (`SortKeyCmp`) — the SAME definition the downstream
// TopNSink sorts with — then keeps every row that is not strictly worse than the
// n-th best:
//
//     nth_element(idx, idx + n - 1, cmp);  b = idx[n-1];
//     keep[r] = !cmp(b, r);          // r is not strictly worse than the boundary
//
// Because the comparator is shared, this is correct BY CONSTRUCTION for every key
// type, for ties (a tied row compares neither-before-nor-after `b`, so `!cmp(b,r)`
// holds and it is kept), and for NULLs — there is no separate null or NaN rule
// written here at all, which is precisely the class of bug the parquet trampoline's
// hand-written reduction shipped twice.
//
// ── Threading ─────────────────────────────────────────────────────────────────────
// Pass 1 is a barrier: no boundary exists until every file's survivors are known.
// Workers claim pass-1 files from an atomic counter and publish their candidates
// under a mutex; whichever worker completes the LAST file runs the reduction and
// releases the barrier. Every worker then streams pass 2 concurrently, claiming work
// items from a second counter over a vector that is read-only from that point on.
//
// Planning — the file list, the pass-1/projection column split, the predicate
// program, and the eligibility gates — stays in Python
// (compiler.py::_skene_latmat_scan_plan). Nothing here touches a PyObject.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <vector>

#include "native_skene_scan_source.hpp"  // SkeneFileMapping, skene_map_decoded_columns
#include "native_sort.hpp"               // build_sort_keys / SortKeyCmp / gather_rows
#include "operator.hpp"

#include "morsels/cxx_morsel.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace opteryx::engine {

// The pass-1 predicate, in the same C ABI the parquet latmat path already uses:
// `int fn(void* ctx, DrakenVector** cols, int ncols, uint32_t num_rows,
//         uint8_t* out_mask)` — opteryx_pass1_predicate_eval (evaluation.pyx),
// with `ctx` a Pass1PredCtx* resolved once on the planning thread by
// Pass1PredResolver and kept alive by the NativePlan. Only draken's DrakenVector
// and that pointer cross the boundary.
using SkeneLatmatPredFn = int (*)(void*, DrakenVector**, int, uint32_t, uint8_t*);

// One ROW GROUP's pass-1 survivors. The unit is (file, row group) throughout —
// see SkeneClaim in native_skene_scan_source.hpp for why a file is too coarse.
struct SkeneLatmatCandidates {
    SkeneClaim            claim{0, 0};
    std::vector<uint32_t> positions;   // ascending ORIGINAL row indices of survivors
    MorselPtr             key;         // ONE column: the sort key, gathered to `positions`
};

// A row group that also survived the top-n reduction.
struct SkeneLatmatPass2Item {
    SkeneClaim            claim{0, 0};
    std::vector<uint32_t> rows;        // ascending ORIGINAL row indices to materialize
};

struct SkeneLatmatGlobal : GlobalSourceState {
    // Every file mapped once and every (file, row group) pair flattened, shared
    // by BOTH passes: pass 2 re-reads a row group pass 1 already opened, so
    // remapping would be pure waste on top of the claim-unit change.
    std::once_flag init;
    bool           init_ok = false;
    std::string    init_err;
    SkeneClaimSet  work_set;

    // ── pass 1 ────────────────────────────────────────────────────────────────────
    std::atomic<size_t> p1_next{0};       // (file, row group) claim counter
    std::atomic<bool>   abort{false};     // a worker failed; stop claiming
    std::atomic<bool>   pass2_ready{false};

    std::mutex              mtx;
    std::condition_variable cv;
    size_t                                p1_completed = 0;
    std::vector<SkeneLatmatCandidates>    candidates;

    // First error wins and is never overwritten: `ErrCtx::msg` is a borrowed
    // `const char*`, so rewriting this string under a reader would dangle it.
    int         err_code = 0;
    std::string err_msg;

    // ── pass 2 ────────────────────────────────────────────────────────────────────
    // Written once during the reduction (under `mtx`, before `pass2_ready`), then
    // read-only — so the claim below needs no lock.
    std::vector<SkeneLatmatPass2Item> work;
    std::atomic<size_t>               p2_next{0};
};

class NativeSkeneLatmatScanSource : public Source {
  public:
    // Every pointer is BORROWED from the plan (NativePlan holds the owning Python
    // objects alive for the driver's lifetime), matching NativeSkeneScanSource.
    NativeSkeneLatmatScanSource(const std::vector<std::string>* files,
                                const std::vector<std::string>* p1_column_names,
                                const std::vector<int>* p1_column_types,
                                const std::vector<int>* p1_retag_units,
                                const std::vector<std::string>* out_column_names,
                                const std::vector<std::string>* out_identities,
                                const std::vector<int>* out_column_types,
                                const std::vector<int>* out_retag_units,
                                SkeneLatmatPredFn pred_fn, void* pred_ctx,
                                const std::vector<int>* pred_col_to_p1,
                                int sort_p1_index, bool sort_ascending,
                                int64_t topn_limit,
                                SkeneZoneMap zone,
                                int64_t* row_groups_total,
                                int64_t* row_groups_pruned,
                                int64_t* bytes_claimed = nullptr)
        : files_(files),
          p1_column_names_(p1_column_names),
          p1_column_types_(p1_column_types),
          p1_retag_units_(p1_retag_units),
          out_column_names_(out_column_names),
          out_identities_(out_identities),
          out_column_types_(out_column_types),
          out_retag_units_(out_retag_units),
          pred_fn_(pred_fn),
          pred_ctx_(pred_ctx),
          pred_col_to_p1_(pred_col_to_p1),
          sort_p1_index_(sort_p1_index),
          sort_ascending_(sort_ascending),
          topn_limit_(topn_limit),
          zone_(zone),
          row_groups_total_(row_groups_total),
          row_groups_pruned_(row_groups_pruned),
          bytes_claimed_(bytes_claimed) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<SkeneLatmatGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<SkeneLatmatGlobal&>(gs);

        // The claim list has to exist before either pass can start. Built once,
        // by whichever worker arrives first; a failure here is recorded like any
        // pass-1 failure so the barrier is released rather than parked on.
        std::call_once(g.init, [&g, this] {
            g.init_ok = g.work_set.build(*files_, zone_, row_groups_total_,
                                         row_groups_pruned_, g.init_err,
                                         0, nullptr, bytes_claimed_);
        });
        if (!g.init_ok) {
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                record_error(g, 1, g.init_err);
                g.pass2_ready.store(true, std::memory_order_release);
            }
            g.cv.notify_all();
            err.code = 1;
            err.msg = g.init_err.c_str();
            return SourceResult::FINISHED;
        }

        if (!g.pass2_ready.load(std::memory_order_acquire)) {
            run_pass1(g);
            // The barrier. A worker that finds pass 1 already claimed out parks here
            // (detached from Python — there is no Python here) until the boundary
            // exists, or until another worker's failure releases everyone.
            std::unique_lock<std::mutex> lock(g.mtx);
            g.cv.wait(lock, [&g] { return g.pass2_ready.load(std::memory_order_acquire); });
        }
        // Ordered by the acquire above, which pairs with the release that publishes
        // `pass2_ready` after `err_code` / `work` are final.
        if (g.err_code != 0) {
            err.code = g.err_code;
            err.msg = g.err_msg.c_str();
            return SourceResult::FINISHED;
        }

        while (true) {
            const size_t i = g.p2_next.fetch_add(1, std::memory_order_relaxed);
            if (i >= g.work.size()) return SourceResult::FINISHED;
            MorselPtr m = run_pass2(g, g.work[i], err);
            if (err.code != 0) return SourceResult::FINISHED;
            // The reduction never emits an item with no rows, so this is a genuine
            // "nothing to yield" only if a gather produced an empty morsel — skip it
            // rather than push a zero-row morsel downstream.
            if (!m || m->num_rows() == 0) continue;
            out = std::move(m);
            return SourceResult::HAVE_MORE;
        }
    }

  private:
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
    // Decode one file's pass-1 columns, evaluate the predicate, and keep the
    // survivors' sort key. Runs on a worker thread with nothing shared.
    bool pass1_row_group(SkeneLatmatGlobal& g, SkeneClaim claim,
                         SkeneLatmatCandidates& out, ErrCtx& err,
                         std::string& err_buf) {
        const std::string& path = (*files_)[claim.file_idx];
        const SkeneFileMapping& mapping = g.work_set.mapping(claim.file_idx);
        skene::ReadOptions options;
        options.columns = *p1_column_names_;

        auto m = std::make_shared<CxxMorsel>();
        skene::Status status =
            skene::read_morsel(mapping.data(), mapping.size(), claim.row_group,
                               options, m.get());
        if (!status.is_ok()) {
            err.code = 1;
            err_buf = "NativeSkeneLatmatScanSource (pass 1): '" + path +
                      "' row group " + std::to_string(claim.row_group) + ": " +
                      status.message();
            err.msg = err_buf.c_str();
            return false;
        }

        std::vector<size_t> decoded_to_plan;
        if (!skene_map_decoded_columns(*m, *p1_column_names_, *p1_column_types_,
                                       *p1_retag_units_, path, decoded_to_plan, err,
                                       err_buf))
            return false;

        const uint32_t nrows = m->num_rows();
        if (nrows == 0) return true;   // an empty row group has no candidates

        // Plan position -> decoded position. The predicate addresses its columns by
        // PLAN position (Pass1PredCtx::col_idx, resolved at plan time), so this is
        // what makes the wiring independent of the order skene hands columns back in.
        std::vector<size_t> plan_to_decoded(p1_column_names_->size(),
                                            p1_column_names_->size());
        for (size_t i = 0; i < decoded_to_plan.size(); ++i)
            plan_to_decoded[decoded_to_plan[i]] = i;

        std::vector<DrakenVector*> cols;
        cols.reserve(pred_col_to_p1_->size());
        for (int pi : *pred_col_to_p1_) {
            if (pi < 0 || static_cast<size_t>(pi) >= plan_to_decoded.size() ||
                plan_to_decoded[static_cast<size_t>(pi)] >= m->columns.size()) {
                err.code = 1;
                err_buf = "NativeSkeneLatmatScanSource: pass-1 predicate column index "
                          "out of range for the pass-1 layout";
                err.msg = err_buf.c_str();
                return false;
            }
            cols.push_back(&m->columns[plan_to_decoded[static_cast<size_t>(pi)]].view);
        }

        const size_t nbytes = (static_cast<size_t>(nrows) + 7u) >> 3;
        std::vector<uint8_t> mask(nbytes, 0u);
        if (pred_fn_(pred_ctx_, cols.data(), static_cast<int>(cols.size()), nrows,
                     mask.data()) != 0) {
            err.code = 1;
            err_buf = "NativeSkeneLatmatScanSource: pass-1 predicate evaluation failed "
                      "on '" + path + "'";
            err.msg = err_buf.c_str();
            return false;
        }
        for (uint32_t r = 0; r < nrows; ++r) {
            if ((mask[r >> 3] >> (r & 7)) & 1u) out.positions.push_back(r);
        }
        if (out.positions.empty()) return true;

        // Carry the SORT KEY only across the barrier — see this file's header. The
        // whole pass-1 morsel (the predicate columns, which for Q24 means every
        // matching URL string) is released as soon as this returns.
        if (sort_p1_index_ < 0 ||
            static_cast<size_t>(sort_p1_index_) >= plan_to_decoded.size()) {
            err.code = 1;
            err_buf = "NativeSkeneLatmatScanSource: sort-key index out of range for the "
                      "pass-1 layout";
            err.msg = err_buf.c_str();
            return false;
        }
        const size_t key_decoded = plan_to_decoded[static_cast<size_t>(sort_p1_index_)];
        if (key_decoded >= m->columns.size()) {
            err.code = 1;
            err_buf = "NativeSkeneLatmatScanSource: the sort key was not decoded in "
                      "pass 1";
            err.msg = err_buf.c_str();
            return false;
        }
        auto key_src = std::make_shared<CxxMorsel>();
        key_src->names.push_back(m->names[key_decoded]);
        key_src->columns.push_back(m->columns[key_decoded]);
        out.key = take_rows(key_src, out.positions, err);
        if (err.code != 0) return false;
        out.claim = claim;
        return true;
    }

    // Claim ROW GROUPS until the list is exhausted (or another worker failed), then
    // make sure the finishing condition is evaluated — the last worker out runs the
    // reduction and releases the barrier.
    void run_pass1(SkeneLatmatGlobal& g) {
        const std::vector<SkeneClaim>& claims = g.work_set.claims();
        const size_t n = claims.size();
        while (!g.abort.load(std::memory_order_relaxed)) {
            const size_t idx = g.p1_next.fetch_add(1, std::memory_order_relaxed);
            if (idx >= n) break;

            SkeneLatmatCandidates cand;
            ErrCtx ferr;
            std::string ferr_buf;
            const bool ok = pass1_row_group(g, claims[idx], cand, ferr, ferr_buf);
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                if (!ok) {
                    record_error(g, ferr.code != 0 ? ferr.code : 1,
                                 ferr.msg != nullptr
                                     ? std::string(ferr.msg)
                                     : std::string("NativeSkeneLatmatScanSource: pass 1 "
                                                   "failed"));
                } else if (!cand.positions.empty()) {
                    g.candidates.push_back(std::move(cand));
                }
                g.p1_completed += 1;
                finish_pass1_if_done(g, n);
            }
        }
        // A worker that never claimed a row group (or broke out on another worker's abort)
        // still has to evaluate the condition: it may be the one that observed the
        // final state.
        std::lock_guard<std::mutex> lock(g.mtx);
        finish_pass1_if_done(g, n);
    }

    // Both called with g.mtx held.
    static void record_error(SkeneLatmatGlobal& g, int code, std::string msg) {
        if (g.err_code == 0) {
            g.err_code = code;
            g.err_msg = std::move(msg);
        }
        g.abort.store(true, std::memory_order_relaxed);
    }

    void finish_pass1_if_done(SkeneLatmatGlobal& g, size_t n) {
        if (g.pass2_ready.load(std::memory_order_relaxed)) return;
        // Release on failure immediately: workers still decoding will find `abort`
        // and stop, and every waiter must be told rather than parked forever.
        if (g.err_code == 0) {
            if (g.p1_completed < n) return;
            reduce_to_topn(g);
        }
        g.candidates.clear();   // the sort-key morsels are dead once `work` exists
        g.pass2_ready.store(true, std::memory_order_release);
        g.cv.notify_all();
    }

    // ── THE REDUCTION ─────────────────────────────────────────────────────────────
    // Shrink the pass-1 survivors to the rows that can still be in the top-n and turn
    // what is left into pass-2 work. Called with g.mtx held, exactly once. See this
    // file's header for why the ordering is draken's own comparator, not a rule
    // written here.
    void reduce_to_topn(SkeneLatmatGlobal& g) {
        std::vector<MorselPtr> ms;
        ms.reserve(g.candidates.size());
        size_t total = 0;
        for (SkeneLatmatCandidates& c : g.candidates) {
            ms.push_back(c.key);
            total += c.key->num_rows();
        }
        if (total == 0) return;

        // `keep[i]` over the CONCATENATED survivor row space (candidate order), which
        // is exactly the order build_sort_keys walks `ms` in.
        std::vector<uint8_t> keep(total, 1u);
        const size_t n = topn_limit_ > 0 ? static_cast<size_t>(topn_limit_) : 0;
        if (n == 0) {
            // A zero LIMIT cannot reach here (the plan-time gate requires a positive
            // one), but a silent "keep everything" would be a wrong answer's shape,
            // so say so instead.
            record_error(g, 1, "NativeSkeneLatmatScanSource: non-positive top-n limit");
            return;
        }
        if (n < total) {
            std::vector<SortKeySpec> spec{SortKeySpec{0, sort_ascending_}};
            std::vector<SortKeyColumn> keys;
            ErrCtx kerr;
            if (!build_sort_keys(ms, spec, total, keys, kerr)) {
                record_error(g, kerr.code != 0 ? kerr.code : 1,
                             kerr.msg != nullptr
                                 ? std::string(kerr.msg)
                                 : std::string("NativeSkeneLatmatScanSource: could not "
                                               "build the top-n sort key"));
                return;
            }
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
            for (size_t i = 0; i < total; ++i)
                keep[i] = cmp(boundary, static_cast<uint32_t>(i)) ? 0u : 1u;
        }

        size_t base = 0;
        for (SkeneLatmatCandidates& c : g.candidates) {
            const uint32_t surv = c.key->num_rows();
            SkeneLatmatPass2Item item;
            item.claim = c.claim;
            for (uint32_t s = 0; s < surv; ++s) {
                if (keep[base + s]) item.rows.push_back(c.positions[s]);
            }
            base += surv;
            if (item.rows.empty()) continue;   // this row group is never decoded again
            g.work.push_back(std::move(item));
        }
    }

    // ── PASS 2 ────────────────────────────────────────────────────────────────────
    // Decode one surviving ROW GROUP's FULL projection and gather the candidate rows.
    MorselPtr run_pass2(SkeneLatmatGlobal& g, const SkeneLatmatPass2Item& item,
                        ErrCtx& err) {
        // Pass-2 error text must outlive the call (ErrCtx.msg is a borrowed const
        // char*) AND survive concurrent workers, which a Source member would not —
        // every worker streams pass 2 at once. Same device as LatmatScanSource's.
        static thread_local std::string err_msg_;
        const std::string& path = (*files_)[item.claim.file_idx];
        const SkeneFileMapping& mapping = g.work_set.mapping(item.claim.file_idx);
        skene::ReadOptions options;
        options.columns = *out_column_names_;

        auto m = std::make_shared<CxxMorsel>();
        skene::Status status =
            skene::read_morsel(mapping.data(), mapping.size(), item.claim.row_group,
                               options, m.get());
        if (!status.is_ok()) {
            err.code = 1;
            err_msg_ = "NativeSkeneLatmatScanSource (pass 2): '" + path +
                       "' row group " + std::to_string(item.claim.row_group) + ": " +
                       status.message();
            err.msg = err_msg_.c_str();
            return nullptr;
        }

        std::vector<size_t> decoded_to_plan;
        if (!skene_map_decoded_columns(*m, *out_column_names_, *out_column_types_,
                                       *out_retag_units_, path, decoded_to_plan, err,
                                       err_msg_))
            return nullptr;

        // Reorder into the plan's projection order and rename to plan identities: the
        // compiler declares the emitted layout as `out_identities_`, positionally.
        auto ordered = std::make_shared<CxxMorsel>();
        ordered->names.resize(out_column_names_->size());
        ordered->columns.resize(out_column_names_->size());
        for (size_t i = 0; i < decoded_to_plan.size(); ++i) {
            const size_t want = decoded_to_plan[i];
            ordered->columns[want] = m->columns[i];
            ordered->names[want] = (*out_identities_)[want];
        }

        // A candidate row index is a position in this ROW GROUP, which pass 1 read at
        // full width — so it must be in range here. If it is not, the two passes saw
        // different content and the gather would silently read a neighbouring row.
        const uint32_t nrows = ordered->num_rows();
        if (!item.rows.empty() && item.rows.back() >= nrows) {
            err.code = 1;
            err_msg_ = "NativeSkeneLatmatScanSource (pass 2): '" + path +
                       "' row group " + std::to_string(item.claim.row_group) +
                       ": candidate row is past the end of the row group — its two "
                       "passes did not read the same content";
            err.msg = err_msg_.c_str();
            return nullptr;
        }
        return take_rows(ordered, item.rows, err);
    }

    const std::vector<std::string>* files_;

    // Pass 1: the predicate's columns plus the sort key, by in-file name, with the
    // bound physical type and TIMESTAMP64 retag unit (-1 = none) per column.
    const std::vector<std::string>* p1_column_names_;
    const std::vector<int>*         p1_column_types_;
    const std::vector<int>*         p1_retag_units_;

    // Pass 2 / output: the scan's full projection, same four parallel arrays plus the
    // plan identities the columns are emitted under.
    const std::vector<std::string>* out_column_names_;
    const std::vector<std::string>* out_identities_;
    const std::vector<int>*         out_column_types_;
    const std::vector<int>*         out_retag_units_;

    SkeneLatmatPredFn        pred_fn_;
    void*                    pred_ctx_;
    // pred_col_to_p1_[k] = index into p1_column_names_ of the predicate's k-th column,
    // in the order Pass1PredCtx's col_idx expects. Resolved at plan time.
    const std::vector<int>*  pred_col_to_p1_;

    int     sort_p1_index_;     // the sort key's position within the pass-1 columns
    bool    sort_ascending_;
    int64_t topn_limit_;
    // ROW-GROUP zone terms and the run-time counts the shared claim builder writes
    // back. Pruning happens once, at claim time, and therefore covers both passes:
    // pass 2's work items are drawn from pass 1's survivors, so a row group that
    // was never claimed cannot reappear.
    SkeneZoneMap zone_;
    int64_t* row_groups_total_;
    int64_t* row_groups_pruned_;
    // On-disk extent of the CLAIMED row groups — see SkeneClaimSet::build. Claim
    // time covers BOTH passes, since pass 2 draws from pass 1's survivors.
    int64_t* bytes_claimed_;
};

}  // namespace opteryx::engine
