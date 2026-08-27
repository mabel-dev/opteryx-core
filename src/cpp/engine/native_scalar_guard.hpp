#pragma once
// src/cpp/engine/native_scalar_guard.hpp — runtime cardinality guard for an
// uncorrelated scalar subquery.
//
// A scalar subquery is one VALUE. When the planner cannot prove from plan
// structure alone that the subquery emits at most one row (decorrelate_subquery's
// `_uncorrelated_single_row_proof`), it emits a ScalarSubqueryGuard step instead
// of refusing the query, and this source is what enforces it: the subquery's
// pipeline materializes into a MorselBuffer (an ordinary breaker), and the
// dependent pipeline reads the buffer through this guard —
//
//   > 1 row  -> the SQL-standard cardinality violation, via ErrCtx. Code
//               kErrCodeDataError marks the message as a USER-FACING data error
//               (build_terminal_exc raises it as opteryx DataError, message
//               verbatim), distinct from code 1's internal-fault channel.
//   exactly 1 row -> passed through untouched, zero copy.
//   0 rows   -> ONE all-NULL row, gathered against a plan-typed zero-row schema
//               morsel exactly the way FULL OUTER's tail emits its NULL probe
//               half (kGatherNullRow over make_empty_col columns). SQL says a
//               zero-row scalar subquery IS NULL — an empty stream here would
//               instead vanish every outer row through the cross join above,
//               which answers `(subq) IS NULL` wrongly.
//
// The guard's decision needs the WHOLE subquery result, so it must sit behind a
// pipeline barrier — that is why it is a Source over a finalized buffer and not
// a streaming Operator (Operators have no end-of-stream hook to detect the
// zero-row case).

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "operator.hpp"
#include "pipeline_buffers.hpp"   // MorselBuffer
#include "native_sort.hpp"        // gather_rows, make_empty_col, kGatherNullRow
#include "native_join2.hpp"       // array_child_missing

namespace opteryx::engine {

struct ScalarGuardGlobal : GlobalSourceState {
    std::atomic<bool> claimed{false};
};

struct ScalarGuardSource : Source {
    MorselBuffer* buf;
    // Zero-row, plan-typed columns (names = column identities) — what the
    // all-NULL row is gathered against when the subquery returns nothing.
    MorselPtr schema;

    ScalarGuardSource(MorselBuffer* b, MorselPtr s) : buf(b), schema(std::move(s)) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<ScalarGuardGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs_, LocalSourceState&,
                            MorselPtr& out, ErrCtx& err) override {
        auto& g = static_cast<ScalarGuardGlobal&>(gs_);
        // One emission total: exactly one worker claims the (<= 1 row) result.
        if (g.claimed.exchange(true)) return SourceResult::FINISHED;

        if (!buf->seal()) {
            err.code = 1;
            err.msg = buf->error().c_str();
            return SourceResult::FINISHED;
        }
        uint64_t rows = 0;
        MorselPtr the_row;
        const size_t n_claims = buf->claim_count();
        for (size_t i = 0; i < n_claims && rows <= 1; ++i) {
            MorselPtr m;
            if (!buf->get(i, m)) {
                err.code = 1;
                err.msg = buf->error().c_str();
                return SourceResult::FINISHED;
            }
            rows += m->num_rows();
            if (m->num_rows() > 0 && the_row == nullptr) the_row = m;
        }
        if (rows > 1) {
            err.code = kErrCodeDataError;
            err.msg = "more than one row returned by a subquery used as an expression";
            return SourceResult::FINISHED;
        }
        if (rows == 1) {
            out = the_row;              // pass through, zero copy
            return SourceResult::HAVE_MORE;
        }
        // Zero rows: SQL's answer is NULL. Same emit as FULL OUTER's NULL half.
        for (const CxxColumn& c : schema->columns) {
            if (array_child_missing(c)) {
                err.code = 1;
                err.msg = "scalar subquery guard: ARRAY output has no child vector "
                          "to emit its NULL against — fail loud, never silent "
                          "corruption";
                return SourceResult::FINISHED;
            }
        }
        std::vector<uint32_t> order(1, kGatherNullRow);
        std::vector<MorselPtr> ms{schema};
        std::vector<uint32_t> row_m(1, 0), row_r(1, 0);
        MorselPtr null_row = gather_rows(ms, order, 0, 1, row_m, row_r,
                                         schema->names, err);
        if (err.code != 0 || null_row == nullptr) return SourceResult::FINISHED;
        out = std::move(null_row);
        return SourceResult::HAVE_MORE;
    }
};

}  // namespace opteryx::engine
