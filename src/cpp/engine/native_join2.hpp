#pragma once
// src/cpp/engine/native_join2.hpp — the engine's general hash join: multi-column
// keys of any supported type (serialized with the SAME key encoding GROUP BY and
// DISTINCT use — null byte + raw native bytes / length-prefixed strings, so
// DECIMAL128 and string keys come for free), and four probe modes:
//
//   INNER       — fan-out matches (build payload first, then probe payload).
//   LEFT_OUTER  — probe side is the PRESERVED side (the compiler maps the plan's
//                 preserved leg to the probe): every probe row emits; unmatched
//                 rows carry NULL build payload.
//   SEMI        — emit probe rows that have >=1 match (probe columns only).
//   ANTI        — null-aware NOT IN: emit probe rows with NO match; a NULL probe
//                 key never matches-out (NULL NOT IN <non-empty> is NULL → drop);
//                 if the build side contained ANY NULL key, NOTHING passes; an
//                 EMPTY build side passes every probe row (NOT IN () is TRUE).
//
// Anything outside a mode's contract sets ErrCtx — loud, never silently wrong.

#include <algorithm>   // sort/lower_bound/upper_bound — ASOF probe
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "operator.hpp"
#include "native_group_sinks.hpp"   // shared engine helpers
#include "native_key_hash.hpp"     // compute_row_hashes — draken owns the key hash
#include "native_sort.hpp"          // gather_rows, sort_row_valid, string helpers
#include "morsels/cxx_hash.h"       // cxx_hash_c — draken owns the join-key hash
#include "carchar_join_index.hpp"   // opteryx::carchar::CarcharJoinIndex

namespace opteryx::engine {

enum class JoinMode : uint8_t { Inner = 0, LeftOuter = 1, Semi = 2, Anti = 3 };

// The build side RETAINS its payload columns rather than copying their values into a
// row-store. `morsels` holds one payload-column-only view per accepted build morsel
// (a CxxColumn is a shared_ptr to its VectorOwner, so slicing to the payload columns
// genuinely releases the rest of the morsel); `row_m`/`row_r` map a build row id to
// (morsel, row within it) — exactly the shape gather_rows already consumes, and the
// same thing SortSink does with its buffered input. That is what lets the emit path
// be the engine's ONE row gather for both halves of the output row, instead of a
// bespoke per-type materializer that has to be taught every new type.
struct Join2BuildLocal : LocalSinkState {
    std::vector<MorselPtr> morsels;    // payload-column views, in arrival order
    std::vector<uint32_t> row_m;       // build row id -> index into `morsels`
    std::vector<uint32_t> row_r;       // build row id -> row within that morsel
    std::vector<uint64_t> row_hashes;  // parallel to build rows: the 64-bit key hash
    std::vector<uint64_t> asof_keys;   // ASOF only: per build row, sort_num_key
    uint32_t next_row = 0;
    bool saw_null_key = false;
};
struct Join2BuildGlobal : GlobalSinkState {
    std::mutex mtx;
    // Global build table, keyed on the 64-bit draken hash → build-row-list. Built in
    // combine() under the mutex (single-threaded index construction, exactly as the
    // old unordered_map merge did); probed read-only/concurrently via const methods.
    opteryx::carchar::CarcharJoinIndex index;
    std::vector<MorselPtr> morsels;    // every worker's retained views, concatenated
    std::vector<uint32_t> row_m;       // global build row id -> index into `morsels`
    std::vector<uint32_t> row_r;       // global build row id -> row within that morsel
    // Zero-row, plan-typed payload columns. Used ONLY when no build morsel was ever
    // retained (a build side that streamed zero rows): gather_rows takes its column
    // count and types from ms.front(), so it needs a schema to emit a LEFT OUTER's
    // all-NULL build half against. Never consulted when real morsels exist — those
    // carry the authoritative (data-observed) types.
    MorselPtr schema_morsel;
    std::vector<uint64_t> asof_keys;   // ASOF only: parallel to build rows
    // ASOF only: draken hash → index into asof_sorted, materialized once at first
    // probe (ensure_sorted). CarcharJoinIndex stores each key's rows unsorted, so the
    // bisect needs this sorted view — keyed on the same 64-bit hash via a CarcharIndex.
    opteryx::carchar::CarcharIndex asof_index;
    std::vector<std::vector<int64_t>> asof_sorted;
    uint32_t total_rows = 0;
    bool saw_null_key = false;
};

struct Join2BuildSink : Sink {
    std::vector<size_t> key_idx;
    std::vector<size_t> payload_col_idx;
    std::vector<DrakenType> payload_types;             // PLAN-known — see engine.hpp
    std::vector<const LogicalType*> payload_logical;   // set_join2_build_sink's comment
    int asof_idx = -1;   // >= 0: ASOF build — capture the asof column's normalized
                         // order key per row (rows with a NULL asof value are
                         // skipped: they can never satisfy the MATCH_CONDITION)

    Join2BuildSink(std::vector<size_t> keys, std::vector<size_t> payload_idx,
                   std::vector<DrakenType> types, std::vector<const LogicalType*> logical,
                   int asof = -1)
        : key_idx(std::move(keys)), payload_col_idx(std::move(payload_idx)),
          payload_types(std::move(types)), payload_logical(std::move(logical)),
          asof_idx(asof) {}

    // Zero-row payload columns at the PLAN-known types. This is the fallback schema
    // for a build side that streams zero rows (a filtered-to-empty subquery): with no
    // retained morsel there is nothing for gather_rows to read a column count or type
    // from, and a LEFT OUTER still has to emit an all-NULL build half. When any real
    // morsel was retained this is never consulted — observed types beat plan types.
    MorselPtr make_schema_morsel() const {
        auto m = std::make_shared<CxxMorsel>();
        m->columns.reserve(payload_col_idx.size());
        for (size_t c = 0; c < payload_col_idx.size(); ++c)
            m->columns.push_back(make_empty_col(payload_types[c], payload_logical[c]));
        m->names.resize(payload_col_idx.size());
        m->zero_col_rows = 0;
        return m;
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        auto g = std::make_unique<Join2BuildGlobal>();
        g->schema_morsel = make_schema_morsel();
        return g;
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<Join2BuildLocal>();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<Join2BuildLocal&>(ls);
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;
        // Retain this morsel's PAYLOAD COLUMNS ONLY. A CxxColumn is a shared_ptr to
        // its VectorOwner, so copying just the payload columns keeps those buffers
        // alive and lets every other column of `in` be released when the build
        // pipeline drops it. No values are copied and nothing is decompressed: a
        // dict-encoded build column stays dict-encoded until the probe gathers it.
        //
        // There is no payload TYPE check here any more, and no type learning: the
        // retained vectors ARE the types, and the emit path is the same gather every
        // other operator uses. A payload column is only ever carried, never compared,
        // so its type is not the join's business.
        auto view = std::make_shared<CxxMorsel>();
        view->columns.reserve(payload_col_idx.size());
        for (size_t pc : payload_col_idx) view->columns.push_back(in->columns[pc]);
        view->names.resize(payload_col_idx.size());
        view->zero_col_rows = rows;
        const uint32_t mi = static_cast<uint32_t>(l.morsels.size());
        const uint32_t rows_before = l.next_row;
        l.morsels.push_back(std::move(view));

        // Draken owns the key hash for the whole morsel; per-row nullness is read from
        // the key columns (not the hash) so NULL keys are excluded as before.
        std::vector<uint64_t> rowh;
        if (!compute_row_hashes(in, key_idx, rowh, err)) return SinkResult::CONTINUE;
        for (uint32_t i = 0; i < rows; ++i) {
            // NULL in ANY key column: the row can never equi-match; record for the
            // null-aware ANTI contract and skip the table insert.
            bool any_null = false;
            for (size_t k : key_idx) {
                if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
            }
            if (any_null) {
                l.saw_null_key = true;
                continue;
            }
            if (asof_idx >= 0
                    && !sort_row_valid(in->columns[static_cast<size_t>(asof_idx)].view, i))
                continue;   // NULL asof value never satisfies the MATCH_CONDITION
            // Record WHERE the row lives instead of copying its values out.
            l.row_m.push_back(mi);
            l.row_r.push_back(i);
            if (asof_idx >= 0)
                l.asof_keys.push_back(
                    sort_num_key(in->columns[static_cast<size_t>(asof_idx)].view, i));
            l.row_hashes.push_back(rowh[i]);   // parallel to the build row just added
            ++l.next_row;
        }
        // Every row was NULL-keyed (or NULL-asof): nothing addresses this morsel, so
        // don't pin its buffers for the lifetime of the build table.
        if (l.next_row == rows_before) l.morsels.pop_back();
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<Join2BuildGlobal&>(gs);
        auto& l = static_cast<Join2BuildLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        g.saw_null_key = g.saw_null_key || l.saw_null_key;
        // Concatenate this worker's retained views, then re-base its row addresses:
        // a local morsel index becomes a global one by adding the number of morsels
        // already present. No value copying and — the point of the whole exercise —
        // no string-arena rebasing, because no arena was ever built: the slots still
        // live in the source vectors, which we hold alive.
        const uint32_t morsel_off = static_cast<uint32_t>(g.morsels.size());
        const uint32_t row_off = g.total_rows;
        g.morsels.insert(g.morsels.end(),
                         std::make_move_iterator(l.morsels.begin()),
                         std::make_move_iterator(l.morsels.end()));
        g.row_m.reserve(g.row_m.size() + l.next_row);
        g.row_r.reserve(g.row_r.size() + l.next_row);
        for (uint32_t r = 0; r < l.next_row; ++r) {
            g.row_m.push_back(morsel_off + l.row_m[r]);
            g.row_r.push_back(l.row_r[r]);
        }
        g.asof_keys.insert(g.asof_keys.end(), l.asof_keys.begin(), l.asof_keys.end());
        // Insert this worker's rows into the global index at the global row offset.
        // row_hashes[r] is the key hash of local build row r → global row row_off+r.
        g.index.reserve(g.total_rows + l.next_row);
        for (uint32_t r = 0; r < l.next_row; ++r)
            g.index.insert_row(l.row_hashes[r], static_cast<int64_t>(row_off + r));
        g.total_rows += l.next_row;
    }

    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

struct Join2Ref {
    const Join2BuildGlobal* g = nullptr;
    std::once_flag asof_sorted;   // ASOF probe: per-group rows sorted by asof key
};

// ---- INNER / LEFT_OUTER probe: fan-out matches, build payload first ----------------

struct Join2ProbeState : OperatorState {
    MorselPtr pending_in;
    uint32_t row = 0;
    std::vector<uint64_t> rowh;   // per-morsel probe-key hashes (draken-owned)
};

struct Join2ProbeOperator : Operator {
    std::vector<size_t> probe_key_idx;
    std::vector<size_t> probe_payload_idx;
    const Join2Ref* ref;
    bool left_outer;
    static constexpr size_t kBatch = 1024;
    static constexpr uint32_t kNoBuildRow = UINT32_MAX;

    Join2ProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, bool outer)
        : probe_key_idx(std::move(keys)), probe_payload_idx(std::move(payload)),
          ref(r), left_outer(outer) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<Join2ProbeState>();
    }

    // Build one output morsel for the parallel (build_row | kNoBuildRow, probe_row)
    // arrays. build_rows/probe_rows are int64 (CarcharJoinIndex row ids); an unmatched
    // LEFT/ASOF row carries kNoBuildRow in build_rows.
    MorselPtr build_output(const MorselPtr& probe_in,
                           const std::vector<int64_t>& build_rows,
                           const std::vector<int64_t>& probe_rows,
                           ErrCtx& err) {
        const Join2BuildGlobal& g = *ref->g;
        uint32_t n = static_cast<uint32_t>(build_rows.size());
        // The schema morsel always carries exactly one column per build payload
        // column, whether or not any row was ever retained.
        const size_t payload_col_count =
            g.schema_morsel ? g.schema_morsel->columns.size() : 0;
        auto out = std::make_shared<CxxMorsel>();
        out->zero_col_rows = n;
        out->columns.reserve(payload_col_count + probe_payload_idx.size());

        // Build payload: the engine's ONE row gather — the same call the probe half
        // makes below. The build side is not materialized into a row-store; its
        // columns were retained by the sink and are addressed by (morsel, row), which
        // is exactly what gather_rows consumes. Consequence: the set of types a join
        // can CARRY is now identical to the set gather_rows supports, for both halves.
        if (payload_col_count > 0) {
            // No retained morsel means the build side streamed zero rows; fall back to
            // the plan-typed zero-row schema so an all-NULL LEFT OUTER half still has
            // a column count and types to be built against.
            const bool empty_build = g.morsels.empty();
            std::vector<MorselPtr> schema_only;
            if (empty_build) schema_only.push_back(g.schema_morsel);
            const std::vector<MorselPtr>& bms = empty_build ? schema_only : g.morsels;
            if (empty_build) {
                // gather_rows recurses into an ARRAY column's child vector, which a
                // zero-row schema column does not carry — fail loud rather than read
                // an uninitialized child view.
                for (const CxxColumn& c : bms.front()->columns) {
                    if (c.view.type == DRAKEN_ARRAY) {
                        err.code = 1;
                        err.msg = "Join2Probe: ARRAY build payload with an empty build "
                                  "side has no child vector to emit NULLs against — "
                                  "fail loud, never silent corruption";
                        return nullptr;
                    }
                }
            }
            std::vector<uint32_t> order(n);
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t br = static_cast<uint32_t>(build_rows[i]);
                // LEFT OUTER / ASOF unmatched probe row: no build row exists, so hand
                // gather_rows its null-row sentinel. A build row that EXISTS but holds
                // a NULL value needs nothing here — the source vector's own validity
                // already says so and the gather honours it.
                order[i] = (br == kNoBuildRow) ? kGatherNullRow : br;
            }
            MorselPtr gathered = gather_rows(bms, order, 0, n, g.row_m, g.row_r,
                                             bms.front()->names, err);
            if (err.code != 0 || gathered == nullptr) return nullptr;
            for (CxxColumn& c : gathered->columns) out->columns.push_back(std::move(c));
        }

        // Probe payload: the engine's one row gather (validity/strings/descriptors).
        if (!probe_payload_idx.empty()) {
            uint32_t pn = probe_in->num_rows();
            auto view = std::make_shared<CxxMorsel>();
            view->columns.reserve(probe_payload_idx.size());
            for (size_t pc : probe_payload_idx) view->columns.push_back(probe_in->columns[pc]);
            view->names.resize(view->columns.size());
            view->zero_col_rows = pn;
            std::vector<uint32_t> order(n);
            for (uint32_t i = 0; i < n; ++i) order[i] = static_cast<uint32_t>(probe_rows[i]);
            std::vector<uint32_t> row_m(pn, 0), row_r(pn);
            for (uint32_t i = 0; i < pn; ++i) row_r[i] = i;
            std::vector<MorselPtr> ms{view};
            MorselPtr gathered = gather_rows(ms, order, 0, n, row_m, row_r, view->names, err);
            if (err.code != 0 || gathered == nullptr) return nullptr;
            for (CxxColumn& c : gathered->columns) out->columns.push_back(std::move(c));
        }
        return out;
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        auto& st = static_cast<Join2ProbeState&>(st_);
        const Join2BuildGlobal& g = *ref->g;
        if (st.pending_in != in) {
            st.pending_in = in;
            st.row = 0;
            if (!compute_row_hashes(in, probe_key_idx, st.rowh, err))
                return OpResult::NEED_INPUT;
        }
        uint32_t n = in->num_rows();
        std::vector<int64_t> build_rows, probe_rows;
        build_rows.reserve(kBatch);
        probe_rows.reserve(kBatch);

        while (st.row < n) {
            uint32_t row = st.row;
            bool any_null = false;
            for (size_t k : probe_key_idx) {
                if (!sort_row_valid(in->columns[k].view, row)) { any_null = true; break; }
            }
            if (any_null) {
                if (left_outer) {   // unmatched preserved-side row → NULL build payload
                    build_rows.push_back(static_cast<int64_t>(kNoBuildRow));
                    probe_rows.push_back(static_cast<int64_t>(row));
                }
            } else {
                size_t before = build_rows.size();
                // const + thread-safe fan-out: appends (build_row, probe_row) for every
                // build row whose key hash matches. Equality is 64-bit hash identity.
                g.index.append_probe_matches(st.rowh[row], static_cast<int64_t>(row),
                                             build_rows, probe_rows);
                if (build_rows.size() == before && left_outer) {
                    build_rows.push_back(static_cast<int64_t>(kNoBuildRow));
                    probe_rows.push_back(static_cast<int64_t>(row));
                }
            }
            ++st.row;
            // Flush a batch after a full probe row (a single high-fan-out row may push
            // slightly past kBatch — correct, just a larger morsel).
            if (build_rows.size() >= kBatch) {
                out = build_output(in, build_rows, probe_rows, err);
                return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::HAVE_MORE;
            }
        }
        if (!build_rows.empty()) {
            out = build_output(in, build_rows, probe_rows, err);
            return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }
};

// ---- ASOF probe: nearest-match by ordered asof key within equi partitions ----------
// LEFT semantics (unmatched probe rows emit with NULL build payload — same emit as
// LEFT OUTER, inherited from Join2ProbeOperator::build_output). Each probe row
// yields EXACTLY ONE (build_row | kNoBuildRow) pair: within its equi-key group
// (sorted by asof key on first use), the match is selected by the MATCH_CONDITION
// operator, mirroring the legacy operator's bisect table:
//   GtEq (probe >= build): largest build key <= probe key   (upper_bound - 1)
//   Gt   (probe >  build): largest build key <  probe key   (lower_bound - 1)
//   LtEq (probe <= build): smallest build key >= probe key  (lower_bound)
//   Lt   (probe <  build): smallest build key >  probe key  (upper_bound)
// Keys are sort_num_key-normalized (uint64 order == value order, IEEE-correct for
// floats), so one comparator covers timestamps, ints and doubles alike.

enum class AsofOp : uint8_t { GtEq = 0, Gt = 1, LtEq = 2, Lt = 3 };

struct AsofProbeOperator : Join2ProbeOperator {
    size_t asof_probe_idx;
    AsofOp op;

    AsofProbeOperator(std::vector<size_t> keys, std::vector<size_t> payload,
                      const Join2Ref* r, size_t asof_idx, int op_code)
        : Join2ProbeOperator(std::move(keys), std::move(payload), r, /*outer=*/true),
          asof_probe_idx(asof_idx), op(static_cast<AsofOp>(op_code)) {}

    // Sort every equi group's row list by asof key, once, before the first probe.
    // Runs under the ref's once_flag: exactly one thread mutates; the rest wait.
    // Materialize, once, a hash → build-rows-sorted-by-asof-key view. CarcharJoinIndex
    // stores each key's rows unsorted; the bisect below needs them ordered.
    void ensure_sorted() {
        auto* mref = const_cast<Join2Ref*>(ref);
        std::call_once(mref->asof_sorted, [&]() {
            auto* g = const_cast<Join2BuildGlobal*>(ref->g);
            auto items = g->index.items();
            g->asof_sorted.reserve(items.size());
            g->asof_index.reserve(items.size());
            for (const auto& kv : items) {
                std::vector<int64_t> rows = g->index.rows_from_payload(kv.second);
                std::sort(rows.begin(), rows.end(), [&](int64_t a, int64_t b) {
                    return g->asof_keys[a] < g->asof_keys[b];
                });
                g->asof_index.insert_new(kv.first,
                                         static_cast<int64_t>(g->asof_sorted.size()));
                g->asof_sorted.push_back(std::move(rows));
            }
        });
    }

    int64_t match_row(const std::vector<int64_t>& rows, uint64_t k,
                      const std::vector<uint64_t>& keys) const {
        const int64_t none = static_cast<int64_t>(kNoBuildRow);
        auto cmp = [&](int64_t r, uint64_t v) { return keys[r] < v; };
        auto cmp2 = [&](uint64_t v, int64_t r) { return v < keys[r]; };
        switch (op) {
            case AsofOp::GtEq: {   // largest build <= k
                auto it = std::upper_bound(rows.begin(), rows.end(), k, cmp2);
                return it == rows.begin() ? none : *(it - 1);
            }
            case AsofOp::Gt: {     // largest build < k
                auto it = std::lower_bound(rows.begin(), rows.end(), k, cmp);
                return it == rows.begin() ? none : *(it - 1);
            }
            case AsofOp::LtEq: {   // smallest build >= k
                auto it = std::lower_bound(rows.begin(), rows.end(), k, cmp);
                return it == rows.end() ? none : *it;
            }
            default: {             // Lt: smallest build > k
                auto it = std::upper_bound(rows.begin(), rows.end(), k, cmp2);
                return it == rows.end() ? none : *it;
            }
        }
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        ensure_sorted();
        auto& st = static_cast<Join2ProbeState&>(st_);
        const Join2BuildGlobal& g = *ref->g;
        if (st.pending_in != in) {
            st.pending_in = in;
            st.row = 0;
            if (!compute_row_hashes(in, probe_key_idx, st.rowh, err))
                return OpResult::NEED_INPUT;
        }
        uint32_t n = in->num_rows();
        std::vector<int64_t> build_rows, probe_rows;
        build_rows.reserve(kBatch);
        probe_rows.reserve(kBatch);
        const DrakenVector& av = in->columns[asof_probe_idx].view;

        while (st.row < n) {
            uint32_t row = st.row;
            int64_t build_row = static_cast<int64_t>(kNoBuildRow);
            bool usable = sort_row_valid(av, row);
            for (size_t k : probe_key_idx) {
                if (!usable) break;
                if (!sort_row_valid(in->columns[k].view, row)) usable = false;
            }
            if (usable) {
                int64_t idx = -1;
                if (g.asof_index.lookup_fast(st.rowh[row], idx))
                    build_row = match_row(g.asof_sorted[static_cast<size_t>(idx)],
                                          sort_num_key(av, row), g.asof_keys);
            }
            build_rows.push_back(build_row);
            probe_rows.push_back(static_cast<int64_t>(row));
            ++st.row;
            if (build_rows.size() >= kBatch) {
                out = build_output(in, build_rows, probe_rows, err);
                return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::HAVE_MORE;
            }
        }
        if (!build_rows.empty()) {
            out = build_output(in, build_rows, probe_rows, err);
            return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }
};

// ---- SEMI / ANTI probe: existence filter over the probe stream ---------------------

struct SemiAntiProbeState : OperatorState {};   // whole-morsel filter; no resume state

struct SemiAntiProbeOperator : Operator {
    std::vector<size_t> probe_key_idx;
    const Join2Ref* ref;
    bool anti;   // false = SEMI, true = null-aware ANTI (NOT IN semantics)

    SemiAntiProbeOperator(std::vector<size_t> keys, const Join2Ref* r, bool anti_)
        : probe_key_idx(std::move(keys)), ref(r), anti(anti_) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<SemiAntiProbeState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        (void)st_;
        const Join2BuildGlobal& g = *ref->g;
        uint32_t n = in->num_rows();
        if (n == 0) return OpResult::NEED_INPUT;

        if (anti && g.saw_null_key) return OpResult::NEED_INPUT;   // x NOT IN {…,NULL} → never true
        bool build_empty = (g.total_rows == 0 && !g.saw_null_key);

        std::vector<uint64_t> rowh;
        if (!build_empty && !compute_row_hashes(in, probe_key_idx, rowh, err))
            return OpResult::NEED_INPUT;

        std::vector<uint32_t> survivors;
        survivors.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            bool any_null = false;
            for (size_t k : probe_key_idx) {
                if (!sort_row_valid(in->columns[k].view, i)) { any_null = true; break; }
            }
            if (any_null) {
                // NULL key: never equi-matches (SEMI drops); NULL NOT IN <non-empty>
                // is NULL (ANTI drops) — but NOT IN an EMPTY set is TRUE.
                if (anti && build_empty) survivors.push_back(i);
                continue;
            }
            // Existence by 64-bit hash identity (const, thread-safe probe).
            bool found = !build_empty && g.index.row_count_for(rowh[i]) > 0;
            if (found != anti) survivors.push_back(i);
        }
        if (survivors.empty()) return OpResult::NEED_INPUT;
        std::vector<MorselPtr> ms{in};
        std::vector<uint32_t> row_m(n, 0), row_r(n);
        for (uint32_t i = 0; i < n; ++i) row_r[i] = i;
        out = gather_rows(ms, survivors, 0, survivors.size(), row_m, row_r, in->names, err);
        return (err.code != 0 || out == nullptr) ? OpResult::NEED_INPUT : OpResult::EMIT;
    }
};

// Deferred construction (build table exists only after the build pipeline runs).
struct DeferredJoin2Probe : Operator {
    std::vector<size_t> key_idx, payload_idx;
    const Join2Ref* ref;
    JoinMode mode;
    int asof_probe_idx = -1;   // >= 0: ASOF probe (asof column index + op below)
    int asof_op = 0;
    std::once_flag once;
    std::unique_ptr<Operator> inner;

    DeferredJoin2Probe(std::vector<size_t> keys, std::vector<size_t> payload,
                       const Join2Ref* r, JoinMode m,
                       int asof_idx = -1, int asof_op_code = 0)
        : key_idx(std::move(keys)), payload_idx(std::move(payload)), ref(r), mode(m),
          asof_probe_idx(asof_idx), asof_op(asof_op_code) {}

    std::unique_ptr<OperatorState> make_state() override {
        std::call_once(once, [this] {
            if (asof_probe_idx >= 0) {
                inner = std::make_unique<AsofProbeOperator>(
                    key_idx, payload_idx, ref,
                    static_cast<size_t>(asof_probe_idx), asof_op);
            } else if (mode == JoinMode::Semi || mode == JoinMode::Anti) {
                inner = std::make_unique<SemiAntiProbeOperator>(
                    key_idx, ref, mode == JoinMode::Anti);
            } else {
                inner = std::make_unique<Join2ProbeOperator>(
                    key_idx, payload_idx, ref, mode == JoinMode::LeftOuter);
            }
        });
        return inner->make_state();
    }
    OpResult execute(const MorselPtr& in, OperatorState& st, MorselPtr& out,
                     ErrCtx& err) override {
        return inner->execute(in, st, out, err);
    }
};

}  // namespace opteryx::engine
