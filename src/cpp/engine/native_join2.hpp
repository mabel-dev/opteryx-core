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
#include "native_hash_join.hpp"     // JoinPayloadColumn, join_elem_size
#include "native_sort.hpp"          // gather_rows, sort_row_valid, string helpers
#include "morsels/cxx_hash.h"       // cxx_hash_c — draken owns the join-key hash
#include "carchar_join_index.hpp"   // opteryx::carchar::CarcharJoinIndex

namespace opteryx::engine {

enum class JoinMode : uint8_t { Inner = 0, LeftOuter = 1, Semi = 2, Anti = 3 };

// compute_row_hashes (draken-owned key hash) is shared from native_hash_join.hpp.

struct Join2BuildLocal : LocalSinkState {
    std::vector<JoinPayloadColumn> payload;
    std::vector<uint64_t> row_hashes;  // parallel to payload rows: the 64-bit key hash
    std::vector<uint64_t> asof_keys;   // ASOF only: per build row, sort_num_key
    uint32_t next_row = 0;
    bool saw_null_key = false;
    bool types_learned = false;        // corrected payload types from the first real morsel
};
struct Join2BuildGlobal : GlobalSinkState {
    std::mutex mtx;
    // Global build table, keyed on the 64-bit draken hash → build-row-list. Built in
    // combine() under the mutex (single-threaded index construction, exactly as the
    // old unordered_map merge did); probed read-only/concurrently via const methods.
    opteryx::carchar::CarcharJoinIndex index;
    std::vector<JoinPayloadColumn> payload;
    std::vector<uint64_t> asof_keys;   // ASOF only: parallel to payload rows
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

    // Size+type one payload row-store from the plan-known types — called for BOTH
    // the global and every worker-local state, so every one of them carries the
    // full column count/type from the moment it exists, independent of whether any
    // row ever actually reaches it (the empty-build-side case this whole scheme
    // exists for).
    void init_payload(std::vector<JoinPayloadColumn>& payload) const {
        payload.resize(payload_col_idx.size());
        for (size_t c = 0; c < payload_col_idx.size(); ++c) {
            payload[c].type = payload_types[c];
            payload[c].elem_size = join_elem_size(payload_types[c]);
            payload[c].logical = payload_logical[c];
        }
    }

    std::unique_ptr<GlobalSinkState> make_global() override {
        auto g = std::make_unique<Join2BuildGlobal>();
        init_payload(g->payload);
        return g;
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        auto l = std::make_unique<Join2BuildLocal>();
        init_payload(l->payload);
        return l;
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<Join2BuildLocal&>(ls);
        uint32_t rows = in->num_rows();
        if (rows == 0) return SinkResult::CONTINUE;
        // The plan-known payload types (init_payload) exist for the empty-build case,
        // but the DATA is authoritative: learn each payload column's real type from the
        // first non-empty morsel. This corrects a plan that couldn't resolve a build
        // column's type and defaulted it (e.g. an aggregate output → VARCHAR), which
        // would otherwise materialize the column through the wrong type arm.
        if (!l.types_learned) {
            for (size_t c = 0; c < l.payload.size(); ++c) {
                DrakenType t = in->columns[payload_col_idx[c]].view.type;
                l.payload[c].type = t;
                l.payload[c].elem_size = join_elem_size(t);
            }
            l.types_learned = true;
        }
        for (size_t c = 0; c < l.payload.size(); ++c) {
            if (l.payload[c].elem_size == 0) {
                err.code = 1;
                err.msg = "Join2BuildSink: unsupported payload column type";
                return SinkResult::CONTINUE;
            }
        }
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
            for (size_t c = 0; c < payload_col_idx.size(); ++c) {
                l.payload[c].append_row(in->columns[payload_col_idx[c]].view, i, err,
                                        "Join2BuildSink: NULL payload value is not "
                                        "supported");
                if (err.code != 0) return SinkResult::CONTINUE;
            }
            if (asof_idx >= 0)
                l.asof_keys.push_back(
                    sort_num_key(in->columns[static_cast<size_t>(asof_idx)].view, i));
            l.row_hashes.push_back(rowh[i]);   // parallel to the payload row just added
            ++l.next_row;
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<Join2BuildGlobal&>(gs);
        auto& l = static_cast<Join2BuildLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        g.saw_null_key = g.saw_null_key || l.saw_null_key;
        // Adopt the data-learned payload types (a worker that saw rows is authoritative;
        // all workers see the same column types). The global keeps the plan types only
        // when NO worker ever saw a row (the empty-build case init_payload exists for).
        if (l.types_learned) {
            for (size_t c = 0; c < g.payload.size(); ++c) {
                g.payload[c].type = l.payload[c].type;
                g.payload[c].elem_size = l.payload[c].elem_size;
            }
        }
        // g.payload and l.payload are both pre-sized/typed from the same plan-known
        // payload_types (make_global/make_local) — always the same shape, even when
        // this worker's local never saw a row (its raw/arena vectors are then simply
        // empty, so the appends below are no-ops).
        uint32_t offset = g.total_rows;
        for (size_t c = 0; c < g.payload.size(); ++c) {
            JoinPayloadColumn& gcol = g.payload[c];
            JoinPayloadColumn& lcol = l.payload[c];
            if (join_type_is_string(gcol.type)) {
                uint32_t arena_base = static_cast<uint32_t>(gcol.arena.size());
                gcol.arena.insert(gcol.arena.end(), lcol.arena.begin(), lcol.arena.end());
                size_t local_rows = lcol.elem_size ? lcol.raw.size() / lcol.elem_size : 0;
                for (size_t r = 0; r < local_rows; ++r) {
                    const auto* slot = reinterpret_cast<const DrakenStringSlot*>(
                        lcol.raw.data() + r * lcol.elem_size);
                    DrakenStringSlot rebased;
                    if (str_is_inline(slot)) rebased = *slot;
                    else str_clone_with_offset(&rebased, slot,
                                               slot->ext.arena_offset + arena_base);
                    const uint8_t* rb = reinterpret_cast<const uint8_t*>(&rebased);
                    gcol.raw.insert(gcol.raw.end(), rb, rb + sizeof(DrakenStringSlot));
                }
            } else {
                gcol.raw.insert(gcol.raw.end(), lcol.raw.begin(), lcol.raw.end());
            }
            merge_payload_validity(gcol, lcol, lcol.row_count(), offset);
        }
        g.asof_keys.insert(g.asof_keys.end(), l.asof_keys.begin(), l.asof_keys.end());
        // Insert this worker's rows into the global index at the global row offset.
        // row_hashes[r] is the key hash of local payload row r → global row offset+r.
        g.index.reserve(g.total_rows + l.next_row);
        for (uint32_t r = 0; r < l.next_row; ++r)
            g.index.insert_row(l.row_hashes[r], static_cast<int64_t>(offset + r));
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
        auto out = std::make_shared<CxxMorsel>();
        out->zero_col_rows = n;
        out->columns.reserve(g.payload.size() + probe_payload_idx.size());
        size_t vbytes = (static_cast<size_t>(n) + 7) / 8;

        for (const JoinPayloadColumn& col : g.payload) {
            uint8_t* vbits = nullptr;
            auto mark_null = [&](uint32_t i) {
                if (vbits == nullptr) {
                    vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                    std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
                }
                vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
            };
            // kNoBuildRow: LEFT OUTER's unmatched probe row (no build row at all).
            // Otherwise NULL iff the build row's own payload value was NULL — the
            // build side's own validity bitmap (lazily allocated, see
            // JoinPayloadColumn::note_null), not the row-store row's absence.
            auto row_is_null = [&](uint32_t build_row) {
                return build_row == kNoBuildRow
                    || (!col.validity.empty()
                        && !((col.validity[build_row >> 3] >> (build_row & 7)) & 1u));
            };
            if (join_type_is_string(col.type)) {
                const auto* src_slots =
                    reinterpret_cast<const DrakenStringSlot*>(col.raw.data());
                const uint8_t* src_arena = col.arena.empty() ? nullptr : col.arena.data();
                size_t total_arena = 0;
                for (uint32_t i = 0; i < n; ++i) {
                    if (row_is_null(static_cast<uint32_t>(build_rows[i]))) continue;
                    const auto* slot = src_slots + static_cast<uint32_t>(build_rows[i]);
                    if (!str_is_inline(slot)) total_arena += str_length(slot);
                }
                size_t slots_off = sizeof(DrakenStringArena);
                size_t arena_off = slots_off
                    + static_cast<size_t>(n == 0 ? 1 : n) * sizeof(DrakenStringSlot);
                uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
                auto* sa = reinterpret_cast<DrakenStringArena*>(blk);
                auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
                uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
                sa->slots = dst; sa->arena = out_arena; sa->length = n;
                sa->arena_used = total_arena; sa->arena_cap = total_arena;
                sa->null_bitmap = nullptr; sa->owns_buffers = 0; sa->type = col.type;
                size_t arena_pos = 0;
                for (uint32_t i = 0; i < n; ++i) {
                    if (row_is_null(static_cast<uint32_t>(build_rows[i]))) {
                        std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                        mark_null(i);
                        continue;
                    }
                    const auto* slot = src_slots + static_cast<uint32_t>(build_rows[i]);
                    if (str_is_inline(slot)) dst[i] = *slot;
                    else {
                        uint32_t slen = str_length(slot);
                        std::memcpy(out_arena + arena_pos, str_data(slot, src_arena), slen);
                        str_clone_with_offset(&dst[i], slot,
                                              static_cast<uint32_t>(arena_pos));
                        arena_pos += slen;
                    }
                }
                uint32_t* sel = static_cast<uint32_t*>(
                    draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
                for (uint32_t i = 0; i < n; ++i) sel[i] = i;
                DrakenVector v;
                v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
                v.validity = vbits; v.type = col.type;
                v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
                CxxColumn c;
                c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                                      OwnedBuffer<uint8_t>(vbits),
                                                      OwnedBuffer<void>(sel));
                c.own->logical_type = col.logical;
                c.view = c.own->vec;
                out->columns.push_back(std::move(c));
                continue;
            }
            void* data;
            if (join_type_is_bool(col.type)) {
                // Re-pack the row-store's unpacked bytes into the canonical bit
                // layout. An unmatched row leaves its bit 0 and is marked NULL —
                // the validity mask, not the bit, carries "no build row".
                uint8_t* bits = join_alloc_bool_bits(n);
                for (uint32_t i = 0; i < n; ++i) {
                    if (row_is_null(static_cast<uint32_t>(build_rows[i]))) {
                        mark_null(i);
                        continue;
                    }
                    if (col.raw[static_cast<uint32_t>(build_rows[i])])
                        bits[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                }
                data = bits;
            } else {
                data = draken_malloc(
                    static_cast<size_t>(n == 0 ? 1 : n) * col.elem_size);
                uint8_t* dst = static_cast<uint8_t*>(data);
                for (uint32_t i = 0; i < n; ++i) {
                    if (row_is_null(static_cast<uint32_t>(build_rows[i]))) {
                        std::memset(dst + static_cast<size_t>(i) * col.elem_size, 0,
                                    col.elem_size);
                        mark_null(i);
                        continue;
                    }
                    std::memcpy(dst + static_cast<size_t>(i) * col.elem_size,
                                col.raw.data()
                                    + static_cast<size_t>(static_cast<uint32_t>(build_rows[i])) * col.elem_size,
                                col.elem_size);
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = data; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = col.type;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = col.logical;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
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
