#pragma once
// src/cpp/engine/native_hash_join.hpp — genuinely native (zero-Python) INNER
// EQUI-JOIN: a build Sink materializes the build-side relation (a compact,
// owned row-store of the payload columns downstream needs, plus a hash index
// key -> build row indices); a probe Operator looks up each probe row's key,
// fans out matches via the existing HAVE_MORE re-drive pattern
// (scan_join_demo.hpp), materializing one output row per (build_row,
// probe_row) match — build-side payload columns first, then probe-side
// payload columns, in that fixed order.
//
// This generalizes scan_join_demo.hpp's HashBuildSink/JoinOperator (which only
// ever computed a match COUNT, key -> int64 tally, no row list, no payload)
// into a real join that can feed an aggregate needing columns from BOTH
// sides — e.g. TPC-H Q14's `SUM(CASE WHEN p_type LIKE 'PROMO%' THEN
// l_extendedprice*(1-l_discount) ELSE 0 END)`.
//
// Scope (first landing, stated honestly, not silently extended):
//   - INNER equi-join only, single-column key. The key column must be
//     INT64-backed (DRAKEN_INT64 or DRAKEN_DECIMAL, read as its raw unscaled
//     value — exact for EQUALITY even though it would not be a valid
//     magnitude comparison; equi-join only ever tests equality). A NULL join
//     key never matches (SQL semantics), on either side.
//   - Payload columns (materialized from the build side into the row-store,
//     or read live from the probe side) may be any fixed-width type this
//     engine already carries elsewhere: INT8/16/32/64, FLOAT32/64, DECIMAL,
//     or VARCHAR — INCLUDING long (arena-backed) values, e.g. TPC-H's
//     `p_type` (18-23 bytes in real data). The row-store consolidates every
//     referenced long string into its own growing arena (JoinPayloadColumn::
//     arena), rebasing each copied slot's offset (str_clone_with_offset,
//     draken/core/string_slot.h) — first when appending a row, again when
//     merging one worker's local row-store into the shared global one
//     (HashJoinBuildSink::combine), and again when gathering one output batch
//     (JoinProbeOperator::build_output) — because each of those is a
//     DIFFERENT arena, and a slot's stored offset is only meaningful relative
//     to whichever arena it was built against.
//   - NULL payload values are NOT supported — fails loud (ErrCtx), never
//     silently coalesced to some sentinel. TPC-H's base fact/dimension
//     columns used here are NOT NULL in practice; this is a real, stated
//     scope boundary, not a hypothetical one.
//   - Build/probe side assignment is NOT decided here — the caller passes
//     whichever relation the (existing, already-shipped) join-ordering
//     optimizer put on the build side; this file has no size/statistics
//     logic of its own, matching the "the planner already decided this"
//     principle used throughout this engine (e.g. decimal scale, join left/
//     right assignment upstream in opteryx/planner/optimizer/strategies/
//     join_ordering.py).
//   - No LEFT/RIGHT/FULL OUTER, no semi/anti, no multi-column key, no
//     non-equi predicates. Each is a real, separate follow-up.

#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "executor.hpp"
#include "native_sort.hpp"     // gather_rows (probe payload), canonical string layout
#include "core/string_slot.h"  // DrakenStringSlot — string payload columns
#include "core/alloc.h"        // draken_malloc / draken_free
#include "core/vector_owner.h" // VectorOwner, OwnedBuffer

namespace opteryx::engine {

// Byte width of one value of `t` in this engine's supported payload type set.
// DrakenStringSlot is a 16-byte POD (draken/core/string_slot.h) — treating it
// as "just another fixed-width element" lets the row-store be one generic
// byte-vector implementation instead of one per type.
inline size_t join_elem_size(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8:                          return 1;
        case DRAKEN_INT16:                          return 2;
        case DRAKEN_INT32: case DRAKEN_FLOAT32:      return 4;
        case DRAKEN_INT64: case DRAKEN_FLOAT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64: return t == DRAKEN_DATE32 ? 4 : 8;
        case DRAKEN_DECIMAL128:                      return 16;
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY:                       return sizeof(DrakenStringSlot);
        default:                                      return 0;  // unsupported
    }
}

inline bool join_key_type_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64: case DRAKEN_DECIMAL:
            return true;
        default:
            return false;
    }
}

// Uniform data[selection[i]] read of a join KEY, widened to int64 exactly (every
// supported key type is integer-backed; DECIMAL joins compare the raw unscaled
// value — correct because the binder casts both legs to one type/scale before
// the join). Returns false (NULL key) without touching `out`.
inline bool join_read_key(const DrakenVector& v, uint32_t row, int64_t& out) {
    if (v.validity != nullptr && !((v.validity[row >> 3] >> (row & 7)) & 1u)) return false;
    uint32_t phys = v.selection[row];
    switch (v.type) {
        case DRAKEN_INT8:   out = static_cast<const int8_t*>(v.data)[phys]; break;
        case DRAKEN_INT16:  out = static_cast<const int16_t*>(v.data)[phys]; break;
        case DRAKEN_INT32:
        case DRAKEN_DATE32: out = static_cast<const int32_t*>(v.data)[phys]; break;
        default:            out = static_cast<const int64_t*>(v.data)[phys]; break;
    }
    return true;
}

inline bool join_type_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// One materialized payload column: `raw` holds `elem_size` bytes per row,
// densely packed in row-store order (NOT the original morsel's own row
// order — rows are appended as build morsels stream in).
struct JoinPayloadColumn {
    DrakenType type = DRAKEN_INT64;
    size_t elem_size = 0;
    const LogicalType* logical = nullptr;  // borrowed; carried to output columns
    std::vector<uint8_t> raw;    // elem_size bytes/row (slots for strings)
    std::vector<uint8_t> arena;  // strings only: consolidated long-string bytes

    void append_row(const DrakenVector& v, uint32_t row, ErrCtx& err, const char* who) {
        if (v.validity != nullptr && !((v.validity[row >> 3] >> (row & 7)) & 1u)) {
            err.code = 1;
            err.msg = who;  // "<Sink>: NULL payload value is not supported"
            return;
        }
        uint32_t phys = v.selection[row];
        if (join_type_is_string(type)) {
            // CANONICAL layout (buffers.h): a string vector's `data` points at a
            // DrakenStringArena STRUCT — slots and arena resolve through it.
            const auto* sa = static_cast<const DrakenStringArena*>(v.data);
            const DrakenStringSlot* slot = &sa->slots[phys];
            DrakenStringSlot rebased;
            if (str_is_inline(slot)) {
                rebased = *slot;
            } else {
                uint32_t slen = str_length(slot);
                size_t arena_pos = arena.size();
                arena.resize(arena_pos + slen);
                std::memcpy(arena.data() + arena_pos, str_data(slot, sa->arena), slen);
                str_clone_with_offset(&rebased, slot, static_cast<uint32_t>(arena_pos));
            }
            const uint8_t* rb = reinterpret_cast<const uint8_t*>(&rebased);
            raw.insert(raw.end(), rb, rb + sizeof(DrakenStringSlot));
            return;
        }
        const uint8_t* src = static_cast<const uint8_t*>(v.data) + static_cast<size_t>(phys) * elem_size;
        raw.insert(raw.end(), src, src + elem_size);
    }
};

// ---- BUILD SIDE ------------------------------------------------------------

struct HashJoinBuildLocal : LocalSinkState {
    std::unordered_map<int64_t, std::vector<uint32_t>> key_to_rows;  // key -> LOCAL row indices
    std::vector<JoinPayloadColumn> payload;  // parallel to build_payload_col_idx
    uint32_t next_row = 0;
    bool initialized = false;
};
struct HashJoinBuildGlobal : GlobalSinkState {
    std::mutex mtx;
    std::unordered_map<int64_t, std::vector<uint32_t>> key_to_rows;  // key -> GLOBAL row indices
    std::vector<JoinPayloadColumn> payload;
    uint32_t total_rows = 0;
    bool initialized = false;
};

struct HashJoinBuildSink : Sink {
    size_t key_col_idx;
    std::vector<size_t> payload_col_idx;

    HashJoinBuildSink(size_t key_idx, std::vector<size_t> payload_idx)
        : key_col_idx(key_idx), payload_col_idx(std::move(payload_idx)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<HashJoinBuildGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<HashJoinBuildLocal>();
    }

    void init_payload_types(std::vector<JoinPayloadColumn>& payload, const MorselPtr& in, ErrCtx& err) {
        payload.resize(payload_col_idx.size());
        for (size_t c = 0; c < payload_col_idx.size(); ++c) {
            DrakenType t = in->columns[payload_col_idx[c]].view.type;
            size_t es = join_elem_size(t);
            if (es == 0) {
                err.code = 1;
                err.msg = "HashJoinBuildSink: unsupported payload column type";
                return;
            }
            payload[c].type = t;
            payload[c].elem_size = es;
            payload[c].logical = in->columns[payload_col_idx[c]].own
                ? in->columns[payload_col_idx[c]].own->logical_type : nullptr;
        }
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls, ErrCtx& err) override {
        auto& l = static_cast<HashJoinBuildLocal&>(ls);
        const DrakenVector& kv = in->columns[key_col_idx].view;
        if (!join_key_type_supported(kv.type)) {
            err.code = 1;
            err.msg = "HashJoinBuildSink: unsupported join-key column type "
                      "(integer-backed keys only)";
            return SinkResult::CONTINUE;
        }
        if (!l.initialized) {
            init_payload_types(l.payload, in, err);
            if (err.code != 0) return SinkResult::CONTINUE;
            l.initialized = true;
        }
        uint32_t n = kv.length;
        for (uint32_t i = 0; i < n; ++i) {
            int64_t key;
            if (!join_read_key(kv, i, key)) continue;  // NULL key never matches
            for (size_t c = 0; c < payload_col_idx.size(); ++c) {
                const CxxColumn& pcol = in->columns[payload_col_idx[c]];
                l.payload[c].append_row(pcol.view, i, err,
                                        "HashJoinBuildSink: NULL payload value is not supported");
                if (err.code != 0) return SinkResult::CONTINUE;
            }
            l.key_to_rows[key].push_back(l.next_row);
            ++l.next_row;
        }
        return SinkResult::CONTINUE;
    }

    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<HashJoinBuildGlobal&>(gs);
        auto& l = static_cast<HashJoinBuildLocal&>(ls);
        if (!l.initialized) return;  // this worker never saw a morsel
        std::lock_guard<std::mutex> lk(g.mtx);
        if (!g.initialized) {
            g.payload.resize(l.payload.size());
            for (size_t c = 0; c < l.payload.size(); ++c) {
                g.payload[c].type = l.payload[c].type;
                g.payload[c].elem_size = l.payload[c].elem_size;
            }
            g.initialized = true;
        }
        uint32_t offset = g.total_rows;
        for (size_t c = 0; c < g.payload.size(); ++c) {
            JoinPayloadColumn& gcol = g.payload[c];
            JoinPayloadColumn& lcol = l.payload[c];
            if (join_type_is_string(gcol.type)) {
                // Each worker's local arena starts at offset 0 — appending
                // local rows into the global row-store means every non-inline
                // slot's offset must shift by however much is ALREADY in the
                // global arena before this worker's bytes are appended.
                uint32_t arena_base = static_cast<uint32_t>(gcol.arena.size());
                gcol.arena.insert(gcol.arena.end(), lcol.arena.begin(), lcol.arena.end());
                size_t local_rows = lcol.raw.size() / lcol.elem_size;
                for (size_t r = 0; r < local_rows; ++r) {
                    const auto* slot = reinterpret_cast<const DrakenStringSlot*>(
                        lcol.raw.data() + r * lcol.elem_size);
                    DrakenStringSlot rebased;
                    if (str_is_inline(slot)) {
                        rebased = *slot;
                    } else {
                        str_clone_with_offset(&rebased, slot, slot->ext.arena_offset + arena_base);
                    }
                    const uint8_t* rb = reinterpret_cast<const uint8_t*>(&rebased);
                    gcol.raw.insert(gcol.raw.end(), rb, rb + sizeof(DrakenStringSlot));
                }
            } else {
                gcol.raw.insert(gcol.raw.end(), lcol.raw.begin(), lcol.raw.end());
            }
        }
        for (auto& [key, rows] : l.key_to_rows) {
            auto& dst = g.key_to_rows[key];
            for (uint32_t r : rows) dst.push_back(r + offset);
        }
        g.total_rows += l.next_row;
        (void)err;
    }

    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

// ---- PROBE SIDE -------------------------------------------------------------

struct JoinProbeOpState : OperatorState {
    MorselPtr pending_in;
    uint32_t row = 0;
    const std::vector<uint32_t>* current_matches = nullptr;
    size_t match_idx = 0;
};

struct JoinProbeOperator : Operator {
    size_t probe_key_col_idx;
    const std::unordered_map<int64_t, std::vector<uint32_t>>* key_to_rows;
    const std::vector<JoinPayloadColumn>* build_payload;   // materialized build row-store
    std::vector<size_t> probe_payload_col_idx;             // probe-side columns to carry forward
    static constexpr size_t kBatch = 64;

    JoinProbeOperator(size_t probe_key_idx,
                      const std::unordered_map<int64_t, std::vector<uint32_t>>* table,
                      const std::vector<JoinPayloadColumn>* build_payload_,
                      std::vector<size_t> probe_payload_idx)
        : probe_key_col_idx(probe_key_idx), key_to_rows(table), build_payload(build_payload_),
          probe_payload_col_idx(std::move(probe_payload_idx)) {}

    std::unique_ptr<OperatorState> make_state() override { return std::make_unique<JoinProbeOpState>(); }

    static const std::vector<uint32_t>* find_matches(
            const std::unordered_map<int64_t, std::vector<uint32_t>>& table, int64_t key) {
        auto it = table.find(key);
        return it == table.end() ? nullptr : &it->second;
    }

    MorselPtr build_output(const MorselPtr& probe_in,
                           const std::vector<std::pair<uint32_t, uint32_t>>& matches,  // (build_row, probe_row)
                           ErrCtx& err) {
        uint32_t n = static_cast<uint32_t>(matches.size());
        auto out = std::make_shared<CxxMorsel>();
        out->zero_col_rows = n;  // CxxMorsel::num_rows() falls back to this when columns is empty
                                 // (e.g. a bare COUNT(*) over the join needs no payload columns at all)
        out->columns.reserve(build_payload->size() + probe_payload_col_idx.size());

        // ---- build-side columns: gathered from the row-store (dense, no NULLs —
        //      append_row rejects them at build time). String output is written in
        //      the CANONICAL layout: one consolidated [DrakenStringArena | slots |
        //      arena] block with `data` at the header (buffers.h contract).
        for (const JoinPayloadColumn& col : *build_payload) {
            if (join_type_is_string(col.type)) {
                const auto* src_slots = reinterpret_cast<const DrakenStringSlot*>(col.raw.data());
                const uint8_t* src_arena = col.arena.empty() ? nullptr : col.arena.data();
                size_t total_arena = 0;
                for (uint32_t i = 0; i < n; ++i) {
                    const auto* slot = src_slots + matches[i].first;
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
                    const auto* slot = src_slots + matches[i].first;
                    if (str_is_inline(slot)) {
                        dst[i] = *slot;
                    } else {
                        uint32_t slen = str_length(slot);
                        std::memcpy(out_arena + arena_pos, str_data(slot, src_arena), slen);
                        str_clone_with_offset(&dst[i], slot, static_cast<uint32_t>(arena_pos));
                        arena_pos += slen;
                    }
                }
                uint32_t* sel = static_cast<uint32_t*>(
                    draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
                for (uint32_t i = 0; i < n; ++i) sel[i] = i;
                DrakenVector v;
                v.data = sa; v.selection = sel; v.data_length = n; v.length = n;
                v.validity = nullptr; v.type = col.type;
                v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
                CxxColumn c;
                c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                                      OwnedBuffer<uint8_t>(nullptr),
                                                      OwnedBuffer<void>(sel));
                c.own->logical_type = col.logical;
                c.view = c.own->vec;
                out->columns.push_back(std::move(c));
                continue;
            }
            void* data = draken_malloc(static_cast<size_t>(n == 0 ? 1 : n) * col.elem_size);
            uint8_t* dst = static_cast<uint8_t*>(data);
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t br = matches[i].first;
                std::memcpy(dst + static_cast<size_t>(i) * col.elem_size,
                           col.raw.data() + static_cast<size_t>(br) * col.elem_size, col.elem_size);
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = data; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = nullptr; v.type = col.type;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                  OwnedBuffer<uint8_t>(nullptr),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = col.logical;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
        }

        // ---- probe-side columns: the engine's ONE row gather (native_sort.hpp) —
        //      canonical string read+write, validity preserved (a NULL probe payload
        //      is legal output, not an error), logical descriptors carried through.
        if (!probe_payload_col_idx.empty()) {
            uint32_t pn = probe_in->num_rows();
            auto view = std::make_shared<CxxMorsel>();
            view->columns.reserve(probe_payload_col_idx.size());
            for (size_t pc : probe_payload_col_idx) view->columns.push_back(probe_in->columns[pc]);
            view->names.resize(view->columns.size());   // select-by-index downstream
            view->zero_col_rows = pn;
            std::vector<uint32_t> order(n);
            for (uint32_t i = 0; i < n; ++i) order[i] = matches[i].second;
            std::vector<uint32_t> row_m(pn, 0), row_r(pn);
            for (uint32_t i = 0; i < pn; ++i) row_r[i] = i;
            std::vector<MorselPtr> ms{view};
            MorselPtr gathered = gather_rows(ms, order, 0, n, row_m, row_r, view->names, err);
            if (err.code != 0 || gathered == nullptr) return nullptr;
            for (CxxColumn& c : gathered->columns) out->columns.push_back(std::move(c));
        }
        return out;
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out, ErrCtx& err) override {
        auto& st = static_cast<JoinProbeOpState&>(st_);
        if (st.pending_in != in) {
            st.pending_in = in;
            st.row = 0;
            st.current_matches = nullptr;
            st.match_idx = 0;
        }

        const DrakenVector& kv = in->columns[probe_key_col_idx].view;
        if (!join_key_type_supported(kv.type)) {
            err.code = 1;
            err.msg = "JoinProbeOperator: unsupported probe join-key column type";
            return OpResult::NEED_INPUT;
        }
        uint32_t n = kv.length;
        std::vector<std::pair<uint32_t, uint32_t>> matches;
        matches.reserve(kBatch);

        while (st.row < n) {
            if (st.current_matches == nullptr) {
                int64_t key;
                if (!join_read_key(kv, st.row, key)) { ++st.row; continue; }  // NULL probe key
                st.current_matches = find_matches(*key_to_rows, key);
                st.match_idx = 0;
                if (st.current_matches == nullptr) { ++st.row; continue; }  // no build-side match
            }
            matches.emplace_back((*st.current_matches)[st.match_idx], st.row);
            ++st.match_idx;
            if (st.match_idx >= st.current_matches->size()) {
                st.current_matches = nullptr;
                ++st.row;
            }
            if (matches.size() >= kBatch) {
                out = build_output(in, matches, err);
                return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::HAVE_MORE;
            }
        }
        if (!matches.empty()) {
            out = build_output(in, matches, err);
            return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
        }
        return OpResult::NEED_INPUT;
    }
};

}  // namespace opteryx::engine
