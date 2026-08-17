#pragma once
// src/cpp/engine/native_grouping_expand.hpp — GROUP BY ROLLUP / grouping sets.
//
// `GROUP BY ROLLUP(a, b, c)` is shorthand for the grouping-set list
// `(a,b,c), (a,b), (a), ()`: progressively coarser subtotals ending in a grand total.
// A row belongs to EVERY set, contributing to one group in each, and the keys a set
// does not name come back NULL on that set's rows.
//
// This operator is the whole of that semantics: it sits directly below the GROUP BY
// sink and, for each input morsel, emits ONE OUTPUT MORSEL PER GROUPING SET via the
// OpResult::HAVE_MORE protocol. The sink downstream is an ordinary grouped aggregate —
// it never learns that grouping sets exist. One pass over the input; the scan and the
// joins below run exactly once.
//
// Two things change per emitted morsel, and BOTH are O(1) per column, not per row:
//
//   * a key the set does not name is replaced by a CONSTANT-shaped view of the SAME
//     column — data borrowed (VectorOwner::data_source), selection pointing at the
//     shared global zero vector, validity at the shared global all-zero bitmap, so
//     every logical row reads NULL. Nothing is allocated and nothing is copied: the
//     type, the logical type and the payload all come from the original column, which
//     is what keeps a masked VARCHAR/DECIMAL/TIMESTAMP key correctly typed in the
//     output schema without this operator knowing anything about types.
//
//   * a `grouping_id` column is appended, constant over the morsel, holding the set's
//     ORDINAL — its index in the set list.
//
// grouping_id is NOT bookkeeping — it is a MANDATORY part of the group key. Without it
// a rolled-up NULL and a NULL that is genuinely in the data collapse into one group:
// for `ROLLUP(a, b)` over data containing `a = NULL`, set `(a)` and set `()` both
// produce the key row `(NULL, NULL)`, and the grand total would silently absorb the
// subtotal.
//
// The ordinal, and not the mask, is what identifies a set. Two DIFFERENT sets can carry
// the SAME mask — `ROLLUP(a, a)` denotes `(a,a), (a), ()`, whose first two sets group
// identically — and the standard gives each set its own rows. Keying on the mask merged
// them and lost three rows of a seven-row answer; the ordinal is distinct by
// construction, and carries no 64-key ceiling of its own.

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/alloc.h"          // draken_malloc
#include "core/buffers.h"        // DrakenVector
#include "core/vector_alloc.h"   // draken_zero_sel, draken_zero_validity, draken_vector_from_constant
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer
#include "morsels/cxx_morsel.h"  // CxxMorsel, CxxColumn
#include "operator.hpp"

namespace opteryx::engine {

// A constant-shaped, all-NULL view of `src`, `n` rows long.
//
// The payload is BORROWED, never copied: `data_source` holds the source column's owner
// alive, which is the sanctioned borrowing path (see vector_owner.h — `data_buf` and
// `arena_buf` stay null exactly because the bytes live in the source). `selection` and
// `validity` point at draken's process-wide shared globals, which are never freed and
// so are not owned here either. The net cost is one VectorOwner allocation per masked
// column per grouping set — no per-row work at all.
//
// Type and logical type ride along from the source, so a masked TIMESTAMP64 key keeps
// its MANDATORY descriptor (a timestamp vector with a null logical_type is a hard error
// in draken) and a masked DECIMAL keeps its precision/scale.
inline CxxColumn grouping_masked_column(const CxxColumn& src, uint32_t n) {
    DrakenVector v = src.view;
    v.selection   = draken_zero_sel(n);
    v.data_length = 1;
    v.length      = n;
    v.validity    = const_cast<uint8_t*>(draken_zero_validity(n));
    // No layout hints survive the reshape: this is neither the source's shape nor a
    // known-identity selection. 0 == "don't know", which is always safe.
    v.flags       = 0;

    auto owner = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(nullptr),
                                               OwnedBuffer<uint8_t>(nullptr));
    owner->logical_type = src.own ? src.own->logical_type : nullptr;
    // Null when the source column is itself unowned (a constant/zero-column morsel);
    // borrowing from nothing is the source's own lifetime story, not a new one.
    owner->data_source  = src.own;

    CxxColumn out;
    out.view = v;
    out.own  = std::move(owner);
    return out;
}

// The synthetic grouping_id key: one INT64 broadcast over the morsel, holding the
// grouping set's ordinal. Returns false (setting `err`) if the one-word allocation fails.
inline bool grouping_id_column(uint64_t ordinal, uint32_t n, CxxColumn& out, ErrCtx& err) {
    OwnedBuffer<void> buf(draken_malloc(sizeof(int64_t)));
    if (!buf) {
        err.code = 1;
        err.msg = "GroupingExpandOperator: out of memory allocating the grouping_id key";
        return false;
    }
    *static_cast<int64_t*>(buf.get()) = static_cast<int64_t>(ordinal);
    DrakenVector v = draken_vector_from_constant(buf.get(), n, DRAKEN_INT64, nullptr);
    auto owner = std::make_shared<VectorOwner>(v, std::move(buf), OwnedBuffer<uint8_t>(nullptr));
    out.view = owner->vec;
    out.own  = std::move(owner);
    return true;
}

struct GroupingExpandOperator : Operator {
    // Positions, in the input layout, of the GROUP BY keys — in the SAME order as the
    // bits of `set_masks`, so bit k refers to key_idx[k].
    std::vector<size_t>   key_idx;
    // One entry per grouping set. Bit k set == key k is ROLLED UP (NULL) in that set.
    std::vector<uint64_t> set_masks;
    std::string           grouping_id_name;

    GroupingExpandOperator(std::vector<size_t> keys, std::vector<uint64_t> masks,
                           std::string id_name)
        : key_idx(std::move(keys)), set_masks(std::move(masks)),
          grouping_id_name(std::move(id_name)) {}

    // Which set this worker emits next. Per-worker (LOCAL) state: the HAVE_MORE
    // protocol re-calls execute() with the SAME input on the SAME thread, so no
    // synchronisation is needed and two workers never share a cursor.
    struct State : OperatorState {
        size_t next_set = 0;
    };

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<State>();
    }

    OpResult execute(const MorselPtr& in, OperatorState& state, MorselPtr& out,
                     ErrCtx& err) override {
        // A zero-row morsel contributes to no group in any set — replicating it would
        // be N times nothing. Mirrors ColumnSelectOperator, which also drops these
        // rather than reason about a morsel narrower than the tracked layout.
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;

        State& st = static_cast<State&>(state);
        const uint32_t n = in->num_rows();
        const size_t   ordinal = st.next_set;
        const uint64_t mask = set_masks[ordinal];

        auto m = std::make_shared<CxxMorsel>();
        m->columns = in->columns;    // shared owners, zero copy
        m->names   = in->names;

        for (size_t k = 0; k < key_idx.size(); ++k) {
            if ((mask & (1ull << k)) == 0) continue;   // this key survives in this set
            const size_t idx = key_idx[k];
            if (idx >= in->columns.size()) {
                err.code = 1;
                err.msg = "GroupingExpandOperator: GROUP BY key index out of range — the "
                          "compiler's layout tracking disagrees with the stream";
                return OpResult::NEED_INPUT;
            }
            m->columns[idx] = grouping_masked_column(in->columns[idx], n);
        }

        CxxColumn gid;
        if (!grouping_id_column(ordinal, n, gid, err)) return OpResult::NEED_INPUT;
        m->columns.push_back(std::move(gid));
        m->names.push_back(grouping_id_name);

        m->zero_col_rows = n;
        m->state         = in->state;
        out = std::move(m);

        // Advance, and tell the driver whether this input still owes it morsels. The
        // cursor must reset on the LAST set, not on the first call, or the next input
        // morsel would resume mid-way through the set list.
        if (++st.next_set < set_masks.size()) return OpResult::HAVE_MORE;
        st.next_set = 0;
        return OpResult::EMIT;
    }
};

// GROUPING(col): whether the GROUP BY key at a fixed bit position (col's index into
// the SAME key list set_masks's bits are numbered against — see GroupingExpandOperator
// above) was rolled up (NULL) to produce THIS output row. Runs strictly AFTER the
// GROUP BY sink, over its emitted `$grouping_id` key column: one elementwise pass per
// GROUPING() call in the query — the same "one operator per computed expression,
// appended in order" shape ExprMultiProjectOperator's callers use. Produces INT64 0/1
// per the SQL standard.
//
// `$grouping_id` carries the grouping set's ORDINAL (its index in set_masks), not the
// mask itself — GroupingExpandOperator's own comment explains why: two DIFFERENT sets
// (`ROLLUP(a, a)`'s first two) can share one mask, and the ordinal is what keeps them
// apart. That means this operator cannot recover the bit by shifting grouping_id
// directly; `bit_by_ordinal[ordinal]` (precomputed at plan time, once, from
// `(set_masks[ordinal] >> bit) & 1`) is the per-row answer instead — a table lookup,
// not an arithmetic shift.
struct GroupingBitOperator : Operator {
    size_t                grouping_id_idx;   // input column index of the emitted $grouping_id key
    std::vector<uint8_t>  bit_by_ordinal;    // one 0/1 entry per grouping set, indexed by ordinal
    std::string           out_name;

    GroupingBitOperator(size_t idx, std::vector<uint8_t> table, std::string name)
        : grouping_id_idx(idx), bit_by_ordinal(std::move(table)), out_name(std::move(name)) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }

    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        const uint32_t n = in->num_rows();

        if (grouping_id_idx >= in->columns.size()) {
            err.code = 1;
            err.msg = "GroupingBitOperator: grouping_id column index out of range — the "
                      "compiler's layout tracking disagrees with the stream";
            return OpResult::NEED_INPUT;
        }
        const DrakenVector& gid = in->columns[grouping_id_idx].view;

        OwnedBuffer<void> buf(draken_malloc(sizeof(int64_t) * n));
        if (!buf) {
            err.code = 1;
            err.msg = "GroupingBitOperator: out of memory allocating the GROUPING() result";
            return OpResult::NEED_INPUT;
        }
        int64_t* out_data = static_cast<int64_t*>(buf.get());
        const int64_t* src = static_cast<const int64_t*>(gid.data);
        const uint32_t* sel = gid.selection;
        const size_t num_sets = bit_by_ordinal.size();
        for (uint32_t i = 0; i < n; ++i) {
            const int64_t ordinal = src[sel[i]];
            if (ordinal < 0 || static_cast<size_t>(ordinal) >= num_sets) {
                err.code = 1;
                err.msg = "GroupingBitOperator: grouping_id value out of range for this "
                          "query's grouping set list";
                return OpResult::NEED_INPUT;
            }
            out_data[i] = static_cast<int64_t>(bit_by_ordinal[static_cast<size_t>(ordinal)]);
        }

        DrakenVector v = draken_vector_from_dense(buf.get(), n, DRAKEN_INT64, nullptr);
        auto owner = std::make_shared<VectorOwner>(v, std::move(buf), OwnedBuffer<uint8_t>(nullptr));

        auto m = std::make_shared<CxxMorsel>();
        m->columns = in->columns;    // shared owners, zero copy
        m->names   = in->names;

        CxxColumn col;
        col.view = owner->vec;
        col.own  = std::move(owner);
        m->columns.push_back(std::move(col));
        m->names.push_back(out_name);

        m->zero_col_rows = n;
        m->state         = in->state;
        out = std::move(m);
        return OpResult::EMIT;
    }
};

}  // namespace opteryx::engine
