#pragma once
// src/cpp/engine/native_cidr_unnest.hpp — CROSS JOIN CIDR_UNNEST operator.
//
// Expands each CIDR block in a VARCHAR column into one row per address it
// covers, repeating the parent row alongside. The inverse of CIDR_AGG.
//
// WHY THIS IS A RESUMABLE OPERATOR AND NOT AN ARRAY FUNCTION. The obvious
// cheaper design — a scalar CIDR_EXPAND(cidr) returning ARRAY<IPV4>, fed to the
// existing CROSS JOIN UNNEST — fails twice over, and both failures are silent
// until they are catastrophic:
//
//   1. the array must be MATERIALIZED. A /8 is 16,777,216 addresses, ~67MB in a
//      single cell, and that is per input ROW;
//   2. UnnestOperator emits "the whole expansion" as ONE morsel (its own header
//      says so, and for arrays that is safe because the array was already
//      materialised). A morsel of a thousand /8 rows would be 16.7 BILLION rows
//      in one batch.
//
// Expansion is explosive in a way array unnest never is: the input text is ~15
// bytes whatever the output size, so nothing about the input bounds the output.
// This operator therefore carries a CURSOR and returns HAVE_MORE, emitting
// bounded batches from one input morsel — the shape the executor already
// supports for join fan-out. Memory stays flat at one morsel no matter the
// prefix length, so unlike CIDR_AGG this needs no memory budget of its own; what
// it can produce is ROWS, which sql_select_limit and the result-size guard
// already govern.
//
// NO MINIMUM PREFIX. A /0 is 4.3 billion rows and is allowed: any floor would be
// an arbitrary number invented here, and a caller who means /0 is not making a
// mistake the engine can detect. The row governance above is the honest limit.
//
// ADDRESS MATH IS NOT REIMPLEMENTED. parse_cidr / netmask / broadcast come from
// draken/core/ipv4.h, which is by its own declaration the one place the
// uint32 <-> IPv4 mapping lives. parse_cidr is deliberately strict (it rejects
// inet_aton shorthand and octal-by-leading-zero, a documented source of ACL
// bugs), and this operator inherits that strictness rather than softening it.

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/ipv4.h"
#include "core/string_slot.h"
#include "core/vector_owner.h"
#include "logical_type.h"
#include "morsels/cxx_morsel.h"
#include "native_sort.hpp"        // string_arena_of, sort_row_valid
#include "operator.hpp"

extern "C" CxxMorsel* cxx_take_c(const CxxMorsel* m, const int32_t* idx, uint32_t n);
extern "C" void cxx_morsel_delete(CxxMorsel* m);

namespace opteryx::engine {

// Output rows per batch. Bounds the fan-out of one execute() call; the cursor
// carries the rest. Sized to a typical morsel rather than tuned — the operator
// is memory-flat, so this trades only per-call overhead, not footprint.
constexpr uint32_t kCidrUnnestBatchRows = 65536;

struct CidrUnnestState : OperatorState {
    uint32_t row    = 0;   // next parent row to read
    uint64_t offset = 0;   // addresses already emitted from THAT row's block
};

struct CidrUnnestOperator : Operator {
    uint32_t    cidr_idx;      // source VARCHAR column holding the blocks
    std::string target_name;   // identity of the emitted IPV4 column
    bool        drop_source;   // replace the source column rather than append

    CidrUnnestOperator(uint32_t idx, std::string name, bool drop)
        : cidr_idx(idx), target_name(std::move(name)), drop_source(drop) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<CidrUnnestState>();
    }

    OpResult execute(const MorselPtr& in, OperatorState& base_state,
                     MorselPtr& out, ErrCtx& err) override {
        CidrUnnestState& st = static_cast<CidrUnnestState&>(base_state);

        if (in->num_rows() == 0) { st.row = 0; st.offset = 0; return OpResult::NEED_INPUT; }
        if (cidr_idx >= in->columns.size()) {
            err.code = 1;
            err.msg = "CidrUnnestOperator: source column index out of range — the "
                      "compiler's layout tracking disagrees with the stream";
            return OpResult::NEED_INPUT;
        }
        const DrakenVector& v = in->columns[cidr_idx].view;
        if (!sort_type_is_string(v.type)) {
            err.code = 1;
            err.msg = "CidrUnnestOperator: CROSS JOIN CIDR_UNNEST source column is not "
                      "text — it takes a CIDR block such as '10.0.0.0/24'";
            return OpResult::NEED_INPUT;
        }
        const DrakenStringArena* arena = string_arena_of(v);
        const uint32_t rows = static_cast<uint32_t>(in->num_rows());

        std::vector<int32_t>  parent;    // parent row per output row
        std::vector<uint32_t> address;   // the expanded address
        parent.reserve(kCidrUnnestBatchRows);
        address.reserve(kCidrUnnestBatchRows);

        while (st.row < rows && parent.size() < kCidrUnnestBatchRows) {
            // A NULL block contributes ZERO rows — INNER semantics, matching
            // CROSS JOIN UNNEST over a NULL array.
            if (!sort_row_valid(v, st.row)) { ++st.row; st.offset = 0; continue; }

            const DrakenStringSlot* slot = &arena->slots[v.selection[st.row]];
            const uint8_t* text = reinterpret_cast<const uint8_t*>(str_data(slot, arena->arena));
            const uint32_t len = str_length(slot);

            uint32_t base = 0, prefix = 0;
            if (!draken::ipv4::parse_cidr(text, len, &base, &prefix)) {
                // Unparseable text is a data error, not an empty expansion.
                // Skipping it would silently return FEWER addresses than the
                // input describes, which no caller can detect; CAST(... AS IPV4)
                // raises on the same input and this agrees with it.
                err.code = 1;
                err.msg = "CidrUnnestOperator: CROSS JOIN CIDR_UNNEST source is not a "
                          "valid CIDR block. Expected strict dotted-decimal with a "
                          "prefix, e.g. '10.0.0.0/24' — shorthand forms and leading "
                          "zeros are rejected rather than guessed at.";
                return OpResult::NEED_INPUT;
            }

            // 64-bit throughout: a /0 spans 2^32 addresses and its last address
            // is 0xFFFFFFFF, so a 32-bit cursor would wrap and re-emit forever.
            const uint64_t total = static_cast<uint64_t>(
                draken::ipv4::broadcast(base, prefix)) - base + 1ULL;
            const uint64_t room = kCidrUnnestBatchRows - parent.size();
            const uint64_t take = (total - st.offset) < room ? (total - st.offset) : room;

            for (uint64_t k = 0; k < take; ++k) {
                parent.push_back(static_cast<int32_t>(st.row));
                address.push_back(static_cast<uint32_t>(base + st.offset + k));
            }
            st.offset += take;
            if (st.offset == total) { ++st.row; st.offset = 0; }
        }

        const bool exhausted = st.row >= rows;
        if (parent.empty()) {
            // Every remaining row was NULL. Nothing to emit; the batch is dropped
            // exactly as a fully-filtered one is.
            st.row = 0; st.offset = 0;
            return OpResult::NEED_INPUT;
        }

        const uint32_t n = static_cast<uint32_t>(parent.size());
        // draken owns the new/delete of the taken morsel (cxx_morsel_delete), so
        // it is freed in the module that allocated it — no cross-.so heap mixing.
        MorselPtr result(cxx_take_c(in.get(), parent.data(), n), cxx_morsel_delete);
        if (!result) {
            err.code = 1;
            err.msg = "CidrUnnestOperator: row replication failed";
            return OpResult::NEED_INPUT;
        }

        CxxColumn ip = _build_ipv4_column(address.data(), n);
        if (drop_source) {
            result->columns[cidr_idx] = std::move(ip);
            result->names[cidr_idx] = target_name;
        } else {
            result->columns.push_back(std::move(ip));
            result->names.push_back(target_name);
        }
        result->state = in->state;
        out = std::move(result);

        if (exhausted) { st.row = 0; st.offset = 0; return OpResult::EMIT; }
        return OpResult::HAVE_MORE;   // re-driven with the SAME input and state
    }

private:
    // A dense UINT32 vector carrying the IPV4 descriptor. The descriptor is what
    // makes it an address rather than a number — without it the column renders as
    // an integer and CIDR_AGG would refuse to take it back.
    static CxxColumn _build_ipv4_column(const uint32_t* values, uint32_t n) {
        const size_t bytes = static_cast<size_t>(n == 0 ? 1 : n) * sizeof(uint32_t);
        uint32_t* data = static_cast<uint32_t*>(draken_malloc(bytes));
        std::memcpy(data, values, static_cast<size_t>(n) * sizeof(uint32_t));
        uint32_t* sel = static_cast<uint32_t*>(draken_malloc(bytes));
        for (uint32_t i = 0; i < n; ++i) sel[i] = i;

        DrakenVector v;
        v.data = data; v.selection = sel; v.data_length = n; v.length = n;
        v.validity = nullptr;   // an expanded address is never NULL
        v.type = DRAKEN_UINT32;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;

        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                              OwnedBuffer<uint8_t>(nullptr),
                                              OwnedBuffer<void>(sel));
        LogicalType lt;
        lt.kind = LogicalKind::IPV4;
        c.own->logical_type = logical_type_intern(lt);
        c.view = c.own->vec;
        return c;
    }
};

}  // namespace opteryx::engine
