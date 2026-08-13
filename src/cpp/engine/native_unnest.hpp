#pragma once
// src/cpp/engine/native_unnest.hpp — CROSS JOIN UNNEST operator.
//
// Single input, fan-out shape: expand ARRAY column `array_idx`, repeating every
// parent row by its element count and appending the flattened element under
// `target_name`.
//
// Row-count semantics (NULL/empty arrays, INNER vs OUTER) are DRAKEN's rule,
// stated in full above cxx_unnest in draken/draken_native.cpp. Not restated
// here — a second statement is a second thing to drift. The consequence this
// operator must handle: a batch can expand to zero rows, and such a morsel is
// dropped like any fully-filtered one.
//
// The array-aware work (replicate all parent columns incl. the source ARRAY, then
// flatten the child subtree by a raw index array) lives in draken's cxx_unnest_c,
// next to cxx_take_c, where the take/child machinery is. This operator is the thin
// engine-side driver that calls it and wraps the result on the carrier — mirroring
// how the Cython edge already calls cxx_take_c. Symbols resolve at load time from
// draken_native, the same dynamic-lookup path gather_rows' draken helpers use.

#include <memory>
#include <string>
#include <vector>

#include "morsels/cxx_morsel.h"   // CxxMorsel, DRAKEN_ARRAY (via core/buffers.h)
#include "carchar_set.hpp"        // CarcharSet — the SAME set DistinctSink dedups with
#include "morsels/cxx_hash.h"     // cxx_hash_c — draken owns the key hash (see below)
#include "native_expression.hpp"  // ExprProgram, ExprEvalFn, format_kernel_error
#include "native_queue_sink.hpp"  // vec_row_is_valid (reused, not duplicated)
#include "operator.hpp"

extern "C" CxxMorsel* cxx_unnest_c(const CxxMorsel* m, uint32_t array_idx,
                                   const char* target_name, uint32_t target_name_len,
                                   int drop_source, const uint8_t* child_mask);
extern "C" CxxMorsel* cxx_unnest_literal_c(const CxxMorsel* m, const CxxMorsel* vals,
                                           const char* target_name, uint32_t target_name_len);
extern "C" void cxx_morsel_delete(CxxMorsel* m);

namespace opteryx::engine {

// Expands one ARRAY column. A single input morsel yields at most one output morsel
// (the whole expansion), so no HAVE_MORE cursor is needed — the batch is already
// bounded by the source's morsel size.
struct UnnestOperator : Operator {
    uint32_t    array_idx;
    std::string target_name;   // identity (opaque bytes) of the flattened column
    // True when nothing above the unnest reads the raw source array: the array is
    // replaced in place by the target. Set by the compiler from projection-pushdown
    // liveness. False keeps the array (SELECT * needs it) — but a surviving ARRAY
    // column then fails loud in a downstream gather_rows join/sort.
    bool        drop_source;

    // PUSHED WHERE on the unnested column. `filter_fn == nullptr` means no pushed
    // filter, which is every plan that does not have one — the cost of the feature
    // on those plans is one null check per morsel.
    //
    // The program is bool-final and resolved against a ONE-COLUMN layout holding the
    // unnest TARGET, because it is evaluated over the array's CHILD (element) vector
    // before expansion, not over the expanded stream. The compiler only folds a
    // predicate that references the target and nothing else — one referencing a
    // parent column cannot be answered before the parent rows are replicated, and is
    // left above the unnest.
    //
    // It is the SAME bytecode the standalone ExprFilter would have run on the
    // expanded column, so folding cannot change an answer: the predicate is
    // element-wise, and evaluating it on the elements or on the copies of the
    // elements gives the same verdict per element.
    ExprProgram filter_prog;
    ExprEvalFn  filter_fn = nullptr;

    // PUSHED DISTINCT on the unnested column: skip an element whose value has already
    // been emitted by THIS worker, so the duplicate is never materialized.
    //
    // This is a PRE-REDUCTION, not a replacement for the DISTINCT — the compiler
    // leaves the DistinctSink in place and must never remove it. The state is
    // per-worker (executor.hpp builds one OperatorState per worker), so N workers
    // each emit their own first-sighting of a value and only the sink can dedup
    // ACROSS them. Dropping a duplicate before a DISTINCT can never change that
    // DISTINCT's answer; dropping one before anything else can, which is why the
    // compiler only sets this when the target is the ONLY column leaving the unnest.
    //
    // Equality is 64-bit hash identity via draken's cxx_hash_c — deliberately the
    // SAME hash and the SAME CarcharSet the DistinctSink uses (native_group_sinks.hpp:
    // "the CarcharSet stores no key bytes"). That identity is what makes the
    // pre-reduction safe rather than merely likely-safe: two values that collide here
    // would collide in the sink too, so this can only ever drop a row the sink was
    // going to collapse anyway. A DIFFERENT hash would be a new way to lose a
    // genuinely distinct value, and would be a wrong answer, not an optimization.
    bool distinct_target = false;

    UnnestOperator(uint32_t idx, std::string name, bool drop)
        : array_idx(idx), target_name(std::move(name)), drop_source(drop) {}

    UnnestOperator(uint32_t idx, std::string name, bool drop,
                   ExprProgram prog, ExprEvalFn fn, bool distinct)
        : array_idx(idx), target_name(std::move(name)), drop_source(drop),
          filter_prog(std::move(prog)), filter_fn(fn), distinct_target(distinct) {}

    // Per-worker dedup set for the pushed DISTINCT. Empty and untouched when
    // `distinct_target` is false.
    struct UnnestState : OperatorState {
        opteryx::carchar::CarcharSet seen;
        std::vector<uint64_t> hashes;   // per-morsel scratch: child element hashes
    };

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<UnnestState>();
    }

    // Fold the pushed DISTINCT into `mask`, which already carries the pushed WHERE's
    // verdict (or is all-keep when there is no filter). Runs SECOND on purpose: an
    // element the filter rejected must not be entered into the dedup set, or the
    // first REJECTED occurrence of a value would suppress a later ACCEPTED one.
    bool apply_distinct(const CxxColumn& arrcol, std::vector<uint8_t>& mask,
                        UnnestState& state, ErrCtx& err) {
        if (!arrcol.own || !arrcol.own->child_owner) return true;

        CxxColumn child_col;
        child_col.own = std::shared_ptr<VectorOwner>(arrcol.own,
                                                     arrcol.own->child_owner.get());
        child_col.view = child_col.own->vec;
        const uint32_t n = child_col.view.length;
        if (n == 0u) return true;

        CxxMorsel child_morsel;
        child_morsel.columns.push_back(std::move(child_col));
        child_morsel.names.push_back(target_name);

        // draken owns the hash. One key column, index 0 — the child vector itself.
        int32_t key_col = 0;
        CxxMorsel* hashm = cxx_hash_c(&child_morsel, &key_col, 1u);
        if (hashm == nullptr) {
            err.code = 1;
            err.msg = "UnnestOperator: pushed DISTINCT could not hash the element "
                      "column — fail loud rather than emit duplicates as distinct";
            return false;
        }
        std::shared_ptr<CxxMorsel> hash_guard(hashm, cxx_morsel_delete);
        const DrakenVector& hv = hash_guard->columns[0].view;
        const uint64_t* hdata = static_cast<const uint64_t*>(hv.data);

        // `mask` is sized to the child length when a filter ran; size it here when
        // DISTINCT is the only fold.
        if (mask.empty()) mask.assign(n, 1u);

        for (uint32_t i = 0u; i < n; ++i) {
            if (mask[i] == 0u) continue;                  // already rejected by WHERE
            const uint64_t h = hdata[hv.selection[i]];
            // insert_or_ignore returns true when the key was NEW to this worker.
            if (!state.seen.insert_or_ignore(h)) mask[i] = 0u;
        }
        return true;
    }

    // Evaluate the pushed predicate over the ARRAY's child vector, producing one
    // byte per LOGICAL child position (1 = keep). Returns false and sets `err` on a
    // kernel failure — never a silent "keep everything", which would leak filtered
    // rows into the answer.
    bool build_child_mask(const CxxColumn& arrcol, std::vector<uint8_t>& mask,
                          ErrCtx& err) {

        // No child owner means no elements anywhere; the expansion is empty and the
        // caller drops the morsel before a mask could matter.
        if (!arrcol.own || !arrcol.own->child_owner) return true;

        // Wrap the EXISTING child vector as a one-column morsel — zero-copy. The
        // aliasing shared_ptr shares ownership with the parent column's owner (so
        // the parent cannot be freed while the child view is live) while pointing at
        // the child, which is held by a unique_ptr and must not be double-owned.
        CxxColumn child_col;
        child_col.own = std::shared_ptr<VectorOwner>(arrcol.own,
                                                     arrcol.own->child_owner.get());
        child_col.view = child_col.own->vec;

        CxxMorsel child_morsel;
        child_morsel.columns.push_back(std::move(child_col));
        child_morsel.names.push_back(target_name);

        DrakenVector v;
        void* data = nullptr;
        uint8_t* validity = nullptr;
        void* sel = nullptr;
        int err_op = 0;
        const char* kernel_msg = nullptr;
        VecResult* child = nullptr;
        int rc = filter_fn(filter_prog.instrs, filter_prog.count, &child_morsel,
                           filter_prog.col_idx.data(), filter_prog.lit_dv.data(),
                           &v, &data, &validity, &sel, &err_op, &kernel_msg, &child);
        if (rc != 0) {
            err.code = 1;
            err.msg = format_kernel_error(
                "UnnestOperator: pushed predicate evaluation failed", err_op, kernel_msg);
            return false;
        }
        // Own the span's buffers so they are released on every path out of here.
        VectorOwner owner(v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(validity),
                          OwnedBuffer<void>(sel));
        if (child != nullptr) { delete child; }   // a BOOL result has no ARRAY child
        if (v.type != DRAKEN_BOOL) {
            err.code = 1;
            err.msg = "UnnestOperator: pushed predicate did not evaluate to BOOL — "
                      "fail loud rather than guess at which elements survive";
            return false;
        }

        // BOOL data is BIT-PACKED and read through the uniform selection path
        // (data[selection[i]]); validity is 1 bit per LOGICAL row (CLAUDE.md §11).
        const uint8_t* bits = static_cast<const uint8_t*>(v.data);
        const uint32_t* codes = v.selection;   // never NULL (draken invariant)
        mask.assign(v.length, 0u);
        for (uint32_t i = 0u; i < v.length; ++i) {
            // A NULL predicate result is UNKNOWN, and UNKNOWN is not TRUE — the
            // element does not survive a WHERE. Same rule the standalone filter
            // applies to the expanded row.
            if (!vec_row_is_valid(v, i)) continue;
            const uint32_t phys = codes[i];
            mask[i] = static_cast<uint8_t>((bits[phys >> 3] >> (phys & 7)) & 1u);
        }
        return true;
    }

    OpResult execute(const MorselPtr& in, OperatorState& st_, MorselPtr& out,
                     ErrCtx& err) override {
        // EOS / empty batch: nothing to expand. Dropped (the pipeline terminates on
        // the source's FINISHED, not on an EOS morsel reaching the sink).
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        if (array_idx >= in->columns.size()) {
            err.code = 1;
            err.msg = "UnnestOperator: array column index out of range — the "
                      "compiler's layout tracking disagrees with the stream";
            return OpResult::NEED_INPUT;
        }
        if (in->columns[array_idx].view.type != DRAKEN_ARRAY) {
            err.code = 1;
            err.msg = "UnnestOperator: CROSS JOIN UNNEST source column is not an ARRAY";
            return OpResult::NEED_INPUT;
        }
        // Pushed WHERE: decide which ELEMENTS survive before anything is built for
        // them. `mask` stays alive across the cxx_unnest_c call below, which only
        // reads it during its own pass 1.
        std::vector<uint8_t> mask;
        if (filter_fn != nullptr) {
            if (!build_child_mask(in->columns[array_idx], mask, err))
                return OpResult::NEED_INPUT;
        }
        if (distinct_target) {
            if (!apply_distinct(in->columns[array_idx], mask,
                                static_cast<UnnestState&>(st_), err))
                return OpResult::NEED_INPUT;
        }
        // draken owns the new()/delete() of the result (cxx_morsel_delete), so the
        // shared_ptr frees it in the TU that allocated it — no cross-.so heap mixing.
        MorselPtr result(cxx_unnest_c(in.get(), array_idx, target_name.data(),
                                      static_cast<uint32_t>(target_name.size()),
                                      drop_source ? 1 : 0,
                                      mask.empty() ? nullptr : mask.data()),
                         cxx_morsel_delete);
        if (!result || result->num_rows() == 0) return OpResult::NEED_INPUT;
        result->state = in->state;
        out = std::move(result);
        return OpResult::EMIT;
    }
};

// CROSS JOIN UNNEST over a LITERAL array: `lit` is a plan-constant one-column
// morsel (materialized once at compile time, same legitimacy as a virtual
// dataset). Each input row is repeated len(lit) times with the literal tiled
// across them, and the target column is APPENDED (there is no source ARRAY column
// to consume). An empty literal yields no rows.
struct UnnestLiteralOperator : Operator {
    MorselPtr   lit;
    std::string target_name;

    UnnestLiteralOperator(MorselPtr literal, std::string name)
        : lit(std::move(literal)), target_name(std::move(name)) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }

    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        MorselPtr result(cxx_unnest_literal_c(in.get(), lit.get(), target_name.data(),
                                              static_cast<uint32_t>(target_name.size())),
                         cxx_morsel_delete);
        if (!result) {
            err.code = 1;
            err.msg = "UnnestLiteralOperator: literal array must be exactly one column";
            return OpResult::NEED_INPUT;
        }
        if (result->num_rows() == 0) return OpResult::NEED_INPUT;
        result->state = in->state;
        out = std::move(result);
        return OpResult::EMIT;
    }
};

}  // namespace opteryx::engine
