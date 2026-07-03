#pragma once
// src/cpp/engine/engine.hpp — the pipeline-graph Engine: THE execution engine.
//
// A compiled physical plan crosses the Python/native boundary ONCE, as a graph of
// pipelines (Source -> Operator* -> Sink) built in topological order by the plan
// compiler (opteryx/managers/execution/compiler.py — planning, Python) through the
// builder methods below (the Cython edge). run() then executes the pipelines in
// creation order at degree `dop` (run_pipeline, executor.hpp): breaker results hand
// off natively (MorselBuffer for materialized morsels, JoinBuildRef for a hash-join
// build table); the terminal pipeline streams into the production MorselQueue the
// (Python) cursor drains. No Python runs inside run() — the one tracked exception
// is StreamingScanSource's pull trampoline (see the engine_cutover_decisions memory:
// interim debt until native_parquet_scan_source covers every scan shape).
//
// The builder API is deliberately index-based and flat (size_t handles, plain
// vectors) so the Cython edge stays a dumb marshaller: all decisions — pipeline
// decomposition, column indices, output names/types — are made at plan time by the
// compiler. Anything the compiler cannot express fails loud THERE, before run().

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor.hpp"
#include "morsel_queue.hpp"
#include "native_expression.hpp"    // ExprFilterOperator, ExprMultiProjectOperator
#include "native_group_sinks.hpp"   // UngroupedAggSink, GroupBySink, DistinctSink, AggSpec2
#include "native_hash_join.hpp"     // HashJoinBuildSink/Global, JoinProbeOperator
#include "native_join2.hpp"         // Join2BuildSink/Probe — multi-key, semi/anti/outer
#include "native_parquet_scan_source.hpp"  // NativeParquetScanSource (zero-Python pull)
#include "native_sort.hpp"          // SortSink, TopNSink, SortKeySpec, gather_rows
#include "pipeline_buffers.hpp"     // MorselBuffer, BufferSource
#include "scan_aggregate_demo.hpp"  // NULL-aware agg helpers (agg_is_valid et al.)
#include "scan_filter_demo.hpp"     // NumericFilterOperator, SimplePredicate, QueueSink/Global
#include "streaming_scan_source.hpp"

namespace opteryx::engine {

// Zero-row typed column — the courtesy empty-result morsel (schema visibility when a
// query legitimately returns no rows; the old ExitNode's `at_least_one` contract).
// String-family columns get a canonical empty DrakenStringArena header (buffers.h:
// a string vector's `data` points at the arena STRUCT, even when empty).
inline CxxColumn make_empty_col(DrakenType t) {
    void* data;
    if (t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY
            || t == DRAKEN_VARIANT) {
        auto* sa = static_cast<DrakenStringArena*>(draken_malloc(sizeof(DrakenStringArena)));
        sa->slots = nullptr; sa->arena = nullptr; sa->length = 0;
        sa->arena_used = 0; sa->arena_cap = 0; sa->null_bitmap = nullptr;
        sa->owns_buffers = 0; sa->type = t;
        data = sa;
    } else {
        data = draken_malloc(1);
    }
    uint32_t* sel = static_cast<uint32_t*>(draken_malloc(sizeof(uint32_t)));
    DrakenVector v;
    v.data = data; v.selection = sel; v.data_length = 0; v.length = 0;
    v.validity = nullptr; v.type = t; v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    CxxColumn c;
    c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                          OwnedBuffer<uint8_t>(nullptr), OwnedBuffer<void>(sel));
    c.view = c.own->vec;
    return c;
}

// ---- LimitOperator: LIMIT/OFFSET over the stream ----------------------------------
// A shared atomic claims each morsel's position in the (arbitrary at dop>1, stream
// at dop=1) row order; rows outside [offset, offset+limit) are dropped, a partial
// overlap is sliced via gather_rows. Once the quota is exhausted the pipeline's halt
// flag stops every worker's source loop — no pointless scan-to-completion.

struct LimitOperator : Operator {
    int64_t offset;
    int64_t limit;                    // INT64_MAX == no limit (OFFSET-only)
    std::atomic<int64_t> seen{0};
    std::atomic<bool>* halt;

    LimitOperator(int64_t off, int64_t lim, std::atomic<bool>* h)
        : offset(off), limit(lim), halt(h) {}

    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        int64_t rows = static_cast<int64_t>(in->num_rows());
        int64_t start = seen.fetch_add(rows);
        int64_t hi = (limit >= INT64_MAX - offset) ? INT64_MAX : offset + limit;
        if (start + rows >= hi && halt != nullptr) halt->store(true, std::memory_order_relaxed);
        int64_t s = start > offset ? start : offset;
        int64_t e = (start + rows) < hi ? (start + rows) : hi;
        if (s >= e) return OpResult::NEED_INPUT;
        if (s == start && e == start + rows) {
            out = in;   // full overlap — pass through, zero copy
            return OpResult::EMIT;
        }
        // Partial overlap: slice rows [s-start, e-start) out of this one morsel.
        uint32_t lo_r = static_cast<uint32_t>(s - start);
        uint32_t cnt = static_cast<uint32_t>(e - s);
        std::vector<MorselPtr> ms{in};
        std::vector<uint32_t> order(cnt), row_m(in->num_rows(), 0), row_r(in->num_rows());
        for (uint32_t i = 0; i < in->num_rows(); ++i) row_r[i] = i;
        for (uint32_t i = 0; i < cnt; ++i) order[i] = lo_r + i;
        out = gather_rows(ms, order, 0, cnt, row_m, row_r, in->names, err);
        return (err.code != 0) ? OpResult::NEED_INPUT : OpResult::EMIT;
    }
};

// ---- ColumnSelectOperator: select columns by index and (re)name them --------------
// Serves plain identifier projection AND the Exit select/rename (final_columns ->
// final_names). Zero data copy: columns share their VectorOwner into the new morsel.

struct ColumnSelectOperator : Operator {
    std::vector<size_t> indices;
    std::vector<std::string> out_names;
    ColumnSelectOperator(std::vector<size_t> idx, std::vector<std::string> names)
        : indices(std::move(idx)), out_names(std::move(names)) {}
    std::unique_ptr<OperatorState> make_state() override {
        return std::make_unique<OperatorState>();
    }
    OpResult execute(const MorselPtr& in, OperatorState&, MorselPtr& out,
                     ErrCtx& err) override {
        // A zero-match scan can emit a zero-row morsel that carries FEWER (or
        // zero) columns than the tracked layout — nothing to select, drop it.
        // A genuine layout disagreement still fails loud below on real rows.
        if (in->num_rows() == 0) return OpResult::NEED_INPUT;
        auto m = std::make_shared<CxxMorsel>();
        m->columns.reserve(indices.size());
        for (size_t idx : indices) {
            if (idx >= in->columns.size()) {
                err.code = 1;
                err.msg = "ColumnSelectOperator: column index out of range — the "
                          "compiler's layout tracking disagrees with the stream";
                return OpResult::NEED_INPUT;
            }
            m->columns.push_back(in->columns[idx]);  // shared owner, zero copy
        }
        m->names = out_names;
        m->zero_col_rows = in->num_rows();
        m->state = in->state;
        out = std::move(m);
        return OpResult::EMIT;
    }
};

// ---- Join wiring: build table filled AFTER the build pipeline runs ----------------
// The probe pipeline's operators are constructed at plan-build time, before the build
// hash table exists. JoinBuildRef is the indirection: the Engine fills `g` the moment
// the build pipeline's finalize completes (dependency order guarantees this precedes
// any probe worker's make_state()).

struct JoinBuildRef {
    const HashJoinBuildGlobal* g = nullptr;
};

struct DeferredJoinProbeOperator : Operator {
    size_t probe_key_idx;
    std::vector<size_t> probe_payload_idx;
    const JoinBuildRef* ref;
    std::once_flag once;
    std::unique_ptr<JoinProbeOperator> inner;

    DeferredJoinProbeOperator(size_t key_idx, std::vector<size_t> payload_idx,
                              const JoinBuildRef* r)
        : probe_key_idx(key_idx), probe_payload_idx(std::move(payload_idx)), ref(r) {}

    std::unique_ptr<OperatorState> make_state() override {
        // First touch is at probe-pipeline worker start — strictly after the build
        // pipeline finalized (Engine::run order), so ref->g is set.
        std::call_once(once, [this] {
            inner = std::make_unique<JoinProbeOperator>(
                probe_key_idx, &ref->g->key_to_rows, &ref->g->payload,
                probe_payload_idx);
        });
        return inner->make_state();
    }
    OpResult execute(const MorselPtr& in, OperatorState& st, MorselPtr& out,
                     ErrCtx& err) override {
        return inner->execute(in, st, out, err);
    }
};

// ---- The Engine: pipeline graph, built by the compiler, run natively --------------

struct PipelineNode {
    std::unique_ptr<Source> source;
    std::vector<std::unique_ptr<Operator>> operators;
    std::unique_ptr<Sink> sink;
    int fill_join_ref = -1;   // join_refs index to point at this sink's global post-run
    int fill_join2_ref = -1;  // join2_refs index (generalized join)
    int dop_override = 0;     // >0 forces this pipeline's degree (order-sensitive
                              // consumers of a sorted buffer run at 1); 0 = engine dop
    std::atomic<bool> halt{false};   // set by LimitOperator when its quota is filled
    std::unique_ptr<GlobalSinkState> result;
};

class Engine {
public:
    std::vector<std::unique_ptr<PipelineNode>> pipelines;  // run in creation order
    std::vector<std::unique_ptr<MorselBuffer>> buffers;
    std::vector<std::unique_ptr<JoinBuildRef>> join_refs;
    std::vector<std::unique_ptr<Join2Ref>> join2_refs;
    MorselQueue* out_q = nullptr;
    std::vector<std::string> final_names;   // terminal schema, for the empty-result morsel
    std::vector<DrakenType>  final_types;

    // ---- builder edge (called from Cython at plan-build time; not the hot path) ----
    size_t new_pipeline() {
        pipelines.push_back(std::make_unique<PipelineNode>());
        return pipelines.size() - 1;
    }
    size_t new_buffer() {
        buffers.push_back(std::make_unique<MorselBuffer>());
        return buffers.size() - 1;
    }
    size_t new_join_ref() {
        join_refs.push_back(std::make_unique<JoinBuildRef>());
        return join_refs.size() - 1;
    }
    size_t new_join2_ref() {
        join2_refs.push_back(std::make_unique<Join2Ref>());
        return join2_refs.size() - 1;
    }
    void set_join2_build_sink(size_t p, std::vector<size_t> key_idx,
                              std::vector<size_t> payload_idx, size_t ref) {
        pipelines[p]->sink =
            std::make_unique<Join2BuildSink>(std::move(key_idx), std::move(payload_idx));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    void add_join2_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                         std::vector<size_t> payload_idx, int mode) {
        pipelines[p]->operators.push_back(std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            static_cast<JoinMode>(mode)));
    }
    // ASOF: build side = Join2BuildSink capturing the asof column's order key;
    // probe side = nearest-match per MATCH_CONDITION op (0 GtEq / 1 Gt / 2 LtEq / 3 Lt).
    void set_asof_build_sink(size_t p, std::vector<size_t> key_idx,
                             std::vector<size_t> payload_idx, size_t asof_idx,
                             size_t ref) {
        pipelines[p]->sink = std::make_unique<Join2BuildSink>(
            std::move(key_idx), std::move(payload_idx), static_cast<int>(asof_idx));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    void add_asof_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                        std::vector<size_t> payload_idx, size_t asof_idx, int op) {
        pipelines[p]->operators.push_back(std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            JoinMode::LeftOuter, static_cast<int>(asof_idx), op));
    }
    void set_scan_source(size_t p, void* scan_ptr, ScanPullFn fn, bool serialize_pull) {
        pipelines[p]->source =
            std::make_unique<StreamingScanSource>(scan_ptr, fn, serialize_pull);
    }
    // Zero-Python scan Source: workers pull decoded row groups straight from the
    // rugo IO pipeline — no GIL trampoline, no per-morsel thread attach. All the
    // borrowed pointers live on the caller's NativeScanPlan, which the NativePlan
    // holds alive for the whole run (closed only after the driver's done-event).
    // Increment-1 scope: fixed-width numeric columns only — the plan-time gate
    // (native_scan_supported) must have proven every projected column eligible;
    // an unsupported kind reaching build_column is a gate bug and fails loud.
    void set_native_scan_source(size_t p, rugo::ParquetIOPipeline* pipeline,
                                const std::unordered_map<std::string, FileStats>* footer_map,
                                const std::vector<std::pair<std::string, int>>* work_items,
                                const std::vector<std::string>* column_names,
                                int in_flight_limit) {
        pipelines[p]->source = std::make_unique<NativeParquetScanSource>(
            pipeline, footer_map, work_items, column_names, in_flight_limit);
    }
    void set_buffer_source(size_t p, size_t buf) {
        pipelines[p]->source = std::make_unique<BufferSource>(buffers[buf].get());
    }
    void add_filter(size_t p, std::vector<SimplePredicate> preds) {
        pipelines[p]->operators.push_back(
            std::make_unique<NumericFilterOperator>(std::move(preds)));
    }
    void add_expr_filter(size_t p, void* instrs, int count, std::vector<int> col_idx,
                         std::vector<void*> lit_dv, ExprFilterFn fn) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        pipelines[p]->operators.push_back(
            std::make_unique<ExprFilterOperator>(std::move(prog), fn));
    }
    void add_expr_project(size_t p, void* instrs, int count, std::vector<int> col_idx,
                          std::vector<void*> lit_dv, ExprEvalFn fn, std::string name,
                          int lt_kind, int lt_unit, int lt_precision, int lt_scale) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        const LogicalType* logical = nullptr;
        if (lt_kind != 0) {
            LogicalType lt;
            lt.kind = static_cast<LogicalKind>(lt_kind);
            lt.unit = static_cast<TimestampUnit>(lt_unit);
            lt.precision = static_cast<uint8_t>(lt_precision);
            lt.scale = static_cast<uint8_t>(lt_scale);
            logical = logical_type_intern(lt);
        }
        // Fuse consecutive computed columns into ONE operator: a 90-projection
        // query (clickbench Q30) paid O(N²) shared_ptr column-vector copies
        // through a chain of single-column operators. Later programs still see
        // earlier outputs — the multi op grows the morsel between programs.
        if (!pipelines[p]->operators.empty()) {
            auto* tail = dynamic_cast<ExprMultiProjectOperator*>(
                pipelines[p]->operators.back().get());
            if (tail != nullptr && tail->fn == fn) {
                tail->progs.push_back(std::move(prog));
                tail->out_names.push_back(std::move(name));
                tail->out_logicals.push_back(logical);
                return;
            }
        }
        std::vector<ExprProgram> ps;
        ps.push_back(std::move(prog));
        pipelines[p]->operators.push_back(std::make_unique<ExprMultiProjectOperator>(
            std::move(ps), fn,
            std::vector<std::string>{std::move(name)},
            std::vector<const LogicalType*>{logical}));
    }
    void add_limit(size_t p, int64_t offset, int64_t limit) {
        pipelines[p]->operators.push_back(
            std::make_unique<LimitOperator>(offset, limit, &pipelines[p]->halt));
    }
    void add_buffer_morsel(size_t buf, MorselPtr m) {
        // Plan-time materialization edge: a virtual dataset's (plan-constant)
        // morsels are placed in a buffer BEFORE run() — execution reads them
        // natively, never re-entering Python.
        buffers[buf]->morsels.push_back(std::move(m));
    }
    void set_pipeline_dop(size_t p, int dop) {
        pipelines[p]->dop_override = dop;
    }
    void set_sort_sink(size_t p, std::vector<SortKeySpec> spec, size_t buf) {
        pipelines[p]->sink = std::make_unique<SortSink>(std::move(spec), buffers[buf].get());
    }
    void set_topn_sink(size_t p, std::vector<SortKeySpec> spec, size_t n, size_t buf) {
        pipelines[p]->sink =
            std::make_unique<TopNSink>(std::move(spec), n, buffers[buf].get());
    }
    // Window ranking: sort_spec = [partition keys asc..., order keys...]; n_part =
    // count of leading partition keys; fn_kinds[i] pairs with fn_names[i].
    void set_window_sink(size_t p, std::vector<SortKeySpec> sort_spec, size_t n_part,
                         std::vector<int> fn_kinds, std::vector<std::string> fn_names,
                         size_t buf) {
        std::vector<WindowFnSpec> funcs;
        funcs.reserve(fn_kinds.size());
        for (size_t i = 0; i < fn_kinds.size(); ++i)
            funcs.push_back({static_cast<WinFn>(fn_kinds[i]), fn_names[i]});
        pipelines[p]->sink = std::make_unique<WindowSink>(
            std::move(sort_spec), n_part, std::move(funcs), buffers[buf].get());
    }
    void add_select(size_t p, std::vector<size_t> indices, std::vector<std::string> names) {
        pipelines[p]->operators.push_back(
            std::make_unique<ColumnSelectOperator>(std::move(indices), std::move(names)));
    }
    void add_join_probe(size_t p, size_t ref, size_t key_idx, std::vector<size_t> payload_idx) {
        pipelines[p]->operators.push_back(std::make_unique<DeferredJoinProbeOperator>(
            key_idx, std::move(payload_idx), join_refs[ref].get()));
    }
    void set_queue_sink(size_t p, MorselQueue* q) {
        pipelines[p]->sink = std::make_unique<QueueSink>(q);
        out_q = q;
    }
    void set_agg_sink(size_t p, std::vector<AggSpec2> specs, size_t buf) {
        pipelines[p]->sink =
            std::make_unique<UngroupedAggSink>(std::move(specs), buffers[buf].get());
    }
    void set_groupby_sink(size_t p, std::vector<size_t> key_idx,
                          std::vector<AggSpec2> specs, size_t buf) {
        pipelines[p]->sink = std::make_unique<GroupBySink>(
            std::move(key_idx), std::move(specs), buffers[buf].get());
    }
    void set_distinct_sink(size_t p, std::vector<size_t> on_idx, size_t buf) {
        pipelines[p]->sink =
            std::make_unique<DistinctSink>(std::move(on_idx), buffers[buf].get());
    }
    void set_buffer_append_sink(size_t p, size_t buf) {
        pipelines[p]->sink = std::make_unique<BufferAppendSink>(buffers[buf].get());
    }
    void set_join_build_sink(size_t p, size_t key_idx, std::vector<size_t> payload_idx,
                             size_t ref) {
        pipelines[p]->sink = std::make_unique<HashJoinBuildSink>(key_idx, std::move(payload_idx));
        pipelines[p]->fill_join_ref = static_cast<int>(ref);
    }
    void set_final_schema(std::vector<std::string> names, std::vector<DrakenType> types) {
        final_names = std::move(names);
        final_types = std::move(types);
    }

    // ---- execution (native; called once from the detached driver task) ------------
    // Invariant (compiler-enforced): the LAST pipeline's sink is the QueueSink.
    void run(int dop, void* pool, ErrCtx& err) {
        for (auto& pn : pipelines) {
            // Consumer abandoned (LIMIT early-exit / cursor dropped): stop between
            // pipelines — running the rest of the graph would be wasted work feeding
            // a closed queue. Not an error; matches QueueSink's dropped-put contract.
            if (out_q != nullptr && out_q->closed()) return;
            Pipeline p;
            p.source = pn->source.get();
            p.operators.reserve(pn->operators.size());
            for (auto& op : pn->operators) p.operators.push_back(op.get());
            p.sink = pn->sink.get();
            p.halt = &pn->halt;
            int pdop = (pn->dop_override > 0 && pn->dop_override < dop)
                           ? pn->dop_override : dop;
            pn->result = run_pipeline(p, pdop, err, pool);
            if (err.code != 0) return;
            if (pn->fill_join_ref >= 0) {
                join_refs[static_cast<size_t>(pn->fill_join_ref)]->g =
                    static_cast<const HashJoinBuildGlobal*>(pn->result.get());
            }
            if (pn->fill_join2_ref >= 0) {
                join2_refs[static_cast<size_t>(pn->fill_join2_ref)]->g =
                    static_cast<const Join2BuildGlobal*>(pn->result.get());
            }
        }
        // Courtesy empty-result morsel: a query that legitimately produced zero rows
        // still surfaces its output schema to the cursor (the old Exit `at_least_one`).
        if (!pipelines.empty() && out_q != nullptr && !out_q->closed()) {
            auto* qg = static_cast<QueueSinkGlobal*>(pipelines.back()->result.get());
            if (qg->rows_out.load() == 0) {
                auto m = std::make_shared<CxxMorsel>();
                m->columns.reserve(final_types.size());
                for (size_t i = 0; i < final_types.size(); ++i) {
                    m->columns.push_back(make_empty_col(final_types[i]));
                }
                m->names = final_names;
                out_q->put(m);
            }
        }
    }
};

}  // namespace opteryx::engine
