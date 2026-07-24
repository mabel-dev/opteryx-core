#pragma once
// src/cpp/engine/engine.hpp — the pipeline-graph Engine: THE execution engine.
//
// A compiled physical plan crosses the Python/native boundary ONCE, as a graph of
// pipelines (Source -> Operator* -> Sink) built in topological order by the plan
// compiler (opteryx/managers/execution/compiler.py — planning, Python) through the
// builder methods below (the Cython edge). run() then executes the pipelines in
// creation order at degree `dop` (run_pipeline, executor.hpp): breaker results hand
// off natively (MorselBuffer for materialized morsels, Join2Ref for a hash-join
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
#include "native_key_hash.hpp"     // compute_row_hashes — the shared equi-key hash
#include "native_join2.hpp"         // Join2BuildSink/Probe — multi-key, semi/anti/outer
#include "native_parquet_scan_source.hpp"  // NativeParquetScanSource (zero-Python pull)
#include "native_sort.hpp"          // SortSink, TopNSink, SortKeySpec, gather_rows
#include "native_unnest.hpp"        // UnnestOperator — CROSS JOIN UNNEST
#include "pipeline_buffers.hpp"     // MorselBuffer, BufferSource
#include "native_queue_sink.hpp"    // QueueSink/Global — the terminal output edge
#include "streaming_scan_source.hpp"
#include "trace.hpp"                 // TraceSpan/trace_begin/trace_drain — execution tracing

namespace opteryx::engine {

// Intern one LogicalType* per column from parallel (kind, unit, precision, scale,
// dimension) int arrays — the wire shape Python's compiler passes for every
// descriptor-carrying call (add_expr_project, set_final_schema, the join build
// sinks below). kind == 0 means "no logical type" (nullptr, not an all-zero
// LogicalType).
inline std::vector<const LogicalType*> intern_logical_vec(
        const std::vector<int>& kind, const std::vector<int>& unit,
        const std::vector<int>& precision, const std::vector<int>& scale,
        const std::vector<int>& dimension) {
    std::vector<const LogicalType*> out;
    out.reserve(kind.size());
    for (size_t i = 0; i < kind.size(); ++i) {
        if (kind[i] == 0) {
            out.push_back(nullptr);
            continue;
        }
        LogicalType lt;
        lt.kind = static_cast<LogicalKind>(kind[i]);
        lt.unit = static_cast<TimestampUnit>(unit[i]);
        lt.precision = static_cast<uint8_t>(precision[i]);
        lt.scale = static_cast<uint8_t>(scale[i]);
        lt.dimension = static_cast<uint32_t>(dimension[i]);
        out.push_back(logical_type_intern(lt));
    }
    return out;
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

// ---- The Engine: pipeline graph, built by the compiler, run natively --------------

struct PipelineNode {
    std::unique_ptr<Source> source;
    std::vector<std::unique_ptr<Operator>> operators;
    std::unique_ptr<Sink> sink;
    int fill_join2_ref = -1;  // join2_refs index to point at this sink's global post-run
    int dop_override = 0;     // >0 forces this pipeline's degree (order-sensitive
                              // consumers of a sorted buffer run at 1); 0 = engine dop
    std::atomic<bool> halt{false};   // set by LimitOperator when its quota is filled
    std::unique_ptr<GlobalSinkState> result;
};

class Engine {
public:
    std::vector<std::unique_ptr<PipelineNode>> pipelines;  // run in creation order
    std::vector<std::unique_ptr<MorselBuffer>> buffers;
    std::vector<std::unique_ptr<Join2Ref>> join2_refs;
    MorselQueue* out_q = nullptr;
    std::vector<std::string> final_names;   // terminal schema, for the empty-result morsel
    std::vector<DrakenType>  final_types;
    std::vector<const LogicalType*> final_logical;  // parallel to final_types; nullptr = none

    // Plan-node identity currently being lowered. The compiler sets this once per plan
    // node (set_current_identity) before that node's builder calls; every operator/sink/
    // source created while it is current inherits it, so the harvest can attribute the
    // per-operator readings back to the plan node. Empty for untagged (demo) call sites.
    std::string current_identity_;
    void set_current_identity(std::string s) { current_identity_ = std::move(s); }

    // Human-readable plan-node kind (e.g. "FilterNode") for the SAME plan node
    // set_current_identity just tagged — set alongside it, never alone. Purely
    // a display concern (see OpStats::display_name / collect_trace_symbols):
    // identity stays the correlation key, this never gets compared/summed on.
    std::string current_display_name_;
    void set_current_display_name(std::string s) { current_display_name_ = std::move(s); }

    // Monotonic per-Engine (i.e. per-query) counter for OpStats.node_id — see
    // trace.hpp. Starts at 1; 0 is reserved for "untagged".
    uint32_t next_trace_node_id_ = 1;

    // One harvested reading row (per operator/source/sink). Several may share an identity
    // (a plan node lowered to multiple operators, or operator fusion) — the Python side
    // sums them per identity.
    struct OpReading {
        std::string identity;
        std::string role;   // "source" | "operator" | "sink"
        uint64_t calls, rows_in, rows_out, bytes_in, bytes_out, exec_ns, cpu_ns;
    };
    std::vector<OpReading> collect_op_stats() const {
        std::vector<OpReading> out;
        auto emit = [&out](const OpStats& s, const char* role) {
            out.push_back(OpReading{
                s.identity, role,
                s.calls.load(), s.rows_in.load(), s.rows_out.load(),
                s.bytes_in.load(), s.bytes_out.load(), s.exec_ns.load(),
                s.cpu_ns.load()});
        };
        for (const auto& pn : pipelines) {
            if (pn->source) emit(pn->source->stats, "source");
            for (const auto& op : pn->operators) emit(op->stats, "operator");
            if (pn->sink) emit(pn->sink->stats, "sink");
        }
        return out;
    }

    // node_id -> display name, for resolving trace.hpp spans (which carry only
    // the compact node_id) back to a human-readable plan-node kind at drain
    // time. Falls back to identity (the opaque correlation key) only for
    // call sites that never set a display name — never the normal case.
    // Same iteration shape as collect_op_stats — one row per tagged operator/
    // source/sink, several may share a name.
    std::vector<std::pair<uint32_t, std::string>> collect_trace_symbols() const {
        std::vector<std::pair<uint32_t, std::string>> out;
        auto emit = [&out](const OpStats& s) {
            if (s.node_id == 0) return;
            const std::string& name = s.display_name.empty() ? s.identity : s.display_name;
            if (!name.empty()) out.emplace_back(s.node_id, name);
        };
        for (const auto& pn : pipelines) {
            if (pn->source) emit(pn->source->stats);
            for (const auto& op : pn->operators) emit(op->stats);
            if (pn->sink) emit(pn->sink->stats);
        }
        return out;
    }

    // ---- builder edge (called from Cython at plan-build time; not the hot path) ----
    // Stamp the current plan-node identity onto a freshly built operator/source/sink and
    // install it. Every builder below routes through these so no reading goes untagged.
    // node_id is assigned here too (monotonic per Engine instance, i.e. per query) so
    // trace.hpp spans can carry a compact id instead of copying the identity string
    // onto every span.
    Operator* add_op_(size_t p, std::unique_ptr<Operator> op) {
        op->stats.identity = current_identity_;
        op->stats.display_name = current_display_name_;
        op->stats.node_id = next_trace_node_id_++;
        pipelines[p]->operators.push_back(std::move(op));
        return pipelines[p]->operators.back().get();
    }
    void set_sink_(size_t p, std::unique_ptr<Sink> s) {
        s->stats.identity = current_identity_;
        s->stats.display_name = current_display_name_;
        s->stats.node_id = next_trace_node_id_++;
        pipelines[p]->sink = std::move(s);
    }
    // Returns the node_id it assigned — set_native_scan_source uses this to tag
    // the underlying ParquetIOPipeline's trace spans with the same identity the
    // wrapping Source's OpStats carries (see set_trace_node_id below). Every
    // other caller ignores the return value.
    uint32_t set_source_(size_t p, std::unique_ptr<Source> s) {
        s->stats.identity = current_identity_;
        s->stats.display_name = current_display_name_;
        s->stats.node_id = next_trace_node_id_++;
        uint32_t node_id = s->stats.node_id;
        pipelines[p]->source = std::move(s);
        return node_id;
    }

    size_t new_pipeline() {
        pipelines.push_back(std::make_unique<PipelineNode>());
        return pipelines.size() - 1;
    }
    size_t new_buffer() {
        buffers.push_back(std::make_unique<MorselBuffer>());
        return buffers.size() - 1;
    }
    size_t new_join2_ref() {
        join2_refs.push_back(std::make_unique<Join2Ref>());
        return join2_refs.size() - 1;
    }
    // payload_types/lt_* are the build-side payload columns' PLAN-KNOWN physical +
    // logical types (same shape as set_final_schema) — sized/typed into the build
    // sink's row-store up front, so a build side that streams zero rows (a filtered-
    // to-empty subquery) still produces a correctly-shaped, correctly-typed empty
    // payload instead of leaving it unsized. Without this, LEFT OUTER/ASOF's
    // unmatched-row emit (which must still produce every probe row) reads past the
    // end of an empty row-store — see ColumnSelectOperator's "layout tracking
    // disagrees with the stream" invariant failure this fixes.
    void set_join2_build_sink(size_t p, std::vector<size_t> key_idx,
                              std::vector<size_t> payload_idx, size_t ref,
                              std::vector<DrakenType> payload_types,
                              std::vector<int> lt_kind, std::vector<int> lt_unit,
                              std::vector<int> lt_precision, std::vector<int> lt_scale,
                              std::vector<int> lt_dimension) {
        auto payload_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                                   lt_scale, lt_dimension);
        set_sink_(p, std::make_unique<Join2BuildSink>(
            std::move(key_idx), std::move(payload_idx),
            std::move(payload_types), std::move(payload_logical)));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    void add_join2_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                         std::vector<size_t> payload_idx, int mode) {
        add_op_(p, std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            static_cast<JoinMode>(mode)));
    }
    // ASOF: build side = Join2BuildSink capturing the asof column's order key;
    // probe side = nearest-match per MATCH_CONDITION op (0 GtEq / 1 Gt / 2 LtEq / 3 Lt).
    void set_asof_build_sink(size_t p, std::vector<size_t> key_idx,
                             std::vector<size_t> payload_idx, size_t asof_idx,
                             size_t ref, std::vector<DrakenType> payload_types,
                             std::vector<int> lt_kind, std::vector<int> lt_unit,
                             std::vector<int> lt_precision, std::vector<int> lt_scale,
                             std::vector<int> lt_dimension) {
        auto payload_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                                   lt_scale, lt_dimension);
        set_sink_(p, std::make_unique<Join2BuildSink>(
            std::move(key_idx), std::move(payload_idx),
            std::move(payload_types), std::move(payload_logical),
            static_cast<int>(asof_idx)));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    void add_asof_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                        std::vector<size_t> payload_idx, size_t asof_idx, int op) {
        add_op_(p, std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            JoinMode::LeftOuter, static_cast<int>(asof_idx), op));
    }
    void set_scan_source(size_t p, void* scan_ptr, ScanPullFn fn, bool serialize_pull) {
        set_source_(p,
            std::make_unique<StreamingScanSource>(scan_ptr, fn, serialize_pull));
    }
    // Zero-Python scan Source: workers pull decoded row groups straight from the
    // rugo IO pipeline — no GIL trampoline, no per-morsel thread attach. All the
    // borrowed pointers live on the caller's NativeScanPlan, which the NativePlan
    // holds alive for the whole run (closed only after the driver's done-event).
    // Increment-1 scope: fixed-width numeric columns only — the plan-time gate
    // (native_scan_supported) must have proven every projected column eligible;
    // an unsupported kind reaching build_column is a gate bug and fails loud.
    // `pool` + `string_types` (parallel to column_names: declared string DrakenType,
    // 0 for non-string) widen this to string projections (WP-01): a DK_POOL string
    // column decodes from `pool`, and every string column is tagged with its exact
    // declared type. Both default null → the original numeric-only behaviour.
    // `decimal_columns` + `logical_coerce` (both parallel to column_names) widen
    // this to WP-11's decimal/temporal projections: `decimal_columns[i]` routes an
    // int64-backed DECIMAL DK_POOL column to the decimal decoder, and
    // `logical_coerce[i]` carries the retag kind + unit / precision-scale so
    // DATE/TIMESTAMP/TIME/DECIMAL columns land byte-identically to the trampoline.
    // Both default null → the original numeric+string behaviour.
    void set_native_scan_source(size_t p, rugo::ParquetIOPipeline* pipeline,
                                const std::unordered_map<std::string, FileStats>* footer_map,
                                const std::vector<std::pair<std::string, int>>* work_items,
                                const std::vector<std::string>* column_names,
                                int in_flight_limit,
                                MemoryPool* pool = nullptr,
                                const std::vector<int>* string_types = nullptr,
                                const std::vector<uint8_t>* decimal_columns = nullptr,
                                const std::vector<int>* logical_coerce = nullptr,
                                const std::vector<uint8_t>* hash_key_columns = nullptr) {
        // docs/EXECUTION_TRACING_DESIGN.md: tag the rugo pipeline's trace spans
        // (TC_QUEUE_WAIT/TC_IO_REQUEST/TC_DECODE — currently node_id=0/untagged,
        // see io_pipeline.hpp's set_trace_node_id) with the SAME node_id this
        // scan's Source/OpStats gets, so a query with more than one scan can
        // attribute IO spans back to the right plan node. The pipeline was
        // already constructed (in Python, before this call) with no node_id
        // available yet — this is the first point node_id exists, so it's set
        // here rather than threaded through the Cython/Python construction path.
        uint32_t node_id = set_source_(p, std::make_unique<NativeParquetScanSource>(
            pipeline, footer_map, work_items, column_names, in_flight_limit,
            pool, decimal_columns, /*varchar_columns=*/nullptr, string_types, logical_coerce,
            hash_key_columns));
        if (pipeline != nullptr) pipeline->set_trace_node_id(node_id);
    }
    void set_buffer_source(size_t p, size_t buf) {
        set_source_(p, std::make_unique<BufferSource>(buffers[buf].get()));
    }
    void add_expr_filter(size_t p, void* instrs, int count, std::vector<int> col_idx,
                         std::vector<void*> lit_dv, ExprFilterFn fn,
                         std::vector<int> const_col_idx = {},
                         std::vector<void*> const_scalar_dv = {}) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        prog.const_col_idx = std::move(const_col_idx);
        prog.const_scalar_dv = std::move(const_scalar_dv);
        add_op_(p, std::make_unique<ExprFilterOperator>(std::move(prog), fn));
    }
    void add_expr_project(size_t p, void* instrs, int count, std::vector<int> col_idx,
                          std::vector<void*> lit_dv, ExprEvalFn fn, std::string name,
                          int lt_kind, int lt_unit, int lt_precision, int lt_scale,
                          int lt_dimension) {
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
            // VECTOR's width. Zero for every other kind — the descriptor channel
            // carried (kind, unit, precision, scale) only, so a computed VECTOR
            // column reached the engine with dimension 0 and every downstream
            // reader (take, to_pylist) rejected it.
            lt.dimension = static_cast<uint32_t>(lt_dimension);
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
        add_op_(p, std::make_unique<ExprMultiProjectOperator>(
            std::move(ps), fn,
            std::vector<std::string>{std::move(name)},
            std::vector<const LogicalType*>{logical}));
    }
    void add_limit(size_t p, int64_t offset, int64_t limit) {
        add_op_(p, std::make_unique<LimitOperator>(offset, limit, &pipelines[p]->halt));
    }
    void add_unnest(size_t p, uint32_t array_idx, std::string target_name,
                    bool drop_source) {
        add_op_(p, std::make_unique<UnnestOperator>(array_idx, std::move(target_name),
                                                    drop_source));
    }
    void add_unnest_literal(size_t p, MorselPtr lit, std::string target_name) {
        add_op_(p, std::make_unique<UnnestLiteralOperator>(std::move(lit),
                                                           std::move(target_name)));
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
        set_sink_(p, std::make_unique<SortSink>(std::move(spec), buffers[buf].get()));
    }
    void set_topn_sink(size_t p, std::vector<SortKeySpec> spec, size_t n, size_t buf) {
        set_sink_(p, std::make_unique<TopNSink>(std::move(spec), n, buffers[buf].get()));
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
        set_sink_(p, std::make_unique<WindowSink>(
            std::move(sort_spec), n_part, std::move(funcs), buffers[buf].get()));
    }
    void add_select(size_t p, std::vector<size_t> indices, std::vector<std::string> names) {
        add_op_(p,
            std::make_unique<ColumnSelectOperator>(std::move(indices), std::move(names)));
    }
    void set_queue_sink(size_t p, MorselQueue* q) {
        set_sink_(p, std::make_unique<QueueSink>(q));
        out_q = q;
    }
    void set_agg_sink(size_t p, std::vector<AggSpec2> specs, size_t buf) {
        set_sink_(p,
            std::make_unique<UngroupedAggSink>(std::move(specs), buffers[buf].get()));
    }
    void set_groupby_sink(size_t p, std::vector<size_t> key_idx,
                          std::vector<AggSpec2> specs, size_t buf) {
        set_sink_(p, std::make_unique<GroupBySink>(
            std::move(key_idx), std::move(specs), buffers[buf].get()));
    }
    void set_distinct_sink(size_t p, std::vector<size_t> on_idx, size_t buf) {
        set_sink_(p,
            std::make_unique<DistinctSink>(std::move(on_idx), buffers[buf].get()));
    }
    void set_buffer_append_sink(size_t p, size_t buf) {
        set_sink_(p, std::make_unique<BufferAppendSink>(buffers[buf].get()));
    }
    void set_final_schema(std::vector<std::string> names, std::vector<DrakenType> types,
                          std::vector<int> lt_kind, std::vector<int> lt_unit,
                          std::vector<int> lt_precision, std::vector<int> lt_scale,
                          std::vector<int> lt_dimension) {
        final_names = std::move(names);
        final_types = std::move(types);
        final_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision, lt_scale,
                                           lt_dimension);
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
                    const LogicalType* lt = i < final_logical.size() ? final_logical[i] : nullptr;
                    m->columns.push_back(make_empty_col(final_types[i], lt));
                }
                m->names = final_names;
                out_q->put(m);
            }
        }
    }
};

}  // namespace opteryx::engine
