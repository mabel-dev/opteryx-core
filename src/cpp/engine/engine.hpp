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
#include <unordered_set>
#include <vector>

#include "executor.hpp"
#include "morsel_queue.hpp"
#include "native_expression.hpp"    // ExprFilterOperator, ExprMultiProjectOperator
#include "native_group_sinks.hpp"   // UngroupedAggSink, GroupBySink, DistinctSink, AggSpec2
#include "native_key_hash.hpp"     // compute_row_hashes — the shared equi-key hash
#include "native_join2.hpp"         // Join2BuildSink/Probe — multi-key, semi/anti/outer
#include "native_parquet_scan_source.hpp"  // NativeParquetScanSource (zero-Python pull)
#include "native_latmat_scan_source.hpp"   // LatmatScanSource (R3 two-pass late-mat)
#include "native_skene_scan_source.hpp"    // NativeSkeneScanSource (zero-Python skene)
#include "native_skene_latmat_scan_source.hpp"  // NativeSkeneLatmatScanSource (two-pass skene)
#include "native_sort.hpp"          // SortSink, TopNSink, SortKeySpec, gather_rows
#include "native_unnest.hpp"        // UnnestOperator — CROSS JOIN UNNEST
#include "native_window_frame.hpp"  // FramedWindowSink — SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE ...)
#include "native_cidr_unnest.hpp"   // CidrUnnestOperator — CROSS JOIN CIDR_UNNEST
#include "native_grouping_expand.hpp"  // GroupingExpandOperator — GROUP BY ROLLUP
#include "pipeline_buffers.hpp"     // MorselBuffer, BufferSource
#include "runtime_bound.hpp"        // RuntimeKeyBound — runtime min/max join filter
#include "native_scalar_guard.hpp"  // ScalarGuardSource — scalar-subquery cardinality
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

// Decode one column's ARRAY element chain (see NativePlan.set_join2_build_sink's
// `elem_chain`): a flat list of SIX ints per nesting level —
// (physical type, logical kind, unit, precision, scale, dimension) — outermost
// element first, so ARRAY<VARCHAR> is one level and ARRAY<ARRAY<INT64>> is two.
// Empty for every non-ARRAY column, and empty for an ARRAY whose element type the
// planner could not resolve (make_empty_col then leaves the column childless and
// the consumers fail loud — see its comment).
inline std::vector<EmptyColElem> decode_elem_chain(const std::vector<int>& flat) {
    std::vector<EmptyColElem> out;
    out.reserve(flat.size() / 6);
    for (size_t i = 0; i + 6 <= flat.size(); i += 6) {
        const LogicalType* lt = nullptr;
        if (flat[i + 1] != 0) {
            LogicalType l;
            l.kind      = static_cast<LogicalKind>(flat[i + 1]);
            l.unit      = static_cast<TimestampUnit>(flat[i + 2]);
            l.precision = static_cast<uint8_t>(flat[i + 3]);
            l.scale     = static_cast<uint8_t>(flat[i + 4]);
            l.dimension = static_cast<uint32_t>(flat[i + 5]);
            lt = logical_type_intern(l);
        }
        out.push_back(EmptyColElem{static_cast<DrakenType>(flat[i]), lt});
    }
    return out;
}

inline std::vector<std::vector<EmptyColElem>> decode_elem_chains(
        const std::vector<std::vector<int>>& flat, size_t ncols) {
    std::vector<std::vector<EmptyColElem>> out(ncols);
    for (size_t i = 0; i < ncols && i < flat.size(); ++i)
        out[i] = decode_elem_chain(flat[i]);
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
    // Runtime min/max join filter: parallel to this build sink's key columns,
    // the runtime_bounds slot each key's observed ordinal range is published
    // into post-run, or -1. Empty for every pipeline that is not an eligible
    // join build. See docs/RUNTIME_MINMAX_FILTER_DESIGN.md.
    std::vector<int> fill_bound_slots;
    int reads_buffer = -1;    // buffers index this pipeline's BufferSource reads, so
                              // run() can free a buffer after its LAST consumer
    int dop_override = 0;     // >0 forces this pipeline's degree (order-sensitive
                              // consumers of a sorted buffer run at 1); 0 = engine dop
    std::atomic<bool> halt{false};   // set by LimitOperator when its quota is filled
    std::unique_ptr<GlobalSinkState> result;
    // Filled by run(). Pipelines execute strictly one at a time, so a PROCESS-wide CPU
    // clock read either side of run_pipeline() attributes all CPU burned in that window
    // to this pipeline — which is what makes `cpu_ns / wall_ns` the average number of
    // cores this pipeline actually kept busy. Per-operator OpStats cannot answer that:
    // they measure the operators that DID run, and the question here is how much of the
    // pool was doing nothing while they ran.
    uint64_t wall_ns = 0;
    uint64_t cpu_ns = 0;
    int dop_used = 0;
    // P3: barrier skew for this pipeline's run — how far apart its workers finished,
    // and what that spread cost in idle worker-time. Filled by run_pipeline().
    PipelineSkew skew{};
};

// Fixpoint loop over a contiguous pipeline span — the recursive-CTE primitive
// (docs/RECURSIVE_CTE_DESIGN.md). run() executes a control step on the driver
// thread each time execution reaches `first` (the same between-pipelines spot
// the runtime min/max filter publishes at — no concurrency): it promotes DELTA
// into RESULT and WORKING, or declares convergence and skips the span. The
// pipeline at `last` jumps back to `first` while the loop is active. The anchor
// term's pipelines sit BEFORE `first` and append into DELTA, so the anchor
// seeds the loop through the identical control path as every iteration.
struct LoopSpan {
    std::string name;        // the CTE's declared name (errors + telemetry)
    size_t first;            // first pipeline of the recursive term
    size_t last;             // last pipeline of the recursive term (its DELTA sink)
    size_t working_buf;      // scratch (never-spill): the frontier the term reads
    size_t delta_buf;        // scratch (never-spill): the term's output this pass
    size_t result_buf;       // accumulated CTE value; consumers read this
    bool   distinct = false; // UNION (true) / UNION ALL (false)
    uint32_t max_iterations = 0;
    bool   active = true;        // false once converged (delta came back empty)
    uint32_t iterations_run = 0; // telemetry: recursive passes executed
    std::string err_msg;         // owned storage for a ceiling error (ErrCtx::msg
                                 // must outlive run(); see ErrCtx contract)
    // UNION's persistent visited set: one 64-bit draken row hash (cxx_hash_c)
    // per emitted row, across ALL iterations — hash identity IS row equality
    // here, the same sanctioned contract native DISTINCT runs on (see
    // DistinctSink, native_group_sinks.hpp). Convergence on a cyclic input is
    // exactly this set refusing the rows it has already seen.
    std::unordered_set<uint64_t> visited;
};

class Engine {
public:
    std::vector<std::unique_ptr<PipelineNode>> pipelines;  // run in creation order
    std::vector<std::unique_ptr<MorselBuffer>> buffers;
    std::vector<LoopSpan> loops;   // v1: at most one (compiler-enforced)
    SpillEnv spill_env;   // shared spill config/store for every buffer this run
    std::vector<std::unique_ptr<Join2Ref>> join2_refs;
    // Runtime min/max join filter slots. unique_ptr because a probe Source holds
    // the ADDRESS of a slot from plan time until it builds its claim list, and
    // the vector grows as more joins are compiled. Written once each, by run(),
    // on the driver thread between two pipelines — never concurrently with any
    // reader.
    std::vector<std::unique_ptr<RuntimeKeyBound>> runtime_bounds;
    // R3 latmat: the plan-time vectors a LatmatScanSource borrows that have no
    // NativeScanPlan of their own (predicate column map, output-assembly maps, output
    // names). Owned here so they outlive the run; unique_ptr keeps the addresses
    // stable as the vectors grow.
    std::vector<std::unique_ptr<std::vector<int>>> latmat_owned_ints;
    std::vector<std::unique_ptr<std::vector<std::string>>> latmat_owned_names;
    // A skene scan's PUSHED predicate program. Same reason as the latmat vectors:
    // NativeSkeneScanSource borrows it and it has nowhere else to live (the
    // ExprProgram is a C++ struct, so the Cython-side SkeneScanPlan cannot hold
    // one). unique_ptr so the address stays stable as more scans are compiled.
    // The `instrs` it points at, and every literal DrakenVector*, are owned by the
    // CompiledBytecode the NativePlan holds — same contract as add_expr_filter.
    std::vector<std::unique_ptr<ExprProgram>> skene_scan_filters;
    // Shared, not raw: the engine writes to this during run() (see the drain at the
    // bottom of this class), and the queue's lifetime must not depend on Python
    // still holding its PyMorselQueue wrapper at that moment. See the ownership note
    // in native_queue_sink.hpp.
    std::shared_ptr<MorselQueue> out_q;
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
        // Breaker cost — see OpStats::combine_ns/finalize_ns. Zero except on sinks.
        uint64_t combine_ns, finalize_ns;
        // Degree of parallelism the pipeline this operator belonged to actually ran
        // at (`dop_used`, set in run()). Carried per-row rather than looked up from
        // collect_pipeline_stats because PipelineReading is keyed by DISPLAY NAME,
        // which is not unique — two scans of the same relation share a label, so a
        // by-name join back to a plan node is ambiguous. Read at teardown from a
        // field the run already set: no execution-path cost.
        int dop;
    };
    std::vector<OpReading> collect_op_stats() const {
        std::vector<OpReading> out;
        auto emit = [&out](const OpStats& s, const char* role, int dop) {
            out.push_back(OpReading{
                s.identity, role,
                s.calls.load(), s.rows_in.load(), s.rows_out.load(),
                s.bytes_in.load(), s.bytes_out.load(), s.exec_ns.load(),
                s.cpu_ns.load(), s.combine_ns.load(), s.finalize_ns.load(), dop});
        };
        for (const auto& pn : pipelines) {
            const int dop = pn->dop_used;
            if (pn->source) emit(pn->source->stats, "source", dop);
            for (const auto& op : pn->operators) emit(op->stats, "operator", dop);
            if (pn->sink) emit(pn->sink->stats, "sink", dop);
        }
        return out;
    }

    // Per-PIPELINE wall/CPU, one row per pipeline in creation (= execution) order.
    // `cpu_ns / wall_ns` is the mean cores busy while that pipeline ran; comparing it
    // against `dop` is how a phase that leaves the pool parked becomes visible, which
    // no per-operator reading can show.
    struct PipelineReading {
        std::string label;      // the pipeline's source display name (what it reads)
        uint64_t wall_ns, cpu_ns;
        int dop;
        // P3: how far apart the workers finished, and the worker-time that spread
        // burned waiting at the barrier. `cpu_ns/wall_ns` says how much of the pool
        // was busy; this says whether the idle half was ONE straggler.
        uint64_t skew_ns, barrier_idle_ns;
    };
    std::vector<PipelineReading> collect_pipeline_stats() const {
        std::vector<PipelineReading> out;
        out.reserve(pipelines.size());
        for (const auto& pn : pipelines) {
            std::string label;
            if (pn->source) {
                const OpStats& s = pn->source->stats;
                label = s.display_name.empty() ? s.identity : s.display_name;
            }
            if (label.empty() && pn->sink) {
                const OpStats& s = pn->sink->stats;
                label = s.display_name.empty() ? s.identity : s.display_name;
            }
            out.push_back(PipelineReading{label, pn->wall_ns, pn->cpu_ns, pn->dop_used,
                                          pn->skew.skew_ns, pn->skew.barrier_idle_ns});
        }
        return out;
    }

    // ---- Recursive fixpoint readings (per LoopSpan) -------------------------------
    // How many recursive passes each WITH RECURSIVE actually ran, and (UNION)
    // how many distinct rows its visited set held at the end. Read after run(),
    // alongside the other harvests — the spans live for the Engine's lifetime.
    struct LoopReading {
        std::string name;        // the CTE's declared name
        uint32_t iterations;     // recursive passes executed
        bool distinct;           // UNION (true) / UNION ALL (false)
        uint64_t visited_rows;   // visited-set size; 0 for UNION ALL
        uint32_t max_iterations; // the ceiling that applied
    };
    std::vector<LoopReading> collect_loop_stats() const {
        std::vector<LoopReading> out;
        out.reserve(loops.size());
        for (const auto& L : loops)
            out.push_back(LoopReading{L.name, L.iterations_run, L.distinct,
                                      static_cast<uint64_t>(L.visited.size()),
                                      L.max_iterations});
        return out;
    }

    // ---- Join2 build-side consolidation decisions (per build sink) ---------------
    // The build sink decides ONCE, at finalize, whether to consolidate its retained
    // payload into one block — which is what decides whether the probe emits the
    // build half as codes over that block or falls back to the dense per-row gather
    // (8-13x on a string-carrying payload). The decision is invisible in the plan:
    // it turns on a cardinality ESTIMATE, so two runs of a byte-identical plan can
    // differ by 4x in wall clock with nothing to show for it. This is that seam.
    //
    // Read after run() returns, alongside the scan diagnostics harvest — the sinks
    // live for the Engine's lifetime, so there is no teardown ordering to respect.
    struct Join2BuildReading {
        std::string identity;      // plan-node identity (the correlation key)
        std::string display_name;  // human-readable plan-node kind
        std::string outcome;       // consolidate_outcome_name — a stable wire spelling
        bool consolidated;         // did the probe actually get the dict path?
        int64_t est_rows;          // the estimate the decision was made ON (-1 unknown)
        uint64_t build_rows;       // the ACTUAL build rows
        uint64_t morsels;          // retained build morsels
        uint64_t block_bytes;      // measured payload bytes (0 = never measured)
        double dense_bpr, code_bpr;  // bytes/output row either way (0 = never measured)
    };
    std::vector<Join2BuildReading> collect_join2_build_stats() const {
        std::vector<Join2BuildReading> out;
        for (const auto& pn : pipelines) {
            if (!pn->sink) continue;
            const auto* bs = dynamic_cast<const Join2BuildSink*>(pn->sink.get());
            if (bs == nullptr) continue;
            out.push_back(Join2BuildReading{
                bs->stats.identity, bs->stats.display_name,
                consolidate_outcome_name(bs->consolidate_outcome),
                bs->consolidate_outcome == ConsolidateOutcome::Consolidated,
                bs->consolidate_est_rows, bs->consolidate_build_rows,
                bs->consolidate_morsels, bs->consolidate_block_bytes,
                bs->consolidate_dense_bpr, bs->consolidate_code_bpr});
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
        buffers.back()->configure(&spill_env);
        return buffers.size() - 1;
    }
    size_t pipeline_count() const { return pipelines.size(); }
    // Scratch buffer for a LoopSpan's WORKING/DELTA: deliberately NOT configured
    // for spill, so reset_with()/take_resident() (driver-thread swap primitives)
    // are always legal on it. It holds one frontier, not the accumulated result.
    size_t new_scratch_buffer() {
        buffers.push_back(std::make_unique<MorselBuffer>());
        return buffers.size() - 1;
    }
    // Register the fixpoint span [first, last] (inclusive). v1: one loop per
    // engine — the compiler rejects nested/multiple recursion at bind time, and
    // this guards the invariant behind it.
    void add_loop_span(size_t first, size_t last, size_t working, size_t delta,
                       size_t result, bool distinct, uint32_t max_iterations,
                       std::string name) {
        if (!loops.empty())
            throw std::runtime_error("engine supports one loop span per plan");
        if (first > last || last >= pipelines.size())
            throw std::runtime_error("loop span out of range");
        LoopSpan L;
        L.name = std::move(name);
        L.first = first;
        L.last = last;
        L.working_buf = working;
        L.delta_buf = delta;
        L.result_buf = result;
        L.distinct = distinct;
        L.max_iterations = max_iterations;
        loops.push_back(std::move(L));
    }
    // Spill root for this run's buffers (docs/MORSEL_SPILL_DESIGN.md). Set at
    // plan build, before any pipeline runs; empty = spill not configured and
    // buffered accumulation is unbounded, exactly the pre-spill engine.
    void set_spill_root(std::string root) { spill_env.root = std::move(root); }
    // Labels the buffer with the sink's display name so a spill failure names
    // the operator that overspent, then hands it to the sink.
    MorselBuffer* sink_buffer_(size_t buf) {
        buffers[buf]->set_label(current_display_name_);
        return buffers[buf].get();
    }
    size_t new_join2_ref() {
        join2_refs.push_back(std::make_unique<Join2Ref>());
        return join2_refs.size() - 1;
    }
    // A fresh, UNFILLED runtime bound slot (valid == 0 -> prunes nothing). The
    // compiler allocates one per (join, eligible key column) pair and hands the
    // index to both the build sink (which fills it) and the probe scan (which
    // reads it).
    size_t new_runtime_bound() {
        runtime_bounds.push_back(std::make_unique<RuntimeKeyBound>());
        return runtime_bounds.size() - 1;
    }
    // Wire an already-created skene Source to a bound slot. Called AFTER
    // set_native_skene_scan_source for the same pipeline: the compiler compiles
    // the probe leg (creating the scan) before it knows the join is eligible.
    // Fails loud rather than silently declining — reaching here with a
    // non-skene source means the compiler's own eligibility test and the plan
    // disagree, which is a bug, not a shape to tolerate.
    void add_skene_runtime_bound(size_t p, size_t bound_idx, std::string column) {
        auto* src = dynamic_cast<NativeSkeneScanSource*>(pipelines[p]->source.get());
        if (src == nullptr)
            throw std::runtime_error(
                "add_skene_runtime_bound: pipeline source is not a skene scan");
        src->add_runtime_bound(std::move(column), runtime_bounds[bound_idx].get());
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
                              std::vector<int> lt_dimension,
                              std::vector<std::vector<int>> elem_chain,
                              bool track_matches = false,
                              int64_t est_output_rows = -1,
                              bool null_equal = false) {
        auto payload_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                                   lt_scale, lt_dimension);
        auto payload_element = decode_elem_chains(elem_chain, payload_types.size());
        set_sink_(p, std::make_unique<Join2BuildSink>(
            std::move(key_idx), std::move(payload_idx),
            std::move(payload_types), std::move(payload_logical),
            std::move(payload_element),
            /*asof=*/-1, /*asof_type=*/0, track_matches, null_equal,
            est_output_rows));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    // RUNTIME MIN/MAX JOIN FILTER: arm bound capture on an ALREADY-CREATED build
    // sink. Separate from set_join2_build_sink because the compiler cannot know
    // yet whether the bound is usable: eligibility depends on the PROBE leg, and
    // the probe leg is compiled after the build sink is wired (it has to be —
    // compile order is run order, and the build must run first).
    //
    // `slots` is parallel to the sink's key_idx; the sink indexes both with the
    // same subscript, so a mismatch would capture one key column's values into
    // another's bound and prune the probe on them. That fails loud here rather
    // than producing a plausible wrong answer.
    void set_join2_bound_slots(size_t p, std::vector<int> slots) {
        auto* sink = dynamic_cast<Join2BuildSink*>(pipelines[p]->sink.get());
        if (sink == nullptr)
            throw std::runtime_error(
                "set_join2_bound_slots: pipeline sink is not a join build sink");
        if (slots.size() != sink->key_idx.size())
            throw std::runtime_error(
                "set_join2_bound_slots: slots must be parallel to the sink's key_idx");
        pipelines[p]->fill_bound_slots = slots;
        sink->bound_slots = std::move(slots);
    }
    // `emit_prune`/`emit_cols` apply to SEMI/ANTI modes only — the probe columns an
    // existence filter still emits, which is normally not its own probe key. Every
    // other mode emits `payload_idx` and ignores these. emit_prune=false means
    // "unknown, emit everything"; an EMPTY emit_cols with emit_prune=true means
    // "emit no columns" (a real plan — `COUNT(*) ... WHERE x IN (...)`).
    void add_join2_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                         std::vector<size_t> payload_idx, int mode,
                         bool emit_prune, std::vector<uint32_t> emit_cols,
                         bool emit_existence = false,
                         bool existence_three_valued = false,
                         std::string existence_name = {}) {
        add_op_(p, std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            static_cast<JoinMode>(mode), -1, 0, ExprProgram(), nullptr,
            emit_prune, std::move(emit_cols), emit_existence, existence_three_valued,
            std::move(existence_name)));
    }
    // FULL OUTER tail pipeline source (see UnmatchedBuildSource): emits the build
    // rows no probe matched, NULL-padded on the probe half. probe_types/lt_* are
    // the PROBE payload columns' plan-known types — the mirror of the build
    // sink's payload_types, for the same zero-rows-streamed reason.
    void set_unmatched_build_source(size_t p, size_t ref,
                                    std::vector<DrakenType> probe_types,
                                    std::vector<int> lt_kind, std::vector<int> lt_unit,
                                    std::vector<int> lt_precision,
                                    std::vector<int> lt_scale,
                                    std::vector<int> lt_dimension,
                                    std::vector<std::vector<int>> elem_chain) {
        auto probe_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                                lt_scale, lt_dimension);
        auto probe_element = decode_elem_chains(elem_chain, probe_types.size());
        auto schema = std::make_shared<CxxMorsel>();
        schema->columns.reserve(probe_types.size());
        for (size_t c = 0; c < probe_types.size(); ++c)
            schema->columns.push_back(make_empty_col(probe_types[c], probe_logical[c],
                                                     probe_element[c]));
        schema->names.resize(probe_types.size());
        schema->zero_col_rows = 0;
        set_source_(p, std::make_unique<UnmatchedBuildSource>(
            join2_refs[ref].get(), std::move(schema)));
    }
    // RIGHT SEMI / RIGHT ANTI, half one: consume the STREAMED leg and mark the build
    // rows it hits. Emits nothing — see Join2MarkSink. `key_idx` are the streamed
    // leg's join keys; `payload_idx` is the streamed leg's payload in the PAIR layout
    // the residual is lowered against (build payload, then streamed payload), exactly
    // as add_join2_probe_residual uses it. A null `instrs`/`fn` is the no-residual
    // case, where a key match alone proves existence.
    void set_join2_mark_sink(size_t p, size_t ref, std::vector<size_t> key_idx,
                             std::vector<size_t> payload_idx,
                             void* instrs, int count, std::vector<int> col_idx,
                             std::vector<void*> lit_dv, ExprEvalFn fn) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        set_sink_(p, std::make_unique<Join2MarkSink>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            std::move(prog), fn));
        // ⛔ Deliberately NO fill_join2_ref here. Only the BUILD pipeline may fill the
        // ref: run() points the ref's global at the filling pipeline's sink result,
        // and this sink's result is an empty base GlobalSinkState — filling from it
        // re-points the ref AWAY from the build table between the mark pass and
        // SemiAntiBuildSource reading the match flags, which emits zero rows for
        // every swapped SEMI/ANTI, silently.
    }
    // RIGHT SEMI / RIGHT ANTI, half two: emit the build rows whose match flag has the
    // requested polarity — `emit_matched` true for SEMI, false for ANTI. Unlike
    // set_unmatched_build_source there is no probe half to type: an existence filter
    // emits its own leg unchanged, so the only columns are the build payload the sink
    // already retained.
    void set_semi_anti_build_source(size_t p, size_t ref, bool emit_matched) {
        set_source_(p, std::make_unique<SemiAntiBuildSource>(
            join2_refs[ref].get(), emit_matched));
    }
    // SEMI/ANTI with a correlated NON-equality residual (TPC-H Q21's
    // `l2.l_suppkey <> l1.l_suppkey`). The residual is evaluated per candidate
    // (build,probe) pair INSIDE the existence test — see SemiAntiProbeOperator — so
    // it needs the build payload the plain SEMI/ANTI path never retains. Column
    // indices are resolved against the pair layout: build payload, then probe payload.
    // `payload_idx` here is the PAIR layout the residual is lowered against and stays
    // full width; `emit_prune`/`emit_cols` are the separate question of what the
    // existence filter emits. See SemiAntiProbeOperator.
    void add_join2_probe_residual(size_t p, size_t ref, std::vector<size_t> key_idx,
                                  std::vector<size_t> payload_idx, int mode,
                                  void* instrs, int count, std::vector<int> col_idx,
                                  std::vector<void*> lit_dv, ExprEvalFn fn,
                                  bool emit_prune, std::vector<uint32_t> emit_cols,
                                  bool emit_existence = false,
                                  bool existence_three_valued = false,
                                  std::string existence_name = {}) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        add_op_(p, std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            static_cast<JoinMode>(mode), -1, 0, std::move(prog), fn,
            emit_prune, std::move(emit_cols), emit_existence, existence_three_valued,
            std::move(existence_name)));
    }
    // ASOF: build side = Join2BuildSink capturing the asof column's order key;
    // probe side = nearest-match per MATCH_CONDITION op (0 GtEq / 1 Gt / 2 LtEq / 3 Lt).
    void set_asof_build_sink(size_t p, std::vector<size_t> key_idx,
                             std::vector<size_t> payload_idx, size_t asof_idx,
                             size_t ref, std::vector<DrakenType> payload_types,
                             std::vector<int> lt_kind, std::vector<int> lt_unit,
                             std::vector<int> lt_precision, std::vector<int> lt_scale,
                             std::vector<int> lt_dimension,
                             std::vector<std::vector<int>> elem_chain,
                             int asof_type, int64_t est_output_rows = -1) {
        auto payload_logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                                   lt_scale, lt_dimension);
        auto payload_element = decode_elem_chains(elem_chain, payload_types.size());
        set_sink_(p, std::make_unique<Join2BuildSink>(
            std::move(key_idx), std::move(payload_idx),
            std::move(payload_types), std::move(payload_logical),
            std::move(payload_element),
            static_cast<int>(asof_idx), asof_type, /*track=*/false,
            /*null_eq=*/false, est_output_rows));
        pipelines[p]->fill_join2_ref = static_cast<int>(ref);
    }
    void add_asof_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                        std::vector<size_t> payload_idx, size_t asof_idx, int op) {
        add_op_(p, std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            JoinMode::LeftOuter, static_cast<int>(asof_idx), op));
    }
    // BAND: build side reuses set_asof_build_sink verbatim — the band's ORDER key is
    // captured and sorted per equi group by exactly the machinery ASOF already needs,
    // so there is no band build sink. Only the probe differs: two bounds instead of
    // one, and a whole contiguous run emitted instead of a single nearest row.
    void add_band_probe(size_t p, size_t ref, std::vector<size_t> key_idx,
                        std::vector<size_t> payload_idx, size_t lo_idx, size_t hi_idx,
                        bool lower_closed, bool upper_closed) {
        auto op = std::make_unique<DeferredJoin2Probe>(
            std::move(key_idx), std::move(payload_idx), join2_refs[ref].get(),
            JoinMode::Inner);
        op->band_lo_idx = static_cast<int>(lo_idx);
        op->band_hi_idx = static_cast<int>(hi_idx);
        op->band_lower_closed = lower_closed;
        op->band_upper_closed = upper_closed;
        add_op_(p, std::move(op));
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
    // Skene scan: workers claim files from one atomic counter and decode them
    // independently (skene::read_morsel is a pure function over a buffer), so
    // there is no pipeline, no in-flight window and no footer map to carry —
    // just the file list, the projected in-file names, the identities to emit
    // them under, the bound physical type per projected column, and the
    // timestamp unit for any column the plan declares TIMESTAMP64 (-1 for the
    // rest) so the Source can honour a scan-declared INT64→TIMESTAMP64 retag.
    // Every pointer is borrowed from the NativePlan, which holds the owners alive.
    // `emit_indices` = the projection's positions in the read set (which is the
    // projection plus any predicate-only column). `instrs`/`count`/`col_idx`/
    // `lit_dv` are the PUSHED predicate, resolved at plan time against that same
    // read set and run inside the Source's decode workers — the Filter node it came
    // from was consumed by the pushdown strategy, so nothing downstream re-applies
    // it. `instrs == nullptr` means nothing was pushed.
    void set_native_skene_scan_source(size_t p,
                                      const std::vector<std::string>* files,
                                      const std::vector<std::string>* column_names,
                                      const std::vector<std::string>* out_identities,
                                      const std::vector<int>* column_types,
                                      const std::vector<int>* retag_units,
                                      const std::vector<int>* emit_indices,
                                      const std::vector<int>* length_only,
                                      void* instrs, int count,
                                      std::vector<int> col_idx,
                                      std::vector<void*> lit_dv,
                                      ExprFilterFn fn,
                                      const std::vector<std::string>* zone_columns,
                                      const std::vector<int>* zone_ops,
                                      const std::vector<int64_t>* zone_ordinals,
                                      int64_t* row_groups_total,
                                      int64_t* row_groups_pruned,
                                      int64_t* row_groups_pruned_runtime = nullptr,
                                      int64_t* bytes_claimed = nullptr) {
        ExprProgram* program = nullptr;
        if (instrs != nullptr) {
            skene_scan_filters.push_back(std::make_unique<ExprProgram>());
            program = skene_scan_filters.back().get();
            program->instrs = instrs;
            program->count = count;
            program->col_idx = std::move(col_idx);
            program->lit_dv = std::move(lit_dv);
        }
        SkeneZoneMap zone;
        zone.columns = zone_columns;
        zone.ops = zone_ops;
        zone.ordinals = zone_ordinals;
        set_source_(p, std::make_unique<NativeSkeneScanSource>(
                           files, column_names, out_identities, column_types,
                           retag_units, emit_indices, length_only,
                           program != nullptr ? fn : nullptr, program, zone,
                           row_groups_total, row_groups_pruned,
                           row_groups_pruned_runtime, bytes_claimed));
    }

    // The two-pass late-materialization skene scan: pass 1 decodes only the
    // predicate columns + the sort key over every file and reduces the survivors to
    // the top-n boundary; pass 2 decodes the full projection for just the files that
    // still hold a candidate. See native_skene_latmat_scan_source.hpp for the
    // algorithm, for why the reduction reuses draken's own sort comparator, and for
    // why the top-n reduction is safe on its own now that the Filter node above the
    // scan is gone (can_push accepts for skene since 2026-08-21).
    //
    // `zone_*` are the same ROW-GROUP zone terms the single-pass Source takes. Pass 1
    // sweeps every row group, so an excluded one is never opened by EITHER pass —
    // pass 2 only revisits row groups pass 1 kept.
    // Every pointer is borrowed from the NativePlan, which holds the owners alive.
    void set_skene_latmat_scan_source(size_t p,
                                      const std::vector<std::string>* files,
                                      const std::vector<std::string>* p1_column_names,
                                      const std::vector<int>* p1_column_types,
                                      const std::vector<int>* p1_retag_units,
                                      const std::vector<int>* p1_length_only,
                                      const std::vector<std::string>* out_column_names,
                                      const std::vector<std::string>* out_identities,
                                      const std::vector<int>* out_column_types,
                                      const std::vector<int>* out_retag_units,
                                      const std::vector<int>* out_length_only,
                                      void* pred_fn, void* pred_ctx,
                                      const std::vector<int>* pred_col_to_p1,
                                      int sort_p1_index, bool sort_ascending,
                                      int64_t topn_limit,
                                      const std::vector<std::string>* zone_columns,
                                      const std::vector<int>* zone_ops,
                                      const std::vector<int64_t>* zone_ordinals,
                                      int64_t* row_groups_total,
                                      int64_t* row_groups_pruned,
                                      int64_t* bytes_claimed = nullptr) {
        SkeneZoneMap zone;
        zone.columns = zone_columns;
        zone.ops = zone_ops;
        zone.ordinals = zone_ordinals;
        set_source_(p, std::make_unique<NativeSkeneLatmatScanSource>(
                           files, p1_column_names, p1_column_types, p1_retag_units,
                           p1_length_only, out_column_names, out_identities,
                           out_column_types, out_retag_units, out_length_only,
                           reinterpret_cast<SkeneLatmatPredFn>(pred_fn), pred_ctx,
                           pred_col_to_p1, sort_p1_index, sort_ascending, topn_limit,
                           zone, row_groups_total, row_groups_pruned,
                           bytes_claimed));
    }

    void set_native_scan_source(size_t p, rugo::ParquetIOPipeline* pipeline,
                                const std::unordered_map<std::string, FileStats>* footer_map,
                                const std::vector<std::pair<std::string, int>>* work_items,
                                const std::vector<std::string>* column_names,
                                int in_flight_limit,
                                MemoryPool* pool = nullptr,
                                const std::vector<int>* string_types = nullptr,
                                const std::vector<uint8_t>* decimal_columns = nullptr,
                                const std::vector<int>* logical_coerce = nullptr,
                                const std::vector<uint8_t>* hash_key_columns = nullptr,
                                const std::vector<uint8_t>* array_columns = nullptr,
                                int64_t row_limit = -1) {
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
            hash_key_columns, array_columns, row_limit));
        if (pipeline != nullptr) pipeline->set_trace_node_id(node_id);
    }
    // R3 (`fused_topn`): the two-pass late-materialization scan Source. Everything the
    // Source borrows lives either on the caller's two NativeScanPlans (the pipelines,
    // footer map, work items, per-column decode flags) or in `latmat_owned_*` below —
    // the small plan-time vectors that have no NativeScanPlan to live on. Both outlive
    // the run. See native_latmat_scan_source.hpp for the algorithm and for why the
    // top-n reduction reuses draken's own sort comparator.
    void set_latmat_scan_source(
            size_t p,
            rugo::ParquetIOPipeline* p1_pipeline,
            const std::unordered_map<std::string, FileStats>* footer_map,
            const std::vector<std::pair<std::string, int>>* work_items,
            const std::vector<std::string>* p1_column_names,
            int in_flight_limit,
            MemoryPool* p1_pool,
            const std::vector<int>* p1_string_types,
            const std::vector<uint8_t>* p1_decimal_columns,
            const std::vector<int>* p1_logical_coerce,
            const std::vector<uint8_t>* p1_hash_key_columns,
            const std::vector<uint8_t>* p1_array_columns,
            rugo::ParquetIOPipeline* p2_pipeline,
            const std::vector<std::string>* p2_column_names,
            MemoryPool* p2_pool,
            const std::vector<int>* p2_string_types,
            const std::vector<uint8_t>* p2_decimal_columns,
            const std::vector<int>* p2_logical_coerce,
            const std::vector<uint8_t>* p2_hash_key_columns,
            const std::vector<uint8_t>* p2_array_columns,
            void* pred_fn, void* pred_ctx, std::vector<int> pred_col_to_p1,
            int sort_p1_index, bool sort_ascending, int64_t topn_limit,
            std::vector<int> out_from_p1, std::vector<int> out_from_p2,
            std::vector<std::string> out_names) {
        latmat_owned_ints.push_back(
            std::make_unique<std::vector<int>>(std::move(pred_col_to_p1)));
        auto* pred_map = latmat_owned_ints.back().get();
        latmat_owned_ints.push_back(
            std::make_unique<std::vector<int>>(std::move(out_from_p1)));
        auto* from_p1 = latmat_owned_ints.back().get();
        latmat_owned_ints.push_back(
            std::make_unique<std::vector<int>>(std::move(out_from_p2)));
        auto* from_p2 = latmat_owned_ints.back().get();
        latmat_owned_names.push_back(
            std::make_unique<std::vector<std::string>>(std::move(out_names)));
        auto* names = latmat_owned_names.back().get();

        auto src = std::make_unique<LatmatScanSource>();
        src->p1_pipeline = p1_pipeline;
        src->footer_map = footer_map;
        src->work_items = work_items;
        src->p1_column_names = p1_column_names;
        src->in_flight_limit = in_flight_limit;
        src->p1_build.pool = p1_pool;
        src->p1_build.string_types = p1_string_types;
        src->p1_build.decimal_columns = p1_decimal_columns;
        src->p1_build.logical_coerce = p1_logical_coerce;
        src->p1_build.hash_key_columns = p1_hash_key_columns;
        src->p1_build.array_columns = p1_array_columns;
        src->p2_pipeline = p2_pipeline;
        src->p2_column_names = p2_column_names;
        src->p2_build.pool = p2_pool;
        src->p2_build.string_types = p2_string_types;
        src->p2_build.decimal_columns = p2_decimal_columns;
        src->p2_build.logical_coerce = p2_logical_coerce;
        src->p2_build.hash_key_columns = p2_hash_key_columns;
        src->p2_build.array_columns = p2_array_columns;
        src->pred_fn = reinterpret_cast<LatmatPredFn>(pred_fn);
        src->pred_ctx = pred_ctx;
        src->pred_col_to_p1 = pred_map;
        src->sort_p1_index = sort_p1_index;
        src->sort_ascending = sort_ascending;
        src->topn_limit = topn_limit;
        src->out_from_p1 = from_p1;
        src->out_from_p2 = from_p2;
        src->out_names = names;
        uint32_t node_id = set_source_(p, std::move(src));
        // Both passes' IO spans attribute back to this one scan plan node.
        if (p1_pipeline != nullptr) p1_pipeline->set_trace_node_id(node_id);
        if (p2_pipeline != nullptr) p2_pipeline->set_trace_node_id(node_id);
    }
    void set_buffer_source(size_t p, size_t buf) {
        set_source_(p, std::make_unique<BufferSource>(sink_buffer_(buf)));
        pipelines[p]->reads_buffer = static_cast<int>(buf);
    }
    // Scalar-subquery cardinality guard over a finalized buffer (see
    // native_scalar_guard.hpp). `names` are the leg's column identities and
    // types/lt_*/elem_chain its plan-known column types — the same plumbing as
    // set_unmatched_build_source, for the same zero-rows-streamed reason: the
    // all-NULL row a zero-row subquery must yield is gathered against these,
    // so the engine never has to learn a type from data that never arrived.
    void set_scalar_guard_source(size_t p, size_t buf,
                                 std::vector<std::string> names,
                                 std::vector<DrakenType> types,
                                 std::vector<int> lt_kind, std::vector<int> lt_unit,
                                 std::vector<int> lt_precision,
                                 std::vector<int> lt_scale,
                                 std::vector<int> lt_dimension,
                                 std::vector<std::vector<int>> elem_chain) {
        auto logical = intern_logical_vec(lt_kind, lt_unit, lt_precision,
                                          lt_scale, lt_dimension);
        auto element = decode_elem_chains(elem_chain, types.size());
        auto schema = std::make_shared<CxxMorsel>();
        schema->columns.reserve(types.size());
        for (size_t c = 0; c < types.size(); ++c)
            schema->columns.push_back(make_empty_col(types[c], logical[c], element[c]));
        schema->names = std::move(names);
        schema->zero_col_rows = 0;
        set_source_(p, std::make_unique<ScalarGuardSource>(sink_buffer_(buf),
                                                           std::move(schema)));
        pipelines[p]->reads_buffer = static_cast<int>(buf);
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
    // N `->`/`->>` extractions on ONE source column, sharing one parse per row.
    // `ctxs` are extraction_ctx blocks owned by the NativePlan (held for the run).
    // Deliberately NOT fused into a neighbouring op the way add_expr_project fuses
    // into ExprMultiProjectOperator: this operator's whole point is that its outputs
    // come from a single kernel call, and later programs that load those outputs are
    // separate operators that must run after it.
    void add_json_extract_multi(size_t p, int src_col_idx,
                                std::vector<void*> ctxs,
                                std::vector<std::string> names) {
        add_op_(p, std::make_unique<JsonExtractMultiOperator>(
                       src_col_idx, std::move(ctxs), std::move(names)));
    }
    void add_limit(size_t p, int64_t offset, int64_t limit) {
        add_op_(p, std::make_unique<LimitOperator>(offset, limit, &pipelines[p]->halt));
    }
    void add_unnest(size_t p, uint32_t array_idx, std::string target_name,
                    bool drop_source) {
        add_op_(p, std::make_unique<UnnestOperator>(array_idx, std::move(target_name),
                                                    drop_source));
    }
    // GROUP BY ROLLUP(...): replicate each morsel once per grouping set, masking the
    // keys that set does not name and appending the grouping_id key. Sits directly
    // below an ordinary GROUP BY sink — see native_grouping_expand.hpp.
    void add_grouping_expand(size_t p, std::vector<size_t> key_idx,
                             std::vector<uint64_t> set_masks, std::string id_name) {
        add_op_(p, std::make_unique<GroupingExpandOperator>(
                       std::move(key_idx), std::move(set_masks), std::move(id_name)));
    }
    // GROUPING(col): 0/1 per output row, read back from the sink's emitted
    // grouping_id key via a plan-time ordinal->bit lookup table — see
    // native_grouping_expand.hpp::GroupingBitOperator.
    void add_grouping_bit(size_t p, size_t grouping_id_idx,
                          std::vector<uint8_t> bit_by_ordinal, std::string out_name) {
        add_op_(p, std::make_unique<GroupingBitOperator>(
                       grouping_id_idx, std::move(bit_by_ordinal), std::move(out_name)));
    }
    // CROSS JOIN UNNEST with a WHERE on the unnested column folded in. The program
    // is bool-final and resolved against a ONE-COLUMN layout holding the target: it
    // runs over the array's child (element) vector before expansion, so the rows it
    // rejects are never built. See UnnestOperator::build_child_mask.
    // `instrs == nullptr` means no pushed WHERE (a DISTINCT-only fold); `distinct`
    // arms the per-worker pre-reduction, which NEVER replaces the DistinctSink.
    void add_unnest_filtered(size_t p, uint32_t array_idx, std::string target_name,
                             bool drop_source, void* instrs, int count,
                             std::vector<int> col_idx, std::vector<void*> lit_dv,
                             ExprEvalFn fn, bool distinct) {
        ExprProgram prog;
        prog.instrs = instrs;
        prog.count = count;
        prog.col_idx = std::move(col_idx);
        prog.lit_dv = std::move(lit_dv);
        add_op_(p, std::make_unique<UnnestOperator>(
                       array_idx, std::move(target_name), drop_source,
                       std::move(prog), instrs != nullptr ? fn : nullptr, distinct));
    }
    // CROSS JOIN CIDR_UNNEST. Unlike add_unnest, this operator is RESUMABLE: one
    // input morsel can expand to billions of rows, so it emits bounded batches
    // and the executor re-drives it (HAVE_MORE) until the input is consumed.
    void add_cidr_unnest(size_t p, uint32_t cidr_idx, std::string target_name,
                         bool drop_source) {
        add_op_(p, std::make_unique<CidrUnnestOperator>(cidr_idx, std::move(target_name),
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
        if (!buffers[buf]->append(m))
            throw std::runtime_error(buffers[buf]->error());
    }
    void set_pipeline_dop(size_t p, int dop) {
        pipelines[p]->dop_override = dop;
    }
    // `emit_prune`/`emit_cols`: the compiler's EMIT set for the sort — the input
    // column positions still wanted above it, which is never the ORDER BY key unless
    // something above also selects it. emit_prune=false means "unknown, keep every
    // column"; an EMPTY emit_cols with emit_prune=true means "emit no columns", which
    // is a real plan (COUNT(*) over an ordered subquery), not a mistake.
    void set_sort_sink(size_t p, std::vector<SortKeySpec> spec, size_t buf,
                       bool emit_prune, std::vector<uint32_t> emit_cols) {
        set_sink_(p, std::make_unique<SortSink>(std::move(spec), sink_buffer_(buf),
                                                131072, emit_prune,
                                                std::move(emit_cols)));
    }
    void set_topn_sink(size_t p, std::vector<SortKeySpec> spec, size_t n, size_t buf,
                       bool emit_prune, std::vector<uint32_t> emit_cols) {
        set_sink_(p, std::make_unique<TopNSink>(std::move(spec), n, sink_buffer_(buf),
                                                emit_prune, std::move(emit_cols)));
    }
    // Window functions: sort_spec = [partition keys asc..., order keys...]; n_part =
    // count of leading partition keys; fn_kinds[i] / fn_names[i] / fn_args[i] /
    // fn_offsets[i] are parallel. fn_args[i] = the input column a navigation
    // function (LAG/LEAD) reads its value from, -1 for the ranking functions;
    // fn_offsets[i] = the navigation row offset (0 for ranking). top_k =
    // WindowTopKFusionStrategy's fused `rank <= K` hint, or -1 if none.
    void set_window_sink(size_t p, std::vector<SortKeySpec> sort_spec, size_t n_part,
                         std::vector<int> fn_kinds, std::vector<std::string> fn_names,
                         std::vector<int> fn_args, std::vector<long long> fn_offsets,
                         int64_t top_k, size_t buf,
                         bool emit_prune, std::vector<uint32_t> emit_cols) {
        std::vector<WindowFnSpec> funcs;
        funcs.reserve(fn_kinds.size());
        for (size_t i = 0; i < fn_kinds.size(); ++i)
            funcs.push_back({static_cast<WinFn>(fn_kinds[i]), fn_names[i],
                             fn_args[i], static_cast<int64_t>(fn_offsets[i])});
        set_sink_(p, std::make_unique<WindowSink>(
            std::move(sort_spec), n_part, std::move(funcs), sink_buffer_(buf), top_k,
            131072, emit_prune, std::move(emit_cols)));
    }
    // Streaming ROW_NUMBER top-K per partition (WindowTopKFusionStrategy) — no full
    // sort. See native_group_sinks.hpp's WindowTopKSink for the eligibility scope
    // the compiler enforces before routing here instead of set_window_sink.
    void set_window_topk_sink(size_t p, std::vector<size_t> part_idx, size_t order_idx,
                              bool ascending, size_t k, std::string out_name, size_t buf) {
        set_sink_(p, std::make_unique<WindowTopKSink>(
            std::move(part_idx), order_idx, ascending, k, std::move(out_name),
            sink_buffer_(buf)));
    }
    // Framed aggregate window functions: SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE
    // BETWEEN ...) — see native_window_frame.hpp. `sort_spec`/`n_part` are the same
    // shape as set_window_sink's. Per function (parallel vectors, one entry each):
    // fn_kinds = WinAggFn as int; fn_args = the argument column, -1 only for
    // COUNT(*); fn_out_type = the DrakenType to emit; fn_out_lt_* decompose the
    // output's LogicalType descriptor (kind==0 means "no logical type", mirroring
    // add_expr_project's convention) — only DECIMAL/DECIMAL128 passthrough and AVG's
    // decimal scale need it. fn_frame_* decompose each function's FrameSpec: units,
    // start/end bound kind (FrameBoundKind as int) and start/end offset (meaningful
    // only for Preceding/Following).
    void set_framed_window_sink(size_t p, std::vector<SortKeySpec> sort_spec, size_t n_part,
                                std::vector<int> fn_kinds, std::vector<std::string> fn_names,
                                std::vector<int> fn_args,
                                std::vector<int> fn_out_type,
                                std::vector<int> fn_out_lt_kind, std::vector<int> fn_out_lt_unit,
                                std::vector<int> fn_out_lt_precision,
                                std::vector<int> fn_out_lt_scale,
                                std::vector<int> fn_out_lt_dimension,
                                std::vector<int> fn_frame_units,
                                std::vector<int> fn_frame_start_kind,
                                std::vector<long long> fn_frame_start_offset,
                                std::vector<int> fn_frame_end_kind,
                                std::vector<long long> fn_frame_end_offset,
                                size_t buf, bool emit_prune, std::vector<uint32_t> emit_cols) {
        std::vector<FramedAggFnSpec> funcs;
        funcs.reserve(fn_kinds.size());
        for (size_t i = 0; i < fn_kinds.size(); ++i) {
            FramedAggFnSpec spec;
            spec.kind = static_cast<WinAggFn>(fn_kinds[i]);
            spec.name = fn_names[i];
            spec.arg_col = fn_args[i];
            spec.out_type = static_cast<DrakenType>(fn_out_type[i]);
            if (fn_out_lt_kind[i] != 0) {
                LogicalType lt;
                lt.kind = static_cast<LogicalKind>(fn_out_lt_kind[i]);
                lt.unit = static_cast<TimestampUnit>(fn_out_lt_unit[i]);
                lt.precision = static_cast<uint8_t>(fn_out_lt_precision[i]);
                lt.scale = static_cast<uint8_t>(fn_out_lt_scale[i]);
                lt.dimension = static_cast<uint32_t>(fn_out_lt_dimension[i]);
                spec.out_logical = logical_type_intern(lt);
            }
            spec.frame.units = static_cast<FrameUnits>(fn_frame_units[i]);
            spec.frame.start.kind = static_cast<FrameBoundKind>(fn_frame_start_kind[i]);
            spec.frame.start.offset = static_cast<int64_t>(fn_frame_start_offset[i]);
            spec.frame.end.kind = static_cast<FrameBoundKind>(fn_frame_end_kind[i]);
            spec.frame.end.offset = static_cast<int64_t>(fn_frame_end_offset[i]);
            funcs.push_back(std::move(spec));
        }
        set_sink_(p, std::make_unique<FramedWindowSink>(
            std::move(sort_spec), n_part, std::move(funcs), sink_buffer_(buf),
            131072, emit_prune, std::move(emit_cols)));
    }
    void add_select(size_t p, std::vector<size_t> indices, std::vector<std::string> names) {
        add_op_(p,
            std::make_unique<ColumnSelectOperator>(std::move(indices), std::move(names)));
    }
    void set_queue_sink(size_t p, std::shared_ptr<MorselQueue> q) {
        set_sink_(p, std::make_unique<QueueSink>(q));
        out_q = std::move(q);
    }
    void set_agg_sink(size_t p, std::vector<AggSpec2> specs, size_t buf) {
        set_sink_(p,
            std::make_unique<UngroupedAggSink>(std::move(specs), sink_buffer_(buf)));
    }
    // `key_emit` has one entry per key_idx entry: false = the key is hashed to
    // separate the groups but its values are never stored or emitted.
    void set_groupby_sink(size_t p, std::vector<size_t> key_idx,
                          std::vector<std::string> key_names,
                          std::vector<uint8_t> key_emit,
                          std::vector<AggSpec2> specs, size_t buf, int64_t ndv_estimate) {
        set_sink_(p, std::make_unique<GroupBySink>(
            std::move(key_idx), std::move(key_names), std::move(key_emit),
            std::move(specs), sink_buffer_(buf), ndv_estimate));
    }
    void set_distinct_sink(size_t p, std::vector<size_t> on_idx, size_t buf,
                           int64_t ndv_estimate) {
        set_sink_(p,
            std::make_unique<DistinctSink>(std::move(on_idx), sink_buffer_(buf),
                                           ndv_estimate));
    }
    void set_buffer_append_sink(size_t p, size_t buf) {
        set_sink_(p, std::make_unique<BufferAppendSink>(sink_buffer_(buf)));
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
    // The fixpoint control step (docs/RECURSIVE_CTE_DESIGN.md §2). Driver thread,
    // between pipelines — no concurrency. Promotes this pass's DELTA: rows are
    // appended to RESULT (shared_ptr splice, no copy; RESULT may spill) and the
    // same pile becomes the next WORKING frontier (semi-naive: the recursive
    // term only ever sees the previous delta). An empty DELTA is convergence.
    // Returns false only with an error latched in `err`.
    bool loop_control_(LoopSpan& L, ErrCtx& err) {
        MorselBuffer& delta = *buffers[L.delta_buf];
        MorselBuffer& working = *buffers[L.working_buf];
        MorselBuffer& result = *buffers[L.result_buf];
        std::vector<MorselPtr> pile = delta.take_resident();
        if (delta.failed()) {
            err.code = 1;
            err.msg = delta.error().c_str();
            return false;
        }
        if (L.distinct) {
            // UNION: keep only rows whose hash is new to the whole fixpoint.
            // The anchor's rows pass through here too (first entry), so the
            // anchor is deduplicated exactly as UNION requires.
            std::vector<MorselPtr> survivors;
            survivors.reserve(pile.size());
            for (auto& m : pile) {
                const uint32_t m_rows = m->num_rows();
                if (m_rows == 0) continue;
                const size_t ncols = m->columns.size();
                std::vector<int32_t> col_idxs(ncols);
                for (size_t c = 0; c < ncols; ++c) col_idxs[c] = static_cast<int32_t>(c);
                CxxMorsel* hashm =
                    cxx_hash_c(m.get(), col_idxs.data(), static_cast<uint32_t>(ncols));
                if (hashm == nullptr) {
                    err.code = 1;
                    err.msg = "recursive UNION: row-hash allocation failed";
                    return false;
                }
                const DrakenVector& hv = hashm->columns[0].view;
                const uint64_t* row_hashes = static_cast<const uint64_t*>(hv.data);
                const uint32_t* codes = hv.selection;   // never NULL (draken invariant)
                std::vector<uint32_t> keep;
                keep.reserve(m_rows);
                for (uint32_t i = 0; i < m_rows; ++i)
                    if (L.visited.insert(row_hashes[codes[i]]).second) keep.push_back(i);
                cxx_morsel_delete(hashm);
                if (keep.size() == m_rows) {
                    survivors.push_back(m);   // all novel — pass through, zero copy
                    continue;
                }
                if (keep.empty()) continue;
                std::vector<MorselPtr> one{m};
                std::vector<uint32_t> row_m(m_rows, 0), row_r(m_rows);
                for (uint32_t i = 0; i < m_rows; ++i) row_r[i] = i;
                MorselPtr filtered =
                    gather_rows(one, keep, 0, static_cast<uint32_t>(keep.size()),
                                row_m, row_r, m->names, err);
                if (err.code != 0) return false;
                survivors.push_back(std::move(filtered));
            }
            pile.swap(survivors);
        }
        size_t rows = 0;
        for (const auto& m : pile) rows += m->num_rows();
        if (rows == 0) {
            L.active = false;
            return true;
        }
        if (L.max_iterations != 0 && L.iterations_run >= L.max_iterations) {
            // User-facing (kErrCodeDataError): the complete, actionable text.
            L.err_msg = "recursive CTE '" + L.name + "' did not converge within " +
                        std::to_string(L.max_iterations) +
                        " iterations; a cycle under UNION ALL recurs forever — use "
                        "UNION, bound the recursion with a depth column, or raise "
                        "MAX_RECURSION_ITERATIONS";
            err.code = kErrCodeDataError;
            err.msg = L.err_msg.c_str();
            return false;
        }
        ++L.iterations_run;
        for (const auto& m : pile) {
            if (!result.append(m)) {
                err.code = 1;
                err.msg = result.error().c_str();
                return false;
            }
        }
        // take_resident() left DELTA open and empty, ready for the next pass;
        // reset_with() reopens WORKING (the pass's BufferSource sealed it).
        if (!working.reset_with(std::move(pile))) {
            err.code = 1;
            err.msg = working.error().c_str();
            return false;
        }
        return true;
    }

    void run(int dop, void* pool, ErrCtx& err) {
        // Free each buffer's morsels after its LAST consumer completes: pipelines
        // run strictly in creation order, so once the highest-indexed pipeline
        // sourcing a buffer is done, nothing can read it again. A shared CTE's
        // buffer holds the whole materialized result — releasing it here caps the
        // window it is held over, and morsels still referenced elsewhere (the
        // output queue) survive via their shared_ptr refcounts.
        std::vector<int> last_consumer(buffers.size(), -1);
        for (size_t i = 0; i < pipelines.size(); ++i) {
            const int b = pipelines[i]->reads_buffer;
            if (b >= 0) last_consumer[static_cast<size_t>(b)] = static_cast<int>(i);
        }
        // Loop-span liveness: a buffer whose last consumer lies INSIDE a span is
        // re-read on every pass, so the per-pipeline release below must skip it —
        // the whole span is ONE consumer for liveness purposes. The control step
        // releases these (plus WORKING/DELTA) when the loop converges.
        std::vector<uint8_t> loop_deferred(buffers.size(), 0);
        for (const auto& L : loops)
            for (size_t b = 0; b < last_consumer.size(); ++b)
                if (last_consumer[b] >= static_cast<int>(L.first) &&
                    last_consumer[b] <= static_cast<int>(L.last))
                    loop_deferred[b] = 1;

        size_t pipeline_index = 0;
        while (pipeline_index < pipelines.size()) {
            // Fixpoint control step: entered before EVERY pass over the span —
            // the first entry promotes the anchor's rows, later entries each
            // iteration's. On convergence the span (and its buffers) are done.
            for (auto& L : loops) {
                if (!L.active || pipeline_index != L.first) continue;
                if (!loop_control_(L, err)) return;
                if (!L.active) {
                    for (size_t b = 0; b < last_consumer.size(); ++b)
                        if (loop_deferred[b]) buffers[b]->release();
                    buffers[L.working_buf]->release();
                    buffers[L.delta_buf]->release();
                    pipeline_index = L.last + 1;
                }
                break;   // one loop per engine (add_loop_span enforces)
            }
            if (pipeline_index >= pipelines.size()) break;
            auto& pn = pipelines[pipeline_index];
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
            pn->dop_used = pdop;
            const uint64_t w0 = telem_now_ns();
            const uint64_t c0 = telem_process_cpu_now_ns();
            pn->result = run_pipeline(p, pdop, err, pool, &pn->skew);
            // Accumulate (not assign): a pipeline inside a LoopSpan runs once per
            // iteration and its reading should cover all of them. Identical for
            // the ordinary single-run pipeline, which starts from zero.
            pn->wall_ns += telem_now_ns() - w0;
            pn->cpu_ns += telem_process_cpu_now_ns() - c0;
            if (err.code != 0) return;
            if (pn->fill_join2_ref >= 0) {
                const auto* bg = static_cast<const Join2BuildGlobal*>(pn->result.get());
                join2_refs[static_cast<size_t>(pn->fill_join2_ref)]->g = bg;
                // RUNTIME MIN/MAX JOIN FILTER — the one publish point.
                //
                // This is where the whole feature's correctness contract lives:
                // the build pipeline has completed (run_pipeline above blocks
                // until every worker combined and finalize() ran) and the probe
                // pipeline's GlobalSourceState does not exist yet (it is
                // constructed by the NEXT run_pipeline call). So a bound is
                // always complete and always visible before the probe scan
                // enumerates its first row group — no timeout, no late arrival,
                // no partially-populated filter, and no atomics: this is the
                // driver thread, between two pipelines.
                for (size_t k = 0; k < pn->fill_bound_slots.size(); ++k) {
                    const int slot = pn->fill_bound_slots[k];
                    if (slot < 0) continue;
                    if (k >= bg->bound_any.size() || bg->bound_any[k] == 0) continue;
                    RuntimeKeyBound& out = *runtime_bounds[static_cast<size_t>(slot)];
                    out.lo = bg->bound_lo[k];
                    out.hi = bg->bound_hi[k];
                    out.valid = 1;
                }
            }
            for (size_t b = 0; b < last_consumer.size(); ++b) {
                if (last_consumer[b] == static_cast<int>(pipeline_index) &&
                    !loop_deferred[b]) {
                    buffers[b]->release();
                }
            }
            // End of an active span: jump back to its control step.
            bool jumped = false;
            for (const auto& L : loops) {
                if (L.active && pipeline_index == L.last) {
                    pipeline_index = L.first;
                    jumped = true;
                    break;
                }
            }
            if (!jumped) ++pipeline_index;
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
