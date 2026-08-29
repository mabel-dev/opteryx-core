#pragma once
// src/cpp/engine/executor.hpp — the morsel-driven parallel pipeline executor.
//
// Runs ONE pipeline at degree `dop`. dop worker threads each pull DISJOINT morsels from
// the source (dynamic assignment = load balance), push them through the operator chain,
// and sink into thread-LOCAL state (lock-free); each worker combines its local into the
// shared global once at the end; finalize runs once. Serial is just dop=1 — no separate
// path. No Python: the loop, operators and states are all C++.
//
// Two dispatch backends share one worker body (`run_worker`):
//   - std::thread per call (demo/test-only call sites — no Python pool to plumb through).
//   - the caller's persistent BSThreadPoolBridge (the real, non-demo cutover path).
// A fresh std::thread-per-run-pipeline-call fan-out of `dop` threads that each touch
// Python on first use was found to deadlock CPython 3.14t's free-threaded runtime at
// dop=8: several brand-new OS threads calling PyGILState_Ensure() for the first time
// concurrently can race inside CPython's own new_threadstate()/stop_the_world(), a
// deadlock in the interpreter's thread-attach path, not in this file's locking. A
// pool whose worker threads persist for the life of the query attaches each thread to
// Python at most once and reuses it across pipeline runs, instead of re-running that
// race on every single pipeline invocation.

#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "operator.hpp"
#include "trace.hpp"
#include "../bs_pool_bridge_c.h"

namespace opteryx::engine {

// Monotonic nanosecond clock for the per-operator telemetry. CLOCK_MONOTONIC is the
// same source the transitional Cython drive loop used; ~20ns per read, taken per morsel
// (not per row), so it is noise against a morsel's kernel work.
inline uint64_t telem_now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
         + static_cast<uint64_t>(ts.tv_nsec);
}

// Per-THREAD CPU-time clock: only advances while this thread is actually scheduled and
// running, not while blocked in a mutex/condvar wait (e.g. the scan's get_morsel() pull
// from the async decode pipeline). Read alongside telem_now_ns() at the same call sites
// to split "real work" (cpu_ns) from "elapsed, possibly-blocked" (exec_ns) per operator.
//
// READ-ORDER CONTRACT for the three brackets below (measured, docs/
// EXECUTION_PROFILING_IMPALA_GAP.md §6.3a). This clock costs ~107ns per read against
// telem_now_ns()'s ~26ns on Apple Silicon, so whichever bracket encloses it absorbs
// that cost as if it were work. The order is therefore fixed as:
//
//     c0 = telem_cpu_now_ns();   <- CPU clock OUTERMOST
//     t0 = telem_now_ns();       <- wall clock innermost
//     ...the call being measured...
//     t1 = telem_now_ns();
//     c1 = telem_cpu_now_ns();
//
// so the WALL bracket contains only the measured call. Reading them the other way
// round (the original order) put the 107ns CPU read inside the wall bracket and
// inflated exec_ns by ~120ns per call — measured a third of the reported time on a
// cheap operator. exec_ns is the published figure (EXPLAIN ANALYZE's time_ms/self_ms),
// so it is the one the ordering protects; cpu_ns absorbs the two ~26ns wall reads
// instead, which is the cheaper end of the trade. Total cost is unchanged (measured
// 226.3 -> 224.3 ns/stage, inside noise) — this buys accuracy, not speed.
//
// Do not reorder these reads, and do not put anything but the measured call between
// t0 and t1.
inline uint64_t telem_cpu_now_ns() {
    timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
         + static_cast<uint64_t>(ts.tv_nsec);
}

// Whole-PROCESS CPU clock: the sum of every thread's running time. Read either side of
// a pipeline (which run one at a time) it gives the CPU that pipeline burned, and
// cpu/wall is then the mean number of cores it kept busy. Distinct from
// telem_cpu_now_ns above, which is this THREAD only and cannot see an idle pool.
inline uint64_t telem_process_cpu_now_ns() {
    timespec ts;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull
         + static_cast<uint64_t>(ts.tv_nsec);
}

// rows * columns * 8 — the byte estimate the Python operator model reports (bytes_in/out).
inline uint64_t telem_nbytes(const MorselPtr& m) {
    if (!m) return 0;
    return static_cast<uint64_t>(m->num_rows()) * static_cast<uint64_t>(m->num_columns())
         * 8ull;
}

// Per-worker state, shared by both dispatch backends below.
struct WorkerCtx {
    Pipeline*           p;
    GlobalSourceState*  gsrc;
    GlobalSinkState*    gsink;
    std::vector<ErrCtx>* errs;
    int                 w;
    // P3 (docs/EXECUTION_PROFILING_IMPALA_GAP.md): when this worker started and
    // when it actually finished. TWO CLOCK READS PER WORKER PER PIPELINE — not
    // per morsel — so the cost is ~52ns per worker per pipeline, immeasurable.
    //
    // Why this cannot be derived from OpStats: exec_ns is SUMMED across workers,
    // which loses the distribution entirely. "Every worker was busy for 10ms" and
    // "one worker was busy for 160ms while fifteen idled" are the same sum and
    // opposite problems, and only the second is fixable. The spread of t_last_ns
    // is the barrier skew.
    uint64_t            t_first_ns = 0;
    uint64_t            t_last_ns  = 0;
};

// Straggler picture for one pipeline run. Filled by run_pipeline_impl from the
// worker contexts once every worker has finished.
struct PipelineSkew {
    // Spread between the first worker to finish and the last: the barrier's width.
    uint64_t skew_ns = 0;
    // Worker-time burned waiting at the barrier — sum over workers of (last worker's
    // finish - this worker's finish). This is the number that says what the skew COST,
    // as opposed to how wide it was.
    uint64_t barrier_idle_ns = 0;
    int      workers = 0;
};

// One worker's full body: claim disjoint morsels from the source (dynamic assignment =
// load balance), push each through the operator chain, sink into thread-LOCAL state
// (lock-free), combine local into shared global once at the end.
inline void run_worker(WorkerCtx* ctx) {
    Pipeline& p = *ctx->p;
    ErrCtx& e = (*ctx->errs)[static_cast<size_t>(ctx->w)];
    // RAII, not a plain assignment before the final combine: run_worker returns
    // early on any worker error (three sites below), and a finish timestamp that
    // is only written on the success path would report a failed run as though its
    // workers never finished — a skew reading that is wrong exactly when something
    // has gone wrong. Declared before `lsink` so it destructs LAST, after combine.
    struct FinishStamp {
        WorkerCtx* c;
        ~FinishStamp() { c->t_last_ns = telem_now_ns(); }
    } _finish{ctx};
    ctx->t_first_ns = telem_now_ns();
    std::unique_ptr<LocalSourceState> lsrc = p.source->make_local(*ctx->gsrc);
    std::unique_ptr<LocalSinkState>   lsink = p.sink->make_local(*ctx->gsink);
    std::vector<std::unique_ptr<OperatorState>> op_states;
    op_states.reserve(p.operators.size());
    for (Operator* op : p.operators) op_states.push_back(op->make_state());

    // Push `m` through operators[stage:] into the sink. An operator may EMIT zero
    // times (NEED_INPUT — fully consumed, drop), once (EMIT), or MANY times
    // (HAVE_MORE — re-driven with the SAME input/state until EMIT/NEED_INPUT). This
    // is what lets ONE input morsel fan out to many outputs (join fan-out, UNNEST)
    // without losing any but the first batch.
    const std::memory_order relaxed = std::memory_order_relaxed;
    std::function<void(MorselPtr, size_t)> push = [&](MorselPtr m, size_t stage) {
        if (e.code != 0) return;
        if (stage == p.operators.size()) {
            OpStats& ss = p.sink->stats;
            ss.calls.fetch_add(1, relaxed);
            ss.rows_in.fetch_add(m ? m->num_rows() : 0, relaxed);
            ss.bytes_in.fetch_add(telem_nbytes(m), relaxed);
            uint64_t c0 = telem_cpu_now_ns();  // read order is fixed — see telem_cpu_now_ns
            // TC_SINK: closes the "operator waterfall goes blank for a plan
            // with no Operator-role nodes" gap — a Source->Sink-only pipeline
            // (e.g. scan with a baked-in residual filter -> TopN/Sort) had
            // NOTHING to show before this, even on real, expensive queries.
            // Opened before t0 so an armed trace's own clock read does not land
            // inside the wall bracket either.
            TraceHandle sh = trace_begin(TC_SINK, ss.node_id, 0, 0xFFFFFFFFu, ctx->w);
            uint64_t t0 = telem_now_ns();
            p.sink->sink(m, *ctx->gsink, *lsink, e);
            uint64_t t1 = telem_now_ns();
            uint64_t c1 = telem_cpu_now_ns();
            ss.exec_ns.fetch_add(t1 - t0, relaxed);
            ss.cpu_ns.fetch_add(c1 - c0, relaxed);
            trace_end(sh, m ? static_cast<uint32_t>(m->num_rows()) : 0,
                      static_cast<uint32_t>(telem_nbytes(m)));
            return;
        }
        OpStats& os = p.operators[stage]->stats;
        os.calls.fetch_add(1, relaxed);
        os.rows_in.fetch_add(m ? m->num_rows() : 0, relaxed);
        os.bytes_in.fetch_add(telem_nbytes(m), relaxed);
        MorselPtr out;
        while (true) {
            uint64_t c0 = telem_cpu_now_ns();  // read order is fixed — see telem_cpu_now_ns
            // Phase 1 of docs/EXECUTION_TRACING_DESIGN.md: TC_OP_EXEC span, same
            // bracket as the always-on exec_ns/cpu_ns sums above — a no-op branch
            // when OPTERYX_TRACE is off (trace_begin short-circuits on the disabled
            // gate before touching the clock).
            TraceHandle th = trace_begin(TC_OP_EXEC, os.node_id, 0, 0xFFFFFFFFu, ctx->w);
            uint64_t t0 = telem_now_ns();
            OpResult orr = p.operators[stage]->execute(m, *op_states[stage], out, e);
            uint64_t t1 = telem_now_ns();
            uint64_t c1 = telem_cpu_now_ns();
            os.exec_ns.fetch_add(t1 - t0, relaxed);  // SELF time — excludes forward
            os.cpu_ns.fetch_add(c1 - c0, relaxed);
            trace_end(th, out ? static_cast<uint32_t>(out->num_rows()) : 0,
                      static_cast<uint32_t>(telem_nbytes(out)));
            if (e.code != 0) return;
            if (orr == OpResult::NEED_INPUT) return;       // consumed, no output
            os.rows_out.fetch_add(out ? out->num_rows() : 0, relaxed);
            os.bytes_out.fetch_add(telem_nbytes(out), relaxed);
            push(out, stage + 1);                          // EMIT or HAVE_MORE: forward
            if (e.code != 0) return;
            if (orr == OpResult::EMIT) return;              // this input is drained
            // HAVE_MORE: loop — re-call execute() with the SAME `m` for the next batch.
        }
    };

    OpStats& src = p.source->stats;
    MorselPtr in;
    while (true) {
        if (p.halt != nullptr && p.halt->load(std::memory_order_relaxed)) break;
        uint64_t c0 = telem_cpu_now_ns();  // read order is fixed — see telem_cpu_now_ns
        // TC_SOURCE_PULL: same gap-closing rationale as TC_SINK above — this
        // is the ONLY per-morsel timing signal for a scan whose predicate is
        // baked in rather than a separate Operator (GCS/trampoline scans).
        TraceHandle sph = trace_begin(TC_SOURCE_PULL, src.node_id, 0, 0xFFFFFFFFu, ctx->w);
        uint64_t t0 = telem_now_ns();
        SourceResult sr = p.source->get_morsel(*ctx->gsrc, *lsrc, in, e);
        uint64_t t1 = telem_now_ns();
        uint64_t c1 = telem_cpu_now_ns();
        src.exec_ns.fetch_add(t1 - t0, relaxed);
        src.cpu_ns.fetch_add(c1 - c0, relaxed);
        trace_end(sph, in ? static_cast<uint32_t>(in->num_rows()) : 0,
                  static_cast<uint32_t>(telem_nbytes(in)));
        if (e.code != 0) return;
        if (sr == SourceResult::FINISHED) break;
        src.calls.fetch_add(1, relaxed);
        src.rows_out.fetch_add(in ? in->num_rows() : 0, relaxed);
        src.bytes_out.fetch_add(telem_nbytes(in), relaxed);
        push(in, 0);
        if (e.code != 0) return;
    }
    // P2: combine() was timed by NOTHING — not OpStats, not a span — so a hash
    // aggregate's cross-worker merge was charged to no plan node at all. Once per
    // worker per pipeline, so the bracket is free at any morsel count. Same fixed
    // read order as the per-morsel brackets (see telem_cpu_now_ns).
    //
    // WALL ONLY, deliberately: combine's CPU is NOT added to cpu_ns. exec_ns does not
    // include combine's wall time, so charging cpu_ns with its CPU would make cpu_ns
    // systematically exceed exec_ns on any sink with a real merge — and time_ms vs
    // cpu_ms is precisely the comparison P1 just put on EXPLAIN ANALYZE. The two stay
    // measuring the same window. If combine ever needs its own blocked/running split
    // (it can contend on the global sink), that is a combine_cpu_ns field, not a
    // reading smuggled into this one.
    {
        OpStats& cs = p.sink->stats;
        TraceHandle ch = trace_begin(TC_COMBINE, cs.node_id, 0, 0xFFFFFFFFu, ctx->w);
        uint64_t ct0 = telem_now_ns();
        p.sink->combine(*ctx->gsink, *lsink, e);
        cs.combine_ns.fetch_add(telem_now_ns() - ct0, relaxed);
        trace_end(ch, 0, 0);
    }
}

// Native-task entry matching BSThreadPoolBridge::submit_native's `void(*)(void*)` shape.
inline void run_worker_task(void* raw) {
    run_worker(static_cast<WorkerCtx*>(raw));
}

// Shared setup: global states + per-worker contexts. `dispatch(ctxs)` runs all of them
// to completion (join or pool-wait — whichever backend the caller chose), then this
// checks errors and finalizes exactly as before.
template <typename DispatchFn>
inline std::unique_ptr<GlobalSinkState>
run_pipeline_impl(Pipeline& p, int dop, ErrCtx& err, DispatchFn&& dispatch,
                  PipelineSkew* skew = nullptr) {
    if (dop < 1) dop = 1;
    std::unique_ptr<GlobalSourceState> gsrc = p.source->make_global();
    std::unique_ptr<GlobalSinkState>   gsink = p.sink->make_global();
    gsink->exec_dop = dop;   // finalize()'s parallel width — see GlobalSinkState::exec_dop
    std::vector<ErrCtx> errs(static_cast<size_t>(dop));
    std::vector<WorkerCtx> ctxs(static_cast<size_t>(dop));
    for (int w = 0; w < dop; ++w) {
        ctxs[static_cast<size_t>(w)] = WorkerCtx{&p, gsrc.get(), gsink.get(), &errs, w};
    }

    dispatch(ctxs);

    // P3: every worker has finished, so the finish timestamps are final. Computed
    // here and not by the workers because it is a property of the SET of them.
    if (skew != nullptr) {
        uint64_t earliest_end = 0, latest_end = 0;
        int counted = 0;
        for (const WorkerCtx& c : ctxs) {
            if (c.t_last_ns == 0) continue;   // never dispatched
            if (counted == 0 || c.t_last_ns < earliest_end) earliest_end = c.t_last_ns;
            if (c.t_last_ns > latest_end) latest_end = c.t_last_ns;
            ++counted;
        }
        skew->workers = counted;
        skew->skew_ns = counted > 0 ? latest_end - earliest_end : 0;
        uint64_t idle = 0;
        for (const WorkerCtx& c : ctxs) {
            if (c.t_last_ns == 0) continue;
            idle += latest_end - c.t_last_ns;
        }
        skew->barrier_idle_ns = idle;
    }

    for (ErrCtx& e : errs) {
        if (e.code != 0) { err = e; return gsink; }  // skip finalize on any worker error
    }
    // P2: finalize() — the breaker's result construction — was, like combine(),
    // timed by nothing. Runs exactly once per pipeline. Wall only, for the same
    // reason combine is (see there).
    {
        OpStats& fs = p.sink->stats;
        TraceHandle fh = trace_begin(TC_FINALIZE, fs.node_id, 0, 0xFFFFFFFFu, 0);
        uint64_t ft0 = telem_now_ns();
        p.sink->finalize(*gsink, err);
        fs.finalize_ns.fetch_add(telem_now_ns() - ft0, std::memory_order_relaxed);
        trace_end(fh, 0, 0);
    }
    return gsink;
}

// Demo/test-only backend: a fresh std::thread per worker, joined before returning. Fine
// at low dop (no Python pool available in these call sites); NOT the real cutover path —
// see the free-threaded deadlock note above before raising dop here.
inline std::unique_ptr<GlobalSinkState>
run_pipeline(Pipeline& p, int dop, ErrCtx& err, PipelineSkew* skew = nullptr) {
    return run_pipeline_impl(p, dop, err, [](std::vector<WorkerCtx>& ctxs) {
        std::vector<std::thread> threads;
        threads.reserve(ctxs.size());
        for (WorkerCtx& ctx : ctxs) threads.emplace_back(run_worker, &ctx);
        for (std::thread& t : threads) t.join();
    }, skew);
}

// Real cutover backend: submit `dop` native tasks to the caller's persistent
// BSThreadPoolBridge and block on wait_native() — no new OS threads, no new
// PyGILState_Ensure() races. `pool` must outlive this call (the caller owns it for the
// life of the query, per CppThreadPool's existing contract elsewhere in this codebase).
//
// `pool` is an OPAQUE BSThreadPoolBridge* — dispatched via bs_pool_submit_native/
// bs_pool_wait_native (bs_pool_bridge_c.h), NEVER by calling BSThreadPoolBridge's C++
// methods directly on the pointer from this file. BSThreadPoolBridge is a header-only,
// non-virtual class with no separate .cpp; this executor.hpp is compiled into
// _operators.so (-std=c++20 -DNB_FREE_THREADED), while the pool object itself is
// constructed by thread_pool.so (-std=c++17). Those two independently-compiled copies
// of BS::thread_pool do not agree on BS::move_only_function's layout (its C++23
// feature-test branch differs by standard version), so calling detach_task() through
// _operators.so's own copy against an object built by thread_pool.so's copy corrupted
// the pool's internal task queue — reproduced as a real SIGSEGV. Routing through the
// bs_pool_bridge_c.h symbols (resolved via RTLD_GLOBAL to thread_pool.so's own compiled
// code, the same pattern draken_native.so uses for draken_vector_unwrap et al.) ensures
// the pool is only ever touched by the code that was compiled against its true layout.
inline std::unique_ptr<GlobalSinkState>
run_pipeline(Pipeline& p, int dop, ErrCtx& err, void* pool, PipelineSkew* skew = nullptr) {
    return run_pipeline_impl(p, dop, err, [pool](std::vector<WorkerCtx>& ctxs) {
        for (WorkerCtx& ctx : ctxs) bs_pool_submit_native(pool, &run_worker_task, &ctx);
        bs_pool_wait_native(pool);
    }, skew);
}

}  // namespace opteryx::engine
