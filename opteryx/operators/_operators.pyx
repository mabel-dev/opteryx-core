# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Umbrella compilation unit for all operator plan nodes.
# Individual .pyx files are kept in per-operator subdirectories for authoring clarity;
# all are compiled into this single extension module.
#
# Common cimports declared here are visible to all included files.

from libcpp.memory cimport shared_ptr
from draken.morsels.morsel cimport Morsel, morsel_to_cxx, cxx_to_morsel

from draken.morsels.morsel cimport cxx_morsel_from_vectors_sp, cxx_select_sp
from draken.morsels.cxx_morsel cimport CxxMorsel, MorselState, ErrCtx, cxx_morsel_new_eos, cxx_morsel_delete
from draken.morsels.cxx_morsel cimport cxx_morsel_nbytes
from draken.morsels.cxx_morsel cimport cxx_slice_c, cxx_hash_c, cxx_take_c, cxx_cast_column_c
from draken.morsels.cxx_morsel cimport cxx_morsel_materialize_native_c
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.vector cimport Vector, mix_hash
from draken.core.buffers cimport (
    DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64,
    DRAKEN_FLOAT32, DRAKEN_FLOAT64,
    DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY,
    DRAKEN_BOOL, DRAKEN_DATE32, DRAKEN_TIMESTAMP64, DRAKEN_INTERVAL,
    DRAKEN_SEL_IDENTITY,
    DrakenType, DrakenVector,
)
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set cimport PerfectHashSet
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from cpython.array cimport array
import draken.draken_native as _draken_native

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t, uint8_t, uint16_t, uint32_t
from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy
from cpython.ref cimport PyObject
from opteryx.compiled.thread_pool cimport CppThreadPool, native_task_fn, spawn_detached_native_task
from opteryx.compiled.expression.compiled_expression cimport (
    CompiledBytecode, BytecodeInstr, BC_LOAD_COL, BC_LOAD_LIT_CONST,
    BC_LOAD_LIT_BOOL, BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF,
    BC_COMPARE, BC_BINARY_OP, BC_CAST,
    BC_CMP_INLIST_INLINE, BC_INSTR_C_NATIVE, BC_C_NATIVE_FIXED, BC_C_NATIVE_STRING,
    BC_C_NATIVE_DESC, BC_C_NATIVE_CHILD, BC_C_NATIVE_ARRAY,
    BC_UNARY_OP, UOP_IS_NULL, UOP_IS_NOT_NULL, BC_FUNCTION,
    UOP_IS_TRUE, UOP_IS_FALSE, UOP_IS_NOT_TRUE, UOP_IS_NOT_FALSE,
    BC_EXTRACTION,
    BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF, BC_COMPARE,
)
from opteryx.expression.evaluator._impl cimport (
    _dv_filter_span_cxx,
    _dv_filter_span_with_consts_cxx,
    _dv_eval_span_cxx,
    VecResult,
)
from opteryx.compiled.morsel_queue cimport PyMorselQueue, MorselQueue
from libcpp.vector cimport vector as cppvector
from libcpp.unordered_map cimport unordered_map
from libcpp.pair cimport pair
from libcpp.string cimport string
from opteryx.connectors.parquet_io.pool_reader cimport NativeScanPlan, ParquetIOPipeline
from rugo.parquet_reader cimport FileStats
from opteryx.compiled.structures.memory_pool cimport MemoryPool, CppMemoryPool

# ScanPullFn: the streaming scan pull-on-demand callback. LIVE — ``_scan_pull_trampoline``
# implements it and ``NativePlan.set_scan_source`` hands it to the engine. (The former
# narrow ``native_engine_real_*`` wrappers and the ``run_real_*``/demo fused pipelines
# that also used it were removed as dead code once the general ``NativePlan``/
# ``native_plan_execute`` engine subsumed them.)
ctypedef void (*ScanPullFn)(void* scan_ptr, shared_ptr[CxxMorsel]* out,
                            int* finished, int* err_code) noexcept nogil

cdef extern from "engine/streaming_scan_source.hpp" namespace "opteryx::engine" nogil:
    pass  # makes StreamingScanSource's header available; the engine drives it via ScanPullFn
# ---- THE ENGINE: the pipeline-graph runner (engine.hpp). The plan compiler
# (opteryx/managers/execution/compiler.py — planning, Python) builds the graph through
# the NativePlan builder edge (see below); execution is ONE detached native driver
# task running the whole graph, streaming the terminal pipeline into the production
# MorselQueue the cursor drains. This is the general form that subsumed (and replaced)
# the four narrow native_engine_real_* entry points, now removed.
cdef extern from "engine/native_sort.hpp" namespace "opteryx::engine" nogil:
    cdef struct SortKeySpec:
        size_t col_idx
        bint ascending

# The engine's general expression operators (native_expression.hpp): programs are
# lowered + resolved at PLAN time; execution calls back into the pure-nogil spans
# in evaluation.pyx through these C fn-pointer shapes (the ScanPullFn idiom).
ctypedef int (*ExprFilterFn)(void* instrs, int count, const CxxMorsel* m,
                             int* col_idx, void** lit_dv,
                             int* const_col_idx, void** const_scalar_dv, int n_consts,
                             CxxMorsel** out_filtered, int* err_op,
                             const char** err_msg) noexcept nogil
ctypedef int (*ExprEvalFn)(void* instrs, int count, const CxxMorsel* m,
                           int* col_idx, void** lit_dv,
                           DrakenVector* out_vec, void** out_data,
                           uint8_t** out_validity, void** out_sel,
                           int* err_op, const char** err_msg,
                           VecResult** out_child) noexcept nogil

cdef extern from "engine/native_group_sinks.hpp" namespace "opteryx::engine" nogil:
    cdef enum class AggFn "opteryx::engine::AggFn":
        CountStar "opteryx::engine::AggFn::CountStar"
        Count "opteryx::engine::AggFn::Count"
        Sum "opteryx::engine::AggFn::Sum"
        Avg "opteryx::engine::AggFn::Avg"
        Min "opteryx::engine::AggFn::Min"
        Max "opteryx::engine::AggFn::Max"
        CountDistinct "opteryx::engine::AggFn::CountDistinct"
        ArrayAgg "opteryx::engine::AggFn::ArrayAgg"
        Stddev "opteryx::engine::AggFn::Stddev"
        Median "opteryx::engine::AggFn::Median"
        AnyValue "opteryx::engine::AggFn::AnyValue"
        ApproxCountDistinct "opteryx::engine::AggFn::ApproxCountDistinct"
        ApproxPercentile "opteryx::engine::AggFn::ApproxPercentile"
    cdef cppclass AggSpec2:
        AggFn fn
        int col_idx
        string name
        bint aa_distinct
        bint aa_ordered
        bint aa_descending
        int64_t aa_limit
        int64_t aa_max_per_group
        double percentile

cdef extern from "core/alloc.h" nogil:
    void draken_free(void* ptr)

cdef extern from "engine/groupby_tel.hpp" namespace "opteryx::engine::groupby_tel" nogil:
    double gb_tel_hash_s "opteryx::engine::groupby_tel::hash_s" ()
    double gb_tel_probe_s "opteryx::engine::groupby_tel::probe_s" ()
    double gb_tel_apply_s "opteryx::engine::groupby_tel::apply_s" ()
    long long gb_tel_calls "opteryx::engine::groupby_tel::calls_count" ()
    void gb_tel_reset "opteryx::engine::groupby_tel::reset" ()

# The bridge is the ONLY correct way to reach the shared execution tracer
# state from this .so — see draken/core/trace_bridge_c.h's header comment.
# Do NOT `cdef extern from "engine/trace.hpp"` any of draken_trace's own
# functions here: that header is a thin per-.so wrapper around this exact
# bridge, precisely so nothing (including this file) ends up compiling its
# own independent copy of the tracer's mutable state.
cdef extern from "core/trace_bridge_c.h" nogil:
    ctypedef struct DrakenTraceSpanC:
        uint64_t t_start_ns
        uint64_t t_end_ns
        uint32_t query_seq
        uint16_t category
        uint16_t worker_id
        uint32_t node_id
        uint32_t corr_id
        uint32_t rg_idx
        uint32_t rows
        uint32_t bytes
        uint32_t detail
        uint32_t file_id
    ctypedef struct DrakenFileSymbolC:
        uint32_t file_id
        char* path
    void draken_trace_set_enabled(int on)
    int draken_trace_enabled()
    uint32_t draken_trace_start_query()
    DrakenTraceSpanC* draken_trace_drain(uint32_t query_seq, size_t* out_count, int* out_truncated)
    DrakenFileSymbolC* draken_trace_drain_file_symbols(size_t* out_count)
    const char* draken_trace_host_info()

cdef extern from "engine/engine.hpp" namespace "opteryx::engine" nogil:
    cdef cppclass OpReading "opteryx::engine::Engine::OpReading":
        string identity
        string role
        uint64_t calls
        uint64_t rows_in
        uint64_t rows_out
        uint64_t bytes_in
        uint64_t bytes_out
        uint64_t exec_ns
        uint64_t cpu_ns
    cdef cppclass Engine:
        Engine() except +
        void set_current_identity(string s)
        void set_current_display_name(string s)
        cppvector[OpReading] collect_op_stats()
        cppvector[pair[uint32_t, string]] collect_trace_symbols()
        size_t new_pipeline()
        size_t new_buffer()
        void set_scan_source(size_t p, void* scan_ptr, ScanPullFn fn, bint serialize_pull)
        void set_native_scan_source(size_t p, ParquetIOPipeline* pipeline,
                                    const unordered_map[string, FileStats]* footer_map,
                                    const cppvector[pair[string, int]]* work_items,
                                    const cppvector[string]* column_names,
                                    int in_flight_limit,
                                    CppMemoryPool* pool,
                                    const cppvector[int]* string_types,
                                    const cppvector[uint8_t]* decimal_columns,
                                    const cppvector[int]* logical_coerce,
                                    const cppvector[uint8_t]* hash_key_columns,
                                    const cppvector[uint8_t]* array_columns,
                                    int64_t row_limit)
        void set_latmat_scan_source(size_t p, ParquetIOPipeline* p1_pipeline,
                                    const unordered_map[string, FileStats]* footer_map,
                                    const cppvector[pair[string, int]]* work_items,
                                    const cppvector[string]* p1_column_names,
                                    int in_flight_limit,
                                    CppMemoryPool* p1_pool,
                                    const cppvector[int]* p1_string_types,
                                    const cppvector[uint8_t]* p1_decimal_columns,
                                    const cppvector[int]* p1_logical_coerce,
                                    const cppvector[uint8_t]* p1_hash_key_columns,
                                    const cppvector[uint8_t]* p1_array_columns,
                                    ParquetIOPipeline* p2_pipeline,
                                    const cppvector[string]* p2_column_names,
                                    CppMemoryPool* p2_pool,
                                    const cppvector[int]* p2_string_types,
                                    const cppvector[uint8_t]* p2_decimal_columns,
                                    const cppvector[int]* p2_logical_coerce,
                                    const cppvector[uint8_t]* p2_hash_key_columns,
                                    const cppvector[uint8_t]* p2_array_columns,
                                    void* pred_fn, void* pred_ctx,
                                    cppvector[int] pred_col_to_p1,
                                    int sort_p1_index, bint sort_ascending,
                                    int64_t topn_limit,
                                    cppvector[int] out_from_p1,
                                    cppvector[int] out_from_p2,
                                    cppvector[string] out_names)
        void set_buffer_source(size_t p, size_t buf)
        void add_expr_filter(size_t p, void* instrs, int count, cppvector[int] col_idx,
                             cppvector[void*] lit_dv, ExprFilterFn fn,
                             cppvector[int] const_col_idx, cppvector[void*] const_scalar_dv)
        void add_expr_project(size_t p, void* instrs, int count, cppvector[int] col_idx,
                              cppvector[void*] lit_dv, ExprEvalFn fn, string name,
                              int lt_kind, int lt_unit, int lt_precision, int lt_scale,
                              int lt_dimension)
        void add_limit(size_t p, int64_t offset, int64_t limit)
        void add_unnest(size_t p, uint32_t array_idx, string target_name, bint drop_source)
        void add_unnest_literal(size_t p, shared_ptr[CxxMorsel] lit, string target_name)
        void add_buffer_morsel(size_t buf, shared_ptr[CxxMorsel] m)
        void set_pipeline_dop(size_t p, int dop)
        void add_select(size_t p, cppvector[size_t] indices, cppvector[string] names)
        void set_queue_sink(size_t p, MorselQueue* q)
        void set_agg_sink(size_t p, cppvector[AggSpec2] specs, size_t buf)
        void set_groupby_sink(size_t p, cppvector[size_t] key_idx,
                              cppvector[string] key_names,
                              cppvector[AggSpec2] specs, size_t buf)
        void set_distinct_sink(size_t p, cppvector[size_t] on_idx, size_t buf)
        void set_buffer_append_sink(size_t p, size_t buf)
        size_t new_join2_ref()
        void set_join2_build_sink(size_t p, cppvector[size_t] key_idx,
                                  cppvector[size_t] payload_idx, size_t ref,
                                  cppvector[DrakenType] payload_types,
                                  cppvector[int] lt_kind, cppvector[int] lt_unit,
                                  cppvector[int] lt_precision, cppvector[int] lt_scale,
                                  cppvector[int] lt_dimension)
        void add_join2_probe_residual(size_t p, size_t ref, cppvector[size_t] key_idx,
                                      cppvector[size_t] payload_idx, int mode,
                                      void* instrs, int count, cppvector[int] col_idx,
                                      cppvector[void*] lit_dv, ExprEvalFn fn)
        void add_join2_probe(size_t p, size_t ref, cppvector[size_t] key_idx,
                             cppvector[size_t] payload_idx, int mode)
        void set_asof_build_sink(size_t p, cppvector[size_t] key_idx,
                                 cppvector[size_t] payload_idx, size_t asof_idx,
                                 size_t ref, cppvector[DrakenType] payload_types,
                                 cppvector[int] lt_kind, cppvector[int] lt_unit,
                                 cppvector[int] lt_precision, cppvector[int] lt_scale,
                                 cppvector[int] lt_dimension)
        void add_asof_probe(size_t p, size_t ref, cppvector[size_t] key_idx,
                            cppvector[size_t] payload_idx, size_t asof_idx, int op)
        void set_sort_sink(size_t p, cppvector[SortKeySpec] spec, size_t buf)
        void set_topn_sink(size_t p, cppvector[SortKeySpec] spec, size_t n, size_t buf)
        void set_window_sink(size_t p, cppvector[SortKeySpec] sort_spec, size_t n_part,
                             cppvector[int] fn_kinds, cppvector[string] fn_names,
                             long long top_k, size_t buf)
        void set_window_topk_sink(size_t p, cppvector[size_t] part_idx, size_t order_idx,
                                  bint ascending, size_t k, string out_name, size_t buf)
        void set_final_schema(cppvector[string] names, cppvector[DrakenType] types,
                              cppvector[int] lt_kind, cppvector[int] lt_unit,
                              cppvector[int] lt_precision, cppvector[int] lt_scale,
                              cppvector[int] lt_dimension)
        void run(int dop, void* pool, ErrCtx& err)

# ---- Genuinely native (zero-Python) UNGROUPED aggregate: NativeParquetScanSource ->
# [NumericFilterOperator] -> NativeAggregateSink. See native_aggregate.hpp for the
# scope boundary — SUM/COUNT/AVG only, fixed-width numeric only, NOT DECIMAL.
cdef extern from "engine/native_aggregate.hpp" namespace "opteryx::engine" nogil:
    cdef enum class ExprKind "opteryx::engine::ExprKind":
        Column "opteryx::engine::ExprKind::Column"
        Literal "opteryx::engine::ExprKind::Literal"
        Add "opteryx::engine::ExprKind::Add"
        Sub "opteryx::engine::ExprKind::Sub"
        Mul "opteryx::engine::ExprKind::Mul"
        Div "opteryx::engine::ExprKind::Div"
    cdef cppclass NativeExpr:
        ExprKind kind
        size_t col_idx
        double literal
        shared_ptr[NativeExpr] left
        shared_ptr[NativeExpr] right

        @staticmethod
        shared_ptr[NativeExpr] make_column(size_t idx)
        @staticmethod
        shared_ptr[NativeExpr] make_literal(double v)
        @staticmethod
        shared_ptr[NativeExpr] make_binary(ExprKind k, shared_ptr[NativeExpr] l,
                                           shared_ptr[NativeExpr] r)

    cdef enum class AggFunc "opteryx::engine::AggFunc":
        Sum "opteryx::engine::AggFunc::Sum"
        Count "opteryx::engine::AggFunc::Count"
        Avg "opteryx::engine::AggFunc::Avg"
    cdef struct AggregateSpec:
        AggFunc func
        bint is_decimal
        shared_ptr[NativeExpr] expr
        shared_ptr[DecimalExpr] decimal_expr

cdef extern from "engine/native_decimal.hpp" namespace "opteryx::engine" nogil:
    cdef enum class DecimalExprKind "opteryx::engine::DecimalExprKind":
        Column "opteryx::engine::DecimalExprKind::Column"
        Literal "opteryx::engine::DecimalExprKind::Literal"
        Add "opteryx::engine::DecimalExprKind::Add"
        Sub "opteryx::engine::DecimalExprKind::Sub"
        Mul "opteryx::engine::DecimalExprKind::Mul"
        Case "opteryx::engine::DecimalExprKind::Case"
    cdef cppclass DecimalExpr:
        DecimalExprKind kind
        size_t col_idx
        uint8_t scale
        shared_ptr[DecimalExpr] left
        shared_ptr[DecimalExpr] right
        size_t cond_col_idx
        string cond_prefix

        @staticmethod
        shared_ptr[DecimalExpr] make_column(size_t idx, uint8_t sc)
        @staticmethod
        shared_ptr[DecimalExpr] make_literal(int64_t unscaled, uint8_t sc)
        @staticmethod
        shared_ptr[DecimalExpr] make_binary(DecimalExprKind k, shared_ptr[DecimalExpr] l,
                                            shared_ptr[DecimalExpr] r)
        @staticmethod
        shared_ptr[DecimalExpr] make_case(size_t cond_idx, string prefix,
                                          shared_ptr[DecimalExpr] then_expr,
                                          shared_ptr[DecimalExpr] else_expr)


# -----------------------------------------------------------------------------
# Shared helper: rebuild a CarcharSetWrapper from a PerfectHashSet.
#
# Used by the PerfectHashSet fast paths in filter_join (semi/anti probe) and
# distinct when a mid-stream morsel turns out to have an encoding the
# PerfectHashSet path can't handle (nullable / non-dense / type drift): the
# narrow-int values already marked as seen are re-inserted as int64 row-hashes
# so the standard carchar path recognises them.
# -----------------------------------------------------------------------------

cdef CarcharSetWrapper _rebuild_carchar_from_phash(PerfectHashSet phs):
    """Reconstruct a hash-based CarcharSetWrapper from an existing PerfectHashSet.

    Iterates the bit-array and inserts the int64 row-hash of each stored value
    directly — mix_hash(0, v) is identically the int64 hash kernel's output for
    value v (the same equivalence the probe path produces), so no per-value
    constant vector or Python hash() round-trip is needed.
    """
    cdef CarcharSetWrapper result = CarcharSetWrapper(<size_t>phs.range() * 2 + 8)
    cdef Py_ssize_t w, bit
    cdef uint64_t word, mask, h
    cdef int64_t slot, val
    cdef int64_t min_val = phs.min_val()
    for w in range(phs.n_words()):
        word = phs.word_at(w)
        if word == 0:
            continue
        for bit in range(64):
            mask = <uint64_t>1 << bit
            if word & mask:
                slot = <int64_t>w * 64 + <int64_t>bit
                val = min_val + slot
                # Must equal simd_hash_i64(val) exactly so probe-side hashes
                # match: mix_hash supplies `val * C + 1`; the int64 kernel then
                # applies the final avalanche `h ^ (h >> 32)`. Omitting it (as a
                # prior version did) leaves the rebuilt set unmatchable.
                h = mix_hash(0, <uint64_t>val)
                result.insert(h ^ (h >> 32))
    return result

# -----------------------------------------------------------------------------
# Foundation: shared types and the BasePlanNode hierarchy.
#
# These classes are defined ONCE here at the top of the compilation unit; every
# operator file included below sees them in scope. Per CLAUDE.md the engine is
# Cython/C++ with Python orchestration — these classes implement the typed push
# pipeline that replaces the pull/generator model.
# -----------------------------------------------------------------------------

cdef extern from "time.h" nogil:
    cdef struct timespec:
        long tv_sec
        long tv_nsec
    int CLOCK_MONOTONIC
    int clock_gettime(int clk_id, timespec *tp)


cdef extern from "pythread.h":
    unsigned long PyThread_get_thread_ident()


# -----------------------------------------------------------------------------
# WP-INSTR: execution-time GIL instrumentation (off by default, ~0 cost when off)
#
# Instruments 1 & 4 of the measurement harness. Times the wall-clock nanoseconds
# spent inside the KNOWN execution-time ``with gil`` bodies — the scan-pull
# trampoline (``_scan_pull_run``, entered once per morsel per worker for a
# StreamingScanSource) and the carrier-flip error stash (``_stash_exc``) — and
# records which OS thread entered which named site. Two derived readings:
#   1. gil_held_ns  — summed over all sites; a native-gated numeric scan touches
#      no execution Python and reports ~0, a trampoline scan reports clearly > 0.
#   4. worker_gil_sites — the enumerated (thread, site) breakdown a purity guard
#      checks: only whitelisted sites may appear; an empty list == zero
#      execution-time Python ran on any worker.
#
# The NativeParquetScanSource path has NO Python callback, so it never records a
# site — that absence IS the measurement.
#
# Armed for the span of one native run by ``execute_native`` when the
# OPTERYX_INSTRUMENT_ENGINE config flag is set. The instrumented sites read a
# single C flag and branch straight past when disarmed. The accumulators are
# module globals mutated only from GIL-held bodies (no extra lock needed), and
# are therefore NOT correct across concurrent queries in one process — this is a
# diagnostic instrument, documented as such.
# -----------------------------------------------------------------------------

cdef struct _GilSite:
    unsigned long tid
    const char* name    # stable C string literal per call-site (compared by pointer)
    long long calls
    long long ns

cdef int _gil_instr_enabled = 0
cdef long long _gil_instr_total_ns = 0
cdef _GilSite _gil_instr_sites[64]
cdef int _gil_instr_site_count = 0

# Stable C string literals (static storage) — safe to stash the pointer and to
# compare by pointer identity; each call-site passes the same constant.
cdef const char* _SITE_SCAN_PULL = "_scan_pull_run"
cdef const char* _SITE_STASH_EXC = "_stash_exc"


cdef inline long long _instr_mono_ns() noexcept:
    cdef timespec ts
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return (<long long>ts.tv_sec) * <long long>1000000000 + <long long>ts.tv_nsec


cdef inline void _instr_record(const char* name, long long ns) noexcept:
    """Attribute ``ns`` and one call to (current-thread, ``name``). Called only from
    GIL-held bodies, so the shared accumulators need no extra lock."""
    global _gil_instr_total_ns, _gil_instr_site_count
    cdef unsigned long tid = PyThread_get_thread_ident()
    cdef int i
    _gil_instr_total_ns += ns
    for i in range(_gil_instr_site_count):
        if _gil_instr_sites[i].tid == tid and _gil_instr_sites[i].name == name:
            _gil_instr_sites[i].calls += 1
            _gil_instr_sites[i].ns += ns
            return
    if _gil_instr_site_count < 64:
        _gil_instr_sites[_gil_instr_site_count].tid = tid
        _gil_instr_sites[_gil_instr_site_count].name = name
        _gil_instr_sites[_gil_instr_site_count].calls = 1
        _gil_instr_sites[_gil_instr_site_count].ns = ns
        _gil_instr_site_count += 1


def instr_gil_set_enabled(bint on):
    """Arm/disarm the execution-time GIL instrumentation (``execute_native`` only)."""
    global _gil_instr_enabled
    _gil_instr_enabled = 1 if on else 0


def instr_gil_is_enabled():
    return _gil_instr_enabled != 0


def instr_gil_reset():
    """Zero the per-query accumulators. Call before an armed run."""
    global _gil_instr_total_ns, _gil_instr_site_count
    _gil_instr_total_ns = 0
    _gil_instr_site_count = 0


def instr_gil_total_ns():
    """Total nanoseconds spent inside instrumented execution-time GIL bodies."""
    return _gil_instr_total_ns


def instr_gil_worker_report():
    """Per (thread, site) breakdown: list of ``{thread_id, site, calls, ns}``. The
    distinct ``site`` values are the enumerated GIL-entry set a purity guard checks;
    an empty list means no execution-time Python ran on any worker thread."""
    cdef int i
    out = []
    for i in range(_gil_instr_site_count):
        out.append({
            "thread_id": <unsigned long>_gil_instr_sites[i].tid,
            "site": (<bytes>_gil_instr_sites[i].name).decode("ascii"),
            "calls": <long long>_gil_instr_sites[i].calls,
            "ns": <long long>_gil_instr_sites[i].ns,
        })
    return out


def reset_groupby_telemetry():
    """Zero the GroupBySink hash/probe/apply phase accumulators (groupby_tel.hpp).
    Diagnostic only — call before a traced query to attribute the reading to it."""
    gb_tel_reset()


def get_groupby_telemetry():
    """Return a dict with GroupBySink's Pass A/B/C phase timing (seconds) since the
    last reset: hash_s (key hashing), probe_s (hash-table find_or_insert + lane
    growth), apply_s (per-aggregate-function state update)."""
    return {
        "hash_s":  gb_tel_hash_s(),
        "probe_s": gb_tel_probe_s(),
        "apply_s": gb_tel_apply_s(),
        "calls":   gb_tel_calls(),
    }


# -----------------------------------------------------------------------------
# Native execution tracing (docs/EXECUTION_TRACING_DESIGN.md) — Phase 1.
#
# Distinct from the per-morsel ``TraceEvent`` packed struct below (EXPLAIN
# ANALYZE's own, unrelated buffer): this is the engine-wide span waterfall
# gated by config.OPTERYX_TRACE. All recording happens in engine/trace.hpp
# (nogil, no Python touched — the old opteryx/tracing/ died from trying to
# call Python per-span from this path). These wrappers only arm/disarm the
# runtime gate and drain the already-closed spans into one bytes blob at
# query teardown, mirroring collect_op_stats' single boundary crossing.
# -----------------------------------------------------------------------------

# Field layout of one TraceSpan (draken/core/trace.hpp), little-endian, matching
# the native struct's own (compiler-inserted) tail padding to 64 bytes. Exposed
# so callers parse the drained blob with struct.iter_unpack instead of
# duplicating the layout by hand: (t_start_ns, t_end_ns, query_seq, category,
# worker_id, node_id, corr_id, rg_idx, rows, bytes, detail, file_id,
# reserved0, reserved1). file_id resolves via native_trace_drain_file_symbols();
# node_id resolves via NativePlan.collect_trace_symbols().
TRACE_SPAN_STRUCT_FORMAT = "<QQIHHIIIIIII2I4x"
TRACE_SPAN_SIZE = 64


def native_trace_set_enabled(bint on):
    """Arm/disarm native span recording, engine- and rugo-side alike (the shared
    bridge in draken/core/trace_bridge_c.h). Driven by config.OPTERYX_TRACE."""
    draken_trace_set_enabled(1 if on else 0)


def native_trace_start_query():
    """Bump the trace generation for a new query. Returns the query_seq to pass to
    native_trace_drain() at teardown. Call before the driver dispatches workers so
    every worker's arena — engine and rugo — lazily resets under the new
    generation on first touch."""
    return draken_trace_start_query()


def native_trace_host_info():
    """"arch=...;host=..." identity of the process that captured this trace —
    lets a comparison between two trace bundles tell a genuine performance
    difference apart from an ARM-vs-x86 (or machine-vs-machine) difference
    without out-of-band knowledge of where each trace came from."""
    return draken_trace_host_info().decode("utf-8")


def native_trace_drain(uint32_t query_seq):
    """Walk every thread's arena (engine's and rugo's alike) for ``query_seq``,
    concatenate into one contiguous blob, and return ``(blob: bytes, truncated:
    bool)``. Must be called AFTER every worker that could still be recording has
    joined — same precondition collect_op_stats documents at its own call site."""
    cdef size_t n = 0
    cdef int truncated = 0
    cdef DrakenTraceSpanC* buf = draken_trace_drain(query_seq, &n, &truncated)
    if buf == NULL or n == 0:
        return b"", bool(truncated)
    blob = (<char*>buf)[:n * sizeof(DrakenTraceSpanC)]
    draken_free(<void*>buf)
    return blob, bool(truncated)


def native_trace_drain_file_symbols():
    """file_id -> path for every file interned this query (draken_trace_intern_file,
    called from rugo's io_pipeline.hpp at row-group submission). Resolves the
    file_id carried on IO/decode trace spans. Same call-after-run precondition
    as native_trace_drain — the intern table resets on the next
    native_trace_start_query()."""
    cdef size_t n = 0
    cdef DrakenFileSymbolC* rows = draken_trace_drain_file_symbols(&n)
    if rows == NULL or n == 0:
        return {}
    out = {}
    cdef size_t i
    for i in range(n):
        out[int(rows[i].file_id)] = (<bytes>rows[i].path).decode("utf-8")
        draken_free(<void*>rows[i].path)
    draken_free(<void*>rows)
    return out


cdef packed struct TraceEvent:
    int64_t morsel_index
    uint64_t rows_in
    uint64_t rows_out
    uint64_t bytes_in
    uint64_t bytes_out
    uint64_t duration_ns
    int produced_output       # 0/1; bint not allowed in packed struct


cdef class PipelineContext:
    """Per-query shared state. Used for backpressure (LIMIT short-circuit).

    Termination is mediated through is_terminated()/terminate() rather than a
    public attribute so the underlying signalling primitive (today: bint; later:
    threading.Event) can change without touching every call site.

    Carrier-flip error stash: with each operator body now in its own `with gil:`
    region (the chain currency is `shared_ptr[CxxMorsel]`, the methods are
    `noexcept nogil`), a downstream Python exception cannot propagate through the
    nogil frames — the failing body stashes it here on the shared context and
    returns a non-OK status; the driver (drive_scan / push_one) re-raises it once
    at the gil boundary. `_exc` holds the first exception seen (later ones are
    dropped — the first failure is the cause).
    """
    cdef bint _terminated
    cdef object _exc

    def __cinit__(self):
        self._terminated = False
        self._exc = None

    cpdef bint is_terminated(self):
        return self._terminated

    cpdef void terminate(self):
        self._terminated = True


# Sentinels — Python-level objects from opteryx top-level. We resolve them at
# module init and store as module-level Python references. Comparisons use `is`
# for identity at the C level.
from opteryx import EOS as _EOS_SENTINEL
from opteryx import EMPTY as _EMPTY_SENTINEL


cdef class BasePlanNode:
    """Base class for every operator on the push pipeline.

    Subclasses override `_push_impl(Morsel)` to do their work and call
    `self.emit(result)` to forward results downstream. They MUST NOT call
    `emit()` for `EMPTY`/`None` results — only for real morsels and for
    terminal `EOS`.

    The fast path (push → _push_impl → emit → downstream.push) is all typed
    Cython calls dispatched via the C-level vtable.

    Morsel ownership contract
    -------------------------
    A pushed morsel is handed to the receiver: once an operator calls
    `emit(morsel)` (or pushes downstream), it transfers ownership and MUST NOT
    retain or mutate that morsel afterwards. Symmetrically, a receiver may
    consume its input however it likes — including in-place mutation (e.g.
    Distinct's `_take_inplace`) — because no upstream operator will read the
    morsel again. Morsels are not shared between consumers in the push
    pipeline (single-output topology), so this exclusive-ownership rule holds
    without copying. Operators that need to keep input across calls (blocking
    operators buffering for a sort/aggregate/join build) take ownership by
    appending to their own state and never emitting those morsels until
    finalisation.
    """
    # Class-level defaults — operators override via the registry in __init__.
    # Declared as `public bint` instance fields so subclasses' __init__ may set
    # them; we initialise from the catalog metadata.
    cdef public bint is_join
    cdef public bint is_scan
    cdef public bint is_not_explained
    cdef public bint is_stateless

    # Pipeline wiring — set by pipeline_compiler before execution starts.
    cdef BasePlanNode _downstream
    cdef PipelineContext _ctx
    # Number of upstream input chains that will each push exactly one EOS into
    # this operator. The pipeline compiler stamps this from the incoming-edge
    # count; defaults to 1 (single-input operators). Multi-input operators that
    # gate their downstream EOS on all inputs closing (e.g. Union) count down
    # against this instead of hardcoding the number of legs.
    cdef int _expected_input_closes
    cdef int _seen_input_closes

    # Hot-path telemetry counters (typed; no Python attr writes per morsel).
    cdef public uint64_t execution_time      # ns; cumulative, INCLUSIVE of downstream
    # Time (ns) spent inside downstream `push()` calls, accumulated in
    # `_emit_cdef`. Subtracting it from `execution_time` yields this operator's
    # SELF time (its own work, excluding the downstream chain it drives).
    # Only accumulated when tracing is enabled (EXPLAIN ANALYZE flips it on),
    # so normal queries pay zero extra clock_gettime cost; when tracing is off
    # `self_time` reported by sensors() equals `execution_time` (inclusive).
    cdef public uint64_t downstream_time
    cdef public uint64_t calls
    cdef public uint64_t records_in
    cdef public uint64_t records_out
    cdef public uint64_t bytes_in
    cdef public uint64_t bytes_out
    cdef int64_t _morsel_index

    # Tracing — gated by flag; off by default so emit() pays zero cost.
    cdef bint _tracing_enabled
    cdef TraceEvent *_trace_buf
    cdef Py_ssize_t _trace_capacity
    cdef Py_ssize_t _trace_count

    # Construction-time configuration (Python objects, not on the hot path).
    cdef public object properties
    cdef public object telemetry
    cdef public dict parameters
    cdef public list columns
    cdef public str identity
    cdef public object _empty_morsel_cache
    cdef public str _time_stat_key
    cdef public object readings   # defaultdict(int); operators use `+= 1` patterns
    # Cached iterator backing the default `next_morsel` implementation —
    # lazy-initialised on first call from drive_scan.
    cdef object _morsel_iter
    # Optimizer/binder-attached metadata copied from the logical plan
    cdef public object manifest
    cdef public object uuid
    # Carrier-flip error stash (per-node fallback). Bodies normally stash on the
    # shared PipelineContext (`_ctx._exc`); this node-local slot is the fallback
    # for nodes with no context (e.g. direct-push unit tests). The driver
    # re-raises at the gil boundary (status-code model — `except +` is
    # unavailable on cdef-class methods, validated by the S-B spike).
    cdef object _cxx_push_exc

    def __cinit__(self, *args, **kwargs):
        self._downstream = None
        self._ctx = None
        self._cxx_push_exc = None
        self._morsel_index = 0
        self._tracing_enabled = False
        self._trace_buf = NULL
        self._trace_capacity = 0
        self._trace_count = 0
        self._morsel_iter = None
        self._expected_input_closes = 1
        self._seen_input_closes = 0

    def __dealloc__(self):
        if self._trace_buf is not NULL:
            free(self._trace_buf)
            self._trace_buf = NULL

    def __init__(self, properties=None, **parameters):
        from collections import defaultdict
        from opteryx.models import QueryTelemetry
        from opteryx.operators.catalog import get_registry
        from opteryx.utils import random_string

        self.properties = properties
        self.telemetry = QueryTelemetry(properties.query_id)
        self.parameters = parameters
        self.execution_time = 0
        self.downstream_time = 0
        self.identity = random_string()
        self.calls = 0
        self.records_in = 0
        self.bytes_in = 0
        self.records_out = 0
        self.bytes_out = 0
        self.columns = parameters.get("columns") or []

        self._time_stat_key = f"time_{self.name.lower().replace(' ', '_')}"
        self._empty_morsel_cache = None
        self.readings = defaultdict(int)

        # Initialise flags from catalog (single source of truth).
        self.is_scan = False
        self.is_join = False
        self.is_stateless = False
        self.is_not_explained = False
        _meta = get_registry().get(self.__class__)
        if _meta is not None:
            self.is_scan = bool(_meta.is_scan)
            self.is_join = bool(_meta.is_join)
            self.is_stateless = bool(_meta.is_stateless)
            self.is_not_explained = bool(_meta.is_not_explained)

    # ---- Worker spec/state contract (native scheduler rewrite, slice 2a) ---------
    # The fan-out replacement for `_clone_op`. Each operator's fields split into
    # SPEC (built once, immutable after `resolve_schema`, shared read-only across
    # worker threads) and STATE (per-worker, mutable during push). See
    # docs/NATIVE_SCHEDULER_REWRITE_DESIGN.md §9.3.

    cdef void resolve_schema(self, object input_schema) except *:
        """Bind-time hook: freeze any first-push-resolved SPEC (column indices,
        types, key-kinds) against the known input schema. Default: no-op — operators
        with lazy first-morsel resolution override this. Called once before
        execution; after it, SPEC is frozen."""
        pass

    cdef bint is_partition_parallel(self):
        """False for operators whose STATE carries global semantics that cannot be
        data-partitioned per worker (window running-counters, global DISTINCT, union
        schema/leg-count). The scheduler runs those serial/merge-only and never calls
        `make_worker` on them. Default: True."""
        return True

    cdef BasePlanNode make_worker(self):
        """Return a worker instance with fresh STATE that borrows this operator's
        SPEC by reference. Replaces `_clone_op`. Default: the interim reflection
        clone (re-runs `__init__`, recompiles) — operators migrated to the contract
        override to share SPEC with no recompile. Never returns `self` (that would
        share mutable STATE — a lost-update race in free-threaded builds)."""
        return <BasePlanNode>type(self)(properties=self.properties, **self.parameters)

    cdef void _copy_worker_base(self, BasePlanNode w) except *:
        """Helper for `make_worker` overrides: copy BasePlanNode SPEC into `w` by
        reference and initialise its base STATE fresh. `w` must be freshly allocated
        via `Cls.__new__(Cls)` (so `__cinit__` has zeroed the pointer/trace infra).
        The override then assigns its OWN spec (by reference) and fresh state."""
        from collections import defaultdict
        from opteryx.utils import random_string
        from opteryx.models import QueryTelemetry
        # SPEC — shared read-only across workers.
        w.properties = self.properties
        w.parameters = self.parameters
        w.columns = self.columns
        w._time_stat_key = self._time_stat_key
        w.is_scan = self.is_scan
        w.is_join = self.is_join
        w.is_stateless = self.is_stateless
        w.is_not_explained = self.is_not_explained
        w.manifest = self.manifest
        w.uuid = self.uuid
        # STATE — fresh per worker (matches __init__; clone stats are not merged
        # back today, so a fresh telemetry/readings is byte-identical to _clone_op).
        w.telemetry = QueryTelemetry(self.properties.query_id)
        w.identity = random_string()
        w.readings = defaultdict(int)
        w.execution_time = 0
        w.downstream_time = 0
        w.calls = 0
        w.records_in = 0
        w.records_out = 0
        w.bytes_in = 0
        w.bytes_out = 0
        w._empty_morsel_cache = None

    # ---- Properties (overridable by subclasses; cdef class supports @property) ----
    @property
    def config(self) -> str:
        return ""

    @property
    def name(self) -> str:
        return "no name"

    @property
    def node_type(self) -> str:
        return self.name

    def to_mermaid(self, nid):
        mermaid = f'NODE_{nid}["**{self.node_type.upper()}**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'

    def __str__(self) -> str:
        return f"{self.name} {self.sensors()}"

    def __call__(self, morsel):
        """Legacy direct invocation for operators that don't enter the push
        pipeline (ShowValue, SetVariable, ShowCreate, ViewManagement,
        TableManagement, RelationManagement, Insert, Explain, ShowColumns).
        These all retain a `def execute(self, morsel)` generator method;
        this wrapper yields from it without any per-morsel telemetry.
        Push-pipeline operators are invoked via push()/_push_impl() and
        never go through this path."""
        execute = getattr(self, "execute", None)
        if execute is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} has no execute() method and is not "
                "compatible with legacy direct invocation."
            )
        yield from execute(morsel)

    def sensors(self):
        # self_time = own work, excluding the downstream chain this operator
        # drives (execution_time is INCLUSIVE of downstream). downstream_time is
        # only accumulated under tracing (EXPLAIN ANALYZE); when tracing is off
        # it is 0 and self_time == execution_time. Clamp at 0 to absorb clock
        # jitter and the join/scan case where push() is never called on the node
        # itself (its downstream emits still accrue downstream_time).
        cdef uint64_t self_time = 0
        if self.execution_time > self.downstream_time:
            self_time = self.execution_time - self.downstream_time
        base = {
            "calls": int(self.calls),
            "execution_time": int(self.execution_time),
            "self_time": int(self_time),
            "downstream_time": int(self.downstream_time),
            "records_in": int(self.records_in),
            "records_out": int(self.records_out),
            "bytes_in": int(self.bytes_in),
            "bytes_out": int(self.bytes_out),
        }
        if self.readings:
            base.update(self.readings)
        return base

    # ---- Carrier-flip error-stash helpers (gil held) ---------------------------
    cdef inline void _stash_exc(self, object exc, ErrCtx* err):
        """Record a body's Python exception so the driver can re-raise it at the
        gil boundary, and flag the status code. Prefer the shared context (every
        node on a pipeline shares it); fall back to the node when there is no
        context (e.g. a direct-push unit test). First exception wins.

        WP-INSTR: this is an execution-time GIL body; when the engine
        instrumentation is armed, bracket it so error-path Python re-entry is
        counted in ``worker_gil_sites`` too (normally ~0 calls — it only runs on a
        body's exception)."""
        cdef long long _t0
        if _gil_instr_enabled:
            _t0 = _instr_mono_ns()
        if self._ctx is not None:
            if self._ctx._exc is None:
                self._ctx._exc = exc
        elif self._cxx_push_exc is None:
            self._cxx_push_exc = exc
        if err != NULL:
            err.code = 1
        if _gil_instr_enabled:
            _instr_record(_SITE_STASH_EXC, _instr_mono_ns() - _t0)

    cdef inline object _peek_exc(self):
        """Return the stashed exception (context first, then node) WITHOUT
        clearing it — used by `emit` to re-raise and unwind its own gil body."""
        if self._ctx is not None and self._ctx._exc is not None:
            return self._ctx._exc
        return self._cxx_push_exc

    cdef object _take_exc(self):
        """Return AND clear the stashed exception — used by drivers
        (drive_scan / push_one) to raise once at the gil boundary."""
        cdef object exc
        if self._ctx is not None and self._ctx._exc is not None:
            exc = self._ctx._exc
            self._ctx._exc = None
            return exc
        exc = self._cxx_push_exc
        self._cxx_push_exc = None
        return exc

    # ---- Push pipeline interface ------------------------------------------------
    cdef int push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """Entry point over the C++ morsel carrier (`shared_ptr[CxxMorsel]`).
        Records timing + per-morsel counters (read nogil from the CxxMorsel),
        then dispatches to `_dispatch_push`. `noexcept nogil`: the chain is driven
        with the GIL released; bodies re-acquire it as needed. Errors surface as a
        status code in `*err` (the driver re-raises the stashed exception).
        Not Python-callable — Python drivers use the `push_one` module helper."""
        cdef timespec ts_start, ts_end
        cdef uint64_t duration_ns
        cdef uint64_t rows = 0
        cdef uint64_t nbytes = 0
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        cdef uint64_t records_out_before
        cdef uint64_t bytes_out_before
        cdef uint64_t rows_emitted
        cdef uint64_t bytes_emitted

        if self._ctx is not None and self._ctx._terminated:
            return 0

        if (not is_eos) and raw != NULL:
            rows = raw.num_rows()
            # Real per-vector footprint (string arena included), not rows×cols×8.
            nbytes = <uint64_t>cxx_morsel_nbytes(raw)
            self.records_in += rows
            self.bytes_in += nbytes
        self.calls += 1

        records_out_before = self.records_out
        bytes_out_before = self.bytes_out

        clock_gettime(CLOCK_MONOTONIC, &ts_start)
        self._dispatch_push(m, err)
        clock_gettime(CLOCK_MONOTONIC, &ts_end)
        duration_ns = (<uint64_t>(ts_end.tv_sec - ts_start.tv_sec)) * <uint64_t>1000000000
        duration_ns += <uint64_t>(ts_end.tv_nsec - ts_start.tv_nsec)
        self.execution_time += duration_ns
        if self._tracing_enabled:
            rows_emitted = self.records_out - records_out_before
            bytes_emitted = self.bytes_out - bytes_out_before
            with gil:
                self._append_trace(rows, rows_emitted, nbytes, bytes_emitted,
                                   duration_ns, 1 if rows_emitted > 0 else 0)

        self._morsel_index += 1
        return err.code if err != NULL else 0

    cdef int _dispatch_push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """Cdef hot path. Cdef-class subclasses override this directly for
        true C-level vtable dispatch. Default = transitional gil-adapter:
        re-acquire the GIL, decode the carrier to a Morsel (or recover the EOS
        sentinel from MorselState), and run the existing `_push_impl(Morsel)` so
        Python-class subclasses (aggregate/unnest/insert) keep working unchanged."""
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_impl(_EOS_SENTINEL)
                else:
                    self._push_impl(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx at the boundary
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cpdef void _push_impl(self, Morsel morsel) except *:
        """Override in Python-class subclasses (aggregates, joins, special
        operators that carry many Python attributes). Default is no-op."""
        pass

    cdef int _emit_cdef(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """Real downstream forwarder over the C++ carrier. Accounts emitted
        rows/bytes (read nogil from the CxxMorsel) and pushes into the downstream
        operator. `noexcept nogil`; errors propagate via `*err`. Converted
        true-nogil bodies call this directly; transitional gil-wrapped bodies
        reach it through the `emit(Morsel)` wrapper below."""
        cdef uint64_t rows = 0
        cdef uint64_t nbytes = 0
        cdef timespec ds_start, ds_end
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        if (not is_eos) and raw != NULL:
            rows = raw.num_rows()
            # Real per-vector footprint (string arena included), not rows×cols×8.
            nbytes = <uint64_t>cxx_morsel_nbytes(raw)
            self.records_out += rows
            self.bytes_out += nbytes
        if self._downstream is not None:
            if self._tracing_enabled:
                # Time the downstream chain so push() can report SELF time
                # (execution_time - downstream_time). Gated on tracing so normal
                # queries pay no extra clock_gettime; EXPLAIN ANALYZE enables it.
                clock_gettime(CLOCK_MONOTONIC, &ds_start)
                self._downstream.push(m, err)
                clock_gettime(CLOCK_MONOTONIC, &ds_end)
                self.downstream_time += (
                    (<uint64_t>(ds_end.tv_sec - ds_start.tv_sec)) * <uint64_t>1000000000
                    + <uint64_t>(ds_end.tv_nsec - ds_start.tv_nsec)
                )
            else:
                self._downstream.push(m, err)
        return err.code if err != NULL else 0

    cpdef void emit(self, Morsel morsel) except *:
        """GIL-side Morsel→carrier wrapper used by transitional gil-wrapped
        bodies (and Python-class operators). Encodes the Morsel (or EOS sentinel)
        as the C++ carrier and forwards via the nogil `_emit_cdef`. If a
        downstream operator failed, it stashed its exception on the shared context
        and returned a non-OK status — re-raise it here so the calling body
        unwinds and its own `except` re-stashes the (same) original exception."""
        cdef shared_ptr[CxxMorsel] cxm
        cdef ErrCtx e
        cdef object exc
        e.code = 0
        e.msg = NULL
        if morsel is None:
            return
        if morsel is _EOS_SENTINEL:
            cxm = shared_ptr[CxxMorsel](cxx_morsel_new_eos())
        else:
            cxm = morsel_to_cxx(morsel)
        self._emit_cdef(cxm, &e)
        if e.code != 0:
            exc = self._peek_exc()
            if exc is not None:
                raise exc
            raise RuntimeError("downstream push failed")

    # ---- Source-side iterator (used by drive_scan) ------------------------------
    cdef shared_ptr[CxxMorsel] next_morsel(self) except *:
        """Cdef hot-path source iterator over the C++ carrier. Returns the next
        morsel as a `shared_ptr[CxxMorsel]`, or a NULL shared_ptr on exhaustion.
        Stays GIL-requiring during S-B.1 (drive_scan pulls it outside the
        `with nogil` push); S-B.2 converts the scan body to true nogil.
        Cdef-class source operators override this directly; Python-class scans
        override `_next_morsel_py` (wrapped here)."""
        cdef object py = self._next_morsel_py()
        cdef shared_ptr[CxxMorsel] out
        if py is not None:
            out = morsel_to_cxx(<Morsel>py)
        return out

    cpdef object _next_morsel_py(self):
        """Default implementation: lazily wrap the source's existing
        `read_morsels()` generator. Source operators that need maximum
        performance can override with a state-machine impl, skipping the
        Python generator boundary altogether."""
        if self._morsel_iter is None:
            self._morsel_iter = iter(self.read_morsels())
        return next(self._morsel_iter, None)

    cpdef void close_source(self) except *:
        """Close the lazily-created source iterator backing `next_morsel`,
        running its finally-block cleanup (e.g. the rugo C++ IO pipeline
        shutdown in `iter_row_groups_ipc`). Called by `drive_scan` on every
        exit path — normal exhaustion, early termination, exception, or caller
        abandonment — so source-side resources never leak. Safe to call more
        than once and on operators that never created an iterator (state-machine
        source overrides leave `_morsel_iter` None and clean up themselves)."""
        cdef object it = self._morsel_iter
        if it is None:
            return
        self._morsel_iter = None
        close = getattr(it, "close", None)
        if close is not None:
            close()

    cpdef bint is_concurrent_pull_safe(self) except *:
        """May N worker threads call ``pull_one(self)`` (i.e. ``next_morsel``)
        CONCURRENTLY and each receive a distinct morsel, with no external lock?

        This is a CORRECTNESS capability, not a performance hint. The default is
        ``False``: the base source iterates a non-reentrant Python generator
        (``_next_morsel_py`` → ``read_morsels()``), so concurrent callers would
        re-enter the same generator and crash (``generator already executing``)
        or corrupt its state. Only a source whose ``next_morsel`` override is
        genuinely reentrant (its own internal mutex hands each caller a disjoint,
        already-decoded unit) may return ``True``. The parallel strategies use
        this to decide between lockless self-pull and a serialised (locked) pull;
        a ``False`` here forces the safe serialised path, never silent breakage."""
        return False

    # ---- Pipeline wiring (called by pipeline_compiler) --------------------------
    cpdef void set_downstream(self, BasePlanNode node) except *:
        self._downstream = node

    cpdef void set_context(self, PipelineContext ctx) except *:
        self._ctx = ctx

    cpdef void set_expected_input_closes(self, int n) except *:
        """Tell this operator how many upstream input chains will each push one
        EOS into it. Stamped by the pipeline compiler from the incoming-edge
        count. Operators that gate downstream EOS on all inputs closing use
        `_record_input_close()` to count down against this."""
        self._expected_input_closes = n

    cdef inline bint _record_input_close(self) except -1:
        """Record one upstream EOS. Returns True once every expected upstream
        input has closed (i.e. this was the final EOS), False otherwise."""
        self._seen_input_closes += 1
        return self._seen_input_closes >= self._expected_input_closes

    cpdef void enable_tracing(self, bint enabled) except *:
        self._tracing_enabled = enabled

    # ---- Trace buffer management (typed C buffer, no Python on hot path) -------
    cdef void _append_trace(self,
                            uint64_t rows_in, uint64_t rows_out,
                            uint64_t bytes_in, uint64_t bytes_out,
                            uint64_t duration_ns, int produced_output) except *:
        cdef Py_ssize_t new_cap
        cdef TraceEvent *new_buf
        if self._trace_count >= self._trace_capacity:
            new_cap = 64 if self._trace_capacity == 0 else self._trace_capacity * 2
            new_buf = <TraceEvent *>realloc(self._trace_buf, new_cap * sizeof(TraceEvent))
            if new_buf is NULL:
                raise MemoryError("trace buffer realloc failed")
            self._trace_buf = new_buf
            self._trace_capacity = new_cap
        self._trace_buf[self._trace_count].morsel_index = self._morsel_index
        self._trace_buf[self._trace_count].rows_in = rows_in
        self._trace_buf[self._trace_count].rows_out = rows_out
        self._trace_buf[self._trace_count].bytes_in = bytes_in
        self._trace_buf[self._trace_count].bytes_out = bytes_out
        self._trace_buf[self._trace_count].duration_ns = duration_ns
        self._trace_buf[self._trace_count].produced_output = produced_output
        self._trace_count += 1

    def get_trace_events(self):
        """Materialise the typed C trace buffer to Python dicts. Called by
        EXPLAIN ANALYZE at query end, NOT in the hot path."""
        cdef Py_ssize_t i
        events = []
        for i in range(self._trace_count):
            events.append({
                "morsel_index": self._trace_buf[i].morsel_index,
                "rows_in": self._trace_buf[i].rows_in,
                "rows_out": self._trace_buf[i].rows_out,
                "bytes_in": self._trace_buf[i].bytes_in,
                "bytes_out": self._trace_buf[i].bytes_out,
                "duration_ns": self._trace_buf[i].duration_ns,
                "produced_output": bool(self._trace_buf[i].produced_output),
            })
        return events


cdef class JoinNode(BasePlanNode):
    """Base class for joins. Two input sides — one feeds build, the other feeds
    probe (build is the LEFT side for every join except FilterJoinNode, which
    builds from the RIGHT). Subclasses override `push_left` and `push_right`
    instead of `_push_impl`. The single `_push_impl` is never called on a
    JoinNode directly; the pipeline compiler routes inputs through adapter nodes.

    Build-before-probe invariant: the engine must fully drain the build-side
    input (including its EOS) before any probe-side morsel arrives. Subclasses
    set `_build_complete = True` on build-side EOS (even when the build side is
    empty) and call `_require_build_complete()` on every probe-side push. A
    probe arriving early is a scheduler bug and raises rather than silently
    probing an absent or partial build table."""
    cdef public object left_readers
    cdef public object right_readers
    cdef public list left_relation_names
    cdef public list right_relation_names
    cdef public object on
    cdef public object _join_key_cast_plan
    cdef public bint _build_complete

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.is_join = True
        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []
        self.on = parameters.get("on")
        self._join_key_cast_plan = None
        self._build_complete = False

    cdef inline void _require_build_complete(self) except *:
        if not self._build_complete:
            from opteryx.exceptions import InvalidInternalStateError
            raise InvalidInternalStateError(
                f"{self.name}: probe-side input arrived before the build side "
                "completed - build-before-probe ordering invariant violated."
            )

    cdef int push_left(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """Build-side input over the C++ carrier. Subclasses override. MUST NOT
        emit EOS — build-side EOS finalises internal state only."""
        return err.code if err != NULL else 0

    cdef int push_right(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        """Probe-side input over the C++ carrier. Subclasses override. On EOS,
        call self.emit(EOS) to terminate the downstream chain."""
        return err.code if err != NULL else 0

    cdef inline void _account_input(self, shared_ptr[CxxMorsel] m, uint64_t dur_ns) noexcept nogil:
        """Roll one adapter-driven input push into the join's own counters.
        Joins are driven via JoinLeft/RightAdapter calling push_left/push_right
        directly (not the join's push()), so without this the join reports 0ms.
        Both adapters call this, so records_in/bytes_in/calls/execution_time
        accumulate the join's TOTAL input work (left build + right probe)."""
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        if (not is_eos) and raw != NULL:
            self.records_in += raw.num_rows()
            self.bytes_in += <uint64_t>raw.num_rows() * <uint64_t>raw.num_columns() * <uint64_t>8
        self.calls += 1
        self.execution_time += dur_ns

    @staticmethod
    def _join_numeric_target_type(left_type, right_type):
        from opteryx.types.logical_type import LogicalCategory, find_compatible_type
        numeric_types = (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL)
        if left_type not in numeric_types or right_type not in numeric_types:
            return None
        if left_type == right_type:
            return None
        return find_compatible_type([left_type, right_type])

    def _build_join_key_cast_plan(self):
        from opteryx.expression import NodeType, get_all_nodes_of_type
        if self._join_key_cast_plan is not None:
            return
        self._join_key_cast_plan = []
        if not self.on:
            return
        comparisons = get_all_nodes_of_type(self.on, (NodeType.COMPARISON_OPERATOR,))
        seen = set()
        for comparison in comparisons:
            if comparison.value != "Eq":
                continue
            left = comparison.left
            right = comparison.right
            if not left or not right:
                continue
            if left.node_type != NodeType.IDENTIFIER or right.node_type != NodeType.IDENTIFIER:
                continue
            if not left.schema_column or not right.schema_column:
                continue
            left_rel = left.source
            right_rel = right.source
            left_identity = left.schema_column.identity
            right_identity = right.schema_column.identity
            left_type = left.schema_column.category
            right_type = right.schema_column.category
            if left_rel in self.left_relation_names and right_rel in self.right_relation_names:
                left_column, right_column = left_identity, right_identity
            elif left_rel in self.right_relation_names and right_rel in self.left_relation_names:
                left_column, right_column = right_identity, left_identity
                left_type, right_type = right_type, left_type
            else:
                continue
            target_type = JoinNode._join_numeric_target_type(left_type, right_type)
            if target_type is None:
                continue
            signature = (left_column, right_column, target_type)
            if signature in seen:
                continue
            seen.add(signature)
            self._join_key_cast_plan.append({
                "left_column": left_column,
                "right_column": right_column,
                "target_type": target_type,
            })

    def _apply_join_key_casts(self, morsel, *, is_left: bool):
        from opteryx.types.logical_type import LogicalCategory, ColumnType
        if morsel is None or morsel is _EOS_SENTINEL:
            return morsel
        self._build_join_key_cast_plan()
        if not self._join_key_cast_plan:
            return morsel
        from draken.morsels.morsel import Morsel as _Morsel
        from opteryx.expression.casts import resolve_cast
        from draken.vectors.vector import Vector
        names = list(morsel.column_names)
        vectors = [morsel.column(n) for n in names]
        changed = False
        for cast_rule in self._join_key_cast_plan:
            column_name = cast_rule["left_column"] if is_left else cast_rule["right_column"]
            if column_name not in names:
                continue
            idx = names.index(column_name)
            target_type = cast_rule["target_type"]
            # Phase 2: target_type is ColumnType; compare via .category
            target_cat = target_type.category if isinstance(target_type, ColumnType) else target_type
            _join_tgt = None
            if target_cat == LogicalCategory.FLOAT:
                _join_tgt = "DOUBLE"
            elif target_cat == LogicalCategory.INTEGER:
                _join_tgt = "INTEGER"
            if _join_tgt is not None:
                # Resolve the exact native kernel from the column's physical type
                # (bind-style, once per join build — not per row), then apply it the
                # same way the bytecode executor does (input/result handling).
                v = vectors[idx]
                kern, needs_nb, returns_raw = resolve_cast(v.type.name, _join_tgt, (), None)
                inp = v._nb if (needs_nb and isinstance(v, Vector)) else v
                res = kern(inp)
                vectors[idx] = Vector(res) if returns_raw else res
                changed = True
            if changed:
                self.readings["feature_implicit_join_key_cast"] = \
                    self.readings.get("feature_implicit_join_key_cast", 0) + 1
        if not changed:
            return morsel
        return _Morsel.from_vectors(names, vectors)

    def to_mermaid(self, nid):
        mermaid = f'NODE_{nid}["**JOIN ({self.join_type.upper()})**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'


def drive_scan(BasePlanNode scan, BasePlanNode chain_head, exit_node, PipelineContext ctx):
    """Drive a single scan's morsels through the chain and yield exit-node
    pending morsels to the caller.

    Per-morsel hot path is all typed cdef-vtable dispatch:
      * `scan.next_morsel()` — typed source pull (no Python iter protocol)
      * `chain_head.push(...)` — typed push into the chain
      * `exit_node.has_pending()` / `pop_pending()` — typed pending drain

    One Python `yield` per emitted result morsel, not one per scan morsel.

    The body runs under try/finally: on EVERY exit path — normal exhaustion,
    LIMIT short-circuit, an exception raised anywhere in the chain, or the
    caller abandoning the result generator (GeneratorExit propagating through
    the `yield from` in the engine) — `scan.close_source()` runs so the
    source's own cleanup (rugo C++ pipeline shutdown, open file handles) is
    not leaked. The original exception/GeneratorExit is not suppressed."""
    cdef bint has_exit = exit_node is not None
    cdef timespec ns_start, ns_end
    # Carrier flip: the chain currency is `shared_ptr[CxxMorsel]` and `push` is
    # `noexcept nogil`, so the chain is driven with the GIL RELEASED (`with
    # nogil`). The source pull (`next_morsel`) still runs GIL-held and returns the
    # carrier directly (a NULL shared_ptr means exhausted); errors come back as a
    # status code in `err` and the stashed exception is re-raised here at the gil
    # boundary (status-code model — `except +` is unavailable on cdef-class
    # methods, validated by the S-B spike).
    cdef ErrCtx err
    cdef object _exc
    cdef shared_ptr[CxxMorsel] cxm

    try:
        while True:
            # Time the source pull so the scan operator gets a real
            # execution_time. Scans are driven via next_morsel(), not push(),
            # so without this they report 0ms in EXPLAIN ANALYZE. The scan does
            # not emit (drive_scan pushes for it), so it has no downstream_time
            # and its self_time == execution_time (pure read/decode).
            clock_gettime(CLOCK_MONOTONIC, &ns_start)
            cxm = scan.next_morsel()
            clock_gettime(CLOCK_MONOTONIC, &ns_end)
            scan.execution_time += (
                (<uint64_t>(ns_end.tv_sec - ns_start.tv_sec)) * <uint64_t>1000000000
                + <uint64_t>(ns_end.tv_nsec - ns_start.tv_nsec)
            )
            scan.calls += 1
            if cxm.get() == NULL:
                break
            if ctx.is_terminated():
                break
            err.code = 0
            err.msg = NULL
            with nogil:
                chain_head.push(cxm, &err)
            if err.code != 0:
                _exc = chain_head._take_exc()
                raise _exc if _exc is not None else RuntimeError("pipeline push failed")
            if has_exit:
                while exit_node.has_pending():
                    yield exit_node.pop_pending()
            if ctx.is_terminated():
                break

        if ctx.is_terminated():
            if has_exit:
                while exit_node.has_pending():
                    yield exit_node.pop_pending()
            return

        cxm = shared_ptr[CxxMorsel](cxx_morsel_new_eos())
        err.code = 0
        err.msg = NULL
        with nogil:
            chain_head.push(cxm, &err)
        if err.code != 0:
            _exc = chain_head._take_exc()
            raise _exc if _exc is not None else RuntimeError("pipeline push failed")
        if has_exit:
            while exit_node.has_pending():
                yield exit_node.pop_pending()
    finally:
        scan.close_source()


def drive_scan_to_sink(BasePlanNode scan, BasePlanNode chain_head, exit_node,
                       PipelineContext ctx, object sink, bint finish=True):
    """Native push drive to a MorselQueue sink — slice 3, option B. The push-loop
    twin of ``drive_scan``: same typed `next_morsel`/`push`/pending-drain hot path,
    but it PUSHES each output morsel into ``sink`` (a PyMorselQueue) instead of
    `yield`ing, so there is NO Python generator on the drive path.

    `sink.put(morsel)` converts to the C++ carrier and releases the GIL during the
    backpressure-blocked enqueue, so the consumer thread can dequeue; a False return
    means the consumer ABANDONED (LIMIT at the cursor / early close) → stop. On
    normal exhaustion the chain is flushed with EOS (so agg/sort emit their finals)
    and `sink.finish()` signals graceful end-of-data. `scan.close_source()` runs on
    every exit path (exhaustion, abandon, error)."""
    cdef ErrCtx err
    cdef object _exc, out
    cdef shared_ptr[CxxMorsel] cxm
    cdef bint has_exit = exit_node is not None
    cdef timespec ns_start, ns_end

    try:
        while True:
            clock_gettime(CLOCK_MONOTONIC, &ns_start)
            cxm = scan.next_morsel()
            clock_gettime(CLOCK_MONOTONIC, &ns_end)
            scan.execution_time += (
                (<uint64_t>(ns_end.tv_sec - ns_start.tv_sec)) * <uint64_t>1000000000
                + <uint64_t>(ns_end.tv_nsec - ns_start.tv_nsec)
            )
            scan.calls += 1
            if cxm.get() == NULL:
                break
            if ctx.is_terminated():
                break
            err.code = 0
            err.msg = NULL
            with nogil:
                chain_head.push(cxm, &err)
            if err.code != 0:
                _exc = chain_head._take_exc()
                raise _exc if _exc is not None else RuntimeError("pipeline push failed")
            if has_exit:
                while exit_node.has_pending():
                    out = exit_node.pop_pending()
                    if not sink.put(out):
                        return  # consumer abandoned (LIMIT / early close)
            if ctx.is_terminated():
                break

        if ctx.is_terminated():
            if has_exit:
                while exit_node.has_pending():
                    out = exit_node.pop_pending()
                    if not sink.put(out):
                        return
            if finish:
                sink.finish()
            return

        # Normal exhaustion: flush the chain with EOS so breakers emit their finals.
        cxm = shared_ptr[CxxMorsel](cxx_morsel_new_eos())
        err.code = 0
        err.msg = NULL
        with nogil:
            chain_head.push(cxm, &err)
        if err.code != 0:
            _exc = chain_head._take_exc()
            raise _exc if _exc is not None else RuntimeError("pipeline push failed")
        if has_exit:
            while exit_node.has_pending():
                out = exit_node.pop_pending()
                if not sink.put(out):
                    return
        if finish:
            sink.finish()
    finally:
        scan.close_source()


cdef inline shared_ptr[CxxMorsel] _carrier_from_py(object morsel):
    """Encode a Python Morsel / EOS sentinel as the C++ carrier. A NULL
    shared_ptr means `morsel was None` (nothing to push)."""
    cdef shared_ptr[CxxMorsel] cxm
    if morsel is _EOS_SENTINEL:
        cxm = shared_ptr[CxxMorsel](cxx_morsel_new_eos())
    elif morsel is not None:
        cxm = morsel_to_cxx(<Morsel>morsel)
    return cxm


def push_one_to_sink(BasePlanNode head, exit_node, object morsel, object sink):
    """Push one Morsel (or the EOS sentinel) through the chain ``head`` `nogil`, then
    drain ``exit_node``'s pending output into ``sink`` (a PyMorselQueue). Returns
    False if the consumer abandoned the sink (LIMIT / early close), else True. The
    native push replacement for the per-morsel ``push_one`` + ``out_q.put`` shim."""
    cdef ErrCtx err
    cdef object out, _exc
    cdef shared_ptr[CxxMorsel] cxm = _carrier_from_py(morsel)
    if cxm.get() == NULL:
        return True
    err.code = 0
    err.msg = NULL
    with nogil:
        head.push(cxm, &err)
    if err.code != 0:
        _exc = head._take_exc()
        raise _exc if _exc is not None else RuntimeError("pipeline push failed")
    if exit_node is not None:
        while exit_node.has_pending():
            out = exit_node.pop_pending()
            if not sink.put(out):
                return False
    return True


def stateless_worker_drive(BasePlanNode head, exit_node, object next_input,
                           PipelineContext ctx, object sink):
    """Native per-worker push loop for the parallel STATELESS shape (slice 4). Pulls
    via ``next_input()`` (shared scan + row-floor buffer under a lock), pushes each
    morsel `nogil` through the chain, drains the Exit clone's pending into ``sink``.
    No EOS push — byte-identical to the current ``_stateless_stream`` worker, whose
    Exit clones never see EOS (the empty-result schema morsel is emitted once by the
    original Exit). Returns on consumer abandonment."""
    cdef object morsel
    while True:
        morsel = next_input()
        if morsel is None:
            break
        if ctx.is_terminated():
            break
        if not push_one_to_sink(head, exit_node, morsel, sink):
            return


def accumulate_worker_drive(BasePlanNode head, object next_input, PipelineContext ctx):
    """Native per-worker ACCUMULATE loop for barrier breakers (agg / distinct / GROUP
    BY). Pulls via ``next_input()`` and pushes each morsel `nogil` through a chain
    whose tail is a breaker clone that ACCUMULATES into its private engine (no emit,
    no downstream) — so there is no sink and no drain. Returns the total row count
    pushed (the worker's ``local_rows``). The native replacement for the breaker
    worker's per-morsel ``push_one``."""
    cdef object morsel
    cdef long long count = 0
    while True:
        morsel = next_input()
        if morsel is None:
            break
        # exit_node=None / sink=None → push only, the breaker accumulates internally.
        push_one_to_sink(head, None, morsel, None)
        count += morsel.num_rows
    return count


cdef struct _AccArg:
    PyObject* head      # worker k's pre-cloned chain head (borrowed)
    PyObject* source    # the shared self-pull source callable (borrowed)
    PyObject* ctx       # the PipelineContext (borrowed)
    PyObject* counts    # shared result list[int]  (borrowed)
    PyObject* errors    # shared result list[exc]  (borrowed)
    int index


cdef void _acc_worker_run(_AccArg* a) noexcept with gil:
    """GIL-held body of the native worker task — holds the Python locals a nogil
    function cannot. Drives one pre-cloned breaker chain via the native ACCUMULATE
    loop (whose per-morsel push releases the GIL again), recording the row count or
    any exception into the shared result lists by index."""
    cdef object exc
    cdef long long count
    try:
        count = accumulate_worker_drive(
            <BasePlanNode>(<object>a.head),
            <object>a.source,
            <PipelineContext>(<object>a.ctx),
        )
        (<object>a.counts)[a.index] = count
    except BaseException as exc:
        (<object>a.errors)[a.index] = exc


cdef void _acc_worker_task(void* arg) noexcept nogil:
    """Native task entry (matches ``native_task_fn``) submitted to
    ``CppThreadPool.submit_native`` — no Python worker closure, no Future. Casts the
    opaque arg and hands to the GIL-held body."""
    _acc_worker_run(<_AccArg*>arg)


def native_accumulate_fanout(CppThreadPool pool, list heads, object source,
                             PipelineContext ctx, list counts, list errors):
    """Native W-way ACCUMULATE fan-out: submit one NATIVE task per pre-cloned worker
    chain to ``pool`` (no Python worker closure, no Future), then barrier on
    ``wait_native``. ``heads[k]`` is worker k's chain head; ``source`` is the shared
    self-pull callable every worker drains disjointly. Row counts land in ``counts[k]``
    and exceptions in ``errors[k]`` — pre-sized length-W lists the caller owns, whose
    elements stay alive across the blocking wait. The native replacement for the
    breaker skeleton's ``[pool.submit(worker, k) for k in range(W)]`` loop + barrier.

    The per-worker ``_AccArg`` structs hold BORROWED PyObject pointers into the caller's
    live lists; the blocking ``wait_native`` guarantees every task has finished reading
    them before the array is freed, so no refcount churn is needed."""
    cdef Py_ssize_t W = len(heads)
    cdef _AccArg* args = <_AccArg*>malloc(W * sizeof(_AccArg))
    if args == NULL:
        raise MemoryError()
    cdef Py_ssize_t k
    try:
        for k in range(W):
            args[k].head = <PyObject*>heads[k]
            args[k].source = <PyObject*>source
            args[k].ctx = <PyObject*>ctx
            args[k].counts = <PyObject*>counts
            args[k].errors = <PyObject*>errors
            args[k].index = <int>k
            pool.submit_native(_acc_worker_task, &args[k])
        with nogil:
            pool.wait_native()
    finally:
        free(args)


cdef struct _ReadoutArg:
    PyObject* breaker   # the ORIGINAL breaker (owns readout_partition) (borrowed)
    PyObject* chunks    # this partition's raw scattered chunk list (borrowed)
    PyObject* ctx       # the PipelineContext (borrowed)
    PyObject* engines   # shared result list[engine] (borrowed)
    PyObject* counts    # shared result list[int]    (borrowed)
    PyObject* errors    # shared result list[exc]    (borrowed)
    int index


cdef void _readout_run(_ReadoutArg* a) noexcept with gil:
    """GIL-held body of the native READ-OUT task: call the breaker's operator-owned
    ``readout_partition`` for one global hash partition (key its chunks into a fresh
    engine), recording ``(engine, row_count)`` or any exception by index. The breaker
    is reached via the PyObject seam (its concrete type varies — grouped agg / distinct
    both expose ``readout_partition``)."""
    cdef object exc, result
    try:
        result = (<object>a.breaker).readout_partition(
            <object>a.chunks, <PipelineContext>(<object>a.ctx)
        )
        (<object>a.engines)[a.index] = result[0]
        (<object>a.counts)[a.index] = result[1]
    except BaseException as exc:
        (<object>a.errors)[a.index] = exc


cdef void _readout_task(void* arg) noexcept nogil:
    """Native task entry (matches ``native_task_fn``) for the READ-OUT fan-out."""
    _readout_run(<_ReadoutArg*>arg)


def native_readout_fanout(CppThreadPool pool, breaker, list chunk_lists,
                          PipelineContext ctx, list engines, list counts, list errors):
    """Native per-partition READ-OUT fan-out (HASH_REPARTITION recombination): submit
    one NATIVE task per global hash partition to ``pool`` (no Python worker closure, no
    Future), each calling ``breaker.readout_partition(chunk_lists[p], ctx)`` to key that
    partition into a fresh engine, then barrier on ``wait_native``. Engines land in
    ``engines[p]``, row counts in ``counts[p]``, faults in ``errors[p]`` (pre-sized
    length-R lists the caller owns). The native replacement for the read-out pool's
    ``[rpool.submit(readout_worker, p) ...]`` loop. Borrowed PyObject pointers are kept
    alive by the caller's live lists across the blocking wait."""
    cdef Py_ssize_t R = len(chunk_lists)
    cdef _ReadoutArg* args = <_ReadoutArg*>malloc(R * sizeof(_ReadoutArg))
    if args == NULL:
        raise MemoryError()
    cdef Py_ssize_t p
    try:
        for p in range(R):
            args[p].breaker = <PyObject*>breaker
            args[p].chunks = <PyObject*>chunk_lists[p]
            args[p].ctx = <PyObject*>ctx
            args[p].engines = <PyObject*>engines
            args[p].counts = <PyObject*>counts
            args[p].errors = <PyObject*>errors
            args[p].index = <int>p
            pool.submit_native(_readout_task, &args[p])
        with nogil:
            pool.wait_native()
    finally:
        free(args)


cdef class NativeFanoutHandle:
    """Keeps a STREAMING fan-out's per-worker arg array alive while the workers run
    asynchronously (they read the borrowed pointers after the fan-out returns). The
    caller holds this handle until it has drained every worker's sink-finish, then drops
    it — ``__dealloc__`` frees the array. Streaming fan-outs cannot block to free the
    array themselves (the consumer must drain concurrently or backpressure deadlocks)."""
    cdef void* _args

    def __cinit__(self):
        self._args = NULL

    def __dealloc__(self):
        if self._args != NULL:
            free(self._args)
            self._args = NULL


cdef void _scan_pull_run(void* scan_ptr, shared_ptr[CxxMorsel]* out,
                         int* finished, int* err_code) noexcept with gil:
    """WP-INSTR timing shim over the trampoline body. When the engine
    instrumentation is disarmed this is a single branch straight into
    ``_scan_pull_run_inner`` (the real GIL-held pull); when armed it brackets the
    body with a monotonic clock so ``gil_held_ns`` / ``worker_gil_sites`` capture
    every per-morsel, per-worker Python re-entry of the StreamingScanSource."""
    cdef long long _t0
    if not _gil_instr_enabled:
        _scan_pull_run_inner(scan_ptr, out, finished, err_code)
        return
    _t0 = _instr_mono_ns()
    _scan_pull_run_inner(scan_ptr, out, finished, err_code)
    _instr_record(_SITE_SCAN_PULL, _instr_mono_ns() - _t0)


cdef void _scan_pull_run_inner(void* scan_ptr, shared_ptr[CxxMorsel]* out,
                               int* finished, int* err_code) noexcept:
    """GIL-held body of the streaming Source trampoline — holds the Python locals a
    nogil function cannot. Calls the existing native scan's ``next_morsel()`` ON
    DEMAND. Skips EOS-state morsels internally (mirrors the demo bridges' pull loops)
    so the C++ side only ever sees real data or genuine exhaustion. A raised exception
    is recorded via ``err_code`` (1); the caller surfaces the real Python exception,
    stashed on the scan node by the existing ``_take_exc``/``_cxx_push_exc``
    contract, at the GIL boundary after the run."""
    cdef shared_ptr[CxxMorsel] cxm
    cdef object scan_obj
    cdef object exc
    try:
        scan_obj = <object><PyObject*>scan_ptr
        while True:
            cxm = (<BasePlanNode>scan_obj).next_morsel()
            if cxm.get() == NULL:
                finished[0] = 1
                err_code[0] = 0
                return
            if cxm.get().state == MorselState.END_OF_STREAM:
                continue
            # Strip any Python-object-backed column ownership before this morsel
            # crosses into the C++ Source (StreamingScanSource::get_morsel's `out`):
            # a fresh, plain-C++-owned copy has no py_deleter, so a concurrent
            # worker tearing down a DIFFERENT pulled morsel can never race a
            # Py_DECREF against this one under free-threaded builds. See
            # cxx_morsel_materialize_native in draken/draken_native.cpp.
            cxm = shared_ptr[CxxMorsel](cxx_morsel_materialize_native_c(cxm.get()))
            out[0] = cxm
            finished[0] = 0
            err_code[0] = 0
            return
    except BaseException as exc:
        finished[0] = 1
        err_code[0] = 1
        # Stash the real Python exception on the scan node so the consumer-side
        # `build_terminal_exc` can re-raise it (rich traceback) instead of the
        # synthetic "scan pull raised" RuntimeError. First exception wins; the
        # `_ctx._exc` slot is preferred (matches `_take_exc`), else the node-local
        # `_cxx_push_exc` fallback (a scan driven without a shared PipelineContext).
        if scan_obj is not None:
            if (<BasePlanNode>scan_obj)._ctx is not None:
                if (<BasePlanNode>scan_obj)._ctx._exc is None:
                    (<BasePlanNode>scan_obj)._ctx._exc = exc
            elif (<BasePlanNode>scan_obj)._cxx_push_exc is None:
                (<BasePlanNode>scan_obj)._cxx_push_exc = exc


cdef void _scan_pull_trampoline(void* scan_ptr, shared_ptr[CxxMorsel]* out,
                                int* finished, int* err_code) noexcept nogil:
    """Native entry (matches ``ScanPullFn``) for ``StreamingScanSource`` — the REAL
    (non-demo) cutover's streaming pull-on-demand callback, called from any worker
    thread once per requested morsel. ``scan_ptr`` is a BORROWED ``PyObject*`` (the
    caller's Python stack frame holds the real reference for the run's duration,
    exactly like the slice 5a-d demo bridges' borrowed pointers)."""
    _scan_pull_run(scan_ptr, out, finished, err_code)


cpdef BasePlanNode spawn_worker(BasePlanNode op):
    """Python-callable edge over the cdef `make_worker` contract — the fan-out
    replacement for `_clone_op`. Used by the (still-Python) scheduler at worker
    fan-out; once the scheduler is native it calls `make_worker` directly.

    Backstop for the partition-parallel contract: an operator whose STATE carries
    global semantics that CANNOT be data-partitioned per worker (a running window
    counter, a global DISTINCT set, a union schema/leg-count) declares
    `is_partition_parallel() == False`. Cloning one across workers would change the
    ANSWER — so fanning it out is a scheduler bug. Fail loud here rather than
    silently mis-split (the old `_clone_op` did the latter)."""
    if not op.is_partition_parallel():
        from opteryx.exceptions import InvalidInternalStateError
        raise InvalidInternalStateError(
            f"{type(op).__name__} carries global-semantics state and cannot be "
            f"data-partitioned across workers; the scheduler must run it "
            f"serial/merge-only, never fan it out."
        )
    return op.make_worker()


cpdef bint operator_is_partition_parallel(BasePlanNode op):
    """Python-callable edge over the cdef `is_partition_parallel` marker."""
    return op.is_partition_parallel()


cpdef void operator_resolve_schema(BasePlanNode op, object input_schema) except *:
    """Python-callable edge over the cdef `resolve_schema` bind-time hook."""
    op.resolve_schema(input_schema)


cpdef void push_one(BasePlanNode head, object morsel) except *:
    """Python-callable driver: push one Morsel (or EOS sentinel) into a chain
    head over the C++ carrier. The sanctioned Morsel→chain entry for Python
    drivers other than drive_scan — the M4 parallel engine's worker threads and
    direct-push unit tests. Encodes under the GIL, pushes with the GIL released,
    and re-raises any stashed pipeline exception at this boundary."""
    cdef shared_ptr[CxxMorsel] cxm = _carrier_from_py(morsel)
    cdef ErrCtx err
    cdef object _exc
    if cxm.get() == NULL:
        return
    err.code = 0
    err.msg = NULL
    with nogil:
        head.push(cxm, &err)
    if err.code != 0:
        _exc = head._take_exc()
        raise _exc if _exc is not None else RuntimeError("pipeline push failed")


cpdef object pull_one(BasePlanNode scan):
    """Python-callable concurrent-pull entry: pull one morsel from `scan` and
    return it as a Python Morsel, or None on exhaustion.

    The sanctioned pull entry for the M4 morsel-driven scheduler's worker
    threads. It calls the scan's typed `next_morsel()` override (the
    thread-safe concurrent-pull path for single-pass parquet — N workers may
    call this on the SAME scan and receive disjoint morsels), NOT the
    `_next_morsel_py` generator wrapper (which is not reentrant). The pull
    itself is GIL-held today (S-B.2 makes it nogil); the decode below is
    parallel and the nogil ingest the puller feeds overlaps across workers."""
    cdef shared_ptr[CxxMorsel] cxm = scan.next_morsel()
    if cxm.get() == NULL:
        return None
    return cxx_to_morsel(cxm)


cpdef void push_left_one(JoinNode join, object morsel) except *:
    """Python-callable driver for a join's build-side input (see push_one)."""
    cdef shared_ptr[CxxMorsel] cxm = _carrier_from_py(morsel)
    cdef ErrCtx err
    cdef object _exc
    if cxm.get() == NULL:
        return
    err.code = 0
    err.msg = NULL
    with nogil:
        join.push_left(cxm, &err)
    if err.code != 0:
        _exc = join._take_exc()
        raise _exc if _exc is not None else RuntimeError("pipeline push failed")


cpdef void push_right_one(JoinNode join, object morsel) except *:
    """Python-callable driver for a join's probe-side input (see push_one)."""
    cdef shared_ptr[CxxMorsel] cxm = _carrier_from_py(morsel)
    cdef ErrCtx err
    cdef object _exc
    if cxm.get() == NULL:
        return
    err.code = 0
    err.msg = NULL
    with nogil:
        join.push_right(cxm, &err)
    if err.code != 0:
        _exc = join._take_exc()
        raise _exc if _exc is not None else RuntimeError("pipeline push failed")


# =====================================================================================
# THE ENGINE — general pipeline-graph execution (engine.hpp). Built by the plan
# compiler (managers/execution/compiler.py) at PLAN time through NativePlan's builder
# methods; run by ONE detached native driver task streaming into the production
# MorselQueue. This subsumed (and replaced) the four narrow native_engine_real_* entry
# points, which have been removed.
# =====================================================================================

cdef int _expr_filter_tramp(void* instrs, int count, const CxxMorsel* m,
                            int* col_idx, void** lit_dv,
                            int* const_col_idx, void** const_scalar_dv, int n_consts,
                            CxxMorsel** out_filtered, int* err_op,
                            const char** err_msg) noexcept nogil:
    """Native entry (matches ExprFilterFn) for ExprFilterOperator — the pure-nogil
    predicate span in evaluation.pyx. No PyObject inside. n_consts == 0 (no
    `IDENTIFIER = LITERAL` const-replacements on this predicate) costs nothing extra
    — _dv_filter_span_with_consts_cxx's const-scan loop is a no-op per column."""
    return _dv_filter_span_with_consts_cxx(
        <BytecodeInstr*>instrs, count, m, col_idx, <DrakenVector**>lit_dv,
        <int32_t*>const_col_idx, <DrakenVector**>const_scalar_dv, <uint32_t>n_consts,
        out_filtered, err_op, err_msg)


cdef int _expr_eval_tramp(void* instrs, int count, const CxxMorsel* m,
                          int* col_idx, void** lit_dv,
                          DrakenVector* out_vec, void** out_data,
                          uint8_t** out_validity, void** out_sel,
                          int* err_op, const char** err_msg,
                          VecResult** out_child) noexcept nogil:
    """Native entry (matches ExprEvalFn) for ExprProjectOperator — the pure-nogil
    computed-column span in evaluation.pyx. Force-densifies the result (the default
    boundary). No PyObject inside."""
    return _dv_eval_span_cxx(<BytecodeInstr*>instrs, count, m, col_idx,
                             <DrakenVector**>lit_dv, out_vec, out_data,
                             out_validity, out_sel, err_op, err_msg, False, out_child)


cdef int _expr_eval_preserve_tramp(void* instrs, int count, const CxxMorsel* m,
                                   int* col_idx, void** lit_dv,
                                   DrakenVector* out_vec, void** out_data,
                                   uint8_t** out_validity, void** out_sel,
                                   int* err_op, const char** err_msg,
                                   VecResult** out_child) noexcept nogil:
    """Shape-PRESERVING ExprEvalFn twin of _expr_eval_tramp — keeps a compressed
    result's encoding (dict/constant) instead of force-densifying. Selected by
    add_expr_project(preserve_shape=True) for computed GROUP BY / DISTINCT keys, whose
    only consumer (the group/distinct sink) is compression-aware. A distinct fn
    pointer also keeps these columns from fusing with dense ExprProject columns in the
    engine's ExprMultiProjectOperator (fusion requires an identical fn). ARRAY results
    are NOT supported on this path (_dv_copy_result_preserve_shape rejects DRAKEN_ARRAY,
    matching today's behaviour) — *out_child always comes back NULL here."""
    return _dv_eval_span_cxx(<BytecodeInstr*>instrs, count, m, col_idx,
                             <DrakenVector**>lit_dv, out_vec, out_data,
                             out_validity, out_sel, err_op, err_msg, True, out_child)


cdef int _resolve_bc_for_layout(CompiledBytecode bc, list layout,
                                cppvector[int]& col_idx,
                                cppvector[void*]& lit_dv) except -1:
    """PLAN-TIME resolve (the compiler's layout replaces the first-morsel resolve the
    old engine needed): LOAD_COL identity -> column index in ``layout`` (bytes
    identities, stream order); LOAD_LIT_CONST -> the bind-time-materialized literal
    Vector's DrakenVector*. Returns 0, or raises — a missing column at plan time is
    a compiler bug, fail loud."""
    cdef Py_ssize_t k
    cdef BytecodeInstr* slot
    cdef bytes ident
    cdef object scalar_obj
    cdef int ci
    for k in range(bc.count):
        slot = &bc.instrs[k]
        ci = -1
        lit_dv.push_back(NULL)
        if slot.opcode == BC_LOAD_COL or (
                (slot.opcode == BC_CAST or slot.opcode == BC_FUNCTION
                 or slot.opcode == BC_EXTRACTION)
                and (slot.flags & BC_C_NATIVE_CHILD) != 0):
            # BC_C_NATIVE_CHILD instructions (ARRAY->VARCHAR cast, SORT, arr[i])
            # carry the ARRAY operand's identity — the eval span resolves the
            # owner-held child element vector through it.
            ident = <bytes>slot.column_identity
            try:
                ci = <int>layout.index(ident)
            except ValueError:
                raise KeyError(
                    f"native engine: expression references column {ident!r} which the "
                    f"stream does not carry (layout: {layout!r})")
        elif slot.opcode == BC_LOAD_LIT_CONST:
            scalar_obj = <object>slot.literal_obj
            lit_dv[lit_dv.size() - 1] = <void*>(<Vector>scalar_obj).unified()
        col_idx.push_back(ci)
    return 0


def bytecode_is_all_c_native(CompiledBytecode bc):
    """PLAN-TIME check for the compiler (the flag is a non-public cdef field): True
    when every instruction runs on the C-native DV* path AND the program ends in a
    compute op — the filter span's contract."""
    return bool(bc.is_all_c_native)


def bytecode_is_c_native_predicate(CompiledBytecode bc):
    """PLAN-TIME filter admission: every instruction c-native (DESC results
    included — intermediates fold raw) AND the FINAL op produces a boolean mask
    (compare / bool algebra / IS NULL) — cxx_mask_c's contract."""
    if not bytecode_ops_all_c_native(bc):
        return False
    cdef int last = bc.instrs[bc.count - 1].opcode
    if last in (BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF,
                BC_COMPARE, BC_UNARY_OP):
        return True
    # BC_LOAD_LIT_BOOL is the ONE load that is already a mask: it materialises a
    # DENSE `num_rows`-wide bitmap in the frame arena (c_execute_dv_inner), which
    # is precisely cxx_mask_c's contract — no other load qualifies, because a
    # BC_LOAD_COL bool column may arrive dict- or constant-shaped. A predicate
    # that constant-folds to a single literal reaches the filter as exactly this
    # one-instruction program: `WHERE 1 = 0`, an integer column compared against
    # a non-integral literal (`id = 4.5`), and the empty visibility-filter set
    # (apply_visibility_filters' TRUE=FALSE block-everything node). Constant TRUE
    # never gets here — ConstantFoldingStrategy deletes that Filter outright —
    # so before this the FALSE half of the same fold was simply unrunnable.
    if last == BC_LOAD_LIT_BOOL:
        return True
    # Bool-returning C-ABI function kernels (LIKE family) — marked at bind time
    # with BC_RESULT_WRAP_AS_BOOL (0x20, compiled_expression.pyx flag contract).
    return (last == BC_FUNCTION
            and (bc.instrs[bc.count - 1].flags & 0x20) != 0)


def bytecode_ops_all_c_native(CompiledBytecode bc):
    """PLAN-TIME check for PROJECTION programs: every instruction runs on the
    C-native DV* path, but the program may END in a load (a plain constant / column
    projection) — _dv_eval_span_cxx deep-copies whatever the result DV is, so the
    "last op must be an arena compute" restriction the GIL path needs does not
    apply. Mirrors build_bytecode's per-instruction admission exactly."""
    return _first_non_c_native(bc) < 0


# Opcode -> the operation a reader recognises, for the refusal message only.
# Not a debug aid: the gate this feeds is the ONLY thing standing between an
# unsupported expression and the user, and "outside the c-native kernel set"
# with no operation named made every refusal look identical.
_BC_OP_NAMES = {
    1: "a column load", 2: "a boolean literal", 3: "a scalar literal",
    4: "a set literal", 5: "AND", 6: "OR", 7: "XOR", 8: "NOT",
    9: "a DNF predicate", 10: "a CNF predicate", 11: "a comparison",
    13: "an arithmetic/binary operator", 14: "a unary operator",
    15: "a function call", 16: "a subscript/extraction (-> ->> [i])",
    17: "a CAST", 18: "a CASE", 19: "a constant literal",
}


def bytecode_non_c_native_op(CompiledBytecode bc):
    """The operation that made `bytecode_ops_all_c_native` say no, named — or ""
    when the program IS admissible.

    Shares `_first_non_c_native` with the gate itself, so the message can never
    name a different instruction than the one that was actually refused."""
    if bc.count == 0:
        return "an empty program"
    cdef Py_ssize_t k = _first_non_c_native(bc)
    if k < 0:
        return ""
    return _BC_OP_NAMES.get(bc.instrs[k].opcode, f"opcode {bc.instrs[k].opcode}")


cdef Py_ssize_t _first_non_c_native(CompiledBytecode bc):
    """Index of the first instruction the C-native path cannot run, or -1 when
    every instruction is admissible. The SINGLE admission loop — both the gate
    and the message that explains it read this, so they cannot disagree."""
    if bc.count == 0:
        return 0
    cdef Py_ssize_t k
    cdef int op, fl, opc
    for k in range(bc.count):
        op = bc.instrs[k].opcode
        fl = bc.instrs[k].flags
        if op in (BC_LOAD_COL, BC_LOAD_LIT_CONST, BC_LOAD_LIT_BOOL,
                  BC_AND, BC_OR, BC_XOR, BC_NOT, BC_DNF, BC_CNF):
            continue
        if op == BC_COMPARE:
            opc = bc.instrs[k].op_code
            if (fl & BC_CMP_INLIST_INLINE) == 0 and 1 <= opc <= 6:
                continue
            return k
        if op == BC_UNARY_OP:
            opc = bc.instrs[k].op_code
            if (opc == UOP_IS_NULL or opc == UOP_IS_NOT_NULL
                    or opc == UOP_IS_TRUE or opc == UOP_IS_FALSE
                    or opc == UOP_IS_NOT_TRUE or opc == UOP_IS_NOT_FALSE):
                continue
            return k
        if op == BC_BINARY_OP or op == BC_CAST:
            if (fl & BC_INSTR_C_NATIVE) != 0 and \
                    (fl & (BC_C_NATIVE_FIXED | BC_C_NATIVE_STRING
                           | BC_C_NATIVE_DESC | BC_C_NATIVE_ARRAY)) != 0:
                continue
            return k
        if op == BC_FUNCTION:
            if (fl & BC_INSTR_C_NATIVE) != 0:
                continue
            return k
        if op == BC_EXTRACTION:
            # `->`, `->>` and str[i] carry a resolved C-ABI kernel whose path/index is
            # bound into extraction_ctx. arr[i] additionally carries the operand's
            # identity (BC_C_NATIVE_CHILD) so the VM can resolve the owner-held child;
            # it sets the flag only when its operand is a column, which is what the
            # compiler's array hoist guarantees.
            if (fl & BC_INSTR_C_NATIVE) != 0:
                continue
            return k
        return k
    return -1


cdef class NativePlan:
    """The compiled-native execution plan: owns the C++ ``Engine`` pipeline graph plus
    the Python references (scan plan nodes, compiled expression programs) whose
    lifetimes must span the run. Builder methods are called by the plan compiler —
    planning, not the hot path; every value crossing here is a plain int/float/bytes
    decided at plan time."""
    cdef Engine* _e
    cdef public list scans   # BasePlanNode scan objects StreamingScanSource borrows
    cdef public list held    # CompiledBytecode programs (own instrs + literal vectors)
    cdef public list scan_plans  # NativeScanPlan objects NativeParquetScanSource borrows

    def __cinit__(self):
        self._e = new Engine()
        self.scans = []
        self.held = []
        self.scan_plans = []

    def __dealloc__(self):
        if self._e != NULL:
            del self._e
            self._e = NULL

    def set_current_identity(self, ident):
        """Tag every operator/source/sink built after this call with ``ident`` (the
        plan-node identity), so ``collect_op_stats`` can attribute the per-operator
        readings back to the plan node. The compiler calls this once per node."""
        cdef string s = (ident if isinstance(ident, bytes) else (<str>ident).encode("utf-8"))
        self._e.set_current_identity(s)

    def set_current_display_name(self, name):
        """Tag every operator/source/sink built after this call with ``name`` (a
        human-readable plan-node kind, e.g. "FilterNode") for trace-symbol display —
        see collect_trace_symbols. Purely cosmetic: identity (set_current_identity)
        stays the correlation key; this is never compared or summed on. Call
        alongside set_current_identity for the same plan node, not instead of it."""
        cdef string s = (name if isinstance(name, bytes) else (<str>name).encode("utf-8"))
        self._e.set_current_display_name(s)

    def collect_op_stats(self):
        """Harvest the per-operator execution telemetry accumulated during ``run``.
        Returns a list of dicts, one per source/operator/sink; callers sum by
        ``identity`` (several rows can share one plan-node identity). Must be called
        AFTER the run has drained — the counters accumulate as morsels flow."""
        cdef cppvector[OpReading] rows = self._e.collect_op_stats()
        cdef OpReading r
        out = []
        for r in rows:
            out.append({
                "identity": r.identity.decode("utf-8"),
                "role": r.role.decode("utf-8"),
                "calls": int(r.calls),
                "records_in": int(r.rows_in),
                "records_out": int(r.rows_out),
                "bytes_in": int(r.bytes_in),
                "bytes_out": int(r.bytes_out),
                "execution_time": int(r.exec_ns),
                "cpu_time": int(r.cpu_ns),
            })
        return out

    def collect_trace_symbols(self):
        """node_id -> human-readable display name (e.g. "FilterNode", set via
        set_current_display_name) for this plan's operators/sources/sinks, resolving
        the compact ids carried on drained TraceSpans (see native_trace_drain).
        Falls back to identity (the opaque correlation key) only for untagged call
        sites. Same call-after-run precondition as collect_op_stats."""
        cdef cppvector[pair[uint32_t, string]] rows = self._e.collect_trace_symbols()
        cdef pair[uint32_t, string] kv
        out = {}
        for kv in rows:
            out[int(kv.first)] = kv.second.decode("utf-8")
        return out

    def new_pipeline(self):
        return self._e.new_pipeline()

    def new_buffer(self):
        return self._e.new_buffer()

    def set_scan_source(self, size_t p, scan, bint serialize_pull=False):
        """Source = the existing native scan, pulled via the GIL trampoline — the ONE
        tracked execution-path Python touch (see engine_cutover_decisions memory).
        ``serialize_pull`` mutex-serialises the pull for scans that are not
        concurrent-pull safe (two-pass latmat); operators/sink stay parallel."""
        self.scans.append(scan)
        self._e.set_scan_source(p, <void*><PyObject*>scan, _scan_pull_trampoline,
                                serialize_pull)

    def set_native_scan_source(self, size_t p, NativeScanPlan splan, object row_limit=None):
        """Source = the fully-native parquet scan (NativeParquetScanSource): workers
        pull decoded row groups straight from the rugo IO pipeline — no GIL
        trampoline, no per-morsel thread attach. Only reachable when the plan-time
        gate (``native_scan_supported``) proved every projected column eligible.
        The Source borrows every pointer from ``splan``; this plan holds it alive
        and ``close_scan_plans`` tears it down only after the driver is done."""
        self.scan_plans.append(splan)
        # The pool is wired regardless (its DK_POOL string path is data-driven);
        # `string_types` tags every projected string column with its declared
        # DrakenType and flags which DK_POOL columns are VARCHAR. An empty plan
        # (no row groups) never allocates a pool — pass NULL; the Source finishes
        # immediately without ever routing a column to the pool path.
        cdef CppMemoryPool* pool_ptr = NULL
        if splan._pool is not None:
            pool_ptr = splan._pool._pool
        # WP-11: `decimal_columns` routes int64-backed DECIMAL DK_POOL columns to
        # the decimal decoder; `logical_coerce` carries the DATE/TIMESTAMP/TIME/
        # DECIMAL retag kind + unit / precision-scale so those projections land
        # byte-identically to the trampoline scan.
        # R6: `array_columns` does the same job for ARRAY (parquet LIST) columns —
        # they always land DK_POOL (repetition levels ⇒ no direct kind), and the
        # flag is what distinguishes that blob from the decimal / varchar pool
        # shapes so the right native decoder claims it.
        # R2: `row_limit` is the scan-pushed LIMIT (None → -1, unlimited). The scan
        # enforces it itself — LimitPushdownStrategy removes the Limit node from the
        # plan when it pushes, so nothing downstream truncates.
        cdef int64_t c_row_limit = -1
        if row_limit is not None:
            c_row_limit = <int64_t>row_limit
        self._e.set_native_scan_source(p, splan.pipeline_ptr, splan.footer_map,
                                       &splan.work_items, &splan.column_names,
                                       splan.in_flight_limit,
                                       pool_ptr, &splan.string_types,
                                       &splan.decimal_columns, &splan.logical_coerce,
                                       &splan.hash_key_columns, &splan.array_columns,
                                       c_row_limit)

    def set_latmat_scan_source(self, size_t p, NativeScanPlan p1_plan,
                               NativeScanPlan p2_plan, size_t pred_fn, size_t pred_ctx,
                               object pred_anchor, list pred_col_to_p1,
                               int sort_p1_index, bint sort_ascending,
                               int64_t topn_limit, list out_from_p1, list out_from_p2,
                               list out_names):
        """Source = the R3 two-pass late-materialization parquet scan
        (LatmatScanSource): pass 1 decodes the predicate + sort-key columns for the
        whole table and reduces the survivors to the top-n boundary; pass 2 decodes the
        remaining projected columns for only those rows, masked. Replaces the
        `fused_topn` trampoline residual — the composed
        `WHERE ... ORDER BY ... LIMIT` shape.

        ``p1_plan`` / ``p2_plan`` are two NativeScanPlans over the SAME files and the
        same pruning, split by column set; both are held here so their pipelines,
        footer map and pool outlive the run. ``pred_anchor`` is the Pass1PredResolver
        that owns ``pred_ctx`` — held for the same reason (the C ABI callback
        dereferences it from native worker threads, with no reference of its own)."""
        self.scan_plans.append(p1_plan)
        self.scan_plans.append(p2_plan)
        self.held.append(pred_anchor)
        cdef CppMemoryPool* p1_pool = NULL
        cdef CppMemoryPool* p2_pool = NULL
        if p1_plan._pool is not None:
            p1_pool = p1_plan._pool._pool
        if p2_plan._pool is not None:
            p2_pool = p2_plan._pool._pool
        cdef cppvector[int] c_pred_map
        cdef cppvector[int] c_from_p1
        cdef cppvector[int] c_from_p2
        cdef cppvector[string] c_names
        for i in pred_col_to_p1:
            c_pred_map.push_back(<int>i)
        for i in out_from_p1:
            c_from_p1.push_back(<int>i)
        for i in out_from_p2:
            c_from_p2.push_back(<int>i)
        for n in out_names:
            c_names.push_back(<string>(n if isinstance(n, bytes) else (<str>n).encode("utf-8")))
        self._e.set_latmat_scan_source(
            p, p1_plan.pipeline_ptr, p1_plan.footer_map, &p1_plan.work_items,
            &p1_plan.column_names, p1_plan.in_flight_limit, p1_pool,
            &p1_plan.string_types, &p1_plan.decimal_columns, &p1_plan.logical_coerce,
            &p1_plan.hash_key_columns, &p1_plan.array_columns,
            p2_plan.pipeline_ptr, &p2_plan.column_names, p2_pool,
            &p2_plan.string_types, &p2_plan.decimal_columns, &p2_plan.logical_coerce,
            &p2_plan.hash_key_columns, &p2_plan.array_columns,
            <void*>pred_fn, <void*>pred_ctx, c_pred_map,
            sort_p1_index, sort_ascending, topn_limit,
            c_from_p1, c_from_p2, c_names)

    def close_scan_plans(self):
        """Cancel + shut down every NativeScanPlan's IO pipeline. MUST only run
        after the engine driver has finished (its workers block inside the
        pipeline's wait); ``execute_native`` calls this after the done-event."""
        cdef NativeScanPlan sp
        for sp in self.scan_plans:
            sp.close()

    def set_buffer_source(self, size_t p, size_t buf):
        self._e.set_buffer_source(p, buf)

    def add_select(self, size_t p, list indices, list names):
        cdef cppvector[size_t] idx
        cdef cppvector[string] nms
        for i in indices:
            idx.push_back(<size_t>i)
        for n in names:
            nms.push_back(<string>(n if isinstance(n, bytes) else (<str>n).encode("utf-8")))
        self._e.add_select(p, idx, nms)

    def set_queue_sink(self, size_t p, PyMorselQueue q):
        self._e.set_queue_sink(p, q._q)

    def set_agg_sink(self, size_t p, list specs, size_t buf):
        """``specs`` = [(identity, fn:'CountStar'|'Count'|'Sum'|'Avg'|'Min'|'Max',
        operand col_idx | -1), ...] in output-column order."""
        self._e.set_agg_sink(p, _agg_spec_from_list(specs), buf)

    def set_groupby_sink(self, size_t p, list key_idx, list key_names, list specs, size_t buf):
        cdef cppvector[size_t] keys
        cdef cppvector[string] knames
        for i in key_idx:
            keys.push_back(<size_t>i)
        for n in key_names:
            knames.push_back(<string>(n if isinstance(n, bytes) else (<str>n).encode("utf-8")))
        self._e.set_groupby_sink(p, keys, knames, _agg_spec_from_list(specs), buf)

    def set_distinct_sink(self, size_t p, list on_idx, size_t buf):
        """``on_idx`` = dedup key column indices; empty list = every column."""
        cdef cppvector[size_t] on
        for i in on_idx:
            on.push_back(<size_t>i)
        self._e.set_distinct_sink(p, on, buf)

    def set_buffer_append_sink(self, size_t p, size_t buf):
        """Stream this pipeline into a (possibly shared) buffer — UNION ALL legs."""
        self._e.set_buffer_append_sink(p, buf)

    def new_join2_ref(self):
        return self._e.new_join2_ref()

    def set_join2_build_sink(self, size_t p, list key_idx, list payload_idx, size_t ref,
                             list payload_types, list payload_logical=None):
        """``payload_types``/``payload_logical`` are the build-side payload columns'
        PLAN-KNOWN physical (DrakenType ints) + logical ((kind, unit, precision, scale,
        dimension) tuples or None) types — same shape as ``set_final_schema`` — so the
        native build sink can size+type its row-store up front instead of learning it
        from the first morsel, which never arrives when the build side streams zero
        rows (a filtered-to-empty subquery)."""
        cdef cppvector[size_t] keys
        cdef cppvector[size_t] pay
        cdef cppvector[DrakenType] ts
        cdef cppvector[int] lk, lu, lp, lsc, ld
        cdef int i
        for i in key_idx:
            keys.push_back(<size_t>i)
        for i in payload_idx:
            pay.push_back(<size_t>i)
        for t in payload_types:
            ts.push_back(<DrakenType><int>t)
        for i in range(len(payload_types)):
            entry = payload_logical[i] if payload_logical is not None and i < len(payload_logical) else None
            if entry is None:
                lk.push_back(0); lu.push_back(0); lp.push_back(0); lsc.push_back(0); ld.push_back(0)
            else:
                lk.push_back(<int>entry[0]); lu.push_back(<int>entry[1])
                lp.push_back(<int>entry[2]); lsc.push_back(<int>entry[3]); ld.push_back(<int>entry[4])
        self._e.set_join2_build_sink(p, keys, pay, ref, ts, lk, lu, lp, lsc, ld)

    def add_join2_probe(self, size_t p, size_t ref, list key_idx, list payload_idx,
                        int mode):
        """mode: 0=inner, 1=left outer (probe side preserved), 2=semi,
        3=null-aware anti (NOT IN), 4=plain anti (NOT EXISTS / EXCEPT).
        3 and 4 differ on NULL handling — see native_join2.hpp's JoinMode."""
        cdef cppvector[size_t] keys
        cdef cppvector[size_t] pay
        for i in key_idx:
            keys.push_back(<size_t>i)
        for i in payload_idx:
            pay.push_back(<size_t>i)
        self._e.add_join2_probe(p, ref, keys, pay, mode)

    def add_join2_probe_residual(self, size_t p, size_t ref, list key_idx,
                                 list payload_idx, int mode, CompiledBytecode bc,
                                 list layout):
        """SEMI/ANTI (mode 2/3/4) whose EXISTENCE test is gated by a correlated
        non-equality residual — TPC-H Q21's `l2.l_suppkey <> l1.l_suppkey`. ``bc`` is
        resolved against ``layout``, the PAIR layout (build payload then probe
        payload), because the predicate reads a column from each side."""
        cdef cppvector[size_t] keys
        cdef cppvector[size_t] pay
        cdef cppvector[int] col_idx
        cdef cppvector[void*] lit_dv
        for i in key_idx:
            keys.push_back(<size_t>i)
        for i in payload_idx:
            pay.push_back(<size_t>i)
        _resolve_bc_for_layout(bc, layout, col_idx, lit_dv)
        self.held.append(bc)
        self._e.add_join2_probe_residual(p, ref, keys, pay, mode,
                                         <void*>bc.instrs, <int>bc.count,
                                         col_idx, lit_dv, _expr_eval_tramp)

    def set_asof_build_sink(self, size_t p, list key_idx, list payload_idx,
                            size_t asof_idx, size_t ref, list payload_types,
                            list payload_logical=None):
        """``payload_types``/``payload_logical``: see set_join2_build_sink — same
        plan-known-type wiring, shared build sink implementation."""
        cdef cppvector[size_t] keys
        cdef cppvector[size_t] pay
        cdef cppvector[DrakenType] ts
        cdef cppvector[int] lk, lu, lp, lsc, ld
        cdef int i
        for i in key_idx:
            keys.push_back(<size_t>i)
        for i in payload_idx:
            pay.push_back(<size_t>i)
        for t in payload_types:
            ts.push_back(<DrakenType><int>t)
        for i in range(len(payload_types)):
            entry = payload_logical[i] if payload_logical is not None and i < len(payload_logical) else None
            if entry is None:
                lk.push_back(0); lu.push_back(0); lp.push_back(0); lsc.push_back(0); ld.push_back(0)
            else:
                lk.push_back(<int>entry[0]); lu.push_back(<int>entry[1])
                lp.push_back(<int>entry[2]); lsc.push_back(<int>entry[3]); ld.push_back(<int>entry[4])
        self._e.set_asof_build_sink(p, keys, pay, asof_idx, ref, ts, lk, lu, lp, lsc, ld)

    def add_asof_probe(self, size_t p, size_t ref, list key_idx, list payload_idx,
                       size_t asof_idx, int op):
        """op: 0=GtEq, 1=Gt, 2=LtEq, 3=Lt (probe <op> build MATCH_CONDITION)."""
        cdef cppvector[size_t] keys
        cdef cppvector[size_t] pay
        for i in key_idx:
            keys.push_back(<size_t>i)
        for i in payload_idx:
            pay.push_back(<size_t>i)
        self._e.add_asof_probe(p, ref, keys, pay, asof_idx, op)

    def add_expr_filter(self, size_t p, CompiledBytecode bc, list layout,
                       list const_col_idx=None, list const_scalar_vecs=None):
        """General WHERE: a plan-lowered, plan-resolved c-native predicate program
        (bool-final; DESC intermediates fold raw in the VM). `const_col_idx` /
        `const_scalar_vecs` (parallel lists, optional) name columns the compiler has
        proven hold one literal value on every row surviving the predicate (an
        `IDENTIFIER = LITERAL` conjunct) — those columns broadcast from the scalar
        Vector in O(1) instead of being gathered and discarded. `const_scalar_vecs`
        elements are length-1 Vectors; `held` keeps them (and their underlying
        DrakenVector*) alive for the Engine's whole run, same as `bc`."""
        if not bytecode_is_c_native_predicate(bc):
            raise ValueError("native engine: add_expr_filter requires a c-native "
                             "bool-final program — the compiler must reject earlier")
        cdef cppvector[int] col_idx
        cdef cppvector[void*] lit_dv
        cdef cppvector[int] c_const_col_idx
        cdef cppvector[void*] c_const_scalar_dv
        cdef Vector scalar_vec
        _resolve_bc_for_layout(bc, layout, col_idx, lit_dv)
        self.held.append(bc)
        if const_col_idx:
            for i in const_col_idx:
                c_const_col_idx.push_back(<int>i)
            for scalar_vec in const_scalar_vecs:
                c_const_scalar_dv.push_back(<void*>scalar_vec.unified())
            self.held.append(const_scalar_vecs)
        self._e.add_expr_filter(p, <void*>bc.instrs, <int>bc.count, col_idx, lit_dv,
                                _expr_filter_tramp, c_const_col_idx, c_const_scalar_dv)

    def add_expr_project(self, size_t p, CompiledBytecode bc, list layout, name,
                         logical=None, bint preserve_shape=False):
        """Append ONE computed column (identity ``name``) evaluated by a plan-lowered,
        plan-resolved c-native program (load-ending programs allowed). ``logical`` =
        (kind, unit, precision, scale, dimension) ints for descriptor-carrying results
        (DECIMAL/TIMESTAMP/VECTOR) — re-attached to the output column's owner natively.

        ``preserve_shape`` keeps a compressed result's encoding (dict/constant) at the
        projection boundary instead of force-densifying. The compiler sets it ONLY for
        computed columns whose sole consumer is compression-aware (a GROUP BY / DISTINCT
        key); it selects a distinct trampoline, which also prevents fusion with dense
        ExprProject columns in the engine's ExprMultiProjectOperator."""
        if not bytecode_ops_all_c_native(bc):
            raise ValueError("native engine: add_expr_project requires a c-native "
                             "program — the compiler must reject this shape earlier")
        cdef cppvector[int] col_idx
        cdef cppvector[void*] lit_dv
        _resolve_bc_for_layout(bc, layout, col_idx, lit_dv)
        self.held.append(bc)
        cdef string nm = <string>(name if isinstance(name, bytes)
                                  else (<str>name).encode("utf-8"))
        cdef int lk = 0, lu = 0, lp = 0, lsc = 0, ld = 0
        if logical is not None:
            lk = <int>logical[0]
            lu = <int>logical[1]
            lp = <int>logical[2]
            lsc = <int>logical[3]
            ld = <int>logical[4]
        cdef ExprEvalFn fn = (_expr_eval_preserve_tramp if preserve_shape
                              else _expr_eval_tramp)
        self._e.add_expr_project(p, <void*>bc.instrs, <int>bc.count, col_idx, lit_dv,
                                 fn, nm, lk, lu, lp, lsc, ld)

    def add_limit(self, size_t p, offset, limit):
        """LIMIT/OFFSET on pipeline ``p``. ``limit`` None = unbounded (OFFSET-only)."""
        cdef int64_t off = 0 if offset is None else <int64_t>offset
        cdef int64_t lim = 0x7FFFFFFFFFFFFFFF if limit is None else <int64_t>limit
        self._e.add_limit(p, off, lim)

    def add_unnest(self, size_t p, size_t array_idx, target_name, bint drop_source):
        """CROSS JOIN UNNEST on pipeline ``p``: expand columns[array_idx] (an ARRAY)
        into the flattened element column ``target_name`` (bytes/str). ``drop_source``
        replaces the consumed array column in place; otherwise the target is appended
        and the raw array survives."""
        cdef string nm = <string>(target_name if isinstance(target_name, bytes)
                                  else (<str>target_name).encode("utf-8"))
        self._e.add_unnest(p, <uint32_t>array_idx, nm, drop_source)

    def add_unnest_literal(self, size_t p, Morsel lit, target_name):
        """CROSS JOIN UNNEST over a LITERAL array: ``lit`` is a plan-constant
        one-column Morsel of the literal's elements (materialized at compile time,
        like a virtual dataset). Each input row is repeated len(lit) times with the
        literal tiled across them; the target column is appended."""
        cdef string nm = <string>(target_name if isinstance(target_name, bytes)
                                  else (<str>target_name).encode("utf-8"))
        self.held.append(lit)   # keep the literal's vectors alive for the whole run
        self._e.add_unnest_literal(p, morsel_to_cxx(lit), nm)

    def add_buffer_morsel(self, size_t buf, Morsel m):
        """Plan-time materialization: place a (plan-constant) morsel into a buffer —
        virtual datasets ($planets, VALUES, GENERATE_SERIES) cross the boundary HERE,
        once, at compile time; execution reads the buffer natively."""
        self._e.add_buffer_morsel(buf, morsel_to_cxx(m))

    def set_pipeline_dop(self, size_t p, int dop):
        """Force pipeline ``p``'s degree (order-sensitive consumers of a sorted
        buffer run at 1). DOP is a number, never a code-path selector."""
        self._e.set_pipeline_dop(p, dop)

    def set_sort_sink(self, size_t p, list spec, size_t buf):
        """``spec`` = [(col_idx:int, ascending:bool), ...] most significant first."""
        self._e.set_sort_sink(p, _sort_spec_from_list(spec), buf)

    def set_topn_sink(self, size_t p, list spec, size_t n, size_t buf):
        self._e.set_topn_sink(p, _sort_spec_from_list(spec), n, buf)

    def set_window_sink(self, size_t p, list sort_spec, size_t n_part,
                        list fn_kinds, list fn_names, long long top_k, size_t buf):
        """``sort_spec`` = [(col_idx, ascending), ...] = partition keys (all asc) then
        order keys; ``n_part`` leading entries are the partition keys. ``fn_kinds`` =
        int codes (0 ROW_NUMBER, 1 RANK, 2 DENSE_RANK); ``fn_names`` the output names.
        ``top_k`` = WindowTopKFusionStrategy's fused `rank <= K` hint, or -1 if none —
        keep only rows whose rank is <= top_k, computed after ranking every row."""
        cdef cppvector[int] kinds
        cdef cppvector[string] names
        for k in fn_kinds:
            kinds.push_back(<int>k)
        for nm in fn_names:
            names.push_back(<string>(nm if isinstance(nm, bytes) else (<str>nm).encode("utf-8")))
        self._e.set_window_sink(p, _sort_spec_from_list(sort_spec), n_part,
                                kinds, names, top_k, buf)

    def set_window_topk_sink(self, size_t p, list part_idx, size_t order_idx,
                             bint ascending, size_t k, out_name, size_t buf):
        """Streaming ROW_NUMBER top-K per partition (WindowTopKFusionStrategy) — no
        full sort. ``part_idx`` = partition-key column indices (hashed); ``order_idx``/
        ``ascending`` = the single ORDER BY column and its direction; ``k`` = the
        fused `rank <= K`. The compiler only routes here when eligible — see
        native_group_sinks.hpp's WindowTopKSink docstring for the exact scope."""
        cdef cppvector[size_t] idx
        for i in part_idx:
            idx.push_back(<size_t>i)
        cdef string nm = <string>(out_name if isinstance(out_name, bytes)
                                  else (<str>out_name).encode("utf-8"))
        self._e.set_window_topk_sink(p, idx, order_idx, ascending, k, nm, buf)

    def set_final_schema(self, list names, list types, list logical=None):
        """``names`` = final display names; ``types`` = DrakenType ints (physical);
        ``logical`` = per-column (kind, unit, precision, scale, dimension) int tuples
        or None (same shape as ``add_expr_project``'s ``logical``) — used for the
        courtesy empty-result morsel when a query yields zero rows. A TIMESTAMP64 /
        DECIMAL / DECIMAL128 column with no entry here would build that empty column
        with no logical-type descriptor, which draken treats as a hard error the
        moment it is re-encoded (e.g. written back out to Parquet)."""
        cdef cppvector[string] nms
        cdef cppvector[DrakenType] ts
        cdef cppvector[int] lk, lu, lp, lsc, ld
        cdef int i
        for n in names:
            nms.push_back(<string>(n if isinstance(n, bytes) else (<str>n).encode("utf-8")))
        for t in types:
            ts.push_back(<DrakenType><int>t)
        for i in range(len(types)):
            entry = logical[i] if logical is not None and i < len(logical) else None
            if entry is None:
                lk.push_back(0); lu.push_back(0); lp.push_back(0); lsc.push_back(0); ld.push_back(0)
            else:
                lk.push_back(<int>entry[0]); lu.push_back(<int>entry[1])
                lp.push_back(<int>entry[2]); lsc.push_back(<int>entry[3]); ld.push_back(<int>entry[4])
        self._e.set_final_schema(nms, ts, lk, lu, lp, lsc, ld)


cdef cppvector[SortKeySpec] _sort_spec_from_list(list spec) except *:
    cdef cppvector[SortKeySpec] out
    cdef SortKeySpec s
    for col_idx, ascending in spec:
        s.col_idx = <size_t>col_idx
        s.ascending = bool(ascending)
        out.push_back(s)
    return out


cdef cppvector[AggSpec2] _agg_spec_from_list(list spec) except *:
    """``spec`` = [(name:bytes|str, fn:str, col_idx:int[, options:dict]), ...]
    in output order. ``col_idx`` is a real column index (>= 0), or one of the
    named sentinels compiler.py mirrors from native_group_sinks.hpp:
    ``_AGG_NO_OPERAND`` (-1, CountStar) / ``_AGG_WHOLE_ROW`` (-2, whole-row
    CountDistinct — COUNT(DISTINCT *)). ``options`` is ARRAY_AGG-only; every
    other function ignores it.
    """
    cdef cppvector[AggSpec2] out
    cdef AggSpec2 s
    cdef object opts
    for item in spec:
        name = item[0]
        fn = item[1]
        col_idx = item[2]
        opts = item[3] if len(item) > 3 else None
        if fn == "CountStar":
            s.fn = AggFn.CountStar
        elif fn == "Count":
            s.fn = AggFn.Count
        elif fn == "CountDistinct":
            s.fn = AggFn.CountDistinct
        elif fn == "Sum":
            s.fn = AggFn.Sum
        elif fn == "Avg":
            s.fn = AggFn.Avg
        elif fn == "Min":
            s.fn = AggFn.Min
        elif fn == "Max":
            s.fn = AggFn.Max
        elif fn == "ArrayAgg":
            s.fn = AggFn.ArrayAgg
        elif fn == "Stddev":
            s.fn = AggFn.Stddev
        elif fn == "Median":
            s.fn = AggFn.Median
        elif fn == "AnyValue":
            s.fn = AggFn.AnyValue
        elif fn == "ApproxCountDistinct":
            s.fn = AggFn.ApproxCountDistinct
        elif fn == "ApproxPercentile":
            s.fn = AggFn.ApproxPercentile
        else:
            raise ValueError(f"native engine: unknown aggregate function {fn!r}")
        s.col_idx = <int>col_idx
        s.name = <string>(name if isinstance(name, bytes) else (<str>name).encode("utf-8"))
        # `s` is reused across iterations — every field is assigned unconditionally
        # so a previous ARRAY_AGG's modifiers can never bleed into a later spec.
        if opts is None:
            s.aa_distinct = False
            s.aa_ordered = False
            s.aa_descending = False
            s.aa_limit = -1
            s.aa_max_per_group = 1000
            s.percentile = 0.5
        else:
            s.aa_distinct = <bint>bool(opts.get("distinct", False))
            s.aa_ordered = <bint>bool(opts.get("ordered", False))
            s.aa_descending = <bint>bool(opts.get("descending", False))
            s.aa_limit = <int64_t>(-1 if opts.get("limit") is None else opts["limit"])
            s.aa_max_per_group = <int64_t>opts.get("max_per_group", 1000)
            s.percentile = <double>opts.get("percentile", 0.5)
        out.push_back(s)
    return out


cdef class NativeErrorSlot:
    """Native terminal-error channel for the plan driver — replaces the borrowed
    Python ``errors`` list the detached driver used to append to. The driver records
    the engine's ``ErrCtx`` here (a C ``int`` code plus a ``std::string`` COPY of the
    message, taken while the ErrCtx pointer is still valid) after ``eng.run()`` returns
    and BEFORE it finishes the output queue; the Python consumer reads it on its own
    thread once the queue is finished and builds/raises the exception. NO Python object
    is created or mutated on the native driver thread — ``code`` is a C int and ``msg``
    a C++ ``std::string`` — so this is a purely-native slot, not a borrowed container."""
    cdef public int code
    cdef string msg

    def __cinit__(self):
        self.code = 0

    def message(self):
        """Consumer-side: the native message copy decoded to str ('' if none)."""
        return self.msg.decode("utf-8") if self.msg.size() else ""


def build_terminal_exc(NativePlan nplan, NativeErrorSlot errslot):
    """Consumer-side (GIL, the legitimate result-marshaling edge): turn the driver's
    terminal ErrCtx into the exception to raise. Prefer a scan's stashed Python
    exception (rich traceback, e.g. a decode error), else synthesize a RuntimeError
    from the native code + message. Called by execute_native AFTER the output queue is
    finished (every worker joined), so reading the scans' stashed exceptions is
    race-free. Returns None when there is no terminal error."""
    cdef object exc
    cdef object scan_obj
    if errslot.code == 0:
        return None
    for scan_obj in nplan.scans:
        exc = (<BasePlanNode>scan_obj)._take_exc()
        if exc is not None:
            return exc
    return RuntimeError(
        "native engine: error code %d: %s" % (errslot.code, errslot.message() or "unknown")
    )


cdef struct _EnginePlanArg:
    void* engine        # Engine* — borrowed; the NativePlan PyObject owns it
    int dop
    PyObject* nplan     # borrowed NativePlan (keeps engine + scans alive via caller)
    PyObject* out_q     # borrowed PyMorselQueue
    PyObject* errslot   # borrowed NativeErrorSlot — the driver records the terminal
                        # ErrCtx (code + msg copy) here; NO Python object is mutated.
                        # The consumer reads it after the queue is finished.
    void* pool          # opaque BSThreadPoolBridge* — see executor.hpp for why void*


cdef void _engine_plan_run(_EnginePlanArg* a) noexcept with gil:
    """Driver-task body. The engine run itself happens DETACHED (``with nogil``):
    this thread blocks indefinitely inside ``bs_pool_wait_native`` with no safe
    point, and on free-threaded CPython a thread that stays ATTACHED while blocked
    in native code stalls every stop-the-world (GC) — which then parks every scan
    worker at its trampoline's attach, wedging the whole query. Proven live: the
    lldb signature is ``_PyThreadState_Attach -> _PyParkingLot_Park`` under
    ``submit_work_native`` on all workers while this thread sits in ``wait_native``.

    Completion/error are coordinated back to the consumer with NATIVE machinery only:
    the terminal ErrCtx is copied into the native ``errslot`` (a C int + a std::string,
    no Python object), and the driver's LAST act is ``out_q.finish()`` — the queue's
    own out-of-band finish signal. The consumer observing the queue finished is itself
    proof this driver has returned from ``eng.run()`` (every worker joined) and stopped
    touching the pool; it then builds the Python exception from ``errslot`` on its own
    thread. No borrowed ``threading.Event`` and no borrowed ``list`` are touched here."""
    cdef ErrCtx err
    cdef object out_q = <object>a.out_q
    cdef NativeErrorSlot errslot = <NativeErrorSlot>(<object>a.errslot)
    cdef Engine* eng = <Engine*>a.engine
    cdef int dop = a.dop
    cdef void* pool = a.pool
    err.code = 0
    err.msg = NULL
    try:
        with nogil:
            eng.run(dop, pool, err)
        # Record the terminal ErrCtx natively (int + std::string copy — the msg
        # pointer is "valid at raise time", i.e. now, so copy it before it can go
        # stale). The consumer turns this into the Python exception after finish().
        errslot.code = err.code
        if err.code != 0 and err.msg != NULL:
            errslot.msg = string(err.msg)
    finally:
        # LAST act: the sole, purely-native completion signal. Ordered AFTER the
        # errslot write (which the consumer reads only once it observes finish, so
        # the write is visible via the finish/get acquire-release handshake).
        out_q.finish()


cdef void _engine_plan_task(void* arg) noexcept nogil:
    _engine_plan_run(<_EnginePlanArg*>arg)


def native_plan_execute(CppThreadPool pool, NativePlan nplan, int dop,
                        PyMorselQueue out_q, NativeErrorSlot errslot):
    """Run a compiled native plan. NON-BLOCKING: submits ONE detached native driver via
    spawn_detached_native_task — a lone detached std::thread, NOT a task on ``pool``:
    the driver blocks in ``eng.run``'s native wait with no safe point, and on
    free-threaded CPython a thread that stays ATTACHED while blocked in native code
    stalls every stop-the-world GC (see ``_engine_plan_run``'s docstring). It runs every
    pipeline in the graph at degree ``dop`` on ``pool`` and streams the terminal
    pipeline into
    ``out_q``. Completion is signalled purely natively via ``out_q.finish()`` (the
    driver's last act) and any terminal error via the native ``errslot``. The caller
    MUST drain ``out_q`` concurrently (bounded; backpressure blocks the producer) and
    hold the returned handle, ``nplan``, ``out_q`` and ``errslot`` alive until it has
    seen the sink finish (normal path) or waited via ``out_q.wait_finished()`` (abandon
    path)."""
    cdef _EnginePlanArg* a = <_EnginePlanArg*>malloc(sizeof(_EnginePlanArg))
    if a == NULL:
        raise MemoryError()
    cdef NativeFanoutHandle handle = NativeFanoutHandle.__new__(NativeFanoutHandle)
    handle._args = <void*>a
    a.engine = <void*>nplan._e
    a.dop = dop
    a.nplan = <PyObject*>nplan
    a.out_q = <PyObject*>out_q
    a.errslot = <PyObject*>errslot
    a.pool = <void*>pool._pool
    spawn_detached_native_task(_engine_plan_task, a)
    return handle


cdef class JoinLeftAdapter(BasePlanNode):
    """Terminal node of a join's left-input chain. Forwards every morsel to
    the join's `push_left`. Has no downstream of its own — left-side EOS
    finalises build state in the JoinNode without propagating downstream."""
    cdef JoinNode _join

    def __init__(self, JoinNode join, *, properties=None):
        # Skip BasePlanNode.__init__ heavy machinery — adapters are wiring nodes
        # not catalogued operators. We still need counters/identity for explain.
        from collections import defaultdict
        from opteryx.utils import random_string
        self.identity = random_string()
        self.parameters = {}
        self.columns = []
        self.readings = defaultdict(int)
        self._time_stat_key = "time_join_left_adapter"
        self.properties = properties
        self.is_scan = False
        self.is_join = False
        self.is_stateless = True
        self.is_not_explained = True
        self._empty_morsel_cache = None
        self._join = join

    @property
    def name(self) -> str:
        return "JoinLeftAdapter"

    cdef int push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        # Attribute the build-side push time to the JOIN, not this hidden
        # adapter, so the join shows real time in EXPLAIN ANALYZE.
        cdef timespec ts_start, ts_end
        if self._ctx is not None and self._ctx._terminated:
            return 0
        clock_gettime(CLOCK_MONOTONIC, &ts_start)
        self._dispatch_push(m, err)
        clock_gettime(CLOCK_MONOTONIC, &ts_end)
        self._join._account_input(
            m,
            (<uint64_t>(ts_end.tv_sec - ts_start.tv_sec)) * <uint64_t>1000000000
            + <uint64_t>(ts_end.tv_nsec - ts_start.tv_nsec),
        )
        return err.code if err != NULL else 0

    cdef int _dispatch_push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        return self._join.push_left(m, err)


cdef class JoinRightAdapter(BasePlanNode):
    """Terminal node of a join's right-input chain. Forwards every morsel to
    the join's `push_right`. On EOS the JoinNode itself calls emit(EOS) to
    terminate the downstream chain."""
    cdef JoinNode _join

    def __init__(self, JoinNode join, *, properties=None):
        from collections import defaultdict
        from opteryx.utils import random_string
        self.identity = random_string()
        self.parameters = {}
        self.columns = []
        self.readings = defaultdict(int)
        self._time_stat_key = "time_join_right_adapter"
        self.properties = properties
        self.is_scan = False
        self.is_join = False
        self.is_stateless = True
        self.is_not_explained = True
        self._empty_morsel_cache = None
        self._join = join

    @property
    def name(self) -> str:
        return "JoinRightAdapter"

    cdef int push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        # Attribute the probe-side push time to the JOIN, not this hidden
        # adapter (see JoinLeftAdapter.push).
        cdef timespec ts_start, ts_end
        if self._ctx is not None and self._ctx._terminated:
            return 0
        clock_gettime(CLOCK_MONOTONIC, &ts_start)
        self._dispatch_push(m, err)
        clock_gettime(CLOCK_MONOTONIC, &ts_end)
        self._join._account_input(
            m,
            (<uint64_t>(ts_end.tv_sec - ts_start.tv_sec)) * <uint64_t>1000000000
            + <uint64_t>(ts_end.tv_nsec - ts_start.tv_nsec),
        )
        return err.code if err != NULL else 0

    cdef int _dispatch_push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        return self._join.push_right(m, err)


# -----------------------------------------------------------------------------
# Expression evaluator — included here so all operator modules can call
# evaluation functions directly at C level (no .so boundary, no Python dispatch).
# Previously compiled as opteryx.expression.evaluator._impl; now part of this
# unit. opteryx/expression/evaluator/__init__.py re-exports from here.
# -----------------------------------------------------------------------------

# Operator codes — integer identifiers for operator strings stamped on Nodes.
# 0 is reserved so a forgotten dispatcher branch fails loud, not silently.
DEF OP_UNKNOWN          = 0
DEF OP_EQ               = 1
DEF OP_NOT_EQ           = 2
DEF OP_LT               = 3
DEF OP_GT               = 4
DEF OP_LT_EQ            = 5
DEF OP_GT_EQ            = 6
DEF OP_IN_LIST          = 7
DEF OP_NOT_IN_LIST      = 8
DEF OP_LIKE             = 9
DEF OP_NOT_LIKE         = 10
DEF OP_ILIKE            = 11
DEF OP_NOT_ILIKE        = 12
DEF OP_RLIKE            = 13
DEF OP_NOT_RLIKE        = 14
DEF OP_IN_STR           = 15
DEF OP_NOT_IN_STR       = 16
DEF OP_I_IN_STR         = 17
DEF OP_NOT_I_IN_STR     = 18

_OP_CODE = {
    "Eq": 1, "NotEq": 2, "Lt": 3, "Gt": 4, "LtEq": 5, "GtEq": 6,
    "InList": 7, "NotInList": 8,
    "Like": 9, "NotLike": 10, "ILike": 11, "NotILike": 12,
    "RLike": 13, "NotRLike": 14,
    "InStr": 15, "NotInStr": 16, "IInStr": 17, "NotIInStr": 18,
}

cdef int _DRAKEN_CMP_OP[19]
_DRAKEN_CMP_OP[0]  = -1  # OP_UNKNOWN
_DRAKEN_CMP_OP[1]  =  0  # OP_EQ        → Draken Eq
_DRAKEN_CMP_OP[2]  =  1  # OP_NOT_EQ    → Draken Ne
_DRAKEN_CMP_OP[3]  =  4  # OP_LT        → Draken Lt
_DRAKEN_CMP_OP[4]  =  2  # OP_GT        → Draken Gt
_DRAKEN_CMP_OP[5]  =  5  # OP_LT_EQ     → Draken Le
_DRAKEN_CMP_OP[6]  =  3  # OP_GT_EQ     → Draken Ge
_DRAKEN_CMP_OP[7]  = -1  # OP_IN_LIST       — own kernel
_DRAKEN_CMP_OP[8]  = -1  # OP_NOT_IN_LIST   — own kernel
_DRAKEN_CMP_OP[9]  = -1  # OP_LIKE          — own kernel
_DRAKEN_CMP_OP[10] = -1  # OP_NOT_LIKE      — own kernel
_DRAKEN_CMP_OP[11] = -1  # OP_ILIKE         — own kernel
_DRAKEN_CMP_OP[12] = -1  # OP_NOT_ILIKE     — own kernel
_DRAKEN_CMP_OP[13] = -1  # OP_RLIKE         — own kernel
_DRAKEN_CMP_OP[14] = -1  # OP_NOT_RLIKE     — own kernel
_DRAKEN_CMP_OP[15] = -1  # OP_IN_STR        — own kernel
_DRAKEN_CMP_OP[16] = -1  # OP_NOT_IN_STR    — own kernel
_DRAKEN_CMP_OP[17] = -1  # OP_I_IN_STR      — own kernel
_DRAKEN_CMP_OP[18] = -1  # OP_NOT_I_IN_STR  — own kernel

cdef int _DRAKEN_CMP_OP_FLIPPED[19]
_DRAKEN_CMP_OP_FLIPPED[0]  = -1
_DRAKEN_CMP_OP_FLIPPED[1]  =  0   # Eq    (symmetric)
_DRAKEN_CMP_OP_FLIPPED[2]  =  1   # Ne    (symmetric)
_DRAKEN_CMP_OP_FLIPPED[3]  =  2   # OP_LT       → Draken Gt
_DRAKEN_CMP_OP_FLIPPED[4]  =  4   # OP_GT       → Draken Lt
_DRAKEN_CMP_OP_FLIPPED[5]  =  3   # OP_LT_EQ    → Draken Ge
_DRAKEN_CMP_OP_FLIPPED[6]  =  5   # OP_GT_EQ    → Draken Le
_DRAKEN_CMP_OP_FLIPPED[7]  = -1
_DRAKEN_CMP_OP_FLIPPED[8]  = -1
_DRAKEN_CMP_OP_FLIPPED[9]  = -1
_DRAKEN_CMP_OP_FLIPPED[10] = -1
_DRAKEN_CMP_OP_FLIPPED[11] = -1
_DRAKEN_CMP_OP_FLIPPED[12] = -1
_DRAKEN_CMP_OP_FLIPPED[13] = -1
_DRAKEN_CMP_OP_FLIPPED[14] = -1
_DRAKEN_CMP_OP_FLIPPED[15] = -1
_DRAKEN_CMP_OP_FLIPPED[16] = -1
_DRAKEN_CMP_OP_FLIPPED[17] = -1
_DRAKEN_CMP_OP_FLIPPED[18] = -1

# -----------------------------------------------------------------------------
# Include order: base classes / shared types before their consumers.
# -----------------------------------------------------------------------------

# ReaderNode is subclassed by function_dataset, parquet_read, show_value
include "read/read.pyx"

include "asof_join/asof_join.pyx"
include "cross_join/cross_join.pyx"
include "csv_read/csv_read.pyx"
include "distinct/distinct.pyx"
include "hashed_inner_join/hashed_inner_join.pyx"
include "exit/exit.pyx"
include "explain/explain.pyx"
include "filter_join/filter_join.pyx"
include "filter/filter.pyx"
include "function_dataset/function_dataset.pyx"
include "heap_sort/heap_sort.pyx"
include "jsonl_read/jsonl_read.pyx"
include "limit/limit.pyx"
include "window/row_number.pyx"
include "nested_loop_join/nested_loop_join.pyx"
include "non_equi_join/non_equi_join.pyx"
include "null_reader/null_reader.pyx"
include "outer_join/outer_join.pyx"
include "parquet_read/parquet_read.pyx"
include "projection/projection.pyx"
include "set_variable/set_variable.pyx"
include "show_columns/show_columns.pyx"
include "show_manifest/show_manifest.pyx"
include "show_create/show_create.pyx"
include "show_value/show_value.pyx"
include "sort/sort.pyx"
include "table_management/table_management.pyx"
include "relation_management/relation_management.pyx"
include "insert/insert.pyx"
include "union/union.pyx"
include "unnest_join/unnest_join.pyx"
include "view_management/view_management.pyx"

# Aggregate: ungrouped engine first (aggregate_node uses its accumulator classes)
include "aggregate/ungrouped_agg.pyx"
include "aggregate/aggregate_node.pyx"

# Grouped aggregate (self-contained via .pxi includes inside _grouped_agg.pyx)
include "grouped_aggregate_hashed/_grouped_agg.pyx"
