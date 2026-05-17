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

from draken.morsels.morsel cimport Morsel
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.string_vector cimport StringVector
from draken.vectors.integer_vector cimport IntegerVector
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.perfect_hash_set cimport PerfectHashSet
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from draken.interop.vector_sequence cimport vector_from_sequence
from cpython.array cimport array

from libc.stdint cimport int8_t, int16_t, int64_t, uint64_t
from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy

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
    """
    cdef bint _terminated

    def __cinit__(self):
        self._terminated = False

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

    # Hot-path telemetry counters (typed; no Python attr writes per morsel).
    cdef public uint64_t execution_time      # ns; cumulative
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

    def __cinit__(self, *args, **kwargs):
        self._downstream = None
        self._ctx = None
        self._morsel_index = 0
        self._tracing_enabled = False
        self._trace_buf = NULL
        self._trace_capacity = 0
        self._trace_count = 0
        self._morsel_iter = None

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
        base = {
            "calls": int(self.calls),
            "execution_time": int(self.execution_time),
            "records_in": int(self.records_in),
            "records_out": int(self.records_out),
            "bytes_in": int(self.bytes_in),
            "bytes_out": int(self.bytes_out),
        }
        if self.readings:
            base.update(self.readings)
        return base

    # ---- Push pipeline interface ------------------------------------------------
    cpdef void push(self, Morsel morsel) except *:
        """Entry point. Records timing + per-morsel counters, then dispatches
        to `_dispatch_push` (cdef vtable; cdef-class subclasses override
        directly for max speed). Callable from Python (via cpdef wrapper)
        and from Cython (via typed cdef vtable call)."""
        cdef timespec ts_start, ts_end
        cdef uint64_t duration_ns
        cdef uint64_t rows = 0
        cdef uint64_t nbytes = 0

        if self._ctx is not None and self._ctx.is_terminated():
            return

        if morsel is not None:
            rows = morsel.num_rows
            nbytes = morsel.nbytes
            self.records_in += rows
            self.bytes_in += nbytes
        self.calls += 1

        clock_gettime(CLOCK_MONOTONIC, &ts_start)
        self._dispatch_push(morsel)
        clock_gettime(CLOCK_MONOTONIC, &ts_end)
        duration_ns = (<uint64_t>(ts_end.tv_sec - ts_start.tv_sec)) * <uint64_t>1000000000
        duration_ns += <uint64_t>(ts_end.tv_nsec - ts_start.tv_nsec)
        self.execution_time += duration_ns
        if self._tracing_enabled:
            self._append_trace(rows, 0, nbytes, 0, duration_ns, 0)

        self._morsel_index += 1

    cdef void _dispatch_push(self, Morsel morsel) except *:
        """Cdef hot path. Cdef-class subclasses override this directly for
        true C-level vtable dispatch (no Python wrapper). Default falls
        through to `_push_impl` (cpdef) so Python-class subclasses can
        still override at the Python level."""
        self._push_impl(morsel)

    cpdef void _push_impl(self, Morsel morsel) except *:
        """Override in Python-class subclasses (aggregates, joins, special
        operators that carry many Python attributes). Default is no-op."""
        pass

    cdef void _emit_cdef(self, Morsel morsel) except *:
        """Cdef hot path for emitting downstream. Cdef-class operators call
        this directly to skip the cpdef Python-wrapper. Python-class
        operators call `emit` (cpdef) instead."""
        cdef uint64_t rows = 0
        cdef uint64_t nbytes = 0
        if morsel is not None:
            rows = morsel.num_rows
            nbytes = morsel.nbytes
            self.records_out += rows
            self.bytes_out += nbytes
        if self._downstream is not None:
            self._downstream.push(morsel)

    cpdef void emit(self, Morsel morsel) except *:
        """Python-callable wrapper around `_emit_cdef`. Used by Python-class
        operators (aggregates, joins) whose `def _push_impl` cannot call
        cdef methods directly."""
        self._emit_cdef(morsel)

    # ---- Source-side iterator (used by drive_scan) ------------------------------
    cdef Morsel next_morsel(self) except *:
        """Cdef hot-path source iterator. Returns the next morsel from this
        scan, or None on exhaustion. Cdef-class source operators can
        override this directly (no Python dispatch). Python-class scans
        override `_next_morsel_py` instead."""
        return <Morsel>self._next_morsel_py()

    cpdef object _next_morsel_py(self):
        """Default implementation: lazily wrap the source's existing
        `read_morsels()` generator. Source operators that need maximum
        performance can override with a state-machine impl, skipping the
        Python generator boundary altogether."""
        if self._morsel_iter is None:
            self._morsel_iter = iter(self.read_morsels())
        return next(self._morsel_iter, None)

    # ---- Pipeline wiring (called by pipeline_compiler) --------------------------
    cpdef void set_downstream(self, BasePlanNode node) except *:
        self._downstream = node

    cpdef void set_context(self, PipelineContext ctx) except *:
        self._ctx = ctx

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
    """Base class for joins. Two input sides — left feeds build, right feeds
    probe. Subclasses override `push_left` and `push_right` instead of
    `_push_impl`. The single `_push_impl` is never called on a JoinNode
    directly; the pipeline compiler routes inputs through adapter nodes."""
    cdef public object left_readers
    cdef public object right_readers
    cdef public list left_relation_names
    cdef public list right_relation_names
    cdef public object on
    cdef public object _join_key_cast_plan

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.is_join = True
        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []
        self.on = parameters.get("on")
        self._join_key_cast_plan = None

    cpdef void push_left(self, Morsel morsel) except *:
        """Build-side input. Subclasses override. MUST NOT call emit(EOS) —
        build-side EOS finalises internal state only."""
        pass

    cpdef void push_right(self, Morsel morsel) except *:
        """Probe-side input. Subclasses override. On EOS, call self.emit(EOS)
        to terminate the downstream chain."""
        pass

    @staticmethod
    def _join_numeric_target_type(left_type, right_type):
        from opteryx.types import OrsoTypes, find_compatible_type
        numeric_types = (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL)
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
            left_type = left.schema_column.type
            right_type = right.schema_column.type
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
        from opteryx.types import OrsoTypes
        if morsel is None or morsel is _EOS_SENTINEL:
            return morsel
        self._build_join_key_cast_plan()
        if not self._join_key_cast_plan:
            return morsel
        from draken.morsels.morsel import Morsel as _Morsel
        from opteryx.expression.casts import cast_to_double, cast_to_int
        names = list(morsel.column_names)
        vectors = [morsel.column(n) for n in names]
        changed = False
        for cast_rule in self._join_key_cast_plan:
            column_name = cast_rule["left_column"] if is_left else cast_rule["right_column"]
            if column_name not in names:
                continue
            idx = names.index(column_name)
            target_type = cast_rule["target_type"]
            if target_type == OrsoTypes.DOUBLE:
                vectors[idx] = cast_to_double(vectors[idx])
                changed = True
            elif target_type == OrsoTypes.INTEGER:
                vectors[idx] = cast_to_int(vectors[idx])
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

    One Python `yield` per emitted result morsel, not one per scan morsel."""
    cdef Morsel morsel
    cdef bint has_exit = exit_node is not None

    while True:
        morsel = scan.next_morsel()
        if morsel is None:
            break
        if ctx.is_terminated():
            break
        chain_head.push(morsel)
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

    chain_head.push(_EOS_SENTINEL)
    if has_exit:
        while exit_node.has_pending():
            yield exit_node.pop_pending()


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

    cdef void _dispatch_push(self, Morsel morsel) except *:
        self._join.push_left(morsel)


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

    cdef void _dispatch_push(self, Morsel morsel) except *:
        self._join.push_right(morsel)


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

include "../expression/evaluator/type_coercion.pyx"
include "../expression/evaluator/function_execution.pyx"
include "../expression/evaluator/arithmetic_dispatch.pyx"
include "../expression/evaluator/temporal_ops.pyx"
include "../expression/evaluator/string_ops.pyx"
include "../expression/evaluator/json_ops.pyx"
include "../expression/evaluator/case_eval.pyx"
include "../expression/evaluator/arithmetic.pyx"
include "../expression/evaluator/comparisons.pyx"
include "../expression/evaluator/evaluation.pyx"


def _verify_node_type_constants():
    """Fail-fast: the compile-time DEF constants in evaluation must mirror the
    runtime NodeType enum. If this assertion fires, update the DEFs in
    evaluator/evaluation.pyx and rebuild.
    """
    from opteryx.expression import NodeType

    expected = {
        "UNKNOWN": 0,
        "AND": 17, "OR": 18, "XOR": 19, "NOT": 20, "DNF": 21, "CNF": 22,
        "CASE": 32, "WILDCARD": 33, "COMPARISON_OPERATOR": 34,
        "BINARY_OPERATOR": 35, "UNARY_OPERATOR": 36, "FUNCTION": 37,
        "IDENTIFIER": 38, "SUBQUERY": 39, "NESTED": 40, "AGGREGATOR": 41,
        "LITERAL": 42, "EXPRESSION_LIST": 43, "EVALUATED": 44, "CAST": 45,
        "EXTRACTION_OPERATOR": 46, "BETWEEN": 47,
    }
    for name, value in expected.items():
        actual = int(getattr(NodeType, name))
        if actual != value:
            raise AssertionError(
                f"NodeType.{name} = {actual}, but evaluation.pyx DEF expects {value}. "
                f"Update the DEF constants at the top of "
                f"opteryx/expression/evaluator/evaluation.pyx and rebuild."
            )


# -----------------------------------------------------------------------------
# Include order: base classes / shared types before their consumers.
# -----------------------------------------------------------------------------

# ReaderNode is subclassed by function_dataset, parquet_read, show_value
include "read/read.pyx"

include "asof_join/asof_join.pyx"
include "cross_join/cross_join.pyx"
include "distinct/distinct.pyx"
include "hashed_inner_join/hashed_inner_join.pyx"
include "exit/exit.pyx"
include "explain/explain.pyx"
include "filter_join/filter_join.pyx"
include "filter/filter.pyx"
include "function_dataset/function_dataset.pyx"
include "heap_sort/heap_sort.pyx"
include "limit/limit.pyx"
include "nested_loop_join/nested_loop_join.pyx"
include "non_equi_join/non_equi_join.pyx"
include "null_reader/null_reader.pyx"
include "outer_join/outer_join.pyx"
include "parquet_read/parquet_read.pyx"
include "projection/projection.pyx"
include "set_variable/set_variable.pyx"
include "show_columns/show_columns.pyx"
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
