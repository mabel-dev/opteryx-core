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
from draken.morsels.cxx_morsel cimport CxxMorsel, MorselState, ErrCtx, cxx_morsel_new_eos, cxx_morsel_delete
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

from libc.stdint cimport int8_t, int16_t, int64_t, uint64_t
from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy


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
        context (e.g. a direct-push unit test). First exception wins."""
        if self._ctx is not None:
            if self._ctx._exc is None:
                self._ctx._exc = exc
        elif self._cxx_push_exc is None:
            self._cxx_push_exc = exc
        if err != NULL:
            err.code = 1

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
            nbytes = <uint64_t>rows * <uint64_t>raw.num_columns() * <uint64_t>8
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
            nbytes = <uint64_t>rows * <uint64_t>raw.num_columns() * <uint64_t>8
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
                       PipelineContext ctx, object sink):
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
include "distinct/distinct.pyx"
include "hashed_inner_join/hashed_inner_join.pyx"
include "exit/exit.pyx"
include "explain/explain.pyx"
include "filter_join/filter_join.pyx"
include "filter/filter.pyx"
include "function_dataset/function_dataset.pyx"
include "heap_sort/heap_sort.pyx"
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
