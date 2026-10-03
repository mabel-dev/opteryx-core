# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# distutils: language = c++

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The query's estimated statistics, native (native plan graph P5).

`StatisticsStore` holds one query's statistics (src/cpp/planner/stats_store.hpp):
the refresh fills it (statistics_refresh.hpp) and the planner reads it through
typed accessors - a node's row count, a column's NDV, null fraction or value
range, a predicate's selectivity against a node's output. There are no Python
statistics objects: consumers ask for the number they use.

Predicate selectivity and evaluation cost are src/cpp/planner/selectivity.hpp.
An expression's column slots are slots of its arena's bound ColumnTable
(`ExprArena.bind_columns`), so every entry point here takes expressions alone.

`StatisticsInput` builds statistics to estimate against WITHOUT a plan - an
input only (architect ruling 2026-09-28): tests of the estimator tiers, which
need a crafted histogram or byte-class profile, construct one.
"""

from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uintptr_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport uint8_t
from libcpp cimport bool as cbool
from libcpp.memory cimport make_shared
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from cpython.ref cimport PyObject

from opteryx.compiled.planner.column_table cimport ColumnRows
from opteryx.compiled.planner.column_table cimport ColumnTable
from opteryx.compiled.planner.column_table cimport kNoSlot
from opteryx.compiled.planner.column_type cimport ColumnTypeTable
from opteryx.compiled.planner.column_type cimport column_type_table
from opteryx.compiled.structures.expressions cimport ExprArena
from opteryx.compiled.structures.expressions cimport ExprTable
from opteryx.compiled.structures.expressions cimport NodeKinds
from opteryx.compiled.structures.expressions cimport node_kinds
from opteryx.compiled.structures.plan_steps cimport PlanStep
from opteryx.compiled.structures.plan_steps cimport StepManifest
from opteryx.compiled.structures.plan_steps cimport StepRow
from opteryx.third_party.maki_nage.distogram cimport Distogram
from opteryx.third_party.maki_nage.distogram cimport DistogramCore

from opteryx.exceptions import InvalidInternalStateError


cdef extern from "planner/stats_store.hpp" namespace "opteryx::planner":
    const int64_t kNoStat

    cdef cppclass StatBound:
        cbool present()
        @staticmethod
        StatBound of_int(int64_t v)
        @staticmethod
        StatBound of_uint(uint64_t v)
        @staticmethod
        StatBound of_float(double v)

    cdef cppclass ColumnStats:
        int64_t distinct_count
        int64_t base_distinct_count
        StatBound lower
        StatBound upper
        shared_ptr[DistogramCore] histogram   # shared_ptr<const Distogram> in C++
        cbool has_null_fraction
        double null_fraction
        cbool has_char_class
        double class_proportions[8]
        double avg_length
        cbool has_ordinal_bounds
        int64_t ordinal_lo
        int64_t ordinal_hi
        cbool has_length_bounds
        int64_t length_lo
        int64_t length_hi
        cbool has_total_bytes
        int64_t domain_distinct_count()

    cdef cppclass RelationStats:
        int64_t row_count_metric
        int64_t row_count_estimate
        int64_t base_row_count
        void set_column(uint32_t root, shared_ptr[ColumnStats] stats)
        void check() except +
        int64_t row_count()
        cbool row_count_is_metric()
        int64_t domain_row_count()
        const ColumnStats* column(uint32_t root)

    cdef cppclass StatsStore:
        const RelationStats* node(uint32_t nid)
        const RelationStats* cte(const string& key)
        void set_node(uint32_t nid, shared_ptr[RelationStats] stats) except +


cdef extern from "planner/selectivity.hpp" namespace "opteryx::planner::selectivity_detail":
    const uint8_t kAsciiClass[128]
    const double kClassCardinality[8]
    const int kExtendedClass


cdef extern from "planner/selectivity.hpp" namespace "opteryx::planner":
    const double kRangeFallbackSelectivity
    const double kLikePrefixSelectivity
    const double kLikeInfixSelectivity

    cdef cppclass SelectivityInputs:
        const ExprTable* exprs
        const ColumnRows* columns
        const ColumnTypeTable* types
        const NodeKinds* kinds

    double c_estimate_selectivity "opteryx::planner::estimate_selectivity"(
        const SelectivityInputs& inputs, int64_t predicate, const RelationStats& stats) except +
    const char* c_predicate_estimator_tag "opteryx::planner::predicate_estimator_tag"(
        const SelectivityInputs& inputs, int64_t predicate, const RelationStats& stats) except +
    double c_predicate_cost "opteryx::planner::predicate_cost"(
        const SelectivityInputs& inputs, int64_t predicate, const unordered_map[string, double]& function_costs) except +
    double c_predicate_base_cost "opteryx::planner::predicate_base_cost"(
        const SelectivityInputs& inputs, int64_t predicate) except +


cdef extern from "planner/plan_topology.hpp" namespace "opteryx::planner":
    cdef cppclass SPlanNode "opteryx::planner::PlanNode":
        uint32_t id

    cdef cppclass SPlanTopology "opteryx::planner::PlanTopology":
        size_t size()
        const SPlanNode& node_at(size_t position)


cdef extern from "planner/_plan_graph.hpp" namespace "opteryx::planner":
    cdef cppclass SPlanGraph "opteryx::planner::PlanGraph"(SPlanTopology):
        PyObject* step_at(size_t position)


cdef extern from "planner/statistics_refresh.hpp" namespace "opteryx::planner":
    cdef cppclass StepKinds:
        int32_t scan, filter, join, aggregate_and_group, aggregate, limit
        int32_t heap_sort, distinct, project, union_ "union_", intersect, except_ "except_"
        int32_t materialized_cte_ref, order

    cdef cppclass PredicateNote:
        uint32_t nid
        cbool scan
        string relation
        int64_t condition
        double selectivity
        cbool has_cost
        double cost
        const char* estimator

    cdef cppclass JoinNote:
        uint32_t nid
        cbool has_join_type
        string join_type
        int64_t left_rows
        int64_t right_rows
        int64_t out_rows
        int64_t key_count

    cdef cppclass RefreshTelemetry:
        vector[PredicateNote] predicates
        vector[JoinNote] joins

    cdef cppclass RefreshInputs:
        const SPlanTopology* graph
        const vector[const StepRow*]* rows
        SelectivityInputs selectivity
        const StepKinds* steps
        const unordered_map[string, double]* function_costs
        StatsStore* store
        RefreshTelemetry* telemetry

    void c_refresh_statistics "opteryx::planner::refresh_statistics"(const RefreshInputs& inputs) except +
    void c_compute_node_statistics "opteryx::planner::compute_node_statistics"(const RefreshInputs& inputs, uint32_t nid) except +


cdef extern from *:
    """
    #include "planner/stats_store.hpp"
    #include "planner/statistics_refresh.hpp"

    // An int128 as the two's-complement halves Python rebuilds it from.
    static inline void int128_parts(__int128 v, int64_t* hi, uint64_t* lo) {
        const unsigned __int128 bits = static_cast<unsigned __int128>(v);
        *hi = static_cast<int64_t>(static_cast<uint64_t>(bits >> 64));
        *lo = static_cast<uint64_t>(bits);
    }
    static inline __int128 int128_of(int64_t hi, uint64_t lo) {
        return static_cast<__int128>((static_cast<unsigned __int128>(static_cast<uint64_t>(hi)) << 64) | lo);
    }
    // A bound's kind (0 none, 1 integer, 2 float) and value.
    static inline int stat_bound_parts(const opteryx::planner::StatBound& b, int64_t* hi, uint64_t* lo, double* d) {
        int128_parts(b.i, hi, lo);
        *d = b.d;
        return static_cast<int>(b.kind);
    }
    static inline bool column_total_bytes(const opteryx::planner::ColumnStats& c, int64_t* hi, uint64_t* lo) {
        if (!c.has_total_bytes) return false;
        int128_parts(c.total_bytes, hi, lo);
        return true;
    }
    static inline void set_column_total_bytes(opteryx::planner::ColumnStats& c, int64_t hi, uint64_t lo) {
        c.has_total_bytes = true;
        c.total_bytes = int128_of(hi, lo);
    }
    static inline bool relation_total_bytes(const opteryx::planner::RelationStats& r, int64_t* hi, uint64_t* lo) {
        __int128 total = 0;
        if (!opteryx::planner::node_total_bytes(r, total)) return false;
        int128_parts(total, hi, lo);
        return true;
    }
    static inline bool store_set_cte(opteryx::planner::StatsStore& store, const std::string& key, uint32_t nid) {
        auto stats = store.node_ptr(nid);
        if (!stats) return false;
        store.set_cte(key, stats);
        return true;
    }
    // A scan step's read bytes: its memoized base statistics' byte total.
    static inline bool scan_read_bytes(const opteryx::planner::SelectivityInputs& in,
                                       opteryx::planner::StatsStore& store, const opteryx::planner::StepRow& row,
                                       int64_t* hi, uint64_t* lo) {
        auto base = opteryx::planner::scan_base_statistics(in, store, row);
        return relation_total_bytes(*base, hi, lo);
    }
    static inline double manifest_selectivity(const opteryx::planner::SelectivityInputs& in,
                                              const opteryx::planner::NativeManifest& m,
                                              const std::vector<uint32_t>& schema_slots, int64_t predicate) {
        auto stats = opteryx::planner::manifest_statistics(in, m, schema_slots);
        return opteryx::planner::estimate_selectivity(in, predicate, *stats);
    }
    """
    int stat_bound_parts(const StatBound& b, int64_t* hi, uint64_t* lo, double* d)
    cbool column_total_bytes(const ColumnStats& c, int64_t* hi, uint64_t* lo)
    void set_column_total_bytes(ColumnStats& c, int64_t hi, uint64_t lo)
    cbool relation_total_bytes(const RelationStats& r, int64_t* hi, uint64_t* lo)
    cbool store_set_cte(StatsStore& store, const string& key, uint32_t nid) except +
    cbool c_scan_read_bytes "scan_read_bytes"(const SelectivityInputs& inputs, StatsStore& store, const StepRow& row,
                                             int64_t* hi, uint64_t* lo) except +
    double c_manifest_selectivity "manifest_selectivity"(const SelectivityInputs& inputs, const StepManifest& m,
                                                         const vector[uint32_t]& schema_slots, int64_t predicate) except +


# The no-statistics fallback constants (fallback_selectivity.py re-exports).
RANGE_FALLBACK_SELECTIVITY = kRangeFallbackSelectivity
LIKE_PREFIX_SELECTIVITY = kLikePrefixSelectivity
LIKE_INFIX_SELECTIVITY = kLikeInfixSelectivity

# The byte classes of ColumnStats.class_proportions, in order; each byte's
# class (every byte >= 0x80 is "extended"); each class's count of byte values.
# The char-class estimator's tables, read from selectivity.hpp.
CHAR_CLASSES = ("upper", "lower", "digit", "whitespace", "punct_text", "semantic", "extended", "control")
BYTE_CLASS = tuple(kAsciiClass[b] if b < 128 else kExtendedClass for b in range(256))
CLASS_CARDINALITY = {CHAR_CLASSES[k]: int(kClassCardinality[k]) for k in range(8)}


# ---------------------------------------------------------------------------
# inputs every native read needs
# ---------------------------------------------------------------------------

cdef StepKinds _STEP_KINDS
cdef bint _STEP_KINDS_LOADED = False


cdef const StepKinds* _step_kinds() except NULL:
    """LogicalPlanStepType's values, read once - never restated."""
    global _STEP_KINDS_LOADED
    if not _STEP_KINDS_LOADED:
        from opteryx.planner.logical_planner import LogicalPlanStepType

        _STEP_KINDS.scan = LogicalPlanStepType.Scan.value
        _STEP_KINDS.filter = LogicalPlanStepType.Filter.value
        _STEP_KINDS.join = LogicalPlanStepType.Join.value
        _STEP_KINDS.aggregate_and_group = LogicalPlanStepType.AggregateAndGroup.value
        _STEP_KINDS.aggregate = LogicalPlanStepType.Aggregate.value
        _STEP_KINDS.limit = LogicalPlanStepType.Limit.value
        _STEP_KINDS.heap_sort = LogicalPlanStepType.HeapSort.value
        _STEP_KINDS.distinct = LogicalPlanStepType.Distinct.value
        _STEP_KINDS.project = LogicalPlanStepType.Project.value
        _STEP_KINDS.union_ = LogicalPlanStepType.Union.value
        _STEP_KINDS.intersect = LogicalPlanStepType.Intersect.value
        _STEP_KINDS.except_ = LogicalPlanStepType.Except.value
        _STEP_KINDS.materialized_cte_ref = LogicalPlanStepType.MaterializedCteRef.value
        _STEP_KINDS.order = LogicalPlanStepType.Order.value
        _STEP_KINDS_LOADED = True
    return &_STEP_KINDS


cdef unordered_map[string, double] _FUNCTION_COSTS
cdef object _FUNCTION_COSTS_VERSION = None


cdef const unordered_map[string, double]* _function_costs() except NULL:
    """Every function name and alias -> its first overload's measured cost (as
    FunctionCatalog.get_cost resolves it), rebuilt when the catalog changes."""
    global _FUNCTION_COSTS_VERSION
    from opteryx.expression.functions import get_catalog

    catalog = get_catalog()
    if _FUNCTION_COSTS_VERSION is not None and _FUNCTION_COSTS_VERSION == catalog.version:
        return &_FUNCTION_COSTS
    _FUNCTION_COSTS.clear()
    for name, definition in catalog._functions.items():
        if definition.overloads:
            _FUNCTION_COSTS[(<str>name).encode("utf-8")] = definition.overloads[0].kernel.cost_us_per_million
    for alias, canonical in catalog._aliases.items():
        definition = catalog._functions.get(canonical)
        if definition is not None and definition.overloads:
            _FUNCTION_COSTS[(<str>alias).encode("utf-8")] = definition.overloads[0].kernel.cost_us_per_million
        else:
            _FUNCTION_COSTS.erase((<str>alias).encode("utf-8"))
    _FUNCTION_COSTS_VERSION = catalog.version
    return &_FUNCTION_COSTS


cdef ColumnTable _columns_of(ExprArena arena):
    if arena._columns is None:
        raise InvalidInternalStateError("An expression arena no query has bound has no column table to read.")
    return <ColumnTable?>arena._columns


cdef inline void _inputs(SelectivityInputs& inputs, ExprArena arena) except *:
    cdef ColumnTable columns = _columns_of(arena)
    inputs.exprs = arena._table
    inputs.columns = &columns._rows
    inputs.types = column_type_table()
    inputs.kinds = node_kinds()


cdef inline object _int128(int64_t hi, uint64_t lo):
    return (<object>hi << 64) + <object>lo


cdef inline object _stat(int64_t value):
    return None if value == kNoStat else value


cdef object _bound(const StatBound& b):
    cdef int64_t hi = 0
    cdef uint64_t lo = 0
    cdef double d = 0.0
    cdef int kind = stat_bound_parts(b, &hi, &lo, &d)
    if kind == 0:
        return None
    if kind == 1:
        return _int128(hi, lo)
    return d


# ---------------------------------------------------------------------------
# predicates, with no plan
# ---------------------------------------------------------------------------

def predicate_cost(predicate):
    """The relative per-row cost of evaluating `predicate`: the measured catalog
    cost of every function in it, else its comparison's."""
    cdef SelectivityInputs inputs
    _inputs(inputs, predicate.arena)
    return c_predicate_cost(inputs, predicate.expr_id, _function_costs()[0])


def predicate_base_cost(predicate):
    """The relative per-row cost of a simple (function-free) comparison."""
    cdef SelectivityInputs inputs
    _inputs(inputs, predicate.arena)
    return c_predicate_base_cost(inputs, predicate.expr_id)


def manifest_selectivity(manifest, predicate):
    """The estimated fraction of `manifest`'s rows matching `predicate`, from the
    manifest's own statistics over the schema it is bound with."""
    cdef SelectivityInputs inputs
    _inputs(inputs, predicate.arena)
    cdef vector[uint32_t] slots
    for column in manifest.schema.columns:
        slots.push_back(column.slot)
    cdef const StepManifest* native = <const StepManifest*><uintptr_t>manifest.native.borrowed_address()
    return c_manifest_selectivity(inputs, native[0], slots, predicate.expr_id)


cdef class StatisticsInput:
    """Statistics to estimate against without a plan - an INPUT only (architect
    ruling 2026-09-28): tests of the estimator tiers build one. Columns are
    keyed by identity and resolved through `columns`, the query's ColumnTable.

    Each column's statistics are a dict of: distinct_count, base_distinct_count,
    value_range (lower, upper - ints or floats, either None), histogram (a
    Distogram), null_fraction, class_proportions ({class: share} over
    CHAR_CLASSES; with avg_length), avg_length, ordinal_bounds (lo, hi),
    length_bounds (lo, hi), total_bytes. Exactly one of row_count_metric /
    row_count_estimate."""

    cdef shared_ptr[RelationStats] _stats

    def __init__(self, ColumnTable columns not None, *, row_count_metric=None, row_count_estimate=None,
                 base_row_count=None, dict column_stats=None):
        self._stats = make_shared[RelationStats]()
        cdef RelationStats* rel = self._stats.get()
        rel.row_count_metric = _optional_int(row_count_metric)
        rel.row_count_estimate = _optional_int(row_count_estimate)
        rel.base_row_count = _optional_int(base_row_count)
        rel.check()
        cdef shared_ptr[ColumnStats] column
        cdef uint32_t root
        for identity, fields in (column_stats or {}).items():
            root = columns._rows.root_of(<bytes?>identity)
            if root == kNoSlot:
                raise KeyError(f"statistics for a column this query never bound: {identity!r}")
            column = make_shared[ColumnStats]()
            _fill_column(column.get(), <dict?>fields)
            rel.set_column(root, column)


cdef set _COLUMN_FIELDS = {
    "distinct_count", "base_distinct_count", "value_range", "histogram", "null_fraction",
    "class_proportions", "avg_length", "ordinal_bounds", "length_bounds", "total_bytes",
}


cdef inline int64_t _optional_int(object value) except? -1:
    return kNoStat if value is None else <int64_t>value


cdef StatBound _to_bound(object value) except *:
    cdef StatBound none
    if value is None:
        return none
    if type(value) is int:
        if value > 9223372036854775807:
            return StatBound.of_uint(<uint64_t>value)
        return StatBound.of_int(<int64_t>value)
    if type(value) is float:
        return StatBound.of_float(<double>value)
    raise TypeError(f"a value_range bound is an int or a float, not {type(value).__name__}")


cdef void _fill_column(ColumnStats* c, dict fields) except *:
    unknown = set(fields) - _COLUMN_FIELDS
    if unknown:
        raise TypeError(f"unknown column statistics: {sorted(unknown)}")
    cdef Distogram histogram
    cdef int k
    c.distinct_count = _optional_int(fields.get("distinct_count"))
    c.base_distinct_count = _optional_int(fields.get("base_distinct_count"))
    value_range = fields.get("value_range")
    if value_range is not None:
        c.lower = _to_bound(value_range[0])
        c.upper = _to_bound(value_range[1])
    if fields.get("histogram") is not None:
        histogram = <Distogram?>fields["histogram"]
        c.histogram = histogram.core
    if fields.get("null_fraction") is not None:
        c.has_null_fraction = True
        c.null_fraction = fields["null_fraction"]
    proportions = fields.get("class_proportions")
    if (proportions is None) != (fields.get("avg_length") is None):
        raise ValueError("class_proportions and avg_length are recorded together")
    if proportions is not None:
        c.has_char_class = True
        for k in range(8):
            c.class_proportions[k] = proportions.get(CHAR_CLASSES[k], 0.0)
        c.avg_length = fields["avg_length"]
    if fields.get("ordinal_bounds") is not None:
        c.has_ordinal_bounds = True
        c.ordinal_lo = fields["ordinal_bounds"][0]
        c.ordinal_hi = fields["ordinal_bounds"][1]
    if fields.get("length_bounds") is not None:
        c.has_length_bounds = True
        c.length_lo = fields["length_bounds"][0]
        c.length_hi = fields["length_bounds"][1]
    total = fields.get("total_bytes")
    if total is not None:
        if total < 0 or total >= (1 << 126):
            raise ValueError(f"total_bytes out of range: {total}")
        set_column_total_bytes(c[0], <int64_t>(total >> 64), <uint64_t>(total & 0xFFFFFFFFFFFFFFFF))


def estimate_selectivity(predicate, StatisticsInput stats not None):
    """The estimated fraction of `stats`' rows matching `predicate`, in [0, 1]."""
    cdef SelectivityInputs inputs
    _inputs(inputs, predicate.arena)
    return c_estimate_selectivity(inputs, predicate.expr_id, stats._stats.get()[0])


def predicate_estimator_tag(predicate, StatisticsInput stats not None):
    """Which estimator tier a LIKE-family predicate uses against `stats`, or None."""
    cdef SelectivityInputs inputs
    _inputs(inputs, predicate.arena)
    cdef const char* tag = c_predicate_estimator_tag(inputs, predicate.expr_id, stats._stats.get()[0])
    return None if tag == NULL else tag.decode("ascii")


# ---------------------------------------------------------------------------
# the store
# ---------------------------------------------------------------------------

cdef void _gather_rows(const SPlanGraph* graph, vector[const StepRow*]& rows) except *:
    """Each node's native row, by node id."""
    cdef size_t i
    cdef uint32_t nid
    cdef PlanStep step
    for i in range(graph.size()):
        nid = graph.node_at(i).id
        if nid >= rows.size():
            rows.resize(nid + 1, NULL)
        step = <PlanStep?>(<object>graph.step_at(i))
        rows[nid] = step._row


cdef class StatisticsStore:
    """One query's estimated statistics: every node the last refresh of its plan
    reached, keyed by node id; shared CTEs by key; the scan base memo.

    Readers take a node id and, for a column, its identity. A node the refresh
    never reached has no statistics: its accessors return None (`has` is False).
    """

    cdef StatsStore _store
    cdef ExprArena _expressions

    def __cinit__(self, ExprArena expressions not None):
        self._expressions = expressions

    cdef void _inputs(self, SelectivityInputs& inputs) except *:
        _inputs(inputs, self._expressions)

    cdef const RelationStats* _node(self, uint32_t nid):
        return self._store.node(nid)

    cdef const ColumnStats* _column(self, uint32_t nid, bytes identity) except? NULL:
        cdef const RelationStats* rel = self._store.node(nid)
        if rel == NULL:
            return NULL
        cdef ColumnTable columns = _columns_of(self._expressions)
        cdef uint32_t root = columns._rows.root_of(identity)
        if root == kNoSlot:
            raise InvalidInternalStateError(f"A column this query never bound was read: {identity!r}.")
        return rel.column(root)

    # --- filling -----------------------------------------------------------

    def refresh(self, plan, bint telemetry=False):
        """Recompute the statistics of every node of `plan` (a LogicalPlan of this
        query). With `telemetry`, returns (predicate notes, join notes): each
        predicate note names its condition by expr_id, for the caller to render."""
        cdef const SPlanGraph* graph = <const SPlanGraph*><uintptr_t>plan.graph_address()
        cdef vector[const StepRow*] rows
        _gather_rows(graph, rows)
        cdef RefreshInputs inputs
        cdef RefreshTelemetry notes
        inputs.graph = graph
        inputs.rows = &rows
        self._inputs(inputs.selectivity)
        inputs.steps = _step_kinds()
        inputs.store = &self._store
        if telemetry:
            inputs.function_costs = _function_costs()
            inputs.telemetry = &notes
        c_refresh_statistics(inputs)
        if not telemetry:
            return None
        predicates = [
            (
                n.nid, n.scan, (<bytes>n.relation).decode("utf-8") if n.scan else None, n.condition,
                n.selectivity, n.cost if n.has_cost else None,
                None if n.estimator == NULL else (<bytes>n.estimator).decode("ascii"),
            )
            for n in notes.predicates
        ]
        joins = [
            (
                j.nid, (<bytes>j.join_type).decode("utf-8") if j.has_join_type else None,
                j.left_rows, j.right_rows, j.out_rows, j.key_count,
            )
            for j in notes.joins
        ]
        return predicates, joins

    def seed(self, uint32_t nid, StatisticsInput stats not None):
        """Record `stats` as node `nid`'s statistics - an INPUT (architect ruling
        2026-09-28): a test of one operator's propagation seeds that node's
        inputs, then `compute`s the node."""
        self._store.set_node(nid, stats._stats)

    def compute(self, plan, uint32_t nid):
        """Compute node `nid` of `plan` alone, from the statistics its inputs
        already hold here (none is recomputed)."""
        cdef const SPlanGraph* graph = <const SPlanGraph*><uintptr_t>plan.graph_address()
        cdef vector[const StepRow*] rows
        _gather_rows(graph, rows)
        cdef RefreshInputs inputs
        inputs.graph = graph
        inputs.rows = &rows
        self._inputs(inputs.selectivity)
        inputs.steps = _step_kinds()
        inputs.store = &self._store
        c_compute_node_statistics(inputs, nid)

    def set_cte(self, str cte_key not None, uint32_t nid):
        """Record node `nid`'s statistics as shared CTE `cte_key`'s output (its
        body's head, or a recursive CTE's anchor head)."""
        if not store_set_cte(self._store, cte_key.encode("utf-8"), nid):
            raise InvalidInternalStateError(f"Node {nid} has no statistics to record for CTE {cte_key!r}.")

    def scan_read_bytes(self, PlanStep scan not None):
        """The dense logical bytes a scan step reads: its base statistics (before
        any predicate) summed over the columns with a known size; 0 when none."""
        cdef SelectivityInputs inputs
        self._inputs(inputs)
        cdef int64_t hi = 0
        cdef uint64_t lo = 0
        if not c_scan_read_bytes(inputs, self._store, scan._row[0], &hi, &lo):
            return 0
        return _int128(hi, lo)

    # --- a node ------------------------------------------------------------

    def has(self, uint32_t nid):
        return self._store.node(nid) != NULL

    def row_count(self, uint32_t nid):
        """The node's working row count, whatever its provenance; None without statistics."""
        cdef const RelationStats* rel = self._store.node(nid)
        return None if rel == NULL else rel.row_count()

    def row_count_estimate(self, uint32_t nid):
        """The row count when it is an ESTIMATE, else None."""
        cdef const RelationStats* rel = self._store.node(nid)
        return None if rel == NULL else _stat(rel.row_count_estimate)

    def has_cte(self, str cte_key not None):
        """Whether shared CTE `cte_key`'s output statistics were recorded."""
        return self._store.cte(cte_key.encode("utf-8")) != NULL

    def row_count_metric(self, uint32_t nid):
        """The row count when it is a METRIC (a number we claim to know), else None."""
        cdef const RelationStats* rel = self._store.node(nid)
        return None if rel == NULL else _stat(rel.row_count_metric)

    def base_row_count(self, uint32_t nid):
        """The pre-filter row count recorded for the node, or None (unset or no statistics)."""
        cdef const RelationStats* rel = self._store.node(nid)
        return None if rel == NULL else _stat(rel.base_row_count)

    def domain_row_count(self, uint32_t nid):
        """The pre-filter row count, falling back to the row count; None without statistics."""
        cdef const RelationStats* rel = self._store.node(nid)
        return None if rel == NULL else rel.domain_row_count()

    def total_bytes(self, uint32_t nid):
        """The node's dense bytes over its columns with a known size; None when none has one."""
        cdef const RelationStats* rel = self._store.node(nid)
        cdef int64_t hi = 0
        cdef uint64_t lo = 0
        if rel == NULL or not relation_total_bytes(rel[0], &hi, &lo):
            return None
        return _int128(hi, lo)

    def estimate_selectivity(self, uint32_t nid, predicate):
        """The estimated fraction of node `nid`'s output rows matching `predicate`."""
        cdef const RelationStats* rel = self._store.node(nid)
        if rel == NULL:
            raise InvalidInternalStateError(f"Node {nid} has no statistics to estimate a predicate against.")
        cdef SelectivityInputs inputs
        self._inputs(inputs)
        return c_estimate_selectivity(inputs, predicate.expr_id, rel[0])

    # --- a column of a node --------------------------------------------------

    def has_column(self, uint32_t nid, bytes identity not None):
        return self._column(nid, identity) != NULL

    def distinct_count(self, uint32_t nid, bytes identity not None):
        """The column's live distinct count at the node, or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL else _stat(c.distinct_count)

    def domain_distinct_count(self, uint32_t nid, bytes identity not None):
        """The column's PRE-filter distinct count (its key domain), or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL else _stat(c.domain_distinct_count())

    def null_fraction(self, uint32_t nid, bytes identity not None):
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL or not c.has_null_fraction else c.null_fraction

    def value_range(self, uint32_t nid, bytes identity not None):
        """The column's (lower, upper) numeric bounds at the node - either may be
        None - or None when neither is known."""
        cdef const ColumnStats* c = self._column(nid, identity)
        if c == NULL or (not c.lower.present() and not c.upper.present()):
            return None
        return _bound(c.lower), _bound(c.upper)

    def column_total_bytes(self, uint32_t nid, bytes identity not None):
        cdef const ColumnStats* c = self._column(nid, identity)
        cdef int64_t hi = 0
        cdef uint64_t lo = 0
        if c == NULL or not column_total_bytes(c[0], &hi, &lo):
            return None
        return _int128(hi, lo)

    def has_histogram(self, uint32_t nid, bytes identity not None):
        cdef const ColumnStats* c = self._column(nid, identity)
        return c != NULL and <bint>c.histogram

    def ordinal_bounds(self, uint32_t nid, bytes identity not None):
        """The column's relation-wide ordinal-key (lo, hi), or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL or not c.has_ordinal_bounds else (c.ordinal_lo, c.ordinal_hi)

    def length_bounds(self, uint32_t nid, bytes identity not None):
        """The column's observed (min, max) value length in bytes, or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL or not c.has_length_bounds else (c.length_lo, c.length_hi)

    def class_proportions(self, uint32_t nid, bytes identity not None):
        """{class: share of the column's bytes} over CHAR_CLASSES, or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        cdef int k
        if c == NULL or not c.has_char_class:
            return None
        return {CHAR_CLASSES[k]: c.class_proportions[k] for k in range(8)}

    def avg_length(self, uint32_t nid, bytes identity not None):
        """The column's mean non-null value length in bytes, or None."""
        cdef const ColumnStats* c = self._column(nid, identity)
        return None if c == NULL or not c.has_char_class else c.avg_length
