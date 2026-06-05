# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Draken-native global aggregation node.

This operator stays on Draken morsels end-to-end. The accumulation work is
delegated to the lower-level ungrouped aggregate engine; the per-morsel hot
path here is deliberately Python-free — typed cdef classes for result specs
and literal accumulators, early-out when no per-row evaluation is needed,
and no instrumentation syscalls.
"""

from libc.stdint cimport uint8_t, int64_t

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import QueryProperties
from opteryx.operators.aggregate.helpers import extract_evaluations
from opteryx.types import OrsoTypes

# BasePlanNode in scope via textual include from _operators.pyx (umbrella unit).
# EOS sentinel available as _EOS_SENTINEL in the same scope.

# Literal-aggregate-kind tags (typed, no string comparisons on hot path)
cdef int _LITERAL_NONE          = 0
cdef int _LITERAL_COUNT         = 1
cdef int _LITERAL_COUNT_DISTINCT = 2
cdef int _LITERAL_SUM           = 3
cdef int _LITERAL_AVG           = 4
cdef int _LITERAL_MIN_MAX_ANY   = 5
cdef int _LITERAL_MEDIAN        = 6


def _column_bytes(identity):
    return identity if isinstance(identity, bytes) else str(identity).encode("utf-8")




def _count_null_bitmap(const uint8_t* bitmap, Py_ssize_t nrows) -> int:
    cdef Py_ssize_t i
    cdef Py_ssize_t count = 0

    if bitmap == NULL:
        return 0

    for i in range(nrows):
        if not ((bitmap[i >> 3] >> (i & 7)) & 1):
            count += 1

    return <int>count


def _vector_null_count(vector) -> int:
    cdef Vector typed_vector = <Vector>vector
    return _count_null_bitmap(typed_vector.null_bitmap_ptr(), len(typed_vector))


def _vector_sum(vector):
    cdef Vector typed_vector = <Vector>vector
    return typed_vector.sum()


def _vector_min(vector):
    cdef Vector typed_vector = <Vector>vector
    return typed_vector.min()


def _vector_max(vector):
    cdef Vector typed_vector = <Vector>vector
    return typed_vector.max()


def _parameter_identity(parameter):
    schema_column = getattr(parameter, "schema_column", None)
    if schema_column is None:
        return None
    identity = getattr(schema_column, "identity", None)
    if identity in (None, "", b""):
        return None
    return _column_bytes(identity)


def _parameter_type(parameter):
    schema_column = getattr(parameter, "schema_column", None)
    if schema_column is None:
        return None
    return getattr(schema_column, "type", None)


def _is_float_type(type_value) -> bool:
    if type_value is None:
        return False
    value = getattr(type_value, "value", type_value)
    return value in ("DOUBLE", "FLOAT", "DECIMAL")


def _is_decimal_type(type_value) -> bool:
    if type_value is None:
        return False
    value = getattr(type_value, "value", type_value)
    return value == "DECIMAL"


def _is_string_type(type_value) -> bool:
    if type_value is None:
        return False
    value = getattr(type_value, "value", type_value)
    return value in ("VARCHAR", "BLOB")


cdef class _LiteralAggState:
    """Typed accumulator for an aggregate whose input is a literal.

    Replaces the dict-based literal spec — fields are looked up directly,
    and `update()`/`finalize()` are cdef so the per-morsel call has no
    Python attribute / dict lookups.
    """
    cdef int      kind
    cdef bint     distinct
    cdef object   literal      # Python scalar (may be None)
    cdef int64_t  count
    cdef object   sum_         # None until first non-zero contribution
    cdef object   value
    cdef bint     seen

    def __cinit__(self, int kind, bint distinct, object literal):
        self.kind = kind
        self.distinct = distinct
        self.literal = literal
        self.count = 0
        self.sum_ = None
        self.value = None
        self.seen = False

    cdef void update(self, Py_ssize_t row_count):
        if row_count == 0:
            return

        cdef int kind = self.kind
        cdef object literal = self.literal

        if kind == _LITERAL_COUNT:
            if self.distinct:
                if literal is not None:
                    self.seen = True
                return
            if literal is not None:
                self.count += row_count
            return

        if kind == _LITERAL_COUNT_DISTINCT:
            if literal is not None:
                self.seen = True
            return

        if literal is None:
            return

        if kind == _LITERAL_SUM:
            contribution = literal * row_count
            self.sum_ = contribution if self.sum_ is None else self.sum_ + contribution
            return

        if kind == _LITERAL_AVG:
            contribution = literal * row_count
            self.sum_ = contribution if self.sum_ is None else self.sum_ + contribution
            self.count += row_count
            return

        if kind == _LITERAL_MIN_MAX_ANY:
            self.value = literal
            self.seen = True
            return

        if kind == _LITERAL_MEDIAN:
            self.value = literal
            self.seen = True
            return

    cdef object finalize(self):
        cdef int kind = self.kind
        cdef object literal = self.literal

        if kind == _LITERAL_COUNT:
            if self.distinct:
                return 1 if literal is not None else 0
            return self.count

        if kind == _LITERAL_COUNT_DISTINCT:
            return 1 if literal is not None else 0

        if kind == _LITERAL_SUM:
            return self.sum_

        if kind == _LITERAL_AVG:
            if self.count == 0 or self.sum_ is None:
                return None
            return self.sum_ / self.count

        if kind == _LITERAL_MIN_MAX_ANY:
            return self.value if self.seen else None

        if kind == _LITERAL_MEDIAN:
            if not self.seen or self.value is None:
                return None
            return float(self.value)

        raise ValueError(f"Unsupported literal aggregate kind: {kind}")


cdef int _literal_kind_for(aggregate_type: str) except -1:
    if aggregate_type == "COUNT":
        return _LITERAL_COUNT
    if aggregate_type == "COUNT_DISTINCT" or aggregate_type == "DISTINCT":
        return _LITERAL_COUNT_DISTINCT
    if aggregate_type == "SUM":
        return _LITERAL_SUM
    if aggregate_type == "AVG":
        return _LITERAL_AVG
    if aggregate_type == "MIN" or aggregate_type == "MAX" or aggregate_type == "ANY_VALUE":
        return _LITERAL_MIN_MAX_ANY
    if aggregate_type == "MEDIAN":
        return _LITERAL_MEDIAN
    raise ValueError(f"Unsupported literal aggregate type: {aggregate_type}")


def _make_literal_state(aggregate):
    parameter = aggregate.parameters[0] if aggregate.parameters else None
    literal = None if parameter is None else parameter.value
    aggregate_type = aggregate.value
    distinct = getattr(aggregate, "duplicate_treatment", None) == "Distinct"
    return _LiteralAggState(_literal_kind_for(aggregate_type), distinct, literal)


cdef class _ResultSpec:
    """Typed result-emit slot.

    For engine-kind specs we hold the alias bytes that addresses the column
    in the engine's finalize() output. For literal-kind specs we hold the
    typed `_LiteralAggState` accumulator updated per morsel.

    `kind`: 0 = engine column, 1 = literal accumulator.
    """
    cdef int                kind
    cdef bytes              output_name
    cdef _LiteralAggState   state

    def __cinit__(self, int kind, bytes output_name, _LiteralAggState state):
        self.kind = kind
        self.output_name = output_name
        self.state = state


def _build_engine_aggregate(aggregate):
    parameter = aggregate.parameters[0] if aggregate.parameters else None
    aggregate_type = aggregate.value
    duplicate_treatment = getattr(aggregate, "duplicate_treatment", None)
    output_name = _column_bytes(aggregate.schema_column.identity)
    parameter_name = _parameter_identity(parameter)
    parameter_type = _parameter_type(parameter)

    if aggregate_type == "COUNT":
        if parameter is not None and parameter.node_type == NodeType.WILDCARD:
            return [CountStarAggregate(output_name)], None, None

        if parameter is not None and parameter.node_type == NodeType.LITERAL:
            if parameter.value == "*":
                return [CountStarAggregate(output_name)], None, None
            return [], None, _make_literal_state(aggregate)

        if duplicate_treatment == "Distinct":
            if parameter_name is None:
                return [], None, _make_literal_state(aggregate)
            return [CountDistinctAggregate(parameter_name, output_name)], None, None

        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)

        return [CountAggregate(parameter_name, output_name)], None, None

    if aggregate_type in ("COUNT_DISTINCT", "DISTINCT"):
         if parameter_name is None:
             return [], None, _make_literal_state(aggregate)
         return [CountDistinctAggregate(parameter_name, output_name)], None, None

    if aggregate_type == "SUM":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        # SUM(DECIMAL) stays DECIMAL (exact) — must precede _is_float_type (which also
        # matches DECIMAL and would route to the lossy float accumulator).
        if _is_decimal_type(parameter_type):
            return [SumDecimalAggregate(parameter_name, output_name)], None, None
        if _is_float_type(parameter_type):
            return [SumFloat64Aggregate(parameter_name, output_name)], None, None
        return [SumInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "AVG":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        sum_alias = _column_bytes(f"__avg_sum_{output_name.decode('utf-8', 'ignore')}")
        count_alias = _column_bytes(f"__avg_count_{output_name.decode('utf-8', 'ignore')}")
        # AVG returns DOUBLE (a ratio, matching DuckDB and the binder's AVG type).
        # DECIMAL columns accumulate the sum EXACTLY (SumDecimalAggregate) and the
        # finalizer divides that exact sum in double — the old all-float path summed
        # `(double)unscaled * 10^-scale` per row, losing precision before the divide.
        # INTEGER/FLOAT columns keep the double-sum accumulator: it avoids the int64
        # sum kernel wrapping on large-magnitude columns (e.g. AVG(UserID)).
        if _is_decimal_type(parameter_type):
            sum_agg = SumDecimalAggregate(parameter_name, sum_alias)
        else:
            sum_agg = SumFloat64Aggregate(parameter_name, sum_alias)
        count_agg = CountAggregate(parameter_name, count_alias)
        return [sum_agg, count_agg], ("avg", sum_alias, count_alias, output_name), None

    if aggregate_type == "MIN":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        if _is_string_type(parameter_type):
            return [MinBytesAggregate(parameter_name, output_name)], None, None
        # MIN(DECIMAL) stays DECIMAL (exact) — must precede _is_float_type (which also
        # matches DECIMAL and would route to the lossy float comparison/passthrough).
        if _is_decimal_type(parameter_type):
            return [MinDecimalAggregate(parameter_name, output_name)], None, None
        if _is_float_type(parameter_type):
            return [MinFloat64Aggregate(parameter_name, output_name)], None, None
        return [MinInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "MAX":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        if _is_string_type(parameter_type):
            return [MaxBytesAggregate(parameter_name, output_name)], None, None
        # MAX(DECIMAL) stays DECIMAL (exact) — must precede _is_float_type.
        if _is_decimal_type(parameter_type):
            return [MaxDecimalAggregate(parameter_name, output_name)], None, None
        if _is_float_type(parameter_type):
            return [MaxFloat64Aggregate(parameter_name, output_name)], None, None
        return [MaxInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "ANY_VALUE":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        return [AnyValueAggregate(parameter_name, output_name)], None, None

    if aggregate_type in ("APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE"):
        raise NotImplementedError(
            f"Approximate aggregate `{aggregate_type}` is no longer supported."
        )

    if aggregate_type == "MEDIAN":
        if parameter_name is None:
            return [], None, _make_literal_state(aggregate)
        type_value = getattr(parameter_type, "value", parameter_type)
        if type_value == "DECIMAL":
            raise NotImplementedError(
                "MEDIAN does not support DECIMAL inputs; CAST the column "
                "to DOUBLE first (e.g. MEDIAN(CAST(col AS DOUBLE)))."
            )
        if type_value in ("VARCHAR", "BLOB", "BOOLEAN", "DATE", "TIMESTAMP", "TIME", "INTERVAL"):
            raise NotImplementedError(
                f"MEDIAN over {type_value} is not supported; only numeric "
                "inputs are accepted."
            )
        return [MedianFloat64Aggregate(parameter_name, output_name)], None, None

    raise ValueError(f"Unsupported aggregate type for Draken global aggregate: {aggregate_type}")


cdef class UngroupedAggregateNode(BasePlanNode):
    cdef public list aggregates
    cdef public list _compiled_evals
    cdef public list all_identifiers
    cdef public UngroupedAggregateEngine _engine
    cdef public list _result_specs
    cdef public list _literal_specs
    cdef public Py_ssize_t _engine_aggregate_count
    cdef public bint _finalized
    cdef public bint _no_eval
    cdef public list _all_identifiers_bytes
    cdef public int _select_state
    cdef public bint _has_literals

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.aggregates = list(parameters.get("aggregates", []))
        eval_nodes = [
            node
            for node in extract_evaluations(self.aggregates)
            if node.node_type != NodeType.LITERAL
        ]
        self._compiled_evals = compile_eval_nodes(eval_nodes)

        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(self.aggregates, select_nodes=(NodeType.IDENTIFIER,))
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))
        self._engine = UngroupedAggregateEngine()
        # Typed parallel arrays for the per-morsel hot path
        self._result_specs = []          # list[_ResultSpec], registration order
        self._literal_specs = []         # list[_ResultSpec] of kind=1 only
        self._engine_aggregate_count = 0
        self._finalized = False

        # Hot-path early-out flags
        self._no_eval = (len(self._compiled_evals) == 0)
        self._all_identifiers_bytes = [
            _column_bytes(i) for i in self.all_identifiers
        ]
        # Whether `chunk.select(...)` is required. Resolved on first ingest by
        # comparing the morsel's column set against `self.all_identifiers`; if
        # all required columns are already present and there are no extra ones
        # we don't need to make a fresh morsel. Until then we conservatively
        # do the select.
        self._select_state = 0  # 0 = unknown, 1 = needed, 2 = unneeded

        cdef bytes ident_bytes
        cdef _LiteralAggState lit_state
        cdef _ResultSpec rspec

        for aggregate in self.aggregates:
            ident_bytes = _column_bytes(aggregate.schema_column.identity)

            engine_aggs, avg_spec, literal_spec = _build_engine_aggregate(aggregate)
            for engine_agg in engine_aggs:
                self._engine.add_aggregate(engine_agg)
                self._engine_aggregate_count += 1

            if avg_spec is not None:
                self._engine.add_avg_finalizer(avg_spec[1], avg_spec[2], avg_spec[3])
                rspec = _ResultSpec(0, ident_bytes, None)
                self._result_specs.append(rspec)
            elif literal_spec is not None:
                lit_state = literal_spec
                rspec = _ResultSpec(1, ident_bytes, lit_state)
                self._result_specs.append(rspec)
                self._literal_specs.append(rspec)
            else:
                rspec = _ResultSpec(0, ident_bytes, None)
                self._result_specs.append(rspec)

        self._has_literals = len(self._literal_specs) > 0

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)})"

    @property
    def name(self):  # pragma: no cover
        return "Ungrouped Aggregate"

    cdef int _resolve_select_state(self, Morsel chunk) except -2:
        """Decide once whether `chunk.select(...)` is needed for this stream.

        Returns the resolved state (1=needed, 2=unneeded). Sets and returns
        `self._select_state`. Called at most once per query.
        """
        cdef dict name_map
        cdef bytes ident

        if not self._all_identifiers_bytes:
            self._select_state = 2
            return 2

        name_map = chunk._ensure_name_map()
        for ident in self._all_identifiers_bytes:
            if ident not in name_map:
                self._select_state = 1
                return 1

        if len(name_map) == len(self._all_identifiers_bytes):
            self._select_state = 2
        elif self._no_eval:
            # Extra columns are harmless when no per-row evaluation runs —
            # the aggregate kernels read by name. Skip the trim.
            self._select_state = 2
        else:
            self._select_state = 1
        return self._select_state

    cdef Morsel _finalize_morsel(self):
        names = []
        vectors = []
        engine_result = None

        if self._engine_aggregate_count:
            engine_result = self._engine.finalize()

        cdef _ResultSpec spec
        for spec in self._result_specs:
            names.append(spec.output_name)
            if spec.kind == 0:
                vectors.append(engine_result.column(spec.output_name))
            else:
                vectors.append(vector_from_sequence([spec.state.finalize()]))

        return Morsel.from_vectors(names, vectors)

    cpdef void _push_impl(self, Morsel morsel) except *:
        cdef int select_state
        cdef Py_ssize_t num_rows
        cdef _ResultSpec lit_spec

        if morsel is _EOS_SENTINEL:
            if self._finalized:
                self.emit(_EOS_SENTINEL)
                return
            self._finalized = True
            self.emit(self._finalize_morsel())
            self.emit(_EOS_SENTINEL)
            return

        num_rows = morsel.num_rows
        if num_rows > 0:
            select_state = self._select_state
            if select_state == 0:
                select_state = self._resolve_select_state(morsel)
            if select_state == 1:
                morsel = morsel.select(self.all_identifiers)
            if not self._no_eval:
                morsel = execute_and_append(self._compiled_evals, morsel)

            self._engine.ingest(morsel)

            if self._has_literals:
                for lit_spec in self._literal_specs:
                    lit_spec.state.update(num_rows)
