# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Grouped aggregate (hashed) node.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
GroupedAggregateHashedNode branch). The compiler reads `.group_by_columns` for
the key indices, `.groups` to materialize a COMPUTED group key and to name keys
in error messages, `.aggregates` for _project_agg_operands / _parse_aggregates,
`.groupby_ndv_estimate` to gate the sink's per-partition parvi front maps, and
`._having_condition` for the post-aggregate filter. A GROUP BY with no aggregate
functions is routed to the native DistinctSink instead of the GroupBySink.

This class is plan-time config only. The Cython execution stack that used to
live here — KeyStore, BaseCollector and the numeric / distinct / approx /
buffered collector families, GroupHashEngine, and the collector factory — was
deleted when the push path went dead.
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type

# BasePlanNode in scope via textual include from _operators.pyx (umbrella unit).


cdef class GroupedAggregateHashedNode(BasePlanNode):
    cdef public list groups
    cdef public list aggregates
    cdef public list group_by_columns
    cdef public object _having_condition
    # Planner distinct-group-count estimate (int or None) — consumed by the
    # native plan compiler to gate GroupBySink's per-partition parvi maps.
    cdef public object groupby_ndv_estimate

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

        # Resolve integer position GROUP BY references (e.g. GROUP BY 1)
        self.groups = [
            (
                group
                if not (group.node_type == NodeType.LITERAL and group.type.__class__.__name__ == "INTEGER")
                else projection[group.value - 1]
            )
            for group in self.groups
        ]

        self.group_by_columns = list({node.schema_column.identity for node in self.groups})

        self.groupby_ndv_estimate = parameters.get("groupby_ndv_estimate")

        self._having_condition = parameters.get("having_condition")

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression
        return (
            f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) "
            f"GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"
        )

    @property
    def name(self):  # pragma: no cover
        return "Grouped Aggregate (Hashed)"
