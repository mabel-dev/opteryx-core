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
Unnest Join Node

This is a SQL Query Execution Plan Node.

This implements a CROSS JOIN UNNEST, this isn't really a JOIN in that it doesn't join two tables
together, but it does unnest a column in a table and repeat the rows in the table for each value.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_unnest / _compile_unnest_literal, which read `._unnest_column`,
`._unnest_target`, `._filters`, `._distinct` and `.pre_update_columns` off this
class and compile them into the engine's add_unnest / add_unnest_literal). This
class is plan-time config only; the old Cython offset-walking expansion was
deleted when the push path went dead.

Note `_filters` and `_distinct` are still READ by the compiler — a pushed value
filter or DISTINCT folded into this node is a plan inconsistency the compiler
refuses loudly rather than silently dropping, so both attributes must survive.
"""

from opteryx.expression import NodeType

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class UnnestJoinNode(BasePlanNode):
    """
    Implements CROSS JOIN UNNEST
    """

    cdef public object left_readers
    cdef public object right_readers
    cdef public list left_relation_names
    cdef public list right_relation_names
    cdef public str join_type
    cdef public object _unnest_column
    cdef public object _unnest_function   # "UNNEST" or "CIDR_UNNEST"
    cdef public object _unnest_target
    cdef public object _filters
    cdef public bint _distinct
    # Predicates on the unnested column folded into this node by predicate_pushdown.
    # A list of expression nodes, ANDed. DISTINCT from the legacy `_filters`, which
    # was a list of literal VALUES for an IN test and which the compiler still
    # refuses loudly — these are full expression nodes the compiler lowers to the
    # same c-native bytecode a standalone FilterNode would have used, evaluated over
    # the array's child vector before the fan-out.
    cdef public object filter_conditions
    # Set by distinct_pushdown when a DISTINCT above this node dedups on the target.
    # An INTENT flag only — the compiler honours it just when the unnest emits the
    # target alone, and never removes the Distinct node (the pre-reduction is
    # per-worker; only the DistinctSink dedups across workers).
    cdef public bint distinct_target
    # Identities still needed ABOVE this node (projection_pushdown liveness). The
    # plan compiler reads it to decide whether the consumed source ARRAY column may
    # be dropped from the unnest output. Empty = unknown -> keep everything.
    cdef public object pre_update_columns

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Initialize join interface (UnnestJoinNode is registered as a join node in catalog).
        # left/right_relation_names mirror the base JoinNode contract so plan machinery
        # (e.g. physical_plan.label_join_legs) can inspect them uniformly; an UnnestJoinNode
        # carries no reader UUIDs or relation names, so these stay empty and the leg-labelling
        # guard skips it.
        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []
        self.join_type = "cross"

        # do we have unnest details?
        self._unnest_column = parameters.get("unnest_column")
        self._unnest_function = parameters.get("unnest_function") or "UNNEST"
        self._unnest_target = parameters.get("unnest_target").schema_column
        self._filters = parameters.get("filters")
        self._distinct = parameters.get("distinct", False)
        self.filter_conditions = parameters.get("filter_conditions") or []
        self.distinct_target = parameters.get("distinct_target", False)

        # handle variation in how the unnested column is represented
        if self._unnest_column.node_type == NodeType.NESTED:
            self._unnest_column = self._unnest_column.centre

        # if we have a literal that's not a tuple, wrap it
        #
        # NOT for CIDR_UNNEST: its literal is a single CIDR STRING, and wrapping it
        # would turn '10.0.0.0/24' into a one-element tuple — a collection where the
        # operator expects one block, which then reads as an array literal and
        # expands to nothing recognisable.
        if (self._unnest_function != "CIDR_UNNEST"
                and self._unnest_column.node_type == NodeType.LITERAL
                and not isinstance(self._unnest_column.value, tuple)):
            self._unnest_column.value = tuple([self._unnest_column.value])

        self.pre_update_columns = parameters.get("pre_update_columns") or set()

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        filters = ""
        if self._filters:
            filters = f"({self._unnest_target.name} IN ({', '.join(self._filters)}))"
        return f"CROSS JOIN {filters}"
