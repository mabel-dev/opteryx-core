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
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Inner join node.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_join, which reads `.join_type`/`.left_columns`/`.right_columns` off
this class and lowers "inner" to JoinMode::Inner — serialized multi-column keys,
build-side hash map, blocked bloom filter, all in native_join2.hpp). This class
is plan-time config only; the old Cython carchar build/probe implementation
(JoinReadings, InnerJoinKernelMetrics, DrakenCarcharJoinMap and the dense /
compressed / multi-column probe kernels) was deleted when the push path went
dead.

`supports()` is NOT dead and must stay: the physical planner calls it to decide
whether a logical inner join becomes this node at all, and raises
UnsupportedSyntaxError when it returns False (see _create_join_node).
"""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class DrakenInnerJoinNode(JoinNode):
    cdef public list left_columns
    cdef public list right_columns
    cdef public object columns

    join_type = "inner"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)
        self.left_columns = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])
        self.on = parameters.get("on")
        self.columns = parameters.get("columns")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

    @staticmethod
    def supports(**parameters) -> bool:
        on = parameters.get("on")
        if on is None:
            return True

        left_relation_names = set(parameters.get("left_relation_names") or [])
        right_relation_names = set(parameters.get("right_relation_names") or [])
        comparisons = get_all_nodes_of_type(on, (NodeType.COMPARISON_OPERATOR,))
        if not comparisons:
            return False

        for comparison in comparisons:
            if comparison.value != "Eq":
                return False
            if comparison.left is None or comparison.right is None:
                return False
            if comparison.left.node_type != NodeType.IDENTIFIER:
                return False
            if comparison.right.node_type != NodeType.IDENTIFIER:
                return False
            if not comparison.left.schema_column or not comparison.right.schema_column:
                return False

            left = comparison.left
            right = comparison.right
            if not (
                (left.source in left_relation_names and right.source in right_relation_names)
                or (left.source in right_relation_names and right.source in left_relation_names)
            ):
                return False

            # A mixed-numeric key pair (INTEGER vs FLOAT vs DECIMAL) used to be
            # DECLINED here, which surfaced as "Draken inner join does not support
            # this query shape". It is now supported: the compiler materializes a
            # CAST column on the narrower side and keys on that, so both sides hash
            # the same representation (_join_key_coercions in
            # opteryx/managers/execution/compiler.py). Declining is no longer
            # correct — it refused a shape the engine can answer.

        return True

    @property
    def name(self):  # pragma: no cover
        return "Inner Join Draken"

    @property
    def config(self):  # pragma: no cover
        return "draken+carchar"
