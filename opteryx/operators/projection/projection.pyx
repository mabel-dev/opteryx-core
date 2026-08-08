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
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
ProjectionNode branch, which reads `.projection` off this class for the output
identities and re-derives the computed expressions from `node.parameters`
— hoisted_columns, projection, passthrough_columns — via _add_computed). This
class is plan-time config only.
"""

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class ProjectionNode(BasePlanNode):
    cdef public list projection

    def __init__(self, properties=None, **parameters):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # `passthrough_columns` are columns this Project must COMPUTE AND EMIT for a
        # consumer above it (ORDER BY, HAVING) that are not part of the query's output
        # row. They ride in the morsel alongside `projection` and are dropped at the
        # Exit node, which prunes to the SELECT list. Contrast `hoisted_columns` below,
        # which are computed but never emitted.
        #
        # Both `projection` and `passthrough_columns` may arrive as None (not just
        # absent): the optimizer treats a node's column lists as "iterable or None"
        # (projection_pushdown.py), and the physical planner forwards a None
        # `passthrough_columns` verbatim. This fires e.g. on COUNT(*) over a subquery,
        # where pushdown leaves the inner Project with no pass-through columns.
        # Normalise both to empty lists — None means "no columns here".
        proj = parameters["projection"] or []
        passthrough = parameters.get("passthrough_columns") or []
        # Columns a fused Project must compute for internal use (a lower-Project
        # expression referenced 2+ times by this node's own columns) but never
        # exposes in its output row — see project_fusion.py. Ordered first so a
        # later program can load an earlier one's output by identity.
        hoisted = parameters.get("hoisted_columns") or []
        projection = proj + passthrough

        self.projection = []
        for column in projection:
            self.projection.append(column.schema_column.identity)

        self.columns = proj

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return ", ".join(format_expression(col) for col in self.columns)

    @property
    def name(self):  # pragma: no cover
        return "Projection"
