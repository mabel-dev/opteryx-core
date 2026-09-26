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
Null Reader Node

The physical form of the Scan under SHOW SNAPSHOTS / LINEAGE / SOURCES FOR. That
Scan exists so the relation is BOUND — the permission gate, the connector, and
the commit history the statement answers from — and its rows are never part of
the answer: serial_engine answers from the Show node above it and never drives
the pipeline. So this node has no read path at all.
"""

# BasePlanNode in scope via _operators.pyx include.


cdef class NullReaderNode(BasePlanNode):  # pragma: no cover
    """A Scan kept only to bind a relation whose history — not its rows — is the answer."""
    # `columns` is a BasePlanNode field; only the scan-specific extras here.
    cdef public object schema

    def __init__(self, properties, step):
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.schema = step.schema

    @property
    def name(self):  # pragma: no cover
        """Friendly name for this step"""
        return "Null Reader"

    @property
    def config(self):
        """Additional details for this step"""
        return "(bound only - never read)"

    def __repr__(self):  # pragma: no cover
        return f"<{self.__class__.__name__}>"
