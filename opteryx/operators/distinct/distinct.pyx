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
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""

from opteryx.compiled.morsel_ops.distinct import distinct as _distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper as _CarcharSetWrapper

# BasePlanNode in scope via _operators.pyx include.


cdef class DistinctNode(BasePlanNode):
    cdef public object _distinct_on
    # The unreduced DISTINCT ON expression nodes (schema_column, node_type,
    # parameters intact) — parallel to GroupedAggregateHashedNode.groups.
    # Needed by the native compiler to materialize a computed DISTINCT ON key
    # (e.g. `DISTINCT ON (payload->'x')`) that the stream doesn't already
    # carry; `_distinct_on` alone (bare identities) throws that expression
    # tree away before the compiler ever sees it.
    cdef public object _distinct_on_exprs
    cdef public str _set_variant
    cdef public object _hash_set
    cdef public bint at_least_one_yielded
    # Row-routing producer seam (M4 parallel DISTINCT). None = normal serial
    # dedup. Mirrors the grouped-agg scatter-collector `_engine` swap.
    cdef public object _scatter_engine

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on_exprs = parameters.get("on")
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            self._distinct_on = [
                col.schema_column.identity for col in self._distinct_on
            ]
        self._set_variant = parameters.get("set_variant", "carchar")
        self._hash_set = None
        self.at_least_one_yielded = False
        self._scatter_engine = None

    cdef BasePlanNode make_worker(self):
        # SPEC: _distinct_on (dedup column identities) + _set_variant. STATE: fresh
        # dedup set + scatter seam.
        cdef DistinctNode w = DistinctNode.__new__(DistinctNode)
        self._copy_worker_base(w)
        w._distinct_on = self._distinct_on
        w._distinct_on_exprs = self._distinct_on_exprs
        w._set_variant = self._set_variant
        w._hash_set = None
        w.at_least_one_yielded = False
        w._scatter_engine = None
        return w

    cpdef readout_partition(self, list chunks, PipelineContext ctx):
        """Operator-owned per-partition DEDUP read-out (HASH_REPARTITION recombination):
        dedup ONE global hash partition's chunks in place against a PRIVATE carchar set
        (``hash(key) % radix`` co-locates every copy of a value in one partition, so the
        partitions are disjoint key slices — no cross-worker merge), returning
        ``(survivor_chunks, row_count)`` for the sink to push downstream. The operator
        owns its recombination; the native read-out fan-out (``native_readout_fanout``)
        drives partitions in parallel. Mirrors the serial dedup kernel exactly."""
        cdef object hash_set = _CarcharSetWrapper()
        cdef list out = []
        cdef Morsel chunk
        cdef long long count = 0
        for chunk in chunks:
            if ctx.is_terminated():
                break
            _distinct(chunk, hash_set, columns=self._distinct_on)
            if chunk.num_rows > 0:
                out.append(chunk)
            count += chunk.num_rows
        return out, count

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

