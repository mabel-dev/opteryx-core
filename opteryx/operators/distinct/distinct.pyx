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

from typing import Generator, Optional
from opteryx.compiled.morsel_ops.distinct import distinct as _distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper as _CarcharSetWrapper
from opteryx.compiled.structures.parvi_set import ParviSetWrapper as _ParviSetWrapper
from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


cdef class DistinctNode(BasePlanNode):
    cdef public object _distinct_on
    cdef public str _set_variant
    cdef public object _hash_set
    cdef public bint at_least_one_yielded
    cdef public bint _promoted

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            self._distinct_on = [
                col.schema_column.identity for col in self._distinct_on
            ]
        self._set_variant = parameters.get("set_variant", "carchar")
        self._hash_set = None
        self.at_least_one_yielded = False
        self._promoted = False

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    cdef void _dispatch_push(self, Morsel morsel) except *:
        if self._hash_set is None:
            if self._set_variant == "parvi" and not self._promoted:
                self._hash_set = _ParviSetWrapper()
            else:
                self._hash_set = _CarcharSetWrapper()

        if morsel is _EOS_SENTINEL:
            self._emit_cdef(_EOS_SENTINEL)
            return

        chunk = morsel
        is_active_parvi = isinstance(self._hash_set, _ParviSetWrapper) and not self._promoted
        promotion_seed = None
        if is_active_parvi and not self._hash_set.full():
            promotion_seed = _CarcharSetWrapper()
            self._hash_set.drain_into_carchar(promotion_seed)

        overflow = _distinct(chunk, self._hash_set, columns=self._distinct_on)

        should_promote = overflow and is_active_parvi
        if should_promote:
            if promotion_seed is not None:
                carchar_set = promotion_seed
            else:
                parvi_set = self._hash_set
                carchar_set = _CarcharSetWrapper()
                parvi_set.drain_into_carchar(carchar_set)
            self._hash_set = carchar_set
            self._promoted = True
            _distinct(chunk, self._hash_set, columns=self._distinct_on)

        if len(chunk) > 0 or not self.at_least_one_yielded:
            self._emit_cdef(chunk)

        self.at_least_one_yielded = True
