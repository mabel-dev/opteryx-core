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

from libc.stdint cimport int32_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free

# BasePlanNode in scope via _operators.pyx include.


cdef class DistinctNode(BasePlanNode):
    cdef public object _distinct_on
    cdef public str _set_variant
    cdef public object _hash_set
    cdef public bint at_least_one_yielded
    cdef public bint _promoted
    cdef public bint _use_phash   # True when PerfectHashSet is active

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
        self._use_phash = False

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    cdef object _try_init_perfect_hash(self, Morsel morsel):
        """Return a PerfectHashSet if the single DISTINCT key is a non-null Int8/Int16 column."""
        cols = self._distinct_on
        if cols is None or len(cols) != 1:
            return None
        col = morsel.column(cols[0])
        if not isinstance(col, IntegerVector):
            return None
        cdef IntegerVector ivec = <IntegerVector>col
        if ivec.null_bitmap_ptr() != NULL:
            return None  # has nulls — fall back
        if ivec.ptr.type == DRAKEN_INT8:
            return PerfectHashSet(-128, 127)
        if ivec.ptr.type == DRAKEN_INT16:
            return PerfectHashSet(-32768, 32767)
        return None

    cdef bint _distinct_phash(self, Morsel morsel) except -1:
        """Filter morsel in-place using PerfectHashSet. Returns False always (no overflow)."""
        cols = self._distinct_on
        col = morsel.column(cols[0])
        if not isinstance(col, IntegerVector):
            return False
        cdef IntegerVector ivec = <IntegerVector>col
        if ivec.null_bitmap_ptr() != NULL:
            return False
        cdef void* dp = ivec.dense_ptr()
        if dp == NULL:
            return False

        cdef Py_ssize_t n = morsel.num_rows
        cdef int32_t* idx_buf = <int32_t*>PyMem_Malloc(n * sizeof(int32_t))
        if idx_buf == NULL:
            raise MemoryError()

        cdef PerfectHashSet phs = <PerfectHashSet>self._hash_set
        cdef Py_ssize_t count

        with nogil:
            if ivec.ptr.type == DRAKEN_INT8:
                count = phs.find_new_indices_out_32_i8(<const int8_t*>dp, idx_buf, n)
            else:
                count = phs.find_new_indices_out_32_i16(<const int16_t*>dp, idx_buf, n)

        if count == 0:
            morsel._empty_inplace()
        else:
            morsel._take_inplace(<int32_t[:<Py_ssize_t>count]>idx_buf)

        PyMem_Free(idx_buf)
        return False  # no overflow concept for PerfectHashSet

    cdef void _dispatch_push(self, Morsel morsel) except *:
        cdef bint is_active_parvi
        if self._hash_set is None:
            # First morsel: try PerfectHashSet for eligible narrow-int columns
            phash = self._try_init_perfect_hash(morsel)
            if phash is not None:
                self._hash_set = phash
                self._use_phash = True
            elif self._set_variant == "parvi" and not self._promoted:
                self._hash_set = _ParviSetWrapper()
            else:
                self._hash_set = _CarcharSetWrapper()

        if morsel is _EOS_SENTINEL:
            self._emit_cdef(_EOS_SENTINEL)
            return

        chunk = morsel

        if self._use_phash:
            self._distinct_phash(chunk)
        else:
            # Variant is fixed at init; `_promoted` flips parvi → carchar.
            is_active_parvi = (self._set_variant == "parvi") and not self._promoted
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
