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
    # Row-routing producer seam (M4 parallel DISTINCT). When set to a scatter
    # collector, `_push_impl` routes each input morsel into per-worker bins by
    # hash(dedup-key) % W instead of deduping — the dedup runs later, in parallel,
    # on the disjoint bins. None = normal serial dedup. Mirrors the grouped-agg
    # `_engine` swap (parallel_engine._ScatterCollectEngine).
    cdef public object _scatter_engine

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
        self._scatter_engine = None

    cdef BasePlanNode make_worker(self):
        # SPEC: _distinct_on (dedup column identities) + _set_variant. STATE: fresh
        # dedup set + the variant-shadow flags (_promoted/_use_phash shadow the
        # read-only _set_variant per worker) + scatter seam.
        cdef DistinctNode w = DistinctNode.__new__(DistinctNode)
        self._copy_worker_base(w)
        w._distinct_on = self._distinct_on
        w._set_variant = self._set_variant
        w._hash_set = None
        w.at_least_one_yielded = False
        w._promoted = False
        w._use_phash = False
        w._scatter_engine = None
        return w

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
        col = morsel._cxx_column(cols[0])
        if getattr(col, "type", None) == _draken_native.INT8:
            if (<Vector>col).null_bitmap_ptr() != NULL:
                return None  # has nulls — fall back
            return PerfectHashSet(-128, 127)
        if getattr(col, "type", None) == _draken_native.INT16:
            if (<Vector>col).null_bitmap_ptr() != NULL:
                return None  # has nulls — fall back
            return PerfectHashSet(-32768, 32767)
        return None

    cdef bint _distinct_phash(self, Morsel morsel) except -1:
        """Filter morsel in-place using PerfectHashSet.

        Returns True when the morsel was handled. Returns False — WITHOUT
        filtering the morsel — when this morsel is ineligible for the
        PerfectHashSet path (nulls, non-dense encoding, or type drift); the
        caller MUST then demote to the carchar path and re-run this morsel,
        or duplicates are silently emitted.
        """
        cols = self._distinct_on
        col = morsel._cxx_column(cols[0])

        cdef Py_ssize_t n = morsel.num_rows
        cdef int32_t* idx_buf = <int32_t*>PyMem_Malloc(n * sizeof(int32_t))
        if idx_buf == NULL:
            raise MemoryError()

        cdef PerfectHashSet phs = <PerfectHashSet>self._hash_set
        cdef Py_ssize_t count
        cdef void* dp

        if getattr(col, "type", None) == _draken_native.INT8:
            if (<Vector>col).null_bitmap_ptr() != NULL:
                PyMem_Free(idx_buf)
                return False  # nulls — caller must demote
            dp = (<Vector>col).unified().data
            # Scanning dp directly is valid ONLY for dense-identity layout. A
            # PERMUTATION (data_length==length, non-identity selection) would be
            # deduped in physical order — wrong rows kept. Require IDENTITY.
            if ((<Vector>col).unified().data_length != (<Vector>col).unified().length
                    or not ((<Vector>col).unified().flags & DRAKEN_SEL_IDENTITY)):
                PyMem_Free(idx_buf)
                return False  # non-dense-identity encoding — caller must demote
            with nogil:
                count = phs.find_new_indices_out_32_i8(<const int8_t*>dp, idx_buf, n)
        elif getattr(col, "type", None) == _draken_native.INT16:
            if (<Vector>col).null_bitmap_ptr() != NULL:
                PyMem_Free(idx_buf)
                return False  # nulls — caller must demote
            dp = (<Vector>col).unified().data
            # Dense-identity only (see INT8 branch above); permutations demote.
            if ((<Vector>col).unified().data_length != (<Vector>col).unified().length
                    or not ((<Vector>col).unified().flags & DRAKEN_SEL_IDENTITY)):
                PyMem_Free(idx_buf)
                return False  # non-dense-identity encoding — caller must demote
            with nogil:
                count = phs.find_new_indices_out_32_i16(<const int16_t*>dp, idx_buf, n)
        else:
            PyMem_Free(idx_buf)
            return False  # type drift — caller must demote

        if count == 0:
            morsel._empty_inplace()
        else:
            morsel._take_inplace(<int32_t[:<Py_ssize_t>count]>idx_buf)

        PyMem_Free(idx_buf)
        return True

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier (recovering the EOS sentinel) and calls this, surfacing any
        # exception via the ErrCtx path.
        cdef bint is_active_parvi
        if self._scatter_engine is not None:
            # Row-routing producer mode: scatter the (already projected-to-key)
            # input into per-worker bins; emit nothing here. EOS is not forwarded
            # — the parallel engine drives the deduped output downstream itself.
            if morsel is not _EOS_SENTINEL:
                self._scatter_engine.ingest(morsel)
            return
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
            self.emit(_EOS_SENTINEL)
            return

        chunk = morsel

        if self._use_phash:
            if not self._distinct_phash(chunk):
                # Mid-stream ineligible morsel (nulls / non-dense / type
                # drift). Demote: rebuild a carchar set seeded with every
                # value already marked seen, then run this chunk through the
                # standard path below. Emitting the chunk unfiltered here
                # would silently return duplicate rows.
                self._hash_set = _rebuild_carchar_from_phash(
                    <PerfectHashSet>self._hash_set)
                self._use_phash = False
                # The rebuilt set is carchar; ensure the parvi logic below
                # never treats it as a parvi set.
                self._promoted = True

        if not self._use_phash:
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

        # num_rows, not len(): Morsel.__len__ returns the COLUMN count, so the
        # old `len(chunk) > 0` guard was always true and emitted 0-row chunks.
        if chunk.num_rows > 0 or not self.at_least_one_yielded:
            self.emit(chunk)

        self.at_least_one_yielded = True
