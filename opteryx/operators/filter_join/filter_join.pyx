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
Filter Join Node

LEFT SEMI and LEFT ANTI join implementations for IN / NOT IN subquery rewrites
and INTERSECT / EXCEPT set operations.

Probe phase uses batch read-only APIs (probe_found_32 / probe_not_found_32) that
call into C++ CarcharSet::probe_found_32 / probe_not_found_32 — const methods
with software prefetch on x86, fully NoGIL.

NULL semantics
--------------
NULL values in join keys hash to NULL_HASH (a fixed sentinel).  This gives three
possible join modes:

  left semi          — IN subquery.  If the right side has any null, left rows
                       whose key is null are excluded (NULL IN (...) = UNKNOWN).
                       If the right side has no null, left null rows hash to
                       NULL_HASH which is absent from the set → correctly excluded.

  left anti          — EXCEPT / INTERSECT anti.  Plain anti-join; callers that need
                       NOT IN null semantics should use left anti null-aware.

  left anti null-aware — NOT IN subquery.  If the right side has any null, all
                       left rows are excluded (NOT IN with a null on the right =
                       UNKNOWN for every row).  Otherwise, left rows whose key is
                       null are also excluded (NULL NOT IN (...) = UNKNOWN).
"""

from typing import Generator, Optional
import time

from libc.stdint cimport int32_t, int64_t, uint64_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from opteryx.models import QueryProperties
# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.

# Actual hash produced for a null key: mix(0, raw_NULL_HASH) where raw_NULL_HASH = 0x4c3f95a36ab8ecca.
# hash_into initialises dest to 0 (calloc), so: mixed = (0 ^ raw) * MIX_CONST + 1, then ^ >> 32.
cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL


# ---------------------------------------------------------------------------
# Build phase
# ---------------------------------------------------------------------------

cdef CarcharSetWrapper _build_filter_hash_set(
    Morsel morsel,
    list columns,
    CarcharSetWrapper seen_hashes,
):
    cdef Py_ssize_t num_rows = morsel.num_rows
    cdef uint64_t[::1] row_hashes

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    if num_rows == 0:
        return seen_hashes

    row_hashes = morsel.hash(columns)
    with nogil:
        seen_hashes._insert_many_nogil(&row_hashes[0], <size_t>num_rows)

    return seen_hashes


# ---------------------------------------------------------------------------
# Compact helper — filter out rows whose hash equals the null sentinel.
# Used when right side has nulls and we need to exclude left-null rows.
# ---------------------------------------------------------------------------

cdef Py_ssize_t _compact_exclude_null(
    int32_t* indices,
    Py_ssize_t count,
    const uint64_t* hashes,
    uint64_t null_hash,
) noexcept nogil:
    """Compact indices[0..count) in-place, dropping entries where hashes[indices[j]] == null_hash."""
    cdef Py_ssize_t r = 0, w = 0
    for r in range(count):
        if hashes[indices[r]] != null_hash:
            indices[w] = indices[r]
            w += 1
    return w


# ---------------------------------------------------------------------------
# Probe kernels — each returns a filtered Morsel
# ---------------------------------------------------------------------------

cdef Morsel _semi_join_filter(
    Morsel relation,
    list join_columns,
    CarcharSetWrapper seen_hashes,
    bint right_has_null,
):
    """
    LEFT SEMI JOIN probe (IN subquery).

    Fast path (right has no nulls): probe_found_32 directly — left null rows
    hash to NULL_HASH which is not in the set, so they are correctly excluded.

    Slow path (right has nulls): probe_found_32, then compact out left null rows
    (NULL IN (NULL, ...) = UNKNOWN = excluded).
    """
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(num_rows * sizeof(int32_t))
    if out_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t n_found
    with nogil:
        n_found = seen_hashes.probe_found_32_nogil(&row_hashes[0], num_rows, out_buf)
        if right_has_null and n_found > 0:
            n_found = _compact_exclude_null(out_buf, n_found, &row_hashes[0], _NULL_HASH)

    cdef Morsel result
    if n_found > 0:
        result = relation.take(<int32_t[:n_found]>out_buf)
    else:
        result = relation.slice(0, 0)

    PyMem_Free(out_buf)
    return result


cdef Morsel _anti_join_filter(
    Morsel relation,
    list join_columns,
    CarcharSetWrapper seen_hashes,
):
    """
    LEFT ANTI JOIN probe (EXCEPT / set operations).

    No null awareness — callers that need NOT IN semantics use
    _anti_join_null_aware_filter instead.
    """
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(num_rows * sizeof(int32_t))
    if out_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t n_not_found
    with nogil:
        n_not_found = seen_hashes.probe_not_found_32_nogil(&row_hashes[0], num_rows, out_buf)

    cdef Morsel result
    if n_not_found > 0:
        result = relation.take(<int32_t[:n_not_found]>out_buf)
    else:
        result = relation.slice(0, 0)

    PyMem_Free(out_buf)
    return result


cdef Morsel _anti_join_null_aware_filter(
    Morsel relation,
    list join_columns,
    CarcharSetWrapper seen_hashes,
):
    """
    LEFT ANTI NULL-AWARE JOIN probe (NOT IN subquery).

    If right side has any null: all left rows return UNKNOWN → return empty.
    Otherwise: use probe_not_found_32, then compact out left null rows
    (NULL NOT IN (...) = UNKNOWN = excluded).
    """
    if seen_hashes.contains(_NULL_HASH):
        # Right side has nulls — NOT IN returns UNKNOWN for every outer row.
        return relation.slice(0, 0)

    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(num_rows * sizeof(int32_t))
    if out_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t n_not_found
    with nogil:
        n_not_found = seen_hashes.probe_not_found_32_nogil(&row_hashes[0], num_rows, out_buf)
        # Exclude left rows whose key is null (NULL NOT IN (...) = UNKNOWN = excluded).
        if n_not_found > 0:
            n_not_found = _compact_exclude_null(out_buf, n_not_found, &row_hashes[0], _NULL_HASH)

    cdef Morsel result
    if n_not_found > 0:
        result = relation.take(<int32_t[:n_not_found]>out_buf)
    else:
        result = relation.slice(0, 0)

    PyMem_Free(out_buf)
    return result


# ---------------------------------------------------------------------------
# FilterJoinNode
# ---------------------------------------------------------------------------

cdef class FilterJoinNode(JoinNode):
    cdef public str join_type
    cdef public object using
    cdef public list left_columns
    cdef public list right_columns
    cdef public CarcharSetWrapper right_hash_set
    cdef public bint _right_has_null
    cdef public bint _build_phase

    def __init__(self, properties=None, **parameters):
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.right_hash_set = CarcharSetWrapper()
        self._right_has_null = False
        self._build_phase = True  # right side arrives first

    @property
    def name(self):  # pragma: no cover
        return self.join_type.replace(" ", "_")

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"

    cpdef void push_right(self, Morsel morsel) except *:
        cdef long long start
        # Build side for filter joins — right side feeds the hash set.
        if morsel is _EOS_SENTINEL:
            self._right_has_null = self.right_hash_set.has(_NULL_HASH)
            return
        morsel = self._apply_join_key_casts(morsel, is_left=False)
        start = time.monotonic_ns()
        self.right_hash_set = _build_filter_hash_set(
            morsel, self.right_columns, self.right_hash_set
        )
        self.readings["time_build_filter_hash_table"] += time.monotonic_ns() - start

    cpdef void push_left(self, Morsel morsel) except *:
        # Probe side for filter joins — filters left through the right-side hash set.
        if morsel is _EOS_SENTINEL:
            self.emit(_EOS_SENTINEL)
            return
        morsel = self._apply_join_key_casts(morsel, is_left=True)
        if morsel.num_rows == 0:
            self.emit(morsel)
            return
        if self.join_type == "left semi":
            self.emit(_semi_join_filter(
                morsel, self.left_columns, self.right_hash_set, self._right_has_null
            ))
        elif self.join_type == "left anti":
            self.emit(_anti_join_filter(morsel, self.left_columns, self.right_hash_set))
        elif self.join_type == "left anti null-aware":
            self.emit(_anti_join_null_aware_filter(
                morsel, self.left_columns, self.right_hash_set
            ))
