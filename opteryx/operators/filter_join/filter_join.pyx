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

PerfectHashSet fast path: when the right-side join key is a non-null Int8 or
Int16 narrow integer column, direct-address probing replaces hash-table probing.
Build detects eligibility at first right morsel.

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

from libc.stdint cimport int32_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from draken.vectors.vector cimport Vector

from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.
# BasePlanNode/JoinNode in scope via _operators.pyx include.

cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL


# ---------------------------------------------------------------------------
# Build phase — CarcharSetWrapper path (existing)
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
# Build phase — PerfectHashSet path
# ---------------------------------------------------------------------------

cdef object _try_build_phash(Morsel morsel, list columns, object current_set):
    """Attempt to insert build-side rows into a PerfectHashSet.

    Returns the (possibly newly created) PerfectHashSet on success, or None
    if the column is ineligible (wrong type, has nulls, wrong encoding).
    On first call (current_set is None), creates the PerfectHashSet from
    type bounds. On subsequent calls it is already a PerfectHashSet.
    """
    if len(columns) != 1:
        return None
    col = morsel.column(columns[0])
    cdef bint is_int8 = isinstance(col, Integer8Vector)
    cdef bint is_int16 = isinstance(col, Integer16Vector)
    if not (is_int8 or is_int16):
        return None  # Int32/Int64 — no type bound in Phase 1

    # Any null on the right build side: track _right_has_null separately;
    # null rows are skipped (not inserted) into PerfectHashSet.
    cdef void* dp
    cdef uint8_t* nulls
    cdef DrakenVector* _fj_uv
    if is_int8:
        _fj_uv = (<Integer8Vector>col).unified()
        dp = _fj_uv.data
        nulls = (<Integer8Vector>col).null_bitmap_ptr()
    else:
        _fj_uv = (<Integer16Vector>col).unified()
        dp = _fj_uv.data
        nulls = (<Integer16Vector>col).null_bitmap_ptr()
    if dp == NULL or _fj_uv.data_length != _fj_uv.length:
        return None  # non-dense encoding → fall back

    cdef PerfectHashSet phs
    if current_set is None:
        if is_int8:
            phs = PerfectHashSet(-128, 127)
        else:
            phs = PerfectHashSet(-32768, 32767)
    else:
        phs = <PerfectHashSet>current_set

    cdef Py_ssize_t n = morsel.num_rows
    cdef Py_ssize_t i
    cdef int64_t val

    if nulls == NULL:
        # No nulls: bulk insert
        with nogil:
            if is_int8:
                for i in range(n):
                    phs.insert_i64(<int64_t>(<const int8_t*>dp)[i])
            else:
                for i in range(n):
                    phs.insert_i64(<int64_t>(<const int16_t*>dp)[i])
    else:
        # Null rows: skip them (tracked via _right_has_null flag in the node)
        with nogil:
            if is_int8:
                for i in range(n):
                    if nulls[i >> 3] & (1 << (i & 7)):
                        phs.insert_i64(<int64_t>(<const int8_t*>dp)[i])
            else:
                for i in range(n):
                    if nulls[i >> 3] & (1 << (i & 7)):
                        phs.insert_i64(<int64_t>(<const int16_t*>dp)[i])

    return phs


# ---------------------------------------------------------------------------
# Rebuild CarcharSetWrapper from PerfectHashSet (rare fallback path)
# ---------------------------------------------------------------------------

cdef CarcharSetWrapper _rebuild_carchar_from_phash(PerfectHashSet phs):
    """Reconstruct a hash-based CarcharSetWrapper from an existing PerfectHashSet.

    Called only when the probe side turns out to have a column encoding the
    PerfectHashSet path can't handle (e.g. nullable or non-dense). Iterates
    the bit-array and hashes each stored value via Draken's scalar hash machinery.
    """
    from draken.vectors.scalar_constructors import from_scalar as _build_scalar
    cdef CarcharSetWrapper result = CarcharSetWrapper(<size_t>phs._range * 2 + 8)
    cdef uint64_t[::1] hash_buf
    cdef Py_ssize_t w, bit
    cdef uint64_t word, mask
    cdef int64_t slot, val
    for w in range(phs._n_words):
        word = phs._words[w]
        if word == 0:
            continue
        for bit in range(64):
            mask = <uint64_t>1 << bit
            if word & mask:
                slot = <int64_t>w * 64 + <int64_t>bit
                val = phs._min_val + slot
                scalar_vec = _build_scalar(val, 1)
                hash_buf = (<Vector>scalar_vec).hash()
                result.insert(hash_buf[0])
    return result


# ---------------------------------------------------------------------------
# Compact helper — filter out rows whose hash equals the null sentinel.
# ---------------------------------------------------------------------------

cdef Py_ssize_t _compact_exclude_null(
    int32_t* indices,
    Py_ssize_t count,
    const uint64_t* hashes,
    uint64_t null_hash,
) noexcept nogil:
    cdef Py_ssize_t r = 0, w = 0
    for r in range(count):
        if hashes[indices[r]] != null_hash:
            indices[w] = indices[r]
            w += 1
    return w


# ---------------------------------------------------------------------------
# Probe kernels — CarcharSetWrapper path (existing)
# ---------------------------------------------------------------------------

cdef Morsel _semi_join_filter(
    Morsel relation,
    list join_columns,
    CarcharSetWrapper seen_hashes,
    bint right_has_null,
):
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
    if seen_hashes.contains(_NULL_HASH):
        return relation.slice(0, 0)

    cdef Py_ssize_t num_rows = relation.num_rows
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(num_rows * sizeof(int32_t))
    if out_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t n_not_found
    with nogil:
        n_not_found = seen_hashes.probe_not_found_32_nogil(&row_hashes[0], num_rows, out_buf)
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
# Probe kernels — PerfectHashSet path
# ---------------------------------------------------------------------------

cdef Morsel _phash_probe(
    Morsel relation,
    list join_columns,
    PerfectHashSet phash,
    bint want_found,       # True = semi, False = anti
    bint right_has_null,   # only relevant for semi and null-aware anti
    bint null_aware,       # True = left anti null-aware (NOT IN)
):
    """Unified PerfectHashSet probe for semi/anti/null-aware-anti joins.

    Falls back to None if the probe-side column is ineligible (has nulls,
    non-dense encoding, wrong type). Caller must handle None by falling back
    to the hash path.
    """
    if len(join_columns) != 1:
        return None
    col = relation.column(join_columns[0])
    cdef bint is_int8 = isinstance(col, Integer8Vector)
    cdef bint is_int16 = isinstance(col, Integer16Vector)
    if not (is_int8 or is_int16):
        return None
    cdef void* dp
    cdef uint8_t* nulls
    cdef DrakenVector* _fj2_uv
    if is_int8:
        _fj2_uv = (<Integer8Vector>col).unified()
        dp = _fj2_uv.data
        nulls = (<Integer8Vector>col).null_bitmap_ptr()
    else:
        _fj2_uv = (<Integer16Vector>col).unified()
        dp = _fj2_uv.data
        nulls = (<Integer16Vector>col).null_bitmap_ptr()
    if dp == NULL or _fj2_uv.data_length != _fj2_uv.length:
        return None

    cdef Py_ssize_t n = relation.num_rows
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(n * sizeof(int32_t))
    if out_buf == NULL:
        raise MemoryError()

    cdef Py_ssize_t count = 0
    cdef Py_ssize_t i

    if null_aware and right_has_null:
        # NOT IN with a null on the right side → every left row is UNKNOWN → empty
        PyMem_Free(out_buf)
        return relation.slice(0, 0)

    if nulls != NULL:
        # Probe side has nulls: null rows are never "found" in PerfectHashSet.
        # For semi-join: null rows correctly excluded (NULL IN (...) = UNKNOWN).
        # For anti-join: null rows NOT excluded by probe_not_found; need manual handling.
        # Simplest: fall back to hash path when probe side has nulls.
        PyMem_Free(out_buf)
        return None

    with nogil:
        if want_found:
            if is_int8:
                count = phash.probe_found_32_i8(<const int8_t*>dp, out_buf, n)
            else:
                count = phash.probe_found_32_i16(<const int16_t*>dp, out_buf, n)
        else:
            if is_int8:
                count = phash.probe_not_found_32_i8(<const int8_t*>dp, out_buf, n)
            else:
                count = phash.probe_not_found_32_i16(<const int16_t*>dp, out_buf, n)

    cdef Morsel result
    if count > 0:
        result = relation.take(<int32_t[:count]>out_buf)
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
    cdef public object right_hash_set   # CarcharSetWrapper or PerfectHashSet
    cdef public bint _right_has_null
    cdef public bint _build_phase
    cdef public bint _use_phash

    def __init__(self, properties=None, **parameters):
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.right_hash_set = None
        self._right_has_null = False
        self._build_phase = True
        self._use_phash = False

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
        if morsel is _EOS_SENTINEL:
            # Finalise: check if right side had any nulls
            if self._use_phash:
                # Nulls tracked during build; PerfectHashSet has no null slot
                pass
            else:
                if self.right_hash_set is not None:
                    self._right_has_null = (<CarcharSetWrapper>self.right_hash_set).has(_NULL_HASH)
                else:
                    self._right_has_null = False
                    self.right_hash_set = CarcharSetWrapper()
            return

        morsel = self._apply_join_key_casts(morsel, is_left=False)
        start = time.monotonic_ns()

        # On first right morsel: decide hash vs perfect-hash path
        if self.right_hash_set is None:
            phash = _try_build_phash(morsel, self.right_columns, None)
            if phash is not None:
                self.right_hash_set = phash
                self._use_phash = True
                # Check for nulls in this morsel
                if len(self.right_columns) == 1:
                    col = morsel.column(self.right_columns[0])
                    if isinstance(col, (Integer8Vector, Integer16Vector)):
                        if (isinstance(col, Integer8Vector) and (<Integer8Vector>col).null_bitmap_ptr() != NULL) or \
                           (isinstance(col, Integer16Vector) and (<Integer16Vector>col).null_bitmap_ptr() != NULL):
                            self._right_has_null = True
                self.readings["time_build_filter_hash_table"] += time.monotonic_ns() - start
                return

        if self._use_phash:
            phash = _try_build_phash(morsel, self.right_columns, self.right_hash_set)
            if phash is None:
                from opteryx.exceptions import InvalidInternalStateError
                raise InvalidInternalStateError(
                    "PerfectHashSet build: right-side morsel incompatible after first morsel "
                    "(non-dense encoding or null appeared mid-stream)"
                )
            self.right_hash_set = phash
            # Track nulls
            if len(self.right_columns) == 1:
                col = morsel.column(self.right_columns[0])
                if isinstance(col, (Integer8Vector, Integer16Vector)):
                    if (isinstance(col, Integer8Vector) and (<Integer8Vector>col).null_bitmap_ptr() != NULL) or \
                       (isinstance(col, Integer16Vector) and (<Integer16Vector>col).null_bitmap_ptr() != NULL):
                        self._right_has_null = True
            self.readings["time_build_filter_hash_table"] += time.monotonic_ns() - start
            return

        # CarcharSetWrapper path
        if self.right_hash_set is None:
            self.right_hash_set = CarcharSetWrapper()
        self.right_hash_set = _build_filter_hash_set(
            morsel, self.right_columns, <CarcharSetWrapper>self.right_hash_set
        )
        self.readings["time_build_filter_hash_table"] += time.monotonic_ns() - start

    cpdef void push_left(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self.emit(_EOS_SENTINEL)
            return
        morsel = self._apply_join_key_casts(morsel, is_left=True)
        if morsel.num_rows == 0:
            self.emit(morsel)
            return

        cdef Morsel result
        cdef PerfectHashSet phash

        if self._use_phash:
            phash = <PerfectHashSet>self.right_hash_set
            if self.join_type == "left semi":
                result = _phash_probe(morsel, self.left_columns, phash,
                                      True, self._right_has_null, False)
            elif self.join_type == "left anti":
                result = _phash_probe(morsel, self.left_columns, phash,
                                      False, False, False)
            elif self.join_type == "left anti null-aware":
                result = _phash_probe(morsel, self.left_columns, phash,
                                      False, self._right_has_null, True)
            else:
                result = None

            if result is not None:
                self.emit(result)
                return
            # PerfectHashSet probe fell back (probe side has nulls / non-dense).
            # Rebuild a CarcharSetWrapper by hashing each stored value and use the hash path.
            self._use_phash = False
            self.right_hash_set = _rebuild_carchar_from_phash(phash)

        cdef CarcharSetWrapper cs = <CarcharSetWrapper>self.right_hash_set
        if self.join_type == "left semi":
            self.emit(_semi_join_filter(morsel, self.left_columns, cs, self._right_has_null))
        elif self.join_type == "left anti":
            self.emit(_anti_join_filter(morsel, self.left_columns, cs))
        elif self.join_type == "left anti null-aware":
            self.emit(_anti_join_null_aware_filter(morsel, self.left_columns, cs))
