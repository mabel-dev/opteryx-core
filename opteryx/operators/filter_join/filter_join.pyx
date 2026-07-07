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

from libc.stdint cimport int32_t, uint8_t, uint32_t
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libcpp.vector cimport vector
from draken.vectors.vector cimport Vector, mix_hash, NULL_HASH
from draken.core.buffers cimport DrakenVector

from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.
# BasePlanNode/JoinNode in scope via _operators.pyx include.
# draken_is_compressed extern is declared in hashed_inner_join.pyx (same umbrella
# module), so it is visible here.

# WP-07: locally-aliased views of the raw C++ set classes for the nogil probe.
# The bare names `CarcharSet` / `CppPerfectHashSet` are NOT cimported at module
# scope — `_collectors_distinct.pxi` (folded into this same umbrella) declares its
# own local `CarcharSet`, so a module-level cimport of that name collides. These
# aliases bind the SAME C++ types under distinct Cython names, declaring only the
# read-only probe methods the nogil push path calls. The raw pointers are read
# from the wrappers' `_ptr` (via <void*>) once, under the GIL, in `_setup_probe`.
from libc.stdint cimport int8_t as _fj_i8, int16_t as _fj_i16, uint64_t as _fj_u64

cdef extern from "carchar_set.hpp" namespace "opteryx::carchar" nogil:
    cdef cppclass _FJCarcharSet "opteryx::carchar::CarcharSet":
        size_t probe_found_32(const _fj_u64* keys, int32_t* out_indices, size_t length) noexcept
        size_t probe_not_found_32(const _fj_u64* keys, int32_t* out_indices, size_t length) noexcept

cdef extern from "perfect_hash_set.hpp" namespace "opteryx::perfect_hash" nogil:
    cdef cppclass _FJPerfectHashSet "opteryx::perfect_hash::PerfectHashSet":
        size_t probe_found_32_i8(const _fj_i8* keys, int32_t* out, size_t length) noexcept
        size_t probe_found_32_i16(const _fj_i16* keys, int32_t* out, size_t length) noexcept
        size_t probe_not_found_32_i8(const _fj_i8* keys, int32_t* out, size_t length) noexcept
        size_t probe_not_found_32_i16(const _fj_i16* keys, int32_t* out, size_t length) noexcept

cdef uint64_t _NULL_HASH = <uint64_t>0x73d59cff8f94d86cULL

# WP-13 Stage 2 — single-column compressed-key shaped k-probe for semi/anti. Probe
# each unique key hash ONCE, then scatter the found/not-found verdict to every row
# sharing that code. Null semantics are UNCHANGED from the per-row path (already
# correct): semi (IN) always excludes null probe rows (NULL IN = UNKNOWN); plain
# anti follows set membership of the null hash; null-aware anti (NOT IN) early-exits
# when the build set contains a null. Toggle for differential testing / kill-switch.
cdef bint _WP13_FILTER_KPROBE = True


cpdef void set_filter_kprobe_enabled(bint enabled):
    global _WP13_FILTER_KPROBE
    _WP13_FILTER_KPROBE = enabled


cpdef bint get_filter_kprobe_enabled():
    return _WP13_FILTER_KPROBE


# Probe modes for _filter_probe_compressed.
DEF _FILTER_MODE_SEMI = 0            # left semi  (IN)
DEF _FILTER_MODE_ANTI = 1            # left anti  (EXCEPT / INTERSECT anti)
DEF _FILTER_MODE_ANTI_NULL_AWARE = 2  # left anti null-aware (NOT IN)


cdef object _filter_probe_compressed(
    Morsel relation,
    list join_columns,
    CarcharSetWrapper seen,
    int mode,
):
    """Shaped k-probe for a single-column COMPRESSED probe key. Returns the
    filtered Morsel, or None to signal "not applicable" (multi-column, dense, or
    kill-switched) so the caller uses the per-row path.

    Correctness is identical to the per-row kernels — the only change is probing k
    unique hashes instead of n row hashes, then scattering through the codes."""
    if not _WP13_FILTER_KPROBE or len(join_columns) != 1:
        return None

    cdef Vector hv = <Vector>relation.hash_keys(join_columns)
    cdef DrakenVector* huv = hv.unified()
    if draken_is_compressed(huv) == 0:
        return None  # dense — no per-unique win; use per-row path

    cdef const uint64_t* khashes = <const uint64_t*>huv.data
    cdef const uint32_t* codes = huv.selection
    cdef Py_ssize_t k_out = <Py_ssize_t>huv.data_length
    cdef Py_ssize_t n = relation.num_rows

    cdef Vector keycol = <Vector>relation._cxx_column(join_columns[0])
    cdef uint8_t* validity = keycol.null_bitmap_ptr()
    cdef bint has_validity = (validity != NULL)

    # NOT IN with a null anywhere on the right → every left row is UNKNOWN → empty.
    if mode == _FILTER_MODE_ANTI_NULL_AWARE and seen.contains(_NULL_HASH):
        return relation.slice(0, 0)

    # Probe the k unique hashes once → found[] bitmap over codes.
    cdef int32_t* found_pos = <int32_t*>PyMem_Malloc(<size_t>(k_out if k_out > 0 else 1) * sizeof(int32_t))
    if found_pos == NULL:
        raise MemoryError()
    cdef vector[uint8_t] found
    found.assign(k_out, 0)
    cdef int32_t* out_buf = <int32_t*>PyMem_Malloc(<size_t>(n if n > 0 else 1) * sizeof(int32_t))
    if out_buf == NULL:
        PyMem_Free(found_pos)
        raise MemoryError()

    cdef Py_ssize_t nf, j, i
    cdef Py_ssize_t w = 0
    with nogil:
        nf = seen.probe_found_32_nogil(<uint64_t*>khashes, k_out, found_pos)
        for j in range(nf):
            found[found_pos[j]] = 1

        if mode == _FILTER_MODE_SEMI:
            # IN: keep found AND non-null (NULL IN (...) = UNKNOWN, always excluded).
            for i in range(n):
                if has_validity and not (validity[i >> 3] & (<uint8_t>1 << (i & 7))):
                    continue
                if found[codes[i]]:
                    out_buf[w] = <int32_t>i
                    w += 1
        elif mode == _FILTER_MODE_ANTI:
            # Plain anti: keep not-found. Null rows follow set membership of the
            # null hash via found[null_slot] — identical to the per-row path.
            for i in range(n):
                if not found[codes[i]]:
                    out_buf[w] = <int32_t>i
                    w += 1
        else:
            # NOT IN, right has no null (early-exit above handled the null case):
            # keep not-found AND non-null (NULL NOT IN (...) = UNKNOWN).
            for i in range(n):
                if has_validity and not (validity[i >> 3] & (<uint8_t>1 << (i & 7))):
                    continue
                if not found[codes[i]]:
                    out_buf[w] = <int32_t>i
                    w += 1

    PyMem_Free(found_pos)

    cdef Morsel result
    if w > 0:
        result = relation.take(<int32_t[:w]>out_buf)
    else:
        result = relation.slice(0, 0)
    PyMem_Free(out_buf)
    return result


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
    col = morsel._cxx_column(columns[0])
    cdef bint is_int8 = getattr(col, "type", None) == _draken_native.INT8
    cdef bint is_int16 = getattr(col, "type", None) == _draken_native.INT16
    if not (is_int8 or is_int16):
        return None  # Int32/Int64 — no type bound in Phase 1

    # Any null on the right build side: track _right_has_null separately;
    # null rows are skipped (not inserted) into PerfectHashSet.
    cdef void* dp
    cdef uint8_t* nulls
    dp = (<Vector>col).unified().data
    nulls = (<Vector>col).null_bitmap_ptr()
    if (<Vector>col).unified().data_length != (<Vector>col).unified().length:
        return None  # non-dense encoding (dict/const) → fall back

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
# Rebuild CarcharSetWrapper from PerfectHashSet: shared helper
# `_rebuild_carchar_from_phash` defined in _operators.pyx (also used by
# distinct's demotion path).
# ---------------------------------------------------------------------------


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
    cdef object fast = _filter_probe_compressed(
        relation, join_columns, seen_hashes, _FILTER_MODE_SEMI)
    if fast is not None:
        return fast

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
    cdef object fast = _filter_probe_compressed(
        relation, join_columns, seen_hashes, _FILTER_MODE_ANTI)
    if fast is not None:
        return fast

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

    cdef object fast = _filter_probe_compressed(
        relation, join_columns, seen_hashes, _FILTER_MODE_ANTI_NULL_AWARE)
    if fast is not None:
        return fast

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
    col = relation._cxx_column(join_columns[0])
    _col_type_probe = getattr(col, "type", None)
    cdef bint is_int8 = (_col_type_probe == _draken_native.INT8)
    cdef bint is_int16 = (_col_type_probe == _draken_native.INT16)
    if not (is_int8 or is_int16):
        return None
    cdef DrakenVector* probe_uv = (<Vector>col).unified()
    cdef void* dp = probe_uv.data
    cdef uint8_t* nulls = (<Vector>col).null_bitmap_ptr()
    if probe_uv.data_length != probe_uv.length:
        return None  # non-dense encoding — fall back

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

    # WP-07 probe-side (push_left) true-nogil state. Everything the per-morsel
    # probe path needs is resolved ONCE under the GIL at the first probe morsel
    # (`_setup_probe`) into these C-level fields, so the steady-state push body
    # touches no PyObject: raw C++ set pointers (extracted from the wrappers, which
    # are kept alive via `right_hash_set`), key/cast column indices, and the mode.
    cdef bint _probe_setup_done
    cdef int32_t* _left_key_idx      # left join-key column indices (malloc'd)
    cdef int32_t _n_left_key
    cdef int32_t* _left_cast_col     # left columns needing an implicit key cast
    cdef int* _left_cast_tgt         # 0 = FLOAT64, 1 = INT64 (parallel to _left_cast_col)
    cdef int32_t _n_left_cast
    cdef _FJCarcharSet* _seen_ptr       # raw hash-set (nogil probe); NULL until setup
    cdef _FJPerfectHashSet* _phash_ptr  # raw perfect-hash set (nogil probe)
    cdef int _filter_mode            # 0 = semi, 1 = anti, 2 = anti null-aware

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

        self._probe_setup_done = False
        self._left_key_idx = NULL
        self._n_left_key = 0
        self._left_cast_col = NULL
        self._left_cast_tgt = NULL
        self._n_left_cast = 0
        self._seen_ptr = NULL
        self._phash_ptr = NULL
        self._filter_mode = 0

    def __dealloc__(self):
        if self._left_key_idx != NULL:
            PyMem_Free(self._left_key_idx)
            self._left_key_idx = NULL
        if self._left_cast_col != NULL:
            PyMem_Free(self._left_cast_col)
            self._left_cast_col = NULL
        if self._left_cast_tgt != NULL:
            PyMem_Free(self._left_cast_tgt)
            self._left_cast_tgt = NULL

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

    cdef int push_right(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_right_gil(_EOS_SENTINEL)
                else:
                    self._push_right_gil(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cdef void _push_right_gil(self, Morsel morsel) except *:
        cdef long long start
        if morsel is _EOS_SENTINEL:
            # FilterJoin builds from the RIGHT side; right EOS finalises the
            # build set. Mark build complete so the left (probe) side may run.
            self._build_complete = True
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
        if morsel.num_rows == 0:
            return  # empty build morsel contributes nothing (mirrors push_left)
        start = time.monotonic_ns()

        # On first right morsel: decide hash vs perfect-hash path
        if self.right_hash_set is None:
            phash = _try_build_phash(morsel, self.right_columns, None)
            if phash is not None:
                self.right_hash_set = phash
                self._use_phash = True
                # Check for nulls in this morsel
                if len(self.right_columns) == 1:
                    col = morsel._cxx_column(self.right_columns[0])
                    _col_type_a = getattr(col, "type", None)
                    if _col_type_a == _draken_native.INT8 or _col_type_a == _draken_native.INT16:
                        if (<Vector>col).null_bitmap_ptr() != NULL:
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
                col = morsel._cxx_column(self.right_columns[0])
                _col_type_b = getattr(col, "type", None)
                if _col_type_b == _draken_native.INT8 or _col_type_b == _draken_native.INT16:
                    if (<Vector>col).null_bitmap_ptr() != NULL:
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

    # ---- WP-07: true-nogil probe side (push_left) --------------------------
    #
    # The steady-state per-morsel path runs with NO GIL and constructs NO Python
    # Morsel: it reads the incoming CxxMorsel carrier, applies any implicit
    # join-key casts (cxx_cast_column_c), keys via cxx_hash_c, probes the raw C++
    # set (CarcharSet / PerfectHashSet) directly, gathers the surviving rows with
    # cxx_take_c/cxx_slice_c, and forwards via _emit_cdef. The GIL is taken only
    # ONCE (first-morsel setup / PerfectHashSet→CarcharSet demotion) and on the
    # error branch — never per morsel on the success path.
    cdef int push_left(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        cdef shared_ptr[CxxMorsel] probe_sp
        cdef shared_ptr[CxxMorsel] out_sp
        cdef CxxMorsel* probe
        cdef CxxMorsel* cptr
        cdef CxxMorsel* result
        cdef uint32_t n
        cdef Py_ssize_t k
        cdef bint demote = False

        # Build-before-probe ordering invariant (fail loud, GIL only on the raise).
        if not self._build_complete:
            with gil:
                from opteryx.exceptions import InvalidInternalStateError
                self._stash_exc(InvalidInternalStateError(
                    f"{self.name}: probe-side input arrived before the build side "
                    "completed - build-before-probe ordering invariant violated."), err)
            return err.code if err != NULL else 0

        if is_eos:
            out_sp = shared_ptr[CxxMorsel](cxx_morsel_new_eos())
            self._emit_cdef(out_sp, err)
            return err.code if err != NULL else 0

        # One-time probe setup (GIL): resolve key/cast column indices, the raw
        # set pointer and the mode. Runs once per query, never per morsel.
        if not self._probe_setup_done:
            with gil:
                try:
                    self._setup_probe(raw)
                    self._probe_setup_done = True
                except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                    self._stash_exc(exc, err)
            if err != NULL and err.code != 0:
                return err.code

        # Apply implicit join-key casts on the carrier (nogil). Each cast returns
        # a new CxxMorsel sharing the untouched columns; probe_sp keeps the live
        # carrier alive across the body and frees intermediates.
        probe_sp = m
        for k in range(self._n_left_cast):
            cptr = cxx_cast_column_c(probe_sp.get(), <uint32_t>self._left_cast_col[k],
                                     self._left_cast_tgt[k])
            if cptr == NULL:
                with gil:
                    from opteryx.exceptions import UnsupportedSyntaxError
                    self._stash_exc(UnsupportedSyntaxError(
                        "FilterJoin: implicit join-key cast failed"), err)
                return err.code if err != NULL else 0
            probe_sp = shared_ptr[CxxMorsel](cptr)
        probe = probe_sp.get()

        n = probe.num_rows()
        if n == 0:
            # semi/anti of an empty probe morsel is empty — emit it (cast) as-is.
            self._emit_cdef(probe_sp, err)
            return err.code if err != NULL else 0

        # NOT IN with a null anywhere on the right → every left row is UNKNOWN → empty.
        if self._filter_mode == 2 and self._right_has_null:
            result = cxx_slice_c(probe, 0, 0)
            out_sp = shared_ptr[CxxMorsel](result)
            self._emit_cdef(out_sp, err)
            return err.code if err != NULL else 0

        result = NULL
        if self._use_phash:
            result = self._phash_probe_nogil(probe, &demote)
            if demote:
                # Probe side has nulls / non-dense narrow ints — rebuild a
                # CarcharSet from the PerfectHashSet once, then use the hash path.
                with gil:
                    try:
                        self._demote_to_carchar()
                    except BaseException as exc:  # noqa: BLE001
                        self._stash_exc(exc, err)
                if err != NULL and err.code != 0:
                    return err.code
        if not self._use_phash:
            result = self._carchar_probe_nogil(probe)

        if result == NULL:
            with gil:
                self._stash_exc(MemoryError("FilterJoin: probe kernel allocation failed"), err)
            return err.code if err != NULL else 0

        out_sp = shared_ptr[CxxMorsel](result)
        self._emit_cdef(out_sp, err)
        return err.code if err != NULL else 0

    cdef void _setup_probe(self, CxxMorsel* probe) except *:
        """One-time GIL setup for the nogil probe path: resolve left key columns,
        the implicit-cast plan, the raw C++ set pointer and the filter mode."""
        from opteryx.exceptions import InvalidInternalStateError
        from opteryx.types.logical_type import LogicalCategory, ColumnType
        cdef Py_ssize_t ncols = <Py_ssize_t>probe.names.size()
        cdef Py_ssize_t i, j
        name_to_idx = {}
        for i in range(ncols):
            name_to_idx[probe.names[i]] = i

        if self.join_type == "left semi":
            self._filter_mode = 0
        elif self.join_type == "left anti":
            self._filter_mode = 1
        elif self.join_type == "left anti null-aware":
            self._filter_mode = 2
        else:
            raise InvalidInternalStateError(
                f"FilterJoin: unsupported join_type {self.join_type!r}")

        cdef list keys = list(self.left_columns or [])
        self._n_left_key = <int32_t>len(keys)
        if self._n_left_key == 0:
            raise InvalidInternalStateError("FilterJoin: no left join-key columns")
        self._left_key_idx = <int32_t*>PyMem_Malloc(<size_t>self._n_left_key * sizeof(int32_t))
        if self._left_key_idx == NULL:
            raise MemoryError()
        for j in range(self._n_left_key):
            name = keys[j]
            keyb = name if isinstance(name, bytes) else str(name).encode("utf8")
            if keyb not in name_to_idx:
                raise InvalidInternalStateError(
                    f"FilterJoin: left key column {keyb!r} not present in probe morsel")
            self._left_key_idx[j] = <int32_t>name_to_idx[keyb]

        # Implicit join-key cast plan → per-column (index, target) for the nogil path.
        self._build_join_key_cast_plan()
        cast_cols = []
        cast_tgts = []
        if self._join_key_cast_plan:
            for rule in self._join_key_cast_plan:
                col = rule["left_column"]
                colb = col if isinstance(col, bytes) else str(col).encode("utf8")
                if colb not in name_to_idx:
                    continue
                target = rule["target_type"]
                cat = target.category if isinstance(target, ColumnType) else target
                if cat == LogicalCategory.FLOAT:
                    cast_cols.append(int(name_to_idx[colb])); cast_tgts.append(0)
                elif cat == LogicalCategory.INTEGER:
                    cast_cols.append(int(name_to_idx[colb])); cast_tgts.append(1)
        self._n_left_cast = <int32_t>len(cast_cols)
        if self._n_left_cast > 0:
            self._left_cast_col = <int32_t*>PyMem_Malloc(<size_t>self._n_left_cast * sizeof(int32_t))
            self._left_cast_tgt = <int*>PyMem_Malloc(<size_t>self._n_left_cast * sizeof(int))
            if self._left_cast_col == NULL or self._left_cast_tgt == NULL:
                raise MemoryError()
            for j in range(self._n_left_cast):
                self._left_cast_col[j] = <int32_t>cast_cols[j]
                self._left_cast_tgt[j] = <int>cast_tgts[j]

        # Extract the raw C++ set pointer (kept alive by right_hash_set).
        if self._use_phash:
            if not isinstance(self.right_hash_set, PerfectHashSet):
                raise InvalidInternalStateError(
                    "FilterJoin: _use_phash set but right_hash_set is not a PerfectHashSet")
            self._phash_ptr = <_FJPerfectHashSet*><void*>(<PerfectHashSet>self.right_hash_set)._ptr
        else:
            if self.right_hash_set is None:
                self.right_hash_set = CarcharSetWrapper()
            self._seen_ptr = <_FJCarcharSet*><void*>(<CarcharSetWrapper>self.right_hash_set)._ptr

    cdef void _demote_to_carchar(self) except *:
        """PerfectHashSet→CarcharSet demotion (GIL, at most once per query): the
        probe side turned out to carry nulls / non-dense narrow ints, which the
        PerfectHashSet cannot answer. Rebuild a hash set and switch permanently."""
        cdef PerfectHashSet phs = <PerfectHashSet>self.right_hash_set
        self.right_hash_set = _rebuild_carchar_from_phash(phs)
        self._use_phash = False
        self._phash_ptr = NULL
        self._seen_ptr = <_FJCarcharSet*><void*>(<CarcharSetWrapper>self.right_hash_set)._ptr

    cdef CxxMorsel* _carchar_probe_nogil(self, CxxMorsel* probe) noexcept nogil:
        """Per-row CarcharSet probe over the carrier (semi/anti/anti-null-aware).
        Output rows are gathered in ascending row order — byte-identical to the
        pre-conversion Morsel path. Returns NULL only on allocation failure."""
        cdef uint32_t n = probe.num_rows()
        cdef CxxMorsel* hashm = cxx_hash_c(probe, self._left_key_idx, <uint32_t>self._n_left_key)
        if hashm == NULL:
            return NULL
        cdef DrakenVector* hview = &hashm.columns[0].view
        cdef const uint64_t* khashes = <const uint64_t*>hview.data
        cdef const uint32_t* codes = hview.selection
        cdef bint compressed = draken_is_compressed(hview) != 0
        cdef uint64_t* rowh_owned = NULL
        cdef uint64_t* rowh
        cdef Py_ssize_t i
        if compressed:
            rowh_owned = <uint64_t*>malloc(<size_t>(n if n > 0 else 1) * sizeof(uint64_t))
            if rowh_owned == NULL:
                cxx_morsel_delete(hashm)
                return NULL
            for i in range(n):
                rowh_owned[i] = khashes[codes[i]]
            rowh = rowh_owned
        else:
            rowh = <uint64_t*>khashes

        cdef int32_t* out_buf = <int32_t*>malloc(<size_t>(n if n > 0 else 1) * sizeof(int32_t))
        if out_buf == NULL:
            if rowh_owned != NULL:
                free(rowh_owned)
            cxx_morsel_delete(hashm)
            return NULL

        cdef _FJCarcharSet* seen = self._seen_ptr
        cdef Py_ssize_t cnt = 0
        if self._filter_mode == 0:            # semi (IN)
            cnt = seen.probe_found_32(rowh, out_buf, n)
            if self._right_has_null and cnt > 0:
                cnt = _compact_exclude_null(out_buf, cnt, rowh, _NULL_HASH)
        elif self._filter_mode == 1:          # anti (EXCEPT / INTERSECT anti)
            cnt = seen.probe_not_found_32(rowh, out_buf, n)
        else:                                 # anti null-aware (NOT IN), right has no null here
            cnt = seen.probe_not_found_32(rowh, out_buf, n)
            if cnt > 0:
                cnt = _compact_exclude_null(out_buf, cnt, rowh, _NULL_HASH)

        cdef CxxMorsel* result
        if cnt > 0:
            result = cxx_take_c(probe, out_buf, <uint32_t>cnt)
        else:
            result = cxx_slice_c(probe, 0, 0)

        free(out_buf)
        if rowh_owned != NULL:
            free(rowh_owned)
        cxx_morsel_delete(hashm)
        return result

    cdef CxxMorsel* _phash_probe_nogil(self, CxxMorsel* probe, bint* demote_out) noexcept nogil:
        """PerfectHashSet probe over a single dense, non-null narrow-int key column.
        Sets demote_out=True (returns NULL) when the probe column is ineligible
        (non-int8/16, non-dense, or nullable), so the caller falls back to the
        rebuilt CarcharSet path — mirroring the pre-conversion _phash_probe."""
        demote_out[0] = False
        cdef DrakenVector* kv = &probe.columns[self._left_key_idx[0]].view
        cdef DrakenType t = kv.type
        cdef bint is_int8 = (t == DRAKEN_INT8)
        cdef bint is_int16 = (t == DRAKEN_INT16)
        if not (is_int8 or is_int16):
            demote_out[0] = True
            return NULL
        if kv.data_length != kv.length:      # non-dense (dict / const) encoding
            demote_out[0] = True
            return NULL
        if kv.validity != NULL:              # probe side has nulls → fall back
            demote_out[0] = True
            return NULL

        cdef uint32_t n = probe.num_rows()
        cdef void* dp = kv.data
        cdef int32_t* out_buf = <int32_t*>malloc(<size_t>(n if n > 0 else 1) * sizeof(int32_t))
        if out_buf == NULL:
            return NULL
        cdef _FJPerfectHashSet* ph = self._phash_ptr
        cdef bint want_found = (self._filter_mode == 0)
        cdef Py_ssize_t cnt = 0
        if want_found:
            if is_int8:
                cnt = ph.probe_found_32_i8(<const _fj_i8*>dp, out_buf, n)
            else:
                cnt = ph.probe_found_32_i16(<const _fj_i16*>dp, out_buf, n)
        else:
            if is_int8:
                cnt = ph.probe_not_found_32_i8(<const _fj_i8*>dp, out_buf, n)
            else:
                cnt = ph.probe_not_found_32_i16(<const _fj_i16*>dp, out_buf, n)

        cdef CxxMorsel* result
        if cnt > 0:
            result = cxx_take_c(probe, out_buf, <uint32_t>cnt)
        else:
            result = cxx_slice_c(probe, 0, 0)
        free(out_buf)
        return result
