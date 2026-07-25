# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

# Buffered collectors — those that must hold every non-null input value
# per group before they can answer (vs streaming collectors which fold
# state on each row). MEDIAN is the canonical example.
#
# Memory: per-group MedianState is hard-capped at MEDIAN_MAX_VALUES_PER_GROUP
# (default 1000). Exceeding the cap raises during accumulate(); we keep the
# state intact so the engine can still surface a clean diagnostic.

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t
from libc.stddef cimport size_t
from libc.string cimport memcpy, memset
from libcpp.vector cimport vector

from draken.vectors.vector cimport Vector, from_decoded as _vector_from_decoded
from draken.core.buffers cimport DrakenVector, DrakenType
from draken.core.buffers cimport DRAKEN_INT64, DRAKEN_FLOAT64
from draken.core.buffers cimport DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_FLOAT32

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil


cdef extern from "../aggregate/_agg_kernels.hpp" namespace "opteryx::ungrouped":
    cdef cppclass MedianState:
        double* buf
        size_t  size
        size_t  cap
        size_t  max_size
        bint    overflowed
        MedianState() except +
        bint append(double v) noexcept
        double finalize_median() noexcept


cdef class _DeferredMedianCollector(BaseCollector):
    """Resolves to MedianFloat64Collector on first accumulate (after type check)."""
    pass


cdef class MedianFloat64Collector(BaseCollector):
    """Per-group exact median using std::nth_element. Coerces int/float to double.

    Cap overflow cannot raise from accumulate() (it's `cdef void` without
    except *), so we silently drop excess values and raise at finalize time.
    The first overflowing group's index and cap are kept for diagnostics.
    """

    cdef vector[MedianState]* _states
    cdef int64_t _capacity
    cdef bint    _any_overflow
    cdef int64_t _first_overflow_group

    def __cinit__(self):
        self._nogil_capable = False  # buffers values per group → GIL accumulate_gil
        self._states = new vector[MedianState]()
        self._capacity = 0
        self._any_overflow = False
        self._first_overflow_group = -1

    def __dealloc__(self):
        if self._states != NULL:
            del self._states
            self._states = NULL

    cdef void grow(self, int64_t new_count):
        if new_count <= self._capacity:
            return
        self._states.resize(<size_t>new_count)
        self._capacity = new_count

    cdef inline void _append(self, int64_t gid, double v) noexcept:
        cdef MedianState* st_ptr = &(self._states[0][<size_t>gid])
        if not st_ptr.append(v):
            if not self._any_overflow:
                self._any_overflow = True
                self._first_overflow_group = gid

    cdef void accumulate_gil(
        self,
        Morsel morsel,
        const uint32_t* state_indices,
        Py_ssize_t n_rows,
    ):
        # GIL path (buffers values per group). Per-row template (D-B): one
        # type-dispatch per morsel, typed pointers cached, pure-C inner loop.
        # Uniform Vector access via vec.unified() — works for Dense/Constant/Dict
        # shapes through data[selection[i]] (CLAUDE.md §11).
        if self._col_idx < 0:
            self._col_idx = morsel._column_index_from_name(self.column_name)
        cdef Vector vec = morsel._get_column(self._col_idx)
        cdef DrakenVector* uv = vec.unified()
        cdef DrakenType t = uv.type
        cdef Py_ssize_t i
        cdef uint8_t* nulls = uv.validity
        cdef const uint32_t* sel = uv.selection

        # Width-aware read straight into the C++ MedianState — no to_pylist.
        # MEDIAN is numeric; non-numeric types fail loud.
        if t == DRAKEN_INT64:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], <double>(<int64_t*>uv.data)[sel[i]])
        elif t == DRAKEN_INT32:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], <double>(<int32_t*>uv.data)[sel[i]])
        elif t == DRAKEN_INT16:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], <double>(<int16_t*>uv.data)[sel[i]])
        elif t == DRAKEN_INT8:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], <double>(<int8_t*>uv.data)[sel[i]])
        elif t == DRAKEN_FLOAT64:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], (<double*>uv.data)[sel[i]])
        elif t == DRAKEN_FLOAT32:
            for i in range(n_rows):
                if nulls == NULL or _num_bitmap_valid(nulls, i):
                    self._append(state_indices[i], <double>(<float*>uv.data)[sel[i]])
        else:
            raise NotImplementedError(
                f"MEDIAN over column type {t} is not supported (numeric only)")

    cpdef Vector finalize(self, int64_t num_groups):
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        # Producer pattern: draken_malloc the output buffer + validity
        # bitmap, fill in a nogil loop, hand ownership to a Vector via
        # from_decoded. The buffers MUST be draken_malloc'd — they are
        # draken_free'd by the Vector on GC.
        cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
        cdef Py_ssize_t bitmap_bytes
        cdef double* out_data
        cdef uint8_t* validity
        cdef Py_ssize_t i
        cdef MedianState* st_ptr
        cdef size_t cap
        cdef bint any_null = False

        if self._any_overflow:
            cap = 0
            if self._capacity > 0:
                cap = self._states[0][0].max_size
            raise ValueError(
                f"MEDIAN — too many values in one group (cap: {cap}; first "
                f"triggered by group {self._first_overflow_group}). Use "
                "APPROX_PERCENTILE(x, 0.5) for approximate median over large "
                "sets of values."
            )

        if length <= 0:
            # Empty result: zero-length all-valid Vector.
            return _vector_from_decoded(NULL, NULL, 0, DRAKEN_FLOAT64)

        out_data = <double*>draken_malloc(<size_t>length * sizeof(double))
        if out_data == NULL:
            raise MemoryError()

        bitmap_bytes = (length + 7) >> 3
        validity = <uint8_t*>draken_malloc(<size_t>bitmap_bytes)
        if validity == NULL:
            draken_free(out_data)
            raise MemoryError()
        memset(validity, 0xFF, bitmap_bytes)

        for i in range(length):
            st_ptr = &(self._states[0][<size_t>(start + i)])
            if st_ptr.size == 0:
                out_data[i] = 0.0
                validity[i >> 3] &= ~(1 << (i & 7))
                any_null = True
            else:
                out_data[i] = st_ptr.finalize_median()

        # All-valid normalization invariant (00_data_model.md):
        # validity==NULL means "all rows valid". Free + drop the bitmap.
        if not any_null:
            draken_free(validity)
            validity = NULL

        return _vector_from_decoded(
            <void*>out_data, validity, <uint32_t>length, DRAKEN_FLOAT64
        )
