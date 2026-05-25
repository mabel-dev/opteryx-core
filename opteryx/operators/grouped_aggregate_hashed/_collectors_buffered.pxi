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

from libc.stdint cimport int64_t, uint8_t
from libc.string cimport memcpy
from libcpp.vector cimport vector

from draken.vectors.vector cimport Vector
from draken.vectors.integer64_vector cimport Integer64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.float64_vector cimport from_decoded as float64_from_decoded
from draken.vectors.decimal_vector cimport DecimalVector
from draken.core.buffers cimport DrakenVector


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

    cdef void accumulate(
        self,
        object morsel,
        const int64_t* state_indices,
        Py_ssize_t n_rows,
    ):
        cdef Vector vec = morsel.column(self.column_name)
        cdef Integer64Vector iv
        cdef Float64Vector fv
        cdef Py_ssize_t i
        cdef double v
        cdef uint8_t* nulls
        cdef int64_t* idata
        cdef double* fdata
        cdef const uint32_t* sel
        cdef DrakenVector* uv
        cdef list pylist

        if isinstance(vec, Integer64Vector):
            iv = <Integer64Vector>vec
            uv = iv.unified()
            idata = <int64_t*>uv.data
            sel = uv.selection
            nulls = uv.validity
            for i in range(n_rows):
                if nulls != NULL and not _num_bitmap_valid(nulls, i):
                    continue
                self._append(state_indices[i], <double>idata[sel[i]])
            return

        if isinstance(vec, Float64Vector):
            fv = <Float64Vector>vec
            uv = fv.unified()
            fdata = <double*>uv.data
            sel = uv.selection
            nulls = uv.validity
            for i in range(n_rows):
                if nulls != NULL and not _num_bitmap_valid(nulls, i):
                    continue
                self._append(state_indices[i], fdata[sel[i]])
            return

        # Fallback: integer-narrow vectors and anything else go via to_pylist.
        pylist = vec.to_pylist()
        for i in range(n_rows):
            val = pylist[i]
            if val is None:
                continue
            self._append(state_indices[i], <double>val)

    cpdef Vector finalize(self, int64_t num_groups):
        return self.finalize_slice(0, num_groups)

    cpdef Vector finalize_slice(self, int64_t start, int64_t stop):
        cdef Py_ssize_t length = <Py_ssize_t>(stop - start)
        cdef Float64Vector out
        cdef double* out_data
        cdef Py_ssize_t i
        cdef MedianState* st_ptr
        cdef size_t cap

        if self._any_overflow:
            cap = 0
            if self._capacity > 0:
                cap = self._states[0][0].max_size
            raise ValueError(
                f"MEDIAN exceeded the per-group cap of {cap} non-null values "
                f"(first triggered by group {self._first_overflow_group}). "
                "Use APPROX_PERCENTILE for larger inputs."
            )

        if length <= 0:
            return float64_from_decoded(NULL, NULL, 0)

        cdef void* out_raw = draken_malloc(<size_t>length * sizeof(double))
        if out_raw == NULL:
            raise MemoryError()
        cdef Py_ssize_t nbytes = (length + 7) >> 3
        cdef uint8_t* out_nulls = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_nulls == NULL:
            draken_free(out_raw)
            raise MemoryError()
        memset(out_nulls, 0xFF, <size_t>nbytes)
        out_data = <double*>out_raw

        for i in range(length):
            st_ptr = &(self._states[0][<size_t>(start + i)])
            if st_ptr.size == 0:
                out_data[i] = 0.0
                _bitmap_clear(out_nulls, i)
            else:
                out_data[i] = st_ptr.finalize_median()

        return float64_from_decoded(out_raw, out_nulls, <size_t>length)
