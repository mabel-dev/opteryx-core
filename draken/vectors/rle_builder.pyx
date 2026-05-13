# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

"""
Typed RLE Builders - compile-time generated builders for all scalar types.

Each builder accumulates runs of values (same value repeated N times) and
produces raw C arrays suitable for DrakenRLEBuffer construction.

All append/append_repeated methods are nogil for hot-path performance.
Builders are type-safe: no Python objects in critical paths except RLEBuilderString.
"""

from libc.stdlib cimport malloc, realloc, free
from libc.string cimport memcpy
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, float


# ============================================================================
# Int64 Builder Implementation
# ============================================================================

cdef class RLEBuilderInt64:
    """Builds RLE-encoded int64 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        """Initialize builder with capacity."""
        if initial_capacity == 0:
            initial_capacity = 1000
        self._capacity = initial_capacity
        self._run_values = <int64_t*>malloc(initial_capacity * sizeof(int64_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        """Cleanup allocations."""
        if self._run_values != NULL:
            free(self._run_values)
            self._run_values = NULL
        if self._run_lengths != NULL:
            free(self._run_lengths)
            self._run_lengths = NULL

    cdef void _ensure_capacity(self) except *:
        """Grow arrays if needed."""
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int64_t* new_values = <int64_t*>realloc(self._run_values, self._capacity * sizeof(int64_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int64_t value) nogil except *:
        """Append a single value. Merges with current run if value matches."""
        if not self._has_value:
            # First value
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            # Extend current run
            self._current_run_length += 1
        else:
            # Flush current run and start new one
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *:
        """Append value repeated count times. Efficient for known runs."""
        if count <= 0:
            return

        if not self._has_value:
            # First value
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            # Extend current run
            self._current_run_length += count
        else:
            # Flush current run and add new one
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        """Finalize and return (run_values, run_lengths, num_runs) as tuple of memoryviews."""
        if self._has_value:
            # Flush final run
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1

        # Create memoryviews and return
        cdef int64_t[::1] values_view = <int64_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Int32 Builder Implementation
# ============================================================================

cdef class RLEBuilderInt32:
    """Builds RLE-encoded int32 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int32_t* new_values = <int32_t*>realloc(self._run_values, self._capacity * sizeof(int32_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int32_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int32_t[::1] values_view = <int32_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Int16 Builder Implementation
# ============================================================================

cdef class RLEBuilderInt16:
    """Builds RLE-encoded int16 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int16_t*>malloc(initial_capacity * sizeof(int16_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int16_t* new_values = <int16_t*>realloc(self._run_values, self._capacity * sizeof(int16_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int16_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int16_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int16_t[::1] values_view = <int16_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Int8 Builder Implementation
# ============================================================================

cdef class RLEBuilderInt8:
    """Builds RLE-encoded int8 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int8_t*>malloc(initial_capacity * sizeof(int8_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int8_t* new_values = <int8_t*>realloc(self._run_values, self._capacity * sizeof(int8_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int8_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int8_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int8_t[::1] values_view = <int8_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Float64 Builder Implementation
# ============================================================================

cdef class RLEBuilderFloat64:
    """Builds RLE-encoded float64 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <double*>malloc(initial_capacity * sizeof(double))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0.0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef double* new_values = <double*>realloc(self._run_values, self._capacity * sizeof(double))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, double value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, double value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef double[::1] values_view = <double[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Float32 Builder Implementation
# ============================================================================

cdef class RLEBuilderFloat32:
    """Builds RLE-encoded float32 data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <float*>malloc(initial_capacity * sizeof(float))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0.0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef float* new_values = <float*>realloc(self._run_values, self._capacity * sizeof(float))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, float value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, float value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef float[::1] values_view = <float[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Bool Builder Implementation
# ============================================================================

cdef class RLEBuilderBool:
    """Builds RLE-encoded bool (uint8) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <uint8_t*>malloc(initial_capacity * sizeof(uint8_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef uint8_t* new_values = <uint8_t*>realloc(self._run_values, self._capacity * sizeof(uint8_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, uint8_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, uint8_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef uint8_t[::1] values_view = <uint8_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Date32 Builder Implementation
# ============================================================================

cdef class RLEBuilderDate32:
    """Builds RLE-encoded Date32 (int32_t days since epoch) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int32_t* new_values = <int32_t*>realloc(self._run_values, self._capacity * sizeof(int32_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int32_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int32_t[::1] values_view = <int32_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Timestamp Builder Implementation
# ============================================================================

cdef class RLEBuilderTimestamp:
    """Builds RLE-encoded Timestamp (int64_t microseconds since epoch) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int64_t*>malloc(initial_capacity * sizeof(int64_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int64_t* new_values = <int64_t*>realloc(self._run_values, self._capacity * sizeof(int64_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int64_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int64_t[::1] values_view = <int64_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Time32 Builder Implementation
# ============================================================================

cdef class RLEBuilderTime32:
    """Builds RLE-encoded Time32 (int32_t milliseconds) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int32_t* new_values = <int32_t*>realloc(self._run_values, self._capacity * sizeof(int32_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int32_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int32_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int32_t[::1] values_view = <int32_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Time64 Builder Implementation
# ============================================================================

cdef class RLEBuilderTime64:
    """Builds RLE-encoded Time64 (int64_t nanoseconds) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int64_t*>malloc(initial_capacity * sizeof(int64_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int64_t* new_values = <int64_t*>realloc(self._run_values, self._capacity * sizeof(int64_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int64_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int64_t[::1] values_view = <int64_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# Interval Builder Implementation
# ============================================================================

cdef class RLEBuilderInterval:
    """Builds RLE-encoded Interval (int64_t months+days+microseconds packed) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._capacity = initial_capacity
        self._run_values = <int64_t*>malloc(initial_capacity * sizeof(int64_t))
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_values == NULL or self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = 0
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_values != NULL:
            free(self._run_values)
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int64_t* new_values = <int64_t*>realloc(self._run_values, self._capacity * sizeof(int64_t))
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_values == NULL or new_lengths == NULL:
                raise MemoryError()
            self._run_values = new_values
            self._run_lengths = new_lengths

    cdef void append(self, int64_t value) nogil except *:
        if not self._has_value:
            self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, int64_t value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._run_values[self._num_runs] = self._last_value
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int64_t[::1] values_view = <int64_t[:self._num_runs]>self._run_values
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (values_view, lengths_view, self._num_runs)


# ============================================================================
# String Builder Implementation
# ============================================================================

cdef class RLEBuilderString:
    """Builds RLE-encoded String (variable-width) data."""

    def __cinit__(self, size_t initial_capacity=1000):
        self._values = []
        self._capacity = initial_capacity
        self._run_lengths = <int32_t*>malloc(initial_capacity * sizeof(int32_t))
        if self._run_lengths == NULL:
            raise MemoryError()
        self._num_runs = 0
        self._last_value = None
        self._current_run_length = 0
        self._has_value = False

    def __dealloc__(self):
        if self._run_lengths != NULL:
            free(self._run_lengths)

    cdef void _ensure_capacity(self) except *:
        if self._num_runs >= self._capacity:
            self._capacity *= 2
            cdef int32_t* new_lengths = <int32_t*>realloc(self._run_lengths, self._capacity * sizeof(int32_t))
            if new_lengths == NULL:
                raise MemoryError()
            self._run_lengths = new_lengths

    cdef void append(self, object value) nogil except *:
        # Note: String appends may acquire GIL for object management
        if not self._has_value:
            with gil:
                self._last_value = value
            self._current_run_length = 1
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += 1
        else:
            with gil:
                self._ensure_capacity()
                self._values.append(self._last_value)
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            with gil:
                self._last_value = value
            self._current_run_length = 1

    cdef void append_repeated(self, object value, int32_t count) nogil except *:
        if count <= 0:
            return
        if not self._has_value:
            with gil:
                self._last_value = value
            self._current_run_length = count
            self._has_value = True
        elif value == self._last_value:
            self._current_run_length += count
        else:
            with gil:
                self._ensure_capacity()
                self._values.append(self._last_value)
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
            with gil:
                self._last_value = value
            self._current_run_length = count

    cdef tuple finish(self):
        if self._has_value:
            self._ensure_capacity()
            self._values.append(self._last_value)
            self._run_lengths[self._num_runs] = self._current_run_length
            self._num_runs += 1
        cdef int32_t[::1] lengths_view = <int32_t[:self._num_runs]>self._run_lengths
        return (self._values, lengths_view, self._num_runs)
