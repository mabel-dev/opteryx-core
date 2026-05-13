# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdlib cimport malloc, free

"""
Vector conversion operations.

Provides conversion of Python sequences to float32 C arrays
for use in vector similarity operations.

Key design:
- Allocate with malloc (caller must free)
- Strict type validation (fail-fast on invalid input)
- No intermediate array copies
"""


cdef class FloatBuffer:
    """
    Wrapper for malloc'd float32 array with automatic cleanup.

    This class owns the allocated buffer and frees it on garbage collection.
    Used to safely return float32 memoryviews from Cython functions.
    """
    cdef float[::1] _view
    cdef float* _buffer
    cdef Py_ssize_t _size

    def __cinit__(self, Py_ssize_t size):
        self._size = size
        self._buffer = <float*>malloc(size * sizeof(float))
        if self._buffer == NULL:
            raise MemoryError(f"Failed to allocate {size} float32 values")
        self._view = <float[:size]>self._buffer

    def __dealloc__(self):
        if self._buffer != NULL:
            free(self._buffer)
            self._buffer = NULL

    def __getitem__(self, Py_ssize_t index):
        return self._view[index]

    def __setitem__(self, Py_ssize_t index, float value):
        self._view[index] = value

    def __len__(self):
        return self._size

    def as_memoryview(self):
        """Return the underlying float32 memoryview."""
        return self._view


def sequence_to_float32_vector(object sequence):
    """
    Convert Python sequence to FloatBuffer.

    Validates:
    - Sequence is iterable and sized
    - All elements are numeric (int, float, bool)
    - No NaN or infinity values
    - No empty sequences

    Args:
        sequence: Python list/tuple of numbers

    Returns:
        FloatBuffer with converted values

    Raises:
        TypeError: If not a sequence or contains non-numeric values
        ValueError: If empty, too large, or contains NaN/infinity
    """
    cdef:
        Py_ssize_t n, i
        FloatBuffer vec
        object item
        float value

    # Validate input is sized and iterable
    try:
        n = len(sequence)
    except TypeError:
        raise TypeError(f"Expected sequence (list/tuple), got {type(sequence).__name__}")

    if n == 0:
        raise ValueError("Cannot convert empty sequence to vector")

    if n > 1_000_000:
        raise ValueError(f"Vector exceeds max size: {n} > 1,000,000")

    # Allocate output vector
    vec = FloatBuffer(n)

    # Convert each element
    for i in range(n):
        try:
            item = sequence[i]
        except (IndexError, KeyError):
            raise ValueError(f"Sequence index {i} out of range")

        # Type coercion: bool, int, float → float32
        if item is None:
            raise TypeError(f"Vector element [{i}] is None")
        elif isinstance(item, bool):
            value = <float>(1.0 if item else 0.0)
        elif isinstance(item, int):
            try:
                value = <float>item
            except OverflowError:
                raise ValueError(f"Vector element [{i}] overflows float32")
        elif isinstance(item, float):
            value = <float>item
        else:
            # Attempt generic float conversion
            try:
                value = <float>float(item)
            except (TypeError, ValueError):
                raise TypeError(f"Vector element [{i}] = {item!r} cannot convert to float32")

        # Validate result (reject NaN, infinity)
        if value != value:  # NaN check (NaN != NaN is True)
            raise ValueError(f"Vector element [{i}] is NaN")

        if value == float('inf') or value == float('-inf'):
            raise ValueError(f"Vector element [{i}] is infinity")

        vec[i] = value

    return vec


cdef inline float _coerce_item_to_float(object item) except *:
    """
    Helper to coerce a single item to float32.
    Used in tight loops for efficiency.
    """
    cdef float value

    if isinstance(item, bool):
        return <float>(1.0 if item else 0.0)
    elif isinstance(item, int):
        return <float>item
    elif isinstance(item, float):
        return <float>item
    else:
        return <float>float(item)


def fill_float32_array(object sequence, float[::1] output_array):
    """
    Fill a pre-allocated float32 array from a Python sequence.

    Used when caller manages memory lifecycle.

    Args:
        sequence: Python list/tuple of numbers
        output_array: float32 memoryview to fill

    Raises:
        ValueError: If sequence length doesn't match array size
        TypeError: If elements cannot convert to float32
        ValueError: If any element is NaN or infinity
    """
    cdef:
        Py_ssize_t n, i
        float value
        object item

    n = len(sequence)
    if n != output_array.shape[0]:
        raise ValueError(f"Sequence length {n} != array size {output_array.shape[0]}")

    for i in range(n):
        item = sequence[i]

        if item is None:
            raise TypeError(f"Element [{i}] is None")

        try:
            value = _coerce_item_to_float(item)
        except (TypeError, ValueError):
            raise TypeError(f"Element [{i}] = {item!r} cannot convert to float32")

        # Reject NaN and infinity
        if value != value:
            raise ValueError(f"Element [{i}] is NaN")
        if value == float('inf') or value == float('-inf'):
            raise ValueError(f"Element [{i}] is infinity")

        output_array[i] = value
