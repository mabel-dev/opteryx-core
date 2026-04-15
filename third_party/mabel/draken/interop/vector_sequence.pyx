# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Generic sequence-to-Draken vector interop helpers.

This module contains sequence and scalar conversion helpers that are not
specific to Arrow. Arrow-specific conversion remains in the Arrow interop
module.
"""

from libc.stdint cimport int64_t, uint8_t

from opteryx.compiled.draken.core.buffers cimport DrakenType
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.bool_vector cimport from_sequence as bool_from_sequence
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport from_sequence as float64_from_sequence
from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from opteryx.compiled.draken.vectors.scalar_constructors cimport from_sequence as constant_from_sequence


cpdef object vector_from_sequence(object data, object dtype=None):
    """
    Create a Draken vector from a typed memoryview or Python sequence.

    Supports:
    - int64 memoryviews
    - float64 memoryviews
    - bool/uint8 memoryviews
    - Python sequences
    - scalar constant encoding
    """
    cdef int64_t[::1] int64_view
    cdef double[::1] float64_view
    cdef uint8_t[::1] bool_view

    # Zero-copy fast path for typed memoryviews.
    try:
        int64_view = data
        return int64_from_sequence(int64_view)
    except (TypeError, ValueError, BufferError):
        pass

    try:
        float64_view = data
        return float64_from_sequence(float64_view)
    except (TypeError, ValueError, BufferError):
        pass

    try:
        bool_view = data
        return bool_from_sequence(bool_view)
    except (TypeError, ValueError, BufferError):
        pass

    # Constant path for scalar-like inputs.
    if data is None or isinstance(data, (bool, int, float, bytes, str)):
        return constant_from_sequence(data, dtype)

    # Python sequences.
    if isinstance(data, (list, tuple)):
        return constant_from_sequence(data, dtype)

    # Fallback for any remaining object forms.
    return constant_from_sequence(data, dtype)


cpdef DrakenType sequence_type_to_draken(object dtype):
    """
    Convert a generic sequence-oriented dtype hint to a DrakenType enum.
    """
    if dtype is None:
        return DrakenType.DRAKEN_NON_NATIVE
    return DrakenType.DRAKEN_NON_NATIVE
