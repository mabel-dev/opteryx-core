# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""Draken-native CROSS JOIN UNNEST implementation (no numpy, no arrow)."""

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from draken.vectors.int64_vector cimport from_rle_builder as int64_from_rle_builder
from draken.vectors.vector cimport Vector
from draken.interop.vector_sequence cimport vector_from_sequence
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from libc.stdint cimport int32_t, int64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy


cpdef tuple build_rows_indices_and_column_draken(object column_vector):
    """Build row indices and flattened column data from ARRAY column (Draken-native).

    Parameters:
        column_vector: Draken Vector (ArrayVector for ARRAY columns)
            The unnest column to flatten

    Returns:
        tuple of (Int64Vector, Draken vector)
            Row indices and flattened typed data
    """
    cdef int64_t row_count = len(column_vector)
    cdef int64_t i, j
    cdef int64_t total_size = 0
    cdef list flat_data_list = []
    cdef IntBuffer indices_buf
    cdef object element

    # First pass: count total elements and collect data
    for i in range(row_count):
        element = column_vector[i]
        if element is not None:
            if hasattr(element, '__iter__') and not isinstance(element, (str, bytes)):
                # It's iterable (array-like)
                for item in element:
                    flat_data_list.append(item)
                    total_size += 1
            else:
                # Single value, treat as 1-element array
                flat_data_list.append(element)
                total_size += 1

    # Early exit if no data
    if total_size == 0:
        return (int64_from_sequence(None), vector_from_sequence([]))

    # Second pass: build indices buffer
    indices_buf = IntBuffer(total_size)
    cdef int64_t idx = 0
    for i in range(row_count):
        element = column_vector[i]
        if element is not None:
            if hasattr(element, '__iter__') and not isinstance(element, (str, bytes)):
                for item in element:
                    indices_buf.append(i)
                    idx += 1
            else:
                indices_buf.append(i)
                idx += 1

    # Create Int64Vector for indices
    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    # Create typed vector from flattened data
    data_vector = vector_from_sequence(flat_data_list)
    return (vec, data_vector)


cpdef tuple build_filtered_rows_indices_and_column_draken(object column_vector, set valid_values):
    """Build row indices and flattened column data for filtered ARRAY column (Draken-native).

    Parameters:
        column_vector: Draken Vector
            The unnest column to flatten and filter
        valid_values: set
            Values to include in results

    Returns:
        tuple of (Int64Vector, Draken vector)
            Row indices and flattened filtered data
    """
    cdef int64_t row_count = len(column_vector)
    cdef int64_t i, j
    cdef int64_t total_matched = 0
    cdef list flat_data_list = []
    cdef IntBuffer indices_buf
    cdef object element
    cdef object item

    # Initialize buffer with estimate
    indices_buf = IntBuffer(row_count)

    # Process rows
    for i in range(row_count):
        element = column_vector[i]
        if element is not None:
            if hasattr(element, '__iter__') and not isinstance(element, (str, bytes)):
                # It's iterable (array-like)
                for item in element:
                    if item in valid_values:
                        flat_data_list.append(item)
                        indices_buf.append(i)
                        total_matched += 1
            else:
                # Single value
                if element in valid_values:
                    flat_data_list.append(element)
                    indices_buf.append(i)
                    total_matched += 1

    # Early exit if no matches
    if total_matched == 0:
        return (int64_from_sequence(None), vector_from_sequence([]))

    # Create Int64Vector for indices
    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    # Create typed vector from flattened data
    data_vector = vector_from_sequence(flat_data_list)
    return (vec, data_vector)


cpdef tuple build_cartesian_indices(int64_t left_rows, int64_t right_rows):
    """
    Build row indices for a Cartesian product (CROSS JOIN) (Draken-native).

    Left index is RLE-encoded (left_rows runs of length right_rows each).
    Right index is dense ([0..right_rows-1] repeated left_rows times).

    Parameters:
        left_rows: Number of rows in the left table
        right_rows: Number of rows in the right table

    Returns:
        tuple of (Int64Vector, Int64Vector)
            Left (RLE) and right (dense) row indices
    """
    cdef int64_t total_rows = left_rows * right_rows
    cdef int64_t i
    cdef int64_t j
    cdef int64_t* left_run_vals
    cdef int32_t* left_run_lens
    cdef Int64Vector left_vec
    cdef IntBuffer right_indices_buf
    cdef const int64_t[::1] right_mv
    cdef Int64Vector right_vec

    if total_rows == 0:
        return (Int64Vector(0), Int64Vector(0))

    # Build RLE left index: left_rows runs, run value i with run length right_rows
    left_run_vals = <int64_t*>malloc(left_rows * sizeof(int64_t))
    left_run_lens = <int32_t*>malloc(left_rows * sizeof(int32_t))
    if left_run_vals == NULL or left_run_lens == NULL:
        free(left_run_vals)
        free(left_run_lens)
        raise MemoryError()

    for i in range(left_rows):
        left_run_vals[i] = i
        left_run_lens[i] = <int32_t>right_rows

    left_vec = int64_from_rle_builder(left_run_vals, left_run_lens, <size_t>left_rows)
    free(left_run_vals)
    free(left_run_lens)

    # Build dense right index: [0, 1, ..., right_rows-1] repeated left_rows times
    right_indices_buf = IntBuffer(total_rows)
    for i in range(left_rows):
        for j in range(right_rows):
            right_indices_buf.append(j)

    right_mv = right_indices_buf.get_buffer()
    right_vec = int64_from_sequence(right_mv)
    right_vec._arrow_data_buf = right_indices_buf

    return (left_vec, right_vec)
