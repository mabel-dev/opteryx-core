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

"""opteryx.compiled.morsel_ops.null_filter

Null filtering operations for relations.

This module provides functions to identify rows where all specified columns are
non-null, using Draken vector capabilities.
"""


from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from opteryx.compiled.structures.buffers cimport IntBuffer

from libc.stdint cimport int64_t, uint8_t, uintptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset


cdef inline Int64Vector non_null_row_indices(object relation, list column_names):
    """
    Compute indices of rows where all `column_names` in `relation` are non-null.
    Returns a native Int64Vector of row indices.

    Uses Draken vector types to wrap results.
    """
    cdef:
        Py_ssize_t num_rows = relation.num_rows
        uint8_t* combined_nulls = <uint8_t*>malloc(num_rows * sizeof(uint8_t))
        object column, chunk, bitmap_buffer
        const uint8_t* validity
        Py_ssize_t i, j, count = 0
        Py_ssize_t offset, length
        uint8_t bit
        Py_ssize_t bit_index, chunk_offset
        IntBuffer indices_buf
        const int64_t[::1] mv
        Int64Vector vec

    if not combined_nulls:
        raise MemoryError()

    # Initialize with 1s (all valid initially)
    memset(combined_nulls, 1, num_rows)

    try:
        for column_name in column_names:
            column = relation.column(column_name)
            offset = 0

            # Iterate through chunks in the Column/ChunkedArray
            if hasattr(column, "to_arrow"):
                column = column.to_arrow()
            for chunk in column.chunks if hasattr(column, "chunks") else [column]:
                length = len(chunk)
                bitmap_buffer = chunk.buffers()[0]  # validity buffer

                if bitmap_buffer is None:
                    # No validity buffer means all values in this chunk are valid
                    offset += length
                    continue

                validity = <const uint8_t*><uintptr_t>bitmap_buffer.address
                if validity == NULL:
                    raise RuntimeError(f"Null validity buffer for column '{column_name}'")

                chunk_offset = chunk.offset
                for j in range(length):
                    bit_index = chunk_offset + j
                    bit = (validity[bit_index >> 3] >> (bit_index & 7)) & 1
                    combined_nulls[offset + j] &= bit

                offset += length

        # Build the resulting index buffer
        # We use IntBuffer to avoid pre-calculating the final count
        indices_buf = IntBuffer(num_rows // 2 if num_rows > 0 else 0)

        for i in range(num_rows):
            if combined_nulls[i]:
                indices_buf.append(i)

        # Convert to Int64Vector
        mv = indices_buf.get_buffer()
        vec = int64_from_sequence(mv)
        # Anchor the buffer object to ensure memory safety
        vec._arrow_data_buf = indices_buf

        return vec

    finally:
        free(combined_nulls)


cpdef Int64Vector non_null_indices(object relation, list column_names):
    """
    Public interface for finding non-null rows, returning a native Draken Int64Vector.

    Parameters
    ----------
    relation : pyarrow.Table
        PyArrow table to filter
    column_names : list of str
        Column names to check for nullness

    Returns
    -------
    Int64Vector
        Draken Int64Vector containing indices of rows where all specified columns are non-null
    """
    return non_null_row_indices(relation, column_names)
