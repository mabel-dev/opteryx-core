# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.compiled.draken.vectors.string_vector cimport StringVectorBuilder

cpdef object vector_arrow_op(object arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        StringVector: A Draken string vector containing the values associated with the key
                     in each dictionary, or None where the key does not exist.
    """
    # Determine the number of items in the input list
    cdef Py_ssize_t n = len(arr)
    cdef dict document
    cdef object value
    cdef bytes value_bytes

    # Use StringVectorBuilder for efficient construction
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(n, 64)

    cdef Py_ssize_t i
    # Iterate over the list of dictionaries
    for i in range(n):
        # Check if the key exists in the dictionary
        document = arr[i]
        if document is not None and key in document:
            value = document[key]
            if value is None:
                builder.append_null()
            else:
                # Convert value to string
                if isinstance(value, bytes):
                    builder.append(value)
                else:
                    value_bytes = str(value).encode('utf-8')
                    builder.append(value_bytes)
        else:
            builder.append_null()

    # Return the constructed Draken string vector
    return builder.finish()
