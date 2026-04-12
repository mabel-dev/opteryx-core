# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy

cpdef object vector_long_arrow_op(object arr, object key):
    """
    Fetch values from a list of dictionaries based on a specified key.

    Parameters:
        data: list
            A list of dictionaries where each dictionary represents a structured record.
        key: str
            The key whose corresponding value is to be fetched from each dictionary.

    Returns:
        numpy.ndarray: An array containing the values associated with the key in each dictionary
                     or None where the key does not exist.
    """
    # Use a Python list for efficient accumulation (avoids numpy.empty allocation)
    cdef list result = []

    cdef Py_ssize_t i
    # Iterate over the list of dictionaries
    for i in range(len(arr)):
        # Check if the key exists in the dictionary
        if key in arr[i]:
            result.append(str(arr[i][key]))
        else:
            # Append None if the key does not exist
            result.append(None)

    # Convert list to numpy array (single allocation at cold path)
    return numpy.asarray(result, dtype=object)
