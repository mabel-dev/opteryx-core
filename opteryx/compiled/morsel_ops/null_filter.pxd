# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from draken.vectors.integer64_vector cimport Integer64Vector

cdef Integer64Vector non_null_row_indices(object relation, list column_names)
