# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector

cdef Int64Vector non_null_row_indices(object relation, list column_names)
