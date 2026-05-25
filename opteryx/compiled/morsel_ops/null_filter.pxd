# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libcpp.vector cimport vector
from libc.stdint cimport int64_t
from draken.morsels.morsel cimport Morsel

cdef vector[int64_t] non_null_row_indices(Morsel morsel, list column_names)
