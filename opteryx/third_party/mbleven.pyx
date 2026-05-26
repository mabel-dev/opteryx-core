# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from cpython.unicode cimport PyUnicode_DATA
from cpython.unicode cimport PyUnicode_GET_LENGTH
from cpython.unicode cimport PyUnicode_KIND
from cpython.unicode cimport PyUnicode_READ

"""Optimized Cython implementation of mbleven algorithm"""

# This is an Opteryx-owned rewrite of the mbleven algorithm. Do not revendor
# this file from the original Python implementation.
#
# This has been optimized for Cython with Opteryx-specific enhancements.
# ~2x faster than the original Python implementation via:
# - Inlined case-insensitive character comparison using bit-level ASCII trick
# - Unrolled models for diff values (0, 1, 2)
#
# This file maintains the Public Domain Licence, this does not change the licence
# of any other files in this project.

DEF REPLACE = 1
DEF INSERT = 2
DEF DELETE = 4

# ASCII case-insensitive comparison: (a | 0x20) == (b | 0x20)
# Works because uppercase A-Z are 0x41-0x5A, lowercase a-z are 0x61-0x7A
# Bit 5 (0x20) distinguishes case. ORing with 0x20 converts both to lowercase.
cdef inline bint eq_lower(unsigned int a, unsigned int b) noexcept:
    """Case-insensitive character comparison using bit-level ASCII trick."""
    return (a | 0x20) == (b | 0x20)


cpdef int compare(str s1, str s2):
    """Compare two strings with edit distance threshold of 2.

    Returns the minimum edit distance (0-2) or -1 if distance > 2.
    Case-insensitive comparison.
    """
    if s1 is s2 or s1 == s2:
        return 0

    cdef Py_ssize_t len1 = PyUnicode_GET_LENGTH(s1)
    cdef Py_ssize_t len2 = PyUnicode_GET_LENGTH(s2)

    # Normalize so len1 >= len2
    if len1 < len2:
        len1, len2 = len2, len1
        s1, s2 = s2, s1

    # Early exit: length difference > 2 means edit distance > 2
    cdef Py_ssize_t diff = len1 - len2
    if diff > 2:
        return -1
    if len2 == 0:
        return <int>len1

    cdef int best = 3
    cdef int cost
    cdef Py_ssize_t idx1, idx2
    cdef int kind1 = PyUnicode_KIND(s1)
    cdef int kind2 = PyUnicode_KIND(s2)
    cdef void* data1 = PyUnicode_DATA(s1)
    cdef void* data2 = PyUnicode_DATA(s2)

    # ----- diff == 0: Try three models -----
    if diff == 0:
        # Model 1: INSERT then DELETE
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                if cost == 1:
                    idx2 += 1  # INSERT
                else:
                    idx1 += 1  # DELETE
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

        # Model 2: DELETE then INSERT
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                if cost == 1:
                    idx1 += 1  # DELETE
                else:
                    idx2 += 1  # INSERT
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

        # Model 3: REPLACE then REPLACE
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                idx1 += 1
                idx2 += 1
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

    # ----- diff == 1: Try two models -----
    elif diff == 1:
        # Model 1: DELETE then REPLACE
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                if cost == 1:
                    idx1 += 1  # DELETE
                else:
                    idx1 += 1
                    idx2 += 1  # REPLACE
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

        # Model 2: REPLACE then DELETE
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                if cost == 1:
                    idx1 += 1
                    idx2 += 1  # REPLACE
                else:
                    idx1 += 1  # DELETE
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

    # ----- diff == 2: One model (DELETE, DELETE) -----
    else:  # diff == 2
        cost = 0
        idx1 = idx2 = 0
        while idx1 < len1 and idx2 < len2:
            if not eq_lower(PyUnicode_READ(kind1, data1, idx1), PyUnicode_READ(kind2, data2, idx2)):
                cost += 1
                if cost > 2:
                    break
                idx1 += 1  # DELETE
            else:
                idx1 += 1
                idx2 += 1
        if cost <= 2:
            cost += (len1 - idx1) + (len2 - idx2)
            if cost < best:
                best = cost

    return best if best < 3 else -1
