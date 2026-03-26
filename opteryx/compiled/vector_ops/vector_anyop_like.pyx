# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint8_t
from libc.string cimport memset
from libc.stdlib cimport malloc, free

from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector


cpdef BoolVector vector_anyop_like(object literal, ArrayVector column):
    """
    Draken '<column array> LIKE ANY (patterns)' — True iff any non-null element in the row
    matches any of the SQL LIKE patterns.

    Parameters:
        literal: SQL LIKE pattern (str/bytes) or list/tuple of patterns.
                 None entries in a list are ignored (SQL NULL semantics).
        column: ArrayVector where each row is a list of string/bytes elements.

    Returns:
        BoolVector: True where ANY element × ANY pattern matches, False otherwise.
                    Null bitmap set for rows where the column value is NULL.
    """
    import re
    from opteryx.utils.sql import sql_like_to_regex

    cdef Py_ssize_t i, n = column.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef bint all_valid = True
    cdef object row, elem, p, p_str, parts_list, joined

    memset(dst, 0, nbytes)

    # Build combined regex from one or more patterns
    if literal is None:
        return out
    if isinstance(literal, (list, tuple)):
        parts_list = []
        for p in literal:
            if p is not None:
                p_str = p.decode('utf-8') if isinstance(p, bytes) else p
                parts_list.append('(?:' + sql_like_to_regex(p_str, case_sensitive=True) + ')')
        if not parts_list:
            return out
        joined = '|'.join(parts_list)
        regex = re.compile(joined)
    else:
        p_str = literal.decode('utf-8') if isinstance(literal, bytes) else literal
        regex = re.compile(sql_like_to_regex(p_str, case_sensitive=True))

    if nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for i in range(n):
        row = column[i]
        if row is None:
            all_valid = False
            continue
        if out_null != NULL:
            out_null[i >> 3] |= (<uint8_t>1 << (i & 7))
        for elem in row:
            if elem is not None:
                if isinstance(elem, bytes):
                    elem = elem.decode('utf-8')
                if regex.match(elem) is not None:
                    dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                    break

    if all_valid:
        if out_null != NULL:
            free(out_null)
        out.ptr.null_bitmap = NULL
    else:
        out.ptr.null_bitmap = out_null
    return out


cpdef BoolVector vector_anyop_ilike(object literal, ArrayVector column):
    """
    Draken '<column array> ILIKE ANY (patterns)' — case-insensitive version of vector_anyop_like.

    Parameters:
        literal: SQL LIKE pattern (str/bytes) or list/tuple of patterns.
                 None entries in a list are ignored (SQL NULL semantics).
        column: ArrayVector where each row is a list of string/bytes elements.

    Returns:
        BoolVector: True where ANY element × ANY pattern matches (case-insensitive).
                    Null bitmap set for rows where the column value is NULL.
    """
    import re
    from opteryx.utils.sql import sql_like_to_regex

    cdef Py_ssize_t i, n = column.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef bint all_valid = True
    cdef object row, elem, p, p_str2, parts_list2, joined2

    memset(dst, 0, nbytes)

    # Build combined regex (case-insensitive) from one or more patterns
    if literal is None:
        return out
    if isinstance(literal, (list, tuple)):
        parts_list2 = []
        for p in literal:
            if p is not None:
                p_str2 = p.decode('utf-8') if isinstance(p, bytes) else p
                parts_list2.append('(?:' + sql_like_to_regex(p_str2, case_sensitive=True) + ')')
        if not parts_list2:
            return out
        joined2 = '|'.join(parts_list2)
        regex = re.compile(joined2, re.IGNORECASE)
    else:
        p_str2 = literal.decode('utf-8') if isinstance(literal, bytes) else literal
        regex = re.compile(sql_like_to_regex(p_str2, case_sensitive=True), re.IGNORECASE)

    if nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    for i in range(n):
        row = column[i]
        if row is None:
            all_valid = False
            continue
        if out_null != NULL:
            out_null[i >> 3] |= (<uint8_t>1 << (i & 7))
        for elem in row:
            if elem is not None:
                if isinstance(elem, bytes):
                    elem = elem.decode('utf-8')
                if regex.match(elem) is not None:
                    dst[i >> 3] |= (<uint8_t>1 << (i & 7))
                    break

    if all_valid:
        if out_null != NULL:
            free(out_null)
        out.ptr.null_bitmap = NULL
    else:
        out.ptr.null_bitmap = out_null
    return out
