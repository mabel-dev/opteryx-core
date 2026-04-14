opteryx-core/opteryx/compiled/joins/filter_join.pyx
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport int64_t, uint64_t

from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer


cpdef CarcharSetWrapper filter_join_set(table, list columns=None, CarcharSetWrapper seen_hashes=None):
    """
    Build or extend a CarcharSetWrapper from the rows of `table` (or Morsel).

    Uses `Morsel.hash(...)` (Draken-native) for per-row hashing. If `table`
    is a PyArrow table, it is converted to a `Morsel` and hashed there.

    Returns the updated `seen_hashes` (created if None).
    """
    cdef Py_ssize_t num_rows = table.num_rows
    cdef list columns_of_interest = columns if columns else table.column_names
    cdef uint64_t[::1] row_hashes
    cdef Py_ssize_t row_idx
    cdef Morsel _m

    if isinstance(table, Morsel):
        row_hashes = table.hash(columns_of_interest)
    else:
        _m = Morsel.from_arrow(table)
        row_hashes = _m.hash(columns_of_interest)

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for row_idx in range(num_rows):
        seen_hashes.insert(row_hashes[row_idx])

    return seen_hashes


cpdef semi_join(object relation, list join_columns, CarcharSetWrapper seen_hashes):
    """
    Return rows from `relation` where the join key (hashed via Morsel.hash)
    exists in `seen_hashes`. `relation` may be a PyArrow table or a Morsel.
    """
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef Py_ssize_t row_idx
    cdef IntBuffer index_buffer = IntBuffer(num_rows)
    cdef uint64_t[::1] row_hashes
    cdef Morsel _m

    if isinstance(relation, Morsel):
        row_hashes = relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(relation)
        row_hashes = _m.hash(join_columns)

    for row_idx in range(num_rows):
        if seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)


cpdef anti_join(object relation, list join_columns, CarcharSetWrapper seen_hashes):
    """
    Return rows from `relation` where the join key (hashed via Morsel.hash)
    does NOT exist in `seen_hashes`. `relation` may be a PyArrow table or a Morsel.
    """
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef Py_ssize_t row_idx
    cdef IntBuffer index_buffer = IntBuffer(num_rows)
    cdef uint64_t[::1] row_hashes
    cdef Morsel _m

    if isinstance(relation, Morsel):
        row_hashes = relation.hash(join_columns)
    else:
        _m = Morsel.from_arrow(relation)
        row_hashes = _m.hash(join_columns)

    for row_idx in range(num_rows):
        if not seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)
