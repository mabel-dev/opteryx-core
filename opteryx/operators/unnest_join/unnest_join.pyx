# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Unnest Join Node

This is a SQL Query Execution Plan Node.

This implements a CROSS JOIN UNNEST, this isn't really a JOIN in that it doesn't join two tables
together, but it does unnest a column in a table and repeat the rows in the table for each value.

Draken-native implementation (no PyArrow).
"""

from typing import Generator, Set, Tuple

from libc.stdint cimport int32_t, int64_t, uint8_t, uint64_t
from libcpp.vector cimport vector as cppvector
from cpython.object cimport PyObject_Hash

from draken.core.buffers cimport DrakenArrayBuffer
from draken.vectors.vector cimport Vector
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.int64_vector cimport from_sequence as int64_from_sequence

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, QueryProperties
from opteryx.types.schema import FlatColumn

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.

INTERNAL_BATCH_SIZE: int = 10000


cdef inline bint _array_row_is_null(DrakenArrayBuffer* ptr, Py_ssize_t idx) nogil:
    """Per-row null check on an ArrayVector buffer."""
    if ptr.null_bitmap == NULL:
        return False
    return ((ptr.null_bitmap[idx >> 3] >> (idx & 7)) & 1) == 0


cpdef tuple build_rows_indices_and_column_draken(ArrayVector column_vector):
    """Build row indices and flattened column data from ARRAY column (Draken-native).

    Walks the array offsets directly: no per-row Python list materialization,
    no intermediate Python list for the flat data.

    Returns:
        tuple of (Int64Vector, Draken vector) — row indices and flattened typed data
    """
    cdef DrakenArrayBuffer* ptr = column_vector.ptr
    cdef Vector child = <Vector>column_vector._child
    cdef Py_ssize_t row_count = <Py_ssize_t>ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end, run_len, k
    cdef const int32_t* offsets = ptr.offsets
    cdef IntBuffer indices_buf
    cdef cppvector[int32_t] child_idx_vec

    # First pass: reserve once based on the total non-null span.
    cdef Py_ssize_t total_size = 0
    for i in range(row_count):
        if _array_row_is_null(ptr, i):
            continue
        total_size += <Py_ssize_t>(offsets[i + 1] - offsets[i])

    if total_size == 0:
        return (int64_from_sequence(None), vector_from_sequence([]))

    indices_buf = IntBuffer(<size_t>total_size)
    child_idx_vec.reserve(<size_t>total_size)

    for i in range(row_count):
        if _array_row_is_null(ptr, i):
            continue
        start = offsets[i]
        end = offsets[i + 1]
        run_len = end - start
        if run_len <= 0:
            continue
        indices_buf.append_repeated(i, <size_t>run_len)
        for k in range(start, end):
            child_idx_vec.push_back(k)

    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    cdef const int32_t[::1] child_idx_view = <const int32_t[:total_size]>child_idx_vec.data()
    flat = child.take(child_idx_view)

    return (vec, flat)


cpdef tuple build_filtered_rows_indices_and_column_draken(ArrayVector column_vector, set valid_values):
    """Build row indices and flattened column data for filtered ARRAY column (Draken-native).

    Walks the array offsets directly; child elements are materialized one at a
    time only to test membership in `valid_values`. The output flat vector is
    built via `child.take(...)` — no intermediate Python list.
    """
    cdef DrakenArrayBuffer* ptr = column_vector.ptr
    cdef Vector child = <Vector>column_vector._child
    cdef Py_ssize_t row_count = <Py_ssize_t>ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end, k
    cdef const int32_t* offsets = ptr.offsets
    cdef IntBuffer indices_buf = IntBuffer(<size_t>row_count)
    cdef cppvector[int32_t] child_idx_vec
    cdef object val

    for i in range(row_count):
        if _array_row_is_null(ptr, i):
            continue
        start = offsets[i]
        end = offsets[i + 1]
        for k in range(start, end):
            val = child[k]
            if val in valid_values:
                indices_buf.append(i)
                child_idx_vec.push_back(k)

    cdef Py_ssize_t total_matched = <Py_ssize_t>child_idx_vec.size()
    if total_matched == 0:
        return (int64_from_sequence(None), vector_from_sequence([]))

    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    cdef const int32_t[::1] child_idx_view = <const int32_t[:total_matched]>child_idx_vec.data()
    flat = child.take(child_idx_view)

    return (vec, flat)


cpdef tuple list_distinct(Vector values, Int64Vector indices, CarcharSetWrapper seen_hashes=None):
    """
    Filter duplicates from values using hash-based deduplication (Draken-native).

    Args:
        values:       Draken Vector of values
        indices:      Int64Vector of row indices
        seen_hashes:  CarcharSetWrapper to track seen hash values (shared across calls)

    Returns:
        tuple of (deduplicated_values, deduplicated_indices_vector, seen_hashes)
    """
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t n = len(values)
    cdef uint64_t hash_value
    cdef object v
    cdef list new_values_list = []
    cdef list new_indices_list = []

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for i in range(n):
        v = values[i]
        hash_value = <uint64_t>(PyObject_Hash(v) & 0xFFFFFFFFFFFFFFFF)
        if seen_hashes.insert(hash_value):
            new_values_list.append(v)
            new_indices_list.append(indices[i])

    return vector_from_sequence(new_values_list), vector_from_sequence(new_indices_list), seen_hashes


def _cross_join_unnest_column(
    *,
    morsel: Morsel = None,
    source: LogicalColumn = None,
    target_column: FlatColumn = None,
    conditions: Set = None,
    distinct: bool = False,
    single_column: bool = False,
    hash_set=None,
) -> Generator[Morsel, None, None]:
    """
    Perform a cross join on an unnested column of Draken morsels (Draken-native, no PyArrow).

    Args:
        morsel: A `Morsel` object to be unnested.
        source: The source node indicating the column.
        target_column: The column to be unnested.
        conditions: Optional set of valid values to filter by.
        distinct: Whether to deduplicate results.
        single_column: Whether the output is only the unnested column.
        hash_set: Optional hash set for deduplication.

    Yields:
        Morsel objects with unnested rows.
    """
    if morsel.num_rows == 0:
        return

    # Get the column to unnest as a Draken vector; identity is bytes
    column_identity = source.schema_column.identity
    column_vector = morsel.column(column_identity)

    # Build row indices and flattened column data using Draken-native helpers
    if conditions is None:
        indices, flattened_data = build_rows_indices_and_column_draken(column_vector)
    else:
        indices, flattened_data = build_filtered_rows_indices_and_column_draken(
            column_vector, conditions
        )

    # Handle deduplication if requested
    if single_column and distinct and indices.length > 0:
        flattened_data, indices, hash_set = list_distinct(flattened_data, indices, hash_set)

    # If no results after filtering/deduplication, yield empty morsel
    if indices.length == 0:
        return

    # Convert indices to Python list for take() operation
    row_indices = indices.to_pylist()

    # Expand the morsel by repeating rows according to indices
    expanded_morsel = morsel.take(row_indices)

    # Append the unnested column to the expanded morsel
    expanded_morsel.append_vector(target_column.identity, flattened_data)

    yield expanded_morsel


def _cross_join_unnest_literal(
    morsel: Morsel, source: Tuple, target_column: FlatColumn
) -> Generator[Morsel, None, None]:
    """
    Perform a cross join with a literal (constant) unnest array (Draken-native, no PyArrow).

    Args:
        morsel: A `Morsel` object to be cross-joined.
        source: A tuple of literal values to unnest.
        target_column: The column to hold the unnested values.

    Yields:
        Morsel objects with unnested literal values repeated.
    """
    from draken.interop.vector_sequence import vector_from_sequence

    if morsel.num_rows == 0:
        return

    joined_list_size = len(source)
    block_size = morsel.num_rows

    # Build repeated row indices: each row repeated joined_list_size times
    repeated_indices = []
    for i in range(block_size):
        for _ in range(joined_list_size):
            repeated_indices.append(i)

    # Expand the morsel by repeating rows
    expanded_morsel = morsel.take(repeated_indices)

    # Create tiled source data: repeat source for each original row
    tiled_source = []
    for _ in range(block_size):
        tiled_source.extend(source)

    # Convert to typed Draken vector
    unnest_vector = vector_from_sequence(tiled_source)

    # Append the unnested column
    expanded_morsel.append_vector(target_column.identity, unnest_vector)

    yield expanded_morsel


cdef class UnnestJoinNode(BasePlanNode):
    """
    Implements CROSS JOIN UNNEST (Draken-native, no PyArrow)
    """

    cdef public object left_readers
    cdef public object right_readers
    cdef public str join_type
    cdef public object _unnest_column
    cdef public object _unnest_target
    cdef public object _filters
    cdef public bint _distinct
    cdef public bint _single_column
    cdef public CarcharSetWrapper hash_set

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Initialize join interface (UnnestJoinNode is registered as a join node in catalog)
        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.join_type = "cross"

        # do we have unnest details?
        self._unnest_column = parameters.get("unnest_column")
        self._unnest_target = parameters.get("unnest_target").schema_column
        self._filters = parameters.get("filters")
        self._distinct = parameters.get("distinct", False)

        # handle variation in how the unnested column is represented
        if self._unnest_column.node_type == NodeType.NESTED:
            self._unnest_column = self._unnest_column.centre

        # if we have a literal that's not a tuple, wrap it
        if self._unnest_column.node_type == NodeType.LITERAL and not isinstance(
            self._unnest_column.value, tuple
        ):
            self._unnest_column.value = tuple([self._unnest_column.value])

        self._single_column = parameters.get("pre_update_columns", set()) == {
            self._unnest_target.identity,
        }

        self.hash_set = CarcharSetWrapper()

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        filters = ""
        if self._filters:
            filters = f"({self._unnest_target.name} IN ({', '.join(self._filters)}))"
        return f"CROSS JOIN {filters}"

    cpdef void _push_impl(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self.emit(_EOS_SENTINEL)
            return

        if isinstance(self._unnest_column.value, tuple):
            for chunk in _cross_join_unnest_literal(
                morsel=morsel,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
            ):
                self.emit(chunk)
            return

        for chunk in _cross_join_unnest_column(
            morsel=morsel,
            source=self._unnest_column,
            target_column=self._unnest_target,
            conditions=self._filters,
            hash_set=self.hash_set,
            distinct=self._distinct,
            single_column=self._single_column,
        ):
            self.emit(chunk)
