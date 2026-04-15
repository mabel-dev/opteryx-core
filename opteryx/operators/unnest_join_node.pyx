# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

"""
Unnest Join Node

This is a SQL Query Execution Plan Node.

This implements a CROSS JOIN UNNEST, this isn't really a JOIN in that it doesn't join two tables
together, but it does unnest a column in a table and repeat the rows in the table for each value.

Draken-native implementation (no PyArrow).
"""

from typing import Generator, Set, Tuple

from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, QueryProperties
from opteryx.types.schema import FlatColumn

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "draken"

INTERNAL_BATCH_SIZE: int = 10000


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
    from opteryx.compiled.joins import build_rows_indices_and_column_draken
    from opteryx.compiled.joins import build_filtered_rows_indices_and_column_draken
    from opteryx.compiled.joins import list_distinct

    if morsel.num_rows == 0:
        return

    # Get the column to unnest as a Draken vector
    column_identity = source.schema_column.identity
    if not isinstance(column_identity, bytes):
        column_identity = column_identity.encode('utf-8')
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
    from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence

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


class UnnestJoinNode(BasePlanNode):
    """
    Implements CROSS JOIN UNNEST (Draken-native, no PyArrow)
    """

    def __init__(self, properties: QueryProperties, **parameters):
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

    def execute(self, morsel):
        morsel = self.ensure_draken_morsel(morsel)

        if morsel == EOS:
            yield EOS
            return

        if isinstance(self._unnest_column.value, tuple):
            yield from _cross_join_unnest_literal(
                morsel=morsel,
                source=self._unnest_column.value,
                target_column=self._unnest_target,
            )
            return

        yield from _cross_join_unnest_column(
            morsel=morsel,
            source=self._unnest_column,
            target_column=self._unnest_target,
            conditions=self._filters,
            hash_set=self.hash_set,
            distinct=self._distinct,
            single_column=self._single_column,
        )
