# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Decode files from a raw binary format to a PyArrow Table.
"""

from enum import Enum
from typing import BinaryIO
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import pyarrow
from orso.tools import random_string
from pyarrow import parquet

import opteryx.rugo.parquet as parquet_meta
from opteryx.compiled.structures.memory_view_stream import MemoryViewStream
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.rugo.converters.orso import rugo_to_orso_schema
from opteryx.utils.arrow import post_read_projector


class ExtentionType(str, Enum):
    """labels for the file extentions"""

    DATA = "DATA"
    CONTROL = "CONTROL"


def convert_arrow_schema_to_orso_schema(
    arrow_schema, row_count_metric: Optional[int] = None, row_count_estimate: Optional[int] = None
):
    from orso.schema import FlatColumn
    from orso.schema import RelationSchema

    return RelationSchema(
        name="arrow",
        row_count_metric=row_count_metric,
        row_count_estimate=row_count_estimate,
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )


def get_decoder(dataset: str) -> Callable:
    """helper routine to get the decoder for a given file"""
    ext = dataset.rpartition(".")[2].lower()
    file_decoder, file_type = KNOWN_EXTENSIONS.get(ext, (None, None))
    if file_type is None:
        raise UnsupportedFileTypeError(f"Unsupported file type: {ext}")
    if file_type != ExtentionType.DATA:  # pragma: no cover
        return do_nothing
    return file_decoder


def do_nothing(buffer: Union[memoryview, bytes], **kwargs):  # pragma: no cover
    """for when you need to look like you're doing something"""
    return None


def filter_records(filters: Optional[list], table: pyarrow.Table) -> pyarrow.Table:
    """
    Apply filters to a PyArrow table that could not be pushed down during the read operation.
    This is a post-read filtering step.

    Parameters:
        filters: Optional[list]
            A list of filter conditions (predicates) to apply to the table.
        table: pyarrow.Table
            The PyArrow table to be filtered.

    Returns:
        pyarrow.Table:
            A new PyArrow table with rows filtered according to the specified conditions.

    Note:
        At this point the columns are the raw column names from the file so we need to ensure
        the filters reference the raw column names not the engine internal 'identity'=
    """
    from opteryx.managers.expression import evaluate
    from opteryx.models import Node

    if isinstance(filters, list) and filters:
        # Create a copy of the filters list to avoid mutating the original.
        filter_copy = [f.copy() for f in filters]
        root = filter_copy.pop()

        # If the left or right side of the root filter node is an identifier, set its identity.
        # This step ensures that the filtering logic aligns with the schema before any renaming.
        if root.left.node_type == NodeType.IDENTIFIER:
            root.left.schema_column.identity = root.left.source_column
        if root.right.node_type == NodeType.IDENTIFIER:
            root.right.schema_column.identity = root.right.source_column

        while filter_copy:
            right = filter_copy.pop()
            if right.left.node_type == NodeType.IDENTIFIER:
                right.left.schema_column.identity = right.left.source_column
            if right.right.node_type == NodeType.IDENTIFIER:
                right.right.schema_column.identity = right.right.source_column
            # Combine the current root with the next filter using an AND node.
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
    else:
        root = filters

    mask = evaluate(root, table)
    return table.filter(mask)


def parquet_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    force_read: bool = False,
    use_threads: bool = True,
) -> Tuple[int, int, pyarrow.Table]:
    """
    Read parquet formatted files.

    Parameters:
        buffer: Union[memoryview, bytes]
            The input buffer containing the parquet file data.
        projection: List, optional
            List of columns to project.
        selection: optional
            The selection filter.
        just_schema: bool, optional
            Flag to indicate if only schema is needed.
        force_read: bool, optional
            Flag to skip some optimizations.
    Returns:
        Tuple containing number of rows, number of columns, and the table or schema.
    """
    # Return just the schema if that's all that's needed
    # We can use rugo's metadata reader which is faster than pyarrow's
    if just_schema:
        if isinstance(buffer, memoryview):
            metadata = parquet_meta.read_metadata_from_memoryview(
                buffer, schema_only=True, max_row_groups=1, include_statistics=False
            )
        else:
            metadata = parquet_meta.read_metadata_from_memoryview(
                memoryview(buffer), schema_only=True, max_row_groups=1, include_statistics=False
            )
        return rugo_to_orso_schema(metadata, "parquet")

    # Use rugo's lightweight metadata reader first (faster than pyarrow)
    if isinstance(buffer, memoryview):
        rmeta = parquet_meta.read_metadata_from_memoryview(buffer)
    else:
        rmeta = parquet_meta.read_metadata_from_memoryview(memoryview(buffer))

    # Build the pieces we need from the rugo metadata
    # schema names (parquet has same columns across row groups usually)
    if rmeta.get("row_groups"):
        schema_names = [c["name"] for c in rmeta["row_groups"][0]["columns"]]
    else:
        schema_names = []

    num_rows = rmeta.get("num_rows")
    # number of columns - try to derive, fallback to length of schema_names
    num_columns = rmeta.get("num_columns") or len(schema_names)

    # total uncompressed size (rugo uses total_byte_size)
    uncompressed_size = sum(
        sum(col.get("total_byte_size", 0) for col in rg.get("columns", []))
        for rg in rmeta.get("row_groups", [])
    )

    # we need to work out if we have a selection which may force us
    # fetching columns just for filtering
    dnf_filter, processed_selection = (
        PredicatePushable.to_dnf(selection) if selection else (None, None)
    )

    # Determine the columns needed for projection and filtering
    projection_set = set(p.source_column for p in projection or [])
    filter_columns = {
        c.value for c in get_all_nodes_of_type(processed_selection, (NodeType.IDENTIFIER,))
    }
    selected_columns = list(projection_set.union(filter_columns).intersection(schema_names))

    # Read all columns if none are selected, unless force_read is set
    if not selected_columns and not force_read:
        selected_columns = []

    # Open the parquet file only once. Fake a file-like object around the buffer
    if isinstance(buffer, memoryview):
        buffer = MemoryViewStream(buffer)

    # Read the parquet table with the optimized column list and selection filters
    table = parquet.read_table(
        buffer,
        columns=selected_columns,
        pre_buffer=True,
        filters=dnf_filter,
        use_threads=use_threads,
        use_pandas_metadata=False,
    )

    # Any filters we couldn't push to PyArrow to read we run here
    if processed_selection:
        table = filter_records(processed_selection, table)

    return (
        num_rows,
        num_columns,
        uncompressed_size,
        table,
    )


def convert_string_view(field: pyarrow.Field) -> pyarrow.Field:
    """
    Recursively replace string_view (type id 39) with string in a pyarrow Field.
    """
    field_type = field.type

    if field_type.id == pyarrow.string_view().id:
        return pyarrow.field(field.name, pyarrow.string(), nullable=field.nullable)

    elif pyarrow.types.is_list(field_type) or pyarrow.types.is_large_list(field_type):
        converted_value_field = convert_string_view(pyarrow.field("item", field_type.value_type))
        if pyarrow.types.is_list(field_type):
            new_type = pyarrow.list_(converted_value_field.type)
        else:
            new_type = pyarrow.large_list(converted_value_field.type)
        return pyarrow.field(field.name, new_type, nullable=field.nullable)

    elif pyarrow.types.is_struct(field_type):
        new_fields = [convert_string_view(subfield) for subfield in field_type]
        new_type = pyarrow.struct(new_fields)
        return pyarrow.field(field.name, new_type, nullable=field.nullable)

    return field


def vortex_decoder(
    buffer: Union[memoryview, bytes, BinaryIO],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    try:
        import vortex
    except ImportError:
        from opteryx.exceptions import MissingDependencyError

        raise MissingDependencyError("vortex-data")

    import os
    import tempfile

    # Current version of vortex appears to not be able to read streams
    # this is painfully slow, don't do this.
    # Convert buffer to bytes if needed
    if isinstance(buffer, memoryview):
        buffer_bytes = buffer.tobytes()
    elif isinstance(buffer, bytes):
        buffer_bytes = buffer
    else:
        buffer_bytes = buffer.read()

    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as f:
        f.write(buffer_bytes)
        f.flush()
        tmp_name = f.name
    try:
        table = vortex.open(tmp_name)
    finally:
        os.remove(tmp_name)

    if just_schema:
        arrow_schema = table.to_arrow().schema
        orso_schema = convert_arrow_schema_to_orso_schema(arrow_schema)
        return orso_schema

    # COUNT(*) fast-path disabled — fall through to a normal table read.

    # we currently aren't pushing filters into vortex so we need to read
    # the columns we're filtering by
    projection_set = set(p.source_column for p in projection or [])
    filter_columns = {c.value for c in get_all_nodes_of_type(selection, (NodeType.IDENTIFIER,))}
    selected_columns = list(projection_set.union(filter_columns))

    # convert to pyarrow table
    table = table.to_arrow(projection=selected_columns).read_all()
    shape = table.shape

    # string views aren't properly supported - only ever seen them in vortex files
    new_schema = pyarrow.schema([convert_string_view(field) for field in table.schema])
    table = table.cast(new_schema)

    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)

    return *shape, 0, table


def jsonl_decoder(
    buffer: Union[memoryview, bytes, BinaryIO],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    # rugo is our own library for fast jsonl reading
    from orso.schema import convert_orso_schema_to_arrow_schema

    import opteryx.rugo.jsonl as rj
    from opteryx.rugo.converters.orso import jsonl_to_orso_schema
    from opteryx.utils import count_instances

    if not isinstance(buffer, memoryview):
        buffer = memoryview(buffer)

    # count newline occurrences in the provided buffer to get the number of rows
    num_rows = count_instances(buffer)

    # COUNT(*) fast-path disabled — fall through to a normal table read.

    orso_schema = jsonl_to_orso_schema(rj.get_jsonl_schema(buffer))

    if just_schema:
        return orso_schema

    # Determine the columns needed for projection and filtering
    projection_set = set(p.source_column for p in projection or [])
    filter_columns = {c.value for c in get_all_nodes_of_type(selection, (NodeType.IDENTIFIER,))}
    selected_columns = list(
        projection_set.union(filter_columns).intersection(orso_schema.column_names)
    )

    table = rj.read_jsonl(
        buffer,
        columns=selected_columns,
        parse_objects=False,
    )

    # Convert the returned columns into PyArrow arrays using the Arrow schema
    # derived from the Orso schema. Doing this per-field ensures that Python
    # lists (JSON arrays) are converted into Arrow ListArray types and that
    # bytes/strings/etc. are coerced to the expected Arrow types. If a direct
    arrow_schema = convert_orso_schema_to_arrow_schema(orso_schema)

    arrays = []
    final_fields = []

    for idx, name in enumerate(table["column_names"]):
        field = arrow_schema.field(name)
        column = table["columns"][idx]
        if hasattr(column, "to_arrow"):
            # rugo returns draken vectors; convert to pyarrow arrays
            arrays.append(column.to_arrow())
        else:
            # fallback: convert using pyarrow array constructor
            arrays.append(pyarrow.array(column, type=field.type))
        final_fields.append(field)

    final_schema = pyarrow.schema(final_fields)
    arrow_table = pyarrow.Table.from_arrays(arrays, schema=final_schema)

    if selection:
        arrow_table = filter_records(selection, arrow_table)
    if projection:
        arrow_table = post_read_projector(arrow_table, projection)

    return num_rows, len(table["column_names"]), len(buffer), arrow_table


# for types we know about, set up how we handle them
KNOWN_EXTENSIONS: Dict[str, Tuple[Callable, str]] = {
    "complete": (do_nothing, ExtentionType.CONTROL),
    "manifest": (do_nothing, ExtentionType.CONTROL),
    "ignore": (do_nothing, ExtentionType.CONTROL),
    "jsonl": (jsonl_decoder, ExtentionType.DATA),
    "parquet": (parquet_decoder, ExtentionType.DATA),
    "vortex": (vortex_decoder, ExtentionType.DATA),
}

VALID_EXTENSIONS = set(f".{ext}" for ext in KNOWN_EXTENSIONS)
TUPLE_OF_VALID_EXTENSIONS = tuple(VALID_EXTENSIONS)
DATA_EXTENSIONS = set(
    f".{ext}" for ext, conf in KNOWN_EXTENSIONS.items() if conf[1] == ExtentionType.DATA
)
