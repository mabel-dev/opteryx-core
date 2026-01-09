# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Read Node

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader),
normalizes the data into the format for internal processing.
"""

import datetime
import time
from collections import defaultdict
from typing import Generator

import orjson
import pyarrow
from orso.schema import RelationSchema
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


def struct_to_jsonb(table: pyarrow.Table) -> pyarrow.Table:
    """
    Converts any STRUCT columns in a PyArrow Table to JSON strings and replaces them
    in the same column position.

    Parameters:
        table (pa.Table): The PyArrow Table to process.

    Returns:
        pa.Table: A new PyArrow Table with STRUCT columns converted to JSON strings.
    """
    for i in range(table.num_columns):
        field = table.schema.field(i)

        # Check if the column is a STRUCT
        if pyarrow.types.is_struct(field.type):
            # Convert each row in the STRUCT column to a JSON string
            json_array = pyarrow.array(
                [None if row is None else orjson.dumps(row) for row in table.column(i).to_pylist()],
                type=pyarrow.binary(),
            )

            # Drop the original STRUCT column
            table = table.drop_columns(field.name)

            # Insert the new JSON column at the same position
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=pyarrow.binary()), json_array
            )

        # Check for LIST<STRUCT>
        if pyarrow.types.is_list(field.type) and pyarrow.types.is_struct(field.type.value_type):
            list_array = table.column(i)

            # Convert each list element
            converted_data = []
            for item in list_array.to_pylist():
                if item is None:
                    converted_data.append(None)
                else:
                    # Each item is a list of structs
                    converted_list = []
                    for struct in item:
                        if struct is None:
                            converted_list.append(None)
                        else:
                            converted_list.append(orjson.dumps(struct))
                    converted_data.append(converted_list)

            # Build the new array
            jsonb_array = pyarrow.array(converted_data, type=pyarrow.list_(pyarrow.binary()))

            # Drop original column and insert new one at same position
            table = table.drop_columns(field.name)
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=jsonb_array.type), jsonb_array
            )

    return table


def normalize_morsel(schema: RelationSchema, morsel: pyarrow.Table) -> pyarrow.Table:
    if morsel.column_names == ["$COUNT(*)"]:
        return morsel
    if len(schema.columns) == 0 and morsel.column_names != ["*"]:
        one_column = pyarrow.array([True] * morsel.num_rows, type=pyarrow.bool_())
        morsel = morsel.append_column("*", one_column)
        return morsel.select(["*"])

    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = set()

    # Find which columns to drop and which columns we already have
    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.add(i)
        else:
            target_column_names.append(str(column_name))

    # Remove from the end otherwise we'll remove the wrong columns after we've removed one
    if droppable_columns:
        keep_indices = [i for i in range(len(morsel.columns)) if i not in droppable_columns]
        morsel = morsel.select(keep_indices)

    # remane columns to the internal names (identities)
    morsel = morsel.rename_columns(target_column_names)

    # add columns we don't have, populate with nulls but try to get the correct type
    for column in schema.columns:
        if column.identity not in target_column_names:
            null_column = pyarrow.nulls(morsel.num_rows, type=column.arrow_field.type)
            field = pyarrow.field(name=column.identity, type=column.arrow_field.type)
            morsel = morsel.append_column(field, null_column)

    # ensure the columns are in the right order
    return morsel.select([col.identity for col in schema.columns])


def merge_schemas(
    hypothetical_schema: RelationSchema, observed_schema: pyarrow.Schema
) -> pyarrow.schema:
    """
    Using the hypothetical schema as the base, replace with fields from the observed schema
    which are a Decimal type.
    """
    # convert the Orso schema to an Arrow schema
    hypothetical_arrow_schema = convert_orso_schema_to_arrow_schema(hypothetical_schema, True)

    # Convert the hypothetical schema to a dictionary for easy modification
    schema_dict = {field.name: field for field in hypothetical_arrow_schema}

    # Iterate through fields in the observed schema
    for observed_field in observed_schema:
        # Check if the field is of type Decimal or List/Array
        if pyarrow.types.is_decimal(observed_field.type) or pyarrow.types.is_list(
            observed_field.type
        ):
            # Replace or add the field to the schema dictionary
            schema_dict[observed_field.name] = observed_field

    # Create a new schema from the updated dictionary of fields
    merged_schema = pyarrow.schema(list(schema_dict.values()))

    return merged_schema


class ReaderNode(BasePlanNode):
    is_scan = True

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.uuid = parameters.get("uuid")
        self.at_date = parameters.get("at_date")
        self.dataset_committed_at = parameters.get("dataset_committed_at")
        self.hints = parameters.get("hints", [])
        self.columns = parameters.get("columns", [])
        self.predicates = parameters.get("predicates", [])
        self.manifest = parameters.get("manifest", [])

        self.connector = parameters.get("connector")
        self.schema = parameters.get("schema")
        self.limit = parameters.get("limit")

        if len(self.hints) != 0:
            self.telemetry.add_message("All HINTS are currently ignored")

        self.telemetry.rows_read += 0
        self.telemetry.columns_read += 0

    def to_mermaid(self, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        if self.connector is None:
            mermaid = f'NODE_{nid}[("**{self.node_type.upper()} (FUNCTION)**<br />'
            mermaid += f"{self.function}<br />"
        else:
            mermaid = f'NODE_{nid}[("**READ**<br />'
            mermaid += f"{self.connector.dataset}<br />"

        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Read"

    def sensors(self):
        base = super().sensors()
        base["committed_at"] = (
            str(datetime.datetime.fromtimestamp(self.dataset_committed_at / 1000))
            if self.dataset_committed_at
            else None
        )
        base["at_date"] = str(self.at_date) if self.at_date else None
        base["limit"] = self.limit
        base["predicates"] = len(self.predicates) if self.predicates else 0
        return base

    @property
    def config(self):
        """Additional details for this step"""
        date_range = ""
        if self.at_date:
            date_range = f" AT ('{self.at_date}')"
        return (
            f"{self.connector.__type__} "
            f"({self.parameters.get('relation')}"
            f"{' AS ' + self.parameters.get('alias') if self.parameters.get('alias') else ''}"
            f"{date_range}"
            f"{' WITH(' + ','.join(self.parameters.get('hints')) + ')' if self.parameters.get('hints') else ''})"
        )

    def plan_config(self) -> dict:
        """
        Structured configuration for planning/telemetry purposes.

        Returns a dict containing:
          - files: list of {file_path, rows, bytes}
          - selection_pushdown: predicates (simple repr)
          - projection_pushdown: list of projected column identities/names
          - connector: connector type
          - relation: dataset name
        """
        config = {
            "connector": getattr(self.connector, "__type__", None),
            "relation": self.parameters.get("relation"),
            "files": [],
        }

        # Projection pushdown: provide schema index and column name for each projected column
        proj = []

        schema_columns = getattr(self.schema, "columns", []) or []
        columns_to_read = []
        for c in self.columns or []:
            # use the column identity (internal identity) as the column_name
            identity = c.schema_column.identity
            schema_index = None
            for idx, sc in enumerate(schema_columns):
                if getattr(sc, "identity", None) == identity:
                    columns_to_read.append(idx)
                    schema_index = idx
                    break
            proj.append({"schema-index": schema_index, "column-name": identity})

        # Initialize column bytes accumulator (uncompressed) for projected columns
        # Projection pushdown: provide schema index and column name for each projected column
        proj = []

        schema_columns = getattr(self.schema, "columns", []) or []
        if len(self.columns) == 0:
            for idx, c in enumerate(self.columns or []):
                # use the column identity (internal identity) as the column_name
                identity = c.schema_column.identity
                column_name = c.schema_column.name
                proj.append(
                    {"schema-index": idx, "column-identity": identity, "column-name": column_name}
                )
        else:
            columns_to_read = []
            for c in self.columns or []:
                # use the column identity (internal identity) as the column_name
                identity = c.schema_column.identity
                column_name = c.schema_column.name
                schema_index = None
                for idx, sc in enumerate(schema_columns):
                    if sc.identity == identity:
                        columns_to_read.append(idx)
                        schema_index = idx
                        break
                proj.append(
                    {
                        "schema-index": schema_index,
                        "column-identity": identity,
                        "column-name": column_name,
                    }
                )

        # Initialize column bytes accumulator (uncompressed) and completeness flags
        column_bytes_totals = defaultdict(int)
        column_bytes_complete = defaultdict(lambda: True)
        config["projection"] = proj

        # If a manifest is attached, prefer its file entries
        manifest = self.manifest
        if manifest is not None:
            # manifest.files contains FileEntry objects
            for f in manifest.files:
                file_entry = {"path": f.file_path}
                # only include rows if known
                if getattr(f, "record_count", None) is not None:
                    file_entry["rows"] = f.record_count
                # include uncompressed bytes only when present (do not fall back)
                if getattr(f, "uncompressed_size_in_bytes", None) is not None:
                    file_entry["bytes"] = f.uncompressed_size_in_bytes

                col_sizes = getattr(f, "column_uncompressed_sizes_in_bytes", None)

                # Per-file column statistics for projected columns (when available)
                if proj and col_sizes:
                    for p in proj:
                        si = p.get("schema-index")
                        if si is None:
                            continue

                        if (
                            col_sizes
                            and isinstance(col_sizes, (list, tuple))
                            and si < len(col_sizes)
                            and col_sizes[si] is not None
                        ):
                            # Accumulate total uncompressed bytes for this projected column
                            column_bytes_totals[si] += col_sizes[si]
                        else:
                            # Missing column size for this file/column -> mark incomplete
                            column_bytes_complete[si] = False

                config["files"].append(file_entry)

            # After processing files, attach accumulated uncompressed bytes to projection entries
            for p in proj:
                schema_index = p.get("schema-index")
                if column_bytes_complete[schema_index]:
                    p["total-bytes"] = column_bytes_totals.get(schema_index, 0)

        # Selection pushdown: represent predicates simply
        try:
            config["predicates"] = [str(p) for p in (self.predicates or [])]
        except Exception:
            config["predicates"] = []

        # Summary: aggregate totals for files/rows/bytes when available
        total_files = len(config["files"])
        # If any file lacks rows/bytes info, mark totals as None
        total_rows = None
        total_bytes = None
        if total_files == 0:
            total_rows = 0
            total_bytes = 0
        else:
            # all files must have the key and a non-None value to be considered known
            rows_known = all(("rows" in f and f["rows"] is not None for f in config["files"]))
            bytes_known = all(("bytes" in f and f["bytes"] is not None for f in config["files"]))
            if rows_known:
                total_rows = sum((f["rows"] for f in config["files"]))
            if bytes_known:
                total_bytes = sum((f["bytes"] for f in config["files"]))

        # Determine total-column-bytes only when all projected columns were complete
        total_column_bytes = None
        if proj and all(column_bytes_complete.values()):
            total_column_bytes = sum(column_bytes_totals.values())

        config["summary"] = {
            "total-files": total_files,
            "total-rows": total_rows,
            "total-files-bytes": total_bytes,
            "total-column-bytes": total_column_bytes,
        }

        return config

    def execute(self, morsel, **kwargs) -> Generator:
        """Perform this step, time how long is spent doing work"""
        if morsel == EOS:
            yield None
            return

        morsel = None
        orso_schema = self.schema
        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols
        arrow_schema = None
        start_clock = time.monotonic_ns()
        reader = self.connector.read_dataset(
            columns=self.columns,
            predicates=self.predicates,
        )

        records_to_read = self.limit if self.limit is not None else float("inf")

        for morsel in reader:
            # try to make each morsel have the same schema

            if records_to_read < morsel.num_rows:
                morsel = morsel.slice(0, records_to_read)
                records_to_read = 0
            else:
                records_to_read -= morsel.num_rows

            morsel = struct_to_jsonb(morsel)
            morsel = normalize_morsel(orso_schema, morsel)
            if arrow_schema is None:
                arrow_schema = merge_schemas(self.schema, morsel.schema)
            if arrow_schema.names:
                morsel = morsel.cast(arrow_schema)

            self.telemetry.time_reading_blobs += time.monotonic_ns() - start_clock
            self.telemetry.blobs_read += 1
            self.telemetry.rows_read += morsel.num_rows
            self.telemetry.bytes_processed += morsel.nbytes
            yield morsel
            start_clock = time.monotonic_ns()

            if records_to_read <= 0:
                break

        if morsel:
            self.telemetry.columns_read += morsel.num_columns
        else:
            self.telemetry.columns_read += len(orso_schema.columns)
