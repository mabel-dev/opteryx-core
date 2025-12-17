# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Iceberg Connector
"""

import datetime
import struct
from decimal import Decimal
from typing import Dict
from typing import List
from typing import Union

import numpy
import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors import GcpCloudStorageConnector
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Asynchronous
from opteryx.connectors.capabilities import Diachronic
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import RelationStatistics
from opteryx.utils.file_decoders import filter_records


@single_item_cache
def to_iceberg_filter(root):
    """
    Convert a filter to Iceberg filter form.

    This is specifically opinionated for the Iceberg reader.
    """
    import pyiceberg
    import pyiceberg.expressions

    ICEBERG_FILTERS = {
        "GtEq": pyiceberg.expressions.GreaterThanOrEqual,
        "Eq": pyiceberg.expressions.EqualTo,
        "Gt": pyiceberg.expressions.GreaterThan,
        "Lt": pyiceberg.expressions.LessThan,
        "LtEq": pyiceberg.expressions.LessThanOrEqual,
        "NotEq": pyiceberg.expressions.NotEqualTo,
    }

    def _predicate_to_iceberg_filter(root):
        # Reduce look-ahead effort by using Exceptions to control flow
        if root.node_type == NodeType.AND:  # pragma: no cover
            left = _predicate_to_iceberg_filter(root.left)
            right = _predicate_to_iceberg_filter(root.right)
            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]
            left.extend(right)
            return left
        if root.node_type != NodeType.COMPARISON_OPERATOR:
            raise NotSupportedError()

        left_node = root.left
        right_node = root.right

        if left_node.node_type != NodeType.IDENTIFIER:
            left_node, right_node = right_node, left_node

        right_value = right_node.value
        right_type = right_node.schema_column.type
        left_type = left_node.schema_column.type

        if right_type == OrsoTypes.DATE:
            date_val = right_value
            if hasattr(date_val, "item"):
                date_val = date_val.item()
            right_value = datetime.datetime.combine(date_val, datetime.time.min)
            right_type = OrsoTypes.TIMESTAMP
        if left_type == OrsoTypes.DATE:
            left_type = OrsoTypes.TIMESTAMP
        if left_node.node_type != NodeType.IDENTIFIER:
            raise NotSupportedError()
        if right_node.node_type != NodeType.LITERAL:
            raise NotSupportedError()
        if left_type == OrsoTypes.VARCHAR:
            left_type = OrsoTypes.BLOB
        if right_type == OrsoTypes.VARCHAR:
            right_type = OrsoTypes.BLOB
        if right_type != left_type:
            raise NotSupportedError(f"{right_type} != {left_type}")
        if right_type == OrsoTypes.DOUBLE:
            # iceberg needs doubles to be cast to floats
            right_value = float(right_value)
        if right_type == OrsoTypes.INTEGER:
            # iceberg doesn't like integers unless we convert to strings
            right_value = str(right_value)
        if right_type == OrsoTypes.TIMESTAMP and isinstance(right_value, numpy.datetime64):
            # iceberg doesn't like timestamps unless we convert to strings
            right_value = right_value.astype(datetime.datetime)
        return ICEBERG_FILTERS[root.value](left_node.value, right_value)

    iceberg_filter = None
    unsupported = []
    if not isinstance(root, list):
        root = [root]
    for predicate in root:
        try:
            converted = _predicate_to_iceberg_filter(predicate)
            if iceberg_filter is None:
                iceberg_filter = converted
            else:
                iceberg_filter = pyiceberg.expressions.And(iceberg_filter, converted)
        except NotSupportedError:
            unsupported.append(predicate)

    return iceberg_filter if iceberg_filter else "True", unsupported


class IcebergConnector(GcpCloudStorageConnector, Statistics):
    __mode__ = "Blob"
    __type__ = "ICEBERG"
    __synchronousity__ = "asynchronous"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,  # nulls not handled correctly
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    def __init__(self, *args, catalog=None, **kwargs):
        GcpCloudStorageConnector.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)

        # The GCP connector changes . to / internally - we need to reverse that
        self.dataset = self.dataset.lower().replace("/", ".")

        import pyiceberg

        try:
            if isinstance(catalog, pyiceberg.catalog.Catalog):
                metastore = catalog
                catalog_name = metastore.name
            else:
                catalog_name, self.dataset = self.dataset.split(".", 1)
                metastore = catalog(
                    catalog_name=catalog_name,
                    firestore_project=kwargs.get("firestore_project"),
                    firestore_database=kwargs.get("firestore_database"),
                    gcs_bucket=kwargs.get("gcs_bucket"),
                )
            self.table = metastore.load_table(self.dataset)

            self.snapshot = self.table.current_snapshot()
            self.snapshot_id = None if self.snapshot is None else self.snapshot.snapshot_id
        except pyiceberg.exceptions.NoSuchTableError:
            raise DatasetNotFoundError(
                dataset=f"{catalog_name}.{self.dataset}", connector=self.__type__
            )

    def get_dataset_schema(self) -> RelationSchema:
        if self.start_date != self.end_date:
            if self.start_date.date() != self.end_date.date():
                raise UnsupportedSyntaxError("This table only supports point in time reads.")
            raise UnsupportedSyntaxError(
                "This table only supports point in time reads. Are you missing the time component from your FOR clause?"
            )

        if self.start_date is not None:
            snapshots = self.table.inspect.snapshots().sort_by("committed_at")
            snapshot_rows = snapshots.to_pylist()

            if not snapshot_rows:
                raise DatasetReadError("No data available for the specified date.")

            # Honor dates before the first snapshot by rejecting them, but treat
            # dates after the latest snapshot as selecting the latest snapshot
            first_committed = snapshot_rows[0]["committed_at"]
            last_committed = snapshot_rows[-1]["committed_at"]

            if self.start_date < first_committed:
                # Point-in-time read is before our first snapshot — no data available then
                raise DatasetReadError("No data available for the specified date.")
            elif self.start_date > last_committed:
                # Point-in-time read after the latest snapshot — return current data
                selected = snapshot_rows[-1]
                # ensure we store the commit time for telemetry/context
                self.telemetry.dataset_committed_at = selected["committed_at"].isoformat()
            else:
                selected = snapshot_rows[0]
                for candidate in snapshot_rows:
                    if candidate["committed_at"] <= self.start_date:
                        self.telemetry.dataset_committed_at = candidate["committed_at"].isoformat()
                        selected = candidate
                    else:
                        break

            self.snapshot_id = selected["snapshot_id"]
            self.snapshot = self.table.snapshot_by_id(self.snapshot_id)

        # If the table has no snapshot and the read is not time-travel, use
        # the table's declared schema (from metadata) and return an empty result set.
        if self.snapshot is None:
            iceberg_schema = self.table.schema()
        else:
            iceberg_schema = self.table.schemas()[self.snapshot.schema_id]
            try:
                self.telemetry.dataset_committed_at = datetime.datetime.fromtimestamp(
                    self.snapshot.timestamp_ms / 1000.0
                ).isoformat()
            except (ValueError, OSError, OverflowError):
                pass
        arrow_schema = iceberg_schema.as_arrow()

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        # Get statistics
        relation_statistics = RelationStatistics()

        column_names = {col.field_id: col.name for col in iceberg_schema.columns}
        column_types = {col.field_id: col.field_type for col in iceberg_schema.columns}

        files = self.table.inspect.files(snapshot_id=self.snapshot_id)

        # No files = empty table, no stats
        if len(files.column("file_path")) == 0:
            self.relation_statistics = relation_statistics
            return self.schema

        relation_statistics.record_count = pyarrow.compute.sum(files.column("record_count")).as_py()

        if "distinct_counts" in files.columns:
            for file in files.column("distinct_counts"):
                for k, v in file:
                    relation_statistics.set_cardinality_estimate(column_names[k], v)

        if "value_counts" in files.columns:
            for file in files.column("value_counts"):
                for k, v in file:
                    relation_statistics.add_count(column_names[k], v)

        #        for file in files.column("lower_bounds"):
        #            for k, v in file:
        #                relation_statistics.update_lower(
        #                    column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k])
        #                )

        #        for file in files.column("upper_bounds"):
        #            for k, v in file:
        #                relation_statistics.update_upper(
        #                    column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k])
        #                )

        self.relation_statistics = relation_statistics

        return self.schema

    def get_list_of_blob_names(self, *, prefix: str = None, predicates: list = []) -> List[str]:
        pushed_filters, _ = to_iceberg_filter(predicates)

        # Get the list of data files to read
        data_files = self.table.scan(
            row_filter=pushed_filters,  # Iceberg expression
            snapshot_id=self.snapshot_id,
        ).plan_files()

        def remove_protocol(text: str, prots: tuple) -> str:
            for prot in prots:
                if text.startswith(prot):
                    return text[len(prot) :]
            return text

        all_blobs = [remove_protocol(task.file.file_path, ("gs://",)) for task in data_files]

        return all_blobs

    @staticmethod
    def decode_iceberg_value(
        value: Union[int, float, bytes], data_type: str, scale: int = None
    ) -> Union[int, float, str, datetime.datetime, Decimal, bool]:
        """
        Decode Iceberg-encoded values based on the specified data type.

        Parameters:
            value: Union[int, float, bytes]
                The encoded value from Iceberg.
            data_type: str
                The type of the value ('int', 'long', 'float', 'double', 'timestamp', 'date', 'string', 'decimal', 'boolean').
            scale: int, optional
                Scale used for decoding decimal types, defaults to None.

        Returns:
            The decoded value in its original form.
        """
        import pyiceberg

        data_type_class = data_type.__class__

        if data_type_class == pyiceberg.types.LongType:
            return int.from_bytes(value, "big", signed=True)
        elif data_type_class == pyiceberg.types.DoubleType:
            # IEEE 754 encoded floats are typically decoded directly
            return struct.unpack(">d", value)[0]  # 8-byte IEEE 754 double
        elif data_type_class in (pyiceberg.types.TimestampType, pyiceberg.types.TimestamptzType):
            # Iceberg stores timestamps as microseconds since epoch
            interval = int.from_bytes(value, "big", signed=True)
            if interval < 0:
                # Windows specifically doesn't like negative timestamps
                return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=interval)
            return datetime.datetime.fromtimestamp(interval / 1_000_000)
        elif data_type == "date":
            # Iceberg stores dates as days since epoch (1970-01-01)
            interval = int.from_bytes(value, "big", signed=True)
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=interval)
        elif data_type_class == pyiceberg.types.StringType:
            # Assuming UTF-8 encoded bytes (or already decoded string)
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        elif data_type_class == pyiceberg.types.BinaryType:
            return value
        elif str(data_type).startswith("decimal"):
            # Iceberg stores decimals as unscaled integers
            int_value = int.from_bytes(value, byteorder="big", signed=True)
            return Decimal(int_value) / (10**data_type.scale)
        elif data_type_class == pyiceberg.types.BooleanType:
            return bool(value)

        ValueError(f"Unsupported data type: {data_type}, {str(data_type)}")
