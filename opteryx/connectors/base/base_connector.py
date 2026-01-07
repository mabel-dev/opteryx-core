# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Base classes for connectors and table readers.

BaseConnector: Gateway interface for long-lived connectors (OpteryxConnector, FileSystemConnector)
BaseTable: Table reader interface for transient readers (OpteryxTable, FileSystemTable)
"""

from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple

import pyarrow
from orso.schema import RelationSchema

from opteryx.connectors import TableType
from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import QueryTelemetry

MIN_CHUNK_SIZE: int = 500
INITIAL_CHUNK_SIZE: int = 500
DEFAULT_MORSEL_SIZE: int = 16 * 1024 * 1024


class BaseConnector:
    """
    Base class for gateway connectors.

    Gateway connectors are long-lived, cached by storage configuration.
    They create transient table reader instances via table_engine().

    Examples: OpteryxConnector, FileSystemConnector
    """

    eidetic = False

    # Capability declarations - what the table readers created by this gateway support
    supports_diachronic = False  # Time-travel/temporal queries
    supports_predicate_pushdown = False  # Filter pushdown to storage
    supports_limit_pushdown = False  # Limit pushdown to storage
    supports_statistics = False  # Statistics gathering
    supports_async = False  # Asynchronous reads

    @property
    def __mode__(self):  # pragma: no cover
        raise NotImplementedError("__mode__ not defined")

    @property
    def interal_only(self):
        return False

    def locate_object(self, name: str) -> Tuple[Optional[Any], Any]:
        """
        Determine if a name refers to a table, view, or doesn't exist.

        This method allows the query planner to determine object type before
        attempting to read it.

        Args:
            name: The fully qualified or relative name of the object

        Returns:
            Tuple of (TableType | None, metadata):
            - If object exists: (TableType.Table or TableType.View, metadata object)
            - If object doesn't exist: (None, None)

        Note:
            Default implementation returns None. Connectors that support views
            or have catalog capabilities should override this method.
        """
        return None, None

    def update_comment(self, object_name: str, comment: str, describer: str = "system"):
        """
        Update the comment/description for an object (table or view).

        Default implementation will locate the object and attempt to delegate
        to connector-specific comment/set methods if available.
        """
        # Determine object type first
        object_type, _ = self.locate_object(object_name)

        if object_type not in (TableType.View, TableType.Table):
            # Let callers decide how to handle missing objects
            raise DatasetNotFoundError(connector=self, dataset=object_name)

        # Prefer a generic `set_comment` if connector implements it
        if hasattr(self, "set_comment"):
            return self.set_comment(object_name, comment, describer=describer)

        raise NotImplementedError("Connector does not support updating comments for this object")

    def table_engine(self, name: str, **kwargs):  # pragma: no cover
        """
        Create a transient table reader for the specified dataset.

        Args:
            name: Dataset name/path
            **kwargs: Additional parameters (telemetry, etc.)

        Returns:
            A table reader instance (e.g., OpteryxTable, FileSystemTable)

        Note:
            Default implementation returns None. Gateway connectors must override
            this method to return appropriate table reader instances.
        """
        return None


class BaseTable:
    """
    Base class for transient table readers.

    Table readers are created per-query to read specific datasets.
    They have dataset, telemetry, and schema attributes, and implement
    the actual data reading logic.

    Examples: OpteryxTable, FileSystemTable, legacy monolithic connectors
    """

    # Capability declarations - what this table reader supports
    supports_diachronic = False  # Time-travel/temporal queries
    supports_predicate_pushdown = False  # Filter pushdown to storage
    supports_limit_pushdown = False  # Limit pushdown to storage
    supports_statistics = False  # Statistics gathering
    supports_async = False  # Asynchronous reads

    @property
    def __mode__(self):  # pragma: no cover
        raise NotImplementedError("__mode__ not defined")

    def __init__(
        self,
        *,
        dataset: str = None,
        config: Dict[str, Any] = None,
        telemetry: QueryTelemetry,
        **kwargs,
    ) -> None:
        """
        Initialize the table reader with configuration.

        Args:
            dataset: The name of the dataset to read.
            config: Configuration information specific to the reader.
            telemetry: Query telemetry object
        """
        if config is None:
            self.config = {}
        else:
            self.config = config.copy()
        self.dataset = dataset
        self.chunk_size = INITIAL_CHUNK_SIZE
        self.schema: RelationSchema = None
        self.telemetry = telemetry
        self.pushed_predicates: list = []

    def get_dataset_schema(self) -> RelationSchema:  # pragma: no cover
        """
        Retrieve the schema of a dataset.

        Returns:
            A RelationSchema representing the schema of the dataset.
        """
        raise NotImplementedError("Subclasses must implement get_dataset_schema method.")

    def read_dataset(self, **kwargs) -> Iterable:  # pragma: no cover
        """
        Read a dataset and return a reader object.

        Args:
            dataset_name: Name of the dataset.

        Returns:
            A reader object for iterating over the dataset.
        """
        raise NotImplementedError("Subclasses must implement read_dataset method.")

    def chunk_dictset(
        self,
        dictset: Iterable[dict],
        columns: Optional[list] = None,
        morsel_size: int = DEFAULT_MORSEL_SIZE,
        initial_chunk_size: int = INITIAL_CHUNK_SIZE,
    ) -> pyarrow.Table:
        chunk = []
        self.chunk_size = initial_chunk_size  # we reset each time
        morsel = None

        for index, record in enumerate(dictset):
            _id = record.pop("_id", None)
            # column selection
            if columns:
                record = {k.source_column: record.get(k.source_column) for k in columns}
            record["id"] = None if _id is None else str(_id)

            chunk.append(record)

            if index == self.chunk_size - 1:
                morsel = pyarrow.Table.from_pylist(chunk)
                # Estimate the number of records to fill the morsel size
                if morsel.nbytes > 0:
                    self.chunk_size = int(morsel_size // (morsel.nbytes / self.chunk_size))
                yield morsel
                chunk = []
            elif (index > self.chunk_size - 1) and (index - self.chunk_size) % self.chunk_size == 0:
                morsel = pyarrow.Table.from_pylist(chunk)
                yield morsel
                chunk = []

        if chunk:
            morsel = pyarrow.Table.from_pylist(chunk)
            yield morsel


class DatasetReader:
    def __init__(self, dataset_name: str, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the reader with configuration.

        Args:
            config: Configuration information specific to the reader.
        """
        self.dataset_name = dataset_name
        self.config = config

    def __iter__(self) -> "DatasetReader":
        """
        Make the reader object iterable.
        """
        return self

    def __next__(self) -> pyarrow.Table:  # pragma: no cover
        """
        Read the next chunk or morsel from the dataset.

        Returns:
            A pyarrow Table representing a chunk or morsel of the dataset.
            raises StopIteration if the dataset is exhausted.
        """
        raise NotImplementedError("Subclasses must implement __next__ method.")

    def close(self) -> None:  # pragma: no cover
        """
        Close the reader and release any resources.
        """
        return None
