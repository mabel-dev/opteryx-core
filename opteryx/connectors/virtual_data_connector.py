# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The 'sample' connector provides readers for the internal sample datasets,
$planets.

- $no_table is used in queries where there is no relation specified 'SELECT 1'
- $derived is used as a schema to align virtual columns to
"""

import datetime
import importlib
import typing
from typing import Tuple

from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils import arrow

WELL_KNOWN_DATASETS = {
    "$planets": ("opteryx.virtual_datasets.planet_data", True),
    "$variables": ("opteryx.virtual_datasets.variables_data", True),
    "$derived": ("opteryx.virtual_datasets.derived_data", False),
    "$no_table": ("opteryx.virtual_datasets.no_table_data", False),
    "$telemetry": ("opteryx.virtual_datasets.telemetry", True),
    "$stop_words": ("opteryx.virtual_datasets.stop_words", True),
    "$user": ("opteryx.virtual_datasets.user", True),
}


def _load_provider(name: str) -> Tuple[object, bool]:
    """Lazily import and return the virtual dataset provider module and suggestable flag.

    Returns (module, suggestable)
    """
    entry = WELL_KNOWN_DATASETS.get(name)
    if entry is None:
        return None, False
    module_path, suggestable = entry
    module = importlib.import_module(module_path)
    return module, suggestable


def suggest(dataset):
    """
    Provide suggestions to the user if they gave a table that doesn't exist.
    """
    from opteryx.utils import suggest_alternative

    known_datasets = (name for name, suggestable in WELL_KNOWN_DATASETS.items() if suggestable)
    suggestion = suggest_alternative(dataset, known_datasets)
    if suggestion is not None:
        return (
            f"The requested dataset, '{dataset}', could not be found. Did you mean '{suggestion}'?"
        )


class VirtualDataConnector(BaseConnector):
    """
    Long-lived gateway for virtual/sample datasets.

    Manages access to built-in datasets like $planets, $variables, etc.
    These are simple, static datasets with no advanced capabilities.
    """

    __mode__ = "Internal"

    @property
    def interal_only(self):
        return True

    def table_engine(self, name: str, **kwargs):
        """
        Create a table reader for a specific virtual dataset.

        Args:
            name: Name of the virtual dataset (e.g., "$planets")
            **kwargs: Additional parameters (telemetry, etc.)

        Returns:
            VirtualDataTable instance
        """
        return VirtualDataTable(dataset=name, **kwargs)


class VirtualDataTable(BaseTable):
    """
    Table reader for virtual/sample datasets.

    Transient object created per query to read specific virtual datasets.
    Simple, static datasets with no advanced capabilities.
    """

    __mode__ = "Internal"
    __type__ = "VIRTUAL"
    __synchronousity__ = "synchronous"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dataset = self.dataset.lower()
        self.variables = kwargs.get("variables")

    @property
    def interal_only(self):
        return True

    def get_dataset_schema(self) -> RelationSchema:
        if self.dataset not in WELL_KNOWN_DATASETS:
            suggestion = suggest(self.dataset)
            raise DatasetNotFoundError(
                suggestion=suggestion, dataset=self.dataset, connector=self.__type__
            )
        data_provider, _ = _load_provider(self.dataset)
        return data_provider.schema()

    def read_dataset(self, columns: list = None, **kwargs):
        """
        Read the virtual dataset and yield chunks.

        Args:
            columns: List of columns to read
            **kwargs: Additional read parameters

        Yields:
            pyarrow.Table chunks
        """
        data_provider, _ = _load_provider(self.dataset)
        if data_provider is None:
            suggestion = suggest(self.dataset.lower())
            raise DatasetNotFoundError(
                suggestion=suggestion, dataset=self.dataset, connector=self.__type__
            )
        table = data_provider.read(at_date=kwargs.get("at_date"), variables=self.variables)
        yield arrow.post_read_projector(table, columns)


class SampleDatasetReader:
    """Legacy reader class - kept for backward compatibility."""

    def __init__(
        self,
        dataset_name: str,
        columns: list,
        config: typing.Optional[typing.Dict[str, typing.Any]] = None,
        date: typing.Union[datetime.datetime, datetime.date, None] = None,
        variables: typing.Dict = None,
    ) -> None:
        """
        Initialize the reader with configuration.

        Args:
            config: Configuration information specific to the reader.
        """
        self.dataset_name = dataset_name
        self.columns = columns
        self.exhausted = False
        self.date = date
        self.variables = variables
        self.config = config

    def __next__(self) -> "pyarrow.Table":
        """
        Read the next chunk or morsel from the dataset.

        Returns:
            A pyarrow Table representing a chunk or morsel of the dataset.
            raises StopIteration if the dataset is exhausted.
        """
        import pyarrow

        if self.exhausted:
            raise StopIteration("Dataset has been read.")

        self.exhausted = True

        data_provider, _ = _load_provider(self.dataset_name)
        if data_provider is None:
            suggestion = suggest(self.dataset_name.lower())
            raise DatasetNotFoundError(
                suggestion=suggestion, dataset=self.dataset_name, connector="SAMPLE"
            )
        table = data_provider.read(self.date, self.variables)
        return arrow.post_read_projector(table, self.columns)
