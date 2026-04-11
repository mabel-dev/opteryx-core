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

from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.schema import RelationSchema

WELL_KNOWN_DATASETS = {
    "$planets": ("opteryx.managers.virtual_datasets.planet_data", True),
    "$variables": ("opteryx.managers.virtual_datasets.variables_data", True),
    "$derived": ("opteryx.managers.virtual_datasets.derived_data", False),
    "$no_table": ("opteryx.managers.virtual_datasets.no_table_data", False),
    "$telemetry": ("opteryx.managers.virtual_datasets.telemetry", True),
    "$user": ("opteryx.managers.virtual_datasets.user", True),
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
    """Provide suggestions to the user if they gave a table that doesn't exist."""
    from opteryx.utils import suggest_alternative

    known_datasets = (name for name, suggestable in WELL_KNOWN_DATASETS.items() if suggestable)
    suggestion = suggest_alternative(dataset, known_datasets)
    if suggestion is not None:
        return (
            f"The requested dataset, '{dataset}', could not be found. Did you mean '{suggestion}'?"
        )


def _project_morsel(morsel: Morsel, columns: list) -> Morsel:
    """Project and rename columns on a Morsel.

    This mirrors the behavior of arrow.post_read_projector() but operates on
    Draken Morsels directly.
    """
    if not columns:
        return morsel

    # Determine which columns are present in the morsel and should be kept.
    selected_actual = []
    selected_canonical = []

    for projection_column in columns:
        canonical = projection_column.schema_column.name
        for alias in projection_column.schema_column.all_names:
            alias_bytes = alias.encode("utf-8") if isinstance(alias, str) else alias
            if alias_bytes in morsel.column_names:
                selected_actual.append(alias_bytes)
                selected_canonical.append(canonical)
                break

    if not selected_actual:
        return morsel

    out = morsel.copy(columns=selected_actual)

    if any(
        actual.decode("utf-8") != canonical
        for actual, canonical in zip(selected_actual, selected_canonical)
    ):
        vectors = [out.column(actual) for actual in selected_actual]
        out = Morsel.from_vectors(selected_canonical, vectors)

    return out


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
        """Read the virtual dataset and yield morsels.

        Args:
            columns: List of columns to read
            **kwargs: Additional read parameters

        Yields:
            Morsel chunks
        """

        data_provider, _ = _load_provider(self.dataset)
        if data_provider is None:
            suggestion = suggest(self.dataset.lower())
            raise DatasetNotFoundError(
                suggestion=suggestion, dataset=self.dataset, connector=self.__type__
            )

        morsel = data_provider.read(at_date=kwargs.get("at_date"), variables=self.variables)
        yield _project_morsel(morsel, columns)


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

    def __next__(self) -> Morsel:
        """Read the next chunk or morsel from the dataset.

        Returns:
            A Morsel representing a chunk of the dataset.
            Raises StopIteration if the dataset is exhausted.
        """
        if self.exhausted:
            raise StopIteration("Dataset has been read.")

        self.exhausted = True

        data_provider, _ = _load_provider(self.dataset_name)
        if data_provider is None:
            suggestion = suggest(self.dataset_name.lower())
            raise DatasetNotFoundError(
                suggestion=suggestion, dataset=self.dataset_name, connector="SAMPLE"
            )

        morsel = data_provider.read(self.date, self.variables)

        if self.columns:
            morsel = _project_morsel(morsel, self.columns)

        return morsel
