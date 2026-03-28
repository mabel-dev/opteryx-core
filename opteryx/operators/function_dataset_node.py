# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Blob Reader Node

This is a SQL Query Execution Plan Node.

This Node creates datasets based on function calls like VALUES and UNNEST.
"""

import copy
import datetime
import time
from numbers import Integral
from typing import Generator

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import SqlError
from opteryx.expression import NodeType
from opteryx.models import QueryProperties
from opteryx.utils import series
from orso.types import OrsoTypes

from .read_node import ReaderNode

_DATA_FORMAT = "draken"
_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DT = datetime.datetime(1970, 1, 1)


def _column_metadata(columns):
    column_names = []
    column_types = []

    for column in columns:
        column_names.append(column.schema_column.identity)
        column_types.append(column.schema_column.type)

    return column_names, column_types


def _as_list(values):
    if values is None:
        return []
    if hasattr(values, "to_pylist"):
        return values.to_pylist()
    if hasattr(values, "tolist"):
        return values.tolist()
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return list(values)


def _build_morsel_from_columns(column_names, column_types, column_values):
    vectors = []
    for index, _ in enumerate(column_names):
        dtype = column_types[index] if index < len(column_types) else None
        values = column_values[index] if index < len(column_values) else []
        if dtype is None:
            vectors.append(vector_from_sequence(values))
        else:
            vectors.append(vector_from_sequence(values, dtype=dtype))
    return Morsel.from_vectors(column_names, vectors)


def _build_morsel_from_rows(columns, rows):
    column_names, column_types = _column_metadata(columns)
    column_values = [[] for _ in column_names]

    for row in rows:
        for index in range(len(column_names)):
            column_values[index].append(row[index] if index < len(row) else None)

    return _build_morsel_from_columns(column_names, column_types, column_values)


def _restore_temporal_series_args(args):
    restored_args = []

    for arg in args:
        if arg.type == OrsoTypes.DATE and isinstance(arg.value, Integral):
            restored = copy.copy(arg)
            restored.value = _EPOCH_DATE + datetime.timedelta(days=int(arg.value))
            restored_args.append(restored)
            continue

        if arg.type == OrsoTypes.TIMESTAMP and isinstance(arg.value, Integral):
            restored = copy.copy(arg)
            restored.value = _EPOCH_DT + datetime.timedelta(microseconds=int(arg.value))
            restored_args.append(restored)
            continue

        restored_args.append(arg)

    return restored_args


def _generate_series(**kwargs):
    column_names, column_types = _column_metadata(kwargs["columns"])
    value_array = _as_list(series.generate_series(*_restore_temporal_series_args(kwargs["args"])))
    return _build_morsel_from_columns(column_names, column_types, [value_array])


def _unnest(**kwargs):
    """unnest converts an list into rows"""
    if kwargs["args"][0].node_type == NodeType.NESTED:
        list_items = [kwargs["args"][0].centre.value]
    else:
        list_items = kwargs["args"][0].value

    column_names, column_types = _column_metadata(kwargs["columns"])
    return _build_morsel_from_columns(column_names, column_types, [_as_list(list_items)])


def _values(**parameters):
    values_array = parameters["values"]
    rows = [tuple(value.value for value in values) for values in values_array]
    return _build_morsel_from_rows(parameters["columns"], rows)


def _fake_data(**kwargs):
    from orso.faker import generate_fake_data

    rows = kwargs["rows"]
    schema = copy.deepcopy(kwargs["schema"])
    for column in schema.columns:
        column.name = column.identity
    data = generate_fake_data(schema, rows)
    return _build_morsel_from_rows(kwargs["columns"], data.fetchall())


DATASET_FUNCTIONS = {
    "FAKE": _fake_data,
    "GENERATE_SERIES": _generate_series,
    "UNNEST": _unnest,
    "VALUES": _values,
}


class FunctionDatasetNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.alias = parameters.get("alias")
        self.function = parameters["function"]
        self.parameters = parameters
        self.columns = parameters.get("columns", [])
        self.args = parameters.get("args", [])

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        if self.function == "FAKE":
            return f"FAKE ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
        if self.function == "GENERATE_SERIES":
            return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in self.args)}){' AS ' + self.alias if self.alias else ''}"
        if self.function == "VALUES":
            return f"VALUES (({', '.join([str(c) for c in self.columns])}) x {self.parameters.get('values', 0)} AS {self.alias})"
        if self.function == "UNNEST":
            return f"UNNEST ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.parameters.get('unnest_target', '')})"

    @property
    def name(self):  # pragma: no cover
        return "Dataset Constructor"

    @property
    def can_push_selection(self):
        return False

    def execute(self, morsel, **kwargs) -> Generator:
        try:
            start_time = time.time_ns()
            result_morsel = DATASET_FUNCTIONS[self.function](**self.parameters)  # type: ignore
            self.readings["time_evaluate_dataset"] += time.time_ns() - start_time
        except TypeError as err:  # pragma: no cover
            if str(err).startswith("_unnest() takes 2"):
                raise SqlError(
                    "UNNEST expects a literal list in paranthesis, or a field name as a parameter."
                )
            raise err

        self.readings["columns_read"] += len(result_morsel.column_names)
        self.readings["rows_read"] += result_morsel.num_rows
        self.readings["bytes_processed"] += result_morsel.nbytes

        yield result_morsel
