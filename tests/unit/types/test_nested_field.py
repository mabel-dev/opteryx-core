# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""NestedField: a table's declared shape for a nested column
(docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.3, ND6)."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.types.schema import ColumnDescriptor
from opteryx.types.schema import NestedField

TREE = (
    "struct", 1, "s",
    (
        ("scalar", 2, "a", ()),
        ("list", 3, "l", (("scalar", 4, "element", ()),)),
        ("map", 5, "m", (("scalar", 6, "key", ()), ("scalar", 7, "value", ()))),
    ),
)


def test_from_tuple_and_flatten_are_pre_order_with_child_counts():
    field = NestedField.from_tuple(TREE)
    assert field.flatten() == [
        ("struct", 1, "s", 3),
        ("scalar", 2, "a", 0),
        ("list", 3, "l", 1),
        ("scalar", 4, "element", 0),
        ("map", 5, "m", 2),
        ("scalar", 6, "key", 0),
        ("scalar", 7, "value", 0),
    ]


@pytest.mark.parametrize(
    "kind, children",
    [("list", ()), ("map", (NestedField(1, "key", "scalar"),)), ("scalar", (NestedField(1, "x", "scalar"),))],
)
def test_a_shape_with_the_wrong_number_of_children_is_refused(kind, children):
    with pytest.raises(ValueError, match="children"):
        NestedField(9, "x", kind, children)


def test_an_unknown_kind_is_refused():
    with pytest.raises(ValueError, match="kind"):
        NestedField(1, "x", "tuple")


def test_a_descriptor_carries_the_shape_without_changing_what_it_equals_or_persists():
    plain = ColumnDescriptor(name="s", field_id=1)
    shaped = ColumnDescriptor(name="s", field_id=1, nested=NestedField.from_tuple(TREE))
    assert plain == shaped
    assert "nested" not in shaped.to_dict()


def test_orc_reader_columns_carry_the_declared_shape_of_a_nested_column_only():
    from collections import namedtuple

    from opteryx.connectors.orc_io import reader_columns
    from opteryx.types import logical_type as lt

    Column = namedtuple("Column", "name field_id column_type nullable")
    shape = NestedField.from_tuple(TREE)
    got = reader_columns(
        "ns.t",
        [Column("id", 9, lt.INT64, True), Column("s", 1, lt.NVARCHAR, True)],
        True,
        {1: shape},
    )
    assert got[0][-1] is None
    assert got[1][-1] == shape.flatten()
