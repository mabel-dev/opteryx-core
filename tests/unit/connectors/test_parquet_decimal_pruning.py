# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Row-group pruning over DECIMAL columns reads the literal as the filter does.

rugo decodes DECIMAL statistics to exact `decimal.Decimal`, but a decimal-point
SQL literal arrives as a float, and a float compares against a Decimal by its
exact binary value: 7.70 is 7.7000000000000001776..., ABOVE a row group's
`Decimal('7.70')` max, so `d = 7.70` and `d >= 7.70` pruned the row group that
holds the value (a wrong answer at the boundary, found through an Iceberg table
whose connector pushes DECIMAL predicates into the scan).
`_decimal_bound_literal` reads the literal as `Decimal(str(value))`, the reading
`rescale_decimal_literal` gives the predicate the engine runs.
"""

import os
import sys
from decimal import Decimal
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.parquet_io.predicates import _DECLINE
from opteryx.connectors.parquet_io.predicates import _can_prune_rowgroup
from opteryx.connectors.parquet_io.predicates import _decimal_bound_literal
from opteryx.types.logical_type import try_parse_column_type

# The row group spans exactly the values the column holds at its ends.
MIN, MAX = Decimal("1.10"), Decimal("7.70")


def _column(type_text):
    return SimpleNamespace(schema_column=SimpleNamespace(column_type=try_parse_column_type(type_text)))


DECIMAL_9_2 = _column("DECIMAL(9, 2)")


@pytest.mark.parametrize(
    "op, literal",
    [
        ("Eq", 7.70),  # the max: float 7.70 sits above Decimal('7.70')
        ("GtEq", 7.70),
        ("Eq", 1.10),  # the min
        ("LtEq", 1.10),
        ("Gt", 7.69),
        ("Lt", 1.11),
        ("Eq", 3.30),
        ("Eq", 3),  # an integer literal
    ],
)
def test_float_literal_on_a_bound_keeps_the_row_group(op, literal):
    value = _decimal_bound_literal(DECIMAL_9_2, literal)
    assert isinstance(value, Decimal)
    assert _can_prune_rowgroup(op, value, MIN, MAX) is False


@pytest.mark.parametrize(
    "op, literal",
    [("Eq", 7.71), ("Gt", 7.70), ("Lt", 1.10), ("Eq", 1.09)],
)
def test_pruning_still_happens_outside_the_bounds(op, literal):
    value = _decimal_bound_literal(DECIMAL_9_2, literal)
    assert _can_prune_rowgroup(op, value, MIN, MAX) is True


def test_in_list_members_are_read_the_same_way():
    members = [_decimal_bound_literal(DECIMAL_9_2, v) for v in (7.70, 99.0)]
    assert _can_prune_rowgroup("InList", members, MIN, MAX) is False


def test_literal_is_the_decimal_as_written():
    assert _decimal_bound_literal(DECIMAL_9_2, 7.70) == Decimal("7.7")
    assert _decimal_bound_literal(DECIMAL_9_2, Decimal("3.30")) == Decimal("3.30")


def test_decimal128_declines():
    """The engine's decimal-literal rewrite does not cover DECIMAL128, so there
    is no established reading to agree with - pruning declines rather than guess."""
    assert _decimal_bound_literal(_column("DECIMAL(38, 2)"), 7.70) is _DECLINE


def test_bool_literal_declines():
    assert _decimal_bound_literal(DECIMAL_9_2, True) is _DECLINE


def test_non_decimal_columns_pass_through():
    assert _decimal_bound_literal(_column("DOUBLE"), 7.70) == 7.70
    assert _decimal_bound_literal(_column("INTEGER"), 3) == 3
