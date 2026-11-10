"""Typed parameters: `{"type": <canonical type>, "value": ...}` binds as CAST(value AS type)."""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from draken.draken_native import DrakenType

import opteryx
from opteryx.exceptions import ParameterError
from opteryx.exceptions import SqlError


def _bind(params):
    session = opteryx.session()
    morsel = next(iter(session.execute_to_morsels("SELECT ? AS v", params=params)))
    return morsel.schema["v"]


@pytest.mark.parametrize(
    "parameter, expected",
    [
        ({"type": "DATE", "value": "2026-01-01"}, DrakenType.DATE32),
        ({"type": "TIMESTAMP", "value": "2026-01-01T12:30:00"}, DrakenType.TIMESTAMP64),
        ({"type": "FLOAT64", "value": "inf"}, DrakenType.FLOAT64),
        ({"type": "INT64", "value": 42}, DrakenType.INT64),
        ({"type": "int64", "value": None}, DrakenType.INT64),
        ({"type": "DECIMAL(10,2)", "value": "1.50"}, DrakenType.DECIMAL),
        ({"type": "BOOL", "value": True}, DrakenType.BOOL),
        ({"type": "VARCHAR", "value": "x"}, DrakenType.VARCHAR),
    ],
)
def test_typed_parameter_binds_its_type(parameter, expected):
    assert _bind([parameter]) == expected


def test_python_temporal_values_bind_as_temporal_types():
    # These were bound as VARCHAR (their isoformat string) before typed parameters.
    assert _bind([datetime.date(2026, 1, 1)]) == DrakenType.DATE32
    assert _bind([datetime.datetime(2026, 1, 1, 12, 30)]) == DrakenType.TIMESTAMP64


def test_named_typed_parameter_filters():
    session = opteryx.session()
    rows = sum(
        m.num_rows
        for m in session.execute_to_morsels(
            "SELECT * FROM $planets WHERE id = :pid", params={"pid": {"type": "INT64", "value": 3}}
        )
    )
    assert rows == 1


def test_null_parameter_adapts_like_a_written_null():
    # A VARCHAR null against an INT column is a type error; a typeless NULL is not.
    session = opteryx.session()
    rows = sum(
        m.num_rows
        for m in session.execute_to_morsels(
            "SELECT COALESCE(id, ?) FROM $planets", params=[{"type": "NULL", "value": None}]
        )
    )
    assert rows == 9


@pytest.mark.parametrize(
    "parameter",
    [
        {"type": "DOUBLE", "value": 1.0},  # canonical names only
        {"type": "BOOLEAN", "value": True},
        {"type": "ARRAY", "value": "[1]"},
        {"type": "INT64); DROP", "value": 1},
        {"type": 7, "value": 1},
        {"type": "INT64"},
        {"type": "INT64", "value": 1, "extra": 1},
        {"type": "INT64", "value": [1]},
        {"type": "DECIMAL", "value": 1.5},  # float has already lost precision
        {"type": "NULL", "value": 0},
    ],
)
def test_malformed_typed_parameter_is_refused(parameter):
    with pytest.raises(ParameterError):
        _bind([parameter])


def test_value_cast_refuses_is_a_cast_error():
    with pytest.raises(SqlError):
        _bind([{"type": "DATE", "value": "nope"}])


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
