import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from tests.helpers import execute_and_get_shape


def test_question_mark():
    shape = execute_and_get_shape("SELECT * FROM $planets WHERE id = ?", params=[1])
    assert shape == (1, 20)

    shape = execute_and_get_shape("SELECT * FROM $planets WHERE id = ? or name = ?", params=[1, "Earth"])
    assert shape == (2, 20)

    shape = execute_and_get_shape(
        "SELECT * FROM (SELECT * FROM $planets WHERE id = ? or name = ?) AS sub",
        params=[1, "Earth"],
    )
    assert shape == (2, 20)


def test_named_parameter():
    shape = execute_and_get_shape("SELECT * FROM $planets WHERE id = :pid", params={"pid": 1})
    assert shape == (1, 20)

    shape = execute_and_get_shape(
        "SELECT * FROM $planets WHERE id = :pid or name = :name", params={"pid": 1, "name": "Earth"}
    )
    assert shape == (2, 20)

    # if we've given named params, provide a dict
    with pytest.raises(opteryx.exceptions.ParameterError):
        session = opteryx.session()
        list(session.execute_to_morsels(
            "SELECT * FROM (SELECT * FROM $planets WHERE id = :pid or name = :name) AS sub",
            params=[1, "Earth"],
        ))

    # we can't use param lists with batched queries
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError):
        session = opteryx.session()
        list(session.execute_to_morsels(
            "SET @apple = ?; SELECT * FROM $planets WHERE id = ? or name = :name",
            params=[1, "Earth"],
        ))

    # we can used named parameters though
    shape = execute_and_get_shape(
        "SET @apple = :apple; SELECT * FROM $planets WHERE id = :apple or name = :name",
        params={"apple": 1, "name": "Earth"},
    )
    assert shape == (2, 20)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
