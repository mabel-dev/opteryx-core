"""Tests for the LENGTH() function and its underlying kernel.

These focus on the regression where the LENGTH kernel was the list-oriented
``vector_length`` kernel. That raised `opteryx.exceptions.FunctionExecutionError`
when the argument arrived as a string vector (the vast majority of real queries).

Here we exercise the function via the planner to ensure the fix is end-to-end
and also call the kernel LENGTH is registered to (`vector_string_length`,
registrar/text.pyx) directly, over plain and dictionary-encoded strings.
"""

import draken
from draken.draken_native import DrakenType
from draken.draken_native import vector_from_string_dict_sequence

import opteryx
from opteryx.compiled.nanobind.vectors import vector_string_length


def test_vector_string_length_handles_string_and_dictionary():
    arr = draken.vector_from_sequence(["a", "bc", None], DrakenType.VARCHAR)
    assert vector_string_length(arr).to_pylist() == [1, 2, None]

    # dictionary encoded version should also work
    dict_arr = vector_from_string_dict_sequence([b"a", b"bc", None, b"a"])
    assert dict_arr.is_dict
    assert vector_string_length(dict_arr).to_pylist() == [1, 2, None, 1]


def test_length_function_via_sql():
    session = opteryx.session()
    rows = [
        value
        for morsel in session.execute_to_morsels(
            "SELECT LENGTH(name) AS n FROM $planets WHERE LENGTH(name) > 5"
        )
        for value in morsel.column("n").to_pylist()
    ]
    # Mercury, Jupiter, Saturn, Uranus, Neptune
    assert sorted(rows) == [6, 6, 7, 7, 7], rows
