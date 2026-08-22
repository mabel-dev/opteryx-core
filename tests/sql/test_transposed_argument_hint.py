"""Transposed function arguments are diagnosed as a REORDER, not as N casts.

`TIME_BUCKET(magnitude, units, date)` called value-first used to answer with a
per-argument cast for every argument - each one individually well-formed, the
set of them nonsense. `'hour'::TIMESTAMP` cannot be part of any working query,
so a reader who did not already know the signature was led AWAY from the fix.

The type diagnosis was never wrong; the remedy was. Where some permutation of
the supplied arguments satisfies a signature, that permutation is reported and
the cast hints are suppressed. Where no permutation does - a genuine type error,
transposed or not - the cast hints are exactly right and must survive.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import IncompatibleTypesError

# Prefix literals were withdrawn from the dialect, so a TIMESTAMP has to be
# spelled as a CAST.
TS = "CAST('2020-01-01' AS TIMESTAMP)"


def _error(sql: str) -> str:
    session = opteryx.session()
    with pytest.raises(IncompatibleTypesError) as raised:
        for _ in session.execute_to_morsels(sql):
            pass
    return str(raised.value)


def test_time_bucket_transposed_reports_the_reorder():
    message = _error(f"SELECT TIME_BUCKET({TS}, 1, 'hour') FROM $planets")

    assert "wrong order" in message, message
    assert "TIME_BUCKET(magnitude, units, date)" in message, message
    assert "(NUMERIC, STRING, TEMPORAL)" in message, message
    assert "(TIMESTAMP, INTEGER, VARCHAR)" in message, message
    # The working call, in the order the signature wants.
    assert "Did you mean" in message, message
    assert message.index("1, 'hour'") > message.index("Did you mean"), message


def test_time_bucket_transposed_suppresses_the_cast_hints():
    # The point of the change: these are not merely redundant here, they are
    # misleading, so not one of them may appear.
    message = _error(f"SELECT TIME_BUCKET({TS}, 1, 'hour') FROM $planets")

    assert "::VARCHAR" not in message, message
    assert "::TIMESTAMP" not in message, message
    assert "::DOUBLE" not in message, message
    assert "arg1" not in message, message


@pytest.mark.parametrize(
    "sql, signature",
    [
        # Every other catalog function that takes its value in a non-first
        # position - the same trap, reached the same way.
        (f"SELECT EXTRACT({TS}, 'day') FROM $planets", "EXTRACT(part, date)"),
        (
            f"SELECT FORMAT_TIMESTAMP({TS}, '%Y') FROM $planets",
            "FORMAT_TIMESTAMP(pattern, date)",
        ),
        (
            f"SELECT DATEDIFF({TS}, {TS}, 'day') FROM $planets",
            "DATEDIFF(part, date, end)",
        ),
        # And one that has nothing to do with dates, to show the check is on the
        # general function error path rather than special-cased.
        ("SELECT LEFT(3, name) FROM $planets", "LEFT(string, length)"),
    ],
)
def test_other_transposable_functions_report_the_reorder(sql, signature):
    message = _error(sql)

    assert "wrong order" in message, message
    assert signature in message, message


def test_single_bad_argument_still_gets_its_cast_suggestion():
    # One argument wrong is a genuine type error. Offering a reorder for it
    # would be the same misdirection in the other direction.
    message = _error("SELECT ROUND(name, 2) FROM $planets")

    assert "wrong order" not in message, message
    assert "arg1 ('name')" in message, message
    assert "`name::DOUBLE`" in message, message


def test_single_bad_argument_in_a_later_position_still_gets_its_cast():
    message = _error("SELECT CONCAT(name, 1) FROM $planets")

    assert "wrong order" not in message, message
    assert "`1::VARCHAR`" in message, message


def test_all_arguments_wrong_with_no_valid_permutation_keeps_the_casts():
    # Majority-mismatched, but no ordering of (VARCHAR, INTEGER, VARCHAR)
    # satisfies (NUMERIC, STRING, TEMPORAL) - there is no temporal argument to
    # move into place. A reorder hint here would be a fabricated remedy.
    message = _error("SELECT TIME_BUCKET(name, 1, 'hour') FROM $planets")

    assert "wrong order" not in message, message
    assert "arg1 ('name')" in message, message
    assert "`'hour'::TIMESTAMP`" in message, message


if __name__ == "__main__":  # pragma: no cover
    test_time_bucket_transposed_reports_the_reorder()
    print("✅ test_time_bucket_transposed_reports_the_reorder")
    test_time_bucket_transposed_suppresses_the_cast_hints()
    print("✅ test_time_bucket_transposed_suppresses_the_cast_hints")
    for _sql, _sig in [
        (f"SELECT EXTRACT({TS}, 'day') FROM $planets", "EXTRACT(part, date)"),
        (f"SELECT FORMAT_TIMESTAMP({TS}, '%Y') FROM $planets", "FORMAT_TIMESTAMP(pattern, date)"),
        (f"SELECT DATEDIFF({TS}, {TS}, 'day') FROM $planets", "DATEDIFF(part, date, end)"),
        ("SELECT LEFT(3, name) FROM $planets", "LEFT(string, length)"),
    ]:
        test_other_transposable_functions_report_the_reorder(_sql, _sig)
        print(f"✅ test_other_transposable_functions_report_the_reorder [{_sig}]")
    test_single_bad_argument_still_gets_its_cast_suggestion()
    print("✅ test_single_bad_argument_still_gets_its_cast_suggestion")
    test_single_bad_argument_in_a_later_position_still_gets_its_cast()
    print("✅ test_single_bad_argument_in_a_later_position_still_gets_its_cast")
    test_all_arguments_wrong_with_no_valid_permutation_keeps_the_casts()
    print("✅ test_all_arguments_wrong_with_no_valid_permutation_keeps_the_casts")
    print("✅ all transposed-argument hint tests passed")
