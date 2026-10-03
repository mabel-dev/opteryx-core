"""Calendar functions refuse TIME and INTERVAL at bind time.

DATEDIFF, TIMEDIFF, EXTRACT, FORMAT_TIMESTAMP, TIME_BUCKET and UNIXTIME were
declared over the `temporal` type family, which also admits TIME and INTERVAL.
Their kernels take DATE or TIMESTAMP only, so a TIME or INTERVAL argument bound
cleanly and then died inside the kernel with a raw Python error:

    TypeError: vector_unixtime: expected TIMESTAMP64 or DATE32 Vector, got DrakenType 4

They are now declared over the `datetime` family (DATE or TIMESTAMP), so the
binder refuses the call with IncompatibleTypesError - and does not suggest a
`::TIMESTAMP` cast, because TIME and INTERVAL have none.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import IncompatibleTypesError

TIME = "CAST('09:30:45' AS TIME)"
INTERVAL = "INTERVAL '1' DAY"
TS = "CAST('2024-01-15 09:30:00' AS TIMESTAMP)"
DATE = "CAST('2024-01-15' AS DATE)"

# Each call with `{}` where the calendar operand goes.
CALLS = [
    "DATEDIFF('minutes', {}, {})",
    "TIMEDIFF({}, {})",
    "EXTRACT(DAY FROM {})",
    "FORMAT_TIMESTAMP('%Y', {})",
    "TIME_BUCKET(15, 'minute', {})",
    "UNIXTIME({})",
]


def _run(expression: str) -> list:
    rows = []
    for morsel in opteryx.session().execute_to_morsels(f"SELECT {expression} AS a"):
        if morsel is not None:
            rows.extend(morsel.column(b"a").to_pylist())
    return rows


def _fill(call: str, operand: str) -> str:
    return call.replace("{}", operand)


@pytest.mark.parametrize("call", CALLS)
@pytest.mark.parametrize("operand, type_name", [(TIME, "TIME"), (INTERVAL, "INTERVAL")])
def test_time_and_interval_are_refused_with_a_type_error(call, operand, type_name):
    with pytest.raises(IncompatibleTypesError) as raised:
        _run(_fill(call, operand))
    message = str(raised.value)

    assert "expected DATE or TIMESTAMP" in message, message
    assert f"{type_name} cannot be cast" in message, message
    assert "::TIMESTAMP" not in message, message


@pytest.mark.parametrize("call", CALLS)
@pytest.mark.parametrize("operand", [DATE, TS])
def test_date_and_timestamp_still_bind(call, operand):
    assert len(_run(_fill(call, operand))) == 1


def test_varchar_still_gets_its_cast_suggestion():
    # A string DOES cast to TIMESTAMP, so the hint is the remedy there.
    with pytest.raises(IncompatibleTypesError) as raised:
        _run("UNIXTIME('2024-01-01')")
    assert "`'2024-01-01'::TIMESTAMP`" in str(raised.value), str(raised.value)


if __name__ == "__main__":  # pragma: no cover
    for _call in CALLS:
        for _operand, _type_name in [(TIME, "TIME"), (INTERVAL, "INTERVAL")]:
            test_time_and_interval_are_refused_with_a_type_error(_call, _operand, _type_name)
            print(f"✅ refused {_fill(_call, _operand)}")
        for _operand in (DATE, TS):
            test_date_and_timestamp_still_bind(_call, _operand)
            print(f"✅ bound {_fill(_call, _operand)}")
    test_varchar_still_gets_its_cast_suggestion()
    print("✅ test_varchar_still_gets_its_cast_suggestion")
    print("✅ all calendar-function TIME/INTERVAL tests passed")
