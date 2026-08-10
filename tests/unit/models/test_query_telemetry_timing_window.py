# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
QueryTelemetry.as_dict() and the query's timing window.

`time_total` is `end_time - start_time`, and `_reading` is a defaultdict(int), so
an unset `end_time` used to read back as 0 and the reported total was
`0 - start_time` — around -1.79 billion seconds, indistinguishable from a real
measurement to anything downstream. `Session.telemetry` stamps `end_time` before
it calls as_dict(); every other route into as_dict() is holding an open window and
must be told so.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.exceptions import InvalidInternalStateError
from opteryx.models.query_telemetry import _QueryTelemetry


def test_as_dict_refuses_a_window_that_never_started():
    telemetry = _QueryTelemetry()

    with pytest.raises(InvalidInternalStateError) as err:
        telemetry.as_dict()

    assert "never started" in str(err.value)


def test_as_dict_refuses_a_window_that_was_never_closed():
    telemetry = _QueryTelemetry()
    telemetry.start_time = 1_000_000_000

    with pytest.raises(InvalidInternalStateError) as err:
        telemetry.as_dict()

    assert "never closed" in str(err.value)
    # The remedy names the sanctioned route in, in a code span.
    assert "`Session.telemetry`" in str(err.value)


def test_as_dict_reports_a_closed_window():
    telemetry = _QueryTelemetry()
    telemetry.start_time = 1_000_000_000
    telemetry.end_time = 1_500_000_000

    assert telemetry.as_dict()["time_total"] == 0.5


def test_session_telemetry_closes_the_window():
    import opteryx

    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT * FROM $planets"):
        pass

    telemetry = session.telemetry

    # The property is what makes the window closeable, so it must never be the
    # thing that raises — and the total it reports has to be a real elapsed time,
    # not the negative epoch that an unset end_time produced.
    assert telemetry["time_total"] > 0
    assert telemetry["time_total"] >= telemetry["time_planning"]


def test_plan_stamps_its_own_window():
    import opteryx

    session = opteryx.session()
    session.plan("SELECT * FROM $planets")

    telemetry = session.telemetry

    # plan() closes its own window, so the property has nothing left to stamp and
    # the total covers the call rather than the planner alone.
    assert telemetry["time_total"] > 0
    assert telemetry["time_total"] >= telemetry["time_planning"]


def test_plan_after_execute_reports_only_its_own_readings():
    import opteryx

    session = opteryx.session()
    for _ in session.execute_to_morsels("SELECT * FROM $planets"):
        pass
    assert session.telemetry["time_executing"] > 0

    session.plan("SELECT * FROM $planets")

    # The readings are per-operation. Nothing was executed by plan(), so there is no
    # execution reading at all — carrying the previous call's forward would attribute
    # work to a statement that never ran.
    assert "time_executing" not in session.telemetry


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
