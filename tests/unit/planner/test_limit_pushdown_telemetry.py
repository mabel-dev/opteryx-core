"""
`optimization_limit_pushdown` counts ONLY a LIMIT the scan took; a LIMIT parked
above a barrier (or a scan that refused it) is `optimization_limit_relocated`.
Before the split both bumped `optimization_limit_pushdown`, so job telemetry read
"pushed" for a filtered scan whose LIMIT never reached it.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _telemetry(sql):
    session = opteryx.session()
    for _ in session.execute_to_morsels(sql):
        pass
    return session.telemetry


def test_limit_reaching_the_scan_is_a_pushdown():
    tel = _telemetry("SELECT name FROM testdata.astronauts LIMIT 3")
    assert tel.get("optimization_limit_pushdown") == 1
    assert tel.get("optimization_limit_relocated") is None


def test_limit_above_a_filtered_scan_is_relocated_not_pushed():
    # The predicate is pushed into the parquet scan, whose connector does not take a
    # LIMIT over a pushed predicate, so the Limit stays a plan node above the scan.
    tel = _telemetry(
        "SELECT name FROM testdata.astronauts WHERE name = 'Neil A. Armstrong' LIMIT 1"
    )
    assert tel.get("optimization_limit_pushdown") is None
    assert tel.get("optimization_limit_relocated") == 1


def test_limit_above_an_aggregate_is_relocated():
    tel = _telemetry("SELECT COUNT(*), name FROM testdata.astronauts GROUP BY name LIMIT 2")
    assert tel.get("optimization_limit_pushdown") is None
    assert tel.get("optimization_limit_relocated") == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
