"""
odata_dashboard benchmark suite - pytest correctness battery.

Query bodies live in ../queries.py (shared with the DuckDB baseline runner and
the top-level comparison runner) - see that module's docstring for the full
provenance/sampling notes. This file just formats them against the local
testdata.public.<table> datasets and runs them as a pytest battery.
"""

import importlib.util
import os
import sys
from typing import Optional

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.utils.formatter import format_sql

# Load ../queries.py by file path rather than sys.path insertion: adding
# tests/performance/odata_dashboard/ to sys.path would let `import opteryx`
# resolve to the sibling opteryx/ subdirectory (this package) instead of the
# real top-level opteryx package (same shadowing hazard pytest collection
# hits across all the performance suites' opteryx/ dirs).
_queries_path = os.path.join(os.path.dirname(__file__), "..", "queries.py")
_spec = importlib.util.spec_from_file_location("odata_dashboard_queries", _queries_path)
_queries_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_queries_module)
QUERIES = _queries_module.QUERIES

TABLES = {
    "GDELT": "testdata.public.gdelt_events",
    "NVD": "testdata.public.nvd_vulnerabilities",
    "EXPLOITED": "testdata.public.exploited_vulnerabilities",
    "VPW": "testdata.public.vulnerabilities_per_week",
    "EXPLOITDB": "testdata.public.exploit_db",
}

STATEMENTS = [
    (f"/* {name} */ " + body.format(**TABLES), None) for name, body in QUERIES
]


@pytest.mark.parametrize("statement, exception", STATEMENTS)
def test_sql_battery(statement: str, exception: Optional[Exception]):
    """
    Test a battery of statements mined from the odata.opteryx.app query log.
    """

    from opteryx.exceptions import MissingSqlStatement, UnsupportedSyntaxError

    session = None
    try:
        # execute_to_morsels avoids Arrow conversion overhead
        session = opteryx.session()
        for _ in session.execute_to_morsels(statement):
            pass
        assert exception is None, (
            f"Exception {exception} not raised but expected\n{format_sql(statement)}"
        )
    except AssertionError as error:
        raise error
    except UnsupportedSyntaxError:
        pytest.skip("query not supported by Draken aggregator")
    except MissingSqlStatement:
        pytest.skip("no actual SQL statement")
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":  # pragma: no cover
    import time

    session = opteryx.session()
    for statement, expected_exception in STATEMENTS:
        start = time.perf_counter_ns()
        try:
            for _ in session.execute_to_morsels(statement):
                pass
            elapsed_ms = (time.perf_counter_ns() - start) / 1e6
            print(f"{elapsed_ms:8.2f} ms  {statement.strip().splitlines()[0][:100]}")
        except Exception as error:
            print(f"{'ERROR':>8}     {type(error).__name__}: {error}")
    session.close()
