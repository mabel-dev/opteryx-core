"""Unit tests for VERSION AS OF <snapshot id | PREVIOUS>.

Drives the SQL rewriter and the planner's extraction helper directly, the
same way ``test_timetravel_evaluation.py`` drives ``extract_timetravel_timestamp``.
Catalog-level resolution (``OpteryxTable._resolve_snapshot``) needs a live
catalog and is exercised by the integration suite, not here.
"""

import os
import sys

# ensure the workspace root is on sys.path so that the local package
# is imported instead of any installed version.  This mimics the behaviour
# of the majority of existing tests, which rely on `pytest` running from the
# repository root.
sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner.logical_planner_builders import (
    extract_timetravel_version,
    is_version_as_of_clause,
)
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _parse_version(sql: str):
    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")[0]
    return parsed["Query"]["body"]["Select"]["from"][0]["relation"]["Table"]["version"]


def test_version_as_of_number_is_a_version_clause():
    version = _parse_version("SELECT * FROM $planets VERSION AS OF 42")
    assert is_version_as_of_clause(version)
    assert extract_timetravel_version(version) == 42


def test_timestamp_as_of_is_not_a_version_clause():
    version = _parse_version("SELECT * FROM $planets TIMESTAMP AS OF '2024-01-01'")
    assert not is_version_as_of_clause(version)


def test_version_as_of_rejects_non_integer():
    version = _parse_version("SELECT * FROM $planets VERSION AS OF 4.5")
    with pytest.raises(UnsupportedSyntaxError):
        extract_timetravel_version(version)


def test_version_as_of_previous_rewrites_to_sentinel_zero():
    rewritten = do_sql_rewrite("SELECT * FROM $planets VERSION AS OF PREVIOUS")
    assert str(rewritten) == "SELECT * FROM $planets VERSION AS OF 0"

    version = _parse_version(str(rewritten))
    assert extract_timetravel_version(version) == 0


def test_version_as_of_literal_zero_is_refused():
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite("SELECT * FROM $planets VERSION AS OF 0")


def test_previous_as_a_plain_identifier_is_untouched():
    """A column or alias literally named `previous` must not be rewritten -
    the rewrite only fires immediately after `VERSION AS OF`."""
    rewritten = do_sql_rewrite("SELECT previous FROM $planets WHERE previous = 'x'")
    assert str(rewritten) == "SELECT previous FROM $planets WHERE previous = 'x'"
