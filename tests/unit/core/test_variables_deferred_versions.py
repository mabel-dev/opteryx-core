"""
The companion-version variables are looked up when READ, not when the engine starts.

`@@opteryx_access_version`, `@@opteryx_catalog_version` and `@@opteryx_iceberg_version`
each need a scan of the installed distributions (and `importlib.metadata` itself,
which drags in `email` and `zipfile`). That used to run while `opteryx.variables`
loaded — on the first query of every process — for three RESTRICTED variables almost
nothing reads. These tests pin the deferral: building sessions and running ordinary
queries never performs the lookup, the first read does, and a caller who is refused
the read cannot trigger it either.

The lookup is replaced with a counting fake, so nothing here depends on which
companion distributions happen to be installed.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
import opteryx.variables as variables_module
from opteryx.exceptions import PermissionsError
from opteryx.variables import SystemVariables

VERSION_VARIABLES = {
    "opteryx_access_version": "opteryx-access",
    "opteryx_catalog_version": "opteryx-catalog",
    "opteryx_iceberg_version": "opteryx-iceberg",
}


@pytest.fixture
def lookups(monkeypatch):
    """Replace the installed-version lookup; returns the list of names it was asked for."""
    calls = []

    def fake(distribution):
        calls.append(distribution)
        return f"9.9.9-{distribution}"

    monkeypatch.setattr(variables_module, "_installed_version", fake)
    return calls


def _rows(session, sql):
    return [tuple(row) for morsel in session.execute_to_morsels(sql) for row in morsel]


_FRESH_PROCESS = """
import importlib.metadata, json, sys
calls = []
real = importlib.metadata.distributions
def counting(*args, **kwargs):
    calls.append(kwargs.get("name"))
    return real(*args, **kwargs)
importlib.metadata.distributions = counting
sys.path.insert(0, {root!r})
import opteryx
session = opteryx.session(entitlements=["platform_admin"])
list(session.execute_to_morsels("SELECT COUNT(*) FROM $planets"))
before = list(calls)
list(session.execute_to_morsels("SELECT @@opteryx_catalog_version"))
print(json.dumps({{"before": before, "after": calls}}))
"""


def test_a_fresh_process_does_not_look_up_versions_until_read():
    # The fixture-based tests below cannot see import time: by the time a fixture
    # runs, `opteryx.variables` is already loaded. This one counts real lookups
    # from interpreter start, which is where the old cost lived.
    import subprocess

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    result = subprocess.run(
        [sys.executable, "-c", _FRESH_PROCESS.format(root=root)],
        capture_output=True,
        text=True,
        cwd=root,
        check=True,
    )
    seen = json.loads(result.stdout.strip().splitlines()[-1])
    assert seen["before"] == [], f"looked up before any read: {seen['before']}"
    assert "opteryx-catalog" in seen["after"]


def test_building_a_session_and_querying_does_not_look_up_versions(lookups):
    session = opteryx.session(entitlements=["platform_admin"])
    _rows(session, "SELECT COUNT(*) FROM $planets")
    _rows(session, "SELECT @@python_version")
    assert lookups == []


@pytest.mark.parametrize("variable, distribution", VERSION_VARIABLES.items())
def test_reading_a_version_variable_looks_it_up(lookups, variable, distribution):
    session = opteryx.session(entitlements=["platform_admin"])
    assert _rows(session, f"SELECT @@{variable}") == [(f"9.9.9-{distribution}",)]
    assert lookups == [distribution]


@pytest.mark.parametrize("variable", VERSION_VARIABLES)
def test_a_refused_read_does_not_trigger_the_lookup(lookups, variable):
    session = opteryx.session(user="nobody", entitlements=[])
    with pytest.raises(PermissionsError):
        _rows(session, f"SELECT @@{variable}")
    assert lookups == []


def test_the_value_is_resolved_once_per_container(lookups):
    container = SystemVariables.snapshot()
    first = container["opteryx_catalog_version"]
    assert container["opteryx_catalog_version"] == first
    assert container.details("opteryx_catalog_version")[1] == first
    assert lookups == ["opteryx-catalog"]


def test_details_never_exposes_the_placeholder(lookups):
    container = SystemVariables.snapshot()
    for variable, distribution in VERSION_VARIABLES.items():
        assert container.details(variable)[1] == f"9.9.9-{distribution}"


def test_show_variables_reports_resolved_versions(lookups):
    session = opteryx.session(entitlements=["platform_admin"])
    listed = {row[0]: row[1] for row in _rows(session, "SHOW VARIABLES")}
    for variable, distribution in VERSION_VARIABLES.items():
        assert listed[variable] == f"9.9.9-{distribution}"


def test_a_missing_distribution_reads_as_null(monkeypatch):
    monkeypatch.setattr(variables_module, "_installed_version", lambda distribution: None)
    session = opteryx.session(entitlements=["platform_admin"])
    assert _rows(session, "SELECT @@opteryx_iceberg_version") == [(None,)]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
