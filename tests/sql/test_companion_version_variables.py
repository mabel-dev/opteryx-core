"""
`@@opteryx_access_version`, `@@opteryx_catalog_version`, `@@opteryx_iceberg_version`.

`@@version`/`@@build` identify the engine; they say nothing about the access, catalog
or iceberg code installed beside it, and "which iceberg backend is this worker running"
had no queryable answer. Each reports its distribution's installed version, or NULL
when that distribution is not installed (opteryx-core depends on none of them).
"""

import importlib.metadata
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import PermissionsError

VARIABLES = {
    "opteryx_access_version": "opteryx-access",
    "opteryx_catalog_version": "opteryx-catalog",
    "opteryx_iceberg_version": "opteryx-iceberg",
}


def _installed(distribution):
    found = next(iter(importlib.metadata.distributions(name=distribution)), None)
    return None if found is None else found.version


def _admin_row(sql):
    session = opteryx.session(entitlements=["platform_admin"])
    for morsel in session.execute_to_morsels(sql):
        for row in morsel:
            return tuple(row)
    raise AssertionError(f"no rows from {sql}")


@pytest.mark.parametrize("variable, distribution", VARIABLES.items())
def test_reports_the_installed_version(variable, distribution):
    assert _admin_row(f"SELECT @@{variable}") == (_installed(distribution),)


@pytest.mark.parametrize("variable", VARIABLES)
def test_is_listed_by_show_variables(variable):
    session = opteryx.session(entitlements=["platform_admin"])
    names = {row[0] for morsel in session.execute_to_morsels("SHOW VARIABLES") for row in morsel}
    assert variable in names


@pytest.mark.parametrize("variable", VARIABLES)
def test_is_restricted_like_python_version(variable):
    """A fact about the DEPLOYMENT, so RESTRICTED alongside `python_version`."""
    session = opteryx.session(user="nobody", entitlements=[])
    with pytest.raises(PermissionsError):
        list(session.execute_to_morsels(f"SELECT @@{variable}"))


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
