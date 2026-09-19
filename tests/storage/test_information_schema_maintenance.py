# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`information_schema.maintenance`: whether the platform keeps this
workspace's data compacted.

One row, about the workspace. The value is not stored anywhere - it is a WRITE
grant held by the platform's maintenance identity - so what this table has to
get right is that it ASKS rather than remembering, and that what it reports is
the same answer the compactor's own permission check would get.
"""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        count = len(next(iter(pydict.values()))) if pydict else 0
        for index in range(count):
            row = {}
            for key, values in pydict.items():
                value = values[index]
                if isinstance(value, bytes):
                    value = value.decode()
                row[key] = value
            rows.append(row)
    return rows


class _FakeCatalog:
    calls = []

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["ops"]

    def list_datasets(self, collection):
        return ["audit_log"]

    def list_views(self, collection):
        return []

    def dataset_exists(self, identifier):
        return True

    def get_relation(self, identifier):
        return (None, None)


class _ScriptedCapability:
    """Answers the maintenance question and records that it was asked."""

    name = "scripted"

    def __init__(self, enabled=False):
        self.enabled = enabled
        self.asked = []
        self.set_calls = []

    def can_perform_action(self, execution_context, resource, action):
        return True

    def can_perform_workspace_action(self, execution_context, workspace, action):
        return True

    def can_principal_perform_action(self, principal, resource, action):
        return True

    def can_principal_own_materialized_view(self, principal):
        return True

    def grants(self, identity, policies):
        return [{"pattern": "*", "level": "", "role": "*", "actions": "*"}]

    def apply_grant(self, execution_context, pattern, role, principal):
        raise AssertionError("apply_grant should not be reached by these tests")

    def apply_revoke(self, execution_context, pattern, role, principal):
        raise AssertionError("apply_revoke should not be reached by these tests")

    def grants_on(self, execution_context, pattern):
        raise AssertionError("grants_on should not be reached by these tests")

    def effective_grants_on(self, execution_context, pattern):
        raise AssertionError("effective_grants_on should not be reached by these tests")

    def effective_grants_in(self, execution_context, workspace, objects):
        raise AssertionError("effective_grants_in should not be reached by these tests")

    def set_workspace_maintenance(self, execution_context, workspace, enabled):
        self.set_calls.append((workspace, enabled))
        self.enabled = enabled

    def workspace_maintenance(self, execution_context, workspace):
        self.asked.append(workspace)
        return self.enabled


@pytest.fixture
def permissions_state():
    from opteryx import managers

    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield module
    module._active, module._consulted = saved_active, saved_consulted


@pytest.fixture
def capability(permissions_state):
    from opteryx.managers.permissions import register_permissions_capability

    installed = _ScriptedCapability()
    permissions_state._active, permissions_state._consulted = permissions_state._CORE, False
    register_permissions_capability(installed)
    return installed


@pytest.fixture
def catalog_workspace():
    register_workspace("cat", OpteryxConnector, catalog=_FakeCatalog)
    return _FakeCatalog


def _read(user="alice"):
    session = opteryx.session(user=user, access_policies=_OWNER_POLICY)
    return _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.maintenance")
    )


def test_one_row_about_the_workspace(catalog_workspace, capability):
    """Not a row per dataset. The setting is held at the workspace and
    everything inside inherits it, so a row per object would be a listing whose
    length was its only content."""
    assert _read() == [{"catalog_name": "cat", "maintenance": False}]


def test_it_reports_what_the_capability_says(catalog_workspace, capability):
    capability.enabled = True

    assert _read() == [{"catalog_name": "cat", "maintenance": True}]
    assert capability.asked == ["cat"]


def test_it_asks_every_time_rather_than_remembering(catalog_workspace, capability):
    """There is no stored flag to cache. A value held here could disagree with
    the policy that actually decides, and the disagreement would be invisible
    until a compaction everyone believed was running turned out not to be."""
    _read()
    capability.enabled = True

    assert _read() == [{"catalog_name": "cat", "maintenance": True}]
    assert capability.asked == ["cat", "cat"]


def test_setting_it_is_visible_here(catalog_workspace, capability):
    """The statement and the table are two ends of one piece of state."""
    session = opteryx.session(user="alice", access_policies=_OWNER_POLICY)
    list(session.execute_to_morsels("ALTER WORKSPACE cat SET maintenance TO ON"))

    assert capability.set_calls == [("cat", True)]
    assert _read() == [{"catalog_name": "cat", "maintenance": True}]


def test_it_is_readable_without_owner_authority(catalog_workspace, capability):
    """"Is my data being maintained" is a property of the workspace, not a fact
    about who holds access to it, and this names no principal. The grant
    underneath stays visible only in `grants`, which is owner-only."""
    capability.enabled = True
    session = opteryx.session(user="mallory", access_policies=[])

    rows = _morsels_to_rows(
        session.execute_to_morsels("SELECT * FROM cat.information_schema.maintenance")
    )

    assert rows == [{"catalog_name": "cat", "maintenance": True}]
