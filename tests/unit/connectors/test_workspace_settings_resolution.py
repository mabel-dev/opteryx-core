"""
The workspace SETTINGS resolution seam.

There are two different questions about a workspace, and this module exists
because one call site used to answer both:

- which connector serves its DATA (external binding, may need that binding's
  stored credential) -- `connector_factory` / `set_workspace_resolver`
- which catalog holds its SETTINGS and lifecycle (always the opteryx catalog
  entry, never needs a credential) -- `workspace_settings_connector` /
  `set_workspace_settings_resolver`

Conflating them meant `ALTER WORKSPACE ... SET egress_protection` decrypted an
Iceberg credential to reach a property sitting in the very Firestore document
the binding lives in, so a workspace whose stored secret had gone bad could
not be repaired OR dropped through SQL -- the repairing statement died at bind
time, before the permission check, on a decrypt it never needed.

What is pinned here is that the settings path never consults the data
resolver, that the two answers cannot collide in the connector cache, and
that a deployment cannot half-install the seam.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx import connectors
from opteryx.connectors import Resolution
from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors import set_workspace_resolver
from opteryx.connectors import set_workspace_settings_resolver
from opteryx.connectors import workspace_settings_connector


class StubConnector:
    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry
        self.kwargs = kwargs


class SettingsConnector(StubConnector):
    pass


class CredentialRequired(Exception):
    """Stands in for the KMS decrypt the data resolver performs."""


def _data_resolver_that_needs_a_credential(workspace):
    raise CredentialRequired(
        f"stored credential for workspace {workspace!r} could not be decrypted"
    )


@pytest.fixture
def clean_registry():
    saved = (
        dict(connectors._storage_prefixes),
        connectors._default_connector,
        connectors._workspace_resolver,
        connectors._workspace_settings_resolver,
        dict(connectors._connector_cache),
        dict(connectors._connector_versions),
    )
    connectors._storage_prefixes.clear()
    connectors._default_connector = None
    connectors._workspace_resolver = None
    connectors._workspace_settings_resolver = None
    connectors._connector_cache.clear()
    connectors._connector_versions.clear()
    try:
        yield
    finally:
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved[0])
        connectors._default_connector = saved[1]
        connectors._workspace_resolver = saved[2]
        connectors._workspace_settings_resolver = saved[3]
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved[4])
        connectors._connector_versions.clear()
        connectors._connector_versions.update(saved[5])


# ---------------------------------------------------------------------------
# The bug this seam exists to prevent
# ---------------------------------------------------------------------------


def test_settings_never_consult_the_data_resolver(clean_registry):
    # The regression, stated directly: a workspace whose DATA binding cannot
    # be resolved (bad credential) still answers the SETTINGS question.
    set_workspace_resolver(_data_resolver_that_needs_a_credential)
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    connector = workspace_settings_connector("polaris_test", telemetry=None)

    assert isinstance(connector, SettingsConnector)
    assert connector.kwargs["marker"] == "settings"


def test_the_data_path_still_fails_for_the_same_workspace(clean_registry):
    # The other half of the point: fixing settings must NOT paper over a
    # genuinely unusable data binding. Reading the workspace's tables still
    # fails loudly.
    set_workspace_resolver(_data_resolver_that_needs_a_credential)
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    workspace_settings_connector("polaris_test", telemetry=None)
    with pytest.raises(CredentialRequired):
        connector_factory("polaris_test.some_table", telemetry=None)


# ---------------------------------------------------------------------------
# A deployment cannot half-install the seam
# ---------------------------------------------------------------------------


def test_data_resolver_without_a_settings_resolver_is_refused(clean_registry):
    # This combination IS the bug: bindings exist but nothing routes settings
    # away from them. Refused rather than silently reinstated.
    set_workspace_resolver(_data_resolver_that_needs_a_credential)

    with pytest.raises(ValueError) as raised:
        workspace_settings_connector("polaris_test", telemetry=None)
    assert "set_workspace_settings_resolver" in str(raised.value)


def test_neither_resolver_installed_uses_the_ordinary_chain(clean_registry):
    # Embedded use and tests: no bindings exist, so the data connector and the
    # settings connector are the same object and no credential is in play.
    register_workspace("ws", StubConnector, marker="static")

    connector = workspace_settings_connector("ws", telemetry=None)

    assert isinstance(connector, StubConnector)
    assert connector.kwargs["marker"] == "static"


def test_a_settings_resolver_returning_none_falls_through(clean_registry):
    register_workspace("ws", StubConnector, marker="static")
    set_workspace_settings_resolver(lambda workspace: None)

    connector = workspace_settings_connector("ws", telemetry=None)

    assert isinstance(connector, StubConnector)


# ---------------------------------------------------------------------------
# The two answers must not collide in the connector cache
# ---------------------------------------------------------------------------


def test_settings_and_data_connectors_do_not_share_a_cache_entry(clean_registry):
    # Same workspace NAME, legitimately different connectors. One cache key
    # for both would hand whichever asked second the other's answer.
    set_workspace_resolver(lambda workspace: Resolution(StubConnector, {"marker": "data"}))
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    settings = workspace_settings_connector("ws", telemetry=None)
    data = connector_factory("ws.table", telemetry=None)

    assert settings.kwargs["marker"] == "settings"
    assert data.kwargs["marker"] == "data"
    assert settings is not data

    # and re-asking in the other order still gets each its own answer
    assert connector_factory("ws.table", telemetry=None) is data
    assert workspace_settings_connector("ws", telemetry=None) is settings


def test_settings_connector_is_cached_and_rotates_on_version(clean_registry):
    versions = iter([1, 1, 2])
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "s"}, version=next(versions))
    )

    first = workspace_settings_connector("ws", telemetry=None)
    assert workspace_settings_connector("ws", telemetry=None) is first
    assert workspace_settings_connector("ws", telemetry=None) is not first


# ---------------------------------------------------------------------------
# Ordinary contract
# ---------------------------------------------------------------------------


def test_settings_resolver_exceptions_propagate(clean_registry):
    def boom(workspace):
        raise RuntimeError("registry unreachable")

    set_workspace_settings_resolver(boom)
    with pytest.raises(RuntimeError):
        workspace_settings_connector("ws", telemetry=None)


def test_settings_resolver_must_return_a_resolution(clean_registry):
    set_workspace_settings_resolver(lambda workspace: {"connector": SettingsConnector})
    with pytest.raises(ValueError):
        workspace_settings_connector("ws", telemetry=None)


def test_settings_resolver_must_be_callable(clean_registry):
    with pytest.raises(ValueError):
        set_workspace_settings_resolver("not callable")


# ---------------------------------------------------------------------------
# information_schema is a question about the WORKSPACE, not about its data
# ---------------------------------------------------------------------------


def test_information_schema_resolves_through_the_settings_chain(clean_registry):
    # `<workspace>.information_schema.<table>` is the workspace's own metadata
    # - its grants, tasks, triggers, listeners and catalog listings - and none
    # of it lives in whatever the workspace's DATA is bound to. Grants are
    # answered by the registered permissions capability, which has no data
    # provider in it at all.
    set_workspace_resolver(lambda workspace: Resolution(StubConnector, {"marker": "data"}))
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    connector = connector_factory("aiven.information_schema.grants", telemetry=None)

    assert isinstance(connector, SettingsConnector)
    assert connector.kwargs["marker"] == "settings"


def test_information_schema_never_touches_the_data_binding(clean_registry):
    # Stronger than "the settings answer wins": the data resolver must not be
    # consulted, so the reserved schema is readable on a workspace whose
    # stored credential has gone bad - the same property ALTER WORKSPACE has.
    set_workspace_resolver(_data_resolver_that_needs_a_credential)
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    connector = connector_factory("aiven.information_schema.grants", telemetry=None)

    assert isinstance(connector, SettingsConnector)


def test_a_real_relation_in_the_same_workspace_still_uses_the_data_binding(clean_registry):
    # The routing is scoped to the reserved schema; everything else in the
    # workspace is still served by whatever its data is bound to.
    set_workspace_resolver(lambda workspace: Resolution(StubConnector, {"marker": "data"}))
    set_workspace_settings_resolver(
        lambda workspace: Resolution(SettingsConnector, {"marker": "settings"})
    )

    connector = connector_factory("aiven.public.orders", telemetry=None)

    assert connector.kwargs["marker"] == "data"


def test_information_schema_is_not_a_top_level_prefix(clean_registry):
    # It is a nested schema inside a workspace, never a workspace of its own,
    # so a leading `information_schema` segment is an ordinary name.
    register_workspace("information_schema", StubConnector, marker="static")

    connector = connector_factory("information_schema.tables", telemetry=None)

    assert connector.kwargs["marker"] == "static"


def test_information_schema_with_no_resolvers_uses_the_ordinary_chain(clean_registry):
    # Embedded use and tests: no bindings exist, so the settings chain lands
    # back on the same connector the data chain would have returned.
    register_workspace("ws", StubConnector, marker="static")

    connector = connector_factory("ws.information_schema.tables", telemetry=None)

    assert isinstance(connector, StubConnector)
    assert connector.kwargs["marker"] == "static"
