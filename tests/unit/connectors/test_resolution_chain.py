"""
The resolution chain in opteryx.connectors.connector_factory.

Covers the contract WORKSPACE_CATALOG_RESOLUTION.md (opteryx-catalog repo)
pins for the resolution-first rewrite:

- chain precedence: static table beats resolver beats static default beats disk
- the two hard requirements have explicit homes: disk is the terminal
  fallback, and a settable global default still works
- version compare: same version reuses the instance, a bumped version
  rebuilds and REPLACES it
- resolver exceptions propagate (never swallowed into a fallback)
- resolver config may be an arbitrarily nested dict and reaches the
  connector intact
- non-identifier dataset names (gs://... protocol paths) skip the resolver
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx import connectors
from opteryx.connectors import Resolution
from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors import set_default_connector
from opteryx.connectors import set_workspace_resolver


class StubConnector:
    """Records what it was constructed with; ignores everything else."""

    def __init__(self, telemetry=None, **kwargs):
        self.telemetry = telemetry
        self.kwargs = kwargs


class OtherStubConnector(StubConnector):
    pass


@pytest.fixture
def clean_registry():
    """Save and restore the module-level registry state around each test."""
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_default = connectors._default_connector
    saved_resolver = connectors._workspace_resolver
    saved_settings_resolver = connectors._workspace_settings_resolver
    saved_cache = dict(connectors._connector_cache)
    saved_versions = dict(connectors._connector_versions)

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
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._default_connector = saved_default
        connectors._workspace_resolver = saved_resolver
        connectors._workspace_settings_resolver = saved_settings_resolver
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)
        connectors._connector_versions.clear()
        connectors._connector_versions.update(saved_versions)


def test_disk_is_terminal_fallback(clean_registry):
    # Hard requirement 1: nothing registered, no default, no resolver -> disk.
    connector = connector_factory("nowhere.some.table", telemetry=None)
    assert type(connector).__name__ == "FileSystemConnector"
    assert connector.storage_type == "LOCAL"


def test_static_default_still_works(clean_registry):
    # Hard requirement 2: a settable global default.
    set_default_connector(StubConnector, marker="default")
    connector = connector_factory("anything.at.all", telemetry=None)
    assert isinstance(connector, StubConnector)
    assert connector.kwargs["marker"] == "default"


def test_static_table_beats_resolver(clean_registry):
    register_workspace("ws", StubConnector, marker="static")
    set_workspace_resolver(
        lambda workspace: Resolution(OtherStubConnector, {"marker": "resolved"})
    )
    connector = connector_factory("ws.table", telemetry=None)
    assert isinstance(connector, StubConnector)
    assert connector.kwargs["marker"] == "static"


def test_resolver_beats_static_default(clean_registry):
    set_default_connector(StubConnector, marker="default")
    set_workspace_resolver(
        lambda workspace: Resolution(OtherStubConnector, {"marker": "resolved"})
    )
    connector = connector_factory("ws.table", telemetry=None)
    assert isinstance(connector, OtherStubConnector)
    assert connector.kwargs["marker"] == "resolved"


def test_resolver_none_falls_through_to_default(clean_registry):
    set_default_connector(StubConnector, marker="default")
    set_workspace_resolver(lambda workspace: None)
    connector = connector_factory("ws.table", telemetry=None)
    assert isinstance(connector, StubConnector)
    assert connector.kwargs["marker"] == "default"


def test_resolver_receives_first_segment_only(clean_registry):
    seen = []

    def resolver(workspace):
        seen.append(workspace)
        return Resolution(StubConnector)

    set_workspace_resolver(resolver)
    connector_factory("tarchia.interop_ns.people", telemetry=None)
    assert seen == ["tarchia"]


def test_resolver_exception_propagates(clean_registry):
    class ResolutionFailure(Exception):
        pass

    def resolver(workspace):
        raise ResolutionFailure("registry unreachable")

    set_default_connector(StubConnector)  # must NOT be fallen back to
    set_workspace_resolver(resolver)
    with pytest.raises(ResolutionFailure):
        connector_factory("ws.table", telemetry=None)


def test_nested_config_reaches_connector_intact(clean_registry):
    nested = {"auth": {"type": "google", "google": {"scopes": ["a", "b"]}}, "uri": "https://x"}
    set_workspace_resolver(lambda workspace: Resolution(StubConnector, dict(nested)))
    connector = connector_factory("ws.table", telemetry=None)
    assert connector.kwargs["auth"] == nested["auth"]
    assert connector.kwargs["uri"] == "https://x"


def test_same_version_reuses_instance(clean_registry):
    set_workspace_resolver(lambda workspace: Resolution(StubConnector, {"a": 1}, version=7))
    first = connector_factory("ws.table", telemetry=None)
    second = connector_factory("ws.other_table", telemetry=None)
    assert first is second


def test_bumped_version_rebuilds_and_replaces(clean_registry):
    version = {"n": 1}
    set_workspace_resolver(
        lambda workspace: Resolution(StubConnector, {"a": 1}, version=version["n"])
    )
    first = connector_factory("ws.table", telemetry=None)
    version["n"] = 2
    second = connector_factory("ws.table", telemetry=None)
    assert first is not second
    # replaced, not accumulated: exactly one cached instance for the workspace
    assert connectors._connector_cache["ws"] is second


def test_static_registration_rotates_on_reregistration(clean_registry):
    # The config-fingerprint version preserves the old behavior where
    # re-registering with different kwargs produced a fresh instance.
    register_workspace("ws", StubConnector, bucket="one")
    first = connector_factory("ws.table", telemetry=None)
    register_workspace("ws", StubConnector, bucket="one")
    assert connector_factory("ws.table", telemetry=None) is first
    register_workspace("ws", StubConnector, bucket="two")
    second = connector_factory("ws.table", telemetry=None)
    assert second is not first
    assert second.kwargs["bucket"] == "two"


def test_default_rotates_on_config_change(clean_registry):
    # The worker/odata pattern: set_default_connector re-called with fresh
    # config must take effect on the next lookup, unchanged config must not
    # rotate the instance.
    set_default_connector(StubConnector, gcs_bucket="one")
    first = connector_factory("ws.table", telemetry=None)
    set_default_connector(StubConnector, gcs_bucket="one")
    assert connector_factory("ws.table", telemetry=None) is first
    set_default_connector(StubConnector, gcs_bucket="two")
    second = connector_factory("ws.table", telemetry=None)
    assert second is not first
    assert second.kwargs["gcs_bucket"] == "two"


def test_protocol_paths_skip_resolver(clean_registry):
    def resolver(workspace):  # pragma: no cover - must not be called
        raise AssertionError("resolver must not see protocol paths")

    set_workspace_resolver(resolver)
    connector = connector_factory("gs://bucket/path", telemetry=None)
    assert type(connector).__name__ == "FileSystemConnector"


def test_resolver_must_return_resolution_or_none(clean_registry):
    set_workspace_resolver(lambda workspace: {"connector": StubConnector})
    with pytest.raises(ValueError, match="Resolution or None"):
        connector_factory("ws.table", telemetry=None)


def test_resolution_rejects_instantiated_connector():
    with pytest.raises(ValueError, match="uninstantiated"):
        Resolution(StubConnector())


def test_dollar_datasets_bypass_the_chain(clean_registry):
    def resolver(workspace):  # pragma: no cover - must not be called
        raise AssertionError("resolver must not see virtual datasets")

    set_workspace_resolver(resolver)
    connector = connector_factory("$planets", telemetry=None)
    assert type(connector).__name__ == "VirtualDataConnector"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
