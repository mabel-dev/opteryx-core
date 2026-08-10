# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Egress protection: a write may not copy another workspace's data into this one.

The decision belongs to the catalog (`egress_protection` on the SOURCE
workspace, on unless explicitly turned off). What is tested here is the engine's
half: turning relation names into workspaces, asking only when a boundary is
actually crossed, and failing closed rather than open when it cannot ask.
"""

import pytest

from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import EgressRestrictedError


class _FakeCatalog:
    """Stands in for opteryx_catalog, recording what it was asked."""

    calls = []
    restricted_workspaces = set()

    def __init__(self, workspace=None, **kwargs):
        pass

    def enforce_egress_policy(self, source_workspaces, destination_workspace, operation):
        from opteryx_catalog.exceptions import EgressRestricted

        source_workspaces = list(source_workspaces)
        _FakeCatalog.calls.append((source_workspaces, destination_workspace, operation))
        for source in source_workspaces:
            if source in _FakeCatalog.restricted_workspaces:
                raise EgressRestricted(
                    f"Cannot {operation}: it would copy data out of workspace "
                    f"'{source}' into '{destination_workspace}', and '{source}' "
                    f"restricts egress. Clear it with ALTER WORKSPACE {source} "
                    "SET egress_protection TO OFF."
                )


@pytest.fixture(autouse=True)
def catalog_signals_egress(monkeypatch):
    """Make `opteryx_catalog.exceptions.EgressRestricted` present.

    opteryx resolves opteryx-catalog from site-packages, not from a sibling
    checkout, so on a machine running the released catalog this exception does
    not exist yet - and its absence is itself a tested behaviour
    (`test_a_catalog_too_old_to_answer_fails_closed`, via a catalog object with
    no gate on it). Standing one in here keeps the *enforcing* tests about
    enforcement rather than about which catalog happens to be installed, and
    they go on testing the real thing once it is.
    """
    import opteryx_catalog.exceptions as catalog_exceptions

    if not hasattr(catalog_exceptions, "EgressRestricted"):

        class EgressRestricted(Exception):
            pass

        monkeypatch.setattr(
            catalog_exceptions, "EgressRestricted", EgressRestricted, raising=False
        )


@pytest.fixture
def connector():
    _FakeCatalog.calls = []
    _FakeCatalog.restricted_workspaces = set()
    register_workspace("mine", OpteryxConnector, catalog=_FakeCatalog)
    register_workspace("landing", OpteryxConnector, catalog=_FakeCatalog)
    return connector_factory("mine.mart.copy", telemetry=None)


def test_same_workspace_sources_never_reach_the_catalog(connector):
    """A copy that stays inside one workspace is not egress. It is also the
    overwhelmingly common case, so it must not cost a Firestore read."""
    connector.enforce_egress_policy("mine.mart.copy", ["mine.src.a", "mine.src.b"])

    assert _FakeCatalog.calls == []


def test_no_sources_at_all_never_reaches_the_catalog(connector):
    """INSERT ... VALUES scans nothing."""
    connector.enforce_egress_policy("mine.mart.copy", [])

    assert _FakeCatalog.calls == []


def test_cross_workspace_source_is_refused_when_protected(connector):
    _FakeCatalog.restricted_workspaces = {"landing"}

    with pytest.raises(
        EgressRestrictedError, match="ALTER WORKSPACE landing SET egress_protection TO OFF"
    ):
        connector.enforce_egress_policy("mine.mart.copy", ["landing.events.raw"])


def test_cross_workspace_source_is_allowed_when_not_protected(connector):
    connector.enforce_egress_policy("mine.mart.copy", ["landing.events.raw"])

    assert _FakeCatalog.calls == [(["landing"], "mine", "write mine.mart.copy")]


def test_only_the_crossing_sources_are_asked_about(connector):
    """Same-workspace sources are dropped before asking, and each foreign
    workspace is asked about once however many of its tables are read."""
    connector.enforce_egress_policy(
        "mine.mart.copy",
        ["mine.src.a", "landing.events.raw", "landing.events.other", "mine.src.b"],
    )

    assert _FakeCatalog.calls == [(["landing"], "mine", "write mine.mart.copy")]


def test_a_catalog_too_old_to_answer_fails_closed(connector):
    """Version skew must not silently disable the control - refusing a valid
    copy is recoverable, an unenforced security boundary is not."""

    class _OldCatalog:
        def __init__(self, workspace=None, **kwargs):
            pass

    register_workspace("old", OpteryxConnector, catalog=_OldCatalog)
    old_connector = connector_factory("old.mart.copy", telemetry=None)

    with pytest.raises(EgressRestrictedError, match="too old to evaluate egress protection"):
        old_connector.enforce_egress_policy("old.mart.copy", ["landing.events.raw"])


def test_writable_default_is_a_no_op():
    """A connector with no workspace concept has no boundary to cross, so the
    base capability returns rather than raising NotImplementedError like its
    neighbours - otherwise every filesystem CTAS would break."""

    class _PlainWritable(Writable):
        pass

    assert _PlainWritable().enforce_egress_policy("a.b", ["c.d"]) is None


# --- the binder actually calls it ---------------------------------------
#
# The tests above check the connector's decision in isolation. These check the
# wiring: that every write path really does consult it. Without them the
# enforcement call could be deleted from the binder and the suite would stay
# green - which was true until they were written.


@pytest.fixture
def recording_workspaces(tmp_path, monkeypatch):
    """Two local-store workspaces whose egress hook records instead of no-opping."""
    import opteryx
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    calls = []

    def record(self, target_relation, source_relations):
        calls.append((target_relation, list(source_relations)))

    monkeypatch.setattr(LocalStoreConnector, "enforce_egress_policy", record)
    register_workspace("src_ws", LocalStoreConnector, store_root=str(tmp_path / "a"))
    register_workspace("dst_ws", LocalStoreConnector, store_root=str(tmp_path / "b"))

    session = opteryx.session(user="alice", access_policies=[{"pattern": "*", "role": "owner"}])
    list(session.execute_to_morsels("CREATE TABLE src_ws.src (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO src_ws.src VALUES (1), (2)"))
    calls.clear()
    return session, calls


def test_ctas_consults_the_egress_hook(recording_workspaces):
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst AS SELECT a FROM src_ws.src"))

    assert calls == [("dst_ws.dst", ["src_ws.src"])]


def test_insert_select_consults_the_egress_hook(recording_workspaces):
    """INSERT copies as durably as CTAS - covering only CREATE would leave the
    boundary two statements from being bypassed."""
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    calls.clear()

    list(session.execute_to_morsels("INSERT INTO dst_ws.dst SELECT a FROM src_ws.src"))

    assert calls == [("dst_ws.dst", ["src_ws.src"])]


def test_refresh_materialized_view_consults_the_egress_hook(recording_workspaces):
    """A refresh is a write like any other and is checked like one - the flag
    that lets REFRESH through the not-a-table guard does not exempt it here."""
    session, calls = recording_workspaces
    list(
        session.execute_to_morsels(
            "CREATE MATERIALIZED VIEW src_ws.mv AS SELECT a FROM src_ws.src"
        )
    )
    calls.clear()

    list(session.execute_to_morsels("REFRESH MATERIALIZED VIEW src_ws.mv"))

    assert calls == [("src_ws.mv", ["src_ws.src"])]


def test_a_refusal_from_the_hook_aborts_the_write(recording_workspaces, monkeypatch):
    """Refused at bind time, so the target is never created."""
    import opteryx
    from opteryx.connectors import connector_factory
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    session, _calls = recording_workspaces

    def refuse(self, target_relation, source_relations):
        raise EgressRestrictedError("egress protection: refused for the test")

    monkeypatch.setattr(LocalStoreConnector, "enforce_egress_policy", refuse)

    with pytest.raises(EgressRestrictedError, match="refused for the test"):
        list(session.execute_to_morsels("CREATE TABLE dst_ws.dst AS SELECT a FROM src_ws.src"))

    assert not connector_factory("dst_ws.dst", telemetry=None).relation_exists("dst_ws.dst")


def test_non_catalog_sources_are_not_offered_to_the_hook(recording_workspaces):
    """$planets belongs to no workspace, so it cannot leave one."""
    session, calls = recording_workspaces

    list(session.execute_to_morsels("CREATE TABLE dst_ws.planets AS SELECT id FROM $planets"))

    assert calls == []


def test_insert_values_never_reaches_the_hook(recording_workspaces):
    """A VALUES insert scans nothing, so there is no source to protect."""
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    calls.clear()

    list(session.execute_to_morsels("INSERT INTO dst_ws.dst VALUES (5)"))

    assert calls == []
