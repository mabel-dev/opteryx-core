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

import importlib.util
import os

import pytest

from opteryx.connectors import connector_factory
from opteryx.connectors import register_workspace
from opteryx.connectors.capabilities import Writable
from opteryx.connectors.capabilities.writable import EgressRefusal
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import EgressRestrictedError


class _FakeRefusal:
    """The shape `opteryx_catalog.EgressRefusal` presents to the connector.

    Duck-typed rather than imported, so the tests below stay about the engine's
    half and run at the speed of not constructing a catalog. What the connector
    actually reads is `workspace`, `remediation`, and `str()` - so that is what
    this provides, and `test_the_fake_has_the_real_types_surface` is what stops
    it quietly drifting away from the thing it stands in for.
    """

    def __init__(self, workspace, destination, operation, secured=None):
        self.workspace = workspace
        self.destination = destination
        self.operation = operation
        self.secured = secured

    @property
    def remediation(self):
        if self.secured:
            narrow = (
                f"ALTER WORKSPACE {self.workspace} SET SECURE {self.secured} "
                f"TO {self.destination}"
            )
        else:
            narrow = (
                f"ALTER WORKSPACE {self.workspace} SET SECURE <object> TO {self.destination} "
                "(for a task or materialized view)"
            )
        return (
            f"{narrow}, or clear it for every copy with "
            f"ALTER WORKSPACE {self.workspace} SET egress_protection TO OFF."
        )

    def __str__(self):
        return (
            f"Cannot {self.operation}: it would copy data out of workspace "
            f"'{self.workspace}' into '{self.destination}', and "
            f"'{self.workspace}' restricts egress. Sanction it with {self.remediation}"
        )


class _FakeCatalog:
    """Stands in for opteryx_catalog, recording what it was asked."""

    calls = []
    restricted_workspaces = set()

    def __init__(self, workspace=None, **kwargs):
        pass

    secure_calls = []

    def egress_verdict(self, source_workspaces, destination_workspace, operation, secured=None):
        source_workspaces = list(source_workspaces)
        _FakeCatalog.calls.append((source_workspaces, destination_workspace, operation, secured))
        return [
            _FakeRefusal(source, destination_workspace, operation, secured)
            for source in source_workspaces
            if source in _FakeCatalog.restricted_workspaces
        ]

    def mark_secure(self, identifier, destinations, author=None):
        _FakeCatalog.secure_calls.append(("mark", identifier, list(destinations), author))

    def clear_secure(self, identifier, author=None, missing_ok=False):
        if identifier == "never.marked.anything":
            raise KeyError(identifier)
        _FakeCatalog.secure_calls.append(("clear", identifier, author))


@pytest.fixture
def connector():
    _FakeCatalog.calls = []
    _FakeCatalog.secure_calls = []
    _FakeCatalog.restricted_workspaces = set()
    register_workspace("mine", OpteryxConnector, catalog=_FakeCatalog)
    register_workspace("landing", OpteryxConnector, catalog=_FakeCatalog)
    return connector_factory("mine.mart.copy", telemetry=None)


def test_same_workspace_sources_never_reach_the_catalog(connector):
    """A copy that stays inside one workspace is not egress. It is also the
    overwhelmingly common case, so it must not cost a Firestore read."""
    assert connector.egress_verdict("mine.mart.copy", ["mine.src.a", "mine.src.b"]) == []

    assert _FakeCatalog.calls == []


def test_no_sources_at_all_never_reaches_the_catalog(connector):
    """INSERT ... VALUES scans nothing."""
    assert connector.egress_verdict("mine.mart.copy", []) == []

    assert _FakeCatalog.calls == []


def test_cross_workspace_source_is_refused_when_protected(connector):
    _FakeCatalog.restricted_workspaces = {"landing"}

    refusals = connector.egress_verdict("mine.mart.copy", ["landing.events.raw"])

    assert [refusal.workspace for refusal in refusals] == ["landing"]
    assert "ALTER WORKSPACE landing SET egress_protection TO OFF" in refusals[0].message


def test_cross_workspace_source_is_allowed_when_not_protected(connector):
    assert connector.egress_verdict("mine.mart.copy", ["landing.events.raw"]) == []

    assert _FakeCatalog.calls == [(["landing"], "mine", "write mine.mart.copy", None)]


def test_only_the_crossing_sources_are_asked_about(connector):
    """Same-workspace sources are dropped before asking, and each foreign
    workspace is asked about once however many of its tables are read."""
    connector.egress_verdict(
        "mine.mart.copy",
        ["mine.src.a", "landing.events.raw", "landing.events.other", "mine.src.b"],
    )

    assert _FakeCatalog.calls == [(["landing"], "mine", "write mine.mart.copy", None)]


def test_every_refusing_workspace_is_reported(connector):
    """Why this returns a list rather than raising at the first refusal: a join
    across three protected workspaces would otherwise take three failed
    statements to discover, and someone asking for access needs to ask once."""
    _FakeCatalog.restricted_workspaces = {"landing", "other"}

    refusals = connector.egress_verdict(
        "mine.mart.copy",
        ["landing.events.raw", "mine.src.a", "other.events.raw"],
    )

    assert [refusal.workspace for refusal in refusals] == ["landing", "other"]
    assert "ALTER WORKSPACE other SET egress_protection TO OFF." in refusals[1].remediation


def test_a_catalog_too_old_to_answer_fails_closed(connector):
    """Version skew must not silently disable the control - refusing a valid
    copy is recoverable, an unenforced security boundary is not. It raises
    rather than returning no refusals, because no refusals means permitted."""

    class _OldCatalog:
        def __init__(self, workspace=None, **kwargs):
            pass

    register_workspace("old", OpteryxConnector, catalog=_OldCatalog)
    old_connector = connector_factory("old.mart.copy", telemetry=None)

    with pytest.raises(EgressRestrictedError, match="too old to evaluate egress protection"):
        old_connector.egress_verdict("old.mart.copy", ["landing.events.raw"])


def test_writable_default_refuses_nothing():
    """A connector with no workspace concept has no boundary to cross, so the
    base capability answers rather than raising NotImplementedError like its
    neighbours - otherwise every filesystem CTAS would break."""

    class _PlainWritable(Writable):
        pass

    assert _PlainWritable().egress_verdict("a.b", ["c.d"]) == []


def test_the_fake_has_the_real_types_surface():
    """A stand-in is only worth what its resemblance is worth.

    Every other test here reads `_FakeRefusal`; this is the one that reads the
    catalog's own type and checks the connector would find the same three things
    on it. Deliberately not skipped when the installed catalog is too old: core
    already refuses cross-workspace writes against such a catalog, so a loud
    failure here says the same thing the engine would say at runtime.
    """
    CatalogRefusal = _sibling_catalog_refusal_type()

    for secured in (None, "mine.ops.ingest"):
        real = CatalogRefusal(
            workspace="landing",
            destination="mine",
            operation="write mine.mart.copy",
            secured=secured,
        )
        fake = _FakeRefusal("landing", "mine", "write mine.mart.copy", secured)

        assert real.workspace == fake.workspace
        assert real.remediation == fake.remediation
        assert str(real) == str(fake)


def _sibling_catalog_refusal_type():
    """`EgressRefusal` from the sibling ../opteryx-catalog checkout, by path.

    Loaded under its own module name rather than imported, because whatever
    `opteryx_catalog` is already in sys.modules may be a stale site-packages
    wheel - and this test exists to compare against the type the engine will
    actually ship beside, which is the sibling repo (release order is catalog,
    then core). Importing by name would compare the fake against last month's
    wording and report drift that is not there, or miss drift that is.
    """
    repo = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
    )
    package_dir = os.path.join(repo, "opteryx_catalog")
    if not os.path.isdir(package_dir):
        raise AssertionError(
            f"sibling opteryx-catalog checkout not found at {repo}; this test compares "
            "against the catalog source, not an installed wheel"
        )
    spec = importlib.util.spec_from_file_location(
        "_sibling_opteryx_catalog",
        os.path.join(package_dir, "__init__.py"),
        submodule_search_locations=[package_dir],
    )
    package = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = package
    spec.loader.exec_module(package)
    return importlib.import_module(f"{spec.name}.opteryx_catalog").EgressRefusal


def test_the_refusal_is_the_engines_own_type(connector):
    """Translated at the connector boundary, so no catalog type travels into
    the engine - the same rule ViewDefinition and Manifest follow."""
    _FakeCatalog.restricted_workspaces = {"landing"}

    refusals = connector.egress_verdict("mine.mart.copy", ["landing.events.raw"])

    assert isinstance(refusals[0], EgressRefusal)


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

    def record(self, target_relation, source_relations, secured=None):
        calls.append((target_relation, list(source_relations), secured))
        return []

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", record)
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

    assert calls == [("dst_ws.dst", ["src_ws.src"], None)]


def test_insert_select_consults_the_egress_hook(recording_workspaces):
    """INSERT copies as durably as CTAS - covering only CREATE would leave the
    boundary two statements from being bypassed."""
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    calls.clear()

    list(session.execute_to_morsels("INSERT INTO dst_ws.dst SELECT a FROM src_ws.src"))

    assert calls == [("dst_ws.dst", ["src_ws.src"], None)]


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

    assert calls == [("src_ws.mv", ["src_ws.src"], None)]


def test_a_refusal_from_the_hook_aborts_the_write(recording_workspaces, monkeypatch):
    """Refused at bind time, so the target is never created."""
    import opteryx
    from opteryx.connectors import connector_factory
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    session, _calls = recording_workspaces

    def refuse(self, target_relation, source_relations, secured=None):
        return [
            EgressRefusal(
                workspace="src_ws",
                remediation="ALTER WORKSPACE src_ws SET egress_protection TO OFF.",
                message="egress protection: refused for the test",
            )
        ]

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", refuse)

    with pytest.raises(EgressRestrictedError, match="refused for the test"):
        list(session.execute_to_morsels("CREATE TABLE dst_ws.dst AS SELECT a FROM src_ws.src"))

    assert not connector_factory("dst_ws.dst", telemetry=None).relation_exists("dst_ws.dst")


def test_a_single_refusal_is_reported_as_the_catalog_worded_it(monkeypatch, recording_workspaces):
    """One refusal passes through untouched. Only several are composed, so the
    ordinary case reads exactly as the store wrote it."""
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    session, _calls = recording_workspaces

    def refuse(self, target_relation, source_relations, secured=None):
        return [EgressRefusal(workspace="src_ws", remediation="clear it", message="ONLY THIS")]

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", refuse)

    with pytest.raises(EgressRestrictedError, match=r"^ONLY THIS$"):
        list(session.execute_to_morsels("CREATE TABLE dst_ws.dst AS SELECT a FROM src_ws.src"))


def test_several_refusals_are_reported_together(monkeypatch, recording_workspaces):
    """The point of the verdict reaching the binder: every workspace that has
    to be cleared is named once, not one failed statement at a time."""
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    session, _calls = recording_workspaces

    def refuse(self, target_relation, source_relations, secured=None):
        return [
            EgressRefusal(workspace="alpha", remediation="ALTER WORKSPACE alpha ...", message="a"),
            EgressRefusal(workspace="beta", remediation="ALTER WORKSPACE beta ...", message="b"),
        ]

    monkeypatch.setattr(LocalStoreConnector, "egress_verdict", refuse)

    with pytest.raises(EgressRestrictedError) as exc:
        list(session.execute_to_morsels("CREATE TABLE dst_ws.dst AS SELECT a FROM src_ws.src"))

    message = str(exc.value)
    assert "'alpha', 'beta'" in message
    assert "ALTER WORKSPACE alpha ..." in message and "ALTER WORKSPACE beta ..." in message


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


# --- SECURE: the object-level exemption -----------------------------------
#
# The catalog decides whether a source has sanctioned an object. The engine's
# half is naming that object: a task EXECUTE expands carries its identifier onto
# the write, and nothing else does - a statement typed by hand names no object a
# source could have sanctioned.


def test_the_object_is_handed_to_the_catalog(connector):
    connector.egress_verdict(
        "mine.mart.copy", ["landing.events.raw"], secured="mine.ops.ingest"
    )

    assert _FakeCatalog.calls == [
        (["landing"], "mine", "write mine.mart.copy", "mine.ops.ingest")
    ]


def test_a_refusal_names_the_exact_secure_statement(connector):
    """The advice is the narrow key first, the whole-building key second."""
    _FakeCatalog.restricted_workspaces = {"landing"}

    (refusal,) = connector.egress_verdict(
        "mine.mart.copy", ["landing.events.raw"], secured="mine.ops.ingest"
    )

    assert "ALTER WORKSPACE landing SET SECURE mine.ops.ingest TO mine" in refusal.remediation
    assert "ALTER WORKSPACE landing SET egress_protection TO OFF" in refusal.remediation
    assert refusal.remediation.index("SET SECURE") < refusal.remediation.index(
        "egress_protection"
    )


def test_an_anonymous_copy_gets_the_template(connector):
    _FakeCatalog.restricted_workspaces = {"landing"}

    (refusal,) = connector.egress_verdict("mine.mart.copy", ["landing.events.raw"])

    assert "SET SECURE <object> TO mine" in refusal.remediation


def test_a_hand_typed_insert_names_no_object(recording_workspaces):
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    calls.clear()

    list(session.execute_to_morsels("INSERT INTO dst_ws.dst SELECT a FROM src_ws.src"))

    assert calls == [("dst_ws.dst", ["src_ws.src"], None)]


def test_an_executed_task_names_itself(recording_workspaces):
    """The statement the task expands to is, textually, the INSERT above. What
    distinguishes it is that a task ran it - and that is what a source can have
    marked SECURE, so that is what reaches the gate."""
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    list(
        session.execute_to_morsels(
            "CREATE TASK src_ws.copier AS INSERT INTO dst_ws.dst SELECT a FROM src_ws.src"
        )
    )
    calls.clear()

    list(session.execute_to_morsels("EXECUTE src_ws.copier"))

    assert calls == [("dst_ws.dst", ["src_ws.src"], "src_ws.copier")]


def test_registering_a_task_names_no_object(recording_workspaces):
    """CREATE TASK stores SQL and copies nothing; only running it does. If the
    registration consulted the gate at all it would do so anonymously."""
    session, calls = recording_workspaces
    list(session.execute_to_morsels("CREATE TABLE dst_ws.dst (a BIGINT)"))
    calls.clear()

    list(
        session.execute_to_morsels(
            "CREATE TASK src_ws.copier AS INSERT INTO dst_ws.dst SELECT a FROM src_ws.src"
        )
    )

    assert all(secured is None for _, _, secured in calls)


# --- SECURE: recording the sanction ----------------------------------------


def test_marking_secure_writes_the_source_workspaces_record(connector):
    """The handle used is the SOURCE's. That is the whole enforcement of "only
    the source may sanction": the catalog writes its own workspace's entry."""
    source = connector_factory("landing.events.raw", telemetry=None)

    source.mark_workspace_secure(
        "landing", "mine.ops.ingest", ["mine", "other"], author="landing-owner"
    )

    assert _FakeCatalog.secure_calls == [
        ("mark", "mine.ops.ingest", ["mine", "other"], "landing-owner")
    ]


def test_clearing_secure_withdraws_it(connector):
    source = connector_factory("landing.events.raw", telemetry=None)

    source.clear_workspace_secure("landing", "mine.ops.ingest", author="landing-owner")

    assert _FakeCatalog.secure_calls == [("clear", "mine.ops.ingest", "landing-owner")]


def test_clearing_something_never_marked_is_a_caller_error(connector):
    source = connector_factory("landing.events.raw", telemetry=None)

    with pytest.raises(ValueError, match="not marked SECURE"):
        source.clear_workspace_secure("landing", "never.marked.anything")


def test_a_catalog_too_old_to_hold_secure_refuses_to_pretend(connector):
    """A statement that reported success while recording nothing would leave
    the operator believing the pipeline was unblocked."""

    class _OldCatalog:
        def __init__(self, workspace=None, **kwargs):
            pass

    register_workspace("old", OpteryxConnector, catalog=_OldCatalog)
    old = connector_factory("old.mart.copy", telemetry=None)

    with pytest.raises(NotImplementedError, match="too old to hold SECURE"):
        old.mark_workspace_secure("old", "mine.ops.ingest", ["mine"])
    with pytest.raises(NotImplementedError, match="too old to hold SECURE"):
        old.clear_workspace_secure("old", "mine.ops.ingest")
