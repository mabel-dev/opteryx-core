# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Does the ENGINE apply a permissions capability correctly?

The capability itself decides who may do what; that is tested where the
capability lives. What is tested here is the half the engine owns, and it is
the half that makes the other half worth anything:

1. Every gated statement asks, rather than deciding for itself.
2. It asks the RIGHT question -- the right resource, and the right action.
   An engine that asked WRITE where it should ask DROP would let a writer
   destroy a relation no matter how correct the capability's answer was.
3. It honours the answer, in both directions.

`ScriptedCapability` below is driven by an explicit allow-list and records
every question, so a test can say "permit exactly this" and then assert what
the engine did with it. Nothing here restates a role or a policy pattern:
what a role confers is not the engine's business.
"""

import pytest

import opteryx
from opteryx import managers
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import InvalidConfigurationError
from opteryx.managers.permissions import PermitAll
from opteryx.managers.permissions import active_permissions_capability
from opteryx.managers.permissions import register_permissions_capability


class ScriptedCapability:
    """Permits exactly what it is told to, and remembers what it was asked."""

    name = "scripted"

    def __init__(self, allow=(), allow_workspace=(), rows=(), allow_local_read=True):
        self.allow = set(allow)
        self.allow_workspace = set(allow_workspace)
        self.rows = list(rows)
        self.allow_local_read = allow_local_read
        self.asked = []
        self.asked_workspace = []

    def can_perform_action(self, execution_context, resource, action):
        self.asked.append((resource, action))
        # A name with no dot is a local or internal relation - `$grants`,
        # `$planets`, a temporary table. The engine asks about these like any
        # other, so every real capability has to answer; opteryx_access allows
        # READ on them and nothing else. Mirrored here so a test can script
        # the interesting gates without also having to permit `$grants` to get
        # a `SHOW GRANTS` out. `test_internal_relations_are_asked_about_too`
        # pins the contract itself.
        if self.allow_local_read and "." not in resource:
            return action == "READ"
        return (resource, action) in self.allow

    def can_perform_workspace_action(self, execution_context, workspace, action):
        self.asked_workspace.append((workspace, action))
        return (workspace, action) in self.allow_workspace

    def grants(self, identity, policies):
        return self.rows


@pytest.fixture(autouse=True)
def permissions_state():
    """Put the module's capability state back after every test in this file.

    Autouse rather than opt-in: a test that merely RUNS a query marks the
    capability as consulted, so a test which installs nothing can still leave
    state behind that makes a later registration fail. Restoring
    unconditionally is the only version of this that cannot rot.
    """
    module = managers.permissions
    saved_active, saved_consulted = module._active, module._consulted
    yield
    module._active, module._consulted = saved_active, saved_consulted


@pytest.fixture
def install():
    """Install a capability for one test.

    The module refuses a capability once a check has been answered -- correct
    in a process, unworkable across tests in one interpreter -- so this resets
    the private state directly rather than adding a production reset hook that
    exists only for tests. `permissions_state` puts it back afterwards.
    """
    module = managers.permissions

    def _install(capability):
        module._active, module._consulted = module._CORE, False
        register_permissions_capability(capability)
        return capability

    return _install


def _workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _seed(tmp_path, install):
    """Create a table and a view with everything permitted, then hand back the
    installer so the test can swap in the capability it actually wants."""
    _workspace(tmp_path)
    permissive = install(
        ScriptedCapability(
            allow={("ws.t", "CREATE"), ("ws.v", "WRITE"), ("ws.t", "READ")},
        )
    )
    session = opteryx.session(user="olive")
    list(session.execute_to_morsels("CREATE TABLE ws.t (id BIGINT)"))
    list(session.execute_to_morsels("CREATE VIEW ws.v AS SELECT * FROM ws.t"))
    return permissive


# --- the seam itself


def test_the_intrinsic_capability_is_permit_all():
    assert isinstance(active_permissions_capability(), PermitAll)
    capability = active_permissions_capability()
    assert capability.can_perform_action(None, "anything.at.all", "DROP")
    assert capability.can_perform_workspace_action(None, "anything", "ALTER")


def test_an_incomplete_capability_is_refused_at_registration(install):
    class MissingGrants:
        def can_perform_action(self, execution_context, resource, action):
            return True

        def can_perform_workspace_action(self, execution_context, workspace, action):
            return True

    with pytest.raises(InvalidConfigurationError):
        install(MissingGrants())


def test_a_capability_cannot_be_swapped_once_a_check_has_been_answered(install):
    capability = install(ScriptedCapability())
    capability.can_perform_action(None, "ws.t", "READ")
    managers.permissions.can_perform_action(None, "ws.t", "READ")

    with pytest.raises(InvalidConfigurationError):
        register_permissions_capability(ScriptedCapability())


# --- the engine asks, and honours the answer


def test_select_is_refused_when_the_capability_says_no(tmp_path, install):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow=set()))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("SELECT * FROM ws.t"))

    assert ("ws.t", "READ") in capability.asked


def test_select_is_allowed_when_the_capability_says_yes(tmp_path, install):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels("SELECT * FROM ws.t"))

    assert ("ws.t", "READ") in capability.asked


def test_permitting_one_relation_does_not_permit_another(tmp_path, install):
    _seed(tmp_path, install)
    install(ScriptedCapability(allow={("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels("SELECT * FROM ws.t"))
    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("SELECT * FROM ws.other"))


def test_reading_a_view_is_gated_on_its_sources_not_on_the_view(tmp_path, install):
    """A view is NOT a permission boundary on read.

    `SELECT * FROM ws.v` is expanded to the view's own query at planning time,
    so the engine asks about the relations the view READS and never about the
    view itself. Two consequences, both deliberate and both worth pinning:
    holding READ on a view name confers nothing on its own, and a caller who
    can already read the sources gains nothing by being denied the view.

    The inverse - gating the view but not its sources - would be a
    confused-deputy hole, letting a view launder access to tables the caller
    cannot read. That is what this test exists to catch if the expansion ever
    moves relative to the gate.
    """
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels("SELECT * FROM ws.v"))

    assert capability.asked == [("ws.t", "READ")]


def test_a_view_cannot_launder_access_to_a_source_the_caller_cannot_read(tmp_path, install):
    _seed(tmp_path, install)
    # READ on the view, nothing on the table behind it.
    install(ScriptedCapability(allow={("ws.v", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("SELECT * FROM ws.v"))


def test_a_refusal_through_a_view_names_the_view(tmp_path, install):
    """The caller wrote `ws.v`, so a bare "cannot read ws.t" names a relation
    they never mentioned and may not know exists. Say which view reads it."""
    _seed(tmp_path, install)
    install(ScriptedCapability(allow={("ws.v", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError, match=r"View ws\.v reads ws\.t"):
        list(session.execute_to_morsels("SELECT * FROM ws.v"))


def test_a_refusal_through_nested_views_names_the_one_doing_the_read(tmp_path, install):
    """With views stacked, the innermost is the one that actually reads the
    refused relation -- that is the one to name, since it is where a grant
    would have to reach."""
    _seed(tmp_path, install)
    install(ScriptedCapability(allow={("ws.v2", "WRITE"), ("ws.v", "READ"), ("ws.t", "READ")}))
    list(
        opteryx.session(user="olive").execute_to_morsels("CREATE VIEW ws.v2 AS SELECT * FROM ws.v")
    )

    install(ScriptedCapability(allow={("ws.v2", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError, match=r"View ws\.v reads ws\.t"):
        list(session.execute_to_morsels("SELECT * FROM ws.v2"))


def test_a_direct_refusal_does_not_mention_a_view(tmp_path, install):
    _seed(tmp_path, install)
    install(ScriptedCapability(allow=set()))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError) as refusal:
        list(session.execute_to_morsels("SELECT * FROM ws.t"))

    assert "View" not in str(refusal.value)


def test_internal_relations_are_asked_about_too(tmp_path, install):
    """`$grants` and friends reach the gate under their bare name.

    They carry no workspace, so a capability sees a dotless resource. It still
    has to answer - a capability that denied everything unrecognised would
    refuse `SHOW GRANTS`, leaving a caller unable to see why they were refused
    anything else.
    """
    capability = install(ScriptedCapability(allow=set()))
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels("SHOW GRANTS"))

    assert ("$grants", "READ") in capability.asked


# --- it asks the RIGHT action for each statement
#
# This is the contract between engine and capability. A capability grants by
# action, so the action the engine names decides which grants are sufficient:
# asking WRITE for a DROP would let a writer destroy a relation.


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("SELECT * FROM ws.t", ("ws.t", "READ")),
        ("CREATE TABLE ws.new (id BIGINT)", ("ws.new", "CREATE")),
        ("DROP TABLE ws.t", ("ws.t", "DROP")),
        ("DROP VIEW ws.v", ("ws.v", "DROP")),
        ("TRUNCATE TABLE ws.t", ("ws.t", "DELETE")),
        ("INSERT INTO ws.t (id) VALUES (1)", ("ws.t", "WRITE")),
        ("CREATE VIEW ws.v2 AS SELECT * FROM ws.t", ("ws.v2", "WRITE")),
        ("SHOW CREATE VIEW ws.v", ("ws.v", "READ")),
        ("COMMENT ON TABLE ws.t IS 'hello'", ("ws.t", "WRITE")),
    ],
)
def test_each_statement_asks_for_its_own_action(tmp_path, install, statement, expected):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow=set()))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels(statement))

    assert expected in capability.asked, (statement, capability.asked)


def test_show_manifest_asks_for_manifest_on_top_of_read(tmp_path, install):
    """MANIFEST is checked after READ on the same relation, so it is only
    reachable once READ is permitted - a caller who cannot read a relation is
    refused before its layout is ever in question."""
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("SHOW MANIFEST FOR ws.t"))

    assert ("ws.t", "MANIFEST") in capability.asked


def test_a_grant_for_the_wrong_action_does_not_clear_the_gate(tmp_path, install):
    """The other half of the matrix: permitting WRITE on a relation must not
    let a DROP through. This is what would break if a gate named the wrong
    action."""
    _seed(tmp_path, install)
    install(ScriptedCapability(allow={("ws.t", "WRITE"), ("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("DROP TABLE ws.t"))


def test_rename_asks_for_both_the_source_and_the_target(tmp_path, install):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.t", "ALTER")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("ALTER TABLE ws.t RENAME TO ws.renamed"))

    # ALTER on the source is not enough; the new name is a CREATE of its own.
    assert ("ws.t", "ALTER") in capability.asked
    assert ("ws.renamed", "CREATE") in capability.asked


# --- workspace-level actions go through the other gate


def test_workspace_action_uses_the_workspace_gate(tmp_path, install):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow_workspace=set()))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET deletion_protection TO OFF"))

    assert ("ws", "ALTER") in capability.asked_workspace
    # ...and it did NOT go through the relation gate.
    assert ("ws", "ALTER") not in capability.asked


def test_workspace_action_is_allowed_when_the_capability_permits_it(tmp_path, install):
    _seed(tmp_path, install)
    capability = install(ScriptedCapability(allow_workspace={("ws", "ALTER")}))
    session = opteryx.session(user="olive")

    # The local store does not implement the property, but the gate cleared -
    # which is what this asserts. A refusal here would be a PermissionError.
    with pytest.raises(NotImplementedError):
        list(session.execute_to_morsels("ALTER WORKSPACE ws SET deletion_protection TO OFF"))

    assert ("ws", "ALTER") in capability.asked_workspace


# --- SHOW GRANTS is answered by the capability, not by the engine


def test_show_grants_reports_what_the_capability_returns(tmp_path, install):
    rows = [
        {"pattern": "ws.*", "role": "reader", "actions": "READ"},
        {"pattern": "other.*", "role": "owner", "actions": "DROP, READ"},
    ]
    install(ScriptedCapability(rows=rows))
    session = opteryx.session(user="olive")

    reported = []
    for morsel in session.execute_to_morsels("SHOW GRANTS"):
        reported.extend(tuple(row) for row in morsel)

    assert reported == [("ws.*", "reader", "READ"), ("other.*", "owner", "DROP, READ")]


def test_show_grants_under_the_intrinsic_capability_says_everything_is_allowed():
    session = opteryx.session(user="olive")

    reported = []
    for morsel in session.execute_to_morsels("SHOW GRANTS"):
        reported.extend(tuple(row) for row in morsel)

    assert reported == [("*", "*", "*")]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()


# ---------------------------------------------------------------------------
# Every relation a statement touches is gated, not just the first one.
#
# A query names more than one relation in plenty of ways. If any of them
# reached the reader without passing the gate, a caller could read a table
# they hold nothing on by mentioning it in the right position.
# ---------------------------------------------------------------------------


def _two_tables(tmp_path, install):
    """Two tables with a row each. The rows matter: materializing an empty
    relation fails in the writer, which would mask the gate under test."""
    _workspace(tmp_path)
    install(
        ScriptedCapability(
            allow={
                ("ws.a", "CREATE"),
                ("ws.b", "CREATE"),
                ("ws.a", "READ"),
                ("ws.b", "READ"),
                ("ws.a", "WRITE"),
                ("ws.b", "WRITE"),
            }
        )
    )
    session = opteryx.session(user="olive")
    list(session.execute_to_morsels("CREATE TABLE ws.a (id BIGINT)"))
    list(session.execute_to_morsels("CREATE TABLE ws.b (id BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.a (id) VALUES (1)"))
    list(session.execute_to_morsels("INSERT INTO ws.b (id) VALUES (1)"))


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT a.id FROM ws.a AS a JOIN ws.b AS b ON a.id = b.id",
        "SELECT * FROM ws.a WHERE id IN (SELECT id FROM ws.b)",
        "WITH c AS (SELECT * FROM ws.b) SELECT * FROM c",
        "SELECT id FROM ws.a UNION SELECT id FROM ws.b",
    ],
)
def test_every_relation_in_a_statement_is_gated(tmp_path, install, statement):
    """`ws.b` is reachable four different ways; none of them skips the gate."""
    _two_tables(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.a", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels(statement))

    assert ("ws.b", "READ") in capability.asked


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT a.id FROM ws.a AS a JOIN ws.b AS b ON a.id = b.id",
        "SELECT * FROM ws.a WHERE id IN (SELECT id FROM ws.b)",
        "WITH c AS (SELECT * FROM ws.b) SELECT * FROM c",
        "SELECT id FROM ws.a UNION SELECT id FROM ws.b",
    ],
)
def test_the_same_statements_run_when_every_relation_is_permitted(tmp_path, install, statement):
    """The other half: these are refused above because of the gate, not
    because the statement was broken to begin with."""
    _two_tables(tmp_path, install)
    install(ScriptedCapability(allow={("ws.a", "READ"), ("ws.b", "READ")}))
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels(statement))


# ---------------------------------------------------------------------------
# A materialized view must not become a confused deputy.
#
# Creating one writes a relation that holds the ROWS of its sources. If the
# target gate were checked and the sources were not, a caller could copy out a
# table they cannot read by pointing an MV at it.
# ---------------------------------------------------------------------------


def test_creating_a_materialized_view_asks_read_on_each_source(tmp_path, install):
    _two_tables(tmp_path, install)
    capability = install(
        ScriptedCapability(allow={("ws.mv", "WRITE"), ("ws.mv", "CREATE"), ("ws.b", "READ")})
    )
    session = opteryx.session(user="olive")

    list(session.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.b"))

    assert ("ws.b", "READ") in capability.asked


def test_a_materialized_view_cannot_copy_out_a_source_the_caller_cannot_read(tmp_path, install):
    """Full authority over the target, none over the source: refused."""
    _two_tables(tmp_path, install)
    capability = install(ScriptedCapability(allow={("ws.mv", "WRITE"), ("ws.mv", "CREATE")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.b"))

    assert ("ws.b", "READ") in capability.asked
    assert not (tmp_path / "ws" / "mv").exists()


def test_a_materialized_view_still_needs_authority_over_its_target(tmp_path, install):
    """Readable source, no authority over the target: also refused."""
    _two_tables(tmp_path, install)
    install(ScriptedCapability(allow={("ws.b", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.b"))

    assert not (tmp_path / "ws" / "mv").exists()


# ---------------------------------------------------------------------------
# A refusal leaves nothing behind.
# ---------------------------------------------------------------------------


def test_a_refused_create_writes_nothing(tmp_path, install):
    _workspace(tmp_path)
    install(ScriptedCapability(allow=set()))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("CREATE TABLE ws.nope (id BIGINT)"))

    assert not (tmp_path / "ws" / "nope").exists()


def test_a_refused_drop_leaves_the_relation(tmp_path, install):
    _seed(tmp_path, install)
    install(ScriptedCapability(allow={("ws.t", "READ")}))
    session = opteryx.session(user="olive")

    with pytest.raises(PermissionError):
        list(session.execute_to_morsels("DROP TABLE ws.t"))

    assert (tmp_path / "ws" / "t").exists()


# ---------------------------------------------------------------------------
# information_schema: the one gate on the EXECUTION path.
#
# These readers opt out of the relation-level gate (`self_governs_permissions`)
# because a caller must be able to reach the metadata view at all. They pay for
# that by filtering every row they emit through the same capability, as rows
# are produced. It is the only place a permission decision happens per-row
# rather than per-statement, and a leak here discloses the shape of a catalog
# the caller holds nothing on.
# ---------------------------------------------------------------------------


class _NoSnapshot:
    """A dataset with nothing committed - enough for the metadata readers."""

    def snapshot(self):
        return None


class MetadataCatalog:
    """Two collections, three datasets. No rows, just names to be filtered."""

    def __init__(self, workspace=None, **kwargs):
        pass

    def list_collections(self):
        return ["coll1", "coll2"]

    def list_datasets(self, collection):
        return {"coll1": ["src", "other"], "coll2": ["far"]}[collection]

    def list_views(self, collection):
        return []

    def dataset_exists(self, identifier):
        return True

    def load_dataset(self, identifier):
        return _NoSnapshot()


@pytest.fixture
def catalog():
    register_workspace("cat", OpteryxConnector, catalog=MetadataCatalog)


def _rows(statement, user="olive"):
    collected = []
    for morsel in opteryx.session(user=user).execute_to_morsels(statement):
        collected.extend(tuple(row) for row in morsel)
    return collected


def test_information_schema_shows_only_the_relations_the_caller_may_read(catalog, install):
    install(ScriptedCapability(allow={("cat.coll1.src", "READ")}))

    assert _rows("SELECT table_schema, table_name FROM cat.information_schema.tables") == [
        ("coll1", "src")
    ]


def test_information_schema_shows_nothing_when_nothing_is_readable(catalog, install):
    install(ScriptedCapability(allow=set()))

    assert _rows("SELECT table_schema, table_name FROM cat.information_schema.tables") == []


def test_information_schema_widens_as_the_capability_widens(catalog, install):
    install(ScriptedCapability(allow={("cat.coll1.src", "READ"), ("cat.coll2.far", "READ")}))

    assert sorted(_rows("SELECT table_schema, table_name FROM cat.information_schema.tables")) == [
        ("coll1", "src"),
        ("coll2", "far"),
    ]


def test_information_schema_asks_per_relation_not_once_for_the_view(catalog, install):
    """The opt-out is not a bypass: the reader asks about each underlying
    relation, so `self_governs_permissions` buys reachability, not access."""
    capability = install(ScriptedCapability(allow={("cat.coll1.src", "READ")}))

    _rows("SELECT table_name FROM cat.information_schema.tables")

    asked = {resource for resource, action in capability.asked if action == "READ"}
    assert {"cat.coll1.src", "cat.coll1.other", "cat.coll2.far"} <= asked


def test_schemata_hides_a_collection_with_nothing_readable_in_it(catalog, install):
    """A collection is listed only if the caller can read something inside it -
    otherwise its existence leaks."""
    install(ScriptedCapability(allow={("cat.coll1.src", "READ")}))

    assert _rows("SELECT schema_name FROM cat.information_schema.schemata") == [("coll1",)]


def test_columns_are_filtered_by_the_same_gate(catalog, install):
    install(ScriptedCapability(allow=set()))

    assert _rows("SELECT table_name FROM cat.information_schema.columns") == []
