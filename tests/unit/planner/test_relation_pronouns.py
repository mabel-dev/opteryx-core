"""
The `$me` pronoun inside a relation name - `personal.$me.dataset`.

The substitution is a planning rewrite with no catalog behind it here, so these
tests read the RESOLVED NAME back out of the rewritten AST rather than running a
query. What matters is which names are rewritten, which are left alone, and that
every refusal is loud.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import SqlError
from opteryx.models import ExecutionContext
from opteryx.planner.ast_rewriter import do_ast_rewriter
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party.sqloxide import parse_sql


def rewrite(statement: str, user: str = "alice", billing_account: str = "acme"):
    context = ExecutionContext(query_id="test", user=user, billing_account=billing_account)
    parsed = parse_sql(do_sql_rewrite(statement), "mysql")
    return do_ast_rewriter(parsed, [], context.variables)


def is_object_name(value):
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(part, dict) and tuple(part) == ("Identifier",) for part in value)
    )


def object_names(node, found=None):
    """Every ObjectName and CompoundIdentifier in the tree, dotted back together."""
    found = [] if found is None else found
    if isinstance(node, list):
        # `DROP TABLE a.b, c.d` carries a LIST of ObjectNames, so a name can be a
        # list element as well as a dict value.
        for item in node:
            if is_object_name(item):
                found.append(".".join(part["Identifier"]["value"] for part in item))
            object_names(item, found)
        return found
    if not isinstance(node, dict):
        return found
    for key, value in node.items():
        if key == "CompoundIdentifier":
            found.append(".".join(part["value"] for part in value))
        elif is_object_name(value):
            found.append(".".join(part["Identifier"]["value"] for part in value))
        object_names(value, found)
    return found


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM personal.$me.dataset",
        "SELECT * FROM other.thing JOIN personal.$me.dataset ON a = b",
        "INSERT INTO personal.$me.dataset (a) VALUES (1)",
        "CREATE TABLE personal.$me.dataset (a INTEGER)",
        "CREATE VIEW personal.$me.dataset AS SELECT 1",
        "DROP TABLE personal.$me.dataset",
        "ALTER TABLE personal.$me.dataset ADD COLUMN z INTEGER",
        "DELETE FROM personal.$me.dataset WHERE a = 1",
        "TRUNCATE TABLE personal.$me.dataset",
        "OPTIMIZE TABLE personal.$me.dataset",
        "ANALYZE TABLE personal.$me.dataset",
        "SHOW COLUMNS FROM personal.$me.dataset",
        "SHOW CREATE TABLE personal.$me.dataset",
        "MERGE INTO personal.$me.dataset t USING other.x s "
        "ON t.a = s.a WHEN MATCHED THEN DELETE",
        # Parsed by the aside parser rather than sqlparser, and resolved by the
        # same walk - which is the point of `src/aside/` existing.
        "CREATE TASK personal.$me.dataset AS SELECT 1",
        "DROP TASK personal.$me.dataset",
        "ALTER TASK personal.$me.dataset AS SELECT 1",
        "CREATE TASK other.t ON personal.$me.dataset AS SELECT 1",
        "CREATE TRIGGER tick ON personal.$me.dataset EXECUTE other.job",
        "DROP TRIGGER tick ON personal.$me.dataset",
        "ALTER TRIGGER tick ON personal.$me.dataset SUSPEND",
        "ALTER TRIGGER tick ON personal.$me.dataset OWNER TO bob",
        "ALTER TRIGGER tick ON personal.$me.dataset SET MINIMUM INTERVAL TO 5",
        "CREATE TRIGGER tick ON SCHEDULE '0 * * * *' EXECUTE personal.$me.dataset",
        "REFRESH MATERIALIZED VIEW personal.$me.dataset",
        "ALTER MATERIALIZED VIEW personal.$me.dataset SUSPEND",
        "ALTER MATERIALIZED VIEW personal.$me.dataset OWNER TO bob",
        "SAVE RESULTS OF 20260921123456-abc AS personal.$me.dataset",
        "LISTEN TO personal.$me.dataset",
        "LISTEN TO personal.$me.dataset FOR ERROR",
        "UNLISTEN personal.$me.dataset",
        "DROP STATISTICS ON personal.$me.dataset",
        "SHOW CREATE TASK personal.$me.dataset",
        "SHOW CREATE TRIGGER tick ON personal.$me.dataset",
        "ALTER TABLE personal.$me.dataset RESYNC",
        "ALTER TABLE personal.$me.dataset DETACH",
        "ALTER WORKSPACE src SET SECURE personal.$me.dataset TO dst",
    ],
)
def test_relation_name_is_resolved(statement):
    names = object_names(rewrite(statement))
    assert "personal.alice.dataset" in names, names
    assert not any("$me" in name.lower() for name in names), names


def test_qualified_column_reference_is_resolved():
    # Without this a relation addressed by pronoun could only be referenced
    # through an alias.
    names = object_names(
        rewrite("SELECT personal.$me.ds.col FROM personal.$me.ds")
    )
    assert "personal.alice.ds.col" in names, names


@pytest.mark.parametrize(
    "statement",
    [
        "SHOW SNAPSHOTS FOR personal.$me.dataset",
        "SHOW ALL SNAPSHOTS FOR personal.$me.dataset",
        "SHOW MANIFEST FOR personal.$me.dataset",
        "SHOW TRIGGERS FOR personal.$me.dataset",
        "SHOW LINEAGE FOR personal.$me.dataset",
    ],
)
def test_show_catch_all_forms_are_resolved(statement):
    # These reach the planner as a flat WORD LIST with the dots dropped, not as
    # an ObjectName, so they need their own position - and they carried the
    # pronoun straight through to a not-found relation until they got one.
    rewritten = str(rewrite(statement))
    assert "$me" not in rewritten.lower(), rewritten
    assert "alice" in rewritten, rewritten


def test_set_does_not_resolve_the_variable_it_assigns():
    # `Set` uses the same `variable` key as the SHOW catch-all, but what it holds
    # is the variable being WRITTEN - resolving it would rewrite the assignment
    # target into its own value.
    assert "@@disable_optimizer" in str(rewrite("SET @@disable_optimizer = true"))



@pytest.mark.parametrize(
    "statement",
    [
        "SELECT @@version",
        "SELECT @@version AS v",
        "SELECT 1 FROM t GROUP BY @@version",
        "SELECT 1 FROM t ORDER BY @@version",
    ],
)
def test_variable_reads_are_untouched(statement):
    # A bare `@@name` is a variable READ, and rewriting one into an identifier
    # would turn `SELECT @@version` into a reference to a column called `0.9.x`.
    # The pronoun lives only in NAME positions and never touches these.
    assert "@@version" in str(rewrite(statement))


@pytest.mark.parametrize(
    "written", ["personal.$nope.ds", "personal.$local_store_root.ds", "personal.$user.ds"]
)
def test_only_the_pronoun_is_substituted(written):
    # There is ONE pronoun, so there is no allowlist to get wrong. This is the
    # whole reason `$me` is better than the `@@external_user` it replaced: a
    # variable namespace invited "which variables?", and a relation that cannot
    # be found reports the name it looked for - which would have made
    # `FROM x.@@local_store_root.y` a read channel for RESTRICTED variables.
    #
    # Anything else `$`-prefixed is left exactly as written. It fails as an
    # unknown relation, and at a permission check as engine-private.
    names = object_names(rewrite(f"SELECT * FROM {written}"))
    assert written in names, names


def test_a_quoted_pronoun_is_a_name_the_reader_escaped():
    names = object_names(rewrite("SELECT * FROM personal.`$me`.ds"))
    assert "personal.$me.ds" in names, names


def test_the_pronoun_is_case_insensitive():
    # It is a keyword, and keywords in this dialect are.
    assert "personal.alice.ds" in object_names(rewrite("SELECT * FROM personal.$ME.ds"))


def test_an_unset_identity_is_refused():
    # `personal..ds` is not a fallback, it is a different name.
    with pytest.raises(SqlError) as refusal:
        rewrite("SELECT * FROM personal.$me.ds", user="")
    assert "has no value" in str(refusal.value)


def test_value_that_is_not_a_name_is_refused():
    # A dot in the value would change the name's ARITY - three parts becoming
    # four - and address a different workspace. It must fail, not be repaired.
    with pytest.raises(SqlError) as refusal:
        rewrite("SELECT * FROM personal.$me.ds", user="justin.joyce@joocer.com")
    assert "justin.joyce@joocer.com" in str(refusal.value)


def test_no_session_is_refused():
    parsed = parse_sql(do_sql_rewrite("SELECT * FROM personal.$me.ds"), "mysql")
    with pytest.raises(SqlError):
        do_ast_rewriter(parsed, [], None)


def test_resolved_name_reaches_the_catalog_lookup():
    # End to end: the name the engine fails to find is the RESOLVED one, which
    # is what proves no later phase sees the pronoun.
    session = opteryx.session(user="alice")
    with pytest.raises(opteryx.exceptions.DatasetNotFoundError) as missing:
        list(session.execute_to_morsels("SELECT * FROM personal.$me.dataset"))
    assert "alice" in str(missing.value)
    assert "$me" not in str(missing.value).lower()


# --- what a pre-flight check reports ---------------------------------------
#
# `analyze_query` and `Session.check` describe the PRE-rewrite AST, so they used
# to report `personal.$me.x` as the relation a statement names. A caller matching
# that against grants matches nothing, and refuses a statement the engine would
# have run - which is how an owned relation was reported as unauthorized rather
# than as missing.


def test_analyze_query_resolves_the_pronoun_for_the_named_user():
    described = opteryx.analyze_query("SELECT * FROM personal.$me.notes", user="alice")
    assert described["tables"] == ["personal.alice.notes"], described["tables"]


def test_analyze_query_without_a_user_leaves_the_pronoun_as_written():
    # There is no identity to substitute, and guessing one would name another
    # user's data. The pronoun survives, unresolved and visible.
    described = opteryx.analyze_query("SELECT * FROM personal.$me.notes")
    assert described["tables"] == ["personal.$me.notes"], described["tables"]


def test_analyze_query_describes_every_statement_of_a_batch():
    described = opteryx.analyze_query(
        "SELECT * FROM personal.$me.a; INSERT INTO personal.$me.b (x) VALUES (1)",
        user="alice",
    )
    assert described["tables"] == ["personal.alice.a", "personal.alice.b"], described["tables"]


def test_analyze_query_still_reports_the_parameters_written():
    # The pronoun pass runs on the PRE-rewrite AST, where a `:name` is still
    # recorded. Resolving one must not cost the other.
    described = opteryx.analyze_query(
        "SELECT * FROM personal.$me.notes WHERE dept = :department", user="alice"
    )
    assert described["tables"] == ["personal.alice.notes"], described["tables"]
    assert described["parameters"] == ["department"], described["parameters"]


def test_analyze_query_refuses_a_user_that_is_not_a_name():
    # Validated, never repaired: a dotted value would change the name's arity and
    # address a different workspace.
    with pytest.raises(SqlError):
        opteryx.analyze_query("SELECT * FROM personal.$me.notes", user="alice.smith")


def test_check_reports_the_resolved_name_and_the_real_failure():
    session = opteryx.session(user="alice")
    checked = session.check("SELECT * FROM personal.$me.notes").as_dict()
    assert checked["tables"] == ["personal.alice.notes"], checked["tables"]
    # The relation is missing, and that is what is reported - not a refusal.
    assert checked["error"]["type"] == "DatasetNotFoundError", checked["error"]


def test_check_reports_an_unresolvable_pronoun_rather_than_raising():
    # A check returns a diagnostic even when the statement cannot be understood:
    # being wrong is the expected case while a statement is typed.
    session = opteryx.session()
    checked = session.check("SELECT * FROM personal.$me.notes").as_dict()
    assert checked["ok"] is False
    assert checked["error"]["type"] == "SqlError", checked["error"]


# --- the permission gate sees the resolved name ----------------------------


def test_the_permission_gate_is_asked_about_the_resolved_name():
    """The composition that matters.

    `$me` is resolved in the AST rewriter, which runs before the binder - so
    the name the permission capability is asked about is the SUBSTITUTED one.
    That is what makes `personal.$me.*` pass without any rule about pronouns
    in the permissions model: by the time it is asked, there is no pronoun.

    Proved by recording what the capability was asked, rather than by asserting
    the query succeeded - a query can succeed for other reasons, and what needs
    pinning is the string the gate received.
    """
    from opteryx import managers
    from opteryx.connectors import register_workspace
    from opteryx.connectors import LocalStoreConnector
    from opteryx.managers.permissions import register_permissions_capability

    class Recorder:
        """Permits everything and remembers every resource it was asked about."""

        name = "recorder"

        def __init__(self):
            self.asked = []

        def can_perform_action(self, execution_context, resource, action):
            self.asked.append(resource)
            return True

        def can_perform_workspace_action(self, execution_context, workspace, action):
            return True

        def can_principal_perform_action(self, principal, resource, action):
            return True

        def can_principal_own_materialized_view(self, principal):
            return True

        def grants(self, identity, policies):
            return []

        def apply_grant(self, execution_context, pattern, role, principal):
            return ""

        def apply_revoke(self, execution_context, pattern, role, principal):
            return ""

        def grants_on(self, execution_context, pattern):
            return []

        def effective_grants_on(self, execution_context, pattern):
            return []

        def effective_grants_in(self, execution_context, workspace, objects):
            return []

        def set_workspace_maintenance(self, execution_context, workspace, state):
            return None

        def workspace_maintenance(self, workspace):
            return None

    module = managers.permissions
    saved = (module._active, module._consulted)
    try:
        module._active, module._consulted = module._CORE, False
        recorder = Recorder()
        register_permissions_capability(recorder)

        session = opteryx.session(user="alice")
        with pytest.raises(Exception):
            list(session.execute_to_morsels("SELECT * FROM personal.$me.nosuch"))

        assert "personal.alice.nosuch" in recorder.asked, recorder.asked
        assert not any("$me" in r.lower() for r in recorder.asked), recorder.asked
    finally:
        module._active, module._consulted = saved


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
