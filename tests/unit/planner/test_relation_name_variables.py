"""
`@@name` parts inside a relation name - `personal.@@external_user.dataset`.

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
        "SELECT * FROM personal.@@external_user.dataset",
        "SELECT * FROM other.thing JOIN personal.@@external_user.dataset ON a = b",
        "INSERT INTO personal.@@external_user.dataset (a) VALUES (1)",
        "CREATE TABLE personal.@@external_user.dataset (a INTEGER)",
        "CREATE VIEW personal.@@external_user.dataset AS SELECT 1",
        "DROP TABLE personal.@@external_user.dataset",
        "ALTER TABLE personal.@@external_user.dataset ADD COLUMN z INTEGER",
        "DELETE FROM personal.@@external_user.dataset WHERE a = 1",
        "TRUNCATE TABLE personal.@@external_user.dataset",
        "OPTIMIZE TABLE personal.@@external_user.dataset",
        "ANALYZE TABLE personal.@@external_user.dataset",
        "SHOW COLUMNS FROM personal.@@external_user.dataset",
        "SHOW CREATE TABLE personal.@@external_user.dataset",
        "MERGE INTO personal.@@external_user.dataset t USING other.x s "
        "ON t.a = s.a WHEN MATCHED THEN DELETE",
    ],
)
def test_relation_name_is_resolved(statement):
    names = object_names(rewrite(statement))
    assert "personal.alice.dataset" in names, names
    assert not any("@@" in name for name in names), names


def test_qualified_column_reference_is_resolved():
    # Without this a relation addressed by variable could only be referenced
    # through an alias.
    names = object_names(
        rewrite("SELECT personal.@@external_user.ds.col FROM personal.@@external_user.ds")
    )
    assert "personal.alice.ds.col" in names, names


@pytest.mark.parametrize(
    "statement",
    [
        "SHOW SNAPSHOTS FOR personal.@@external_user.dataset",
        "SHOW ALL SNAPSHOTS FOR personal.@@external_user.dataset",
        "SHOW MANIFEST FOR personal.@@external_user.dataset",
        "SHOW TRIGGERS FOR personal.@@external_user.dataset",
        "SHOW LINEAGE FOR personal.@@external_user.dataset",
    ],
)
def test_show_catch_all_forms_are_resolved(statement):
    # These reach the planner as a flat WORD LIST with the dots dropped, not as
    # an ObjectName, so they need their own position - and they carried the `@@`
    # straight through to a not-found relation until they got one.
    rewritten = str(rewrite(statement))
    assert "@@" not in rewritten, rewritten
    assert "alice" in rewritten, rewritten


def test_set_does_not_resolve_the_variable_it_assigns():
    # `Set` uses the same `variable` key as the SHOW catch-all, but what it holds
    # is the variable being WRITTEN - resolving it would rewrite the assignment
    # target into its own value.
    assert "@@disable_optimizer" in str(rewrite("SET @@disable_optimizer = true"))


def test_billing_account_is_resolved():
    assert "personal.acme.ds" in object_names(rewrite("SELECT * FROM personal.@@billing_account.ds"))


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
    assert "@@version" in str(rewrite(statement))


def test_restricted_variable_is_refused_not_leaked():
    # The refusal must not depend on the variable's VALUE reaching the message,
    # or a not-found relation name becomes a read channel for RESTRICTED.
    with pytest.raises(SqlError) as refusal:
        rewrite("SELECT * FROM personal.@@local_store_root.ds")
    assert "@@local_store_root" in str(refusal.value)
    assert "cannot be used inside a relation name" in str(refusal.value)


def test_unknown_variable_is_refused_the_same_way():
    # Same message as a real-but-not-allowlisted variable: the allowlist is
    # public, so the refusal says nothing about what else exists.
    with pytest.raises(SqlError):
        rewrite("SELECT * FROM personal.@@nosuch.ds")


def test_unset_variable_is_refused():
    # `personal..ds` is not a fallback, it is a different name.
    with pytest.raises(SqlError) as refusal:
        rewrite("SELECT * FROM personal.@@external_user.ds", user="")
    assert "is not set" in str(refusal.value)


def test_value_that_is_not_a_name_is_refused():
    # A dot in the value would change the name's ARITY - three parts becoming
    # four - and address a different workspace. It must fail, not be repaired.
    with pytest.raises(SqlError) as refusal:
        rewrite("SELECT * FROM personal.@@external_user.ds", user="justin.joyce@joocer.com")
    assert "justin.joyce@joocer.com" in str(refusal.value)


def test_no_session_variables_is_refused():
    parsed = parse_sql(do_sql_rewrite("SELECT * FROM personal.@@external_user.ds"), "mysql")
    with pytest.raises(SqlError):
        do_ast_rewriter(parsed, [], None)


def test_resolved_name_reaches_the_catalog_lookup():
    # End to end: the name the engine fails to find is the RESOLVED one, which is
    # what proves no later phase sees the `@@`.
    session = opteryx.session(user="alice")
    with pytest.raises(opteryx.exceptions.DatasetNotFoundError) as missing:
        list(session.execute_to_morsels("SELECT * FROM personal.@@external_user.dataset"))
    assert "alice" in str(missing.value)
    assert "@@" not in str(missing.value)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
