"""UPDATE and DELETE, end to end through the engine — local disk, no GCS.

Both are MERGE with a degenerate source: no join, one constant action, the same
row addresses and the same sink. These tests prove the parts that are NOT
shared with MERGE — that the degenerate plan classifies every surviving row,
that a DELETE writes no data file at all, and that an UPDATE rebuilds a whole
row from a partial SET list — plus the refusals that keep the statements from
silently acting on the wrong rows.

The environment (a real catalog-backed dataset on local disk) is the merge
suite's, imported rather than copied so the two cannot drift.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

# The package spelling, not a bare module name: `tests.integration` is a package,
# and importing the merge module under a second name would give it a second set
# of module globals - two `_FakeCatalog` classes, two connector registrations -
# so the two suites would clobber each other when run together.
from tests.integration.test_merge_into_local import TARGET  # noqa: E402,F401
from tests.integration.test_merge_into_local import _rows  # noqa: E402
from tests.integration.test_merge_into_local import _target_rows  # noqa: E402
from tests.integration.test_merge_into_local import merge_env  # noqa: E402,F401
from tests.integration.test_merge_into_local import pytestmark  # noqa: E402,F401


def _run(sql):
    return list(opteryx.session(user="tester").execute_to_morsels(sql))


# ── DELETE ──────────────────────────────────────────────────────────────────


def test_delete_removes_the_rows_the_predicate_names(merge_env):
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]

    _run(f"DELETE FROM {TARGET} WHERE cve = 2")

    assert _target_rows() == [(1, 10, 1), (3, 30, 1)]


def test_delete_writes_no_data_file(merge_env):
    """A DELETE never appends. It projects no payload columns at all, so there
    is nothing to gather and no file to write — the whole statement is delete
    positions."""
    target = merge_env["col.tgt"]

    _run(f"DELETE FROM {TARGET} WHERE cve > 1")

    snap = target.snapshot(None)
    assert snap.summary["added-records"] == 0
    assert snap.summary["deleted-records"] == 2


def test_delete_without_a_predicate_empties_the_relation(merge_env):
    _run(f"DELETE FROM {TARGET}")
    assert _target_rows() == []


def test_delete_matching_nothing_commits_nothing(merge_env):
    """No row acted on is a successful DELETE that did no work — not a failure,
    and not a snapshot describing nothing."""
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    _run(f"DELETE FROM {TARGET} WHERE cve = 999")

    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_delete_is_one_snapshot(merge_env):
    target = merge_env["col.tgt"]
    before = len(target.metadata.snapshots)
    _run(f"DELETE FROM {TARGET} WHERE cve = 3")
    assert len(target.metadata.snapshots) == before + 1


def test_repeated_delete_is_idempotent(merge_env):
    _run(f"DELETE FROM {TARGET} WHERE cve = 2")
    first = _target_rows()
    target = merge_env["col.tgt"]
    after_first = target.metadata.current_snapshot_id

    # The row is gone, so the second pass matches nothing and commits nothing.
    _run(f"DELETE FROM {TARGET} WHERE cve = 2")
    assert _target_rows() == first
    assert target.metadata.current_snapshot_id == after_first


def test_delete_accepts_an_alias(merge_env):
    _run(f"DELETE FROM {TARGET} AS t WHERE t.cve = 1")
    assert _target_rows() == [(2, 20, 1), (3, 30, 1)]


def test_delete_predicate_may_use_a_subquery(merge_env):
    _run(f"DELETE FROM {TARGET} WHERE cve IN (SELECT cve FROM {TARGET} WHERE details > 25)")
    assert _target_rows() == [(1, 10, 1), (2, 20, 1)]


def test_delete_predicate_may_read_the_target_itself(merge_env):
    """A sub-query over the relation being deleted from. It is lowered after
    binding, so the stamped Scan is still the one that addresses the rows."""
    _run(f"DELETE FROM {TARGET} WHERE cve = (SELECT MAX(cve) FROM {TARGET})")
    assert _target_rows() == [(1, 10, 1), (2, 20, 1)]


# ── UPDATE ──────────────────────────────────────────────────────────────────


def test_update_replaces_the_rows_the_predicate_names(merge_env):
    _run(f"UPDATE {TARGET} SET details = 99 WHERE cve = 3")
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 99, 1)]


def test_update_reads_the_row_it_replaces(merge_env):
    """The property that makes UPDATE more than DELETE-plus-INSERT: the new
    value is computed from the old one."""
    _run(f"UPDATE {TARGET} SET revision = revision + 1 WHERE cve = 2")
    assert _target_rows() == [(1, 10, 1), (2, 20, 2), (3, 30, 1)]


def test_update_carries_unset_columns_through(merge_env):
    """A partial SET list rebuilds a whole row; the columns it omits keep the
    old row's values rather than becoming NULL."""
    _run(f"UPDATE {TARGET} SET details = 55 WHERE cve = 1")
    assert _target_rows() == [(1, 55, 1), (2, 20, 1), (3, 30, 1)]


def test_update_sets_several_columns_at_once(merge_env):
    _run(f"UPDATE {TARGET} SET details = 77, revision = revision + 10 WHERE cve = 2")
    assert _target_rows() == [(1, 10, 1), (2, 77, 11), (3, 30, 1)]


def test_update_without_a_predicate_touches_every_row(merge_env):
    _run(f"UPDATE {TARGET} SET revision = 9")
    assert _target_rows() == [(1, 10, 9), (2, 20, 9), (3, 30, 9)]


def test_update_retires_the_old_row_and_appends_the_new_one(merge_env):
    """UPDATE is a delete position plus an append — there is no in-place
    mutation anywhere, so a two-row update moves two rows both ways."""
    target = merge_env["col.tgt"]

    _run(f"UPDATE {TARGET} SET details = details + 1 WHERE cve > 1")

    snap = target.snapshot(None)
    assert snap.summary["deleted-records"] == 2
    assert snap.summary["added-records"] == 2


def test_update_matching_nothing_commits_nothing(merge_env):
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    _run(f"UPDATE {TARGET} SET details = 1 WHERE cve = 999")

    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_update_accepts_an_alias(merge_env):
    _run(f"UPDATE {TARGET} AS t SET details = t.details * 2 WHERE t.cve = 2")
    assert _target_rows() == [(1, 10, 1), (2, 40, 1), (3, 30, 1)]


def test_update_column_names_are_not_case_sensitive(merge_env):
    _run(f"UPDATE {TARGET} SET DETAILS = 5 WHERE CVE = 1")
    assert _target_rows() == [(1, 5, 1), (2, 20, 1), (3, 30, 1)]


def test_update_to_an_unknown_column_is_refused(merge_env):
    """Silently dropping the assignment would report a successful update that
    did not make the change it was asked for."""
    from opteryx.exceptions import ColumnNotFoundError

    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    with pytest.raises(ColumnNotFoundError, match="no_such_column"):
        _run(f"UPDATE {TARGET} SET no_such_column = 1 WHERE cve = 1")

    assert target.metadata.current_snapshot_id == before


# ── What the statement reports ──────────────────────────────────────────────


def test_the_row_count_is_the_rows_acted_on(merge_env):
    """A row count is the whole observable output of either statement — there
    is no result set to check it against."""
    session = opteryx.session(user="tester")
    list(session.execute_to_morsels(f"DELETE FROM {TARGET} WHERE cve > 1"))
    assert session.rowcount == 2

    session = opteryx.session(user="tester")
    list(session.execute_to_morsels(f"UPDATE {TARGET} SET details = 5"))
    assert session.rowcount == 1  # the DELETE above left one row


# ── Refusals ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sql, message",
    [
        ("DELETE FROM {t} WHERE cve = 1 RETURNING *", "RETURNING"),
        ("DELETE FROM {t} ORDER BY cve LIMIT 1", "ORDER BY"),
        ("DELETE FROM {t} LIMIT 1", "LIMIT"),
        ("DELETE FROM {t} USING {t} AS u WHERE u.cve = 1", "USING"),
        ("DELETE a, b FROM {t}", "one relation"),
        ("UPDATE {t} SET details = 1 WHERE cve = 1 RETURNING *", "RETURNING"),
        ("UPDATE {t} SET details = 1 LIMIT 1", "LIMIT"),
        ("UPDATE {t} SET details = 1 FROM {t} AS u", "FROM"),
        ("UPDATE OR REPLACE {t} SET details = 1", "OR"),
        ("UPDATE {t} SET details = 1, details = 2", "more than once"),
    ],
)
def test_unsupported_forms_are_refused(merge_env, sql, message):
    from opteryx.exceptions import UnsupportedSyntaxError

    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    with pytest.raises(UnsupportedSyntaxError, match=message):
        _run(sql.format(t=TARGET))

    assert target.metadata.current_snapshot_id == before


# ── EXPLAIN ─────────────────────────────────────────────────────────────────


def test_explain_shows_the_plan_without_running_it(merge_env):
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    tree = " ".join(str(r[0]) for r in _rows(f"EXPLAIN DELETE FROM {TARGET} WHERE cve = 1"))
    assert "Delete" in tree

    tree = " ".join(
        str(r[0]) for r in _rows(f"EXPLAIN UPDATE {TARGET} SET details = 1 WHERE cve = 1")
    )
    assert "Update" in tree

    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_explain_analyze_refuses_to_run_a_write(merge_env):
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError, match="EXPLAIN ANALYZE"):
        _run(f"EXPLAIN ANALYZE DELETE FROM {TARGET} WHERE cve = 1")
