"""What the CHECKER reports for statements it cannot fully bind — local disk, no GCS.

A check binds with `schema_only=True`, which deliberately does not read a
relation's Manifest. Three statements care about the Manifest, and each of them
read the resulting `None` as "this connector has none": every UPDATE, DELETE and
MERGE was reported as an error against a catalog-backed target, and SHOW MANIFEST
/ SHOW SNAPSHOTS refused outright. All five are valid SQL that runs, so a checker
drawing them as errors is drawing the wrong thing.

The environment is the merge suite's — a real catalog-backed dataset on local
disk — imported rather than copied so the two cannot drift.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

from opteryx.models.execution_context import ExecutionContext  # noqa: E402
from opteryx.models.query_telemetry import QueryTelemetry  # noqa: E402
from opteryx.planner.query_check import check_statement  # noqa: E402

from tests.integration.test_merge_into_local import SOURCE  # noqa: E402
from tests.integration.test_merge_into_local import TARGET  # noqa: E402
from tests.integration.test_merge_into_local import merge_env  # noqa: E402,F401
from tests.integration.test_merge_into_local import pytestmark  # noqa: E402,F401


def _check(sql):
    return check_statement(
        sql,
        execution_context=ExecutionContext(query_id="check", user="tester"),
        query_id="check",
        telemetry=QueryTelemetry.detached(),
    )


@pytest.mark.parametrize(
    "sql",
    [
        f"UPDATE {TARGET} SET details = 1 WHERE cve = 2",
        f"DELETE FROM {TARGET} WHERE cve = 2",
        f"MERGE INTO {TARGET} AS t USING {SOURCE} AS n ON t.cve = n.cve "
        "WHEN MATCHED THEN UPDATE SET details = n.details",
        f"SHOW MANIFEST FOR {TARGET}",
        f"SHOW SNAPSHOTS FOR {TARGET}",
    ],
)
def test_a_statement_that_runs_checks_clean(merge_env, sql):
    """The check does not read a Manifest. Not having read one is not the same
    answer as there not being one, and only the second is a reason to refuse."""
    result = _check(sql)
    assert result.ok, result.error


def test_the_checker_still_reports_what_is_actually_wrong(merge_env):
    """The point is not that mutations always pass — it is that they are bound.
    A column that is not there is still a column that is not there."""
    result = _check(f"UPDATE {TARGET} SET no_such_column = 1")
    assert not result.ok
    assert "no_such_column" in str(result.error)


@pytest.mark.parametrize(
    ("sql", "statement"),
    [
        (f"UPDATE {TARGET} SET details = 1 WHERE cve = 2", "UPDATE"),
        (f"DELETE FROM {TARGET} WHERE cve = 2", "DELETE FROM"),
        (
            f"MERGE INTO {TARGET} AS t USING {SOURCE} AS n ON t.cve = n.cve "
            "WHEN MATCHED THEN DELETE",
            "MERGE INTO",
        ),
    ],
)
def test_running_without_a_manifest_is_still_refused(merge_env, monkeypatch, sql, statement):
    """The refusal the check path was borrowing is real when the statement is
    RUN: no manifest then means no file list, and a row address would point at
    no file. It names the statement the reader wrote, not always MERGE."""
    from opteryx.connectors.opteryx_connector import OpteryxTable

    read_metadata = OpteryxTable.get_dataset_metadata
    monkeypatch.setattr(
        OpteryxTable,
        "get_dataset_metadata",
        lambda self: (read_metadata(self)[0], None),
    )

    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError) as refusal:
        list(opteryx.session(user="tester").execute_to_morsels(sql))

    assert "cannot provide row identity" in str(refusal.value)
    assert f"**{statement}**" in str(refusal.value)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
