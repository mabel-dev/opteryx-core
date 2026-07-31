"""
ANALYZE / DROP STATISTICS dispatch for catalog-backed datasets.

`ANALYZE TABLE <name>` used to raise UnsupportedSyntaxError for every
catalog-backed (production) dataset — the whole local-filesystem statistics
pipeline in `_analyze.py` is inapplicable there, since the catalog owns its
own manifest and snapshot chain. It now delegates to the catalog's own
`SimpleDataset.refresh_manifest`, which runs the SAME native statistics
kernels and commits a `statistics-refresh` snapshot without rewriting any
data files (deliberately lighter than compaction).

These tests exercise the DISPATCH decision — that the right method is called
with the right arguments, and that the two still-unsupported shapes fail
loudly BEFORE doing any work — plus the operator wiring that carries the
session's user through to the catalog as the snapshot's author. The
catalog-side behaviour they delegate to is tested in the catalog repo's own
`tests/test_refresh_manifest.py`; the local-filesystem path is tested in
`test_analyze_statistics.py`, which must keep passing untouched.

No GCP credentials needed: `OpteryxTable` accepts a `prefetched_table=`
kwarg, so a Mock stands in for the catalog's `SimpleDataset` while the object
under test is a REAL `OpteryxTable` — the `isinstance` check the dispatch
relies on is exercised for real, not duck-typed around.
"""

import os
import sys
from unittest.mock import Mock
from unittest.mock import patch

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.operators.table_management import TableManagementNode
from opteryx.operators.table_management._analyze import analyze_table
from opteryx.operators.table_management._analyze import drop_statistics


def _catalog_table_engine():
    """A real OpteryxTable whose catalog dataset is a Mock."""
    simple_dataset = Mock()
    table_engine = OpteryxTable(
        "namespace.table", Mock(), "workspace", prefetched_table=simple_dataset
    )
    return table_engine, simple_dataset


def _run_node(action, variables, columns=None):
    """Drive a real TableManagementNode end to end, with only the connector
    lookup stubbed. This is the path `ANALYZE TABLE` actually takes at
    execution time — `_author` is a property on the COMPILED operator
    (table_management.pyx), so calling `analyze_table()` directly (as the
    tests above do) never exercises it."""
    table_engine, simple_dataset = _catalog_table_engine()
    connector = Mock()
    connector.table_engine.return_value = table_engine

    node = TableManagementNode(
        QueryProperties(query_id="test-qid", variables=variables),
        action=action,
        table_name="namespace.table",
        analyze_columns=columns,
    )
    with patch("opteryx.connectors.connector_factory", return_value=connector):
        result = node()
    return result, simple_dataset


def test_analyze_delegates_to_catalog_refresh_manifest():
    table_engine, simple_dataset = _catalog_table_engine()

    result = analyze_table(table_engine, [], author="alice")

    simple_dataset.refresh_manifest.assert_called_once_with(
        agent="opteryx-analyze", author="alice"
    )
    assert result == 1


def test_analyze_passes_none_author_through_unsubstituted():
    # An unauthenticated session must not have an identity invented for it —
    # the catalog decides whether to reject an unattributed write.
    table_engine, simple_dataset = _catalog_table_engine()

    analyze_table(table_engine, [])

    simple_dataset.refresh_manifest.assert_called_once_with(
        agent="opteryx-analyze", author=None
    )


def test_analyze_for_columns_rejected_before_any_refresh_work():
    table_engine, simple_dataset = _catalog_table_engine()

    with pytest.raises(UnsupportedSyntaxError) as exc:
        analyze_table(table_engine, ["col_a"], author="alice")

    # Fail loud, fail EARLY — the refresh must not have started.
    simple_dataset.refresh_manifest.assert_not_called()
    assert "FOR COLUMNS" in str(exc.value)


def test_drop_statistics_rejected_for_catalog_dataset():
    table_engine, simple_dataset = _catalog_table_engine()

    with pytest.raises(UnsupportedSyntaxError) as exc:
        drop_statistics(table_engine, [])

    simple_dataset.refresh_manifest.assert_not_called()
    assert "DROP STATISTICS" in str(exc.value)


def test_drop_statistics_rejected_for_catalog_dataset_with_columns():
    table_engine, simple_dataset = _catalog_table_engine()

    with pytest.raises(UnsupportedSyntaxError):
        drop_statistics(table_engine, ["col_a"])

    simple_dataset.refresh_manifest.assert_not_called()


def test_catalog_refresh_failure_propagates():
    # refresh_manifest raises ManifestRefreshError when it can't recompute
    # every file's stats. That must surface, not be swallowed into a
    # success-looking result.
    table_engine, simple_dataset = _catalog_table_engine()
    simple_dataset.refresh_manifest.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        analyze_table(table_engine, [])


# ── operator wiring: session user -> snapshot author ────────────────────────
#
# `_author` lives on the COMPILED TableManagementNode (table_management.pyx),
# so none of the tests above — which call `analyze_table()` directly — touch
# it. These drive the real operator, which is the only path that proves the
# session's identity actually reaches the catalog.


def test_node_carries_the_session_user_through_as_the_snapshot_author():
    from opteryx.constants import QueryStatus

    result, simple_dataset = _run_node(
        "analyze_table", {"external_user": "alice"}, columns=[]
    )

    simple_dataset.refresh_manifest.assert_called_once_with(
        agent="opteryx-analyze", author="alice"
    )
    assert result.status == QueryStatus.SQL_SUCCESS
    assert result.record_count == 1


def test_node_passes_none_author_for_an_unauthenticated_session():
    # No `external_user` set: the catalog must receive None and decide for
    # itself whether to reject an unattributed write — an identity is never
    # invented here (same contract as Insert/RelationManagement).
    _result, simple_dataset = _run_node("analyze_table", {}, columns=[])

    simple_dataset.refresh_manifest.assert_called_once_with(
        agent="opteryx-analyze", author=None
    )


def test_node_treats_empty_external_user_as_unauthenticated():
    # `external_user` defaults to "" (see variables.py) rather than being
    # absent, so the empty string must normalise to None, not be passed
    # through as a zero-length author.
    _result, simple_dataset = _run_node(
        "analyze_table", {"external_user": ""}, columns=[]
    )

    simple_dataset.refresh_manifest.assert_called_once_with(
        agent="opteryx-analyze", author=None
    )


def test_node_rejects_for_columns_before_reaching_the_catalog():
    with pytest.raises(UnsupportedSyntaxError):
        _run_node("analyze_table", {"external_user": "alice"}, columns=["col_a"])


def test_node_rejects_drop_statistics_for_a_catalog_dataset():
    with pytest.raises(UnsupportedSyntaxError):
        _run_node("drop_statistics", {"external_user": "alice"}, columns=[])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
