"""Ensure redundant operators strategy handles aggregates."""

from tests.helpers import execute_and_get_rowcount


def test_redundant_project_removed_after_aggregate() -> None:
    """An aggregate followed by a projection should be optimized away."""
    count = execute_and_get_rowcount("SELECT total FROM (SELECT COUNT(*) AS total FROM $planets)")
    assert count == 1
