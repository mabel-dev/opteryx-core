import pytest

from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.exceptions import UnsupportedSyntaxError


def test_rewrite_explain_json_unsupported():
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite("EXPLAIN ANALYZE FORMAT JSON SELECT 1")


def test_rewrite_explain_graphviz_unsupported():
    with pytest.raises(UnsupportedSyntaxError):
        do_sql_rewrite("EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT 1")


def test_rewrite_explain_mermaid_rewrites_to_graphviz():
    out = do_sql_rewrite("EXPLAIN ANALYZE FORMAT MERMAID SELECT 1")
    assert "FORMAT GRAPHVIZ" in out
    # ensure we didn't accidentally raise
    assert "MERMAID" not in out
