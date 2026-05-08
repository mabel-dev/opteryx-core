# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for Layer B: pushdown of unary metadata-answerable predicates
(IsNull / IsNotNull / IsEmpty / IsNotEmpty) into ParquetReadNode.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors.filesystem_connector import FileSystemTable


def _read_node_predicate_ops(sql: str):
    """Run a query and return a list of (node_class_name, [op_value, ...])
    for every read node that received a pushed predicate."""
    session = opteryx.session()
    try:
        list(session.execute_to_morsels(sql))
        out = []
        for nid in session._plan.nodes():
            op = session._plan[nid]
            name = type(op).__name__
            if "Read" not in name and "Scan" not in name:
                continue
            preds = getattr(op, "predicates", None) or []
            if preds:
                out.append((name, [getattr(p, "value", None) for p in preds]))
        return out
    finally:
        session.close()


def _rowcount(sql: str) -> int:
    session = opteryx.session()
    try:
        n = 0
        for morsel in session.execute_to_morsels(sql):
            n += morsel.num_rows
        return n
    finally:
        session.close()


# ─── plan-inspection: predicates land on ParquetReadNode ─────────────────────


def test_eq_empty_is_pushed_to_parquet_read():
    pushed = _read_node_predicate_ops(
        "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase = ''"
    )
    assert pushed, "expected predicate to be pushed onto a read node"
    name, ops = pushed[0]
    assert "ParquetRead" in name
    assert "IsEmpty" in ops


def test_neq_empty_is_pushed_to_parquet_read():
    pushed = _read_node_predicate_ops(
        "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase <> ''"
    )
    assert pushed
    name, ops = pushed[0]
    assert "ParquetRead" in name
    assert "IsNotEmpty" in ops


def test_is_null_is_pushed_to_parquet_read():
    pushed = _read_node_predicate_ops(
        "SELECT name FROM testdata.satellites WHERE name IS NULL"
    )
    assert pushed
    name, ops = pushed[0]
    assert "ParquetRead" in name
    assert "IsNull" in ops


def test_is_not_null_is_pushed_to_parquet_read():
    pushed = _read_node_predicate_ops(
        "SELECT name FROM testdata.satellites WHERE name IS NOT NULL"
    )
    assert pushed
    name, ops = pushed[0]
    assert "ParquetRead" in name
    assert "IsNotNull" in ops


def test_eq_string_literal_still_pushed_regression():
    """Regression: pre-existing comparison pushdown must keep working."""
    pushed = _read_node_predicate_ops(
        "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase = 'baby'"
    )
    assert pushed
    name, ops = pushed[0]
    assert "ParquetRead" in name
    assert "Eq" in ops


# ─── mixed unary + comparison ────────────────────────────────────────────────


def test_unary_or_comparison_does_not_crash():
    """OR of a unary and a comparison shouldn't crash; predicate-pushdown's
    split_conjunctive_predicates only handles AND, so the OR remains as a
    FilterNode predicate. Just verify the query runs and returns a sane count."""
    n = _rowcount(
        "SELECT URL FROM testdata.clickbench_tiny "
        "WHERE SearchPhrase = '' OR URL = 'http://example.com/'"
    )
    assert n > 0


# ─── end-to-end correctness ──────────────────────────────────────────────────


def test_neq_empty_correctness():
    """Push-down result must equal the alternative formulation length() > 0."""
    pushed = _rowcount(
        "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase <> ''"
    )
    alt = _rowcount(
        "SELECT URL FROM testdata.clickbench_tiny WHERE LENGTH(SearchPhrase) > 0"
    )
    assert pushed == alt
    assert pushed > 0


def test_eq_empty_correctness():
    pushed = _rowcount(
        "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase = ''"
    )
    alt = _rowcount(
        "SELECT URL FROM testdata.clickbench_tiny WHERE LENGTH(SearchPhrase) = 0"
    )
    assert pushed == alt
    assert pushed > 0


# ─── connector that doesn't opt in falls back to FilterNode ──────────────────


def test_connector_without_opt_in_falls_back():
    """If a connector's PUSHABLE_OPS doesn't include the unary op, the predicate
    must NOT land on the read node — it stays in a FilterNode and still returns
    correct results."""
    cls = FileSystemTable
    saved = dict(cls.PUSHABLE_OPS)
    cls.PUSHABLE_OPS = {**saved, "IsEmpty": False, "IsNotEmpty": False}
    try:
        pushed = _read_node_predicate_ops(
            "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase = ''"
        )
        # No predicate should land on the read node.
        for _name, ops in pushed:
            assert "IsEmpty" not in ops, (
                "IsEmpty must not be pushed when connector opts out"
            )
        # And the query still returns rows (FilterNode handles it).
        n = _rowcount(
            "SELECT URL FROM testdata.clickbench_tiny WHERE SearchPhrase = ''"
        )
        assert n > 0
    finally:
        cls.PUSHABLE_OPS = saved


if __name__ == "__main__":  # pragma: no cover
    test_eq_empty_is_pushed_to_parquet_read()
    test_neq_empty_is_pushed_to_parquet_read()
    test_is_null_is_pushed_to_parquet_read()
    test_is_not_null_is_pushed_to_parquet_read()
    test_eq_string_literal_still_pushed_regression()
    test_unary_or_comparison_does_not_crash()
    test_neq_empty_correctness()
    test_eq_empty_correctness()
    test_connector_without_opt_in_falls_back()
    print("All Layer B unary-pushdown tests passed.")
