# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Test suite for short-circuit logic null semantics validation.

This test suite validates the boolean AND/OR semantics, particularly around null
handling, to establish a baseline before Phase 6b.1 refactoring replaces numpy-based
boolean masking with Python list-based filtering.

Test Matrix:
- AND operator: TRUE/FALSE/NULL × TRUE/FALSE/NULL (9 combinations)
- OR operator: TRUE/FALSE/NULL × TRUE/FALSE/NULL (9 combinations)
- Multi-operand expressions (nesting, combinations)

Expected Semantics (SQL Standard):
AND:
  T AND T = T    T AND F = F    T AND N = N
  F AND T = F    F AND F = F    F AND N = F (short-circuit)
  N AND T = N    N AND F = F    N AND N = N

OR:
  T OR T = T     T OR F = T     T OR N = T (short-circuit)
  F OR T = T     F OR F = F     F OR N = N
  N OR T = T     N OR F = N     N OR N = N
"""

import os
import sys

import pytest

# Add parent to path for opteryx import
sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx


def create_test_table(data_dict):
    """Create a simple test table from a dictionary of column data."""
    import pyarrow as pa

    arrays = []
    names = []
    for name, values in data_dict.items():
        names.append(name)
        # Convert Python Nones to PyArrow nulls
        arrays.append(pa.array(values))

    return pa.table(dict(zip(names, arrays)))


class TestANDSemantics:
    """Test AND operator null semantics."""

    def test_and_true_true(self):
        """TRUE AND TRUE = TRUE."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [True]})
        result = session.execute_to_morsels("SELECT a AND b FROM input").to_morsels()

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True]

    def test_and_true_false(self):
        """TRUE AND FALSE = FALSE."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [False]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [False]

    def test_and_true_null(self):
        """TRUE AND NULL = NULL."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [None]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_and_false_true(self):
        """FALSE AND TRUE = FALSE."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [True]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [False]

    def test_and_false_false(self):
        """FALSE AND FALSE = FALSE."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [False]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [False]

    def test_and_false_null_short_circuits(self):
        """FALSE AND NULL = FALSE (short-circuit behavior)."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [None]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # Short-circuit: FALSE AND anything = FALSE
        assert values == [False]

    def test_and_null_true(self):
        """NULL AND TRUE = NULL."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [True]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_and_null_false(self):
        """NULL AND FALSE = FALSE (no short-circuit for NULL operand)."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [False]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # NULL AND FALSE = FALSE (FALSE is known-false)
        assert values == [False]

    def test_and_null_null(self):
        """NULL AND NULL = NULL."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [None]})
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_and_multiple_rows(self):
        """AND with multiple rows preserves per-row semantics."""
        session = opteryx.session()
        table = create_test_table(
            {"a": [True, False, None, True, False], "b": [True, False, True, None, None]}
        )
        result = session.execute_to_morsels("SELECT a AND b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True, False, None, None, False]


class TestORSemantics:
    """Test OR operator null semantics."""

    def test_or_true_true(self):
        """TRUE OR TRUE = TRUE."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [True]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True]

    def test_or_true_false(self):
        """TRUE OR FALSE = TRUE."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [False]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True]

    def test_or_true_null_short_circuits(self):
        """TRUE OR NULL = TRUE (short-circuit behavior)."""
        session = opteryx.session()
        table = create_test_table({"a": [True], "b": [None]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # Short-circuit: TRUE OR anything = TRUE
        assert values == [True]

    def test_or_false_true(self):
        """FALSE OR TRUE = TRUE."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [True]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True]

    def test_or_false_false(self):
        """FALSE OR FALSE = FALSE."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [False]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [False]

    def test_or_false_null(self):
        """FALSE OR NULL = NULL (no short-circuit for NULL)."""
        session = opteryx.session()
        table = create_test_table({"a": [False], "b": [None]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_or_null_true(self):
        """NULL OR TRUE = TRUE (TRUE is known-true)."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [True]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True]

    def test_or_null_false(self):
        """NULL OR FALSE = NULL."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [False]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_or_null_null(self):
        """NULL OR NULL = NULL."""
        session = opteryx.session()
        table = create_test_table({"a": [None], "b": [None]})
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [None]

    def test_or_multiple_rows(self):
        """OR with multiple rows preserves per-row semantics."""
        session = opteryx.session()
        table = create_test_table(
            {"a": [True, False, None, True, False], "b": [True, False, True, None, None]}
        )
        result = session.execute_to_morsels("SELECT a OR b FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        assert values == [True, False, True, True, None]


class TestComplexExpressions:
    """Test complex boolean expressions with nulls."""

    def test_nested_and_or(self):
        """(a AND b) OR (c AND d) with mixed nulls."""
        session = opteryx.session()
        table = create_test_table(
            {
                "a": [True, False, None],
                "b": [True, True, True],
                "c": [False, False, False],
                "d": [True, None, None],
            }
        )
        result = session.execute_to_morsels("SELECT (a AND b) OR (c AND d) FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # Row 0: (T AND T) OR (F AND T) = T OR F = T
        # Row 1: (F AND T) OR (F AND N) = F OR F = F
        # Row 2: (N AND T) OR (F AND N) = N OR F = N
        assert values == [True, False, None]

    def test_three_operand_and(self):
        """a AND b AND c with mixed nulls."""
        session = opteryx.session()
        table = create_test_table(
            {
                "a": [True, False, None, True],
                "b": [True, True, True, False],
                "c": [True, True, True, True],
            }
        )
        result = session.execute_to_morsels("SELECT a AND b AND c FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # Row 0: T AND T AND T = T
        # Row 1: F AND T AND T = F (short-circuit)
        # Row 2: N AND T AND T = N
        # Row 3: T AND F AND T = F (short-circuit after AND b)
        assert values == [True, False, None, False]

    def test_three_operand_or(self):
        """a OR b OR c with mixed nulls."""
        session = opteryx.session()
        table = create_test_table(
            {
                "a": [False, True, None, False],
                "b": [False, True, True, False],
                "c": [False, True, True, None],
            }
        )
        result = session.execute_to_morsels("SELECT a OR b OR c FROM input")

        morsels = list(result)
        assert len(morsels) == 1
        values = morsels[0][0].to_pylist()
        # Row 0: F OR F OR F = F
        # Row 1: T OR T OR T = T (short-circuit)
        # Row 2: N OR T OR T = T
        # Row 3: F OR F OR N = N
        assert values == [False, True, True, None]
