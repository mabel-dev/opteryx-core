"""
Targeted expression engine tests (value-checked gates for bytecode executor).

This suite validates the expression execution path with focus on:
- Binary operators (arithmetic, bitwise, string concat) with null handling
- CAST operations
- EXTRACTION (array/string indexing)
- CASE expressions
- Known correctness regressions (standing bugs)

Gate: `make et` (expression tests). Runs seconds; fast enough for every development cycle.
Scope: correctness only; performance (C-native dispatch) is secondary.
"""

import pytest
import opteryx


def column_values(morsel, col_idx=0):
    """Return a column's values as a list.

    Morsel subscript is row access, so address the column explicitly by name.
    """
    return list(morsel.column(morsel.column_names[col_idx]))


def get_first_value(morsel, col_idx=0):
    """Extract first value from a morsel column."""
    col = column_values(morsel, col_idx)
    return col[0] if col else None


@pytest.fixture
def session():
    """Create a fresh session for each test."""
    return opteryx.session()


# ===========================================================================
# BINARY OPERATORS: ARITHMETIC
# ===========================================================================
class TestBinaryOpArithmetic:
    """Test arithmetic binary operators with null propagation."""

    def test_add_positive(self, session):
        """Test: 2 + 3 = 5."""
        morsels = list(session.execute_to_morsels("SELECT 2 + 3 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 5

    def test_add_with_null(self, session):
        """Test: NULL + 1 = NULL (null propagation)."""
        morsels = list(session.execute_to_morsels("SELECT NULL + 1 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) is None

    def test_subtract(self, session):
        """Test: 10 - 3 = 7."""
        morsels = list(session.execute_to_morsels("SELECT 10 - 3 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 7

    def test_multiply(self, session):
        """Test: 4 * 5 = 20."""
        morsels = list(session.execute_to_morsels("SELECT 4 * 5 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 20

    def test_divide(self, session):
        """Test: 20 / 4 = 5."""
        morsels = list(session.execute_to_morsels("SELECT 20 / 4 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 5

    def test_modulo(self, session):
        """Test: 17 % 5 = 2."""
        morsels = list(session.execute_to_morsels("SELECT 17 % 5 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 2


# ===========================================================================
# BINARY OPERATORS: BITWISE
# ===========================================================================
class TestBinaryOpBitwise:
    """Test bitwise binary operators (regression guards)."""

    def test_bitwise_or(self, session):
        """Test: 1 | 2 = 3 (regression: Phase 9c SIGBUS)."""
        morsels = list(session.execute_to_morsels("SELECT 1 | 2 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 3

    def test_bitwise_and(self, session):
        """Test: 5 & 3 = 1."""
        morsels = list(session.execute_to_morsels("SELECT 5 & 3 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 1

    def test_bitwise_xor(self, session):
        """Test: 5 ^ 3 = 6."""
        morsels = list(session.execute_to_morsels("SELECT 5 ^ 3 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 6



# ===========================================================================
# BINARY OPERATORS: STRING CONCATENATION
# ===========================================================================
class TestBinaryOpStringConcat:
    """Test string concatenation (regression guard)."""

    def test_string_concat(self, session):
        """Test: 'a' || 'b' = 'ab' (regression: Phase 9c SIGBUS)."""
        morsels = list(session.execute_to_morsels("SELECT 'a' || 'b' AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'ab'

    def test_string_concat_exclamation(self, session):
        """Test: 'hello' || '!' = 'hello!' (ticket regression repro)."""
        morsels = list(session.execute_to_morsels("SELECT 'hello' || '!' AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'hello!'


# ===========================================================================
# BINARY OPERATORS: NULL HANDLING (PARTIAL NULL COLUMNS)
# ===========================================================================
class TestBinaryOpNullHandling:
    """Test null propagation for binary ops on partial-null columns."""

    def test_add_with_literal_and_null_result(self, session):
        """Test: 1 + 10 = 11 (simple non-null case)."""
        morsels = list(session.execute_to_morsels("SELECT 1 + 10 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 11

    def test_null_plus_null(self, session):
        """Test: NULL + NULL = NULL."""
        morsels = list(session.execute_to_morsels("SELECT NULL + NULL AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) is None


# ===========================================================================
# CAST OPERATIONS
# ===========================================================================
class TestCast:
    """Test CAST operations (C-native dispatch, working)."""

    def test_cast_int_to_string(self, session):
        """Test: CAST(123 AS VARCHAR)."""
        morsels = list(session.execute_to_morsels("SELECT CAST(123 AS VARCHAR) AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == '123'

    def test_cast_string_to_int(self, session):
        """Test: CAST('456' AS INTEGER)."""
        morsels = list(session.execute_to_morsels("SELECT CAST('456' AS INTEGER) AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 456

    def test_cast_float_to_int(self, session):
        """Test: CAST(3.7 AS INTEGER) truncates to 3."""
        morsels = list(session.execute_to_morsels("SELECT CAST(3.7 AS INTEGER) AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 3

    def test_cast_with_null(self, session):
        """Test: CAST(NULL AS VARCHAR) = NULL."""
        morsels = list(session.execute_to_morsels("SELECT CAST(NULL AS VARCHAR) AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) is None

    def test_cast_bool_to_int(self, session):
        """Test: CAST(TRUE AS INTEGER) = 1, CAST(FALSE AS INTEGER) = 0."""
        morsels = list(session.execute_to_morsels("SELECT CAST(TRUE AS INTEGER) AS t, CAST(FALSE AS INTEGER) AS f"))
        assert len(morsels) > 0
        t_col = column_values(morsels[0], 0)
        f_col = column_values(morsels[0], 1)
        assert t_col[0] == 1
        assert f_col[0] == 0


# ===========================================================================
# EXTRACTION (ARRAY/STRING SUBSCRIPT)
# ===========================================================================
class TestExtraction:
    """Test extraction operations (C-native dispatch, working)."""

    def test_string_subscript(self, session):
        """Test: 'hello'[0] = 'h'."""
        morsels = list(session.execute_to_morsels("SELECT 'hello'[0] AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'h'


# ===========================================================================
# CASE EXPRESSIONS
# ===========================================================================
class TestCase:
    """Test CASE expression evaluation."""

    def test_case_simple_when_true(self, session):
        """Test: CASE WHEN TRUE THEN 'yes' ELSE 'no' END = 'yes'."""
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN TRUE THEN 'yes' ELSE 'no' END AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'yes'

    def test_case_simple_when_false(self, session):
        """Test: CASE WHEN FALSE THEN 'yes' ELSE 'no' END = 'no'."""
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN FALSE THEN 'yes' ELSE 'no' END AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'no'

    def test_case_multiple_when(self, session):
        """Test CASE with multiple WHEN branches."""
        sql = "SELECT CASE WHEN FALSE THEN 'a' WHEN TRUE THEN 'b' ELSE 'c' END AS result"
        morsels = list(session.execute_to_morsels(sql))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'b'

    def test_case_no_else_true_branch(self, session):
        """Test CASE without ELSE when condition matches."""
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN TRUE THEN 'yes' END AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'yes'

    def test_case_no_else_false_branch(self, session):
        """Test CASE without ELSE when no condition matches (returns NULL)."""
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN FALSE THEN 'yes' END AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) is None

    def test_case_with_int_result(self, session):
        """Test CASE with integer result."""
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN TRUE THEN 10 WHEN FALSE THEN 20 ELSE 30 END AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 10


# ===========================================================================
# STANDING CORRECTNESS BUGS (REGRESSION GUARDS)
# ===========================================================================
class TestStandingBugs:
    """
    Guard against known correctness regressions.
    These are separate tickets; this suite ensures they're not masked.
    """

    def test_case_when_without_else_returns_null(self, session):
        """
        Standing bug: CASE WHEN FALSE THEN 1 END should return NULL, not some other sentinel.
        """
        morsels = list(session.execute_to_morsels("SELECT CASE WHEN FALSE THEN 1 END AS result"))
        assert len(morsels) > 0
        result = get_first_value(morsels[0], 0)
        assert result is None

    def test_case_when_fixed_width_no_else_all_match(self, session):
        """CASE WHEN with fixed-width result and no ELSE, all rows match."""
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        assert col == [1, 1, 1, 1]

    def test_case_when_fixed_width_no_else_no_match(self, session):
        """CASE WHEN with fixed-width result and no ELSE, no rows match."""
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id < 0 THEN 1 END FROM $planets LIMIT 4"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        assert col == [None, None, None, None]

    def test_case_when_fixed_width_else_partial_match(self, session):
        """CASE WHEN with ELSE clause and fixed-width result, partial match."""
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id = 1 THEN 99 ELSE 88 END FROM $planets LIMIT 4"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        # Row 0 has id=1, rows 1-3 have id!=1
        assert col[0] == 99
        assert col[1] == 88
        assert col[2] == 88
        assert col[3] == 88

    def test_case_when_null_then_else_column(self, session):
        """Repro A: NULL-producing THEN branch with a fixed-width COLUMN ELSE.

        Was SIGBUS: the bare ``ELSE id`` result compiled to a single
        BC_LOAD_COL flagged is_pure_bitmap; evaluate_bitmap's prepass detected
        the non-bool column and returned its -1 fall-back sentinel, which the
        ``except -1`` declaration mis-propagated as a (non-existent) exception.
        """
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id > 4 THEN NULL ELSE id END FROM $planets LIMIT 6"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        assert col == [1, 2, 3, 4, None, None]

    def test_case_when_column_result_no_else(self, session):
        """Repro B: partial match, no ELSE, bare COLUMN result.

        Was SIGBUS for the same root cause as repro A — the bare ``THEN id``
        result is a single is_pure_bitmap BC_LOAD_COL of a non-bool column.
        """
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        assert col == [1, None, None, None]

    def test_case_when_column_result_all_match(self, session):
        """Control for repros A/B: bare COLUMN result, every row matches."""
        morsels = list(session.execute_to_morsels(
            "SELECT CASE WHEN id < 100 THEN id END FROM $planets LIMIT 4"
        ))
        assert len(morsels) > 0
        col = column_values(morsels[0], 0)
        assert col == [1, 2, 3, 4]

    def test_case_when_no_else_two_queries_same_session(self, session):
        """Two distinct CASE-no-ELSE-INT queries in one session must not crash.

        Was SIGSEGV: the second query crashed with memory corruption left by
        the first. Same assemble_fixed root cause as repros A/B; this guards
        the session-reuse path that single-query tests don't exercise.
        """
        col1 = column_values(list(session.execute_to_morsels(
            "SELECT CASE WHEN id < 100 THEN 1 END FROM $planets LIMIT 4"
        ))[0], 0)
        assert col1 == [1, 1, 1, 1]

        col2 = column_values(list(session.execute_to_morsels(
            "SELECT CASE WHEN id = 1 THEN id END FROM $planets LIMIT 4"
        ))[0], 0)
        assert col2 == [1, None, None, None]


class TestZeroColumnAggregates:
    """COUNT(*) over a filtered relation — query-level gate for the
    zero-column ``num_rows`` defect (``bug-count-star-where-zero-col-select.md``).

    The filter projects away every column for COUNT(*), producing a
    zero-column morsel; ``Morsel.select([])`` must carry the surviving row
    count or the filter drops the morsel and COUNT(*) returns 0.
    """

    def _count(self, session, sql):
        morsels = list(session.execute_to_morsels(sql))
        assert len(morsels) > 0
        return get_first_value(morsels[0], 0)

    def test_count_star_where_gt(self, session):
        assert self._count(session, "SELECT COUNT(*) FROM $planets WHERE id > 5") == 4

    def test_count_star_where_eq(self, session):
        assert self._count(session, "SELECT COUNT(*) FROM $planets WHERE id = 3") == 1

    def test_count_star_where_none_match(self, session):
        assert self._count(session, "SELECT COUNT(*) FROM $planets WHERE id < 0") == 0

    def test_count_star_no_filter_unchanged(self, session):
        assert self._count(session, "SELECT COUNT(*) FROM $planets") == 9

    def test_count_star_with_other_agg_unchanged(self, session):
        # MAX(id) keeps `id` alive, so this path never hit the zero-column bug.
        morsels = list(session.execute_to_morsels(
            "SELECT COUNT(*), MAX(id) FROM $planets WHERE id > 5"
        ))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 4


# ===========================================================================
# INTEGRATION: COMBINED OPERATORS IN ONE EXPRESSION
# ===========================================================================
class TestCombinedExpressions:
    """Test expressions combining multiple operator types."""

    def test_cast_then_arithmetic(self, session):
        """Test: CAST('10' AS INTEGER) + 5 = 15."""
        morsels = list(session.execute_to_morsels("SELECT CAST('10' AS INTEGER) + 5 AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 15

    def test_arithmetic_and_comparison(self, session):
        """Test: (2 + 3) = 5."""
        morsels = list(session.execute_to_morsels("SELECT (2 + 3) AS result"))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 5

    def test_string_ops_in_case(self, session):
        """Test CASE with string concatenation."""
        sql = "SELECT CASE WHEN TRUE THEN 'a' || 'b' ELSE 'c' END AS result"
        morsels = list(session.execute_to_morsels(sql))
        assert len(morsels) > 0
        assert get_first_value(morsels[0], 0) == 'ab'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
