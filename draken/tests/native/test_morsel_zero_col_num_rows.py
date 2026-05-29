"""
draken/tests/native/test_morsel_zero_col_num_rows.py

Regression tests: Morsel methods that produce zero-column results must
preserve the source row count via _zero_col_num_rows.

Root cause: select([]) / rename([]) / copy(columns=[]) all called
_make_morsel() and never set _zero_col_num_rows, so num_rows returned 0
even when the source had N rows. The filter operator drops morsels with
num_rows == 0, which caused COUNT(*) WHERE ... to return 0 instead of N.
"""

import pytest
import draken.draken_native as dn
from draken.morsels.morsel import Morsel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_morsel_with_id_col(values):
    m = Morsel()
    v = dn.vector_from_sequence(values)
    m.append_vector(b"id", v)
    return m


# ---------------------------------------------------------------------------
# select
# ---------------------------------------------------------------------------

class TestSelectZeroColNumRows:
    def test_select_empty_preserves_num_rows(self):
        m = _make_morsel_with_id_col([1, 2, 3, 4])
        result = m.select([])
        assert result.num_rows == 4
        assert result.num_columns == 0

    def test_select_empty_single_row(self):
        m = _make_morsel_with_id_col([42])
        result = m.select([])
        assert result.num_rows == 1

    def test_select_with_cols_still_correct(self):
        m = _make_morsel_with_id_col([1, 2, 3])
        result = m.select([b"id"])
        assert result.num_rows == 3
        assert result.num_columns == 1

    def test_select_empty_from_zero_row_source(self):
        m = _make_morsel_with_id_col([])
        result = m.select([])
        assert result.num_rows == 0


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------

class TestRenameZeroColNumRows:
    def test_rename_empty_preserves_num_rows(self):
        m = _make_morsel_with_id_col([10, 20, 30])
        result = m.rename([])
        assert result.num_rows == 3
        assert result.num_columns == 0

    def test_rename_with_names_still_correct(self):
        m = _make_morsel_with_id_col([5, 6])
        result = m.rename([b"new_id"])
        assert result.num_rows == 2
        assert result.num_columns == 1


# ---------------------------------------------------------------------------
# copy — three paths
# ---------------------------------------------------------------------------

class TestCopyZeroColNumRows:
    def test_copy_columns_no_match_preserves_num_rows(self):
        m = _make_morsel_with_id_col([1, 2, 3, 4, 5])
        result = m.copy(columns=[b"nonexistent"])
        assert result.num_rows == 5
        assert result.num_columns == 0

    def test_copy_columns_no_match_with_mask_uses_mask_len(self):
        m = _make_morsel_with_id_col([1, 2, 3, 4, 5])
        result = m.copy(columns=[b"nonexistent"], mask=[0, 1, 2])
        assert result.num_rows == 3
        assert result.num_columns == 0

    def test_copy_mask_only_zero_col_source_uses_mask_len(self):
        m = Morsel()
        m._zero_col_num_rows = 5
        result = m.copy(mask=[0, 1, 2])
        assert result.num_rows == 3
        assert result.num_columns == 0

    def test_copy_bare_zero_col_source_propagates(self):
        m = Morsel()
        m._zero_col_num_rows = 7
        result = m.copy()
        assert result.num_rows == 7
        assert result.num_columns == 0

    def test_copy_with_matching_cols_still_correct(self):
        m = _make_morsel_with_id_col([1, 2, 3])
        result = m.copy(columns=[b"id"])
        assert result.num_rows == 3
        assert result.num_columns == 1
