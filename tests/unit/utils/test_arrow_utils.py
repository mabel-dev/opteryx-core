"""
Tests for draken.morsels.morsel.align_tables (formerly opteryx.utils.arrow.align_tables)
"""

import array

import draken.draken_native as dn
from draken.morsels.morsel import Morsel, align_tables


def _morsel(name, values):
    return Morsel.from_vectors([name], [dn.vector_from_sequence(values)])


def _view(indices):
    """Build the int32 memoryview align_tables expects. -1 means 'no match' (null row)."""
    return array.array("i", [-1 if i is None else i for i in indices])


class TestAlignTables:
    """Tests for the align_tables function"""

    def test_align_tables_normal_path_both_have_columns(self):
        """Test normal path when both tables have columns"""
        source_data = _morsel(b"x", [10, 20, 30])
        append_data = _morsel(b"y", [40, 50, 60])
        source_indices = _view([0, 1, 2])
        append_indices = _view([2, 1, 0])

        result = align_tables(source_data, append_data, source_indices, append_indices)

        assert result.num_rows == 3
        assert result.column_names == [b"x", b"y"]
        assert result.column(b"x").to_pylist() == [10, 20, 30]
        assert result.column(b"y").to_pylist() == [60, 50, 40]

    def test_align_tables_empty_indices(self):
        """Test when indices arrays are empty"""
        source_data = _morsel(b"x", [10, 20, 30])
        append_data = _morsel(b"y", [40, 50, 60])
        source_indices = _view([])
        append_indices = _view([])

        result = align_tables(source_data, append_data, source_indices, append_indices)

        assert result.num_rows == 0
        assert result.column_names == [b"x", b"y"]

    def test_align_tables_with_none_indices_outer_join(self):
        """Test that -1 indices (outer join 'no match' sentinel) are handled correctly"""
        # This simulates an outer join where some rows don't match
        source_data = _morsel(b"x", [10, 20, 30])
        append_data = _morsel(b"y", [40, 50])
        source_indices = _view([0, 1, None])  # Third row has no match in source
        append_indices = _view([0, 1, 1])

        result = align_tables(source_data, append_data, source_indices, append_indices)

        # Should have 3 rows, with null in x for the third row
        assert result.num_rows == 3
        assert result.column_names == [b"x", b"y"]
        assert result.column(b"x").to_pylist()[2] is None  # no-match index -> null
        assert result.column(b"y").to_pylist() == [40, 50, 50]

    def test_align_tables_empty_source_table_with_schema(self):
        """Test when source table has columns but no data rows (outer join unmatched case)"""
        # This simulates a RIGHT OUTER JOIN where the left (source) side has no matches
        source_data = _morsel(b"satellite_id", [])
        append_data = _morsel(b"planet_id", [1, 2, 3])
        source_indices = _view([None, None, None])  # No matches
        append_indices = _view([0, 1, 2])

        result = align_tables(source_data, append_data, source_indices, append_indices)

        # Should preserve schema from source even though it has no data
        assert result.num_rows == 3
        assert b"satellite_id" in result.column_names
        assert b"planet_id" in result.column_names
        # All satellite_ids should be null
        assert all(v is None for v in result.column(b"satellite_id").to_pylist())
        assert result.column(b"planet_id").to_pylist() == [1, 2, 3]
