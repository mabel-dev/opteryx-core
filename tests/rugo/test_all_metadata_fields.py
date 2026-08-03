"""
Test that all metadata fields from the C++ ColumnStats struct are exposed to Python.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import glob

import pytest

import rugo.parquet as parquet
import rugo.rugo_native as _native
from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info


def _first_column_metadata(file_path):
    """Merge row-group stats + chunk info for the first row group's first
    column: the current API splits what used to be one dict-shaped
    `read_metadata` result across `read_rowgroup_stats` (min/max/null_count/
    distinct_count/bloom) and `fetch_column_chunk_info` (offsets/encodings/
    codec/num_values). `path_in_schema` is folded into `name` (dotted path)
    in the current model, not a separate field. `index_page_offset` and
    `key_value_metadata` exist on the C++ ColumnStats struct but are not
    currently exposed to Python by either function.
    """
    with open(file_path, "rb") as f:
        data = f.read()
    rg_stats = _native.read_rowgroup_stats(data)[0]["columns"][0]
    # fetch_column_chunk_info matches by top-level (dot-truncated) name, which
    # is exactly what read_rowgroup_stats already reports as "name" — pass it
    # straight through rather than the fully dotted schema_columns name (that
    # mismatches for nested/repeated schemas, e.g. binary.parquet's "foo").
    chunk_info = fetch_column_chunk_info(file_path, 0, [rg_stats["name"]])[rg_stats["name"]]
    return {**rg_stats, **chunk_info}


def test_all_metadata_fields_exposed():
    """Test that all currently-exposed ColumnStats fields are present."""
    files_to_test = glob.glob("testdata/**/*.parquet", recursive=True)
    assert files_to_test, "No parquet test files found"

    for file_path in files_to_test:
        print(f"\nTesting file: {file_path}")

        col = _first_column_metadata(file_path)

        # Fields currently exposed to Python across read_rowgroup_stats +
        # fetch_column_chunk_info. Not included (not exposed anywhere):
        # index_page_offset, key_value_metadata. path_in_schema is folded
        # into "name".
        expected_fields = {
            # Basic fields
            "name",
            "physical_type",
            "logical_type",
            # Sizes & counts
            "num_values",
            "total_compressed_size",
            # Offsets
            "data_page_offset",
            "dictionary_page_offset",
            # telemetry
            "min",
            "max",
            "null_count",
            "distinct_count",
            # Bloom filter
            "bloom_offset",
            "bloom_length",
            # Clustering: this row group's parquet SortingColumn claim for the
            # column, surfaced only for files rugo itself wrote (created_by
            # trust gate) — always False for a foreign writer's file.
            "is_sorted",
            "sort_descending",
            "sort_nulls_first",
            # Encodings & codec
            "encodings",
            "compression_codec",
            "max_definition_level",
            "max_repetition_level",
            "type_length",
        }

        actual_fields = set(col.keys())
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields

        assert not missing_fields, f"Missing fields in column metadata: {missing_fields}"
        assert not extra_fields, f"Unexpected fields in column metadata: {extra_fields}"

        print(f"✅ All {len(expected_fields)} expected fields are present in column metadata")


def test_metadata_field_types():
    """Test that metadata fields have the correct types."""
    files_to_test = glob.glob("testdata/**/*.parquet", recursive=True)
    assert files_to_test, "No parquet test files found"

    for file_path in files_to_test:
        col = _first_column_metadata(file_path)

        # Check types
        assert isinstance(col["name"], str)
        assert isinstance(col["physical_type"], str)
        assert isinstance(col["logical_type"], str)

        # These can be int or None
        assert col["num_values"] is None or isinstance(col["num_values"], int)
        assert col["total_compressed_size"] is None or isinstance(col["total_compressed_size"], int)
        assert col["data_page_offset"] is None or isinstance(col["data_page_offset"], int)
        assert col["dictionary_page_offset"] is None or isinstance(
            col["dictionary_page_offset"], int
        )
        assert col["null_count"] is None or isinstance(col["null_count"], int)
        assert col["distinct_count"] is None or isinstance(col["distinct_count"], int)
        assert col["bloom_offset"] is None or isinstance(col["bloom_offset"], int)
        assert col["bloom_length"] is None or isinstance(col["bloom_length"], int)

        # Encodings should be a list of strings
        assert isinstance(col["encodings"], list)
        assert all(isinstance(enc, str) for enc in col["encodings"])

        # Compression codec should be string or None
        assert col["compression_codec"] is None or isinstance(col["compression_codec"], str)

        print("✅ All field types are correct")


def test_metadata_field_values():
    """Test that metadata field values are reasonable."""
    files_to_test = glob.glob("testdata/**/*.parquet", recursive=True)
    assert files_to_test, "No parquet test files found"

    for file_path in files_to_test:
        col = _first_column_metadata(file_path)

        # Basic fields should be present
        assert col.get("name") is not None
        assert col.get("physical_type") is not None
        assert col.get("logical_type") is not None

        # Sizes should be positive if present
        if col["num_values"] is not None:
            assert col["num_values"] > 0
        if col["total_compressed_size"] is not None:
            assert col["total_compressed_size"] > 0

        # Offsets should be non-negative if present
        if col["data_page_offset"] is not None:
            assert col["data_page_offset"] >= 0
        if col["dictionary_page_offset"] is not None:
            assert col["dictionary_page_offset"] >= 0

        # Encodings should be non-empty
        assert len(col["encodings"]) > 0

        # Compression codec should be a known value if present
        if col["compression_codec"] is not None:
            known_codecs = {
                "UNCOMPRESSED",
                "SNAPPY",
                "GZIP",
                "LZO",
                "BROTLI",
                "LZ4",
                "ZSTD",
                "LZ4_RAW",
                "UNKNOWN",
            }
            assert col["compression_codec"] in known_codecs, (
                f"Unknown codec: {col['compression_codec']}"
            )

        print("✅ Field values are reasonable")
        print(f"   - Name: {col['name']}")
        print(f"   - Type: {col['physical_type']} ({col['logical_type']})")
        print(f"   - Num values: {col['num_values']}")
        print(f"   - Compressed size: {col['total_compressed_size']} bytes")
        print(f"   - Encodings: {col['encodings']}")
        print(f"   - Codec: {col['compression_codec']}")


def test_multiple_columns():
    """Test that all columns have the complete chunk metadata via fetch_column_chunk_info."""
    path = "testdata/planets/planets.parquet"
    schema = parquet.read_metadata(path)
    column_names = [c.name for c in schema.schema_columns]

    col_info = fetch_column_chunk_info(path, 0, column_names)

    expected_fields = {
        "name",
        "physical_type",
        "logical_type",
        "num_values",
        "total_compressed_size",
        "data_page_offset",
        "dictionary_page_offset",
        "encodings",
        "compression_codec",
        "max_definition_level",
        "max_repetition_level",
    }

    for col_name, col in col_info.items():
        actual_fields = set(col.keys())
        missing_fields = expected_fields - actual_fields
        assert not missing_fields, f"Column '{col_name}' missing fields: {missing_fields}"

    print(f"✅ All {len(col_info)} columns have complete metadata")


if __name__ == "__main__":
    pytest.main([__file__])
