"""
Test fixtures and data conversion for _jsonl reader tests
"""

import pytest
import json
import os
from pathlib import Path


def pytest_configure(config):
    """
    Convert existing parquet test datasets to JSONL on first test run.
    This allows side-by-side comparison with PyArrow and existing reader.
    """
    testdata_dir = Path(__file__).parent.parent.parent.parent / "testdata"

    # Create JSONL versions of test datasets
    convert_testdata_to_jsonl(testdata_dir)


def convert_testdata_to_jsonl(testdata_dir):
    """Convert parquet datasets to JSONL format."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("PyArrow not available for test data conversion")
        return

    # TPCH tiny
    tpch_dir = testdata_dir / "tpch_tiny"
    tpch_jsonl_dir = testdata_dir / "tpch_tiny_jsonl"
    if tpch_dir.exists() and not tpch_jsonl_dir.exists():
        tpch_jsonl_dir.mkdir(exist_ok=True)
        for parquet_file in tpch_dir.glob("*/*.parquet"):
            table = pq.read_table(str(parquet_file))
            jsonl_file = tpch_jsonl_dir / parquet_file.parent.name / f"{parquet_file.stem}.jsonl"
            jsonl_file.parent.mkdir(exist_ok=True)
            write_table_as_jsonl(table, str(jsonl_file))

    # ClickBench tiny
    clickbench_file = testdata_dir / "clickbench_tiny" / "hits_48.parquet"
    clickbench_jsonl = testdata_dir / "clickbench_tiny" / "hits_48.jsonl"
    if clickbench_file.exists() and not clickbench_jsonl.exists():
        table = pq.read_table(str(clickbench_file))
        write_table_as_jsonl(table, str(clickbench_jsonl))


def write_table_as_jsonl(table, output_path):
    """Write PyArrow table to JSONL format."""
    import datetime

    class JSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            if isinstance(obj, (bytes, bytearray)):
                return obj.hex()
            return super().default(obj)

    with open(output_path, "w") as f:
        for row in table.to_pylist():
            json.dump(row, f, cls=JSONEncoder)
            f.write("\n")


@pytest.fixture
def sample_jsonl_bytes():
    """Simple JSONL sample for quick tests."""
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    lines = [json.dumps(row) for row in data]
    return "\n".join(lines).encode("utf-8")


@pytest.fixture
def escaped_jsonl_bytes():
    """JSONL with escaped quotes and special characters."""
    data = [
        {"id": 1, "text": 'Hello "World"', "value": 'A\\B'},
        {"id": 2, "text": "Line\nBreak", "value": None},
        {"id": 3, "text": "✓ Unicode", "value": 123.45},
    ]
    lines = [json.dumps(row) for row in data]
    return "\n".join(lines).encode("utf-8")


@pytest.fixture
def wide_jsonl_bytes():
    """JSONL with many columns (projection pushdown test)."""
    data = [{f"col{i}": i * j for i in range(100)} for j in range(1, 11)]
    lines = [json.dumps(row) for row in data]
    return "\n".join(lines).encode("utf-8")
