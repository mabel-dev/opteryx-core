"""
A 0-row row group is never handed to the decoder.

pyarrow (and BigQuery exports) write an empty table as ONE row group with
num_rows == 0 whose column chunks hold a lone empty dictionary page and no data
page. Decoding such a chunk walked off its end: the thrift reader threw "EOF"
looking for a data page header, surfacing as
"Decode failed for column '<first column>': EOF" (a DatasetReadError in
production). Both submission paths must skip the row group instead.
"""

import os
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pyarrow as pa
import pyarrow.parquet as pq


def _empty_parquet(directory):
    path = os.path.join(directory, "empty.parquet")
    table = pa.table({"a": pa.array([], pa.string()), "b": pa.array([], pa.float64())})
    pq.write_table(table, path)
    metadata = pq.ParquetFile(path).metadata
    assert metadata.num_row_groups == 1 and metadata.row_group(0).num_rows == 0
    return path


def test_read_parquet_empty_row_group_returns_no_rows():
    import opteryx

    with tempfile.TemporaryDirectory() as directory:
        path = _empty_parquet(directory)
        session = opteryx.session()
        for sql in (
            f"SELECT * FROM READ_PARQUET('{path}')",
            f"SELECT a FROM READ_PARQUET('{path}')",
            f"SELECT * FROM READ_PARQUET('{path}') WHERE b > 1",
        ):
            rows = sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))
            assert rows == 0, sql


def test_ipc_source_skips_empty_row_group():
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    with tempfile.TemporaryDirectory() as directory:
        path = _empty_parquet(directory)
        # native-footer path
        assert sum(1 for _ in iter_row_groups_ipc(None, [path], ["a", "b"])) == 0
        # prefetched-footer path
        prefetched = {path: {"row_groups": [{"num_rows": 0, "columns": []}]}}
        yielded = iter_row_groups_ipc(None, [path], ["a", "b"], prefetched_footers=prefetched)
        assert sum(1 for _ in yielded) == 0


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
