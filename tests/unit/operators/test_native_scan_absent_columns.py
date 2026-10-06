"""Schema evolution on the native parquet scan: a projected column a file does not
hold (added after the file was written) reads as NULL from that file.

The footer gate records which projected columns each file lacks; the file is read
without them, and NativeParquetScanSource emits each as an all-NULL constant of the
column's declared type — nothing read, nothing decoded. A file holding NONE of the
read columns is not read at all: its row groups' rows come from the footer.
"""

import datetime
import decimal
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx

_DIFFERENT = "testdata/flat/different"   # tweets (196,893 rows) + planets (9 rows)

NEW_ROWS = 3
OLD_ROWS = 5000


def run(sql):
    session = opteryx.session()
    tables = [m.to_arrow() for m in session.execute_to_morsels(sql) if m.num_rows]
    sources = dict(session.telemetry.get("scan_sources", {}))
    rows = pa.concat_tables(tables).to_pylist() if tables else []
    return rows, sources


@pytest.fixture(scope="module")
def evolved(tmp_path_factory):
    """`a_new` holds every column (and names the folder's schema); `b_old` holds only
    `id`, in several row groups."""
    folder = tmp_path_factory.mktemp("evolved")
    pq.write_table(pa.table({
        "id": pa.array([10_001, 10_002, 10_003], pa.int64()),
        "s": pa.array(["x", "y", None]),
        "f": pa.array([1.5, 2.5, 3.5]),
        "b": pa.array([True, False, True]),
        "ts": pa.array([datetime.datetime(2026, 1, d) for d in (1, 2, 3)], pa.timestamp("us")),
        "d": pa.array([decimal.Decimal("1.25"), decimal.Decimal("2.50"), None],
                      pa.decimal128(10, 2)),
        "dt": pa.array([datetime.date(2026, 1, d) for d in (1, 2, 3)], pa.date32()),
    }), folder / "a_new.parquet")
    pq.write_table(pa.table({"id": pa.array(list(range(1, OLD_ROWS + 1)), pa.int64())}),
                   folder / "b_old.parquet", row_group_size=1000)
    return str(folder)


def test_a_partly_evolved_folder_reads_nulls_from_the_old_file():
    rows, sources = run(f"SELECT COUNT(*) AS n, COUNT(followers) AS f, COUNT(user_name) AS u "
                        f"FROM '{_DIFFERENT}'")
    assert rows == [{"n": 196_902, "f": 196_893, "u": 196_893}]
    assert set(sources.values()) == {"NativeParquetScanSource"}


def test_a_file_holding_no_read_column_counts_its_rows_from_the_footer():
    rows, _ = run(f"SELECT COUNT(*) AS n FROM '{_DIFFERENT}' WHERE followers IS NULL")
    assert rows == [{"n": 9}]


@pytest.mark.parametrize("column", ["s", "f", "b", "ts", "d", "dt"])
def test_every_filled_type_counts_only_the_new_rows(evolved, column):
    rows, sources = run(f"SELECT COUNT(*) AS n, COUNT({column}) AS c FROM '{evolved}'")
    expected = sum(1 for v in {
        "s": ["x", "y", None], "f": [1, 1, 1], "b": [1, 1, 1], "ts": [1, 1, 1],
        "d": [1, 1, None], "dt": [1, 1, 1]}[column] if v is not None)
    assert rows == [{"n": NEW_ROWS + OLD_ROWS, "c": expected}]
    assert set(sources.values()) == {"NativeParquetScanSource"}


def test_an_old_row_is_null_in_every_added_column(evolved):
    rows, _ = run(f"SELECT * FROM '{evolved}' WHERE id = 7")
    assert rows == [{"id": 7, "s": None, "f": None, "b": None, "ts": None, "d": None,
                     "dt": None}]


def test_a_new_row_keeps_its_values(evolved):
    rows, _ = run(f"SELECT id, s, f, b, ts, d, dt FROM '{evolved}' WHERE id = 10001")
    assert rows == [{"id": 10_001, "s": "x", "f": 1.5, "b": True,
                     "ts": datetime.datetime(2026, 1, 1), "d": decimal.Decimal("1.25"),
                     "dt": datetime.date(2026, 1, 1)}]


def test_a_predicate_on_an_added_column(evolved):
    rows, _ = run(f"SELECT COUNT(*) AS n FROM '{evolved}' WHERE s IS NULL")
    assert rows == [{"n": OLD_ROWS + 1}]
    rows, _ = run(f"SELECT id FROM '{evolved}' WHERE s = 'y'")
    assert rows == [{"id": 10_002}]


def test_a_predicate_on_a_held_column_projecting_an_added_one(evolved):
    """The scan prefilter's survivor path: the old file's survivors carry the fill."""
    rows, _ = run(f"SELECT id, s, d FROM '{evolved}' WHERE id IN (3, 4999, 10002) ORDER BY id")
    assert rows == [{"id": 3, "s": None, "d": None},
                    {"id": 4999, "s": None, "d": None},
                    {"id": 10_002, "s": "y", "d": decimal.Decimal("2.50")}]


def test_grouping_on_an_added_column(evolved):
    rows, _ = run(f"SELECT b, COUNT(*) AS n FROM '{evolved}' GROUP BY b ORDER BY b")
    assert rows == [{"b": None, "n": OLD_ROWS}, {"b": False, "n": 1}, {"b": True, "n": 2}]


def test_a_limit_over_only_added_columns(evolved):
    rows, _ = run(f"SELECT s FROM '{evolved}' WHERE id < 100 LIMIT 5")
    assert rows == [{"s": None}] * 5


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
