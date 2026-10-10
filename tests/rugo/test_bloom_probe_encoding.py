"""The bloom probe is encoded in the COLUMN's PLAIN form, not the literal's.

`_bloom_plain_encode` used to pick an encoding from the Python value alone: every
int as 8 little-endian bytes, every float as a double. The writer hashes an INT32
column over 4 bytes, so `a = 5` on an int32 column hashed the wrong bytes, the
bloom answered "definitely absent", and the row group was PRUNED — the file path
returned no rows while the same bytes source (no bloom stage) returned the right
one. A float literal on an int column, an int literal on a float or DECIMAL
column, unsigned values above the signed range and `= 0.0` against a stored -0.0
failed the same way.

Two properties are tested: every predicate answers the same from a file (bloom
stage on) as from bytes (no bloom stage), and the encodings that ARE exact still
prune — each value sits in exactly one row group and every row group's min/max
spans the whole domain, so only the bloom filter can skip the others.
"""

import datetime
import decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rugo import parquet

N = 1_000
GROUPS = 4                  # row group k holds the values i with i % 4 == k
ORDER = sorted(range(N), key=lambda i: (i % GROUPS, i))
DAY0 = datetime.date(2024, 1, 1)
TS0 = datetime.datetime(2024, 1, 1)


def _table():
    return pa.table({
        "i32": pa.array(ORDER, pa.int32()),
        "i64": pa.array(ORDER, pa.int64()),
        "i8": pa.array([i % 100 for i in ORDER], pa.int8()),
        "u32": pa.array([i + 3_000_000_000 for i in ORDER], pa.uint32()),
        "f32": pa.array([float(i) for i in ORDER], pa.float32()),
        "f64": pa.array([float(i) for i in ORDER], pa.float64()),
        "z64": pa.array([-0.0 if i == 0 else float(i) for i in ORDER], pa.float64()),
        "dec": pa.array([decimal.Decimal(i) for i in ORDER], pa.decimal128(9, 2)),
        "dt": pa.array([DAY0 + datetime.timedelta(days=i) for i in ORDER], pa.date32()),
        "ts": pa.array([TS0 + datetime.timedelta(minutes=i) for i in ORDER], pa.timestamp("ms")),
        "s": pa.array([str(i) for i in ORDER]),
    })


@pytest.fixture(scope="module")
def blooms(tmp_path_factory):
    """(path, bytes) of a file with a bloom filter on every column."""
    table = _table()
    path = tmp_path_factory.mktemp("bloom") / "bloom.parquet"
    # ndv well above the per-row-group count keeps the false-positive rate
    # negligible, so a surviving row group means "might match", not noise.
    pq.write_table(
        table, path, row_group_size=N // GROUPS, use_dictionary=False,
        bloom_filter_options={c: {"ndv": 100_000} for c in table.column_names},
    )
    return str(path), path.read_bytes()


def _morsel_rows(source, predicate):
    with parquet.read_parquet(source, predicates=[predicate]) as r:
        return [m.num_rows for m in r]


ANSWERS = [
    (("i32", "=", 5), 1),
    (("i32", "in", [5, 7]), 2),
    (("i32", "=", 5.0), 1),
    (("i64", "=", 5.0), 1),
    (("i8", "=", 5), 10),
    (("u32", "=", 3_000_000_005), 1),
    (("f32", "=", 5.0), 1),
    (("f32", "=", 5), 1),
    (("f64", "=", 5), 1),
    (("z64", "=", 0.0), 1),
    (("dec", "=", 5), 1),
    (("dec", "=", decimal.Decimal(5)), 1),
]


@pytest.mark.parametrize("predicate, expected", ANSWERS, ids=lambda x: repr(x))
def test_file_answers_like_bytes(blooms, predicate, expected):
    path, data = blooms
    assert sum(_morsel_rows(path, predicate)) == expected
    assert sum(_morsel_rows(data, predicate)) == expected


PRUNES = [
    ("i32", 501),
    ("i64", 501),
    ("u32", 3_000_000_501),
    ("s", "501"),
    ("dt", DAY0 + datetime.timedelta(days=501)),
    ("ts", TS0 + datetime.timedelta(minutes=501)),
]


@pytest.mark.parametrize("column, value", PRUNES, ids=lambda x: repr(x))
def test_exact_encodings_still_prune(blooms, column, value):
    path, data = blooms
    # Every row group's min/max spans the domain: without the bloom stage
    # (bytes source) all four are decoded.
    assert len(_morsel_rows(data, (column, "=", value))) == GROUPS
    # With it, only the one row group holding the value is read.
    assert _morsel_rows(path, (column, "=", value)) == [1]
    assert _morsel_rows(path, (column, "in", [value])) == [1]
