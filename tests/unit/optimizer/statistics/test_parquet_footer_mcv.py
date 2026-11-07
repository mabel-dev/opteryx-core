# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Point frequencies from rugo's `rugo.mcv` footer key (rugo/src/parquet/file_mcv.hpp).

A uniform 1/NDV prices `col = x` as if every value were equally common. On a
skewed column the values a query names are the common ones: JOB 25c's
`movie_info.info IN (6 genres)` was estimated at 31 rows and returned 186,590,
and every join above it collapsed to an estimate of 1. The writer states each
column's heaviest values with their counts; the estimator prices = / IN / <> /
NOT IN from them.

Files are written by rugo's own writer at small row groups, and the estimate is
the scan's est_rows from EXPLAIN - the same path as a real query.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.parquet import append_row_groups
from rugo.parquet import patch_columns
from rugo.parquet import write_parquet

import opteryx

ROWS_PER_GROUP = 1024
ROWS = ROWS_PER_GROUP * 32


def _file(values, dtype, name="c"):
    morsel = Morsel.from_vectors([name], [Vector(vector_from_sequence(values, dtype))])
    return write_parquet(morsel, max_rows_per_row_group=ROWS_PER_GROUP)


def _dataset(tmp_path, *files):
    directory = tmp_path / "ds"
    directory.mkdir()
    for i, data in enumerate(files):
        (directory / f"part-{i:03d}.parquet").write_bytes(data)
    return str(directory)


def _scan_estimate(dataset, predicate):
    """The scan's est_rows for `SELECT * FROM dataset WHERE predicate`."""
    sql = f"EXPLAIN SELECT * FROM '{dataset}' WHERE {predicate}"
    morsel = Morsel.combine(list(opteryx.session().execute_to_morsels(sql)))
    names = [c.decode() if isinstance(c, bytes) else c for c in morsel.column_names]

    def column(name):
        values = morsel.column(morsel.column_names[names.index(name)]).to_pylist()
        return [v.decode() if isinstance(v, bytes) else v for v in values]

    scans = [e for t, e in zip(column("tree"), column("est_rows")) if t and "Parquet Read" in t]
    assert len(scans) == 1, scans
    return scans[0]


def _skewed_strings():
    rng = random.Random(3)
    return [
        "hot" if k % 4 == 0 else ("warm" if k % 16 == 1 else f"v{rng.randrange(100_000)}")
        for k in range(ROWS)
    ]


def test_skewed_strings_price_their_heavy_values(tmp_path):
    values = _skewed_strings()
    data = _file(values, DrakenType.VARCHAR)
    assert b"rugo.mcv.c" in data
    dataset = _dataset(tmp_path, data)
    hot, warm = values.count("hot"), values.count("warm")
    # every row group holds both, so the per-group counts sum to the exact total
    assert _scan_estimate(dataset, "c = 'hot'") == hot
    assert _scan_estimate(dataset, "c = 'warm'") == warm
    assert _scan_estimate(dataset, "c IN ('hot', 'warm')") == hot + warm
    assert _scan_estimate(dataset, "c <> 'hot'") == ROWS - hot
    assert _scan_estimate(dataset, "c NOT IN ('hot', 'warm')") == ROWS - hot - warm
    # an unlisted value takes the rest rate: the tail's rows over its values
    assert _scan_estimate(dataset, "c = 'not-there'") <= 2


def test_skewed_int32_prices_its_heavy_value(tmp_path):
    rng = random.Random(5)
    values = [7 if k % 3 == 0 else rng.randrange(1_000, 2_000_000) for k in range(ROWS)]
    dataset = _dataset(tmp_path, _file(values, DrakenType.INT32))
    assert _scan_estimate(dataset, "c = 7") == values.count(7)


def test_skewed_int64_prices_its_heavy_value(tmp_path):
    rng = random.Random(9)
    values = [42 if k % 5 == 0 else rng.randrange(1_000, 2_000_000) for k in range(ROWS)]
    dataset = _dataset(tmp_path, _file(values, DrakenType.INT64))
    assert _scan_estimate(dataset, "c = 42") == values.count(42)


def test_uniform_column_states_nothing(tmp_path):
    # all distinct: nothing is above twice the uniform rate, so no key, and
    # the estimate is the NDV tier's
    values = list(range(ROWS))
    random.Random(7).shuffle(values)
    data = _file(values, DrakenType.INT64)
    assert b"rugo.mcv." not in data
    assert _scan_estimate(_dataset(tmp_path, data), "c = 12345") == 1


def test_files_sum_their_counts(tmp_path):
    # 'hot' heavy in both files; 'only_a' listed in file a only, so file b
    # contributes its rest rate (its tail rows over its tail values) for it
    rng = random.Random(21)
    a = ["hot" if k % 4 == 0 else ("only_a" if k % 8 == 1 else f"a{rng.randrange(50_000)}") for k in range(ROWS)]
    b = ["hot" if k % 8 == 0 else f"b{rng.randrange(50_000)}" for k in range(ROWS)]
    dataset = _dataset(tmp_path, _file(a, DrakenType.VARCHAR), _file(b, DrakenType.VARCHAR))
    assert _scan_estimate(dataset, "c = 'hot'") == a.count("hot") + b.count("hot")
    only_a = _scan_estimate(dataset, "c = 'only_a'")
    assert a.count("only_a") <= only_a <= a.count("only_a") + 2, only_a


def test_patch_carries_and_append_drops_the_key(tmp_path):
    data = _file(_skewed_strings(), DrakenType.VARCHAR)
    renamed = patch_columns(data, rename={"c": "d"})
    assert b"rugo.mcv.d" in renamed and b"rugo.mcv.c" not in renamed
    # row groups copied across files: a file-level count is not additive
    appended = append_row_groups(data, data)
    assert b"rugo.mcv." not in appended


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
