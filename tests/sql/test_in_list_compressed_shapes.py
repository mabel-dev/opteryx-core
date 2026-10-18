# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`x IN (...)` / `NOT IN` on draken_in_list across operand SHAPES.

draken_in_list (the VM's C-ABI IN kernel) splits on shape (§11, ratified
2026-10-18): a compressed operand — dict or constant — tests membership once per
KEY and scatters the key bits to rows; a dense operand tests per row. Both must
give the same answer, so the same queries run over a dictionary-encoded parquet
file (dict vectors, plus one constant row group) and a plain-encoded copy (dense
vectors), and every result is checked against a Python oracle row by row (`rid`)
— never against the engine's own `=`/`OR` (those are different kernels).

Each fixture asserts the shapes it promises actually reach the engine, so a reader
change that stops producing dict/constant vectors fails here instead of silently
reducing this to a dense-only test.
"""

import datetime
import decimal
import math
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskConnector

N = 4000
MODES = ["MAIL", "SHIP", "AIR", "REG AIR", "TRUCK", "FOB", "RAIL"]
BASE_DATE = datetime.date(2024, 1, 1)


def _columns():
    s = [None if r % 37 == 0 else MODES[r % 7] for r in range(N)]
    s[3000:4000] = ["SHIP"] * 1000  # last row group holds one value → constant vector
    return {
        "rid": [r for r in range(N)],
        "s": s,
        "i": [None if r % 41 == 0 else (r % 9) - 2 for r in range(N)],
        "j": [None if r % 43 == 0 else (r % 11) * 1_000_003 for r in range(N)],
        "d": [None if r % 47 == 0 else BASE_DATE + datetime.timedelta(days=r % 8) for r in range(N)],
        "u": [r % 5 for r in range(N)],
        "f": [None if r % 53 == 0 else (math.nan if r % 13 == 0 else (r % 6) / 2) for r in range(N)],
        "m": [None if r % 59 == 0 else decimal.Decimal(r % 7) / 4 for r in range(N)],
    }


SCHEMA = pa.schema(
    [
        ("rid", pa.int64()),
        ("s", pa.string()),
        ("i", pa.int16()),
        ("j", pa.int64()),
        ("d", pa.date32()),
        ("u", pa.uint8()),
        ("f", pa.float64()),
        ("m", pa.decimal128(9, 2)),
    ]
)

SHAPED = ["s", "i", "j", "d", "u", "f", "m"]


@pytest.fixture(scope="module")
def workspaces(tmp_path_factory):
    root = tmp_path_factory.mktemp("in_list_shapes")
    cols = _columns()
    table = pa.table({name: pa.array(cols[name], SCHEMA.field(name).type) for name in SCHEMA.names})
    for ws, use_dictionary in (("inl_dict", True), ("inl_plain", False)):
        path = root / ws / "t"
        path.mkdir(parents=True)
        pq.write_table(table, str(path / "data.parquet"), use_dictionary=use_dictionary, row_group_size=1000)
    cwd = os.getcwd()
    os.chdir(root)
    opteryx.register_workspace("inl_dict", DiskConnector)
    opteryx.register_workspace("inl_plain", DiskConnector)
    yield cols
    os.chdir(cwd)


def _shapes(ws):
    seen = {name: set() for name in SHAPED}
    sql = f"SELECT {', '.join(SHAPED)} FROM {ws}.t"
    for morsel in opteryx.session().execute_to_morsels(sql):
        for name in SHAPED:
            vec = morsel.column(name.encode())._nb
            seen[name].add("constant" if vec.is_constant else "dict" if vec.is_dict else "dense")
    return seen


def test_dict_fixture_reaches_engine_compressed(workspaces):
    seen = _shapes("inl_dict")
    for name in SHAPED:
        assert "dict" in seen[name], f"{name} never arrived dict-shaped: {seen[name]}"
    assert "constant" in seen["s"], f"s never arrived constant-shaped: {seen['s']}"


def test_plain_fixture_reaches_engine_dense(workspaces):
    seen = _shapes("inl_plain")
    for name in SHAPED:
        assert seen[name] == {"dense"}, f"{name} arrived {seen[name]} from the plain file"


def _same(a, b):
    # The engine's IN equality: total-order for floats (NaN = NaN).
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b


def _oracle(values, needles, negate):
    out = set()
    for rid, value in enumerate(values):
        if value is None:
            continue  # NULL IN (...) and NULL NOT IN (...) are both NULL
        hit = any(_same(value, needle) for needle in needles)
        if hit != negate:
            out.add(rid)
    return out


def _rids(ws, where):
    out = []
    for morsel in opteryx.session().execute_to_morsels(f"SELECT rid FROM {ws}.t WHERE {where}"):
        out.extend(morsel.column(b"rid").to_pylist())
    assert len(out) == len(set(out))
    return set(out)


NAN = "CAST('nan' AS FLOAT64)"

CASES = [
    # (column, SQL list, Python needles)
    ("s", "('REG AIR', 'MAIL')", ["REG AIR", "MAIL"]),
    ("s", "('SHIP')", ["SHIP"]),
    ("s", "('NOPE', 'ZZZ')", ["NOPE", "ZZZ"]),
    ("i", "(-1, 3, 6)", [-1, 3, 6]),
    ("i", "(100)", [100]),
    ("j", "(5000015, 7000021, 1)", [5_000_015, 7_000_021, 1]),
    ("d", "(CAST('2024-01-03' AS DATE), CAST('2024-01-06' AS DATE))", [BASE_DATE + datetime.timedelta(days=2), BASE_DATE + datetime.timedelta(days=5)]),
    ("u", "(0, 4)", [0, 4]),
    ("f", "(0.5, 2.0)", [0.5, 2.0]),
    ("f", f"({NAN}, 1.0)", [math.nan, 1.0]),
    ("m", "(1.25, 0.5)", [decimal.Decimal("1.25"), decimal.Decimal("0.5")]),
]


@pytest.mark.parametrize("ws", ["inl_dict", "inl_plain"])
@pytest.mark.parametrize("negate", [False, True], ids=["IN", "NOT_IN"])
@pytest.mark.parametrize("column, sql_list, needles", CASES, ids=[f"{c}{l}" for c, l, _ in CASES])
def test_in_list_matches_oracle(workspaces, ws, negate, column, sql_list, needles):
    op = "NOT IN" if negate else "IN"
    got = _rids(ws, f"{column} {op} {sql_list}")
    assert got == _oracle(workspaces[column], needles, negate)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
