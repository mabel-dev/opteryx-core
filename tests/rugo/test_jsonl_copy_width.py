"""read_jsonl copies a column's values while parsing only when its head-sample values are
short (head_copy_columns, kCopyMaxWidth = 32 bytes); longer ones are read from the source.

The decision is made on the head sample alone, so the rest of the column may fall on the
other side of the line. Either way every value must come back as written.
"""

import json
import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

ROWS = 20_000  # several MB, so the read is split across threads


def _values(head, tail):
    return [head(n) if n < 100 else tail(n) for n in range(ROWS)]


def _short(n):
    return f"s{n}"


def _long(n):
    return f"{n:08d}" + "x" * 900


def _escaped_long(n):
    return f"{n}\n\t\"quoted\" \\ é" * 10


CASES = {
    "short head, long tail (copied)": _values(_short, _long),
    "long head, short tail (not copied)": _values(_long, _short),
    "escaped long values (not copied)": _values(_escaped_long, _escaped_long),
    "exactly 32 bytes (copied)": _values(lambda n: f"{n:032d}", lambda n: f"{n:032d}"),
    "33 bytes (not copied)": _values(lambda n: f"{n:033d}", lambda n: f"{n:033d}"),
}


@pytest.mark.parametrize("name", CASES)
def test_every_value_reads_back(name):
    values = CASES[name]
    data = "".join(json.dumps({"i": n, "v": v}) + "\n" for n, v in enumerate(values)).encode()
    res = read_jsonl(data, infer_sample_size=100)
    got = res["columns"][res["column_names"].index("v")].to_pylist()
    assert got == [json.loads(line)["v"] for line in data.decode().splitlines()]
    assert res["columns"][res["column_names"].index("i")].to_pylist() == list(range(ROWS))
