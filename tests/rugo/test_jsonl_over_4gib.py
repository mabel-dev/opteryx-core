"""read_jsonl over a single buffer larger than 4 GiB.

Every position the JSONL parser stores (FieldSpan, the structural index, LineSpan) is a
uint32_t offset from the start of the buffer it walks, so an input over 4 GiB is read in
chunks of at most 4 GiB, each cut after a newline and walked from its own start
(interpret_jsonl_threaded, ColumnMap::chunk_row). Before that, everything past byte 2**32
wrapped: read_jsonl returned too few rows and counted most of the tail as malformed, with no
error.

One file of ~4.3 GiB (~4 KB rows, so ~1.07M rows) is written once and read several ways;
every column shape the builders have crosses the chunk boundary:
  * `i`  — a copied scalar (INT64)
  * `s`  — a copied string (VARCHAR)
  * `o`  — an uncopied object (VARIANT), whose spans index the source per chunk
  * `o->>'k'` — a nested extraction
plus the raw prefilter (a selective string predicate) and the malformed-line report, whose
byte offset is past 2**32.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

PAD = "x" * 4000
FOUR_GIB = 1 << 32
TARGET = FOUR_GIB + (256 << 20)  # past 4 GiB by 256 MiB


def _line(n):
    return f'{{"i":{n},"s":"s{n % 1000}","o":{{"k":"k{n % 7}","n":{n}}},"pad":"{PAD}"}}\n'


@pytest.fixture(scope="module")
def big_file(tmp_path_factory):
    """Rows 0..N-1, and ONE malformed line placed just past the 4 GiB mark."""
    path = tmp_path_factory.mktemp("over4gib") / "big.jsonl"
    written = 0
    n = 0
    lines = 0
    bad_line = bad_offset = None
    with open(path, "w", buffering=1 << 24) as f:
        while written < TARGET:
            if bad_line is None and written > FOUR_GIB + (1 << 20):
                bad = '{"i": oops\n'
                bad_line, bad_offset = lines + 1, written
                f.write(bad)
                written += len(bad)
                lines += 1
                continue
            line = _line(n)
            f.write(line)
            written += len(line)
            lines += 1
            n += 1
    assert written > FOUR_GIB
    yield str(path), n, bad_line, bad_offset
    os.remove(path)


def _col(res, name):
    return res["columns"][res["column_names"].index(name)].to_pylist()


@pytest.mark.slow
def test_every_row_read_across_the_4gib_boundary(big_file):
    path, n, _, _ = big_file
    res = read_jsonl(path, columns=["i", "s", "o", "o->>'k'"], fail_on_error=False)
    assert res["num_rows"] == n
    assert res["malformed_count"] == 1
    assert _col(res, "i") == list(range(n))
    assert _col(res, "s") == [f"s{k % 1000}" for k in range(n)]
    assert _col(res, "o->>'k'") == [f"k{k % 7}" for k in range(n)]
    o = _col(res, "o")
    for k in (0, n // 2, n - 2, n - 1):
        assert o[k] == f'{{"k":"k{k % 7}","n":{k}}}'


@pytest.mark.slow
def test_unprojected_read_across_the_4gib_boundary(big_file):
    path, n, _, _ = big_file
    res = read_jsonl(path, fail_on_error=False)
    assert res["num_rows"] == n
    assert res["malformed_count"] == 1
    assert _col(res, "i") == list(range(n))


@pytest.mark.slow
def test_prefiltered_read_across_the_4gib_boundary(big_file):
    path, n, _, _ = big_file
    res = read_jsonl(path, columns=["i"], predicates=[("s", "==", "s7")], fail_on_error=False)
    assert _col(res, "i") == [k for k in range(n) if k % 1000 == 7]


@pytest.mark.slow
def test_malformed_line_past_4gib_is_reported_at_its_true_offset(big_file):
    path, _, bad_line, bad_offset = big_file
    assert bad_offset > FOUR_GIB
    with pytest.raises(ValueError) as e:
        read_jsonl(path, columns=["i"], fail_on_error=True)
    msg = str(e.value)
    assert f"line {bad_line} " in msg
    assert str(bad_offset) in msg
