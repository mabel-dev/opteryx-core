"""read_jsonl refuses a string column holding more than 4 GiB of string bytes.

A string slot addresses its bytes with a uint32_t arena offset (draken string_slot.h).
Before the refusal, an arena past 4 GiB wrapped: the column's tail overwrote its head and
~60k rows came back holding another row's value, with no error.

The column here is an ARRAY of one ~4 KB string per row, so its bytes are not copied while
parsing and the refusal costs no more than the parse (~0.1 GB). Read as arrays it goes
through the array builder; with parse_arrays=False the same bytes go through the VARCHAR /
VARIANT builder that plain string columns use. (A plain string column is copied while
parsing, so refusing one peaks near 9 GB - too much for a 16 GB CI runner.)

Its own module, so the file in test_jsonl_over_4gib.py is gone before this one is written.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_jsonl

PAD = "x" * 3990
TARGET = (1 << 32) + (256 << 20)  # past 4 GiB by 256 MiB
REFUSAL = r"a column holds \d+ bytes of string values; a column holds at most 4294967295"


@pytest.fixture(scope="module")
def big_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("jsonlstr4gib") / "big.jsonl"
    written = 0
    n = 0
    with open(path, "w", buffering=1 << 24) as f:
        while written < TARGET:
            line = f'{{"i":{n},"v":["{n:010d}{PAD}"]}}\n'
            f.write(line)
            written += len(line)
            n += 1
    yield str(path)
    os.remove(path)


@pytest.mark.slow
def test_array_of_strings_over_4gib_is_refused(big_file):
    with pytest.raises(RuntimeError, match=REFUSAL):
        read_jsonl(big_file, columns=["v"])


@pytest.mark.slow
def test_string_text_over_4gib_is_refused(big_file):
    with pytest.raises(RuntimeError, match=REFUSAL):
        read_jsonl(big_file, columns=["v"], parse_arrays=False)


@pytest.mark.slow
def test_other_columns_still_read(big_file):
    res = read_jsonl(big_file, columns=["i"])
    assert res["columns"][0].to_pylist() == list(range(res["num_rows"]))
