"""read_csv over a single buffer larger than 4 GiB.

Every position the CSV parser stores (the structural scan's marker positions, the safe
splits, a field's start) is a uint32_t offset from the start of the buffer it walks, so a
body over 4 GiB is read in chunks of at most 4 GiB (rugo::kMaxChunkBytes), each cut one past
a newline OUTSIDE any quoted field and walked from its own start (build_columns_streaming).
Before that, everything past byte 2**32 wrapped, with no error: a threaded read returned
~2.07M rows for a 1.13M-row file, a single-threaded one ~1.07M rows with garbage at the end.

One file of ~4.3 GiB (~4 KB rows, so ~1.07M rows) is written once and read several ways.
Every row's `pad` is a quoted field holding a newline, a delimiter and an escaped quote, and
the row straddling the end of the first 4 GiB window has its embedded newline just BEFORE
that end — the last raw '\\n' in the window is inside quotes, so a cut that is not
quote-aware splits a field in two.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo.rugo_native import read_csv

HEADER = "i,s,f,t,pad\n"
FOUR_GIB = 1 << 32
WINDOW_END = len(HEADER) + FOUR_GIB - 1  # absolute end of the first chunk's window
TARGET = FOUR_GIB + (256 << 20)  # past 4 GiB by 256 MiB


def _row(n, pad):
    return f'{n},s{n % 1000},{n * 0.5},long-string-value-{n},"{pad}"\n'


def _pad(left, right):
    return "x" * left + '\n,""' + "y" * right


PAD = _pad(1990, 1990)


@pytest.fixture(scope="module")
def big_file(tmp_path_factory):
    """Rows 0..N-1; returns the row straddling the first window's end, and its pad."""
    path = tmp_path_factory.mktemp("csvover4gib") / "big.csv"
    written = 0
    n = 0
    straddle = None
    with open(path, "w", buffering=1 << 24) as f:
        f.write(HEADER)
        written = len(HEADER)
        while written < TARGET:
            line = _row(n, PAD)
            if straddle is None and written + len(line) > WINDOW_END:
                # Put this row's embedded newline 16 bytes before the window's end.
                prefix = len(_row(n, ""))  - 2  # bytes before the pad's first byte
                left = WINDOW_END - 16 - written - prefix
                pad = _pad(left, 1990)
                line = _row(n, pad)
                assert written + prefix + left < WINDOW_END < written + len(line)
                straddle = (n, pad.replace('""', '"'))
            f.write(line)
            written += len(line)
            n += 1
    assert written > FOUR_GIB
    yield str(path), n, straddle
    os.remove(path)


def _cols(res):
    return {k: v.to_pylist() for k, v in zip(res["column_names"], res["columns"])}


@pytest.mark.slow
@pytest.mark.parametrize("use_threads", [True, False])
def test_every_row_read_across_the_4gib_boundary(big_file, use_threads):
    path, n, _ = big_file
    res = read_csv(path, columns=["i", "s", "f", "t"], use_threads=use_threads)
    assert res["num_rows"] == n
    cols = _cols(res)
    assert cols["i"] == list(range(n))
    assert cols["s"] == [f"s{k % 1000}" for k in range(n)]
    assert cols["f"] == [k * 0.5 for k in range(n)]
    assert cols["t"] == [f"long-string-value-{k}" for k in range(n)]


@pytest.mark.slow
def test_quoted_field_straddling_the_window_end(big_file):
    path, _, (row, pad) = big_file
    res = read_csv(path, columns=["i", "pad"], predicates=[("i", ">=", row - 1), ("i", "<=", row + 1)])
    cols = _cols(res)
    assert cols["i"] == [row - 1, row, row + 1]
    assert cols["pad"] == [PAD.replace('""', '"'), pad, PAD.replace('""', '"')]


@pytest.mark.slow
def test_predicate_across_the_4gib_boundary(big_file):
    path, n, _ = big_file
    res = read_csv(path, columns=["i"], predicates=[("s", "==", "s7")])
    assert _cols(res)["i"] == [k for k in range(n) if k % 1000 == 7]


@pytest.mark.slow
def test_string_column_over_4gib_fails_loud(big_file):
    # `pad` alone is ~4.3 GiB of out-of-line string bytes; a string slot addresses its
    # bytes with a uint32_t offset, so the column cannot be built.
    path, _, _ = big_file
    with pytest.raises(RuntimeError, match=r"column 'pad' holds \d+ bytes of string values"):
        read_csv(path, columns=["pad"])


@pytest.mark.slow
def test_row_longer_than_4gib_fails_loud(tmp_path):
    path = tmp_path / "one_row.csv"
    with open(path, "w", buffering=1 << 24) as f:
        f.write("a\n")
        block = "x" * (64 << 20)
        written = 0
        while written <= FOUR_GIB:
            f.write(block)
            written += len(block)
        f.write("\n")
    try:
        with pytest.raises(RuntimeError, match=r"the row at byte 2 is longer than 4294967295 bytes"):
            read_csv(str(path), columns=["a"])
    finally:
        os.remove(path)
