"""Predicates must not narrow the projection.

With predicates and no projection (columns=None = every column), the minimal-extent map
was built for the predicate columns ONLY, so the morsel came back holding just the
predicate columns and every other column was silently dropped. columns=None must return
every column, filtered to the matching rows -- the parquet and CSV readers' contract.

With an explicit projection the predicate column is read for filtering, and returned only
when the projection names it.

The column SET is discovered from the head of the input (the first infer_sample_size
records), never from the rows the predicates kept: a column absent from every matching row
comes back all-null rather than vanishing, so the relation's shape does not depend on the
filter.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from rugo import jsonl

DATA = b'{"a":1,"s":"x","o":{"k":[1,2]}}\n{"a":null,"s":"y","o":{"k":[]}}\n{"a":3,"s":null,"o":null}\n'


def _read(**kwargs):
    with jsonl.read_jsonl(DATA, **kwargs) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    morsel = morsels[0]
    return {
        (name.decode() if isinstance(name, bytes) else name): morsel.column(name).to_pylist()
        for name in morsel.column_names
    }


def test_no_projection_predicate_on_first_column_returns_every_column():
    result = _read(predicates=[("a", "==", 1)])
    assert list(result) == ["a", "s", "o"]
    assert result["a"] == [1]
    assert result["s"] == ["x"]
    assert len(result["o"]) == 1


def test_no_projection_predicate_on_later_column_returns_every_column():
    result = _read(predicates=[("s", "==", "y")])
    assert list(result) == ["a", "s", "o"]
    assert result["a"] == [None]
    assert result["s"] == ["y"]


def test_no_projection_multi_row_match_returns_every_column():
    result = _read(predicates=[("a", ">=", 1)])
    assert list(result) == ["a", "s", "o"]
    assert result["a"] == [1, 3]
    assert result["s"] == ["x", None]


def test_projection_including_predicate_column():
    result = _read(columns=["a", "s"], predicates=[("a", "==", 1)])
    assert result == {"a": [1], "s": ["x"]}


def test_projection_excluding_predicate_column_filters_but_does_not_return_it():
    result = _read(columns=["s"], predicates=[("a", "==", 3)])
    assert result == {"s": [None]}


def test_no_projection_no_predicate_unchanged():
    result = _read()
    assert list(result) == ["a", "s", "o"]
    assert result["a"] == [1, None, 3]


# `t` appears only on the row the predicate REJECTS.
SPARSE = b'{"a":1,"t":"only-here"}\n{"a":2}\n{"a":3}\n'


def _read_sparse(**kwargs):
    with jsonl.read_jsonl(SPARSE, **kwargs) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    morsel = morsels[0]
    return {
        (name.decode() if isinstance(name, bytes) else name): morsel.column(name).to_pylist()
        for name in morsel.column_names
    }


def test_column_absent_from_matching_rows_is_returned_all_null():
    result = _read_sparse(predicates=[("a", ">=", 2)])
    assert result == {"a": [2, 3], "t": [None, None]}


def test_column_absent_from_matching_rows_same_set_as_unfiltered():
    unfiltered = _read_sparse()
    filtered = _read_sparse(predicates=[("a", "==", 3)])
    assert list(filtered) == list(unfiltered) == ["a", "t"]


def test_projected_column_absent_from_matching_rows_is_returned_all_null():
    result = _read_sparse(columns=["t"], predicates=[("a", "==", 2)])
    assert result == {"t": [None]}


def test_prefiltered_string_predicate_keeps_head_columns():
    # A long, selective string-equality predicate arms the raw prefilter, which drops
    # records before parsing; discovery must still see the unfiltered head.
    needle = "needle-value-that-is-long-enough"
    rows = [b'{"k":"other","extra":1}'] + [b'{"k":"filler-%d"}' % i for i in range(2000)]
    rows.append(('{"k":"%s"}' % needle).encode())
    data = b"\n".join(rows) + b"\n"
    with jsonl.read_jsonl(data, predicates=[("k", "==", needle)]) as reader:
        (morsel,) = list(reader)
    names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
    assert names == ["k", "extra"]
    assert morsel.column(morsel.column_names[1]).to_pylist() == [None]


def test_discovery_window_skips_blank_lines_in_head():
    data = b"\n\n\n\n\n\n" + b'{"a":1}\n' + b'{"a":2,"b":9}\n'
    with jsonl.read_jsonl(data, predicates=[("a", "==", 1)]) as reader:
        (morsel,) = list(reader)
    names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
    assert names == ["a", "b"]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
