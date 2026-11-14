# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A Morsel holding two columns of one name refuses every by-name read that would
otherwise silently pick one of them (first match) or collapse them (dict).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from draken.draken_native import DrakenType  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel, _select_for_test  # noqa: E402

DUP = "column name 'id' appears more than once"


def _vecs(n):
    return [vector_from_sequence([i, i + 1], dtype=DrakenType.INT64) for i in range(n)]


@pytest.fixture(params=["pyobject", "cxx"])
def dup(request):
    build = Morsel.from_vectors if request.param == "pyobject" else Morsel.from_cxx_vectors
    return build([b"id", b"x", b"id"], _vecs(3))


def test_column():
    # PyObject only: a Cxx-backed morsel refuses column() before any name lookup.
    m = Morsel.from_vectors([b"id", b"x", b"id"], _vecs(3))
    with pytest.raises(ValueError, match=DUP):
        m.column("id")
    assert m.column("x").to_pylist() == [1, 2]


def test_column_fallback():
    m = Morsel.from_vectors([b"id", b"id"], _vecs(2))
    with pytest.raises(ValueError, match=DUP):
        m.column("missing", fallback="id")


def test_select(dup):
    with pytest.raises(ValueError, match=DUP):
        dup.select(["id"])
    assert dup.select(["x"]).column_names == [b"x"]


def test_native_select_seam(dup):
    with pytest.raises(ValueError, match="appears more than once"):
        _select_for_test(dup, [b"id"])


def test_hash_by_name(dup):
    with pytest.raises(ValueError, match=DUP):
        dup.hash(col_names=["id"])


def test_schema(dup):
    with pytest.raises(ValueError, match=DUP):
        dup.schema


def test_iter(dup):
    with pytest.raises(ValueError, match=DUP):
        next(iter(dup))


def test_to_arrow():
    # PyObject only: a Cxx-backed morsel refuses to_arrow() before any name check.
    with pytest.raises(ValueError, match=DUP):
        Morsel.from_vectors([b"id", b"x", b"id"], _vecs(3)).to_arrow()


def test_cxx_column_binding():
    # The native CxxMorsel.column lookup operators use, below Morsel.column().
    cxx = Morsel.from_vectors([b"id", b"x", b"id"], _vecs(3))._get_cxx()
    with pytest.raises(ValueError, match=DUP):
        cxx.column(b"id")
    assert cxx.column(b"x").to_pylist() == [1, 2]


def test_unique_names_unaffected():
    m = Morsel.from_vectors([b"a", b"b"], _vecs(2))
    assert m.column("b").to_pylist() == [1, 2]
    assert list(m.schema) == ["a", "b"]
    assert len(list(m)) == 2
