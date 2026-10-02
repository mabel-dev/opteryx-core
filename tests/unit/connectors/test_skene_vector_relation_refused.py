"""A skene relation carrying a VECTOR column is refused.

VECTOR is not a SQL value (architect ruling 2026-10-01): vectors exist only inside
vector indexes. skene can still store one, so the place a skene relation's schema enters
a query refuses it — naming the column — rather than letting a vector reach a projection,
a filter or a client.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
import skene
from draken import draken_native
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from opteryx.exceptions import UnsupportedTypeError


def _write(dataset_dir, with_vector: bool):
    os.makedirs(dataset_dir, exist_ok=True)
    names = ["id"]
    vectors = [vector_from_sequence([1, 2], DrakenType.INT64)]
    if with_vector:
        names.append("emb")
        vectors.append(Vector(draken_native.vector_fp16_from_sequence([[1.0, 0.0], [0.0, 1.0]], 2)))
    buf = skene.write_morsel(Morsel.from_vectors(names, vectors))
    with open(os.path.join(dataset_dir, "part.skene"), "wb") as handle:
        handle.write(bytes(buf))


def test_relation_with_a_vector_column_is_refused(tmp_path):
    dataset = str(tmp_path / "with_vector")
    _write(dataset, with_vector=True)
    with pytest.raises(UnsupportedTypeError) as err:
        list(opteryx.session().execute_to_morsels(f"SELECT id FROM '{dataset}'"))
    assert "emb" in str(err.value), str(err.value)
    assert "vector indexes" in str(err.value), str(err.value)


def test_relation_without_a_vector_column_still_reads(tmp_path):
    dataset = str(tmp_path / "plain")
    _write(dataset, with_vector=False)
    morsels = list(opteryx.session().execute_to_morsels(f"SELECT id FROM '{dataset}'"))
    assert sum(m.num_rows for m in morsels) == 2


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
