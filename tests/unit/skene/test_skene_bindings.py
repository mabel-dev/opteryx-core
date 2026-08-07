"""skene Python boundary: write → probe/metadata/read round-trip.

The format's own correctness suite is C++ (skene/tests, `make -C skene test`);
this covers only the binding layer — marshalling, projection, and the
Status→SkeneError translation.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import pytest

import skene
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel


def _sample_morsel():
    ints = vector_from_sequence([5, 3, 3, None, 9], DrakenType.INT64)
    strs = vector_from_sequence(["red", "blue", "red", "green", None], DrakenType.VARCHAR)
    return Morsel.from_vectors(["a", "colour"], [ints, strs])


def test_roundtrip_values():
    buf = skene.write_morsel(_sample_morsel(), read_acceleration=True, zstd_level=1)
    m = skene.read_morsel(buf)
    m.materialize()
    assert m.num_rows == 5
    assert m.column("a").to_pylist() == [5, 3, 3, None, 9]
    assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]


def test_probe_and_metadata():
    buf = skene.write_morsel(_sample_morsel(), read_acceleration=True, writer_tag="t")
    assert skene.probe_version(buf) == 1

    meta = skene.read_metadata(buf)
    assert meta["row_count"] == 5
    assert meta["writer_tag"] == "t"
    by_name = {c["name"]: c for c in meta["columns"]}
    a = by_name["a"]
    # value-ordered: data_length is the exact distinct count (5, 3, 9)
    assert a["data_length"] == 3
    stats = a["statistics"]
    assert stats["min_ordinal"] == 3
    assert stats["max_ordinal"] == 9
    assert stats["null_count"] == 1
    assert stats["sum"] == 20


def test_projection_is_strict():
    buf = skene.write_morsel(_sample_morsel())
    m = skene.read_morsel(buf, columns=["colour"])
    m.materialize()
    assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]
    with pytest.raises(skene.SkeneError):
        skene.read_morsel(buf, columns=["nope"])


def test_corruption_fails_loud():
    buf = skene.write_morsel(_sample_morsel())
    with pytest.raises(skene.SkeneError):
        skene.read_morsel(buf[:200])
    with pytest.raises(skene.SkeneError):
        skene.probe_version(b"PAR1....")


def test_spill_posture_roundtrip():
    # for_spill: no acceleration, no compression — still lossless
    buf = skene.write_morsel(_sample_morsel())
    m = skene.read_morsel(buf)
    m.materialize()
    assert m.column("a").to_pylist() == [5, 3, 3, None, 9]


if __name__ == "__main__":
    test_roundtrip_values()
    test_probe_and_metadata()
    test_projection_is_strict()
    test_corruption_fails_loud()
    test_spill_posture_roundtrip()
    print("✅ okay")
