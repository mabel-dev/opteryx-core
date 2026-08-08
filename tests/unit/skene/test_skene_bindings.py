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
    buf = skene.write_morsel(
        _sample_morsel(), read_acceleration=True, codec="zstd", zstd_level=1
    )
    m = skene.read_morsel(buf, 0)
    m.materialize()
    assert m.num_rows == 5
    assert m.column("a").to_pylist() == [5, 3, 3, None, 9]
    assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]


def test_probe_and_metadata():
    buf = skene.write_morsel(_sample_morsel(), read_acceleration=True, writer_tag="t")
    assert skene.probe_version(buf) == 1

    # The FILE footer: schema, the row group directory, and per-row-group
    # statistics. No row group footer is parsed, so nothing per-row-group about a
    # COLUMN (data_length, encoding shape, extents) appears here — that is the
    # split, not an omission.
    meta = skene.read_metadata(buf)
    assert meta["row_count"] == 5
    assert meta["writer_tag"] == "t"
    assert len(meta["row_groups"]) == 1
    assert meta["row_groups"][0]["row_count"] == 5
    assert meta["row_groups"][0]["first_row"] == 0
    assert [c["name"] for c in meta["columns"]] == ["a", "colour"]

    # Statistics are per row group, and their slots are depth-first over the
    # schema — so slot 0 is "a".
    stats = meta["row_groups"][0]["column_statistics"][0]
    assert stats["min_ordinal"] == 3
    assert stats["max_ordinal"] == 9
    assert stats["null_count"] == 1
    assert stats["sum"] == 20

    # The per-row-group detail, which costs a row group footer parse.
    detail = skene.read_row_group_metadata(buf, 0)
    assert detail["row_count"] == 5
    by_name = {c["name"]: c for c in detail["columns"]}
    # value-ordered: data_length is the exact distinct count (5, 3, 9)
    assert by_name["a"]["data_length"] == 3
    assert by_name["a"]["statistics"]["min_ordinal"] == 3


def test_multi_row_group_file():
    """The packed shape: many row groups in one file, each read on its own."""
    writer = skene.SkeneWriter(read_acceleration=True)
    for _ in range(3):
        writer.add_row_group(_sample_morsel())
    assert writer.row_group_count == 3
    buf = writer.finish()

    meta = skene.read_metadata(buf)
    assert meta["row_count"] == 15
    assert [g["row_count"] for g in meta["row_groups"]] == [5, 5, 5]
    assert [g["first_row"] for g in meta["row_groups"]] == [0, 5, 10]

    for index in range(3):
        m = skene.read_morsel(buf, index)
        m.materialize()
        assert m.num_rows == 5
        assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]

    # Past the last row group is an error, not an empty result.
    with pytest.raises(skene.SkeneError):
        skene.read_morsel(buf, 3)


def test_row_groups_must_share_a_schema():
    """A row group whose columns differ is refused, because the file footer's
    schema directory would then describe only part of the file."""
    writer = skene.SkeneWriter()
    writer.add_row_group(_sample_morsel())
    other = Morsel.from_vectors(["a"], [vector_from_sequence([1, 2], DrakenType.INT64)])
    with pytest.raises(skene.SkeneError):
        writer.add_row_group(other)


def test_writer_refuses_an_empty_file(tmp_path):
    writer = skene.SkeneWriter()
    with pytest.raises(skene.SkeneError):
        writer.finish()


def test_write_to_avoids_the_copy(tmp_path):
    writer = skene.SkeneWriter(read_acceleration=True)
    writer.add_row_group(_sample_morsel())
    writer.add_row_group(_sample_morsel())
    target = tmp_path / "packed.skene"
    written = writer.write_to(str(target))

    assert written == target.stat().st_size
    data = target.read_bytes()
    assert skene.read_metadata(data)["row_count"] == 10


def test_projection_is_strict():
    buf = skene.write_morsel(_sample_morsel())
    m = skene.read_morsel(buf, 0, columns=["colour"])
    m.materialize()
    assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]
    with pytest.raises(skene.SkeneError):
        skene.read_morsel(buf, 0, columns=["nope"])


def test_corruption_fails_loud():
    buf = skene.write_morsel(_sample_morsel())
    with pytest.raises(skene.SkeneError):
        skene.read_morsel(buf[:200], 0)
    with pytest.raises(skene.SkeneError):
        skene.probe_version(b"PAR1....")


def test_spill_posture_roundtrip():
    # for_spill: no acceleration, no compression — still lossless
    buf = skene.write_morsel(_sample_morsel())
    m = skene.read_morsel(buf, 0)
    m.materialize()
    assert m.column("a").to_pylist() == [5, 3, 3, None, 9]


@pytest.mark.parametrize(
    "options",
    [
        {"codec": "none"},
        {"codec": "lz4"},
        {"codec": "zstd", "zstd_level": 1},
        {"codec": "zstd", "zstd_level": 9},
    ],
)
def test_every_codec_round_trips_through_the_binding(options):
    buf = skene.write_morsel(_sample_morsel(), read_acceleration=True, **options)
    m = skene.read_morsel(buf, 0)
    m.materialize()
    assert m.column("a").to_pylist() == [5, 3, 3, None, 9]
    assert m.column("colour").to_pylist() == ["red", "blue", "red", "green", None]


def test_codec_and_level_must_agree():
    # A level set without the codec to match it used to be silently ignored, so
    # a caller asking for compression got none and nothing said so. Both halves
    # of the contradiction now fail loud, which is why the two tests that passed
    # a bare zstd_level had to be updated rather than kept working.
    with pytest.raises(skene.SkeneError):
        skene.write_morsel(_sample_morsel(), zstd_level=1)
    with pytest.raises(skene.SkeneError):
        skene.write_morsel(_sample_morsel(), codec="lz4", zstd_level=1)
    with pytest.raises(skene.SkeneError):
        skene.write_morsel(_sample_morsel(), codec="zstd")
    with pytest.raises(ValueError):
        skene.write_morsel(_sample_morsel(), codec="brotli")


if __name__ == "__main__":
    test_roundtrip_values()
    test_probe_and_metadata()
    test_projection_is_strict()
    test_corruption_fails_loud()
    test_spill_posture_roundtrip()
    test_codec_and_level_must_agree()
    print("✅ okay")
