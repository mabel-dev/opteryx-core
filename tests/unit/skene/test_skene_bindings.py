"""skene Python boundary: write → probe/metadata/read round-trip.

The format's own correctness suite is C++ (skene/tests, `make -C skene test`);
this covers only the binding layer — marshalling, projection, and the
Status→SkeneError translation.
"""

import datetime
import os
import sys
from decimal import Decimal

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import pytest

import skene
from draken import draken_native
from draken.draken_native import DrakenType
from draken.draken_native import LogicalKind
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector


def meta_logical_kind(meta, index=0):
    """The logical kind in the FILE schema directory, or None when absent."""
    logical = meta["columns"][index]["logical"]
    return None if logical is None else logical["kind"]


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
    assert skene.probe_version(buf) == 2  # the current writer emits v2

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


# ---------------------------------------------------------------------------
# LogicalType descriptor round-trip
#
# Carrying the descriptor is the reason this format exists (format.h's opening
# comment), but nothing in this suite pinned it. IPV4 is the case that needs a
# test rather than an argument: every other kind refines a physical tag that is
# already parameterized, so a dropped descriptor fails loud downstream — a
# TIMESTAMP64 with a nullptr descriptor is a hard error in draken. IPV4 is
# DRAKEN_UINT32 plus a descriptor, and a descriptor-less UINT32 is a perfectly
# well-formed integer column, so losing it degrades silently: addresses render
# as integers and CIDR_AGG refuses the column. Nothing fails, and that is what
# makes it expensive to find.
# ---------------------------------------------------------------------------

# 0 and 2**32-1 are the ends of the range; 3232235777 is 192.168.1.1, the value
# that catches an octet order reversed anywhere along the write/read path.
IPV4_VALUES = [0, 3232235777, None, 4294967295]
IPV4_TEXT = ["0.0.0.0", "192.168.1.1", None, "255.255.255.255"]


def _ipv4_morsel():
    nb = draken_native.vector_retag_uint32_as_ipv4(
        draken_native.vector_uint32_from_sequence(IPV4_VALUES)
    )
    return Morsel.from_vectors(["source_ip"], [Vector(nb)])


def test_ipv4_descriptor_survives_the_file():
    """A UINT32 column marked IPV4 must read back IPV4, not bare UINT32."""
    buf = skene.write_morsel(_ipv4_morsel(), read_acceleration=True)

    # Both directories carry it: the FILE schema (what a reader consults before
    # touching a row group) and the ROW GROUP column entry (what the decode
    # reconstructs the vector from). A descriptor in only one of them reads back
    # correctly by luck of which path a consumer takes.
    assert meta_logical_kind(skene.read_metadata(buf)) == LogicalKind.IPV4.value
    rg = skene.read_row_group_metadata(buf, 0)
    assert rg["columns"][0]["logical"]["kind"] == LogicalKind.IPV4.value

    m = skene.read_morsel(buf, 0)
    m.materialize()
    column = m.column("source_ip")
    # The descriptor itself, then the two things that are only true because of
    # it: the SQL name, and dotted-decimal rendering rather than integers.
    assert column._nb.logical_type_kind is LogicalKind.IPV4
    assert column.type is DrakenType.UINT32
    assert column.type_name == "IPV4"
    assert column.to_pylist() == IPV4_TEXT


def test_plain_uint32_gains_no_descriptor():
    """The other half of the contract — the writer must not invent one."""
    nb = draken_native.vector_uint32_from_sequence(IPV4_VALUES)
    buf = skene.write_morsel(
        Morsel.from_vectors(["port"], [Vector(nb)]), read_acceleration=True
    )
    assert meta_logical_kind(skene.read_metadata(buf)) is None

    m = skene.read_morsel(buf, 0)
    m.materialize()
    assert m.column("port")._nb.logical_type_kind is None
    assert m.column("port").type_name == "UINT32"
    assert m.column("port").to_pylist() == IPV4_VALUES


@pytest.mark.parametrize(
    "codec_options",
    [
        {},                                              # spill posture
        {"read_acceleration": True},                     # value ordering + stats
        {"read_acceleration": True, "codec": "lz4"},     # for_fast_reads
        {"read_acceleration": True, "codec": "zstd", "zstd_level": 7},  # for_storage
        {"read_acceleration": True, "bloom_columns": ["source_ip"]},
    ],
    ids=["spill", "accelerated", "lz4", "zstd7", "bloom"],
)
def test_ipv4_descriptor_survives_every_write_posture(codec_options):
    """The descriptor is schema, not payload — no posture may trade it away.

    Value ordering rewrites `data` and the selection codes, and compression
    rewrites the section bytes; both are transformations the descriptor has to
    ride over untouched.
    """
    buf = skene.write_morsel(_ipv4_morsel(), **codec_options)
    m = skene.read_morsel(buf, 0)
    m.materialize()
    assert m.column("source_ip").type_name == "IPV4"
    assert m.column("source_ip").to_pylist() == IPV4_TEXT


def test_every_logical_kind_round_trips():
    """Each kind, with a NON-DEFAULT parameter wherever it has one.

    A descriptor that is dropped and then rebuilt from defaults round-trips a
    TIMESTAMP[us] or a DECIMAL(0,0) convincingly, so a case built on default
    parameters cannot tell carriage from reconstruction. Every parameter here is
    off-default for that reason: a lost descriptor changes the name.
    """
    cases = [
        ("ipv4", draken_native.vector_retag_uint32_as_ipv4(
            draken_native.vector_uint32_from_sequence([3232235777, None])), "IPV4"),
        ("ts_ms", draken_native.vector_timestamp_from_sequence(
            [datetime.datetime(2023, 11, 14), None], "ms"), "TIMESTAMP[ms]"),
        ("ts_ns", draken_native.vector_timestamp_from_sequence(
            [datetime.datetime(2023, 11, 14), None], "ns"), "TIMESTAMP[ns]"),
        ("time32_ms", draken_native.vector_time32_from_sequence(
            [datetime.time(1, 2, 3), None], "ms"), "TIME[ms]"),
        ("time64_us", draken_native.vector_time64_from_sequence(
            [datetime.time(1, 2, 3), None], "us"), "TIME[us]"),
        ("dec", draken_native.vector_decimal_from_sequence(
            [Decimal("1.25"), None], 10, 2), "DECIMAL(10, 2)"),
        ("dec128", draken_native.vector_decimal128_from_sequence(
            [Decimal("1.25"), None], 30, 4), "DECIMAL(30, 4)"),
        ("fp16", draken_native.vector_fp16_from_sequence(
            [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], 3), "VECTOR(3)"),
    ]
    names = [name for name, _, _ in cases]
    morsel = Morsel.from_vectors(names, [Vector(nb) for _, nb, _ in cases])

    buf = skene.write_morsel(morsel, read_acceleration=True, codec="zstd", zstd_level=7)
    m = skene.read_morsel(buf, 0)
    m.materialize()

    assert [m.column(name).type_name for name in names] == [
        expected for _, _, expected in cases
    ]


if __name__ == "__main__":
    test_roundtrip_values()
    test_probe_and_metadata()
    test_projection_is_strict()
    test_corruption_fails_loud()
    test_spill_posture_roundtrip()
    test_codec_and_level_must_agree()
    test_ipv4_descriptor_survives_the_file()
    test_plain_uint32_gains_no_descriptor()
    for _options in ({}, {"read_acceleration": True},
                     {"read_acceleration": True, "codec": "lz4"},
                     {"read_acceleration": True, "codec": "zstd", "zstd_level": 7},
                     {"read_acceleration": True, "bloom_columns": ["source_ip"]}):
        test_ipv4_descriptor_survives_every_write_posture(_options)
    test_every_logical_kind_round_trips()
    print("✅ okay")
