# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Draken logical kinds parquet cannot express survive a rugo write -> read trip.

Parquet has a logical type for DATE, TIME, TIMESTAMP, DECIMAL and INTERVAL, so
those round-trip through the schema annotation. IPV4 has none: it is
DRAKEN_UINT32 plus a descriptor that lives on the VectorOwner, outside the
DrakenVector. Written without a side channel it comes back as a bare unsigned
integer — a perfectly well-formed column of the wrong type, with no error
anywhere. rugo therefore records the draken LogicalKind in the file's
FileMetaData key-value metadata and the reader reconstructs from it.

Two traps this file exists to avoid:

  * `col.type` is UINT32 for an address column AND for a plain unsigned one —
    IPv4 IS uint32, only the descriptor separates them. Every assertion here
    goes through `type_name` (or the metadata's kind), never `type`.
  * `<<=` (CIDR containment) answers correctly over a descriptor-less uint32:
    it is rewritten to an integer range comparison and never touches the type,
    so it cannot tell a working column from a broken one. It is not used here.

ABSENCE IS NOT A NEGATIVE: a file written before this side channel existed
carries no entry, and must read back exactly as it did — "don't know", never
"not IPV4". `test_plain_uint32_is_untouched` and
`test_foreign_file_without_annotation_reads_unchanged` pin that end.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
import rugo.parquet as rp
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# draken LogicalKind ordinal for IPV4 (draken/core/draken_bridge.h).
IPV4_KIND = 5

# Values chosen to span the signed midpoint: 255.255.255.255 is 0xFFFFFFFF, so it
# occupies a NEGATIVE physical int32 slot on the wire. A descriptor that survived
# only for small addresses would pass a test that stopped at 192.168.x.x.
ADDRESSES = [0x7F000001, 0x0A000001, 0xC0A80101, 0x00000000, 0xFFFFFFFF]
DOTTED = ["127.0.0.1", "10.0.0.1", "192.168.1.1", "0.0.0.0", "255.255.255.255"]


def _ipv4_vector(values=ADDRESSES):
    return Vector(dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(values)))


def _morsel(values=ADDRESSES):
    """One annotated column beside an IDENTICALLY VALUED plain uint32 one.

    The plain column is the control: it proves the annotation is per-column and
    that a uint32 without one is left alone, which a single-column fixture
    cannot show.
    """
    return Morsel.from_vectors(
        ["source_ip", "plain_u32"],
        [_ipv4_vector(values), Vector(dn.vector_uint32_from_sequence(values))],
    )


def _kinds(buf):
    meta = rp.read_metadata_from_memoryview(memoryview(buf))
    return {c.name: c.draken_logical_kind for c in meta.schema_columns}


def _read_one(buf):
    with rp.read_parquet(buf) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    return morsels[0]


# --- the writer records it -----------------------------------------------------


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("dictionary", [True, False])
def test_writer_records_the_kind(compression, dictionary):
    buf = rp.write_parquet(_morsel(), compression=compression, dictionary=dictionary)
    assert _kinds(buf) == {"source_ip": IPV4_KIND, "plain_u32": 0}


def test_no_annotation_no_key_value_metadata():
    """A file with nothing to annotate must be byte-identical to one written
    before the side channel existed — the field is omitted, not written empty."""
    plain = Morsel.from_vectors(["a"], [Vector(dn.vector_uint32_from_sequence(ADDRESSES))])
    buf = rp.write_parquet(plain, compression="none")
    assert b"draken.logical" not in buf
    assert _kinds(buf) == {"a": 0}


# --- the reader reconstructs from it -------------------------------------------


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("dictionary", [True, False])
def test_roundtrips_through_rugo(compression, dictionary):
    """rugo write -> rugo read hands back an IPV4 column, not a bare uint32."""
    buf = rp.write_parquet(_morsel(), compression=compression, dictionary=dictionary)
    out = _read_one(buf)

    ip = out.column("source_ip")
    assert ip.type_name == "IPV4"
    assert ip.to_pylist() == DOTTED


@pytest.mark.parametrize("dictionary", [True, False])
def test_plain_uint32_is_untouched(dictionary):
    """The control column: no annotation, no descriptor, integer values."""
    buf = rp.write_parquet(_morsel(), compression="none", dictionary=dictionary)
    plain = _read_one(buf).column("plain_u32")

    assert plain.type_name == "UINT32"
    assert plain.to_pylist() == ADDRESSES


def test_nulls_keep_the_descriptor():
    """Interior nulls must not disturb the annotation — the descriptor is a
    property of the column, not of the values that happen to be present."""
    values = [ADDRESSES[0], None, ADDRESSES[-1]]
    src = Morsel.from_vectors(["source_ip", "plain_u32"], [
        Vector(dn.vector_retag_uint32_as_ipv4(dn.vector_uint32_from_sequence(values))),
        Vector(dn.vector_uint32_from_sequence(values)),
    ])

    out = _read_one(rp.write_parquet(src, compression="none", dictionary=False))
    assert out.column("source_ip").type_name == "IPV4"
    assert out.column("source_ip").to_pylist() == [DOTTED[0], None, DOTTED[-1]]
    assert out.column("plain_u32").type_name == "UINT32"


def test_projection_carries_the_annotation():
    """The annotation is keyed by column name, so a projection that reorders or
    omits columns must still resolve it to the right one."""
    buf = rp.write_parquet(_morsel(), compression="none")
    with rp.read_parquet(buf, columns=["source_ip"]) as reader:
        out = list(reader)[0]
    assert out.column("source_ip").type_name == "IPV4"


# --- files that carry no annotation --------------------------------------------


def test_foreign_file_without_annotation_reads_unchanged():
    """A pyarrow-written uint32 column has no key-value metadata at all and must
    read back exactly as it does today: a plain unsigned integer column."""
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    sink = io.BytesIO()
    table = pa.table({"source_ip": pa.array(ADDRESSES, type=pa.uint32())})
    pq.write_table(table, sink)
    buf = sink.getvalue()

    assert _kinds(buf) == {"source_ip": 0}
    col = _read_one(buf).column("source_ip")
    assert col.type_name == "UINT32"
    assert col.to_pylist() == ADDRESSES


def test_pyarrow_still_reads_an_annotated_file():
    """The hard interoperability criterion: the side channel is file-level
    key-value metadata a reader that has never heard of draken ignores. It must
    not change what anything else sees — the column stays a spec-conformant
    uint32 carrying the same values."""
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = rp.write_parquet(_morsel(), compression="none")
    table = pq.read_table(io.BytesIO(buf))

    assert str(table.schema.field("source_ip").type) == "uint32"
    assert table.column("source_ip").to_pylist() == ADDRESSES
    assert table.column("plain_u32").to_pylist() == ADDRESSES


# --- the footer patcher must not strip it ---------------------------------------


def test_patch_rename_carries_the_annotation():
    """patch_columns rebuilds the footer from the source file's schema. A rebuild
    that forgot the side channel would silently strip the descriptor off every
    column it kept — the same loss, one layer up. The rename also proves the key
    tracks the column's NEW name rather than stranding the old one."""
    buf = rp.write_parquet(_morsel(), compression="none")
    patched = rp.patch_columns(buf, rename={"source_ip": "client_ip"})

    assert _kinds(patched) == {"client_ip": IPV4_KIND, "plain_u32": 0}
    out = _read_one(patched)
    assert out.column("client_ip").type_name == "IPV4"
    assert out.column("client_ip").to_pylist() == DOTTED


def test_patch_drop_keeps_the_survivor_annotated():
    buf = rp.write_parquet(_morsel(), compression="none")
    patched = rp.patch_columns(buf, drop=["plain_u32"])

    assert _kinds(patched) == {"source_ip": IPV4_KIND}
    assert _read_one(patched).column("source_ip").type_name == "IPV4"


# --- end to end through the engine ----------------------------------------------


def test_engine_reads_an_annotated_file_as_ipv4(tmp_path):
    """The defect this side channel exists to close, end to end.

    The scan's schema here comes from the FILE, not a catalog declaration, so
    before the annotation existed the engine saw a bare uint32: the column
    rendered as an integer and CAST(... AS VARCHAR) produced '2130706433'
    instead of an address. CAST is the probe of record — `<<=` cannot see this
    defect at all (see the module docstring).
    """
    import opteryx

    dataset = tmp_path / "ipv4_dataset"
    dataset.mkdir()
    (dataset / "part.parquet").write_bytes(rp.write_parquet(_morsel(), compression="none"))

    sql = (
        "SELECT CAST(source_ip AS VARCHAR) AS ip_text, "
        "CAST(plain_u32 AS VARCHAR) AS plain_text "
        f"FROM '{dataset}'"
    )
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(zip(morsel.column("ip_text").to_pylist(),
                        morsel.column("plain_text").to_pylist()))

    assert [ip for ip, _ in rows] == DOTTED
    assert [plain for _, plain in rows] == [str(v) for v in ADDRESSES]


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
