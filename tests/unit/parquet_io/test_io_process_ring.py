import pyarrow

from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.connectors.parquet_io.io_process_ring import _build_row_group_from_payload
from opteryx.connectors.parquet_io.io_process_ring import _serialize_morsel
from opteryx.connectors.parquet_io.io_process_ring import _slice_and_serialize


def _sample_morsel(rows: int = 64) -> Morsel:
    values = list(range(rows))
    strings = [f"value-{i:04d}" for i in range(rows)]
    table = pyarrow.table({"c0": values, "c1": strings})
    return Morsel.from_arrow(table)


def test_slice_and_serialize_single_transfer():
    morsel = _sample_morsel(rows=32)
    transfers, serialize_ns = _slice_and_serialize(
        morsel,
        slot_payload_bytes=1024 * 1024,
        max_fragments_per_transfer=8,
        target_slice_bytes=16 * 1024,
    )
    assert serialize_ns > 0
    assert len(transfers) == 1
    assert transfers[0]["rows_in_slice"] == 32
    assert transfers[0]["fragment_count"] == 1
    assert transfers[0]["slice_count"] == 1


def test_slice_and_serialize_applies_row_slicing():
    morsel = _sample_morsel(rows=128)
    transfers, _ = _slice_and_serialize(
        morsel,
        slot_payload_bytes=512,
        max_fragments_per_transfer=1,
        target_slice_bytes=256,
    )
    assert len(transfers) > 1
    assert all(entry["fragment_count"] <= 1 for entry in transfers)
    assert sum(entry["rows_in_slice"] for entry in transfers) == 128
    assert transfers[-1]["slice_count"] == len(transfers)


def test_payload_roundtrip_to_row_group_dict():
    morsel = _sample_morsel(rows=10)
    payload, _ = _serialize_morsel(morsel)
    metadata = {"__path__": "x.parquet", "__row_group__": 7}
    row_group, deserialize_ns = _build_row_group_from_payload(payload, metadata)

    assert deserialize_ns > 0
    assert row_group["__path__"] == "x.parquet"
    assert row_group["__row_group__"] == 7
    assert len(row_group["c0"]) == 10
    assert len(row_group["c1"]) == 10
