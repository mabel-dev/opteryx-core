"""
A LIST column's ELEMENT keeps its own width through a rugo write -> read trip.

The scalar rules already said so: parquet stores every integer narrower than 64
bits on physical int32/int64 plus an INTEGER(bitWidth, isSigned) annotation, and
a float at its own physical width. A list element is a column like any other and
follows the same rules — `list<int32>` is `list<element: int32>` on disk, not
`list<element: int64>`.

It did not until 2026-08-21. Three separate widenings stacked up:

  * draken's `vector_array_from_sequence` inferred the child type from the leaf
    PYTHON objects, so an `int` could only ever produce an INT64 child and a
    `float` a FLOAT64 one — the `element_type` argument was consulted only when
    every row was null/empty. Only DRAKEN_UINT64 overrode inference.
  * rugo's writer staged every integer leaf in an int64 buffer and every float
    leaf in a double buffer, then declared `PT_INT64` / `PT_DOUBLE` — and tagged
    EVERY unsigned leaf `INTEGER(64, unsigned)` whatever its real width.
  * rugo's reader mapped both int32 and int64 leaves onto an INT64 child and
    both float widths onto FLOAT64.

⛔ ASSERT THE TYPE, NOT JUST THE VALUES. Every narrow value is exact at the wider
width, so a widened list compares equal element for element. Only the child
DrakenType and the parquet leaf type show the difference — which is exactly why
this went unnoticed. `test_arrays_are_not_all_secretly_the_same_child` guards
against the whole table silently collapsing back onto INT64/FLOAT64.

pyarrow is the oracle for the file itself: a file only rugo can read is a defect.
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

# (label, draken element type, arrow leaf type, rows)
# Every integer case carries a value at the edge of its declared range, so a
# width or signedness drift changes the VALUE too and cannot pass on values
# alone. The float32 values are exact in binary32, so they need no tolerance.
CASES = [
    ("int8", dn.INT8, "int8", [[1, -2], [-128, 127]]),
    ("int16", dn.INT16, "int16", [[1, -2], [-32768, 32767]]),
    ("int32", dn.INT32, "int32", [[1, -2], [-2147483648, 2147483647]]),
    ("int64", dn.INT64, "int64", [[1, -2], [-(2**63), 2**63 - 1]]),
    ("uint8", dn.UINT8, "uint8", [[0, 200], [255]]),
    ("uint16", dn.UINT16, "uint16", [[0, 40000], [65535]]),
    ("uint32", dn.UINT32, "uint32", [[0, 3000000000], [4294967295]]),
    ("uint64", dn.UINT64, "uint64", [[0, 2**63], [2**64 - 1]]),
    ("float32", dn.FLOAT32, "float", [[0.125, -1.5], [3.5]]),
    ("float64", dn.FLOAT64, "double", [[0.1, -1.5], [3.5]]),
    ("bool", dn.BOOL, "bool", [[True, False], [True]]),
    ("varchar", dn.VARCHAR, "string", [["a", "b"], ["c"]]),
]


def _array_morsel(element_type, rows, name="a", depth=1):
    vector = Vector(dn.vector_array_from_sequence(rows, int(element_type.value), depth))
    return Morsel.from_vectors([name], [vector])


def _read_back(buf, name="a"):
    with rp.read_parquet(buf) as reader:
        out = list(reader)[0]
    column = out.column(name)
    return column._nb.array_child_type, column.to_pylist()


@pytest.mark.parametrize("compression", ["none", "zstd"])
@pytest.mark.parametrize("label,element_type,arrow_leaf,rows", CASES)
def test_leaf_width_survives_rugo_roundtrip(label, element_type, arrow_leaf, rows, compression):
    """rugo write -> rugo read returns the same child DrakenType and values."""
    source = _array_morsel(element_type, rows)
    assert source.column("a")._nb.array_child_type == element_type  # guards the fixture

    buf = rp.write_parquet(source, compression=compression)
    child_type, values = _read_back(buf)

    assert child_type == element_type, f"{label}: {child_type} != {element_type}"
    assert values == rows


@pytest.mark.parametrize("label,element_type,arrow_leaf,rows", CASES)
def test_pyarrow_reads_the_declared_leaf_type(label, element_type, arrow_leaf, rows):
    """The file is spec-conformant: pyarrow sees the declared leaf width."""
    import pyarrow.parquet as pq

    buf = rp.write_parquet(_array_morsel(element_type, rows), compression="none")
    parquet_file = pq.ParquetFile(io.BytesIO(buf))

    assert str(parquet_file.schema_arrow.field(0).type) == f"list<element: {arrow_leaf}>"
    assert parquet_file.read().column(0).to_pylist() == rows


@pytest.mark.parametrize("label,element_type,arrow_leaf,rows", [
    case for case in CASES if case[0] in ("int32", "uint32", "float32", "int8")
])
def test_nested_lists_keep_the_leaf_width(label, element_type, arrow_leaf, rows):
    """A list<list<T>> leaf follows the same rule — the element type is threaded
    down through the nesting, so depth is not a place it can be lost."""
    import pyarrow.parquet as pq

    nested = [[rows[0]], [rows[1]]]
    buf = rp.write_parquet(_array_morsel(element_type, nested, depth=2), compression="none")

    assert str(pq.ParquetFile(io.BytesIO(buf)).schema_arrow.field(0).type) == (
        f"list<element: list<element: {arrow_leaf}>>"
    )
    _, values = _read_back(buf)
    assert values == nested


@pytest.mark.parametrize("label,element_type,arrow_leaf,rows", CASES)
def test_null_and_empty_lists_do_not_disturb_the_leaf_width(label, element_type, arrow_leaf, rows):
    """A null list and an empty list write no element at all, so the leaf type
    has to come from the DECLARATION on those rows, not from a value."""
    with_gaps = [rows[0], None, [], rows[1]]

    buf = rp.write_parquet(_array_morsel(element_type, with_gaps), compression="none")
    child_type, values = _read_back(buf)

    assert child_type == element_type
    assert values == with_gaps


def test_array_agg_over_a_narrow_column_roundtrips_at_its_width():
    """The engine really does produce narrow-child ARRAYs — `ARRAY_AGG` over an
    INT32/FLOAT32 column is the shape that made this reachable from SQL, not just
    from the draken constructor."""
    import tempfile

    import opteryx
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "k": pa.array(["a", "b", "a"], pa.string()),
        "i32": pa.array([1, 2, 3], pa.int32()),
        "f32": pa.array([0.125, 0.25, 0.5], pa.float32()),
    })
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, "narrow")
        os.makedirs(data_dir)
        pq.write_table(table, os.path.join(data_dir, "data.parquet"))

        session = opteryx.session()
        for column, element_type, arrow_leaf in (
            ("i32", dn.INT32, "int32"),
            ("f32", dn.FLOAT32, "float"),
        ):
            sql = f"SELECT k, ARRAY_AGG({column}) AS a FROM '{data_dir}' GROUP BY k"
            morsel = next(iter(session.execute_to_morsels(sql)))
            assert morsel.column(b"a")._nb.array_child_type == element_type

            buf = rp.write_parquet(morsel, compression="none")
            assert str(pq.ParquetFile(io.BytesIO(buf)).schema_arrow.field("a").type) == (
                f"list<element: {arrow_leaf}>"
            )
            child_type, _ = _read_back(buf)
            assert child_type == element_type


@pytest.mark.parametrize("label,element_type,arrow_leaf,rows", CASES)
def test_opteryx_scan_binds_and_produces_the_same_leaf_width(
    label, element_type, arrow_leaf, rows, tmp_path
):
    """The width has to agree in THREE places, not two: the file, the type the
    planner binds from the footer, and the vector the scan hands execution.

    Kernels are chosen at plan time from the declared type, so a declared
    ARRAY<INT8> over a runtime INT32 child is the same silent-wrong-answer setup
    as the scalar case — and opteryx reaches the leaf through its OWN decoders
    (the IPC child tags / the native array pool), not through rugo's reader, so
    passing `test_leaf_width_survives_rugo_roundtrip` does not imply this."""
    import opteryx
    from opteryx.connectors._rugo_schema import rugo_to_relation_schema

    data_dir = tmp_path / "arrays"
    data_dir.mkdir()
    buf = rp.write_parquet(_array_morsel(element_type, rows), compression="none")
    (data_dir / "data.parquet").write_bytes(buf)

    declared = {
        column.name: column.column_type
        for column in rugo_to_relation_schema(
            rp.read_metadata(str(data_dir / "data.parquet")), "t"
        ).columns
    }
    assert str(declared["a"]) == f"ARRAY<{element_type.name}>", declared

    session = opteryx.session()
    morsel = next(iter(session.execute_to_morsels(f"SELECT * FROM '{data_dir}'")))
    column = morsel.column(b"a")
    assert column._nb.array_child_type == element_type
    assert column.to_pylist() == rows


def test_arrays_are_not_all_secretly_the_same_child():
    """Guards the table above: if every case collapsed back onto an INT64 /
    FLOAT64 child, every value assertion here would still pass. Assert that the
    twelve cases really do produce twelve distinct child types."""
    produced = {
        label: _array_morsel(element_type, rows).column("a")._nb.array_child_type
        for label, element_type, _, rows in CASES
    }
    assert len(set(produced.values())) == len(CASES), produced


if __name__ == "__main__":  # pragma: no cover
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-q"]))
