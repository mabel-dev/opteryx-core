"""
Regression tests for the rugo parquet ARRAY (list) column reader
(`_make_array_vector` in rugo/src/parquet/parquet_reader.pyx).

Covers:
  1. Reading a real manifest parquet whose schema mixes single-level lists
     (list<int64>, list<string>) and nested lists (list<list<int64>>), with
     every column's values verified against pyarrow (pyarrow is allowed in
     tests only).
  2. rugo write -> read round-trip for single-level int / string arrays,
     including empty lists and null lists.
  3. Honest failure surface: float / bool element arrays are not yet
     constructible by Draken and must raise rather than silently mis-type.

pyarrow is used here purely as the test oracle.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow.parquet as pq  # test oracle only

import rugo.parquet as rp
import rugo.parquet as pr
import draken.draken_native as dn
from draken.vectors.vector import Vector
from draken.morsels.morsel import Morsel

MANIFEST = (
    REPO_ROOT
    / "testdata"
    / "parquet_tests"
    / "opteryx_store_test_tweets_metadata_manifest-1771602522625.parquet"
)


def _morsels_to_dict(morsels):
    out = {}
    for m in morsels:
        for name in m.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            col = m.column(name.encode() if isinstance(name, str) else name)
            out.setdefault(key, []).extend(col.to_pylist())
    return out


def _norm(x):
    """Normalize nested lists and bytes->str so rugo (str) and pyarrow agree."""
    res = []
    for row in x:
        if row is None:
            res.append(None)
        elif isinstance(row, (list, tuple)):
            res.append(_norm(list(row)))
        elif isinstance(row, bytes):
            res.append(row.decode("utf-8"))
        else:
            res.append(row)
    return res


def test_manifest_all_columns_match_pyarrow():
    """Every column of the real manifest round-trips identically to pyarrow,
    including single-level and nested list columns."""
    data = MANIFEST.read_bytes()
    expected = pq.read_table(str(MANIFEST)).to_pydict()
    got = _morsels_to_dict(pr.read_parquet(data))

    # Every column — single-level lists AND nested lists — must report its plain
    # top-level field name (e.g. "min_k_hashes", not "min_k_hashes.list.element").
    assert set(got) == set(expected), (
        f"column name mismatch: missing={sorted(set(expected) - set(got))} "
        f"extra={sorted(set(got) - set(expected))}"
    )
    for col, exp in expected.items():
        assert _norm(got[col]) == _norm(exp), f"value mismatch for column {col!r}"


def _roundtrip(seq, name="a"):
    vec = Vector(dn.vector_array_from_sequence(seq))
    morsel = Morsel.from_vectors([name], [vec])
    buf = rp.write_parquet(morsel, compression="none", bloom_filters=False)
    got = _morsels_to_dict(pr.read_parquet(buf))
    return _norm(got[name])


def test_roundtrip_int_list_with_empty_and_null():
    seq = [[10, 20], [], None, [30, 40, 50]]
    assert _roundtrip(seq) == seq


def test_roundtrip_string_list_with_empty_and_null():
    seq = [["a", "bb"], [], None, ["c"]]
    assert _roundtrip(seq) == seq


def test_roundtrip_int_list_all_present():
    seq = [[1], [2, 3], [4, 5, 6]]
    assert _roundtrip(seq) == seq


def test_roundtrip_float_list_with_empty_and_null():
    seq = [[1.5, 2.5], [], None, [3.5, 4.5, 5.5]]
    assert _roundtrip(seq) == seq


def test_roundtrip_bool_list_with_empty_and_null():
    seq = [[True, False], [], None, [True]]
    assert _roundtrip(seq) == seq


def _read_pyarrow(arrow_type, data, name="a"):
    """Write `data` with pyarrow (oracle) and read it back through rugo."""
    import io
    import pyarrow as pa

    t = pa.table({name: pa.array(data, type=arrow_type)})
    buf = io.BytesIO()
    pq.write_table(t, buf, compression=None)
    got = _morsels_to_dict(pr.read_parquet(buf.getvalue()))
    key = name if name in got else f"{name}.list.element"
    return _norm(got[key])


def test_read_pyarrow_float_and_bool_and_nested_float():
    import pyarrow as pa

    cases = [
        (pa.list_(pa.float64()), [[1.5, 2.5], [], None, [3.5, None]]),
        (pa.list_(pa.float32()), [[1.5, 2.5], [7.0]]),
        (pa.list_(pa.bool_()), [[True, False], [], None, [True]]),
        (pa.list_(pa.list_(pa.float64())), [[[1.5], [2.5, 3.5]], [[]]]),
    ]
    for arrow_type, data in cases:
        assert _read_pyarrow(arrow_type, data) == data, arrow_type


if __name__ == "__main__":
    test_manifest_all_columns_match_pyarrow()
    test_roundtrip_int_list_with_empty_and_null()
    test_roundtrip_string_list_with_empty_and_null()
    test_roundtrip_int_list_all_present()
    test_roundtrip_float_list_with_empty_and_null()
    test_roundtrip_bool_list_with_empty_and_null()
    test_read_pyarrow_float_and_bool_and_nested_float()
    print("✅ all array reader tests passed")
