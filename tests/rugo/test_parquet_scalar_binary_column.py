"""
Regression tests for SCALAR BYTE_ARRAY columns in the rugo parquet reader
(`_make_string_vector` in rugo/src/parquet/parquet_reader.pxi).

This is the scalar twin of test_parquet_array_binary_leaf.py. Parquet stores
VARCHAR and opaque BINARY identically on the wire, as BYTE_ARRAY, and separates
them ONLY by the String logical annotation. The array leaf path was taught to
read that annotation; the scalar materializer was not, so EVERY unannotated
BYTE_ARRAY column came back tagged VARCHAR — lying about its type, and raising
UnicodeDecodeError on any value that is not valid UTF-8.

All three byte_array shapes (dense, dictionary-encoded, and the single-value
constant) route through `_make_string_vector`, so all three are asserted here:
the tag is derived once, but a future shape-specialised path must not be able
to reintroduce the divergence unnoticed.

pyarrow is used here purely as a test-fixture writer (tests only).
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pyarrow as pa  # fixture writer only
import pyarrow.parquet as pq  # fixture writer only
import pytest

import draken.draken_native as dn
import rugo.parquet as rp

# Not valid UTF-8 in any position: 0xff is never a legal lead byte, and this is
# exactly the truncated-bound sentinel shape the array-leaf fixture carries.
NON_UTF8 = b"pre\xff"


def _read(path, columns):
    """({name: [values]}, {name: DrakenType}) concatenated across morsels."""
    values = {name: [] for name in columns}
    types = {}
    with rp.read_parquet(str(path), columns=columns) as reader:
        for morsel in reader:
            for name in columns:
                vector = morsel.column(name)
                types[name] = vector.type
                values[name].extend(vector[i] for i in range(morsel.num_rows))
    return values, types


def _roundtrip(tmp_path, table, **write_kwargs):
    path = tmp_path / "scalar.parquet"
    pq.write_table(table, path, **write_kwargs)
    return _read(path, list(table.column_names))


def test_scalar_binary_column_is_varbinary_and_keeps_its_bytes(tmp_path):
    """The repro: an unannotated BYTE_ARRAY column stays opaque bytes."""
    values, types = _roundtrip(
        tmp_path,
        pa.table(
            {
                "bin": pa.array([b"a", NON_UTF8, None], pa.binary()),
                "txt": pa.array(["a", "b", None], pa.string()),
            }
        ),
        use_dictionary=False,
    )

    assert types == {"bin": dn.VARBINARY, "txt": dn.VARCHAR}
    # Byte-for-byte: a reader that decoded these could only fail or corrupt them.
    assert values["bin"] == [b"a", NON_UTF8, None]
    assert values["txt"] == ["a", "b", None]


def test_scalar_binary_column_dictionary_encoded(tmp_path):
    """Dictionary shape routes through the same materializer."""
    values, types = _roundtrip(
        tmp_path,
        pa.table(
            {
                "bin": pa.array([b"a", NON_UTF8, b"a", NON_UTF8], pa.binary()),
                "txt": pa.array(["a", "b", "a", "b"], pa.string()),
            }
        ),
        use_dictionary=True,
    )

    assert types == {"bin": dn.VARBINARY, "txt": dn.VARCHAR}
    assert values["bin"] == [b"a", NON_UTF8, b"a", NON_UTF8]
    assert values["txt"] == ["a", "b", "a", "b"]


def test_scalar_binary_column_constant_shape(tmp_path):
    """A single-distinct-value, all-valid column takes the constant path."""
    values, types = _roundtrip(
        tmp_path,
        pa.table(
            {
                "bin": pa.array([NON_UTF8] * 4, pa.binary()),
                "txt": pa.array(["a"] * 4, pa.string()),
            }
        ),
        use_dictionary=True,
    )

    assert types == {"bin": dn.VARBINARY, "txt": dn.VARCHAR}
    assert values["bin"] == [NON_UTF8] * 4
    assert values["txt"] == ["a"] * 4


def test_all_null_binary_column_takes_its_type_from_the_annotation(tmp_path):
    """No value exists to infer from, so the annotation must carry the tag.

    This is the case that would mis-tag VARBINARY->VARCHAR without ever raising.
    """
    values, types = _roundtrip(
        tmp_path,
        pa.table(
            {
                "bin": pa.array([None, None], pa.binary()),
                "txt": pa.array([None, None], pa.string()),
            }
        ),
    )

    assert types == {"bin": dn.VARBINARY, "txt": dn.VARCHAR}
    assert values == {"bin": [None, None], "txt": [None, None]}


def test_empty_binary_value_is_empty_bytes_not_empty_str(tmp_path):
    """A zero-length BYTE_ARRAY value must not collapse to "" of the wrong type."""
    values, types = _roundtrip(
        tmp_path,
        pa.table(
            {
                "bin": pa.array([b"", NON_UTF8], pa.binary()),
                "txt": pa.array(["", "b"], pa.string()),
            }
        ),
        use_dictionary=False,
    )

    assert types == {"bin": dn.VARBINARY, "txt": dn.VARCHAR}
    assert values["bin"] == [b"", NON_UTF8]
    assert values["txt"] == ["", "b"]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
