"""
Regression tests for BYTE_ARRAY list leaves in the rugo parquet ARRAY reader
(`_array_leaf_values` / `_make_array_vector` in
rugo/src/parquet/parquet_reader.pxi).

Parquet stores VARCHAR and opaque BINARY identically on the wire, as
BYTE_ARRAY, and separates them ONLY by the String logical annotation. The reader
used to `.decode("utf-8")` every leaf unconditionally, which both collapsed
VARBINARY into VARCHAR and raised UnicodeDecodeError on any leaf that is not
valid UTF-8.

The canonical such leaf is a TRUNCATED STRING BOUND: appending 0xff to a
truncated prefix makes it sort above every string sharing that prefix, which is
the whole point of a truncated upper bound and is necessarily not valid UTF-8.
`testdata/metadata/metadata.parquet` is a real, well-formed manifest of exactly
that shape — its scalar `file_path` column IS annotated String while its
`min_values`/`max_values` list leaves are NOT:

    optional binary field_id=-1 file_path (String);
    optional group min_values (List) {
      repeated group list { optional binary element; }   <- no String annotation
    }

⛔ That file is the only repro of this in the tree. Do not "fix" the data and do
not delete it.

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

MANIFEST = str(REPO_ROOT / "testdata" / "metadata" / "metadata.parquet")

# Pinned from the fixture, PER bound column (min_values and max_values happen to
# be identical here): the leaf count, and how many of those leaves carry the
# 0xff truncation sentinel. They are asserted rather than merely counted so that
# a future reader change which silently drops or re-encodes leaves cannot pass
# by producing a shorter, tidier list.
EXPECTED_LEAVES = 4574
EXPECTED_SENTINEL_LEAVES = 1020


def _read_columns(source, columns):
    """{name: [row value, ...]} for `columns`, concatenated across morsels."""
    out = {name: [] for name in columns}
    with rp.read_parquet(source, columns=columns) as reader:
        for morsel in reader:
            for name in columns:
                vector = morsel.column(name)
                out[name].extend(vector[i] for i in range(morsel.num_rows))
    return out


def _child_types(source, columns):
    """{name: DrakenType of the ARRAY column's child vector}."""
    out = {}
    with rp.read_parquet(source, columns=columns) as reader:
        for morsel in reader:
            for name in columns:
                out[name] = morsel.column(name).array_child.type
    return out


def test_manifest_binary_bounds_read_as_bytes_with_sentinel_intact():
    """The repro: unannotated BYTE_ARRAY leaves come back as opaque bytes."""
    rows = _read_columns(MANIFEST, ["min_values", "max_values"])

    for name in ("min_values", "max_values"):
        total = 0
        sentinel = 0
        non_utf8 = 0
        for row in rows[name]:
            if row is None:
                continue
            for leaf in row:
                if leaf is None:
                    continue
                total += 1
                assert isinstance(leaf, bytes), (
                    "unannotated BYTE_ARRAY leaf must stay opaque bytes, got "
                    f"{type(leaf).__name__} in {name}"
                )
                if leaf.endswith(b"\xff"):
                    sentinel += 1
                try:
                    leaf.decode("utf-8")
                except UnicodeDecodeError:
                    non_utf8 += 1

        assert total == EXPECTED_LEAVES, name
        assert sentinel == EXPECTED_SENTINEL_LEAVES, name
        # The point of the fixture: these bytes are NOT representable as text, so
        # a reader that decodes them cannot be merely lossy — it must fail
        # outright. Every sentinel-terminated leaf is one such value.
        assert non_utf8 == EXPECTED_SENTINEL_LEAVES, name

    assert _child_types(MANIFEST, ["min_values", "max_values"]) == {
        "min_values": dn.VARBINARY,
        "max_values": dn.VARBINARY,
    }


def test_manifest_annotated_string_column_still_reads_as_str():
    """The same file's String-annotated scalar column is unaffected."""
    rows = _read_columns(MANIFEST, ["file_path"])["file_path"]
    assert rows, "fixture has no rows"
    assert all(isinstance(value, str) for value in rows if value is not None)


def _roundtrip(tmp_path, table):
    path = tmp_path / "leaf.parquet"
    pq.write_table(table, path)
    return _read_columns(str(path), list(table.column_names))


def test_binary_and_string_list_leaves_are_typed_separately(tmp_path):
    """list<string> -> str, list<binary> -> bytes, in the same file."""
    got = _roundtrip(
        tmp_path,
        pa.table(
            {
                "text": pa.array([["a", "b"], ["c"], None, []], pa.list_(pa.string())),
                "binary": pa.array(
                    [[b"a", b"\xff\xfe"], [b""], None, []], pa.list_(pa.binary())
                ),
            }
        ),
    )

    assert got["text"] == [["a", "b"], ["c"], None, []]
    assert got["binary"] == [[b"a", b"\xff\xfe"], [b""], None, []]


def test_binary_list_leaf_survives_nesting_and_nulls(tmp_path):
    """Nested lists and null ELEMENTS keep the binary leaf typing."""
    got = _roundtrip(
        tmp_path,
        pa.table(
            {
                "holes": pa.array(
                    [[b"\xff", None, b"z"], None], pa.list_(pa.binary())
                ),
                "nested": pa.array(
                    [[[b"\x00\xff"], []], None], pa.list_(pa.list_(pa.binary()))
                ),
            }
        ),
    )

    assert got["holes"] == [[b"\xff", None, b"z"], None]
    assert got["nested"] == [[[b"\x00\xff"], []], None]


def test_all_null_binary_list_takes_its_type_from_the_schema(tmp_path):
    """No leaf value exists to infer from, so the annotation must carry it.

    This is the path that would silently mis-tag the child VARBINARY->VARCHAR
    even when no value ever raised, so it is asserted on the draken child type
    rather than only on the rendered rows.
    """
    path = tmp_path / "leaf.parquet"
    pq.write_table(
        pa.table(
            {
                "empty_binary": pa.array([None, [], None], pa.list_(pa.binary())),
                "empty_text": pa.array([None, [], None], pa.list_(pa.string())),
            }
        ),
        path,
    )
    columns = ["empty_binary", "empty_text"]

    assert _read_columns(str(path), columns) == {
        "empty_binary": [None, [], None],
        "empty_text": [None, [], None],
    }
    assert _child_types(str(path), columns) == {
        "empty_binary": dn.VARBINARY,
        "empty_text": dn.VARCHAR,
    }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
