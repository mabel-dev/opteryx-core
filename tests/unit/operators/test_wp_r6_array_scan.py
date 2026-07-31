"""R6 — admit ARRAY (parquet LIST) columns to the native parquet scan.

ARRAY was the last observed `non_admissible_kind` residual and, on ordinary data,
the biggest real-world source of the Python trampoline: a plain `SELECT *` over
`testdata.astronauts` or `testdata/flat/formats/parquet` dropped the WHOLE scan to
`StreamingScanSource` because one column was a list.

A list column always lands DK_POOL — rugo's `direct_kind_for` routes any column
with repetition levels to the pool, regardless of encoding — and is serialized as
TAG_ARRAY (11) by `ipc_serialize.hpp::serialize_list_column`. Both scan paths share
that PRODUCER verbatim; only the consumer differed. `native_array_pool_decode.hpp`
is the PyObject-free port of the trampoline's Cython `_build_array_vector*`
(opteryx/compiled/structures/column_deserializer.pyx), and `array_columns` — a
plan-time flag parallel to column_names, the same mechanism as `decimal_columns` /
`varchar_columns` — is what tells the Source which decoder owns a given pool blob.

The correctness gate is A/B PARITY: the native path must produce the same rows, in
the same nesting shape, as the forced-trampoline path. These are DIFFERENT things
and both must be right:

  * a NULL list          (`None`)              — parent validity bit clear
  * an EMPTY list        (`[]`)                — offsets[i] == offsets[i+1]
  * a list of NULLs      (`[None]`)            — child validity bit clear
  * a NULL nested list   (`[[7], None, []]`)   — inner level's own validity

`testdata/flat/array_types` (written by dev/generate_array_testdata.py) carries all
of them across every element type the wire format can express, over two row groups.
The pre-existing ARRAY datasets are exercised too, because they are the shapes real
files actually produce (dictionary-encoded string children, all-null columns, a
100k-row table).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx
from opteryx.connectors.parquet_io import pool_reader

# The purpose-built parity corpus + the pre-existing ARRAY datasets.
_ARRAY_TYPES = "testdata/flat/array_types"
_STRUCT_ARRAY = "testdata/flat/struct_array"
_TWEETS = "testdata/flat/formats/parquet"
_NVD = "testdata/nvd"
_META = "testdata/metadata"


def _drain(sql, force_trampoline, monkeypatch):
    """Drain `sql`; return (sorted row multiset, scan sources).

    Rows are compared by `repr`, which for an ARRAY column renders the whole nested
    Python structure — so a lost NULL, a shifted offset, a dropped element or a
    child left untagged (raw ints where datetimes belong) all change the result.
    Comparison is order-insensitive: a concurrent scan legitimately reorders row
    groups on either path.
    """
    if force_trampoline:
        monkeypatch.setattr(pool_reader, "native_scan_supported", lambda *a, **k: False)
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = list(morsel.column_names)
        for i in range(morsel.num_rows):
            rows.append(tuple(
                repr(None if morsel.column(n) is None else morsel.column(n)[i])
                for n in names
            ))
    src = sorted(set(session._telemetry.as_dict()["scan_sources"].values()))
    if force_trampoline:
        monkeypatch.undo()
    return tuple(sorted(rows)), src


def _assert_parity(monkeypatch, sql, *, expect_native=True):
    """Run `sql` natively and forced-trampoline in ONE process; assert identical
    rows. When `expect_native`, also assert the two runs really did take different
    Sources — otherwise the parity assertion would be comparing a path with itself."""
    nat, nat_src = _drain(sql, False, monkeypatch)
    tmp, tmp_src = _drain(sql, True, monkeypatch)
    assert nat == tmp, "native rows differ from the trampoline for: %s" % sql
    if expect_native:
        assert nat_src == ["NativeParquetScanSource"], nat_src
        assert tmp_src == ["StreamingScanSource"], tmp_src
    return nat


# ── per-element-type parity over the purpose-built corpus ────────────────────

@pytest.mark.parametrize("column", [
    "ints",     # list<int64>   — CHILD_INT64
    "strs",     # list<string>  — CHILD_STRING (inline AND arena-resident values)
    "floats",   # list<double>  — CHILD_FLOAT64
    "bools",    # list<bool>    — CHILD_BOOL (bit-packed child body)
    "stamps",   # list<timestamp[us]> — CHILD_INT64 + the ARRAY<TIMESTAMP> child retag
    "smalls",   # list<int32>   — CHILD_INT32
    "uints",    # list<uint64>  — CHILD_UINT64
    "nested",   # list<list<int64>> — CHILD_ARRAY, the recursive level
])
def test_array_element_type_parity(monkeypatch, column):
    """Every element tag the TAG_ARRAY wire format can carry, in isolation, over a
    corpus that mixes NULL lists, empty lists, NULL elements and ordinary values."""
    rows = _assert_parity(monkeypatch, "SELECT id, %s FROM '%s'" % (column, _ARRAY_TYPES))
    assert len(rows) == 12


def test_array_null_shapes_are_distinguished(monkeypatch):
    """The four null-ish shapes are NOT interchangeable — pinned by value, so a
    decoder that collapsed any pair (e.g. emitted `[]` for a NULL list, or dropped
    a NULL element and shortened the list) fails here rather than silently."""
    rows = _assert_parity(monkeypatch, "SELECT id, ints, nested FROM '%s'" % _ARRAY_TYPES)
    by_id = {eval(r[0]): (eval(r[1]), eval(r[2])) for r in rows}  # noqa: S307 — reprs of our own data
    assert by_id[2] == (None, None)                    # NULL list
    assert by_id[3] == ([], [])                        # EMPTY list
    assert by_id[4] == ([None], [None])                # one NULL element / one NULL inner list
    assert by_id[5][0] == [7, None, 9]                 # NULL element among values
    assert by_id[5][1] == [[7, None], None, []]        # nested: values / NULL inner / empty inner
    assert by_id[1][1] == [[1, 2], [3]]                # ordinary nesting


def test_array_timestamp_child_is_retagged(monkeypatch):
    """ARRAY<TIMESTAMP>: parquet stores the leaf as physical int64 and the IPC list
    format carries no logical type, so without the child retag the elements come
    back as raw micros. Parity with the trampoline (which applies exactly this retag
    via `_sp_array_ts_unit_map`) is what proves the unit survived."""
    import datetime

    rows = _assert_parity(monkeypatch, "SELECT id, stamps FROM '%s'" % _ARRAY_TYPES)
    by_id = {eval(r[0]): eval(r[1], {"datetime": datetime}) for r in rows}  # noqa: S307
    assert by_id[1] == [datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)]
    assert by_id[5][1] is None
    assert by_id[5][2] == datetime.datetime(2038, 1, 19, 3, 14, 7,
                                            tzinfo=datetime.timezone.utc)


# ── the array in company: SELECT *, mixed projections, role-3 ────────────────

@pytest.mark.parametrize("sql", [
    "SELECT * FROM '%s'" % _ARRAY_TYPES,
    "SELECT id, ints, strs, floats FROM '%s'" % _ARRAY_TYPES,
    # the array sits alongside ordinary columns in real datasets
    "SELECT * FROM testdata.astronauts",
    "SELECT name, alma_mater, missions FROM testdata.astronauts",
    "SELECT id, data FROM '%s'" % _STRUCT_ARRAY,
    "SELECT user_id, hash_tags FROM '%s'" % _TWEETS,
    "SELECT cwes, references FROM '%s'" % _NVD,
    # array<uint64> and array<byte_array> as real files produce them
    "SELECT min_k_hashes, null_counts FROM '%s'" % _META,
])
def test_array_alongside_ordinary_columns_parity(monkeypatch, sql):
    _assert_parity(monkeypatch, sql)


def test_array_role3_filter_only_parity(monkeypatch):
    """ROLE-3: the ARRAY column is in the pushed predicate's read set but is never
    emitted. The R6 guard was deliberately strict about this — a filter-only column
    had to be admissible too — so the close-out has to hold for it as well."""
    _assert_parity(monkeypatch, "SELECT id FROM '%s' WHERE ints IS NULL" % _ARRAY_TYPES)
    _assert_parity(monkeypatch, "SELECT id FROM '%s' WHERE ints IS NOT NULL" % _ARRAY_TYPES)
    _assert_parity(monkeypatch, "SELECT COUNT(*) FROM '%s' WHERE strs IS NULL" % _ARRAY_TYPES)


@pytest.mark.parametrize("sql", [
    "SELECT id, ARRAY_CONTAINS(ints, 5) FROM '%s'" % _ARRAY_TYPES,
    "SELECT id, LENGTH(strs) FROM '%s'" % _ARRAY_TYPES,
    "SELECT id, u FROM '%s' CROSS JOIN UNNEST(ints) AS u" % _ARRAY_TYPES,
    "SELECT id, s FROM '%s' CROSS JOIN UNNEST(strs) AS s" % _ARRAY_TYPES,
    "SELECT t FROM testdata.astronauts CROSS JOIN UNNEST(missions) AS t",
])
def test_array_consuming_sql_parity(monkeypatch, sql):
    """The natively-decoded vector has to survive the operators that actually read
    a list — UNNEST gathers the child through the parent's offsets, ARRAY_CONTAINS
    and LENGTH read it in place. A child owned or offset wrongly shows up here even
    when a plain projection round-trips."""
    _assert_parity(monkeypatch, sql)


# ── written-here shapes the committed corpus cannot express ──────────────────

def _write(dataset_dir, columns, **kw):
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def test_all_null_array_column_parity(tmp_path, monkeypatch):
    """Every list NULL: the parent validity bitmap is present and all-clear, and the
    child is length 0. A decoder that treated "no children" as "no validity" would
    return `[]` for every row."""
    ds = _write(str(tmp_path / "allnull"),
                {"n": (pa.int64(), list(range(20))),
                 "a": (pa.list_(pa.int64()), [None] * 20)})
    _assert_parity(monkeypatch, "SELECT n, a FROM '%s'" % ds)


def test_all_empty_array_column_parity(tmp_path, monkeypatch):
    """Every list present but empty: no validity bitmap at all, and offsets that are
    all equal — the mirror image of the all-null case."""
    ds = _write(str(tmp_path / "allempty"),
                {"n": (pa.int64(), list(range(20))),
                 "a": (pa.list_(pa.int64()), [[]] * 20)})
    _assert_parity(monkeypatch, "SELECT n, a FROM '%s'" % ds)


def test_multi_row_group_array_parity(tmp_path, monkeypatch):
    """Offsets are per-row-group, so a decoder that leaked state across row groups
    (or rebased offsets wrongly) only shows up with more than one."""
    ds = _write(str(tmp_path / "multirg"),
                {"n": (pa.int64(), list(range(300))),
                 "a": (pa.list_(pa.int64()),
                       [None if i % 7 == 0 else ([] if i % 5 == 0 else [i, i + 1, None])
                        for i in range(300)])},
                row_group_size=64)
    rows = _assert_parity(monkeypatch, "SELECT n, a FROM '%s'" % ds)
    assert len(rows) == 300


def test_long_string_elements_parity(tmp_path, monkeypatch):
    """String elements longer than STR_INLINE_MAX (12 bytes) live in the arena and
    are addressed by a byte OFFSET, not a pointer — the consolidated block has to
    carry them, and the offsets have to stay valid after the copy."""
    ds = _write(str(tmp_path / "longstr"),
                {"n": (pa.int64(), list(range(50))),
                 "a": (pa.list_(pa.string()),
                       [["short", "x" * 200, None, ""] if i % 3 else None for i in range(50)])})
    _assert_parity(monkeypatch, "SELECT n, a FROM '%s'" % ds)


def test_deeply_nested_array_parity(tmp_path, monkeypatch):
    """Three levels: the recursive CHILD_ARRAY path has to chain ownership all the
    way down and keep each level's own validity."""
    ds = _write(str(tmp_path / "deep"),
                {"n": (pa.int64(), [1, 2, 3, 4]),
                 "a": (pa.list_(pa.list_(pa.list_(pa.int64()))),
                       [[[[1, 2], [3]], [[4]]], None, [], [[None, []], None]])})
    _assert_parity(monkeypatch, "SELECT n, a FROM '%s'" % ds)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
