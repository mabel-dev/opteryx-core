"""R6 — admit ARRAY (parquet LIST) columns to the native parquet scan.

ARRAY was the last observed `non_admissible_kind` residual: a plain `SELECT *` over
`testdata.astronauts` or `testdata/flat/formats/parquet` used to be refused by the
native scan because one column was a list.

A list column always lands DK_POOL — rugo's `direct_kind_for` routes any column
with repetition levels to the pool, regardless of encoding — and is serialized as
TAG_ARRAY (11) by `ipc_serialize.hpp::serialize_list_column`.
`native_array_pool_decode.hpp` is the PyObject-free consumer of that blob, and
`array_columns` — a plan-time flag parallel to column_names, the same mechanism as
`decimal_columns` / `varchar_columns` — is what tells the Source which decoder owns
a given pool blob.

The correctness gate is an INDEPENDENT plain-Python oracle (the Python per-morsel
scan that used to serve as the A/B baseline was deleted, ruling 2026-10-03):

  * `testdata/flat/array_types` — the oracle is `ROWS` in
    dev/generate_array_testdata.py, the exact Python values the corpus was written
    from; WHERE / `= ANY` / LENGTH / UNNEST are evaluated over them in Python.
  * the files written in this module — the oracle is the Python list written.
  * the pre-existing datasets — the oracle is the same columns read straight from
    the file by `rugo.parquet.read_parquet`, which shares the producer but not the
    native Source's array consumer under test.

The null-ish shapes are DIFFERENT things and all must be right:

  * a NULL list          (`None`)              — parent validity bit clear
  * an EMPTY list        (`[]`)                — offsets[i] == offsets[i+1]
  * a list of NULLs      (`[None]`)            — child validity bit clear
  * a NULL nested list   (`[[7], None, []]`)   — inner level's own validity

Every query must also select NativeParquetScanSource (a refused scan raises).
"""

import glob
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../..", "dev"))

import pyarrow as pa  # test-only dep (allowed in tests/) — WRITER only
import pyarrow.parquet as pq
import pytest
import rugo.parquet

import opteryx
from generate_array_testdata import ROWS  # dev/generate_array_testdata.py

# The purpose-built corpus + the pre-existing ARRAY datasets.
_ARRAY_TYPES = "testdata/flat/array_types"
_STRUCT_ARRAY = "testdata/flat/struct_array"
_TWEETS = "testdata/flat/formats/parquet"
_NVD = "testdata/nvd"
_META = "testdata/metadata"
_ASTRONAUTS = "testdata/astronauts"

#: Column order of dev/generate_array_testdata.py's ROWS tuples (== its SCHEMA).
_CORPUS_COLUMNS = ("id", "ints", "strs", "floats", "bools", "stamps", "nested",
                   "smalls", "uints")


def _corpus(*columns):
    """The array_types corpus as plain Python rows, projected to `columns`."""
    idx = [_CORPUS_COLUMNS.index(c) for c in columns]
    return [tuple(row[i] for i in idx) for row in ROWS]


def _drain(sql):
    """Drain `sql` natively; return the result rows as Python tuples.

    Asserts the scan selected NativeParquetScanSource and recorded no residual
    reason — a parity check against a scan that did not run natively proves
    nothing about the native decoder."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = list(morsel.column_names)
        for i in range(morsel.num_rows):
            rows.append(tuple(morsel.column(n)[i] for n in names))
    telemetry = session.telemetry
    sources = sorted(set(telemetry["scan_sources"].values()))
    assert sources == ["NativeParquetScanSource"], sources
    return rows


def _canonical(value):
    """Apply draken's documented ingest canonicalisation to an oracle value:
    -0.0 → +0.0 (draken/draken_native.cpp: "Values are canonicalized at ingestion").
    Recurses through lists; every other value is returned unchanged."""
    if type(value) is list:
        return [_canonical(v) for v in value]
    if type(value) is float and value == 0.0:
        return 0.0
    return value


def _same_rows(actual, expected, sql):
    """Order-insensitive multiset comparison by `repr`, which for an ARRAY renders
    the whole nested Python structure — so a lost NULL, a shifted offset, a dropped
    element or a child left untagged (raw ints where datetimes
    belong) all fail. A concurrent scan legitimately reorders row groups.
    The oracle side is passed through `_canonical` first."""
    expected = [tuple(_canonical(v) for v in row) for row in expected]
    assert sorted(map(repr, actual)) == sorted(map(repr, expected)), (
        "native rows differ from the Python oracle for: %s\n actual=%r\n expected=%r"
        % (sql, sorted(actual, key=repr), sorted(expected, key=repr)))


def _rugo_rows(folder, columns=None):
    """Oracle for a pre-existing dataset: `columns` read straight from every file
    in `folder` with rugo. Returns (column names, rows)."""
    names = None
    rows = []
    for path in sorted(glob.glob(os.path.join(folder, "*.parquet"))):
        with rugo.parquet.read_parquet(path, columns=columns) as reader:
            for morsel in reader:
                file_names = list(morsel.column_names)
                assert names is None or names == file_names, (names, file_names)
                names = file_names
                rows.extend(
                    tuple(morsel.column(n)[i] for n in file_names)
                    for i in range(morsel.num_rows))
    assert names is not None, "no parquet files under %s" % folder
    return names, rows


# ── per-element-type correctness over the purpose-built corpus ───────────────

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
def test_array_element_type_matches_oracle(column):
    """Every element tag the TAG_ARRAY wire format can carry, in isolation, over a
    corpus that mixes NULL lists, empty lists, NULL elements and ordinary values."""
    sql = "SELECT id, %s FROM '%s'" % (column, _ARRAY_TYPES)
    rows = _drain(sql)
    _same_rows(rows, _corpus("id", column), sql)
    assert len(rows) == 12


def test_array_null_shapes_are_distinguished():
    """The four null-ish shapes are NOT interchangeable — pinned by value, so a
    decoder that collapsed any pair (e.g. emitted `[]` for a NULL list, or dropped
    a NULL element and shortened the list) fails here rather than silently."""
    sql = "SELECT id, ints, nested FROM '%s'" % _ARRAY_TYPES
    rows = _drain(sql)
    _same_rows(rows, _corpus("id", "ints", "nested"), sql)
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id[2] == (None, None)                    # NULL list
    assert by_id[3] == ([], [])                        # EMPTY list
    assert by_id[4] == ([None], [None])                # one NULL element / one NULL inner list
    assert by_id[5][0] == [7, None, 9]                 # NULL element among values
    assert by_id[5][1] == [[7, None], None, []]        # nested: values / NULL inner / empty inner
    assert by_id[1][1] == [[1, 2], [3]]                # ordinary nesting


def test_array_timestamp_child_is_retagged():
    """ARRAY<TIMESTAMP>: parquet stores the leaf as physical int64 and the IPC list
    format carries no logical type, so without the child retag the elements come
    back as raw micros. The oracle is the UTC datetimes the corpus was written
    from — what proves the unit survived."""
    import datetime

    sql = "SELECT id, stamps FROM '%s'" % _ARRAY_TYPES
    rows = _drain(sql)
    _same_rows(rows, _corpus("id", "stamps"), sql)
    by_id = dict(rows)
    assert by_id[1] == [datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)]
    assert by_id[5][1] is None
    assert by_id[5][2] == datetime.datetime(2038, 1, 19, 3, 14, 7,
                                            tzinfo=datetime.timezone.utc)


# ── the array in company: SELECT *, mixed projections, role-3 ────────────────

@pytest.mark.parametrize("columns", [
    None,  # SELECT *
    ("id", "ints", "strs", "floats"),
])
def test_array_corpus_projection_matches_oracle(columns):
    projection = "*" if columns is None else ", ".join(columns)
    sql = "SELECT %s FROM '%s'" % (projection, _ARRAY_TYPES)
    _same_rows(_drain(sql), _corpus(*(columns or _CORPUS_COLUMNS)), sql)


# (dataset, projected columns or None for SELECT *) — the array sits alongside
# ordinary columns in real datasets; array<uint64> and array<byte_array> as real
# files produce them come from testdata/metadata.
@pytest.mark.parametrize("folder,columns", [
    (_ASTRONAUTS, None),
    (_ASTRONAUTS, ("name", "alma_mater", "missions")),
    (_STRUCT_ARRAY, ("id", "data")),
    (_TWEETS, ("user_id", "hash_tags")),
    (_NVD, ("cwes", "references")),
    (_META, ("min_k_hashes", "null_counts")),
])
def test_array_alongside_ordinary_columns_matches_rugo(folder, columns):
    projection = "*" if columns is None else ", ".join(columns)
    sql = "SELECT %s FROM '%s'" % (projection, folder)
    names, expected = _rugo_rows(folder, None if columns is None else list(columns))
    _same_rows(_drain(sql), expected, sql)
    assert len(expected) > 0, "oracle read nothing - not a meaningful comparison"
    if columns is not None:
        assert names == [c.encode() for c in columns], names  # rugo names are bytes


def test_array_role3_filter_only_matches_oracle():
    """ROLE-3: the ARRAY column is in the pushed predicate's read set but is never
    emitted. The R6 guard was deliberately strict about this — a filter-only column
    had to be admissible too — so the close-out has to hold for it as well."""
    sql = "SELECT id FROM '%s' WHERE ints IS NULL" % _ARRAY_TYPES
    _same_rows(_drain(sql), [(i,) for i, ints in _corpus("id", "ints") if ints is None], sql)

    sql = "SELECT id FROM '%s' WHERE ints IS NOT NULL" % _ARRAY_TYPES
    _same_rows(_drain(sql),
               [(i,) for i, ints in _corpus("id", "ints") if ints is not None], sql)

    sql = "SELECT COUNT(*) FROM '%s' WHERE strs IS NULL" % _ARRAY_TYPES
    _same_rows(_drain(sql), [(sum(1 for (s,) in _corpus("strs") if s is None),)], sql)


def _any_eq(needle, values):
    """`needle = ANY(values)`: a NULL list yields NULL; otherwise TRUE iff a
    non-NULL element equals `needle`, else FALSE. A NULL ELEMENT does not make the
    answer NULL (ruled 2026-10-05): the comparison is against an array that holds a
    NULL, not against NULL, so `5 = ANY([7, NULL, 9])` is FALSE."""
    if values is None:
        return None
    return any(v == needle for v in values if v is not None)


def test_any_over_an_array_holding_a_null_element():
    """The `= ANY` NULL-element ruling (2026-10-05), stated as literals rather than
    through `_any_eq`: corpus row 5 is `[7, NULL, 9]`, row 4 is `[NULL]`, row 2 is a
    NULL list. A miss over an array holding a NULL is FALSE, a hit is TRUE, and only
    a NULL list yields NULL."""
    rows = _drain("SELECT id, 5 = ANY(ints), 7 = ANY(ints) FROM '%s' WHERE id IN (2, 4, 5)"
                  % _ARRAY_TYPES)
    assert sorted(rows) == [(2, None, None), (4, False, False), (5, False, True)]


@pytest.mark.parametrize("sql,oracle", [
    ("SELECT id, 5 = ANY(ints) FROM '%s'" % _ARRAY_TYPES,
     lambda: [(i, _any_eq(5, ints)) for i, ints in _corpus("id", "ints")]),
    ("SELECT id, LENGTH(strs) FROM '%s'" % _ARRAY_TYPES,
     lambda: [(i, None if s is None else len(s)) for i, s in _corpus("id", "strs")]),
    ("SELECT id, u FROM '%s' CROSS JOIN UNNEST(ints) AS u" % _ARRAY_TYPES,
     lambda: [(i, u) for i, ints in _corpus("id", "ints") for u in (ints or [])]),
    ("SELECT id, s FROM '%s' CROSS JOIN UNNEST(strs) AS s" % _ARRAY_TYPES,
     lambda: [(i, s) for i, strs in _corpus("id", "strs") for s in (strs or [])]),
    ("SELECT t FROM testdata.astronauts CROSS JOIN UNNEST(missions) AS t",
     lambda: [(t,) for (m,) in _rugo_rows(_ASTRONAUTS, ["missions"])[1] for t in (m or [])]),
], ids=["any", "length", "unnest_ints", "unnest_strs", "unnest_astronauts"])
def test_array_consuming_sql_matches_oracle(sql, oracle):
    """The natively-decoded vector has to survive the operators that actually read
    a list — UNNEST gathers the child through the parent's offsets, `= ANY`
    and LENGTH read it in place. A child owned or offset wrongly shows up here even
    when a plain projection round-trips."""
    _same_rows(_drain(sql), oracle(), sql)


# ── written-here shapes the committed corpus cannot express ──────────────────

def _write(dataset_dir, columns, **kw):
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"), **kw)
    return dataset_dir


def _assert_written(dataset_dir, columns, **kw):
    """Write `columns`, read them back natively, compare against the Python lists
    that were written."""
    ds = _write(dataset_dir, columns, **kw)
    names = list(columns)
    sql = "SELECT %s FROM '%s'" % (", ".join(names), ds)
    expected = list(zip(*(vals for _, vals in columns.values())))
    rows = _drain(sql)
    _same_rows(rows, expected, sql)
    return rows


def test_all_null_array_column(tmp_path):
    """Every list NULL: the parent validity bitmap is present and all-clear, and the
    child is length 0. A decoder that treated "no children" as "no validity" would
    return `[]` for every row."""
    _assert_written(str(tmp_path / "allnull"),
                    {"n": (pa.int64(), list(range(20))),
                     "a": (pa.list_(pa.int64()), [None] * 20)})


def test_all_empty_array_column(tmp_path):
    """Every list present but empty: no validity bitmap at all, and offsets that are
    all equal — the mirror image of the all-null case."""
    _assert_written(str(tmp_path / "allempty"),
                    {"n": (pa.int64(), list(range(20))),
                     "a": (pa.list_(pa.int64()), [[]] * 20)})


def test_multi_row_group_array(tmp_path):
    """Offsets are per-row-group, so a decoder that leaked state across row groups
    (or rebased offsets wrongly) only shows up with more than one."""
    rows = _assert_written(
        str(tmp_path / "multirg"),
        {"n": (pa.int64(), list(range(300))),
         "a": (pa.list_(pa.int64()),
               [None if i % 7 == 0 else ([] if i % 5 == 0 else [i, i + 1, None])
                for i in range(300)])},
        row_group_size=64)
    assert len(rows) == 300


def test_long_string_elements(tmp_path):
    """String elements longer than STR_INLINE_MAX (12 bytes) live in the arena and
    are addressed by a byte OFFSET, not a pointer — the consolidated block has to
    carry them, and the offsets have to stay valid after the copy."""
    _assert_written(str(tmp_path / "longstr"),
                    {"n": (pa.int64(), list(range(50))),
                     "a": (pa.list_(pa.string()),
                           [["short", "x" * 200, None, ""] if i % 3 else None
                            for i in range(50)])})


def test_deeply_nested_array(tmp_path):
    """Three levels: the recursive CHILD_ARRAY path has to chain ownership all the
    way down and keep each level's own validity."""
    _assert_written(str(tmp_path / "deep"),
                    {"n": (pa.int64(), [1, 2, 3, 4]),
                     "a": (pa.list_(pa.list_(pa.list_(pa.int64()))),
                           [[[[1, 2], [3]], [[4]]], None, [], [[None, []], None]])})


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
