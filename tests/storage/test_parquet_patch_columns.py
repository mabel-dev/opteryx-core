# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""rugo.parquet.patch_columns - drop/rename columns by editing only the footer.

The property that matters and is asserted directly here: a surviving column's
encoded pages are COPIED, never decoded and re-encoded. A decode-and-rewrite
implementation returns identical VALUES and different BYTES, so the value
assertions alone cannot tell the two apart - `test_rename_is_byte_identical`
can, and is the reason this file exists.

These exercise the native patcher on its own. The SQL surface that will drive it
(ALTER TABLE ... DROP/RENAME COLUMN) is covered in test_ddl_column_operations.py.
"""

import glob

import pytest

import opteryx
import rugo.parquet as rp
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector

# Types that INSERT ... VALUES can populate; see test_ddl_column_operations.py
# for why the parameterized/temporal types are absent (an insert-path gap, not
# a patcher one - the patcher is type-agnostic by construction).
_COLUMNS = [
    ("i8", "INT8", "12"),
    ("i16", "INT16", "300"),
    ("i32", "INT32", "70000"),
    ("i64", "INT64", "5000000000"),
    ("s", "VARCHAR", "'hello'"),
    ("b", "BOOL", "TRUE"),
    ("bin", "VARBINARY", "b'abc'"),
]
_NAMES = [name for name, _, _ in _COLUMNS]


def _page_region(raw: bytes) -> bytes:
    """The encoded pages, and nothing else.

    Parquet's trailer is self-describing - [PAR1][pages][footer][u32 len][PAR1] -
    so the boundary is found without decoding anything, which is the point: this
    helper must not lean on the machinery it is used to check.
    """
    assert raw[:4] == b"PAR1" and raw[-4:] == b"PAR1", "not a parquet file"
    footer_len = int.from_bytes(raw[-8:-4], "little")
    return raw[4 : len(raw) - 8 - footer_len]


def _rows(path):
    out = []
    with rp.read_parquet(path) as reader:
        for morsel in reader:
            pydict = morsel.to_arrow().to_pydict()
            n = len(next(iter(pydict.values())))
            for i in range(n):
                out.append({k: v[i] for k, v in pydict.items()})
    return out


@pytest.fixture
def sample(tmp_path):
    """A real parquet file with one column per insertable type, written by the
    same writer production uses - not a hand-rolled fixture that might not look
    like what the engine actually stores."""
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    cols = ", ".join(f"{n} {t}" for n, t, _ in _COLUMNS)
    vals = ", ".join(v for _, _, v in _COLUMNS)
    list(session.execute_to_morsels(f"CREATE TABLE ws.t ({cols})"))
    list(session.execute_to_morsels(f"INSERT INTO ws.t VALUES ({vals}), ({vals})"))
    path = [p for p in glob.glob(str(tmp_path / "ws" / "t" / "*.parquet")) if "manifest" not in p][0]
    return tmp_path, path, open(path, "rb").read()


def _write(tmp_path, name, raw):
    path = tmp_path / name
    path.write_bytes(raw)
    return str(path)


# --- the load-bearing property -------------------------------------------------


@pytest.mark.parametrize("column", _NAMES)
def test_rename_is_byte_identical(sample, column):
    """A rename touches no data at all: every encoded page comes out
    bit-for-bit identical, whichever column is renamed and whatever it holds."""
    _, _, src = sample

    out = rp.patch_columns(src, rename={column: column + "_renamed"})

    assert _page_region(out) == _page_region(src)


def test_a_same_length_rename_changes_only_the_name(sample):
    """Not merely the same bytes - the same bytes in the same PLACES. The writer
    puts each bloom filter immediately before its own data page so a reader can
    fetch bloom+data in one range read; re-emitting them in column order instead
    of source order would keep the file correct and quietly destroy that.

    A same-length new name keeps the footer the same size too, so the whole FILE
    stays the same length and differs only where the name is spelled.
    """
    _, _, src = sample

    out = rp.patch_columns(src, rename={"i64": "x64"})

    assert len(out) == len(src)
    assert _page_region(out) == _page_region(src)
    differing = sum(1 for a, b in zip(out, src) if a != b)
    assert differing <= len("i64"), f"{differing} bytes changed for a 3-character rename"


def test_a_longer_rename_grows_only_the_footer(sample):
    """The pages are untouched, so a longer column name can only make the FOOTER
    bigger - by the extra characters, which parquet stores twice (the schema
    element and the chunk's path_in_schema)."""
    _, _, src = sample

    out = rp.patch_columns(src, rename={"i64": "renamed"})

    assert _page_region(out) == _page_region(src)
    assert len(out) - len(src) == 2 * (len("renamed") - len("i64"))


def test_drop_last_column_leaves_a_prefix(sample):
    """Dropping the final column of a single-row-group file leaves the earlier
    chunks exactly where they were, so the new page region is a byte-for-byte
    PREFIX of the old one. (Only for one row group - with several, the dropped
    chunks sit in the middle of the file and a prefix is not expected.)"""
    _, _, src = sample

    out = rp.patch_columns(src, drop=[_NAMES[-1]])

    region_before, region_after = _page_region(src), _page_region(out)
    assert len(region_after) < len(region_before)
    assert region_before.startswith(region_after)


# --- values survive ------------------------------------------------------------


@pytest.mark.parametrize("victim", _NAMES)
def test_drop_one_column_keeps_the_others_exact(sample, victim):
    tmp_path, path, src = sample
    before = _rows(path)[0]

    out = rp.patch_columns(src, drop=[victim])

    after = _rows(_write(tmp_path, "dropped.parquet", out))[0]
    assert set(after) == set(before) - {victim}
    assert all(after[k] == before[k] for k in after)


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5, 6])
def test_drop_n_of_seven_columns(sample, count):
    tmp_path, path, src = sample
    before = _rows(path)[0]
    victims = _NAMES[:count]

    out = rp.patch_columns(src, drop=victims)

    after = _rows(_write(tmp_path, "dropped.parquet", out))[0]
    assert after == {k: v for k, v in before.items() if k not in victims}


def test_drop_all_but_one_column(sample):
    tmp_path, path, src = sample
    before = _rows(path)[0]

    out = rp.patch_columns(src, drop=_NAMES[:-1])

    after = _rows(_write(tmp_path, "one.parquet", out))[0]
    assert after == {_NAMES[-1]: before[_NAMES[-1]]}


@pytest.mark.parametrize("column", _NAMES)
def test_rename_keeps_the_values(sample, column):
    tmp_path, path, src = sample
    before = _rows(path)[0]

    out = rp.patch_columns(src, rename={column: "moved"})

    after = _rows(_write(tmp_path, "renamed.parquet", out))[0]
    assert after["moved"] == before[column]
    assert column not in after


def test_rename_every_column_at_once(sample):
    tmp_path, path, src = sample
    before = _rows(path)[0]

    out = rp.patch_columns(src, rename={n: n.upper() for n in _NAMES})

    after = _rows(_write(tmp_path, "allrenamed.parquet", out))[0]
    assert after == {k.upper(): v for k, v in before.items()}
    assert _page_region(out) == _page_region(src)


def test_drop_and_rename_together(sample):
    tmp_path, path, src = sample
    before = _rows(path)[0]

    out = rp.patch_columns(src, drop=["i8", "s"], rename={"b": "flag"})

    after = _rows(_write(tmp_path, "both.parquet", out))[0]
    expected = {("flag" if k == "b" else k): v for k, v in before.items() if k not in ("i8", "s")}
    assert after == expected


def test_rename_round_trip_restores_the_original_file(sample):
    """a -> b -> a returns the identical bytes, footer included: a rename
    carries no residue."""
    _, _, src = sample

    once = rp.patch_columns(src, rename={"i64": "temp"})
    back = rp.patch_columns(once, rename={"temp": "i64"})

    assert back == src


# --- more than one row group ---------------------------------------------------


@pytest.fixture
def multi_row_group(sample, tmp_path):
    """The same data rewritten across several row groups. The patcher walks row
    groups, so a single-row-group-only test would leave that loop unexercised."""
    _, path, _ = sample
    with rp.read_parquet(path) as reader:
        morsel = next(iter(reader))
    raw = rp.write_parquet(morsel, max_rows_per_row_group=1)
    return _write(tmp_path, "multi.parquet", raw), raw


def test_multi_row_group_rename_is_byte_identical(multi_row_group):
    path, raw = multi_row_group
    assert rp.read_metadata(path).num_rows == 2

    out = rp.patch_columns(raw, rename={"i64": "renamed"})

    assert _page_region(out) == _page_region(raw)


def test_multi_row_group_drop_keeps_every_row(multi_row_group, tmp_path):
    path, raw = multi_row_group
    before = _rows(path)

    out = rp.patch_columns(raw, drop=["i8", "i16", "i32", "s", "b"])

    after = _rows(_write(tmp_path, "multi_dropped.parquet", out))
    assert len(after) == len(before)
    assert [r["i64"] for r in after] == [r["i64"] for r in before]
    assert [r["bin"] for r in after] == [r["bin"] for r in before]
    assert all(set(r) == {"i64", "bin"} for r in after)


# --- refusals ------------------------------------------------------------------


def test_dropping_an_absent_column_is_refused(sample):
    """The caller believes something about this file that is not true; doing
    nothing would leave it believing that."""
    _, _, src = sample

    with pytest.raises(RuntimeError, match="no column named"):
        rp.patch_columns(src, drop=["nope"])


def test_renaming_an_absent_column_is_refused(sample):
    _, _, src = sample

    with pytest.raises(RuntimeError, match="no column named"):
        rp.patch_columns(src, rename={"nope": "other"})


def test_dropping_every_column_is_refused(sample):
    _, _, src = sample

    with pytest.raises(RuntimeError, match="no relation"):
        rp.patch_columns(src, drop=_NAMES)


def test_empty_source_is_refused():
    with pytest.raises((ValueError, RuntimeError)):
        rp.patch_columns(b"")


def test_a_non_parquet_source_is_refused():
    with pytest.raises(RuntimeError):
        rp.patch_columns(b"this is not a parquet file, not even close")


def test_no_op_patch_returns_an_equivalent_file(sample):
    """Neither dropping nor renaming anything still has to produce a valid file
    with the same pages - the identity case must not be a special path that
    quietly does something else."""
    tmp_path, path, src = sample

    out = rp.patch_columns(src)

    assert _page_region(out) == _page_region(src)
    assert _rows(_write(tmp_path, "noop.parquet", out)) == _rows(path)


# --- adding columns ------------------------------------------------------------
#
# An ADDed column has no pages to copy, so its chunk is synthesised from a
# DONOR: a one-column, one-row file written by rugo's own writer, carrying the
# new column's name/type and the value existing rows should be filled with.
# The donor exists so the added column is annotated by exactly the code that
# writes that type normally - these tests pin both halves of that: the value
# reads back correctly, and the columns that were already there are untouched.


def _donor(name, value, sql_type):
    """A donor built the way `build_column_donor` builds one, via the SQL
    surface rather than by hand, so the shape under test is the shape the
    connector actually produces."""
    from opteryx.connectors.capabilities.writable import build_column_donor
    from opteryx.planner.logical_planner.logical_planner_builders import column_type_from_ast
    from opteryx.third_party.sqloxide import parse_sql

    ast = parse_sql(f"CREATE TABLE t (c {sql_type})", "opteryx")[0]
    column_type = column_type_from_ast(ast["CreateTable"]["columns"][0])
    return build_column_donor(name, column_type, value)


@pytest.mark.parametrize(
    "sql_type,value",
    [
        ("INT8", 12),
        ("INT16", 300),
        ("INT32", 70000),
        ("INT64", 5000000000),
        ("VARCHAR", "backfilled"),
        ("BOOL", True),
        ("FLOAT64", 1.5),
        ("DECIMAL(10,2)", 3.14),
        ("DECIMAL(38,18)", 2.5),
    ],
)
def test_add_column_backfills_a_literal(sample, tmp_path, sql_type, value):
    """One value, repeated for every existing row, for every type a donor can
    describe - including the FLBA-backed decimals, whose on-disk big-endian
    form has to be reversed back on the way in."""
    _, path, src = sample

    out = rp.patch_columns(src, add=[_donor("extra", value, sql_type)])

    rows = _rows(_write(tmp_path, "added.parquet", out))
    assert len(rows) == len(_rows(path))
    distinct = {r["extra"] for r in rows}
    assert len(distinct) == 1, f"the fill value should be the same on every row, got {distinct}"
    (got,) = distinct
    if isinstance(value, float):
        # DECIMAL reads back as Decimal; the literal it was built from is a float
        assert float(got) == pytest.approx(value)
    else:
        assert got == value


@pytest.mark.parametrize("sql_type", ["INT64", "VARCHAR", "BOOL", "FLOAT64", "DECIMAL(10,2)"])
def test_add_column_with_no_default_backfills_null(sample, tmp_path, sql_type):
    _, _, src = sample

    out = rp.patch_columns(src, add=[_donor("extra", None, sql_type)])

    rows = _rows(_write(tmp_path, "added_null.parquet", out))
    assert [r["extra"] for r in rows] == [None] * len(rows)


def test_add_column_copies_the_existing_pages_verbatim(sample):
    """The added column's chunks go AFTER everything copied, so the existing
    page region is not merely equivalent - it is a byte-for-byte prefix of the
    new file's. Nothing that was already there was decoded."""
    _, _, src = sample

    out = rp.patch_columns(src, add=[_donor("extra", 1, "INT64")])

    before = _page_region(src)
    assert _page_region(out)[: len(before)] == before


def test_add_column_costs_almost_nothing_however_many_rows(tmp_path):
    """The whole point of a repeated value: the file grows by a constant, not
    by bytes-per-row. Ten thousand rows, one added column, well under a
    kilobyte - a PLAIN per-row encoding would cost eighty."""
    register_workspace("ws_big", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws_big.t (id INT64)"))
    values = ", ".join(f"({i})" for i in range(10000))
    list(session.execute_to_morsels(f"INSERT INTO ws_big.t VALUES {values}"))
    path = [
        p for p in glob.glob(str(tmp_path / "ws_big" / "t" / "*.parquet")) if "manifest" not in p
    ][0]
    src = open(path, "rb").read()

    literal = rp.patch_columns(src, add=[_donor("extra", 7, "INT64")])
    nulls = rp.patch_columns(src, add=[_donor("extra", None, "INT64")])

    assert len(literal) - len(src) < 1000, "a repeated literal should not cost per-row bytes"
    assert len(nulls) - len(src) < 1000, "an all-NULL column should not cost per-row bytes"
    rows = _rows(_write(tmp_path, "big.parquet", literal))
    assert len(rows) == 10000
    assert {r["extra"] for r in rows} == {7}


def test_add_many_columns_at_once(sample, tmp_path):
    _, path, src = sample

    donors = [_donor(f"a{i:02d}", i, "INT64") for i in range(18)]
    out = rp.patch_columns(src, add=donors)

    rows = _rows(_write(tmp_path, "many.parquet", out))
    assert len(rows) == len(_rows(path))
    for i in range(18):
        assert {r[f"a{i:02d}"] for r in rows} == {i}
    for name in _NAMES:
        assert name in rows[0]


def test_add_drop_and_rename_together(sample, tmp_path):
    """One pass, all three edits - the added chunk must be placed after the
    copied ones and the footer must agree with where everything landed."""
    _, _, src = sample

    out = rp.patch_columns(
        src, drop=["b"], rename={"i64": "big"}, add=[_donor("flag", True, "BOOL")]
    )

    rows = _rows(_write(tmp_path, "combined.parquet", out))
    assert "b" not in rows[0]
    assert "i64" not in rows[0]
    assert rows[0]["big"] == 5000000000
    assert {r["flag"] for r in rows} == {True}


def test_adding_a_name_that_already_exists_is_refused(sample):
    _, _, src = sample

    with pytest.raises(RuntimeError, match="two columns named"):
        rp.patch_columns(src, add=[_donor("i64", 1, "INT64")])


def test_adding_over_a_dropped_name_is_allowed(sample, tmp_path):
    """Dropping `s` and adding a new `s` in one statement is not a collision -
    the old column is gone from the result before the new one is placed."""
    _, _, src = sample

    out = rp.patch_columns(src, drop=["s"], add=[_donor("s", "replaced", "VARCHAR")])

    rows = _rows(_write(tmp_path, "replaced.parquet", out))
    assert {r["s"] for r in rows} == {"replaced"}


def test_a_donor_that_is_not_parquet_is_refused(sample):
    _, _, src = sample

    with pytest.raises(RuntimeError):
        rp.patch_columns(src, add=[b"not a parquet file"])


def test_a_multi_column_donor_is_refused(sample):
    """A donor describes exactly one column. Two would leave the patcher
    guessing which one was meant."""
    _, _, src = sample

    with pytest.raises(RuntimeError, match="exactly one column"):
        rp.patch_columns(src, add=[src])


@pytest.mark.parametrize("sql_type", ["UINT8", "UINT16", "UINT32", "UINT64"])
def test_adding_an_unsigned_column_to_a_populated_relation_refuses(sample, sql_type):
    """RECORDED, not endorsed: `draken.interop.vector_sequence`'s dispatch table
    has no UINT8/16/32/64 entry, although `vector_uint*_from_sequence` all
    exist in draken_native. So a donor for an unsigned column cannot be built
    and the ADD fails loudly.

    The patcher itself is fine with unsigned columns - it carries the
    INTEGER(bitWidth, isSigned) annotation like any other - so this is an
    ingestion gap upstream of it, not a patcher limitation. Adding an unsigned
    column to an EMPTY relation needs no donor and works today; this is what
    will notice when the dispatch table gains those entries.
    """
    _, _, src = sample

    with pytest.raises(ValueError, match="unsupported dtype name"):
        rp.patch_columns(src, add=[_donor("extra", 1, sql_type)])


# --- retyping columns ----------------------------------------------------------
#
# A widening reaches the patcher as a donor carrying the TARGET annotation. Most
# of the lattice costs nothing on disk, because parquet has no physical int8 or
# int16 (all three ride physical int32) and rugo already writes FLOAT32 as
# float64 - so the bytes already are what the new annotation says, and the pages
# are copied verbatim. Only a widening to INT64 changes the physical type, and
# then that ONE column is decoded and re-encoded.


def _physical(raw, name):
    for c in rp.read_metadata(raw).schema_columns:
        if c.name == name:
            return c.physical_type, c.logical_type
    raise AssertionError(f"no column {name!r}")


@pytest.mark.parametrize("target", ["INT16", "INT32"])
def test_annotation_only_widen_is_byte_identical(sample, target):
    """INT8 -> INT16/INT32 is an annotation change over unchanged bytes, so it
    has to cost exactly what a rename costs: nothing."""
    _, _, src = sample

    out = rp.patch_columns(src, retype={"i8": _donor("i8", None, target)})

    assert _page_region(out) == _page_region(src)
    assert _physical(out, "i8") == ("int32", target.lower())
    assert _physical(src, "i8") == ("int32", "int8")


@pytest.mark.parametrize("start", ["i8", "i16", "i32"])
def test_widen_to_int64_changes_the_physical_type(sample, tmp_path, start):
    _, path, src = sample
    before = _rows(path)

    out = rp.patch_columns(src, retype={start: _donor(start, None, "INT64")})

    assert _physical(out, start) == ("int64", "int64")
    after = _rows(_write(tmp_path, f"widened_{start}.parquet", out))
    assert [r[start] for r in after] == [r[start] for r in before]


def test_widen_to_int64_leaves_every_other_column_untouched(sample, tmp_path):
    """The point of doing this in the patcher at all: only the retyped column is
    decoded. Every other column reads back exactly as before."""
    _, path, src = sample
    before = _rows(path)

    out = rp.patch_columns(src, retype={"i16": _donor("i16", None, "INT64")})

    after = _rows(_write(tmp_path, "one_widened.parquet", out))
    for name in _NAMES:
        if name == "i16":
            continue
        assert [r[name] for r in after] == [r[name] for r in before], name


def test_widen_preserves_nulls_and_negatives_at_scale(tmp_path):
    """Enough rows to exercise the dictionary and multi-page paths, with nulls
    and negative values - a sign-extension slip on the unsigned reinterpretation
    or an off-by-one between present-index and row-index shows up here and
    nowhere in a two-row fixture."""
    register_workspace("ws_wide", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws_wide.t (id INT64, v INT16)"))
    values = ", ".join(
        f"({i}, {'NULL' if i % 7 == 0 else (i % 600) - 300})" for i in range(5000)
    )
    list(session.execute_to_morsels(f"INSERT INTO ws_wide.t VALUES {values}"))
    path = [
        p for p in glob.glob(str(tmp_path / "ws_wide" / "t" / "*.parquet")) if "manifest" not in p
    ][0]
    src = open(path, "rb").read()
    before = _rows(path)

    out = rp.patch_columns(src, retype={"v": _donor("v", None, "INT64")})

    after = _rows(_write(tmp_path, "wide.parquet", out))
    assert [r["v"] for r in after] == [r["v"] for r in before]
    assert sum(1 for r in after if r["v"] is None) == sum(1 for r in before if r["v"] is None) > 0
    assert min(r["v"] for r in after if r["v"] is not None) == -300


def test_retype_composes_with_the_other_edits(sample, tmp_path):
    _, path, src = sample
    before = _rows(path)

    out = rp.patch_columns(
        src,
        drop=["b"],
        rename={"s": "label"},
        retype={"i16": _donor("i16", None, "INT64")},
        add=[_donor("flag", True, "BOOL")],
    )

    after = _rows(_write(tmp_path, "everything.parquet", out))
    assert "b" not in after[0]
    assert after[0]["label"] == before[0]["s"]
    assert [r["i16"] for r in after] == [r["i16"] for r in before]
    assert _physical(out, "i16") == ("int64", "int64")
    assert {r["flag"] for r in after} == {True}


def test_retyping_an_absent_column_is_refused(sample):
    _, _, src = sample

    with pytest.raises(RuntimeError, match="to retype"):
        rp.patch_columns(src, retype={"nope": _donor("nope", None, "INT64")})


def test_an_unsupported_physical_change_is_refused(sample):
    """Only physical int32 -> int64 is implemented. Anything else says so rather
    than writing a footer that lies about the bytes underneath it."""
    _, _, src = sample

    with pytest.raises(RuntimeError, match="only a physical int32 to int64"):
        rp.patch_columns(src, retype={"i64": _donor("i64", None, "FLOAT64")})
