# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the rugo.parquet read/write facade and the E.28 reader reconstruction.

PyArrow is the read-side oracle only.
"""

import glob
import os
import pathlib
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo import parquet


def _planets_path() -> str:
    return glob.glob("**/planets/planets.parquet", recursive=True)[0]


def test_read_all_columns_match_pyarrow():
    """Every planets column decodes correctly (E.28 reconstruction)."""
    import decimal
    import re

    import pyarrow.parquet as pq

    path = _planets_path()
    truth = pq.read_table(path).to_pydict()
    meta = parquet.read_metadata(path)
    schema_by_name = {c.name: c for c in meta.schema_columns}

    with parquet.read_parquet(path) as reader:
        morsels = list(reader)
    assert len(morsels) == 1
    m = morsels[0]
    for name, col in schema_by_name.items():
        got = m.column(name.encode()).to_pylist()
        # DECIMAL columns now materialize as native DRAKEN_DECIMAL/DECIMAL128
        # vectors, so to_pylist yields decimal.Decimal directly — compare against
        # PyArrow's already-scaled Decimal values with no manual rescaling.
        assert got == truth[name], name


def test_read_from_bytes_and_path_agree():
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"]) as r:
        by_path = list(r)[0].column(b"name").to_pylist()
    with parquet.read_parquet(open(path, "rb").read(), columns=["name"]) as r:
        by_bytes = list(r)[0].column(b"name").to_pylist()
    assert by_path == by_bytes


def test_filter_keeps_matching_row_group():
    """The surviving row group is ROW-filtered, not just kept.

    Asserting only the morsel count passed even when the predicate was silently
    dropped and every row came back — check the rows themselves.
    """
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"], predicates=[("id", ">", 4)]) as r:
        morsels = list(r)
    assert len(morsels) == 1  # ids 1..9 — row group survives
    # id > 4 keeps ids 5..9: Jupiter, Saturn, Uranus, Neptune, Pluto
    assert morsels[0].column(b"name").to_pylist() == [
        "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"
    ]


def _multi_row_group_path(tmp_path) -> str:
    """100,000 rows in 10,000-row row groups — a shape where a dropped predicate
    returns a plausible-looking WRONG SUBSET (the row groups min/max pruning
    happened to keep) rather than an obviously unfiltered whole file. The
    single-row-group planets fixture cannot show that."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    n = 100_000
    morsel = Morsel.from_vectors(
        ["tweet_id", "user_name", "followers"],
        [
            Vector(dn.vector_int64_from_sequence(list(range(n)))),
            Vector(dn.vector_from_string_sequence(
                [b"user_%d" % (i % 5_000) for i in range(n)])),
            Vector(dn.vector_int64_from_sequence([i for i in range(n)])),
        ],
    )
    path = str(tmp_path / "multi_row_group.parquet")
    with open(path, "wb") as f:
        f.write(parquet.write_parquet(morsel, max_rows_per_row_group=10_000))
    return path


def test_filter_on_unprojected_column(tmp_path):
    """A predicate on a column absent from `columns=` must still filter.

    "Project narrow, filter on something else" is an ordinary access pattern.
    The predicate column is read internally and projected away afterwards; it
    must never be silently dropped, which returned unfiltered data.
    """
    path = _multi_row_group_path(tmp_path)

    def read(predicates, columns):
        with parquet.read_parquet(path, columns=columns, predicates=predicates) as r:
            morsels = list(r)
        for m in morsels:
            # only the REQUESTED columns come back
            assert list(m.column_names) == [c.encode() for c in columns]
        return sum(m.num_rows for m in morsels)

    # projected == predicate column: the case that already worked
    assert read([("user_name", "==", "user_1")], ["user_name"]) == 20
    # predicate column NOT projected — same answer
    assert read([("user_name", "==", "user_1")], ["tweet_id"]) == 20
    # numeric, spanning every row group
    assert read([("followers", ">", 10_000)], ["tweet_id"]) == 89_999
    # a predicate row groups PARTIALLY prune: the plausible-wrong-subset shape
    assert read([("tweet_id", "<", 5)], ["followers"]) == 5
    # two unprojected predicate columns
    assert read([("tweet_id", ">=", 10), ("followers", "<", 20)], ["user_name"]) == 10


def test_unprojected_filter_returns_the_right_rows(tmp_path):
    """Row COUNTS alone can coincide — check the surviving values."""
    path = _multi_row_group_path(tmp_path)
    with parquet.read_parquet(
        path, columns=["tweet_id"], predicates=[("user_name", "==", "user_1")]
    ) as r:
        got = [v for m in r for v in m.column(b"tweet_id").to_pylist()]
    assert got == [1 + 5_000 * k for k in range(20)]

    # caller's projection ORDER is preserved, predicate column projected away
    with parquet.read_parquet(
        path, columns=["followers", "tweet_id"], predicates=[("user_name", "==", "user_1")]
    ) as r:
        morsel = next(iter(r))
    assert list(morsel.column_names) == [b"followers", b"tweet_id"]


def test_empty_row_group_yields_an_empty_morsel():
    """A row group that survives pruning but filters to nothing yields a
    ZERO-ROW morsel — it is not skipped. read_parquet's docstring used to claim
    the opposite; this pins the behaviour it now documents."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    morsel = Morsel.from_vectors(
        ["x"], [Vector(dn.vector_int64_from_sequence([0, 100, 200, 300]))]
    )
    data = parquet.write_parquet(morsel)
    # 40 < x < 60 matches no row, but neither bound prunes the row group.
    with parquet.read_parquet(data, predicates=[("x", ">", 40), ("x", "<", 60)]) as r:
        morsels = list(r)
    assert [m.num_rows for m in morsels] == [0]
    assert sum(m.num_rows for m in morsels) == 0


def test_bytes_column_names_prune_like_str(tmp_path):
    """A bytes predicate column name must prune row groups exactly as str does.

    Footer statistics are keyed by str, so a bytes name missed the lookup and
    quietly turned stage-1 pruning into a no-op: the rows were still correct
    (stage 2 filters on bytes), only every row group got decoded.
    """
    path = _multi_row_group_path(tmp_path)
    data = open(path, "rb").read()
    assert len(parquet._native.read_rowgroup_stats(data)) == 10

    by_str = parquet._row_group_mask(data, None, [("tweet_id", "<", 5)])
    by_bytes = parquet._row_group_mask(data, None, [(b"tweet_id", "<", 5)])
    assert by_str == by_bytes
    assert sum(by_str) == 1  # only the first row group can hold ids < 5

    for name in ("tweet_id", b"tweet_id"):
        with parquet.read_parquet(
            path, columns=["user_name"], predicates=[(name, "<", 5)]
        ) as r:
            morsels = list(r)
        assert sum(m.num_rows for m in morsels) == 5
        for m in morsels:
            assert list(m.column_names) == [b"user_name"]


def test_bytes_projection_names_select_real_columns(tmp_path):
    """columns=[b"name"] must project that column, not silently return a
    zero-column morsel (the native reader stringifies what it is handed)."""
    path = _planets_path()
    with parquet.read_parquet(path, columns=[b"name"]) as r:
        morsel = next(iter(r))
    assert list(morsel.column_names) == [b"name"]
    assert morsel.column(b"name").to_pylist()[0] == "Mercury"

    # and with a predicate on an unprojected column, spelled in bytes
    with parquet.read_parquet(
        path, columns=[b"name"], predicates=[(b"id", ">", 4)]
    ) as r:
        morsel = next(iter(r))
    assert morsel.column(b"name").to_pylist() == [
        "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"
    ]


def _text_row_groups_bytes() -> bytes:
    """40,000 rows of sortable text in 10,000-row row groups, so a text
    predicate prunes SOME but not all row groups."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    n = 40_000
    morsel = Morsel.from_vectors(
        ["s", "i"],
        [
            Vector(dn.vector_from_string_sequence(
                [b"user_%05d" % (i % 20_000) for i in range(n)])),
            Vector(dn.vector_int64_from_sequence(list(range(n)))),
        ],
    )
    return parquet.write_parquet(morsel, max_rows_per_row_group=10_000)


def test_bytes_predicate_value_prunes_like_str():
    """A bytes predicate VALUE must prune a string column exactly as str does.

    decode_value returns str for a STRING-annotated BYTE_ARRAY, so a bytes value
    made the comparison raise TypeError into the "don't prune" guard: the rows
    stayed right (stage 2 compares bytes) while stage 1 quietly gave up.
    """
    data = _text_row_groups_bytes()
    assert len(parquet._native.read_rowgroup_stats(data)) == 4

    for probe in ("user_00003", "zzz"):
        by_str = parquet._row_group_mask(data, None, [("s", "=", probe)])
        by_bytes = parquet._row_group_mask(data, None, [("s", "=", probe.encode())])
        assert by_str == by_bytes, probe
    # and pruning really happens — an unpruned mask would compare equal too
    assert sum(parquet._row_group_mask(data, None, [("s", "=", b"user_00003")])) == 2
    assert sum(parquet._row_group_mask(data, None, [("s", "=", b"zzz")])) == 0


def test_str_predicate_value_prunes_binary_column():
    """The mirror case: a VARBINARY column's bounds decode as BYTES, so a str
    predicate value hit the same TypeError guard from the other side."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    morsel = Morsel.from_vectors(
        ["b", "i"],
        [Vector(dn.vector_varbinary_from_constant(b"abc", 4)),
         Vector(dn.vector_int64_from_sequence([1, 2, 3, 4]))],
    )
    data = parquet.write_parquet(morsel)
    assert parquet._row_group_mask(data, None, [("b", "=", "zzz")]) == [0]
    assert parquet._row_group_mask(data, None, [("b", "=", b"zzz")]) == [0]
    assert parquet._row_group_mask(data, None, [("b", "=", "abc")]) == [1]


@pytest.mark.parametrize("op", ["=", "==", "!=", ">", ">=", "<", "<="])
def test_text_pruning_never_drops_matching_rows(op):
    """Pruning discards whole row groups, so a coercion that changed a
    comparison's ANSWER would silently drop rows. Check every operator, in both
    spellings, against a brute-force oracle."""
    import operator as _op

    data = _text_row_groups_bytes()
    truth = [(b"user_%05d" % (i % 20_000)).decode() for i in range(40_000)]
    compare = {"=": _op.eq, "==": _op.eq, "!=": _op.ne, ">": _op.gt,
               ">=": _op.ge, "<": _op.lt, "<=": _op.le}[op]

    for probe in ("user_00003", "user_19999", "user_00000", "aaa", "zzz"):
        want = sorted(i for i, s in enumerate(truth) if compare(s, probe))
        for value in (probe, probe.encode()):
            with parquet.read_parquet(
                data, columns=["i"], predicates=[("s", op, value)]
            ) as r:
                got = sorted(v for m in r for v in m.column(b"i").to_pylist())
            assert got == want, (op, probe, type(value).__name__)


def test_in_predicate_members_are_coerced():
    """`in` / `not in` carry a COLLECTION: every text member must be aligned.

    One bytes member among strs made the whole row-group test raise, abandoning
    pruning for every member at once — and because `in` short-circuits on the
    first match, whether it raised depended on MEMBER ORDER.
    """
    data = _text_row_groups_bytes()

    # a hit: only the row groups holding user_00003 survive, whatever the spelling
    for members in (["user_00003"], [b"user_00003"], ["user_00003", b"user_00007"],
                    [b"user_00003", "user_00007"]):
        assert sum(parquet._row_group_mask(data, None, [("s", "in", members)])) == 2, members

    # a miss: every row group prunes, whatever the spelling
    for members in (["zzz"], [b"zzz"], ["zzz", b"aaa"], [b"aaa", "zzz"]):
        assert parquet._row_group_mask(data, None, [("s", "in", members)]) == [0, 0, 0, 0], members

    # non-text members are left alone and still fall into the don't-prune guard
    assert parquet._row_group_mask(data, None, [("s", "in", [17])]) == [1, 1, 1, 1]
    # an empty set excludes everything, as before
    assert parquet._row_group_mask(data, None, [("s", "in", [])]) == [0, 0, 0, 0]


@pytest.mark.parametrize("op", ["in", "not in"])
def test_in_pruning_never_drops_matching_rows(op):
    """Soundness: a row group holding ANY row that satisfies the predicate must
    survive stage 1, in every spelling. Pruning discards whole row groups, so a
    coercion that changed the test's answer would silently drop rows."""
    data = _text_row_groups_bytes()
    truth = [(b"user_%05d" % (i % 20_000)).decode() for i in range(40_000)]
    per = len(truth) // 4

    for members in (["user_00003"], [b"user_00003"], ["user_00003", b"user_00007"],
                    ["zzz"], [b"zzz"], [b"aaa", "zzz"], []):
        wanted = [m.decode("utf-8") if isinstance(m, bytes) else m for m in members]
        mask = parquet._row_group_mask(data, None, [("s", op, members)])
        for group in range(4):
            rows = truth[group * per:(group + 1) * per]
            matches = any((r in wanted) if op == "in" else (r not in wanted)
                          for r in rows)
            if matches:
                assert mask[group] == 1, (op, members, group)


@pytest.mark.parametrize("op", ["in", "not in"])
def test_in_predicate_filters_rows(op):
    """`in` / `not in` filter at the ROW level, not just at the row group.

    They pruned row groups but had no row-level implementation, so every `in`
    read died with "unsupported predicate operator" — the operator was listed
    in read_parquet's docstring and unusable. Checked against a brute-force
    oracle, in every member spelling.
    """
    data = _text_row_groups_bytes()
    truth = [(b"user_%05d" % (i % 20_000)).decode() for i in range(40_000)]

    for members in (["user_00003"], [b"user_00003"],
                    ["user_00003", "user_00007"], [b"user_00003", "user_00007"],
                    ["user_00003", "user_00003"],   # duplicates
                    ["user_00003", "zzz"],          # one hit, one miss
                    ["zzz"], []):
        wanted = {m.decode("utf-8") if isinstance(m, bytes) else m for m in members}
        want = sorted(i for i, t in enumerate(truth)
                      if (t in wanted) == (op == "in"))
        with parquet.read_parquet(
            data, columns=["i"], predicates=[("s", op, members)]
        ) as r:
            got = sorted(v for m in r for v in m.column(b"i").to_pylist())
        assert got == want, (op, members)


def test_in_predicate_on_numeric_column():
    """Membership is not string-only — the same lowering serves int columns."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    morsel = Morsel.from_vectors(
        ["i"], [Vector(dn.vector_int64_from_sequence([1, 2, 3, 4, 5]))]
    )
    data = parquet.write_parquet(morsel)
    with parquet.read_parquet(data, predicates=[("i", "in", [2, 4])]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == [2, 4]
    with parquet.read_parquet(data, predicates=[("i", "not in", [2, 4])]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == [1, 3, 5]


def test_in_predicate_null_semantics():
    """A null row satisfies NEITHER `in` nor `not in` (SQL 3VL), the same
    convention the scalar comparisons already follow. An EMPTY list asks no
    comparison, so `not in []` keeps the null row."""
    sql = """
    SELECT * FROM (VALUES
      (1, 'alpha'), (2, 'beta'), (3, NULL), (4, 'delta'), (NULL, 'epsilon')
    ) AS t(i, s)
    """
    morsel = next(iter(opteryx.session().execute_to_morsels(sql)))
    data = parquet.write_parquet(morsel)

    def read(predicates, column):
        with parquet.read_parquet(data, predicates=predicates) as r:
            return [v for m in r for v in m.column(column).to_pylist()]

    assert read([("s", "in", ["alpha", "delta"])], b"s") == ["alpha", "delta"]
    # the NULL row is absent from NOT IN too — not selected, not "everything else"
    assert read([("s", "not in", ["alpha", "delta"])], b"s") == ["beta", "epsilon"]
    # matching the existing scalar convention
    assert read([("s", "!=", "alpha")], b"s") == ["beta", "delta", "epsilon"]
    # nulls in an int column behave the same
    assert read([("i", "not in", [1, 3])], b"i") == [2, 4]
    # an empty list raises no 3VL question
    assert read([("s", "in", [])], b"s") == []
    assert read([("s", "not in", [])], b"s") == ["alpha", "beta", None, "delta", "epsilon"]


def test_in_predicate_membership_is_verified_not_hashed():
    """Stage 2 is the EXACT stage, so membership must not rest on a hash-only
    probe: Vector.in_list() is a CarcharSet lookup with no key verification and
    is deliberately not used here. Every surviving row really equals a member."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    values = [b"value_%04d" % i for i in range(2_000)]
    morsel = Morsel.from_vectors(
        ["s", "i"],
        [Vector(dn.vector_from_string_sequence(values)),
         Vector(dn.vector_int64_from_sequence(list(range(len(values)))))],
    )
    data = parquet.write_parquet(morsel)
    wanted = [b"value_0007", b"value_1234", b"value_1999"]
    with parquet.read_parquet(data, predicates=[("s", "in", wanted)]) as r:
        got = [v for m in r for v in m.column(b"s").to_pylist()]
    assert sorted(got) == sorted(w.decode() for w in wanted)


def test_path_source_is_never_read_into_memory(tmp_path):
    """A path + predicates must not slurp the whole file to reach its footer.

    `_row_group_mask` only reads the footer, but it takes a buffer spanning the
    file, so the facade used to read the entire file into a bytes object and
    THEN hand the same path to stream_parquet_from_path, which mmaps it again.
    Pinned as an invariant rather than a byte count: for a path source nothing
    in the read may call `_to_bytes` at all.
    """
    path = _multi_row_group_path(tmp_path)
    real_to_bytes = parquet._to_bytes

    def _no_slurp(source):
        if isinstance(source, str):
            raise AssertionError("whole file read into memory: %r" % source)
        return real_to_bytes(source)

    parquet._to_bytes = _no_slurp
    try:
        with parquet.read_parquet(
            path, columns=["user_name"], predicates=[("tweet_id", "<", 5)]
        ) as r:
            assert sum(m.num_rows for m in r) == 5
        # and with no predicates, where there was never a mask to build
        with parquet.read_parquet(path, columns=["user_name"]) as r:
            assert sum(m.num_rows for m in r) == 100_000
    finally:
        parquet._to_bytes = real_to_bytes


def test_mapped_releases_its_view(tmp_path):
    """_mapped must drop the memoryview before closing the mapping — a live
    view makes mmap.close() raise BufferError, which would leak the mapping."""
    path = _multi_row_group_path(tmp_path)
    with parquet._mapped(path) as view:
        assert len(view) == os.path.getsize(path)
    with pytest.raises(ValueError):
        len(view)  # released with the mapping, not left dangling


def test_memory_source_still_filters(tmp_path):
    """The buffer path has no file to map and must keep working unchanged."""
    path = _multi_row_group_path(tmp_path)
    data = open(path, "rb").read()
    with parquet.read_parquet(
        data, columns=["user_name"], predicates=[("tweet_id", "<", 5)]
    ) as r:
        morsels = list(r)
    assert sum(m.num_rows for m in morsels) == 5
    for m in morsels:
        assert list(m.column_names) == [b"user_name"]


def _bool_fixture_bytes():
    """A BOOLEAN column with an interior NULL, and an id column to read back."""
    sql = """
    SELECT * FROM (VALUES
      (1, true), (2, false), (3, true), (4, NULL), (5, false)
    ) AS t(i, b)
    """
    morsel = next(iter(opteryx.session().execute_to_morsels(sql)))
    return parquet.write_parquet(morsel)


@pytest.mark.parametrize("op", ["=", "==", "!=", ">", ">=", "<", "<="])
@pytest.mark.parametrize("probe", [True, False])
def test_bool_predicate_operators(op, probe):
    """Every operator works on a BOOLEAN column, ordered FALSE < TRUE.

    Draken has no compare arm for DRAKEN_BOOL — compare_scalar AND
    compare_vector both refuse it — so these all died with "unsupported type"
    after their row groups had already been pruned. A bool column needs no
    comparison: it IS the mask.
    """
    import operator as _op

    compare = {"=": _op.eq, "==": _op.eq, "!=": _op.ne, ">": _op.gt,
               ">=": _op.ge, "<": _op.lt, "<=": _op.le}[op]
    values = [True, False, True, None, False]
    ids = [1, 2, 3, 4, 5]
    # a NULL row satisfies no comparison, as everywhere else in this facade
    want = [i for i, v in zip(ids, values) if v is not None and compare(v, probe)]

    data = _bool_fixture_bytes()
    with parquet.read_parquet(data, predicates=[("b", op, probe)]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == want


@pytest.mark.parametrize("members,want", [
    ([True], [1, 3]),
    ([False], [2, 5]),
    ([True, False], [1, 2, 3, 5]),   # every non-null row
    ([], []),
])
def test_bool_membership_predicates(members, want):
    """`in` / `not in` over a BOOLEAN column, through the same lowering."""
    data = _bool_fixture_bytes()
    with parquet.read_parquet(data, predicates=[("b", "in", members)]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == want

    # the complement, except that NULL satisfies neither — and an empty list
    # asks no comparison at all, so `not in []` keeps the null row
    inverse = ([1, 2, 3, 4, 5] if not members
               else [i for i in [1, 2, 3, 5] if i not in want])
    with parquet.read_parquet(data, predicates=[("b", "not in", members)]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == inverse


@pytest.mark.parametrize("bad", [1, 0, 5, "true", "false", b"x"])
def test_bool_predicate_rejects_non_bool_values(bad):
    """A BOOLEAN column demands real bools, and the check runs BEFORE pruning.

    `bool(value)` would make `b = "false"` mean TRUE. Worse, the check has to
    precede stage 1: `b = 5` prunes every row group on min/max, so a late check
    would never run and the read would answer "no rows" instead of failing.
    """
    data = _bool_fixture_bytes()
    with pytest.raises(ValueError, match="needs True or False"):
        with parquet.read_parquet(data, predicates=[("b", "=", bad)]) as r:
            list(r)
    # and inside a membership collection
    with pytest.raises(ValueError, match="needs True or False"):
        with parquet.read_parquet(data, predicates=[("b", "in", [True, bad])]) as r:
            list(r)


def test_bool_row_group_pruning():
    """Bool min/max still prunes whole row groups, and the rows that survive
    are exact — an all-TRUE row group cannot hold a FALSE."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    flags = ([True] * 10_000 + [False] * 10_000 + [True] * 10_000
             + [i % 2 == 0 for i in range(10_000)])
    morsel = Morsel.from_vectors(
        ["b", "i"],
        [Vector(dn.vector_int64_from_sequence(flags)),
         Vector(dn.vector_int64_from_sequence(list(range(len(flags)))))],
    )
    data = parquet.write_parquet(morsel, max_rows_per_row_group=10_000)
    assert len(parquet._native.read_rowgroup_stats(data)) == 4

    # the all-FALSE row group is pruned for `= True`, and vice versa
    assert parquet._row_group_mask(data, None, [("b", "=", True)]) == [1, 0, 1, 1]
    assert parquet._row_group_mask(data, None, [("b", "=", False)]) == [0, 1, 0, 1]

    for probe, want in ((True, 25_000), (False, 15_000)):
        with parquet.read_parquet(
            data, columns=["i"], predicates=[("b", "=", probe)]
        ) as r:
            assert sum(m.num_rows for m in r) == want


def _null_fixture_bytes():
    """Columns with interior NULLs, plus an id column with none at all."""
    sql = """
    SELECT * FROM (VALUES
      (1, 'alpha', 10), (2, NULL, 20), (3, 'gamma', NULL),
      (4, NULL, NULL), (5, 'eps', 50)
    ) AS t(i, s, n)
    """
    morsel = next(iter(opteryx.session().execute_to_morsels(sql)))
    return parquet.write_parquet(morsel)


@pytest.mark.parametrize("column,op,want", [
    ("s", "is null", [2, 4]),
    ("s", "is not null", [1, 3, 5]),
    ("n", "is null", [3, 4]),
    ("n", "is not null", [1, 2, 5]),
    ("i", "is null", []),            # a column with no nulls at all
    ("i", "is not null", [1, 2, 3, 4, 5]),
])
def test_null_predicates(column, op, want):
    """`is null` / `is not null` filter rows, across types and a null-free column.

    No comparison can answer these: every compare kernel propagates nullness, so
    the rows IS NULL must keep are exactly the ones a comparison marks null, and
    filter_mask (valid AND true) drops them. The mask comes from the validity
    bitmap instead — draken's is_null_mask / is_not_null_mask.
    """
    data = _null_fixture_bytes()
    with parquet.read_parquet(data, predicates=[(column, op, None)]) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == want


def test_null_predicate_on_unprojected_column():
    """A null test on a column the caller did not project still filters."""
    data = _null_fixture_bytes()
    with parquet.read_parquet(
        data, columns=["i"], predicates=[("s", "is null", None)]
    ) as r:
        morsels = list(r)
    assert [v for m in morsels for v in m.column(b"i").to_pylist()] == [2, 4]
    for m in morsels:
        assert list(m.column_names) == [b"i"]


def test_null_predicate_combines_with_others():
    """Null tests AND with the other predicates like any conjunct."""
    data = _null_fixture_bytes()
    with parquet.read_parquet(
        data, predicates=[("s", "is not null", None), ("i", ">", 1)]
    ) as r:
        assert [v for m in r for v in m.column(b"i").to_pylist()] == [3, 5]


@pytest.mark.parametrize("op", ["=", "==", "!=", "<", "<=", ">", ">="])
def test_none_value_is_rejected(op):
    """`x = None` can match no row, so it must fail rather than answer "none".

    It used to reach the compare kernel as a bare nanobind TypeError; and left
    to stage 1 it could prune the file to nothing and return an empty result
    that reads like real data.
    """
    data = _null_fixture_bytes()
    with pytest.raises(ValueError, match="use 'is null'"):
        with parquet.read_parquet(data, predicates=[("s", op, None)]) as r:
            list(r)


def test_none_member_in_collection_is_rejected():
    """A None inside `in` / `not in` is the same unanswerable comparison.

    It cannot be quietly dropped either: `in ['alpha', None]` would then mean
    `in ['alpha']`, which is not what SQL's 3VL says the caller asked for.
    """
    data = _null_fixture_bytes()
    for op in ("in", "not in"):
        with pytest.raises(ValueError, match="use 'is null'"):
            with parquet.read_parquet(
                data, predicates=[("s", op, ["alpha", None])]
            ) as r:
                list(r)


def test_null_predicate_row_group_pruning():
    """Row groups prune on null_count, and ONLY when the file recorded one."""
    import draken.draken_native as dn
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector

    # rg0 has no nulls, rg1 is entirely null, rg2 is mixed
    values = ([1] * 10_000 + [None] * 10_000
              + [None if i % 2 else i for i in range(10_000)])
    morsel = Morsel.from_vectors(
        ["v", "i"],
        [Vector(dn.vector_int64_from_sequence(values)),
         Vector(dn.vector_int64_from_sequence(list(range(len(values)))))],
    )
    data = parquet.write_parquet(morsel, max_rows_per_row_group=10_000)
    stats = parquet._native.read_rowgroup_stats(data)
    assert len(stats) == 3

    # the null-free row group cannot answer IS NULL; the all-null one cannot
    # answer IS NOT NULL
    assert parquet._row_group_mask(data, None, [("v", "is null", None)]) == [0, 1, 1]
    assert parquet._row_group_mask(data, None, [("v", "is not null", None)]) == [1, 0, 1]

    with parquet.read_parquet(data, columns=["i"], predicates=[("v", "is null", None)]) as r:
        assert sum(m.num_rows for m in r) == 15_000
    with parquet.read_parquet(data, columns=["i"], predicates=[("v", "is not null", None)]) as r:
        assert sum(m.num_rows for m in r) == 15_000


def test_absent_null_count_does_not_prune():
    """null_count is -1 when the file recorded none: absent means "don't know",
    never "zero". Pruning on a missing count would discard row groups that do
    hold nulls — a silently short answer."""
    stats = [{"num_rows": 100, "columns": [
        {"name": "v", "physical_type": "int64", "logical_type": "int64",
         "min": None, "max": None, "null_count": -1, "bloom_offset": -1,
         "bloom_length": -1, "max_repetition_level": 0},
    ]}]
    real = parquet._native.read_rowgroup_stats
    parquet._native.read_rowgroup_stats = lambda data: stats
    try:
        for op in ("is null", "is not null"):
            assert parquet._row_group_mask(b"", None, [("v", op, None)]) == [1], op
    finally:
        parquet._native.read_rowgroup_stats = real


def _null_count_stats(null_count, num_rows, max_repetition_level):
    return [{"num_rows": num_rows, "columns": [
        {"name": "v", "physical_type": "int64", "logical_type": "int64",
         "min": None, "max": None, "null_count": null_count,
         "bloom_offset": -1, "bloom_length": -1,
         "max_repetition_level": max_repetition_level},
    ]}]


def test_is_not_null_does_not_prune_a_nested_leaf():
    """`null_count == num_rows` means "every row is null" for a FLAT column only.

    On a nested leaf, null_count counts null LEAF VALUES — a NULL list, an EMPTY
    list and a null element inside a non-null list all add to it — so it shares
    no denominator with num_rows and the two can coincide while non-null rows
    are present. Pruning there drops row groups that hold matching rows.

    The two stat sets below differ in nothing but max_repetition_level.
    """
    real = parquet._native.read_rowgroup_stats
    try:
        parquet._native.read_rowgroup_stats = lambda data: _null_count_stats(100, 100, 0)
        assert parquet._row_group_mask(b"", None, [("v", "is not null", None)]) == [0]

        parquet._native.read_rowgroup_stats = lambda data: _null_count_stats(100, 100, 1)
        assert parquet._row_group_mask(b"", None, [("v", "is not null", None)]) == [1]
    finally:
        parquet._native.read_rowgroup_stats = real


def test_is_null_still_prunes_a_nested_leaf():
    """The other direction stays armed: a null row ALWAYS writes a null leaf
    entry, so null_count == 0 really does mean no null rows, nested or not."""
    real = parquet._native.read_rowgroup_stats
    try:
        parquet._native.read_rowgroup_stats = lambda data: _null_count_stats(0, 100, 1)
        assert parquet._row_group_mask(b"", None, [("v", "is null", None)]) == [0]
    finally:
        parquet._native.read_rowgroup_stats = real


def test_nested_null_predicates_on_a_real_list_file():
    """End to end on a list column, against the row-level validity bitmap.

    The leaf null_count here (2) is not the row-level null count (1) — the
    divergence the guard above exists for, on a real file rather than a
    synthesised stat.
    """
    path = "testdata/flat/null_lists/00002.parquet"
    if not os.path.exists(path):
        pytest.skip("fixture not present")
    with open(path, "rb") as f:
        data = f.read()

    leaf = [c for c in parquet._native.read_rowgroup_stats(data)[0]["columns"]
            if c["name"] == "list"][0]
    assert leaf["max_repetition_level"] > 0
    assert leaf["null_count"] == 2

    with parquet.read_parquet(data, columns=["key"],
                              predicates=[("list", "is null", None)]) as r:
        assert sum(m.num_rows for m in r) == 1
    with parquet.read_parquet(data, columns=["key"],
                              predicates=[("list", "is not null", None)]) as r:
        assert sum(m.num_rows for m in r) == 4


def test_filter_on_column_absent_from_file_raises(tmp_path):
    """A predicate naming a column that is in NO projection and NOT in the file
    is unanswerable — fail loud rather than return unfiltered rows."""
    path = _multi_row_group_path(tmp_path)
    with pytest.raises(ValueError, match="not in this parquet file"):
        with parquet.read_parquet(
            path, columns=["tweet_id"], predicates=[("no_such_column", "==", 1)]
        ) as r:
            list(r)


def test_filter_prunes_row_group():
    path = _planets_path()
    with parquet.read_parquet(path, columns=["name"], predicates=[("id", ">", 10_000)]) as r:
        assert list(r) == []  # pruned, nothing decoded
    with parquet.read_parquet(path, predicates=[("name", "=", "Zzz")]) as r:
        assert list(r) == []  # string min/max prune


def test_write_then_read_roundtrip_with_nulls():
    """Facade write -> facade read, across types and interior nulls."""
    sql = """
    SELECT * FROM (VALUES
      (1, 1.5, true, 'alpha'),
      (-2, 2.25, false, 'beta'),
      (NULL, NULL, NULL, NULL),
      (7, 1e30, false, 'delta')
    ) AS t(i, d, b, s)
    """
    morsel = next(iter(opteryx.session().execute_to_morsels(sql)))
    for compression in ("zstd", "none"):
        data = parquet.write_parquet(morsel, compression=compression)
        with parquet.read_parquet(data) as reader:
            out = list(reader)[0]
        assert out.column(b"i").to_pylist() == [1, -2, None, 7]
        assert out.column(b"d").to_pylist() == [1.5, 2.25, None, 1e30]
        assert out.column(b"b").to_pylist() == [True, False, None, False]
        assert out.column(b"s").to_pylist() == ["alpha", "beta", None, "delta"]


def test_write_planets_roundtrip():
    """Narrow ints (planets.id is INT8) widen and round-trip."""
    morsel = next(
        iter(opteryx.session().execute_to_morsels("SELECT id, name FROM $planets"))
    )
    data = parquet.write_parquet(morsel)
    with parquet.read_parquet(data) as reader:
        out = list(reader)[0]
    assert out.column(b"id").to_pylist() == list(range(1, 10))
    assert out.column(b"name").to_pylist()[0] == "Mercury"


if __name__ == "__main__":
    test_read_all_columns_match_pyarrow()
    test_read_from_bytes_and_path_agree()
    test_filter_keeps_matching_row_group()
    test_filter_prunes_row_group()
    with tempfile.TemporaryDirectory() as _tmp:
        test_filter_on_unprojected_column(pathlib.Path(_tmp))
        test_unprojected_filter_returns_the_right_rows(pathlib.Path(_tmp))
        test_bytes_column_names_prune_like_str(pathlib.Path(_tmp))
        test_bytes_projection_names_select_real_columns(pathlib.Path(_tmp))
        test_path_source_is_never_read_into_memory(pathlib.Path(_tmp))
        test_mapped_releases_its_view(pathlib.Path(_tmp))
        test_memory_source_still_filters(pathlib.Path(_tmp))
        test_filter_on_column_absent_from_file_raises(pathlib.Path(_tmp))
    test_empty_row_group_yields_an_empty_morsel()
    test_bytes_predicate_value_prunes_like_str()
    test_str_predicate_value_prunes_binary_column()
    for _op_name in ("=", "==", "!=", ">", ">=", "<", "<="):
        test_text_pruning_never_drops_matching_rows(_op_name)
    test_in_predicate_members_are_coerced()
    for _in_op in ("in", "not in"):
        test_in_pruning_never_drops_matching_rows(_in_op)
    for _in_op in ("in", "not in"):
        test_in_predicate_filters_rows(_in_op)
    test_in_predicate_on_numeric_column()
    test_in_predicate_null_semantics()
    test_in_predicate_membership_is_verified_not_hashed()
    for _bool_op in ("=", "==", "!=", ">", ">=", "<", "<="):
        for _probe in (True, False):
            test_bool_predicate_operators(_bool_op, _probe)
    for _members, _want in (([True], [1, 3]), ([False], [2, 5]),
                            ([True, False], [1, 2, 3, 5]), ([], [])):
        test_bool_membership_predicates(_members, _want)
    for _bad in (1, 0, 5, "true", "false", b"x"):
        test_bool_predicate_rejects_non_bool_values(_bad)
    test_bool_row_group_pruning()
    for _c, _o, _w in (("s", "is null", [2, 4]),
                       ("s", "is not null", [1, 3, 5]),
                       ("n", "is null", [3, 4]),
                       ("n", "is not null", [1, 2, 5]),
                       ("i", "is null", []),
                       ("i", "is not null", [1, 2, 3, 4, 5])):
        test_null_predicates(_c, _o, _w)
    test_null_predicate_on_unprojected_column()
    test_null_predicate_combines_with_others()
    for _o in ("=", "==", "!=", "<", "<=", ">", ">="):
        test_none_value_is_rejected(_o)
    test_none_member_in_collection_is_rejected()
    test_null_predicate_row_group_pruning()
    test_absent_null_count_does_not_prune()
    test_write_then_read_roundtrip_with_nulls()
    test_write_planets_roundtrip()
    print("✅ okay")
