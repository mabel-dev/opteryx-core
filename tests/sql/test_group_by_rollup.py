"""Value-level regression tests for `GROUP BY ROLLUP(...)`.

`GROUP BY ROLLUP(a, b, c)` is the grouping-set list `(a,b,c), (a,b), (a), ()`:
progressively coarser subtotals ending in a grand total. A row belongs to EVERY set,
and the keys a set does not name come back NULL on that set's rows.

These tests assert on the ACTUAL SUBTOTAL AND GRAND-TOTAL ROWS and on where the NULLs
land — not that the query parses. Parsing was never the interesting part: before this
landed, `ROLLUP` was parsed as an ordinary scalar function call and failed catalog
lookup with "Function **ROLLUP** cannot be found", which is what a GROUP BY modifier
falling through to function resolution looks like.

The case that carries the whole design is `test_data_null_is_not_a_rolled_up_null`.
Over data that itself contains NULL keys, `(a)` and `()` both produce the key row
`(NULL, NULL)`, and so does the genuine data row — three DIFFERENT groups that render
identically. They stay apart because the native grouping-expand operator adds a
`grouping_id` key (src/cpp/engine/native_grouping_expand.hpp); drop it and the grand
total silently absorbs the subtotal, which is a wrong answer wearing the right shape.

Expected values here are hard-coded, and were cross-checked against DuckDB reading the
same relation — DuckDB is the oracle, but a test that needs it installed to say
anything is a test that goes quiet the day it is missing.

`GROUPING(col)` — ROLLUP's companion function, `test_grouping_*` below — is what
answers the question a rolled-up NULL and a genuine one both look like from the
outside: "was `col` rolled up to produce THIS row?" It is a lookup against the
grouping set's `grouping_id`, not an arithmetic shift of it — see
`test_grouping_keys_off_the_set_ordinal_not_the_mask` for why that distinction is
load-bearing, not stylistic.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import UnsupportedSyntaxError

# Genuine NULLs in BOTH key columns, so a rolled-up NULL and a data NULL are in the
# same result and have to be told apart. Sums are powers of two: every subtotal
# identifies exactly which rows produced it, so a wrong grouping cannot coincide with
# a right total.
VALUES = "(VALUES ('x','p',1),('x',NULL,2),(NULL,'p',4),(NULL,NULL,8),('y','q',16))"
REL = f"(SELECT * FROM {VALUES} AS v(a,b,n))"


def _rows(sql):
    """Result rows as tuples, in the morsel's column order, sorted for comparison.

    ROLLUP output has no inherent row order, so every assertion here is against a
    SORTED multiset. NULLs sort first, deterministically, rather than by whatever
    ordering the sink happened to produce."""
    session = opteryx.session()
    columns: dict = {}
    order = None
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            if order is None or key not in columns:
                columns.setdefault(key, [])
            columns[key].extend(values)
        if order is None:
            order = list(morsel.to_arrow().to_pydict())
    if order is None:
        return []
    rows = [
        tuple(
            value.decode() if isinstance(value, bytes) else value
            for value in (columns[name][index] for name in order)
        )
        for index in range(len(columns[order[0]]))
    ]
    return sorted(rows, key=lambda row: tuple((cell is None, str(cell)) for cell in row))


def test_single_level_rollup_adds_a_grand_total():
    """`ROLLUP(a)` is the sets `(a)` and `()` — the per-key rows plus one row over
    everything, with the key NULL."""
    rows = _rows(f"SELECT a, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(a)")
    assert rows == sorted(
        [
            ("x", 3),  # 1 + 2
            ("y", 16),
            (None, 12),  # the DATA rows where a IS NULL: 4 + 8
            (None, 31),  # the grand total: 1 + 2 + 4 + 8 + 16
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_data_null_is_not_a_rolled_up_null():
    """The case `grouping_id` exists for.

    Three groups render as `(NULL, NULL)` and all three must survive:
      * 8  — the genuine data row a=NULL, b=NULL, from the finest set `(a, b)`
      * 12 — the subtotal for a=NULL (4 + 8), from set `(a)`, b rolled up
      * 31 — the grand total, from set `()`, both keys rolled up
    Collapsing any pair of them is the defect this asserts against."""
    rows = _rows(f"SELECT a, b, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(a, b)")

    null_null = sorted(row[2] for row in rows if row[0] is None and row[1] is None)
    assert null_null == [8, 12, 31], null_null

    assert rows == sorted(
        [
            ("x", "p", 1),
            ("x", None, 2),  # the DATA row where b IS NULL
            ("x", None, 3),  # the subtotal for a='x' (1 + 2)
            ("y", "q", 16),
            ("y", None, 16),  # subtotal for a='y'
            (None, "p", 4),
            (None, None, 8),  # the DATA row a IS NULL, b IS NULL
            (None, None, 12),  # subtotal for a IS NULL
            (None, None, 31),  # grand total
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_rollup_row_count_is_one_per_group_per_set():
    """A three-key rollup over a real relation: four sets, and every row of the
    result belongs to exactly one of them. Row COUNT is the cheapest witness that
    no set was dropped and none was double-counted."""
    finest = _rows(
        "SELECT i_category, i_class, i_brand, COUNT(*) AS c "
        "FROM testdata.tpcds_001.item GROUP BY i_category, i_class, i_brand"
    )
    mid = _rows(
        "SELECT i_category, i_class, COUNT(*) AS c "
        "FROM testdata.tpcds_001.item GROUP BY i_category, i_class"
    )
    coarse = _rows(
        "SELECT i_category, COUNT(*) AS c FROM testdata.tpcds_001.item GROUP BY i_category"
    )
    rolled = _rows(
        "SELECT i_category, i_class, i_brand, COUNT(*) AS c "
        "FROM testdata.tpcds_001.item GROUP BY ROLLUP(i_category, i_class, i_brand)"
    )
    # + 1 for the grand total, the set that names no key at all.
    assert len(rolled) == len(finest) + len(mid) + len(coarse) + 1, (
        len(rolled),
        len(finest),
        len(mid),
        len(coarse),
    )

    # The grand total counts every row of the relation, once.
    total = _rows("SELECT COUNT(*) AS c FROM testdata.tpcds_001.item")[0][0]
    grand = [row[3] for row in rolled if row[0] is None and row[1] is None and row[2] is None]
    assert grand == [total], (grand, total)


def test_masked_key_keeps_its_type():
    """A rolled-up key is NULL, not a differently-typed column: the expand operator
    reuses the source column's type and logical type (a masked DECIMAL that lost its
    precision/scale, or a masked TIMESTAMP64 that lost its MANDATORY descriptor, would
    be a hard error in draken rather than a NULL)."""
    rows = _rows("SELECT gravity, COUNT(*) AS c FROM $planets GROUP BY ROLLUP(gravity)")
    grand = [row for row in rows if row[0] is None]
    assert grand == [(None, 9)], grand
    # DECIMAL keys, not floats — the type survived the mask.
    import decimal

    assert all(isinstance(row[0], decimal.Decimal) for row in rows if row[0] is not None), rows


def test_rollup_composes_with_a_plain_key():
    """The GROUP BY list is a sequence of grouping ELEMENTS combined by cross product,
    so `a, ROLLUP(b)` is `(a,b), (a)` — and, unlike `ROLLUP(a,b)`, has NO grand total."""
    rows = _rows(f"SELECT a, b, SUM(n) AS s FROM {REL} GROUP BY a, ROLLUP(b)")
    assert not [row for row in rows if row[0] is None and row[1] is None and row[2] == 31], rows
    assert rows == sorted(
        [
            ("x", "p", 1),
            ("x", None, 2),
            ("x", None, 3),
            ("y", "q", 16),
            ("y", None, 16),
            (None, "p", 4),
            (None, None, 8),
            (None, None, 12),
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_composite_element_rolls_up_as_one_unit():
    """`ROLLUP((a, b))` has ONE element, a composite of two columns, so it is the sets
    `(a,b)` and `()` — three result groups plus a grand total, NOT the four-set chain
    `ROLLUP(a, b)` produces. The nesting is load-bearing."""
    rows = _rows(f"SELECT a, b, SUM(n) AS s FROM {REL} GROUP BY ROLLUP((a, b))")
    assert rows == sorted(
        [
            ("x", "p", 1),
            ("x", None, 2),
            ("y", "q", 16),
            (None, "p", 4),
            (None, None, 8),
            (None, None, 31),  # grand total; no per-`a` subtotal exists
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_duplicate_grouping_sets_produce_duplicate_rows():
    """`ROLLUP(a, a)` denotes `(a,a), (a), ()`. The first two group IDENTICALLY, but they
    are still two sets, and each contributes its own rows — so every per-`a` row appears
    TWICE and the answer is seven rows, not four.

    This is why `grouping_id` carries the set's ORDINAL and not its mask: the two sets
    share a mask, so keying on the mask merged them, halving the row count AND doubling
    every surviving sum (the two copies landed in one group). Both symptoms are here."""
    rows = _rows(f"SELECT a, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(a, a)")
    assert rows == sorted(
        [
            ("x", 3),
            ("x", 3),
            ("y", 16),
            ("y", 16),
            (None, 12),
            (None, 12),
            (None, 31),  # the grand total appears once — `()` is a single set
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_rollup_over_a_computed_key():
    """The rolled-up key does not have to be a stored column — a computed key is
    projected to the stream first and masked there like any other."""
    rows = _rows(f"SELECT UPPER(a) AS u, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(UPPER(a))")
    assert rows == sorted(
        [("X", 3), ("Y", 16), (None, 12), (None, 31)],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_having_filters_subtotal_rows_too():
    """HAVING is applied to the aggregate's OUTPUT, so it sees subtotal and
    grand-total rows on the same terms as the finest-grain ones."""
    rows = _rows(f"SELECT a, b, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(a, b) HAVING SUM(n) > 8")
    assert rows == sorted(
        [("y", "q", 16), ("y", None, 16), (None, None, 12), (None, None, 31)],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_no_aggregate_rollup_is_refused_not_answered_wrongly():
    """A GROUP BY with no aggregate is a DISTINCT over the keys, and a DISTINCT has no
    key beyond the columns themselves — it would collapse the identical key rows two
    different grouping sets produce. Refused by name rather than answered short."""
    try:
        _rows(f"SELECT a, b FROM {REL} GROUP BY ROLLUP(a, b)")
    except NotSupportedError as err:
        assert "ROLLUP" in str(err), str(err)
        return
    raise AssertionError("a no-aggregate ROLLUP was answered instead of refused")


def test_cube_and_grouping_sets_are_refused_by_name():
    """CUBE and GROUPING SETS parse — the dialect enables the whole grouping-set
    production — but nothing lowers them. They must be refused by NAME, not reach the
    aggregate half-understood."""
    for sql, spelling in (
        (f"SELECT a, SUM(n) AS s FROM {REL} GROUP BY CUBE(a, b)", "CUBE"),
        (f"SELECT a, SUM(n) AS s FROM {REL} GROUP BY GROUPING SETS ((a), ())", "GROUPING SETS"),
    ):
        try:
            _rows(sql)
        except UnsupportedSyntaxError as err:
            assert spelling in str(err), (spelling, str(err))
            continue
        raise AssertionError(f"{spelling} was not refused")


def test_grouping_marks_the_grand_total_not_the_data_row():
    """`GROUPING(a)` is 1 exactly on the row where `a` was rolled up (the grand
    total) and 0 everywhere else — including the subtotal row where `a` genuinely
    IS NULL in the data. GROUPING() answers "was THIS key rolled up", not "is this
    row's key NULL": those coincide for `b` in `ROLLUP(a)` (there is no `b` key to
    roll up) but must not be confused for `a` itself once a real NULL is in play —
    see the two-key test below for that split."""
    rows = _rows(f"SELECT a, SUM(n) AS s, GROUPING(a) AS g FROM {REL} GROUP BY ROLLUP(a)")
    assert rows == sorted(
        [
            ("x", 3, 0),
            ("y", 16, 0),
            (None, 12, 0),  # the DATA rows where a IS NULL — a was NOT rolled up
            (None, 31, 1),  # the grand total — a WAS rolled up
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_grouping_distinguishes_data_null_from_rolled_up_null():
    """The case GROUPING() exists for: three rows render as `(a, b) = (NULL, NULL)`
    (see `test_data_null_is_not_a_rolled_up_null`), and GROUPING(a)/GROUPING(b)
    tell them apart precisely —
      * s=8  (a, b both genuinely NULL in the data): GROUPING(a)=0, GROUPING(b)=0
      * s=12 (subtotal for a IS NULL, b rolled up):  GROUPING(a)=0, GROUPING(b)=1
      * s=31 (grand total, both rolled up):          GROUPING(a)=1, GROUPING(b)=1
    A GROUPING(a)+GROUPING(b) `lochierarchy`-style expression (TPC-DS Q70/Q86's
    idiom) would rank these 0, 1, 2 respectively — exactly the subtotal ordering
    those queries sort on."""
    rows = _rows(
        f"SELECT a, b, SUM(n) AS s, GROUPING(a) AS ga, GROUPING(b) AS gb "
        f"FROM {REL} GROUP BY ROLLUP(a, b)"
    )
    null_null = sorted(
        (row[2], row[3], row[4]) for row in rows if row[0] is None and row[1] is None
    )
    assert null_null == [(8, 0, 0), (12, 0, 1), (31, 1, 1)], null_null

    assert rows == sorted(
        [
            ("x", "p", 1, 0, 0),
            ("x", None, 2, 0, 0),  # the DATA row where b IS NULL
            ("x", None, 3, 0, 1),  # the subtotal for a='x' (b rolled up)
            ("y", "q", 16, 0, 0),
            ("y", None, 16, 0, 1),  # subtotal for a='y'
            (None, "p", 4, 0, 0),
            (None, None, 8, 0, 0),  # the DATA row a IS NULL, b IS NULL
            (None, None, 12, 0, 1),  # subtotal for a IS NULL
            (None, None, 31, 1, 1),  # grand total
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_grouping_keys_off_the_set_ordinal_not_the_mask():
    """`ROLLUP(a, a)` denotes the sets `(a,a), (a), ()` — the first two group
    IDENTICALLY (same mask) but are still two DIFFERENT sets, neither of which
    rolls `a` up. If GROUPING() were computed by shifting the raw `grouping_id`
    ordinal as though it WERE the mask (rather than looking the ordinal up
    against each set's actual mask), the two duplicate sets would report
    different, wrong GROUPING(a) values instead of both reporting 0. See
    src/cpp/engine/native_grouping_expand.hpp::GroupingBitOperator."""
    rows = _rows(f"SELECT a, SUM(n) AS s, GROUPING(a) AS g FROM {REL} GROUP BY ROLLUP(a, a)")
    assert rows == sorted(
        [
            ("x", 3, 0),
            ("x", 3, 0),
            ("y", 16, 0),
            ("y", 16, 0),
            (None, 12, 0),
            (None, 12, 0),
            (None, 31, 1),  # the grand total appears once — `()` is a single set
        ],
        key=lambda row: tuple((cell is None, str(cell)) for cell in row),
    ), rows


def test_grouping_requires_rollup_in_scope():
    """A plain GROUP BY has exactly one grouping set — no key is ever rolled up —
    so GROUPING() has nothing to answer and is refused rather than silently
    returning 0 for every row."""
    try:
        _rows(f"SELECT a, GROUPING(a) AS g, SUM(n) AS s FROM {REL} GROUP BY a")
    except UnsupportedSyntaxError as err:
        assert "GROUPING" in str(err) and "ROLLUP" in str(err), str(err)
        return
    raise AssertionError("GROUPING() without ROLLUP/CUBE/GROUPING SETS was answered")


def test_grouping_argument_must_be_a_group_by_key():
    """GROUPING(expr) only means something when `expr` is one of the columns the
    ROLLUP names — anything else has no grouping set membership to report."""
    try:
        _rows(f"SELECT a, GROUPING(n) AS g, SUM(n) AS s FROM {REL} GROUP BY ROLLUP(a)")
    except UnsupportedSyntaxError as err:
        assert "GROUPING" in str(err), str(err)
        return
    raise AssertionError("GROUPING() over a non-GROUP-BY-key column was answered")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
