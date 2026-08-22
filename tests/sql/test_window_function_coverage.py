"""The six window functions that were missing: NTILE, PERCENT_RANK, CUME_DIST,
FIRST_VALUE, LAST_VALUE, NTH_VALUE.

WHAT WAS THERE, AND WHAT WAS NOT

ROW_NUMBER, RANK, DENSE_RANK, LAG and LEAD worked; the other six had zero
occurrences anywhere in the tree. The window machinery was never the problem —
this was a coverage gap in one small closed registry
(opteryx/operators/window/helpers.py) mirrored by hand into the `WinFn` enum in
src/cpp/engine/native_sort.hpp.

Delivering them meant restructuring the sink's per-row pass into a
partition-at-a-time, peer-group-at-a-time walk, because the previous single
forward pass could not serve any of them: NTILE, PERCENT_RANK and CUME_DIST need
the partition's SIZE, and LAST_VALUE and NTH_VALUE need its END, none of which is
known until the partition closes. That restructure touched the five that already
worked, so they are re-pinned here alongside the new six.

TIES ARE THE POINT

Most of the interesting behaviour only shows up when rows tie on the ORDER BY
key, and the six differ from each other precisely there:

  * PERCENT_RANK is built on RANK, so tied rows SHARE a value.
  * CUME_DIST counts through the tied group's LAST member, so tied rows share a
    value too — but a different one, and CUME_DIST is never 0 while PERCENT_RANK
    always starts at 0.
  * NTILE does NOT keep tied rows together: buckets are fixed-size, so a tie can
    straddle a boundary.

The fixture below therefore has ties in both partitions and two partitions of
different sizes (5 and 3), so an off-by-one in the partition-size arithmetic
cannot pass by coincidence.

LAST_VALUE / NTH_VALUE DIVERGE FROM THE STANDARD, DELIBERATELY

They are computed over the WHOLE ordered partition. Under the SQL standard's
default frame (RANGE UNBOUNDED PRECEDING AND CURRENT ROW) LAST_VALUE returns the
current row's last tied PEER instead — the well-known footgun. This engine
rejects a frame clause on every function in this registry, so the frame-relative
reading has no spelling here and the whole-partition reading is the only coherent
one. It is also what ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
means, and what callers writing LAST_VALUE almost always intend. Pinned below so
the divergence is a decision on the record rather than a surprise.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

# Two partitions of DIFFERENT sizes, with ties on the order key in both:
#   'a' -> 10, 10, 20, 30, 30   (5 rows, two tied pairs)
#   'b' ->  5,  7,  7           (3 rows, one tied pair)
DATA = [
    ("a", 10),
    ("a", 10),
    ("a", 20),
    ("a", 30),
    ("a", 30),
    ("b", 5),
    ("b", 7),
    ("b", 7),
]
SOURCE = "(VALUES " + ", ".join(f"('{g}', {v})" for g, v in DATA) + ") AS t(g, v)"

# Nine rows, one partition, no ties — for the bucket-count cases, where a tie
# would make which row lands in which bucket unspecified.
NINE = "(VALUES " + ", ".join(f"({i})" for i in range(1, 10)) + ") AS n(i)"


def rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        out.extend(zip(*(table[name] for name in table)))
    return out


def windowed(expression):
    """(g, v, window value) for every row, in a fixed order.

    Sorted null-last so a NULL window value (NTH_VALUE past the partition's end)
    does not make the comparison itself raise.
    """
    result = rows(
        f"SELECT g, v, {expression} OVER (PARTITION BY g ORDER BY v) AS w FROM {SOURCE}"
    )
    return sorted(result, key=lambda r: tuple((x is None, x) for x in r))


# ------------------------------------------------------------------ NTILE ----


@pytest.mark.parametrize(
    "buckets, expected",
    [
        # 9 rows, evenly divisible: three buckets of three.
        (3, [1, 1, 1, 2, 2, 2, 3, 3, 3]),
        # 9 into 2: the FIRST (9 mod 2) = 1 bucket takes the extra row.
        (2, [1, 1, 1, 1, 1, 2, 2, 2, 2]),
        # 9 into 4: the first 1 bucket takes 3, the rest take 2.
        (4, [1, 1, 1, 2, 2, 3, 3, 4, 4]),
        # One bucket is the whole partition.
        (1, [1] * 9),
        # Exactly one row per bucket.
        (9, [1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ],
)
def test_ntile_bucket_counts(buckets, expected):
    assert [r[0] for r in rows(
        f"SELECT NTILE({buckets}) OVER (ORDER BY i) AS b FROM {NINE}"
    )] == expected


@pytest.mark.parametrize("buckets", [10, 100, 1000])
def test_ntile_with_more_buckets_than_rows(buckets):
    """More buckets than rows: the first n take one row each, the rest are empty.

    The failure this guards is the tempting `idx * buckets / n + 1` formula, which
    for 9 rows and 10 buckets numbers them 1, 2, 4, 5, 6, 7, 8, 9, 10 — skipping
    bucket 3 and reaching 10 — instead of 1..9. No row may ever be given a bucket
    number above the row count.
    """
    result = [r[0] for r in rows(
        f"SELECT NTILE({buckets}) OVER (ORDER BY i) AS b FROM {NINE}"
    )]

    assert result == [1, 2, 3, 4, 5, 6, 7, 8, 9], (buckets, result)


def test_ntile_is_per_partition():
    """Each partition is divided independently, so numbering restarts."""
    assert windowed("NTILE(2)") == [
        ("a", 10, 1),
        ("a", 10, 1),
        ("a", 20, 1),
        ("a", 30, 2),
        ("a", 30, 2),
        ("b", 5, 1),
        ("b", 7, 1),
        ("b", 7, 2),
    ]


def test_ntile_does_not_keep_ties_together():
    """Buckets are fixed-size; a tied pair can straddle a boundary.

    Partition 'b' is 5, 7, 7 — into two buckets that is [5, 7] and [7], splitting
    the tied pair. This is standard NTILE behaviour and the reason NTILE is
    flagged non-deterministic in reference/windows.json: which of the two tied
    rows lands in which bucket is not specified.
    """
    buckets = {(g, v): w for g, v, w in windowed("NTILE(2)") if g == "b"}
    assert buckets[("b", 5)] == 1
    assert sorted(w for g, v, w in windowed("NTILE(2)") if g == "b") == [1, 1, 2]


@pytest.mark.parametrize("bad", ["0", "-1", "i"])
def test_ntile_rejects_a_bucket_count_it_cannot_use(bad):
    """The bucket count is fixed before any row is read, so it must be a literal >= 1."""
    with pytest.raises(UnsupportedSyntaxError):
        rows(f"SELECT NTILE({bad}) OVER (ORDER BY i) AS b FROM {NINE}")


def test_ntile_requires_its_argument():
    with pytest.raises(UnsupportedSyntaxError, match="one argument"):
        rows(f"SELECT NTILE() OVER (ORDER BY i) AS b FROM {NINE}")


# ------------------------------------------------- PERCENT_RANK / CUME_DIST ---


def test_percent_rank_shares_a_value_across_ties():
    """(RANK - 1) / (rows - 1): starts at 0, ends at 1, tied rows agree."""
    assert windowed("PERCENT_RANK()") == [
        ("a", 10, 0.0),
        ("a", 10, 0.0),
        ("a", 20, 0.5),
        ("a", 30, 0.75),
        ("a", 30, 0.75),
        ("b", 5, 0.0),
        ("b", 7, 0.5),
        ("b", 7, 0.5),
    ]


def test_percent_rank_of_a_single_row_partition_is_zero():
    """A one-row partition has no spread to be a fraction of.

    The arithmetic is (rank - 1) / (rows - 1), which is 0/0 here. The standard
    fixes it at 0; the implementation must special-case it rather than divide.
    """
    assert [r[0] for r in rows(
        "SELECT PERCENT_RANK() OVER (ORDER BY i) AS p FROM (VALUES (1)) AS s(i)"
    )] == [0.0]


def test_cume_dist_counts_through_the_last_tied_peer():
    """Never 0, always 1 for the last row — the difference from PERCENT_RANK."""
    assert windowed("CUME_DIST()") == [
        ("a", 10, 0.4),  # 2 of 5 rows are <= 10
        ("a", 10, 0.4),
        ("a", 20, 0.6),
        ("a", 30, 1.0),
        ("a", 30, 1.0),
        ("b", 5, 1 / 3),
        ("b", 7, 1.0),
        ("b", 7, 1.0),
    ]


def test_percent_rank_and_cume_dist_are_floats_not_truncated_integers():
    """The output column is FLOAT64.

    Every other non-gathered window output is INT64, and the sink's emit path
    hardcoded that width. A fraction written into an INT64 column would read back
    as 0 for every row but the last — a plausible-looking, entirely wrong answer.
    """
    for expression in ("PERCENT_RANK()", "CUME_DIST()"):
        produced = [w for _g, _v, w in windowed(expression)]
        assert all(isinstance(w, float) for w in produced), (expression, produced)
        assert any(0.0 < w < 1.0 for w in produced), (expression, produced)


# ----------------------------------------- FIRST_VALUE / LAST_VALUE / NTH ----


def test_first_value_is_the_partitions_first_row():
    assert windowed("FIRST_VALUE(v)") == [
        ("a", 10, 10),
        ("a", 10, 10),
        ("a", 20, 10),
        ("a", 30, 10),
        ("a", 30, 10),
        ("b", 5, 5),
        ("b", 7, 5),
        ("b", 7, 5),
    ]


def test_last_value_is_the_partitions_last_row():
    """The WHOLE-PARTITION reading — see this file's docstring.

    Under the SQL standard's default frame this would return the current row's
    last tied peer, so partition 'a' would read 10, 10, 20, 30, 30 rather than 30
    on every row. That is the answer this test exists to say we do NOT give.
    """
    assert windowed("LAST_VALUE(v)") == [
        ("a", 10, 30),
        ("a", 10, 30),
        ("a", 20, 30),
        ("a", 30, 30),
        ("a", 30, 30),
        ("b", 5, 7),
        ("b", 7, 7),
        ("b", 7, 7),
    ]


def test_nth_value_counts_from_one():
    assert windowed("NTH_VALUE(v, 1)") == windowed("FIRST_VALUE(v)")
    assert [w for _g, _v, w in windowed("NTH_VALUE(v, 2)")] == [10, 10, 10, 10, 10, 7, 7, 7]


def test_nth_value_past_the_end_of_a_partition_is_null():
    """Partition 'b' has 3 rows, so its 4th value does not exist.

    Partition 'a' has 5 and still answers — the NULL is per partition, not per
    query, which is what makes this worth pinning separately.
    """
    assert windowed("NTH_VALUE(v, 4)") == [
        ("a", 10, 30),
        ("a", 10, 30),
        ("a", 20, 30),
        ("a", 30, 30),
        ("a", 30, 30),
        ("b", 5, None),
        ("b", 7, None),
        ("b", 7, None),
    ]


def test_value_functions_take_the_arguments_type():
    """The output is a gathered VALUE, so its type is the argument's, not INT64."""
    result = rows(
        "SELECT FIRST_VALUE(name) OVER (ORDER BY id) AS f FROM $planets LIMIT 1"
    )
    assert result == [("Mercury",)], result


@pytest.mark.parametrize(
    "call",
    ["FIRST_VALUE()", "LAST_VALUE()", "NTH_VALUE(v)", "NTH_VALUE(v, 0)", "NTH_VALUE(v, v)"],
)
def test_value_functions_reject_malformed_calls(call):
    with pytest.raises(UnsupportedSyntaxError):
        rows(f"SELECT {call} OVER (ORDER BY v) AS w FROM {SOURCE}")


# --------------------------------------------------------- shared contract ---

_ALL_SIX = (
    "NTILE(2)",
    "PERCENT_RANK()",
    "CUME_DIST()",
    "FIRST_VALUE(v)",
    "LAST_VALUE(v)",
    "NTH_VALUE(v, 1)",
)


@pytest.mark.parametrize("call", _ALL_SIX)
def test_the_new_six_require_an_over_clause(call):
    with pytest.raises(UnsupportedSyntaxError, match="OVER"):
        rows(f"SELECT {call} AS w FROM {SOURCE}")


@pytest.mark.parametrize("call", _ALL_SIX)
def test_the_new_six_require_an_order_by(call):
    with pytest.raises(UnsupportedSyntaxError, match="ORDER BY"):
        rows(f"SELECT {call} OVER (PARTITION BY g) AS w FROM {SOURCE}")


@pytest.mark.parametrize("call", _ALL_SIX)
def test_the_new_six_reject_a_frame_clause(call):
    """A frame is rejected on every function in this registry.

    This is the rule that makes the whole-partition reading of LAST_VALUE the only
    coherent one, so it is pinned for all six rather than left implicit.
    """
    with pytest.raises(UnsupportedSyntaxError):
        rows(
            f"SELECT {call} OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND "
            f"CURRENT ROW) AS w FROM {SOURCE}"
        )


def test_several_window_functions_share_one_sort():
    """Functions over the same PARTITION BY / ORDER BY are computed in one pass.

    All three output shapes — INT64, FLOAT64 and a gathered value — are produced
    by that single pass, which is the case that would break if the emit path
    assumed one width for all of them.
    """
    result = rows(
        "SELECT g, v, "
        "  ROW_NUMBER()      OVER (PARTITION BY g ORDER BY v) AS rn, "
        "  NTILE(2)          OVER (PARTITION BY g ORDER BY v) AS bucket, "
        "  CUME_DIST()       OVER (PARTITION BY g ORDER BY v) AS cd, "
        "  LAST_VALUE(v)     OVER (PARTITION BY g ORDER BY v) AS lv "
        f"FROM {SOURCE} ORDER BY g, v, rn"
    )

    assert [r[2] for r in result] == [1, 2, 3, 4, 5, 1, 2, 3]
    assert [r[3] for r in result] == [1, 1, 1, 2, 2, 1, 1, 2]
    assert [r[4] for r in result] == [0.4, 0.4, 0.6, 1.0, 1.0, 1 / 3, 1.0, 1.0]
    assert [r[5] for r in result] == [30, 30, 30, 30, 30, 7, 7, 7]


def test_the_five_that_already_worked_still_do():
    """The sink's per-row pass was restructured to deliver the six above.

    These five were computed by the loop that was replaced, so they are re-pinned
    here: a regression in them is a regression caused by this work, not a gap.
    """
    # LAG/LEAD are read in ROW order rather than through `windowed`, whose null-last
    # sort would reorder the very rows whose position is the answer. ROW_NUMBER
    # supplies that order, so the expectations below read down the partition.
    def in_row_order(expression):
        result = rows(
            "SELECT rn, w FROM ("
            "  SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn, g, "
            f"    {expression} OVER (PARTITION BY g ORDER BY v) AS w FROM {SOURCE}"
            ") AS r ORDER BY g, rn"
        )
        return [w for _rn, w in result]

    assert [w for _g, _v, w in windowed("ROW_NUMBER()")] == [1, 2, 3, 4, 5, 1, 2, 3]
    assert [w for _g, _v, w in windowed("RANK()")] == [1, 1, 3, 4, 4, 1, 2, 2]
    assert [w for _g, _v, w in windowed("DENSE_RANK()")] == [1, 1, 2, 3, 3, 1, 2, 2]
    # 'a' is 10, 10, 20, 30, 30 and 'b' is 5, 7, 7.
    assert in_row_order("LAG(v)") == [None, 10, 10, 20, 30, None, 5, 7]
    assert in_row_order("LEAD(v)") == [10, 20, 30, 30, None, 7, 7, None]


def test_a_fused_top_k_filter_still_applies_only_to_ranks():
    """WindowTopKFusionStrategy fuses `rank <= K`; the new six are not rank-valued.

    NTILE's output is an ordinal but its bucket boundaries depend on the partition
    size, so there is no constant K to fuse, and PERCENT_RANK/CUME_DIST are
    fractions. The query below must answer correctly whether or not anything
    upstream decides to fuse it.
    """
    result = rows(
        "SELECT v, rn FROM ("
        "  SELECT v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn "
        f"  FROM {SOURCE}"
        ") AS r WHERE rn <= 2 ORDER BY v, rn"
    )
    assert result == [(5, 1), (7, 2), (10, 1), (10, 2)], result


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
