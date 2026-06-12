"""
COUNT(DISTINCT col) null-handling regression tests, exercised through the
public session API (stable across the Arrow-elimination substrate change, unlike
the draken-internal harness in test_ungrouped_agg_dict_paths.py).

Guards the dense-path bug where NULL was counted as one extra distinct value:
the aggregate built its null sentinel with the scalar mix_hash() while the
hashing kernel mixes null rows via simd_hash_i64(NULL_HASH) — the two differ, so
the sentinel compare never matched and NULL survived. The dense path now drops
nulls by the validity bitmap (matching the compressed fast path). COUNT(DISTINCT)
must ignore NULL.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import opteryx


def _scalar(sql: str):
    session = opteryx.session()
    try:
        out = None
        for morsel in session.execute_to_morsels(sql):
            out = morsel.column(morsel.column_names[0]).to_pylist()[0]
        return out
    finally:
        session.close()


def test_all_null_string_distinct_is_zero():
    # NULLIF(name, name) is all-NULL VARCHAR (dense). COUNT(DISTINCT) ignores NULL.
    assert _scalar("SELECT COUNT(DISTINCT NULLIF(name, name)) FROM $planets;") == 0


def test_all_null_integer_distinct_is_zero():
    assert _scalar("SELECT COUNT(DISTINCT NULLIF(id, id)) FROM $planets;") == 0


def test_mixed_null_string_distinct_excludes_null():
    # Five rows keep `name`, the rest become NULL — only the five distinct names count.
    assert (
        _scalar(
            "SELECT COUNT(DISTINCT CASE WHEN id > 4 THEN name ELSE NULL END) FROM $planets;"
        )
        == 5
    )


def test_non_null_distinct_unchanged():
    # $planets has 9 rows, all distinct names — no nulls, sanity that the fix
    # did not perturb the common case.
    assert _scalar("SELECT COUNT(DISTINCT name) FROM $planets;") == 9


def test_constant_distinct_is_one():
    assert _scalar("SELECT COUNT(DISTINCT 1) FROM $planets;") == 1


def test_empty_input_distinct_is_zero():
    assert _scalar("SELECT COUNT(DISTINCT name) FROM $planets WHERE id < 0;") == 0


if __name__ == "__main__":
    test_all_null_string_distinct_is_zero()
    test_all_null_integer_distinct_is_zero()
    test_mixed_null_string_distinct_excludes_null()
    test_non_null_distinct_unchanged()
    test_constant_distinct_is_one()
    test_empty_input_distinct_is_zero()
    print("✅ all COUNT(DISTINCT) null regression tests passed")
