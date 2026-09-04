"""Rule C, delete debt: the one rule that rewrites a file purely to shed
merge-on-read deletes.

Ported from opteryx-catalog's `tests/test_mor_deletes.py` when compaction moved
into the engine (docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md). There the rule was
exercised end-to-end through `DatasetCompactor.compact(rule="debt")`, which no
longer exists; here it is exercised at the selector, which is where the RULE now
lives. What the file actually rewrites is the sink's job and is covered by the
catalog's `compaction_commit` row-count invariant.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.models.file_entry import FileEntry
from opteryx.planner.compaction.constants import DELETE_DEBT_THRESHOLD
from opteryx.planner.compaction.selection import _select_delete_debt


def _entry(path, record_count, deleted_record_count, size=1024):
    return FileEntry(
        file_path=path,
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=size,
        uncompressed_size_in_bytes=size,
        deleted_record_count=deleted_record_count,
    )


def test_heavy_debt_is_selected():
    """2/10 deleted = 20%, clear of the 10% default."""
    entries = [_entry("mem://data/f1.parquet", 10, 2)]

    plan = _select_delete_debt(entries, sort_column=None)

    assert plan is not None
    assert plan.mode == "brute"
    assert [f.file_path for f in plan.files] == ["mem://data/f1.parquet"]
    assert "delete-debt" in plan.reason


def test_debt_below_threshold_is_not_selected():
    """1/100 = 1%. Rewriting a whole file to reclaim one row is not worth it."""
    entries = [_entry("mem://data/f1.parquet", 100, 1)]

    assert _select_delete_debt(entries, sort_column=None) is None


def test_a_file_with_no_deletes_is_never_selected():
    """This rule exists only for debt - it must not become a general sweeper."""
    entries = [_entry("mem://data/f1.parquet", 100, 0)]

    assert _select_delete_debt(entries, sort_column=None) is None


def test_the_worst_offender_wins_and_only_one_file_is_taken():
    """One file per pass, worst ratio first; repeated passes clear the backlog."""
    entries = [
        _entry("mem://data/light.parquet", 100, 15),  # 15%
        _entry("mem://data/heavy.parquet", 100, 80),  # 80%
        _entry("mem://data/clean.parquet", 100, 0),
    ]

    plan = _select_delete_debt(entries, sort_column=None)

    assert plan is not None
    assert [f.file_path for f in plan.files] == ["mem://data/heavy.parquet"]


def test_an_unknown_record_count_is_skipped_not_treated_as_zero():
    """None means UNKNOWN (see FileEntry.record_count); a ratio cannot be formed
    from it, and dividing by a fabricated 0 would raise."""
    entries = [_entry("mem://data/f1.parquet", None, 5)]

    assert _select_delete_debt(entries, sort_column=None) is None


def test_the_threshold_boundary_is_inclusive():
    """`ratio >= threshold`, so a file sitting exactly on the default is taken."""
    at = [_entry("mem://data/f1.parquet", 100, int(100 * DELETE_DEBT_THRESHOLD))]
    under = [_entry("mem://data/f1.parquet", 100, int(100 * DELETE_DEBT_THRESHOLD) - 1)]

    assert _select_delete_debt(at, sort_column=None) is not None
    assert _select_delete_debt(under, sort_column=None) is None


def test_an_explicit_threshold_overrides_the_default():
    """The selector's own `threshold=` argument works; what is missing is a
    caller that fills it from the dataset's policy - see the xfail below."""
    entries = [_entry("mem://data/f1.parquet", 10, 2)]  # 20%

    assert _select_delete_debt(entries, sort_column=None, threshold=0.5) is None
    assert _select_delete_debt(entries, sort_column=None, threshold=0.15) is not None


@pytest.mark.xfail(
    strict=True,
    reason=(
        "KNOWN GAP: the engine has no reader for the catalog's maintenance policy, so "
        "`maintenance_policy['delete-debt-threshold']` is never plumbed into "
        "select_compaction_plan and every dataset is planned against the default. "
        "See the KNOWN GAP comment in planner/optimizer/strategies/compaction_planning.py."
    ),
)
def test_a_per_dataset_threshold_override_is_honoured():
    """Ported from opteryx-catalog's `test_delete_debt_threshold_override`.

    A dataset that raises its own threshold to 50% must not have a 20%-debt file
    selected. The rule can express this; nothing reads the policy to say it.
    """
    from opteryx.planner.compaction import select_compaction_plan

    entries = [_entry("mem://data/f1.parquet", 10, 2)]  # 20% debt
    maintenance_policy = {"delete-debt-threshold": 0.5}

    result = select_compaction_plan(
        entries, sort_column=None, key_ranges=None, maintenance_policy=maintenance_policy
    )

    assert result.plan is None or "delete-debt" not in (result.plan.reason or "")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
