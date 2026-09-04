# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compaction file selection.

Pure functions over ``FileEntry`` statistics. No storage access, no data
movement — everything read here came off the manifest the binder already
fetched.

Three rule families, tried in the order ``compact()`` used:

  A  brute merge      sub-floor files, concatenated without sorting. Scattered
                      tiny files hurt reads most and are cheapest to fix, and
                      sorting them is wasted until they graduate the floor.
  B  sort-aware       files above the floor, either declustered out of an
                      overlapping group or bin-packed along the key order.
  C  delete debt      one file rewritten purely to shed merge-on-read deletes.
                      The case A and B never reach: a lone at-target file that
                      is heavily deleted.
"""

import random
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from opteryx.models.file_entry import FileEntry

from .constants import DELETE_DEBT_THRESHOLD
from .constants import MIN_FILE_SIZE_BYTES
from .constants import MIN_SIZE_BYTES
from .constants import PASS_BUDGET_BYTES
from .constants import SMALL_FILE_BYTES
from .constants import SORT_AWARE_FLOOR_BYTES
from .constants import TARGET_SIZE_BYTES


class SelectionOutcome(Enum):
    """Why selection returned the plan it did — or did not return one."""

    PLANNED = "planned"
    NOTHING_TO_DO = "nothing-to-do"
    NO_SNAPSHOT = "no-snapshot"


@dataclass
class CompactionPlan:
    """One compaction pass: which files are rewritten, and why."""

    strategy: str
    """``combine`` (concatenate, no sort) or ``combine-split`` (sort, then split
    into k disjoint key ranges)."""

    mode: str
    """``brute`` or ``sort-aware``. ``brute`` never sorts."""

    files: List[FileEntry] = field(default_factory=list)
    reason: str = ""
    sort_column: Optional[str] = None
    expected_outputs: int = 1

    @property
    def input_bytes(self) -> int:
        return sum(entry_size(entry) for entry in self.files)

    @property
    def input_records(self) -> Optional[int]:
        """Total rows rewritten, or None when any file's count is unknown.

        None is not zero. ``FileEntry.record_count`` is ``Optional[int]`` and a
        producer with no count passes None, so summing with a 0 default would
        report a confident row count for files nobody counted.
        """
        counts = [entry.record_count for entry in self.files]
        if any(count is None for count in counts):
            return None
        return sum(counts)  # type: ignore[arg-type]

    def __len__(self) -> int:
        return len(self.files)


@dataclass
class SelectionResult:
    outcome: SelectionOutcome
    plan: Optional[CompactionPlan] = None
    detail: str = ""


@dataclass
class FileRange:
    """A file's extent on the sort key."""

    entry: FileEntry
    low: Any
    high: Any

    @property
    def size(self) -> int:
        return entry_size(self.entry)

    @property
    def is_degenerate(self) -> bool:
        """A single-value file: min == max."""
        return self.low == self.high


def entry_size(entry: FileEntry) -> int:
    """A file's size in the budget unit, NULL-safe.

    ``uncompressed_size_in_bytes`` is ``Optional[int]`` and is None for files
    written before it was recorded. Those read as 0, which is what the catalog
    did — a file of unknown size never blocks a merge by looking enormous.
    """
    return int(entry.uncompressed_size_in_bytes or 0)


# --- Rule A: brute -----------------------------------------------------------


def _select_combine_small(entries: Sequence[FileEntry]) -> Optional[CompactionPlan]:
    """Combine sub-``SMALL_FILE_BYTES`` files toward ``TARGET_SIZE_BYTES``.

    The no-sort-key path. Smallest first, accumulate while the pass budget
    allows, stop once the target is reached and at least two files are in hand.
    No splitting.
    """
    small_files = [entry for entry in entries if entry_size(entry) < SMALL_FILE_BYTES]
    if len(small_files) < 2:
        return None

    selected: List[FileEntry] = []
    total_size = 0

    for entry in sorted(small_files, key=entry_size):
        size = entry_size(entry)
        if total_size + size > PASS_BUDGET_BYTES:
            continue
        selected.append(entry)
        total_size += size
        if total_size >= TARGET_SIZE_BYTES and len(selected) >= 2:
            break

    if len(selected) < 2:
        return None

    return CompactionPlan(
        strategy="combine", mode="brute", files=selected, reason="small-files"
    )


def _select_brute_consolidation(
    sub_floor: Sequence[FileEntry], sort_column: Optional[str]
) -> Optional[CompactionPlan]:
    """Merge two or more sub-floor files, smallest first, toward TARGET.

    No volume threshold to wait for: a drip-fed dataset gets its handful of tiny
    files merged now, because waiting is the small-files problem by another
    name. A single leftover sub-floor remainder is fine — the merge only needs
    two files to make progress, and it accretes across passes until it crosses
    the floor and settles.

    Emits ``combine``, which never sorts.
    """
    if len(sub_floor) < 2:
        return None

    selected: List[FileEntry] = []
    total = 0
    for entry in sorted(sub_floor, key=entry_size):
        size = entry_size(entry)
        if selected and total + size > TARGET_SIZE_BYTES:
            break
        selected.append(entry)
        total += size

    if len(selected) < 2:
        return None

    return CompactionPlan(
        strategy="combine",
        mode="brute",
        files=selected,
        reason="small-file-brute",
        sort_column=sort_column,
    )


# --- Rule B: sort-aware ------------------------------------------------------


def _overlap_amount(candidate: FileRange, group_low: Any, group_high: Any) -> Any:
    """How much ``candidate`` overlaps ``[group_low, group_high]``; <= 0 is none.

    A degenerate candidate (min == max) carries a REAL observed value rather
    than a synthetic split edge, so touching either edge of the group's range is
    genuine overlap — the group provably contains that same value. Inclusive on
    both sides.

    A non-degenerate candidate gets the strict interval test. A boundary shared
    with a non-degenerate neighbour is the artifact of a clean prior split, not
    real overlap; counting it would stop declustering ever converging, because
    it would endlessly re-merge its own disjoint outputs.
    """
    if candidate.is_degenerate:
        if group_low <= candidate.low <= group_high:
            return group_high - group_low  # always > 0: the seed is never degenerate
        return -1
    low = max(candidate.low, group_low)
    high = min(candidate.high, group_high)
    return high - low


def _select_overlap_decluster(
    file_ranges: Sequence[FileRange], sort_column: str, rng=None
) -> Optional[CompactionPlan]:
    """Grow one overlapping group out of a random seed and decluster it.

    ⛔ The seed is picked at RANDOM, and that is load-bearing rather than
    incidental. When one overlap region can never fully resolve in a single pass
    — a popular sort-key value whose boundary file structurally always overlaps
    its siblings — a deterministic scan from the smallest key lands on that same
    unresolvable region every call and starves every other region forever. That
    was observed live on ``opteryx.test.pypi``, where two resolvable clusters sat
    untouched for a whole session behind one that could not resolve.
    """
    if not file_ranges:
        return None

    rng = rng or random
    seed = rng.choice(list(file_ranges))

    if seed.is_degenerate:
        # Every other file at that same value adds nothing to reorder.
        return None

    group = [seed]
    total = seed.size
    remaining = [candidate for candidate in file_ranges if candidate is not seed]

    while remaining:
        try:
            group_low = min(member.low for member in group)
            group_high = max(member.high for member in group)
            best = max(
                remaining, key=lambda candidate: _overlap_amount(candidate, group_low, group_high)
            )
            best_overlap = _overlap_amount(best, group_low, group_high)
        except TypeError:
            break  # sort-key values are not mutually comparable

        if best_overlap <= 0:
            break  # nothing left genuinely overlaps

        if total + best.size > PASS_BUDGET_BYTES:
            # Does not fit this pass; a smaller, less-overlapping file still might.
            remaining.remove(best)
            continue

        group.append(best)
        total += best.size
        remaining.remove(best)

    if len(group) < 2:
        return None

    combined = sum(member.size for member in group)
    outputs = max(1, -(-combined // TARGET_SIZE_BYTES))
    return CompactionPlan(
        strategy="combine-split",
        mode="sort-aware",
        files=[member.entry for member in group],
        reason="overlap-decluster",
        sort_column=sort_column,
        expected_outputs=outputs,
    )


def _select_binpack(
    file_ranges: Sequence[FileRange], sort_column: str
) -> Optional[CompactionPlan]:
    """Pack consecutive, already-disjoint medium files toward TARGET.

    Only unsettled files (below ``MIN_SIZE_BYTES``) are packed; one already near
    target is left alone so packing converges.

    Packing only CONSECUTIVE files keeps the merged key range tight, so the
    result stays disjoint from its neighbours instead of manufacturing new
    overlap for the decluster rule to chase.
    """
    try:
        ordered = sorted(file_ranges, key=lambda candidate: candidate.low)
    except TypeError:
        return None

    index = 0
    count = len(ordered)
    while index < count:
        if ordered[index].size >= MIN_SIZE_BYTES:
            index += 1
            continue  # settled near target, leave alone

        group = [ordered[index]]
        total = ordered[index].size
        probe = index + 1
        while probe < count:
            candidate = ordered[probe]
            if candidate.size >= MIN_SIZE_BYTES:
                break  # a settled file breaks the packable run
            if total + candidate.size > TARGET_SIZE_BYTES:
                break
            group.append(candidate)
            total += candidate.size
            probe += 1

        if len(group) >= 2:
            return CompactionPlan(
                strategy="combine-split",
                mode="sort-aware",
                files=[member.entry for member in group],
                reason="bin-pack",
                sort_column=sort_column,
                expected_outputs=1,
            )
        index += 1

    return None


def _select_sort_aware(
    key_ranges: Sequence[Tuple[FileEntry, Any, Any]], sort_column: str, rng=None
) -> Optional[CompactionPlan]:
    """Rule B: decluster an overlapping group, else pack a disjoint run.

    Files at or below the sort-aware floor are excluded, and so are files whose
    key extent is unknown — ``key_ranges`` only carries those the manifest could
    answer for. A single file already over the hard cap is NOT re-split on its
    own: being oversized is not by itself a reason to rewrite it.
    """
    ranges = [
        FileRange(entry=entry, low=low, high=high)
        for entry, low, high in key_ranges
        if entry_size(entry) > SORT_AWARE_FLOOR_BYTES
    ]
    if not ranges:
        return None

    return _select_overlap_decluster(ranges, sort_column, rng=rng) or _select_binpack(
        ranges, sort_column
    )


# --- Rule C: delete debt -----------------------------------------------------


def _select_delete_debt(
    entries: Sequence[FileEntry],
    sort_column: Optional[str],
    threshold: float = DELETE_DEBT_THRESHOLD,
) -> Optional[CompactionPlan]:
    """Rewrite the single worst delete-debt file, if any clears the threshold.

    No size floor and no partner requirement: this rule exists precisely for the
    file rules A and B never select, and a tiny file with debt is cheap to
    rewrite. One file per pass, worst ratio first; repeated passes work through
    the backlog.

    A single-file ``combine`` preserves the file's existing row order, so a
    sorted file comes out still sorted and no sort is needed or wanted.
    """
    worst: Optional[FileEntry] = None
    worst_ratio = 0.0

    for entry in entries:
        deleted = int(entry.deleted_record_count or 0)
        if not deleted:
            continue
        records = entry.record_count
        if not records or records <= 0:
            continue
        ratio = deleted / records
        if ratio >= threshold and ratio > worst_ratio:
            worst = entry
            worst_ratio = ratio

    if worst is None:
        return None

    return CompactionPlan(
        strategy="combine",
        mode="brute",
        files=[worst],
        reason=f"delete-debt {worst_ratio:.0%}",
        sort_column=sort_column,
    )


# --- Dispatch ----------------------------------------------------------------


def select_compaction_plan(
    entries: Sequence[FileEntry],
    sort_column: Optional[str] = None,
    key_ranges: Optional[Sequence[Tuple[FileEntry, Any, Any]]] = None,
    delete_debt_threshold: float = DELETE_DEBT_THRESHOLD,
    rng=None,
) -> SelectionResult:
    """Choose one compaction pass for ``entries``.

    Rules are attempted A, then B, then C — the order ``compact()`` used, and
    the reason is priority rather than exclusivity: scattered tiny files hurt
    reads most and cost least to fix, so they are dealt with before anything
    that has to sort.

    ``key_ranges`` is the per-file sort-key extent from
    ``Manifest.file_key_ranges``, and is required for rule B. Its absence is not
    an error — a dataset with no sort key legitimately has none — but rule B
    cannot fire without it.
    """
    if not entries:
        return SelectionResult(SelectionOutcome.NO_SNAPSHOT, detail="manifest is empty")

    plan: Optional[CompactionPlan] = None

    if sort_column is None:
        # No usable sort key: the brute rule is the only one that can make
        # progress, and rules B and C both want a sort column to record.
        plan = _select_combine_small(entries)
    else:
        # Rule A, over the sub-floor pool only.
        sub_floor = [entry for entry in entries if entry_size(entry) < MIN_FILE_SIZE_BYTES]
        plan = _select_brute_consolidation(sub_floor, sort_column)

        # Rule B.
        if plan is None and key_ranges:
            plan = _select_sort_aware(key_ranges, sort_column, rng=rng)

        # Rule C.
        if plan is None:
            plan = _select_delete_debt(entries, sort_column, threshold=delete_debt_threshold)

    if plan is None:
        return SelectionResult(
            SelectionOutcome.NOTHING_TO_DO,
            detail="no rule selected files: nothing small, overlapping or heavily deleted",
        )

    return SelectionResult(SelectionOutcome.PLANNED, plan=plan, detail=plan.reason)
