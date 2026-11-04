# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compaction file selection.

Pure functions over ``CompactionFile`` records - one per manifest file, the
facts the rules read plus the file's manifest row. No storage access, no data
movement — everything read here came off the manifest the binder already
fetched.

Three rule families, tried in the order ``compact()`` used:

  A  brute merge      sub-floor files, concatenated without sorting. Scattered
                      tiny files hurt reads most and are cheapest to fix, and
                      sorting them is wasted until they graduate the floor.
  B  sort-aware       files above the floor, either declustered out of an
                      overlapping group or bin-packed along the key order.
  C  delete debt      one file rewritten purely to shed merge-on-read deletes.
                      The case A and B never reach: an at-target file that is
                      heavily deleted.

A dataset whose manifest holds a SINGLE file is rejected unless rule C fires:
there is nothing to merge it with, so only a rewrite-in-place to shed delete
debt is worth a pass.

``OPTIMIZE TABLE x FINAL`` uses ``select_final_compaction_plan`` instead: one
contiguous key window, as large as the disk budget allows, in a single pass.
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

from .constants import DELETE_DEBT_THRESHOLD
from .constants import MIN_FILE_SIZE_BYTES
from .constants import MIN_SIZE_BYTES
from .constants import PASS_BUDGET_BYTES
from .constants import SMALL_FILE_BYTES
from .constants import SORT_AWARE_FLOOR_BYTES
from .constants import TARGET_SIZE_BYTES


@dataclass(eq=False)
class CompactionFile:
    """One manifest file as selection sees it. Compared by identity: two files
    with the same facts are still two files."""

    row: int
    """The file's row in the manifest it came from - what ``Manifest.subset`` takes."""

    file_path: str
    record_count: Optional[int]
    """None is UNKNOWN, never zero."""

    uncompressed_size: Optional[int]
    """None for files written before it was recorded."""

    deleted_record_count: int = 0


def compaction_files(manifest) -> List[CompactionFile]:
    """The manifest's files, in manifest order, as selection reads them."""
    return [
        CompactionFile(row, path, records, size, deleted)
        for row, (path, records, size, deleted) in enumerate(
            zip(
                manifest.get_file_paths(),
                manifest.record_counts(),
                manifest.uncompressed_sizes(),
                manifest.deleted_record_counts(),
                strict=True,
            )
        )
    ]


class SelectionOutcome(Enum):
    """Why selection returned the plan it did — or did not return one."""

    PLANNED = "planned"
    NOTHING_TO_DO = "nothing-to-do"
    NO_SNAPSHOT = "no-snapshot"
    SINGLE_FILE = "single-file"


@dataclass
class CompactionPlan:
    """One compaction pass: which files are rewritten, and why."""

    strategy: str
    """``combine`` (concatenate, no sort) or ``combine-split`` (sort, then split
    into k disjoint key ranges)."""

    mode: str
    """``brute`` or ``sort-aware``. ``brute`` never sorts."""

    files: List[CompactionFile] = field(default_factory=list)
    reason: str = ""
    sort_column: Optional[str] = None
    expected_outputs: int = 1

    @property
    def input_bytes(self) -> int:
        return sum(entry_size(entry) for entry in self.files)

    @property
    def input_records(self) -> Optional[int]:
        """Total rows rewritten, or None when any file's count is unknown.

        None is not zero. ``CompactionFile.record_count`` is ``Optional[int]`` and a
        producer with no count gives None, so summing with a 0 default would
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

    entry: CompactionFile
    low: Any
    high: Any

    @property
    def size(self) -> int:
        return entry_size(self.entry)

    @property
    def is_degenerate(self) -> bool:
        """A single-value file: min == max."""
        return self.low == self.high


def entry_size(entry: CompactionFile) -> int:
    """A file's size in the budget unit, NULL-safe.

    ``uncompressed_size`` is ``Optional[int]`` and is None for files written
    before it was recorded. Those read as 0, which is what the catalog
    did — a file of unknown size never blocks a merge by looking enormous.
    """
    return int(entry.uncompressed_size or 0)


# --- Rule A: brute -----------------------------------------------------------


def _select_combine_small(entries: Sequence[CompactionFile]) -> Optional[CompactionPlan]:
    """Combine sub-``SMALL_FILE_BYTES`` files toward ``TARGET_SIZE_BYTES``.

    The no-sort-key path. Smallest first, accumulate while the pass budget
    allows, stop once the target is reached and at least two files are in hand.
    No splitting.
    """
    small_files = [entry for entry in entries if entry_size(entry) < SMALL_FILE_BYTES]
    if len(small_files) < 2:
        return None

    selected: List[CompactionFile] = []
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
    sub_floor: Sequence[CompactionFile], sort_column: Optional[str]
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

    selected: List[CompactionFile] = []
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
    key_ranges: Sequence[Tuple[CompactionFile, Any, Any]], sort_column: str, rng=None
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


def _delete_debt_ratio(entry: CompactionFile) -> float:
    """Deleted rows over physical rows; 0.0 when either count is absent."""
    deleted = int(entry.deleted_record_count or 0)
    records = entry.record_count
    if not deleted or not records or records <= 0:
        return 0.0
    return deleted / records



def _select_delete_debt(
    entries: Sequence[CompactionFile],
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
    worst: Optional[CompactionFile] = None
    worst_ratio = 0.0

    for entry in entries:
        ratio = _delete_debt_ratio(entry)
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
    entries: Sequence[CompactionFile],
    sort_column: Optional[str] = None,
    key_ranges: Optional[Sequence[Tuple[CompactionFile, Any, Any]]] = None,
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

    if len(entries) == 1:
        # A one-file dataset has nothing to merge with, so rules A and B — which
        # both need a partner — cannot make progress and are skipped.
        #
        # Rule C is the exception, and the ONLY reason a single file is ever
        # rewritten: shedding merge-on-read delete debt is a rewrite in place,
        # so it does not need a partner. A lone file with no debt is declined.
        plan = _select_delete_debt(entries, sort_column, threshold=delete_debt_threshold)
        if plan is None:
            return SelectionResult(
                SelectionOutcome.SINGLE_FILE,
                detail="manifest holds a single file with no delete debt: nothing to compact",
            )
        return SelectionResult(SelectionOutcome.PLANNED, plan=plan, detail=plan.reason)

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


# --- FINAL -------------------------------------------------------------------


def _overlap_components(ranges: Sequence[FileRange]) -> List[List[FileRange]]:
    """Group files into maximal runs whose key ranges overlap, in key order.

    Overlap is the same relation rule B uses: strict interval overlap, except
    that a degenerate (min == max) file touching an edge is genuine overlap.
    A boundary shared by two non-degenerate files is a clean prior split.

    Raises TypeError when sort-key values are not mutually comparable.
    """
    components: List[List[FileRange]] = []
    high: Any = None
    degenerate_at_high = False
    for candidate in sorted(ranges, key=lambda r: (r.low, r.high)):
        joins = bool(components) and (
            candidate.low < high
            or (candidate.low == high and (candidate.is_degenerate or degenerate_at_high))
        )
        if not joins:
            components.append([candidate])
            high, degenerate_at_high = candidate.high, candidate.is_degenerate
            continue
        components[-1].append(candidate)
        if candidate.high > high:
            high, degenerate_at_high = candidate.high, candidate.is_degenerate
        elif candidate.high == high:
            degenerate_at_high = degenerate_at_high or candidate.is_degenerate
    return components


def _select_final_window(
    key_ranges: Sequence[Tuple[CompactionFile, Any, Any]],
    sort_column: str,
    disk_budget: int,
    threshold: float,
) -> Optional[CompactionPlan]:
    """The contiguous run of overlap components, within ``disk_budget``, that
    rewrites the most bytes needing work - or, when it fixes more, part of one
    component too large for any window.

    A component needs work when it overlaps (two or more files), is unsettled
    (a file under ``MIN_SIZE_BYTES``) or carries delete debt. A settled lone
    file inside the window is rewritten too: it is the bridge that keeps the
    window contiguous, which is what lets one global sort, rolled at TARGET,
    produce outputs disjoint from every file left alone - so no group
    boundaries ever reach the writer.

    A component larger than the budget cannot sit in any window. It is offered
    instead as its smallest files up to the budget: sorted, they still collapse
    many files into a few, though the outputs overlap the files it leaves. The
    smallest are the cheapest to fix and stop being smallest once merged, so the
    next pass moves on - the starvation that makes rule B seed at random does
    not arise. Files with no known key extent are not in ``key_ranges`` and so
    never selected here.
    """
    components = _overlap_components(
        [FileRange(entry=entry, low=low, high=high) for entry, low, high in key_ranges]
    )

    costs = [sum(member.size for member in component) for component in components]
    needs_work = [
        len(component) > 1
        or any(
            member.size < MIN_SIZE_BYTES or _delete_debt_ratio(member.entry) >= threshold
            for member in component
        )
        for component in components
    ]
    # Ranked by bytes needing work, then by files needing work - the second so
    # files of unknown size (which weigh 0 bytes) still count.
    values = [cost if work else 0 for cost, work in zip(costs, needs_work)]
    work_files = [len(c) if work else 0 for c, work in zip(components, needs_work)]

    # Costs are non-negative, so the cheapest-feasible start for each end is
    # also the most valuable: one two-pointer sweep finds the best window.
    best: Optional[Tuple[int, int, int, int]] = None  # (value, files, start, end)
    start = 0
    cost = value = files = 0
    for end in range(len(components)):
        cost += costs[end]
        value += values[end]
        files += work_files[end]
        while start <= end and cost > disk_budget:
            cost -= costs[start]
            value -= values[start]
            files -= work_files[start]
            start += 1
        if start > end or files == 0:
            continue
        if best is None or (value, files) > best[:2]:
            best = (value, files, start, end)

    # Candidates as (value, files, entries, reason), ranked like the window.
    candidates = []
    if best is not None:
        _, _, start, end = best
        # Trim settled components off both ends: they cost bytes and fix nothing.
        while not needs_work[start]:
            start += 1
        while not needs_work[end]:
            end -= 1
        window = [m.entry for component in components[start : end + 1] for m in component]
        # A lone file without debt has nothing to merge with.
        if len(window) > 1 or _delete_debt_ratio(window[0]) >= threshold:
            candidates.append(
                (best[0], best[1], window, f"final-window {end - start + 1} component(s)")
            )

    for component, component_cost in zip(components, costs):
        if component_cost <= disk_budget:
            continue
        partial = []
        total = 0
        for member in sorted(component, key=lambda m: m.size):
            if total + member.size > disk_budget:
                break
            partial.append(member.entry)
            total += member.size
        if len(partial) >= 2:
            candidates.append(
                (total, len(partial), partial, f"final-partial {len(partial)} of {len(component)}")
            )

    if not candidates:
        return None

    _, _, chosen, reason = max(candidates, key=lambda candidate: candidate[:2])
    total = sum(entry_size(entry) for entry in chosen)
    return CompactionPlan(
        strategy="combine-split",
        mode="sort-aware",
        files=chosen,
        reason=reason,
        sort_column=sort_column,
        expected_outputs=max(1, -(-total // TARGET_SIZE_BYTES)),
    )


def select_final_compaction_plan(
    entries: Sequence[CompactionFile],
    sort_column: Optional[str],
    key_ranges: Optional[Sequence[Tuple[CompactionFile, Any, Any]]],
    disk_budget: int,
    delete_debt_threshold: float = DELETE_DEBT_THRESHOLD,
) -> SelectionResult:
    """Choose the one pass ``OPTIMIZE TABLE x FINAL`` runs.

    With a sort key: one contiguous key window (``_select_final_window``),
    sorted and rolled at TARGET in a single commit, bounded by ``disk_budget``
    in the uncompressed-estimate unit - an upper bound on the lz4 sort spill.

    Without one: every unsettled file and every file with delete debt,
    concatenated. A brute merge never sorts, so it spills nothing and the disk
    budget does not apply.
    """
    if not entries:
        return SelectionResult(SelectionOutcome.NO_SNAPSHOT, detail="manifest is empty")

    if len(entries) == 1:
        return select_compaction_plan(
            entries, sort_column=sort_column, delete_debt_threshold=delete_debt_threshold
        )

    plan: Optional[CompactionPlan] = None
    if sort_column is None:
        selected = [
            entry
            for entry in entries
            if entry_size(entry) < SMALL_FILE_BYTES
            or _delete_debt_ratio(entry) >= delete_debt_threshold
        ]
        if len(selected) >= 2:
            plan = CompactionPlan(
                strategy="combine", mode="brute", files=selected, reason="final-brute"
            )
        elif selected and _delete_debt_ratio(selected[0]) >= delete_debt_threshold:
            plan = _select_delete_debt(selected, None, threshold=delete_debt_threshold)
    elif key_ranges:
        try:
            plan = _select_final_window(
                key_ranges, sort_column, disk_budget, delete_debt_threshold
            )
        except TypeError:
            return SelectionResult(
                SelectionOutcome.NOTHING_TO_DO,
                detail=f"values of sort key {sort_column} are not mutually comparable",
            )

    if plan is None:
        detail = (
            f"no file has a known extent on sort key {sort_column}"
            if sort_column is not None and not key_ranges
            else "no contiguous window within the disk budget needs work"
        )
        return SelectionResult(SelectionOutcome.NOTHING_TO_DO, detail=detail)
    return SelectionResult(SelectionOutcome.PLANNED, plan=plan, detail=plan.reason)
