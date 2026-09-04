# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compaction planning — selecting which data files a compaction pass rewrites.

This is the engine-side home of the selection rules that used to live in
opteryx-catalog's ``DatasetCompactor``. See
``docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md``; the short version is that
choosing which files to merge is reasoning over statistics, which is planning,
and planning belongs with the planner (contract §1).

Selection reads a ``Manifest`` and emits a ``CompactionPlan``. It moves no data
and touches no storage: every input is a manifest statistic the binder already
fetched. Execution is a separate concern and is built from this plan by
``CompactionPlanningStrategy``.

All three rule families live here: brute merge, sort-aware decluster/bin-pack,
and delete debt. ``CompactionPlanningStrategy`` turns the chosen plan into a
narrowed scan manifest, and ``operators/compaction_commit`` writes and commits
it. The catalog's compactor is gone — this is the only selector.
"""

from .constants import DELETE_DEBT_THRESHOLD
from .constants import MAX_FILE_SIZE_BYTES
from .constants import MIN_FILE_SIZE_BYTES
from .constants import MIN_SIZE_BYTES
from .constants import PASS_BUDGET_BYTES
from .constants import PASS_BUDGET_RATIO
from .constants import SMALL_FILE_BYTES
from .constants import SORT_AWARE_FLOOR_BYTES
from .constants import TARGET_SIZE_BYTES
from .selection import CompactionPlan
from .selection import FileRange
from .selection import SelectionOutcome
from .selection import SelectionResult
from .selection import entry_size
from .selection import select_compaction_plan

__all__ = [
    "CompactionPlan",
    "FileRange",
    "SelectionOutcome",
    "SelectionResult",
    "entry_size",
    "select_compaction_plan",
    "DELETE_DEBT_THRESHOLD",
    "MAX_FILE_SIZE_BYTES",
    "MIN_FILE_SIZE_BYTES",
    "MIN_SIZE_BYTES",
    "PASS_BUDGET_BYTES",
    "PASS_BUDGET_RATIO",
    "SMALL_FILE_BYTES",
    "SORT_AWARE_FLOOR_BYTES",
    "TARGET_SIZE_BYTES",
]
