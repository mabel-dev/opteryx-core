"""Accounting checks for the binder's OPERATOR_MAP.

The map's header comment states how many entries it carries. That number had gone
stale at least twice — it read 330 while the map held 317 — and a count nobody
maintains is worse than no count, because the next reader believes it. These tests
make the statement self-verifying: change the map and the comment has to follow, or
this goes red.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.planner.binder.operator_map import OPERATOR_MAP
from opteryx.types.logical_type import LogicalCategory as LC


def _documented_entry_count() -> int:
    """The count claimed by the header comment, read from the source."""
    import opteryx.planner.binder.operator_map as module

    source = open(module.__file__).read()
    match = re.search(r"the map carries (\d+) entries", source)
    assert match, "the OPERATOR_MAP header no longer states an entry count"
    return int(match.group(1))


def test_operator_map_entry_count_is_accurate():
    """The header's stated entry count must match the map."""
    documented = _documented_entry_count()
    assert len(OPERATOR_MAP) == documented, (
        f"OPERATOR_MAP holds {len(OPERATOR_MAP)} entries but its header comment claims "
        f"{documented}. Update the comment in opteryx/planner/binder/operator_map.py — "
        f"the number is load-bearing documentation, not decoration."
    )


def test_string_concat_is_homogeneous_only():
    """Only the three matching string pairs may carry StringConcat.

    RATIFIED/string-concatenation-requires-homogeneous-string-types. The rule is
    enforced by ABSENCE from this map, which makes it invisible — nothing reads as
    "mixed concat is refused", it just is. This says it out loud, so re-adding a
    mixed pair fails here instead of silently reopening the defect that let
    `CONCAT('p', b'a')` leak a Vector repr into user data.
    """
    string_categories = {LC.VARCHAR, LC.NVARCHAR, LC.VARBINARY}
    concat_pairs = {
        (left, right)
        for (left, right, operator) in OPERATOR_MAP
        if operator == "StringConcat"
    }

    assert concat_pairs == {(c, c) for c in string_categories}, (
        f"StringConcat must be declared for exactly the three same-type string pairs; "
        f"found {sorted((l.name, r.name) for l, r in concat_pairs)}. A MIXED pair here "
        f"makes the binder promise a result the kernel cannot produce — the kernel gate "
        f"is `lt_is_string && lt == rt` (draken/ops/kernels/binop_dispatch.cpp)."
    )
