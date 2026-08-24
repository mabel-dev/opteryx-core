# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for _fuse_json_extractions' admission rule.

Parsing dominates JSON extraction, so the compiler materializes the `->`/`->>`
outputs of one filter with a single parse per row and rewrites the extraction
nodes to read those columns. The rule for WHEN to do that is the subject here.

It counts EXTRACTIONS, not distinct paths. Repeated occurrences of one path are
the same bound expression and so share one out-identity — but the lowerer emits
one extract instruction per textual occurrence, so `x->>'k' < 'a' OR x->>'k' >
'z'` parses every document twice. Counting distinct paths saw a single entry
there and declined, leaving a predicate with N references to one path paying N
parses per row.

The fixture's `dict.string` is present on two of six rows and absent on the
rest, so these queries also cover a null-bearing extraction.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "..", "..", ".."))

import opteryx
from opteryx.managers.execution import compiler as _compiler

STRUCTS = "testdata.flat.struct"


def _layout_growth(sql: str) -> list:
    """Columns each _fuse_json_extractions call added to its pipeline's layout.

    A non-zero entry means that call materialized extractions into a shared
    JsonExtractMulti operator; zero means it declined and the extractions stayed
    inline in the filter program.
    """
    growth = []
    original = _compiler._Compiler._fuse_json_extractions

    def wrapper(self, p, eval_nodes, layout):
        before = len(layout)
        grown = original(self, p, eval_nodes, layout)
        growth.append(len(grown) - before)
        return grown

    _compiler._Compiler._fuse_json_extractions = wrapper
    try:
        session = opteryx.session()
        for _ in session.execute_to_morsels(sql):
            pass
    finally:
        _compiler._Compiler._fuse_json_extractions = original
    return growth


def _rows(sql: str) -> int:
    session = opteryx.session()
    return sum(m.num_rows for m in session.execute_to_morsels(sql))


def test_one_path_referenced_twice_is_materialized():
    """Two references, one path. Both point at the same out-identity, so the
    shared column is one wide — but it replaces two parses per row."""
    sql = f"SELECT * FROM {STRUCTS} WHERE dict->>'string' < 'a' OR dict->>'string' > 'z'"
    assert max(_layout_growth(sql)) == 1


def test_one_path_referenced_once_is_left_inline():
    """A single extraction has nothing to share with; materializing it would add
    an operator and a column to save no parses at all."""
    sql = f"SELECT * FROM {STRUCTS} WHERE dict->>'string' > 'z'"
    assert max(_layout_growth(sql)) == 0


def test_two_distinct_paths_are_materialized():
    """The case the rule already admitted — pinned so widening it to repeated
    single paths did not cost the original one."""
    sql = f"SELECT * FROM {STRUCTS} WHERE dict->>'string' > 'z' AND dict->>'once' > 'z'"
    assert max(_layout_growth(sql)) == 2


def test_materialization_does_not_change_the_answer():
    """The fused path and the inline path must agree — including on the four rows
    whose key is absent, where the extraction is NULL and neither comparison is
    TRUE."""
    both = f"SELECT * FROM {STRUCTS} WHERE dict->>'string' < 'a' OR dict->>'string' > 'zzz'"
    assert _rows(both) == 0

    either = f"SELECT * FROM {STRUCTS} WHERE dict->>'string' < 'a' OR dict->>'string' > 'a'"
    assert _rows(either) == 2   # the two rows that carry the key; NULLs are not TRUE


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
