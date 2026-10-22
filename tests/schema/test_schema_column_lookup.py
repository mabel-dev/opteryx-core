"""Case-insensitive `RelationSchema.column` answers from a per-schema lowercase index.

The index is built on first use and must give exactly what the linear scan it replaced
gave: the FIRST column, in column order, carrying the name - a column's own name
before its aliases - and None for an absent name. It must never outlive the columns it
indexed: `with_columns` (and so `without_column`) drops it.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema


def build(*specs):
    """A schema of columns minted in one query's ColumnTable: (name, aliases) per column."""
    columns = PlanContext().columns
    return RelationSchema(
        name="t",
        columns=tuple(
            columns.relation_column("t", name, column_type=INT64, aliases=list(aliases))
            for name, aliases in specs
        ),
    )


def linear_scan(schema, name):
    """The lookup as it was written before the index: the reference answer."""
    name_lower = name.lower()
    for col in schema.columns:
        if col.name.lower() == name_lower:
            return col
        if col.aliases:
            for alias in col.aliases:
                if alias.lower() == name_lower:
                    return col
    return None


def test_every_spelling_matches_the_linear_scan():
    schema = build(("Id", ()), ("Name", ("Label", "TITLE")), ("value", ("ID",)), ("other", ()))
    probes = ["id", "ID", "Id", "name", "NAME", "label", "Label", "title", "Value", "VALUE", "other", "missing", ""]
    for probe in probes:
        assert schema.column(probe, case_insensitive=True) is linear_scan(schema, probe), probe


def test_the_first_column_wins_and_a_name_beats_a_later_alias():
    schema = build(("a", ("shared",)), ("shared", ()), ("A", ()))
    assert schema.column("SHARED", case_insensitive=True) is schema.columns[0]
    assert schema.column("a", case_insensitive=True) is schema.columns[0]


def test_case_sensitive_lookup_is_unchanged():
    schema = build(("Id", ("Key",)), ("id", ()))
    assert schema.column("id") is schema.columns[1]
    assert schema.column("Key") is schema.columns[0]
    assert schema.column("key") is None


def test_with_columns_does_not_keep_the_old_index():
    schema = build(("a", ()), ("b", ()), ("c", ()))
    assert schema.column("B", case_insensitive=True) is schema.columns[1]  # index now built

    narrowed = schema.with_columns(schema.columns[:1])
    assert narrowed.column("B", case_insensitive=True) is None
    assert narrowed.column("A", case_insensitive=True) is narrowed.columns[0]

    assert schema.without_column("b").column("B", case_insensitive=True) is None
    assert schema.column("B", case_insensitive=True) is schema.columns[1]  # the original is untouched


def test_a_clone_answers_the_same_and_diverges_cleanly():
    schema = build(("a", ()), ("b", ("bee",)))
    assert schema.column("BEE", case_insensitive=True) is schema.columns[1]  # index built, then cloned
    clone = schema.clone()
    assert clone.column("BEE", case_insensitive=True) is clone.columns[1]
    assert clone.with_columns(()).column("BEE", case_insensitive=True) is None


def test_the_index_is_not_part_of_the_schema_value():
    one = build(("a", ()))
    one.column("A", case_insensitive=True)
    assert repr(one) == "RelationSchema(name='t', num_columns=1)"
    assert one.with_columns(one.columns) == one


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
