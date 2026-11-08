"""
SHOW FUNCTIONS [LIKE '<pattern>' | ILIKE '<pattern>' | WHERE <condition>]

Lists every function name a query can call, one row each: `name`, `kind`
(scalar, aggregate or window) and a one-line `summary`. It used to be refused
outright; it is supported so clients have something to call, and so that a
static name list (opteryx-sqlalchemy's FUNCTION_NAMES, which Apache Superset's
SQL Lab autocomplete reads) can be generated from it.

The rows come from the engine's registries at runtime (see
opteryx/managers/virtual_datasets/functions_data.py); the static lists today
come from reference/*.json. The test that matters most here holds those two
to each other, so the statement and the exported catalogs cannot drift.
"""

import json
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

_REFERENCE = Path(__file__).resolve().parents[3] / "reference"

# The rule opteryx-sqlalchemy's scripts/generate_function_names.py applies to
# reference/, restated so this test fails here, in the engine, when the two
# sources stop agreeing rather than in a client's generated list.
_PLANNER_FUNCTIONS = {"DATE_TRUNC", "DATETRUNC", "GROUPING"}
_CALL = re.compile(r"^([A-Z_][A-Z0-9_]*)\s*\(")


def _reference_names() -> set:
    scalars = json.loads((_REFERENCE / "function_signatures.json").read_text())
    aggregates = json.loads((_REFERENCE / "aggregates.json").read_text())
    windows = json.loads((_REFERENCE / "windows.json").read_text())

    names = set(_PLANNER_FUNCTIONS)
    for name, entry in scalars.items():
        if entry.get("lifecycle", {}).get("status", "active") != "active":
            continue
        names.add(name)
        names.update(entry.get("aliases") or [])
    called = [v for v in aggregates.values() if v.get("status") == "active"]
    called += list(windows["functions"].values())
    for entry in called:
        for form in entry.get("sql_forms") or []:
            match = _CALL.match(form)
            if match:
                names.add(match.group(1))
    return {name for name in names if not name.startswith("_")}


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def _rows(sql) -> list:
    session = opteryx.session()
    columns: dict = {}
    for morsel in session.execute_to_morsels(sql):
        for name in morsel.column_names:
            values = columns.setdefault(_text(name), [])
            if morsel.num_rows:
                values.extend(_text(v) for v in morsel.column(name).to_pylist())
    if not columns:
        return []
    assert list(columns) == ["name", "kind", "summary"], list(columns)
    return list(zip(columns["name"], columns["kind"], columns["summary"]))


def test_show_functions_lists_what_the_reference_catalogs_describe():
    rows = _rows("SHOW FUNCTIONS")
    names = [name for name, _kind, _summary in rows]

    assert len(names) == len(set(names)), "a name is listed twice"
    assert names == sorted(names)
    assert set(names) == _reference_names()


def test_every_row_has_a_kind_and_a_summary():
    for name, kind, summary in _rows("SHOW FUNCTIONS"):
        assert kind in ("scalar", "aggregate", "window"), (name, kind)
        assert summary, name


def test_kinds():
    kinds = {name: kind for name, kind, _summary in _rows("SHOW FUNCTIONS")}

    assert kinds["UPPER"] == "scalar"
    assert kinds["UCASE"] == "scalar"  # an alias
    assert kinds["DATE_TRUNC"] == "scalar"  # rewritten by the planner
    assert kinds["COUNT"] == "aggregate"
    assert kinds["COUNT_DISTINCT"] == "aggregate"
    assert kinds["GROUPING"] == "aggregate"
    assert kinds["ROW_NUMBER"] == "window"


def test_an_alias_carries_its_functions_summary():
    summaries = {name: summary for name, _kind, summary in _rows("SHOW FUNCTIONS")}
    assert summaries["UCASE"] == summaries["UPPER"]


def test_window_kind_matches_the_window_registry():
    from opteryx.operators.window.helpers import WINDOW_FUNCTIONS

    rows = _rows("SHOW FUNCTIONS WHERE kind = 'window'")
    assert {name for name, _kind, _summary in rows} == set(WINDOW_FUNCTIONS)


@pytest.mark.parametrize(
    "sql",
    [
        "SHOW FUNCTIONS LIKE 'DATE%'",
        "SHOW FUNCTIONS LIKE 'date%'",  # case-insensitive, like every SHOW filter
        "SHOW FUNCTIONS ILIKE 'Date%'",
    ],
)
def test_like_filters_on_the_name(sql):
    names = [name for name, _kind, _summary in _rows(sql)]
    expected = sorted(n for n, _k, _s in _rows("SHOW FUNCTIONS") if n.startswith("DATE"))

    assert names == expected
    assert "DATE_TRUNC" in names


def test_like_with_no_match_is_empty():
    assert _rows("SHOW FUNCTIONS LIKE '%zz'") == []


def test_a_bare_pattern_is_refused():
    """`SHOW FUNCTIONS 'x'` (no LIKE) parses; it is refused rather than guessed at."""
    with pytest.raises(UnsupportedSyntaxError, match="LIKE"):
        _rows("SHOW FUNCTIONS 'DATE%'")


def test_functions_relation_is_not_addressable_by_name():
    """`$functions` is internal-only: SHOW FUNCTIONS is its single surface."""
    with pytest.raises(UnsupportedSyntaxError, match="SHOW FUNCTIONS"):
        _rows("SELECT * FROM $functions")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
