"""
reference/types.json documents only the spellings CAST actually accepts.

It used to carry a hand-written alias table (DOUBLE, BIGINT, INT, TEXT, STRING, BLOB,
BYTES) that the canonical-only ruling refuses as CAST targets, so dbt read the
reference, wrote `CAST(x AS DOUBLE)`, and was told DOUBLE is not a type.
"""

import json
from pathlib import Path

import pytest

from opteryx.exceptions import SqlError
from opteryx.planner.logical_planner.logical_planner_builders import _normalize_cast_type

_CATALOG = json.loads(
    (Path(__file__).resolve().parents[3] / "reference" / "types.json").read_text(encoding="utf8")
)
_DOCUMENTED = sorted(
    {spelling for entry in _CATALOG.values() for spelling in entry["accepted_spellings"]}
)
_REFUSED = ("double", "bigint", "int", "text", "string", "blob", "bytes")


@pytest.mark.parametrize("spelling", _DOCUMENTED)
def test_every_documented_spelling_is_a_cast_target(spelling):
    _normalize_cast_type(spelling, refused_as="a CAST target")


@pytest.mark.parametrize("spelling", _REFUSED)
def test_refused_spellings_are_not_documented(spelling):
    assert spelling not in _DOCUMENTED
    with pytest.raises(SqlError):
        _normalize_cast_type(spelling, refused_as="a CAST target")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
