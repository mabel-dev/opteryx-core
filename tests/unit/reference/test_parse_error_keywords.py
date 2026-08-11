"""
The keyword list the parse-error typo detector matches against must cover every
keyword the generated catalogs name.

`parse_error._KEYWORDS` used to be assembled at runtime, reading the clause, join
and operator spellings out of `reference/`. That package is generated for
documentation tooling and lives at the repo root, so it is not in the wheel: in a
deployed install the import raised ModuleNotFoundError from the error path, and
every parse failure was reported as `No module named 'reference'` instead of
saying which token the parser stopped on.

The list is now written down in the package, and this test is what keeps it
honest. A new clause, join or operator that names a word the list lacks fails
here - so the catalogs stay the source of truth for what is suggestable, without
anything having to be importable when a query fails.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.planner.parse_error import _KEYWORDS
from reference.clauses_catalog import CLAUSE_DEFINITIONS
from reference.joins_catalog import JOIN_DEFINITIONS
from reference.operator_catalog import OPERATOR_DEFINITIONS


def _catalogued_keywords():
    """Every alphabetic word the catalogs name, as `_keywords()` once collected them."""
    catalogued = set()
    for entry in list(CLAUSE_DEFINITIONS.values()) + list(JOIN_DEFINITIONS.values()):
        catalogued.update(entry["canonical_name"].split())
    # Clause and join entries are plain dicts; operator entries are OperatorDefinition
    # objects. The catalogs are generated, so this asymmetry is theirs to change, not
    # this test's to paper over.
    for entry in OPERATOR_DEFINITIONS.values():
        symbol = entry.sql_symbol or ""
        catalogued.update(part for part in symbol.split() if part.isalpha())
    return {word.upper() for word in catalogued if word.isalpha()}


def test_every_catalogued_keyword_is_suggestable():
    missing = sorted(_catalogued_keywords() - set(_KEYWORDS))

    assert not missing, (
        "opteryx/planner/parse_error.py::_KEYWORDS is missing keywords the "
        f"generated catalogs name: {missing}. Add them to the list - a keyword "
        "absent from it is not merely un-suggestable, the detector treats it as "
        "an unknown word and offers a near-miss FOR it."
    )


def test_keywords_are_upper_case_and_unique_enough_to_order():
    # The list is ordered most-used first because `suggest_alternative` keeps the
    # first best match; duplicates are harmless but say the ordering was not thought
    # about, and a lower-case entry would never match an upper-cased token.
    assert all(word.isupper() for word in _KEYWORDS)


def test_the_detector_reads_the_written_list():
    from opteryx.planner.parse_error import _keywords

    assert _keywords() is _KEYWORDS


def test_nothing_shipped_imports_the_reference_package():
    """`reference/` is a repo-root package; setup.py ships only the four below.

    An import of it from shipped code works in a source checkout and raises
    ModuleNotFoundError everywhere else, which is how a parse failure in
    production came back as `No module named 'reference'`. The catalogs are for
    generating documentation - anything the engine needs at runtime belongs in
    the package that runs.
    """
    import re
    from pathlib import Path

    imports_reference = re.compile(r"^\s*(?:from|import)\s+reference\b", re.MULTILINE)
    root = Path(__file__).resolve().parents[3]

    offenders = []
    for package in ("opteryx", "draken", "rugo", "skene"):
        for path in (root / package).rglob("*.py*"):
            if path.suffix not in {".py", ".pyx", ".pxd"} or "tests" in path.parts:
                continue
            if imports_reference.search(path.read_text(encoding="utf8", errors="ignore")):
                offenders.append(str(path.relative_to(root)))

    assert not offenders, (
        f"shipped modules importing the unshipped `reference` package: {offenders}"
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
