"""Every name the reference catalog documents must be callable.

WHY THIS FILE EXISTS

GENERATE_SERIES was reported as "documented but not callable": it is listed in
`reference/signatures.py` under "Utility Functions", which reads as a scalar
call, and `SELECT GENERATE_SERIES(1, 3)` answered

    FunctionNotFoundError: Function **GENERATE_SERIES** cannot be found.

A documented name that cannot be called is worse than an undocumented one. The
caller does not learn that the capability is missing; they conclude they typed
it wrong, and go looking for the spelling that works. Eleven names in that same
table (TODAY, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEK, QUARTER, TITLECASE,
CHAR) had never been registered at all, and TITLECASE was being published as a
"related function" on seventeen live string functions — a dead link in shipped
documentation.

WHAT IS PINNED

Three separate claims, because they fail for different reasons:

1. `test_every_documented_name_is_callable_somewhere` — no name in
   `_DOCUMENTATION_CATEGORIES` is a dead end. A name is either a live scalar
   function (or alias) or is classified in `_NON_SCALAR_DOCUMENTED_NAMES`.

2. `test_non_scalar_classifications_are_not_stale` — a classification cannot be
   used to launder a dead name. Each entry is resolved against the LIVE registry
   for its namespace, so claiming a deleted function is "an aggregate" fails just
   as loudly as omitting it. This is the assertion that would have caught
   GENERATE_SERIES's real defect: it was reachable, but not in the namespace its
   category implied.

3. `test_published_related_functions_are_live` — every `related_functions` entry
   in the SHIPPED `function_signatures.json` names a function the same file
   documents. This is the one that reaches users directly.

The categories table deliberately spans namespaces — a reader looking under
"Date & Time Functions" wants CURRENT_DATE and `x::TIMESTAMP` in one place — so
"not in the function catalog" is not by itself an error. Being in NO namespace
is.
"""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from reference.signatures import _DOCUMENTATION_CATEGORIES
from reference.signatures import _NON_SCALAR_DOCUMENTED_NAMES
from reference.signatures import export_function_signatures

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _scalar_function_names() -> set:
    """Every name a scalar call can resolve: catalog names plus their aliases."""
    exported = export_function_signatures(include_aliases=True, include_internal=False)
    names = set(exported)
    for entry in exported.values():
        names.update(entry["aliases"])
    return names


def _documented_names() -> set:
    return {name for names in _DOCUMENTATION_CATEGORIES.values() for name in names}


def _classified_names() -> set:
    return {name for names in _NON_SCALAR_DOCUMENTED_NAMES.values() for name in names}


def test_every_documented_name_is_callable_somewhere():
    """No name in the category table is a dead end."""
    reachable = _scalar_function_names() | _classified_names()
    unreachable = sorted(_documented_names() - reachable)

    assert not unreachable, (
        f"reference/signatures.py documents {unreachable}, which are callable in no "
        "namespace. Either register them, or remove them from "
        "_DOCUMENTATION_CATEGORIES. If a name IS callable but not as a scalar "
        "function, classify it in _NON_SCALAR_DOCUMENTED_NAMES under the namespace "
        "it is reachable in."
    )


def test_documented_names_have_exactly_one_classification():
    """A name is classified under one namespace, so the failure message can name it."""
    seen: dict = {}
    duplicated = []
    for namespace, names in _NON_SCALAR_DOCUMENTED_NAMES.items():
        for name in names:
            if name in seen:
                duplicated.append((name, seen[name], namespace))
            seen[name] = namespace

    assert not duplicated, (
        f"_NON_SCALAR_DOCUMENTED_NAMES classifies {duplicated} under two namespaces. "
        "One name, one namespace."
    )


def test_classified_names_are_actually_documented():
    """A classification for a name no category lists is dead weight.

    It would go on passing after the name it describes had been removed, and the
    next person to read it would believe the catalog still documents that name.
    """
    orphans = sorted(_classified_names() - _documented_names())

    assert not orphans, (
        f"_NON_SCALAR_DOCUMENTED_NAMES classifies {orphans}, which no category in "
        "_DOCUMENTATION_CATEGORIES lists. Remove the classification, or add the name "
        "to the category it belongs to."
    )


def test_no_name_is_classified_when_it_is_a_scalar_function():
    """A name the function catalog answers for needs no classification.

    GENERATE_SERIES was classified as a table function until it gained a scalar
    overload. Leaving the classification behind would have hidden a later
    regression: the name would keep passing the reachability test on the strength
    of a namespace it no longer needs.
    """
    redundant = sorted(_classified_names() & _scalar_function_names())

    assert not redundant, (
        f"_NON_SCALAR_DOCUMENTED_NAMES classifies {redundant}, which the function "
        "catalog already answers for. Drop the classification — the scalar catalog "
        "is the check."
    )


def test_non_scalar_classifications_are_not_stale():
    """Each classified name resolves in the LIVE registry it claims.

    Without this, `_NON_SCALAR_DOCUMENTED_NAMES` becomes a second hand-maintained
    list that can drift exactly the way the first one did — an allowlist that
    launders a dead name into looking documented.
    """
    from opteryx.operators.aggregate.helpers import AGGREGATORS
    from opteryx.utils.query_parser import _TABLE_FUNCTIONS

    type_names = {
        name.upper()
        for name in json.loads(
            (_REPO_ROOT / "reference" / "types.json").read_text(encoding="utf8")
        )
    }

    registries = {
        "aggregate": set(AGGREGATORS),
        "table_function": set(_TABLE_FUNCTIONS),
        "type_name": type_names,
    }

    stale = []
    for namespace, registry in registries.items():
        for name in _NON_SCALAR_DOCUMENTED_NAMES[namespace]:
            if name not in registry:
                stale.append((namespace, name))

    assert not stale, (
        f"_NON_SCALAR_DOCUMENTED_NAMES claims {stale}, but the live registry for that "
        "namespace does not hold the name. The catalog is documenting something the "
        "engine does not have."
    )


def test_hidden_classifications_are_actually_hidden():
    """The "hidden" namespace means the exporter withholds the name, not that it is absent."""
    from reference.signatures import _HIDDEN_FUNCTIONS

    not_hidden = sorted(set(_NON_SCALAR_DOCUMENTED_NAMES["hidden"]) - _HIDDEN_FUNCTIONS)

    assert not not_hidden, (
        f"{not_hidden} are classified 'hidden' but are not in _HIDDEN_FUNCTIONS, so the "
        "exporter does not withhold them. Classify them under the namespace they are "
        "really reachable in."
    )


@pytest.mark.parametrize(
    "name, statement",
    [
        ("CAST", "SELECT CAST(1 AS VARCHAR)"),
        ("TRY_CAST", "SELECT TRY_CAST(1 AS VARCHAR)"),
    ],
)
def test_syntax_forms_run(name, statement):
    """The 'syntax' namespace has no registry, so the check is that it RUNS.

    A grammar form cannot be looked up in a table; executing it is the only
    evidence that the documented spelling is the one the dialect accepts.
    """
    assert name in _NON_SCALAR_DOCUMENTED_NAMES["syntax"]
    session = opteryx.session()
    rows = [morsel for morsel in session.execute_to_morsels(statement) if morsel is not None]
    assert rows, f"{name} is documented as syntax but `{statement}` returned nothing"


def test_syntax_classifications_are_all_exercised():
    """Every name in the 'syntax' namespace has a statement above proving it runs.

    Without this, adding a name to that namespace would silently opt it out of
    every check in this file — it has no registry to be resolved against.
    """
    exercised = {"CAST", "TRY_CAST"}
    unexercised = sorted(set(_NON_SCALAR_DOCUMENTED_NAMES["syntax"]) - exercised)

    assert not unexercised, (
        f"{unexercised} are classified as syntax but no statement in "
        "test_syntax_forms_run exercises them. Add one — a syntax form has no "
        "registry, so running it is the only check there is."
    )


def test_published_related_functions_are_live():
    """No `related_functions` entry in the SHIPPED catalog is a dead link.

    This is the assertion closest to the user: `related_functions` is published in
    reference/function_signatures.json, and TITLECASE — never a registered
    function — was listed on seventeen live string functions.
    """
    catalog = json.loads(
        (_REPO_ROOT / "reference" / "function_signatures.json").read_text(encoding="utf8")
    )
    documented = set(catalog)
    for entry in catalog.values():
        documented.update(entry["aliases"])
    # A cross-reference to a CAST or a table function is not a dead link — it
    # points at a real capability documented under another namespace. Only a name
    # reachable NOWHERE is.
    documented.update(_classified_names())

    dangling = {}
    for name, entry in catalog.items():
        for overload in entry["overloads"]:
            for related in overload["related_functions"]:
                if related not in documented:
                    dangling.setdefault(related, []).append(name)

    assert not dangling, (
        "reference/function_signatures.json points `related_functions` at names that "
        "are callable in no namespace: "
        f"{ {k: sorted(set(v)) for k, v in dangling.items()} }. "
        "Every published cross-reference must resolve."
    )


def test_generate_series_is_callable_as_a_scalar_function():
    """The reported defect, pinned by name.

    `SELECT GENERATE_SERIES(1, 3)` raised FunctionNotFoundError while
    reference/signatures.py listed the name under "Utility Functions".
    """
    assert "GENERATE_SERIES" in _scalar_function_names()

    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels("SELECT GENERATE_SERIES(1, 3) AS s"):
        if morsel is None:
            continue
        values.extend(morsel.to_arrow().to_pydict()["s"])

    assert values == [[1, 2, 3]], values


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
