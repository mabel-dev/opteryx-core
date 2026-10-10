"""
The predicate-subquery fuzzer's defect register.

WHAT THIS IS, AND WHAT IT IS NOT
--------------------------------
A register of engine defects this fuzzer has found and reported. NOT an
allowlist of "errors that are fine". The difference is enforced: every entry
carries a minimal `repro` that
`test_sql_fuzzer_predicate_subquery.py::test_registered_defect_still_reproduces`
executes on every run and requires to STILL FAIL in the recorded way. A fixed
defect turns this file RED, and the only way back to green is to delete the
entry — which puts the construct straight back into ordinary fuzzing.

A WRONG ANSWER IS NEVER ABSORBED. `match()` only ever looks at EXCEPTIONS. An
oracle violation always fails the run, because a substring match on "the results
differed" would swallow every future wrong answer of that oracle's shape.
Wrong-answer entries (`error_type="WrongAnswer"`) work the other way round: each
is pinned by its own explicit test asserting the broken behaviour, and
`applicable_oracles()` declines the affected oracle on the exact query SHAPE
that triggers it, naming the entry. That exclusion is visible in code, is scoped
to a shape rather than to a message, and disappears with the entry.

THE HANG THIS FILE USED TO CARRY IS FIXED
-----------------------------------------
`HANGS` recorded one entry: an IN-subquery outside a top-level conjunct made the
planner loop forever, because `_build_filter_join` discarded the found flag from
`_split_out` and `_rewrite_filters` re-found the node it had failed to remove.
That is now a guard at the top of `_build_filter_join` and an
UnsupportedSyntaxError naming the position, so the list is gone with it — along
with `exists-outside-a-top-level-conjunct-blames-the-correlation`, which was the
same root cause wearing a misleading message (the first pass lifted the
correlation out, the second pass found none left, and blamed the user for it).

Those positions are now pinned by `NESTED_POSITIONS` in
`subquery_grammar.py`, checked in a SUBPROCESS with a deadline. The subprocess is
not superstition: if the guard is ever removed, an in-process check would hang
the whole suite with no output instead of failing it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet
from typing import List
from typing import Optional


@dataclass(frozen=True)
class RegisteredDefect:
    id: str
    repro: str
    #: Exception class NAME, or "WrongAnswer" for a defect that raises nothing.
    error_type: str
    #: Substring the exception message must contain. "" matches on type alone.
    signature: str
    detail: str
    #: A tag the generated case must carry for this entry to absorb its failure.
    #:
    #: The honest limitation of a message-substring register is that a genuinely
    #: NEW bug producing an already-registered message is absorbed into an old
    #: entry instead of failing the run. Where the engine's message carries no
    #: query-specific text — `a build-side join key the engine could not resolve
    #: here` names nothing at all — the substring is the whole predicate, and it
    #: is far too wide. So an entry may additionally require the case to have
    #: been generated in the SHAPE the defect is about. The same failure from any
    #: other shape then fails the run, which is what should happen.
    requires_tag: Optional[str] = None


REGISTER: List[RegisteredDefect] = [
    # ─────────────────────────────────────────────────────────────────────────
    # WRONG ANSWERS — no exception, just the wrong rows.
    # ─────────────────────────────────────────────────────────────────────────
    # `correlated-scalar-subquery-drops-unmatched-outer-rows` was registered here.
    # FIXED: an outer row with no matching inner group now receives the
    # aggregate's empty-set value (`(SELECT COUNT(*) ...) = 0` returns Mercury and
    # Venus), so the pin test and the COUNT exclusion in applicable_oracles() are
    # gone and subquery_matches_join_rewrite covers correlated COUNT again.
    # ─────────────────────────────────────────────────────────────────────────
    # ERRORS — the query is refused, but for the wrong reason or with an
    # internal message. Each is a real limitation; what is registered is that
    # the DIAGNOSIS misdescribes it.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="in-subquery-under-an-expression-blames-an-outer-scope",
        repro=(
            "SELECT sq_o.name FROM testdata.planets AS sq_o WHERE sq_o.id + 0 IN "
            "(SELECT sq_i.planetId FROM testdata.satellites AS sq_i)"
        ),
        error_type="UnsupportedSyntaxError",
        signature="belongs to a scope further out",
        detail=(
            "`<expression> IN (subquery)` — as opposed to `<column> IN (subquery)` — is "
            "refused with `A correlated EXISTS/IN subquery correlates on `None`, which belongs "
            "to a scope further out than the subquery enclosing it.` There is no correlation "
            "in the repro at all and no outer scope beyond the one it is in; the `None` in the "
            "message is a column that was never resolved.\n"
            "\n"
            "The limitation is that the membership test's left operand must be a plain column "
            "reference, because it becomes a join key. Saying so would take the reader "
            "straight to the fix (`WHERE sq_o.id IN (...)`); the current message describes a "
            "scoping problem that does not exist."
        ),
    ),
]


def match(error: BaseException, tags: FrozenSet[str] = frozenset()) -> Optional[RegisteredDefect]:
    """The register entry this exception belongs to, if any.

    Matches on exception class NAME rather than on the class itself, so the
    register does not have to import every Opteryx exception type, and on a
    message substring. An entry with an empty signature matches on type alone.

    `tags` are the generated case's shape tags. An entry carrying
    `requires_tag` absorbs a failure only from a case of that shape — see the
    field's own note for why a message substring is not enough on its own.
    """
    name = type(error).__name__
    message = str(error)
    for defect in REGISTER:
        if defect.error_type != name:
            continue
        if defect.signature and defect.signature not in message:
            continue
        if defect.requires_tag is not None and defect.requires_tag not in tags:
            continue
        return defect
    return None
