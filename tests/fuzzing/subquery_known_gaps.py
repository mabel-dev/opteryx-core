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

Those positions are now pinned by `POSITIONS_REFUSED_PROMPTLY` in
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
    RegisteredDefect(
        id="correlated-scalar-subquery-drops-unmatched-outer-rows",
        repro=(
            "SELECT sq_o.id FROM testdata.planets AS sq_o WHERE "
            "(SELECT COUNT(*) FROM testdata.satellites AS sq_i "
            "WHERE sq_i.planetId = sq_o.id) = 0"
        ),
        error_type="WrongAnswer",
        signature="",
        detail=(
            "A correlated scalar subquery is decorrelated to an INNER join, so an outer row "
            "with no matching inner group is DROPPED instead of receiving the aggregate's "
            "empty-set value. `_decorrelate` in "
            "opteryx/planner/optimizer/strategies/decorrelate_subquery.py sets "
            "`join.type = \"inner\" if local_pairs else \"cross join\"`; the correlated case "
            "always has local pairs, so it is always INNER.\n"
            "\n"
            "Mercury (id 1) and Venus (id 2) have no satellites. COUNT over an empty group is "
            "0, not NULL, so `(SELECT COUNT(*) ...) = 0` must return both. It returns "
            "nothing.\n"
            "\n"
            "Three independent spellings, all measured on testdata.planets:\n"
            "  (SELECT COUNT(*) ... ) = 0                     -> []      expected [(1,), (2,)]\n"
            "  (SELECT COUNT(*) ... ) < 1                     -> []      expected [(1,), (2,)]\n"
            "  (SELECT MAX(sq_i.radius) ... ) IS NULL         -> []      expected [(1,), (2,)]\n"
            "  COALESCE((SELECT MAX(...)), -1.0) < 0.0        -> []      expected [(1,), (2,)]\n"
            "and `sq_o.id > (SELECT COUNT(*) ...)` returns 3 rows where 5 are correct.\n"
            "\n"
            "WHY IT HID: under a bare comparison the two errors cancel. MIN/MAX/SUM/AVG over "
            "an empty group are NULL, a comparison against NULL is UNKNOWN, and WHERE drops "
            "that row — the same row the INNER join already dropped. Only COUNT (whose empty-"
            "group value is 0, not NULL) and predicates that ASK about the NULL (IS NULL, "
            "COALESCE) can tell the two apart. That is why `subquery_matches_join_rewrite` "
            "runs happily on MIN/MAX/SUM/AVG and stands down only on COUNT.\n"
            "\n"
            "THE FIX IS DESIGN-IMPACTING, which is why this is registered rather than fixed: "
            "the join has to become LEFT OUTER, and the substituted value has to carry the "
            "aggregate's empty-set value (COALESCE(x, 0) for COUNT). LEFT OUTER is a different "
            "operator with different cost, so which join the correlated scalar path uses is "
            "the architect's call, not this fuzzer's."
        ),
    ),
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
    RegisteredDefect(
        id="comparing-an-aggregate-column-from-an-empty-left-join-leg-raises",
        repro=(
            "SELECT sq_o.id FROM testdata.planets AS sq_o LEFT JOIN "
            "(SELECT sq_i.planetId AS sq_key, MAX(sq_i.id) AS sq_agg "
            "FROM testdata.satellites AS sq_i WHERE sq_i.radius > 1000000.0 "
            "GROUP BY sq_i.planetId) AS sq_j ON sq_j.sq_key = sq_o.id "
            "WHERE sq_j.sq_agg > 0"
        ),
        error_type="RuntimeError",
        signature="ExprFilterOperator: predicate evaluation failed (err_op=11)",
        requires_tag="inner-filter:empty",
        detail=(
            "A LEFT JOIN whose right leg is an EMPTY grouped aggregate produces an all-NULL "
            "aggregate column, and COMPARING that column raises out of the native filter "
            "instead of evaluating to UNKNOWN. Every outer row should simply be dropped by "
            "the WHERE.\n"
            "\n"
            "NOT A SUBQUERY BUG. The repro contains no subquery predicate at all — this fuzzer "
            "found it because the `corr_scalar` join rewrite builds exactly this shape, and the "
            "corpus carries an inner filter that matches nothing so the empty-set branch is "
            "reachable. It is a defect in the JOIN path.\n"
            "\n"
            "MEASURED — what does and does not trip it (satellites has no radius > 1000000):\n"
            "  LEFT JOIN, empty grouped aggregate, `sq_j.sq_agg > 0`            RAISES\n"
            "  LEFT JOIN, empty grouped aggregate, `sq_o.id < sq_j.sq_agg`      RAISES\n"
            "  ... with COUNT(*) instead of MAX                                 RAISES\n"
            "  LEFT JOIN, empty grouped aggregate, `sq_j.sq_agg IS NULL`        ok (9 rows)\n"
            "  LEFT JOIN, empty grouped aggregate, no filter at all             ok (9 rows)\n"
            "  LEFT JOIN, NON-empty grouped aggregate, same comparison          ok (5 rows)\n"
            "  LEFT JOIN, empty GROUP BY leg with NO aggregate, same comparison ok (0 rows)\n"
            "  INNER JOIN, empty grouped aggregate, same comparison             ok (0 rows)\n"
            "  CROSS JOIN, empty UNGROUPED aggregate, same comparison           ok (0 rows)\n"
            "\n"
            "So it is specifically the AGGREGATE output column of an EMPTY grouped leg, reached "
            "by a COMPARISON. `IS NULL` over the same column is fine, which says the column is "
            "present and marked null — what the comparison kernel cannot handle is the buffer "
            "behind it. That is the `ptr.data == NULL` family the vector-model rules warn "
            "about, arriving through the join rather than through a scan."
        ),
    ),
    RegisteredDefect(
        id="skip-level-exists-over-two-aliased-derived-relations-is-refused",
        repro=(
            "SELECT sq_o.id FROM testdata.planets AS sq_o WHERE EXISTS "
            "(SELECT 1 FROM (SELECT planetId AS k FROM testdata.satellites) AS sq_i "
            "WHERE sq_i.k = sq_o.id AND EXISTS "
            "(SELECT 1 FROM (SELECT planetId AS k FROM testdata.satellites) AS sq_n "
            "WHERE sq_n.k = sq_i.k))"
        ),
        error_type="NotSupportedError",
        signature="a build-side join key the engine could not resolve here",
        requires_tag="nested-existence",
        detail=(
            "An EXISTS nested inside an EXISTS, correlated to the MIDDLE scope, is refused "
            "when BOTH nested relations are derived tables that expose the correlation key "
            "under a projection ALIAS. The refusal comes from the compiler "
            "(opteryx/managers/execution/compiler.py:206), so it survives planning and dies at "
            "compile time.\n"
            "\n"
            "The repro is an exact no-op: `sq_i` is itself a row of the same relation with the "
            "same key, so the inner EXISTS is satisfied whenever `sq_i.k` is non-NULL, which "
            "the enclosing correlation already guarantees. Deleting the inner EXISTS gives the "
            "same seven planets. That is what makes this a clean find — the query the engine "
            "refuses is provably equivalent to one it answers.\n"
            "\n"
            "MEASURED — the alias on BOTH sides is what does it:\n"
            "  middle `(SELECT planetId AS k …)`, inner `(SELECT planetId AS k …)`   REFUSED\n"
            "  middle `(SELECT planetId AS ka …)`, inner `(SELECT planetId AS kb …)` REFUSED\n"
            "  middle `(SELECT planetId AS k …)`, inner `(SELECT * …)`               ok\n"
            "  middle `(SELECT * …)`,             inner `(SELECT planetId AS k …)`   ok\n"
            "  middle `(SELECT * …)`,             inner `(SELECT * …)`               ok\n"
            "  middle base relation,              inner base relation                ok\n"
            "  middle `(SELECT planetId + 0 AS k …)`, inner base relation            ok\n"
            "so it is not the derived table and it is not the computed column — it is a "
            "renamed key on both legs of a deferred (skip-level) correlation.\n"
            "\n"
            "`_defer_existence_to_ancestor` in decorrelate_subquery.py is what carries a "
            "correlation that names a relation below no join it can see, and the message names "
            "a BUILD-SIDE key, so the deferred pair is reaching the ancestor join with a "
            "reference the operator cannot attribute to either leg."
        ),
    ),
    RegisteredDefect(
        id="scalar-subquery-in-having-leaks-a-native-keyerror",
        repro=(
            "SELECT sq_o.id FROM testdata.planets AS sq_o GROUP BY sq_o.id "
            "HAVING COUNT(*) > (SELECT MIN(sq_i.planetId) FROM testdata.satellites AS sq_i)"
        ),
        error_type="KeyError",
        signature="which the stream does not carry",
        detail=(
            "A scalar subquery in HAVING is not planned: the decorrelated value column is "
            "never routed to the aggregate's output stream, and the failure surfaces from the "
            "native engine as a bare KeyError naming a `$derived_...` identity.\n"
            "\n"
            "A KeyError is not a user-facing error — it names an internal column identity the "
            "caller has no way to act on, and it is an exception class no caller can "
            "reasonably catch. Either HAVING joins WHERE as a supported position, or it is "
            "refused in the planner with the same UnsupportedSyntaxError the SELECT list gets "
            "(`Scalar subqueries are supported in the **WHERE** clause but not yet in the "
            "**SELECT** list`). Leaking a KeyError is neither."
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
