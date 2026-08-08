"""The join fuzzer's defect register.

WHAT THIS IS, AND WHAT IT IS NOT
--------------------------------
A register of engine defects the fuzzer has already found and reported. It is NOT
an allowlist of "errors that are fine", and it is NOT a way to make an inconvenient
failure go away.

The difference is enforced, not asserted. Every entry carries a minimal `repro` that
`test_sql_fuzzer_join.py::test_registered_defect_still_reproduces` executes on every
run and requires to STILL FAIL in the recorded way. So:

  * while a defect is open, the fuzzer does not spend every run re-reporting it;
  * the moment it is fixed the register goes RED and the entry must be deleted,
    which puts the construct straight back into ordinary fuzzing;
  * and an entry cannot be added without a repro that demonstrably fails.

A WRONG ANSWER IS NEVER ABSORBED. The join-algebra identities and the two
metamorphic oracles raise `AssertionError`, which nothing here matches: there is no
entry, and no possible entry, that can silence one. The gate this register gives is
"no NEW failure classes", over a fuzzer whose wrong-answer detection is untouched.

Deliberately kept to defects that are NOT the join fuzzer's to fix. Everything the
join rewrite itself found — the FULL OUTER ARRAY probe payload, the DECIMAL join-key
coercion, ILIKE on VARBINARY — was fixed rather than registered, and is pinned by a
`test_regression_*` in the fuzzer module.

THE HONEST LIMITATION: signature matching is by message substring, so a genuinely
new bug producing an already-registered message would be absorbed into that entry's
count rather than failing the run. Signatures are kept as narrow as the engine's
messages allow, and per-entry hit counts are printed at the end of every run so a
class that suddenly gets much louder is visible.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List
from typing import Optional


@dataclass(frozen=True)
class RegisteredDefect:
    id: str
    repro: str
    #: Exception class NAME (matched by name so the register need not import every
    #: Opteryx exception type).
    error_type: str
    #: Substring the exception message must contain. "" matches on type alone.
    signature: str
    detail: str


REGISTER: List[RegisteredDefect] = [
    # ─────────────────────────────────────────────────────────────────────────
    # WRONG ANSWER — no exception, just the wrong rows. Absorbed by NOTHING: the
    # `match()` below refuses AssertionError outright, so an oracle violation is
    # always fatal. A WrongAnswer entry is documentation plus a structural
    # exclusion, and is pinned by its own `test_wrong_answer_*` test.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="nan-join-keys-match-themselves-in-every-join-but-inner",
        repro=(
            "SELECT COUNT(*) FROM testdata.satellites AS r0 LEFT SEMI JOIN "
            "testdata.satellites AS r1 ON r0.albedo = r1.magnitude"
        ),
        error_type="WrongAnswer",
        signature="",
        detail=(
            "A NaN join key matches itself in every join type EXCEPT INNER, so the "
            "same ON condition gets two different answers depending on the join.\n"
            "`testdata.satellites` has no NULL albedo or magnitude, but 6 of each are "
            "NaN. Under IEEE, NaN = NaN is FALSE, so nothing should match:\n"
            "                 engine   truth\n"
            "  INNER               0       0   correct\n"
            "  LEFT SEMI           6       0   WRONG\n"
            "  LEFT ANTI         171     177   WRONG\n"
            "  LEFT              207     177   WRONG\n"
            "  RIGHT             207     177   WRONG\n"
            "  FULL              378     177   WRONG\n"
            "`CROSS JOIN ... WHERE r0.albedo = r1.magnitude` returns 0, agreeing with "
            "INNER and with the truth. The shape reads as the hash/existence path "
            "treating two NaNs as equal because their bit patterns are equal, while "
            "INNER applies a real comparison.\n"
            "A NaN-free float pair on the same relations is correct in every join "
            "type, so this is NaN-specific, not a float-key problem.\n"
            "Which NaN semantics to adopt is the architect's call and is ALREADY open "
            "— see the single-table register's "
            "`nan-rows-fall-outside-every-predicate-bucket`, and the recorded NaN "
            "divergence in sort ordering. That INNER and SEMI must agree with each "
            "other is not a matter of semantics.\n"
            "Pinned by test_wrong_answer_nan_join_key_still_matches_itself; the "
            "generator declines NaN-bearing columns as join keys (Relation.key_fields, "
            "which names this entry) so the algebra oracles are not permanently red."
        ),
    ),
    RegisteredDefect(
        id="asof-tie-breaking-is-not-deterministic",
        repro=(
            "SELECT r0.name, r1.s_high FROM testdata.astronauts AS r0 ASOF JOIN "
            "testdata.fuzzing.mixed AS r1 "
            "MATCH_CONDITION(r0.undergraduate_major > r1.s_null)"
        ),
        error_type="WrongAnswer",
        signature="",
        detail=(
            "An ASOF join whose match column has TIES returns a DIFFERENT matched row "
            "on different executions of the identical SQL. `s_null` holds 9 distinct "
            "values across 2,000 rows, so the nearest-match boundary is a large tie "
            "group; five consecutive runs of the repro returned five different right "
            "halves for the same left row:\n"
            "  row-000211-020617 / row-001314-005f8b / row-000602-04bb31 /\n"
            "  row-000211-020617 / row-000987-082f5b\n"
            "The build's per-group ordering is a std::sort (not stable), and the rows "
            "reach it in whatever order the parallel build combined them, so which "
            "tied row lands on the bisect boundary varies run to run.\n"
            "WHETHER a tie winner should be defined at all is the architect's call — "
            "every tied row satisfies the MATCH_CONDITION, so no individual answer is "
            "wrong. That the same query is not reproducible is the reportable part.\n"
            "It also disarms every oracle that compares two executions, which is why "
            "_applicable_oracles stands the multiset oracles down on an ASOF leg, "
            "naming this entry.\n"
            "Pinned by test_wrong_answer_asof_tie_breaking_is_still_unstable."
        ),
    ),
    RegisteredDefect(
        id="asof-using-an-array-key-drops-every-row",
        repro=(
            "SELECT COUNT(*) FROM testdata.fuzzing.mixed AS a ASOF JOIN "
            "testdata.fuzzing.mixed AS b MATCH_CONDITION(a.i_null <= b.d_null) "
            "USING (arr_str)"
        ),
        error_type="WrongAnswer",
        signature="",
        detail=(
            "`USING` bypasses the type check `ON` applies, so an ARRAY equi key is "
            "refused one way and silently wrong the other:\n"
            "  ON a.arr_str = b.arr_str        -> IncorrectTypeError   (correct)\n"
            "  USING (arr_str)                 -> 0 rows               (WRONG)\n"
            "  USING (i_group)                 -> 2000 rows            (correct)\n"
            "  no USING                        -> 2000 rows            (correct)\n"
            "ASOF is LEFT semantics — every left row is emitted exactly once, matched "
            "or not — so 0 rows from a 2,000-row left relation is not a partitioning "
            "that found nothing, it is every row dropped.\n"
            "Found by the asof_left_semantics oracle. The generator declines ARRAY as "
            "a USING key (_make_leg), naming this entry.\n"
            "Pinned by test_wrong_answer_asof_using_an_array_key_still_drops_every_row."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # RATIFIED REFUSALS — pinned so they are asserted rather than assumed, and go
    # red if someone implements them without telling the fuzzer.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="theta-join-on-decimal-is-refused",
        repro=(
            "SELECT COUNT(*) FROM testdata.fuzzing.mixed AS a INNER JOIN "
            "testdata.fuzzing.mixed AS b ON a.d_value > b.d_null"
        ),
        error_type="UnsupportedSyntaxError",
        signature="JOINs on DECIMAL types only supports Equals and Not Equals",
        detail=(
            "NOT A DEFECT as far as this fuzzer is concerned — a deliberate binder "
            "refusal (opteryx/planner/binder/join.py), pinned so it is asserted "
            "rather than assumed.\n"
            "A non-equality ON condition on a DECIMAL column is rejected; the same "
            "shape on FLOAT runs. An EQUALITY on DECIMAL runs, and so does an ASOF "
            "MATCH_CONDITION on a DECIMAL column — that takes a different path.\n"
            "The generator declines DECIMAL for theta conjuncts (_theta_conjunct) and "
            "only there, naming this entry. Delete both together if the restriction "
            "is ever lifted."
        ),
    ),
    RegisteredDefect(
        id="cross-type-temporal-asof-is-refused",
        repro=(
            "SELECT COUNT(*) FROM testdata.astronauts AS r0 ASOF JOIN "
            "testdata.missions AS r1 MATCH_CONDITION(r0.death_date > r1.Lauched_at)"
        ),
        error_type="NotSupportedError",
        signature="only numeric match columns are coerced",
        detail=(
            "A REFUSAL THAT REPLACED A SILENT WRONG ANSWER, pending a ruling.\n"
            "An ASOF MATCH_CONDITION comparing DATE to TIMESTAMP used to run and emit "
            "matches that violated it — 9 of 52 matched rows, e.g. death_date "
            "1966-02-28 matched Lauched_at 1969-12-27 under `>`. Root cause is the "
            "one the numeric coercion now fixes: DATE32 normalises to days, "
            "TIMESTAMP64 to microseconds, and the two are not comparable.\n"
            "It is refused rather than coerced because `find_compatible_type([DATE, "
            "TIMESTAMP])` answers VARCHAR, which is not an ordering anyone asked for; "
            "casting DATE to TIMESTAMP would be a coercion the EQUI key path does not "
            "perform either, so it is a decision to take rather than a mechanical "
            "extension of the numeric one.\n"
            "Delete this entry if the temporal coercion is added — the fuzzer already "
            "generates the shape and its match-condition oracle already checks it."
        ),
    ),
    RegisteredDefect(
        id="right-semi-and-right-anti-are-refused",
        repro=(
            "SELECT COUNT(*) FROM testdata.planets AS r0 RIGHT SEMI JOIN "
            "testdata.satellites AS r1 ON r0.id = r1.planetId"
        ),
        error_type="UnsupportedSyntaxError",
        signature="RIGHT SEMI JOIN not supported",
        detail=(
            "NOT A DEFECT — a ratified refusal, pinned so the fuzzer asserts it.\n"
            "`RIGHT SEMI JOIN` and `RIGHT ANTI JOIN` are rejected by the planner with "
            "'use LEFT variations only'. They are pure operand swaps — "
            "`A RIGHT SEMI JOIN B ON p` is exactly `B LEFT SEMI JOIN A ON p`, which "
            "the engine runs — so this is a dialect gap, not a capability gap, and "
            "per architect ruling (2026-08-08) the refusal stands.\n"
            "This entry exists so the refusal cannot change silently: if RIGHT mark "
            "joins are ever implemented this test goes RED, and the fix is to delete "
            "the entry and add them to `_JOIN_TYPES` in the fuzzer — where a comment "
            "points back here."
        ),
    ),
    RegisteredDefect(
        id="rlike-in-a-three-term-disjunction",
        repro=(
            "SELECT COUNT(*) FROM testdata.astronauts "
            "WHERE name RLIKE 'A' OR year = 1963 OR gender = 'Male'"
        ),
        error_type="RuntimeError",
        signature="draken_rlike: pattern operand must be a compiled DFA blob",
        detail=(
            "NOT THIS FUZZER'S DEFECT — reported separately and owned elsewhere; "
            "registered here only so it stops blocking join fuzzing.\n"
            "An RLIKE inside a THREE-term OR fails at execution; the same predicate "
            "with two terms runs:\n"
            "  WHERE name RLIKE 'A' OR year = 1963                      -> 68 rows\n"
            "  WHERE name RLIKE 'A' OR year = 1963 OR gender = 'Male'   -> raises\n"
            "The RLIKE may be in any position, and NOT RLIKE behaves the same. LIKE "
            "in the identical shape is unaffected (307 rows). The join fuzzer reaches "
            "it because its WHERE generator chains disjuncts, e.g.\n"
            "  WHERE missions.Location NOT RLIKE 'B9d3' OR astronauts.name RLIKE "
            "'4hqK' OR astronauts.status != 'djjC'\n"
            "Same family as the single-table register's "
            "rlike-outside-top-level-predicate-position: RLIKE evaluates correctly "
            "only as a whole predicate or as a direct child of one connective, and "
            "the pattern reaches the kernel un-compiled once it is nested deeper."
        ),
    ),
]

_BY_ID = {defect.id: defect for defect in REGISTER}
if len(_BY_ID) != len(REGISTER):
    raise AssertionError("duplicate id in the defect register")


def match(error: BaseException) -> Optional[RegisteredDefect]:
    """The register entry this exception belongs to, if any.

    An `AssertionError` can never match: no entry names that type, because every
    oracle violation in this fuzzer raises one and absorbing a wrong answer is the
    one thing this register must never do.
    """
    name = type(error).__name__
    if name == "AssertionError":
        return None
    message = str(error)
    for defect in REGISTER:
        if defect.error_type != name:
            continue
        if defect.signature and defect.signature not in message:
            continue
        return defect
    return None
