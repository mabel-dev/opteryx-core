"""
The single-table fuzzer's defect register.

WHAT THIS IS, AND WHAT IT IS NOT
--------------------------------
This is a register of engine defects the fuzzer has already found and reported.
It is NOT an allowlist of "errors that are fine".

The difference is enforced, not asserted. Every entry carries a minimal `repro`
that `test_sql_fuzzer_single_table_select.py::test_registered_defect_still_reproduces`
executes on every run and requires to STILL FAIL in the recorded way. So:

  * while a defect is open, the fuzzer does not spend every run re-reporting it;
  * the moment it is fixed, the register goes RED and the entry must be deleted,
    which puts the construct straight back into ordinary fuzzing;
  * and an entry cannot be added without a repro that demonstrably fails, so
    "register it" is not a way to make an inconvenient failure go away.

HOW A FUZZ FAILURE IS CLASSIFIED
--------------------------------
Only EXCEPTIONS are matched here, against `error_type` + `signature`. A match is
reported as a known-defect hit and does not fail the case; anything else fails
the case. The gate this gives is "no NEW failure classes".

A WRONG ANSWER IS NEVER ABSORBED. An oracle violation always fails the run —
there is no message-matching path that can silence one, because a substring
match on "the results differed" would swallow every future wrong answer of that
oracle's shape. The wrong-answer entries below (`error_type="WrongAnswer"`)
work differently: each is pinned by an explicit `test_wrong_answer_*` test that
asserts the broken behaviour directly, and `applicable_oracles()` declines to
run the affected oracle on the exact structural shape that triggers it, naming
the entry. That exclusion is visible in code, is scoped to a query shape rather
than to an error message, and disappears with the register entry.

RATIFIED SEMANTICS ARE A SEPARATE LIST
-------------------------------------
`RATIFIED` below is NOT part of the register and must never be confused with it.
A registered defect is engine behaviour that is wrong and will one day be fixed,
which is why every entry must still reproduce. A ratified semantic is behaviour
the architect has ruled CORRECT — there is nothing to fix, so "does it still
reproduce" is not a question that can be asked of it. What an oracle needs from
such a ruling is the opposite of a pin: it needs to know that a property it was
about to assert was never promised.

The two lists are kept apart because collapsing them would destroy the register's
one real property. The register's discipline is that an entry is a liability with
a deadline. A ruling has no deadline, and an entry that can never go red sitting
in a list whose whole contract is "these go red when fixed" teaches the reader to
stop believing the contract.

The corresponding risk is that `RATIFIED` becomes the place inconvenient failures
go to die. Two things hold against it: a ruling names the architect and the date
it was made, and `test_ratified_semantic_is_named_in_code` requires every entry to
be cited by the code that stands down because of it. An entry nothing acts on is
not a ruling, it is a note, and the test says so.

SIGNATURES TRACK THE ENGINE'S WORDING, NOT THE OTHER WAY ROUND
--------------------------------------------------------------
A `signature` is a substring of a user-facing error message, and those messages
are rewritten deliberately (the markdown-markup contract put `*column*` and
`**FROM**` into them, and replaced several internal phrasings with ones a caller
can act on). A rewording is not a defect and not a fix: the entry still
reproduces, it just stops MATCHING, and an entry that stops matching silently
stops absorbing its class — the fuzzer then fails on a defect that is already
registered. Four entries were in exactly that state and have been re-pointed at
the current wording. When a message changes, update the signature here; never
hold a message still to keep this file matching.

THE HONEST LIMITATION: signature matching is by message substring, so a genuinely
new bug that happens to produce an already-registered message — most plausibly
another hole in the c-native kernel set — would be absorbed into an existing
entry's count rather than failing the run. The signatures are kept as narrow as
the engine's messages allow, and the per-entry hit counts are printed at the end
of every run so a class that suddenly gets much louder is visible. This is a
known trade against the alternative of a permanently-red suite; it is recorded
here rather than left for someone to discover.
"""

from __future__ import annotations

from dataclasses import dataclass
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
    #: An optimizer strategy to DISABLE while running `repro`. Set only for a
    #: defect that needs one off to appear — the optimizer differential oracle
    #: finds these, and without this the entry could not carry a repro that
    #: reproduces, so the register's one real gate would not apply to it. The
    #: default of None means "run it exactly as written", which is every other
    #: entry here.
    disabled_strategy: Optional[str] = None


@dataclass(frozen=True)
class RatifiedSemantic:
    """Behaviour ruled CORRECT, that an oracle would otherwise assert against.

    Not a defect and not a suppression: an oracle that stands down because of one
    of these was asserting a property the SQL never promised. `ruling` records who
    decided and when, because a ruling with no author is indistinguishable from a
    convenience.
    """

    id: str
    #: A statement exhibiting the semantic, so the reader can see it directly.
    example: str
    #: "<who>, YYYY-MM-DD".
    ruling: str
    detail: str


# The relations the repros read are created by `dev/generate_fuzz_testdata.py`
# and by the pre-existing `testdata/` corpus.
REGISTER: List[RegisteredDefect] = [
    # ─────────────────────────────────────────────────────────────────────────
    # WRONG ANSWERS — no exception, just the wrong rows. The most serious class
    # here, and the reason the oracles exist at all.
    # ─────────────────────────────────────────────────────────────────────────
    # `iif-in-a-where-clause-raises-a-raw-valueerror` was registered here. FIXED,
    # both halves. A boolean-branched IIF is now marked BC_RESULT_WRAP_AS_BOOL at
    # bind time (compiled_expression.pyx) — draken_iif's BOOL arm returns a dense
    # `length`-wide bitmap, which IS a mask — so `WHERE IIF(cond, TRUE, FALSE)`
    # runs. The raw ValueError class is gone too: `_lower_scan_predicate`
    # (managers/execution/compiler.py) now tests bool-finalness at plan time for
    # every scan-pushed predicate, so anything that still cannot be a filter
    # (COALESCE over BOOLEAN branches, today) is refused with NotSupportedError
    # instead of reaching add_expr_filter's internal invariant check.
    #
    # `concat-of-a-varbinary-literal-leaks-a-vector-repr` was registered here as
    # a WrongAnswer, and `varbinary-concat-has-no-kernel-and-refuses-in-the-
    # wrong-class` below it as a diagnostic. Both are RESOLVED BY RULING — see
    # RATIFIED/string-concatenation-requires-homogeneous-string-types. The
    # premise both entries rested on ("there is no VARBINARY concat kernel") was
    # wrong: VARBINARY||VARBINARY has always worked and still does. The kernel
    # gate is `lt_is_string && lt == rt` (draken/ops/kernels/binop_dispatch.cpp),
    # so what had no kernel was every MIXED pair, VARCHAR||NVARCHAR included.
    # The binder promised those pairs a result type anyway, which is what split
    # the two symptoms: a mixed COLUMN pair reached the plan compiler and was
    # refused, while a mixed pair of LITERALS was constant-folded through the
    # Python coercion closure in expression/evaluator/arithmetic.pyx, whose
    # `str(v)` arm stringified a VARBINARY Vector's repr into the answer. The six
    # mixed rows are gone from planner/binder/operator_map.py, so `||` refuses at
    # the binder, and CONCAT/CONCAT_WS now carry one overload per string type so
    # a mixed call matches no overload and is refused by resolution.
    # `count-star-over-a-distinct-set-operation-returns-one` was registered here
    # as a WrongAnswer. It is FIXED — ProjectionPushdownStrategy.visit
    # (opteryx/planner/optimizer/strategies/projection_pushdown.py) recorded the
    # OUTER demand as `pre_update_columns` on every node below a Distinct. A
    # Distinct with no ON dedups on every column that reaches it, and COUNT(*)
    # demands none, so the semi/anti join a DISTINCT set operation is rewritten
    # to (except_to_anti_join / intersect_to_inner_join) pruned its EMIT set to
    # ZERO columns — the Distinct then dedupped on nothing and collapsed the
    # whole set operation into one bucket. The ALL variants and both UNIONs were
    # unaffected because neither carries that Distinct-straight-onto-a-join
    # shape. Below an unclosed Distinct the pass now records the empty UNKNOWN
    # sentinel every consumer already reads as "keep every column", so the join
    # emits its payload and the dedup key survives. The
    # `count_star_matches_materialised_rows` exclusion this drove in
    # applicable_oracles(), and the Statement flag it read, are gone with it.
    # `is-null-over-a-mixed-type-comparison-returns-false-for-every-row` was
    # registered here as a WrongAnswer. It is FIXED — draken_numeric_cmp
    # (draken/ops/kernels/function_kernels.cpp), the kernel every mixed-type
    # numeric comparison routes to, returned `validity = nullptr`. It cleared the
    # null row's DATA bit but left the row marked VALID, so UNKNOWN read back as
    # a definite FALSE. Enough for a WHERE filter, which drops the row either
    # way — which is exactly why the matched and rejected counts stayed right
    # while the UNKNOWN bucket vanished. The result now carries the AND of both
    # operands' validity, the contract draken_compare_dv's own paths obey. The
    # DECIMAL exclusion this drove in applicable_oracles() is gone with it —
    # predicate_partition now runs on mixed-type numeric predicates.
    # `filter-over-a-limited-subquery-is-non-deterministic` was registered here as
    # a WrongAnswer. It was RECLASSIFIED — see RATIFIED/limit-and-offset-select-an-
    # arbitrary-subset below. It was never an oracle-visible defect: every row set
    # it produced was a legal answer.
    # `grouped-count-distinct-is-not-deterministic` was registered here as a
    # WrongAnswer. It is FIXED — gb_mix2 (src/cpp/engine/native_group_sinks.hpp)
    # combined the group id with the value hash as `mix_K(gid) ^ value_hash`
    # using draken's own value-hash finalizer as mix_K. Draken's per-value hash
    # for the integer/BOOL family IS mix_K(raw), so the dedup key was symmetric:
    # group 0 holding value 1 and group 1 holding value 0 produced the SAME key
    # and the second was dropped. Both arguments are now pre-mixed by different
    # constructions. The exclusion this drove in applicable_oracles(), and the
    # Statement/SelectQuery flag it read, are gone with it — every oracle now
    # runs on DISTINCT aggregates.
    # `not-over-a-boolean-tautology-disjunction-overlaps-itself` was registered
    # here as a WrongAnswer. It is FIXED — _simplify_and_chain
    # (opteryx/planner/optimizer/strategies/boolean_simplication.py) deduped the
    # conjuncts of a flattened AND chain by `Node.uuid`. A uuid names a node
    # INSTANCE, not a value: the binder hands out uuid-preserving copies of
    # expressions that rendered alike, and the same strategy's NOT(A = B) => A
    # != B rule inverts the comparison in place without refreshing it. De
    # Morgan over NOT((A OR NOT A) OR NOT B) produced the chain
    # `A != TRUE AND A = TRUE AND B`, whose first two conjuncts carried the SAME
    # uuid — dedup deleted the contradiction and left `A != TRUE AND B`, which
    # matched 492 rows instead of 0. Dedup now keys on predicate_key(), the same
    # content-plus-bound-identity key DisjunctionSimplificationStrategy uses.
    # `bare-case-is-refused-as-a-where-clause-but-bare-iif-is-not` was registered
    # here. It is FIXED, under a RULING (architect, 2026-08-10) recorded on
    # logical_planner._validate_where_clause_expression: a WHERE/ON clause must be a
    # boolean VALUE EXPRESSION, and "is it boolean" is a TYPE question answered by
    # binder/filter.py::visit_filter against bound types — NOT a syntax question
    # answered by node kind. The planner now admits every value expression (CASE, CAST,
    # `->`/`[i]` included) and refuses only what has its own reason: a bare literal
    # (unpushable AND a typo), a bare column (a typo), and the non-value kinds
    # (WILDCARD / AGGREGATOR / SUBQUERY). Two layers had to move: the catch-all in the
    # planner, and BC_RESULT_WRAP_AS_BOOL on the IF_THEN_ELSE slot
    # (compiled_expression.pyx) — compiler._rewrite_case folds a CASE into a c-native
    # draken_if_then_else chain, which was c-native but not bool-final, so
    # bytecode_is_c_native_predicate refused it one layer BELOW the planner guard.
    # Admitting it in the planner alone only moved the refusal. draken_if_then_else's
    # BOOL arm allocates its own (length+7)/8 bitmap with an identity selection, which
    # is cxx_mask_c's dense-mask contract — the same proof the IIF arm carries.
    # ─────────────────────────────────────────────────────────────────────────
    # SQL-92 SPELLINGS THE DIALECT DOES NOT REACH. Each is a construct the
    # standard defines and the engine's own vocabulary already contains — the
    # gap is the syntax, not the capability.
    # ─────────────────────────────────────────────────────────────────────────
    # `trim-rejects-the-sql-92-trim-character-form` was registered here. FIXED.
    # TRIM/LTRIM/RTRIM take an optional second parameter now (registrar/text.pyx),
    # so `TRIM(BOTH 'a' FROM name)` binds, and draken_trim grew the arity to match:
    # the argument is a SET of characters (`TRIM(BOTH 'ab' FROM 'baXab')` is 'X'),
    # constant-only so the kernel keeps its shape preservation, and scanned by
    # CODEPOINT rather than by byte over an NVARCHAR operand so a multibyte
    # character can never be split. `_sql92_spelling` emits it now.
    #
    # Its second half did NOT close with it, so it is registered below on its own
    # rather than deleted alongside — the entry said the two would "close
    # together", and that turned out to be wrong.
    RegisteredDefect(
        id="trim-with-no-trim-character-is-unparseable",
        repro="SELECT TRIM(BOTH FROM name) AS x FROM testdata.planets",
        error_type="QueryParseError",
        signature="Expected: ), found: name",
        detail=(
            "SQL-92 allows the direction WITHOUT a character — `TRIM(BOTH FROM str)` — which "
            "means the same as `TRIM(str)`: strip whitespace at the named end. The dialect's "
            "TRIM production requires a `trim_what` expression before FROM, so all three "
            "directions are unparseable in that spelling:\n"
            "  TRIM(BOTH FROM name)      -> QueryParseError 'Expected: ), found: name'\n"
            "  TRIM(LEADING FROM name)   -> the same\n"
            "  TRIM(TRAILING FROM name)  -> the same\n"
            "\n"
            "This is PARSER-ONLY and the capability behind it is complete: the one-argument "
            "kernel arm this would lower to has always existed (`TRIM(name)` works, and "
            "LTRIM/RTRIM reach the leading/trailing variants by name). It was recorded inside "
            "trim-rejects-the-sql-92-trim-character-form, on the expectation that both halves "
            "would close at once. The binder half closed; this did not, so it is its own entry.\n"
            "\n"
            "The catalog does not overstate it — `reference/function_signatures.json` describes "
            "the FUNCTION's arity, and nothing there claims a surface spelling. "
            "single_table_grammar._sql92_spelling emits the trim-character form and not this one."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # CATALOG DISAGREED WITH THE ENGINE — this section is EMPTY, and the reason
    # it is empty is worth recording, because "no entries" and "nobody looked"
    # look identical from here.
    #
    # Seven entries lived here. Four were fixed in the ENGINE, so the register
    # went red and they were deleted:
    #   shift-operators-unparseable          the dialect gained an infix parse for
    #                                        `<<` / `>>`. draken's bitwise_shl /
    #                                        bitwise_shr, the BOP_SHIFT_* opcodes
    #                                        and the operator_map entries were all
    #                                        already there; the operator was
    #                                        implemented end to end except for
    #                                        being reachable.
    #   two-argument-substring-is-unbindable the SUBSTRING plan builder padded the
    #                                        missing FOR slot with an untyped NULL,
    #                                        so every call carried three arguments
    #                                        and SUBSTRING_2 was unreachable. It
    #                                        emits what the caller wrote now.
    #   coalesce-single-argument-raises-...  COALESCE(a) folds to `a` at plan-build
    #                                        time. The first non-null of one
    #                                        argument IS that argument, so the
    #                                        catalog's declared minimum arity of 1
    #                                        is now true rather than retracted.
    #   timestamp-to-date-cast-has-no-kernel draken_cast_timestamp_to_date32 runs
    #                                        the column path; the LITERAL path was
    #                                        still handing the raw epoch tick count
    #                                        to a date parser, and now floor-divides
    #                                        by the source unit's ticks-per-day like
    #                                        the kernel does.
    #
    # Three were fixed in the CATALOG, which is why they are not simply "still
    # open with a nicer excuse" — the engine behaviour is unchanged, but it is
    # now DECLARED, so a generator reading `reference/` does not emit it:
    #   at-question-has-no-native-kernel     operators.json records
    #                                        `implemented: false` for `@?`.
    #   array-agg-global-claimed-but-rejected aggregates.json records
    #                                        `support.global: false`, the same
    #                                        shape ANY_VALUE already had.
    #   regexp-replace-is-only-partly-...    the pattern parameter declares
    #                                        `value_format: "dfa-regex"` and the
    #                                        replacement `domain: ["\\1"]`, so the
    #                                        catalog states the whole-match-capture
    #                                        restriction instead of declaring the
    #                                        general three-argument form.
    #
    # A catalog fix is only a fix when the catalog becomes TRUE. Declaring a
    # capability the engine lacks and declining to declare a restriction the
    # engine enforces are the same defect seen from two sides.
    # ─────────────────────────────────────────────────────────────────────────
    # ─────────────────────────────────────────────────────────────────────────
    # AN OPTIMIZER PASS IS LOAD-BEARING — disabling it does not just cost speed,
    # it breaks the plan. Found by the optimizer differential oracle.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="having-in-a-derived-table-needs-predicate-pushdown-to-run",
        repro=(
            "SELECT name FROM (SELECT name, COUNT(*) AS c FROM testdata.planets "
            "GROUP BY name HAVING COUNT(*) < 1) AS sub"
        ),
        error_type="KeyError",
        signature="which the stream does not carry",
        disabled_strategy="disable_predicate_pushdown",
        detail=(
            "A HAVING inside a DERIVED TABLE only runs because predicate pushdown runs. Turn "
            "that one strategy off and the query dies:\n"
            "    expression references column b'$derived_qxJeRfqC' which the\n"
            "    stream does not carry (layout: [...])\n"
            "The same query with the strategy enabled is fine, and the FLAT spelling — the same "
            "GROUP BY/HAVING without the enclosing derived table — is fine either way. So it is "
            "the derived-table boundary that loses the HAVING's `$derived_` column when the "
            "predicate is not pushed.\n"
            "\n"
            "This matters beyond the oracle. An optimizer strategy is semantics-preserving by "
            "definition: disabling one may cost time but must never change the answer, let alone "
            "raise. A pass that is load-bearing is a pass whose kill switch is a lie, and the "
            "kill switches are what DISABLE_OPTIMIZER debugging rests on — the tool you reach "
            "for when a plan is suspect is the one that breaks. It is the same class as the "
            "projection-pushdown case that was fixed by having the binder seed Scan.columns.\n"
            "\n"
            "The aggregate FILTER in the statement that first produced this is NOT required; it "
            "reduces to a plain COUNT(*). Registered with disabled_strategy rather than left to "
            "fail the run at random, because seeds are random per run and this would surface "
            "only when the oracle happened to draw this one strategy out of fifteen.\n"
            "\n"
            "FOUND BY: the single-table fuzzer on the CONCAT one-overload-per-string-type run, "
            "which shifted the RNG draw and so the statements generated. Unrelated to that "
            "change — there is no string concatenation anywhere on this path."
        ),
    ),
    # `repeated-nullary-function-in-one-projection-fails-to-bind` was registered
    # here for less than an hour. It is FIXED — inner_binder's "adopt a derived
    # column an earlier occurrence already registered" branch REWRITES the node
    # into a LITERAL when the found column is a ConstantColumn (a nullary constant
    # function folds to its value), and the function-reference bind that follows
    # was guarded on the `node_type` LOCAL captured before that rewrite. The local
    # still said FUNCTION, so it bound a function reference off a node whose
    # `value` was now a float, and the catalog's `resolve(name: str)` raised a
    # Cython coercion error naming neither the function nor the query. The guard
    # now reads `node.node_type`. `SELECT PI() AS a, PI() AS b` runs.
    # ─────────────────────────────────────────────────────────────────────────
    # REDUNDANT SYNTAX CHANGES THE OUTCOME — semantically identical spellings
    # where one works and the other does not. These share a root: redundant
    # parentheses are not normalised away before the plan is built.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="parenthesised-expression-loses-its-alias",
        repro="SELECT x FROM (SELECT (id + 1) AS x FROM testdata.planets) AS s",
        error_type="ColumnNotFoundError",
        signature="cannot be found",
        detail=(
            "An alias on a PARENTHESISED expression is not registered, so the column cannot be "
            "referenced from ORDER BY or from an enclosing query. Without the parentheses the "
            "identical query works (`SELECT id + 1 AS x ...`), and `LENGTH(name) AS x` works — it "
            "is the redundant parentheses specifically. Also reproduces as "
            "`SELECT (id + 1) AS x FROM testdata.planets ORDER BY x`."
        ),
    ),
    RegisteredDefect(
        id="float-in-list-only-works-at-top-level",
        repro=(
            "SELECT COUNT(*) FROM testdata.planets "
            "WHERE (mass <> -1.0) OR (orbital_eccentricity IN (-1.0, -2.0))"
        ),
        error_type="NotSupportedError",
        signature="filter predicate outside the c-native kernel set",
        detail=(
            "An IN-list on a FLOAT column has a native kernel only when it is the entire "
            "predicate. Every one of these fails while the bare form runs:\n"
            "  NOT ((f_null IN (1.5, 2.5)))          -- one redundant paren pair\n"
            "  NOT (f_null NOT IN (1.5, 2.5))        -- under a NOT\n"
            "  (mass <> -1.0) OR (ecc IN (-1.0))     -- as a disjunct\n"
            "  COUNT(*) FILTER (WHERE gm IN (1.5, 2.5))   -- as a FILTER predicate\n"
            "The same shapes over an INTEGER or VARCHAR column all run.\n"
            "The FILTER door is worth spelling out because it does not LOOK nested: the "
            "predicate is written at the top level of the clause. It becomes nested because "
            "`AGG(x) FILTER (WHERE p)` is lowered to `AGG(IIF(p, x, NULL))`, so `p` ends up as "
            "IIF's condition and the message names IIF rather than the filter:\n"
            "  NotSupportedError: a comparison in `IIF(gm IN [1.5, 2.5],1,null)`, outside the\n"
            "  c-native kernel set\n"
            "That is a DIFFERENT message from the one this entry matches, so the register does "
            "not absorb it; the generator emits FILTER predicates at nesting depth 1 instead, "
            "which applies the same rule that already keeps a FLOAT IN-list out of a disjunct."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # POSITION-DEPENDENT EVALUATION — an expression that works standalone but
    # fails once it is nested inside another expression.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="rlike-outside-top-level-predicate-position",
        repro="SELECT (CASE WHEN name RLIKE '^a' THEN name ELSE name END) FROM testdata.planets",
        error_type="RuntimeError",
        signature="err_op=15",
        detail=(
            "RLIKE evaluates correctly only as a whole predicate or as a direct child of one "
            "connective. Nested deeper it fails at execution with err_op=15:\n"
            "  inside CASE/IIF   -> ExprMultiProjectOperator: expression evaluation failed\n"
            "  inside nested OR  -> ExprFilterOperator: predicate evaluation failed, for\n"
            "     WHERE ((name NOT RLIKE '[0-9]') OR (id > 3)) OR (name IS NULL)\n"
            "     while the two-term WHERE (name NOT RLIKE '[0-9]') OR (id > 3) runs.\n"
            "LIKE and ILIKE are unaffected in every one of those positions."
        ),
    ),
    RegisteredDefect(
        id="temporal-function-call-inside-a-case-branch",
        repro=(
            "SELECT CASE WHEN id > 1 THEN TRUNC('2040-01-04'::DATE, 'month') "
            "ELSE '2000-01-01'::TIMESTAMP END FROM testdata.planets"
        ),
        error_type="NotSupportedError",
        signature="a function call in",
        detail=(
            "A temporal-returning function call in a CASE branch raises 'a function call in "
            "`IF_THEN_ELSE(...)`, outside the c-native kernel set'. The same TRUNC outside the "
            "CASE runs, and numeric (ABS) and string (UPPER) calls inside the same CASE run."
        ),
    ),
    RegisteredDefect(
        id="aggregate-window-over-a-derived-table",
        repro=(
            "SELECT val, SUM(val) OVER (PARTITION BY val) AS w FROM "
            "(SELECT val FROM testdata.fuzzing.wide) AS s"
        ),
        error_type="UnexpectedDatasetReferenceError",
        signature="does not appear in a **FROM** or **JOIN**",
        detail=(
            "An AGGREGATE window over a derived table fails with an error naming the BASE "
            "relation. The same shape with a CTE fails identically:\n"
            "  WITH c AS (SELECT gravity, SUM(mass) OVER (PARTITION BY gravity) AS w\n"
            "             FROM testdata.planets) SELECT gravity FROM c\n"
            "A RANKING window (ROW_NUMBER/RANK/DENSE_RANK) in either position works, and an "
            "aggregate window applied directly to a base table works."
        ),
    ),
    RegisteredDefect(
        id="count-star-over-a-ranking-window-subquery",
        repro=(
            "SELECT COUNT(*) FROM (SELECT row_id, ROW_NUMBER() OVER (ORDER BY row_id) AS rn "
            "FROM testdata.fuzzing.mixed) AS t"
        ),
        error_type="NotSupportedError",
        signature="a window ORDER BY column the engine could not resolve",
        detail=(
            "COUNT(*) over a subquery containing a ranking window fails, even though the inner "
            "SELECT projects the ORDER BY column explicitly. Any PROJECTING outer query over the "
            "same subquery works (`SELECT rn FROM (...)`, `SELECT * FROM (...)`, "
            "`SELECT row_id FROM (...)`) — only the empty projection of COUNT(*) fails. Same "
            "class as the READ_JSONL COUNT(*) empty-projection bug."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # INTERNAL ERRORS REACHING THE CALLER — raw Python exceptions, and internal
    # mangled column names, escaping as the user-facing diagnostic.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="time-bucket-non-integer-magnitude",
        repro=(
            "SELECT TIME_BUCKET(-125533.0000, 'week', '2021-02-03'::DATE) FROM testdata.planets"
        ),
        error_type="TypeError",
        signature="Failed to extract integer scalar from constant vector",
        detail=(
            "A raw Python TypeError naming an internal operation ('extract integer scalar from "
            "constant vector') where a SQL error belongs. THE CATALOG HALF OF THIS IS FIXED: "
            "TIME_BUCKET's `magnitude` now declares `minimum: 1` and `excludes: [DECIMAL]`, so "
            "the constraint is stated and the fuzzer no longer emits a call that violates it. "
            "What survives is the diagnostic: a caller who writes this by hand still gets an "
            "internal exception instead of a message naming the argument."
        ),
    ),
    RegisteredDefect(
        id="case-rejects-two-identical-array-branches",
        repro=(
            "SELECT (CASE WHEN b_value = FALSE THEN arr_int ELSE arr_int END) "
            "FROM testdata.fuzzing.mixed"
        ),
        error_type="IncompatibleTypesError",
        signature="which CASE cannot blend",
        detail=(
            "A CASE whose two branches are the SAME ARRAY column is rejected. THE MESSAGE HALF "
            "OF THIS IS FIXED (2026-08-09): it used to contradict itself — \"column 'arr_int' is "
            "ARRAY<INT64> but column 'arr_int' is ARRAY<INT64> — CASE branches must share a "
            "compatible type\" — while listing families that excluded ARRAY entirely. It now "
            "states the real rule: \"is ARRAY<INT64>, which CASE cannot blend\". Signature "
            "re-pointed at the new wording per this file's rewording contract; the construct "
            "still reproduces, so the entry still absorbs its class.\n"
            "\n"
            "WHAT SURVIVES IS A CLASSIFICATION QUESTION, NOT A MESSAGE BUG. The architect ruled "
            "on 2026-08-09 that an ARRAY branch in CASE/IIF/COALESCE is a plan-time REFUSAL, not "
            "a kernel gap to close (draken_if_then_else cannot read an ARRAY operand at all — "
            "the elements hang off VectorOwner::child_owner, unreachable from a DrakenVector). "
            "If that ruling is read as covering this construct, the entry is ratified behaviour "
            "and belongs in RATIFIED, not here — but nothing in the oracles or the generator "
            "would then cite it, so test_ratified_semantic_is_named_in_code would go red and the "
            "move needs a decision about what stands down. Left in the register, correctly "
            "matching, pending that call rather than moved unilaterally."
        ),
    ),
    # `reverse-corrupts-multi-byte-utf8` was registered here. It is not a defect \u2014
    # see RATIFIED/varchar-is-ascii-bytes-and-non-ascii-content-is-undefined.
    RegisteredDefect(
        id="json-function-on-non-json-text-raises-valueerror",
        repro="SELECT JSONB_OBJECT_KEYS('delta') FROM testdata.planets",
        error_type="RuntimeError",
        signature="jsonb_object_keys: invalid JSON",
        detail=(
            "JSONB_OBJECT_KEYS fails at execution on input that is not a JSON document. THE "
            "CATALOG HALF OF THIS IS FIXED: the `json` parameter declares "
            "`value_format: \"json\"`, a constraint no type family can carry, so a type-directed "
            "generator now knows a plain VARCHAR does not satisfy it. What survives is the "
            "diagnostic: in a bare projection the failure surfaces as a raw ValueError, and "
            "inside a larger expression the multi-project operator wraps it as 'expression "
            "evaluation failed (err_op=15)' — an opcode number, which is the form recorded here."
        ),
    ),
    RegisteredDefect(
        id="order-by-a-boolean-expression-has-no-sort-key",
        repro=(
            "SELECT NULLIF((b'0' <= b'eta'), (TRUE = TRUE)) AS x FROM testdata.fuzzing.mixed "
            "ORDER BY x DESC"
        ),
        error_type="RuntimeError",
        signature="SortSink: unsupported ORDER BY key column type",
        detail=(
            "Sorting on a BOOLEAN-valued EXPRESSION fails, while sorting on a BOOLEAN COLUMN "
            "works: `SELECT b_value FROM t ORDER BY b_value DESC` runs. types.json does not "
            "record BOOLEAN as unsortable, and the engine sorts the column form, so the two "
            "disagree. The error is at least loud — 'fail loud, never a silent wrong order'."
        ),
    ),
    # `window-plus-folded-literal-concat-filter-raises-typeerror` was registered
    # here. It is FIXED — and it was never about the window, the ORDER BY or the
    # parentheses. CONCAT is REWRITE-ONLY: it has no callable_ref of its own and is
    # desugared into a StringConcat chain by a strategy that runs AFTER constant
    # folding, so an all-literal CONCAT reached the expression VM with
    # `callable_obj` None. fold_constants already desugared such a call when it WAS
    # the whole expression; one nested inside a comparison was missed, which is the
    # entire difference between the two spellings — the redundant parens changed
    # which node the fold started from, not what the engine could execute.
    # `_desugar_rewrite_only` (constant_folding.py) now walks the whole subtree, and
    # identifies a rewrite-only function by asking its resolved overload for a
    # callable rather than by matching a name list. Seven functions qualify and they
    # are exactly the desugared family (CONCAT, CONCAT_WS, COALESCE, IFNULL,
    # IFNOTNULL, IIF, RANDOM_STRING).
    # `identical-function-call-in-both-case-branches-is-unbound` was registered
    # here. It is FIXED — and it was common-subexpression sharing, as the entry
    # guessed. inner_binder (binder.py) adopts the derived column an earlier
    # occurrence of the same rendered expression registered, and returned without
    # resolving the catalog: the second node kept its FUNCTION shape but had no
    # `function_ref`, which is where the expression compiler reads the kernel from.
    # It only showed when both occurrences are COMPILED, which is what the
    # CASE -> IF_THEN_ELSE rewrite does to two identical branches. Both binding
    # paths now go through `_bind_function_reference`, so they cannot drift.
    # `folded-string-function-predicate-under-an-outer-filter-raises-typeerror` was
    # registered here. It is FIXED — in the COST MODEL, not in the comparison the
    # message named. Nothing executed a bytes-against-str compare: the two
    # predicates each contributed a range bound for `Rocket_Status` to
    # `_narrow_filter_columns` (statistics_refresh.py), whose `max`/`min` is
    # deliberately unguarded, and a parsed IN-list literal is `bytes` while a
    # constant-folded INITCAP is `str`. `_scan_stats` already refused to put
    # anything but int/float in `value_range` for exactly this reason;
    # `_orderable_bound` now applies the same gate to bounds harvested from
    # predicates. The str/bytes split in literal representation is real and lives
    # upstream — this keeps the cost model out of it rather than picking a side in
    # passing. Equality cardinality still caps NDV for every type.
    RegisteredDefect(
        id="from-unixtime-out-of-range",
        repro="SELECT FROM_UNIXTIME(374000000000.0) FROM testdata.planets LIMIT 1",
        error_type="ValueError",
        signature="year must be in 1..9999",
        detail=(
            "FROM_UNIXTIME raises a raw Python ValueError on an epoch value outside year "
            "1..9999. THE CATALOG HALF OF THIS IS FIXED: the `ts` parameter now declares "
            "`minimum: -62135596800` and `maximum: 253402300799`, the exact inclusive endpoints "
            "of that window, so the bound is stated rather than discovered. What survives is "
            "the diagnostic — the kernel's own limit is far wider (~year 294247, where the "
            "microsecond tick stops fitting int64) and the real ceiling is imposed later, when "
            "the TIMESTAMP is materialised, which is why the error names neither the function "
            "nor the argument."
        ),
    ),
    # `temporal-function-result-compared-to-a-literal-raises-typeerror` was
    # registered here. It is FIXED, and like its sibling above it was the COST
    # MODEL, not execution. `rewrite_date_trunc_to_range` turns
    # `TRUNC(ts, 'hour') <= <lit>` into a plain range on `ts` — a good rewrite —
    # but built the bound literal holding a `datetime` while declaring it
    # TIMESTAMP. A TIMESTAMP literal in this engine is an epoch INTEGER in the
    # column's unit, which that same function's own parsing step goes out of its
    # way to read, so it consumed the canonical form and emitted a
    # non-canonical one. The bound then met a manifest bound of 946693884000000
    # in an unguarded `min`. `_canonical_temporal_literal_value`
    # (predicate_rewriter.py) converts by the column's declared unit; every TRUNC
    # form now returns exactly what its hand-written equivalent range returns.
    RegisteredDefect(
        id="to-char-out-of-range-codepoint",
        repro="SELECT TO_CHAR(-303083) FROM testdata.planets",
        error_type="ValueError",
        signature="is not a Unicode scalar value",
        detail=(
            "TO_CHAR's argument is a Unicode CODEPOINT. THE CATALOG HALF OF THIS IS FIXED: `num` "
            "now declares `minimum: 0` and `maximum: 1114111`, and documents that the surrogate "
            "range U+D800..U+DFFF is excluded as well (a bound cannot express a hole). What "
            "survives is the diagnostic: an out-of-range value raises a raw Python ValueError "
            "naming the kernel rather than a SQL error naming the argument."
        ),
    ),
    RegisteredDefect(
        id="regexp-replace-is-only-partly-implemented",
        repro="SELECT REGEXP_REPLACE('theta', 'x', '[aeiou]') FROM testdata.planets",
        error_type="UnsupportedSyntaxError",
        signature="REGEXP_REPLACE is only supported natively",
        detail=(
            "REGEXP_REPLACE is implemented only 'as whole-match capture extraction'. "
            "reference/function_signatures.json declares the general three-argument form with no "
            "such restriction, so the catalog overstates what the engine does."
        ),
    ),
    # `cast-binary-to-varchar-yields-undecodable-text` was registered here. It is
    # not a defect — see
    # RATIFIED/varchar-is-ascii-bytes-and-non-ascii-content-is-undefined.
    # `cast-to-bare-decimal-raises-valueerror` was registered here. It is FIXED —
    # `CAST(x AS DECIMAL)` is rejected by the BINDER now, with an
    # UnsupportedSyntaxError that names the clause and gives the spelling
    # (`DECIMAL(precision, scale)`). It stays an error rather than acquiring a
    # default: a DECIMAL's descriptor IS its type, so inventing one would silently
    # decide how the caller's numbers round. The expression compiler's own check
    # survives as an internal invariant that binding now guarantees.
    # `decimal-case-blend-with-a-literal-overflows` was registered here. It is
    # FIXED — `_rewrite_case` (compiler.py) pinned a literal branch to
    # DECIMAL(18, scale) whenever the CASE's result type was DECIMAL128, on the
    # since-stale grounds that a DECIMAL128 literal could not be materialised. At
    # the ordinary result type for this shape, DECIMAL(38,18), that pin is not
    # merely redundant but impossible: precision 18 at scale 18 represents nothing
    # but a fraction, so rescaling 1.5 produced 19 unscaled digits against a
    # declared 18. The literal carries the CASE's own declared type now, both tiers
    # alike, and _materialise_constant_literal routes it on the tag.
    # `cte-projecting-a-column-the-stream-does-not-carry` was registered here. It is
    # FIXED, and the recorded detail was WRONG about the shape: neither the CTE nor
    # the unused `row_id AS e1` nor the WHERE mattered. The minimal repro is
    # `SELECT COUNT(ts), COUNT_DISTINCT(ts) FROM <any table>`. `decompose_aggregates`
    # (logical_planner_rewriter.py) keyed its de-duplication on function name plus
    # operand, and COUNT_DISTINCT is rewritten to COUNT + duplicate_treatment before
    # it runs — so the two collided and the DISTINCT one was dropped without a word.
    # Nothing then computed the column the projection asked for. The key now carries
    # every modifier that changes the value.
    RegisteredDefect(
        id="outer-filter-on-a-limited-grouped-cte-column",
        repro=(
            "WITH c AS (SELECT grp_wide, val_special, COUNT(cat) AS a2 "
            "FROM testdata.fuzzing.wide WHERE val_special IN (-830422.625879) "
            "GROUP BY grp_wide, val_special HAVING COUNT(*) <= 1 LIMIT 1) "
            "SELECT a2 AS e FROM c WHERE val_special BETWEEN -260351.4 AND 271709.7"
        ),
        error_type="NotSupportedError",
        signature="projecting a column the engine could not resolve",
        detail=(
            "An outer WHERE on a column of a CTE that GROUPs, HAVINGs and LIMITs fails with "
            "'projecting a column the engine could not resolve here'. Every ingredient is "
            "load-bearing — dropping ANY ONE of the inner WHERE, the HAVING, the LIMIT, or "
            "either GROUP BY key makes it run — and none of the obvious suspects matter: "
            "DISTINCT on the aggregate, the CAST the fuzzer wrapped the result in, and "
            "projecting `val_special` in the outer SELECT all make no difference.\n"
            "\n"
            "REGISTERED AFTER a same-message entry was DELETED, and deliberately not folded "
            "into it. The old entry (cte-projecting-a-column-the-stream-does-not-carry) named "
            "COUNT + COUNT_DISTINCT over one column, which is fixed — its dedup key ignored "
            "DISTINCT. This is a different defect that produced the same sentence, and it was "
            "invisible for exactly as long as that entry sat in front of it. Confirmed "
            "independent of the fix by re-running it against the old de-duplication key: it "
            "fails identically."
        ),
    ),
    # `subquery-alias-filtered-but-not-projected-raises-keyerror` was registered
    # here. It is FIXED — and it was a real pruning bug, as the entry read it.
    # PredicatePushdownStrategy moves a predicate onto the scan and deletes the
    # Filter node it came from; ProjectionPushdownStrategy, which runs afterwards,
    # derived the scan's read set from the surviving projection alone and never
    # looked at `Scan.predicates`. So the scan was told to read `flag` and then
    # asked to evaluate a predicate over `ts`. Those columns are collected now.
    # This is also where the fuzzer's mangled-identity variants came from — the
    # KeyError payload was the pruned column's identity.
    RegisteredDefect(
        id="expression-evaluation-failed-err-op",
        repro="SELECT IIF(id > 2, NULL, NULL) FROM testdata.planets",
        error_type="RuntimeError",
        signature="evaluation failed (err_op=",
        detail=(
            "Several expression shapes fail at execution with '(err_op=NN)', which names an "
            "opcode rather than a cause — as 'ExprMultiProjectOperator: expression evaluation "
            "failed' in a projection and 'ExprFilterOperator: predicate evaluation failed' in a "
            "WHERE clause. An err_op=NN with no further detail is the engine's marker for a "
            "kernel/type divergence.\n"
            "\n"
            "REPRO RE-POINTED 2026-08-09. The previous repro — NULLIF over a DECIMAL column, "
            "`NULLIF(gravity, -1.5)` — is FIXED: NULLIF lowers to IIF(a = b, NULL, a), whose one "
            "non-NULL branch is DECIMAL, and the bind-time branch check now refuses that at plan "
            "time ('is DECIMAL(18, 6), which IIF cannot blend') instead of letting it reach "
            "nc_dispatch, which rejects DECIMAL by design (scale is out-of-band, so a raw blend "
            "across differing scales would be silently wrong). The entry is NOT deleted, because "
            "its signature is deliberately broad and the class it absorbs is still open — the new "
            "repro, `IIF(c, NULL, NULL)`, still reaches draken_iif and dies with 'every branch is "
            "NULL — no result type' behind err_op=15. Deleting it would unmask the whole err_op "
            "class at once rather than the one shape that was fixed.\n"
            "\n"
            "Adjacent, same root, NOT covered by this entry's signature: `COALESCE(NULL, NULL)` "
            "raises a bare Python ValueError ('draken_ifnull: every branch is NULL — no result "
            "type') with no err_op wrapper and no SQL context at all. Both shapes are arguably "
            "answerable rather than refusable — CASE already answers its all-NULL form as an "
            "all-NULL column (draken_if_then_else's a_isnull && b_isnull block) — so what IIF and "
            "COALESCE should do here is an open architect question, not a settled gap."
        ),
    ),
    RegisteredDefect(
        id="non-native-function-call-in-an-expression-tree",
        repro=(
            "SELECT REGEXP_REPLACE(CAST(bin_null AS VARCHAR), '[aeiou]', '[0-9]+') "
            "FROM testdata.fuzzing.mixed"
        ),
        error_type="NotSupportedError",
        signature="a function call in",
        detail=(
            "A family of function calls has no native implementation once it appears inside "
            "another expression. Shapes the fuzzer hit: "
            "REGEXP_REPLACE over a CAST, RIGHT(CONCAT(...)), IFNOTNULL(TRUNC(...), CAST(...)), "
            "and TIME_BUCKET over a computed argument. Each names itself in the message: "
            "'a function call in `...`, outside the c-native kernel set'."
        ),
    ),
]

_BY_ID = {defect.id: defect for defect in REGISTER}
if len(_BY_ID) != len(REGISTER):
    raise AssertionError("duplicate id in the defect register")


# ─────────────────────────────────────────────────────────────────────────────
# RATIFIED SEMANTICS — ruled correct. Read the module docstring before adding
# one: this list has no "still reproduces" gate, and the only thing keeping it
# from becoming a dumping ground is that each entry must be cited by the code
# that stands down because of it.
# ─────────────────────────────────────────────────────────────────────────────
RATIFIED: List[RatifiedSemantic] = [
    RatifiedSemantic(
        id="string-concatenation-requires-homogeneous-string-types",
        example="SELECT CONCAT('p', b'a') AS x FROM testdata.planets LIMIT 1",
        ruling="architect, 2026-08-09",
        detail=(
            "`||` and CONCAT/CONCAT_WS concatenate ONE string type at a time. The string "
            "types are VARCHAR, NVARCHAR and VARBINARY; any MIX of them is an "
            "IncorrectTypeError the caller resolves with an explicit cast. Concatenating "
            "a VARBINARY with a VARBINARY is fine, and always was.\n"
            "\n"
            "Two entries were registered here, a WrongAnswer and a diagnostic, both on the "
            "premise that 'there is no VARBINARY concat kernel'. That premise was FALSE — "
            "`name::VARBINARY || name::VARBINARY` has always returned VARBINARY on the "
            "native column path. The kernel gate is `lt_is_string && lt == rt` "
            "(draken/ops/kernels/binop_dispatch.cpp), so what had no kernel was every "
            "MIXED pair, and that is six pairs, not one: VARCHAR||NVARCHAR was equally "
            "unsupported and nobody had noticed.\n"
            "\n"
            "The binder disagreed with the kernel, which is what produced two symptoms "
            "from one root. OPERATOR_MAP carried all six mixed pairs with a result type "
            "(VARBINARY dominant, else NVARCHAR), so the binder promised a result the "
            "engine could not produce, and WHERE the promise was broken depended on "
            "whether the operands were columns or literals:\n"
            "  * a mixed COLUMN pair reached the plan compiler, which found no C-native\n"
            "    binop and refused it as 'outside the c-native kernel set' — 'we have not\n"
            "    built it yet' for something we had in fact decided not to build.\n"
            "  * a mixed pair of LITERALS was constant-folded, and constant folding is the\n"
            "    one path that can still reach the Python coercion closure in\n"
            "    expression/evaluator/arithmetic.pyx. `_to_string_vec` had arms for\n"
            "    VARCHAR, NVARCHAR, NULL, bytes and str, and NONE for a VARBINARY Vector,\n"
            "    so it fell through to `str(v)` and stringified the Vector's Python repr —\n"
            "    a heap ADDRESS — into the answer. Identical queries returned different\n"
            "    strings run to run.\n"
            "\n"
            "That closure was the real defect: a second, COERCING implementation of concat "
            "living in Python, reachable only at plan time, disagreeing with the native "
            "kernel about what mixed operands mean. compiled_expression.pyx said so out "
            "loud — 'Mixed/non-string operands stay on the closure (which coerces)'.\n"
            "\n"
            "The six mixed rows are gone from planner/binder/operator_map.py, so `||` now "
            "refuses at the binder in the same class `id || ''` gets. CONCAT binds through "
            "the catalog with `any` parameters and desugars to a StringConcat chain "
            "post-bind, so the operator map never sees its operands; it enforces the same "
            "rule in `_concat_chain_type` (planner/optimizer/strategies/predicate_rewriter.py), "
            "which also resolves the ONE type every node of the desugared chain carries.\n"
            "\n"
            "The rule binds the operands that ARE strings. A non-string operand is still "
            "cast to VARCHAR, so `CONCAT(id, name)` keeps working — rendering an integer as "
            "text is total and lossless in a way binary->text is not. It follows that "
            "`CONCAT(id, b'a')` is refused: the integer becomes VARCHAR and VARCHAR does "
            "not mix with VARBINARY.\n"
            "\n"
            "CONCAT and CONCAT_WS now declare ONE OVERLOAD PER STRING TYPE in the "
            "registrar (opteryx/expression/functions/registrar/text.pyx), against three new "
            "`varchar`/`nvarchar`/`varbinary` parameter families. That was the architect's "
            "call on 2026-08-09 and it moved the rule out of code and into `reference/`: "
            "mixed operands now match no overload and are refused by resolution, and the "
            "RETURN TYPE follows the operand type, so `CONCAT(b'a', b'b')` is VARBINARY like "
            "`b'a' || b'b'` instead of the hardcoded VARCHAR it used to claim. The single "
            "`any`-typed overload that used to coerce non-strings went with it: "
            "`CONCAT(id, name)` is now an error naming the cast, matching `||`, which never "
            "coerced.\n"
            "\n"
            "Nothing in the generator stands down for this any more, which is the point of "
            "recording it: homogeneity became STRUCTURAL. The generator picks one overload and "
            "every parameter of it takes one string type, so it cannot build a mixed call. The "
            "_RATIFIED_NARROWINGS dict that withheld VARBINARY from CONCAT was deleted along "
            "with the narrowing, and homogeneous VARBINARY concat is now generated rather than "
            "excluded. The ruling is cited by "
            "test_string_concatenation_requires_homogeneous_string_types, which asserts the "
            "refusals, the return types and the absence of any object repr in a result.\n"
            "\n"
            "An untyped NULL stays legal and stays TRANSPARENT to overload selection "
            "(catalog.pyx scores it 0.0 for these three families): `CONCAT(name, NULL)` is NULL, "
            "as `name || NULL` always was. `NULL || NULL` is refused — no operand carries a "
            "string type for the result to adopt, and it only ever produced an answer because "
            "constant folding ran it through the Python closure that has since been deleted."
        ),
    ),
    RatifiedSemantic(
        id="varchar-is-ascii-bytes-and-non-ascii-content-is-undefined",
        example="SELECT REVERSE('ÅΩ漢字') AS r FROM testdata.planets LIMIT 1",
        ruling="architect, 2026-08-09",
        detail=(
            "VARCHAR is ASCII BYTES. Putting non-ASCII content in one is undefined "
            "behaviour, and an operation that mangles such content is behaving to "
            "contract — not failing. NVARCHAR is the UTF-8 type and the answer to every "
            "question of this shape.\n"
            "\n"
            "Two entries were registered here as diagnostic defects. Both are ruled "
            "correct, so both are gone from the register:\n"
            "  * REVERSE reverses BYTES for VARCHAR/VARBINARY and CODEPOINTS for\n"
            "    NVARCHAR. `REVERSE('ÅΩ漢字')` binds VARCHAR, so it returns an invalid\n"
            "    UTF-8 sequence and materialising it raises UnicodeDecodeError. That is\n"
            "    the contract, and it is consistent with the rest of the type: LENGTH on\n"
            "    the same literal already answers 9, the byte count.\n"
            "  * CAST(<VARBINARY> AS VARCHAR) reinterprets bytes as ASCII text and does\n"
            "    not validate UTF-8, for the same reason. Arbitrary bytes cast to VARCHAR\n"
            "    make an undecodable VARCHAR; the cast is not the thing that went wrong.\n"
            "\n"
            "types.json already states this for the type as a whole ('Non-ASCII bytes "
            "stored in a VARCHAR column produce undefined behaviour — use NVARCHAR for "
            "Unicode'), and the VARBINARY->VARCHAR cast now says it at the conversion "
            "too, which was the one place the documentation was silent.\n"
            "\n"
            "Acted on by single_table_grammar.py in two places, because a fuzzer that "
            "generates undefined-behaviour input and then asserts on the outcome is "
            "asking a question the contract never answered: CAST_TARGETS omits "
            "VARBINARY -> VARCHAR, and _STRING_LITERALS is ASCII-only. Neither is a "
            "coverage gap to be closed later — closing either would be generating input "
            "the engine makes no promise about. Exercising these deliberately belongs "
            "with NVARCHAR, where the promise exists."
        ),
    ),
    RatifiedSemantic(
        id="limit-and-offset-select-an-arbitrary-subset",
        example=(
            "SELECT * FROM (SELECT row_id, txt FROM testdata.fuzzing.wide OFFSET 6) AS s "
            "WHERE txt IS NOT NULL"
        ),
        ruling="architect, 2026-08-08",
        detail=(
            "LIMIT and OFFSET select an ARBITRARY subset, and which rows they select may "
            "differ between two executions of identical SQL. ORDER BY with ties leaves the "
            "tied rows in an arbitrary order on the same terms. Both are permitted; neither "
            "is a defect.\n"
            "\n"
            "This was filed as a WrongAnswer by the count_star_matches_materialised_rows "
            "oracle, which saw the example above return 170050 / 170051 / 170052 rows across "
            "runs while COUNT(*) over it returned a different one of those values again. Every "
            "one of those answers is legal. The oracle compares two SEPARATE executions, so it "
            "silently assumes the statement is deterministic — an assumption a limited "
            "statement never satisfies. The finding was about the oracle, not the engine.\n"
            "\n"
            "The two things the engine actually does, both investigated, both ruled fine:\n"
            "  * NativeParquetScanSource emits row groups in decode-COMPLETION order — it takes\n"
            "    whichever row group finished first, not the next work item. Proven directly:\n"
            "    `SELECT row_id FROM testdata.fuzzing.wide OFFSET 6 LIMIT 5` returns 6..10 on\n"
            "    most runs and 50006..50010 when the second row group wins the race. Delivering\n"
            "    in work-item order instead would make this reproducible, but reproducible is\n"
            "    not ordered: a parquet file gives no row order across row groups and a blob\n"
            "    listing gives none across files, so the work queue's order is itself arbitrary.\n"
            "    Freezing an arbitrary choice buys nothing and costs head-of-line blocking in\n"
            "    the decode pipeline.\n"
            "  * Every buffering sink combines worker-local morsels in completion order\n"
            "    (WindowSink::combine, src/cpp/engine/native_sort.hpp), so a LIMIT above a\n"
            "    window/sort/aggregate sees ties broken differently run to run. Visible as\n"
            "    `SELECT * FROM (SELECT row_id, cat FROM testdata.fuzzing.wide ORDER BY cat\n"
            "    LIMIT 20) AS s WHERE row_id > 100` returning 8 or 20. Making this deterministic\n"
            "    means an order-preserving engine — a sequence identity on CxxMorsel propagated\n"
            "    through every operator, DuckDB's preserve_insertion_order. Not being built to\n"
            "    satisfy an oracle that was asking the wrong question.\n"
            "\n"
            "WHAT IS STILL PROMISED, and still asserted: a LIMIT returns EXACTLY the requested "
            "number of rows when the relation has them, and every row it returns comes from the "
            "unlimited result. Those are limit_returns_the_right_number_of_rows and "
            "limit_rows_come_from_the_unlimited_result, and they run on limited statements.\n"
            "\n"
            "Acted on by applicable_oracles(), which declines count_star_matches_materialised_rows "
            "on any statement carrying a LIMIT or an OFFSET. That is a permanent exclusion, not a "
            "deadline: it costs ~25% of generated statements one oracle, and it is the price of "
            "that oracle assuming determinism."
        ),
    ),
]

_RATIFIED_BY_ID = {entry.id: entry for entry in RATIFIED}
if len(_RATIFIED_BY_ID) != len(RATIFIED):
    raise AssertionError("duplicate id in the ratified-semantics list")
if set(_RATIFIED_BY_ID) & set(_BY_ID):
    raise AssertionError("an id cannot be both a registered defect and a ratified semantic")


def match(error: BaseException) -> Optional[RegisteredDefect]:
    """The register entry this exception belongs to, if any.

    Matches on exception class NAME rather than on the class itself so the
    register does not have to import every Opteryx exception type, and on a
    message substring. An entry with an empty signature matches on type alone —
    used only where the message carries no stable text (a KeyError's payload is
    a random internal identity).
    """
    name = type(error).__name__
    message = str(error)
    for defect in REGISTER:
        if defect.error_type != name:
            continue
        if defect.signature and defect.signature not in message:
            continue
        return defect
    return None
