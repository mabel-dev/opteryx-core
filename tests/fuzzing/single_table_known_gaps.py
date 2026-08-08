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
    RegisteredDefect(
        id="nan-rows-fall-outside-every-predicate-bucket",
        repro=(
            "SELECT COUNT(*) AS n FROM testdata.satellites WHERE NOT (density > 72971.564572)"
        ),
        error_type="WrongAnswer",
        signature="",
        detail=(
            "testdata.satellites has 177 rows, no NULL densities, and one NaN (row 176). That "
            "row is selected by NOTHING:\n"
            "  WHERE density > 72971.564572          ->   0\n"
            "  WHERE NOT (density > 72971.564572)    -> 176   <- should be 177\n"
            "  WHERE (density > 72971.564572) IS NULL ->   0\n"
            "Both defensible semantics are violated. If a NaN comparison is FALSE (IEEE), then "
            "`NOT (...)` must be TRUE and the second line must be 177. If it is UNKNOWN (SQL "
            "three-valued logic), the third line must be 1. The engine answers FALSE to all "
            "three, so the row is invisible to any filter written over that column.\n"
            "Which semantics to adopt is the architect's call; that the three buckets must "
            "partition the relation is not.\n"
            "Related to the recorded NaN divergence in sort ordering.\n"
            "Pinned by test_wrong_answer_nan_row_still_falls_outside_every_bucket."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # CATALOG DISAGREES WITH THE ENGINE — `reference/` declares support the
    # engine does not provide. Either the engine or the catalog is wrong; both
    # are worth a decision.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="shift-operators-unparseable",
        repro="SELECT i_group << 2 FROM testdata.fuzzing.mixed",
        error_type="SqlError",
        signature="No infix parser for token ShiftLeft",
        detail=(
            "reference/operators.json declares ShiftLeft and ShiftRight as supported binary "
            "operators; the parser has no infix handler for either."
        ),
    ),
    RegisteredDefect(
        id="at-question-has-no-native-kernel",
        repro="SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE json_doc @? 'name'",
        error_type="NotSupportedError",
        signature="filter predicate outside the c-native kernel set",
        detail=(
            "reference/operators.json declares AtQuestion (@?) as a supported comparison; the "
            "native engine has no filter kernel for it."
        ),
    ),
    RegisteredDefect(
        id="array-agg-global-claimed-but-rejected",
        repro="SELECT ARRAY_AGG(s_low) FROM testdata.fuzzing.mixed",
        error_type="UnsupportedSyntaxError",
        signature="ARRAY_AGG requires a GROUP BY clause",
        detail=(
            "reference/aggregates.json records ARRAY_AGG support.global = true; the planner "
            "rejects ARRAY_AGG without a GROUP BY."
        ),
    ),
    RegisteredDefect(
        id="two-argument-substring-is-unbindable",
        repro="SELECT SUBSTRING(name, 2) FROM testdata.planets",
        error_type="IncompatibleTypesError",
        signature="SUBSTRING arg3 (NULL)",
        detail=(
            "reference/function_signatures.json declares overload SUBSTRING_2 (string, from_pos). "
            "Calling it raises 'SUBSTRING arg3 (NULL): expected INTEGER - an untyped NULL has no "
            "type to match' — the binder objects to a third argument the caller never wrote, so "
            "the two-argument form is unreachable."
        ),
    ),
    RegisteredDefect(
        id="coalesce-single-argument-raises-valueerror",
        repro="SELECT COALESCE(1.0) FROM testdata.fuzzing.mixed LIMIT 1",
        error_type="ValueError",
        signature="draken_coalesce: expected at least 2 arguments",
        detail=(
            "The catalog declares COALESCE arity minimum = 1 and the binder accepts one argument; "
            "the kernel then raises a raw Python ValueError. Either the catalog minimum is wrong "
            "or a one-argument COALESCE should fold to its argument."
        ),
    ),
    RegisteredDefect(
        id="timestamp-to-date-cast-has-no-kernel",
        repro="SELECT CAST(ts_value AS DATE) FROM testdata.fuzzing.mixed",
        error_type="NotSupportedError",
        signature="outside the c-native kernel set",
        detail=(
            "types.json documents TIMESTAMP -> DATE ('Truncates the time component'); the native "
            "engine has no kernel for it."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # REDUNDANT SYNTAX CHANGES THE OUTCOME — semantically identical spellings
    # where one works and the other does not. These share a root: redundant
    # parentheses are not normalised away before the plan is built.
    # ─────────────────────────────────────────────────────────────────────────
    RegisteredDefect(
        id="parenthesised-expression-loses-its-alias",
        repro="SELECT x FROM (SELECT (id + 1) AS x FROM testdata.planets) AS s",
        error_type="ColumnNotFoundError",
        signature="Unknown column 'x'",
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
            "The same shapes over an INTEGER or VARCHAR column all run."
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
        signature="doesn't appear in a FROM or JOIN",
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
        signature="window ORDER BY column the stream does not carry",
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
            "reference/function_signatures.json types TIME_BUCKET's `magnitude` parameter as "
            "`number`, so a DECIMAL (or a negative) satisfies the signature; the kernel then "
            "raises a raw Python TypeError. Either the catalog should type it `integer` and "
            "constrain the sign, or the kernel should reject it as a SQL error."
        ),
    ),
    RegisteredDefect(
        id="case-rejects-two-identical-array-branches",
        repro=(
            "SELECT (CASE WHEN b_value = FALSE THEN arr_int ELSE arr_int END) "
            "FROM testdata.fuzzing.mixed"
        ),
        error_type="IncompatibleTypesError",
        signature="CASE branches must share a compatible type",
        detail=(
            "A CASE whose two branches are the SAME ARRAY column is rejected, with a message "
            "that contradicts itself: \"column 'arr_int' is ARRAY<INT64> but column 'arr_int' is "
            "ARRAY<INT64> — CASE branches must share a compatible type\". The list of compatible "
            "families in the message excludes ARRAY entirely, so the real rule appears to be "
            "'CASE does not support ARRAY' — which the message should say."
        ),
    ),
    RegisteredDefect(
        id="reverse-corrupts-multi-byte-utf8",
        repro="SELECT REVERSE('\u00c5\u03a9\u6f22\u5b57') AS r FROM testdata.planets LIMIT 1",
        error_type="UnicodeDecodeError",
        signature="invalid start byte",
        detail=(
            "REVERSE reverses BYTES, not codepoints, so any multi-byte UTF-8 input comes back as "
            "an invalid byte sequence. The corruption is in the kernel; it surfaces at the Python "
            "boundary as UnicodeDecodeError('utf-8' codec can't decode byte 0x97 in position 0) "
            "when the result vector is materialised. A consumer that never decodes the string "
            "gets silently mojibake instead of an error."
        ),
    ),
    RegisteredDefect(
        id="json-function-on-non-json-text-raises-valueerror",
        repro="SELECT JSONB_OBJECT_KEYS('delta') FROM testdata.planets",
        error_type="RuntimeError",
        signature="jsonb_object_keys: invalid JSON",
        detail=(
            "JSONB_OBJECT_KEYS fails at execution on input that is not a JSON document. "
            "reference/function_signatures.json types the parameter as a plain `varchar` with no "
            "indication that it must parse as JSON, so every VARCHAR is a legal argument by the "
            "signature and most of them fail at run time. In a bare projection the failure "
            "surfaces as a ValueError; inside a larger expression the multi-project operator "
            "wraps it as 'expression evaluation failed (err_op=15)', which is the form recorded "
            "here."
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
    RegisteredDefect(
        id="window-plus-folded-literal-concat-filter-raises-typeerror",
        repro=(
            "SELECT flag, COUNT(grp_wide) OVER (PARTITION BY flag) AS w "
            "FROM testdata.fuzzing.wide WHERE (CONCAT(647310.0000, b'item') < 'zeta')"
        ),
        error_type="TypeError",
        signature="'NoneType' object is not callable",
        detail=(
            "An AGGREGATE window combined with a WHERE clause whose predicate is a CONCAT over "
            "LITERALS raises a bare Python TypeError. Each half is fine on its own: the same "
            "window with `WHERE ('a' < 'zeta')` or `WHERE row_id > 5` runs, the same CONCAT "
            "predicate without the window runs, and a RANKING window with the literal predicate "
            "runs. Removing the ORDER BY does not help, so it is the window plus the folded "
            "literal predicate.\n"
            "AND ONE REDUNDANT PAREN PAIR DECIDES IT: `WHERE (CONCAT(...) < 'zeta')` raises, "
            "`WHERE CONCAT(...) < 'zeta'` runs. Third member of the same family as "
            "parenthesised-expression-loses-its-alias and "
            "float-in-list-only-works-at-top-level — redundant parentheses are not normalised "
            "away before the plan is built, and they change what the engine can execute."
        ),
    ),
    RegisteredDefect(
        id="identical-function-call-in-both-case-branches-is-unbound",
        repro=(
            "SELECT CASE WHEN id > 1 THEN UPPER(name) ELSE UPPER(name) END AS x "
            "FROM testdata.planets"
        ),
        error_type="ValueError",
        signature="has no function_ref — not bound",
        detail=(
            "A CASE whose two branches are the IDENTICAL function call raises "
            "ValueError('compiled_expression: FUNCTION 'UPPER' has no function_ref — not bound'). "
            "Not specific to UPPER — MD5 and HASH fail the same way. Every variation works:\n"
            "  THEN UPPER(name) ELSE LOWER(name)  -> ok   (different calls)\n"
            "  THEN UPPER(name) ELSE 'x'          -> ok   (one branch a literal)\n"
            "  THEN name        ELSE name         -> ok   (identical COLUMNS, not calls)\n"
            "The shape reads as common-subexpression sharing binding the call once and leaving "
            "the second reference unbound. A raw ValueError naming an internal field is not an "
            "actionable diagnostic either."
        ),
    ),
    RegisteredDefect(
        id="filtered-aggregate-window-cardinality-estimate-explodes",
        repro=(
            "SELECT flag, grp_wide, AVG(row_id) OVER (PARTITION BY flag) AS w "
            "FROM testdata.fuzzing.wide WHERE flag = TRUE"
        ),
        error_type="ResultTooLargeError",
        signature="estimated to return 2,000,000,000 rows",
        detail=(
            "An aggregate window plus ANY filter makes the row-count estimator report "
            "2,000,000,000 rows for a relation of 200,000 — a 10,000x over-estimate — and the "
            "`sql_select_limit` guard then REFUSES to run the query. Nothing is wrong with the "
            "query: dropping the WHERE runs it and returns 200,000 rows, and an ORDER BY makes "
            "no difference either way.\n"
            "A filter can only reduce cardinality, so an estimate that rises when one is added "
            "is wrong on its face. The consequence is not a slow plan, it is an ordinary query "
            "the engine will not execute at all."
        ),
    ),
    RegisteredDefect(
        id="folded-string-function-predicate-under-an-outer-filter-raises-typeerror",
        repro=(
            "SELECT Rocket_Status FROM (SELECT Rocket_Status FROM testdata.missions "
            "WHERE (INITCAP('item') > Rocket_Status)) AS s "
            "WHERE Rocket_Status IN ('0', 'zeta')"
        ),
        error_type="TypeError",
        signature="not supported between instances of 'bytes' and 'str'",
        detail=(
            "A subquery predicate comparing a CONSTANT-FOLDED string function against a column, "
            "combined with a filter on the OUTER query, raises a raw Python TypeError: one side "
            "arrives as bytes and the other as str. Each half is fine alone:\n"
            "  the subquery on its own                                      -> 11 rows\n"
            "  SELECT ... FROM (<subquery>) AS s  (no outer filter)         -> 11 rows\n"
            "  WHERE INITCAP('item') > Rocket_Status  un-nested             -> ok\n"
            "  the same shape with a plain literal ('Item' > Rocket_Status) -> ok\n"
            "so it needs the folded function call AND the outer filter. Sibling of "
            "temporal-function-result-compared-to-a-literal-raises-typeerror: a folded value "
            "reaches a comparison in a different representation from the column."
        ),
    ),
    RegisteredDefect(
        id="from-unixtime-out-of-range",
        repro="SELECT FROM_UNIXTIME(374000000000.0) FROM testdata.planets LIMIT 1",
        error_type="ValueError",
        signature="year must be in 1..9999",
        detail=(
            "FROM_UNIXTIME raises a raw Python ValueError on an epoch value past year 9999. "
            "reference/function_signatures.json types the parameter as an unbounded `number`, so "
            "the out-of-range value satisfies the signature and fails at execution."
        ),
    ),
    RegisteredDefect(
        id="temporal-function-result-compared-to-a-literal-raises-typeerror",
        repro=(
            "SELECT COUNT(*) AS n FROM testdata.fuzzing.wide "
            "WHERE TRUNC(ts, 'hour') <= '1995-07-04 00:00:00'::TIMESTAMP"
        ),
        error_type="TypeError",
        signature="not supported between instances of 'datetime.datetime' and 'int'",
        detail=(
            "Comparing a temporal FUNCTION RESULT against a temporal literal raises a raw Python "
            "TypeError: one side arrives as a datetime and the other as an epoch int. "
            "`SELECT TRUNC(ts, 'hour') FROM ...` on its own is fine, and `WHERE ts <= <literal>` "
            "on the raw column is fine — it is the function result in a comparison. Also "
            "reproduces with the literal written as CAST('1995-07-04'::DATE AS TIMESTAMP)."
        ),
    ),
    RegisteredDefect(
        id="to-char-out-of-range-codepoint",
        repro="SELECT TO_CHAR(-303083) FROM testdata.planets",
        error_type="ValueError",
        signature="is not a Unicode scalar value",
        detail=(
            "TO_CHAR's argument is a Unicode CODEPOINT, but reference/function_signatures.json "
            "types it as a plain `integer` with no range. Out-of-range values raise a raw Python "
            "ValueError ('draken_to_char: codepoint -303083 is not a Unicode scalar value') "
            "instead of a SQL error."
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
    RegisteredDefect(
        id="cast-binary-to-varchar-yields-undecodable-text",
        repro="SELECT CAST(bin_value AS VARCHAR) FROM testdata.fuzzing.mixed",
        error_type="UnicodeDecodeError",
        signature="codec can't decode",
        detail=(
            "CAST(<VARBINARY> AS VARCHAR) reinterprets arbitrary bytes as text without checking "
            "that they are valid UTF-8. The cast itself succeeds; the failure only appears when "
            "the result is materialised at the Python boundary. A consumer that never decodes "
            "gets mojibake with no indication anything went wrong. types.json documents the "
            "conversion without stating what happens to non-UTF-8 input."
        ),
    ),
    RegisteredDefect(
        id="nan-comparison-in-a-filter-raises-valueerror",
        repro="SELECT COUNT(*) FROM testdata.planets WHERE orbital_period >= SQRT(-390664.0)",
        error_type="ValueError",
        signature="cannot convert float NaN to integer",
        detail=(
            "Comparing a column against a NaN-valued expression raises a raw Python "
            "ValueError from inside the filter. `SELECT SQRT(-390664.0) FROM ...` on its own "
            "returns NaN happily, so the NaN is produced fine and then breaks the comparison. "
            "Under IEEE semantics every comparison with NaN is FALSE; whatever the engine "
            "decides here, a Python ValueError is not it."
        ),
    ),
    RegisteredDefect(
        id="cast-to-bare-decimal-raises-valueerror",
        repro="SELECT CAST(i_value AS DECIMAL) FROM testdata.fuzzing.mixed",
        error_type="ValueError",
        signature="CAST to DECIMAL requires (precision, scale)",
        detail=(
            "A raw Python ValueError rather than an Opteryx SqlError, so a plain user mistake "
            "surfaces as an internal error."
        ),
    ),
    RegisteredDefect(
        id="decimal-case-blend-with-a-literal-overflows",
        repro=(
            "SELECT (CASE WHEN row_id > 1 THEN d_value ELSE 1.5000 END) FROM testdata.fuzzing.mixed"
        ),
        error_type="OverflowError",
        signature="decimal:",
        detail=(
            "A CASE blending a DECIMAL column with a DECIMAL literal raises a raw Python "
            "OverflowError ('value exceeds declared precision', or 'unscaled value does not fit "
            "in int64 range' for a larger literal). Blending two DECIMAL COLUMNS is fine, and "
            "`d_value + 1.5000` outside a CASE is fine — it is the CASE blend's rescale against "
            "a literal."
        ),
    ),
    RegisteredDefect(
        id="cte-projecting-a-column-the-stream-does-not-carry",
        repro=(
            "WITH c AS (SELECT row_id AS e1, ts FROM testdata.fuzzing.wide) "
            "SELECT COUNT(ts) AS a3, COUNT_DISTINCT(ts) AS a4 FROM c WHERE NOT (ts IS NULL)"
        ),
        error_type="NotSupportedError",
        signature="projecting a column the stream does not carry",
        detail=(
            "A CTE that projects a column the outer query does not use, read by TWO aggregates "
            "under an outer WHERE, fails with 'projecting a column the stream does not carry'. "
            "Dropping either aggregate makes it work, as does dropping the unused `row_id AS e1`. "
            "Same family as subquery-alias-filtered-but-not-projected-raises-keyerror: a column "
            "is pruned from the stream while something downstream still references it."
        ),
    ),
    RegisteredDefect(
        id="subquery-alias-filtered-but-not-projected-raises-keyerror",
        repro=(
            "SELECT flag FROM (SELECT ts AS e1, flag FROM testdata.fuzzing.wide) AS sub "
            "WHERE e1 IS NOT NULL"
        ),
        error_type="KeyError",
        signature="",
        detail=(
            "Filtering on a subquery column the OUTER query does not project raises a bare "
            "Python KeyError: 'CxxMorsel.column: not found'. Dropping the WHERE clause makes the "
            "same query work, so the column is being pruned from the stream while the filter "
            "still references it. Related variants seen from the fuzzer carry an internal "
            "mangled identity in the payload: KeyError: b'tes_ts_ZCPzZpBJ', and \"native engine: "
            "expression references column b'tes_f_n_UB0TfUCS' which the stream does not carry\". "
            "A KeyError on an internal name is never an actionable diagnostic."
        ),
    ),
    RegisteredDefect(
        id="expression-evaluation-failed-err-op",
        repro="SELECT NULLIF(gravity, -1.5) FROM testdata.planets",
        error_type="RuntimeError",
        signature="evaluation failed (err_op=",
        detail=(
            "Several expression shapes fail at execution with '(err_op=NN)', which names an "
            "opcode rather than a cause — as 'ExprMultiProjectOperator: expression evaluation "
            "failed' in a projection and 'ExprFilterOperator: predicate evaluation failed' in a "
            "WHERE clause. Confirmed triggers: NULLIF over a DECIMAL column (the repro), and "
            "RLIKE in operand position (registered separately). An err_op=NN with no further "
            "detail is the engine's marker for a kernel/type divergence."
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
