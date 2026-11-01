"""The reference fuzzer's register of known Opteryx/DuckDB differences.

Same contract as `join_known_gaps.py`: a register of differences already found,
not an allowlist. Every entry carries a live repro that
`test_sql_fuzzer_reference.py::test_registered_gap_still_reproduces` runs on every
invocation and requires to STILL differ in the recorded way — so a fix turns the
register red and the entry must be deleted, which puts the shape straight back
into fuzzing.

THREE KINDS OF ENTRY
--------------------
* WITHHELD — the generator does not emit the shape (`reference_grammar` names the
  entry at the exclusion site). Used for every known WRONG ANSWER: a wrong answer
  is never absorbed by matching, so the only way to stop it re-reporting on every
  run is not to generate it. Also used for refusals whose shape is easy to name.
  The repro is Opteryx SQL plus the wrong/refused outcome it must still produce.

* SIGNATURE — the outcome is "one engine raised, the other answered" and the
  raising side's message contains `signature`. The repro is a `Query`, run
  through the same `compare()` the fuzzer uses.

* CRASH — the engine raised something outside its rejection types (a raw
  `decimal.InvalidOperation`, an `OverflowError`). Matched on exception type name
  plus `signature`; the repro is a `Query`.

THE HONEST LIMITATION, as in the other registers: a SIGNATURE or CRASH entry
absorbs any new bug that happens to produce the same message. Signatures are kept
as narrow as the messages allow and hit counts are printed at the end of a run.
"""

from __future__ import annotations

import decimal
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from tests.fuzzing.reference_grammar import Agg
from tests.fuzzing.reference_grammar import Between
from tests.fuzzing.reference_grammar import Binary
from tests.fuzzing.reference_grammar import Case
from tests.fuzzing.reference_grammar import Col
from tests.fuzzing.reference_grammar import Column
from tests.fuzzing.reference_grammar import INT64_MAX
from tests.fuzzing.reference_grammar import INT64_MIN
from tests.fuzzing.reference_grammar import IsNull
from tests.fuzzing.reference_grammar import Join
from tests.fuzzing.reference_grammar import Lit
from tests.fuzzing.reference_grammar import Query
from tests.fuzzing.reference_grammar import Table
from tests.fuzzing.reference_grammar import Ty

WITHHELD, SIGNATURE, CRASH = "withheld", "signature", "crash"


@dataclass(frozen=True)
class Gap:
    id: str
    kind: str
    detail: str
    #: SIGNATURE: "opteryx_error" or "reference_error". CRASH: exception class name.
    outcome: str = ""
    #: Substrings of the raising side's message; any one matches. "" matches on
    #: outcome alone.
    signatures: Tuple[str, ...] = ("",)
    #: SIGNATURE / CRASH repro, or a WITHHELD repro that needs both engines.
    query: Optional[Callable[[], Query]] = None
    #: WITHHELD repro: Opteryx SQL (with `still_present(rows, error)`), or `query`
    #: (with `still_present(verdict)`). True while the defect is still present.
    sql: str = ""
    still_present: Optional[Callable] = None


def _one_row(*columns_and_values) -> Table:
    columns = tuple(Column(f"c{i}", ty) for i, (ty, _) in enumerate(columns_and_values))
    return Table("t0", columns, (tuple(v for _, v in columns_and_values),))


def _raised(error_name: str, fragment: str):
    return lambda rows, error: error is not None and type(error).__name__ == error_name and fragment in str(error)


REGISTER: List[Gap] = [
    # ─────────────────────────────────────────────────────────────────────────
    # WITHHELD — divergences
    # ─────────────────────────────────────────────────────────────────────────
    Gap(
        id="numeric-to-integer-cast-truncates",
        kind=WITHHELD,
        sql="SELECT CAST(CAST('-2.500' AS DECIMAL(12,3)) AS INT64) AS a, CAST(CAST('3.5' AS FLOAT64) AS INT64) AS b",
        still_present=lambda rows, error: error is None and rows == [(-2, 3)],
        detail=(
            "DIVERGENCE (semantics for the architect). CAST from DECIMAL or FLOAT64 to "
            "INT64 TRUNCATES toward zero: -2.5 -> -2, 3.5 -> 3, -2.7 -> -2. PostgreSQL "
            "and DuckDB both ROUND (PostgreSQL half away from zero for numeric, DuckDB "
            "half away for DECIMAL and half-even for DOUBLE): -2.5 -> -3, 3.5 -> 4. "
            "Withheld: reference_grammar._CASTS emits no FLOAT/DEC -> INT cast."
        ),
    ),
    Gap(
        id="cast-boolean-to-varchar-capitalised",
        kind=WITHHELD,
        sql="SELECT CAST(TRUE AS VARCHAR) AS x",
        still_present=lambda rows, error: error is None and rows == [("True",)],
        detail=(
            "DIVERGENCE. CAST(TRUE AS VARCHAR) is 'True' (Python's spelling); PostgreSQL "
            "and DuckDB give 'true'. Withheld: reference_grammar._CASTS emits no BOOL -> "
            "VARCHAR cast."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # WITHHELD — refusals
    # ─────────────────────────────────────────────────────────────────────────
    Gap(
        id="where-bare-boolean-column-refused",
        kind=WITHHELD,
        sql="SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE b_value",
        still_present=_raised("UnsupportedSyntaxError", "cannot be a bare column name"),
        detail=(
            "RULED DIALECT (2026-08-10, logical_planner._validate_where_clause_expression): "
            "a bare identifier as a WHERE/ON clause is refused as a probable typo ('Say "
            "what to compare it to'), boolean or not; standard SQL and DuckDB accept "
            "`WHERE bool_col`. Withheld: the generator writes `col = TRUE`."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # SIGNATURE — one side raised, the other answered
    # ─────────────────────────────────────────────────────────────────────────
    Gap(
        id="division-by-zero-answers",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=("division by zero",),
        query=lambda: Query(
            tables=(_one_row((Ty.FLOAT, 1.0)),),
            items=(Binary("/", Col("t0", "c0", Ty.FLOAT), Lit(Ty.FLOAT, 0.0), Ty.FLOAT),),
        ),
        detail=(
            "DIALECT, RULED. Division by zero does not raise: true division `/` is IEEE "
            "(Infinity, NaN for 0/0 — draken float ruling 2026-05-22, kept when integer "
            "DIV/% was made to raise 2026-09-29) and DECIMAL `x / 0` is NULL (ruled "
            "2026-08-17, TPC-DS Q90). The standard and PostgreSQL raise; the reference text "
            "raises via `error('division by zero')`. `%` by zero DOES raise in Opteryx, so "
            "it agrees."
        ),
    ),
    Gap(
        id="reference-decimal-multiply-overflow",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=("Overflow in multiplication of DECIMAL",),
        query=lambda: Query(
            tables=(_one_row((Ty.DEC, decimal.Decimal("999999999.999"))),),
            items=(Binary("*", Col("t0", "c0", Ty.DEC), Col("t0", "c0", Ty.DEC), Ty.DEC),),
        ),
        detail=(
            "REFERENCE LIMITATION — Opteryx is right. DECIMAL(12,3) * DECIMAL(12,3) "
            "overflows DuckDB's DECIMAL(18) result type; Opteryx widens and answers "
            "999999999998000000.000001, which is what PostgreSQL's numeric gives."
        ),
    ),
    Gap(
        id="domain-error-on-rows-never-output",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=("modulo by zero", "out of range for DECIMAL"),
        query=lambda: Query(
            tables=(Table("t0", (Column("c0", Ty.INT),), ()),),
            items=(Binary("%", Lit(Ty.INT, 1), Binary("%", Lit(Ty.INT, 2), Lit(Ty.INT, 1), Ty.INT), Ty.INT),),
        ),
        detail=(
            "EVALUATION ORDER, not a wrong answer. Opteryx constant-folds `1 % (2 % 1)` "
            "(or `CAST(-Infinity AS DECIMAL(12,3))`) and raises even over an empty table; "
            "DuckDB never evaluates it and answers 0 rows. PostgreSQL constant-folds too and "
            "raises the same way. When DuckDB "
            "DOES evaluate a modulo by zero the reference text raises and the engines "
            "agree, so this signature only fires when the reference skipped the rows."
        ),
    ),
    Gap(
        id="sum-int64-overflow-raises",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=("SUM overflow",),
        query=lambda: Query(
            tables=(Table("t0", (Column("c0", Ty.INT),), ((INT64_MAX,), (INT64_MAX,))),),
            items=(Agg("SUM", Col("t0", "c0", Ty.INT), Ty.INT),),
        ),
        detail=(
            "DIVERGENCE, ruled. SUM over INT64 raises when the exact sum exceeds INT64 "
            "('fail loud, never a wrapped answer'); DuckDB widens SUM(BIGINT) to HUGEINT "
            "and answers, PostgreSQL widens to numeric. Registered ahead of "
            "'overflow-on-rows-never-output' so the SUM message is not absorbed there."
        ),
    ),
    Gap(
        id="overflow-on-rows-never-output",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=(
            "multiplication overflow",
            "addition overflow",
            "subtraction overflow",
            "negation overflow",
            "overflows DECIMAL",
            "out of range for INT64",
        ),
        query=lambda: Query(
            tables=(
                Table("t0", (Column("c0", Ty.INT),), ((1,),)),
                Table("t1", (Column("c0", Ty.INT), Column("c1", Ty.INT)), ((2, INT64_MAX),)),
            ),
            items=(Col("t0", "c0", Ty.INT),),
            join=Join("INNER", Binary("=", Col("t0", "c0", Ty.INT), Col("t1", "c0", Ty.INT), Ty.BOOL)),
            where=Binary(">", Binary("*", Col("t1", "c1", Ty.INT), Lit(Ty.INT, 2), Ty.INT), Lit(Ty.INT, 0), Ty.BOOL),
        ),
        detail=(
            "EVALUATION ORDER, not a wrong answer. Opteryx evaluates an expression on rows "
            "DuckDB never evaluates, and raises a correct overflow on one: a WHERE pushed "
            "below a join raising on a row the join discards (the repro), or a projection "
            "under `LIMIT 0` raising `cast integer->decimal: value overflows "
            "DECIMAL(12, 3)`. SQL does not fix evaluation order, so raising is "
            "permitted; when DuckDB does evaluate the row it raises too and the engines "
            "agree. LIMITATION: these signatures also absorb an overflow on a row that "
            "DOES reach the output, if DuckDB answered for it without overflowing."
        ),
    ),
    Gap(
        id="reference-int64-min-modulo-minus-one",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=("Overflow in division of -9223372036854775808 / -1",),
        query=lambda: Query(
            tables=(_one_row((Ty.INT, INT64_MIN)),),
            items=(Binary("%", Col("t0", "c0", Ty.INT), Lit(Ty.INT, -1), Ty.INT),),
        ),
        detail=(
            "REFERENCE LIMITATION — Opteryx is right. INT64_MIN % -1 is 0 (Opteryx, and "
            "PostgreSQL, which special-cases it); DuckDB computes it through the "
            "overflowing division and raises."
        ),
    ),
    Gap(
        id="reference-null-case-between-bind-error",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=("BindBuiltinScalarFunction did not return a BoundFunctionExpression",),
        query=lambda: Query(
            tables=(_one_row((Ty.DEC, decimal.Decimal("1.250"))),),
            items=(Col("t0", "c0", Ty.DEC),),
            where=Between(
                Case(IsNull(Col("t0", "c0", Ty.DEC), True), Lit(Ty.STR, None), Lit(Ty.STR, "é"), Ty.STR),
                Lit(Ty.STR, "_a"),
                Lit(Ty.STR, "z"),
            ),
        ),
        detail=(
            "REFERENCE LIMITATION — Opteryx is right. `CASE WHEN c IS NOT NULL THEN "
            "CAST(NULL AS VARCHAR) ELSE 'é' END BETWEEN '_a' AND 'z'` makes DuckDB 2.0.0 "
            "raise an internal binder error; the CASE is NULL for the row, so Opteryx's 0 "
            "rows is the answer."
        ),
    ),
    Gap(
        id="reference-overflow-on-rows-never-output",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=(
            "Overflow in addition of INT64",
            "Overflow in subtraction of INT64",
            "Overflow in multiplication of INT64",
            "Could not cast value",
        ),
        query=lambda: Query(
            tables=(
                Table("t0", (Column("c0", Ty.STR),), ()),
                Table("t1", (Column("c0", Ty.INT), Column("c1", Ty.STR)), ((INT64_MAX, "a"),)),
            ),
            items=(Col("t0", "c0", Ty.STR),),
            join=Join("LEFT", Binary("=", Col("t0", "c0", Ty.STR), Col("t1", "c1", Ty.STR), Ty.BOOL)),
            where=Binary(">=", Binary("+", Col("t1", "c0", Ty.INT), Lit(Ty.INT, 3), Ty.INT), Lit(Ty.FLOAT, -1e12), Ty.BOOL),
        ),
        detail=(
            "EVALUATION ORDER, the mirror of 'overflow-on-rows-never-output': DuckDB "
            "pushes a WHERE on the right side of a LEFT JOIN below the join and raises "
            "INT64 overflow on a row no output row can carry (the left side is empty); "
            "Opteryx answers 0 rows. Same for a CAST it pushes below an INNER join (`CAST("
            "BIGINT_MAX AS DECIMAL(12,3))` on a row whose key `Infinity` matches nothing). LIMITATION: this would also absorb Opteryx silently "
            "wrapping a value DuckDB overflowed on; Opteryx's ruled behaviour is to raise "
            "on INT64 overflow, which 'overflow-on-rows-never-output' shows it does."
        ),
    ),
]

BY_ID = {gap.id: gap for gap in REGISTER}


def match_verdict(verdict) -> Optional[Gap]:
    """The SIGNATURE entry an error-vs-answer verdict belongs to. A wrong answer
    has no entry and can never match."""
    if verdict.kind == "opteryx_error":
        message = str(verdict.opteryx.error)
    elif verdict.kind == "reference_error":
        message = str(verdict.reference.error)
    else:
        return None
    for gap in REGISTER:
        if gap.kind == SIGNATURE and gap.outcome == verdict.kind and any(sig in message for sig in gap.signatures):
            return gap
    return None


def match_crash(error: BaseException) -> Optional[Gap]:
    for gap in REGISTER:
        if gap.kind == CRASH and type(error).__name__ == gap.outcome and any(sig in str(error) for sig in gap.signatures):
            return gap
    return None

