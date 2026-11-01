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
from tests.fuzzing.reference_grammar import Binary
from tests.fuzzing.reference_grammar import Cast
from tests.fuzzing.reference_grammar import Col
from tests.fuzzing.reference_grammar import Column
from tests.fuzzing.reference_grammar import INT64_MAX
from tests.fuzzing.reference_grammar import INT64_MIN
from tests.fuzzing.reference_grammar import Join
from tests.fuzzing.reference_grammar import Lit
from tests.fuzzing.reference_grammar import Not
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
        id="typed-null-literal-refused",
        kind=WITHHELD,
        sql="SELECT AVG(CAST(NULL AS DECIMAL(12,3))) FROM $planets",
        still_present=_raised("NotSupportedError", "null::DECIMAL(12,3)"),
        detail=(
            "REFUSAL. A typed NULL literal loses its type and is refused in many positions: "
            "`CAST(NULL AS DECIMAL(12,3))` in SUM/AVG/CASE/arithmetic ('a CAST ... outside "
            "the c-native kernel set'), MIN/MAX(CAST(NULL AS TIMESTAMP)) ('over a column of "
            "type NULL'), ORDER BY on one, IN/NOT IN lists containing one (mixed-type "
            "array, native scan refused), BETWEEN with one as a bound ('Temporal "
            "comparison requires literals to be explicitly cast'). DuckDB answers all of "
            "them. Withheld: the generator emits no NULL literals; NULLs arrive only "
            "through the data."
        ),
    ),
    Gap(
        id="where-bare-boolean-column-refused",
        kind=WITHHELD,
        sql="SELECT COUNT(*) FROM testdata.fuzzing.mixed WHERE b_value",
        still_present=_raised("UnsupportedSyntaxError", "cannot be a bare column name"),
        detail=(
            "REFUSAL. `WHERE bool_col` and `WHERE NOT bool_col` are refused ('Say what to "
            "compare it to'); standard SQL and DuckDB accept a boolean column as a search "
            "condition. Withheld: the generator writes `col = TRUE`."
        ),
    ),
    Gap(
        id="join-keys-must-share-a-type",
        kind=WITHHELD,
        query=lambda: Query(
            tables=(_one_row((Ty.FLOAT, 1.0)), Table("t1", (Column("c0", Ty.INT),), ((1,),))),
            items=(Col("t0", "c0", Ty.FLOAT),),
            join=Join("INNER", Binary("=", Col("t0", "c0", Ty.FLOAT), Col("t1", "c0", Ty.INT), Ty.BOOL)),
        ),
        still_present=lambda verdict: verdict.kind == "opteryx_error"
        and type(verdict.opteryx.error).__name__ == "IncompatibleTypesError",
        detail=(
            "REFUSAL. An equi-join of a FLOAT64 key to an INT64 key read from parquet is "
            "refused ('cannot be compared, because their types do not match'), though "
            "the same comparison is accepted in WHERE, and `$planets.id = "
            "testdata.planets.escape_velocity` joins. DuckDB and PostgreSQL join them. "
            "(Jepsen found the same gap in ParticleDB, #58.) Withheld: join keys are "
            "same-typed."
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
        id="cast-of-compound-predicate-refused-in-filter",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=("a CAST in a filter predicate",),
        query=lambda: Query(
            tables=(_one_row((Ty.INT, 1)),),
            items=(Col("t0", "c0", Ty.INT),),
            where=Binary(
                "<",
                Cast(Not(Binary(">", Col("t0", "c0", Ty.INT), Lit(Ty.INT, 3), Ty.BOOL)), Ty.INT),
                Lit(Ty.INT, 5),
                Ty.BOOL,
            ),
        ),
        detail=(
            "REFUSAL. In a WHERE, CAST(<AND / BETWEEN / NOT predicate> AS INT64) is refused "
            "('outside the c-native kernel set'); the same cast over a plain comparison or "
            "an OR runs, and every form runs in a projection. Note the refusal message "
            "renders `CAST(NOT (id > 3) AS INT64) < 5` as `id <= 3::INT64 < 5`."
        ),
    ),
    Gap(
        id="decimal-compare-fast-path-declined",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=("BC_COMPARE fast path declined",),
        query=lambda: Query(
            tables=(_one_row((Ty.DEC, decimal.Decimal("1.250"))),),
            items=(Col("t0", "c0", Ty.DEC),),
            where=Binary(
                "<=",
                Binary("*", Col("t0", "c0", Ty.DEC), Lit(Ty.DEC, decimal.Decimal("0")), Ty.DEC),
                Binary("*", Lit(Ty.DEC, decimal.Decimal("0.001")), Lit(Ty.DEC, decimal.Decimal("999999999.999")), Ty.DEC),
                Ty.BOOL,
            ),
        ),
        detail=(
            "ENGINE ERROR. Comparing two DECIMAL products of different result scales in a "
            "WHERE fails mid-execution with RuntimeError 'BC_COMPARE fast path declined "
            "(unsupported operand type or cross-type/cross-scale comparison)'. An internal "
            "fast path declining is surfacing to the user as a query failure. DuckDB "
            "answers. The same shape over `CAST(id AS DECIMAL(12,3))` from $planets runs."
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
        id="reference-overflow-on-rows-never-output",
        kind=SIGNATURE,
        outcome="reference_error",
        signatures=("Overflow in addition of INT64", "Overflow in subtraction of INT64", "Overflow in multiplication of INT64"),
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
            "Opteryx answers 0 rows. LIMITATION: this would also absorb Opteryx silently "
            "wrapping a value DuckDB overflowed on; Opteryx's ruled behaviour is to raise "
            "on INT64 overflow, which 'overflow-on-rows-never-output' shows it does."
        ),
    ),
    Gap(
        id="decimal-compare-to-nan-constant-raises",
        kind=SIGNATURE,
        outcome="opteryx_error",
        signatures=("cannot convert NaN to integer",),
        query=lambda: Query(
            tables=(_one_row((Ty.DEC, decimal.Decimal("1"))),),
            items=(Col("t0", "c0", Ty.DEC),),
            where=Binary(
                "<=",
                Col("t0", "c0", Ty.DEC),
                Binary("*", Lit(Ty.FLOAT, -0.0), Lit(Ty.FLOAT, float("inf")), Ty.FLOAT),
                Ty.BOOL,
            ),
        ),
        detail=(
            "A DECIMAL column compared with a FLOAT64 constant that folds to NaN "
            "(`-0.0 * Infinity`) raises a bare ValueError from the decimal-literal "
            "rescale; DuckDB answers. Same root as 'decimal-compare-to-infinite-constant-"
            "crashes'."
        ),
    ),
    # ─────────────────────────────────────────────────────────────────────────
    # CRASH — raised outside the engine's rejection types
    # ─────────────────────────────────────────────────────────────────────────
    Gap(
        id="decimal-compare-to-infinite-constant-crashes",
        kind=CRASH,
        outcome="InvalidOperation",
        query=lambda: Query(
            tables=(_one_row((Ty.DEC, decimal.Decimal("1"))),),
            items=(Col("t0", "c0", Ty.DEC),),
            where=Binary("=", Lit(Ty.FLOAT, float("-inf")), Col("t0", "c0", Ty.DEC), Ty.BOOL),
        ),
        detail=(
            "CRASH. A DECIMAL compared with a FLOAT64 constant the decimal rescale cannot "
            "represent (±Infinity in the repro) escapes as a raw "
            "`decimal.InvalidOperation` from logical_type.rescale_decimal_literal "
            "(compiler._rewrite_decimal_compares) — not an Opteryx error at all."
        ),
    ),
    Gap(
        id="filter-implied-group-key-literal-has-wrong-native-type",
        kind=CRASH,
        outcome="TypeError",
        signatures=("holds a int", "holds a float", "holds a Decimal"),
        query=lambda: Query(
            tables=(Table("t0", (Column("c0", Ty.STR), Column("c1", Ty.DEC)), ()),),
            items=(Col("t0", "c1", Ty.DEC), Col("t0", "c0", Ty.STR)),
            where=Binary("=", Col("t0", "c1", Ty.DEC), Lit(Ty.INT, 3), Ty.BOOL),
            group_by=(Col("t0", "c1", Ty.DEC), Col("t0", "c0", Ty.STR)),
        ),
        detail=(
            "CRASH. `WHERE key = <literal> GROUP BY key, other` where the literal's type "
            "differs from the key's (DECIMAL key = 3, DECIMAL key = 3.0, FLOAT64 key = 1, "
            "INT64 key = a DECIMAL expression) raises a "
            "raw TypeError ('Literal #N of type DECIMAL(12, 3) holds a int (3)') from "
            "FilterImpliedGroupKeyReductionStrategy: it replaces the group key with the "
            "filter's literal, re-typed as the key's type but still holding the original "
            "Python value."
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

