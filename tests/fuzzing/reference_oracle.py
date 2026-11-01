"""
The reference oracle: run one generated query on Opteryx and on DuckDB, compare.

WHY THIS EXISTS
---------------
Every other oracle in this directory compares the engine with itself, so an engine
that is CONSISTENTLY wrong — `0.0 = 0` false everywhere, an overflow that wraps on
every plan — passes all of them. DuckDB is an independent answer.

THE ONE PLACE IN THIS DIRECTORY THAT CATCHES
--------------------------------------------
`harness.py` says nothing catches, and for the metamorphic oracles that is right.
Here an error IS an outcome that must be compared — "Opteryx raised, DuckDB
answered" is the silent-wrong-answer class this oracle exists for. The catch is
deliberately narrow: the exception types each engine raises for a query it
rejects. Anything else (an AssertionError, a crash surfacing as something
unexpected) propagates and fails the case.

ValueError, ZeroDivisionError, RuntimeError and IncompatibleTypesError are on the
Opteryx list because they reach the caller outside the `opteryx.exceptions.Error`
hierarchy today: integer overflow and modulo-by-zero surface as bare ValueError
when constant-folded, and as RuntimeError ("[1]: ExprFilterOperator: ...") when a
native operator fails mid-execution; `IncompatibleTypesError` derives from
`Exception`, not `Error`.

OUTCOMES
--------
  both answered, same rows        -> agree
  both answered, different rows   -> WRONG ANSWER, never absorbed by anything
  Opteryx raised, DuckDB answered -> finding, unless a registered refusal
  Opteryx answered, DuckDB raised -> finding, unless a registered divergence
  both raised                     -> agree (messages are not compared)
"""

from __future__ import annotations

import datetime
import decimal
import math
import os
import re
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import duckdb

import opteryx
from opteryx.exceptions import Error as OpteryxError
from opteryx.exceptions import IncompatibleTypesError
from tests.fuzzing.reference_grammar import DUCKDB
from tests.fuzzing.reference_grammar import OPTERYX
from tests.fuzzing.reference_grammar import Query
from tests.fuzzing.reference_grammar import describe_data
from tests.fuzzing.reference_grammar import query_reductions
from tests.fuzzing.reference_grammar import write_parquet

OPTERYX_REJECTIONS = (OpteryxError, IncompatibleTypesError, ValueError, ZeroDivisionError, RuntimeError)
DUCKDB_REJECTIONS = (duckdb.Error,)

# Significant digits for float comparison. Plain projections are compared at 15
# (a double's round-trip precision is 17, so 15 absorbs only last-bit formatting);
# aggregate outputs at 10, because a parallel SUM/AVG legitimately accumulates in
# a different order — see single_table_oracles._render for the measurement.
EXACT_DIGITS = 15
AGGREGATE_DIGITS = 10


@dataclass(frozen=True)
class Outcome:
    rows: Optional[List[Tuple]]
    error: Optional[BaseException]

    def describe(self) -> str:
        if self.error is not None:
            return f"ERROR {type(self.error).__name__}: {str(self.error).splitlines()[0][:300]}"
        return f"{len(self.rows)} rows"


@dataclass(frozen=True)
class Verdict:
    kind: str  # "agree" / "wrong_answer" / "opteryx_error" / "reference_error"
    opteryx: Outcome
    reference: Outcome
    detail: str = ""

    @property
    def agrees(self) -> bool:
        return self.kind == "agree"


def run_opteryx(sql: str) -> Outcome:
    try:
        rows: List[Tuple] = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            for i in range(len(morsel)):
                rows.append(tuple(morsel[i]))
        return Outcome(rows, None)
    except OPTERYX_REJECTIONS as error:
        return Outcome(None, error)


def run_duckdb(sql: str) -> Outcome:
    connection = duckdb.connect()
    try:
        return Outcome(connection.sql(sql).fetchall(), None)
    except DUCKDB_REJECTIONS as error:
        return Outcome(None, error)
    finally:
        connection.close()


def canonical(value, digits: int) -> str:
    """One cell, rendered so that equal SQL values render equally across engines.

    Numbers compare by value, not by Python type: Opteryx may answer a DECIMAL
    where DuckDB answers a DOUBLE for the same expression. Integers render exactly;
    any non-integral or float-typed number renders at `digits` significant digits.
    -0.0 renders as 0 (SQL equality). Timestamps compare as UTC wall time — Opteryx
    returns them tz-aware in UTC, DuckDB naive.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, decimal.Decimal):
        if value == value.to_integral_value() and abs(value) < 10**15:
            return str(int(value))
        return canonical(float(value), digits)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0:
            return "0"
        if value == int(value) and abs(value) < 10**15:
            return str(int(value))
        return f"{value:.{digits}g}"
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode()
    return repr(value)


def canonical_rows(rows: Sequence[Tuple], aggregate_columns: Sequence[bool]) -> List[str]:
    return [
        "(" + ", ".join(canonical(v, AGGREGATE_DIGITS if agg else EXACT_DIGITS) for v, agg in zip(row, aggregate_columns)) + ")"
        for row in rows
    ]


def compare(query: Query) -> Tuple[Verdict, str, str]:
    """Execute `query` on both engines over freshly written copies of its tables."""
    with tempfile.TemporaryDirectory(prefix="opteryx_reference_") as directory:
        paths = []
        for table in query.tables:
            path = os.path.join(directory, f"{table.alias}.parquet")
            write_parquet(table, path)
            paths.append(path)
        opteryx_sql = query.render(OPTERYX, paths)
        duckdb_sql = query.render(DUCKDB, paths)
        verdict = judge(run_opteryx(opteryx_sql), run_duckdb(duckdb_sql), query)
    return verdict, opteryx_sql, duckdb_sql


def judge(ours: Outcome, theirs: Outcome, query: Query) -> Verdict:
    if ours.error is not None and theirs.error is not None:
        return Verdict("agree", ours, theirs)
    if ours.error is not None:
        return Verdict("opteryx_error", ours, theirs)
    if theirs.error is not None:
        return Verdict("reference_error", ours, theirs)

    aggregate_columns = query.aggregate_columns
    width = len(query.items)
    for row in ours.rows:
        if len(row) != width:
            return Verdict("wrong_answer", ours, theirs, f"Opteryx returned a row of width {len(row)}, expected {width}")
    left = canonical_rows(ours.rows, aggregate_columns)
    right = canonical_rows(theirs.rows, aggregate_columns)
    ordered = query.order_by
    if not ordered:
        left, right = sorted(left), sorted(right)
    if left == right:
        return Verdict("agree", ours, theirs)
    return Verdict("wrong_answer", ours, theirs, _difference(left, right, ordered))


def _difference(left: List[str], right: List[str], ordered: bool) -> str:
    lines = [f"{'ordered' if ordered else 'multiset'}: Opteryx {len(left)} rows, DuckDB {len(right)} rows"]
    if ordered:
        for i, (a, b) in enumerate(zip(left, right)):
            if a != b:
                lines.append(f"  first difference at row {i}: Opteryx {a}  DuckDB {b}")
                break
    else:
        only_ours = list(left)
        only_theirs = []
        for row in right:
            if row in only_ours:
                only_ours.remove(row)
            else:
                only_theirs.append(row)
        lines.append(f"  only in Opteryx: {only_ours[:5]}")
        lines.append(f"  only in DuckDB:  {only_theirs[:5]}")
    lines.append(f"  Opteryx: {left[:8]}")
    lines.append(f"  DuckDB:  {right[:8]}")
    return "\n".join(lines)


def _error_identity(error: BaseException) -> Tuple[type, str]:
    return type(error), re.sub(r"\d+", "N", str(error).splitlines()[0])[:60]


def same_failure(a: Verdict, b: Verdict) -> bool:
    """Whether a shrink step preserved the failure: same outcome class, and for an
    error, the same exception type and message stem on the side that raised —
    without the stem a shrink drifts from one refusal to an unrelated one."""
    if a.kind != b.kind:
        return False
    if a.kind == "opteryx_error":
        return _error_identity(a.opteryx.error) == _error_identity(b.opteryx.error)
    if a.kind == "reference_error":
        return _error_identity(a.reference.error) == _error_identity(b.reference.error)
    return True


SHRINK_BUDGET = 300


def shrink(query: Query, verdict: Verdict) -> Tuple[Query, Verdict, int]:
    """Greedy reduction: take the first candidate that keeps the same failure,
    restart from it, stop when no candidate does or the budget is spent.

    A candidate that crashes the engine is rejected, not raised: it is a
    DIFFERENT failure, and the one being shrunk is already in hand."""
    executions = 0
    improved = True
    while improved and executions < SHRINK_BUDGET:
        improved = False
        for candidate in query_reductions(query):
            executions += 1
            try:
                candidate_verdict, _, _ = compare(candidate)
            except Exception:  # a different failure; see docstring
                candidate_verdict = None
            if candidate_verdict is not None and same_failure(verdict, candidate_verdict):
                query, verdict, improved = candidate, candidate_verdict, True
                break
            if executions >= SHRINK_BUDGET:
                break
    return query, verdict, executions


def report(query: Query, verdict: Verdict) -> str:
    placeholder = [f"<{t.alias}.parquet>" for t in query.tables]
    lines = [
        f"verdict: {verdict.kind}",
        f"opteryx:   {verdict.opteryx.describe()}",
        f"reference: {verdict.reference.describe()}",
    ]
    if verdict.detail:
        lines.append(verdict.detail)
    lines += [
        "opteryx sql:",
        "  " + query.render(OPTERYX, placeholder),
        "duckdb sql:",
        "  " + query.render(DUCKDB, placeholder),
        describe_data(query),
    ]
    return "\n".join(lines)
