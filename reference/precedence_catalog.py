"""Operator precedence: how tightly each operator binds when SQL is written without parentheses.

WHAT THIS IS
------------
The published statement of Opteryx's operator precedence. It is attached, as a
`precedence` object, to every operator entry in `operators.json`, `unary_ops.json`
and (for the operator-shaped expression forms) `expressions.json`, and the docs site
renders its precedence table from those fields.

It is HAND-WRITTEN, deliberately. The binding powers the parser actually uses live
in Rust - the Opteryx dialect's own override (`OpteryxDialect::get_next_precedence`
in src/opteryx_dialect.rs) and, for everything else, the sqlparser default table the
dialect falls back to (`Dialect::prec_value`). Deriving this file from that code
would publish whatever the parser does, including a regression. Stating it by hand
makes it a CLAIM that can be checked: the single-table SQL fuzzer reads its
precedence table from these exported fields and renders predicates with only the
parentheses this table says are needed (`parenthesisation_is_neutral` in
tests/fuzzing/single_table_oracles.py). If the parser binds any operator
differently from what is written here, that oracle fails.

HOW TO READ IT
--------------
Tiers are listed tightest first; `level` 1 binds tightest. Operators in one tier
bind equally and group left to right: `a - b + c` is `(a - b) + c`. A prefix
operator's operand extends over everything in a tighter tier, so `NOT a = b` is
`NOT (a = b)` and `-a * b` is `(-a) * b`.

Where this departs from standard SQL / Postgres / MySQL the tier says so in its
`note`. Each one is what the parser does today, and the `||`, `^ << >>` and LIKE
departures were ruled to be documented as the intended behaviour (architect,
2026-09-22). Changing any of them is a parser change, and the fuzzer will fail
until this file and the parser agree. (XOR was the fourth departure - sqlparser
binds it above `&` and the comparisons - and was ruled a parser DEFECT the same
day; the dialect now places it between AND and OR, where this file lists it.)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

OPERATORS = "operators"
UNARY_OPS = "unary_ops"
EXPRESSIONS = "expressions"

INFIX = "infix"
PREFIX = "prefix"
POSTFIX = "postfix"


@dataclass(frozen=True)
class Member:
    """One catalog entry and the SQL spellings of it this tier covers."""

    catalog: str
    key: str
    spellings: Tuple[str, ...]
    position: str = INFIX


@dataclass(frozen=True)
class Tier:
    members: Tuple[Member, ...]
    note: Optional[str] = None


def _infix(key: str, *spellings: str) -> Member:
    return Member(OPERATORS, key, spellings)


_IS_FORMS = (
    ("IsNull", "IS NULL"),
    ("IsNotNull", "IS NOT NULL"),
    ("IsTrue", "IS TRUE"),
    ("IsNotTrue", "IS NOT TRUE"),
    ("IsFalse", "IS FALSE"),
    ("IsNotFalse", "IS NOT FALSE"),
    ("IsJsonValue", "IS JSON"),
    ("IsNotJsonValue", "IS NOT JSON"),
    ("IsJsonScalar", "IS JSON SCALAR"),
    ("IsNotJsonScalar", "IS NOT JSON SCALAR"),
    ("IsJsonArray", "IS JSON ARRAY"),
    ("IsNotJsonArray", "IS NOT JSON ARRAY"),
    ("IsJsonObject", "IS JSON OBJECT"),
    ("IsNotJsonObject", "IS NOT JSON OBJECT"),
)

#: Tightest first. Source for every tier: sqlparser 0.63 `Dialect::prec_value` and
#: `get_next_precedence_default`, as overridden by `OpteryxDialect::get_next_precedence`.
PRECEDENCE_TIERS: Tuple[Tier, ...] = (
    Tier(
        (
            _infix("Arrow", "->"),
            _infix("LongArrow", "->>"),
            _infix("AtArrow", "@>"),
            _infix("ArrayContainsAll", "@>>"),
            _infix("AtQuestion", "@?"),
        ),
        note=(
            "Opteryx's own choice, above `::` and `[]`: `payload->>'id'::INTEGER` casts "
            "the extracted value, and `a->'b'[1]` indexes the extracted value. Postgres "
            "binds `::` tighter and casts the key. A cast on a containment operand "
            "regroups too: `a @> ['x']::ARRAY<VARCHAR>` is `(a @> ['x'])::ARRAY<VARCHAR>`."
        ),
    ),
    Tier(
        (
            Member(EXPRESSIONS, "CAST", ("::",), POSTFIX),
            Member(OPERATORS, "MapAccess", ("[]",), POSTFIX),
        )
    ),
    Tier(
        (
            Member(UNARY_OPS, "UnaryMinus", ("-",), PREFIX),
            Member(UNARY_OPS, "UnaryPlus", ("+",), PREFIX),
        )
    ),
    Tier(
        (
            _infix("Multiply", "*"),
            _infix("Divide", "/"),
            _infix("Modulo", "%"),
            _infix("MyIntegerDivide", "DIV"),
            _infix("StringConcat", "||"),
        ),
        note=(
            "`||` binds like `*`. Postgres puts `||` below `+` and `-`; the two only "
            "meet in an expression that mixes strings and numbers."
        ),
    ),
    Tier((_infix("Plus", "+"), _infix("Minus", "-"))),
    Tier((_infix("BitwiseAnd", "&"),)),
    Tier(
        (
            _infix("BitwiseXor", "^"),
            _infix("ShiftLeft", "<<"),
            _infix("ShiftRight", ">>"),
            _infix("IPContainedBy", "<<="),
            _infix("IPContains", ">>="),
        ),
        note=(
            "`^`, `<<` and `>>` share one tier, between `&` and `|`. MySQL binds `^` "
            "above `*` and the shifts above `&`, so `a & b << c` is `(a & b) << c` here "
            "and `a & (b << c)` in MySQL. The IPv4 containment operators `<<=` and `>>=` "
            "begin with the shift tokens and bind at this tier, not as comparisons."
        ),
    ),
    Tier((_infix("BitwiseOr", "|"),)),
    Tier(
        (
            _infix("Eq", "="),
            _infix("NotEq", "!=", "<>"),
            _infix("Lt", "<"),
            _infix("LtEq", "<="),
            _infix("Gt", ">"),
            _infix("GtEq", ">="),
            _infix("InList", "IN"),
            _infix("NotInList", "NOT IN"),
            Member(EXPRESSIONS, "BETWEEN", ("BETWEEN", "NOT BETWEEN")),
        )
    ),
    Tier(
        (
            _infix("Like", "LIKE"),
            _infix("NotLike", "NOT LIKE"),
            _infix("ILike", "ILIKE"),
            _infix("NotILike", "NOT ILIKE"),
            _infix("RLike", "RLIKE"),
            _infix("NotRLike", "NOT RLIKE"),
            Member(EXPRESSIONS, "SIMILAR_TO", ("SIMILAR TO", "NOT SIMILAR TO")),
        ),
        note=(
            "Pattern matching binds LOOSER than comparison. Postgres has it tighter, so "
            "`a = b LIKE c` is `(a = b) LIKE c` here and `a = (b LIKE c)` there."
        ),
    ),
    Tier(
        tuple(Member(UNARY_OPS, key, (spelling,), POSTFIX) for key, spelling in _IS_FORMS)
        + (Member(EXPRESSIONS, "IS_DISTINCT_FROM", ("IS DISTINCT FROM", "IS NOT DISTINCT FROM")),)
    ),
    Tier((Member(UNARY_OPS, "Not", ("NOT",), PREFIX),)),
    Tier((_infix("And", "AND"),)),
    # The Opteryx dialect places XOR here itself (OpteryxDialect::get_next_precedence);
    # sqlparser's own table put it above `&` and the comparisons.
    Tier((_infix("Xor", "XOR"),)),
    Tier((_infix("Or", "OR"),)),
)


def _index() -> Dict[Tuple[str, str], Dict[str, Any]]:
    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for level, tier in enumerate(PRECEDENCE_TIERS, start=1):
        for member in tier.members:
            where = (member.catalog, member.key)
            if where in index:
                raise ValueError(f"{member.catalog}/{member.key} is placed in two precedence tiers")
            entry: Dict[str, Any] = {
                "level": level,
                "levels": len(PRECEDENCE_TIERS),
                "position": member.position,
                "spellings": list(member.spellings),
            }
            if tier.note:
                entry["note"] = tier.note
            index[where] = entry
    return index


_INDEX = _index()


def precedence_for(catalog: str, key: str) -> Optional[Dict[str, Any]]:
    """The `precedence` object for one catalog entry, or None if it is not an operator."""
    found = _INDEX.get((catalog, key))
    return None if found is None else dict(found)


def check_precedence_coverage(catalog: str, keys, required: bool) -> None:
    """Fail loudly on a catalog/precedence mismatch.

    Every tier member must name an entry that exists. With `required`, every entry in
    the catalog must also have a tier - true of the operator and unary catalogs, where
    every entry is an operator; the expression catalog holds literals, CASE, EXISTS and
    the like, which have no binding power, so only its listed members are checked.
    """
    keys = set(keys)
    placed = {key for (where, key) in _INDEX if where == catalog}
    stale = sorted(placed - keys)
    if stale:
        raise ValueError(
            f"precedence_catalog places {catalog} entries that do not exist: {', '.join(stale)}"
        )
    if required:
        missing = sorted(keys - placed)
        if missing:
            raise ValueError(
                f"{catalog} entries with no precedence tier: {', '.join(missing)}. Add each "
                f"to PRECEDENCE_TIERS in reference/precedence_catalog.py."
            )
