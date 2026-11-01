"""
Dual-dialect SQL generator for the reference fuzzer.

WHY A SEPARATE GRAMMAR
----------------------
The other fuzzers compare Opteryx with itself. This one compares it with DuckDB,
so every generated query has to be written twice — once per dialect — and the
two texts must mean the same thing. The catalog-driven `single_table_grammar.py`
emits Opteryx's whole function surface; translating that to DuckDB would need a
hand-kept function map, and that map would itself become the thing under test.
So this grammar is deliberately small (Jepsen's equivalent was a few basic types,
literals, arithmetic, comparisons, boolean logic and LIMIT/OFFSET/ORDER BY) and
every node renders BOTH dialects. The texts differ only where a known dialect
difference is normalised on purpose:

* Integer literals are `CAST(n AS BIGINT)` in DuckDB, which otherwise types a bare
  `5` as INT32 and overflows at a different point.
* ORDER BY carries explicit NULLS FIRST/LAST in both texts — the engine's ruled
  default is NULL-lowest; DuckDB's default is NULLS LAST.
* `/` and `%` raise on a zero divisor in the DuckDB text (via `error()`), giving
  the PostgreSQL semantics a division by zero is held to. DuckDB alone would
  answer NULL.

Values are the reference fuzzer's own random tables, written to parquet once per
case and read by both engines from the same file, so column types agree by
construction.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import random
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, List, Optional, Sequence, Tuple


class Ty(Enum):
    INT = "INT"
    FLOAT = "FLOAT"
    DEC = "DEC"
    STR = "STR"
    BOOL = "BOOL"
    DATE = "DATE"
    TS = "TS"


NUMERIC = (Ty.INT, Ty.FLOAT, Ty.DEC)
ORDERED = (Ty.INT, Ty.FLOAT, Ty.DEC, Ty.STR, Ty.DATE, Ty.TS)

DEC_PRECISION, DEC_SCALE = 12, 3

# (opteryx, duckdb) type names for CAST.
_CAST_NAMES = {
    Ty.INT: ("INT64", "BIGINT"),
    Ty.FLOAT: ("FLOAT64", "DOUBLE"),
    Ty.DEC: (f"DECIMAL({DEC_PRECISION},{DEC_SCALE})", f"DECIMAL({DEC_PRECISION},{DEC_SCALE})"),
    Ty.STR: ("VARCHAR", "VARCHAR"),
    Ty.BOOL: ("BOOLEAN", "BOOLEAN"),
    Ty.DATE: ("DATE", "DATE"),
    Ty.TS: ("TIMESTAMP", "TIMESTAMP"),
}

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)

# Small domains so equality predicates and join keys actually match, plus the
# edge values that broke ParticleDB. ±Infinity is in (arithmetic over it reaches
# NaN mid-expression, which both engines order as the greatest value); NaN is not
# stored, because the join register's open NaN-join-key defect would then fire on
# every FLOAT join key.
VALUE_POOLS = {
    Ty.INT: [0, 1, -1, 2, 3, 5, 7, 10, -10, 100, 2**31, 2**53 + 1, INT64_MAX, INT64_MIN],
    # Dyadic and small, so a float SUM of column values is exact in every
    # accumulation order: a parallel SUM's order is not the engine's to fix, and
    # cancellation (1e15 + 0.1 - 1e15) makes order show in every digit.
    Ty.FLOAT: [0.0, -0.0, 0.5, 1.0, -1.5, 2.5, 3.0, 0.125, 64.0, -64.0, float("inf"), float("-inf")],
    Ty.DEC: [decimal.Decimal(v) for v in ("0", "1.250", "-2.500", "0.001", "3", "999999999.999", "-0.500")],
    Ty.STR: ["", "a", "A", "b", "B", "_a", "a b", "abc", "Abc", "é", "z"],
    Ty.BOOL: [True, False],
    Ty.DATE: [
        datetime.date(1970, 1, 1),
        datetime.date(1969, 12, 31),
        datetime.date(2000, 2, 29),
        datetime.date(2020, 1, 1),
        datetime.date(1900, 1, 1),
    ],
    Ty.TS: [
        datetime.datetime(1970, 1, 1),
        datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime.datetime(2000, 2, 29, 12, 0, 0, 1),
        datetime.datetime(2020, 1, 1, 1, 2, 3, 456789),
    ],
}

# Values the generator will never put in a literal (they only arrive via data):
# INT64 extremes as literals double every overflow path for no new information.
_LITERAL_EXCLUDED = {Ty.INT: {INT64_MAX, INT64_MIN}}


# ─────────────────────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Column:
    name: str
    ty: Ty


@dataclass(frozen=True)
class Table:
    alias: str
    columns: Tuple[Column, ...]
    rows: Tuple[Tuple, ...]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


def random_table(rng: random.Random, alias: str) -> Table:
    count = rng.randint(1, 4)
    columns = tuple(Column(f"c{i}", rng.choice(list(Ty))) for i in range(count))
    row_count = rng.choice([0, 1, 2, 3, 5, 8, 13, 21, 40])
    null_rate = rng.choice([0.0, 0.1, 0.3])
    rows = tuple(
        tuple(None if rng.random() < null_rate else rng.choice(VALUE_POOLS[c.ty]) for c in columns)
        for _ in range(row_count)
    )
    return Table(alias, columns, rows)


def write_parquet(table: Table, path: str) -> None:
    import pyarrow as pa  # test-only writer, as in dev/generate_fuzz_testdata.py
    import pyarrow.parquet as pq

    arrow_types = {
        Ty.INT: pa.int64(),
        Ty.FLOAT: pa.float64(),
        Ty.DEC: pa.decimal128(DEC_PRECISION, DEC_SCALE),
        Ty.STR: pa.string(),
        Ty.BOOL: pa.bool_(),
        Ty.DATE: pa.date32(),
        Ty.TS: pa.timestamp("us"),
    }
    arrays = {
        c.name: pa.array([row[i] for row in table.rows], arrow_types[c.ty]) for i, c in enumerate(table.columns)
    }
    schema = pa.schema([(c.name, arrow_types[c.ty]) for c in table.columns])
    pq.write_table(pa.table(arrays, schema=schema), path)


# ─────────────────────────────────────────────────────────────────────────────
# Expressions
# ─────────────────────────────────────────────────────────────────────────────

OPTERYX, DUCKDB = 0, 1


class Node:
    ty: Ty

    def render(self, dialect: int) -> str:
        raise NotImplementedError

    def children(self) -> Tuple["Node", ...]:
        return ()

    def with_children(self, children: Sequence["Node"]) -> "Node":
        return self


def _sql_literal(ty: Ty, value, dialect: int) -> str:
    opteryx_type, duckdb_type = _CAST_NAMES[ty]
    type_name = duckdb_type if dialect == DUCKDB else opteryx_type
    if value is None:
        return f"CAST(NULL AS {type_name})"
    if ty == Ty.INT:
        return f"CAST({value} AS BIGINT)" if dialect == DUCKDB else str(value)
    if ty == Ty.BOOL:
        return "TRUE" if value else "FALSE"
    if ty == Ty.FLOAT:
        text = {float("inf"): "Infinity", float("-inf"): "-Infinity"}.get(value, repr(value))
        return f"CAST('{text}' AS {type_name})"
    if ty == Ty.STR:
        return "'" + value.replace("'", "''") + "'"
    if ty == Ty.TS:
        return f"CAST('{value.isoformat(sep=' ')}' AS {type_name})"
    return f"CAST('{value}' AS {type_name})"


@dataclass(frozen=True)
class Lit(Node):
    ty: Ty
    value: object

    def render(self, dialect: int) -> str:
        return _sql_literal(self.ty, self.value, dialect)


@dataclass(frozen=True)
class Col(Node):
    table: str
    name: str
    ty: Ty

    def render(self, dialect: int) -> str:
        return f"{self.table}.{self.name}"


@dataclass(frozen=True)
class Binary(Node):
    op: str
    left: Node
    right: Node
    ty: Ty

    def render(self, dialect: int) -> str:
        left, right = self.left.render(dialect), self.right.render(dialect)
        if dialect == DUCKDB and self.op in ("/", "%"):
            return f"(CASE WHEN ({right}) = 0 THEN error('division by zero') ELSE ({left}) {self.op} ({right}) END)"
        return f"({left}) {self.op} ({right})"

    def children(self):
        return (self.left, self.right)

    def with_children(self, children):
        return dataclasses.replace(self, left=children[0], right=children[1])


@dataclass(frozen=True)
class Not(Node):
    operand: Node
    ty: Ty = Ty.BOOL

    def render(self, dialect: int) -> str:
        return f"NOT ({self.operand.render(dialect)})"

    def children(self):
        return (self.operand,)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0])


@dataclass(frozen=True)
class IsNull(Node):
    operand: Node
    negated: bool
    ty: Ty = Ty.BOOL

    def render(self, dialect: int) -> str:
        return f"({self.operand.render(dialect)}) IS {'NOT ' if self.negated else ''}NULL"

    def children(self):
        return (self.operand,)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0])


@dataclass(frozen=True)
class Between(Node):
    operand: Node
    low: Node
    high: Node
    ty: Ty = Ty.BOOL

    def render(self, dialect: int) -> str:
        return f"({self.operand.render(dialect)}) BETWEEN ({self.low.render(dialect)}) AND ({self.high.render(dialect)})"

    def children(self):
        return (self.operand, self.low, self.high)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0], low=children[1], high=children[2])


@dataclass(frozen=True)
class InList(Node):
    operand: Node
    members: Tuple[Node, ...]
    negated: bool
    ty: Ty = Ty.BOOL

    def render(self, dialect: int) -> str:
        members = ", ".join(m.render(dialect) for m in self.members)
        return f"({self.operand.render(dialect)}) {'NOT ' if self.negated else ''}IN ({members})"

    def children(self):
        return (self.operand, *self.members)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0], members=tuple(children[1:]))


@dataclass(frozen=True)
class Case(Node):
    condition: Node
    then: Node
    otherwise: Node
    ty: Ty

    def render(self, dialect: int) -> str:
        return (
            f"CASE WHEN {self.condition.render(dialect)} THEN {self.then.render(dialect)} "
            f"ELSE {self.otherwise.render(dialect)} END"
        )

    def children(self):
        return (self.condition, self.then, self.otherwise)

    def with_children(self, children):
        return dataclasses.replace(self, condition=children[0], then=children[1], otherwise=children[2])


@dataclass(frozen=True)
class Coalesce(Node):
    first: Node
    second: Node
    ty: Ty

    def render(self, dialect: int) -> str:
        return f"COALESCE({self.first.render(dialect)}, {self.second.render(dialect)})"

    def children(self):
        return (self.first, self.second)

    def with_children(self, children):
        return dataclasses.replace(self, first=children[0], second=children[1])


@dataclass(frozen=True)
class Cast(Node):
    operand: Node
    ty: Ty

    def render(self, dialect: int) -> str:
        return f"CAST({self.operand.render(dialect)} AS {_CAST_NAMES[self.ty][dialect]})"

    def children(self):
        return (self.operand,)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0])


@dataclass(frozen=True)
class Agg(Node):
    func: str
    operand: Optional[Node]  # None => COUNT(*)
    ty: Ty

    def render(self, dialect: int) -> str:
        return f"{self.func}({'*' if self.operand is None else self.operand.render(dialect)})"

    def children(self):
        return () if self.operand is None else (self.operand,)

    def with_children(self, children):
        return dataclasses.replace(self, operand=children[0]) if children else self


# Casts the generator emits: target type -> source types. Withheld, each with a
# reference_known_gaps entry: FLOAT/DEC -> INT ("numeric-to-integer-cast-truncates")
# and BOOL -> STR ("cast-boolean-to-varchar-capitalised").
_CASTS = {
    Ty.INT: (Ty.BOOL,),
    Ty.FLOAT: (Ty.INT, Ty.DEC),
    Ty.DEC: (Ty.INT, Ty.FLOAT),
    Ty.STR: (Ty.INT, Ty.DATE),
    Ty.BOOL: (Ty.INT,),
    Ty.DATE: (Ty.TS,),
    Ty.TS: (Ty.DATE,),
}


class ExprGen:
    def __init__(self, rng: random.Random, scope: Sequence[Col]):
        self.rng = rng
        self.scope = list(scope)

    def literal(self, ty: Ty) -> Lit:
        # No NULL literals: a typed NULL (`CAST(NULL AS DECIMAL(12,3))`) loses its
        # type and is refused in many positions — reference_known_gaps
        # "typed-null-literal-refused". NULLs still arrive through the data.
        excluded = _LITERAL_EXCLUDED.get(ty, set())
        return Lit(ty, self.rng.choice([v for v in VALUE_POOLS[ty] if v not in excluded]))

    def columns(self, ty: Ty) -> List[Col]:
        return [c for c in self.scope if c.ty == ty]

    def leaf(self, ty: Ty) -> Node:
        columns = self.columns(ty)
        if columns and self.rng.random() < 0.7:
            return self.rng.choice(columns)
        return self.literal(ty)

    def expr(self, ty: Ty, depth: int) -> Node:
        if depth <= 0 or self.rng.random() < 0.3:
            return self.leaf(ty) if ty != Ty.BOOL else self.predicate(0)
        if ty == Ty.BOOL:
            return self.predicate(depth)
        choice = self.rng.random()
        if choice < 0.1:
            return Case(self.predicate(depth - 1), self.expr(ty, depth - 1), self.expr(ty, depth - 1), ty)
        if choice < 0.2:
            return Coalesce(self.expr(ty, depth - 1), self.expr(ty, depth - 1), ty)
        if choice < 0.3:
            source = self.rng.choice(_CASTS[ty])
            return Cast(self.expr(source, depth - 1), ty)
        if ty in NUMERIC:
            ops = ["+", "-", "*"]
            if ty == Ty.INT:
                ops.append("%")
            if ty == Ty.FLOAT:
                ops.append("/")
            op = self.rng.choice(ops)
            return Binary(op, self.expr(ty, depth - 1), self.expr(ty, depth - 1), ty)
        if ty == Ty.STR:
            return Binary("||", self.expr(Ty.STR, depth - 1), self.expr(Ty.STR, depth - 1), ty)
        return self.leaf(ty)

    def comparable_pair(self, depth: int) -> Tuple[Node, Node]:
        """Two operands a comparison accepts. Numeric types mix on purpose:
        INT = FLOAT, INT = DEC and DEC = FLOAT are where ParticleDB lost 0 = 0.0."""
        ty = self.rng.choice(ORDERED)
        other = self.rng.choice(NUMERIC) if ty in NUMERIC else ty
        return self.expr(ty, depth), self.expr(other, depth)

    def predicate(self, depth: int) -> Node:
        choice = self.rng.random()
        if depth > 0 and choice < 0.25:
            op = self.rng.choice(["AND", "OR"])
            return Binary(op, self.predicate(depth - 1), self.predicate(depth - 1), Ty.BOOL)
        if depth > 0 and choice < 0.33:
            return Not(self.predicate(depth - 1))
        if choice < 0.42:
            return IsNull(self.expr(self.rng.choice(list(Ty)), max(depth - 1, 0)), self.rng.random() < 0.5)
        if choice < 0.5:
            ty = self.rng.choice(ORDERED)
            return Between(self.expr(ty, depth - 1), self.leaf(ty), self.leaf(ty))
        if choice < 0.58:
            ty = self.rng.choice(ORDERED)
            members = tuple(self.literal(ty) for _ in range(self.rng.randint(1, 3)))
            return InList(self.leaf(ty), members, self.rng.random() < 0.3)
        if choice < 0.65 and self.columns(Ty.BOOL):
            # `col = TRUE`, never the bare column: a bare (or negated) boolean
            # column in WHERE is refused — "where-bare-boolean-column-refused".
            return Binary("=", self.rng.choice(self.columns(Ty.BOOL)), Lit(Ty.BOOL, True), Ty.BOOL)
        left, right = self.comparable_pair(max(depth - 1, 0))
        return Binary(self.rng.choice(["=", "<>", "<", "<=", ">", ">="]), left, right, Ty.BOOL)

    def column_predicate(self, depth: int) -> Node:
        """A predicate that references at least one column.

        The engine refuses a WHERE that is a bare literal, and an all-literal
        predicate tells us nothing about rows anyway."""
        for _ in range(20):
            candidate = self.predicate(depth)
            if any(isinstance(n, Col) for n in walk(candidate)):
                return candidate
        column = self.rng.choice(self.scope)
        return IsNull(column, self.rng.random() < 0.5)


def walk(node: Node) -> Iterator[Node]:
    yield node
    for child in node.children():
        yield from walk(child)


# ─────────────────────────────────────────────────────────────────────────────
# Queries
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Join:
    kind: str  # "INNER" / "LEFT" / "FULL" / "CROSS"
    condition: Optional[Node]


@dataclass(frozen=True)
class Query:
    tables: Tuple[Table, ...]
    items: Tuple[Node, ...]
    join: Optional[Join] = None
    where: Optional[Node] = None
    group_by: Tuple[Node, ...] = ()
    distinct: bool = False
    order_by: bool = False  # ORDER BY every output column, so the order is total
    descending: Tuple[bool, ...] = ()
    limit: Optional[int] = None
    offset: Optional[int] = None

    @property
    def aggregate_columns(self) -> Tuple[bool, ...]:
        return tuple(isinstance(item, Agg) for item in self.items)

    def render(self, dialect: int, paths: Sequence[str]) -> str:
        select = ", ".join(f"{item.render(dialect)} AS o{i}" for i, item in enumerate(self.items))
        sql = f"SELECT {'DISTINCT ' if self.distinct else ''}{select}"
        if self.tables:
            reader = "READ_PARQUET" if dialect == OPTERYX else "read_parquet"
            sources = [f"{reader}('{path}') AS {t.alias}" for t, path in zip(self.tables, paths)]
            sql += f" FROM {sources[0]}"
            if self.join is not None:
                if self.join.kind == "CROSS":
                    sql += f" CROSS JOIN {sources[1]}"
                else:
                    sql += f" {self.join.kind} JOIN {sources[1]} ON {self.join.condition.render(dialect)}"
        if self.where is not None:
            sql += f" WHERE {self.where.render(dialect)}"
        if self.group_by:
            sql += " GROUP BY " + ", ".join(g.render(dialect) for g in self.group_by)
        if self.order_by:
            keys = []
            for i, desc in enumerate(self.descending):
                # The ruled NULL-lowest order, made explicit for DuckDB.
                keys.append(f"o{i} {'DESC NULLS LAST' if desc else 'ASC NULLS FIRST'}")
            sql += " ORDER BY " + ", ".join(keys)
        if self.limit is not None:
            sql += f" LIMIT {self.limit}"
        if self.offset is not None:
            sql += f" OFFSET {self.offset}"
        return sql

    def nodes(self) -> Iterator[Node]:
        for root in self.roots():
            yield from walk(root)

    def roots(self) -> List[Node]:
        roots = list(self.items) + list(self.group_by)
        if self.where is not None:
            roots.append(self.where)
        if self.join is not None and self.join.condition is not None:
            roots.append(self.join.condition)
        return roots


def _scope(tables: Sequence[Table]) -> List[Col]:
    return [Col(t.alias, c.name, c.ty) for t in tables for c in t.columns]


def _agg(gen: ExprGen, rng: random.Random) -> Agg:
    choice = rng.random()
    if choice < 0.15:
        return Agg("COUNT", None, Ty.INT)
    if choice < 0.3:
        return Agg("COUNT", gen.leaf(rng.choice(list(Ty))), Ty.INT)
    if choice < 0.6:
        ty = rng.choice(ORDERED)
        return Agg(rng.choice(["MIN", "MAX"]), gen.expr(ty, 1), ty)
    ty = rng.choice(NUMERIC)
    func = rng.choice(["SUM", "AVG"])
    # A float SUM/AVG takes a bare column or literal: over a computed operand
    # (`1e12 * c`, a cast from INT64) cancellation makes the result depend on the
    # accumulation order. INT and DECIMAL sums are exact, so they take expressions.
    operand = gen.leaf(ty) if ty == Ty.FLOAT else gen.expr(ty, 1)
    return Agg(func, operand, Ty.FLOAT if func == "AVG" and ty != Ty.DEC else ty)


def random_query(rng: random.Random) -> Query:
    shape = rng.random()
    if shape < 0.07:
        # No FROM: literal expressions only, the `SELECT 5 WHERE FALSE` class.
        gen = ExprGen(rng, [])
        items = tuple(gen.expr(rng.choice(list(Ty)), 2) for _ in range(rng.randint(1, 3)))
        return Query(tables=(), items=items)

    tables = [random_table(rng, "t0")]
    join = None
    if rng.random() < 0.35:
        tables.append(random_table(rng, "t1"))
        kind = rng.choice(["INNER", "LEFT", "FULL", "CROSS"])
        condition = None
        if kind != "CROSS":
            pairs = [
                (a, b)
                for a in tables[0].columns
                for b in tables[1].columns
                if a.ty == b.ty or (a.ty in NUMERIC and b.ty in NUMERIC)
            ]
            if not pairs:
                kind = "CROSS"
            else:
                a, b = rng.choice(pairs)
                condition = Binary("=", Col("t0", a.name, a.ty), Col("t1", b.name, b.ty), Ty.BOOL)
        join = Join(kind, condition)

    gen = ExprGen(rng, _scope(tables))
    where = gen.column_predicate(2) if rng.random() < 0.6 else None

    if shape < 0.4:
        group_by: Tuple[Node, ...] = ()
        if rng.random() < 0.6:
            group_by = tuple(rng.sample(gen.scope, rng.randint(1, min(2, len(gen.scope)))))
        aggs = tuple(_agg(gen, rng) for _ in range(rng.randint(1, 3)))
        items = tuple(group_by) + aggs
        query = Query(tuple(tables), items, join, where, group_by)
    else:
        items = tuple(gen.expr(rng.choice(list(Ty)), 2) for _ in range(rng.randint(1, 3)))
        query = Query(tuple(tables), items, join, where, distinct=rng.random() < 0.2)

    if rng.random() < 0.4:
        limit = rng.choice([None, 0, 1, 2, 5]) if rng.random() < 0.7 else None
        offset = rng.choice([None, 1, 3]) if rng.random() < 0.3 else None
        query = dataclasses.replace(
            query,
            order_by=True,
            descending=tuple(rng.random() < 0.4 for _ in query.items),
            limit=limit,
            offset=offset,
        )
    return query


# ─────────────────────────────────────────────────────────────────────────────
# Shrinking candidates
# ─────────────────────────────────────────────────────────────────────────────


def _replace_at(node: Node, path: Tuple[int, ...], replacement: Node) -> Node:
    if not path:
        return replacement
    children = list(node.children())
    children[path[0]] = _replace_at(children[path[0]], path[1:], replacement)
    return node.with_children(children)


def _paths(node: Node, prefix: Tuple[int, ...] = ()) -> Iterator[Tuple[Tuple[int, ...], Node]]:
    yield prefix, node
    for i, child in enumerate(node.children()):
        yield from _paths(child, prefix + (i,))


def _expression_reductions(node: Node) -> Iterator[Node]:
    """Smaller same-typed variants of `node`: a subtree replaced by one of its
    same-typed children, or by NULL / a simple literal of its type."""
    for path, sub in _paths(node):
        if isinstance(sub, (Lit, Col)) or isinstance(sub, Agg):
            continue
        for child in sub.children():
            if child.ty == sub.ty:
                yield _replace_at(node, path, child)
        simple = {Ty.INT: 1, Ty.FLOAT: 1.0, Ty.DEC: decimal.Decimal("1"), Ty.STR: "a", Ty.BOOL: True}
        if sub.ty in simple:
            yield _replace_at(node, path, Lit(sub.ty, simple[sub.ty]))


def query_reductions(query: Query) -> Iterator[Query]:
    """Candidate simplifications of `query`, most aggressive first."""
    if query.where is not None:
        yield dataclasses.replace(query, where=None)
    if query.offset is not None:
        yield dataclasses.replace(query, offset=None)
    if query.limit is not None:
        yield dataclasses.replace(query, limit=None)
    if query.order_by and query.limit is None and query.offset is None:
        yield dataclasses.replace(query, order_by=False, descending=())
    if query.distinct:
        yield dataclasses.replace(query, distinct=False)
    if query.join is not None and query.join.kind != "INNER" and query.join.condition is not None:
        yield dataclasses.replace(query, join=Join("INNER", query.join.condition))
    if len(query.items) > 1:
        for i in range(len(query.items)):
            if query.items[i] in query.group_by:
                continue
            items = query.items[:i] + query.items[i + 1 :]
            descending = query.descending[:i] + query.descending[i + 1 :] if query.order_by else ()
            yield dataclasses.replace(query, items=items, descending=descending)
    # Rows: halve, then drop single rows.
    for index, table in enumerate(query.tables):
        if len(table.rows) > 1:
            half = len(table.rows) // 2
            for rows in (table.rows[:half], table.rows[half:]):
                yield _with_table(query, index, dataclasses.replace(table, rows=rows))
        for r in range(len(table.rows)):
            rows = table.rows[:r] + table.rows[r + 1 :]
            yield _with_table(query, index, dataclasses.replace(table, rows=rows))
    # Expressions.
    for i, item in enumerate(query.items):
        if item in query.group_by:
            continue
        for reduced in _expression_reductions(item):
            yield dataclasses.replace(query, items=query.items[:i] + (reduced,) + query.items[i + 1 :])
    if query.where is not None:
        for reduced in _expression_reductions(query.where):
            yield dataclasses.replace(query, where=reduced)


def _with_table(query: Query, index: int, table: Table) -> Query:
    tables = query.tables[:index] + (table,) + query.tables[index + 1 :]
    return dataclasses.replace(query, tables=tables)


def describe_data(query: Query) -> str:
    """The tables as readable VALUES snippets, for a reproduction."""
    lines = []
    for table in query.tables:
        columns = ", ".join(f"{c.name} {_CAST_NAMES[c.ty][1]}" for c in table.columns)
        values = ", ".join(
            "(" + ", ".join(_sql_literal(c.ty, v, DUCKDB) for c, v in zip(table.columns, row)) + ")"
            for row in table.rows
        )
        lines.append(f"-- {table.alias} ({columns}): {len(table.rows)} rows")
        lines.append(f"--   VALUES {values or '(none)'}")
    return "\n".join(lines)
