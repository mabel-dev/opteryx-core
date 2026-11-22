# GQL → Logical Plan Subsystem

**Status:** PROPOSAL, 2026-11-22. Nothing built.

**Ruled by the architect, 2026-11-22:**

- The language is **GQL** (ISO/IEC 39075:2024). Not Cypher. Not SQL/PGQ's
  `GRAPH_TABLE` inside SQL.
- **We write our own parser, in Python.** It uses no sqlparser and no Rust.
  sqlparser has no PGQ grammar (issue apache/datafusion-sqlparser-rs#1572 has
  been open since 2024-11), and vendoring a fork's PGQ patch was rejected.
- **It is a separate entry point that hands a logical plan to the engine**, the
  way the OData service does (`session.execute_logical_plan`). The subsystem is
  GQL → `LogicalPlan`, and nothing more. Everything from relation resolution
  onwards is the existing engine.

Open decisions D1–D7 are listed under
[Open decisions](#open-decisions-for-the-architect). **D1 (what the lowering
emits) shapes the whole subsystem.**

## Scope

v1 covers **read queries over fixed-length patterns**. Every path is a fixed
sequence of vertices and edges, with no quantifiers. A fixed-length match is a
conjunctive query: inner equi-joins, a filter and a projection. So a GQL query
in this scope lowers entirely onto relational operators the engine already
has. There is no new operator and no execution change.

Graph data is **relational tables viewed as a graph**. A property graph
definition maps labels to existing relations, which can be datasets, views or
federated connectors. GQL's native, typed graphs (`CREATE GRAPH ... TYPED`)
are out of scope. Opteryx has no graph storage, and this proposal adds none.

## The example

```
USE exposure_graph
MATCH (s:Supplier)-[:BILLED_BY]->(i:Invoice)
WHERE i.due_date < CURRENT_DATE + DURATION 'P30D'
MATCH (s)-[:ULTIMATELY_OWNED_BY]->(o:Person)
RETURN s.name AS supplier,
       SUM(i.amount) AS exposure,
       COLLECT_LIST(DISTINCT o.name) AS owners
GROUP BY supplier
ORDER BY exposure DESC
LIMIT 10
```

It lowers to the query the engine would run for this SQL:

```sql
SELECT s.name AS supplier, SUM(i.amount) AS exposure, ARRAY_AGG(DISTINCT o.name) AS owners
FROM erp.suppliers AS s
JOIN erp.invoices       AS "$e1" ON "$e1".supplier_id = s.supplier_id
JOIN erp.invoices       AS i     ON "$e1".invoice_id  = i.invoice_id
JOIN register.ownership AS "$e2" ON "$e2".supplier_id = s.supplier_id
JOIN register.persons   AS o     ON "$e2".person_id   = o.person_id
WHERE i.due_date < CURRENT_DATE + INTERVAL '30' DAY
GROUP BY s.name
ORDER BY exposure DESC
LIMIT 10
```

(This query inflates `exposure` by each supplier's owner count, because the two
`MATCH` statements cross-multiply per supplier. That is the semantics of the
query as written. The lowering must reproduce it, not correct it.)

## Architecture

```
GQL text
  │  opteryx/planner/gql/lexer.py       tokens with source positions
  ▼
  │  opteryx/planner/gql/parser.py      recursive descent → GQL AST (dataclasses)
  ▼
  │  opteryx/planner/gql/graph.py       USE graph → PropertyGraphDefinition (catalog);
  │                                     labels, properties, variables checked
  ▼
  │  opteryx/planner/gql/lowering.py    GQL AST → logical-planner input (see D1)
  ▼
LogicalPlan ──► execute_logical_plan:  resolve → rewrite → bind → optimize → physical → execute
```

- **Planning only, so Python.** This follows CLAUDE.md §1: Python decides the
  work and native code does it. Once the plan is handed over, the subsystem
  has no part in execution.
- **Location: `opteryx/planner/gql/`.** The planner is Cython-compiled, so the
  new modules follow whatever the rest of the planner does in `setup.py`. The
  stale-build guard in `opteryx/planner/__init__.py` covers them automatically.
- **Entry point: a session method beside `execute_to_morsels`.** It parses,
  lowers, and calls `execute_logical_plan` with the session as `connection`, so
  that permissions, visibility filters and telemetry are the session's. Its
  exact surface is [D7](#d7-entry-point-surface).

## 1. Lexer and parser

The parser is hand-written recursive descent with one token of lookahead. The
grammar is small enough that a parser generator would cost more than it saves.

**v1 grammar (informal):**

```
query        := [USE graph_name] match_stmt+ return_stmt
match_stmt   := [OPTIONAL] MATCH path (',' path)* [WHERE expr]
path         := node (edge node)*
node         := '(' [var] [':' label] [WHERE expr] ')'
edge         := '-' '[' [var] [':' label] [WHERE expr] ']' '->'
              | '<-' '[' [var] [':' label] [WHERE expr] ']' '-'
return_stmt  := RETURN [DISTINCT] item (',' item)*
                [GROUP BY name (',' name)*]
                [ORDER BY expr [ASC|DESC] (',' ...)*]
                [OFFSET n] [LIMIT n]
item         := expr [AS name]
```

**v1 expressions:**
- literals: integer, decimal, string, `TRUE`/`FALSE`/`NULL`, `DATE '...'`,
  `TIMESTAMP '...'`, `DURATION '...'`;
- `$name` parameters;
- property reference `v.p`;
- arithmetic, comparison, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `||`, `CASE`,
  and parentheses;
- function calls through an explicit GQL → Opteryx function map;
- aggregates `COUNT(*)`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` and
  `COLLECT_LIST`, each with optional `DISTINCT`.

**Lexical rules:**
- keywords are case-insensitive;
- string literals are single-quoted;
- delimited identifiers follow 39075 (see [Spec checks](#spec-checks));
- every token carries its line and column. Errors are positioned and use the
  same message style as SQL errors: keywords in bold and a suggested fix.

**Every unrecognised or out-of-scope construct is refused by name.** Nothing is
skipped and nothing is approximated. The refusal list is
[Refused in v1](#refused-in-v1).

## 2. Graph resolution

`USE <graph>` names the graph. Without `USE`, the query is refused: v1 has no
session default graph.

`graph.py` fetches the definition (see [§4](#4-graph-definitions-catalog)) and
checks:
- every label exists in the graph. An unknown label gets the existing typo
  suggestion;
- every property referenced exists on its element's label. The error names
  the label and its properties;
- a variable is never bound to two different labels;
- edge direction agrees with the edge table's `SOURCE`/`DESTINATION`
  (see [D6](#d6-wrong-way-edges));
- variables are scoped: a variable is visible from its first binding onwards,
  and an `OPTIONAL MATCH` variable is nullable after its block.

This step is structural. It needs names only, never schemas or types: those
are the binder's job, after lowering.

## 3. Lowering

| GQL | Relational |
|---|---|
| first binding of vertex `(v:L)` | relation of `L`, aliased `v` |
| `(a)-[e:L]->(b)` | relation of `L` aliased `e`, joined `e.<src key> = a.<key>`; `b` joined `e.<dst key> = b.<key>` |
| `(a)<-[e:L]-(b)` | the same, with source and destination swapped |
| composite keys | AND of the column equalities, in declaration order |
| a variable bound again (later path or later `MATCH`) | no new relation; join predicates only |
| anonymous element | a generated `$`-prefixed alias, which cannot collide with a user variable |
| `MATCH ... WHERE`, element `WHERE` | ANDed into the filter of that `MATCH` |
| successive `MATCH` statements | inner joins on their shared variables (linear composition) |
| `OPTIONAL MATCH` block | the block lowered **as one derived relation** (its own joins and its own `WHERE`), LEFT OUTER JOINed on the shared variables |
| `RETURN` items | the projection; aggregates → aggregate, `GROUP BY` → grouping |
| `RETURN DISTINCT` | `SELECT DISTINCT` |
| `ORDER BY`, `OFFSET`, `LIMIT` | the same |
| `COLLECT_LIST` | `ARRAY_AGG` (null handling must match: see [Spec checks](#spec-checks)) |
| `CURRENT_DATE + DURATION 'P30D'` | `CURRENT_DATE + INTERVAL '30' DAY`, constant-folded as usual |
| property `v.p` | `v.<column of p>`: identity when the names match, a single substitution for `PROPERTIES (col AS p)` |

The `OPTIONAL MATCH` rule matters for correctness. Lowering an optional block
as a chain of LEFT JOINs is wrong whenever the block has more than one element
or its own `WHERE`. A filter on the block's inner elements would drop rows,
where the block should instead have padded them with NULLs. The block is
built as a whole and then outer-joined.

The join order written is irrelevant. The optimizer reorders inner joins, and
pushes filters and projections down, exactly as it would for the hand-written
SQL.

**Output names.**
- `v.p` with no `AS` is named `p`, as in SQL under the bare-names ruling
  (qualified only on collision; duplicates refused).
- Any other item needs `AS`.
- Returning a bare element variable (`RETURN s`) is refused in v1, because
  there is no vertex value type.

**Spans.** The GQL source position of every expression travels into the
lowered form, so a binder error (an unknown column, a type mismatch) points at
the GQL text the user wrote.

## 4. Graph definitions (catalog)

A new definition kind beside `ViewDefinition` in
[eidetic.py](../opteryx/connectors/capabilities/eidetic.py):

```python
@dataclasses.dataclass
class PropertyGraphDefinition:
    name: str
    statement: str          # the CREATE PROPERTY GRAPH text, properties expanded
    owner: Optional[str] = None
    description: Optional[str] = None
```

with `get_property_graph` / `create_property_graph` / `drop_property_graph`.

- **The DDL is SQL/PGQ's `CREATE PROPERTY GRAPH`.** It is the standard way to
  map existing tables onto a graph. GQL's own `CREATE GRAPH` defines native
  graphs, which are out of scope. Where this statement is parsed is
  [D2](#d2-where-the-graph-ddl-lives).
- **Stored as text and parsed on use**, as views are, with the parse cached.
- **`PROPERTIES ALL COLUMNS`** (and an element table with no `PROPERTIES`
  clause, which means the same) is expanded to an explicit column list
  **at CREATE time**, and the expanded text is what gets stored. Resolution
  therefore needs no schemas. A column added to the table later is not a
  property until the graph is re-created. This is the same trade-off CREATE
  VIEW's schema snapshot makes.
- **Validated at CREATE time:**
  - element tables resolve;
  - `KEY`, `SOURCE KEY`, `DESTINATION KEY` and `REFERENCES` columns exist,
    checked by binding a synthesized `SELECT <cols> FROM <table>`;
  - `REFERENCES` targets are vertex tables of this graph;
  - key arities match;
  - element table names are unique;
  - no label is used on more than one element table in v1.
- **Foreign-key defaults.** Edge `KEY ... REFERENCES` may default from the
  informational `FOREIGN KEY ... NOT ENFORCED` that Opteryx already records
  (`_read_table_constraints` / `_validate_foreign_key`). Vertex `KEY` is
  mandatory, because Opteryx accepts no PRIMARY KEY.
- **Permissions.** Relations reached through a graph are checked against the
  invoker, as relations reached through a view are. Scans carry the graph name
  the way view-spliced scans carry `via_view`, so the binder's existing refusal
  in [binder/dataset.py](../opteryx/planner/binder/dataset.py) applies. Its
  wording says "View" and needs a graph variant.

## Refused in v1

Each of these raises `UnsupportedSyntaxError` naming the construct:

- quantified paths (`{m,n}`, `*`, `+`, `?`), path modes (`WALK`/`TRAIL`/
  `ACYCLIC`/`SIMPLE`), and selectors (`ANY`, `ANY SHORTEST`, `ALL SHORTEST`,
  `SHORTEST k`);
- label expressions (`A|B`, `A&B`, `!A`, `%`), unlabelled elements, and labels
  on more than one element table. Each of these expands to a `UNION ALL`;
- any-direction and undirected edges (`-[...]-`, `<-[...]->`, `~[...]~`) and
  the abbreviated edges `->`, `<-`, `-` (these are unlabelled);
- path variables (`p = ...`), `ELEMENT_ID`, `IS SAME AS`, `SOURCE OF`,
  `DESTINATION OF`, `IS LABELED`, and returning bare element variables;
- linear composition beyond successive `MATCH` statements: `NEXT`, `LET`,
  `FILTER`, `FOR`, `YIELD`, and `RETURN ... NEXT`;
- explicit match modes (`DIFFERENT EDGES` / `REPEATABLE ELEMENTS`) until
  [D4](#d4-matching-semantics) settles the default;
- data-modifying statements (`INSERT`, `SET`, `REMOVE`, `DELETE`), session and
  transaction commands, and catalog commands other than the graph DDL;
- aggregates mixed with non-aggregated items without `GROUP BY`, until the spec
  check on implicit grouping is done. Refusing now is safe, because allowing
  it later is additive;
- expression properties in the DDL (`PROPERTIES (amount * 100 AS cents)`).

## Spec checks

These are points where I am not certain of ISO/IEC 39075's wording. Each must
be confirmed against the text before the relevant piece is built:

1. the default match mode (see [D4](#d4-matching-semantics));
2. whether a `RETURN` mixing aggregates and non-aggregates without `GROUP BY`
   is an error or groups implicitly;
3. whether `COLLECT_LIST` skips NULLs, and whether Opteryx's `ARRAY_AGG` does;
4. the default output name of a `RETURN` item without `AS`;
5. the delimited-identifier forms (double quotes, backticks, or both);
6. `OFFSET` versus `SKIP`, and `LIMIT` placement relative to `ORDER BY`;
7. the `DURATION` literal format, and date + duration arithmetic.

## Open decisions for the architect

### D1: what the lowering emits

- **(A) `PlanStep`s directly**, as the OData service does: `ScanStep`,
  `JoinStep`, `FilterStep`, `AggregateStep`, `ProjectStep`, `OrderStep`,
  `LimitStep` and `ExitStep`.
- **(B) The logical planner's input:** the statement dict that
  `do_logical_planning_phase` already consumes, built for the equivalent
  `SELECT`. The existing logical planner then produces the plan. This uses the
  *shape* of sqlparser's output and none of its code. The GQL parser stays
  ours.

Recommendation: **(B)**.
- **No duplicated planning logic.** Under (A), the GQL subsystem re-implements
  what the SQL logical planner already does to build joins, grouping, ordering,
  DISTINCT and the exit projection. That duplication is what CLAUDE.md §2
  forbids.
- **(A) re-learns hidden contracts.** The OData builder shows what that costs:
  it carries workarounds for the binder dropping the `$derived` schema before
  a wildcard Exit, and for Exit needing explicit columns.
- **Semantics match SQL by construction.** Grouping, `DISTINCT`, NULL ordering
  and output naming are SQL's because they *are* SQL's planner, so the
  equivalence tests below test the lowering alone.
- **Later phases come almost free.** Quantified paths lower to
  `WITH RECURSIVE`, and label unions to `UNION ALL`. Both are inputs the
  planner already accepts.
- **The cost:** the lowering is tied to that dict shape, which moves with
  sqlparser releases. The SQL planner is tied to the same shape, so a shape
  change breaks both in the same place and the same tests catch it.

### D2: where the graph DDL lives

`CREATE` / `DROP PROPERTY GRAPH` is SQL (SQL/PGQ), and sqlparser cannot parse
it.
- **(a) A SQL statement in the aside parser.** It sits beside the other
  Opteryx-only catalog DDL. This is the only Rust in the feature.
- **(b) Accepted only through the GQL entry point**, parsed by the Python
  parser. That makes it an Opteryx extension to GQL, because standard GQL has
  no table-mapped graph DDL.

Recommendation: **(a)**. It is standard SQL/PGQ syntax and catalog DDL like
`CREATE VIEW`, so it belongs where the rest of the catalog DDL is issued
(jobs, IDE, DBAPI). With (b), a graph could only be defined through an entry
point those surfaces don't have.

### D3: graph namespace

Does a graph share the relation namespace or have its own?
- **Shared:** `CREATE PROPERTY GRAPH x` collides with a table `x`, and
  `SELECT * FROM x` needs a refusal.
- **Separate:** neither problem arises, but it adds a lookup kind.

Either way the catalog repo changes and is released before core.

### D4: matching semantics

I believe GQL's default match mode is `DIFFERENT EDGES`: within one `MATCH`,
two edge variables never bind the same edge. If so, the lowering adds
`NOT (e1.key = e2.key)` for every pair of edge variables on the same edge table
within one `MATCH`. If the default is `REPEATABLE ELEMENTS`, no predicates are
added. This needs confirming against 39075 (spec check 1). The example query
contains no such pair, so it is unaffected either way.

### D5: self-joins from edge tables that are also vertex tables

`BILLED_BY` is backed by `erp.invoices`, so the lowering joins `invoices` to
itself on its key. The options:
- emit the self-join;
- have the lowering recognise "edge table = destination vertex table and
  `DESTINATION KEY` = its `KEY`" and reuse one alias;
- rely on a general self-join elimination rule.

Recommendation: the lowering rule. It is local and provably safe (same
relation, same key).

### D6: wrong-way edges

`(i:Invoice)-[:BILLED_BY]->(s:Supplier)`, against the declared direction:
refuse it with an error naming the declared direction, or return the empty
result the semantics strictly imply? The error is friendlier and the empty
result is literal.

### D7: entry-point surface

- **(a)** A new session method, e.g. `session.execute_gql(text, params)`,
  returning what `execute_to_morsels` returns.
- **(b)** A `language="gql"` argument on the existing execute methods.

(b) reaches the jobs API, IDE and DBAPI with one parameter, but every one of
those surfaces then has to carry it. (a) is narrower and explicit. Whichever is
chosen, the jobs API, IDE and DBAPI changes are follow-ups in their own repos.

## Tests

- **Parser:** every grammar form, every refusal, and error positions, as unit
  tests on `opteryx/planner/gql`.
- **Lowering equivalence:** each GQL query is paired with its hand-written SQL,
  and the two results must be identical. This is the correctness contract.
  There is no built-in graph (`$planets` has no foreign key), so the graphs
  come from `testdata/`:
  - `testdata/tpch_01`: multi-hop N:1 edges (customer → orders → lineitem,
    supplier → nation → region) and a composite key (`partsupp` on
    `(partkey, suppkey)`);
  - `testdata/org/company` (`manager_id` → `id`) and `testdata/org/emp`
    (`mgr` → `empno`): an edge on its own vertex table, which is the D5 case
    and the first test for quantified paths later.
- **`OPTIONAL MATCH`:** cases where the block has a filter on an inner element.
  Here a LEFT-JOIN chain and the correct block lowering give different
  answers, which proves the rule in [§3](#3-lowering).
- **Catalog (`tests/storage`):** create, drop, or-replace and if-exists; the
  stored expanded text; and the permission refusal on an underlying relation.

## Later phases

1. **Label unions** (label expressions, unlabelled elements, multi-table
   labels, undirected edges): `UNION ALL` over fixed-label lowerings. The
   number of branches multiplies with each ambiguous element, so it needs a
   stated cap.
2. **Quantified paths:** `WITH RECURSIVE`, which already exists as a
   semi-naive fixpoint ([RECURSIVE_CTE_DESIGN.md](RECURSIVE_CTE_DESIGN.md)).
   Bounded `{1,n}` under `WALK` fits it directly.
3. **Path modes and shortest paths** need per-path state (the elements already
   visited). That is expressible in a recursive CTE, but probably too slowly to
   keep. This is the phase where a native operator may be justified, after
   measurement.
4. **Linear composition** (`NEXT`, `LET`, `FILTER`, `RETURN ... NEXT`) lowers
   to chained derived relations.
5. **`GRAPH_TABLE` inside SQL**, if sqlparser ever parses SQL/PGQ. The pattern
   AST, graph resolution and lowering are reused, and only the wrapper is new.

## Not in scope

Cypher; graph writes; GQL native and typed graphs; graph storage or indexes;
GQL sessions and transactions; cost-model use of foreign keys; changes to the
jobs API, IDE or DBAPI.
