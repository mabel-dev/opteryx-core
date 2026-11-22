# SQL/PGQ Property Graph Queries — Fixed-Length Patterns

**Status:** PROPOSAL, 2026-11-22. Nothing built. Five decisions for the architect
are listed under [Open decisions](#open-decisions-for-the-architect); D1
(parsing) blocks everything else.

## Scope

SQL/PGQ (ISO/IEC 9075-16, SQL:2023) adds two things to SQL:

- `CREATE PROPERTY GRAPH` — a catalog object that declares which existing tables
  are vertices, which are edges, how edges reference vertices, and which columns
  are exposed as properties under which labels.
- `GRAPH_TABLE (graph MATCH ... COLUMNS (...))` — a `FROM` item whose rows are the
  matches of a graph pattern, projected through `COLUMNS`.

Its pattern language is a derivation of Cypher's and is shared with GQL
(ISO/IEC 39075). DuckDB (DuckPGQ) and Oracle ship it; PostgreSQL is adding it.

**This document covers fixed-length patterns only**: every path is a fixed
sequence of vertices and edges, with no quantifiers (`{1,5}`, `*`, `+`). A
fixed-length match is a conjunctive query — inner equi-joins, a filter, a
projection — so it can be **lowered to a query the engine already plans**.
There is no new operator, no execution change, and no binder change. Quantified
paths are a later phase ([Later phases](#later-phases)).

## The example

```sql
CREATE PROPERTY GRAPH exposure_graph
  VERTEX TABLES (
    erp.suppliers     KEY (supplier_id) LABEL Supplier PROPERTIES (supplier_id, name),
    erp.invoices      KEY (invoice_id)  LABEL Invoice  PROPERTIES (invoice_id, amount, due_date),
    register.persons  KEY (person_id)   LABEL Person   PROPERTIES (person_id, name)
  )
  EDGE TABLES (
    erp.invoices AS billed_by KEY (invoice_id)
      SOURCE      KEY (supplier_id) REFERENCES suppliers (supplier_id)
      DESTINATION KEY (invoice_id)  REFERENCES invoices  (invoice_id)
      LABEL BILLED_BY NO PROPERTIES,
    register.ownership KEY (supplier_id, person_id)
      SOURCE      KEY (supplier_id) REFERENCES suppliers (supplier_id)
      DESTINATION KEY (person_id)   REFERENCES persons   (person_id)
      LABEL ULTIMATELY_OWNED_BY NO PROPERTIES
  );

SELECT g.supplier, SUM(g.amount) AS exposure, ARRAY_AGG(DISTINCT g.owner) AS owners
FROM GRAPH_TABLE (exposure_graph
       MATCH (s:Supplier)-[:BILLED_BY]->(i:Invoice),
             (s)-[:ULTIMATELY_OWNED_BY]->(o:Person)
       WHERE i.due_date < CURRENT_DATE + INTERVAL '30' DAY
       COLUMNS (s.name AS supplier, i.amount AS amount, o.name AS owner)
     ) AS g
GROUP BY g.supplier
ORDER BY exposure DESC
LIMIT 10;
```

`erp.*` and `register.*` may be any relations the resolver can resolve —
datasets, views, or federated connectors. The graph adds no data and no IO path.

(The query as written inflates `exposure` by the owner count, because the two
paths cross-multiply per supplier — exactly as the equivalent Cypher does. That
is the user's semantics, not the lowering's; the lowering must reproduce it.)

## Where it lands

```
parse → logical plan → RESOLVE → plan rewrite → bind → optimize → physical
          ▲               ▲
          │               └─ GraphTableStep → lowered query AST → plan → _splice
          └─ GRAPH_TABLE factor → GraphTableStep
```

The precedent is view expansion. The Relation Resolver
([relation_resolver/__init__.py](../opteryx/planner/relation_resolver/__init__.py))
turns a name in `FROM` into a plan: a view's SQL is fetched from the catalog,
planned by the ordinary logical planner (`_view_as_plan` in
[managers/views](../opteryx/managers/views/__init__.py)), and spliced in place of
the Scan behind a `SubqueryStep` boundary (`_splice`). The binder never sees a
view. The resolver is explicitly *structural* — names and plans, never schemas —
and lowering a fixed-length pattern needs only names: label → element table,
key columns, property → column. All of those come from the graph definition.

So `GRAPH_TABLE` is resolved exactly like a view, except that the plan spliced in
is synthesized from the pattern instead of read from the catalog as SQL text.

## 1. Parsing

sqlparser 0.63 has no PGQ grammar. Its `Dialect` trait extends only expressions
(`parse_prefix`, `parse_infix`) and statements (`parse_statement`) — there is no
table-factor hook. The aside parser ([ASIDE_PARSER_DESIGN.md](ASIDE_PARSER_DESIGN.md))
is statement-level, dispatched on leading tokens.

- **`CREATE PROPERTY GRAPH` / `DROP PROPERTY GRAPH`** are statements, so they can
  go in the aside parser. Note the grammar is less flat than the existing aside
  forms: two parenthesised lists of element tables, each with optional `AS`,
  `KEY (...)`, `SOURCE ... REFERENCES ...`, `DESTINATION ... REFERENCES ...`, and
  one or more `LABEL ... PROPERTIES (...)` clauses.
- **`GRAPH_TABLE`** sits inside a `SELECT`, so it is not reachable from either
  layer as they stand. See [D1](#d1-how-graph_table-is-parsed).

The pattern parser itself is small: node `(var? (:Label)? (WHERE expr)?)`, edge
`-[var? (:Label)? (WHERE expr)?]->` or `<-[...]-`, comma-separated paths, then an
optional `WHERE` and a mandatory `COLUMNS (expr AS name, ...)`. Embedded
expressions are parsed by sqlparser's own expression parser, so they arrive in
the AST shape the planner builders already read.

## 2. Catalog: the graph definition

A new definition kind beside `ViewDefinition` in
[eidetic.py](../opteryx/connectors/capabilities/eidetic.py):

```python
@dataclasses.dataclass
class PropertyGraphDefinition:
    name: str
    statement: str          # the CREATE PROPERTY GRAPH text, properties EXPANDED
    owner: Optional[str] = None
    description: Optional[str] = None
```

with `get_property_graph` / `create_property_graph` / `drop_property_graph` on
the capability.

**Stored as text, parsed on use** — the same contract as views (`statement` is
re-parsed; the parse is cached as `_parse_view` caches). That keeps the catalog
change to "a new kind holding text", not a structured schema the catalog repo
must model.

**`PROPERTIES ALL COLUMNS`** (and an element table with no `PROPERTIES` clause,
which means the same) needs the table's schema, so it is expanded **once, at
CREATE time**, into an explicit column list, and the expanded text is what gets
stored. A column added to the table later is not a property until the graph is
re-created. This matches how CREATE VIEW snapshots a schema, and is what keeps
resolution structural.

**CREATE-time validation** (all errors, none deferred to query time):

- every element table resolves;
- every `KEY`, `SOURCE KEY`, `DESTINATION KEY` and `REFERENCES` column exists on
  its table — checked by binding a synthesized `SELECT <cols> FROM <table>`, which
  is the existing binder doing existing work;
- every `REFERENCES` target names a vertex element table **of this graph**;
- `SOURCE KEY` and the referenced vertex key have the same arity;
- element table names (the `AS` name, defaulting to the table name) are unique
  within the graph;
- a label used on several element tables is refused in v1 (see
  [Refused in v1](#refused-in-v1)).

**Defaults from foreign keys.** SQL/PGQ lets `SOURCE`/`DESTINATION` omit
`KEY ... REFERENCES` when a foreign key supplies it. Opteryx already records
informational `FOREIGN KEY ... REFERENCES ... NOT ENFORCED`
(`_read_table_constraints` / `_validate_foreign_key` in the logical planner), so
edge references can default from them. Vertex `KEY` cannot default — Opteryx
accepts no PRIMARY KEY — so in v1 it is mandatory.

## 3. Logical planner: `GraphTableStep`

[`create_node_relation`](../opteryx/planner/logical_planner/logical_planner.py)
today ends unknown table factors with
`**{factor}** is not a supported FROM item`. A `GraphTable` branch emits one
unresolved node:

```python
class GraphTableStep(PlanStep):
    graph: str            # graph name, as written
    paths: tuple          # parsed path patterns (elements: var, label, direction, element WHERE AST)
    where: dict | None    # the MATCH WHERE, as AST
    columns: tuple        # (expression AST, output name) pairs from COLUMNS
    alias: str            # the GRAPH_TABLE alias; mandatory (as for other table functions)
```

This step only carries syntax. It does no catalog read and binds nothing.

## 4. Resolver: lowering

When `_resolve` meets a `GraphTableStep`, it fetches the definition (one catalog
round trip, memoised like relations), parses it, and builds a **query AST** — the
same dict shape sqlparser produces for a hand-written `SELECT` — then plans it
with `plan_query` and `_splice`s it in under the `GRAPH_TABLE` alias. Splicing,
the resolver's later passes, and the plan rewriter run over the result unchanged.

### Rules

| Pattern | Lowered to |
|---|---|
| first occurrence of vertex `(v:L)` | `FROM`/`JOIN <table of L> AS v` |
| edge `(a)-[e:L]->(b)` | `JOIN <table of L> AS e ON e.<src key> = a.<key>`, then `b` joined `ON e.<dst key> = b.<key>` |
| edge `(a)<-[e:L]-(b)` | the same, source and destination swapped |
| composite keys | AND of the column equalities, in declaration order |
| later occurrence of `v` (same or another path) | no new table — only the join predicates against the existing alias |
| anonymous vertex or edge | a generated `$`-prefixed alias (cannot collide with a user variable) |
| element `WHERE` and `MATCH ... WHERE` | ANDed into the lowered `WHERE` |
| `COLUMNS (expr AS name, ...)` | the `SELECT` list |
| property `v.p` | `v.<column of p on v's label>` — identity when the property name is the column name, one identifier substitution when it was renamed with `PROPERTIES (col AS p)` |

The element variable **is** the table alias, so a property reference is already
a qualified column reference; the WHERE and COLUMNS ASTs are dropped in verbatim
except for renamed properties.

The join order the lowering writes is irrelevant: it emits inner joins, and the
optimizer reorders them.

### The example, lowered

```sql
SELECT s.name AS supplier, i.amount AS amount, o.name AS owner
FROM erp.suppliers AS s
JOIN erp.invoices       AS "$e1" ON "$e1".supplier_id = s.supplier_id
JOIN erp.invoices       AS i     ON "$e1".invoice_id  = i.invoice_id
JOIN register.ownership AS "$e2" ON "$e2".supplier_id = s.supplier_id
JOIN register.persons   AS o     ON "$e2".person_id   = o.person_id
WHERE i.due_date < CURRENT_DATE + INTERVAL '30' DAY
```

`BILLED_BY` is backed by `erp.invoices` itself (the foreign key lives on the
invoice row), so the lowering joins `invoices` to itself on its key. See
[D4](#d4-self-joins-from-edge-tables-that-are-also-vertex-tables).

### Errors raised during lowering

- unknown graph → `DatasetNotFoundError`-class error naming the graph;
- unknown label → names the graph's labels, with the existing typo suggestion;
- property not declared on the element's label → names the label and its properties;
- a variable used with two different labels (`(s:Supplier)`, later `(s:Person)`) → refused;
- an edge whose endpoint label doesn't match the edge table's `REFERENCES` target
  (`(i:Invoice)-[:BILLED_BY]->(s:Supplier)`) → refused, naming the declared
  direction. In v1 this is an error rather than an empty result, because a
  wrong-way edge is almost always a mistake, not an intended empty match;
- duplicate `COLUMNS` output names → refused, consistent with the bare-names
  ruling (duplicates are refused, not qualified).

### Permissions and lineage

Scans spliced from a graph carry the graph name the way view-spliced Scans carry
`via_view`, so the binder's existing invoker-permission check on underlying
relations ([binder/dataset.py](../opteryx/planner/binder/dataset.py), the
"View X reads Y" refusal) and lineage apply unchanged. The refusal's wording
says "View" today and needs to say "Property graph" for these scans.

## 5. Binder, optimizer, execution

No change. After splicing, what reaches the binder is Scans, inner Joins, a
Filter and a Project behind a Subquery boundary — the same plan as the
hand-written SQL above. That means:

- join reordering, filter pushdown (`due_date` into the invoice scan), projection
  pushdown and every scan optimisation apply as usual;
- federated element tables are probed, pushed down to, or scanned exactly as they
  would be in the hand-written query;
- `EXPLAIN` shows the joins under a boundary named by the `GRAPH_TABLE` alias.

The informational foreign keys behind edge references could also tell the cost
estimator that an edge→vertex join is N:1 (no fan-out). The optimizer and cost
estimator read no foreign keys today; that is a separate piece of work and is not
part of this proposal.

## Refused in v1

Each of these raises `UnsupportedSyntaxError` that names the construct:

- **quantified paths** — `{m,n}`, `*`, `+`, `?`;
- **path modes and selectors** — `WALK`/`TRAIL`/`ACYCLIC`/`SIMPLE`, `ANY`,
  `ANY SHORTEST`, `ALL SHORTEST`;
- **label expressions** — `:A|B`, `:A&B`, `:!A`, `:%`;
- **unlabelled elements** — `(x)`, `-[e]->` with no label (these match every
  element table, which is a `UNION ALL`);
- **one label on several element tables** (also a `UNION ALL`);
- **any-direction edges** — `-[...]-`, `<-[...]->`, `~[...]~`;
- **path variables** — `p = (a)-[]->(b)`;
- **element functions and predicates** — `ELEMENT_ID`, `IS SAME AS`,
  `SOURCE OF`, `DESTINATION OF`, `IS LABELED`;
- **`COLUMNS (v.*)`**;
- **expression properties** — `PROPERTIES (amount * 100 AS cents)`; columns and
  `column AS name` only;
- **references to columns outside the `GRAPH_TABLE`** (lateral) — these fail
  naturally, because the spliced sub-plan is an independent subquery;
- **`ALTER PROPERTY GRAPH`** — drop and re-create.

## Later phases

Not proposed here; listed so v1 does not design itself into a corner.

1. **Label unions** (label expressions, unlabelled elements, multi-table labels,
   any-direction edges). Each expands to the `UNION ALL` of the fixed-label
   lowerings. Still pure resolver work. The expansion is multiplicative (two
   ambiguous elements × three tables each = nine branches), so it needs a stated
   bound.
2. **Quantified paths.** The engine already has `WITH RECURSIVE` as a semi-naive
   fixpoint with `UNION` and `UNION ALL` ([RECURSIVE_CTE_DESIGN.md](RECURSIVE_CTE_DESIGN.md)).
   `(a)-[:L]->{1,n}(b)` lowers to a recursive CTE over the edge table with a
   depth counter. The bound `n` and `WALK` semantics fit it directly.
3. **Path modes** (`TRAIL`, `ACYCLIC`, `SIMPLE`) and **shortest-path selectors**
   need per-path state (the visited elements). That is expressible in a recursive
   CTE as an array column checked by `NOT ARRAY_CONTAINS`, but probably too slowly
   to keep; this is the phase where a native operator may be justified. It needs
   measuring first.

## Open decisions for the architect

### D1: how `GRAPH_TABLE` is parsed

- **(a) Upstream PR to sqlparser** adding `TableFactor::GraphTable` (and the
  property graph DDL), carried as a `[patch.crates-io]` git dependency until it is
  released. The planner sees a real AST node. Carrying a patched dependency is
  vendoring, which needs agreement.
- **(b) Splice at the aside token layer.** Scan the token vector for
  `GRAPH_TABLE (`, parse the balanced region with an Opteryx pattern parser, and
  replace it with a stand-in factor that the planner recognises. No dependency
  change, but the stand-in is the disguise ASIDE_PARSER_DESIGN §2.2 argues
  against.

Recommendation: **(a)**. It is the only option where the AST tells the truth,
and the grammar is upstreamable — DuckDB and Postgres both accept it.

### D2: graph namespace

Does a property graph share the relation namespace (Postgres makes it a relation
kind) or have its own? Sharing it means `CREATE PROPERTY GRAPH x` collides with a
table `x`, and `SELECT * FROM x` on a graph needs its own refusal. A separate
namespace avoids both but adds a lookup kind to the catalog. Either way this is a
catalog-repo change and lands before core (as views did).

### D3: matching semantics

The lowering above is homomorphic: two edge variables in one pattern may bind
the same edge row, so no `e1 <> e2` predicates are emitted. My understanding is
that this is SQL/PGQ's default for fixed-length patterns (unlike Cypher's
relationship isomorphism), but it must be confirmed against ISO/IEC 9075-16
before building. If the standard is isomorphic, the lowering adds pairwise
key-inequality predicates between same-table edge variables — still pure joins,
just more predicates.

### D4: self-joins from edge tables that are also vertex tables

An edge backed by a foreign key on a vertex table (the `BILLED_BY` case) lowers
to a self-join on that table's key. That join is redundant, but nothing in the
optimizer removes key self-joins today as far as I know. Choose one:

- emit it and accept the cost;
- have the lowering recognise "edge table is the destination vertex table and
  `DESTINATION KEY` is its `KEY`" and use one alias for both;
- leave it to a general self-join elimination rule.

Recommendation: the lowering-side rule. It is local, provably safe (same table,
same key), and needs no optimizer work.

### D5: wrong-way edges

Error, as proposed above, or an empty result, which is what the standard's
semantics strictly imply? The error is friendlier; the empty result is the
standard. If the standard behaviour is wanted, the lowering emits an empty
relation for that path.

## Tests

- **Lowering equivalence (sqllogictest):** each `GRAPH_TABLE` query is paired with
  its hand-written join query, and the two results must be identical. This is the
  correctness contract — the lowering is right exactly when it agrees with SQL the
  engine already answers correctly. There is no built-in graph (`$planets` has no
  foreign key), so the graphs come from `testdata/`:
  - `testdata/tpch_01` — multi-hop N:1 edges (customer → orders → lineitem,
    supplier → nation → region) and a composite key (`partsupp` on
    `(partkey, suppkey)`);
  - `testdata/org/company` (`manager_id` → `id`) and `testdata/org/emp`
    (`mgr` → `empno`) — an edge that lives on its own vertex table, which is the
    D4 case, and the natural first test for quantified paths later.
- **Plan shape (unit):** pattern → lowered AST, asserted structurally — aliases,
  join predicates, composite keys, a shared variable, anonymous elements,
  property renames, self-join collapse (if D4 rules that way).
- **Refusals (unit):** one test per item in [Refused in v1](#refused-in-v1), plus
  every lowering error and every CREATE-time validation, each asserting the error
  type and message.
- **Catalog (`tests/storage`):** create / drop / or-replace / if-exists, the
  stored expanded text, and the permission refusal on an underlying table read
  through a graph.

## Not in scope

GQL as a standalone query language (`MATCH ... RETURN` without SQL around it);
Cypher; graph writes; graph-specific indexes or storage; cost-model use of
foreign keys; anything in [Later phases](#later-phases).
