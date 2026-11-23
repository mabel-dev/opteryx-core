# Property Graphs (SQL/PGQ DDL) and GQL over Opteryx

**Status:** PROPOSAL, 2026-11-23. Nothing built in opteryx-core.

## The split

GQL querying is **not an opteryx-core feature**. It is built from three parts:

| Part | Where | What it does |
|---|---|---|
| Property graph DDL | **opteryx-core** (this document) | `CREATE` / `DROP PROPERTY GRAPH`, catalog storage, `SHOW CREATE PROPERTY GRAPH` |
| GQL → SQL | **gqlparser** (PyPI, `mabel-dev/gqlparser`) | parse GQL; lower a query against a graph definition to a SQL statement ([its design doc](https://github.com/mabel-dev/gqlparser/blob/main/docs/LOWERING_DESIGN.md)) |
| Glue | **the user's code** | read the graph definition, call gqlparser, render SQL, run it |

opteryx-core never imports gqlparser, does not vendor it, and gains no GQL
entry point. A GQL query reaches the engine as ordinary SQL.

```python
import gqlparser
import opteryx
from opteryx.compute import restore_ast

session = opteryx.session()
ddl = ...  # the text returned by: SHOW CREATE PROPERTY GRAPH exposure_graph
graph = gqlparser.parse_property_graph(ddl)
statement = gqlparser.lower(gqlparser.parse(gql_text), graph)  # sqlparser statement dict
sql = restore_ast([statement])[0]
morsels = session.execute_to_morsels(sql)
```

## Rulings (architect, 2026-11-22 / 23)

**Language and parser**
- **Language:** GQL (ISO/IEC 39075). Not Cypher, and not `GRAPH_TABLE`
  inside SQL.
- **Parser:** our own, in Python, published separately as `gqlparser`.
  sqlparser has no PGQ grammar, and vendoring a fork's patch was rejected.
- **gqlparser's error contract (narrower):** an unrecognised GQL construct may
  be reported as a syntax error.

**Lowering and the glue**
- **D1:** the lowering emits the sqlparser statement dict, because it is
  portable to other users. It lives in gqlparser.
- **2a:** the dict is rendered to SQL text and run as ordinary SQL, so
  opteryx-core needs no change for querying.
- **3:** the glue is the user's code.
- **D7 / D8:** opteryx-core is not where GQL lives, and gqlparser is not
  vendored.

**Graph definitions and catalog**
- **D2:** the graph DDL is parsed by the aside parser. The GQL side stays
  Python-only.
- **D3:** a graph shares the relation namespace, because it is another way to
  interact with the tables.
- **`SHOW CREATE PROPERTY GRAPH`** is added, so that user code can read a
  definition.

**Lowering semantics (in gqlparser)**
- **D4:** the default match mode is repeatable elements, so no edge-inequality
  predicates are emitted. This is not confirmed from the standard's text, but
  Microsoft Fabric and BigQuery document it as GQL's default.
- **D5:** an edge stored on a vertex table lowers to a plain self-join for now.
  Reading a dataset once for several scans is a future optimizer improvement.
- **D6:** a wrong-way edge gets a friendly error naming the declared direction.

## 1. DDL (aside parser)

sqlparser cannot parse any of these statements, so each one is an aside
production, as `SHOW CREATE MATERIALIZED VIEW` and `TASK` already are.

```sql
CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] <name>
  VERTEX TABLES (
    <relation> [AS <element name>] KEY (<col>, ...)
      LABEL <label> [PROPERTIES (<col> [AS <property>], ...) | PROPERTIES ALL COLUMNS | NO PROPERTIES]
    , ...
  )
  [EDGE TABLES (
    <relation> [AS <element name>] KEY (<col>, ...)
      SOURCE      KEY (<col>, ...) REFERENCES <vertex element> (<col>, ...)
      DESTINATION KEY (<col>, ...) REFERENCES <vertex element> (<col>, ...)
      LABEL <label> [PROPERTIES (...) | PROPERTIES ALL COLUMNS | NO PROPERTIES]
    , ...
  )]

DROP PROPERTY GRAPH [IF EXISTS] <name>

SHOW CREATE PROPERTY GRAPH <name>
```

**This is a deliberate subset of SQL/PGQ's DDL, and anything outside it is
refused by name.** It allows:
- one `LABEL` per element table;
- columns as properties, renamed or not;
- no expression properties;
- no `DEFAULT LABEL`;
- no `ALTER PROPERTY GRAPH`;
- `OR REPLACE` and `IF NOT EXISTS` as alternatives, never together (the same
  rule as CREATE VIEW, TASK and TRIGGER).

The vertex `KEY` is mandatory, because Opteryx accepts no PRIMARY KEY. An edge's
`SOURCE` / `DESTINATION` `KEY ... REFERENCES` may be omitted when an
informational `FOREIGN KEY ... NOT ENFORCED` on the edge relation supplies it
(`_read_table_constraints` / `_validate_foreign_key` already record these).

## 2. CREATE-time validation and the stored form

The binder can validate every check below at CREATE time, so nothing is
deferred to query time:
- every element relation resolves;
- every `KEY`, `SOURCE KEY`, `DESTINATION KEY`, `REFERENCES` and property
  column exists, checked by binding a synthesized `SELECT <cols> FROM
  <relation>`;
- `REFERENCES` names a vertex element of this graph, and the key arities
  match;
- element names are unique within the graph;
- labels are unique across element tables (one label = one table);
- property names are unique within a label.

**The stored form is canonical, expanded DDL text:**
- `PROPERTIES ALL COLUMNS` and an absent `PROPERTIES` clause are both
  expanded into the explicit column list;
- foreign-key defaults are written out;
- every relation is fully qualified.

`SHOW CREATE PROPERTY GRAPH` returns exactly this text. A column added to a
relation later is not a property until the graph is re-created, the same
trade-off CREATE VIEW's schema snapshot makes.

**Why canonical text.** gqlparser parses what `SHOW CREATE` returns. A
canonical form means gqlparser only has to read one fully explicit shape, and
never needs a schema to know what "all columns" means.

## 3. Catalog

A new definition kind beside `ViewDefinition` in
[eidetic.py](../opteryx/connectors/capabilities/eidetic.py):

```python
@dataclasses.dataclass
class PropertyGraphDefinition:
    name: str
    statement: str          # canonical CREATE PROPERTY GRAPH text
    owner: Optional[str] = None
    description: Optional[str] = None
```

It comes with `get_property_graph`, `create_property_graph` and
`drop_property_graph` on the capability, implemented in the catalog repo, which
is released before core (as views were).

**Shared namespace (D3).** A property graph is a relation kind, alongside
tables and views:
- `CREATE PROPERTY GRAPH x` fails if a table or view `x` exists, and the
  reverse is also true;
- `SELECT ... FROM x` on a graph is refused with "`x` is a property graph;
  query it with GQL (see gqlparser)", not "dataset not found";
- `SHOW` listings that enumerate relations include graphs, marked with their
  kind.

**Permissions.** Creating and dropping a graph follow the same rules as
creating and dropping a view. Reading a graph's definition through `SHOW CREATE`
follows view rules too. Querying needs nothing new, because the lowered SQL
reads the underlying relations, and the invoker's permissions on them apply.

## 4. `SHOW CREATE PROPERTY GRAPH`

- Added to `plan_show_create_query`'s object types as `PROPERTY GRAPH`, through
  the aside parser.
- Executed in [show_create.pyx](../opteryx/operators/show_create/show_create.pyx)
  by a `_property_graph_statement` method, which renders with a new
  `render_create_property_graph` in `opteryx/models/create_statement.py`.
- Returns the canonical text from [§2](#2-create-time-validation-and-the-stored-form).

## Tests (opteryx-core)

- **Aside parser:** every DDL form, plus one refusal per out-of-subset form.
- **CREATE validation:** one test per check in [§2](#2-create-time-validation-and-the-stored-form).
- **Canonical form:** `ALL COLUMNS` expansion, foreign-key defaults and
  qualification. `CREATE` followed by `SHOW CREATE` round-trips to the
  canonical text, and creating from that text again gives the same text.
- **Namespace:** collisions in both directions, and the friendly
  `SELECT FROM <graph>` refusal.
- **`tests/storage`:** create, drop, or-replace and if-exists, plus the
  permission rules.
- **End to end, test-only:** GQL queries lowered by gqlparser (installed from
  PyPI as a test dependency, never imported by `opteryx/`) return the same
  results as hand-written SQL over `testdata/tpch_01` and `testdata/org`.

## Follow-ups (not in this proposal)

- **Scan sharing (from D5):** read a dataset once when a plan scans it more
  than once. This is general optimizer work, and the self-joins produced by
  GQL edges are one beneficiary.
- **Cost model:** use the informational foreign keys to treat edge→vertex
  joins as N:1.
