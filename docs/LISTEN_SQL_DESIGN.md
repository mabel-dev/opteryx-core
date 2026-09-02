# LISTEN / UNLISTEN — Task Notification Subscription Surface

**Status:** DELIVERED 2026-09-02, except `SHOW LISTENERS` (§4) — both sides
implemented. opteryx-catalog: `listeners` subcollection, `add_listener` /
`drop_listener` / `list_listeners` / `list_listeners_for_user`,
`ListenerAlreadyExists` / `ListenerNotFound`, `drop_task` sweep. opteryx-core:
pre-parse grammar (`planner/pre_parse.py`), `Listen`/`Unlisten` logical nodes
and EXPLAIN renderers, the READ gate (`binder/relation.py`
`_bind_task_subscription`), Relation Management `listen`/`unlisten` actions,
`Writable` capability members implemented on both `OpteryxConnector` and
`LocalStoreConnector`, `information_schema.listeners`, the `listening` column
on `information_schema.tasks`, `query_parser` preflight entries. Tests:
`tests/storage/test_listen_ddl.py` (22),
`tests/storage/test_information_schema_listeners.py` (8) in the engine;
`tests/test_listeners.py` (13) in the catalog.

**`SHOW LISTENERS` is recognized and refused** — it names no object, and
`information_schema` in this engine is always workspace-qualified with no
session current-workspace to fall back on. See §4.

**Architect rulings (2026-09-02):**

- The subscribable object is the **TASK**, not the trigger.
- Default filter is **every outcome**; `FOR ERROR` / `FOR SUCCESS` /
  `FOR EVERYTHING` narrows it.
- **One subscription per user per task.** A second `LISTEN` is refused, and the
  refusal hands the caller the `UNLISTEN`/`LISTEN` pair that changes it.
- Subscriptions are a **property of the task**: `DROP TASK` takes them with it.
- `CREATE OR REPLACE TASK` does **not** destroy them.
- `UNLISTEN *` is **not** supported — the task must be named.
- `SHOW CREATE TASK` does **not** render listeners: they are not portable,
  because Alice cannot subscribe Bob.
- Visibility is `SHOW LISTENERS` and `information_schema.listeners`, and, if it
  can be done, a column on `SHOW TASKS` / `information_schema.tasks`.
- Delivery is the in-platform notification system. There is no endpoint
  selection in v1.
- **`LISTEN` is a READ activity**, gated on the caller being able to see the
  table the task affects, checked at subscription time.
- Subscriptions are stored under the task's own document.
- Triggers are a related but distinct concept: a task with no triggers keeps
  its subscriptions.

**Motivation:** a task is a statement the platform runs unattended. Today the
only signal that one failed is the operator alert stream
(`opteryx_catalog/alerts/`), which is process-wide, environment-configured, and
explicitly scoped to *the system itself is wrong* — not to the person who owns
the pipeline. `information_schema.tasks` exposes `last_fired_status`, but
reading it requires knowing to look. A subscription turns "my nightly load
broke" from something you discover into something you are told.

**Implementation substrate (read 2026-09-02):**

- `fire_triggers` (`opteryx-catalog/opteryx_catalog/trigger_firing.py`) already
  classifies every outcome and never raises: failure (audited *and* alerted),
  suspended (deliberately not an error, deliberately not alerted), blocked
  refresh (a fire failure like any other). It stamps the trigger *before*
  alerting so a stamping failure still alerts. This is the emit point, and the
  outcome vocabulary in §2 names distinctions the dispatcher already draws.
- Tasks are Firestore documents under a per-collection `TASKS_SUBCOLLECTION`,
  addressed by `_task_doc_ref(collection, task_name)`.
- `drop_task` already explicitly empties the task's `statement` subcollection,
  because Firestore does not cascade. §8 adds one more call beside it.
- `information_schema.tasks` exists and is AUTOMATE-gated. §4 and §6 are about
  the consequences of that gate.

---

## 1. SQL surface (all of it)

```sql
LISTEN TO <task> [FOR ERROR | SUCCESS | EVERYTHING]
UNLISTEN <task>
SHOW LISTENERS
```

- `<task>` is a single identifier slot — `<task>`, `<collection>.<task>`, or
  `<workspace>.<collection>.<task>`, resolved by the catalog's existing
  `_task_parts`, which accepts all three spellings.
- The slot admits **no placeholder**, for the reason every relation-shaped slot
  in `pre_parse.py` admits none: a parameterised name would let runtime data
  decide which task you are subscribed to.
- `FOR` is omitted in the common case and means `EVERYTHING`.
- No `VIA <endpoint>` clause. With one delivery path there is nothing to
  choose, and an optional slot with one legal value is a lie about what is
  configurable. Adding endpoints later appends a clause and disturbs nothing
  above.
- No `UNLISTEN *`. No `ALTER`.

### Why the task and not the trigger

A task can have several triggers: `CREATE TASK <name> ON <table>` makes one,
and `CREATE TRIGGER <name> ON <table> EXECUTE <task>` adds more — which is how
a task fed by three source tables is wired. Subscribing per-trigger costs three
statements to be told one thing, and silently misses the fourth trigger someone
adds next month.

It also resolves the grammar. A trigger's identity is two-part — `<name> ON
<table>` in every existing statement — so `LISTEN TO <trigger> ON <table> FOR
ERROR` has two `ON`s doing unrelated jobs. A task's name is one part.

## 2. Outcome vocabulary

| Keyword      | Fires on                                                    |
|--------------|-------------------------------------------------------------|
| `ERROR`      | A fire failure: the run raised, or a refresh was blocked.     |
| `SUCCESS`    | A completed run.                                             |
| `EVERYTHING` | Both of the above. The default.                              |

A suspended trigger is **not** an outcome and notifies nobody. `fire_triggers`
already treats suspension as "not an error and not alerted" — an operator
turned it off on purpose, and a notification per skipped fire would be a
heartbeat, not a signal.

The vocabulary is deliberately three closed keywords, not a list. `FOR ERROR,
SUCCESS` is spelled `FOR EVERYTHING`.

## 3. Subscription model

**One subscription per user per task.** Not idempotent: a second `LISTEN` to a
task you already listen to is refused. The refusal knows both the task and the
outcome you just asked for, so it renders the exact pair rather than describing
them:

> You already listen to **daily_load**, for **ERROR**. A task has one
> subscription per user; change it with:
> **UNLISTEN** daily_load; **LISTEN TO** daily_load **FOR EVERYTHING**

**`UNLISTEN` on a task you do not listen to is an error, not a no-op** —
symmetric with the above. Both fail rather than quietly doing nothing, and the
failure is what tells you your model of the world was wrong. Named here because
the lazy implementation of `UNLISTEN` is a delete that succeeds on zero rows.

**A caller can only ever act on their own subscription.** There is no
`LISTEN ... FOR USER <someone>`. Alice cannot subscribe Bob — a subscription is
a claim on someone's attention, and one person cannot make it for another. This
is the rule from which §4's scoping and the `SHOW CREATE TASK` exclusion both
follow.

## 4. Visibility

```sql
SHOW LISTENERS                     -- your subscriptions
SELECT * FROM information_schema.listeners
```

Columns: `task_catalog`, `task_collection`, `task_name`, `outcome`,
`created_at`.

**Rows are self-scoped.** `SHOW LISTENERS` and `information_schema.listeners`
return the caller's own subscriptions and nobody else's — the direct
consequence of "Alice cannot subscribe Bob". There is no `user` column to
filter on because there is only ever one user in the answer.

**`SHOW LISTENERS` did not land — it has no workspace to read.** Every
`information_schema` reader is built by `OpteryxConnector.table_engine()` for
ONE workspace, and every existing use of the schema qualifies it
(`<workspace>.information_schema.tables`); unqualified, `information_schema
.listeners` resolves to a workspace literally named `information_schema`. There
is no session current-workspace to fall back on — `execution_context.schema` is
carried for `current_schema()` and routes nothing — and `SHOW TASKS`, which
would have had the same problem, does not exist. So a bare `SHOW LISTENERS` has
nothing to read.

It is intercepted and refused by name, pointing at the qualified read, rather
than shipped as a keyword that fails with "dataset information_schema/listeners
cannot be found" — and rather than quietly given a workspace slot that was not
ruled. **Open for the architect:** give it one (`SHOW LISTENERS IN
<workspace>`), give the session a current workspace, or drop the statement and
let the table be the surface.

**Otherwise the table is the primary surface, not a convenience.**
`information_schema.tasks` is AUTOMATE-gated and so readable only by the task's
owner, while §6 admits non-owner subscribers. Anyone who subscribes under a read
grant alone cannot read that table at all, and `SHOW LISTENERS` is the only
place they can see what they subscribed to.

**The `SHOW TASKS` / `information_schema.tasks` column is your own subscription,
not a subscriber count.** Spelled `listening`: `ERROR` / `SUCCESS` /
`EVERYTHING` / null. It is the owner's shortcut — the same answer
`SHOW LISTENERS` gives, in the table an owner is already reading. A count would
tell everyone who can read the task how many people watch it, and on a small
team that is the subscriber list — the same leak the `SHOW CREATE TASK`
exclusion closes.

**`SHOW CREATE TASK` renders nothing about listeners.** A recreated task in
another workspace must not arrive with an audience, and the definition is
readable by people who should not learn who is watching.

## 5. Lifecycle

- **`DROP TASK`** deletes the task's subscriptions with it. They are a property
  of the task and there is nothing left to notify about. Subscribers are not
  told — the drop is the notification, if they can see the task at all.
- **`CREATE OR REPLACE TASK`** preserves them. The name is the identity and it
  survives; people subscribed to the task, not to its body. Consequence worth
  stating plainly: a replace hands the new statement an existing audience who
  never saw it change.
- **Dropping the last trigger** on a task leaves subscriptions intact (ruled).
  Triggers are a related but distinct concept: the task still exists and
  `EXECUTE` still fires it by hand, so a subscription to a task nothing fires is
  dormant, not wrong.

## 6. Authority — RULED 2026-09-02

**`LISTEN` is a READ activity.** The gate is the caller's ability to see the
table the task affects — *not* AUTOMATE on the task. A subscription tells you
that a dataset was refreshed or failed to refresh; that is a fact about the
dataset, and the people entitled to it are the people who can read the dataset.
Owning the automation is a different question, and gating on it would mean the
only people who can be told a table is stale are the people who already knew.

**The table is the task's `writes`.** `information_schema.tasks` already derives
it from the statement's own AST at registration — never declared, so it cannot
disagree with what the task will actually do. `LISTEN` reads the same field.

- **`writes` names more than one relation:** READ is required on **all** of
  them. A notification about a task writing A and B is a fact about both.
- **`writes` is empty — refuse.** It is empty for a task that writes no relation
  contents, and for a task registered before the field existed: "a record that
  was never asked the question answers nothing". There is no table to gate on,
  so there is no grant that admits a subscriber, and the statement is refused
  naming that reason. Failing open here would make every pre-`writes` task
  subscribable by anyone.

**Checked at subscription time, once.** The ruling is "at point of creation":
the grant is evaluated when `LISTEN` runs and never re-evaluated at delivery.
Consequence, stated plainly: a user whose READ is later revoked keeps receiving
notifications for that task until they `UNLISTEN` or the task is dropped.
Bounded by the payload being status only — what leaks is that a table they can
no longer read succeeded or failed at a time — but it is a leak, and `REVOKE`
does not sweep subscriptions. If that is not wanted, the fix is a sweep on
revoke, and it is not in this design.

**The refusal must not distinguish "no such task" from "you cannot see its
output".** Both return the same error. Otherwise `LISTEN` is a probe: a caller
with no grants anywhere could enumerate which task names exist by reading which
refusal they got.

## 7. Engine mechanics

**Parse — `pre_parse.py`, not sqlparser.** sqlparser 0.62 has `LISTEN` /
`UNLISTEN` / `NOTIFY`, but they are gated behind
`Dialect::supports_listen_notify()`, which defaults false and is overridden only
by `PostgreSqlDialect`; `OpteryxDialect` does not override it. More decisively,
its grammar is `LISTEN <channel>` — a bare identifier, no `TO`, no modifier —
and its AST returns `Span::empty()` for all three statements, which forfeits the
position information our error contract carries. So:

- `supports_listen_notify()` **stays false**. Enabling it would make `LISTEN
  foo` parse as a Postgres channel subscription we do not implement.
- `LISTEN` / `UNLISTEN` take the `pre_parse.py` route, exactly as `CREATE TASK`
  and `CREATE TRIGGER` do, and for the same reason: sqlparser cannot spell them.
- Statement dicts: `Listen { task_name, outcome }`, `Unlisten { task_name }`.
- Lead-and-match regex pairs, so a malformed statement gets the shape error
  (`Expected: **LISTEN TO** <task> [**FOR ERROR**|**SUCCESS**|**EVERYTHING**]`)
  rather than a parser error pointing at the word `LISTEN`.

**Bind** — resolve the task, read its `writes`, and require READ on every
relation named there (§6). The gate is a relation permission check, not an
AUTOMATE check, so it reuses the same path a `SELECT` against those relations
would take. Refuse in the binder, with the single undifferentiated error §6
requires. `SHOW LISTENERS` follows `ShowGrantsNode`: a physical node reading the
catalog, self-governing its own row filter.

**`information_schema.listeners`** is a new table beside
`InformationSchemaTasksTable`, registered in the same table map. It is
self-scoped rather than AUTOMATE-gated (§4), which makes it the one
`information_schema` table whose row set depends on the caller's identity rather
than their grants — worth flagging in review. It needs no permission check of
its own: every row in it was already gated when the `LISTEN` that wrote it ran.

## 8. Catalog mechanics — RULED 2026-09-02

A `listeners` subcollection **under the task document**, keyed by user identity,
one document per subscriber holding `{outcome, created_at}`. That gives:

- the one-per-user rule for free — the user id is the document id, so a second
  `LISTEN` is a document that already exists, not a query;
- `DROP TASK` cascade as one more `_delete_subcollection(doc_ref.collection(
  "listeners"))` beside the existing `statement` sweep;
- `CREATE OR REPLACE` preservation for free — replace rewrites the statement
  subcollection and leaves the document's other subcollections alone.

This deliberately stays **under the task's own document**. `drop_task`'s
docstring warns that reaching across to other datasets' documents from a drop is
how a partial failure leaves an unreachable orphan — the reason it does not
sweep triggers. Listeners are not across anything.

New catalog API: `add_listener`, `drop_listener`, `list_listeners_for_user`,
`list_listeners(task)` — the last for the fan-out at fire time, not for SQL.

## 9. Not in v1

- Endpoint selection (`VIA <endpoint>`). One delivery path today.
- `UNLISTEN *`.
- Subscribing to anything other than a task — a dataset, a workspace, a trigger.
- `SUCCESS`-only firehose protection. A task firing per-commit with
  `FOR EVERYTHING` will notify per commit; that is what was asked for.
- Any `NOTIFY` statement. Nothing in SQL raises a notification by hand.
- A subscription sweep on `REVOKE`. The §6 gate is checked once, at
  subscription time, and a later revoke leaves the subscription running.
