# Snapshot Tags — Design

**Status:** PROPOSAL, 2026-08-25. Five decisions are ruled by the architect and are
treated as settled below: tags **pin retention**; tags are **immutable**; tags **live
forever and are charged** (which requires a change to the storage collector, §5); the
listing surface is a **`tags` column on `SHOW SNAPSHOTS`** (§8). **Four** decisions
remain open — see [Open decisions](#12-open-decisions-for-the-architect). Nothing is
implemented.

**Proposed surface:**

```sql
ALTER TABLE reports CREATE TAG 'report_202602' AS OF VERSION CURRENT;
ALTER TABLE reports DROP TAG 'report_202602';
SELECT * FROM reports VERSION AS OF 'report_202602';
SHOW SNAPSHOTS FOR reports;   -- gains a `tags` column
```

This is Iceberg's spelling (`ALTER TABLE t CREATE TAG x AS OF VERSION n`), which is
worth keeping — it is the only prior art users will arrive with. Two deliberate
divergences are called out in [§10](#10-deliberate-divergences-from-iceberg).

---

## 1. Problem

`VERSION AS OF <snapshot_id>` already works
([opteryx_connector.py:371](../opteryx/connectors/opteryx_connector.py) —
`_resolve_snapshot`), but a snapshot id is a 64-bit number nobody can hold in their
head, and it does not survive retention: `retained-snapshot-age-days` tombstones the
snapshot, the orphan quarantine takes its manifest and data files, and at roughly T+8
days the files hard-delete
(`opteryx-catalog: opteryx_catalog/catalog/expiration.py`, module docstring). So there is today no way to say "the data the February report was
built from" and still be able to read it in March.

A tag is a **name bound to a snapshot id, which keeps that snapshot alive**. The name
is the small half of the feature; the keeping-alive is the whole point.

---

## 2. Model

A tag is:

* a **name**, unique within one dataset;
* bound to **one snapshot id**, chosen at creation;
* **immutable** — the binding never changes. To move a name, `DROP TAG` then
  `CREATE TAG`; the drop is visible, and unpins the old snapshot as an explicit act;
* a **retention root** — while the tag exists, its snapshot and every file that
  snapshot references are protected from expiration and from the orphan sweep;
* **billable** — the bytes it holds alive are metered like any other stored bytes;
* **owner-only** to create or drop, and **audited**, like every other catalog mutation.

A tag is NOT a branch. Nothing writes to a tag, nothing moves it, there is no
`FAST FORWARD`. Branches are out of scope ([§11](#11-out-of-scope)).

Tags name only **live** snapshots. A tombstoned snapshot cannot be tagged — the
metadata loader filters tombstones out of `metadata.snapshots`, so the id will not
resolve, and `CREATE TAG` fails with "no snapshot N ... it may not exist, or may have
expired", the message `_resolve_snapshot` already produces for reads.

---

## 3. Storage (opteryx-catalog)

Tags live on the **dataset root document**, as a new `DatasetMetadata` field:

```python
# opteryx_catalog/catalog/metadata.py
tags: list[dict] = field(default_factory=list)
# each: {"name", "snapshot_id", "created_at_ms", "created_by", "comment"|None}
```

Root document, not a subcollection, for one reason: the connector already loads the
dataset metadata to resolve any read, so a tag read costs **zero extra round trips**.
A subcollection would put a catalog fetch on the read path for the sake of a handful
of rows.

⚠️ **`save_dataset_metadata` writes the whole document with `set()`.** A field the
dataclass does not carry is *destroyed* by the next commit, not left stale — this is
why `sort_orders`, `maintenance_policy`, `statement_id`, `source_tables` and `runs_as`
are all explicitly on `DatasetMetadata` with comments saying so. `tags` must round-trip
through **both** the loader and the saver, and the test that proves it is "a plain
append commit preserves an existing tag" — not "create a tag and read it back".

**Cap: 100 tags per dataset.** The root document has a 1 MiB Firestore limit shared
with schemas, annotations and the snapshot list; an uncapped tag list is an unbounded
write-amplification and a document-size failure waiting for a script that tags every
refresh. Exceeding the cap is an error naming the cap, not a silent drop.

**The create race.** Expiration tombstones snapshots by writing the same dataset
document. `CREATE TAG` must therefore re-read the snapshot's liveness and append the
tag **inside one transaction on that document**, or a tag can be created against a
snapshot being retired in the same instant — producing exactly the dangling tag the
pinning rule exists to make impossible.

---

## 4. Retention pinning (opteryx-catalog)

Three call sites in `expiration.py` change, and they are not optional relative to one
another — any one of them missed leaves a tag pointing at deleted files:

1. **`_expire_dataset`** — a snapshot whose id appears in `metadata.tags` is never an
   expiry candidate, regardless of `retained-snapshot-age-days`. This is a filter on
   the candidate set, alongside the existing "keep the latest" and
   `USER_SNAPSHOT_LOOKBACK` protections.
2. **`_get_files_in_snapshots` / `_find_full_orphaned_data_files`** — tagged snapshots
   join the retained set whose files populate `kept_files`. The orphan tests are
   subtractions from that set, so a tagged snapshot's files stop being orphan
   candidates automatically once it is in it.
3. **Manifest protection** — a tagged snapshot's manifest is a *required* read. If it
   cannot be read, expiration raises `ManifestProtectionError` and aborts rather than
   computing an orphan set that would delete the tag's data. This is the existing
   behaviour for retained snapshots; tagged snapshots join that class.

`DROP TAG` unpins immediately: the snapshot returns to the normal retention rules on
the next expiration run, and if it is already past the window it expires then. That
is the intended consequence and should be said out loud in the statement's response —
dropping a tag is how you agree to lose the data.

---

## 5. Billing

Tagged bytes ride the existing `DATA_STORAGE_BYTES` event
([opteryx/managers/billing/__init__.py:17](../opteryx/managers/billing/__init__.py))
rather than getting an event type of their own. A tag does not create a new *kind* of
usage; it stops bytes from going away.

⚠️ **The collector does not count them today — verified.** The storage sampler is
`xb500.opteryx: app/operations/record_storage_billing.py`. Per (workspace, collection)
it does:

```python
snapshot = dataset.snapshot()          # the CURRENT snapshot, and only that one
if snapshot is not None:
    collection_bytes += snapshot.summary.get("total-data-size", 0)
```

So bytes held by any non-current snapshot are **billed to nobody**. That is not
specific to tags — every day of `retained-snapshot-age-days` history is unmetered
storage today — but tags make it open-ended, so the collector change is part of this
feature, not adjacent to it. Tracked separately; see [§13](#13-work-breakdown).

Two things for that work to settle:

* **Which measure.** The collector reads `total-data-size`; `total-files-size` is the
  sibling key written next to it on every commit
  (`opteryx-catalog: catalog/dataset.py:782`, `:1214`, `:1547`). Storage billing should
  be the physical bytes GCS holds. If those are the same number the choice is moot; if
  they are not, the collector is currently pricing the wrong one.
* **Old snapshots read zero.** `total-data-size` is absent from the `Snapshot` summary
  default in `catalog/metadata.py`, so any snapshot written before that key existed
  contributes 0 rather than failing. Union-ing over history exposes that where sampling
  only the current snapshot mostly hid it.

**Metering rule: union-dedup.** Count every distinct file referenced by any live
snapshot, once, at the granularity the collector already emits (workspace, collection).
A tag's cost then emerges as exactly the files that would have expired and did not.
Per-tag attribution is rejected: two tags pinning the same file each look free in
isolation, so the numbers do not sum to the bill.

`CREATE TAG` reports the pinned byte count in its response, so the person taking on an
open-ended storage commitment sees its size at the moment they take it on. Payer is the
dataset's workspace billing account, not the tag's creator.

---

## 6. Parser feasibility — the hard part

sqlparser is a **crates.io dependency at 0.62.0**, unmodified
([Cargo.toml](../Cargo.toml)); only the dialect is ours
([src/opteryx_dialect.rs](../src/opteryx_dialect.rs)). Neither half of the proposed
syntax parses today, and `maybe_parse_table_version` is on `Parser`, not `Dialect`, so
there is no override point.

Both gaps are therefore handled the way `COLLECTION` → `SCHEMA` and `WORKSPACE` →
`FUNCTION` already are ([sql_rewriter/__init__.py](../opteryx/planner/sql_rewriter/__init__.py)):
**re-spell the reader's text onto a grammar slot opteryx does not otherwise use.** The
rewriter is offset-preserving, so error positions still point at what the reader wrote.
The rewriter performs **no catalog lookup** — it re-spells, it does not resolve.

### 6.1 Reads — `VERSION AS OF '<tag>'`

sqlparser hard-codes `Expr::Value(self.parse_number_value()?)` for `VERSION AS OF`
(parser/mod.rs:16861), so a string literal is a parse error. Two carriers were checked
against the actual parser:

* **Placeholder** — `VERSION AS OF :report_202602` *does* parse (`parse_number_value`
  accepts `Value::Placeholder`). **Rejected**: opteryx already uses named placeholders
  for query parameters, and `ast_rewriter` substitutes them
  ([ast_rewriter/__init__.py:96](../opteryx/planner/ast_rewriter/__init__.py)) — a tag
  name would collide with a parameter name.
* **`FOR SYSTEM_TIME AS OF <expr>`** — parses via `parse_expr`, so it carries a string
  literal intact, and opteryx **deliberately rejects it today** ("FOR SYSTEM_TIME AS OF
  is not accepted; use TIMESTAMP AS OF", `reference/clauses_catalog.py:756`). An unused
  slot with the right shape. **Recommended carrier.**

So `_rewrite_version_as_of_tag` maps `VERSION AS OF '<tag>'` →
`FOR SYSTEM_TIME AS OF '<tag>'`, and `ForSystemTimeAsOf` becomes, in the builders,
*exclusively* a tag reference — it has no other meaning and no user-facing spelling.

Cost of this choice: the `FOR SYSTEM_TIME AS OF` slot is spent, so it can never later
be given its ANSI meaning without moving tags first.

**In parallel, send the upstream PR**: relax `VersionAsOf` to `parse_value()`, matching
`TimestampAsOf` directly above it. It is a small, generic change (Iceberg and Delta both
name refs there), and when it lands the rewrite is deleted and the builders read the
string straight off `VersionAsOf`. Adds to the list in
`sqlparser_upstream_pr_candidates`.

### 6.2 DDL — `CREATE TAG` / `DROP TAG`

sqlparser has no tag operation on `ALTER TABLE`, and its `Tag` AST node is Snowflake's
key-value governance tag — a **different concept wearing our word**, which is a naming
hazard worth stating once and then avoiding.

Verified to parse in our dialect today, and unused by `plan_alter_table`
([logical_planner.py:3951](../opteryx/planner/logical_planner/logical_planner.py),
which handles only ClusterBy, RenameTable, AddColumn, DropColumn, RenameColumn,
AlterColumn):

```sql
ALTER TABLE reports ADD  PARTITION (tag = 'report_202602', version = 12345)
ALTER TABLE reports DROP PARTITION (tag = 'report_202602')
```

**Recommended carrier.** It carries the tag name and the resolved version selector as
correctly-typed literals, and add/drop are symmetric, so create and drop do not take
different routes. `SET TBLPROPERTIES` was considered and rejected: it makes a tag look
like a property, and properties re-`SET`, which is precisely the mutability the ruling
forbids.

### 6.3 The version selector

`AS OF VERSION <n> | CURRENT | PREVIOUS`, and omitting the clause means `CURRENT`.
`CURRENT` and `PREVIOUS` are resolved to a concrete snapshot id **at creation time** —
a tag stores an id, never a selector, or it would not be immutable.

`CURRENT` is DDL-only. `VERSION AS OF CURRENT` on a read is just a read, and adding a
second spelling for "no time travel" earns nothing.

---

## 7. Read path

Tag resolution happens in **`OpteryxConnector._resolve_snapshot`**
([opteryx_connector.py:371](../opteryx/connectors/opteryx_connector.py)), as a fourth
arm beside the existing id, `PREVIOUS` and `at_date` arms. That function exists so a
statement cannot resolve to one snapshot for its schema and another for its data; a tag
resolved anywhere else would break that property.

The lookup is a scan of the already-loaded `metadata.tags` — **no extra catalog
round trip**, matching the current cost of `VERSION AS OF <id>`.

`Diachronic.version` (`connectors/capabilities/diachronic.py`) currently holds
`int | None`. It gains the tag case; the bind-time gate at
[binder/dataset.py:1157](../opteryx/planner/binder/dataset.py) (`supports_version_travel`)
is unchanged and already rejects both forms against connectors without snapshots.

**Errors.** An unknown tag names the dataset and the tag, and does not enumerate the
tags that do exist — a reader who may not see a dataset's tags should not learn them
from an error. A tag that resolves to a missing snapshot is, by the pinning rule,
**impossible**; if it happens it is a bug in pinning and must say so, not fall back to
current data.

---

## 8. Listing

`SHOW SNAPSHOTS FOR <relation>` gains a `tags` column (list of names on that snapshot),
built in `normalize_snapshot` ([opteryx/models/snapshot_history.py](../opteryx/models/snapshot_history.py))
and typed in the shared schema map there.

Ruled 2026-08-25: this is the listing surface. A separate `SHOW TAGS FOR` is not
proposed: every tag is on exactly one snapshot, so the snapshot listing already has a
row for it, and a second statement would be a second thing to keep consistent. If tags
ever outgrow that — a dataset near the 100 cap — it can be added then.

Without *some* listing the feature is unshippable: tags accumulate invisibly while
pinning storage that someone is paying for.

---

## 9. Test plan

Catalog side:
* a plain append commit preserves existing tags (the `set()` trap — the single most
  important test here);
* expiration does not tombstone a tagged snapshot that is past the retention window;
* the orphan sweep does not quarantine a file referenced only by a tagged snapshot;
* an unreadable manifest on a tagged snapshot aborts expiration rather than computing
  an orphan set;
* `DROP TAG` makes an over-age snapshot expire on the next run;
* tagging a tombstoned snapshot id fails;
* the 101st tag fails, naming the cap.

Engine side:
* rewriter: `VERSION AS OF 'x'` re-spells and preserves offsets; a parse error inside
  the clause still points at the reader's own text;
* rewriter: the re-spelling does not fire inside string literals (the `_QUOTED_SPAN`
  guard every other rewrite carries);
* `CREATE TAG` with `CURRENT`, `PREVIOUS`, explicit `<n>`, and omitted;
* re-creating an existing tag name fails (immutability), and the message says
  `DROP TAG` first;
* reading via tag returns byte-identical results to reading via the underlying id;
* unknown tag error does not enumerate existing tags;
* tag DDL by a non-owner is refused;
* `SHOW SNAPSHOTS FOR` shows the tag on the right row;
* `make q` and `tests/sql`.

---

## 10. Deliberate divergences from Iceberg

* **No `RETAIN n DAYS`.** Iceberg tags carry a max ref age. Ours pin until dropped
  (ruled 2026-08-25: tags live forever, and are charged) — and a self-expiring tag would reintroduce the exact
  failure ("your report tag silently died") the feature exists to remove. The cost of
  forever is answered by billing, not by a timer.
* **Tag names are strings**, matching the proposed surface, where Iceberg uses
  (backtick-quoted) identifiers. Accepting a bare identifier too is
  [D4](#12-open-decisions-for-the-architect).

---

## 11. Out of scope

Branches and any moving ref; writing to a tag; `FAST FORWARD` / `CHERRYPICK`; tags on
views or materialized views (their snapshots belong to the backing table — whether a
tag on that table blocks a view refresh is [D6](#12-open-decisions-for-the-architect));
cross-workspace tag references.

---

## 12. Open decisions for the architect

**D1 — Read carrier.** Recommend the `FOR SYSTEM_TIME AS OF` re-spelling (§6.1), which
spends that slot permanently, plus the upstream PR that later removes it. Alternative:
wait for upstream and ship reads later than DDL.

**D2 — DDL carrier.** Recommend `ADD/DROP PARTITION` (§6.2). The cost is that a
malformed tag statement can surface a parser message mentioning PARTITION; the offset
is still right. Alternative: `ADD CONSTRAINT … CHECK`, same class of cost.

**D3 — RULED 2026-08-25.** Tags live forever and are charged. The collector must
change (§5); union-dedup over live snapshots, at the (workspace, collection)
granularity it already emits.

**D4 — Tag name rules.** Proposed: 1–128 chars, `[A-Za-z0-9_.-]`, **case-insensitive**
matching (stored as written) to match relation and column naming. Accept a bare
identifier as well as a string literal?

**D5 — Tag cap.** 100 per dataset proposed (§3). Right number? It matters more under
the forever ruling than it did before: nothing ages a tag out, so the cap is the only
bound on how much history one dataset can pin.

**D6 — Materialized views.** A tag on an MV's backing table pins a snapshot that the
next refresh would otherwise supersede. Refuse tags on MV backing tables, or allow and
let them pin?

---

## 13. Work breakdown

**opteryx-catalog** (`~/Nextcloud/opteryx-catalog`) — must land first; a tag that does
not pin is worse than no tag:
1. `tags` on `DatasetMetadata`, loader + `save_dataset_metadata` round-trip, cap;
2. transactional `create_tag` / `drop_tag`, owner check, audit entries;
3. expiration: candidate filter, `kept_files`, manifest protection (§4);
4. storage sampler check, and union-dedup metering if it is wrong (§5).

**opteryx-core:**
5. `_rewrite_version_as_of_tag` and the tag-DDL re-spelling in the sql_rewriter;
6. builders: `ForSystemTimeAsOf` → tag reference; `Diachronic.version` widened;
7. `plan_alter_table`: `CreateTag` / `DropTag` logical nodes + operators;
8. `_resolve_snapshot` tag arm;
9. `tags` column on `SHOW SNAPSHOTS FOR`;
10. `reference/` catalogs regenerated from the generators in `dev/` (never hand-edited),
    and `version_as_of` / `alter_table` entries updated;
11. tests (§9).

**xb500.opteryx** (the storage collector, `app/operations/record_storage_billing.py`)
— union over live snapshots instead of the current one, and settle the two questions
in §5. Independent of the engine work and can run in parallel; it is also a live
under-billing fix on its own.

**Upstream:** sqlparser PR relaxing `VersionAsOf` to `parse_value()`; delete the read
rewrite when it lands.
