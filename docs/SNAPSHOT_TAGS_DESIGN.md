# Snapshot Tags — Design

**Status:** PROPOSAL, 2026-08-25. Ruled by the architect and settled below: tags **pin
retention**; tags are **immutable**; tags **live forever unless dropped, and are
charged** (which requires a change to the storage collector, §5); a tag names a
**snapshot, never a timestamp**; tag names are **SQL identifiers** starting with a
letter and **normalized to lowercase**; **100** per dataset; **materialized views can
be tagged**; the listing surface is a **`tags` column on `SHOW SNAPSHOTS`**; and the DDL
is parsed by **our own dialect**, not a fork. Every decision raised is ruled except **D7 (truncate)**,
raised during implementation ([§12](#12-decisions)). The catalog side is built. The
engine side is built except for integration tests: reads, tag DDL and the
`SHOW SNAPSHOTS` column all plan and execute, `make q` is green.

**Proposed surface:**

```sql
ALTER TABLE reports CREATE TAG report_202602 AS OF VERSION CURRENT;
ALTER TABLE reports DROP TAG report_202602;
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
* **immortal** — a tag lives forever unless it is dropped. Nothing ages it out, no
  refresh supersedes it, and no retention policy reaches it. It is killed or it stays;
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

**Direction: a tag points at a snapshot; a snapshot knows nothing about its tags.**
A tag is a name that owns a target, not a property a snapshot carries. That matters
practically as well as semantically: snapshot documents are written once and then only
tombstoned, so making a snapshot gain and lose tag names would turn a write-once
document into a mutable one.

**Firestore.** Datasets already sit at

```
{catalog_root}/{collection}/datasets/{dataset}          <- the dataset document
{catalog_root}/{collection}/datasets/{dataset}/snapshots/{snapshot_id}
{catalog_root}/{collection}/datasets/{dataset}/schemas/{schema_id}
{catalog_root}/{collection}/datasets/{dataset}/triggers/...
```

Tags get a sibling subcollection, **document id = the normalized (lowercase) tag
name** (§12, D4):

```
{catalog_root}/{collection}/datasets/{dataset}/tags/{tag_name}
    { snapshot_id, created_at_ms, created_by, comment | null }
```

The document id doing the naming is the whole reason for this shape:

* **uniqueness is Firestore's**, not ours — a create-if-absent write is the constraint,
  with no read-then-write race to lose;
* **immutability is structural** — tag documents are created and deleted, never
  updated. "Never update this document" is a rule that can be enforced and reviewed;
  "never change this array element" is not;
* it **avoids the `set()` trap entirely.** `save_dataset_metadata` writes the dataset
  document whole with `set()`, so a field the dataclass does not carry is *destroyed*
  by the next commit — which is why `sort_orders`, `maintenance_policy`, `statement_id`,
  `source_tables` and `runs_as` are all explicitly on `DatasetMetadata` with comments
  saying so. Tags in a subcollection are never in that document's blast radius, and
  tag writes never contend with commits;
* it matches how snapshots and schemas are already stored, so it needs no new idea.

Cost: a tag read is one extra document get (`tags/{name}`) — a direct fetch by id, not
a query, and paid only by statements that actually name a tag. That is honest and small.
The alternative, an array on the dataset root document, would make that read free but
buys it with the `set()` trap, contention with every commit, and hand-rolled uniqueness.

**Cap: 100 tags per dataset.** Under the forever rule nothing ages a tag out, so the cap
is the only bound on how much history one dataset can pin. Exceeding it is an error
naming the cap, never a silent drop.

**The create race.** Expiration tombstones a snapshot by writing its snapshot document.
`CREATE TAG` must therefore check that snapshot's liveness and create the tag document
**in one transaction across both documents**, or a tag can be created against a snapshot
being retired in the same instant — producing exactly the dangling tag that pinning
exists to make impossible.

---

## 4. Retention pinning (opteryx-catalog)

**The change is ONE insertion, into `snapshots_to_keep`.** Everything else in the
method is derived from that list, so naming the derived sites as separate edits (as an
earlier draft did) invites three hand-maintained copies of one rule and permanent drift.
Add the tagged snapshots to the retained set, after BOTH retention branches so the
`retention_days` None/0 "keep only the latest" branch is covered too, and the three
protections follow on their own:

1. **Not an expiry candidate** — `snapshots_to_delete` is the complement of the
   retained set, so a tagged snapshot is never tombstoned, at any age.
2. **Its files are not orphans** — every `kept_files` computation reads the same list,
   and the orphan tests are subtractions from it.
3. **Its manifest is protected** — those reads are `required=True`, so an unreadable
   manifest raises `ManifestProtectionError` and aborts the dataset rather than
   computing a short protected set and deleting the tag's data.

`deep_clean` needs no change: it protects the files of every LIVE snapshot, a superset.

`identify_expiring_datasets` — the preview calculation feeding `inspect_snapshots.py`
and `expiration_quick_ref.py` — is a SECOND retention calculation and must filter the
same pins, or it reports a tagged snapshot as expiring. It deletes nothing, so it may
degrade on an unreadable tag set where `_expire_dataset` must not.

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

⚠️ **Correction, 2026-08-25.** An earlier draft of this section called the
collector's choice of measure a defect. It is not — it is a pricing decision,
already made:

* **Logical bytes are charged deliberately.** `total-data-size` sums
  `uncompressed_size_in_bytes`; `total-files-size` sums `file_size_in_bytes`,
  the compressed on-disk size. Billing meters the LOGICAL size: a customer is
  charged for the data they handed over, whatever we compress it to, and the
  spread is margin. Metering the physical size instead would cut charged storage
  by ~96% on real workspaces. This is documented at length in the collector and
  pinned by `test_the_charge_is_logical_bytes_not_what_the_bucket_holds`. The
  physical total is computed and reported as `bytes_on_disk`, never billed.
  **Consequence for this design:** `CREATE TAG` reports the LOGICAL size as the
  bytes it pins, because that is the number that will appear on the invoice;
  the on-disk size rides alongside it and is never the answer.
* **Old snapshots read zero.** `total-data-size` is absent from the `Snapshot`
  summary default in `catalog/metadata.py`, so a snapshot written before that key
  existed contributes 0 rather than failing. The collector now reports a missing
  size as a gap rather than counting it as zero, and an unreadable manifest drops
  that dataset whole and named. Unioning over history is what exposed this, where
  sampling only the current snapshot mostly hid it.

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

Ruled 2026-08-25: **a tag names a snapshot, never a timestamp.** That decides the
carrier as well as the semantics — a re-spelling that parks a tag in the timestamp
grammar is wrong even when no user ever sees it.

sqlparser hard-codes `Expr::Value(self.parse_number_value()?)` for `VERSION AS OF`
(parser/mod.rs:16861), so a string literal is a parse error there. Three carriers were
checked against the actual parser:

* **Placeholder** — `VERSION AS OF :report_202602` parses (`parse_number_value` accepts
  `Value::Placeholder`). **Rejected**: opteryx already substitutes named placeholders as
  query parameters ([ast_rewriter/__init__.py:96](../opteryx/planner/ast_rewriter/__init__.py)),
  so a tag name would collide with a parameter name.
* **`FOR SYSTEM_TIME AS OF <expr>`** — parses, and opteryx rejects it today, so the slot
  is free. **Rejected by the ruling**: it is the timestamp wall.
* **`AT(TAG => '<tag>')`** — parses today in our dialect as `TableVersion::Function`,
  which is *inside the version space*, not the timestamp expression. This is Snowflake
  and Databricks' own time-travel spelling, where `AT(TAG => …)` means precisely what we
  mean by it. **Recommended carrier.**

So `_rewrite_version_as_of_tag` maps `VERSION AS OF '<tag>'` → `AT(TAG => '<tag>')`. No
grammar slot is stolen and no upstream change is needed for reads.

⚠️ **There is dead code in the way.** `_extract_version_expression`
([logical_planner_builders.py:484](../opteryx/planner/logical_planner/logical_planner_builders.py))
already recognises `AT`, walks into its argument list, checks the argument count — and
then unconditionally raises "Time-travel syntax must be `TIMESTAMP AS OF <expression>`".
Every path through that branch raises, so the walking is unreachable work, while the
docstring above it advertises `AT(TIMESTAMP => …)` as supported "legacy/alternate
syntax". That branch has to be either completed or cut; this design completes it for
`TAG =>` and it should not be left half-alive for `TIMESTAMP =>`.

The upstream PR relaxing `VersionAsOf` to `parse_value()` is still worth sending — it
would let the reader's own spelling reach the planner unrewritten — but reads no longer
depend on it.

### 6.2 DDL — `CREATE TAG` / `DROP TAG`

**We parse it ourselves, in our own dialect. No fork, no carrier, no stolen slot.**

There is no dialect *flag* for this — `supports_*` predicates gate behaviour that
upstream already implements, and upstream implements nothing here. But `Dialect`
exposes `parse_statement`, and a dialect that overrides it parses whatever it likes:
Snowflake does exactly this (`snowflake.rs`, `parse_statement` plus its own
`parse_create_table`, `parse_alter_session`, `parse_alter_dynamic_table`), and **so do
we already** — `OpteryxDialect::parse_statement`
([src/opteryx_dialect.rs:211](../src/opteryx_dialect.rs)) intercepts
`ALTER TABLE … ADD COLUMN IF NOT EXISTS`, which upstream gates to a fixed dialect list,
and returns a hand-built `Statement::AlterTable`. Tag DDL is the same move against the
same statement:

```
peek ALTER  ->  maybe_parse(ALTER TABLE <name> (CREATE|DROP) TAG …)
                   miss -> rewind, upstream parses it unchanged
                   hit  -> we parse the whole thing and build the Statement
```

Everything the first draft worried about disappears with it. No `ALTER TABLE` slot is
spent, so partitions, constraints and the rest stay available (D2's objection). No
re-spelling happens in the sql_rewriter, so a malformed tag statement produces **our**
error text at the reader's own offsets, not a parser message about some other feature.

**The one residual: the AST variant we hand back.** `AlterTableOperation` has no
`CreateTag`, and adding one is the only part that would need a fork, so the parsed
result travels to the planner inside an existing variant —
`SetTblProperties` with reserved `__opteryx.tag.*` keys — read by exactly one branch of
`plan_alter_table`. This is an internal transport produced only by our hook, never a
second user-facing spelling, and it is stated here so it is not later mistaken for
property support.

That transport would be spoofable if the prefix were the only thing marking it: a reader
could hand-write the same `SET TBLPROPERTIES` and reach the tag branch. **As built, the
two are told apart by the SHAPE of the key, in the planner** — the dialect emits an
unquoted identifier containing dots, which reader text cannot produce, because a bare
key cannot contain a dot and a quoted key arrives carrying its quote style. A reserved
key that came from reader text is refused by name. This is a change from the first
draft, which put the check in the hook: the planner sees the parsed keys and their quote
styles, where the hook would have to re-tokenise to find them.

A slot table for the carriers considered before this route was found is kept in
[appendix A](#appendix-a-alter-table-carrier-slots-superseded), because it is the
evidence for *why* the dialect hook is the answer rather than a matter of taste.

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

⚠️ **Resolve through `catalog.resolve_tag()` — one document get by id.** NOT by
scanning `metadata.tags`: that map is populated only by a history load
(`load_history=True`), and the read path deliberately does not do one, so a scan there
would find an empty map on every real read and fail every tag as unknown. `tags_loaded`
is the field that says which of those two states the metadata is in, and it defaults to
False precisely so a cheaply-loaded metadata cannot read as "checked, nothing pinned".
The cost model is the one in §3: paid only by statements that actually name a tag.

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
* the 101st tag fails, naming the cap;
* `rename_dataset` carries the tags to the new name — Firestore does not cascade, and a
  rename that left them behind would unpin every tagged snapshot and let the next
  expiration run reclaim exactly the pinned data;
* `drop_dataset` removes the tag documents rather than orphaning them;
* `TRUNCATE` — what a truncate does to a tagged snapshot is **not yet examined**
  (§12, D7).

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
  (ruled 2026-08-25: tags live forever unless dropped, and are charged). A self-expiring
  tag would reintroduce the exact failure ("your report tag silently died") the feature exists to remove. The cost of
  forever is answered by billing, not by a timer.
* **Tag names are identifiers**, per §12 D4 — the same rule Iceberg reaches by using
  backtick-quoted identifiers, arrived at directly. A quoted-string spelling is accepted
  and means the same thing.

---

## 11. Out of scope

Branches and any moving ref; writing to a tag; `FAST FORWARD` / `CHERRYPICK`;
cross-workspace tag references; tag-aware retention policies (a tag is not a policy,
it is a pin).

---

## 12. Decisions

**D1 — RULED 2026-08-25.** A tag names a **snapshot, never a timestamp**. Carrier is
`AT(TAG => '<tag>')`, inside the version space (§6.1). No upstream change needed for
reads.

**D2 — RULED 2026-08-25.** Not a dialect flag, but the dialect owns the parse:
`OpteryxDialect::parse_statement` already intercepts `ALTER TABLE` for guarded
`ADD COLUMN`, exactly as the Snowflake dialect does for its own statements. Tag DDL is
parsed there. **No fork**, no `ALTER TABLE` slot spent, and errors are ours. The only
residual is the AST variant carrying the parsed result to the planner (§6.2).

**D3 — RULED 2026-08-25.** Tags live forever unless dropped, and are charged. The
storage collector must change (§5).

**D4 — RULED 2026-08-25.** A tag name is a valid SQL identifier starting with a letter:
`[A-Za-z][A-Za-z0-9_]*`, no dots, no hyphens, 128 chars. It is **normalized to
lowercase** on the way in — stored lowercase, displayed lowercase, matched lowercase.
`MyTag` and `mytag` are one tag with one spelling, and nothing anywhere has to remember
which casing was typed. Both `CREATE TAG report_202602` and `CREATE TAG 'report_202602'`
are accepted and mean the same thing.

The Firestore document id is that normalized name (§3), so document-id uniqueness and
tag-name uniqueness are the same constraint rather than two that could disagree.

**D5 — RULED 2026-08-25.** 100 tags per dataset.

**D7 — OPEN.** `TRUNCATE` on a dataset with a tagged snapshot has not been examined.
Truncate removes every row, which means a commit whose retained set may not include the
tagged snapshot's files. Either it pins like everything else or truncate is a way to
silently unpin; nobody has looked.

**D6 — RULED 2026-08-25.** Materialized views have snapshots, so their snapshots can be
tagged. An MV backing table is a dataset with `dataset_type="materialized_view"` and a
normal snapshot history; a tag on one pins it exactly as it would on any other dataset,
and a refresh that supersedes the snapshot does not unpin it. That is the vampire rule
applied consistently, and it means an MV that refreshes every 15 minutes can pin an
arbitrary amount of history if someone tags it — bounded only by D5.

---

## 13. Work breakdown

**opteryx-catalog** (`~/Nextcloud/opteryx-catalog`) — must land first; a tag that does
not pin is worse than no tag:
1. the `tags` subcollection, doc id = the normalized tag name, with the cap (§3);
2. transactional `create_tag` / `drop_tag`, audit entries. NOT the permission
   check: the catalog takes an `author` and enforces nothing, because permission
   is bind-time and engine-side - `can_perform_action(..., action="ALTER")` in
   `opteryx/planner/binder/relation.py`, which every other `ALTER TABLE` path
   already goes through;
3. expiration: candidate filter, `kept_files`, manifest protection (§4);
4. nothing on billing — the collector lives in a third repo, below.

**opteryx-core:**
5. `_rewrite_version_as_of_tag`: `VERSION AS OF '<tag>'` → `AT(TAG => '<tag>')`;
6. builders: complete or cut the dead `AT` branch in `_extract_version_expression`
   (§6.1), route `AT(TAG => …)` to a tag reference, widen `Diachronic.version`;
7. `OpteryxDialect::parse_statement`: parse `ALTER TABLE … CREATE/DROP TAG`, and reject
   hand-written `SET TBLPROPERTIES` carrying a reserved `__opteryx.` key (§6.2);
8. `plan_alter_table`: `CreateTag` / `DropTag` logical nodes + operators, with the
   bind-time `ALTER` permission check in `binder/relation.py` and the pinned-byte
   count reported in the statement's response (§5);
9. `_resolve_snapshot` tag arm;
10. `tags` column on `SHOW SNAPSHOTS FOR`;
11. `reference/` catalogs regenerated from the generators in `dev/` (never hand-edited),
    and `version_as_of` / `alter_table` entries updated;
12. tests (§9).

**xb500.opteryx** (the storage collector, `app/operations/record_storage_billing.py`)
— union over live snapshots instead of the current one, and settle the two questions
in §5. Independent of the engine work and can run in parallel; it is also a live
under-billing fix on its own.

**Upstream:** sqlparser PR relaxing `VersionAsOf` to `parse_value()`; delete the read
rewrite when it lands.


---

## Appendix A. `ALTER TABLE` carrier slots (superseded)

Before the dialect hook (§6.2) settled this, the question was which existing
`ALTER TABLE` slot could carry tag DDL. Every row was parse-tested against our dialect.
It is kept because it is the evidence that no re-spelling was safe — the two slots we
would never want are too small to carry a name *and* a version, and every slot big
enough has a plausible future.

| slot | parses | carries | verdict |
|------|--------|---------|---------|
| `ADD/DROP PARTITION (k = v, …)` | yes | name + version, correctly typed | we may want partitions |
| `ADD CONSTRAINT n CHECK (…)` | yes | identifier + expression | we may want CHECK constraints |
| `ENABLE/DISABLE TRIGGER t` | yes | one identifier | we already have triggers |
| `SWAP WITH other` | yes | one object name | atomic swap is a plausible feature |
| `REPLICA IDENTITY FULL` | yes | an enum, or one identifier | dead for us, too small |
| `AUTO_INCREMENT = n` | yes | one number | dead for us, too small |
| `SET TBLPROPERTIES ('a'='b')` | yes | key-value pairs | now the internal transport only (§6.2) |
| `ADD/DROP PROJECTION p` | parses as ADD/DROP COLUMN | — | not a real slot in our dialect |
| `FREEZE PARTITION p` | no | — | — |
| `SET TAG a = 'b'` / `UNSET TAG` | no | — | Snowflake governance tags; not in our dialect |
