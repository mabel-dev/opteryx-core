# Snapshot rollback, and calling the head `current`

Status: implemented 2026-08-29.
Companion to [SNAPSHOT_TAGS_DESIGN.md](SNAPSHOT_TAGS_DESIGN.md), which this builds on.

```sql
ALTER TABLE reports ROLLBACK TO VERSION before_the_migration;
SELECT * FROM reports VERSION AS OF PREVIOUS;
SHOW SNAPSHOTS FOR reports;   -- is_current, and `current`/`previous` in the tags column
```

---

## 1. What this changes

Three things that were one thing:

1. **The head has a name, and it is `current`.** It is `current_snapshot_id` and
   `current_snapshot()` in the catalog, `AS OF VERSION CURRENT` in SQL, and `is_current`
   in `SHOW SNAPSHOTS FOR`. One concept, one word, on both sides of the boundary.
2. **The head can move backwards.** `ALTER TABLE ... ROLLBACK TO VERSION` points it at an
   existing snapshot. That is the whole operation — no data is copied and none is deleted.
3. **`PREVIOUS` means the previous version of the DATA**, not the parent snapshot.

They are one change because (2) breaks the assumptions (1) and (3) were written under.
"Current" and "newest" were interchangeable while the head only ever advanced; once it can
move backwards, every place that reached for `snapshots[-1]` or ranked by recency is a
place that answers a question about the present with a version somebody has retired.

## 2. Rollback is a pointer move

The stored head is one field on the dataset document. `rollback_dataset`
(`opteryx_catalog/opteryx_catalog.py`) reads it, checks the target, and writes it — one
Firestore transaction, conditional on the head still being where it was read.

The condition is not decoration. A commit that landed between the read and the write would
be silently discarded by an unconditional set, which is exactly the failure
`SnapshotRaceError` exists to prevent on the write path.

**Nothing is deleted.** The snapshots the head moves off keep their documents, their
manifests and their data files. They stay in `SHOW SNAPSHOTS FOR` and stay readable by id.
That is what makes a rollback reversible: rolling forward to the id it moved off is just
another rollback, and the id is in the record `rollback_dataset` returns because nothing
else records it.

**They are not pinned.** Ordinary retention still applies to them, and once a rolled-off
snapshot expires the rollback can no longer be undone. This is deliberate — pinning them
would make every rollback an open-ended storage commitment that nobody agreed to, and
there is already a mechanism for "keep this indefinitely": a tag. The documentation says
to tag before rolling back if you may want to come back.

**The schema pointer moves with the head.** A dataset whose data is at yesterday's
snapshot but which still advertises today's schema describes columns its files do not
have, and the next append would be built against that schema.

### Alternative considered: roll forward with a new snapshot

The other shape is a rollback that COMMITS a new snapshot whose manifest is the old one's,
so the head only ever advances. It keeps history append-only, needs no changes to
expiration or to anything that ranks snapshots by recency, and makes the rollback itself an
auditable commit.

Rejected because it makes the cheap operation cost a commit, and because "undo" showing up
in the history as a new version is the thing people find confusing about it in the systems
that do it that way. The append-only property was bought with a worse mental model. What it
would have saved is the work in §4.

## 3. `PREVIOUS` walks past maintenance commits

`VERSION AS OF PREVIOUS` used to resolve to `current.parent_snapshot_id`. Compaction and
statistics refresh commit snapshots that change no rows, so on any table with maintenance
enabled the literal parent is routinely the same data an unqualified read returns.

That is the worst available answer. It is not an error, it is not empty, and it is
indistinguishable from a successful time-travel read — somebody comparing "now" against
"before" gets two identical numbers and concludes nothing changed.

`SimpleDataset.previous_user_snapshot()` walks the `parent_snapshot_id` chain and skips
every snapshot that is not `user_created`, twice: once to find the user commit the head
currently rests on (the current version of the data, which may be the head itself), and
again from that commit's parent. `user_created` must be explicitly `True` — a missing
value means "not known to be a user commit", and guessing puts a maintenance commit in
front of somebody asking what they changed.

It walks the CHAIN rather than ranking by recency because of §2: after a rollback the
newest snapshots are not ancestors of the head, they are the version that was undone.

Cost is one document read per hop not already in memory, and the gap between two user
commits is normally zero to a few maintenance commits. `MAX_ANCESTOR_WALK` bounds the
pathological case with `SnapshotAncestryTooDeep` — raised rather than answered with `None`,
because `None` means "there is no previous version", a different fact that would send a
reader looking for data that is there.

## 4. What a backwards-moving head broke

Each of these was correct while the head only advanced.

| Site | Was | Now |
|---|---|---|
| `load_dataset(load_history=True)` | head = `snaps[-1]` of the Firestore stream | head = the stored pointer; falls back to newest-by-sequence only when no pointer is recorded |
| `expire_dataset` | head = `snapshots[-1]` | head = `metadata.current_snapshot()` |
| `last_user_snapshot()` | ranks every live snapshot by recency | ranks `visible_history` only |
| expiration's protected user commit | ranks every live snapshot by recency | ranks `visible_history` only |
| `previous_user_snapshot()` | (did not exist; `PREVIOUS` used the parent) | walks the chain — §3 |
| `TIMESTAMP AS OF` | selects from every live snapshot | selects from `visible_history` only |

`visible_history(head, snapshots)` (`opteryx_catalog/catalog/dataset.py`) is the one rule
those three share, so a rolled-off snapshot cannot be invisible to one of them and visible
to another. It is **ancestry**, not ordering, and that is not a stylistic choice: after a
rollback the next commit's sequence number is allocated from what its writer had in memory,
so a rolled-off snapshot can share a sequence number with a live one and no ordering
separates the two.

It falls back to ordering for a history with no `parent_snapshot_id` recorded anywhere.
Ancestry would collapse such a history to the head alone, and the fallback is safe for
exactly one reason: a dataset with no parent links cannot have been rolled back, because
rollback is newer than parent links are.

The history-load one was already a latent bug: Firestore streams documents in id order,
which is lexicographic on the id string and only accidentally chronological. Rollback turned
it from "wrong when ids change width" into "wrong every time" — expiration loads history on
every run, so it would have rolled a rolled-back dataset forward again without anybody
asking.

`TIMESTAMP AS OF` is a judgement call rather than a bug fix. A point-in-time read could
argue it should return what was live at time T, rolled back or not. It does not: a rollback
is somebody saying "nobody should be reading that version", and honouring that everywhere
except one clause would make the guarantee useless. Naming a rolled-off snapshot's id
explicitly still reads it — the version is retired, not hidden.

## 5. The virtual `current` and `previous` tags

`SHOW SNAPSHOTS FOR` lists `current` in the `tags` column of the head, and `previous` on
the previous version of the DATA, alongside any real
tags. It is not in the tags subcollection, nothing created it, and it holds nothing back
from reclamation.

It is in that column because that column is where a reader finds out what names resolve,
and `VERSION AS OF current` reads exactly like `VERSION AS OF <any tag>`. A name shown in a
listing that could not then be written would be a worse listing.

`normalize_tag_name` refuses `current` and `previous` as tag names. A tag is IMMUTABLE, so a
real tag called `current` would stop meaning "the head" the instant it was created while
`VERSION AS OF current` kept resolving — returning the same frozen snapshot forever. That
reservation is also what makes the version grammar unambiguous: a bare word after
`ROLLBACK TO VERSION` is a tag name or it is nothing.

`current` resolves without a catalog lookup (`OpteryxTable._resolve_snapshot`), since the
head is already in memory.

## 6. Grammar

`ALTER TABLE [IF EXISTS] [ONLY] <name> ROLLBACK TO VERSION <id | tag | CURRENT | PREVIOUS>`

Parsed in `OpteryxDialect::parse_rollback_ddl` (`src/opteryx_dialect.rs`) and carried to
the planner inside `SetTblProperties` under `__opteryx.rollback.version`, for exactly the
reasons tag DDL does it that way — see SNAPSHOT_TAGS_DESIGN.md §6. The key is an unquoted
identifier containing dots, a shape reader text cannot produce, so the transport cannot
become a second spelling of the statement.

The version is carried as TEXT and resolved by the connector, where the catalog is.
`OpteryxConnector._resolve_version_spec` is the single resolver for every statement that
names a version — `CREATE TAG ... AS OF VERSION`, `ROLLBACK TO VERSION`, and the read
path's words — so a word cannot come to mean one thing on a read and another in DDL. Only
rollback passes `allow_tag=True`: a tag whose version is another tag is a copy that
silently stops tracking it.

`ROLLBACK TO VERSION` binds through `_bind_snapshot_ddl`, the same gate as tag DDL: the
`owner` role, and a connector with `supports_version_travel`. A rollback replaces what every
reader of the relation sees; it is not a writer's call.

## 7. The pointer is called `current`

`current-snapshot-id` on the dataset document, and `current_snapshot` in a local store's
`dataset.json`, are unchanged — every document already written carries them. The in-code
name now AGREES with them: `DatasetMetadata.current_snapshot_id`, renamed from
`latest_snapshot_id`, so the stored key and the field a reader writes by hand are one word.

`latest` was retired rather than aliased. It is container-image vocabulary and it claims
something this pointer does not guarantee: a rollback moves the head BACKWARDS, so the
snapshot it names is routinely not the newest one generated. `current` is what dataset
versioning already calls it — Iceberg's `current-snapshot-id`, Delta and Hudi's current
version pointer, `is_current` in SCD Type 2 — and asserts only what is true: this is the
snapshot the catalog points at right now.

The SQL surface was renamed WITH the field, not left behind it: `VERSION AS OF CURRENT`,
the virtual `current` tag in the `tags` column, and the `is_current` column of `SHOW
SNAPSHOTS`. One word everywhere is the point; a field called `current` feeding a column
called `is_latest` would have reintroduced, on the surface readers actually see, exactly
the two-name split this section exists to close.

The cutover is HARD — `latest` is retired, not aliased. `VERSION AS OF LATEST`, `AS OF
VERSION LATEST` and `ROLLBACK TO VERSION LATEST` are refused BY NAME, in the dialect
(`parse_version_spec`) and in the rewriter, with a message naming `CURRENT`. Aliasing it
would have left the misleading word in circulation indefinitely and given two spellings to
every future reader; failing loudly costs one edit and says what happened.

## 8. Tests

* `opteryx-catalog/tests/test_rollback.py` — the move, and every refusal (unknown, expired,
  manifest-less, locked, unknown dataset, no author), each asserting the head did not move.
* `opteryx-catalog/tests/test_previous_user_snapshot.py` — the chain walk, the rollback
  cases, the read cost, the two failures, and `visible_history` (including the shared
  sequence number that defeats ordering, and the no-parent-links fallback).
* `opteryx-catalog/tests/test_expiration_retains_user_snapshot.py` — the head is retained
  when it is not the newest snapshot, and the protected user commit is the one the head
  rests on.
* `opteryx-catalog/tests/test_history_load_head.py` — the head comes from the pointer.
* `opteryx-core/tests/planner/test_alter_table_rollback.py` — the grammar and the transport.
* `opteryx-core/tests/storage/test_version_spec_resolution.py` — the shared resolver.
* `opteryx-core/tests/storage/test_show_snapshots.py` — `is_current` and the virtual
  `current`/`previous` tags, including the no-earlier-version case.
