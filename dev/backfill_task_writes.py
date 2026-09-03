#!/usr/bin/env python3
"""Backfill `writes` on tasks registered before the field existed.

WHY THIS EXISTS. A task's `writes` is the list of relations its statement
writes, derived from the statement's own AST at registration. Tasks registered
before the field existed carry an empty list, which is indistinguishable from a
task that writes nothing - "a record that was never asked the question answers
nothing". Two things read it:

  - `information_schema.tasks.writes`, the edge that lets a pipeline be followed
    THROUGH a task rather than ending at it. A legacy task currently claims to
    feed nothing, so `raw -> task -> curated` reads as two fragments.
  - `LISTEN TO`, whose gate is READ on what the task writes. With nothing to
    gate on there is no grant that admits a subscriber, so a legacy task cannot
    be subscribed to by anyone - which is the failure that prompted this.

WHY NOT `CREATE OR REPLACE TASK`. The error message names it, and for one task
by hand it is the right answer. For a sweep it is the wrong tool, twice over:

  1. `create_task` writes the task document with `doc_ref.set(...)`, carrying an
     explicit field list. Any field a deployed record holds that the list does
     not name is DESTROYED by the rewrite. A backfill must not be the thing that
     discovers a field nobody remembered.
  2. It writes a new statement document and bumps `sequence-number` for every
     task, so every task's version history gains a no-op revision attributed to
     whoever ran the sweep, and `last-updated-by` / `last-updated-at-ms` stop
     describing the last real change.

So this patches ONE FIELD with `update()`. The trigger pointer, the window
guard, the suspension state and the fire history are not touched because they
are not written.

DERIVATION. `writes` comes from `extract_write_targets` - the same function
`plan_create_task` uses - over the task's own recorded statement, parsed with
the Opteryx dialect. It cannot disagree with what the task will do, because it
is the same reading of the same text.

USAGE. Reports and changes nothing by default:

    python dev/backfill_task_writes.py --workspace <ws> [--collection <c>]
    python dev/backfill_task_writes.py --workspace <ws> --apply

A task whose statement no longer parses, or whose record has no statement at
all, is REPORTED AND SKIPPED: this is a backfill, not a repair, and a record it
cannot read is a finding rather than something to guess at.
"""

import argparse
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

# The catalog lives in its OWN Firestore database, not `(default)`. Every
# service resolves it the same way - see control.opteryx's `_catalog_database`,
# and policy.opteryx, which hardcodes the same name - so a tool that defaults to
# `(default)` connects successfully to the wrong database and reports every
# workspace as missing. Overridable for parity with those services.
_CATALOG_DATABASE_DEFAULT = "catalogs"


def _default_project():
    """The catalog's GCP project, resolved as control.opteryx resolves it."""
    return (
        os.environ.get("FIRESTORE_PROJECT_ID")
        or os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or None
    )


def _derive_writes(sql: str):
    """The relations `sql` writes, as `plan_create_task` would record them."""
    from opteryx.third_party import sqloxide
    from opteryx.utils.query_parser import extract_write_targets

    parsed = sqloxide.parse_sql(sql, _dialect="opteryx")
    if len(parsed) != 1:
        raise ValueError(f"a task runs ONE statement; this record holds {len(parsed)}")
    return extract_write_targets(parsed[0])


def _tasks(catalog, only_collection):
    collections = [only_collection] if only_collection else catalog.list_collections()
    for collection in collections:
        for name in catalog.list_tasks(collection):
            yield collection, name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--collection", default=None, help="one collection, else all")
    parser.add_argument(
        "--firestore-project",
        default=None,
        help="defaults to FIRESTORE_PROJECT_ID / GCP_PROJECT_ID / GCP_PROJECT / "
        "GOOGLE_CLOUD_PROJECT, as the services resolve it",
    )
    parser.add_argument(
        "--firestore-database",
        default=_CATALOG_DATABASE_DEFAULT,
        help=f"the catalog's Firestore database (default: {_CATALOG_DATABASE_DEFAULT})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the derived `writes`. Without it nothing is changed.",
    )
    args = parser.parse_args()

    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    catalog = OpteryxCatalog(
        workspace=args.workspace,
        firestore_project=args.firestore_project or _default_project(),
        firestore_database=args.firestore_database,
    )

    # PREFLIGHT. `_task_doc_ref` is a private helper and this tool runs against
    # whichever opteryx_catalog wheel is installed, which is not always the one
    # in the sibling repo. Proved present BEFORE the sweep so a missing helper
    # is one clear message rather than a crash partway through, with some tasks
    # patched and no record of which.
    if not callable(getattr(catalog, "_task_doc_ref", None)):
        print(
            "This opteryx_catalog does not expose `_task_doc_ref`, so this tool "
            "cannot address a task document.\n"
            f"  in use: {OpteryxCatalog.__module__} from "
            f"{sys.modules[OpteryxCatalog.__module__].__file__}\n"
            "Install a newer opteryx_catalog, or run this from a checkout that "
            "has one.",
            file=sys.stderr,
        )
        return 2

    changed = 0
    agreed = 0
    skipped = 0

    for collection, name in _tasks(catalog, args.collection):
        identifier = f"{collection}.{name}"
        record = catalog.get_task(identifier)
        recorded = list(record.get("writes") or [])
        sql = record.get("sql")

        if not sql:
            print(f"SKIP    {identifier}: no statement recorded")
            skipped += 1
            continue

        try:
            derived = _derive_writes(sql)
        except Exception as exc:  # noqa: BLE001 - a finding, not a failure to hide
            print(f"SKIP    {identifier}: statement does not parse - {exc}")
            skipped += 1
            continue

        if derived == recorded:
            agreed += 1
            continue

        # A DIFFERENCE THAT IS NOT A BACKFILL. An empty recorded list is the
        # legacy case this exists for. A non-empty one that disagrees means the
        # record and the statement have diverged some other way - reported
        # loudly and left alone, because overwriting it would destroy the only
        # evidence of whatever caused it.
        if recorded:
            print(
                f"DIVERGED {identifier}: recorded {recorded}, statement writes "
                f"{derived} - left alone, investigate"
            )
            skipped += 1
            continue

        print(f"BACKFILL {identifier}: {derived}")
        changed += 1
        if args.apply:
            # `collection` and `name` come from the listing above, so nothing
            # here re-parses the identifier - the catalog helper that does that
            # is newer than some deployed wheels, and a sweep must not depend on
            # a private parsing helper when it already holds both parts.
            catalog._task_doc_ref(collection, name).update({"writes": derived})

    verb = "patched" if args.apply else "would patch"
    print(f"\n{verb} {changed}, already correct {agreed}, skipped {skipped}")
    if changed and not args.apply:
        print("Nothing was written. Re-run with --apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
