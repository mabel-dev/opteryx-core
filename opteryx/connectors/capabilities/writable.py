# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Marker mixin for connectors that support DDL/DML operations.
"""

from typing import List, Optional


class Writable:
    """Marker mixin: connector supports DDL/DML.

    Connectors mixing this in must implement all methods below.
    The binder uses isinstance(connector, Writable) to gate CREATE / INSERT /
    DROP / TRUNCATE / CREATE OR REPLACE.
    """

    # Whether `replace_relation` can change the relation's schema, not just its
    # data. False by default: most write targets have no schema-evolution
    # primitive to fall back on (e.g. the catalog connector - see
    # relation_column_names), so REPLACE with a differently-shaped SELECT is
    # rejected at bind time rather than failing partway through a write.
    supports_schema_evolution_on_replace: bool = False

    def create_relation(
        self, relation_name: str, schema: "RelationSchema", author: Optional[str] = None
    ) -> None:
        """Create a new relation (table) with the given schema.

        Args:
            relation_name: Fully-qualified relation name (e.g., "schema.table")
            schema: RelationSchema defining the table structure
            author: session user this creation is attributed to. A store with no
                attribution concept ignores it; a store that requires one rejects
                None rather than inventing an identity.

        Raises:
            ValueError: If relation already exists
        """
        raise NotImplementedError

    def drop_relation(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a relation.

        Args:
            relation_name: Fully-qualified relation name
            if_exists: If True, do not raise error if relation doesn't exist
            author: session user the drop is attributed to. A catalog that
                tombstones the dropped relation records this as the dropper.

        Raises:
            ValueError: If relation doesn't exist and if_exists is False
        """
        raise NotImplementedError

    def truncate_relation(self, relation_name: str, author: Optional[str] = None) -> None:
        """Remove all rows from a relation.

        Args:
            relation_name: Fully-qualified relation name
            author: session user this truncation is attributed to (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
        """
        raise NotImplementedError

    def relation_exists(self, relation_name: str) -> bool:
        """Check if a relation exists.

        Args:
            relation_name: Fully-qualified relation name

        Returns:
            True if relation exists, False otherwise
        """
        raise NotImplementedError

    def write_morsel(self, relation_name: str, morsel) -> "FileEntry":
        """Write a single morsel as one data file, wherever this connector's
        relations live, and return a FileEntry describing it.

        Called once per morsel, before the relation is created/replaced in
        the catalog (see `create_relation`/`replace_relation`) - a connector's
        write target for this must not depend on the relation already being
        registered there.

        Args:
            relation_name: Fully-qualified relation name
            morsel: Draken Morsel to write

        Returns:
            FileEntry describing the written file
        """
        raise NotImplementedError

    def insert(
        self,
        relation_name: str,
        file_entries: "List[FileEntry]",
        author: Optional[str] = None,
    ) -> None:
        """Commit pre-written data files into a new snapshot, appending to
        whatever the relation already contains.

        Caller must have already written parquet files into the relation
        folder.

        Args:
            relation_name: Fully-qualified relation name
            file_entries: List of FileEntry objects to commit
            author: session user this append is attributed to (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        raise NotImplementedError

    def replace_relation(
        self,
        relation_name: str,
        schema: "RelationSchema",
        file_entries: "List[FileEntry]",
        author: Optional[str] = None,
    ) -> None:
        """Atomically replace all of a relation's data with the given files,
        as a single new snapshot (CREATE OR REPLACE ... AS SELECT).

        `schema` must match the relation's current schema - this does not
        evolve schema. Never deletes the old data files; the old snapshot's
        files are simply no longer referenced by the new snapshot (the same
        lineage-preserving model `insert`'s append uses, not DROP's tombstone
        mechanism).

        Args:
            relation_name: Fully-qualified relation name
            schema: RelationSchema the new data conforms to (unchanged from current)
            file_entries: List of FileEntry objects that become the relation's entire contents
            author: session user this replace is attributed to (see create_relation)

        Raises:
            ValueError: If relation doesn't exist
        """
        raise NotImplementedError

    def is_materialized_view(self, relation_name: str) -> bool:
        """Whether the named relation is the backing table of a materialized view.

        False by default: most write targets have no materialized-view concept,
        and DROP TABLE's type guard must be able to ask this of any Writable
        connector without an error.

        Args:
            relation_name: Fully-qualified relation name

        Returns:
            True if the relation is a materialized view, False otherwise
        """
        return False

    def register_materialized_view(
        self,
        relation_name: str,
        sql: str,
        source_tables: "List[str]",
        author: Optional[str] = None,
    ) -> None:
        """Register an already-created relation as a materialized view.

        Called at the end of the CTAS write path (the backing table and its
        data already exist) with the defining SELECT as text and the catalog
        tables it reads - a refresh trigger lands on each source.

        Args:
            relation_name: Fully-qualified name of the (existing) backing table
            sql: the defining SELECT, as executable text
            source_tables: fully-qualified names of every catalog table the SELECT reads
            author: session user the registration is attributed to

        Raises:
            ValueError: If the backing table doesn't exist or a source is invalid
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support CREATE MATERIALIZED VIEW"
        )

    def drop_materialized_view(
        self, relation_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop a materialized view: its refresh triggers, then its backing table.

        Args:
            relation_name: Fully-qualified relation name
            if_exists: If True, do not raise error if the relation doesn't exist
            author: session user the drop is attributed to

        Raises:
            ValueError: If the relation doesn't exist (and if_exists is False),
                or is not a materialized view
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support DROP MATERIALIZED VIEW"
        )

    def drop_trigger(
        self,
        relation_name: str,
        trigger_name: str,
        author: Optional[str] = None,
        missing_ok: bool = False,
    ) -> None:
        """Remove a trigger from the relation that carries it.

        Dropping a materialized view's refresh trigger orphans the view: it
        stays queryable but stops refreshing. That is the supported way to
        pause an MV; `information_schema.triggers` is where the absence shows.

        Args:
            relation_name: Fully-qualified name of the relation carrying the trigger
            trigger_name: Name of the trigger to remove
            author: session user the drop is attributed to
            missing_ok: If True, a missing trigger is not an error

        Raises:
            ValueError: If the trigger doesn't exist (and missing_ok is False)
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support DROP TRIGGER"
        )

    def list_triggers(self, relation_name: str) -> "List[dict]":
        """The triggers attached to a relation, as plain dicts (catalog field
        names: name, kind, target-view, statement-id, created-by,
        created-at-ms, last-fired-at-ms, last-fired-status).

        Empty by default: most write targets have no trigger concept.
        """
        return []

    def collection_exists(self, collection_name: str) -> bool:
        """Check if a collection exists.

        Args:
            collection_name: Fully-qualified collection name (e.g. "workspace.collection")

        Returns:
            True if the collection exists, False otherwise
        """
        raise NotImplementedError

    def create_collection(
        self, collection_name: str, if_not_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Create a collection.

        A collection owns no storage of its own - it is a namespace between a
        workspace and its relations - so this registers it and nothing more.
        Creating one is not a precondition for creating relations in it: a
        relation created in an unknown collection brings the collection into
        existence. This statement exists so a collection can be made ahead of
        its first relation, and so DROP COLLECTION has a counterpart.

        Args:
            collection_name: Fully-qualified collection name (e.g. "workspace.collection")
            if_not_exists: If True, an already-existing collection is not an error
            author: session user the creation is attributed to

        Raises:
            ValueError: If the collection already exists and if_not_exists is False
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support CREATE COLLECTION"
        )

    def drop_collection(
        self, collection_name: str, if_exists: bool = False, author: Optional[str] = None
    ) -> None:
        """Drop an empty collection.

        Args:
            collection_name: Fully-qualified collection name (e.g. "workspace.collection")
            if_exists: If True, do not raise error if collection doesn't exist
            author: session user the drop is attributed to

        Raises:
            ValueError: If collection doesn't exist and if_exists is False
            CollectionNotEmptyError: If the collection still contains datasets or views
        """
        raise NotImplementedError

    def rename_relation(
        self, relation_name: str, new_relation_name: str, author: Optional[str] = None
    ) -> None:
        """Rename a relation, keeping its data, schema and history.

        The two names always share a workspace (the logical planner rejects a
        cross-workspace rename), so a single connector handles both ends. The
        collection may differ - a rename doubles as a move between collections.
        Callers guarantee the source exists and the target does not.

        Args:
            relation_name: Fully-qualified current name
            new_relation_name: Fully-qualified new name, same workspace
            author: session user this rename is attributed to (see create_relation)

        Raises:
            ValueError: If the source doesn't exist or the target already does
        """
        raise NotImplementedError

    def set_cluster_by(
        self, relation_name: str, cluster_columns: "List[str]", author: Optional[str] = None
    ) -> None:
        """Set the columns a relation should be sorted/clustered by.

        Declares intent for future compaction; it does not itself reorder any
        existing data files. Replaces the relation's entire clustering
        configuration rather than adding to it.

        Args:
            relation_name: Fully-qualified relation name
            cluster_columns: Clustering columns, in priority order
            author: session user this change is attributed to (see create_relation)
        """
        # Unlike the methods above, this default is reachable: a Writable
        # connector with no catalog (e.g. LocalStoreConnector) has nowhere to
        # persist a sort order, and must say so rather than silently doing
        # nothing. The message is part of the contract - callers match on it.
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support ALTER TABLE ... CLUSTER BY"
        )

    def set_comment(self, object_name: str, comment: str, describer: Optional[str] = None) -> None:
        """Store a descriptive comment against a relation or view.

        Args:
            object_name: Fully-qualified relation or view name
            comment: The comment text
            describer: session user the description is attributed to
        """
        # Reachable for the same reason as set_cluster_by: a connector with no
        # catalog has nowhere to store a comment.
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support updating comments"
        )

    def set_workspace_property(
        self, workspace_name: str, property_name: str, value, author: Optional[str] = None
    ) -> None:
        """Set a property on the workspace itself, not on a relation within it.

        The property name has already been checked against Opteryx's supported
        set and its value coerced to the declared type (see
        logical_planner.WORKSPACE_PROPERTIES), so a connector receives only
        names it is expected to understand.

        Args:
            workspace_name: The workspace whose property is being set
            property_name: The property to set, e.g. "deletion_protection"
            value: The already-typed value to store
            author: session user this change is attributed to (see create_relation)
        """
        raise NotImplementedError

    def materialized_view_definition(self, relation_name: str) -> str:
        """The SELECT a materialized view is defined by, as executable text.

        `REFRESH MATERIALIZED VIEW` is planned by re-running this SELECT into
        the view's backing table, so the definition has to be readable at plan
        time. Read fresh rather than carried on the statement: a refresh runs
        the view's *current* definition, which is what makes redefining a view
        take effect on its next refresh rather than at some later moment nobody
        can name.

        Args:
            relation_name: Fully-qualified name of the materialized view

        Raises:
            ValueError: If the relation is not a materialized view, or is one
                with no recorded defining SQL.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support REFRESH MATERIALIZED VIEW"
        )

    def set_materialized_view_owner(
        self, relation_name: str, new_owner: str, author: str = None
    ) -> None:
        """Repoint the identity a materialized view's refresh runs as.

        The only thing that moves a view's pinned owner - redefining a view
        records a new statement author but deliberately leaves this alone, so
        that editing someone's view does not silently make you responsible for
        it (nor hand your authority to whoever edits next).

        Args:
            relation_name: Fully-qualified name of the materialized view
            new_owner: The principal refreshes should run as
            author: session user this change is attributed to
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support ALTER MATERIALIZED VIEW ... OWNER TO"
        )

    def mark_materialized_view_refreshed(
        self, relation_name: str, status: str, author: str = None
    ) -> None:
        """Record that a materialized view's refresh completed.

        Called by the INSERT operator once a `REFRESH MATERIALIZED VIEW` write
        has committed, so a manual refresh stamps its own state. The failure
        path cannot be recorded from here for the obvious reason - a refresh
        that raised never reaches this call - so failures are stamped from
        outside the query, by the worker that ran it.

        Args:
            relation_name: Fully-qualified name of the materialized view
            status: The outcome to record, e.g. "succeeded"
            author: session user this refresh is attributed to
        """
        raise NotImplementedError

    def enforce_egress_policy(
        self, target_relation: str, source_relations: "List[str]"
    ) -> None:
        """Refuse a write that would copy data out of a protected workspace.

        Called at bind time by the INSERT/CTAS path, before anything is
        written, with the relation being written and every catalog relation the
        statement reads. A connector that recognises a workspace boundary
        between them raises; one that does not, returns.

        **No-op by default, and that is not a gap.** Egress protection is a
        boundary between workspaces, and a connector with no workspace concept
        - a filesystem, a single-store backend - has no boundary to cross. It
        would be wrong to make this `NotImplementedError` like its neighbours
        above: those describe capabilities a connector may genuinely lack,
        whereas here "nothing to check" is the correct and complete answer.

        Args:
            target_relation: Fully-qualified relation being written
            source_relations: Fully-qualified names of the catalog relations the
                statement reads. Non-catalog sources ($planets,
                information_schema, files) are filtered out before this call -
                they belong to no workspace and so cannot leave one.

        Raises:
            EgressRestrictedError: If the write would copy data out of a
                workspace whose `egress_protection` is on.
        """
        return None

    def relation_column_names(self, relation_name: str) -> "List[str]":
        """Return the relation's current column names only (not full type
        fidelity) - used to detect a schema-changing CREATE OR REPLACE before
        any data is written, since no schema-evolution primitive exists yet.

        Args:
            relation_name: Fully-qualified relation name

        Raises:
            ValueError: If relation doesn't exist
        """
        raise NotImplementedError
