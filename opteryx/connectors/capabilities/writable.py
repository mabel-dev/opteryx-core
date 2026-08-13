# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Marker mixin for connectors that support DDL/DML operations.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class EgressRefusal:
    """One source workspace refusing to let a copy of its data leave it.

    The engine's own type, built at the connector boundary from whatever the
    backing store reports - so a store's representation never travels into the
    engine, the same way `ViewDefinition` and `Manifest` are the engine's.

    Carries the remediation separately from the message because a caller
    composing several refusals wants to list what has to change without
    repeating a full sentence per workspace.
    """

    workspace: str
    remediation: str
    message: str


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

    def add_column(
        self,
        relation_name: str,
        column_name: str,
        column_type: "ColumnType",
        nullable: bool = True,
        default: object = None,
        if_not_exists: bool = False,
        author: Optional[str] = None,
    ) -> None:
        """Add a column to a relation, without rewriting existing rows' data.

        `default` is a WRITE-TIME FILL VALUE, not a stored constraint: it is
        `None` (existing rows read back as NULL for the new column) or an
        already bind-time-validated literal (existing rows read back as that
        value), and nothing consults it again afterwards. Either way this is a
        metadata-scale change per row - a single repeated value, never one
        computed per row.

        Args:
            relation_name: Fully-qualified relation name
            column_name: Name of the new column
            column_type: Declared type of the new column
            nullable: Recorded on the column. Opteryx has no NULL constraints,
                so nothing enforces it - it is carried for the catalog's and
                readers' benefit only
            default: NULL, or the literal value existing rows should read back
            if_not_exists: If True, do not raise if the column already exists
            author: session user this change is attributed to (see create_relation)

        Raises:
            ValueError: If the relation doesn't exist, or the column already
                exists and if_not_exists is False
        """
        raise NotImplementedError

    def drop_column(
        self,
        relation_name: str,
        column_name: str,
        if_exists: bool = False,
        author: Optional[str] = None,
    ) -> None:
        """Drop a column from a relation, without rewriting other columns' data.

        Args:
            relation_name: Fully-qualified relation name
            column_name: Name of the column to remove
            if_exists: If True, do not raise if the column doesn't exist
            author: session user this change is attributed to (see create_relation)

        Raises:
            ValueError: If the relation doesn't exist, or the column doesn't
                exist and if_exists is False
        """
        raise NotImplementedError

    def rename_column(
        self,
        relation_name: str,
        old_column_name: str,
        new_column_name: str,
        author: Optional[str] = None,
    ) -> None:
        """Rename a column, without rewriting any column's data.

        The column keeps its identity (stable field-id, where the store has
        one) - only its name changes, so a value written under the old name
        reads back correctly under the new one.

        Args:
            relation_name: Fully-qualified relation name
            old_column_name: The column's current name
            new_column_name: The column's new name

        Raises:
            ValueError: If the relation doesn't exist, the source column
                doesn't exist, or the target name is already taken
        """
        raise NotImplementedError

    def alter_column_type(
        self,
        relation_name: str,
        column_name: str,
        new_type: "ColumnType",
        author: Optional[str] = None,
    ) -> None:
        """Widen a column's type, rewriting only that column's data.

        Callers guarantee `new_type` is already a bind-time-validated legal
        widening of the column's current type (see
        `opteryx.types.is_legal_widen`) - this method does not re-derive
        legality, only applies it. Every other column in every affected file
        is untouched.

        Args:
            relation_name: Fully-qualified relation name
            column_name: Name of the column to retype
            new_type: The new, wider type
            author: session user this change is attributed to (see create_relation)

        Raises:
            ValueError: If the relation or column doesn't exist
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

    def optimize_relation(self, relation_name: str, author: Optional[str] = None) -> bool:
        """Compact a relation's small data files into fewer, larger ones.

        Strategy (bin-pack vs. sort-aware) is auto-detected from whatever
        clustering the relation already declares (see set_cluster_by) - this
        call carries no strategy of its own.

        Args:
            relation_name: Fully-qualified relation name
            author: session user this compaction is attributed to (see create_relation)

        Returns:
            True if a new snapshot was committed, False if compaction declined
            (nothing cleared the size/count thresholds - not an error).
        """
        # Reachable for the same reason as set_cluster_by: a connector with no
        # catalog has no file layout to compact.
        raise NotImplementedError(f"{self.__class__.__name__} does not support OPTIMIZE")

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

    def materialized_view_sources(self, relation_name: str) -> List[str]:
        """The catalog tables a materialized view reads, as recorded at registration.

        Read from the record rather than recovered by re-planning the defining
        SELECT: the sources were resolved once, when the view was bound, and
        that list is what the view's refresh triggers were landed against.

        Needed wherever a view's sources must be judged without a plan in hand -
        `ALTER MATERIALIZED VIEW ... OWNER TO` has to know what the incoming
        owner would be refreshing before it pins them to it, and its statement
        has no SELECT subtree to walk.

        Args:
            relation_name: Fully-qualified name of the materialized view

        Raises:
            ValueError: If the relation is not a materialized view.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support ALTER MATERIALIZED VIEW ... OWNER TO"
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

    def set_materialized_view_suspended(
        self, relation_name: str, suspended: bool, author: str = None
    ) -> None:
        """Suspend or resume a materialized view's automatic refresh.

        The state belongs to the VIEW, not to the triggers that drive it. A view
        with four sources has four triggers, and suspending them individually could
        leave it refreshing from a subset of its sources - silently partial
        data. One flag cannot be partially applied.

        Args:
            relation_name: Fully-qualified name of the materialized view
            suspended: True to suspend refreshes, False to resume them
            author: session user this change is attributed to
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support ALTER MATERIALIZED VIEW ... SUSPEND"
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

    def egress_verdict(
        self, target_relation: str, source_relations: "List[str]"
    ) -> "List[EgressRefusal]":
        """Every workspace that refuses this write, without refusing it here.

        Called at bind time by the INSERT/CTAS path, before anything is written,
        with the relation being written and every catalog relation the statement
        reads. The caller decides what to do with the refusals - see
        `_enforce_egress` in the binder, which reports all of them at once so
        that clearing egress across several workspaces is not a sequence of
        failed statements.

        The ONLY shape of this decision. There is deliberately no enforcing
        sibling: two methods would let a connector implement one and leave the
        other answering permissively, and the permissive answer here is the
        dangerous one.

        **An empty list means allowed.** A connector that cannot reach the
        decision at all must raise rather than return `[]`: "I could not ask"
        and "nothing objected" are different answers, and only one of them is
        safe to act on.

        **Empty by default, and that is not a gap.** Egress protection is a
        boundary between workspaces, and a connector with no workspace concept -
        a filesystem, a single-store backend - has no boundary to cross, so
        "nothing refuses" is the correct and complete answer. It would be wrong
        to make this `NotImplementedError` like its neighbours above: those
        describe capabilities a connector may genuinely lack.

        Args:
            target_relation: Fully-qualified relation being written
            source_relations: Fully-qualified names of the catalog relations the
                statement reads, non-catalog sources already filtered out.
        """
        return []

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

    def relation_column_types(self, relation_name: str) -> "Dict[str, ColumnType]":
        """Return the relation's current column name -> ColumnType mapping.

        Used at bind time by `ALTER COLUMN ... TYPE` to check the requested
        change against the column's actual current type before anything is
        written - the same "ask the connector, don't assume" posture as
        `relation_column_names`, just with the type fidelity that call
        deliberately drops.

        Args:
            relation_name: Fully-qualified relation name

        Raises:
            ValueError: If relation doesn't exist
        """
        raise NotImplementedError


def build_column_donor(column_name: str, column_type: "ColumnType", value: object) -> bytes:
    """Build the one-row parquet file that describes a column being ADDed.

    `rugo.parquet.patch_columns(..., add=[...])` takes a new column's parquet
    annotation from a DONOR file rather than from a DrakenType -> parquet
    mapping of its own. This builds that donor: a single-column, single-row
    file holding `value` (or a NULL row when `value` is None), written
    uncompressed and without a dictionary so the patcher can lift the value
    straight out of the page.

    Going the long way round - through the real write path and back out of the
    file it produced - is deliberate. It means an ADDed column is annotated by
    exactly the code that would have written that same column in a CTAS, so the
    two cannot drift into disagreeing about widths, signedness, decimal
    precision/scale, or timestamp units.

    A literal the declared type cannot hold raises out of Draken's ingestion,
    which is the right place for it to fail: the value never reaches a file.

    Args:
        column_name: Name the new column will carry
        column_type: Declared type of the new column
        value: The fill value for existing rows, or None for NULL

    Returns:
        The donor parquet file's bytes.
    """
    from decimal import Decimal

    import rugo.parquet as _rugo_parquet
    from draken import draken_native as _draken
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    from draken.draken_native import LogicalKind

    # The canonical TimestampUnit spelling, not a second copy of it - draken's
    # sequence constructors take the same "s"/"ms"/"us"/"ns" strings the SQL
    # surface uses.
    from opteryx.types.logical_type import _UNIT_TO_SQL

    physical = column_type.physical
    logical = column_type.logical
    values = [value]

    if logical is not None and logical.kind == LogicalKind.DECIMAL:
        # SQL exact-numeric literals bind as float64, so the value arriving here
        # is typically a float; Decimal(str(...)) reads the digits the user
        # wrote rather than the binary approximation of them.
        if value is not None and not isinstance(value, Decimal):
            values = [Decimal(str(value))]
        builder = (
            _draken.vector_decimal128_from_sequence
            if physical == _draken.DrakenType.DECIMAL128
            else _draken.vector_decimal_from_sequence
        )
        vector = builder(values, logical.precision, logical.scale)
    elif logical is not None and logical.kind == LogicalKind.TIMESTAMP:
        vector = _draken.vector_timestamp_from_sequence(
            values, _UNIT_TO_SQL[logical.unit], logical.offset_minutes
        )
    elif logical is not None and logical.kind == LogicalKind.TIME:
        builder = (
            _draken.vector_time32_from_sequence
            if physical == _draken.DrakenType.TIME32
            else _draken.vector_time64_from_sequence
        )
        vector = builder(values, _UNIT_TO_SQL[logical.unit])
    else:
        # Everything else is fully described by its physical type. IPV4 rides a
        # plain UINT32 here on purpose: parquet has no notion of it, and the
        # descriptor lives on the relation's schema, which the connector writes.
        vector = vector_from_sequence(values, dtype=physical)

    morsel = Morsel.from_vectors([column_name], [vector])
    return _rugo_parquet.write_parquet(
        morsel, compression="none", dictionary=False, bloom_filters=False
    )
