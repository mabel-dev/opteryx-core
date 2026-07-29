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
