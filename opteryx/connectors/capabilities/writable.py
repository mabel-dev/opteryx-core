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

    Connectors mixing this in must implement all five methods below.
    The binder uses isinstance(connector, Writable) to gate CREATE / INSERT /
    DROP / TRUNCATE.
    """

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

    def insert(self, relation_name: str, file_entries: "List[FileEntry]") -> None:
        """Commit pre-written data files into a new snapshot.

        Stage 1: this method is implemented but not exercised end-to-end.
        Caller must have already written parquet files into the relation
        folder (Stage 2 provides the writer; Stage 1 tests construct
        FileEntry objects manually pointing at hand-placed files).

        Args:
            relation_name: Fully-qualified relation name
            file_entries: List of FileEntry objects to commit

        Raises:
            ValueError: If relation doesn't exist
            ConcurrentModificationError: If relation was modified concurrently
        """
        raise NotImplementedError
