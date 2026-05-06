# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
In-memory representation of dataset.json metadata.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from opteryx.types.schema import RelationSchema


@dataclass
class DatasetDescriptor:
    """In-memory representation of dataset metadata.

    This is serialized to/from dataset.json on disk.

    Attributes:
        format_version: Metadata format version (currently 1)
        relation_name: Fully-qualified relation name (e.g., "schema.sub.events")
        schema: RelationSchema defining the table structure
        current_snapshot: Filename of current snapshot, or None for empty relation
        created_at: ISO 8601 UTC timestamp when relation was created
    """

    format_version: int
    relation_name: str
    schema: RelationSchema
    current_snapshot: Optional[str]
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "format_version": self.format_version,
            "relation_name": self.relation_name,
            "schema": self.schema.to_dict(),
            "current_snapshot": self.current_snapshot,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetDescriptor":
        """Create from dictionary (from JSON deserialization)."""
        data = data.copy()
        # Convert schema dict to RelationSchema if needed
        if isinstance(data.get("schema"), dict):
            data["schema"] = RelationSchema.from_dict(data["schema"])
        return cls(**data)
