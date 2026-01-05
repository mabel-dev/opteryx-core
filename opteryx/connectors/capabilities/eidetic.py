import dataclasses
from typing import Optional


@dataclasses.dataclass
class ViewDefinition:
    name: str
    statement: str
    owner: Optional[str] = None
    last_row_count: Optional[int] = None
    description: Optional[str] = None
    describer: Optional[str] = None


class Eidetic:
    """Capability for connectors that are Eidetic (views)."""

    eidetic = True

    def __init__(self, **kwargs):
        pass

    def get_view(self, view_name) -> ViewDefinition:
        """Retrieve the definition of the specified view."""
        # Placeholder implementation; actual implementation would retrieve
        # the view definition from the connector's metadata.
        raise NotImplementedError("get_view method must be implemented by subclasses.")

    def list_views(self, prefix: Optional[str] = None) -> list[ViewDefinition]:
        """List all available views in the specified catalog and schema."""
        # Placeholder implementation; actual implementation would query
        # the connector's metadata for available views.
        raise NotImplementedError("list_views method must be implemented by subclasses.")

    def create_view(self, view_name: str, statement: str, owner: Optional[str] = None):
        """Create a new view with the given name and definition."""
        # Placeholder implementation; actual implementation would add
        # the view to the connector's metadata.
        raise NotImplementedError("create_view method must be implemented by subclasses.")

    def drop_view(self, view_name):
        """Drop the specified view."""
        # Placeholder implementation; actual implementation would remove
        # the view from the connector's metadata.
        raise NotImplementedError("drop_view method must be implemented by subclasses.")
