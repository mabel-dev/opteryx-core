import dataclasses
from typing import TYPE_CHECKING
from typing import Optional

if TYPE_CHECKING:  # pragma: no cover - typing only
    from opteryx.types.schema import RelationSchema


@dataclasses.dataclass
class ViewDefinition:
    name: str
    statement: str
    owner: Optional[str] = None
    last_row_count: Optional[int] = None
    description: Optional[str] = None
    describer: Optional[str] = None
    # The view's output columns, named and typed, as the binder resolved them
    # when the definition was stored. None only for a view recorded before
    # schemas were kept. Metadata: a view is always expanded by re-planning
    # `statement`, so this describes the view but never determines it - and for
    # a definition with a wildcard it is a snapshot that the source gaining a
    # column leaves stale.
    schema: Optional["RelationSchema"] = None


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

    def create_view(
        self,
        view_name: str,
        statement: str,
        update_if_exists: bool = False,
        owner: Optional[str] = None,
        schema: Optional["RelationSchema"] = None,
    ):
        """Create a new view with the given name and definition."""
        # Placeholder implementation; actual implementation would add
        # the view to the connector's metadata.
        raise NotImplementedError("create_view method must be implemented by subclasses.")

    def drop_view(self, view_name, author: Optional[str] = None):
        """Drop the specified view.

        `author` is the session user the drop is attributed to; a catalog that
        records or announces who dropped a view needs it.
        """
        # Placeholder implementation; actual implementation would remove
        # the view from the connector's metadata.
        raise NotImplementedError("drop_view method must be implemented by subclasses.")
