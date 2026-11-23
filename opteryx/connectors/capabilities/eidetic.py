import dataclasses
from typing import TYPE_CHECKING
from typing import Optional

if TYPE_CHECKING:  # pragma: no cover - typing only
    from opteryx.types.schema import RelationDescriptor


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
    schema: Optional["RelationDescriptor"] = None


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
        schema: Optional["RelationDescriptor"] = None,
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

    # Property graphs (SQL/PGQ) are catalog text in the same namespace as views:
    # a graph is a canonical CREATE PROPERTY GRAPH statement and nothing else
    # (docs/PROPERTY_GRAPH_DESIGN.md). Unlike the view methods these refuse in
    # the base rather than raise NotImplementedError: a catalog that has not
    # added graphs yet is a real, reachable state, and the reader should be told
    # so rather than shown an internal error.

    def get_property_graph(self, graph_name: str) -> "PropertyGraphDefinition":
        """Retrieve the definition of the specified property graph."""
        raise _no_property_graphs(self)

    def create_property_graph(
        self,
        graph_name: str,
        statement: str,
        update_if_exists: bool = False,
        owner: Optional[str] = None,
        vertex_tables: Optional[list] = None,
        edge_tables: Optional[list] = None,
    ):
        """Create (or, with `update_if_exists`, replace) a property graph.

        `vertex_tables` / `edge_tables` are the bound element tables the text was
        rendered from (binder/property_graph.py `_Element.rendered()`). A store
        may keep them for readers that draw the graph; the text stays the
        definition, and a store that ignores them loses nothing it needs.
        """
        raise _no_property_graphs(self)

    def drop_property_graph(self, graph_name: str, author: Optional[str] = None):
        """Drop the specified property graph."""
        raise _no_property_graphs(self)


@dataclasses.dataclass
class PropertyGraphDefinition:
    name: str
    # The canonical CREATE PROPERTY GRAPH text: every element, key, label and
    # property written out, so a reader of it never needs a schema.
    statement: str
    owner: Optional[str] = None
    description: Optional[str] = None


def _no_property_graphs(store) -> Exception:
    from opteryx.exceptions import UnsupportedSyntaxError

    return UnsupportedSyntaxError(
        f"The catalog behind this workspace ({type(store).__name__}) cannot store "
        "property graphs yet."
    )
