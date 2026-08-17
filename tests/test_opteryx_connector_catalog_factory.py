import pytest

from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.types import LogicalCategory
from opteryx.types.logical_type import VARCHAR
from opteryx.types.schema import RelationSchema


def test_instantiates_class_catalog():
    class DummyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace

        def load_dataset(self, identifier):
            return f"loaded:{identifier}"

    conn = OpteryxConnector(catalog=DummyCatalog)
    cat = conn._get_catalog("ws1")
    assert isinstance(cat, DummyCatalog)
    assert cat.load_dataset("a.b") == "loaded:a.b"


def test_callable_factory():
    class DummyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace

    def factory(workspace=None, **kwargs):
        return DummyCatalog(workspace=workspace)

    conn = OpteryxConnector(catalog=factory)
    cat = conn._get_catalog("ws2")
    assert isinstance(cat, DummyCatalog)


def test_instance_passed_through_and_cached():
    class DummyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace

    inst = DummyCatalog(workspace="pre")
    conn = OpteryxConnector(catalog=inst)
    cat = conn._get_catalog("any")
    assert cat is inst
    cat2 = conn._get_catalog("any")
    assert cat2 is inst


def test_cache_per_catalog_name():
    class DummyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace

    conn = OpteryxConnector(catalog=DummyCatalog)
    a = conn._get_catalog("x")
    b = conn._get_catalog("x")
    c = conn._get_catalog("y")
    assert a is b
    assert a is not c


def test_cache_hit_evicts_and_rebuilds_when_deleted_since_caching():
    """The connector (and the module-level cache handing it out) is
    process-long-lived, so a workspace soft-deleted after its first query
    must not stay queryable forever from a stale cache entry - see the
    comment in OpteryxConnector._get_catalog."""

    class DummyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self._deleted = False

        def get_workspace_properties(self):
            return {"deleted-at-ms": 123 if self._deleted else None}

    instances = []

    def factory(workspace=None, **kwargs):
        inst = DummyCatalog(workspace=workspace)
        instances.append(inst)
        return inst

    conn = OpteryxConnector(catalog=factory)
    first = conn._get_catalog("ws")
    assert first is instances[0]

    # Still live: a second call is a genuine cache hit, no reconstruction.
    again = conn._get_catalog("ws")
    assert again is first
    assert len(instances) == 1

    # Soft-deleted between queries against an already-warm connector.
    first._deleted = True
    refreshed = conn._get_catalog("ws")
    assert refreshed is not first
    assert len(instances) == 2


def test_cache_hit_survives_transient_properties_read_failure():
    """A Firestore hiccup on the cheap re-check must not evict (and force a
    full reconstruction of) an otherwise-healthy cached catalog handle."""

    class FlakyCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace

        def get_workspace_properties(self):
            raise RuntimeError("firestore unavailable")

    conn = OpteryxConnector(catalog=FlakyCatalog)
    first = conn._get_catalog("ws")
    again = conn._get_catalog("ws")
    assert again is first


def test_bubbles_type_error_on_unexpected_kwargs():
    class DummyCatalog:
        def __init__(self, workspace=None):
            # does not accept unexpected kwargs like 'telemetry'
            self.workspace = workspace

    conn = OpteryxConnector(catalog=DummyCatalog, telemetry="not-allowed")
    with pytest.raises(TypeError):
        conn._get_catalog("ws")


def test_normalize_external_schema_to_internal_relationschema():
    class ExternalColumn:
        def __init__(self, name, type_name, element_type=None):
            self.name = name
            self.type = type_name
            self.element_type = element_type
            self.nullable = True
            self.precision = None
            self.scale = None

    class ExternalSchema:
        def __init__(self):
            self.name = "external"
            self.columns = [
                ExternalColumn("id", "INTEGER"),
                ExternalColumn("tags", "ARRAY", element_type="VARCHAR"),
                {"name": "payload", "type": "JSONB"},
            ]

    schema = OpteryxTable._normalize_schema(ExternalSchema(), relation_name="public.github.events")

    assert isinstance(schema, RelationSchema)
    assert schema.name == "public.github.events"
    assert [c.name for c in schema.columns] == ["id", "tags", "payload"]
    assert schema.columns[0].category == LogicalCategory.INTEGER
    assert schema.columns[1].category == LogicalCategory.ARRAY
    assert schema.columns[1].column_type.element == VARCHAR
    # JSONB resolves to NVARCHAR under the current type vocabulary
    # (_SQL_NAME_ALIASES["JSONB"] -> NVARCHAR; there is no JSONB physical type).
    assert schema.columns[2].category == LogicalCategory.NVARCHAR
