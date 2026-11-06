import pytest

from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.connectors.opteryx_connector import OpteryxTable
from opteryx.types import LogicalCategory
from opteryx.types.logical_type import VARCHAR
from opteryx.types.schema import RelationDescriptor


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
            # does not accept unexpected kwargs (telemetry is the connector's own,
            # never forwarded to the catalog, so it cannot be the probe here)
            self.workspace = workspace

    conn = OpteryxConnector(catalog=DummyCatalog, unexpected_option="not-allowed")
    with pytest.raises(TypeError):
        conn._get_catalog("ws")


def test_normalize_external_schema_to_relation_descriptor():
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

    assert isinstance(schema, RelationDescriptor)
    assert schema.name == "public.github.events"
    assert [c.name for c in schema.columns] == ["id", "tags", "payload"]
    assert schema.columns[0].category == LogicalCategory.INTEGER
    assert schema.columns[1].category == LogicalCategory.ARRAY
    assert schema.columns[1].column_type.element == VARCHAR
    # JSONB resolves to NVARCHAR under the current type vocabulary
    # (_SQL_NAME_ALIASES["JSONB"] -> NVARCHAR; there is no JSONB physical type).
    assert schema.columns[2].category == LogicalCategory.NVARCHAR


def test_cached_catalog_is_reverified_unless_trusted():
    """A cache hit re-reads `$properties` to notice a dropped workspace - except under
    `trust_cached_catalogs`, which the edit-time check opts into."""
    from opteryx.connectors import trust_cached_catalogs

    class DummyCatalog:
        reads = 0

        def __init__(self, workspace=None, **kwargs):
            pass

        def get_workspace_properties(self):
            type(self).reads += 1
            return {"name": "ws"}

    conn = OpteryxConnector(catalog=DummyCatalog)
    first = conn._get_catalog("ws3")
    assert DummyCatalog.reads == 0  # built, not re-verified

    assert conn._get_catalog("ws3") is first
    assert DummyCatalog.reads == 1  # default: every hit is re-verified

    with trust_cached_catalogs():
        assert conn._get_catalog("ws3") is first
    assert DummyCatalog.reads == 1  # trusted: no read

    assert conn._get_catalog("ws3") is first
    assert DummyCatalog.reads == 2  # and the opt-out does not leak past the block


def _counting_catalog():
    class CountingCatalog:
        reads = 0
        deleted = False

        def __init__(self, workspace=None, **kwargs):
            pass

        def get_workspace_properties(self):
            type(self).reads += 1
            return {"deleted-at-ms": 123 if type(self).deleted else None}

    return CountingCatalog


def test_trust_window_is_bounded_by_max_age(monkeypatch):
    """With a `max_age`, a cached handle is trusted until that long after it last passed
    the existence check, then verified once and trusted again."""
    import opteryx.connectors.opteryx_connector as oc
    from opteryx.connectors import trust_cached_catalogs

    now = [1000.0]
    monkeypatch.setattr(oc.time, "monotonic", lambda: now[0])

    Catalog = _counting_catalog()
    conn = OpteryxConnector(catalog=Catalog)
    first = conn._get_catalog("ws")  # built through the gate at t=1000

    with trust_cached_catalogs(5.0):
        now[0] = 1003.0
        assert conn._get_catalog("ws") is first
        assert Catalog.reads == 0  # inside the window

        now[0] = 1006.0
        assert conn._get_catalog("ws") is first
        assert Catalog.reads == 1  # window passed: one re-check...

        now[0] = 1008.0
        assert conn._get_catalog("ws") is first
        assert Catalog.reads == 1  # ...which starts a new window


def test_dropped_workspace_is_noticed_once_window_passes_and_never_when_untrusted(monkeypatch):
    import opteryx.connectors.opteryx_connector as oc
    from opteryx.connectors import trust_cached_catalogs

    now = [0.0]
    monkeypatch.setattr(oc.time, "monotonic", lambda: now[0])

    Catalog = _counting_catalog()
    conn = OpteryxConnector(catalog=Catalog)
    first = conn._get_catalog("ws")
    Catalog.deleted = True

    # Outside any trust scope (DDL, writes, execution): noticed on the very next use.
    assert conn._get_catalog("ws") is not first

    # Inside the window the drop is not yet noticed; after it, it is.
    Catalog.deleted = False
    rebuilt = conn._get_catalog("ws")
    Catalog.deleted = True
    with trust_cached_catalogs(5.0):
        now[0] = 2.0
        assert conn._get_catalog("ws") is rebuilt
        now[0] = 9.0
        assert conn._get_catalog("ws") is not rebuilt


def test_failed_properties_read_does_not_start_a_trust_window(monkeypatch):
    import opteryx.connectors.opteryx_connector as oc
    from opteryx.connectors import trust_cached_catalogs

    now = [0.0]
    monkeypatch.setattr(oc.time, "monotonic", lambda: now[0])

    class Flaky:
        reads = 0

        def __init__(self, workspace=None, **kwargs):
            pass

        def get_workspace_properties(self):
            type(self).reads += 1
            raise RuntimeError("firestore unavailable")

    conn = OpteryxConnector(catalog=Flaky)
    first = conn._get_catalog("ws")
    with trust_cached_catalogs(5.0):
        now[0] = 6.0
        assert conn._get_catalog("ws") is first  # kept: a blip is not a drop
        assert Flaky.reads == 1
        assert conn._get_catalog("ws") is first
        assert Flaky.reads == 2  # but the blip bought no window: it looked again


def test_zero_max_age_never_trusts():
    from opteryx.connectors import trust_cached_catalogs

    Catalog = _counting_catalog()
    conn = OpteryxConnector(catalog=Catalog)
    conn._get_catalog("ws")
    with trust_cached_catalogs(0):
        conn._get_catalog("ws")
        conn._get_catalog("ws")
    assert Catalog.reads == 2


def test_query_planner_opens_the_trust_window(monkeypatch):
    """Planning is the caller that wants the window; it takes its length from config."""
    from opteryx import config
    from opteryx import connectors
    import opteryx.planner as planner

    seen = []
    original = connectors.trust_cached_catalogs

    from contextlib import contextmanager

    @contextmanager
    def spy(max_age=float("inf")):
        seen.append(max_age)
        with original(max_age):
            yield

    monkeypatch.setattr(connectors, "trust_cached_catalogs", spy)
    monkeypatch.setattr(config, "CATALOG_LIVENESS_RECHECK_SECONDS", 7.5)
    try:
        planner.query_planner(
            operation="SELECT 1 AS one",
            parameters=None,
            visibility_filters=None,
            execution_context=type("C", (), {"variables": None})(),
            query_id="q",
            telemetry=None,
        )
    except Exception:
        pass  # planning itself needs a real context; only the scope is under test
    assert seen and seen[0] == 7.5
