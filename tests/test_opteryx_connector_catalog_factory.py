import pytest

from opteryx.connectors.opteryx_connector import OpteryxConnector


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


def test_bubbles_type_error_on_unexpected_kwargs():
    class DummyCatalog:
        def __init__(self, workspace=None):
            # does not accept unexpected kwargs like 'telemetry'
            self.workspace = workspace

    conn = OpteryxConnector(catalog=DummyCatalog, telemetry="not-allowed")
    with pytest.raises(TypeError):
        conn._get_catalog("ws")
