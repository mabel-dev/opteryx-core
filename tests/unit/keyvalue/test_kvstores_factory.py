from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores import FileKeyValueStore, S3KeyValueStore, GCSKeyValueStore
from opteryx.managers.kvstores import LayeredKeyValueStore, MemoryPoolKeyValueStore
from opteryx.managers.kvstores import ScopedKeyValueStore
from opteryx.exceptions import MissingDependencyError
import opteryx.config as opteryx_config
import pytest


_CTX = {"query_id": "q1", "operator_id": "op1"}


def _inner_store(store):
    assert isinstance(store, ScopedKeyValueStore)
    return store._store


def test_create_kv_store_detects_file_scheme(tmp_path):
    s = str(tmp_path)
    store = create_kv_store(s)
    assert isinstance(_inner_store(store), FileKeyValueStore)

    store2 = create_kv_store(f"file://{s}")
    assert isinstance(_inner_store(store2), FileKeyValueStore)


def test_create_kv_store_detects_s3_scheme():
    try:
        store = create_kv_store("s3://mybucket/prefix")
        assert isinstance(_inner_store(store), S3KeyValueStore)
    except MissingDependencyError:
        # acceptable if boto3 is not installed in the test environment
        pass


def test_create_kv_store_detects_gs_scheme():
    try:
        store = create_kv_store("gs://mybucket/prefix")
        assert isinstance(_inner_store(store), GCSKeyValueStore)
    except MissingDependencyError:
        # acceptable if google-cloud deps are not installed
        pass


def test_create_kv_store_detects_memory_scheme():
    store = create_kv_store("memory://test-factory-memory?pool_size_bytes=1024")
    assert isinstance(_inner_store(store), MemoryPoolKeyValueStore)


def test_create_kv_store_detects_layered_string(tmp_path):
    first = "memory://test-factory-layered?pool_size_bytes=128&max_bytes=8"
    second = f"file://{tmp_path / 'layer2'}"
    store = create_kv_store(f"{first};{second}")
    assert isinstance(_inner_store(store), LayeredKeyValueStore)


def test_create_kv_store_uses_configured_location(tmp_path, monkeypatch):
    monkeypatch.setattr(opteryx_config, "KVSTORE_LAYERS", [], raising=False)
    monkeypatch.setattr(opteryx_config, "KVSTORE_LOCATION", f"file://{tmp_path / 'cfg'}", raising=False)
    monkeypatch.setattr(opteryx_config, "KVSTORE_KEY_PREFIX", "cfg", raising=False)

    store = create_kv_store(None)
    assert isinstance(_inner_store(store), FileKeyValueStore)
    store.set(b"k", b"v", **_CTX)
    assert store.get(b"k", **_CTX) == b"v"


def test_create_kv_store_renders_query_id_prefix(tmp_path):
    store = create_kv_store(
        f"file://{tmp_path}",
        key_prefix="engine/{query_id}",
        query_id="q-123",
    )
    assert isinstance(_inner_store(store), FileKeyValueStore)
    store.set(b"k", b"v", query_id="q-123", operator_id="op5")
    assert store.get(b"k", query_id="q-123", operator_id="op5") == b"v"
    files = {path.name for path in tmp_path.iterdir()}
    assert "engine_q-123_query_id=q-123_operator_id=op5_k" in files


def test_create_kv_store_enforces_query_id_context(tmp_path):
    store = create_kv_store(
        f"file://{tmp_path}",
    )
    assert isinstance(store, ScopedKeyValueStore)

    with pytest.raises(ValueError):
        store.set(b"k", b"v", query_id="q1")

    store.set(b"k", b"v", query_id="q1", operator_id="op5")
    assert store.get(b"k", query_id="q1", operator_id="op5") == b"v"


def test_create_kv_store_enforcement_from_config(tmp_path, monkeypatch):
    store = create_kv_store(f"file://{tmp_path}")
    assert isinstance(store, ScopedKeyValueStore)

    with pytest.raises(ValueError):
        store.set(b"k", b"v")
    with pytest.raises(ValueError):
        store.set(b"k", b"v", query_id="q7")
    store.set(b"k", b"v", query_id="q7", operator_id="op7")
    assert store.get(b"k", query_id="q7", operator_id="op7") == b"v"
