from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores import FileKeyValueStore, GCSKeyValueStore
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


@pytest.mark.parametrize(
    "location",
    [
        "valkey://localhost:6379",
        "valkeys://localhost:6379",
        "redis://localhost:6379",
        "rediss://localhost:6379",
        # a real-world managed-Valkey URL shape: TLS, credentials, non-default port.
        "rediss://default:pw@example-cache.aivencloud.com:10068",
    ],
)
def test_create_kv_store_routes_all_valkey_client_schemes(location):
    # The `valkey` client's own parse_url accepts all four of these (TCP/TLS x
    # Valkey/Redis-compatible naming) — our factory must route every one of them to
    # ValkeyCache rather than raising "Unknown KV store scheme". Construction itself
    # doesn't connect eagerly, so no live server is needed for this to prove routing.
    from opteryx.managers.kvstores import ValkeyCache

    # `enforce_context_fields=()` opts out of scoping (as the manifest/footer caches do),
    # so the store comes back unwrapped — no ScopedKeyValueStore to unwrap here.
    store = create_kv_store(location, enforce_context_fields=())
    assert isinstance(store, ValkeyCache)


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
