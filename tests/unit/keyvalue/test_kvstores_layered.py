from pathlib import Path
from uuid import uuid4

import pytest

from opteryx.managers.kvstores import LayeredKeyValueStore
from opteryx.managers.kvstores import MemoryPoolKeyValueStore
from opteryx.managers.kvstores import ScopedKeyValueStore
from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores import initialize_global_memory_pools
from opteryx.managers.kvstores import list_memory_pools
import opteryx.config as opteryx_config

_CTX = {"query_id": "q1", "operator_id": "op1"}


def test_memory_kv_store_basic_round_trip():
    pool_name = f"test-memory-{uuid4().hex}"
    store = create_kv_store(f"memory://{pool_name}?pool_size_bytes=1024")
    assert isinstance(store, ScopedKeyValueStore)
    assert isinstance(store._store, MemoryPoolKeyValueStore)

    key = b"alpha"
    value = b"value"
    assert store.get(key, **_CTX) is None
    store.set(key, value, **_CTX)
    assert store.get(key, **_CTX) == value
    assert store.contains([key, b"other"], **_CTX) == [key]
    store.delete(key, **_CTX)
    assert store.get(key, **_CTX) is None


def test_memory_kv_store_is_shared_by_pool_name():
    pool_name = f"test-shared-memory-{uuid4().hex}"
    store_a = create_kv_store(f"memory://{pool_name}?pool_size_bytes=1024")
    store_b = create_kv_store(f"memory://{pool_name}?pool_size_bytes=1024")

    store_a.set(b"shared-key", b"shared-value", **_CTX)
    assert store_b.get(b"shared-key", **_CTX) == b"shared-value"


def test_layered_kv_store_routes_by_layer_threshold(tmp_path):
    pool_name = f"test-layered-{uuid4().hex}"
    store = create_kv_store(
        {
            "layers": [
                {
                    "location": f"memory://{pool_name}?pool_size_bytes=4096",
                    "max_bytes": 8,
                },
                {
                    "location": f"file://{tmp_path / 'cold'}",
                },
            ]
        }
    )
    assert isinstance(store, ScopedKeyValueStore)
    assert isinstance(store._store, LayeredKeyValueStore)

    store.set(b"small", b"1234", **_CTX)
    assert store.layer_for_key(b"small", **_CTX) == 0
    assert store.get(b"small", **_CTX) == b"1234"

    store.set(b"large", b"0123456789", **_CTX)
    assert store.layer_for_key(b"large", **_CTX) == 1
    assert store.get(b"large", **_CTX) == b"0123456789"


def test_layered_kv_store_applies_root_and_layer_prefixes(tmp_path):
    hot_dir = tmp_path / "hot"
    cold_dir = tmp_path / "cold"
    config = {
        "key_prefix": "query-42",
        "layers": [
            {"location": f"file://{hot_dir}", "key_prefix": "hot", "max_bytes": 4},
            {"location": f"file://{cold_dir}", "key_prefix": "cold"},
        ],
    }
    store = create_kv_store(config)
    assert isinstance(store, ScopedKeyValueStore)
    assert isinstance(store._store, LayeredKeyValueStore)

    store.set(b"row", b"12345", **_CTX)
    assert store.layer_for_key(b"row", **_CTX) == 1
    assert store.get(b"row", **_CTX) == b"12345"

    cold_files = {path.name for path in Path(cold_dir).iterdir()}
    assert "cold_query-42_query_id=q1_operator_id=op1_row" in cold_files


def test_layered_kv_store_supports_up_to_three_layers(tmp_path):
    config = [
        f"file://{tmp_path / 'l1'}",
        f"file://{tmp_path / 'l2'}",
        f"file://{tmp_path / 'l3'}",
        f"file://{tmp_path / 'l4'}",
    ]
    with pytest.raises(ValueError):
        create_kv_store(config)


def test_initialize_global_memory_pools_from_explicit_layers():
    pool_name = f"test-prewarm-{uuid4().hex}"
    initialized = initialize_global_memory_pools(
        [
            f"memory://{pool_name}?pool_size_bytes=4096&max_bytes=1024",
            "null://unused",
        ]
    )
    assert pool_name in initialized
    assert pool_name in list_memory_pools()


def test_create_kv_store_uses_configured_layers(monkeypatch, tmp_path):
    pool_name = f"test-config-layer-{uuid4().hex}"
    monkeypatch.setattr(
        opteryx_config,
        "KVSTORE_LAYERS",
        [
            {
                "location": f"memory://{pool_name}?pool_size_bytes=4096",
                "max_bytes": 4,
            },
            {
                "location": f"file://{tmp_path / 'cold'}",
            },
        ],
        raising=False,
    )
    monkeypatch.setattr(opteryx_config, "KVSTORE_LOCATION", "", raising=False)
    monkeypatch.setattr(opteryx_config, "KVSTORE_KEY_PREFIX", "cfg-layer", raising=False)

    store = create_kv_store(None)
    assert isinstance(store, ScopedKeyValueStore)
    assert isinstance(store._store, LayeredKeyValueStore)

    store.set(b"row", b"12345", **_CTX)
    assert store.layer_for_key(b"row", **_CTX) == 1


def test_layered_kv_store_renders_query_id_in_layer_prefix(tmp_path):
    store = create_kv_store(
        {
            "layers": [
                {"location": f"file://{tmp_path / 'l1'}", "key_prefix": "l1/{query_id}", "max_bytes": 4},
                {"location": f"file://{tmp_path / 'l2'}", "key_prefix": "l2/{query_id}"},
            ]
        },
        query_id="qid-9",
    )
    assert isinstance(store, ScopedKeyValueStore)
    assert isinstance(store._store, LayeredKeyValueStore)
    store.set(b"row", b"12345", query_id="qid-9", operator_id="op9")
    assert store.layer_for_key(b"row", query_id="qid-9", operator_id="op9") == 1
    files = {path.name for path in (tmp_path / "l2").iterdir()}
    assert "l2_qid-9_query_id=qid-9_operator_id=op9_row" in files
