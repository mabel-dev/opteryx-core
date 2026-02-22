from pathlib import Path
from uuid import uuid4

import pytest

from opteryx.managers.kvstores import LayeredKeyValueStore
from opteryx.managers.kvstores import MemoryPoolKeyValueStore
from opteryx.managers.kvstores import create_kv_store


def test_memory_kv_store_basic_round_trip():
    pool_name = f"test-memory-{uuid4().hex}"
    store = create_kv_store(f"memory://{pool_name}?pool_size_bytes=1024")
    assert isinstance(store, MemoryPoolKeyValueStore)

    key = b"alpha"
    value = b"value"
    assert store.get(key) is None
    store.set(key, value)
    assert store.get(key) == value
    assert store.contains([key, b"other"]) == [key]
    store.delete(key)
    assert store.get(key) is None


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
    assert isinstance(store, LayeredKeyValueStore)

    store.set(b"small", b"1234")
    assert store.layer_for_key(b"small") == 0
    assert store.get(b"small") == b"1234"

    store.set(b"large", b"0123456789")
    assert store.layer_for_key(b"large") == 1
    assert store.get(b"large") == b"0123456789"


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
    assert isinstance(store, LayeredKeyValueStore)

    store.set(b"row", b"12345")
    assert store.layer_for_key(b"row") == 1
    assert store.get(b"row") == b"12345"

    cold_files = {path.name for path in Path(cold_dir).iterdir()}
    assert "cold_query-42_row" in cold_files


def test_layered_kv_store_supports_up_to_three_layers(tmp_path):
    config = [
        f"file://{tmp_path / 'l1'}",
        f"file://{tmp_path / 'l2'}",
        f"file://{tmp_path / 'l3'}",
        f"file://{tmp_path / 'l4'}",
    ]
    with pytest.raises(ValueError):
        create_kv_store(config)
