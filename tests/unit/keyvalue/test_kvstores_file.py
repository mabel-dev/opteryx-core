import tempfile
from pathlib import Path

from opteryx.managers.kvstores import create_kv_store, FileKeyValueStore, ScopedKeyValueStore


_CTX = {"query_id": "q1", "operator_id": "op1"}


def test_file_kv_store_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        # create via factory with file://
        uri = f"file://{tmpdir}"
        store = create_kv_store(uri)
        assert isinstance(store, ScopedKeyValueStore)
        assert isinstance(store._store, FileKeyValueStore)

        key = b"0xdeadbeef"
        value = b"hello world"

        # initially not present
        assert store.get(key, **_CTX) is None
        assert store.contains([key], **_CTX) == []

        # set and get
        store.set(key, value, **_CTX)
        assert store.get(key, **_CTX) == value
        assert store.contains([key], **_CTX) == [key]

        # touch should not raise
        store.touch(key, **_CTX)

        # delete
        store.delete(key, **_CTX)
        assert store.get(key, **_CTX) is None
        assert store.contains([key], **_CTX) == []


def test_file_kv_store_prefixes_keys():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_kv_store(f"file://{tmpdir}", key_prefix="query123")
        store.set(b"abc", b"payload", **_CTX)
        assert store.get(b"abc", **_CTX) == b"payload"
        files = {path.name for path in Path(tmpdir).iterdir()}
        assert "query123_query_id=q1_operator_id=op1_abc" in files
