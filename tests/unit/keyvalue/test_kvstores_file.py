import tempfile
from pathlib import Path

from opteryx.managers.kvstores import create_kv_store, FileKeyValueStore


def test_file_kv_store_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        # create via factory with file://
        uri = f"file://{tmpdir}"
        store = create_kv_store(uri)
        assert isinstance(store, FileKeyValueStore)

        key = b"0xdeadbeef"
        value = b"hello world"

        # initially not present
        assert store.get(key) is None
        assert store.contains([key]) == []

        # set and get
        store.set(key, value)
        assert store.get(key) == value
        assert store.contains([key]) == [key]

        # touch should not raise
        store.touch(key)

        # delete
        store.delete(key)
        assert store.get(key) is None
        assert store.contains([key]) == []


def test_file_kv_store_prefixes_keys():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = create_kv_store(f"file://{tmpdir}", key_prefix="query123")
        store.set(b"abc", b"payload")
        assert store.get(b"abc") == b"payload"
        files = {path.name for path in Path(tmpdir).iterdir()}
        assert "query123_abc" in files
