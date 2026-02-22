from uuid import uuid4

import pytest

from opteryx.managers.kvstores import create_kv_store
from opteryx.operators.shuffle import BinStore


_CTX = {"query_id": "q1", "operator_id": "shuffle-op"}


def _create_memory_store():
    pool_name = f"shuffle-test-{uuid4().hex}"
    return create_kv_store(f"memory://{pool_name}?pool_size_bytes=1048576")


def test_append_chunk_generates_monotonic_sequence_and_round_trip_payload():
    store = _create_memory_store()
    bin_store = BinStore(store)

    first = bin_store.append_chunk(
        pass_id="p1", bin_id=3, payload=memoryview(b"first"), scope_key="p1", **_CTX
    )
    second = bin_store.append_chunk(
        pass_id="p1", bin_id=3, payload=b"second", scope_key="p1", **_CTX
    )

    assert first["chunk_seq"] == 0
    assert second["chunk_seq"] == 1
    assert first["chunk_key"] != second["chunk_key"]
    assert first["chunk_key"].startswith("pass/p1/bin/3/chunk/")
    assert second["chunk_key"].startswith("pass/p1/bin/3/chunk/")
    assert bin_store.get_chunk(first["chunk_key"], **_CTX) == b"first"
    assert bin_store.get_chunk(second["chunk_key"], **_CTX) == b"second"


def test_manifest_append_and_read_uses_append_order():
    store = _create_memory_store()
    bin_store = BinStore(store)

    bin_key = "pass/p2/bin/5"
    bin_store.append_manifest(
        bin_key,
        {"chunk_key": "pass/p2/bin/5/chunk/00000000000000000003", "chunk_seq": 3},
        scope_key="p2",
        **_CTX,
    )
    bin_store.append_manifest(
        bin_key,
        {"chunk_key": "pass/p2/bin/5/chunk/00000000000000000001", "chunk_seq": 1},
        scope_key="p2",
        **_CTX,
    )

    manifest = bin_store.iter_manifest(bin_key, **_CTX)
    assert [entry["chunk_seq"] for entry in manifest] == [3, 1]


def test_delete_scope_removes_all_recorded_keys():
    store = _create_memory_store()
    bin_store = BinStore(store)

    scope_key = "scope-p3"
    chunk = bin_store.append_chunk(
        pass_id="p3", bin_id=0, payload=b"payload", scope_key=scope_key, **_CTX
    )
    bin_store.append_manifest(
        "pass/p3/bin/0",
        {"chunk_key": chunk["chunk_key"], "chunk_seq": chunk["chunk_seq"]},
        scope_key=scope_key,
        **_CTX,
    )

    assert bin_store.get_chunk(chunk["chunk_key"], **_CTX) == b"payload"
    assert len(bin_store.iter_manifest("pass/p3/bin/0", **_CTX)) == 1

    deleted = bin_store.delete_scope(scope_key, **_CTX)
    assert deleted >= 3
    assert bin_store.get_chunk(chunk["chunk_key"], **_CTX) is None
    assert bin_store.iter_manifest("pass/p3/bin/0", **_CTX) == []


def test_put_chunk_rejects_missing_context_values():
    store = _create_memory_store()
    bin_store = BinStore(store)

    with pytest.raises(ValueError):
        bin_store.put_chunk("pass/p9/bin/0/chunk/0", b"v", query_id="", operator_id="op")

    with pytest.raises(ValueError):
        bin_store.put_chunk("pass/p9/bin/0/chunk/0", b"v", query_id="q", operator_id="")
