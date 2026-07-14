"""ValkeyCache construction.

There was no coverage here, which is how the store came to be unconstructible without
anyone noticing: `_valkey_server` took `**kwargs` but was decorated with
`single_item_cache`, whose wrapper accepts exactly one positional argument. Every call
raised `TypeError: unexpected keyword argument 'server'` before it reached the client.

These tests pin construction end-to-end, with a stub client standing in for the valkey
package so they run without it installed.
"""

import sys
import types

import pytest

from opteryx.managers.kvstores import ScopedKeyValueStore
from opteryx.managers.kvstores import ValkeyCache
from opteryx.managers.kvstores import create_kv_store
from opteryx.managers.kvstores.valkey import _valkey_client
from opteryx.managers.kvstores.valkey import _valkey_server

CONNECTION = "valkey://10.0.0.1:6379"


class FakeClient:
    def __init__(self, url):
        self.url = url
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value


@pytest.fixture
def fake_valkey(monkeypatch):
    """Stand in for the `valkey` package, which core deliberately does not depend on."""
    module = types.ModuleType("valkey")
    module.from_url = FakeClient
    monkeypatch.setitem(sys.modules, "valkey", module)
    _valkey_client.cache_clear()
    yield module
    _valkey_client.cache_clear()


def test_valkey_server_accepts_a_server_kwarg(fake_valkey):
    # The regression: this raised TypeError before reaching the client.
    client = _valkey_server(server=CONNECTION)

    assert isinstance(client, FakeClient)
    assert client.url == CONNECTION


def test_valkey_server_is_none_without_a_connection(fake_valkey, monkeypatch):
    monkeypatch.delenv("VALKEY_CONNECTION", raising=False)

    assert _valkey_server() is None


def test_client_is_pooled_per_connection_string(fake_valkey):
    first = _valkey_server(server=CONNECTION)
    again = _valkey_server(server=CONNECTION)
    other = _valkey_server(server="valkey://10.0.0.2:6379")

    assert first is again, "one pooled client per connection string"
    assert first is not other


def test_factory_builds_a_valkey_store_from_a_valkey_uri(fake_valkey):
    store = create_kv_store(CONNECTION)

    assert isinstance(store, ScopedKeyValueStore), "scoped by default, as spill requires"
    assert isinstance(store._store, ValkeyCache)


def test_content_addressed_stores_opt_out_of_query_scoping(fake_valkey):
    # What the manifest cache relies on: keys must not be namespaced by query/operator,
    # or a manifest cached by one query could never be read by the next.
    store = create_kv_store(CONNECTION, enforce_context_fields=())

    assert isinstance(store, ValkeyCache)
    assert not isinstance(store, ScopedKeyValueStore)


def test_round_trip_through_the_store(fake_valkey):
    store = create_kv_store(CONNECTION, enforce_context_fields=())

    store.set(b"manifest/abc", b"PAR1payloadPAR1")

    assert store.get(b"manifest/abc") == b"PAR1payloadPAR1"
    assert store.get(b"manifest/missing") is None


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
