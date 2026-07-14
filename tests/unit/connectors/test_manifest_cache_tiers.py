"""Manifest cache: tier stack, promotion, and the immutability gate.

The cache's licence is that a manifest URI names one payload forever. These tests pin
the behaviour that depends on it — what is cached, what is bypassed, and what happens
when a tier misbehaves — because eroding any of it turns a correct cache into a stale one.
"""

import io

import pytest

from opteryx.connectors.manifest_disk_cache import CachingFileIO
from opteryx.connectors.manifest_disk_cache import ManifestDiskCache
from opteryx.connectors.manifest_disk_cache import RemoteManifestCache
from opteryx.connectors.manifest_disk_cache import is_manifest_uri

MANIFEST = "gs://bucket/ws/ds/metadata/manifest-1737000000000.parquet"
DATA_FILE = "gs://bucket/ws/ds/data/part-0001.parquet"

# The tiers validate parquet framing, so payloads must be plausibly framed.
PAYLOAD = b"PAR1" + b"manifest-bytes" + b"PAR1"
OTHER = b"PAR1" + b"other-bytes" + b"PAR1"


class FakeInputFile:
    def __init__(self, location, content):
        self.location = location
        self._content = content

    def open(self):
        return io.BytesIO(self._content)


class FakeFileIO:
    """Stands in for the catalog's FileIO; counts reads so we can prove they stop."""

    def __init__(self, content=PAYLOAD):
        self.content = content
        self.reads = 0

    def new_input(self, location):
        self.reads += 1
        return FakeInputFile(location, self.content)

    def new_output(self, location):
        return ("output", location)

    def delete(self, location):
        return ("delete", location)

    def exists(self, location):
        return True

    def list_files(self, prefix):
        return [prefix]


class FakeKVStore:
    """Minimal BaseKeyValueStore-shaped store, with a switch to make it fail."""

    def __init__(self, failing=False):
        self.data = {}
        self.failing = failing
        self.gets = 0
        self.sets = 0

    def get(self, key):
        self.gets += 1
        if self.failing:
            raise ConnectionError("valkey is down")
        return self.data.get(key)

    def set(self, key, value):
        self.sets += 1
        if self.failing:
            raise ConnectionError("valkey is down")
        self.data[key] = value


def _remote(store, max_value_bytes=1024):
    return RemoteManifestCache(store, max_value_bytes=max_value_bytes)


def test_manifest_uris_are_recognised_by_snapshot_addressing():
    # The snapshot id in the name is what makes the payload immutable. A path without
    # one is not cacheable, no matter where it sits.
    assert is_manifest_uri(MANIFEST)
    assert not is_manifest_uri(DATA_FILE)
    assert not is_manifest_uri("gs://bucket/ws/ds/metadata/manifest.parquet")


def test_data_files_are_never_cached(tmp_path):
    # Data files are mutable relative to a snapshot pointer; caching them would be the
    # one change that makes this cache able to serve stale reads.
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    inner = FakeFileIO()
    caching = CachingFileIO(inner, [disk])

    caching.new_input(DATA_FILE)
    caching.new_input(DATA_FILE)

    assert inner.reads == 2, "data file reads must always go to origin"
    assert disk.get(DATA_FILE) is None


def test_disk_tier_serves_second_read_without_touching_origin(tmp_path):
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    inner = FakeFileIO()
    caching = CachingFileIO(inner, [disk])

    first = caching.new_input(MANIFEST).open().read()
    second = caching.new_input(MANIFEST).open().read()

    assert first == second == PAYLOAD
    assert inner.reads == 1


def test_remote_hit_is_promoted_into_the_disk_tier(tmp_path):
    # The point of promotion: an instance pays the network round-trip once, then reads
    # locally. Without this, every read on the instance costs a Valkey trip.
    store = FakeKVStore()
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    remote = _remote(store)
    inner = FakeFileIO()

    # Seed the remote tier as if another instance had populated it.
    remote.put(MANIFEST, PAYLOAD)

    caching = CachingFileIO(inner, [disk, remote])
    assert caching.new_input(MANIFEST).open().read() == PAYLOAD

    assert inner.reads == 0, "remote hit must not fall through to origin"
    assert disk.get(MANIFEST) == PAYLOAD, "remote hit must backfill the local tier"

    gets_before = store.gets
    assert caching.new_input(MANIFEST).open().read() == PAYLOAD
    assert store.gets == gets_before, "second read must be served locally"


def test_full_miss_populates_every_tier(tmp_path):
    store = FakeKVStore()
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    remote = _remote(store)
    inner = FakeFileIO()

    caching = CachingFileIO(inner, [disk, remote])
    assert caching.new_input(MANIFEST).open().read() == PAYLOAD

    assert inner.reads == 1
    assert disk.get(MANIFEST) == PAYLOAD
    assert remote.get(MANIFEST) == PAYLOAD


def test_remote_outage_degrades_to_origin_rather_than_failing(tmp_path):
    # A cache is an optimisation. Losing Valkey must cost latency, never availability.
    store = FakeKVStore(failing=True)
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    inner = FakeFileIO()

    caching = CachingFileIO(inner, [disk, _remote(store)])
    assert caching.new_input(MANIFEST).open().read() == PAYLOAD
    assert inner.reads == 1


def test_remote_corrupt_entry_is_a_miss_not_an_exception():
    # Unlike the disk tier, a bad entry here is being served to every instance; raising
    # would take them all down. Drop it and re-read from origin instead.
    store = FakeKVStore()
    remote = _remote(store)
    store.data[RemoteManifestCache._key(MANIFEST)] = b"not-parquet"

    assert remote.get(MANIFEST) is None


def test_oversized_manifests_are_not_written_to_the_remote_tier():
    store = FakeKVStore()
    remote = _remote(store, max_value_bytes=len(PAYLOAD) - 1)

    remote.put(MANIFEST, PAYLOAD)

    assert store.sets == 0
    assert remote.get(MANIFEST) is None


def test_distinct_manifests_do_not_collide():
    # Snapshot ids are millisecond timestamps, not UUIDs, so two datasets can share one.
    # Keys must therefore be derived from the full URI.
    store = FakeKVStore()
    remote = _remote(store)
    other_uri = "gs://bucket/ws/OTHER/metadata/manifest-1737000000000.parquet"

    remote.put(MANIFEST, PAYLOAD)
    remote.put(other_uri, OTHER)

    assert remote.get(MANIFEST) == PAYLOAD
    assert remote.get(other_uri) == OTHER


def test_non_manifest_operations_delegate_untouched(tmp_path):
    disk = ManifestDiskCache(directory=str(tmp_path), max_bytes=10_000)
    caching = CachingFileIO(FakeFileIO(), [disk])

    assert caching.new_output("x") == ("output", "x")
    assert caching.delete("x") == ("delete", "x")
    assert caching.exists("x") is True
    assert caching.list_files("p") == ["p"]
    assert caching.ls("p") == ["p"]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
