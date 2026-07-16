# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a Base class for KV Value Storage adapter.

This is used by the in-memory buffer cache.
"""

from typing import Iterable
from typing import Union


class BaseKeyValueStore:
    """
    Base class for cache objects
    """

    def __init__(self, location, key_prefix: bytes | str | None = None):
        self._location = location
        if key_prefix is None:
            self._key_prefix = b""
        elif isinstance(key_prefix, bytes):
            self._key_prefix = key_prefix
        else:
            self._key_prefix = str(key_prefix).encode("utf-8")

        if self._key_prefix and not self._key_prefix.endswith(b"/"):
            self._key_prefix += b"/"

    def _normalize_key(self, key: bytes) -> bytes:
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise TypeError("key must be bytes-like")
        key_bytes = bytes(key)
        if not self._key_prefix:
            return key_bytes
        return self._key_prefix + key_bytes

    def get(self, key: bytes) -> Union[bytes, None]:
        """
        Overwrite this method to retrieve a value from the cache, or None if the
        value is not in the cache.
        """
        raise NotImplementedError("`get` method on cache object not overridden.")

    def set(self, key: bytes, value: bytes) -> None:
        """
        Overwrite this method to place a value in the cache.
        """
        raise NotImplementedError("`set` method on cache object not overridden.")

    def get_many(self, keys: Iterable) -> dict:
        """Fetch several keys at once, returning ``{key: value}`` for those present.

        The returned dict is keyed by the caller's original (un-normalized) keys, and
        omits misses entirely. The default implementation loops ``get``; a backend with
        a native multi-get (e.g. Valkey ``MGET``) should override this to collapse the
        N round trips into one — that is the whole reason the method exists.
        """
        out = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    def set_many(self, items: dict) -> None:
        """Store several key/value pairs at once.

        `items` is ``{key: value}``, bytes-like throughout. The default loops ``set``; a
        backend with a native multi-set (e.g. Valkey ``MSET``) should override this to
        collapse the N round trips into one — the write-side twin of ``get_many``.
        """
        for key, value in items.items():
            self.set(key, value)

    def contains(self, keys: Iterable) -> Iterable:
        """
        Overwrite this method to return a list of items which are in the cache from
        a given list
        """
        # default to returning no matches
        return []

    def delete(self, key: bytes) -> None:
        """
        Overwrite this method to delete a value from the cache.
        """
        pass

    def touch(self, key: bytes):
        return None
