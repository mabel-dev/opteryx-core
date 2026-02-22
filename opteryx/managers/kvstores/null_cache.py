"""
Null cache implementation in kvstores namespace used as a default remote cache.
"""

from __future__ import annotations

from typing import Any

from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore


class NullCache(BaseKeyValueStore):
    def __init__(self, location: str | None = None, key_prefix: bytes | str | None = None):
        super().__init__(location, key_prefix=key_prefix)

    def get(self, key: bytes) -> None:
        return None

    def set(self, key: bytes, value: Any) -> None:
        return None

    def touch(self, key: bytes) -> None:
        return None
