"""
Context-scoped KV store wrapper.

Enforces required context fields (for example `query_id`) on writes and
automatically composes scoped keys before delegating to an underlying store.
"""

from __future__ import annotations

from typing import Any
from typing import Iterable
from typing import Union

from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore


class ScopedKeyValueStore(BaseKeyValueStore):
    """Wrap a KV store and enforce context fields on set operations."""

    def __init__(
        self,
        store: BaseKeyValueStore,
        required_context_fields: list[str] | tuple[str, ...],
    ):
        self._store = store
        self._required_context_fields = tuple(
            field.strip() for field in required_context_fields if field and field.strip()
        )
        super().__init__(getattr(store, "_location", None))

    def __getattr__(self, item: str) -> Any:
        return getattr(self._store, item)

    @staticmethod
    def _validate_key_type(key: bytes) -> bytes:
        if not isinstance(key, (bytes, bytearray, memoryview)):
            raise TypeError("key must be bytes-like")
        return bytes(key)

    def _compose_scoped_key(
        self,
        key: bytes,
        context: dict[str, Any],
        require_all_fields: bool,
    ) -> bytes:
        key_bytes = self._validate_key_type(key)

        missing = []
        segments: list[bytes] = []
        for field in self._required_context_fields:
            value = context.get(field)
            if value is None or str(value).strip() == "":
                missing.append(field)
                continue
            segments.append(f"{field}={value}".encode("utf-8"))

        if require_all_fields and missing:
            raise ValueError("Missing required key context fields: " + ", ".join(missing))

        for field in sorted(context):
            if field in self._required_context_fields:
                continue
            value = context[field]
            if value is None or str(value).strip() == "":
                continue
            segments.append(f"{field}={value}".encode("utf-8"))

        if not segments:
            return key_bytes
        return b"/".join(segments + [key_bytes])

    def get(self, key: bytes, **context) -> Union[bytes, None]:
        scoped_key = self._compose_scoped_key(key, context, require_all_fields=False)
        return self._store.get(scoped_key)

    def set(self, key: bytes, value: bytes, **context) -> None:
        scoped_key = self._compose_scoped_key(key, context, require_all_fields=True)
        self._store.set(scoped_key, value)

    def contains(self, keys: Iterable, **context) -> Iterable:
        key_list = list(keys)
        scoped_keys = [
            self._compose_scoped_key(key, context, require_all_fields=False) for key in key_list
        ]
        contains_scoped = set(self._store.contains(scoped_keys))
        return [key for key, scoped in zip(key_list, scoped_keys) if scoped in contains_scoped]

    def delete(self, key: bytes, **context) -> None:
        scoped_key = self._compose_scoped_key(key, context, require_all_fields=False)
        self._store.delete(scoped_key)

    def touch(self, key: bytes, **context):
        scoped_key = self._compose_scoped_key(key, context, require_all_fields=False)
        return self._store.touch(scoped_key)

    def layer_for_key(self, key: bytes, **context):
        if getattr(self._store, "layer_for_key", None) is None:
            return None
        scoped_key = self._compose_scoped_key(key, context, require_all_fields=False)
        return self._store.layer_for_key(scoped_key)
