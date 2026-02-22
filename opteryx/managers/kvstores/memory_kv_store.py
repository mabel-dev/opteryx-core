"""
MemoryPool-backed Key-Value Store.

Expects a location like: memory://[pool-name]

Values are stored in a local MemoryPool instance and addressed by key -> ref_id mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Iterable
from typing import Union
from urllib.parse import parse_qs
from urllib.parse import urlparse

from opteryx.compiled.structures.memory_pool import MemoryPool
from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore

_POOL_LOCK = RLock()


@dataclass
class _PoolState:
    pool: MemoryPool
    refs: dict[bytes, tuple[int, int]]
    lock: RLock


_POOLS: dict[str, _PoolState] = {}


def _get_pool_state(pool_name: str, size_bytes: int) -> _PoolState:
    with _POOL_LOCK:
        state = _POOLS.get(pool_name)
        if state is None:
            pool = MemoryPool(
                size=size_bytes, name=f"KV:{pool_name}", auto_resize=False, alignment=8
            )
            state = _PoolState(pool=pool, refs={}, lock=RLock())
            _POOLS[pool_name] = state
        return state


def ensure_memory_pool(location: str, **kwargs) -> str | None:
    """
    Ensure a global MemoryPool exists for a memory:// location.

    Returns the pool name when a memory location is provided, otherwise None.
    """
    parsed = urlparse(location)
    if parsed.scheme != "memory":
        return None

    query = parse_qs(parsed.query, keep_blank_values=True)
    pool_name = parsed.netloc or parsed.path.lstrip("/") or str(kwargs.get("pool_name", "default"))
    size_default = kwargs.get("pool_size_bytes", 256 * 1024 * 1024)
    pool_size_bytes = int(query.get("pool_size_bytes", [size_default])[0])
    if pool_size_bytes <= 0:
        raise ValueError("pool_size_bytes must be positive")

    _get_pool_state(pool_name, pool_size_bytes)
    return pool_name


def list_memory_pools() -> list[str]:
    """List globally-registered memory pool names."""
    with _POOL_LOCK:
        return list(_POOLS.keys())


class MemoryPoolKeyValueStore(BaseKeyValueStore):
    """In-process KV store backed by the compiled MemoryPool."""

    def __init__(self, location: str, key_prefix: bytes | str | None = None, **kwargs):
        parsed = urlparse(location)
        if parsed.scheme != "memory":
            raise ValueError("location must be a memory:// URI")

        query = parse_qs(parsed.query, keep_blank_values=True)
        pool_name = (
            parsed.netloc or parsed.path.lstrip("/") or str(kwargs.get("pool_name", "default"))
        )

        size_default = kwargs.get("pool_size_bytes", 256 * 1024 * 1024)
        pool_size_bytes = int(query.get("pool_size_bytes", [size_default])[0])
        if pool_size_bytes <= 0:
            raise ValueError("pool_size_bytes must be positive")

        self._pool_name = pool_name
        self._state = _get_pool_state(pool_name, pool_size_bytes)
        self._pool = self._state.pool
        super().__init__(location, key_prefix=key_prefix)

    def get(self, key: bytes) -> Union[bytes, None]:
        normalized_key = self._normalize_key(key)
        with self._state.lock:
            ref_meta = self._state.refs.get(normalized_key)
            if ref_meta is None:
                return None
            ref_id, _size = ref_meta
        try:
            value = self._pool.read(ref_id, zero_copy=False, latch=False)
            return bytes(value)
        except ValueError:
            with self._state.lock:
                self._state.refs.pop(normalized_key, None)
            return None

    def set(self, key: bytes, value: bytes) -> None:
        normalized_key = self._normalize_key(key)
        payload = bytes(value)
        ref_id = self._pool.commit(payload)
        if ref_id == -1:
            raise MemoryError(f"memory kv store '{self._pool_name}' is out of space")

        with self._state.lock:
            existing = self._state.refs.get(normalized_key)
            self._state.refs[normalized_key] = (int(ref_id), len(payload))

        if existing is not None:
            try:
                self._pool.release(existing[0])
            except ValueError:
                pass

    def contains(self, keys: Iterable) -> Iterable:
        key_list = list(keys)
        with self._state.lock:
            existing = set(self._state.refs.keys())
        return [k for k in key_list if self._normalize_key(k) in existing]

    def delete(self, key: bytes) -> None:
        normalized_key = self._normalize_key(key)
        with self._state.lock:
            existing = self._state.refs.pop(normalized_key, None)
        if existing is not None:
            try:
                self._pool.release(existing[0])
            except ValueError:
                pass

    def touch(self, key: bytes):
        # In-process MemoryPool has no TTL semantics.
        return None
