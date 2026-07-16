"""
Valkey-backed Key-Value Store moved into kvstores namespace.
"""

from __future__ import annotations

import os
from typing import Union

from opteryx.config import MAX_CONSECUTIVE_CACHE_FAILURES
from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore
from opteryx.utils import single_item_cache


# A cache must never cost more than the thing it replaces. Without these, the client
# inherits no timeout at all and an unreachable server falls back to the OS TCP connect
# timeout (~130s on Linux) -- so a cache that cannot be reached would block every query
# that consults it, on every process, turning an optimisation into an outage. These
# bounds are generous for an in-region hop (~1ms RTT) and are overridden by any
# `?socket_connect_timeout=`/`?socket_timeout=` given on the connection URL.
_CONNECT_TIMEOUT_SECONDS = 0.25
_OPERATION_TIMEOUT_SECONDS = 0.5


@single_item_cache
def _valkey_client(connection: str):
    """One pooled client per connection string.

    `single_item_cache` memoises on a single *positional* argument, so the connection
    string is resolved by the caller and handed in as one. Decorating a `**kwargs`
    function with it raises TypeError on every call ("unexpected keyword argument
    'server'"), which is what made this store impossible to construct.
    """
    try:
        import valkey
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err

    return valkey.from_url(
        connection,
        socket_connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=_OPERATION_TIMEOUT_SECONDS,
    )


def _valkey_server(**kwargs):
    connection = kwargs.get("server") or os.environ.get("VALKEY_CONNECTION")
    if connection is None:
        return None

    return _valkey_client(connection)


class ValkeyCache(BaseKeyValueStore):
    hits: int = 0
    misses: int = 0
    skips: int = 0
    errors: int = 0
    sets: int = 0

    def __init__(
        self,
        location: str | None = None,
        key_prefix: bytes | str | None = None,
        **kwargs,
    ):
        self._server = _valkey_server(**kwargs)
        super().__init__(location, key_prefix=key_prefix)
        if self._server is None:
            import datetime

            print(f"{datetime.datetime.now()} [CACHE] Unable to set up valkey cache.")
            self._consecutive_failures: int = MAX_CONSECUTIVE_CACHE_FAILURES
        else:
            self._consecutive_failures = 0

    def get(self, key: bytes) -> Union[bytes, None]:
        key = self._normalize_key(key)
        if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
            self.skips += 1
            return None
        try:
            response = self._server.get(key)
            self._consecutive_failures = 0
            if response:
                self.hits += 1
                return bytes(response)
        except Exception as err:  # pragma: no cover
            self._consecutive_failures += 1
            if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err})."
                )
            self.errors += 1
            return None

        self.misses += 1
        return None

    def get_many(self, keys) -> dict:
        """One ``MGET`` for many keys, returning ``{key: value}`` for those present.

        Keyed by the caller's original keys. A single round trip regardless of key
        count — the reason a batched footer probe is worth doing at all. Failures
        degrade to an empty dict (every key a miss) and trip the same consecutive-
        failure disable as ``get``; a cache outage slows queries, never fails them.
        """
        keys = list(keys)
        if not keys:
            return {}
        if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
            self.skips += len(keys)
            return {}
        normalized = [self._normalize_key(k) for k in keys]
        try:
            responses = self._server.mget(normalized)
            self._consecutive_failures = 0
        except Exception as err:  # pragma: no cover
            self._consecutive_failures += 1
            if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err}) [MGET]."
                )
            self.errors += 1
            return {}

        out = {}
        for key, response in zip(keys, responses):
            if response:
                out[key] = bytes(response)
                self.hits += 1
            else:
                self.misses += 1
        return out

    def set(self, key: bytes, value: bytes) -> None:
        key = self._normalize_key(key)
        if self._consecutive_failures < MAX_CONSECUTIVE_CACHE_FAILURES:
            try:
                self._server.set(key, value)
                self.sets += 1
            except Exception as err:  # pragma: no cover
                self._consecutive_failures = MAX_CONSECUTIVE_CACHE_FAILURES
                self.errors += 1
                import datetime

                print(
                    f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err}) [SET]."
                )
        else:
            self.skips += 1

    def set_many(self, items) -> None:
        """One ``MSET`` for many key/value pairs — the write-side twin of ``get_many``.

        A single round trip regardless of pair count. Failures trip the same disable as
        ``set`` and are swallowed; a cache outage slows queries, never fails them.
        """
        items = dict(items)
        if not items:
            return
        if self._consecutive_failures >= MAX_CONSECUTIVE_CACHE_FAILURES:
            self.skips += len(items)
            return
        normalized = {self._normalize_key(k): v for k, v in items.items()}
        try:
            self._server.mset(normalized)
            self.sets += len(normalized)
        except Exception as err:  # pragma: no cover
            self._consecutive_failures = MAX_CONSECUTIVE_CACHE_FAILURES
            self.errors += 1
            import datetime

            print(
                f"{datetime.datetime.now()} [CACHE] Disabling remote Valkey cache due to persistent errors ({err}) [MSET]."
            )

    def delete(self, key):
        key = self._normalize_key(key)
        try:
            self._server.delete(key)
        except Exception as err:
            self.errors += 1
