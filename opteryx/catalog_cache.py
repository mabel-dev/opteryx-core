# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A caller-owned, time-limited cache of catalog relation lookups.

WHAT IT HOLDS
-------------
One entry per relation name: the `(kind, object)` pair `get_relation` returns - a
dataset handle, a view definition, or "not here". That call is a Firestore round trip
and it is the first cloud cost of planning any statement, paid once per relation named.

WHY IT IS NOT ON BY DEFAULT
---------------------------
The dataset document IS the version pointer: the catalog re-reads it on every call
precisely so a plan is built against the current snapshot. Holding it for a minute
trades that guarantee away. That is the right trade for a check that never reads a row
and the wrong one for a statement that does, so this is never wired into the execute
path - `Session.check()` takes one and `query_planner` cannot.

A cached entry can therefore be behind the catalog by up to `ttl` seconds. For an
editor that means a table created moments ago may briefly still read as unknown, and a
column added moments ago may briefly not offer itself for completion. Nothing a check
reports can be acted on destructively, so the cost of being a minute stale is a
redrawn squiggle.

SCOPE AND SHARING
-----------------
Entries are keyed by relation name alone. They carry no user identity and holding one
grants nothing: every permission gate in the binder is evaluated live, per call,
against the session's own execution context. So one cache may serve many users.

It IS specific to a catalog: two workspaces reachable in one process resolve different
relations under the same names, so give each its own cache rather than sharing one.
"""

import threading
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

__all__ = ["CatalogCache"]

# A minute. Long enough that a burst of keystrokes costs one round trip per relation,
# short enough that a schema change shows up while the person who made it is still
# looking at the editor.
DEFAULT_CATALOG_CACHE_TTL: float = 60.0


class CatalogCache:
    """A TTL cache of catalog relation lookups, owned by the caller.

    Create one per catalog and hold it for as long as you want the entries to be
    reusable - typically for the lifetime of a web session or a worker process.

    Parameters:
        ttl: seconds an entry stays usable. Must be positive; a cache that never
            expires is not a thing this offers, because an editor left open for a day
            would then never see a schema change.
        maxsize: entries retained. The oldest is evicted when full.
    """

    __slots__ = ("_entries", "_lock", "ttl", "maxsize", "_hits", "_misses")

    def __init__(self, ttl: float = DEFAULT_CATALOG_CACHE_TTL, maxsize: int = 512):
        if ttl <= 0:
            raise ValueError("CatalogCache ttl must be a positive number of seconds.")
        if maxsize <= 0:
            raise ValueError("CatalogCache maxsize must be at least one entry.")
        self.ttl = float(ttl)
        self.maxsize = int(maxsize)
        self._entries: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Return the cached value for `key`, or None if absent or expired.

        None is not a cacheable value here - `resolve_relation` stores the whole
        `(kind, object)` tuple, and "this relation is not in the catalog" is the
        tuple `(None, None)`, which is truthy as a tuple and so round-trips.
        """
        entry = self._entries.get(key)
        if entry is None:
            self._misses += 1
            return None
        value, stored_at = entry
        if (time.monotonic() - stored_at) >= self.ttl:
            with self._lock:
                # Re-check: another thread may have refreshed it since the read above.
                current = self._entries.get(key)
                if current is not None and current[1] == stored_at:
                    del self._entries[key]
            self._misses += 1
            return None
        self._hits += 1
        return value

    def put(self, key: str, value: Any) -> None:
        """Store `value` under `key`, evicting the oldest entry if full."""
        with self._lock:
            if key not in self._entries and len(self._entries) >= self.maxsize:
                # dicts preserve insertion order, so the first key is the oldest
                del self._entries[next(iter(self._entries))]
            self._entries[key] = (value, time.monotonic())

    def invalidate(self, key: str) -> None:
        """Drop one relation's entry.

        Call it when this process has just changed that relation - a DDL statement run
        in the same session makes its own cached entry a lie immediately, rather than
        in a minute.
        """
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        """Drop every entry."""
        with self._lock:
            self._entries.clear()

    def stats(self) -> Dict[str, int]:
        """Hits, misses and current size - for reporting, not for decisions."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._entries),
            "maxsize": self.maxsize,
        }

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"<CatalogCache ttl={self.ttl}s entries={len(self._entries)}/{self.maxsize}>"
