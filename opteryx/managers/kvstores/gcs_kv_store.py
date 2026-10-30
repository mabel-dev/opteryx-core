"""
GCS-backed Key-Value Store

Expects a location like: gs://bucket/[optional-prefix]
The key provided will be the filename portion of the object key.
"""

from __future__ import annotations

import functools
import importlib
from typing import Iterable, Union
from urllib.parse import urlparse

from opteryx.exceptions import MissingDependencyError
from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore

NotFound = None  # google.api_core NotFound, bound by _gcs_client() before any store exists


@functools.lru_cache(maxsize=1)
def _gcs_client():
    """One client per process. `google.cloud.storage.Client()` takes its config from
    Application Default Credentials, not from arguments, so there is nothing to key a
    cache on — `single_item_cache` (used elsewhere for this exact "construct once"
    need) requires exactly one positional argument and does not fit a zero-argument
    singleton; `lru_cache(maxsize=1)` does."""
    try:
        storage = importlib.import_module("google.cloud.storage")
    except ImportError as err:  # pragma: no cover - optional dependency
        raise MissingDependencyError("google-cloud-storage") from err
    global NotFound
    # Only "no such object" is a miss; a 403 or a 5xx must surface, not look like
    # a permanently cold cache. google.api_core ships with google-cloud-storage.
    NotFound = importlib.import_module("google.api_core.exceptions").NotFound
    return storage.Client()


class GCSKeyValueStore(BaseKeyValueStore):
    def __init__(self, location: str, key_prefix: bytes | str | None = None, **_kwargs):
        parsed = urlparse(location)
        if parsed.scheme != "gs":
            raise ValueError("location must be a gs:// URI")

        self._bucket_name = parsed.netloc
        self._prefix = parsed.path.lstrip("/")
        self._client = _gcs_client()
        self._bucket = self._client.bucket(self._bucket_name)
        super().__init__(location, key_prefix=key_prefix)

    def _object_name(self, key: bytes) -> str:
        key = self._normalize_key(key)
        try:
            key_str = key.decode("utf-8")
        except UnicodeDecodeError:
            key_str = key.hex()
        if self._prefix:
            return f"{self._prefix}/{key_str}"
        return key_str

    def get(self, key: bytes) -> Union[bytes, None]:
        name = self._object_name(key)
        blob = self._bucket.blob(name)
        try:
            return blob.download_as_bytes()
        except NotFound:
            return None

    def set(self, key: bytes, value: bytes) -> None:
        name = self._object_name(key)
        blob = self._bucket.blob(name)
        blob.upload_from_string(value)

    def contains(self, keys: Iterable) -> Iterable:
        result = []
        for k in keys:
            # exists() already answers False for a missing object; anything it
            # raises is a real failure.
            if self._bucket.blob(self._object_name(k)).exists():
                result.append(k)
        return result

    def delete(self, key: bytes) -> None:
        blob = self._bucket.blob(self._object_name(key))
        try:
            blob.delete()
        except NotFound:
            pass
