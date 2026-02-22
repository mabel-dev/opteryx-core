"""
Factory for creating KeyValueStore instances from URI-like locations.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse

from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore
from opteryx.managers.kvstores.file_kv_store import FileKeyValueStore
from opteryx.managers.kvstores.gcs_kv_store import GCSKeyValueStore
from opteryx.managers.kvstores.layered_kv_store import LayeredKeyValueStore
from opteryx.managers.kvstores.memory_kv_store import MemoryPoolKeyValueStore
from opteryx.managers.kvstores.null_cache import NullCache
from opteryx.managers.kvstores.s3_kv_store import S3KeyValueStore
from opteryx.managers.kvstores.valkey import ValkeyCache


def _parse_max_bytes(value: Any) -> int | None:
    if value is None or value == "":
        return None
    max_bytes = int(value)
    if max_bytes < 0:
        raise ValueError("max_bytes must be zero or a positive integer")
    return max_bytes


def _normalize_prefix(prefix: bytes | str | None) -> str | None:
    if prefix in (None, "", b""):
        return None
    if isinstance(prefix, bytes):
        return prefix.decode("utf-8")
    return str(prefix)


def _merge_prefix(base_prefix: bytes | str | None, extra_prefix: bytes | str | None) -> str | None:
    left = _normalize_prefix(base_prefix)
    right = _normalize_prefix(extra_prefix)

    if left is None:
        return right
    if right is None:
        return left

    left = left.strip("/")
    right = right.strip("/")
    if not left:
        return right
    if not right:
        return left
    return f"{left}/{right}"


def _extract_uri_options(location: str) -> tuple[str, int | None, str | None]:
    parsed = urlparse(location)
    if not parsed.query:
        return location, None, None

    query = parse_qs(parsed.query, keep_blank_values=True)
    max_bytes = _parse_max_bytes(query.pop("max_bytes", [None])[0])
    uri_prefix = query.pop("key_prefix", query.pop("prefix", [None]))[0]

    clean_query = urlencode(query, doseq=True)
    clean_location = urlunparse(parsed._replace(query=clean_query))
    return clean_location, max_bytes, _normalize_prefix(uri_prefix)


def _coerce_layer_spec(layer_spec: Any) -> tuple[str, int | None, str | None]:
    if isinstance(layer_spec, str):
        return _extract_uri_options(layer_spec.strip())

    if not isinstance(layer_spec, Mapping):
        raise TypeError("Layer definition must be a string URI or mapping")

    raw_location = layer_spec.get("location", layer_spec.get("uri"))
    if not raw_location:
        raise ValueError("Layer definition must include `location` or `uri`")

    location = str(raw_location).strip()
    clean_location, uri_max_bytes, uri_prefix = _extract_uri_options(location)
    dict_max_bytes = _parse_max_bytes(layer_spec.get("max_bytes"))
    dict_prefix = _normalize_prefix(layer_spec.get("key_prefix", layer_spec.get("prefix")))

    layer_prefix = _merge_prefix(dict_prefix, uri_prefix)
    return (
        clean_location,
        (dict_max_bytes if dict_max_bytes is not None else uri_max_bytes),
        layer_prefix,
    )


def _create_single_store(
    location: str, key_prefix: bytes | str | None = None, **kwargs: Any
) -> BaseKeyValueStore:
    parsed = urlparse(location)
    scheme = parsed.scheme or "file"

    if scheme in ("file", ""):
        return FileKeyValueStore(location, key_prefix=key_prefix, **kwargs)
    if scheme in ("s3", "minio"):
        return S3KeyValueStore(location, key_prefix=key_prefix, **kwargs)
    if scheme in ("gs", "gcs"):
        return GCSKeyValueStore(location, key_prefix=key_prefix, **kwargs)
    if scheme == "valkey":
        server = location if "://" in location else f"valkey://{location}"
        return ValkeyCache(location=location, key_prefix=key_prefix, server=server, **kwargs)
    if scheme == "memory":
        return MemoryPoolKeyValueStore(location, key_prefix=key_prefix, **kwargs)
    if scheme == "null":
        return NullCache(location=location, key_prefix=key_prefix)

    raise ValueError(f"Unknown KV store scheme: {scheme}")


def _create_layered_store(
    layer_specs: Sequence[Any],
    *,
    key_prefix: bytes | str | None = None,
    location: str = "layered://",
    **kwargs: Any,
) -> LayeredKeyValueStore:
    if not layer_specs:
        raise ValueError("Layered KV configuration requires at least one layer")
    if len(layer_specs) > 3:
        raise ValueError("Layered KV configuration supports up to three layers")

    layers: list[tuple[BaseKeyValueStore, int | None]] = []
    for raw_layer in layer_specs:
        layer_location, max_bytes, layer_prefix = _coerce_layer_spec(raw_layer)
        layer_store = _create_single_store(layer_location, key_prefix=layer_prefix, **kwargs)
        layers.append((layer_store, max_bytes))

    return LayeredKeyValueStore(layers=layers, location=location, key_prefix=key_prefix)


def create_kv_store(
    location: str | Mapping[str, Any] | Sequence[Any] | BaseKeyValueStore | None, **kwargs: Any
) -> BaseKeyValueStore | None:
    """Create a suitable KeyValueStore based on a URI-like `location`.

    `location` supports:
    - single URI/path: `memory://spill?pool_size_bytes=...`, `valkey://...`, `gs://...`
    - layered string: `<uri1>;<uri2>[;<uri3>]` (ordered tiers)
    - layered config:
      `{"layers":[{"location":"memory://...", "max_bytes": 200_000_000}, "gs://bucket/pfx"]}`
    - direct list of layer URIs/config mappings

    Accepts:
    - file:///path or /path
    - s3://bucket[/prefix]
    - gs://bucket[/prefix]
    - valkey://connection
    - memory://pool-name
    - null://anything
    - layered combinations (max three layers)
    Query-string options:
    - `max_bytes`: capacity threshold for layer placement
    - `key_prefix` / `prefix`: key namespace prefix for that store/layer
    """
    if not location:
        return None

    if isinstance(location, BaseKeyValueStore):
        return location

    create_kwargs = dict(kwargs)
    root_prefix = create_kwargs.pop("key_prefix", None)

    if isinstance(location, Mapping):
        root_prefix = _merge_prefix(root_prefix, location.get("key_prefix", location.get("prefix")))
        layers = location.get("layers")
        if layers is not None:
            if isinstance(layers, (str, bytes, bytearray)):
                raise TypeError("`layers` must be a sequence of layer definitions")
            return _create_layered_store(
                list(layers), key_prefix=root_prefix, location="layered://config", **create_kwargs
            )

        mapped_location = location.get("location", location.get("uri"))
        if not mapped_location:
            raise ValueError("KV config mapping requires `location`/`uri` or `layers`")
        clean_location, _max_bytes, uri_prefix = _extract_uri_options(str(mapped_location))
        return _create_single_store(
            clean_location, key_prefix=_merge_prefix(root_prefix, uri_prefix), **create_kwargs
        )

    if isinstance(location, Sequence) and not isinstance(location, (str, bytes, bytearray)):
        return _create_layered_store(
            list(location), key_prefix=root_prefix, location="layered://sequence", **create_kwargs
        )

    location_str = str(location).strip()
    layered_parts = [part.strip() for part in location_str.split(";") if part.strip()]
    if len(layered_parts) > 1:
        return _create_layered_store(
            layered_parts,
            key_prefix=root_prefix,
            location="layered://delimited",
            **create_kwargs,
        )

    clean_location, _max_bytes, uri_prefix = _extract_uri_options(location_str)
    return _create_single_store(
        clean_location, key_prefix=_merge_prefix(root_prefix, uri_prefix), **create_kwargs
    )
