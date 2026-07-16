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
from opteryx.managers.kvstores.memory_kv_store import ensure_memory_pool
from opteryx.managers.kvstores.null_cache import NullCache
from opteryx.managers.kvstores.scoped_kv_store import ScopedKeyValueStore
from opteryx.managers.kvstores.valkey import ValkeyCache

_REQUIRED_CONTEXT_FIELDS = ("query_id", "operator_id")

# Schemes the `valkey` client's own `from_url`/`parse_url` accepts (TCP + TLS, under
# either the Valkey or Redis-compatible names). `unix://` is deliberately excluded here —
# no call site constructs one today, and adding it blind would be an unreviewed scope
# increase, not a fix for the rediss:// gap this set closes.
_VALKEY_SCHEMES = ("valkey", "valkeys", "redis", "rediss")

# Distinguishes "caller said nothing" (scope by query, the safe default) from
# "caller explicitly asked for no scoping" (a content-addressed store).
_UNSET = object()


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


def _render_prefix_template(prefix: bytes | str | None, query_id: str | None = None) -> str | None:
    normalized = _normalize_prefix(prefix)
    if normalized is None:
        return None
    if query_id is None:
        return normalized
    return normalized.replace("{query_id}", str(query_id))


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


def _coerce_layer_spec(
    layer_spec: Any, query_id: str | None = None
) -> tuple[str, int | None, str | None]:
    if isinstance(layer_spec, str):
        clean_location, max_bytes, layer_prefix = _extract_uri_options(layer_spec.strip())
        return clean_location, max_bytes, _render_prefix_template(layer_prefix, query_id=query_id)

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
    layer_prefix = _render_prefix_template(layer_prefix, query_id=query_id)
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
    if scheme in ("gs", "gcs"):
        return GCSKeyValueStore(location, key_prefix=key_prefix, **kwargs)
    if scheme in _VALKEY_SCHEMES:
        # The `valkey` client itself accepts valkey(s):// and redis(s):// (TLS variants
        # `valkeys`/`rediss`) — see its own `parse_url`. Our factory only special-cases
        # `valkey` for a bare `host:port` location (no scheme to preserve); every full
        # URL, whichever of the four schemes it uses, is passed through unchanged so the
        # client's own scheme dispatch (including which one selects TLS) is untouched.
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
    query_id: str | None = None,
    **kwargs: Any,
) -> LayeredKeyValueStore:
    if not layer_specs:
        raise ValueError("Layered KV configuration requires at least one layer")
    if len(layer_specs) > 3:
        raise ValueError("Layered KV configuration supports up to three layers")

    layers: list[tuple[BaseKeyValueStore, int | None]] = []
    for raw_layer in layer_specs:
        layer_location, max_bytes, layer_prefix = _coerce_layer_spec(raw_layer, query_id=query_id)
        layer_store = _create_single_store(layer_location, key_prefix=layer_prefix, **kwargs)
        layers.append((layer_store, max_bytes))

    return LayeredKeyValueStore(layers=layers, location=location, key_prefix=key_prefix)


def _load_kv_defaults_from_config() -> tuple[Any, str | None]:
    from opteryx import config as opteryx_config

    configured_layers = getattr(opteryx_config, "KVSTORE_LAYERS", None)
    configured_location = getattr(opteryx_config, "KVSTORE_LOCATION", "")
    configured_prefix = getattr(opteryx_config, "KVSTORE_KEY_PREFIX", None)

    if configured_layers:
        return configured_layers, _normalize_prefix(configured_prefix)
    if configured_location:
        return configured_location, _normalize_prefix(configured_prefix)
    return None, _normalize_prefix(configured_prefix)


def _wrap_with_context_enforcement(
    store: BaseKeyValueStore,
    enforced_context_fields: list[str] | tuple[str, ...] | None,
) -> BaseKeyValueStore:
    if not enforced_context_fields:
        return store
    if isinstance(store, ScopedKeyValueStore):
        return store
    return ScopedKeyValueStore(store=store, required_context_fields=list(enforced_context_fields))


def _extract_all_locations(spec: Any) -> list[str]:
    if spec is None:
        return []
    if isinstance(spec, Mapping):
        layers = spec.get("layers")
        if layers is not None:
            if isinstance(layers, (str, bytes, bytearray)):
                raise TypeError("`layers` must be a sequence of layer definitions")
            locations: list[str] = []
            for layer in layers:
                clean_location, _max_bytes, _prefix = _coerce_layer_spec(layer, query_id=None)
                locations.append(clean_location)
            return locations
        mapped_location = spec.get("location", spec.get("uri"))
        if not mapped_location:
            return []
        clean_location, _max_bytes, _prefix = _extract_uri_options(str(mapped_location))
        return [clean_location]

    if isinstance(spec, Sequence) and not isinstance(spec, (str, bytes, bytearray)):
        locations: list[str] = []
        for layer in spec:
            clean_location, _max_bytes, _prefix = _coerce_layer_spec(layer, query_id=None)
            locations.append(clean_location)
        return locations

    location_str = str(spec).strip()
    if not location_str:
        return []

    layered_parts = [part.strip() for part in location_str.split(";") if part.strip()]
    if len(layered_parts) > 1:
        locations: list[str] = []
        for part in layered_parts:
            clean_location, _max_bytes, _prefix = _extract_uri_options(part)
            locations.append(clean_location)
        return locations

    clean_location, _max_bytes, _prefix = _extract_uri_options(location_str)
    return [clean_location]


def initialize_global_memory_pools(location: Any | None = None, **kwargs: Any) -> list[str]:
    """
    Prewarm global memory:// pools from explicit or configured KV layer definitions.
    """
    if location is None:
        location, _configured_prefix = _load_kv_defaults_from_config()

    initialized: list[str] = []
    for layer_location in _extract_all_locations(location):
        pool_name = ensure_memory_pool(layer_location, **kwargs)
        if pool_name is not None:
            initialized.append(pool_name)
    return initialized


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
    - gs://bucket[/prefix]
    - valkey://connection, valkeys://connection (TLS), redis://connection, rediss://connection (TLS)
    - memory://pool-name
    - null://anything
    - layered combinations (max three layers)
    Query-string options:
    - `max_bytes`: capacity threshold for layer placement
    - `key_prefix` / `prefix`: key namespace prefix for that store/layer

    `enforce_context_fields` selects the key-scoping policy. It defaults to
    `_REQUIRED_CONTEXT_FIELDS`, which namespaces every key by query and operator --
    correct for per-query scratch/spill, where one query must never read another's
    bytes. Pass an empty value to opt out and get a *content-addressed* store, whose
    keys are global: the caller is then asserting the key already identifies the
    bytes uniquely and immutably, so a hit across queries is not a leak but the whole
    point (see the manifest cache).
    """
    create_kwargs = dict(kwargs)
    enforced_context_fields = create_kwargs.pop("enforce_context_fields", _UNSET)
    if enforced_context_fields is _UNSET:
        enforced_context_fields = _REQUIRED_CONTEXT_FIELDS

    if isinstance(location, BaseKeyValueStore):
        return _wrap_with_context_enforcement(location, enforced_context_fields)

    query_id = create_kwargs.pop("query_id", None)
    root_prefix = _render_prefix_template(create_kwargs.pop("key_prefix", None), query_id=query_id)

    if not location:
        location, configured_prefix = _load_kv_defaults_from_config()
        if not location:
            return None
        root_prefix = _merge_prefix(
            root_prefix, _render_prefix_template(configured_prefix, query_id=query_id)
        )

    if isinstance(location, Mapping):
        root_prefix = _merge_prefix(
            root_prefix,
            _render_prefix_template(
                location.get("key_prefix", location.get("prefix")), query_id=query_id
            ),
        )
        layers = location.get("layers")
        if layers is not None:
            if isinstance(layers, (str, bytes, bytearray)):
                raise TypeError("`layers` must be a sequence of layer definitions")
            store = _create_layered_store(
                list(layers),
                key_prefix=root_prefix,
                location="layered://config",
                query_id=query_id,
                **create_kwargs,
            )
            return _wrap_with_context_enforcement(store, enforced_context_fields)

        mapped_location = location.get("location", location.get("uri"))
        if not mapped_location:
            raise ValueError("KV config mapping requires `location`/`uri` or `layers`")
        clean_location, _max_bytes, uri_prefix = _extract_uri_options(str(mapped_location))
        store = _create_single_store(
            clean_location,
            key_prefix=_merge_prefix(
                root_prefix, _render_prefix_template(uri_prefix, query_id=query_id)
            ),
            **create_kwargs,
        )
        return _wrap_with_context_enforcement(store, enforced_context_fields)

    if isinstance(location, Sequence) and not isinstance(location, (str, bytes, bytearray)):
        store = _create_layered_store(
            list(location),
            key_prefix=root_prefix,
            location="layered://sequence",
            query_id=query_id,
            **create_kwargs,
        )
        return _wrap_with_context_enforcement(store, enforced_context_fields)

    location_str = str(location).strip()
    layered_parts = [part.strip() for part in location_str.split(";") if part.strip()]
    if len(layered_parts) > 1:
        store = _create_layered_store(
            layered_parts,
            key_prefix=root_prefix,
            location="layered://delimited",
            query_id=query_id,
            **create_kwargs,
        )
        return _wrap_with_context_enforcement(store, enforced_context_fields)

    clean_location, _max_bytes, uri_prefix = _extract_uri_options(location_str)
    store = _create_single_store(
        clean_location,
        key_prefix=_merge_prefix(
            root_prefix, _render_prefix_template(uri_prefix, query_id=query_id)
        ),
        **create_kwargs,
    )
    return _wrap_with_context_enforcement(store, enforced_context_fields)
