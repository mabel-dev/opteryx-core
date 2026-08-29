"""
Layered key-value store.

Routes writes across a small number of ordered KV store layers using per-layer
capacity thresholds. Reads use key-placement metadata first, then fallback scan.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from threading import RLock
from typing import Iterable
from typing import Union

from opteryx import config
from opteryx.managers.kvstores.base_kv_store import BaseKeyValueStore

logger = logging.getLogger(__name__)


@dataclass
class _LayerState:
    store: BaseKeyValueStore
    max_bytes: int | None
    used_bytes: int = 0


class LayeredKeyValueStore(BaseKeyValueStore):
    """
    Multi-layer KV store with ordered tier fallback.

    Layers are evaluated in order for writes. The first layer that can accept
    the payload (threshold and backend acceptance) receives the key.
    """

    def __init__(
        self,
        layers: list[tuple[BaseKeyValueStore, int | None]],
        location: str | None = None,
        key_prefix: bytes | str | None = None,
    ):
        if not layers:
            raise ValueError("LayeredKeyValueStore requires at least one layer")
        if len(layers) > 3:
            raise ValueError("LayeredKeyValueStore supports up to three layers")

        self._layers = [
            _LayerState(store=store, max_bytes=max_bytes) for store, max_bytes in layers
        ]
        self._placements: dict[bytes, tuple[int, int]] = {}
        self._lock = RLock()
        super().__init__(location or "layered://", key_prefix=key_prefix)

    def _can_fit(self, layer_index: int, payload_size: int, existing_size: int | None) -> bool:
        layer = self._layers[layer_index]
        if layer.max_bytes is None:
            return True

        projected = layer.used_bytes + payload_size
        if existing_size is not None:
            projected -= existing_size
        return projected <= layer.max_bytes

    def get(self, key: bytes) -> Union[bytes, None]:
        normalized_key = self._normalize_key(key)

        with self._lock:
            placement = self._placements.get(normalized_key)

        if placement is not None:
            layer_index, _ = placement
            value = self._layers[layer_index].store.get(normalized_key)
            if value is not None:
                return bytes(value)
            with self._lock:
                self._placements.pop(normalized_key, None)

        for layer_index, layer in enumerate(self._layers):
            if placement is not None and layer_index == placement[0]:
                continue
            value = layer.store.get(normalized_key)
            if value is None:
                continue
            payload = bytes(value)
            with self._lock:
                if normalized_key not in self._placements:
                    self._placements[normalized_key] = (layer_index, len(payload))
                    if layer.max_bytes is not None:
                        layer.used_bytes += len(payload)
            return payload
        return None

    def set(self, key: bytes, value: bytes) -> None:
        normalized_key = self._normalize_key(key)
        payload = bytes(value)
        payload_size = len(payload)
        last_error = None

        with self._lock:
            existing = self._placements.get(normalized_key)

            for layer_index, layer in enumerate(self._layers):
                existing_size = existing[1] if (existing and existing[0] == layer_index) else None
                if not self._can_fit(layer_index, payload_size, existing_size):
                    continue

                if (
                    getattr(layer.store, "_consecutive_failures", 0)
                    >= config.MAX_CONSECUTIVE_CACHE_FAILURES
                ):
                    last_error = RuntimeError(
                        f"layer {layer_index} unavailable due to consecutive backend failures"
                    )
                    continue

                try:
                    layer.store.set(normalized_key, payload)
                except MemoryError as err:
                    last_error = err
                    continue
                except Exception as err:
                    last_error = err
                    continue

                if (
                    getattr(layer.store, "_consecutive_failures", 0)
                    >= config.MAX_CONSECUTIVE_CACHE_FAILURES
                ):
                    last_error = RuntimeError(
                        f"layer {layer_index} unavailable due to consecutive backend failures"
                    )
                    continue

                if existing is not None and existing[0] != layer_index:
                    old_layer = self._layers[existing[0]]
                    if old_layer.max_bytes is not None:
                        old_layer.used_bytes = max(0, old_layer.used_bytes - existing[1])
                    try:
                        old_layer.store.delete(normalized_key)
                    except Exception as err:
                        logger.debug(f"Failed to delete from old layer: {err}")

                if layer.max_bytes is not None:
                    if existing is not None and existing[0] == layer_index:
                        layer.used_bytes = max(0, layer.used_bytes - existing[1])
                    layer.used_bytes += payload_size

                self._placements[normalized_key] = (layer_index, payload_size)
                return

        if last_error is not None:
            raise MemoryError(
                f"unable to place key in layered kv store: {last_error}"
            ) from last_error
        raise MemoryError("unable to place key in layered kv store: all layers are at capacity")

    def contains(self, keys: Iterable) -> Iterable:
        key_list = list(keys)
        return [key for key in key_list if self.get(key) is not None]

    def delete(self, key: bytes) -> None:
        normalized_key = self._normalize_key(key)

        with self._lock:
            placement = self._placements.pop(normalized_key, None)
            if placement is not None:
                layer_index, size = placement
                layer = self._layers[layer_index]
                if layer.max_bytes is not None:
                    layer.used_bytes = max(0, layer.used_bytes - size)
                try:
                    layer.store.delete(normalized_key)
                except Exception as err:
                    logger.debug(f"Failed to delete from layer {layer_index}: {err}")
                return

        for layer_index, layer in enumerate(self._layers):
            try:
                layer.store.delete(normalized_key)
            except Exception as err:
                logger.debug(f"Failed to delete from layer {layer_index}: {err}")

    def touch(self, key: bytes):
        normalized_key = self._normalize_key(key)
        with self._lock:
            placement = self._placements.get(normalized_key)

        if placement is not None:
            try:
                self._layers[placement[0]].store.touch(normalized_key)
            except Exception as err:
                logger.debug(f"Failed to touch in layer {placement[0]}: {err}")
            return None

        for layer_index, layer in enumerate(self._layers):
            try:
                layer.touch(normalized_key)
            except Exception as err:
                logger.debug(f"Failed to touch in layer {layer_index}: {err}")

    # Testing and telemetry helpers.
    def layer_usage_bytes(self) -> list[int]:
        with self._lock:
            return [layer.used_bytes for layer in self._layers]

    def layer_for_key(self, key: bytes) -> int | None:
        normalized_key = self._normalize_key(key)
        with self._lock:
            placement = self._placements.get(normalized_key)
        if placement is None:
            return None
        return placement[0]
