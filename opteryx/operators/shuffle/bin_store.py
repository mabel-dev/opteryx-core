# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

import json
import secrets
from collections import defaultdict
from threading import RLock
from typing import Any

_MANIFEST_VERSION = 1
_SCOPE_VERSION = 1


class BinStore:
    """
    Append-only shuffle spill adapter backed by a scoped KV store.

    The underlying KV store is expected to support `set/get/delete` with
    context keywords (`query_id`, `operator_id`).
    """

    def __init__(self, store):
        if store is None:
            raise ValueError("store is required")
        self._store = store
        self._lock = RLock()
        self._chunk_sequences: dict[tuple[str, int], int] = defaultdict(int)
        self._manifest_sequences: dict[str, int] = defaultdict(int)

    @staticmethod
    def _require_context(query_id: str, operator_id: str) -> dict[str, str]:
        if not query_id or not str(query_id).strip():
            raise ValueError("query_id is required")
        if not operator_id or not str(operator_id).strip():
            raise ValueError("operator_id is required")
        return {"query_id": str(query_id), "operator_id": str(operator_id)}

    @staticmethod
    def _normalize_key(raw_key: str | bytes | bytearray | memoryview) -> bytes:
        if isinstance(raw_key, str):
            value = raw_key.strip("/")
            if not value:
                raise ValueError("key cannot be empty")
            return value.encode("utf-8")
        if isinstance(raw_key, (bytes, bytearray, memoryview)):
            value = bytes(raw_key).strip(b"/")
            if not value:
                raise ValueError("key cannot be empty")
            return value
        raise TypeError("key must be str or bytes-like")

    @staticmethod
    def _decode_key(raw_key: str | bytes | bytearray | memoryview) -> str:
        return BinStore._normalize_key(raw_key).decode("utf-8")

    @staticmethod
    def _scope_index_key(scope_key: str | bytes | bytearray | memoryview) -> bytes:
        scope_name = BinStore._decode_key(scope_key)
        return f"scope/{scope_name}/index".encode("utf-8")

    @staticmethod
    def _manifest_index_key(bin_key: str | bytes | bytearray | memoryview) -> bytes:
        bin_name = BinStore._decode_key(bin_key)
        return f"{bin_name}/manifest/index".encode("utf-8")

    @staticmethod
    def _to_json_bytes(payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, separators=(",", ":"), sort_keys=False).encode("utf-8")

    @staticmethod
    def _from_json_bytes(
        payload: bytes | memoryview | None, default: dict[str, Any]
    ) -> dict[str, Any]:
        if payload is None:
            return dict(default)
        decoded = json.loads(bytes(payload).decode("utf-8"))
        if not isinstance(decoded, dict):
            return dict(default)
        return decoded

    def _track_scope_key(
        self,
        scope_key: str | bytes | bytearray | memoryview | None,
        tracked_key: str | bytes | bytearray | memoryview,
        context: dict[str, str],
    ) -> None:
        if scope_key is None:
            return
        scope_index_key = self._scope_index_key(scope_key)
        tracked_key_text = self._decode_key(tracked_key)

        if self._decode_key(scope_index_key) == tracked_key_text:
            return

        existing_payload = self._store.get(scope_index_key, **context)
        existing = self._from_json_bytes(existing_payload, {"version": _SCOPE_VERSION, "keys": []})
        keys = existing.setdefault("keys", [])
        if tracked_key_text not in keys:
            keys.append(tracked_key_text)
        existing["version"] = _SCOPE_VERSION
        self._store.set(scope_index_key, self._to_json_bytes(existing), **context)

    def put_chunk(
        self,
        raw_key: str | bytes | bytearray | memoryview,
        payload: bytes | bytearray | memoryview,
        *,
        query_id: str,
        operator_id: str,
        scope_key: str | bytes | bytearray | memoryview | None = None,
    ) -> None:
        context = self._require_context(query_id, operator_id)
        key_bytes = self._normalize_key(raw_key)
        self._store.set(key_bytes, bytes(payload), **context)
        self._track_scope_key(scope_key, key_bytes, context)

    def get_chunk(
        self,
        raw_key: str | bytes | bytearray | memoryview,
        *,
        query_id: str,
        operator_id: str,
    ) -> bytes | memoryview | None:
        context = self._require_context(query_id, operator_id)
        key_bytes = self._normalize_key(raw_key)
        return self._store.get(key_bytes, **context)

    def append_chunk(
        self,
        pass_id: str,
        bin_id: int,
        payload: bytes | bytearray | memoryview,
        *,
        query_id: str,
        operator_id: str,
        scope_key: str | bytes | bytearray | memoryview | None = None,
    ) -> dict[str, Any]:
        if not pass_id or not str(pass_id).strip():
            raise ValueError("pass_id is required")
        if bin_id < 0:
            raise ValueError("bin_id must be zero or positive")

        with self._lock:
            seq_key = (str(pass_id), int(bin_id))
            chunk_seq = self._chunk_sequences[seq_key]
            self._chunk_sequences[seq_key] = chunk_seq + 1

        random_suffix = secrets.token_hex(2)
        chunk_key = f"pass/{pass_id}/bin/{bin_id}/chunk/{chunk_seq:020d}-{random_suffix}"

        self.put_chunk(
            chunk_key,
            payload,
            query_id=query_id,
            operator_id=operator_id,
            scope_key=scope_key,
        )
        return {
            "chunk_key": chunk_key,
            "chunk_seq": chunk_seq,
            "size_bytes": len(bytes(payload)),
        }

    def append_manifest(
        self,
        bin_key: str | bytes | bytearray | memoryview,
        chunk_meta: dict[str, Any],
        *,
        query_id: str,
        operator_id: str,
        scope_key: str | bytes | bytearray | memoryview | None = None,
    ) -> dict[str, Any]:
        context = self._require_context(query_id, operator_id)
        bin_name = self._decode_key(bin_key)
        manifest_index_key = self._manifest_index_key(bin_name)

        with self._lock:
            manifest_seq = self._manifest_sequences[bin_name]
            self._manifest_sequences[bin_name] = manifest_seq + 1

            segment_key = f"{bin_name}/manifest/{manifest_seq:020d}".encode("utf-8")
            segment_payload = self._to_json_bytes(
                {
                    "version": _MANIFEST_VERSION,
                    "segment_seq": manifest_seq,
                    "chunk_meta": chunk_meta,
                }
            )
            self._store.set(segment_key, segment_payload, **context)
            self._track_scope_key(scope_key, segment_key, context)

            existing_index_payload = self._store.get(manifest_index_key, **context)
            existing_index = self._from_json_bytes(
                existing_index_payload, {"version": _MANIFEST_VERSION, "segments": []}
            )
            segments = existing_index.setdefault("segments", [])
            segment_key_text = segment_key.decode("utf-8")
            if segment_key_text not in segments:
                segments.append(segment_key_text)
            existing_index["version"] = _MANIFEST_VERSION
            self._store.set(manifest_index_key, self._to_json_bytes(existing_index), **context)
            self._track_scope_key(scope_key, manifest_index_key, context)

        return {"segment_key": segment_key.decode("utf-8"), "segment_seq": manifest_seq}

    def iter_manifest(
        self,
        bin_key: str | bytes | bytearray | memoryview,
        *,
        query_id: str,
        operator_id: str,
    ) -> list[dict[str, Any]]:
        context = self._require_context(query_id, operator_id)
        manifest_index_key = self._manifest_index_key(bin_key)

        existing_index_payload = self._store.get(manifest_index_key, **context)
        if existing_index_payload is None:
            return []

        existing_index = self._from_json_bytes(
            existing_index_payload, {"version": _MANIFEST_VERSION, "segments": []}
        )
        entries: list[dict[str, Any]] = []
        for segment_key in existing_index.get("segments", []):
            payload = self._store.get(self._normalize_key(segment_key), **context)
            if payload is None:
                continue
            parsed = self._from_json_bytes(payload, {})
            chunk_meta = parsed.get("chunk_meta")
            if isinstance(chunk_meta, dict):
                entries.append(chunk_meta)
        return entries

    def delete_scope(
        self,
        scope_key: str | bytes | bytearray | memoryview,
        *,
        query_id: str,
        operator_id: str,
    ) -> int:
        context = self._require_context(query_id, operator_id)
        scope_index_key = self._scope_index_key(scope_key)
        scope_payload = self._store.get(scope_index_key, **context)
        if scope_payload is None:
            return 0

        parsed = self._from_json_bytes(scope_payload, {"version": _SCOPE_VERSION, "keys": []})
        deleted = 0
        for raw_key in parsed.get("keys", []):
            if not raw_key:
                continue
            self._store.delete(self._normalize_key(raw_key), **context)
            deleted += 1
        self._store.delete(scope_index_key, **context)
        return deleted
