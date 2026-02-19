# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
opteryx/iops/messages.py
========================
Zero-pickle wire protocol for the EXEC ↔ IO worker pipe (§7 of design.md).

All control messages travel over ``multiprocessing.Pipe`` using
``send_bytes`` / ``recv_bytes`` + ``struct.pack`` / ``struct.unpack``.
No pickle, no marshal — fixed-width binary headers only.

Wire formats
------------
EXEC → IO worker:
  ReadRequest  –  ``_REQ.pack(request_id, path_len) + path_bytes``
  SHUTDOWN     –  empty bytes ``b""``

IO worker → EXEC:
  Startup OK   –  ``b"R"`` (single byte)
  Startup ERR  –  ``b"E"`` + utf-8 error message
  ReadComplete  –  ``_COMP.pack(request_id, slot_id, length,
                                gcs_latency_s, error_len) + error_bytes``

ReadComplete layout (``_COMP``):
  request_id    int32   4 B   monotonic request counter
  slot_id       int32   4 B   ring slot index; -1 on error
  length        uint32  4 B   payload bytes written into slot
  gcs_latency_s float32 4 B   wall-clock download time in seconds
  error_len     uint16  2 B   byte length of trailing error string (0 = ok)
  [error bytes] …      M B   utf-8 error description (only if error_len > 0)

Total on the happy path: 18 bytes per completion — no heap allocation,
no interpreter overhead.
"""

from __future__ import annotations

import struct
from typing import NamedTuple
from typing import Optional

# ── Wire structs ───────────────────────────────────────────────────────────────

# ReadRequest header: request_id (uint32) + path byte-length (uint16)
_REQ = struct.Struct(">IH")

# ReadComplete header: request_id (int32) + slot_id (int32) + length (uint32)
#                    + gcs_latency_s (float32) + error_len (uint16)
_COMP = struct.Struct(">iiIfH")


# ── Startup sentinels (single-byte or prefixed) ────────────────────────────────

READY_BYTES: bytes = b"R"  # worker → EXEC on successful startup
SHUTDOWN: bytes = b""  # EXEC → worker: drain and exit


def encode_startup_error(msg: str) -> bytes:
    return b"E" + msg.encode()


def decode_startup(data: bytes) -> str:
    """Return ``"READY"`` or raise ``RuntimeError`` with the error text."""
    if data == READY_BYTES:
        return "READY"
    raise RuntimeError(data[1:].decode(errors="replace"))


# ── ReadRequest encode / decode ────────────────────────────────────────────────


def encode_request(request_id: int, blob_path: str) -> bytes:
    path_b = blob_path.encode()
    return _REQ.pack(request_id, len(path_b)) + path_b


def decode_request(data: bytes) -> tuple[int, str]:
    request_id, path_len = _REQ.unpack_from(data)
    blob_path = data[_REQ.size : _REQ.size + path_len].decode()
    return request_id, blob_path


# ── ReadComplete encode / decode ───────────────────────────────────────────────


class ReadComplete(NamedTuple):
    """Decoded ReadComplete — used only inside the EXEC process after decoding."""

    request_id: int
    slot_id: int
    length: int
    gcs_latency_s: float = 0.0
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def encode_complete(
    request_id: int,
    slot_id: int,
    length: int,
    gcs_latency_s: float = 0.0,
    error: Optional[str] = None,
) -> bytes:
    err_b = error.encode() if error else b""
    return _COMP.pack(request_id, slot_id, length, gcs_latency_s, len(err_b)) + err_b


def decode_complete(data: bytes) -> ReadComplete:
    request_id, slot_id, length, gcs_latency_s, error_len = _COMP.unpack_from(data)
    error = data[_COMP.size :].decode(errors="replace") if error_len else None
    return ReadComplete(request_id, slot_id, length, gcs_latency_s, error)
