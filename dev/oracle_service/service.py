"""
The oracle's HTTP layer (stdlib only).

    POST /v1/inspect?format=parquet|orc|avro      body = the raw file bytes
    GET  /healthz

Every failure is a non-2xx response with a JSON error body; a response is either the
complete canonical description or an error, never a partial answer.
"""

import hashlib
import json
import threading
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import SCHEMA_VERSION, avro_oracle, orc_oracle, parquet_oracle
from .errors import BadRequest, BodyTooLarge, LengthRequired, OracleError

FORMATS = {"parquet": parquet_oracle, "orc": orc_oracle, "avro": avro_oracle}
DEFAULT_MAX_BODY_BYTES = 64 * 1024 * 1024


def readers() -> dict:
    return {fmt: module.READER for fmt, module in FORMATS.items()}


def inspect(fmt: str, data: bytes) -> dict:
    """The complete response document for one file."""
    body = FORMATS[fmt].inspect(data)
    return {
        "schema_version": SCHEMA_VERSION,
        "format": fmt,
        "input": {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()},
        "reader": FORMATS[fmt].READER,
        **body,
    }


class OracleServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple, max_body_bytes: int):
        super().__init__(address, OracleHandler)
        self.max_body_bytes = max_body_bytes


class OracleHandler(BaseHTTPRequestHandler):
    # HTTP/1.0: one request per connection, so an early error response never desyncs a stream.
    server_version = "OpteryxOracle/%d" % SCHEMA_VERSION

    def _send(self, status: int, document: dict) -> None:
        payload = json.dumps(document, allow_nan=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_error(self, status: int, kind: str, message: str, detail: str = "") -> None:
        error = {"kind": kind, "message": message}
        if detail:
            error["traceback"] = detail
        self._send(status, {"schema_version": SCHEMA_VERSION, "error": error})

    def do_GET(self) -> None:
        if urllib.parse.urlsplit(self.path).path != "/healthz":
            self._send_error(404, "not_found", "unknown path %s" % self.path)
            return
        self._send(
            200,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "ok",
                "max_body_bytes": self.server.max_body_bytes,
                "readers": readers(),
            },
        )

    def do_POST(self) -> None:
        url = urllib.parse.urlsplit(self.path)
        if url.path != "/v1/inspect":
            self._send_error(404, "not_found", "unknown path %s" % self.path)
            return
        # Everything from here converts a failure into a status; nothing is swallowed.
        try:
            fmt = self._format(url.query)
            data = self._body()
            document = inspect(fmt, data)
        except OracleError as exc:
            self._send_error(exc.status, exc.kind, str(exc))
            return
        except Exception as exc:  # reader failure: report the reader's own text, loudly
            self._send_error(422, "reader_error", "%s: %s" % (type(exc).__name__, exc), traceback.format_exc())
            return
        self._send(200, document)

    def _format(self, query: str) -> str:
        values = urllib.parse.parse_qs(query).get("format", [])
        if len(values) != 1:
            raise BadRequest("exactly one ?format= is required, one of %s" % sorted(FORMATS))
        if values[0] not in FORMATS:
            raise BadRequest("unknown format %r; expected one of %s" % (values[0], sorted(FORMATS)))
        return values[0]

    def _body(self) -> bytes:
        if "Transfer-Encoding" in self.headers:
            raise LengthRequired("chunked uploads are not supported; send Content-Length")
        header = self.headers.get("Content-Length")
        if header is None:
            raise LengthRequired("Content-Length is required")
        if not header.isdigit():
            raise BadRequest("Content-Length %r is not a non-negative integer" % header)
        length = int(header)
        if length > self.server.max_body_bytes:
            raise BodyTooLarge("body of %d bytes exceeds the %d byte limit" % (length, self.server.max_body_bytes))
        if length == 0:
            raise BadRequest("empty body")
        data = self.rfile.read(length)
        if len(data) != length:
            raise BadRequest("body ended after %d of %d bytes" % (len(data), length))
        return data


def start_in_thread(host: str, port: int, max_body_bytes: int) -> OracleServer:
    """Serve on a background thread (port 0 = ephemeral); the caller shuts it down."""
    server = OracleServer((host, port), max_body_bytes)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
