"""
Client for the oracle service. Stdlib only, so it runs on a host with no PyArrow.

Every failure raises OracleClientError: an unreachable service, a non-2xx answer, a
response that is not the schema version this client understands, or a response whose
sha256 does not match the bytes that were sent. There is no skip and no fallback.
"""

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request

from . import SCHEMA_VERSION

FORMATS = ("parquet", "orc", "avro")


class OracleClientError(RuntimeError):
    """`status` is the HTTP status, or None when the service could not be reached."""

    def __init__(self, message: str, status: int = None):
        super().__init__(message)
        self.status = status


def inspect_bytes(data: bytes, fmt: str, base_url: str, timeout: float = 120.0) -> dict:
    """POST file bytes to the oracle; returns the parsed canonical JSON."""
    if fmt not in FORMATS:
        raise OracleClientError("format must be one of %s, got %r" % (FORMATS, fmt))
    url = "%s/v1/inspect?%s" % (base_url.rstrip("/"), urllib.parse.urlencode({"format": fmt}))
    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise OracleClientError("oracle answered %d: %s" % (exc.code, exc.read().decode("utf-8", "replace")), exc.code) from exc
    except OSError as exc:  # URLError, refused/reset connections and timeouts are all OSErrors
        raise OracleClientError("oracle at %s is unreachable: %s" % (base_url, exc)) from exc
    document = json.loads(body)
    if document["schema_version"] != SCHEMA_VERSION:
        raise OracleClientError(
            "oracle speaks schema_version %r, this client understands %d" % (document["schema_version"], SCHEMA_VERSION)
        )
    sent = hashlib.sha256(data).hexdigest()
    if document["input"]["sha256"] != sent:
        raise OracleClientError("oracle read different bytes: sent sha256 %s, it hashed %s" % (sent, document["input"]["sha256"]))
    return document


def inspect_path(path: str, fmt: str, base_url: str, timeout: float = 120.0) -> dict:
    with open(path, "rb") as f:
        data = f.read()
    return inspect_bytes(data, fmt, base_url, timeout)


def healthz(base_url: str, timeout: float = 10.0) -> dict:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/healthz", timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise OracleClientError("oracle /healthz answered %d" % exc.code, exc.code) from exc
    except OSError as exc:
        raise OracleClientError("oracle at %s is unreachable: %s" % (base_url, exc)) from exc
