"""A failed HTTP call must never carry a presigned URL's credential in its message.

A presigned URL holds its credential in the query string (X-Amz-Signature,
X-Amz-Credential, X-Amz-Security-Token, X-Goog-Signature). An error message is
copied into job documents, service logs and API responses by code that has no
reason to think it holds one, so every hard-failure message names the object and
drops the query. These tests drive the real C++ client against a server that
answers 403 to everything.
"""

import http.server
import os
import sys
import threading

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.compiled.http_client import HttpClient
from opteryx.compiled.http_client import HttpStatusError

SIGNATURE = "0123456789abcdef0123456789abcdef"
CREDENTIAL = "AKIAEXAMPLEKEYID"
TOKEN = "session-token-value"
QUERY = f"X-Amz-Credential={CREDENTIAL}&X-Amz-Signature={SIGNATURE}&X-Amz-Security-Token={TOKEN}"


class _Refuse(http.server.BaseHTTPRequestHandler):
    def _refuse(self):
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_GET = _refuse
    do_HEAD = _refuse

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def url():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Refuse)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/lake/data/f1.parquet?{QUERY}"
    finally:
        server.shutdown()
        server.server_close()


def _assert_no_credential(message):
    for secret in (SIGNATURE, CREDENTIAL, TOKEN, "X-Amz-Signature"):
        assert secret not in message, f"{secret!r} leaked into: {message}"


def test_get_many_hard_failure_names_the_object_not_the_credential(url):
    with HttpClient() as client:
        with pytest.raises(HttpStatusError) as caught:
            client.get_many([(url, {})])

    assert caught.value.status == 403
    assert "/lake/data/f1.parquet" in str(caught.value)
    _assert_no_credential(str(caught.value))


def test_head_many_hard_failure_names_the_object_not_the_credential(url):
    with HttpClient() as client:
        with pytest.raises(HttpStatusError) as caught:
            client.head_many([(url, {})])

    assert caught.value.status == 403
    assert "/lake/data/f1.parquet" in str(caught.value)
    _assert_no_credential(str(caught.value))


def test_get_hard_failure_names_the_object_not_the_credential(url):
    with HttpClient() as client:
        with pytest.raises(HttpStatusError) as caught:
            client.get(url)

    assert caught.value.status == 403
    _assert_no_credential(str(caught.value))
