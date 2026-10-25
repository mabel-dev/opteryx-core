"""An evicted footer is re-read WITH the filesystem's credential.

`native_scan_rejection` acquires every remote footer in one authenticated batch and
then looks each one up in the parsed-footer cache. That cache is bounded, so on a
scan over many files a footer can be evicted between the two, and the lookup falls
back to a per-file fetch. The single-file C++ fetch carries no Authorization header,
so on a bearer-authenticated filesystem (GCS without GCS_SIGN_URLS) the fallback went
out anonymous and a private bucket answered 403:

    DatasetDataReadError: Unable to read a Parquet footer from storage: HTTP 403

These tests serve parquet files from a server that refuses any request without the
bearer token, and run the gate in a subprocess with PARQUET_FOOTER_CACHE_BYTES=1 so
every parsed footer is evicted the moment it is stored and every file takes the
fallback. The failure also must not put the object's URL in the user-facing message.
"""

import http.server
import os
import subprocess
import sys
import textwrap
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))

TOKEN = "Bearer footer-fallback-test-token"
N_FILES = 4


class _Private(http.server.BaseHTTPRequestHandler):
    """Serves `root` with Range support; 403 to any request lacking the bearer."""

    root = ""

    def log_message(self, *args):
        pass

    def _authorised(self):
        if self.headers.get("Authorization") == TOKEN:
            return True
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False

    def _file(self):
        with open(os.path.join(self.root, self.path.lstrip("/")), "rb") as f:
            return f.read()

    def do_HEAD(self):
        if not self._authorised():
            return
        data = self._file()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()

    def do_GET(self):
        if not self._authorised():
            return
        data = self._file()
        rng = self.headers.get("Range")
        if rng is None:
            status, body, content_range = 200, data, None
        else:
            start, end = rng.split("=", 1)[1].split("-")
            if start == "":                        # suffix range: last N bytes
                n = min(int(end), len(data))
                start, end = len(data) - n, len(data) - 1
            else:
                start, end = int(start), min(int(end), len(data) - 1)
            status, body = 206, data[start:end + 1]
            content_range = f"bytes {start}-{end}/{len(data)}"
        self.send_response(status)
        if content_range:
            self.send_header("Content-Range", content_range)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture(scope="module")
def served(tmp_path_factory):
    root = tmp_path_factory.mktemp("private_bucket")
    sizes = {}
    for i in range(N_FILES):
        name = f"part-{i}.parquet"
        pq.write_table(pa.table({"x": list(range(100 * i, 100 * i + 100))}), root / name)
        sizes[name] = os.path.getsize(root / name)

    handler = type("Handler", (_Private,), {"root": str(root)})
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        yield [f"{base}/{name}" for name in sizes], {f"{base}/{name}": size for name, size in sizes.items()}
    finally:
        httpd.shutdown()
        httpd.server_close()


def _run_gate(urls, sizes, auth_header):
    """Run the footer gate over `urls` in a process whose footer cache holds nothing."""
    script = textwrap.dedent(f"""
        import sys
        sys.path.insert(1, {REPO_ROOT!r})
        from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection

        class BearerFileSystem:
            # What GCS exposes with signing off: no rewrite hook, a bearer header.
            signs_urls = False

            def native_auth_header(self):
                return {auth_header!r}

        try:
            reason = native_scan_rejection({urls!r}, ["x"], ["int"], file_sizes={sizes!r},
                                           filesystem=BearerFileSystem())
        except Exception as err:
            print("RAISED", type(err).__name__, err)
        else:
            print("REASON", reason)
    """)
    env = dict(os.environ, PARQUET_FOOTER_CACHE_BYTES="1")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip().splitlines()[-1]


def test_evicted_footers_are_refetched_with_the_bearer(served):
    urls, sizes = served
    assert _run_gate(urls, sizes, TOKEN) == "REASON None"


def test_a_refused_footer_does_not_name_the_object(served):
    urls, sizes = served
    outcome = _run_gate(urls, sizes, "Bearer wrong-token")
    assert outcome.startswith("RAISED DatasetDataReadError"), outcome
    assert "access to the data file was denied" in outcome, outcome
    assert "127.0.0.1" not in outcome and "part-" not in outcome, outcome
