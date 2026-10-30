# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A local object store for tests that must prove what goes over the wire: serves GCS JSON-API and S3
ListObjectsV2 listings and plain object GETs from a dict, RECORDS every request (path, query, headers),
and can be told to deny everything."""

import http.server
import json
import threading
import urllib.parse

UPDATED = "2026-10-25T21:02:08.697Z"


def s3_page(keys, truncated=False, token=None):
    contents = "".join(
        f"<Contents><Key>{k}</Key><Size>{len(k)}</Size><LastModified>{UPDATED}</LastModified></Contents>" for k in keys
    )
    more = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>" if truncated else "<IsTruncated>false</IsTruncated>"
    return f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{contents}{more}</ListBucketResult>'.encode()


class Store(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.objects = {}      # "bucket/key" -> bytes
        self.requests = []     # (path, query, headers)
        self.deny = False
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def base(self):
        return f"http://127.0.0.1:{self.server_address[1]}"


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):  # pragma: no cover
        pass

    def do_GET(self):
        parts = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parts.query)
        self.server.requests.append((parts.path, parts.query, dict(self.headers)))
        if self.server.deny:
            return self._send(403, b"denied")
        if parts.path.startswith("/storage/v1/b/"):          # GCS JSON API listing
            bucket = parts.path.split("/")[4]
            prefix = query.get("prefix", [""])[0]
            names = sorted(k.split("/", 1)[1] for k in self.server.objects if k.startswith(bucket + "/") and k.split("/", 1)[1].startswith(prefix))
            body = {"items": [{"name": n, "size": str(len(self.server.objects[f"{bucket}/{n}"])), "updated": UPDATED} for n in names]}
            return self._send(200, json.dumps(body).encode())
        if "list-type" in query:                              # S3 ListObjectsV2, path-style
            bucket = parts.path.strip("/").split("/")[0]
            prefix = query.get("prefix", [""])[0]
            keys = sorted(k.split("/", 1)[1] for k in self.server.objects if k.startswith(bucket + "/") and k.split("/", 1)[1].startswith(prefix))
            return self._send(200, s3_page(keys))
        data = self.server.objects.get(parts.path.lstrip("/"))
        self._send(200 if data is not None else 404, data or b"")

    def do_HEAD(self):
        parts = urllib.parse.urlsplit(self.path)
        self.server.requests.append((parts.path, parts.query, dict(self.headers)))
        data = None if self.server.deny else self.server.objects.get(parts.path.lstrip("/"))
        self.send_response(403 if self.server.deny else 200 if data is not None else 404)
        self.send_header("Content-Length", str(len(data or b"")))
        self.end_headers()

    def _send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


