# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Anonymous (no platform credential) GCS access for bare dataset functions.

READ_JSONL, READ_PARQUET, and READ_CSV, used as table functions (e.g.
`SELECT * FROM READ_JSONL('gs://bucket/file.jsonl')`), are bare dataset functions
with no per-query authorization layer -- unlike catalog-backed table scans
(opteryx.planner.binder.dataset's visit_scan), which are gated by
`can_perform_action` before any connector is even opened, these take a path (or
glob) straight out of the SQL text. OpteryxGcsFileSystem authenticates with this
PROCESS's own ambient service-account credentials
(opteryx.connectors.io_systems.gcs_filesystem.get_storage_credentials ->
google.auth.default()), not anything scoped to the requesting user -- so it must
never be used for a user-supplied path here. Doing so would let arbitrary SQL text
read or list anything that credential can reach, regardless of whether the
requesting user is authorized to see it (an IDOR: the "object" is an
attacker-controlled bucket/path, with no ownership/authorization check tying it to
the caller).

Instead, a `gs://bucket/object` (or `gcs://...`) path is translated to its public
`https://storage.googleapis.com/bucket/object` form and fetched with NO
Authorization header at all, via OpteryxHttpFileSystem. GCS's own object-level IAM
decides the outcome: a public object is read; a private one 403s from GCS itself --
Opteryx makes no allow/deny decision of its own.

Deliberately has no `list_files`: GCS bucket LISTING is a separate IAM permission
from object GET and is not assumed granted anonymously, so glob patterns over
gs:// are rejected outright by callers (see the READ_JSONL/READ_PARQUET/READ_CSV
binder branches) rather than silently escalating to an authenticated listing call.
"""


class AnonymousGcsFileSystem:
    """See module docstring for the full rationale."""

    def __init__(self):
        from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem

        self._http = OpteryxHttpFileSystem()

    @staticmethod
    def _to_public_https_url(path: str) -> str:
        import urllib.parse

        from opteryx.utils import paths as path_utils

        stripped = path[5:] if path.startswith("gs://") else path
        stripped = stripped[6:] if stripped.startswith("gcs://") else stripped
        bucket, _, _, _ = path_utils.get_parts(stripped)
        object_path = urllib.parse.quote(stripped[len(bucket) + 1 :], safe="")
        return f"https://storage.googleapis.com/{bucket}/{object_path}"

    def open_input_file(self, path: str, columns=None, filters=None):
        return self._http.open_input_file(self._to_public_https_url(path), columns=columns, filters=filters)

    def open_input_stream(self, path: str, columns=None, filters=None):
        return self._http.open_input_stream(self._to_public_https_url(path), columns=columns, filters=filters)

    def get_file_info(self, paths):
        if isinstance(paths, str):
            return self._http.get_file_info(self._to_public_https_url(paths))
        return self._http.get_file_info([self._to_public_https_url(p) for p in paths])


def anonymous_gcs_filesystem() -> "AnonymousGcsFileSystem":
    """A filesystem for bare dataset functions' `gs://` support that never uses
    platform credentials. See the module docstring for the full rationale.

    Used at both bind time (schema/manifest resolution) and execution time (the
    real data read) by whichever caller needs it, so a `gs://` query can never
    authenticate at one stage and go anonymous at the other.
    """
    return AnonymousGcsFileSystem()
