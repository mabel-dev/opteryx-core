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

LISTING is tried ANONYMOUSLY too (docs/ANONYMOUS_REMOTE_READS_DESIGN.md): `list_files`
asks the JSON API for the prefix with NO Authorization header, through the credential-free
`object_listing` module, so a glob over a public bucket works. A bucket that does not allow
anonymous listing answers 401/403 and the query fails saying so - it is NEVER retried with the
process's credentials. That is the rule this class exists to keep: READ_* never uses the
platform's identity. This module imports nothing from the credentialed filesystem.
"""


class AnonymousGcsFileSystem:
    """See module docstring for the full rationale."""

    def __init__(self):
        from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem
        from opteryx.connectors.io_systems.object_listing import GCS_API

        self._http = OpteryxHttpFileSystem()
        # Where listing requests go. A deployment constant (tests point it at a local
        # object store); it decides WHERE an anonymous request goes, never what it may read.
        self.api = GCS_API

    def list_file_infos(self, base_dir: str, recursive: bool = True) -> list:
        """The objects under `base_dir` (gs://bucket/prefix), listed anonymously."""
        from opteryx.connectors.io_systems.object_listing import anonymous_listing_limit
        from opteryx.connectors.io_systems.object_listing import list_gcs_objects

        def get(url, headers):
            # An anonymous request carries NO headers - the client call below cannot even express one.
            if headers:
                raise ValueError("an anonymous listing carries no request headers")
            return self._http.http_client.get(url)

        return list_gcs_objects(
            get,
            base_dir,
            recursive,
            headers={},
            anonymous=True,
            limit=anonymous_listing_limit(),
            api=self.api,
        )

    def list_files(self, base_dir: str, recursive: bool = True) -> list:
        return [info.path for info in self.list_file_infos(base_dir, recursive)]

    @staticmethod
    def _to_public_https_url(path: str) -> str:
        import urllib.parse

        from opteryx.utils import paths as path_utils

        stripped = path[5:] if path.startswith("gs://") else path
        stripped = stripped[6:] if stripped.startswith("gcs://") else stripped
        bucket, _, _, _ = path_utils.get_parts(stripped)
        object_path = urllib.parse.quote(stripped[len(bucket) + 1 :], safe="")
        return f"https://storage.googleapis.com/{bucket}/{object_path}"

    # ── Native scan-path contract ───────────────────────────────────────────
    #
    # The same contract AnonymousS3FileSystem exposes: a native Source fetches the
    # URL `rewrite_to_signed_url` returns with the header `native_auth_header`
    # returns. Here the "signed" URL is the plain, credential-free public object URL
    # and there is no header - the native fetch is anonymous end to end, exactly as
    # open_input_file below is.

    @property
    def signs_urls(self) -> bool:
        return True

    def native_auth_header(self):
        return None

    def rewrite_to_signed_url(self, path: str, expiry_seconds: int = 3600) -> str:
        return self._to_public_https_url(path)

    def open_input_file(self, path: str, columns=None, filters=None):
        return self._http.open_input_file(self._to_public_https_url(path), columns=columns, filters=filters)

    def open_input_stream(self, path: str, columns=None, filters=None):
        return self._http.open_input_stream(self._to_public_https_url(path), columns=columns, filters=filters)

    def read_ranges(self, path: str, ranges):
        return self._http.read_ranges(self._to_public_https_url(path), ranges)

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
