# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Anonymous (no platform credential) S3 access for bare dataset functions.

The S3 twin of anonymous_gcs_filesystem, for the same reason: READ_JSONL,
READ_PARQUET and READ_CSV take a path straight out of the SQL text with no
per-query authorization layer, and OpteryxS3FileSystem signs every request with
this PROCESS's own AWS credential chain (environment, shared file, web identity,
container or instance role). Handing it a user-supplied path would let arbitrary
SQL read or list anything that credential can reach - an IDOR with an
attacker-chosen bucket and key.

Instead an `s3://bucket/key` path is translated to its plain object URL - on the
configured AWS_S3_ENDPOINT if there is one, else on AWS itself, addressed exactly
as OpteryxS3FileSystem would address it (s3_filesystem.endpoint_parts) - and
fetched with NO signature and NO Authorization header, via OpteryxHttpFileSystem.
S3's own bucket policy / object ACL decides the outcome: a public object is read;
a private one is refused by S3 itself. Opteryx makes no allow/deny decision.

LISTING is tried ANONYMOUSLY too (docs/ANONYMOUS_REMOTE_READS_DESIGN.md): `list_files` sends an
UNSIGNED ListObjectsV2 through the credential-free `object_listing` module, so a glob over a public
bucket works. A bucket that does not allow anonymous listing answers 403 and the query fails
saying so - it is NEVER retried with a signed request. That is the rule this class exists to
keep: READ_* never uses the platform's identity.
"""

import urllib.parse


class AnonymousS3FileSystem:
    """See module docstring for the full rationale."""

    def __init__(self):
        from opteryx import config
        from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem
        from opteryx.connectors.io_systems.s3_filesystem import _resolve_region

        self._http = OpteryxHttpFileSystem()
        # Deployment configuration, not user input: the endpoint and region
        # decide WHERE an anonymous request goes, never what it may read.
        self.endpoint = (config.get("AWS_S3_ENDPOINT") or "").rstrip("/")
        self.region = _resolve_region()

    def _page_url(self, bucket: str, query: dict) -> str:
        """The UNSIGNED URL of one ListObjectsV2 page, addressed as the object URLs are."""
        from opteryx.connectors.io_systems.s3_filesystem import endpoint_parts

        scheme, host, path_prefix = endpoint_parts(bucket, self.endpoint, self.region)
        return urllib.parse.urlunsplit(
            (scheme, host, f"{path_prefix}/", urllib.parse.urlencode(query, quote_via=urllib.parse.quote), "")
        )

    def list_file_infos(self, base_dir: str, recursive: bool = True) -> list:
        """The objects under `base_dir` (s3://bucket/prefix), listed anonymously."""
        from opteryx.connectors.io_systems.object_listing import anonymous_listing_limit
        from opteryx.connectors.io_systems.object_listing import list_s3_objects

        def get(url, headers):
            # An anonymous request carries NO headers - the client call below cannot even express one.
            if headers:
                raise ValueError("an anonymous listing carries no request headers")
            return self._http.http_client.get(url)

        return list_s3_objects(
            get,
            self._page_url,
            base_dir,
            recursive,
            anonymous=True,
            limit=anonymous_listing_limit(),
        )

    def list_files(self, base_dir: str, recursive: bool = True) -> list:
        return [info.path for info in self.list_file_infos(base_dir, recursive)]

    def _to_public_url(self, path: str) -> str:
        from opteryx.connectors.io_systems.s3_filesystem import _quote_key
        from opteryx.connectors.io_systems.s3_filesystem import endpoint_parts
        from opteryx.connectors.io_systems.s3_filesystem import split_path

        bucket, key = split_path(path)
        scheme, host, path_prefix = endpoint_parts(bucket, self.endpoint, self.region)
        return urllib.parse.urlunsplit((scheme, host, f"{path_prefix}/{_quote_key(key)}", "", ""))

    # ── Native scan-path contract ───────────────────────────────────────────
    #
    # The C++ pipeline has no s3:// scheme of its own (gs:// is rewritten to
    # storage.googleapis.com natively; s3:// is not), so a READ_PARQUET scan
    # reaches it through `pool_reader._sign_paths`. The "signed" URL here is the
    # plain, credential-free object URL, and there is no auth header - the fetch
    # is anonymous end to end.

    @property
    def signs_urls(self) -> bool:
        return True

    def native_auth_header(self):
        return None

    def rewrite_to_signed_url(self, path: str, expiry_seconds: int = 3600) -> str:
        return self._to_public_url(path)

    # ── Reads ───────────────────────────────────────────────────────────────

    def open_input_file(self, path: str, columns=None, filters=None):
        return self._http.open_input_file(self._to_public_url(path), columns=columns, filters=filters)

    def open_input_stream(self, path: str, columns=None, filters=None):
        return self._http.open_input_stream(self._to_public_url(path), columns=columns, filters=filters)

    def read_ranges(self, path: str, ranges):
        return self._http.read_ranges(self._to_public_url(path), ranges)

    def get_file_info(self, paths):
        if isinstance(paths, str):
            return self._http.get_file_info(self._to_public_url(paths))
        return self._http.get_file_info([self._to_public_url(p) for p in paths])


def anonymous_s3_filesystem() -> "AnonymousS3FileSystem":
    """A filesystem for bare dataset functions' `s3://` support that never uses
    platform credentials. See the module docstring for the full rationale.

    Used at both bind time and execution time, so an `s3://` query can never
    authenticate at one stage and go anonymous at the other.
    """
    return AnonymousS3FileSystem()
