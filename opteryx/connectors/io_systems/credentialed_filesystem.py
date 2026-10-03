# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Filesystems for `READ_*(..., credentials => '<workspace>.<name>')`.

The siblings of `anonymous_gcs_filesystem` / `anonymous_s3_filesystem`: a
READ_* over `gs://` or `s3://` is anonymous unless the statement names a
stored customer secret, and then it reads with THAT secret and nothing else
(jobs.opteryx docs/design/secrets.md §8.2).

- **No ambient fallback.** The GCS filesystem's credential is built from the
  secret's service-account key; `google.auth.default()` is never called. The S3
  filesystem is given the secret's key pair explicitly; the AWS credential chain
  is never consulted. A store that refuses the key is reported as the store's
  refusal.
- **SCOPE on every path.** Every method that takes a path checks it against the
  secret's SCOPE first, so a path can reach the store through this object only
  if the secret was created for it - whichever code path handed it over. The
  binder checks the literal path and every glob-expanded file as well; this is
  the second wall, not the only one.
- **Listing is allowed.** Globs are refused on the anonymous path because bucket
  listing is a permission an anonymous caller is not assumed to have; a
  customer's credential either has it or the store says no.
- **One query.** Built per READ_* at bind time from a credential resolved for
  that query, held on that query's plan node, dropped with it. The GCS reader
  sends a minted access token, not the key.
"""

from opteryx.connectors.io_systems.gcs_filesystem import OpteryxGcsFileSystem
from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem
from opteryx.exceptions import DatasetReadError

# The only token endpoints a service-account key may name. google-auth POSTs a
# signed assertion to the key's own `token_uri` when it refreshes, so a key
# naming anything else would turn a customer secret into a request from this
# process to a customer-chosen URL.
_TOKEN_URIS = frozenset(
    {"https://oauth2.googleapis.com/token", "https://accounts.google.com/o/oauth2/token"}
)

# Reads only. A secret used for READ_* never needs more, whatever the key's own
# IAM would allow.
_GCS_READ_ONLY = ("https://www.googleapis.com/auth/devstorage.read_only",)


class ScopeRefused(PermissionError):
    """A path outside the secret's SCOPE. Names the secret, never its contents."""


class _ScopeGuard:
    """Checks every path against the credential's SCOPE before the parent sees it."""

    _credential = None

    def _guard(self, path: str) -> str:
        if not self._credential.admits(path):
            raise ScopeRefused(
                f"'{path}' is outside the SCOPE of secret {self._credential.reference}"
            )
        return path

    def _guard_all(self, paths):
        if isinstance(paths, str):
            return self._guard(paths)
        return [self._guard(p) for p in paths]

    def list_file_infos(self, base_dir: str, recursive: bool = True) -> list:
        self._guard(base_dir if base_dir.endswith("/") else base_dir + "/")
        infos = super().list_file_infos(base_dir, recursive=recursive)
        return [info for info in infos if self._credential.admits(info.path)]

    def list_files(self, base_dir: str, recursive: bool = True) -> list:
        return [info.path for info in self.list_file_infos(base_dir, recursive=recursive)]

    def get_file_info(self, paths):
        return super().get_file_info(self._guard_all(paths))

    def read_ranges(self, path, ranges):
        return super().read_ranges(self._guard(path), ranges)

    def stream_to(self, path, sink, chunk_size: int = 1 << 20) -> int:
        return super().stream_to(self._guard(path), sink, chunk_size=chunk_size)

    def open_input_stream(self, path, columns=None, filters=None):
        return super().open_input_stream(self._guard(path), columns=columns, filters=filters)

    def open_input_file(self, path, columns=None, filters=None):
        return super().open_input_file(self._guard(path), columns=columns, filters=filters)

    def rewrite_to_signed_url(self, path: str, expiry_seconds: int = 3600) -> str:
        return super().rewrite_to_signed_url(self._guard(path), expiry_seconds=expiry_seconds)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._credential.reference}>"

    def __reduce__(self):
        raise TypeError("a credentialed filesystem cannot be serialised")

    def __deepcopy__(self, memo):
        return self


class CredentialedGcsFileSystem(_ScopeGuard, OpteryxGcsFileSystem):
    """GCS with a customer's service-account key, read-only, inside its SCOPE."""

    def __init__(self, credential):
        import threading

        try:
            from google.auth.transport.requests import Request
            from google.oauth2 import service_account
        except ImportError as err:  # pragma: no cover
            from opteryx.exceptions import MissingDependencyError

            raise MissingDependencyError(getattr(err, "name", None) or str(err)) from err
        from opteryx.compiled.http_client import HttpClient

        key = (credential.secret or {}).get("key")
        if not isinstance(key, dict) or key.get("type") != "service_account":
            raise PermissionError(f"secret {credential.reference} does not hold a service-account key")
        if key.get("token_uri") not in _TOKEN_URIS:
            raise PermissionError(
                f"secret {credential.reference}'s key names a token endpoint that is not Google's"
            )

        self.bucket = None
        self._credential = credential
        self.client_credentials = service_account.Credentials.from_service_account_info(
            key, scopes=_GCS_READ_ONLY
        )
        self._Request = Request
        self._token_lock = threading.Lock()
        self.http_client = HttpClient(max_connections=128, timeout_ms=60000)

    @property
    def signs_urls(self) -> bool:
        # The bearer header, always: one token covers every object, and the
        # header never appears in a URL that an error message might quote. The
        # one reader that cannot send a header (the native JSONL source) asks
        # for signed URLs explicitly - see jsonl_read.native_file_locations.
        return False

    @property
    def _bearer(self) -> str:
        try:
            return OpteryxGcsFileSystem._bearer.fget(self)
        except Exception as err:
            # The store's refusal - google-auth's message is the token
            # endpoint's error, which carries no key material.
            raise DatasetReadError(
                f"Google Cloud Storage refused the credential in secret "
                f"{self._credential.reference}: {str(err).splitlines()[0][:200]}"
            ) from None


class _FixedAwsCredentials:
    """The `frozen()` contract `OpteryxS3FileSystem` signs with, over ONE key.

    In place of the module's `CredentialChain`, which walks the environment,
    the shared file and the metadata endpoints - every one of them the
    process's own identity, which a customer's read must never fall back to.
    """

    __slots__ = ("_triple",)

    def __init__(self, access_key: str, secret_key: str, token=None):
        self._triple = (access_key, secret_key, token)

    def frozen(self):
        return self._triple

    def __repr__(self) -> str:
        return "<_FixedAwsCredentials>"

    def __reduce__(self):
        raise TypeError("a resolved credential cannot be serialised")


class CredentialedS3FileSystem(_ScopeGuard, OpteryxS3FileSystem):
    """S3 with a customer's access key, inside its SCOPE. Presigned URLs, SigV4."""

    def __init__(self, credential):
        secret = credential.secret or {}
        access_key = secret.get("access_key_id")
        secret_key = secret.get("secret_access_key")
        if not access_key or not secret_key:
            raise PermissionError(f"secret {credential.reference} does not hold an access key")
        self._credential = credential
        OpteryxS3FileSystem.__init__(
            self,
            region=secret.get("region"),
            credentials=_FixedAwsCredentials(access_key, secret_key, secret.get("session_token")),
        )


def credentialed_filesystem(credential, protocol: str):
    """The filesystem for one credentialed READ_* over `protocol`."""
    if credential.scheme != protocol:
        raise PermissionError(
            f"secret {credential.reference} is a {credential.kind} secret and cannot read "
            f"{protocol}:// paths"
        )
    if protocol == "gs":
        return CredentialedGcsFileSystem(credential)
    return CredentialedS3FileSystem(credential)
