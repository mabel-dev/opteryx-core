"""
Amazon S3 filesystem implementation using Opteryx's optimized I/O.

Uses the same compiled libcurl client as the GCS and HTTP filesystems, so an
`s3://` scan takes the identical range-read path — the only S3-specific work
here is turning an object path into a URL the C++ fetcher can GET.

── Why every URL is pre-signed ──────────────────────────────────────────────
The GCS filesystem authenticates with a bearer token: one credential for the
caller, attached as a header to every object it touches, and signing is the
expensive opt-in (`GCS_SIGN_URLS`) because minting a signed URL there costs an
IAM signBlob round trip per object.

S3 has neither property. SigV4 signs the *request* — the signature covers the
method, the URI and the timestamp — so there is no single Authorization header
that authenticates reads of two different objects, which is exactly what
`pool_reader._native_auth_header` would need. Pre-signing is therefore not a
fallback here, it is the only mechanism: `signs_urls` is unconditionally True
and `native_auth_header` returns None, satisfying the pipeline's rule that
exactly one of the two authenticates a read.

The cost that made signing the exception on GCS does not exist on S3. A
presigned URL is an HMAC chain over local strings — no network, no KMS, no IAM
call — so it is microseconds per object, not the ~63ms signBlob RPC. Signing
every path is the cheap path here, not the expensive one.

── No SDK, anywhere ─────────────────────────────────────────────────────────
Nothing here imports boto3, botocore, s3fs or minio. Signing is stdlib
`hmac`/`hashlib` over strings; object reads go through the same compiled
libcurl client the GCS and HTTP filesystems use; the credential chain below is
`urllib`, `json`, `configparser` and `xml`. An SDK on the read path would mean
a second HTTP stack with its own connection pool, its own buffering and its own
GIL behaviour sitting underneath a scan that the C++ pipeline is supposed to
own end to end.

The one place botocore appears at all is the test suite, where it signs the
same request as an independent oracle. That is a check on our arithmetic, not
a dependency of the code.

── Credentials ──────────────────────────────────────────────────────────────
Resolved lazily, on the first signature rather than at construction: the
router builds a filesystem per scan (`resolve_scan_filesystem`), so
construction must stay free of network calls. The chain is the conventional
one - environment, web identity, shared credentials file, container endpoint,
instance metadata - and its result is cached for the process, since every
source is process-wide. Temporary credentials carry their expiry and are
re-resolved a few minutes ahead of it, so a rotating role is picked up without
rebuilding anything.
"""

import hashlib
import hmac
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import md_cause
from opteryx.exceptions import md_code

_ALGORITHM = "AWS4-HMAC-SHA256"
_SERVICE = "s3"
# Presigned URLs cover the URI and the timestamp, never the body; a range GET
# has no body, and S3 accepts the literal marker in place of a payload hash.
_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
_DEFAULT_REGION = "us-east-1"
# Long enough that a signed URL outlives the scan it was minted for, short
# enough that one leaking into a log is not a durable credential.
_DEFAULT_EXPIRY_SECONDS = 3600
# S3 caps a ListObjectsV2 page at 1000 keys regardless; asking for the maximum
# minimises round trips on a large dataset.
_LIST_PAGE_SIZE = 1000
# Re-resolve a temporary credential this far ahead of its expiry - see
# `AWSCredentials.is_stale` for why the margin is not optional.
_REFRESH_MARGIN_SECONDS = 300
# Fast fail: a link-local metadata address that is not there should cost the
# chain no more than a moment before it moves on.
_METADATA_TIMEOUT_SECONDS = 2
_STS_TIMEOUT_SECONDS = 5
_IMDS_TOKEN_TTL_SECONDS = 21600
_DEFAULT_METADATA_ENDPOINT = "http://169.254.169.254"
_CONTAINER_METADATA_HOST = "http://169.254.170.2"
_STS_NAMESPACE = "{https://sts.amazonaws.com/doc/2011-06-15/}"
_LIST_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class FileType(Enum):
    """File type enumeration."""

    File = "file"
    Directory = "directory"
    NotFound = "not_found"


@dataclass
class FileInfo:
    """File metadata container (standalone)."""

    path: str
    type: FileType
    size: int = 0


def split_path(path: str) -> Tuple[str, str]:
    """Split ``s3://bucket/key`` (or a bare ``bucket/key``) into its two parts.

    The scheme is optional on the way IN — every method here accepts both, the
    way the GCS filesystem does — but paths handed BACK out of `list_files` are
    always fully schemed, because `pool_reader._is_local_path` classifies a
    path with no scheme as local and would have the C++ reader try to open an
    S3 key as an on-disk file.
    """
    if path.startswith("s3://"):
        path = path[5:]
    bucket, _, key = path.partition("/")
    if not bucket:
        raise ValueError(f"Invalid S3 path {path!r} - an S3 path must include a bucket")
    return bucket, key


def _quote_key(key: str) -> str:
    """Percent-encode an object key for the canonical URI.

    ``/`` stays literal because it is the path separator, and S3 - alone among
    AWS services - does NOT double-encode the canonical URI. Python leaves the
    RFC 3986 unreserved set (``A-Za-z0-9_.-~``) alone already, which is what
    SigV4 requires.
    """
    return urllib.parse.quote(key, safe="/")


def _quote_param(value: str) -> str:
    """Percent-encode a query-string name or value for the canonical query.

    Stricter than `_quote_key`: ``/`` is encoded here, since inside a query
    parameter it is data rather than structure.
    """
    return urllib.parse.quote(str(value), safe="-_.~")


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret_key: str, datestamp: str, region: str) -> bytes:
    """The SigV4 four-stage derived key: date -> region -> service -> request."""
    key = _sign(f"AWS4{secret_key}".encode("utf-8"), datestamp)
    key = _sign(key, region)
    key = _sign(key, _SERVICE)
    return _sign(key, "aws4_request")


class AWSCredentials:
    """One resolved credential, with the expiry the source reported.

    `expires_at` is epoch seconds, or None for a credential that does not
    expire (environment variables, a key in the shared file). The chain uses it
    to decide when to go back to the source; nothing else reads it.
    """

    __slots__ = ("access_key", "secret_key", "token", "expires_at")

    def __init__(self, access_key, secret_key, token=None, expires_at=None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token
        self.expires_at = expires_at

    @property
    def triple(self) -> Tuple[str, str, Optional[str]]:
        return self.access_key, self.secret_key, self.token

    def is_stale(self, now: float) -> bool:
        """True when this credential should be re-resolved.

        The margin matters more than it looks: a presigned URL is minted now
        and fetched by the C++ pipeline some seconds later, so a credential
        that is merely *not yet* expired can still expire mid-scan. Refreshing
        early costs one metadata call; refreshing late costs the query.
        """
        if self.expires_at is None:
            return False
        return now >= (self.expires_at - _REFRESH_MARGIN_SECONDS)


def _parse_expiry(value) -> Optional[float]:
    """AWS's ISO-8601 expiry as epoch seconds; None when absent or unparsable.

    An unreadable expiry is deliberately treated as "no expiry known" rather
    than as an error - the credential itself is still usable, and the chain
    will find out the hard way at the next 403 rather than refusing to start.
    """
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text).timestamp()
    except ValueError:  # pragma: no cover - AWS has not changed this format
        return None


def _http(url: str, headers=None, timeout=_METADATA_TIMEOUT_SECONDS, method="GET", data=None):
    """One stdlib HTTP call for the credential chain, or None on any failure.

    Not the compiled client: that one speaks GET and HEAD only, and IMDSv2's
    token handshake is a PUT. These are rare control-plane calls on a
    link-local address, so `urllib` is the right size of tool - it keeps the
    chain dependency-free, which is the whole point.

    Every failure returns None rather than raising. A credential source that is
    not present (no instance metadata on a laptop, no container endpoint
    outside ECS) must fall through to the next source, not end the chain.
    """
    import urllib.error
    import urllib.request

    request = urllib.request.Request(url, headers=headers or {}, data=data, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()
    except (urllib.error.URLError, OSError, ValueError):
        return None


def _from_environment() -> Optional[AWSCredentials]:
    """AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN."""
    from opteryx import config

    access_key = config.get("AWS_ACCESS_KEY_ID")
    secret_key = config.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return AWSCredentials(access_key, secret_key, config.get("AWS_SESSION_TOKEN"))
    return None


def _from_web_identity(region: str) -> Optional[AWSCredentials]:
    """AssumeRoleWithWebIdentity, from a projected OIDC token on disk.

    This is the federated path: a workload identity from another cloud (or a
    Kubernetes service account) is exchanged for AWS credentials, so nothing
    long-lived is stored anywhere. The exchange is a plain form POST to STS -
    unsigned, because the identity token IS the proof - which is why it needs
    no SDK and no bootstrap credential.
    """
    import xml.etree.ElementTree as ElementTree

    from opteryx import config

    token_file = config.get("AWS_WEB_IDENTITY_TOKEN_FILE")
    role_arn = config.get("AWS_ROLE_ARN")
    if not token_file or not role_arn:
        return None

    try:
        with open(token_file, "r", encoding="utf-8") as handle:
            web_identity_token = handle.read().strip()
    except OSError:
        return None
    if not web_identity_token:
        return None

    body = urllib.parse.urlencode(
        {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "RoleArn": role_arn,
            "RoleSessionName": config.get("AWS_ROLE_SESSION_NAME") or "opteryx",
            "WebIdentityToken": web_identity_token,
        }
    ).encode("utf-8")

    # POST, not GET: an OIDC token is comfortably long enough to run into URL
    # length limits, and the token would then sit in access logs besides.
    raw = _http(
        f"https://sts.{region}.amazonaws.com/",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=_STS_TIMEOUT_SECONDS,
        method="POST",
        data=body,
    )
    if not raw:
        return None

    try:
        root = ElementTree.fromstring(raw)
    except ElementTree.ParseError:  # pragma: no cover - STS returned non-XML
        return None

    element = root.find(f".//{_STS_NAMESPACE}Credentials")
    if element is None:
        return None

    def _text(name):
        found = element.find(f"{_STS_NAMESPACE}{name}")
        return found.text if found is not None else None

    access_key, secret_key = _text("AccessKeyId"), _text("SecretAccessKey")
    if not access_key or not secret_key:
        return None
    return AWSCredentials(
        access_key, secret_key, _text("SessionToken"), _parse_expiry(_text("Expiration"))
    )


def _from_shared_file() -> Optional[AWSCredentials]:
    """The profile in ~/.aws/credentials - the developer-machine case."""
    import configparser
    import os

    from opteryx import config

    path = config.get("AWS_SHARED_CREDENTIALS_FILE") or os.path.expanduser(
        "~/.aws/credentials"
    )
    if not os.path.exists(path):
        return None

    profile = config.get("AWS_PROFILE") or "default"
    parser = configparser.RawConfigParser()
    try:
        parser.read(path)
    except configparser.Error:
        return None
    if not parser.has_section(profile):
        return None

    section = parser[profile]
    access_key = section.get("aws_access_key_id")
    secret_key = section.get("aws_secret_access_key")
    if not access_key or not secret_key:
        return None
    return AWSCredentials(access_key, secret_key, section.get("aws_session_token"))


def _from_container() -> Optional[AWSCredentials]:
    """ECS / EKS task role, served from the container credentials endpoint."""
    from opteryx import config

    full_uri = config.get("AWS_CONTAINER_CREDENTIALS_FULL_URI")
    relative_uri = config.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    url = full_uri or (f"{_CONTAINER_METADATA_HOST}{relative_uri}" if relative_uri else None)
    if not url:
        return None

    headers = {}
    token_file = config.get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
    if token_file:
        try:
            with open(token_file, "r", encoding="utf-8") as handle:
                headers["Authorization"] = handle.read().strip()
        except OSError:
            return None
    elif config.get("AWS_CONTAINER_AUTHORIZATION_TOKEN"):
        headers["Authorization"] = config.get("AWS_CONTAINER_AUTHORIZATION_TOKEN")

    return _credentials_from_json(_http(url, headers=headers))


def _from_instance_metadata() -> Optional[AWSCredentials]:
    """EC2 instance role, via IMDSv2.

    IMDSv2 only: the token handshake is what makes the endpoint unreachable to
    a confused-deputy request (a proxied fetch cannot set the PUT header), and
    falling back to IMDSv1 on failure would give that protection away for the
    convenience of an instance configuration nobody should still be running.
    """
    from opteryx import config

    disabled = str(config.get("AWS_EC2_METADATA_DISABLED") or "").lower()
    if disabled in ("1", "true", "yes", "on"):
        return None

    endpoint = config.get("AWS_EC2_METADATA_SERVICE_ENDPOINT") or _DEFAULT_METADATA_ENDPOINT
    endpoint = endpoint.rstrip("/")

    token = _http(
        f"{endpoint}/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": str(_IMDS_TOKEN_TTL_SECONDS)},
        method="PUT",
    )
    if not token:
        return None
    headers = {"X-aws-ec2-metadata-token": token.decode("utf-8")}

    role = _http(f"{endpoint}/latest/meta-data/iam/security-credentials/", headers=headers)
    if not role:
        return None
    # The listing can name more than one role; only the first is attached.
    role_name = role.decode("utf-8").splitlines()[0].strip()
    if not role_name:
        return None

    return _credentials_from_json(
        _http(
            f"{endpoint}/latest/meta-data/iam/security-credentials/{role_name}",
            headers=headers,
        )
    )


def _credentials_from_json(raw) -> Optional[AWSCredentials]:
    """Both metadata endpoints answer in the same JSON shape."""
    import json

    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:  # pragma: no cover - endpoint returned non-JSON
        return None

    access_key = payload.get("AccessKeyId")
    secret_key = payload.get("SecretAccessKey")
    if not access_key or not secret_key:
        return None
    return AWSCredentials(
        access_key,
        secret_key,
        payload.get("Token") or payload.get("SessionToken"),
        _parse_expiry(payload.get("Expiration")),
    )


class CredentialChain:
    """The credential sources, tried in order, with the answer cached.

    Order follows the convention every AWS client uses, and the reasoning is
    the same: the environment is an explicit instruction and wins; federation
    comes next because a workload configured for it has no other credential;
    the shared file is a developer's machine; the two metadata endpoints are
    the deployment's own identity and are last because they cost a network
    round trip to even ask.

    Cached at module scope because every source is process-wide - there is no
    per-bucket or per-caller credential here, which is a real limit of routing
    by protocol alone and is documented as one on `OpteryxS3FileSystem`.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._credentials: Optional[AWSCredentials] = None

    def frozen(self) -> Tuple[str, str, Optional[str]]:
        now = time.time()
        current = self._credentials
        if current is not None and not current.is_stale(now):
            return current.triple

        with self._lock:
            # Double-checked: a concurrent signer may have refreshed while we
            # waited, and each resolution can cost a metadata round trip.
            current = self._credentials
            if current is not None and not current.is_stale(time.time()):
                return current.triple

            resolved = self.resolve()
            if resolved is None:
                raise DatasetReadError(
                    "No AWS credentials found. Set AWS_ACCESS_KEY_ID and "
                    "AWS_SECRET_ACCESS_KEY, or run where a role is attached."
                )
            self._credentials = resolved
            return resolved.triple

    def resolve(self) -> Optional[AWSCredentials]:
        """Walk the chain once. Separated so tests can drive it directly."""
        return (
            _from_environment()
            or _from_web_identity(_resolve_region())
            or _from_shared_file()
            or _from_container()
            or _from_instance_metadata()
        )

    def reset(self) -> None:
        with self._lock:
            self._credentials = None


_CHAIN = CredentialChain()


def reset_credential_cache() -> None:
    """Drop the cached credential - for tests, and for a deliberate re-read."""
    _CHAIN.reset()


def _resolve_region() -> str:
    """The region used in the credential scope and the endpoint host.

    Explicit configuration wins, then the shared config file's profile. A wrong
    region is not silent: S3 answers a signed request sent to the wrong
    regional endpoint with a redirect, which surfaces as a read error naming
    the object.
    """
    import configparser
    import os

    from opteryx import config

    region = config.get("AWS_REGION") or config.get("AWS_DEFAULT_REGION")
    if region:
        return region

    path = config.get("AWS_CONFIG_FILE") or os.path.expanduser("~/.aws/config")
    if os.path.exists(path):
        profile = config.get("AWS_PROFILE") or "default"
        # ~/.aws/config names the default profile "default" and every other one
        # "profile <name>" - a quirk of the file format, not of this code.
        parser = configparser.RawConfigParser()
        try:
            parser.read(path)
            for section in (profile, f"profile {profile}"):
                if parser.has_section(section):
                    region = parser[section].get("region")
                    if region:
                        return region
        except configparser.Error:
            pass

    return _DEFAULT_REGION


class S3File:
    """
    File-like wrapper for S3 objects.

    Reads the entire object into memory on open. Holds the raw bytes directly
    rather than copying into a BytesIO buffer - callers only ever access
    `.memoryview`, so the copy would be waste.
    """

    __slots__ = ("_data",)

    def __init__(self, url: str, http_client):
        try:
            self._data = http_client.get(url, headers={"Accept-Encoding": "identity"})
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read {md_code(url)}. {md_cause(err)}") from err

    @property
    def memoryview(self) -> memoryview:
        """Return a zero-copy memoryview of the file content."""
        return memoryview(self._data)

    def close(self) -> None:
        self._data = b""


class OpteryxS3FileSystem:
    """
    S3 filesystem using presigned URLs and direct HTTP range requests.

    Every method resolves an object path to a presigned HTTPS URL and then
    behaves exactly like the HTTP filesystem - one code path for reads, and
    nothing S3-specific below the signing layer.

    Scope of one instance: the router
    (`parquet_read.resolve_scan_filesystem`) builds a filesystem per scan from
    the scheme of the paths it was given, so a dataset gets exactly one of
    these and a query touching S3 and GCS datasets gets one of each. What an
    instance does NOT carry is a caller - credentials come from the process's
    own chain, so every `s3://` read in this process authenticates as the same
    identity. Per-workspace or per-bucket credentials would have to arrive
    through the router, which today passes only a protocol string.
    """

    def __init__(self, bucket=None, region: Optional[str] = None, credentials=None, **kwargs):
        self.bucket = bucket

        try:
            from opteryx.compiled.http_client import HttpClient
        except (ImportError, AttributeError) as err:  # pragma: no cover
            raise RuntimeError(
                f"HTTP client extension import failed: {err}\n\n"
                "This should not happen - http_client is a required extension. "
                "The build system should have failed if it couldn't be built."
            ) from err

        from opteryx import config

        self.region = region or _resolve_region()
        # An endpoint override points the filesystem at an S3-compatible store
        # (MinIO, Ceph, a VPC endpoint). Those are addressed path-style, since
        # a bucket-as-subdomain host only exists for AWS itself.
        self.endpoint = (config.get("AWS_S3_ENDPOINT") or "").rstrip("/")
        # The chain, not a resolved credential: resolution is deferred to the
        # first signature so building a filesystem stays free of network calls.
        self.credentials = credentials or _CHAIN

        # max_connections caps per-host concurrency inside each get_many()
        # call's CURLM event loop and sets the connection cache size, matching
        # the GCS and HTTP filesystems so a wide projection is never queued.
        self.http_client = HttpClient(max_connections=128, timeout_ms=60000)

    # ── URL construction ────────────────────────────────────────────────────

    def _endpoint_parts(self, bucket: str) -> Tuple[str, str, str]:
        """(scheme, host, path prefix) for a bucket.

        Virtual-hosted addressing (``bucket.s3.region.amazonaws.com``) is the
        default and the only style AWS still adds features to. Two cases fall
        back to path-style: an explicit endpoint override, and a bucket whose
        name contains a dot - the latter because the wildcard certificate
        covers one label only, so ``my.bucket.s3...`` fails TLS verification
        rather than returning data.
        """
        if self.endpoint:
            parts = urllib.parse.urlsplit(self.endpoint)
            scheme = parts.scheme or "https"
            host = parts.netloc or parts.path
            return scheme, host, f"/{bucket}"

        if "." in bucket:
            return "https", f"s3.{self.region}.amazonaws.com", f"/{bucket}"

        return "https", f"{bucket}.s3.{self.region}.amazonaws.com", ""

    def presign(
        self,
        bucket: str,
        key: str,
        method: str = "GET",
        query: Optional[Dict[str, str]] = None,
        expiry_seconds: int = _DEFAULT_EXPIRY_SECONDS,
    ) -> str:
        """A SigV4 query-string-signed URL for one object or one listing call.

        Entirely local - an HMAC chain over strings, no network - which is what
        makes signing every path affordable here. `query` carries the operation's
        own parameters (a ListObjectsV2 prefix, a continuation token); they are
        signed alongside the ``X-Amz-*`` ones, because SigV4 covers the whole
        canonical query string and a parameter added after signing invalidates
        the signature.

        Deliberately NOT covered: request headers other than ``host``. That is
        what lets one signed URL serve every byte range of an object - the
        ``Range`` header is unsigned, so the C++ fetcher can vary it per row
        group without re-signing.
        """
        access_key, secret_key, token = self.credentials.frozen()

        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        scope = f"{datestamp}/{self.region}/{_SERVICE}/aws4_request"

        scheme, host, path_prefix = self._endpoint_parts(bucket)
        canonical_uri = f"{path_prefix}/{_quote_key(key)}" if key else f"{path_prefix}/"

        params: Dict[str, str] = dict(query or {})
        params["X-Amz-Algorithm"] = _ALGORITHM
        params["X-Amz-Credential"] = f"{access_key}/{scope}"
        params["X-Amz-Date"] = amz_date
        params["X-Amz-Expires"] = str(expiry_seconds)
        params["X-Amz-SignedHeaders"] = "host"
        if token:
            params["X-Amz-Security-Token"] = token

        canonical_query = "&".join(
            f"{_quote_param(name)}={_quote_param(value)}" for name, value in sorted(params.items())
        )

        canonical_request = "\n".join(
            (
                method,
                canonical_uri,
                canonical_query,
                f"host:{host}\n",
                "host",
                _UNSIGNED_PAYLOAD,
            )
        )
        string_to_sign = "\n".join(
            (
                _ALGORITHM,
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            )
        )
        signature = hmac.new(
            _signing_key(secret_key, datestamp, self.region),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return f"{scheme}://{host}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}"

    def _object_url(self, path: str, expiry_seconds: int = _DEFAULT_EXPIRY_SECONDS) -> str:
        bucket, key = split_path(path)
        return self.presign(bucket, key, expiry_seconds=expiry_seconds)

    # ── Native scan-path contract ───────────────────────────────────────────

    @property
    def signs_urls(self) -> bool:
        """Always True - see the module docstring.

        SigV4 signs the request, not the caller, so there is no static header
        that authenticates every object in a scan. Unlike GCS this is also the
        cheap option: signing is local HMAC, not an IAM round trip.
        """
        return True

    def native_auth_header(self):
        """None - the URLs this filesystem hands out already carry a credential.

        Returning a header here as WELL as signing would put two credentials on
        one request; S3 rejects that outright rather than picking one.
        """
        return None

    def rewrite_to_signed_url(
        self, path: str, expiry_seconds: int = _DEFAULT_EXPIRY_SECONDS
    ) -> str:
        """Convert an ``s3://`` path to a presigned HTTPS URL.

        The hook `pool_reader._sign_paths` calls so the C++ pipeline can fetch
        through libcurl with no Authorization header of its own.
        """
        return self._object_url(path, expiry_seconds=expiry_seconds)

    # ── Reads ───────────────────────────────────────────────────────────────

    def list_files(self, base_dir: str, recursive: bool = True) -> List[str]:
        """Return the objects under ``base_dir`` as ``s3://bucket/key`` paths.

        The prefix is always terminated with ``/`` before listing: S3 prefix
        matching is a plain string match, so listing ``space_missions`` would
        also return ``space_missions_backup/...``, silently pulling a sibling
        dataset's objects into this one. This mirrors the local filesystem's
        directory semantics, and the GCS filesystem's.

        Paginates - a dataset can exceed the 1000-key page limit, and a
        truncated listing would silently under-read rather than fail.
        """
        import xml.etree.ElementTree as ElementTree

        bucket, prefix = split_path(base_dir.rstrip("/"))
        # Trailing slash = directory semantics (see docstring). An empty prefix
        # means the whole bucket, where no prefix filter is correct.
        if prefix:
            prefix = f"{prefix}/"

        blobs: List[str] = []
        continuation_token = None

        while True:
            query = {"list-type": "2", "max-keys": str(_LIST_PAGE_SIZE)}
            if prefix:
                query["prefix"] = prefix
            if not recursive:
                # S3 is flat; a delimiter is what makes a listing non-recursive.
                query["delimiter"] = "/"
            if continuation_token:
                query["continuation-token"] = continuation_token

            url = self.presign(bucket, "", query=query)
            try:
                raw = self.http_client.get(url)
            except RuntimeError as err:
                raise DatasetReadError(f"Unable to list '{base_dir}' - {err}") from err

            root = ElementTree.fromstring(raw) if raw else None
            if root is None:
                return blobs

            for contents in root.findall(f"{_LIST_NAMESPACE}Contents"):
                key_element = contents.find(f"{_LIST_NAMESPACE}Key")
                key = key_element.text if key_element is not None else None
                # Skip the zero-byte placeholder objects consoles create for
                # "folders" - they are not readable data files.
                if key and not key.endswith("/"):
                    blobs.append(f"s3://{bucket}/{key}")

            truncated = root.find(f"{_LIST_NAMESPACE}IsTruncated")
            if truncated is None or (truncated.text or "").lower() != "true":
                return blobs

            next_token = root.find(f"{_LIST_NAMESPACE}NextContinuationToken")
            continuation_token = next_token.text if next_token is not None else None
            if not continuation_token:
                return blobs

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about S3 objects via HEAD requests."""
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        # Fast path: avoid batch overhead for the common single-path case.
        if len(paths) == 1:
            try:
                headers = self.http_client.head(self._object_url(paths[0]))
            except RuntimeError:
                info = FileInfo(path=paths[0], type=FileType.NotFound)
                return info if single_path else [info]
            size = int(headers.get("content-length", 0))
            info = FileInfo(path=paths[0], type=FileType.File, size=size)
            return info if single_path else [info]

        # Fan out all HEAD requests in ONE native libcurl batch (a single C++
        # CURLM event loop, one GIL release for the whole call) rather than a
        # Python thread pool, matching the GCS filesystem.
        requests = [(self._object_url(path), {}) for path in paths]
        headers_list = self.http_client.head_many(requests)
        return [
            FileInfo(path=path, type=FileType.File, size=int(headers.get("content-length", 0)))
            for path, headers in zip(paths, headers_list)
        ]

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from an S3 object using HTTP range requests.

        One signature serves the whole batch - ``Range`` is not a signed header
        - and all ranges are fetched concurrently via a single get_many() call,
        so the C++ CURLM loop handles every transfer on one thread with the GIL
        released.

        Args:
            path: S3 object path, with or without the ``s3://`` prefix.
            ranges: List of (offset, length) tuples specifying byte ranges.

        Returns:
            List of byte buffers in the same order as ranges.
        """
        if not ranges:
            return []

        url = self._object_url(path)
        requests = [
            (url, {"Range": f"bytes={offset}-{offset + length - 1}"}) for offset, length in ranges
        ]

        try:
            return self.http_client.get_many(requests)
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read {md_code(path)}. {md_cause(err)}") from err

    def stream_to(self, path: str, sink, chunk_size: int = 1 << 20) -> int:
        """Stream an S3 object directly into *sink* without an intermediate buffer.

        Calls ``sink.write(chunk)`` for each chunk, giving callers a zero-copy
        path when *sink* writes into a shared-memory slot.

        Returns:
            Total bytes written to *sink*.
        """
        url = self._object_url(path)
        try:
            data = self.http_client.get(url, headers={"Accept-Encoding": "identity"})
        except RuntimeError as err:
            raise DatasetReadError(f"Unable to read {md_code(path)}. {md_cause(err)}") from err

        view = memoryview(data)
        total = 0
        for start in range(0, len(data), chunk_size):
            sink.write(view[start : start + chunk_size])
            total += min(chunk_size, len(data) - start)
        return total

    def open_input_stream(self, path: str, columns=None, filters=None):
        """Open an S3 object for reading as a stream."""
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for S3 open_input_stream/file. "
                "Column-selective reads go through the native Parquet scan path."
            )
        return S3File(self._object_url(path), self.http_client)

    def open_input_file(self, path: str, columns=None, filters=None):
        """Open an S3 object for random access reading."""
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for S3 open_input_stream/file. "
                "Column-selective reads go through the native Parquet scan path."
            )
        return S3File(self._object_url(path), self.http_client)
