"""Unit tests for OpteryxS3FileSystem.

No network: the compiled HTTP client is replaced with a mock everywhere, and
the signing tests are pure computation. The one test that reaches outside the
module cross-checks our SigV4 presigner against botocore's, which is the only
way to prove the canonical request is right rather than merely self-consistent.
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import Mock

import pytest

from opteryx.compiled.http_client import HttpStatusError
from opteryx.connectors.io_systems import create_filesystem
from opteryx.connectors.io_systems import s3_filesystem as s3_module
from opteryx.connectors.io_systems.s3_filesystem import FileType
from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem
from opteryx.connectors.io_systems.s3_filesystem import CredentialChain
from opteryx.connectors.io_systems.s3_filesystem import reset_credential_cache
from opteryx.connectors.io_systems.s3_filesystem import split_path
from opteryx.exceptions import DatasetReadError
from opteryx.operators._operators import resolve_scan_filesystem

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
FIXED_NOW = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)


class _FrozenDatetime:
    """Stands in for `datetime` so a signature is reproducible."""

    @staticmethod
    def now(tz=None):
        return FIXED_NOW


_AWS_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_EC2_METADATA_DISABLED",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
    "AWS_S3_ENDPOINT",
)


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    """No AWS anything - including the developer's own ~/.aws files."""
    for name in _AWS_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-config"))
    reset_credential_cache()
    yield monkeypatch
    reset_credential_cache()


@pytest.fixture
def aws_env(clean_env, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
    return monkeypatch


@pytest.fixture
def fs(aws_env):
    filesystem = OpteryxS3FileSystem()
    filesystem.http_client = Mock()
    return filesystem


def _query_of(url: str) -> dict:
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))


# ── paths ────────────────────────────────────────────────────────────────────


def test_split_path_with_scheme():
    assert split_path("s3://bucket/a/b.parquet") == ("bucket", "a/b.parquet")


def test_split_path_without_scheme():
    assert split_path("bucket/a/b.parquet") == ("bucket", "a/b.parquet")


def test_split_path_bucket_only():
    assert split_path("s3://bucket") == ("bucket", "")


def test_split_path_rejects_empty_bucket():
    with pytest.raises(ValueError, match="must include a bucket"):
        split_path("s3:///key")


# ── registration ─────────────────────────────────────────────────────────────


def test_create_filesystem_resolves_s3(aws_env):
    assert isinstance(create_filesystem("s3"), OpteryxS3FileSystem)


# ── the native scan-path contract ────────────────────────────────────────────


def test_signs_urls_is_always_true(fs):
    """SigV4 is per-request, so a static header cannot authenticate a scan."""
    assert fs.signs_urls is True


def test_native_auth_header_is_none(fs):
    """Exactly one of signing and a header authenticates - never both."""
    assert fs.native_auth_header() is None


def test_rewrite_to_signed_url_produces_https(fs):
    url = fs.rewrite_to_signed_url("s3://bucket/data/file.parquet")
    assert url.startswith("https://bucket.s3.eu-west-2.amazonaws.com/data/file.parquet?")
    assert "X-Amz-Signature=" in url


# ── signing ──────────────────────────────────────────────────────────────────


def test_presign_includes_required_parameters(fs):
    query = _query_of(fs.presign("bucket", "key.parquet", expiry_seconds=900))
    assert query["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
    assert query["X-Amz-SignedHeaders"] == "host"
    assert query["X-Amz-Expires"] == "900"
    assert query["X-Amz-Credential"].startswith(f"{ACCESS_KEY}/")
    assert query["X-Amz-Credential"].endswith("/eu-west-2/s3/aws4_request")
    assert len(query["X-Amz-Signature"]) == 64


def test_presign_carries_session_token(aws_env):
    aws_env.setenv("AWS_SESSION_TOKEN", "session-token-value")
    filesystem = OpteryxS3FileSystem()
    query = _query_of(filesystem.presign("bucket", "key.parquet"))
    assert query["X-Amz-Security-Token"] == "session-token-value"


def test_presign_encodes_the_key_but_not_separators(fs):
    url = fs.presign("bucket", "a folder/part=1/file.parquet")
    path = urllib.parse.urlsplit(url).path
    assert path == "/a%20folder/part%3D1/file.parquet"


def test_dotted_bucket_uses_path_style(fs):
    """A dotted bucket breaks the wildcard certificate, so it must not be a subdomain."""
    url = fs.presign("my.data.bucket", "key.parquet")
    assert url.startswith("https://s3.eu-west-2.amazonaws.com/my.data.bucket/key.parquet?")


def test_endpoint_override_uses_path_style(aws_env):
    aws_env.setenv("AWS_S3_ENDPOINT", "http://minio.internal:9000")
    filesystem = OpteryxS3FileSystem()
    url = filesystem.presign("bucket", "key.parquet")
    assert url.startswith("http://minio.internal:9000/bucket/key.parquet?")


def test_region_defaults_when_unset(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setattr(s3_module, "_resolve_region", lambda: s3_module._DEFAULT_REGION)
    assert OpteryxS3FileSystem().region == "us-east-1"


def test_presigned_url_matches_botocore(fs, monkeypatch):
    """Cross-check the canonical request against the reference implementation.

    Everything else here proves our signature is well FORMED; only this proves
    it is CORRECT - a canonical request that differs from AWS's by a byte still
    produces 64 plausible hex characters, and fails at the first read.
    """
    botocore_auth = pytest.importorskip("botocore.auth")
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    monkeypatch.setattr(s3_module, "datetime", _FrozenDatetime)
    monkeypatch.setattr(botocore_auth, "get_current_datetime", lambda: FIXED_NOW)

    ours = fs.presign("my-bucket", "data/part-1.parquet", expiry_seconds=900)

    request = AWSRequest(
        method="GET",
        url="https://my-bucket.s3.eu-west-2.amazonaws.com/data/part-1.parquet",
    )
    signer = botocore_auth.S3SigV4QueryAuth(
        Credentials(ACCESS_KEY, SECRET_KEY), "s3", "eu-west-2", expires=900
    )
    signer.add_auth(request)

    assert _query_of(ours) == _query_of(request.url)


# ── reads ────────────────────────────────────────────────────────────────────


def test_read_ranges_signs_once_and_sets_range_headers(fs):
    fs.http_client.get_many = Mock(return_value=[b"aaa", b"bbb"])

    result = fs.read_ranges("s3://bucket/file.parquet", [(0, 3), (10, 3)])

    assert result == [b"aaa", b"bbb"]
    requests = fs.http_client.get_many.call_args[0][0]
    assert [headers["Range"] for _, headers in requests] == ["bytes=0-2", "bytes=10-12"]
    # One signature serves the whole batch - Range is not a signed header.
    assert len({url for url, _ in requests}) == 1


def test_read_ranges_empty_is_a_no_op(fs):
    fs.http_client.get_many = Mock()
    assert fs.read_ranges("s3://bucket/file.parquet", []) == []
    fs.http_client.get_many.assert_not_called()


def test_read_ranges_wraps_transport_errors(fs):
    fs.http_client.get_many = Mock(side_effect=RuntimeError("403 Forbidden"))
    with pytest.raises(DatasetReadError):
        fs.read_ranges("s3://bucket/file.parquet", [(0, 3)])


def test_get_file_info_single(fs):
    fs.http_client.head = Mock(return_value={"content-length": "2048"})
    info = fs.get_file_info("s3://bucket/file.parquet")
    assert info.type == FileType.File
    assert info.size == 2048


def test_get_file_info_missing_object(fs):
    fs.http_client.head = Mock(side_effect=HttpStatusError("HTTP 404: ...", 404))
    info = fs.get_file_info("s3://bucket/missing.parquet")
    assert info.type == FileType.NotFound


def test_get_file_info_reports_non_404_failures(fs):
    """A failure that is not a 404 leaves the object's existence UNKNOWN.

    Folding it into NotFound is a lie the caller cannot see through - and it is
    what a wrong-region 301, or the GET-signed HEAD this filesystem used to
    issue, produced for objects that were sitting right there.
    """
    fs.http_client.head = Mock(side_effect=HttpStatusError("HTTP 301: ...", 301))
    with pytest.raises(DatasetReadError):
        fs.get_file_info("s3://bucket/elsewhere.parquet")


def test_get_file_info_signs_for_head_not_get(fs):
    """SigV4 covers the METHOD, so a GET-signed URL cannot authenticate a HEAD.

    Signing every stat as a GET meant S3 answered SignatureDoesNotMatch for
    every object in the bucket.
    """
    fs.http_client.head = Mock(return_value={"content-length": "1"})
    fs.get_file_info("s3://bucket/file.parquet")
    single_url = fs.http_client.head.call_args[0][0]

    fs.http_client.head_many = Mock(return_value=[{"content-length": "1"}] * 2)
    fs.get_file_info(["s3://bucket/a.parquet", "s3://bucket/b.parquet"])
    batch_urls = [url for url, _ in fs.http_client.head_many.call_args[0][0]]

    for url in [single_url, *batch_urls]:
        head_signed = fs.presign("bucket", url.split("/")[3].split("?")[0], method="HEAD")
        get_signed = fs.presign("bucket", url.split("/")[3].split("?")[0], method="GET")
        signature = url.split("X-Amz-Signature=")[1]
        assert signature == head_signed.split("X-Amz-Signature=")[1]
        assert signature != get_signed.split("X-Amz-Signature=")[1]


def test_get_file_info_batches_multiple_paths(fs):
    fs.http_client.head_many = Mock(
        return_value=[{"content-length": "10"}, {"content-length": "20"}]
    )
    infos = fs.get_file_info(["s3://bucket/a.parquet", "s3://bucket/b.parquet"])
    assert [info.size for info in infos] == [10, 20]
    assert len(fs.http_client.head_many.call_args[0][0]) == 2


def test_stream_to_writes_every_chunk(fs):
    fs.http_client.get = Mock(return_value=b"0123456789")
    written = []

    class Sink:
        def write(self, chunk):
            written.append(bytes(chunk))
            return len(chunk)

    total = fs.stream_to("s3://bucket/file.parquet", Sink(), chunk_size=4)

    assert total == 10
    assert written == [b"0123", b"4567", b"89"]


def test_open_input_stream_exposes_memoryview(fs):
    fs.http_client.get = Mock(return_value=b"payload")
    handle = fs.open_input_stream("s3://bucket/file.parquet")
    assert bytes(handle.memoryview) == b"payload"
    handle.close()


def test_open_input_stream_rejects_projection(fs):
    with pytest.raises(NotImplementedError):
        fs.open_input_stream("s3://bucket/file.parquet", columns=["a"])


# ── listing ──────────────────────────────────────────────────────────────────


def _listing(keys, truncated=False, token=None):
    entries = "".join(f"<Contents><Key>{key}</Key></Contents>" for key in keys)
    next_token = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>"
        f"{entries}{next_token}</ListBucketResult>"
    ).encode("utf-8")


def test_list_files_returns_schemed_paths(fs):
    fs.http_client.get = Mock(return_value=_listing(["data/a.parquet", "data/b.parquet"]))
    assert fs.list_files("s3://bucket/data") == [
        "s3://bucket/data/a.parquet",
        "s3://bucket/data/b.parquet",
    ]


def test_list_files_terminates_the_prefix(fs):
    """Without the trailing slash, `data` would also match `data_backup/...`."""
    fs.http_client.get = Mock(return_value=_listing([]))
    fs.list_files("s3://bucket/data")
    assert "prefix=data%2F" in fs.http_client.get.call_args[0][0]


def test_list_files_skips_folder_placeholders(fs):
    fs.http_client.get = Mock(return_value=_listing(["data/", "data/a.parquet"]))
    assert fs.list_files("s3://bucket/data") == ["s3://bucket/data/a.parquet"]


def test_list_files_paginates(fs):
    pages = [
        _listing(["data/a.parquet"], truncated=True, token="TOKEN-2"),
        _listing(["data/b.parquet"]),
    ]
    fs.http_client.get = Mock(side_effect=pages)

    assert fs.list_files("s3://bucket/data") == [
        "s3://bucket/data/a.parquet",
        "s3://bucket/data/b.parquet",
    ]
    second_url = fs.http_client.get.call_args_list[1][0][0]
    assert "continuation-token=TOKEN-2" in second_url


def test_list_files_non_recursive_sets_delimiter(fs):
    fs.http_client.get = Mock(return_value=_listing([]))
    fs.list_files("s3://bucket/data", recursive=False)
    assert "delimiter=%2F" in fs.http_client.get.call_args[0][0]


def test_list_files_wraps_transport_errors(fs):
    fs.http_client.get = Mock(side_effect=RuntimeError("403 Forbidden"))
    with pytest.raises(DatasetReadError):
        fs.list_files("s3://bucket/data")


# ── credential chain ─────────────────────────────────────────────────────────


def _future(seconds=3600):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_chain_prefers_the_environment(aws_env):
    resolved = CredentialChain().resolve()
    assert resolved.triple == (ACCESS_KEY, SECRET_KEY, None)
    assert resolved.expires_at is None


def test_chain_reads_the_shared_credentials_file(clean_env, tmp_path):
    credentials_file = tmp_path / "credentials"
    credentials_file.write_text(
        "[default]\n"
        f"aws_access_key_id = {ACCESS_KEY}\n"
        f"aws_secret_access_key = {SECRET_KEY}\n"
        "aws_session_token = file-token\n"
    )
    clean_env.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))

    assert CredentialChain().resolve().triple == (ACCESS_KEY, SECRET_KEY, "file-token")


def test_chain_honours_the_selected_profile(clean_env, tmp_path):
    credentials_file = tmp_path / "credentials"
    credentials_file.write_text(
        "[default]\naws_access_key_id = WRONG\naws_secret_access_key = WRONG\n"
        f"[work]\naws_access_key_id = {ACCESS_KEY}\naws_secret_access_key = {SECRET_KEY}\n"
    )
    clean_env.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))
    clean_env.setenv("AWS_PROFILE", "work")

    assert CredentialChain().resolve().access_key == ACCESS_KEY


def test_chain_reads_the_container_endpoint(clean_env):
    clean_env.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/abc")
    clean_env.setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN", "container-token")
    seen = {}

    def fake_http(url, headers=None, timeout=None, method="GET", data=None):
        seen["url"] = url
        seen["headers"] = headers
        return json.dumps(
            {
                "AccessKeyId": ACCESS_KEY,
                "SecretAccessKey": SECRET_KEY,
                "Token": "container-session",
                "Expiration": _future(),
            }
        ).encode("utf-8")

    clean_env.setattr(s3_module, "_http", fake_http)

    resolved = CredentialChain().resolve()
    assert resolved.triple == (ACCESS_KEY, SECRET_KEY, "container-session")
    assert seen["url"] == "http://169.254.170.2/v2/credentials/abc"
    assert seen["headers"]["Authorization"] == "container-token"
    assert resolved.expires_at is not None


def test_chain_reads_instance_metadata_over_imdsv2(clean_env):
    calls = []

    def fake_http(url, headers=None, timeout=None, method="GET", data=None):
        calls.append((method, url))
        if method == "PUT" and url.endswith("/latest/api/token"):
            return b"imds-token"
        # Every data call must present the token - IMDSv1 is not a fallback.
        assert headers["X-aws-ec2-metadata-token"] == "imds-token"
        if url.endswith("/security-credentials/"):
            return b"opteryx-worker-role\n"
        if url.endswith("/security-credentials/opteryx-worker-role"):
            return json.dumps(
                {
                    "AccessKeyId": ACCESS_KEY,
                    "SecretAccessKey": SECRET_KEY,
                    "Token": "imds-session",
                    "Expiration": _future(),
                }
            ).encode("utf-8")
        return None

    clean_env.setattr(s3_module, "_http", fake_http)

    assert CredentialChain().resolve().triple == (ACCESS_KEY, SECRET_KEY, "imds-session")
    assert calls[0][0] == "PUT"


def test_chain_exchanges_a_web_identity_token(clean_env, tmp_path):
    """The federated path - a foreign workload identity, no stored secret."""
    token_file = tmp_path / "token"
    token_file.write_text("oidc-token-value")
    clean_env.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))
    clean_env.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/opteryx")
    posted = {}

    def fake_http(url, headers=None, timeout=None, method="GET", data=None):
        posted["url"] = url
        posted["method"] = method
        posted["body"] = data.decode("utf-8")
        return (
            "<AssumeRoleWithWebIdentityResponse "
            'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
            "<AssumeRoleWithWebIdentityResult><Credentials>"
            f"<AccessKeyId>{ACCESS_KEY}</AccessKeyId>"
            f"<SecretAccessKey>{SECRET_KEY}</SecretAccessKey>"
            "<SessionToken>sts-session</SessionToken>"
            f"<Expiration>{_future()}</Expiration>"
            "</Credentials></AssumeRoleWithWebIdentityResult>"
            "</AssumeRoleWithWebIdentityResponse>"
        ).encode("utf-8")

    clean_env.setattr(s3_module, "_http", fake_http)

    assert CredentialChain().resolve().triple == (ACCESS_KEY, SECRET_KEY, "sts-session")
    assert posted["method"] == "POST"
    assert posted["url"] == "https://sts.eu-west-2.amazonaws.com/"
    # The token travels in the body, never the URL - it is long, and URLs are logged.
    assert "oidc-token-value" in posted["body"]
    assert "oidc-token-value" not in posted["url"]


def test_chain_caches_a_resolved_credential(clean_env):
    class CountingChain(CredentialChain):
        calls = 0

        def resolve(self):
            CountingChain.calls += 1
            return s3_module.AWSCredentials(ACCESS_KEY, SECRET_KEY)

    chain = CountingChain()
    chain.frozen()
    chain.frozen()
    assert CountingChain.calls == 1


def test_chain_refreshes_before_expiry(clean_env):
    """A credential inside the refresh margin is re-resolved, not reused."""

    class ExpiringChain(CredentialChain):
        calls = 0

        def resolve(self):
            ExpiringChain.calls += 1
            return s3_module.AWSCredentials(
                ACCESS_KEY,
                SECRET_KEY,
                "temp",
                expires_at=time.time() + (s3_module._REFRESH_MARGIN_SECONDS / 2),
            )

    chain = ExpiringChain()
    chain.frozen()
    chain.frozen()
    assert ExpiringChain.calls == 2


def test_chain_without_any_source_fails_the_read(clean_env):
    clean_env.setattr(s3_module, "_http", lambda *args, **kwargs: None)
    with pytest.raises(DatasetReadError, match="No AWS credentials"):
        CredentialChain().frozen()


def test_no_sdk_is_imported_at_runtime():
    """The read path must not pull in boto3, botocore, s3fs or minio.

    Asserted in a subprocess because this test module imports botocore itself
    as a signing oracle - in-process, `sys.modules` proves nothing.
    """
    program = (
        "import sys\n"
        "from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem\n"
        "fs = OpteryxS3FileSystem(region='eu-west-2')\n"
        "fs.presign('bucket', 'key.parquet')\n"
        "banned = {'boto3', 'botocore', 's3fs', 'minio', 'aiobotocore', 'boto'}\n"
        "print(','.join(sorted(m for m in sys.modules if m.split('.')[0] in banned)))\n"
    )
    environment = dict(os.environ)
    environment["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
    environment["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY

    result = subprocess.run(
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ""


# ── the router ───────────────────────────────────────────────────────────────
#
# `resolve_scan_filesystem` is the dispatch the whole design rests on: it runs
# per SCAN, and picks the filesystem from the scheme of the paths that scan was
# handed. The single-protocol-per-dataset half of the constraint is enforced
# upstream, where the manifest is read (`OpteryxConnector`, "Mixed protocols in
# manifest"); these pin the routing half.


def test_router_dispatches_s3_by_scheme(aws_env):
    filesystem, connector_type = resolve_scan_filesystem(None, ["s3://bucket/a.parquet"])
    assert isinstance(filesystem, OpteryxS3FileSystem)
    assert connector_type == "S3"


def test_router_gives_each_dataset_its_own_filesystem(aws_env):
    """One query, two datasets, two schemes - two filesystems, no interference."""
    s3_fs, s3_type = resolve_scan_filesystem(None, ["s3://bucket/a.parquet"])
    local_fs, local_type = resolve_scan_filesystem(None, ["file:///tmp/b.parquet"])

    assert isinstance(s3_fs, OpteryxS3FileSystem)
    assert not isinstance(local_fs, OpteryxS3FileSystem)
    assert (s3_type, local_type) == ("S3", "FILE")


def test_router_prefers_a_connector_supplied_filesystem(aws_env):
    """A connector that owns its filesystem is not re-routed by path scheme."""

    class Connector:
        filesystem = "owned-by-the-connector"
        storage_type = "TEST"

    filesystem, connector_type = resolve_scan_filesystem(Connector(), ["s3://bucket/a.parquet"])
    assert filesystem == "owned-by-the-connector"
    assert connector_type == "TEST"


def test_router_rejects_an_unknown_scheme():
    with pytest.raises(ValueError, match="Unsupported storage protocol"):
        resolve_scan_filesystem(None, ["azure://container/a.parquet"])


@pytest.mark.parametrize(
    "url", ["file:///etc/passwd", "data:text/plain,creds", "169.254.170.2/creds"]
)
def test_credential_http_rejects_non_http_schemes(url):
    # A misconfigured endpoint must not reach urlopen, which would happily open
    # file:/ or data: and hand the result back as a credential document.
    with pytest.raises(ValueError, match="must use http or https"):
        s3_module._http(url)


def test_container_endpoint_with_bad_scheme_is_not_swallowed(clean_env, monkeypatch):
    # The chain turns absent sources into None; a misconfigured source is an
    # error and must surface, not fall through to the next source.
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "file:///tmp/creds.json")
    with pytest.raises(ValueError, match="must use http or https"):
        s3_module._from_container()
