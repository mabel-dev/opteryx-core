"""S3 support, end to end, against a real S3-compatible server (hadro).

The unit tests in tests/unit/connectors/test_s3_filesystem.py mock the HTTP
client, so they prove the SigV4 signer and the XML parsing are right in
isolation. Nothing there proves that a query naming s3:// data actually comes
back with the right rows. This module does: every test runs SQL through the
engine against hadro (mabel-dev/hadro), a read-only S3 server that serves a
local directory and, with keys set, checks SigV4 signatures on every request -
so a request that is mis-signed, mis-addressed or mis-parsed fails here the
same way it would against AWS.

Each s3:// answer is checked against the same query over the same bytes on
local disk, so the local reader is the oracle and the only variable is the
transport.

Both routes an s3:// read can take are covered:

* READ_PARQUET / READ_JSONL / READ_CSV('s3://bucket/key') - bare dataset
  functions, which must read ANONYMOUSLY (never with this process's AWS
  credentials - see anonymous_s3_filesystem), so they are run against a second,
  open hadro, and checked to be refused by the signature-checking one;
* a catalog (OpteryxConnector) table whose data files live at s3:// paths -
  the production route, and the one GCS-backed tables use today.

hadro is found as an installed package, else as the sibling checkout
../hadro/src, else the module is skipped.
"""

import os
import shutil
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

_NEXTCLOUD = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
for _sibling in ("hadro/src", "opteryx-catalog"):
    _path = os.path.join(_NEXTCLOUD, _sibling)
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(1, _path)

hadro = pytest.importorskip("hadro", reason="hadro (S3 test server) is not importable")

import opteryx  # noqa: E402
from opteryx.connectors.io_systems.s3_filesystem import reset_credential_cache  # noqa: E402
from opteryx.exceptions import DatasetReadError  # noqa: E402
from opteryx.exceptions import NotSupportedError  # noqa: E402

TESTDATA = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "testdata"))

ACCESS_KEY = "hadro-access"
SECRET_KEY = "hadro-secret"

_AWS_VARS = (
    "AWS_S3_ENDPOINT",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_EC2_METADATA_DISABLED",
)

# bucket -> {key: source file under testdata/}
_BUCKETS = {
    "planets": {"planets.parquet": "planets/planets.parquet"},
    # Keys with spaces: the canonical-request quoting has to match what the
    # server reconstructs, or every one of these is a signature mismatch.
    "tweets": {
        f"ten_files/tweets-0000 copy {i}.parquet": f"flat/ten_files/tweets-0000 copy {i}.parquet"
        for i in range(1, 11)
    },
    "text": {
        "customers.csv": "generated/customers.csv",
        "users.jsonl": "jsonl_perf/data.jsonl",
    },
}


@pytest.fixture(scope="module")
def s3_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("hadro")
    for bucket, objects in _BUCKETS.items():
        for key, source in objects.items():
            target = root / bucket / key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(os.path.join(TESTDATA, source), target)
    (root / "lake").mkdir()
    return root


@pytest.fixture(scope="module")
def s3(s3_root):
    """A signature-checking hadro, and an environment that points Opteryx at it."""
    saved = {name: os.environ.get(name) for name in _AWS_VARS}
    for name in _AWS_VARS:
        os.environ.pop(name, None)
    config = hadro.Config(data=str(s3_root), access_key=ACCESS_KEY, secret_key=SECRET_KEY)
    with hadro.Server(config=config, port=0) as server, hadro.Server(data=str(s3_root)) as open_server:
        server.open_endpoint = open_server.endpoint
        os.environ.update(
            AWS_S3_ENDPOINT=server.endpoint,
            AWS_ACCESS_KEY_ID=ACCESS_KEY,
            AWS_SECRET_ACCESS_KEY=SECRET_KEY,
            AWS_REGION="eu-west-2",
            # Never let a missing credential fall through to a real metadata
            # endpoint from a developer laptop or a CI runner.
            AWS_EC2_METADATA_DISABLED="true",
            AWS_SHARED_CREDENTIALS_FILE=str(s3_root / "absent-credentials"),
            AWS_CONFIG_FILE=str(s3_root / "absent-config"),
        )
        reset_credential_cache()
        try:
            yield server
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            reset_credential_cache()


def _rows(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel.to_arrow().to_pylist())
    return rows


def _sorted(rows):
    return sorted(rows, key=repr)


# ── Bare dataset functions ──────────────────────────────────────────────────


@pytest.fixture
def s3_public(s3, monkeypatch):
    """The open hadro: an anonymous request is answered, as for a public bucket.

    The platform credentials stay in the environment, so a bare function that
    wrongly signed with them would still succeed here - which is why
    test_read_functions_never_sign exists alongside.
    """
    monkeypatch.setenv("AWS_S3_ENDPOINT", s3.open_endpoint)
    return s3


def test_read_parquet_single_object(s3_public):
    remote = _rows("SELECT * FROM READ_PARQUET('s3://planets/planets.parquet')")
    local = _rows(f"SELECT * FROM READ_PARQUET('{TESTDATA}/planets/planets.parquet')")
    assert len(remote) == 9
    assert _sorted(remote) == _sorted(local)


def test_read_parquet_pushed_predicate_and_projection(s3_public):
    sql = "SELECT name FROM READ_PARQUET('{}') WHERE id > 7 ORDER BY name"
    remote = _rows(sql.format("s3://planets/planets.parquet"))
    assert remote == [{"name": "Neptune"}, {"name": "Pluto"}]
    assert remote == _rows(sql.format(f"{TESTDATA}/planets/planets.parquet"))


def test_read_parquet_key_with_spaces(s3_public):
    sql = "SELECT COUNT(*) AS n, MAX(followers) AS m FROM READ_PARQUET('{}')"
    remote = _rows(sql.format("s3://tweets/ten_files/tweets-0000 copy 3.parquet"))
    local = _rows(sql.format(f"{TESTDATA}/flat/ten_files/tweets-0000 copy 3.parquet"))
    assert remote[0]["n"] == 25
    assert remote == local


def test_read_jsonl(s3_public):
    sql = "SELECT COUNT(*) AS n, SUM(age) AS s FROM READ_JSONL('{}') WHERE active IS TRUE"
    remote = _rows(sql.format("s3://text/users.jsonl"))
    assert remote == _rows(sql.format(f"{TESTDATA}/jsonl_perf/data.jsonl"))
    assert remote[0]["n"] > 0


def test_read_csv(s3_public):
    sql = "SELECT country, COUNT(*) AS n FROM READ_CSV('{}') GROUP BY country"
    remote = _rows(sql.format("s3://text/customers.csv"))
    assert _sorted(remote) == _sorted(_rows(sql.format(f"{TESTDATA}/generated/customers.csv")))
    assert remote


def test_missing_object_is_a_read_error(s3_public):
    with pytest.raises(DatasetReadError):
        _rows("SELECT * FROM READ_PARQUET('s3://planets/no-such-file.parquet')")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM READ_PARQUET('s3://tweets/ten_files/*.parquet')",
        "SELECT * FROM READ_JSONL('s3://text/*.jsonl')",
        "SELECT * FROM READ_CSV('s3://text/*.csv')",
    ],
)
def test_read_functions_refuse_globs(s3_public, sql):
    """A glob is a bucket LISTING, which is not assumed granted anonymously."""
    with pytest.raises(NotSupportedError):
        _rows(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM READ_PARQUET('s3://planets/planets.parquet')",
        "SELECT * FROM READ_JSONL('s3://text/users.jsonl')",
        "SELECT * FROM READ_CSV('s3://text/customers.csv')",
    ],
)
def test_read_functions_never_sign(s3, sql):
    """SECURITY: against the signature-checking hadro, with valid platform
    credentials in the environment, a bare dataset function is REFUSED - it
    made an anonymous request instead of signing a user-supplied path with
    this process's AWS credentials."""
    assert os.environ["AWS_S3_ENDPOINT"] == s3.endpoint
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == SECRET_KEY
    with pytest.raises(DatasetReadError):
        _rows(sql)


# ── Catalog table with s3:// data files (the production route) ──────────────


@pytest.fixture(scope="module")
def s3_catalog_table(s3, s3_root, tmp_path_factory):
    """A real SimpleDataset whose manifest points at s3:// data files.

    The catalog's own metadata stays on local disk: this is testing the
    engine's reads of data files, which is the part that differs by store.
    """
    opteryx_catalog = pytest.importorskip("opteryx_catalog")  # noqa: F841
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    import opteryx.connectors as connectors
    from opteryx.connectors import OpteryxConnector
    from opteryx_catalog.catalog.dataset import SimpleDataset
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
    from opteryx_catalog.catalog.metadata import DatasetMetadata
    from opteryx_catalog.catalog.metadata import Snapshot
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    class _LocalDiskIO:
        class _In:
            def __init__(self, path):
                self._path = path

            def open(self):
                return open(self._path, "rb")

        class _Out:
            def __init__(self, path):
                self._path = path
                self._chunks = []

            def create(self):
                return self

            def write(self, data):
                self._chunks.append(data)

            def close(self):
                with open(self._path, "wb") as f:
                    f.write(b"".join(self._chunks))

        def new_input(self, path):
            return self._In(path)

        def new_output(self, path):
            return self._Out(path)

    clear_parsed_manifest_cache()
    location = str(tmp_path_factory.mktemp("s3ws"))
    os.makedirs(f"{location}/metadata", exist_ok=True)

    files = {
        "data/f1.parquet": [1, 2, 3, 4, 5, 6, 7, 8],
        "data/f2.parquet": [100, 101, 102, 103],
    }
    entries = []
    for key, values in files.items():
        morsel = Morsel()
        morsel.append_vector("a", vector_from_sequence(values, dtype="INTEGER"))
        data = write_parquet(morsel, compression="zstd")
        target = s3_root / "lake" / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        entries.append(
            build_parquet_manifest_entry_from_bytes(
                data, f"s3://lake/{key}", len(data), field_id_by_name={"a": 1}
            ).to_dict()
        )

    class _ManifestWriterCatalog:
        io = _LocalDiskIO()
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    snapshot_id = 1000
    manifest_path = _ManifestWriterCatalog().write_parquet_manifest(snapshot_id, entries, location)
    meta = DatasetMetadata(
        dataset_identifier="col.ds",
        location=location,
        schema=None,
        properties={},
        schemas=[{"schema_id": "s1", "columns": [{"id": 1, "name": "a", "type": "INTEGER"}]}],
        current_schema_id="s1",
    )
    meta.snapshots.append(
        Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author="seed",
            sequence_number=1,
            user_created=True,
            operation_type="append",
            manifest_list=manifest_path,
            schema_id="s1",
        )
    )
    meta.current_snapshot_id = snapshot_id
    dataset = SimpleDataset(identifier="col.ds", _metadata=meta)
    dataset.io = _ManifestWriterCatalog.io

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = dataset.io

        def load_dataset(self, identifier):
            if identifier != "col.ds":
                raise KeyError(identifier)
            return dataset

        def get_relation(self, identifier):
            if identifier == "col.ds":
                return "dataset", dataset
            return None, None

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop("s3ws", None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield "s3ws.col.ds"
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def test_catalog_scan_reads_s3_data_files(s3_catalog_table):
    values = sorted(r["a"] for r in _rows(f"SELECT a FROM {s3_catalog_table}"))
    assert values == [1, 2, 3, 4, 5, 6, 7, 8, 100, 101, 102, 103]


def test_catalog_scan_with_predicate(s3_catalog_table):
    values = sorted(r["a"] for r in _rows(f"SELECT a FROM {s3_catalog_table} WHERE a BETWEEN 4 AND 101"))
    assert values == [4, 5, 6, 7, 8, 100, 101]


def test_catalog_aggregate_forces_a_read(s3_catalog_table):
    """SUM is not answerable from manifest stats, so the bytes must come off S3."""
    assert _rows(f"SELECT SUM(a) AS s FROM {s3_catalog_table}") == [{"s": 36 + 406}]


def test_catalog_scan_signs_with_platform_credentials(s3_catalog_table):
    """The catalog route DOES sign - and the server really is checking: with
    the wrong secret, the same scan is refused."""
    os.environ["AWS_SECRET_ACCESS_KEY"] = "not-the-secret"
    reset_credential_cache()
    try:
        with pytest.raises(DatasetReadError):
            _rows(f"SELECT SUM(a) AS s FROM {s3_catalog_table}")
    finally:
        os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
        reset_credential_cache()
