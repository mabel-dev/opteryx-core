"""The vector index's remote reads, against a real signature-checking S3 server (hadro).

Every remote read the index makes goes through a SIGNED URL - an hours-long native build
cannot refresh a credential, so each object is read through a self-contained presigned URL
by HTTP range GETs, never a header and never a HEAD. The fakes in the unit tests serve
ranges; this proves the same reads against an S3 server that checks the SigV4 signature
on every request, so a URL that is mis-signed, mis-scoped or expired fails here as it
would against AWS. Local disk is the oracle each time:

  * the BUILD reads its data file through a presigned URL and writes a byte-identical index
    file;
  * the SEARCH reads the index file through a presigned URL - its footer in one request,
    then the blocks it needs - and answers exactly as the local search;
  * compaction's CARRY reads its inputs' index files through presigned URLs and writes
    byte-identical outputs;
  * a URL signed with the wrong secret is refused by the server, and the read fails loud.
"""

import os
import shutil
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(1, os.path.dirname(__file__))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "unit", "core"))

from test_s3_hadro import s3  # noqa: E402,F401
from test_s3_hadro import s3_root  # noqa: E402,F401

from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo.parquet import write_parquet  # noqa: E402

from opteryx.connectors.io_systems.s3_filesystem import OpteryxS3FileSystem  # noqa: E402
from opteryx.connectors.io_systems.s3_filesystem import reset_credential_cache  # noqa: E402
from opteryx.operators._operators import RowOriginRecorder  # noqa: E402
from opteryx.operators._operators import build_vector_index_local  # noqa: E402
from opteryx.operators._operators import carry_vector_index_local  # noqa: E402
from opteryx.operators._operators import search_vector_index_file  # noqa: E402

from test_vector_index_build import _embed  # noqa: E402

BUCKET = "vidx"
WORDS = ["red", "planet", "gas", "giant", "ice", "moon", "ring", "storm", "dust", "orbit", "comet", "sun"]
ROWS = 1200
QUERY = "storm moon ring"


@pytest.fixture(scope="module")
def built(s3_root, tmp_path_factory):
    """A data file and its index, on local disk AND in the hadro bucket, byte for byte."""
    local = tmp_path_factory.mktemp("vidx_local")
    fn, dims = _embed()
    texts = [None if i % 53 == 9 else " ".join(WORDS[(i * k + i // 7) % 12] for k in (1, 5, 7)) + f" {i % 11}"
             for i in range(ROWS)]
    morsel = Morsel()
    morsel.append_vector("body", vector_from_sequence(texts, dtype="VARCHAR"))
    (local / "d.parquet").write_bytes(write_parquet(morsel, max_rows_per_row_group=256))
    info = build_vector_index_local(str(local / "d.parquet"), "body", [], fn, dims, str(local / "i.vidx"),
                                    flush_rows=32)
    (s3_root / BUCKET).mkdir(exist_ok=True)
    for name in ("d.parquet", "i.vidx"):
        shutil.copyfile(local / name, s3_root / BUCKET / name)
    return local, info


def _signed(name):
    return OpteryxS3FileSystem().rewrite_to_signed_url(f"s3://{BUCKET}/{name}")


def _size(local, name):
    return os.path.getsize(local / name)


def test_the_build_reads_its_data_file_through_a_presigned_url(s3, built, tmp_path):
    local, _ = built
    fn, dims = _embed()
    url = _signed("d.parquet")
    build_vector_index_local(url, "body", [], fn, dims, str(tmp_path / "i.vidx"),
                             data_bytes=_size(local, "d.parquet"), flush_rows=32)
    assert (tmp_path / "i.vidx").read_bytes() == (local / "i.vidx").read_bytes()


def test_the_search_reads_its_index_through_a_presigned_url(s3, built):
    local, info = built
    fn, dims = _embed()

    def search(path, nprobe):
        return search_vector_index_file(path, info["file_bytes"], info["footer_bytes"], QUERY, fn, dims, 10,
                                        nprobe, ROWS)

    for nprobe in (0, 2, info["clusters"]):              # 0 = exact (the default)
        remote = search(_signed("i.vidx"), nprobe)
        assert remote == search(str(local / "i.vidx"), nprobe)
        assert remote[0]                                           # it found something
    # Exact: the footer in one request, the body in one more.
    assert search(_signed("i.vidx"), 0)[1]["requests"] == 2


def test_carry_reads_its_inputs_through_a_presigned_url(s3, built, tmp_path):
    local, info = built
    _, dims = _embed()

    def carry(location, out):
        recorder = RowOriginRecorder("$f", "$o")
        m = Morsel()
        m.append_vector("$f", vector_from_sequence([0] * ROWS, dtype="INT64"))
        m.append_vector("$o", vector_from_sequence(list(reversed(range(ROWS))), dtype="INT64"))
        recorder.take(m)
        out.mkdir()
        return carry_vector_index_local([(location, info["file_bytes"], info["footer_bytes"], [], "")],
                                        [recorder], dims, [str(out / "i.vidx")], flush_rows=32)

    carry(_signed("i.vidx"), tmp_path / "remote")
    carry(str(local / "i.vidx"), tmp_path / "local")
    assert (tmp_path / "remote" / "i.vidx").read_bytes() == (tmp_path / "local" / "i.vidx").read_bytes()


def test_a_url_signed_with_the_wrong_secret_is_refused(s3, built, monkeypatch):
    local, info = built
    fn, dims = _embed()
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-the-secret")
    reset_credential_cache()
    try:
        bad = _signed("i.vidx")
        with pytest.raises(RuntimeError, match="403|cannot read"):
            search_vector_index_file(bad, info["file_bytes"], info["footer_bytes"], QUERY, fn, dims, 10, 0, ROWS)
    finally:
        monkeypatch.undo()
        reset_credential_cache()
