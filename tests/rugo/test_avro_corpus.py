# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Rugo's Avro reader over the third-party corpora in testdata/avro — files rugo did not
write and whose shapes we did not choose:

  apache/data/        apache/avro share/test/data (see its README for the commit)
  fastavro/           fastavro tests/avro-files, several written by Java avro-tools
  kafka_connect/      real Confluent Kafka Connect S3-sink output (dev/kafka_avro)

Every file is either read and compared value for value against BOTH oracles, fastavro
and Apache's reference `avro` package, or refused with the exact message listed in
REFUSED. A file an oracle cannot read is listed, with the reason, in that oracle's
gap set — never skipped silently. A new file therefore has to land in exactly one of
"agrees with the oracles" or "refused for a stated reason".

Comparison rules follow docs/AVRO_READER_DESIGN.md §17: a whole record/map/array
column is JSON text (decimals as bare numbers with their scale, bytes as base64), so
it is parsed with Decimal floats and the oracle value has its bytes base64-encoded.
"""

import base64
import decimal
import json
import math
import sys
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import fastavro  # noqa: E402
from avro.datafile import DataFileReader  # noqa: E402
from avro.io import DatumReader  # noqa: E402

from rugo.rugo_native import read_avro  # noqa: E402

AVRO_ROOT = REPO_ROOT / "testdata" / "avro"
CORPORA = ("apache/data", "fastavro", "kafka_connect")

# Files rugo refuses, with the message it must refuse them with. Refusals are
# decisions (docs/AVRO_READER_DESIGN.md), not failures to tolerate: if one starts
# reading, it moves out of here and gets compared.
REFUSED = {
    "apache/data/test.avro12": "Avro container: not an Avro object container file (bad magic)",
    "fastavro/recursive.avro": "Avro schema at 'next': recursive type 'LongList' is not supported",
    "fastavro/testDataFileMeta.avro": "Avro: column 'value' has type null, which is not supported",
}
# Whole folders refused with one message; none at present (bzip2 reads since
# docs/AVRO_READER_DESIGN.md §29).
REFUSED_PREFIXES = {}

# Files an oracle cannot read, and why. Each such file is still compared against the
# other oracle; a file neither oracle reads would fail test_every_read_file_has_an_oracle.
FASTAVRO_GAPS = {
    # The sync marker's bytes appear inside the header metadata; fastavro decodes the
    # metadata map as UTF-8 and fails. Apache reads it.
    "apache/data/syncInMeta.avro",
}
APACHE_GAPS = {
    # Written by Java reflection: record names carry `$` (e.g. `TestReflectJob$`), which
    # the spec does not allow and Apache's Python package rejects. fastavro reads them.
    "fastavro/double_float.avro",
    "fastavro/lines.avro",
    "fastavro/part-00000.avro",
    "fastavro/test.avro",
}


def _corpus_files():
    files = []
    for corpus in CORPORA:
        root = AVRO_ROOT / corpus
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix in (".avsc", ".json", ".txt", ".md") or path.name in ("LICENSE", "NOTICE"):
                continue
            files.append(path.relative_to(AVRO_ROOT).as_posix())
    return files


FILES = _corpus_files()


def _expected_refusal(name):
    if name in REFUSED:
        return REFUSED[name]
    for prefix, message in REFUSED_PREFIXES.items():
        if name.startswith(prefix):
            return message
    return None


def _rugo_columns(path):
    with open(path, "rb") as fh:
        res = read_avro(fh.read())
    columns = {name: [] for name in res["column_names"]}
    for batch in res["batches"]:
        for name, vec in zip(res["column_names"], batch):
            columns[name].extend(vec.to_pylist())
    return columns, res["num_rows"]


def _fastavro(path):
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        return reader.writer_schema, list(reader)


def _apache(path):
    with open(path, "rb") as fh:
        reader = DataFileReader(fh, DatumReader())
        schema = json.loads(reader.meta["avro.schema"])
        rows = list(reader)
        reader.close()
    return schema, rows


def _json_ready(value):
    # The oracle's view of a nested value, in the form rugo's JSON text decodes to.
    if type(value) is dict:
        return {k: _json_ready(v) for k, v in value.items()}
    if type(value) is list:
        return [_json_ready(v) for v in value]
    if type(value) is bytes:
        return base64.b64encode(value).decode()
    return value


def _same(got, want):
    if type(want) in (dict, list) and type(got) is str:
        return json.loads(got, parse_float=decimal.Decimal) == _json_ready(want)
    if type(want) is float and type(got) is float and math.isnan(want):
        return math.isnan(got)
    # Apache's `avro` ignores logicalType uuid and hands back the text; rugo (and
    # fastavro) give uuid.UUID. Same value, so compare in the oracle's form.
    if type(got) is uuid.UUID and type(want) is str:
        return got == uuid.UUID(want)
    return got == want


def _compare(name, columns, rows, schema, want, oracle):
    assert rows == len(want), f"{name}: rugo read {rows} rows, {oracle} {len(want)}"
    is_record = type(schema) is dict and schema["type"] in ("record", "error")
    if is_record:
        expected_columns = [field["name"] for field in schema["fields"]]
    else:
        expected_columns = ["value"]
    assert list(columns) == expected_columns or rows == 0, (
        f"{name}: rugo columns {list(columns)}, {oracle} schema has {expected_columns}"
    )
    for column, values in columns.items():
        for i, got in enumerate(values):
            expected = want[i][column] if is_record else want[i]
            assert _same(got, expected), f"{name}: {column}[{i}] rugo={got!r} {oracle}={expected!r}"


@pytest.mark.parametrize("name", FILES)
def test_corpus_file(name):
    path = AVRO_ROOT / name
    refusal = _expected_refusal(name)
    if refusal is not None:
        with pytest.raises(RuntimeError) as err:
            _rugo_columns(path)
        assert str(err.value) == refusal
        return

    columns, rows = _rugo_columns(path)
    if name not in FASTAVRO_GAPS:
        schema, want = _fastavro(path)
        _compare(name, columns, rows, schema, want, "fastavro")
    if name not in APACHE_GAPS:
        schema, want = _apache(path)
        _compare(name, columns, rows, schema, want, "apache")


def test_every_read_file_has_an_oracle():
    both_gaps = FASTAVRO_GAPS & APACHE_GAPS
    assert not both_gaps, f"files no oracle reads: {sorted(both_gaps)}"


def test_listed_files_exist():
    # A renamed or removed fixture must not leave a stale entry that silently matches nothing.
    present = set(FILES)
    listed = set(REFUSED) | FASTAVRO_GAPS | APACHE_GAPS
    assert listed <= present, f"listed but missing: {sorted(listed - present)}"
    for prefix in REFUSED_PREFIXES:
        assert any(f.startswith(prefix) for f in present), f"no files under {prefix}"


def test_corpora_are_present():
    for corpus in CORPORA:
        assert any(f.startswith(corpus + "/") for f in FILES), f"no files in testdata/avro/{corpus}"
