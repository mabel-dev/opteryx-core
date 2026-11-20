"""Parquet VARIANT columns written as shredded Parquet VARIANT.

docs/parquet_variant_design.md §5.4. A VARIANT column is written as its shredded group,
learnt from the file's first row group. DuckDB is the second reader: what it reads from our
file is what it reads from its own, and both are the JSON that was written. rugo's own reader
returns the VARIANT as its JSON text (W3), and a patch carries it through unchanged (W4).
"""

import json
import os
import sys
from decimal import Decimal

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx import config
from rugo import parquet as rp

pytestmark = pytest.mark.usefixtures("allow_local_reads")

ROWS = 4096


def _document(i):
    shape = i % 12
    if shape == 0:
        return {"kind": "commit", "n": i, "price": "12.25", "ok": True,
                "commit": {"collection": f"c{i % 7}", "rev": i}}
    if shape == 1:
        return None  # SQL NULL
    if shape == 2:
        return "JSON null"
    if shape == 3:
        return {"kind": 7, "n": 1.5, "price": 12.5, "commit": "not an object", "ok": "yes"}
    if shape == 4:
        return [1, {"kind": "in an array"}, [], {}]
    if shape == 5:
        return {"kind": None, "ok": False, "commit": {"collection": {"x": [1, "é"]}, "rev": "r"}}
    if shape == 6:
        # names the shredded layout itself uses, at every level
        return {"kind": "names", "typed_value": {"value": i, "metadata": "m", "typed_value": "t"},
                "value": "v", "metadata": None}
    if shape == 7:
        # what DuckDB holds exactly; wider numbers: test_wide_numbers_are_written_exactly
        return {"kind": "numbers", "n": 9223372036854775807, "neg": -0.0, "f": 1e300, "tiny": 5e-324}
    if shape == 8:
        # (an empty key: test_wide_numbers_are_written_exactly - DuckDB renames it on the way in)
        return {"kind": 'esc "q" / \\ \n \t \u0001 é 😀', "commit": {"a'b": 1, "a.b": 3}}
    if shape == 9:
        return "a string"
    if shape == 10:
        return {"kind": "commit", "n": -i, "price": 0.01, "ok": True,
                "commit": {"collection": "z", "rev": 7, "extra": [i]}}
    return {"kind": "account", f"extra_{i % 3}": i, "commit": {}}


def _text(document):
    if document is None:
        return None
    if document == "JSON null":
        return "null"
    return json.dumps(document).replace('"12.25"', "12.25")   # the literal, not a float's repr


def _value(text):
    return None if text is None else json.loads(text, parse_float=Decimal)


def _duckdb_file(path, documents):
    """DuckDB's own VARIANT file of `documents` (ids from 0)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE src (id INTEGER, j VARCHAR)")
    con.executemany("INSERT INTO src VALUES (?, ?)", [(i, _text(d)) for i, d in enumerate(documents)])
    con.execute(f"COPY (SELECT id, j::JSON::VARIANT AS data FROM src ORDER BY id) TO '{path}' (FORMAT PARQUET)")


def _morsel(sql):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) == 1, len(morsels)
    return morsels[0]


def _source_morsel(tmp_path, documents, name="source.parquet"):
    """A morsel holding `documents` as a VARIANT column (read from DuckDB's file)."""
    path = str(tmp_path / name)
    _duckdb_file(path, documents)
    return path, _morsel(f"SELECT id, data FROM READ_PARQUET('{path}') ORDER BY id")


def _duck_values(path, id_column="id"):
    # DuckDB renders an empty key as "element<n>", reading its own files too: compare what it
    # reads from ours with what it reads from its own, and our readers with the JSON written.
    sql = f"SELECT {id_column}, data::JSON::VARCHAR FROM '{path}' ORDER BY {id_column}"
    return [_value(t) for _, t in duckdb.sql(sql).fetchall()]


def _json_cells(rows):
    # Object members come back in the file's order: DuckDB's own, ours sorted (the spec's).
    return [tuple(_value(c) if type(c) is str and c[:1] in "{[" else c for c in row) for row in rows]


def _write(tmp_path, morsel, name="ours.parquet", **options):
    path = str(tmp_path / name)
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsel, **options))
    return path


def _schema(path):
    return duckdb.sql(
        f"SELECT name, type, repetition_type, num_children, field_id FROM parquet_schema('{path}')"
    ).fetchall()


def _rows(sql, pushed):
    config.features.disable_parquet_variant_extraction = not pushed
    try:
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
        return sorted(rows, key=repr)
    finally:
        config.features.disable_parquet_variant_extraction = False


DOCUMENTS = [_document(i) for i in range(ROWS)]
WANT = [_value(_text(d)) for d in DOCUMENTS]


@pytest.fixture(scope="module")
def written(tmp_path_factory):
    folder = tmp_path_factory.mktemp("variant_write")
    source, morsel = _source_morsel(folder, DOCUMENTS)
    return source, _write(folder, morsel, max_rows_per_row_group=1024)


def test_the_column_is_written_as_a_shredded_variant(written):
    _, ours = written
    schema = {row[0]: row for row in _schema(ours)}
    assert duckdb.sql(f"DESCRIBE SELECT data FROM '{ours}'").fetchall()[0][1] == "VARIANT"
    assert [c.logical_type for c in rp.read_metadata(ours).schema_columns] == ["int32", "variant"]
    names = [row[0] for row in _schema(ours)]
    # the shredded fields, each a required group of {value, typed_value}
    for field in ("kind", "n", "ok", "commit", "collection", "rev"):
        assert field in names, field
    assert schema["metadata"][2] == "REQUIRED"
    assert rp.read_metadata(ours).num_row_groups == ROWS // 1024


def test_duckdb_reads_every_value_we_write(written):
    source, ours = written
    assert _duck_values(ours) == _duck_values(source)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id, data FROM {f}",
        "SELECT id, data ->> 'kind', data -> 'kind', data ->> 'n', data -> 'price', "
        "data -> 'ok', data -> 'f', data -> 'neg', data -> 'tiny' FROM {f}",
        "SELECT id, data -> 'commit' ->> 'collection', data -> 'commit' -> 'rev', data -> 'commit', "
        "data -> 'commit' -> 'a''b', data -> 'commit' -> 'a.b' FROM {f}",
        "SELECT id, data -> 'typed_value' -> 'value', data -> 'typed_value' ->> 'typed_value', "
        "data ->> 'value', data -> 'metadata' FROM {f}",
        "SELECT COUNT(*) FROM {f} WHERE data ->> 'kind' = 'commit'",
        "SELECT data -> 'commit' ->> 'collection' AS c, COUNT(*) FROM {f} GROUP BY c",
        "SELECT id, data ->> 'kind' AS k FROM {f} ORDER BY k, id LIMIT 30",
    ],
)
def test_paths_read_from_our_file_as_from_duckdbs(written, sql):
    source, ours = written
    theirs = _json_cells(_rows(sql.format(f=f"READ_PARQUET('{source}')"), pushed=True))
    assert _json_cells(_rows(sql.format(f=f"READ_PARQUET('{ours}')"), pushed=True)) == theirs
    assert _json_cells(_rows(sql.format(f=f"READ_PARQUET('{ours}')"), pushed=False)) == theirs


def test_the_whole_value_is_the_json_written(written):
    _, ours = written
    rows = dict(_rows(f"SELECT id, data FROM READ_PARQUET('{ours}')", pushed=True))
    assert [_value(rows[i]) for i in range(ROWS)] == WANT


def test_the_shredding_is_learnt_from_the_first_row_group(tmp_path):
    # `k` is a string through the first row group and an integer after it: it is shredded as
    # a string, and the integers are held in its residual `value`.
    documents = [{"k": f"s{i}"} if i < 1024 else {"k": i} for i in range(2048)]
    _, morsel = _source_morsel(tmp_path, documents)
    ours = _write(tmp_path, morsel, max_rows_per_row_group=1024)
    types = [row[1] for row in _schema(ours) if row[0] == "typed_value"]
    assert types == [None, "BYTE_ARRAY"]
    assert _duck_values(ours) == [_value(_text(d)) for d in documents]


def test_a_streamed_file_keeps_the_shredding_of_its_first_row_group(tmp_path):
    first = [{"a": i, "b": "x"} for i in range(1000)]
    second = [{"a": "now a string", "c": [i]} if i % 2 else None for i in range(1000)]
    _, m1 = _source_morsel(tmp_path, first, "one.parquet")
    _, m2 = _source_morsel(tmp_path, second, "two.parquet")
    parts = []
    with rp.open_parquet_writer(parts.append) as writer:
        writer.write_row_group(m1)
        writer.write_row_group(m2)
    ours = str(tmp_path / "streamed.parquet")
    with open(ours, "wb") as f:
        f.write(b"".join(parts))
    names = [row[0] for row in _schema(ours)]
    assert "a" in names and "b" in names and "c" not in names
    want = [_value(_text(d)) for d in first + second]
    got = [_value(t) for t in duckdb.sql(f"SELECT data::JSON::VARCHAR FROM '{ours}'").fetchall() for t in t]
    assert got == want


@pytest.mark.parametrize(
    "options",
    [
        {"max_page_bytes": 2048},
        {"max_page_bytes": 2048, "page_index": False},
        {"dictionary": False},
        {"statistics": False},
        {"compression": "none", "max_rows_per_row_group": 0},
    ],
)
def test_the_writer_options_keep_the_values(written, tmp_path, options):
    source, _ = written
    morsel = _morsel(f"SELECT id, data FROM READ_PARQUET('{source}') ORDER BY id")
    ours = _write(tmp_path, morsel, **options)
    assert _duck_values(ours) == _duck_values(source)
    rows = _rows(f"SELECT id, data -> 'commit' ->> 'collection' FROM READ_PARQUET('{ours}')", pushed=True)
    assert rows == _rows(f"SELECT id, data -> 'commit' ->> 'collection' FROM READ_PARQUET('{source}')", pushed=True)


def test_the_variant_takes_its_field_id_on_its_group(written, tmp_path):
    source, _ = written
    morsel = _morsel(f"SELECT id, data FROM READ_PARQUET('{source}') ORDER BY id")
    ours = _write(tmp_path, morsel, field_ids={"id": 1, "data": 2})
    ids = {row[0]: row[4] for row in _schema(ours) if row[4] is not None}
    assert ids == {"id": 1, "data": 2}


@pytest.mark.parametrize(
    "options, refusal",
    [
        ({"sorted_by": "data"}, "sorted_by names column 'data', a VARIANT"),
        ({"bloom_filters": ["data"]}, "bloom_filters names column 'data', a VARIANT"),
        ({"alp": ["data"]}, "alp names column 'data', which is not FLOAT32/FLOAT64"),
    ],
)
def test_options_that_cannot_apply_to_a_variant_are_refused(written, options, refusal):
    source, _ = written
    morsel = _morsel(f"SELECT id, data FROM READ_PARQUET('{source}') ORDER BY id")
    with pytest.raises(ValueError, match=refusal):
        rp.write_parquet(morsel, **options)


def test_a_column_may_be_named_like_one_of_the_variants_leaves(written, tmp_path):
    source, _ = written
    morsel = _morsel(f"SELECT id, data, CAST(id AS VARCHAR) AS \"data.metadata\" FROM READ_PARQUET('{source}') ORDER BY id")
    ours = _write(tmp_path, morsel)
    assert [c.name for c in rp.read_metadata(ours).schema_columns] == ["id", "data", "data.metadata"]
    assert _duck_values(ours) == _duck_values(source)


def test_bounds_are_keyed_by_the_morsels_column(written):
    source, _ = written
    morsel = _morsel(f"SELECT id, data, CAST(id AS VARCHAR) AS s FROM READ_PARQUET('{source}') ORDER BY id")
    _, bounds = rp.write_parquet_with_bounds(morsel)
    assert bounds == {2: ("0", "999")}   # `s`, after the VARIANT's leaves (an INT32 has no bound)


@pytest.mark.parametrize("which", [0, 1])
def test_rugo_reads_a_variant_as_its_json_text(written, which):
    path = written[which]   # DuckDB's file, then ours
    ids, texts = [], []
    with rp.read_parquet(path) as reader:
        for morsel in reader:
            assert morsel.column_names == [b"id", b"data"]
            ids.extend(morsel.column("id").to_pylist())
            texts.extend(morsel.column("data").to_pylist())
    assert ids == list(range(ROWS))
    assert [_value(t) for t in texts] == WANT
    with rp.read_parquet(path, columns=["data"], predicates=[("id", "<", 100)]) as reader:
        only = [t for morsel in reader for t in morsel.column("data").to_pylist()]
    assert [_value(t) for t in only] == WANT[:100]


def test_a_patch_carries_a_variant_through(written, tmp_path):
    _, ours = written
    with open(ours, "rb") as f:
        source = f.read()
    patched = str(tmp_path / "patched.parquet")
    with open(patched, "wb") as f:
        f.write(rp.patch_columns(source, rename={"id": "ident"}))
    assert [c.name for c in rp.read_metadata(patched).schema_columns] == ["ident", "data"]
    assert _duck_values(patched, "ident") == _duck_values(ours)
    for change in ({"drop": ["data"]}, {"rename": {"data": "d"}}):
        with pytest.raises(RuntimeError, match="'data' is a VARIANT; a patch carries it through unchanged"):
            rp.patch_columns(source, **change)


def test_wide_numbers_are_written_exactly(tmp_path):
    # Built by rugo's JSONL reader, which keeps each document's text: integers past int64 and
    # decimals of up to 38 digits are held as decimals (D3), shredded at their scale where one
    # dominates, and read back exactly - by DuckDB and by us.
    from draken.morsels.morsel import Morsel
    from rugo.rugo_native import read_jsonl

    lines = []
    for i in range(2000):
        lines.append(json.dumps({"id": i}).replace("}", ', "data": {"big": 12345678901234567890.%03d, '
                                                     '"wide": 9223372036854775808, "p": %d.25, "neg": -%d, "": %d}}' % (i % 1000, i, i, i)))
    result = read_jsonl(("\n".join(lines) + "\n").encode(), explicit_schema={"id": "INT64", "data": "VARIANT"})
    morsel = Morsel.from_vectors(result["column_names"], result["columns"])
    ours = _write(tmp_path, morsel)
    leaves = {row[0]: row for row in _schema(ours)}
    assert "big" in leaves and "p" in leaves
    want = [_value(line[line.index('"data": ') + len('"data": '):-1]) for line in lines]
    assert _duck_values(ours) == want
    rows = dict(_rows(f"SELECT id, data FROM READ_PARQUET('{ours}')", pushed=True))
    assert [_value(rows[i]) for i in range(2000)] == want
    exact = dict(_rows(f"SELECT id, data ->> 'big' FROM READ_PARQUET('{ours}')", pushed=True))
    assert exact[7] == "12345678901234567890.007"
