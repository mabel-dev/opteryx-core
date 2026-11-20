"""ParquetVariantExtractionStrategy: `data -> 'commit' ->> 'collection'` over a READ_PARQUET
VARIANT column is read by the scan from the columns the file shredded the value into, instead
of rendering every row's whole value to JSON text and parsing it again
(docs/parquet_variant_design.md section 4.3).

The strategy may only ever change HOW a value is read, never WHAT it is: every query here runs
with the strategy on and off and must return the same rows, over a file that is not shredded,
one DuckDB shredded as it chose, and one shredded to a schema we gave it - so a path is read
from a typed column, from the residual binary value, or from both, depending on the row. A spy
on the scan's plan asserts what rugo was asked for.

The same holds for a file that holds the column as JSON text (how a VARIANT was written before
the Parquet VARIANT type): mixed with a VARIANT-form file in one glob, and as a table's files.
"""

import json
import os
import sys

import duckdb
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

# The catalog is the sibling checkout on a dev machine and the installed package in CI
# (tests/tools/local_catalog.py).
_CATALOG_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../opteryx-catalog"))
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

import opteryx
from opteryx import config
from opteryx.connectors import OpteryxConnector
from opteryx.connectors.parquet_io import pool_reader
from opteryx.exceptions import NativeScanRefusedError

try:
    import opteryx_catalog  # noqa: F401

    _HAVE_CATALOG = True
except ImportError:
    _HAVE_CATALOG = False

ROWS = 2500

SHREDDINGS = {
    "unshredded": ", SHREDDING {data: 'VARIANT'}",
    "auto": "",
    # `text`, `n` and the rest stay in the residual value; `commit` is shredded one level down
    "partial": ", SHREDDING {data: 'STRUCT(kind VARCHAR, \"commit\" STRUCT(collection VARCHAR, rev BIGINT))'}",
}


def _document(i):
    shape = i % 16
    commit = {"collection": f"c{i % 7}", "operation": "create", "rev": i, "record": {"text": f"t{i}", "langs": ["en", None]}}
    if shape == 0:
        return {"kind": "commit", "n": i, "commit": commit, "text": "plain"}
    if shape == 1:
        return {"kind": "commit", "n": 1.5, "commit": commit, "text": 'esc "q" / \\ \n \t \u0001 é 😀'}
    if shape == 2:
        return {"kind": 7, "commit": "not an object", "text": "null"}
    if shape == 3:
        return None  # SQL NULL
    if shape == 4:
        return "JSON null"
    if shape == 5:
        return "a string"
    if shape == 6:
        return {}
    if shape == 7:
        return [1, {"kind": "in an array"}]
    if shape == 8:
        return {"kind": None, "commit": {"collection": None, "rev": "not a number"}}
    if shape == 9:
        return {"kind": "identity", "commit": {"collection": {"nested": "object"}, "rev": 2.5}}
    if shape == 10:
        return {"kind": ["a", "list"], "commit": {"collection": ["a", "b"]}}
    if shape == 11:
        return {"kind": "keys", "commit": {"a'b": "quote", "a.b": "dotted", "0": "zero", "->": "arrow", "": "empty"}}
    if shape == 12:
        return {"kind": "numbers", "n": 9007199254740993, "f": 1e300, "t": True, "z": False, "commit": {}}
    if shape == 13:
        return {"kind": "", "text": "", "commit": {"collection": "", "rev": 0}}
    if shape == 14:
        return {"kind": "commit", "commit": {"collection": "x" * 300, "rev": -1}, "text": "y" * 300}
    return {"kind": "account", f"extra_{i % 3}": i, "commit": [1, 2]}


def _json_text(i):
    document = _document(i)
    if document is None:
        return None
    return "null" if document == "JSON null" else json.dumps(document)


def _write(path, shredding, rows=ROWS):
    con = duckdb.connect()
    con.execute("CREATE TABLE src (id INTEGER, j VARCHAR)")
    con.executemany("INSERT INTO src VALUES (?, ?)", [(i, _json_text(i)) for i in range(rows)])
    con.execute(
        f"COPY (SELECT id, j::JSON::VARIANT AS data FROM src ORDER BY id) TO '{path}' "
        f"(FORMAT PARQUET, ROW_GROUP_SIZE 1024{SHREDDINGS[shredding]})"
    )


@pytest.fixture(scope="module", params=sorted(SHREDDINGS))
def data(request, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("variant_paths") / f"{request.param}.parquet")
    _write(path, request.param)
    return f"READ_PARQUET('{path}')"


def _run(sql, pushed):
    """Rows of `sql` (sorted by repr) with the strategy on/off, the columns the scan asked rugo
    for, and the session."""
    original_flag = config.features.disable_parquet_variant_extraction
    original_open = pool_reader.open_native_scan_plan
    asked = set()

    def spy(paths, names, **kwargs):
        asked.update(names)
        return original_open(paths, names, **kwargs)

    config.features.disable_parquet_variant_extraction = not pushed
    pool_reader.open_native_scan_plan = spy
    try:
        session = opteryx.session(user="tester")
        rows = []
        for morsel in session.execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(name).to_pylist() for name in morsel.column_names]))
    finally:
        config.features.disable_parquet_variant_extraction = original_flag
        pool_reader.open_native_scan_plan = original_open
    return sorted(rows, key=repr), asked, session


def _same_both_ways(sql):
    on, asked, _ = _run(sql, pushed=True)
    off, asked_off, _ = _run(sql, pushed=False)
    assert on == off
    assert not any("->" in name for name in asked_off)
    return on, asked


def test_paths_are_read_by_the_scan(data):
    rows, asked = _same_both_ways(
        "SELECT id, data ->> 'kind', data -> 'kind', data -> 'commit' ->> 'collection', "
        "data -> 'commit' -> 'collection', data -> 'commit' ->> 'rev', data -> 'commit', "
        "data -> 'commit' -> 'record' -> 'langs', data -> 'commit' -> 'record' ->> 'text', "
        f"data ->> 'text', data -> 'text', data ->> 'n', data ->> 'f', data ->> 't', data -> 'nope' ->> 'x' FROM {data}"
    )
    assert len(rows) == ROWS
    assert {"data->>'kind'", "data->'commit'->>'collection'", "data->'commit'->'record'->'langs'",
            "data->'nope'->>'x'"} <= asked
    assert "data" not in asked
    by_id = {row[0]: row[1:] for row in rows}
    assert by_id[0][:3] == ("commit", '"commit"', "c0")
    assert by_id[2][:3] == ("7", "7", None)            # a number where a string is shredded; `commit` is no object
    assert by_id[2][8] == "null"                       # the string "null", not NULL
    assert by_id[8][:4] == (None, None, None, None)    # JSON null reads as NULL through either operator
    assert by_id[9][2:4] == ('{"nested":"object"}', '{"nested":"object"}')
    assert by_id[7] == (None,) * 14                    # an array has no keys
    assert by_id[3] == (None,) * 14                    # SQL NULL


def test_keys_that_need_quoting(data):
    rows, asked = _same_both_ways(
        "SELECT id, data -> 'commit' ->> 'a''b', data -> 'commit' ->> '->', data -> 'commit' ->> 'a.b', "
        f"data -> 'commit' ->> '0' FROM {data}"
    )
    assert {"data->'commit'->>'a''b'", "data->'commit'->>'->'"} <= asked
    # A dotted path and an array index are several steps, or an element, to `->>`: not one key.
    assert not {name for name in asked if "a.b" in name or "'0'" in name}
    by_id = {row[0]: row[1:] for row in rows}
    assert by_id[11][:2] == ("quote", "arrow")


def test_the_whole_value_beside_a_path(data):
    rows, asked = _same_both_ways(f"SELECT id, data, data ->> 'kind' FROM {data}")
    assert {"data", "data->>'kind'"} <= asked
    assert len(rows) == ROWS


@pytest.mark.parametrize(
    "condition",
    [
        "data ->> 'kind' = 'commit'",
        "data ->> 'kind' <> 'commit'",
        "data ->> 'kind' IN ('commit', '7', '')",
        "data ->> 'kind' IS NULL",
        "data ->> 'kind' IS NOT NULL",
        "data -> 'commit' ->> 'collection' = 'c3'",
        "data -> 'commit' ->> 'collection' LIKE 'c%'",
        "data -> 'commit' ->> 'rev' = '16'",
        "data ->> 'text' = 'null'",
        "data ->> 'kind' = 'commit' AND id < 700",
        "data ->> 'kind' = 'commit' OR data -> 'commit' ->> 'rev' = '-1'",
    ],
)
def test_filters_on_paths(data, condition):
    rows, asked = _same_both_ways(f"SELECT id FROM {data} WHERE {condition}")
    assert "data" not in asked
    assert 0 < len(rows) < ROWS


def test_count_with_only_a_filter_on_a_path(data):
    rows, asked = _same_both_ways(f"SELECT COUNT(*) FROM {data} WHERE data ->> 'kind' = 'commit'")
    assert asked == {"data->>'kind'"}
    assert rows == [(sum(1 for i in range(ROWS) if i % 16 in (0, 1, 14)),)]


def test_grouping_and_ordering_by_a_path(data):
    rows, asked = _same_both_ways(
        f"SELECT data -> 'commit' ->> 'collection' AS c, COUNT(*) AS n FROM {data} GROUP BY c"
    )
    assert asked == {"data->'commit'->>'collection'"}
    assert sum(n for _, n in rows) == ROWS
    _same_both_ways(f"SELECT id, data ->> 'kind' AS k FROM {data} ORDER BY k, id LIMIT 40")
    _same_both_ways(f"SELECT id FROM {data} WHERE id > 100 ORDER BY data -> 'commit' ->> 'collection', id LIMIT 25")


def test_columns_keep_their_places_when_paths_share_a_read(data):
    # Every column read out of `data` shares one read of it, wherever it sits in the projection.
    rows, asked = _same_both_ways(
        "SELECT data ->> 'kind', id, data -> 'commit' ->> 'rev', data, id + 1, data ->> 'n', "
        f"data -> 'commit' FROM {data}"
    )
    assert {"data", "data->>'kind'", "data->'commit'->>'rev'", "data->>'n'", "data->'commit'"} <= asked
    by_id = {row[1]: row for row in rows}
    assert by_id[16][:3] == ("commit", 16, "16") and by_id[16][4:6] == (17, "16")
    assert json.loads(by_id[16][3])["commit"] == json.loads(by_id[16][6])


def test_every_column_read_out_of_a_variant_shares_one_read_of_it(tmp_path):
    # An unshredded file: every path needs the same two chunks, and so does the whole value.
    path = str(tmp_path / "unshredded.parquet")
    _write(path, "unshredded")
    held = lambda select: _run(f"SELECT {select} FROM READ_PARQUET('{path}')", pushed=True)[2].telemetry[
        "io_held_peak_bytes"
    ]
    one = held("data ->> 'kind'")
    assert held("data ->> 'kind', data -> 'commit' ->> 'rev', data ->> 'text', data -> 'commit'") == one
    assert held("data, data ->> 'kind', data -> 'commit' ->> 'rev'") == held("data")


def test_re_parsing_a_string_is_not_a_chain(data):
    # `(data ->> 'commit') ->> 'collection'` parses the TEXT the inner step yields - another
    # operation. The inner step is still a path; the outer one is evaluated as before.
    rows, asked = _same_both_ways(
        f"SELECT id, (data ->> 'commit') ->> 'collection' FROM {data} WHERE id % 16 = 0"
    )
    assert {name for name in asked if "->" in name} == {"data->>'commit'"}
    assert rows[0] == (0, "c0")


def test_a_path_reads_less_than_the_value_it_is_in(tmp_path):
    # The point of the strategy: `text` is not shredded, so it lives in the top-level residual
    # value, and a path into a shredded field does not read that.
    path = str(tmp_path / "partial.parquet")
    _write(path, "partial")
    sql = f"SELECT data -> 'commit' ->> 'collection' FROM READ_PARQUET('{path}')"
    _, _, pushed = _run(sql, pushed=True)
    _, _, whole = _run(sql, pushed=False)
    assert pushed.telemetry["io_held_peak_bytes"] * 2 < whole.telemetry["io_held_peak_bytes"]


# Every Variant scalar type, read through both operators from a typed column and from Variant
# binary: the path read renders each as the whole-value read does.
SCALARS = {
    "d": "DATE '2024-01-02'",
    "ts": "TIMESTAMP '2024-01-02 03:04:05.123456'",
    "tz": "TIMESTAMPTZ '2024-01-02 03:04:05+00'",
    "tm": "TIME '01:02:03'",
    "dec": "1.50::DECIMAL(10,2)",
    "big": "12345678901234567890.12::DECIMAL(38,2)",
    "b": "'ab'::BLOB",
    "u": "'123e4567-e89b-12d3-a456-426614174000'::UUID",
    "f": "1.5::FLOAT",
    "dbl": "2.5e10::DOUBLE",
    "nan": "'NaN'::DOUBLE",
    "inf": "'-Infinity'::DOUBLE",
    "i8": "1::TINYINT",
    "i64": "-5000000000::BIGINT",
    "bo": "true",
    "long": "repeat('x', 100)",
    "short": "'hi'",
    "quoted": "'\"hi\"'",
}


@pytest.mark.parametrize("shredding", ["unshredded", "auto"])
def test_every_scalar_type_reads_alike(shredding, tmp_path):
    path = str(tmp_path / f"scalars_{shredding}.parquet")
    members = ", ".join(f"'{name}': {sql}" for name, sql in SCALARS.items())
    duckdb.sql(
        f"COPY (SELECT {{{members}}}::VARIANT AS data FROM range(3)) TO '{path}' (FORMAT PARQUET{SHREDDINGS[shredding]})"
    )
    select = ", ".join(f"data ->> '{name}', data -> '{name}'" for name in SCALARS)
    rows, asked = _same_both_ways(f"SELECT {select} FROM READ_PARQUET('{path}')")
    assert "data" not in asked
    row = dict(zip([f"{name}{op}" for name in SCALARS for op in ("->>", "->")], rows[0]))
    assert (row["d->>"], row["d->"]) == ("2024-01-02", '"2024-01-02"')
    assert (row["short->>"], row["short->"]) == ("hi", '"hi"')
    assert (row["quoted->>"], row["quoted->"]) == ('"hi"', '"\\"hi\\""')
    assert (row["nan->>"], row["nan->"], row["inf->>"]) == (None, None, None)   # JSON has no NaN: null
    assert (row["dec->>"], row["bo->>"]) == ("1.50", "true")


def test_a_file_without_the_column_reads_null(tmp_path):
    _write(str(tmp_path / "a.parquet"), "auto", rows=40)
    duckdb.sql(f"COPY (SELECT (range + 1000)::INTEGER AS id FROM range(5)) TO '{tmp_path / 'b.parquet'}' (FORMAT PARQUET)")
    rows, asked = _same_both_ways(
        f"SELECT id, data ->> 'kind', data -> 'commit' ->> 'collection' FROM READ_PARQUET('{tmp_path}/*.parquet')"
    )
    assert "data" not in asked
    assert [row for row in rows if row[0] >= 1000] == [(1000 + i, None, None) for i in range(5)]


# ── The same column held as JSON text ────────────────────────────────────────────────────────
# How a VARIANT column was written before the Parquet VARIANT type; such files stay readable
# (D4). The scan answers a path from the text: one parse of each row, shared by every path
# over the column - the parse, the walk and the rendering `->` / `->>` use unpushed.

TEXT_FIRST_ID = 100_000


def _write_text(path, rows=ROWS, dictionary=False):
    """The documents of `_write`, held as a plain string column; ids from TEXT_FIRST_ID."""
    con = duckdb.connect()
    con.execute("CREATE TABLE src (id INTEGER, data VARCHAR)")
    con.executemany(
        "INSERT INTO src VALUES (?, ?)",
        [(TEXT_FIRST_ID + i, _json_text(i % 16 if dictionary else i)) for i in range(rows)],
    )
    con.execute(f"COPY (SELECT * FROM src ORDER BY id) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 1024)")


@pytest.fixture(scope="module", params=["plain", "dictionary"])
def mixed(request, tmp_path_factory):
    """A glob of two files: `data` as a shredded VARIANT in one (which binds the column
    VARIANT), as JSON text in the other - few distinct documents, so dictionary pages, or many."""
    folder = tmp_path_factory.mktemp(f"mixed_{request.param}")
    _write(str(folder / "a.parquet"), "auto")
    _write_text(str(folder / "b.parquet"), dictionary=request.param == "dictionary")
    return f"READ_PARQUET('{folder}/*.parquet')"


def test_paths_are_read_from_a_file_that_holds_the_column_as_text(mixed):
    rows, asked = _same_both_ways(
        "SELECT id, data ->> 'kind', data -> 'kind', data -> 'commit' ->> 'collection', "
        "data -> 'commit' -> 'collection', data -> 'commit' ->> 'rev', data -> 'commit', "
        "data -> 'commit' -> 'record' -> 'langs', data -> 'commit' -> 'record' ->> 'text', "
        f"data ->> 'text', data -> 'text', data ->> 'n', data ->> 'f', data ->> 't', data -> 'nope' ->> 'x' FROM {mixed}"
    )
    assert len(rows) == 2 * ROWS
    assert "data" not in asked and "data->'commit'->>'collection'" in asked
    by_id = {row[0]: row[1:] for row in rows}
    text = lambda i: by_id[TEXT_FIRST_ID + i]
    assert text(0)[:3] == ("commit", '"commit"', "c0")
    assert text(2)[:3] == ("7", "7", None)             # `commit` is a string there, not an object
    assert text(2)[8] == "null"                        # the string "null", not NULL
    assert text(8)[:4] == (None, None, None, None)     # JSON null reads as NULL through either operator
    assert text(9)[2:4] == ('{"nested":"object"}', '{"nested":"object"}')
    assert text(1)[8] == 'esc "q" / \\ \n \t \u0001 é 😀'
    assert text(7) == (None,) * 14                     # an array has no keys
    assert text(3) == (None,) * 14                     # SQL NULL
    assert text(4) == (None,) * 14                     # the document `null`


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM {mixed} WHERE data ->> 'kind' = 'commit'",
        "SELECT id FROM {mixed} WHERE data -> 'commit' ->> 'collection' LIKE 'c%' AND id % 3 = 0",
        "SELECT id FROM {mixed} WHERE data ->> 'kind' IS NULL",
        "SELECT COUNT(*) FROM {mixed} WHERE data ->> 'kind' = 'commit'",
        "SELECT data -> 'commit' ->> 'collection' AS c, COUNT(*) AS n FROM {mixed} GROUP BY c",
        "SELECT id, data ->> 'kind' AS k FROM {mixed} ORDER BY k, id LIMIT 40",
        "SELECT id, data, data ->> 'kind' FROM {mixed} WHERE id % 50 = 0",
        "SELECT id, data ->> 'kind' FROM {mixed} WHERE id > 100500 AND id < 100600",
    ],
)
def test_queries_over_both_forms(mixed, sql):
    rows, asked = _same_both_ways(sql.format(mixed=mixed))
    assert rows and any("->" in name for name in asked)


def test_text_is_parsed_only_for_the_rows_the_scan_keeps(tmp_path):
    # Row 5 of the text file is not JSON. A filter the scan itself applies drops it before
    # the path is read, so the query answers; reading the row is an error, never a NULL.
    _write(str(tmp_path / "a.parquet"), "auto", rows=40)
    duckdb.sql(
        f"COPY (SELECT ({TEXT_FIRST_ID} + range)::INTEGER AS id, "
        "CASE WHEN range = 5 THEN '{not json' ELSE '{\"kind\": \"text\"}' END AS data "
        f"FROM range(10)) TO '{tmp_path / 'b.parquet'}' (FORMAT PARQUET)"
    )
    source = f"READ_PARQUET('{tmp_path}/*.parquet')"
    rows, _ = _same_both_ways(f"SELECT id, data ->> 'kind' FROM {source} WHERE id = {TEXT_FIRST_ID + 4}")
    assert rows == [(TEXT_FIRST_ID + 4, "text")]
    with pytest.raises(Exception, match="invalid JSON at row 5"):
        _run(f"SELECT id, data ->> 'kind' FROM {source}", pushed=True)


def test_a_file_holding_the_column_as_neither_is_refused(tmp_path):
    # A path can be read out of a VARIANT or out of JSON text, not out of an integer: the
    # scan says so, naming the file.
    _write(str(tmp_path / "a.parquet"), "auto", rows=40)
    duckdb.sql(f"COPY (SELECT 1000 AS id, 7 AS data) TO '{tmp_path / 'b.parquet'}' (FORMAT PARQUET)")
    with pytest.raises(NativeScanRefusedError, match="b.parquet.*neither a VARIANT nor a string"):
        _run(f"SELECT id, data ->> 'kind' FROM READ_PARQUET('{tmp_path}/*.parquet')", pushed=True)


# ── A table's VARIANT column ─────────────────────────────────────────────────────────────────
# The catalog declares the column VARIANT. The engine's writer holds it as a shredded VARIANT;
# files written before it hold JSON text and stay in the table, so a table scan reads both
# forms: manifest pruning, merge-on-read deletes and all.

TABLE = "varws.col.events"


@pytest.fixture
def table(tmp_path):
    """A local-disk catalog table of `_write`'s documents twice over: ids below ROWS in the
    file CTAS writes (a shredded VARIANT), ids from ROWS in a JSON-text file added after it."""
    if not _HAVE_CATALOG:
        pytest.skip(f"opteryx_catalog not importable (expected sibling repo at {_CATALOG_REPO})")
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors
    from tests.tools.local_catalog import LocalDiskIO
    from tests.tools.local_catalog import local_catalog_class

    source = str(tmp_path / "source.parquet")
    _write(source, "unshredded")
    saved = (connectors._default_connector, dict(connectors._storage_prefixes), dict(connectors._connector_cache))
    connectors._connector_cache.clear()
    clear_parsed_manifest_cache()
    catalog = local_catalog_class(str(tmp_path / "catalog"), LocalDiskIO())
    opteryx.set_default_connector(OpteryxConnector, catalog=catalog)
    try:
        _run(f"CREATE TABLE {TABLE} AS SELECT id, data FROM READ_PARQUET('{source}')", pushed=False)
        ds = catalog.store[TABLE]
        text = os.path.join(ds.metadata.location, "data", "text.parquet")
        con = duckdb.connect()
        con.execute("CREATE TABLE src (id INTEGER, data VARCHAR)")
        con.executemany("INSERT INTO src VALUES (?, ?)", [(ROWS + i, _json_text(i)) for i in range(ROWS)])
        con.execute(f"COPY (SELECT * FROM src ORDER BY id) TO '{text}' (FORMAT PARQUET, ROW_GROUP_SIZE 1024)")
        ds.add_files([text], author="tester", read_sources=[])
        clear_parsed_manifest_cache()
        yield TABLE
    finally:
        connectors._default_connector = saved[0]
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved[1])
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved[2])
        clear_parsed_manifest_cache()


def test_paths_are_read_by_a_table_scan(table):
    rows, asked = _same_both_ways(
        "SELECT id, data ->> 'kind', data -> 'kind', data -> 'commit' ->> 'collection', "
        f"data -> 'commit' -> 'record' -> 'langs', data ->> 'text', data ->> 'n', data -> 'nope' ->> 'x' FROM {table}"
    )
    assert len(rows) == 2 * ROWS
    assert "data" not in asked and "data->'commit'->>'collection'" in asked
    by_id = {row[0]: row[1:] for row in rows}
    assert by_id[ROWS + 16] == ("commit", '"commit"', "c2", '["en",null]', "plain", str(16), None)
    assert by_id[3] == (None,) * 7


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM {table} WHERE data ->> 'kind' = 'commit' AND id < 3000",
        "SELECT COUNT(*) FROM {table} WHERE data -> 'commit' ->> 'collection' = 'c3'",
        "SELECT data ->> 'kind' AS k, COUNT(*) AS n, MIN(data -> 'commit' ->> 'rev') FROM {table} GROUP BY k",
        "SELECT COUNT(DISTINCT data -> 'commit' ->> 'collection') FROM {table}",
        "SELECT id, data ->> 'kind' AS k FROM {table} ORDER BY k, id LIMIT 40",
        "SELECT id, data ->> 'text' FROM {table} WHERE id > 100 ORDER BY id DESC LIMIT 25",
        "SELECT id, data, data ->> 'kind' FROM {table} WHERE id % 50 = 0",
        "SELECT a.id, b.data ->> 'kind' FROM {table} AS a JOIN {table} AS b ON a.id = b.id WHERE a.data ->> 'kind' = 'identity'",
    ],
)
def test_queries_over_a_table(table, sql):
    rows, asked = _same_both_ways(sql.format(table=table))
    assert rows and any("->" in name for name in asked)


def test_a_table_scan_with_nothing_left_to_read(table):
    rows, _ = _same_both_ways(f"SELECT id, data ->> 'kind' FROM {table} WHERE id < 0")
    assert rows == []


def test_paths_over_a_table_with_deleted_rows(table):
    _run(f"DELETE FROM {table} WHERE id % 3 = 0", pushed=False)
    rows, asked = _same_both_ways(f"SELECT id, data ->> 'kind', data -> 'commit' ->> 'rev' FROM {table}")
    assert "data" not in asked
    assert [row[0] for row in sorted(rows)] == [i for i in range(2 * ROWS) if i % 3]


def test_the_table_holds_both_forms(table, tmp_path):
    # What the scans above read: the engine wrote a shredded VARIANT, the added file JSON text.
    from rugo import parquet as rp

    forms = {}
    for root, _, files in os.walk(tmp_path / "catalog"):
        for f in files:
            if os.path.basename(root) == "data":
                columns = rp.read_metadata(os.path.join(root, f)).schema_columns
                forms[f == "text.parquet"] = {c.name: c.logical_type for c in columns}["data"]
    assert forms == {False: "variant", True: "varchar"}


def test_every_path_over_a_field_shares_one_read_of_it(table):
    # However many paths read one field - one chunk of JSON text, or a VARIANT field's leaves -
    # they hold what one path does.
    def held(sql):
        return _run(sql, pushed=True)[2].telemetry["io_held_peak_bytes"]

    assert held(f"SELECT data ->> 'kind', data -> 'kind' FROM {table}") == held(f"SELECT data ->> 'kind' FROM {table}")
    assert held(f"SELECT data -> 'commit', data -> 'commit' ->> 'rev' FROM {table}") == held(
        f"SELECT data -> 'commit' FROM {table}"
    )
