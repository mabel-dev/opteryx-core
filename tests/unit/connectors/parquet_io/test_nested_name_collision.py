"""A top-level column keeps its own type when a nested field elsewhere in the file has the same
name. rugo's footer reader keyed its logical-type map by each nested element's bare name as well
as its full name, so a STRUCT member, a nested LIST or a shredded VARIANT field named like a
top-level column overwrote that column's entry, and the scan refused the column
(e.g. "physical type 'int64' and logical type 'json'").
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import duckdb

import opteryx
from rugo import parquet as rp


def _rows(sql):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return sorted(rows, key=repr)


def test_a_shredded_variant_field(tmp_path):
    src = tmp_path / "src.jsonl"
    src.write_text('{"kind":"a","id":1}\n{"kind":"b","id":2}\n')
    morsel = next(iter(opteryx.session().execute_to_morsels(
        f"SELECT 7 AS kind, 8 AS id, data FROM READ_JSONL('{src}', as_variant => 'data')"
    )))
    path = str(tmp_path / "v.parquet")
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsel))
    assert _rows(f"SELECT kind, id, data->>'kind' FROM READ_PARQUET('{path}')") == [(7, 8, "a"), (7, 8, "b")]


def test_a_struct_member_and_a_nested_list(tmp_path):
    # The STRUCT member `a` and the nested LIST `l` come after the top-level `a` and `l`.
    path = str(tmp_path / "s.parquet")
    duckdb.sql(
        "COPY (SELECT 1::BIGINT AS a, 'x' AS l, {'a': 'text', 'l': [1, 2]} AS s) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    assert _rows(f"SELECT a, l FROM READ_PARQUET('{path}')") == [(1, "x")]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
