"""A `->>` path ending at a shredded string leaf is read as that leaf (Part A of
docs/parquet_variant_path_read_design.md): in a row group where the destination field's `value`
holds nothing, the scan emits the leaf itself, dictionary kept, instead of answering row by row.

Every test compares the answers with the switch (FEATURE_DISABLE_VARIANT_LEAF_PASSTHROUGH) on and
off; where the condition holds it also checks the column came back dictionary-shaped, which only
the pass-through produces.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import duckdb
import pytest

import opteryx
from opteryx import config
from rugo import parquet as rp

KINDS = ["commit", "identity", "account"]
OPS = ["create", "delete", "update"]


def _doc(i):
    d = {"id": i, "kind": KINDS[i % 3], "commit": {"operation": OPS[i % 3], "collection": f"c{i % 4}"}}
    if i % 7 == 0:
        del d["kind"]              # a missing field: NULL
    if i % 11 == 0:
        del d["commit"]            # a missing parent: NULL for every path under it
    return d


def _jsonl(tmp_path, docs, name="src.jsonl"):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(d, separators=(",", ":")) for d in docs) + "\n")
    return str(path)


def _write(tmp_path, jsonl, name="v.parquet", **options):
    """`jsonl` read whole into one VARIANT column (with a plain `id` beside it) and written by rugo."""
    morsels = list(opteryx.session().execute_to_morsels(
        f"SELECT CAST(data->>'id' AS INT64) AS id, data "
        f"FROM READ_JSONL('{jsonl}', as_variant => 'data') ORDER BY id"
    ))
    assert len(morsels) == 1
    path = str(tmp_path / name)
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsels[0], **options))
    return path


def _read(sql, passthrough):
    config.features.disable_variant_leaf_passthrough = not passthrough
    try:
        rows, shapes = [], []
        for morsel in opteryx.session().execute_to_morsels(sql):
            columns = [morsel.column(n) for n in morsel.column_names]
            shapes.extend(c.is_dict for c in columns)
            rows.extend(zip(*[c.to_pylist() for c in columns]))
        return sorted(rows, key=repr), shapes
    finally:
        config.features.disable_variant_leaf_passthrough = False


def _same(sql):
    on, on_shapes = _read(sql, True)
    off, off_shapes = _read(sql, False)
    assert on == off
    assert not any(off_shapes)       # the general path never builds a dictionary
    return on, on_shapes


DOCS = [_doc(i) for i in range(400)]


def test_a_dictionary_leaf_passes_through(tmp_path):
    path = _write(tmp_path, _jsonl(tmp_path, DOCS))
    rows, shapes = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}')")
    assert rows == sorted([(d["id"], d.get("kind")) for d in DOCS], key=repr)
    assert shapes[1::2] and all(shapes[1::2])


def test_a_nested_path(tmp_path):
    path = _write(tmp_path, _jsonl(tmp_path, DOCS))
    rows, shapes = _same(
        f"SELECT id, data->'commit'->>'collection', data->'commit'->>'operation' FROM READ_PARQUET('{path}')"
    )
    want = [(d["id"], d.get("commit", {}).get("collection"), d.get("commit", {}).get("operation")) for d in DOCS]
    assert rows == sorted(want, key=repr)
    assert all(shapes[1::3]) and all(shapes[2::3])


def test_a_leaf_present_in_every_row(tmp_path):
    # No NULLs and no mask: the indices are narrowed in one pass. `kind` has a one-byte code,
    # `tag` (300 entries) a two-byte one.
    docs = [{"id": i, "kind": KINDS[i % 3], "tag": f"t{i % 300}"} for i in range(1200)]
    path = _write(tmp_path, _jsonl(tmp_path, docs))
    rows, shapes = _same(f"SELECT id, data->>'kind', data->>'tag' FROM READ_PARQUET('{path}')")
    assert rows == sorted([(d["id"], d["kind"], d["tag"]) for d in docs], key=repr)
    assert all(shapes[1::3]) and all(shapes[2::3])


def test_a_file_without_statistics(tmp_path):
    # No null count in the footer to trust: `value` is proven empty from its levels, as before.
    path = _write(tmp_path, _jsonl(tmp_path, DOCS), statistics=False, page_index=False)
    rows, shapes = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}')")
    assert rows == sorted([(d["id"], d.get("kind")) for d in DOCS], key=repr)
    assert shapes[1::2] and all(shapes[1::2])


def test_plain_pages(tmp_path):
    path = _write(tmp_path, _jsonl(tmp_path, DOCS), dictionary=False)
    rows, _ = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}')")
    assert rows == sorted([(d["id"], d.get("kind")) for d in DOCS], key=repr)


def test_a_row_group_holding_a_non_string_takes_the_general_path(tmp_path):
    # Shredding is learnt from the first row group (all strings); the second holds a number and
    # a JSON null in `kind`, so its `value` is not empty there and that row group falls back.
    docs = [{"id": i, "kind": f"k{i % 2}"} for i in range(8)]
    docs[5]["kind"] = 5
    docs[6]["kind"] = None
    path = _write(tmp_path, _jsonl(tmp_path, docs), max_rows_per_row_group=4)
    rows, _ = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}')")
    assert rows == [(0, "k0"), (1, "k1"), (2, "k0"), (3, "k1"), (4, "k0"), (5, "5"), (6, None), (7, "k1")]


def test_under_a_row_mask(tmp_path):
    # The pushed `id` filter masks rows before the fold; the pass-through honours the mask.
    path = _write(tmp_path, _jsonl(tmp_path, DOCS), max_rows_per_row_group=64)
    rows, _ = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}') WHERE id % 5 = 0 AND id > 100")
    assert rows == sorted([(d["id"], d.get("kind")) for d in DOCS if d["id"] % 5 == 0 and d["id"] > 100], key=repr)


def test_a_filter_and_group_on_the_path(tmp_path):
    path = _write(tmp_path, _jsonl(tmp_path, DOCS))
    rows, _ = _same(
        f"SELECT data->>'kind' AS k, COUNT(*) FROM READ_PARQUET('{path}') "
        f"WHERE data->'commit'->>'operation' = 'create' GROUP BY k"
    )
    want = {}
    for d in DOCS:
        if d.get("commit", {}).get("operation") == "create":
            want[d.get("kind")] = want.get(d.get("kind"), 0) + 1
    assert rows == sorted(want.items(), key=repr)


def test_null_records(tmp_path):
    # A NULL VARIANT row reads NULL down every path.
    src = str(tmp_path / "duck.parquet")
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT * FROM (VALUES (1, '{\"kind\":\"a\"}'::JSON::VARIANT), (2, NULL), "
        "(3, '{\"kind\":\"b\"}'::JSON::VARIANT)) t(id, data)) TO '" + src + "' (FORMAT PARQUET)"
    )
    morsel = next(iter(opteryx.session().execute_to_morsels(f"SELECT id, data FROM READ_PARQUET('{src}') ORDER BY id")))
    path = str(tmp_path / "ours.parquet")
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsel))
    rows, _ = _same(f"SELECT id, data->>'kind' FROM READ_PARQUET('{path}')")
    assert rows == [(1, "a"), (2, None), (3, "b")]


def test_the_json_form_is_not_passed_through(tmp_path):
    # `->` answers JSON: a string comes back quoted, so the leaf is not the answer.
    path = _write(tmp_path, _jsonl(tmp_path, DOCS[:10]))
    rows, shapes = _same(f"SELECT id, data->'kind' FROM READ_PARQUET('{path}')")
    assert rows == sorted([(d["id"], json.dumps(d["kind"]) if "kind" in d else None) for d in DOCS[:10]], key=repr)
    assert not any(shapes)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
