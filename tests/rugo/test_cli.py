# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for rugo.cli — the `rugo` command-line tool.

Coverage:
  - info / schema / columns / count on parquet, csv, jsonl
  - preview / head row limiting and column projection
  - describe / stats aggregate column statistics (parquet only)
  - inspect footer/row-group dump (parquet only)
  - describe/stats/inspect reject csv/jsonl with a clean error, not a crash
  - diff: identical schemas, added/removed/changed columns
  - convert: parquet -> jsonl -> csv round trip preserves row count
  - merge: schema-identical files concatenate; mismatched schemas reject
  - split: row-count-bounded chunks that recombine to the original row count
  - missing-file paths error cleanly (not a native process abort)
  - --json emits parseable JSON for every read-side verb
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from rugo.cli import main as cli_main


def _run(capsys, *argv):
    exit_code = cli_main(list(argv))
    out = capsys.readouterr()
    return exit_code, out.out, out.err


@pytest.fixture
def fixture_paths(tmp_path):
    """A small, known morsel written out as parquet/csv/jsonl siblings."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel
    from rugo import parquet as rugo_parquet
    from rugo import csv as rugo_csv
    from rugo import jsonl as rugo_jsonl

    ids = [1, 2, 3, 4, 5]
    names = ["alpha", "bravo", "charlie", "delta", "echo"]
    scores = [1.5, 2.5, None, 4.5, 5.5]

    morsel = Morsel.from_vectors(
        ["id", "name", "score"],
        [
            Vector(dn.vector_from_sequence(ids)),
            Vector(dn.vector_from_string_sequence([n.encode() for n in names])),
            Vector(dn.vector_float64_from_sequence(scores)),
        ],
    )

    paths = {
        "parquet": str(tmp_path / "fixture.parquet"),
        "csv": str(tmp_path / "fixture.csv"),
        "jsonl": str(tmp_path / "fixture.jsonl"),
    }
    with open(paths["parquet"], "wb") as f:
        f.write(rugo_parquet.write_parquet(morsel))
    with open(paths["csv"], "wb") as f:
        f.write(rugo_csv.write_csv(morsel))
    with open(paths["jsonl"], "wb") as f:
        f.write(rugo_jsonl.write_jsonl(morsel))

    return paths


# ---------------------------------------------------------------------------
# info / schema / columns / count
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fmt", ["parquet", "csv", "jsonl"])
def test_info(fixture_paths, capsys, fmt):
    code, out, err = _run(capsys, "info", "--json", fixture_paths[fmt])
    assert code == 0, err
    info = json.loads(out)
    assert info["num_rows"] == 5
    assert info["num_columns"] == 3
    assert info["format"] == fmt


@pytest.mark.parametrize("fmt", ["parquet", "csv", "jsonl"])
def test_schema(fixture_paths, capsys, fmt):
    code, out, err = _run(capsys, "schema", "--json", fixture_paths[fmt])
    assert code == 0, err
    cols = {c["name"] for c in json.loads(out)["columns"]}
    assert cols == {"id", "name", "score"}


def test_columns_text_output(fixture_paths, capsys):
    code, out, err = _run(capsys, "columns", fixture_paths["parquet"])
    assert code == 0, err
    assert out.splitlines() == ["id", "name", "score"]


def test_count(fixture_paths, capsys):
    code, out, err = _run(capsys, "count", fixture_paths["parquet"])
    assert code == 0, err
    assert out.strip() == "5"


# ---------------------------------------------------------------------------
# preview / head
# ---------------------------------------------------------------------------

def test_preview_limit(fixture_paths, capsys):
    code, out, err = _run(capsys, "preview", "-n", "2", "--json", fixture_paths["parquet"])
    assert code == 0, err
    rows = json.loads(out)["rows"]
    assert len(rows) == 2
    assert rows[0]["id"] == 1


def test_preview_column_projection(fixture_paths, capsys):
    code, out, err = _run(capsys, "preview", "-n", "5", "-c", "id,name", "--json", fixture_paths["parquet"])
    assert code == 0, err
    rows = json.loads(out)["rows"]
    assert set(rows[0].keys()) == {"id", "name"}


def test_head_is_alias_for_preview(fixture_paths, capsys):
    code_a, out_a, _ = _run(capsys, "preview", "-n", "3", "--json", fixture_paths["parquet"])
    code_b, out_b, _ = _run(capsys, "head", "-n", "3", "--json", fixture_paths["parquet"])
    assert code_a == code_b == 0
    assert out_a == out_b


# ---------------------------------------------------------------------------
# describe / stats / inspect — parquet only
# ---------------------------------------------------------------------------

def test_describe_aggregates_stats(fixture_paths, capsys):
    code, out, err = _run(capsys, "describe", "--json", fixture_paths["parquet"])
    assert code == 0, err
    cols = {c["name"]: c for c in json.loads(out)["columns"]}
    assert cols["id"]["min"] == 1
    assert cols["id"]["max"] == 5
    assert cols["score"]["null_count"] == 1


def test_stats_is_alias_for_describe(fixture_paths, capsys):
    code_a, out_a, _ = _run(capsys, "describe", "--json", fixture_paths["parquet"])
    code_b, out_b, _ = _run(capsys, "stats", "--json", fixture_paths["parquet"])
    assert code_a == code_b == 0
    assert out_a == out_b


def test_inspect_reports_row_groups(fixture_paths, capsys):
    code, out, err = _run(capsys, "inspect", "--json", fixture_paths["parquet"])
    assert code == 0, err
    report = json.loads(out)
    assert report["num_rows"] == 5
    assert report["num_row_groups"] >= 1


@pytest.mark.parametrize("verb", ["describe", "stats", "inspect"])
@pytest.mark.parametrize("fmt", ["csv", "jsonl"])
def test_footer_verbs_reject_non_parquet(fixture_paths, capsys, verb, fmt):
    code, out, err = _run(capsys, verb, fixture_paths[fmt])
    assert code == 1
    assert "not parquet" in err


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

def test_diff_identical(fixture_paths, capsys):
    code, out, err = _run(capsys, "diff", "--json", fixture_paths["parquet"], fixture_paths["parquet"])
    assert code == 0, err
    assert json.loads(out)["identical"] is True


def test_diff_detects_added_and_removed_columns(fixture_paths, capsys, tmp_path):
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel
    from rugo import parquet as rugo_parquet

    morsel = Morsel.from_vectors(
        ["id", "extra"],
        [Vector(dn.vector_from_sequence([1, 2])), Vector(dn.vector_from_string_sequence([b"x", b"y"]))],
    )
    other_path = str(tmp_path / "other.parquet")
    with open(other_path, "wb") as f:
        f.write(rugo_parquet.write_parquet(morsel))

    code, out, err = _run(capsys, "diff", "--json", fixture_paths["parquet"], other_path)
    assert code == 1
    report = json.loads(out)
    assert report["identical"] is False
    assert "extra" in report["columns_added"]
    assert "name" in report["columns_removed"]
    assert "score" in report["columns_removed"]


# ---------------------------------------------------------------------------
# convert
# ---------------------------------------------------------------------------

def test_convert_round_trip_preserves_row_count(fixture_paths, capsys, tmp_path):
    jsonl_out = str(tmp_path / "converted.jsonl")
    code, out, err = _run(capsys, "convert", fixture_paths["parquet"], jsonl_out)
    assert code == 0, err
    assert os.path.exists(jsonl_out)

    code, out, err = _run(capsys, "count", jsonl_out)
    assert code == 0, err
    assert out.strip() == "5"

    csv_out = str(tmp_path / "converted.csv")
    code, out, err = _run(capsys, "convert", jsonl_out, csv_out)
    assert code == 0, err
    code, out, err = _run(capsys, "count", csv_out)
    assert code == 0, err
    assert out.strip() == "5"


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------

def test_merge_concatenates_schema_identical_files(fixture_paths, capsys, tmp_path):
    merged = str(tmp_path / "merged.parquet")
    code, out, err = _run(capsys, "merge", fixture_paths["parquet"], fixture_paths["parquet"], merged)
    assert code == 0, err
    code, out, err = _run(capsys, "count", merged)
    assert code == 0, err
    assert out.strip() == "10"


def test_merge_rejects_schema_mismatch(fixture_paths, capsys, tmp_path):
    dest = str(tmp_path / "bad.parquet")
    code, out, err = _run(capsys, "merge", fixture_paths["parquet"], fixture_paths["csv"], dest)
    assert code == 1
    assert "schema mismatch" in err
    assert not os.path.exists(dest)


# ---------------------------------------------------------------------------
# split
# ---------------------------------------------------------------------------

def test_split_and_recombine_preserves_row_count(fixture_paths, capsys, tmp_path):
    code, out, err = _run(capsys, "split", "--rows", "2", "--json", fixture_paths["parquet"])
    assert code == 0, err
    outputs = json.loads(out)["outputs"]
    assert [o["num_rows"] for o in outputs] == [2, 2, 1]
    for o in outputs:
        assert os.path.exists(o["path"])

    merged = str(tmp_path / "recombined.parquet")
    code, out, err = _run(capsys, "merge", *[o["path"] for o in outputs], merged)
    assert code == 0, err
    code, out, err = _run(capsys, "count", merged)
    assert code == 0, err
    assert out.strip() == "5"


def test_split_rejects_non_positive_rows(fixture_paths, capsys):
    code, out, err = _run(capsys, "split", "--rows", "0", fixture_paths["parquet"])
    assert code == 1
    assert "--rows must be a positive integer" in err


# ---------------------------------------------------------------------------
# error handling — missing files must not crash the process
# ---------------------------------------------------------------------------

def test_missing_file_errors_cleanly(capsys, tmp_path):
    missing = str(tmp_path / "does_not_exist.parquet")
    code, out, err = _run(capsys, "info", missing)
    assert code == 1
    assert "no such file" in err


def test_merge_missing_file_errors_cleanly(capsys, tmp_path):
    missing_a = str(tmp_path / "missing_a.parquet")
    missing_b = str(tmp_path / "missing_b.parquet")
    dest = str(tmp_path / "out.parquet")
    code, out, err = _run(capsys, "merge", missing_a, missing_b, dest)
    assert code == 1
    assert "no such file" in err


def test_unrecognized_extension_errors_cleanly(capsys, tmp_path):
    bogus = tmp_path / "data.xyz"
    bogus.write_text("not a real file")
    code, out, err = _run(capsys, "info", str(bogus))
    assert code == 1
    assert "cannot infer format" in err
