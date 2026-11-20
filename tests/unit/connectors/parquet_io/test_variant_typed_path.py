"""CAST(<path> ->> AS INT64) read straight from a shredded integer leaf (Part C of
docs/parquet_variant_path_read_design.md): the optimizer records the CAST as a candidate, and
the compiler reads it typed only where every file's footer proves the field's `value` empty in
every row group. Anywhere unproven the CAST runs over the text exactly as before, so answers AND
errors are those of the unfused query (FEATURE_DISABLE_VARIANT_TYPED_PATH)."""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

import opteryx
import opteryx.connectors.parquet_io.pool_reader as pool_reader
from opteryx import config
from rugo import parquet as rp


def _write(tmp_path, docs, name, **options):
    src = tmp_path / (name + ".jsonl")
    src.write_text("\n".join(json.dumps(d, separators=(",", ":")) for d in docs) + "\n")
    morsel = next(iter(opteryx.session().execute_to_morsels(
        f"SELECT data FROM READ_JSONL('{src}', as_variant => 'data')"
    )))
    path = str(tmp_path / (name + ".parquet"))
    with open(path, "wb") as f:
        f.write(rp.write_parquet(morsel, **options))
    return path


def _run(sql, typed):
    config.features.disable_variant_typed_path = not typed
    try:
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql):
            rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
        return sorted(rows, key=repr)
    finally:
        config.features.disable_variant_typed_path = False


@pytest.fixture
def proofs(monkeypatch):
    """Every name the compiler asked to have proven, and what came back."""
    seen = []
    real = pool_reader.typed_paths_proven

    def spy(filesystem, paths, names, file_sizes, cache):
        out = real(filesystem, paths, names, file_sizes, cache)
        seen.append((list(names), set(out)))
        return out

    monkeypatch.setattr(pool_reader, "typed_paths_proven", spy)
    return seen


INTS = [{"t": 1000 + i, "k": f"k{i % 3}"} for i in range(40)]
SQL_MINMAX = "SELECT MIN(CAST(data->>'t' AS INT64)), MAX(CAST(data->>'t' AS INT64)) FROM READ_PARQUET('{p}')"


def test_proven_reads_the_leaf(tmp_path, proofs):
    path = _write(tmp_path, INTS, "ints")
    sql = SQL_MINMAX.format(p=path)
    assert _run(sql, True) == _run(sql, False) == [(1000, 1039)]
    assert proofs == [(["data->>'t'::INT64"], {"data->>'t'::INT64"})]


def test_grouped_and_filtered(tmp_path):
    path = _write(tmp_path, INTS, "ints")
    sql = (f"SELECT data->>'k' AS k, MIN(CAST(data->>'t' AS INT64)) FROM READ_PARQUET('{path}') "
           f"WHERE data->>'k' <> 'k1' GROUP BY k")
    assert _run(sql, True) == _run(sql, False) == [("k0", 1000), ("k2", 1002)]


def test_a_row_group_holding_a_string_is_not_proven(tmp_path, proofs):
    # The second row group holds `t` as a string, so its `value` is not empty there: unproven,
    # and the CAST runs over the text - "77" still casts.
    docs = [{"t": 1000 + i} for i in range(8)]
    docs[6]["t"] = "77"
    path = _write(tmp_path, docs, "mixed", max_rows_per_row_group=4)
    sql = SQL_MINMAX.format(p=path)
    assert _run(sql, True) == _run(sql, False) == [(77, 1007)]
    assert proofs == [(["data->>'t'::INT64"], set())]


def test_a_bad_value_fails_the_same_way(tmp_path):
    docs = [{"t": 1000 + i} for i in range(8)]
    docs[6]["t"] = "not a number"
    path = _write(tmp_path, docs, "bad", max_rows_per_row_group=4)
    sql = SQL_MINMAX.format(p=path)
    errors = []
    for typed in (True, False):
        with pytest.raises(Exception) as caught:
            _run(sql, typed)
        errors.append((type(caught.value), str(caught.value)))
    assert errors[0] == errors[1]


def test_every_file_must_prove_it(tmp_path, proofs):
    good = _write(tmp_path, INTS, "a_good")
    docs = [{"t": 2000 + i} for i in range(8)]
    docs[2]["t"] = "5"
    _write(tmp_path, docs, "b_mixed", max_rows_per_row_group=4)
    sql = SQL_MINMAX.format(p=str(tmp_path / "*.parquet"))
    assert _run(sql, True) == _run(sql, False) == [(5, 2007)]
    assert proofs == [(["data->>'t'::INT64"], set())]
    assert good


def test_a_path_also_read_as_text_is_not_fused(tmp_path, proofs):
    path = _write(tmp_path, INTS[:3], "ints")
    sql = f"SELECT data->>'t', CAST(data->>'t' AS INT64) FROM READ_PARQUET('{path}')"
    assert _run(sql, True) == _run(sql, False) == [("1000", 1000), ("1001", 1001), ("1002", 1002)]
    assert proofs == []


def test_try_cast_is_not_fused(tmp_path, proofs):
    path = _write(tmp_path, INTS[:3], "ints")
    sql = f"SELECT TRY_CAST(data->>'t' AS INT64) FROM READ_PARQUET('{path}')"
    assert _run(sql, True) == _run(sql, False) == [(1000,), (1001,), (1002,)]
    assert proofs == []


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
