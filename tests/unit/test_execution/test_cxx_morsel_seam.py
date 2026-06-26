"""
C++-first morsel substrate (CxxMorsel) — tests on the REAL path only.

No test-only seam hooks: every assertion goes through the wired path — the cdef
Morsel's dual representation (`from_cxx` / lazy materialization / Cxx-native
select·take·slice) and the parquet scan that emits Cxx-backed morsels. Data
correctness at scale is the value-checked make q / tpch / clickbench suites.

See docs/M4_CPP_MORSEL_DESIGN.md, docs/M4_S1_DUAL_MORSEL_DESIGN.md.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

DT = dn.DrakenType


def _cols(m, ncols):
    # COLUMN access (m[i] is ROW access per the Morsel contract). `_cxx_column`
    # reads the i-th column from whichever representation backs the morsel — the
    # Cxx substrate when Cxx-backed (no whole-morsel materialization), the
    # PyObject store otherwise — so the comparison is column-data byte-identical
    # without collapsing a Cxx-backed morsel.
    names = m.column_names
    return [m._cxx_column(names[i]).to_pylist() for i in range(ncols)]


def test_dual_morsel_from_cxx_and_cxx_native_ops():
    """from_cxx carrier: cheap accessors don't materialize; _get_cxx is zero-work;
    select/take/slice stay Cxx-backed (no materialization) and are byte-identical to
    the PyObject path; first real column access materializes byte-identically."""
    vs = [
        vector_from_sequence([10, 20, 30, None], dtype=DT.INT64),
        vector_from_sequence([b"a", b"bb", b"long arena string", b"d"], dtype=DT.VARCHAR),
        vector_from_sequence([True, False, None, True], dtype=DT.BOOL),
    ]
    names = [b"id", b"name", b"flag"]
    ref = Morsel.from_vectors(names, vs)

    def fresh():
        return Morsel.from_cxx(dn.cxx_morsel_from_vectors(vs, names))

    # Cheap accessors answer from _cxx WITHOUT materializing.
    m = fresh()
    assert m._cxx is not None
    assert m.num_rows == ref.num_rows
    assert m.num_columns == ref.num_columns
    assert m._cxx is not None, "num_* must not materialize"
    assert m._get_cxx() is m._cxx, "_get_cxx returns the handle, zero work"
    assert m._cxx is not None, "_get_cxx must not materialize"

    # select / take / slice stay Cxx-backed (substrate survives the operator).
    sel = fresh().select([b"flag", b"id"])
    assert sel._cxx is not None
    assert _cols(sel, 2) == _cols(ref.select([b"flag", b"id"]), 2)

    tk = fresh().take([3, 0, 2, 2])
    assert tk._cxx is not None
    assert _cols(tk, 3) == _cols(ref.take([3, 0, 2, 2]), 3)

    sl = fresh().slice(1, 2)
    assert sl._cxx is not None
    assert _cols(sl, 3) == _cols(ref.slice(1, 2), 3)

    # Explicit materialization (the sole sanctioned PyObject collapse) is
    # byte-identical and clears the Cxx carrier. Metadata (column_names) is cheap
    # and does NOT materialize on its own.
    mat = fresh()
    assert mat.column_names == ref.column_names
    assert mat._cxx is not None, "metadata access must not materialize"
    mat.materialize()
    assert mat._cxx is None
    for name in mat.column_names:
        assert mat.column(name).to_pylist() == ref.column(name).to_pylist()
        assert mat.column(name).type == ref.column(name).type

    # Substrate column read (_cxx_column — the converted-operator accessor that
    # reads through a Cxx-backed morsel without materializing; `column` fails loud
    # there), and the keying hash, match the PyObject path.
    assert fresh()._cxx_column(b"name").to_pylist() == ref.column(b"name").to_pylist()
    assert list(fresh().hash()) == list(ref.hash())


def test_scan_emits_cxx_morsel():
    """The single-pass parquet scan emits Cxx-backed morsels unconditionally; they
    materialize lazily downstream. Smoke the path on a representative query."""
    import opteryx

    s = opteryx.session()
    ids = []
    for m in s.execute_to_morsels(
        "SELECT id, name FROM testdata.planets WHERE id > 3 ORDER BY id"
    ):
        ids.extend(m.column(b"id").to_pylist())
    assert ids == sorted(ids)
    assert all(i > 3 for i in ids)
    assert ids == [i for i in range(1, 10) if i > 3]  # planet ids 1..9


if __name__ == "__main__":
    test_dual_morsel_from_cxx_and_cxx_native_ops()
    test_scan_emits_cxx_morsel()
    print("✅ CxxMorsel substrate — dual Morsel + Cxx-native ops + scan, real path only")
