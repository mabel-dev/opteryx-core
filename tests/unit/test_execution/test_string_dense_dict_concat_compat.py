"""Stage 4b foundation: dense and dict VARCHAR are concat/CASE interchangeable.

The string-direct scan path (Stage 4b) was once attempted and reverted with the
conclusion that a direct-built *dense* VARCHAR could not concat-compose with a
pool-built *dict* VARCHAR (a string column is plain-encoded in some parquet row
groups and dict-encoded in others). This test settles that question at the draken
level: per the §11 unified vector format, every kernel accesses values through the
uniform `data[selection[i]]` path, so dense and dict shapes of the same DrakenType
are fully interchangeable.

If this passes, the prior failure (clickbench Q24 `SELECT *` → "concat: all inputs
must share one type"; Q40 `CASE..THEN Referer` → SIGSEGV) was NOT a concat
incompatibility but a memory-ownership bug — the direct path's arena/codes buffers
being double-freed (`draken_vector_own_string` copies + frees its inputs, AND the
MorselRef destructor frees them again unless they are nulled on transfer). The
re-implementation gate is therefore correct buffer ownership, not a draken change.
"""

import glob
import importlib.util
import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import draken.draken_native as dn

LONG = "this is a long string well over twelve bytes"  # forces the arena (>12)


def _load_concat_ext():
    pattern = os.path.join(
        os.path.dirname(__file__), "../../..",
        "opteryx", "compiled", "nanobind", "vectors*.so",
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip("vector_selection_concat extension not built")
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vectors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _dense(values):
    """Dense VARCHAR (the plain-row-group / direct-path shape)."""
    return dn.vector_from_string_sequence(
        [v.encode() if isinstance(v, str) else v for v in values]
    )


def _dict(values):
    """Dict VARCHAR (the dict-encoded-row-group / pool shape)."""
    return dn.vector_from_string_dict_sequence(
        [v.encode() if isinstance(v, str) else v for v in values]
    )


def test_concat_dense_then_dict_varchar():
    # Q24-style: the same column concatenated across a plain RG and a dict RG.
    dense = _dense(["short", LONG, "another"])
    dictv = _dict(["repeat", "repeat", LONG, "repeat"])
    out = dn.vector_concat([dense, dictv])
    assert out.to_pylist() == ["short", LONG, "another", "repeat", "repeat", LONG, "repeat"]


def test_concat_dict_then_dense_varchar():
    out = dn.vector_concat([_dict([LONG, "a", LONG]), _dense(["b", "c"])])
    assert out.to_pylist() == [LONG, "a", LONG, "b", "c"]


def test_iif_mixes_dense_and_dict_varchar():
    # Q40-style: a CASE/iif whose branches are a dense and a dict VARCHAR.
    vector_iif = _load_concat_ext().vector_iif
    cond = dn.vector_from_bool_sequence([True, False, True])
    out = vector_iif(cond, _dense(["aaa", LONG, "ccc"]), _dict(["xxx", "xxx", LONG]))
    assert out.to_pylist() == ["aaa", "xxx", "ccc"]
    out2 = vector_iif(cond, _dict([LONG, "p", LONG]), _dense(["q", "q", "q"]))
    assert out2.to_pylist() == [LONG, "q", LONG]


if __name__ == "__main__":
    test_concat_dense_then_dict_varchar()
    test_concat_dict_then_dense_varchar()
    test_iif_mixes_dense_and_dict_varchar()
    print("✅ dense + dict VARCHAR are concat/CASE interchangeable")
