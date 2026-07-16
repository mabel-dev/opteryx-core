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

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import draken.draken_native as dn

LONG = "this is a long string well over twelve bytes"  # forces the arena (>12)


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


# The Q40-style `test_iif_mixes_dense_and_dict_varchar` that lived here is gone: it
# called the nanobind `vector_iif`, which was deleted when IIF became a C-ABI kernel
# (draken/ops/kernels/function_null_conditional.cpp). A C-ABI kernel has no
# Python-callable entry point, so the mixed dense/dict branches cannot be built by
# hand here any more; IIF is now covered end-to-end through the engine
# (tests/unit/functions/test_null_aware.py and the SQL battery). The property this
# asserted still holds by construction: that kernel reads every branch through the
# uniform data[selection[i]] path and never discriminates on encoding shape (§11).
# The concat half of the question — the part this file is named for — is unchanged
# and still covered above.


if __name__ == "__main__":
    test_concat_dense_then_dict_varchar()
    test_concat_dict_then_dense_varchar()
    print("✅ dense + dict VARCHAR are concat interchangeable")
