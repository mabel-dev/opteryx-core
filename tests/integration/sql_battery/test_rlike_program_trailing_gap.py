"""
Regression tests: RLIKE SIMD op-program (blob v2) support for a trailing
`.*$` gap — i.e. a pattern that ends in a non-dotall wildcard with nothing
between the last gap and `$` (e.g. `^http://.*catalog.*$`).

Before this work compile_rlike_program refused any pattern ending in a
trailing gap (`expect_literal` still true after the last `.*`/`.+`) and fell
back to the correct-but-slower transition-table DFA. It now compiles these
via a new terminal op, LMOP_TAIL_NO_NEWLINE (draken/ops/kernels/like_program.h):
accept iff there is no '\\n' between the cursor and the (already
newline-bounded) window end.

Oracle: NOT Python `re` — RE2's `$` is strict end-of-text, and does not share
Python re's "before a trailing newline" special case (see the module comment
in like_program.h). The oracle here is the existing transition-table DFA
(compile_rlike_dfa / draken_rlike), forced by monkeypatching
compile_rlike_program to return None so the query falls through to the DFA
kernel — the same execution path, just the previously-correct one.
"""

import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
import opteryx.compiled.vector_ops as vo


TRAILING_GAP_PATTERNS = [
    "^foo.*$",
    "^http://.*catalog.*$",
    "^http.*catalog.*$",
    "^a.*b.*$",
    "catalog.*$",
    "http.*catalog.*$",
]

EDGE_VALUES = [
    "foo", "foobar", "foo\n", "foo\nbar", "", "\n", "\n\n",
    "http://x.com/catalog", "catalogfoo\ncatalogbar",
    "acatalogb\ncatalog", "xcatalogx", "a\nb", "ab", "aXXXb",
    "http://catalog", "xhttp://catalogx", "http://x\ncatalog",
    "catalog", "catalog\n", "\ncatalog", "acatalog\nbcatalog",
]


def _match(pattern, value):
    """subject RLIKE pattern via a VALUES table + bind param (avoids SQL
    literal newline round-trip mangling — see test_rlike_dfa_regression.py's
    module comment on SUBJECTS)."""
    sql = "SELECT s RLIKE '%s' AS m FROM (VALUES (?)) AS t(s)" % pattern.replace("'", "''")
    for morsel in opteryx.session().execute_to_morsels(sql, params=[value]):
        for row in morsel:
            return list(row)[0]
    raise AssertionError("no row returned")


@pytest.mark.parametrize("pattern", TRAILING_GAP_PATTERNS)
def test_trailing_gap_now_compiles_to_program(pattern):
    """Previously these all returned None (v1 refused); now they route to the
    SIMD op-program (blob version 2)."""
    blob = vo.compile_rlike_program(pattern.encode())
    assert blob is not None, pattern
    assert blob[0] == 2, "expected op-program blob version 2"


@pytest.mark.parametrize("pattern", TRAILING_GAP_PATTERNS)
def test_trailing_gap_matches_dfa_oracle(pattern):
    """The op-program result must equal the transition-table DFA result
    (the established-correct oracle) across newline-heavy edge cases."""
    random.seed(hash(pattern) & 0xFFFF)
    alphabet = "abcxyz\n"
    values = list(EDGE_VALUES)
    for _ in range(100):
        n = random.randint(0, 14)
        values.append("".join(random.choice(alphabet) for _ in range(n)))

    orig = vo.compile_rlike_program
    try:
        for value in values:
            vo.compile_rlike_program = orig
            program_answer = _match(pattern, value)

            vo.compile_rlike_program = lambda p: None
            dfa_answer = _match(pattern, value)

            assert program_answer == dfa_answer, (pattern, value)
    finally:
        vo.compile_rlike_program = orig


def test_unanchored_trailing_gap_still_refused():
    """A trailing gap with no `$` anchor stays out of scope (LMOP_TAIL_NO_NEWLINE
    relies on the line-driver's newline-bounded window, which requires
    LFLAG_ANCHOR_END) — the compiler must keep refusing it, not guess."""
    assert vo.compile_rlike_program(b"^foo.*") is None
    assert vo.compile_rlike_program(b"foo.*") is None


def test_like_trailing_percent_unaffected():
    """SQL LIKE's trailing `%` is dotall — 'accept regardless' was already
    correct and is untouched by this change (still LMOP_ACCEPT, not the new
    RLIKE-only tail op)."""
    blob = vo.compile_like_program(b"foo%")
    assert blob is not None
    assert blob[-1] == 7  # LMOP_ACCEPT, unchanged


if __name__ == "__main__":  # pragma: no cover
    import traceback

    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        params = None
        marks = getattr(t, "pytestmark", [])
        for m in marks:
            if m.name == "parametrize":
                params = m.args[1]
        cases = params if params is not None else [None]
        for case in cases:
            try:
                t(case) if case is not None else t()
                passed += 1
            except Exception:  # noqa: BLE001
                failed += 1
                print(f"FAILED {t.__name__}({case!r})")
                traceback.print_exc()
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
