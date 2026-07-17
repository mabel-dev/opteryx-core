"""
Regression tests: RLIKE / NOT RLIKE / `~` backed by the native byte-DFA engine.

Before this work RLIKE had no native dispatch path at all — every RLIKE query
failed at plan time with NotSupportedError (the generic BC_COMPARE gate only
admits op codes 1-6, and there was no draken_rlike kernel or bind-time
lowering arm). Now:

  - predicate_rewriter._rewrite_rlike_to_dfa compiles a *literal* pattern into
    a byte-DFA blob at plan time (RE2 parser only — vector_dfa_compile.pyx),
  - compiled_expression.pyx lowers RLike/NotRLike to the draken_rlike C-ABI
    kernel (BC_FUNCTION | BC_INSTR_C_NATIVE | BC_RESULT_WRAP_AS_BOOL),
  - draken_rlike (function_rlike.cpp) walks the blob per row — no RE2, no
    backtracking, works in both wheels.

Independent oracle: Python `re`. The DFA is byte-oriented and matches
partial (search, not fullmatch) semantics, so we compare against
`re.compile(pattern).search(subject)`.

Deliberate scope limits (assert they refuse cleanly, never crash / never wrong):
  - non-literal pattern (`col RLIKE col`)          -> NotSupportedError
  - lookaround / backreferences / case-fold `(?i)` -> NotSupportedError
"""

import os
import re
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import NotSupportedError


# Subjects deliberately span: empty, ASCII words, digits, punctuation,
# whitespace, a long run, and multi-byte UTF-8 (é, 😀) to prove byte-level
# matching of an ASCII pattern doesn't corrupt or false-match multi-byte
# sequences it isn't matching against.
SUBJECTS = [
    "abc", "xabc", "abcx", "xabcx", "ab", "ABC", "", "aabc",
    "Mercury", "Mars", "Jupiter", "Saturn",
    "colour", "color", "colouur", "colr",
    "a1b2c3", "   ", "x" * 40,
    "has-a-dash", "multi word phrase",
    "café", "naïve", "😀smile", "emoji😀here",
]

# Every pattern here is within the DFA compiler's supported dialect.
SUPPORTED_PATTERNS = [
    "abc", "^abc", "abc$", "^abc$",
    "a+", "a|b", "[a-z]+", "a{2,4}", "colou?r",
    r"\d+", ".", "a.c", "[^x]", "^$", "a*",
    "^M", "[JS]", "wor.", "^h", "sh$",
    "(a|b)c", "^(Mer|Sat)", "[0-9]{2,}", "a.*c",
]


def _rlike(subjects, pattern, negate=False):
    """Run `subject RLIKE pattern` (or NOT RLIKE) over a VALUES table and
    return the set of matching subjects."""
    op = "NOT RLIKE" if negate else "RLIKE"
    values = ", ".join("('" + s.replace("'", "''") + "')" for s in subjects)
    pat = pattern.replace("'", "''")
    sql = f"SELECT s FROM (VALUES {values}) AS t(s) WHERE s {op} '{pat}'"
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.column(b"s").to_pylist())
    return set(out)


@pytest.mark.parametrize("pattern", SUPPORTED_PATTERNS)
def test_rlike_matches_python_re(pattern):
    """DFA RLIKE result must equal Python re.search over the same inputs.

    A literal '\\n'-bearing subject is excluded from SUBJECTS because the SQL
    string literal round-trips newlines differently — an unrelated concern to
    regex matching."""
    rx = re.compile(pattern)
    expected = {s for s in SUBJECTS if rx.search(s)}
    assert _rlike(SUBJECTS, pattern) == expected, pattern


@pytest.mark.parametrize("pattern", SUPPORTED_PATTERNS)
def test_not_rlike_is_complement(pattern):
    """NOT RLIKE must be the exact complement of RLIKE over non-null rows."""
    matched = _rlike(SUBJECTS, pattern, negate=False)
    not_matched = _rlike(SUBJECTS, pattern, negate=True)
    # Every subject is non-null here, so the two sets partition SUBJECTS.
    assert matched.isdisjoint(not_matched), pattern
    assert matched | not_matched == set(SUBJECTS), pattern


def test_rlike_tilde_operator_is_rlike():
    """`~` is a synonym for RLIKE and must take the same DFA path."""
    values = ", ".join("('" + s + "')" for s in ["Mercury", "Mars", "Venus"])
    sql = f"SELECT s FROM (VALUES {values}) AS t(s) WHERE s ~ '^M'"
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.column(b"s").to_pylist())
    assert set(out) == {"Mercury", "Mars"}


def test_rlike_over_real_column():
    """Not just VALUES — a real dataset column operand."""
    sql = "SELECT name FROM $planets WHERE name RLIKE '^[JS]'"
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.column(b"name").to_pylist())
    assert set(out) == {"Jupiter", "Saturn"}


def test_rlike_null_row_propagates():
    """A NULL subject row is neither matched nor anti-matched (NULL, dropped
    from a WHERE filter) — mirrors SQL comparison-with-NULL semantics."""
    sql = (
        "SELECT s FROM (VALUES ('abc'), (NULL), ('xyz')) AS t(s) "
        "WHERE s RLIKE 'b'"
    )
    out = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        out.extend(morsel.column(b"s").to_pylist())
    assert out == ["abc"]


def test_rlike_multibyte_not_falsematched():
    """An ASCII pattern must not spuriously match inside a multi-byte UTF-8
    sequence. 'é' is U+00E9 -> bytes C3 A9; pattern '[a-z]' (0x61-0x7A) must
    not match either byte."""
    assert _rlike(["é", "abc", " é"], "^[a-z]+$") == {"abc"}


def test_rlike_empty_pattern_matches_all():
    """An empty pattern matches every (non-null) subject, same as re.search('', s)."""
    subjects = ["", "a", "abc", "😀"]
    assert _rlike(subjects, "") == set(subjects)


# --- deliberate scope-limit refusals: clean plan-time error, never a crash ---


def test_rlike_non_literal_pattern_refused():
    with pytest.raises(NotSupportedError):
        for _ in opteryx.session().execute_to_morsels(
            "SELECT name FROM $planets WHERE name RLIKE name"
        ):
            pass


@pytest.mark.parametrize(
    "pattern",
    [
        "(?!x)abc",       # negative lookahead
        "(?=abc)",        # positive lookahead
        r"(a)\1",         # backreference
        "(?i)abc",        # inline case-fold
    ],
)
def test_rlike_unsupported_syntax_refused(pattern):
    pat = pattern.replace("'", "''")
    with pytest.raises(NotSupportedError):
        for _ in opteryx.session().execute_to_morsels(
            f"SELECT s FROM (VALUES ('abc')) AS t(s) WHERE s RLIKE '{pat}'"
        ):
            pass


def test_rlike_state_cap_overflow_refused():
    """A pattern whose minimal DFA exceeds the state-count cap (the classic
    'a exactly N chars from the end' exponential blowup) must be refused
    cleanly at plan time — never a hang or an unbounded-memory compile."""
    # a.{9}$ needs the DFA to remember the last 9 bytes -> 2^9 states > cap.
    with pytest.raises(NotSupportedError):
        for _ in opteryx.session().execute_to_morsels(
            "SELECT s FROM (VALUES ('abcdefghij')) AS t(s) WHERE s RLIKE 'a.{9}$'"
        ):
            pass


def test_rlike_state_cap_boundary_compiles():
    """The compiler itself: a small bounded window compiles, a large one is
    refused (returns None) — the boundary is a real, deterministic cap, not
    luck. Direct compiler check so the assertion doesn't depend on dataset."""
    from opteryx.compiled.vector_ops import compile_rlike_dfa

    assert compile_rlike_dfa(b"a.{3}$") is not None
    assert compile_rlike_dfa(b"a.{12}$") is None


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
