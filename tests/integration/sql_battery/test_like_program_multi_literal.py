"""
LIKE op-program: multi-literal patterns (`%a%b%c%`, with optional anchored
prefix/suffix) across subject lengths that straddle the 16-byte NEON block
boundary.

The SEARCH op scans with a two-byte (first+last) anchored 16-wide compare on
NEON builds (search_neon in draken/ops/kernels/like_program.h), with a scalar
tail and an overlapped final block, and reads at up to `limit + len - 2`. The
risks are an off-by-one at a block boundary, a literal that straddles the end
of the search window (tail_reserve), and a second literal that must NOT reuse
bytes of the first. Oracle: the pattern translated to a Python regex
(`%` -> `.*`, DOTALL), evaluated per subject.
"""

import os
import random
import re
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

# Literals of 2+ bytes take the two-byte anchor; the 1-byte ones keep memchr.
PATTERNS = [
    "%unusual%accounts%",
    "%ab%ba%",
    "%ab%ab%",
    "%aa%aa%aa%",
    "%abc%cba%",
    "%xy%",
    "%ab%cd",
    "ab%cd%ef",
    "ab%cd%ef%",
    "%a%bc%d%",
    "%aXb%",
    "%abab%baba%",
    "%unusual%",
]


def _oracle(pattern, value):
    regex = ".*".join(re.escape(part) for part in pattern.split("%"))
    return re.fullmatch(regex, value, re.DOTALL) is not None


def _subjects():
    rng = random.Random(20260928)
    fixed = [
        "", "a", "ab", "ba", "aba", "abba", "abab", "ababab", "ab" * 9,
        "unusual accounts", "unusual", "accounts", "accounts unusual",
        "xunusualxaccountsx", "unusualaccounts", "unusual" * 3 + "accounts",
        "the unusual pending accounts sleep", "abcdef", "abcxcdxef", "abXcdXef",
    ]
    subjects = list(fixed)
    # Every length across the 16-byte block boundaries, with a literal planted
    # at every offset in the tail region and at the very end.
    for length in range(0, 70):
        for literal in ("ab", "ba", "unusual", "accounts", "cd", "xy"):
            if length >= len(literal):
                for offset in {0, 1, 15, 16, 17, length - len(literal), max(0, length - len(literal) - 1)}:
                    if 0 <= offset <= length - len(literal):
                        filler = "".join(rng.choice("abcdxy ") for _ in range(length))
                        subjects.append(filler[:offset] + literal + filler[offset + len(literal):])
    for _ in range(400):
        length = rng.randint(0, 90)
        subjects.append("".join(rng.choice("abcd") for _ in range(length)))
    return sorted(set(subjects))


SUBJECTS = _subjects()


def _run(pattern, negate=False):
    values = ", ".join("('%s')" % s.replace("'", "''") for s in SUBJECTS)
    operator = "NOT LIKE" if negate else "LIKE"
    sql = "SELECT s FROM (VALUES %s) AS t(s) WHERE s %s '%s'" % (values, operator, pattern)
    got = set()
    for morsel in opteryx.session().execute_to_morsels(sql):
        for row in morsel:
            got.add(list(row)[0])
    return got


@pytest.mark.parametrize("pattern", PATTERNS)
def test_like_matches_oracle(pattern):
    expected = {s for s in SUBJECTS if _oracle(pattern, s)}
    assert _run(pattern) == expected


@pytest.mark.parametrize("pattern", PATTERNS)
def test_not_like_matches_oracle(pattern):
    expected = {s for s in SUBJECTS if not _oracle(pattern, s)}
    assert _run(pattern, negate=True) == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
