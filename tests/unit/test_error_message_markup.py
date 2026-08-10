"""The error-message markup contract.

Error messages ARE markdown - `str(exception)` emits it, and a separate surface
renders it. The two sides never inspect each other, so this file is the contract:
it pins what this side emits against the marks that side renders.

    **text**    bold          SQL syntax
    *text*      italic        column and variable names
    __text__    underline     the underlying error
    `text`      code chip     table names, suggestions, literals

Two rules of that renderer drive most of what is asserted here:

  - `_text_` is NOT italic. SQL is full of snake_case, and a single underscore
    pair would otherwise italicise the middle of a column name. So a lone
    underscore must reach the renderer unescaped.
  - `__text__` is ignored when the marks sit inside a word, but NOT when they sit
    at the edge of another span - which is exactly where a column named
    `__dunder__` puts them.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import compose
from opteryx.exceptions import did_you_mean
from opteryx.exceptions import md_cause
from opteryx.exceptions import md_code
from opteryx.exceptions import md_column
from opteryx.exceptions import md_syntax
from opteryx.exceptions import md_table


def test_each_mark_is_the_one_the_renderer_draws():
    assert md_column("name") == "*name*"
    assert md_syntax("select") == "**SELECT**"
    assert md_code("planets") == "`planets`"
    assert md_table("planets") == "`planets`"
    assert md_cause("boom") == "__boom__"


def test_a_lone_underscore_is_never_escaped():
    """snake_case is the common case; a backslash in front of every `user_id`
    would be the defect, not the protection."""
    assert md_column("user_id") == "*user_id*"
    assert md_syntax("cidr_agg") == "**CIDR_AGG**"
    assert md_column("_leading") == "*_leading*"
    assert md_column("trailing_") == "*trailing_*"


def test_an_underscore_run_is_escaped_so_it_cannot_open_an_underline():
    """`*__dunder__*` sits the run against a `*`, where it is not 'inside a word'
    and the renderer would draw an underline nobody asked for."""
    assert md_column("__dunder__") == "*\\_\\_dunder\\_\\_*"
    assert "__" not in md_column("a__b").strip("*")


def test_an_underlying_error_cannot_smuggle_an_underline_mark():
    """md_cause wraps arbitrary text in `__...__`; a cause mentioning `my__col`
    would otherwise pair its own run with the closing mark."""
    rendered = md_cause("no such column: my__col")
    assert rendered.startswith("__") and rendered.endswith("__")
    assert "__" not in rendered[2:-2]


def test_a_span_cannot_be_ended_early_by_its_own_content():
    assert md_column("a*b") == "*a\\*b*"
    assert md_syntax("a`b") == "**A\\`B**"


def test_backslashes_survive_unescaped():
    """The renderer consumes a backslash only in front of one of its own marks and
    leaves every other one alone. Doubling them here would turn `C:\\temp` into
    `C:\\\\temp` and every regex in an error message into nonsense."""
    assert md_column(r"C:\temp\file.csv") == r"*C:\temp\file.csv*"
    assert md_cause(r"pattern '\d+' did not match \w") == r"__pattern '\d+' did not match \w__"


def test_a_bracket_is_left_alone():
    """No evidence `[` is a mark on that surface, and a backslash in front of a
    non-mark stays put - so escaping it would only show a stray backslash."""
    assert md_column("missions[0]") == "*missions[0]*"


def test_a_code_span_grows_its_fence_past_the_content():
    """Backslashes do not escape inside a code span - the fence has to be longer
    than the longest backtick run, and a value touching a backtick needs padding."""
    assert md_code("a`b") == "``a`b``"
    assert md_code("a``b") == "```a``b```"
    assert md_code("`x`") == "`` `x` ``"


@pytest.mark.parametrize(
    "parts, expected",
    [
        (("One", "Two"), "One. Two."),
        (("One.", "Two."), "One. Two."),
        (("Already?", "Yes"), "Already? Yes."),
        (("Kept", None, "", "Last"), "Kept. Last."),
        (("wrapped\n   across   lines",), "wrapped across lines."),
    ],
)
def test_compose_makes_whole_sentences(parts, expected):
    """The ` .`, `?.`, doubled-space and trailing-comma defects were all in the
    joins, so the joins are done in one place."""
    assert compose(*parts) == expected


def test_a_suggestion_is_a_code_chip_and_its_own_sentence():
    assert did_you_mean("name") == "Did you mean `name`?"
    assert did_you_mean(None) == ""

    message = compose("Column *nam* cannot be found", did_you_mean("name"))
    assert message == "Column *nam* cannot be found. Did you mean `name`?"
    # never appended after a comma, hyphen or colon
    for lead_in in (", did you mean", "- did you mean", ": did you mean"):
        assert lead_in not in message.lower()


def test_the_not_found_message_carries_its_parts_as_fields():
    """The renderer works from the text, but the position and the names stay on
    the exception so a caller can highlight them without re-parsing prose."""
    error = ColumnNotFoundError(column="nam", dataset="a.b.c", suggestion="name")

    assert error.column == "nam"
    assert error.dataset == "a.b.c"
    assert error.suggestion == "name"

    message = str(error)
    assert "*nam*" in message  # column, italic
    assert "`a.b.c`" in message  # table, code chip
    assert "Did you mean `name`?" in message  # suggestion, code chip, own sentence
    assert "**SHOW COLUMNS FROM**" in message  # SQL syntax, bold uppercase
    assert "does not exist" not in message  # things cannot be found


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
