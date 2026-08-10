import re
from functools import lru_cache
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

ESCAPE_SPECIAL_CHARS = re.compile(r"([.^$*+?{}[\]|()\\])")


@lru_cache(maxsize=512)
def sql_like_to_regex(pattern: str, full_match: bool = True, case_sensitive: bool = True) -> str:
    """
    Converts an SQL `LIKE` pattern into a regular expression.

    SQL `LIKE` syntax:
    - `%` matches zero or more characters (similar to `.*` in regex).
    - `_` matches exactly one character (similar to `.` in regex).
    - Special regex characters are escaped to ensure literal matching.

    Args:
        pattern (str): The SQL LIKE pattern.

    Returns:
        str: The equivalent regex pattern.

    Examples:
        sql_like_to_regex("a%")       -> "^a.*?$"
        sql_like_to_regex("_b")       -> "^.b$"
    """
    if pattern is None:
        raise ValueError("Pattern cannot be None")

    if isinstance(pattern, bytes):
        pattern = pattern.decode("utf-8")

    # Escape special regex characters in the pattern
    escaped_pattern = ESCAPE_SPECIAL_CHARS.sub(r"\\\1", pattern)

    # Replace SQL wildcards with regex equivalents
    regex_pattern = escaped_pattern.replace("%", ".*?").replace("_", ".")

    if full_match:
        regex_pattern = f"^{regex_pattern}$"
    else:
        # For partial matches, trim leading/trailing wildcards
        if regex_pattern.startswith(".*?"):
            regex_pattern = regex_pattern[3:]
        if regex_pattern.endswith(".*?"):
            regex_pattern = regex_pattern[:-3]

    if not case_sensitive:
        regex_pattern = f"(?i:{regex_pattern})"
    return regex_pattern


def offset_of(text: str, line: int, column: int) -> Optional[int]:
    """The 0-based character offset of a 1-based (line, column) in `text`."""
    if line is None or column is None or line < 1 or column < 1:
        return None
    offset = 0
    for _ in range(line - 1):
        found = text.find("\n", offset)
        if found < 0:
            return None
        offset = found + 1
    return min(offset + column - 1, len(text))


def position_of(text: str, offset: int) -> Tuple[int, int]:
    """The 1-based (line, column) of a 0-based character offset in `text`."""
    offset = max(0, min(offset, len(text)))
    line = text.count("\n", 0, offset) + 1
    line_start = text.rfind("\n", 0, offset) + 1
    return (line, offset - line_start + 1)


def underline(sql: str, position) -> Optional[str]:
    """Render `position` as an underlined line of `sql`, for a TERMINAL.

    This is the fallback for surfaces with no editor to mark up - a REPL, a log, a
    traceback. It is NOT what an error message contains and must not be put in one:
    error messages are text, the position is data, and the drawing belongs to whoever
    is displaying them. A surface that has an editor should read `error.position` and
    underline in place; one that does not calls this.

    Indented four spaces so it renders as a markdown code block - the underline only
    lines up in a monospaced font, so the block is load-bearing, not decoration. Long
    lines are windowed around the mark rather than wrapped, because a wrapped line puts
    the mark under the wrong characters. An empty range draws a single caret; a range
    that runs past the end of its line stops there, because a multi-line underline in
    two dimensions is a job for the editor, not for this.
    """
    if not sql or position is None:
        return None
    lines = sql.split("\n")
    if position.start_line < 1 or position.start_line > len(lines):
        return None
    text = lines[position.start_line - 1]

    mark_at = max(0, position.start_column - 1)
    end = position.end_column - 1 if position.end_line == position.start_line else len(text)
    width = max(0, min(end, len(text)) - mark_at)

    window = 72
    prefix = ""
    if mark_at > window:
        cut = mark_at - window // 2
        text = text[cut:]
        mark_at -= cut
        prefix = "..."
    if len(prefix) + len(text) > window * 2:
        text = text[: window * 2 - len(prefix)] + "..."

    mark = "^" if width <= 1 else "^" + "~" * (width - 1)
    return f"    {prefix}{text}\n    {' ' * (len(prefix) + mark_at)}{mark}"


class SqlStatement(NamedTuple):
    """One statement out of a batch, and where it starts in the batch.

    `offset` is what lets a position inside `text` be reported against the text the
    reader actually submitted - without it, every statement after the first would have
    its line and column numbers quoted from the wrong place.
    """

    text: str
    offset: int


def split_sql_statements(sql: str) -> List[SqlStatement]:
    """
    Split a batch of semicolon-separated SQL statements.

    Comments are recognised but NOT removed: a `;` inside `-- ...` or `/* ... */` must
    not split the batch, while the comment itself is the parser's to tokenize and the
    reader's to see quoted back at them in an error. (Stripping comments beforehand was
    the old approach and it corrupted backtick-quoted identifiers containing `--`, which
    is exactly how a hyphenated blob-store path has to be written in this dialect.)

    A chunk with no SQL in it - a trailing comment after the last `;`, or blank lines -
    is dropped rather than handed to the parser as an empty statement.

    Parameters:
        sql: str
            A string containing one or more SQL statements.

    Returns:
        List[SqlStatement]: each statement's text, and its offset in `sql`.
    """
    statements: List[SqlStatement] = []
    length = len(sql)
    start = 0
    index = 0
    has_code = False
    in_single_quote = False
    in_double_quote = False
    in_backtick_quote = False

    def _emit(begin: int, end: int) -> None:
        text = sql[begin:end]
        stripped = text.lstrip()
        begin += len(text) - len(stripped)
        statements.append(SqlStatement(stripped.rstrip(), begin))

    while index < length:
        char = sql[index]

        if in_single_quote:
            # A doubled quote closes and immediately reopens, which lands on the same
            # state as treating it as an escape - `'don''t'` ends where it should.
            in_single_quote = char != "'"
        elif in_double_quote:
            in_double_quote = char != '"'
        elif in_backtick_quote:
            in_backtick_quote = char != "`"
        elif char == "'":
            in_single_quote = True
            has_code = True
        elif char == '"':
            in_double_quote = True
            has_code = True
        elif char == "`":
            in_backtick_quote = True
            has_code = True
        elif char == "-" and sql.startswith("--", index):
            newline = sql.find("\n", index)
            index = length if newline < 0 else newline
            continue
        elif char == "/" and sql.startswith("/*", index):
            close = sql.find("*/", index + 2)
            # An unterminated block comment runs to the end. The parser reports that,
            # with a position; splitting it apart here first would only obscure it.
            index = length if close < 0 else close + 2
            continue
        elif char == ";":
            if has_code:
                _emit(start, index)
            start = index + 1
            has_code = False
            index += 1
            continue
        elif not char.isspace():
            has_code = True

        index += 1

    if has_code:
        _emit(start, length)

    return statements


def convert_camel_to_sql_case(s: str) -> str:
    """
    Convert a PascalCase or camelCase string to an SQL-style uppercase string with spaces.

    Parameters:
        s: str
            The input string in PascalCase or camelCase.

    Returns:
        str: The converted string in SQL format.
    """
    return re.sub(r"([A-Z])", r" \1", s).strip().upper()
