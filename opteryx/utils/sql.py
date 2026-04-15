import re
from functools import lru_cache
from typing import List

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


# Compile regex patterns at module level for reuse
_COMMENT_REGEX = re.compile(
    r"(\"[^\"]*\"|\'[^\']*\')|(/\*.*?\*/|--[^\r\n]*$)", re.MULTILINE | re.DOTALL
)


def remove_comments(string: str) -> str:
    """
    Remove comments from the string.

    Parameters:
        string: str
            The SQL query string from which comments are to be removed.

    Returns:
        str: The SQL query string with comments removed.
    """

    def _replacer(match):
        if match.group(2) is not None:
            return ""  # Remove the comment
        else:
            return match.group(1)  # Keep the quoted string

    return _COMMENT_REGEX.sub(_replacer, string)


_WHITESPACE_REGEX = re.compile(
    r"(\"[^\"]*\"|\'[^\']*\'|\`[^\`]*\`)|(\s+)", re.MULTILINE | re.DOTALL
)


def clean_statement(string: str) -> str:
    """
    Remove carriage returns and all whitespace to single spaces.

    Avoid removing whitespace in quoted strings.
    """

    def _replacer(match):
        if match.group(2) is not None:
            return " "
        return match.group(1)  # captured quoted-string

    return _WHITESPACE_REGEX.sub(_replacer, string).strip()


def split_sql_statements(sql: str) -> List[str]:
    """
    Splits multiple SQL statements separated by semicolons into a list.

    Parameters:
        sql: str
            A string containing one or more SQL statements.

    Returns:
        List[str]: A list of individual SQL statements.
    """
    statements = []
    buffer: list = []
    in_single_quote = False
    in_double_quote = False
    in_backtick_quote = False

    for char in sql:
        if char == "'" and not in_double_quote and not in_backtick_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not in_backtick_quote:
            in_double_quote = not in_double_quote
        elif char == "`" and not in_single_quote and not in_double_quote:
            in_backtick_quote = not in_backtick_quote
        elif char == ";" and not in_single_quote and not in_double_quote and not in_backtick_quote:
            statements.append("".join(buffer).strip())
            buffer = []
            continue

        buffer.append(char)

    # Append any remaining text
    if buffer:
        statements.append("".join(buffer).strip())

    return [s for s in statements if s != ""]


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
