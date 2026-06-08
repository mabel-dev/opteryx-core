# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import re

# Precompile regex patterns at module level
_TOKEN_PATTERN = re.compile(r"[(),;._]|[\r\n]+|[^\s(),;._\r\n]+")
_COMMENT_PATTERN = re.compile(
    r"(\"[^\"]\"|\'[^\']\'|\`[^\`]\`)|(/\*[^\*/]*\*/|--[^\r\n]*$)", re.MULTILINE | re.DOTALL
)
_ANSI_ESCAPE_PATTERN = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")
_NUMBER_PATTERN = re.compile(r"^[+-]?((\d+(\.\d*)?)|(\.\d+))([eE][+-]?\d+)?$")

_RESET = "\033[0m"
_COLOR_COMMENT = "\033[38;2;98;114;164m\033[3m"
_COLOR_PUNCTUATION = "\033[38;5;102m"
_COLOR_STRING = "\033[38;2;255;171;82m"
_COLOR_KEYWORD = "\033[38;2;139;233;253m"
_COLOR_BOOLEAN = "\033[38;2;255;184;188m"
_COLOR_FUNCTION = "\033[38;2;80;250;123m"
_COLOR_OPERATOR = "\033[38;2;189;147;249m"
_COLOR_NUMBER = "\033[38;2;255;184;108m"
_COLOR_COLUMN = "\033[38;2;150;150;150m"
_COLOR_RELATION = "\033[38;2;166;226;46m"

_PUNCTUATION_TOKENS = {"(", ")", ",", ";", "[", "]", ".", "_"}
_BOOLEAN_LITERALS = {"TRUE", "FALSE", "NULL"}
_OPERATOR_TOKENS = {
    "=",
    "==",
    ">=",
    "<=",
    "!=",
    "%",
    "<",
    ">",
    "<>",
    "-",
    "+",
    "*",
    "/",
    "//",
    "||",
    "|",
    "DIV",
    "LIKE",
    "ILIKE",
    "RLIKE",
    "NOT",
    "AND",
    "OR",
    "XOR",
    "IN",
    "SIMILAR",
    "TO",
    "BETWEEN",
    "IS",
    "->",
    "->>",
    "::",
    "@?",
    "@>",
    "@>>",
}
_KEYWORDS = {
    "ANALYZE",
    "ANTI",
    "AS",
    "ASC",
    "BY",
    "CASE",
    "CREATE",
    "CROSS",
    "DATE",
    "DATES",
    "DELETE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "EXECUTE",
    "EXPLAIN",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "INNER",
    "INTERVAL",
    "INTO",
    "INSERT",
    "JOIN",
    "LEFT",
    "LIMIT",
    "MONTH",
    "NATURAL",
    "OFFSET",
    "ON",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "RIGHT",
    "SELECT",
    "SEMI",
    "SET",
    "SHOW",
    "SINCE",
    "THEN",
    "TODAY",
    "UNION",
    "UNNEST",
    "UPDATE",
    "USE",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
    "YESTERDAY",
}

# Keywords that can appear as column/table names
_RESERVED_WORD_AS_IDENTIFIER = {
    "GROUP",
    "ORDER",
    "WHERE",
    "LIMIT",
    "OFFSET",
    "FROM",
    "JOIN",
    "ON",
    "USING",
    "PARTITION",
    "HAVING",
    "UNION",
}

# Tokens that precede relation names (tables, aliases, subqueries)
_RELATION_CONTEXT = {"FROM", "JOIN", "INTO", "UPDATE", "AS"}

# Tokens that precede column names
_COLUMN_CONTEXT = {
    "SELECT",
    ",",
    "BY",
    "ON",
    "USING",
    "PARTITION",
    "HAVING",
    "=",
    "==",
    ">=",
    "<=",
    "!=",
    "<",
    ">",
    "<>",
    "LIKE",
    "ILIKE",
    "RLIKE",
    "BETWEEN",
    "IN",
    "IS",
}
try:  # pragma: no cover - best-effort enrichment
    from opteryx.expression.functions import functions as _list_functions

    _FUNCTION_LIKE = set(_list_functions())
except Exception:  # pragma: no cover
    _FUNCTION_LIKE = set()


def tokenize_string(string):
    # Use the regular expression to find all tokens in the string
    tokens = _TOKEN_PATTERN.findall(string)

    # Remove any leading or trailing whitespace from the tokens
    tokens = ["\n" if token == "\n" else token.strip() for token in tokens]

    return tokens


def format_sql(sql):  # pragma: no cover
    """
    Adds colorization to SQL statements to make it easier to find keywords and literals

    It's not intended to be perfect, it's just to assist reading test outputs
    """

    def color_comments(string):  # pragma: no cover
        def _replacer(match):
            if match.group(2) is not None:
                return f"{_COLOR_COMMENT}{_ANSI_ESCAPE_PATTERN.sub('', match.group(2))}{_RESET}"
            return match.group(1)

        return _COMMENT_PATTERN.sub(_replacer, string)

    if getattr(sql, "decode", None) is not None:
        sql = sql.decode()
    words = tokenize_string(sql)

    formatted_sql = ""
    in_string_literal = None

    def _closes_literal(token, quote_char):
        """Return True if the token closes the currently open quoted literal."""
        if not token or token[-1] != quote_char:
            return False
        trailing = 0
        for char in reversed(token):
            if char == quote_char:
                trailing += 1
            else:
                break
        return trailing % 2 == 1

    def _leading_quote(token):
        if not token:
            return None
        first = token[0]
        if first in ("'", '"', "`"):
            return first
        if len(token) > 1 and token[1] == "'" and token[0].upper() in {"N", "E", "B", "X"}:
            return "'"
        return None

    for i, word in enumerate(words):
        if in_string_literal:
            formatted_sql += " " + word
            if _closes_literal(word, in_string_literal):
                formatted_sql += _RESET + " "
                in_string_literal = None
            continue

        if word == "\n":
            formatted_sql = formatted_sql.strip() + "\n"
            continue

        quote_char = _leading_quote(word)
        if quote_char:
            formatted_sql += _COLOR_STRING + word
            if not _closes_literal(word, quote_char):
                in_string_literal = quote_char
            else:
                formatted_sql += _RESET + " "
            continue

        upper_word = word.upper()

        # Check if previous token indicates this should be treated as an identifier
        prev_token = None
        if i > 0:
            # Skip newlines to find the actual previous token
            for j in range(i - 1, -1, -1):
                if words[j] != "\n":
                    prev_token = words[j].upper()
                    break

        # For punctuation like . or _, skip back to find the context keyword
        # This handles qualified names like schema.table
        context_token = prev_token
        if prev_token in (".", "_"):
            for j in range(i - 2, -1, -1):
                if words[j] != "\n" and words[j] not in (".", "_"):
                    tk = words[j].upper()
                    # If we find a context keyword (FROM, JOIN, etc.), use it
                    if tk in _RELATION_CONTEXT or tk in ("SELECT", ",") or tk in _COLUMN_CONTEXT:
                        context_token = tk
                        break

        # Contexts where reserved words can be used as plain identifiers (preserve case, no color)
        is_reserved_word_context = prev_token in (
            "SELECT",
            ",",
            "FROM",
            "AS",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
            "ANTI",
            "SEMI",
            "NATURAL",
        )

        if word in _PUNCTUATION_TOKENS:
            # Don't add space after dots and underscores for qualified names
            if word in (".", "_"):
                formatted_sql += f"{_COLOR_PUNCTUATION}{word}{_RESET}"
            else:
                formatted_sql += f"{_COLOR_PUNCTUATION}{word}{_RESET} "
        elif upper_word in _KEYWORDS:
            # Handle reserved words that can be used as identifiers
            if upper_word in _RESERVED_WORD_AS_IDENTIFIER:
                if is_reserved_word_context:
                    # In SELECT/FROM list, preserve as plain identifier
                    formatted_sql += word + " "
                elif prev_token in _COLUMN_CONTEXT or prev_token in ("WHERE", "AND", "OR", "NOT"):
                    # In conditions/expressions, color as column
                    formatted_sql += f"{_COLOR_COLUMN}{word}{_RESET} "
                else:
                    # Default: color as keyword
                    formatted_sql += f"{_COLOR_KEYWORD}{upper_word}{_RESET} "
            else:
                # Non-reserved keyword
                formatted_sql += f"{_COLOR_KEYWORD}{upper_word}{_RESET} "
        elif upper_word in _BOOLEAN_LITERALS:
            formatted_sql += f"{_COLOR_BOOLEAN}{upper_word}{_RESET} "
        elif ((i + 1) < len(words) and words[i + 1] == "(") or upper_word in _FUNCTION_LIKE:
            formatted_sql += f"{_COLOR_FUNCTION}{upper_word}{_RESET} "
        elif upper_word in _OPERATOR_TOKENS:
            formatted_sql += f"{_COLOR_OPERATOR}{upper_word}{_RESET} "
        elif _NUMBER_PATTERN.match(word.replace(",", "")):
            formatted_sql += f"{_COLOR_NUMBER}{word}{_RESET} "
        elif context_token in _RELATION_CONTEXT:
            # Color relation names (tables, aliases, subqueries, including qualified names like schema.table)
            formatted_sql += f"{_COLOR_RELATION}{word}{_RESET} "
        elif context_token in _COLUMN_CONTEXT or context_token in ("WHERE", "AND", "OR", "NOT"):
            # Color column names in conditions and other contexts
            formatted_sql += f"{_COLOR_COLUMN}{word}{_RESET} "
        else:
            formatted_sql += word + " "

    formatted_sql += _RESET

    spaces_after = (
        "FROM",
        "WHERE",
        "JOIN",
        "/",
        "AND",
        "OR",
        "NOT",
        "XOR",
        "+",
        "-",
        "*",
        "UNION",
        "ON",
    )

    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION}(", f"{_COLOR_PUNCTUATION}(")
    for item in spaces_after:
        formatted_sql = formatted_sql.replace(
            f"{item}{_RESET}{_COLOR_PUNCTUATION}(", f"{item}{_RESET} {_COLOR_PUNCTUATION}("
        )
    formatted_sql = formatted_sql.replace(f"({_RESET} ", f"({_RESET}")
    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION})", f"{_COLOR_PUNCTUATION})")
    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION},", f"{_COLOR_PUNCTUATION},")
    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION};", f"{_COLOR_PUNCTUATION};")
    # Remove spaces around dots and underscores for qualified names
    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION}.", f"{_COLOR_PUNCTUATION}.")
    formatted_sql = formatted_sql.replace(f" {_COLOR_PUNCTUATION}_", f"{_COLOR_PUNCTUATION}_")
    formatted_sql = formatted_sql.replace(
        f"{_RESET} {_COLOR_PUNCTUATION}.", f"{_RESET}{_COLOR_PUNCTUATION}."
    )
    formatted_sql = formatted_sql.replace(
        f"{_RESET} {_COLOR_PUNCTUATION}_", f"{_RESET}{_COLOR_PUNCTUATION}_"
    )

    return color_comments(formatted_sql).strip()
