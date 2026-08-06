# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Render a collected result set for the command line.

This is presentation only - it works on Python values already pulled out of the
morsels, so nothing here sits on the execution path.

`draken.Morsel.to_string()` renders a morsel for debugging: the first and last
five rows, columns capped at thirty characters, its own row/column footer. That
is the wrong contract for a user asking for their result at a terminal, who is
owed every row they asked for and a truncation they chose, so the CLI renders
its own.
"""

import base64
import datetime
import decimal
import json
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

# Terminal palette, matching draken's morsel renderer.
_RESET = "\033[0m"
_PUNCTUATION = "\033[38;5;102m"
_HEADING = "\033[1m"
_TYPE = "\033[38;2;98;114;164m"
_INDEX = "\033[38;2;98;114;164m"
_NULL = "\033[38;2;64;75;108m\033[3m"
_BOOLEAN = "\033[38;2;139;233;253m\033[3m"
_INTEGER = "\033[38;2;189;147;249m"
_DECIMAL = "\033[38;2;255;121;198m"
_VARCHAR = "\033[38;2;255;171;82m"
_TIMESTAMP = "\033[38;2;80;250;123m"
_TIME = "\033[38;2;26;185;67m"
_BLOB = "\033[38;2;196;160;0m"

_NULL_TEXT = "null"
_ELLIPSIS = "\u2026"

# Columns are squeezed to fit the terminal, but never below this - past it the
# values are ellipsis and no longer worth the horizontal space.
_MINIMUM_COLUMN_WIDTH = 6


def _cell(value: Any) -> Tuple[str, str, bool]:
    """
    Convert a single value to its display form.

    Returns:
        (text, color, numeric) - `color` is the ANSI prefix for the value's type,
        `numeric` marks values which right-align in their column.
    """
    if value is None:
        return _NULL_TEXT, _NULL, False
    # bool before int - bool is a subclass of int
    if isinstance(value, bool):
        return str(value), _BOOLEAN, False
    if isinstance(value, int):
        return str(value), _INTEGER, True
    if isinstance(value, (float, decimal.Decimal)):
        return str(value), _DECIMAL, True
    if isinstance(value, str):
        return value, _VARCHAR, False
    if isinstance(value, bytes):
        return _decode_blob(value), _BLOB, False
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=" "), _TIMESTAMP, False
    if isinstance(value, datetime.date):
        return value.isoformat(), _TIMESTAMP, False
    if isinstance(value, datetime.time):
        return value.isoformat(), _TIME, False
    return str(value), _RESET, False


def _decode_blob(value: bytes) -> str:
    """Render a BLOB as text where it is text, and as hex where it is not."""
    decoded = value.decode("utf-8", errors="ignore")
    if decoded.encode("utf-8") == value:
        return decoded
    return value.hex()


def _fit(text: str, width: int) -> str:
    """Truncate `text` to `width` characters, marking that it was truncated."""
    if len(text) <= width:
        return text
    return text[: width - 1] + _ELLIPSIS


def _squeeze(widths: List[int], fixed: int, display_width: int) -> List[int]:
    """
    Shrink columns to fit `display_width` characters of terminal.

    Repeatedly takes a character off the widest column, and once every column is
    at the minimum width drops columns from the right rather than wrapping.

    Returns:
        The widths of the columns which are shown - shorter than `widths` when
        columns had to be dropped.
    """
    widths = list(widths)
    while widths:
        total = fixed + sum(widths) + 3 * (len(widths) - 1)
        if total <= display_width:
            break
        widest = max(widths)
        if widest <= _MINIMUM_COLUMN_WIDTH:
            widths.pop()
            continue
        widths[widths.index(widest)] = widest - 1
    return widths


def format_table(
    column_names: Sequence[str],
    column_types: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    display_width: Optional[int] = None,
    colorize: bool = True,
    max_column_width: int = 64,
) -> str:
    """
    Render a result set as a box-drawn table.

    Parameters:
        column_names: the column headings.
        column_types: the type of each column, shown under its heading.
        rows: the result, one sequence of values per row.
        display_width: characters of terminal to fit the table into, None for
            unlimited. Columns are narrowed, then dropped, to fit.
        colorize: emit ANSI color codes.
        max_column_width: characters after which a value is truncated.

    Returns:
        The rendered table, without a trailing newline.
    """
    if not column_names:
        return ""

    # Render every cell up front - the widths depend on what the values look
    # like, so there is no way to size the table without doing this first.
    cells: List[List[Tuple[str, str]]] = []
    # A column right-aligns when it holds numbers and nothing else; a column of
    # nothing but nulls is not a number column.
    numbers = [False] * len(column_names)
    others = [False] * len(column_names)
    for row in rows:
        rendered = []
        for index, value in enumerate(row):
            text, color, is_numeric = _cell(value)
            if value is not None:
                if is_numeric:
                    numbers[index] = True
                else:
                    others[index] = True
            rendered.append((text, color))
        cells.append(rendered)

    numeric = [number and not other for number, other in zip(numbers, others)]

    widths = [
        max(
            1,
            min(
                max_column_width,
                max(
                    len(name),
                    len(str(kind)),
                    max((len(row[index][0]) for row in cells), default=0),
                ),
            ),
        )
        for index, (name, kind) in enumerate(zip(column_names, column_types))
    ]

    # the row number, padded either side
    index_width = max(len(str(len(rows))), 1) + 2
    # "|" + index + "| " ... " |" - the fixed cost of the frame around the columns
    frame_width = index_width + 5

    dropped = 0
    if display_width is not None:
        squeezed = _squeeze(widths, frame_width, display_width)
        dropped = len(widths) - len(squeezed)
        widths = squeezed
        if not widths:
            return f"[ {len(column_names)} columns too wide to display ]"

    columns = len(widths)

    def paint(text: str, color: str) -> str:
        return (color + text + _RESET) if colorize else text

    def punctuate(text: str) -> str:
        return paint(text, _PUNCTUATION)

    def rule(left: str, fill: str, join: str, right: str) -> str:
        return punctuate(
            left
            + (fill * index_width)
            + join
            + fill
            + (fill + join + fill).join(fill * width for width in widths)
            + fill
            + right
        )

    def heading(values: Sequence[str], color: str) -> str:
        painted = punctuate(" \u2502 ").join(
            paint(_fit(value, width).center(width), color) for value, width in zip(values, widths)
        )
        return (
            punctuate("\u2502")
            + (" " * index_width)
            + punctuate("\u2502 ")
            + painted
            + punctuate(" \u2502")
        )

    lines = [
        rule("\u250c", "\u2500", "\u252c", "\u2510"),
        heading([str(name) for name in column_names[:columns]], _HEADING),
        heading([str(kind) for kind in column_types[:columns]], _TYPE),
        rule("\u255e", "\u2550", "\u256a", "\u2561"),
    ]

    for position, row in enumerate(cells, start=1):
        values = []
        for index in range(columns):
            text, color = row[index]
            text = _fit(text, widths[index])
            text = text.rjust(widths[index]) if numeric[index] else text.ljust(widths[index])
            values.append(paint(text, color))
        lines.append(
            punctuate("\u2502")
            + paint(str(position).rjust(index_width - 1), _INDEX)
            + punctuate(" \u2502 ")
            + punctuate(" \u2502 ").join(values)
            + punctuate(" \u2502")
        )

    lines.append(rule("\u2514", "\u2500", "\u2534", "\u2518"))

    # Anything the display hid is said out loud - a result which silently loses
    # columns is a wrong answer the reader cannot see.
    if dropped:
        lines.append(f"[ {dropped} columns too wide to display ]")

    return "\n".join(lines)


def format_markdown(
    column_names: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> str:
    """
    Render a result set as a Markdown table.

    Parameters:
        column_names: the column headings.
        rows: the result, one sequence of values per row.

    Returns:
        The rendered table, without a trailing newline.
    """
    if not column_names:
        return ""

    def escape(text: str) -> str:
        return text.replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(escape(str(name)) for name in column_names) + " |",
        "| " + " | ".join("---" for _ in column_names) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(escape(_cell(value)[0]) for value in row) + " |")

    return "\n".join(lines)


def _jsonable(value: Any) -> Any:
    """Convert values JSON has no representation for. Unknown types are an error."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"Cannot serialize {type(value).__name__} to JSON")


def format_jsonl(column_names: Sequence[str], row: Sequence[Any]) -> bytes:
    """
    Render one result row as a JSON document.

    BLOB values are base64 encoded; DECIMAL values become JSON numbers.
    """
    return json.dumps(dict(zip(column_names, row)), default=_jsonable).encode("utf-8")
