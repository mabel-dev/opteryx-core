# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Bespoke error types for Opteryx and error types and structure as defined in PEP-0249.

Exception Hierarchy:

Exception
 ├── MissingDependencyError
 ├── UnmetRequirementError
 └── Error [PEP-0249] *
     └── DatabaseError [PEP-0249] *
         ├── IncompleteImplementationError
         ├── InvalidConfigurationError
         ├── InvalidInternalStateError
         ├── NotSupportedError
         ├── UnsupportedFileTypeError
         ├── UnsupportedTypeError
         └── ProgrammingError [PEP-0249] *
             ├── DataError *
             │   ├── InconsistentSchemaError
             │   ├── DatasetReadError
             │   ├── EmptyDatasetError
             │   └── EmptyResultSetError
             ├── ExecutionError *
             │   └── RemoteConnectionError
             ├── MissingSqlStatement
             ├── InvalidCursorStateError
             ├── ParameterError
             ├── SecurityError *
             │   └── PermissionsError
             └── SqlError *
                 ├── AmbiguousDatasetError
                 ├── AmbiguousIdentifierError
                 ├── ArrayWithMixedTypesError
                 ├── ColumnNotFoundError
                 ├── ColumnReferencedBeforeEvaluationError
                 ├── DatasetNotFoundError
                 ├── FunctionExecutionError
                 │   └── InvalidFunctionParameterError
                 ├── FunctionNotFoundError
                 ├── IncorrectTypeError
                 ├── InvalidTemporalRangeFilterError
                 ├── IncompatibleTypesError
                 ├── UnexpectedDatasetReferenceError
                 ├── UnnamedColumnError
                 ├── UnsupportedSyntaxError
                 └── VariableNotFoundError
"""

import re
from typing import Any, Iterable, NamedTuple, Optional, Union

# ======================== Begin Message Markup ========================
# Error messages ARE markdown. The surface that displays them renders it; this
# module owns what the markup means, and that separation is the whole contract -
# the renderer never inspects the text, and nothing here knows how it is drawn.
#
#   column / variable name   italic          *user_id*
#   SQL syntax               bold uppercase  **SHOW COLUMNS FROM**
#   table name               code span       `public.astronomy.planets`
#   suggestion               code span       `planetId`   (copyable in the UI)
#   underlying error         underline       __Expected: end of statement__
#
# Markdown has no underline of its own. `__text__` is the token limited-markdown
# renderers use for it (CommonMark would read it as bold), so it is the one
# convention here that the renderer has to agree with. Every message goes through
# these helpers so all five can be changed in one place.

# Only the characters the display surface actually treats as marks. It consumes a
# backslash ONLY in front of one of its own marks and leaves every other backslash
# alone, which decides the two exclusions here:
#
#   '\\' is NOT escaped. Raw backslashes survive intact - `C:\temp\file.csv` and
#        `'\d+'` render as written - so escaping them would DOUBLE every backslash
#        in a path or a regex pattern. The cost is a name containing a literal
#        backslash before a mark (`a\*b`), which is not a thing anyone has.
#   '['  is NOT escaped. There is no evidence it is a mark on that surface, and a
#        backslash in front of a non-mark is left in place - so escaping it would
#        just show the reader a stray backslash.
#
# A SINGLE '_' is likewise absent: `_text_` is not italic there, deliberately,
# because SQL is full of snake_case. `user_id` and `CIDR_AGG` must reach the
# reader unbackslashed.
_EMPHASIS_SPECIALS = "`*"

# A RUN of underscores is different: `__text__` IS the underline mark. The surface
# ignores it inside a word (`foo__bar__baz` stays literal), but at the edge of a
# span it is not inside a word - `*__dunder__*` puts the run against a `*`, and an
# underlying error containing `my__col` can pair its run with the closing mark of
# md_cause. Both cases underline something nobody asked to underline.
_UNDERSCORE_RUN = re.compile(r"__+")


def _escape_emphasis(value: str) -> str:
    """Backslash-escape the characters that would end an emphasis span early.

    A column really can be named `a*b` - quoted identifiers permit it, and JSONL
    keys are whatever the file says. Rendering `*a*b*` would break the span and
    swallow the rest of the message, which is the one moment the reader most needs
    to see the name intact.
    """
    text = "".join(f"\\{ch}" if ch in _EMPHASIS_SPECIALS else ch for ch in str(value))
    return _UNDERSCORE_RUN.sub(lambda run: "\\_" * len(run.group()), text)


def md_column(name: Any) -> str:
    """A column or variable name, in italic."""
    return f"*{_escape_emphasis(name)}*"


def md_syntax(text: Any) -> str:
    """SQL syntax, in bold uppercase."""
    return f"**{_escape_emphasis(str(text).upper())}**"


def md_code(value: Any) -> str:
    """A code span - table names, suggestions, literals, type names, settings.

    A code span cannot be escaped with backslashes; the fence has to grow past the
    longest backtick run in the content, and a value that starts or ends with a
    backtick needs padding spaces (CommonMark strips exactly one).
    """
    text = str(value)
    longest = 0
    run = 0
    for ch in text:
        run = run + 1 if ch == "`" else 0
        longest = max(longest, run)
    fence = "`" * (longest + 1)
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def md_table(name: Any) -> str:
    """A table name, in a code span. Named for the call sites' sake - a table and a
    suggestion are drawn the same way, but they are not the same thing."""
    return md_code(name)


def md_cause(text: Any) -> str:
    """The underlying error, underlined."""
    return f"__{_escape_emphasis(text)}__"


def md_list(values: Iterable[Any], style=md_code) -> str:
    """A comma-separated list of styled values, for 'supported values are ...'."""
    return ", ".join(style(value) for value in values)


_TERMINATORS = ".?!"


def compose(*parts: Optional[str]) -> str:
    """Join message fragments into whole sentences.

    Every message is built through this rather than by concatenation, because the
    defects concatenation produced were all in the joins: `Unknown column 'x' .`
    (space before the period), `Unknown column 'x'  in 'y'.` (doubled space from a
    fragment that already carried its own leading one), `Did you mean 'LENGTH'?.`
    (terminator on top of a terminator), and one message that simply ended in a
    comma. Empty and None fragments drop out, so a caller can pass an optional
    clause without guarding it.

    Runs of whitespace collapse, so callers may wrap fragments across source lines
    freely. Messages that need real line structure (the parser error's snippet)
    assemble themselves and do not come through here.
    """
    sentences = []
    for part in parts:
        if part is None:
            continue
        text = " ".join(str(part).split())
        if not text:
            continue
        if text[-1] not in _TERMINATORS:
            text += "."
        sentences.append(text)
    return " ".join(sentences)


def did_you_mean(suggestion: Optional[Any]) -> str:
    """The suggestion clause - always a sentence of its own, never a tail.

    Two rules live here so they cannot drift across the call sites that offer
    suggestions: the suggestion is a code span (the display surface makes those
    copyable, which is the point of showing it), and it is its own sentence -
    never appended after a comma, hyphen or colon. Returns "" when there is
    nothing to suggest, so callers pass it to `compose` unguarded.
    """
    if suggestion is None:
        return ""
    return f"Did you mean {md_code(suggestion)}?"


# ======================== End Message Markup ==========================


# ======================== Begin Codebase Errors ========================
class MissingDependencyError(Exception):  # pragma: no cover
    def __init__(self, dependency: str, hint: str = None):
        self.dependency = dependency
        if hint:
            message = hint
        else:
            message = f"No module named '{dependency}' can be found, please install or include in requirements.txt/pyproject.toml."
        super().__init__(message)


# ======================== End Codebase Errors ==========================


# ======================== Begin PEP-0249 Exceptions ========================
# These should not be thrown directly unless explicitly required for standards compliance
class Error(Exception):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception that is the base class of all other error exceptions. You can use this to
    catch all errors with one single except statement. Warnings are not considered
    errors and thus should not use this class as base. It must be a subclass of the
    Python StandardError (defined in the module exceptions).
    """


class DatabaseError(Error):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for errors that are related to the database. It must be a subclass
    of Error.
    """


class ProgrammingError(DatabaseError):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for programming errors, e.g. table not found or already exists,
    syntax error in the SQL statement, wrong number of parameters specified, etc. It
    must be a subclass of DatabaseError.
    """


class ReadOnlyConnectorError(DatabaseError):
    """
    Exception raised when attempting a write operation (CREATE, DROP, TRUNCATE) on
    a read-only connector that does not support the Writable capability.
    """


# ======================== End PEP-0249 Exceptions ==========================


class SourcePosition(NamedTuple):
    """Where in the submitted SQL an error is, for an editor to underline.

    A RANGE, not a point: the whole offending name gets marked, not one character of
    it. Ranges are HALF-OPEN and follow the two conventions editors actually use, so
    neither consumer has to do arithmetic that could be got wrong:

    - `start_line` / `start_column` / `end_line` / `end_column` are 1-based, which is
      what a person reads off a gutter and what Monaco's `IRange` wants.
    - `start_offset` / `end_offset` are 0-based character offsets into the statement,
      which is what CodeMirror's `from`/`to` wants and what `sql[start:end]` slices.

    The two describe the same span. `end` is exclusive in both, so an empty range
    (`start == end`) is a legitimate "here, between these characters" - it is what a
    position with no known extent reduces to, and an editor draws it as a caret.

    Coordinates index the SQL AS SUBMITTED - comments, line breaks and all - never the
    rewritten text handed to the parser. Mapping between the two is the SQL rewriter's
    job and it has already happened by the time this exists.
    """

    start_line: int
    start_column: int
    end_line: int
    end_column: int
    start_offset: int
    end_offset: int


# ======================== Begin Opteryx Superclasses ========================
# These should not be thrown directly
class SqlError(ProgrammingError):
    """
    Used as a superclass for errors users can resolve by updating the SQL statement.

    Where possible, SqlErrors in particular, should provide messages appropriate for
    end-users who may not know, or care, about the underlying SQL platform.

    POINTING AT THE SQL
    -------------------
    Every one of these is about something the reader wrote, so every one of them can
    say WHERE. `position` is that: a `SourcePosition` range over the statement as
    submitted, for the editor to underline.

    It is data, not drawing. The message never contains a caret, a snippet or any
    other rendering of the position - this repo owns the TEXT and the renderer owns
    the DRAWING, and a position printed into the message is the reader being asked to
    line two things up by eye. `opteryx.utils.sql.underline` exists for terminals,
    which have no editor to underline in, and is called by the surface that wants it.

    Two attributes, split because the two halves are known in different places:

    - `span` is set at the RAISE site, which knows which AST node went wrong but has
      no idea what text the statement was. Flattened sqlparser coordinates,
      `(start_line, start_column, end_line, end_column)`, indexing the text the PARSER
      was given. Internal - consumers want `position`.
    - `position` is set at the planner BOUNDARY, which has the statement and can map
      those coordinates onto the text the reader submitted. See
      `opteryx.planner.attach_source_position`.

    Both stay None for anything the reader did not write at a place we can name - a
    wildcard expansion, a predicate the optimizer moved, a plan built by an API rather
    than parsed. No position is a normal outcome, not a defect.
    """

    #: Class-level defaults, so no subclass has to remember to initialise them.
    span = None
    position = None


class DataError(ProgrammingError):
    """Superclass for data-related errors."""


class SecurityError(ProgrammingError):
    """Superclass for security-related errors."""


class ExecutionError(ProgrammingError):
    """Superclass for execution-related errors."""


# ======================== End Opteryx Superclasses ==========================


class RemoteConnectionError(ExecutionError):
    """Exception raised when remote systems don't repond in a timely manner"""


# ======================== Begin SQL-Specific Exceptions ========================
class ColumnNotFoundError(SqlError):
    """Exception raised for Column Not Found errors."""

    def __init__(
        self,
        message: str = None,
        column: str = None,
        dataset: str = None,
        suggestion: str = None,
        span=None,
    ):
        """
        Return as helpful a Column Not Found error as we can, by being specific and
        offering suggestions.

        The advice is offered even when there IS a suggestion: the suggestion is a
        typo detector's best guess and can be wrong, and a reader it guessed wrong
        for still needs somewhere to go.

        The advice used to open "Column names are case sensitive". They are not -
        the binder resolves every identifier with `case_insensitive=True` (see
        `locate_identifier_in_loaded_schemas`), so casing cannot be the cause of any
        error this class reports. It sent readers off to audit the one thing that was
        certainly not wrong. Do not reintroduce it: DATASET names can be case
        sensitive, because they can resolve to a filesystem path, but column names
        never are.

        `span` is where the name was written - see `SqlError`, which turns it into a
        caret at the planner boundary. None whenever the reference was not something
        the reader wrote at that spot: a wildcard expansion, a predicate the optimizer
        moved, a column named from a plan rather than from SQL.
        """
        self.column = column
        self.suggestion = suggestion
        self.dataset = dataset
        self.span = span

        if column is not None:
            where = f" in {md_table(dataset)}" if dataset else ""
            found = f"Column {md_column(column)} cannot be found{where}"
            if dataset:
                advice = (
                    f"List the columns it does have with "
                    f"{md_syntax('SHOW COLUMNS FROM')} {md_table(dataset)}"
                )
            else:
                advice = (
                    f"List a table's columns with {md_syntax('SHOW COLUMNS FROM')} "
                    f"and the table name"
                )
            message = compose(found, did_you_mean(suggestion), advice)
        if message is None:  # pragma: no cover
            message = compose("The query referenced columns that cannot be found")
        super().__init__(message)


class ColumnReferencedBeforeEvaluationError(SqlError):
    """
    Return an error message when the column reference order is incorrect
    """

    def __init__(self, column: str):
        self.column = column
        super().__init__(
            compose(
                f"Column {md_column(column)} cannot be referenced here - it has not been "
                f"evaluated at this point in the query",
                f"A column created in {md_syntax('SELECT')} is not available to "
                f"{md_syntax('WHERE')} or {md_syntax('GROUP BY')} in the same query; "
                f"repeat the expression, or wrap the query as a subquery and reference "
                f"the column outside it",
            )
        )


class DatasetNotFoundError(SqlError):
    """Exception raised when a dataset is not found."""

    def __init__(self, connector: str, dataset: str = None, suggestion: Optional[str] = None):
        self.dataset = dataset
        self.connector = connector
        self.suggestion = suggestion
        super().__init__(
            compose(
                f"Dataset {md_table(dataset)} cannot be found",
                did_you_mean(suggestion),
                # Deliberately no "list them with ..." here: Opteryx has no
                # dataset-listing statement (SHOW covers VARIABLES, USER, GRANTS,
                # TRIGGERS FOR and MANIFEST FOR only), and sending the reader to a
                # command that does not exist costs them a second error.
                None
                if suggestion
                else "Dataset names are case sensitive, and may need qualifying with "
                "their workspace and collection",
            )
        )


class CollectionNotEmptyError(SqlError):
    """Exception raised when DROP COLLECTION targets a non-empty collection."""

    def __init__(self, collection: str):
        self.collection = collection
        super().__init__(
            compose(
                f"Collection {md_table(collection)} is not empty",
                f"Drop its datasets and views first, then drop the collection",
            )
        )


class FunctionNotFoundError(SqlError):
    """Exception raised when a function is not found."""

    def __init__(
        self,
        message: str = None,
        function: str = None,
        suggestion: Optional[str] = None,
        span=None,
    ):
        """
        Return as helpful Function Not Found error as we can by being specific and offering
        suggestions.

        `span` is where the function name was written - see `SqlError`.
        """
        self.function = function
        self.suggestion = suggestion
        self.span = span

        if message is None:
            message = compose(
                f"Function {md_syntax(function)} cannot be found",
                did_you_mean(suggestion),
            )
        super().__init__(message)


class VariableNotFoundError(SqlError):
    """Exception raised when a variable is not found."""

    def __init__(self, variable: str, suggestion: Optional[str] = None):
        self.variable = variable
        self.suggestion = suggestion
        if variable is None:
            super().__init__()
            return
        super().__init__(
            compose(
                f"Variable {md_column(variable)} cannot be found",
                did_you_mean(suggestion),
                None if suggestion else f"{md_syntax('SHOW VARIABLES')} lists the "
                f"variables that are set",
            )
        )


class AmbiguousIdentifierError(SqlError):
    """Exception raised for ambiguous identifier references."""

    def __init__(self, identifier: Union[str, list, None] = None, message: Optional[str] = None):
        self.identifier = identifier
        if message is None:
            message = compose(
                f"Column {md_column(identifier)} is ambiguous - more than one relation "
                f"in this query has a column with that name",
                f"Qualify it with the relation it should come from, "
                f"for example {md_code(f'dataset.{identifier}')}",
            )
        super().__init__(message)


class AmbiguousDatasetError(SqlError):
    """Exception raised for ambiguous dataset references."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        super().__init__(
            compose(
                f"Dataset {md_table(dataset)} is referenced more than once in this query, "
                f"so a reference to it is ambiguous",
                f"Give each reference its own alias with {md_syntax('AS')}, for example "
                f"{md_code(f'{dataset} AS a')} and {md_code(f'{dataset} AS b')}",
            )
        )


class UnexpectedDatasetReferenceError(SqlError):
    """Exception raised for unexpected dataset references."""

    def __init__(self, dataset: str, message: Optional[str] = None):
        self.dataset = dataset
        if not message:
            message = compose(
                f"Dataset {md_table(dataset)} is referenced in the query, but it does not "
                f"appear in a {md_syntax('FROM')} or {md_syntax('JOIN')} clause",
                f"Add it to the query, or check whether the reference was meant to name "
                f"one of the relations already there",
            )
        super().__init__(message)


class InvalidTemporalRangeFilterError(SqlError):
    """Exception raised for invalid temporal range filters."""


class FunctionExecutionError(SqlError):
    """Exception raised for function execution errors."""

    def __init__(self, message: Optional[str] = None, function: Optional[str] = None):
        self.function = function
        if not message and function is not None:
            message = f"Function '{function}' call failed."
        if message and function is not None:
            message = f"{message} - Function: '{function}'"
        super().__init__(message)


class InvalidFunctionParameterError(FunctionExecutionError):
    """Exception raised for invalid function parameters."""


class UnsupportedSyntaxError(SqlError):
    """Exception raised for unsupported syntax."""


class QueryParseError(SqlError):
    """Raised when a statement is not valid SQL and cannot be parsed at all.

    Distinct from UnsupportedSyntaxError, which means the statement parsed fine and
    Opteryx will not run it. Here nothing was understood, so there is no clause to
    name and the only orientation available is a position.

    `position` is the range to underline, over the statement as submitted - the caller
    has already mapped it off the text the parser was given. `line`/`column` remain as
    the start of that range, for callers that only want a point. The message never
    draws it: `opteryx.planner.parse_error` works out WHERE, this class says WHAT, and
    the editor does the marking.
    """

    def __init__(
        self,
        *,
        sql: Optional[str] = None,
        line: Optional[int] = None,
        column: Optional[int] = None,
        cause: Optional[str] = None,
        suggestion: Optional[str] = None,
        hint: Optional[str] = None,
        position: Optional[SourcePosition] = None,
    ):
        self.sql = sql
        self.line = line
        self.column = column
        self.cause = cause
        self.suggestion = suggestion
        self.hint = hint
        self.position = position

        # Naming the position in the text as well as carrying it: a reader looking at a
        # log, or an API response with no editor behind it, has nothing else to go on.
        # On a single-line statement there is no line worth quoting.
        if column is None:
            headline = "The query could not be parsed"
        elif sql and "\n" in sql and line is not None:
            headline = f"The query could not be parsed at line {line}, column {column}"
        else:
            headline = f"The query could not be parsed at column {column}"

        # Advice, in descending order of how much we actually know. A suggestion
        # beats a shape-based guess, and a guess beats the generic fallback - but
        # something is always said, because "could not be parsed" on its own leaves
        # a reader with nowhere to go.
        advice = did_you_mean(suggestion) or hint or (
            "Check for a missing comma, an unclosed bracket or quote, or a keyword "
            "that is not spelled the way SQL expects"
        )

        message = compose(headline, advice)
        if cause:
            message = f"{message}\n\nParser: {md_cause(cause)}"

        super().__init__(message)


class ResultTooLargeError(SqlError):
    """Raised when a query's result exceeds `sql_select_limit`.

    Deliberately an ERROR and not a truncation: silently returning the first N rows
    of a larger result is a wrong answer wearing the shape of a right one, and the
    caller has no way to tell. A caller who genuinely wants the first N says so with
    a LIMIT — which is what the message tells them to do.

    Raised from two places, because neither alone is sufficient:
      - PLAN time, from the estimate, but ONLY when every input relation has real
        row-count statistics (an estimate resting on a fabricated default could
        reject a query that returns a handful of rows);
      - RUN time, from the rows actually delivered, which catches the cases the
        estimate was too low to predict.
    """

    def __init__(self, rows, limit: int, estimated: bool = False):
        self.rows = rows
        self.limit = limit
        self.estimated = estimated
        how = "is estimated to return" if estimated else "returned"
        super().__init__(
            compose(
                f"The query {how} {rows:,} rows, which is over the {limit:,} row limit "
                f"set by {md_code('sql_select_limit')}",
                # Not "or raise sql_select_limit": setting it needs a platform-admin
                # entitlement, so for almost every reader that advice is a second error.
                f"Add a {md_syntax('LIMIT')} clause to bound the result, for example "
                f"{md_code('... LIMIT 1000')}",
            )
        )


class MergeTooLargeError(SqlError):
    """Raised when a MERGE's set of acted-on row addresses exceeds the set budget.

    A merge holds the address of every target row it has acted on until it
    commits, because the commit is atomic: the appends and the row-deletes must
    land in one snapshot, so neither half can be flushed early. That set is a
    roaring bitmap over file-local ordinals, which is bounded by construction
    and dense for the shape merges actually produce - so reaching this means a
    genuinely enormous delta, or a target whose live rows are scattered thinly
    across many files.

    Deliberately an ERROR and not a partial commit. Committing what fits would
    leave the target holding some of the merge and not the rest, with nothing to
    say which - a wrong answer wearing the shape of a right one, and one written
    to storage.

    This is the ceiling that replaced MERGE's original plan-time row cap. The cap
    was a proxy: it bounded rows because the address set was a hash set costing
    ~48 bytes each, and rows were the thing that could be counted before running.
    With the set on roaring bitmaps the memory is bounded directly, so the limit
    is now stated in the terms that actually bind.
    """


class IncorrectTypeError(SqlError):
    """Exception raised for incorrect types."""


class VariantKeyError(IncorrectTypeError):
    """Raised when a GROUP BY / DISTINCT [ON] / ORDER BY key resolves to VARIANT.

    VARIANT (a dynamic JSON value — object, array, scalar, or null; e.g. the `->`
    operator's result) has no fixed type to hash or compare, so it can never be a
    key. This is a permanent restriction, not a coverage gap — the message says so
    and names the fix, rather than implying the case is merely unported.

    Raised from two places, deliberately: the BINDER (as early as possible — before
    the optimizer or native compiler do any work) for the common case, and the
    native compiler's own key-type gate as a backstop for any plan-construction path
    that bypasses normal binding. Both raise through this one class so the message
    can't drift between them.
    """

    def __init__(self, what: str, name: str):
        self.what = what
        self.name = name
        super().__init__(
            compose(
                f"{md_syntax(what)} on column {md_column(name)} is not supported, because "
                f"the column is {md_code('VARIANT')} - a dynamic JSON value, which has no "
                f"fixed type to key on",
                f"Cast it to a concrete type first: use {md_code('->>')} instead of "
                f"{md_code('->')} to extract JSON text, or "
                f"{md_code('CAST(... AS VARCHAR)')}",
            )
        )


class CidrAggTypeError(IncorrectTypeError):
    """Raised when CIDR_AGG's operand is not an IPV4 column.

    IPV4 refines UINT32, and that descriptor is the only thing separating an address
    from any other 32-bit number — CIDR_AGG over an id or a count would fold plain
    integers into well-formed, confident, entirely invented network ranges. The gate
    is therefore on the descriptor rather than the width, and the refusal is
    PERMANENT, not a coverage gap: hence IncorrectTypeError rather than
    NotSupportedError, whose "not supported yet" wording told the reader to wait for
    a feature that is never coming.

    Raised from the native compiler's plan-time gate, which knows the column and its
    type and so names both. The native sink refuses too, as a run-time backstop for a
    vector that reaches it without the descriptor (cidr_operand_supported in
    src/cpp/engine/native_group_sinks.hpp); that message is the no-name INTEGER form
    of this one, word for word — keep the two in step.

    ``operand_kind`` selects the two variable clauses, and the three values are the
    three genuinely different things to tell the reader:

      INTEGER — the case that needs explaining. An integer is bit-for-bit
        indistinguishable from an address, so the refusal looks arbitrary until you
        are told what it prevents. Gets the rationale AND the cast. The run-time
        backstop is always this kind: reaching it means a UINT32 arrived without its
        descriptor.
      TEXT — nothing surprising about text not being an address, and the rationale
        would be a non-sequitur ("a plain integer column is refused" about a
        VARCHAR), so it gets the cast only.
      OTHER — DECIMAL, FLOAT, TIMESTAMP and friends have NO cast to IPV4 at all
        (see casts.pyx's IPV4 target: string family and integer widths only), so
        suggesting `col::IPV4` would send the reader to a second error. Says the
        column cannot hold addresses instead.
    """

    INTEGER = "integer"
    TEXT = "text"
    OTHER = "other"

    def __init__(
        self,
        name: Optional[str] = None,
        type_name: Optional[str] = None,
        operand_kind: str = INTEGER,
    ):
        self.name = name
        self.type_name = type_name
        self.operand_kind = operand_kind
        column = md_code(f"{name}::IPV4" if name is not None else "<column>::IPV4")
        if name is not None and type_name is not None:
            requires = (
                f"{md_syntax('CIDR_AGG')} requires an {md_code('IPV4')} column, but "
                f"{md_column(name)} is {md_code(type_name)}"
            )
        else:
            requires = f"{md_syntax('CIDR_AGG')} requires an {md_code('IPV4')} column"
        if operand_kind == self.INTEGER:
            rest = (
                f"A plain integer column is refused because folding arbitrary integers "
                f"into network ranges produces a well-formed, confident, wrong answer. "
                f"Use {column} to cast"
            )
        elif operand_kind == self.TEXT:
            rest = f"Use {column} to cast"
        else:
            rest = (
                f"There is no cast from {md_code(type_name) if type_name else 'this type'} "
                f"to {md_code('IPV4')} - only text and integer columns can hold addresses"
            )
        super().__init__(compose(requires, rest))


class IncompatibleTypesError(Exception):
    """
    Raised when attempting to join fields of incompatible types.

    Parameters:
        left_type: str
            The type of the left field.
        right_type: str
            The type of the right field.
        column: Optional[str]
            If the incompatibility occurs in a single column
        left_column: Optional[str]
            The column name where the error occurs.
        right_columns: Optional[str]
            The column name where the error occurs.

    Attributes:
        left_type (str): The type of the left field.
        right_type (str): The type of the right field.
        column (str): The column name where the error occurs.
        left_column (str): The column name where the error occurs.
        right_column (str): The column name where the error occurs.
    """

    def __init__(
        self,
        left_type: str = None,
        right_type: str = None,
        column: Optional[str] = None,
        left_column: Optional[str] = None,
        right_column: Optional[str] = None,
        left_node: Optional[Any] = None,
        right_node: Optional[Any] = None,
        message: Optional[str] = None,
    ):
        def _format_col(_type, _node, _name):
            if _node.node_type == 42:
                return f"literal {md_code(_node.value)} ({md_code(_type)})"
            if _node.node_type == 38:
                return f"column {md_column(_name)} ({md_code(_type)})"
            return md_column(_name)

        self.left_type = left_type
        self.right_type = right_type
        self.column = column
        self.left_column = left_column
        self.right_column = right_column
        if message:
            super().__init__(message)
        elif self.column:
            super().__init__(
                compose(
                    f"Column {md_column(column)} has incompatible types on each side: "
                    f"{md_code(left_type)} and {md_code(right_type)}",
                    f"Cast one side to match the other with "
                    f"{md_code('CAST(column AS type)')}",
                )
            )
        elif self.left_column or self.right_column:
            super().__init__(
                compose(
                    f"{_format_col(left_type, left_node, left_column)} and "
                    f"{_format_col(right_type, right_node, right_column)} cannot be "
                    f"compared, because their types do not match",
                    f"Cast one side to match the other with "
                    f"{md_code('CAST(column AS type)')}",
                )
            )
        else:
            super().__init__(
                compose(
                    "These column types cannot be compared",
                    f"Cast one side to match the other with "
                    f"{md_code('CAST(column AS type)')}",
                )
            )


class ArrayWithMixedTypesError(SqlError):
    """Exception raised when arrays have mixed types."""


class PermissionsError(SecurityError):
    """Exception raised for permissions errors."""


class EgressRestrictedError(SecurityError):
    """Raised when a write would copy a workspace's data into a different one
    and the source workspace has `egress_protection` on.

    Deliberately not a `PermissionsError`: the user may hold every permission
    the statement needs. What is refused is the *destination* - the data may
    not leave its workspace by this route - so reporting it as a missing grant
    would send people to fix the wrong thing. Clearing it is an
    `ALTER WORKSPACE <source> SET egress_protection TO OFF` by that workspace's
    owner, and the message says so.
    """


# ======================== End SQL-Specific Exceptions ==========================


# ======================== Begin Miscellaneous Database Errors ========================
class UnsupportedTypeError(DatabaseError):
    """Exception raised when an unsupported type is encountered."""


class UnmetRequirementError(Exception):
    """Exception raised when a requirement for operation is not met."""


class NotSupportedError(DatabaseError):
    """Exception raised when an unsupported operation is attempted."""


class UnsupportedFileTypeError(DatabaseError):
    """Exception raised when an unsupported file type is encountered."""


class MissingSqlStatement(ProgrammingError):
    """Exception raised for missing SQL statement."""


class InconsistentSchemaError(DataError):
    """Raised when, despite efforts, we can't get a consistent schema."""


class DatasetReadError(DataError):
    """Raised when we can't read the data we're pretty sure is there"""


class EmptyDatasetError(DataError):
    """Exception raised when a dataset is empty."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        super().__init__(
            compose(
                f"Dataset {md_table(dataset)} was found, but it has no valid partition to "
                f"read",
                f"If the query is time-travelling, the range may fall outside the data "
                f"that has been committed",
            )
        )


class UnnamedColumnError(SqlError):
    """Exception raised for unnamed columns."""


class ConcurrentModificationError(DatabaseError):
    """Raised when another writer committed to a relation while this statement was
    building its own commit.

    Every commit is built against a snapshot it read, and is published by moving
    one pointer. If another writer moved that pointer first, publishing anyway
    would drop their work: this statement's manifest was built from a parent
    that no longer describes the relation. The store refuses the write, so
    NOTHING was published - the data files this statement wrote are orphans the
    reclamation sweeps collect.

    Deliberately NOT retried automatically. Whether the work survives the race
    depends on what won it: a winner that appended leaves this statement's
    assumptions intact, but a winner that compacted has moved rows between
    files, which invalidates any row addresses computed against the old manifest
    - the case MERGE depends on. Rather than encode that judgement in the
    engine, the statement fails and the caller re-runs it, which rebuilds
    against whatever is current. A caller re-running is always correct; the
    engine guessing is correct only sometimes.
    """

    def __init__(self, relation: str, message: Optional[str] = None):
        self.relation = relation
        super().__init__(
            compose(
                message
                or (
                    f"Another writer committed to {md_code(relation)} while this "
                    "statement was preparing its own commit, so it was refused. "
                    "Nothing was written."
                ),
                "Re-run the statement - it will be rebuilt against the current "
                "state of the relation.",
            )
        )


# ======================== End Miscellaneous Database Errors ==========================


# ======================== Begin Configuration & Internal Errors ========================
class InvalidConfigurationError(DatabaseError):
    """Exception raised for invalid configuration."""

    def __init__(
        self,
        *,
        config_item: str,
        provided_value: str,
        valid_value_description: str = None,
    ):
        DISPLAY_LIMIT: int = 32

        self.config_item = config_item
        self.provided_value = provided_value
        self.valid_value_description = valid_value_description

        shown = str(provided_value)[:DISPLAY_LIMIT]
        if len(provided_value) > DISPLAY_LIMIT:
            shown += "..."
        super().__init__(
            compose(
                f"{md_code(shown)} cannot be used as the value of "
                f"{md_column(config_item)}",
                f"The value should be {valid_value_description}"
                if valid_value_description
                else None,
            )
        )


class InvalidInternalStateError(DatabaseError):
    """Exception raised for invalid internal states."""


class InvalidCursorStateError(ProgrammingError):
    """Exception raised for invalid cursor states."""


class ParameterError(ProgrammingError):
    """Exception raised for parameter errors."""


# ======================== End Configuration & Internal Errors ==========================
