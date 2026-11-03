"""The running statement's cancel request, for the plan-time work before the native
engine starts.

The engine checks a statement's CancelToken per morsel once it runs, but compiling the
plan comes first - and on a cold scan of many files that is the slow part (footer
fetches, per-file row-group pruning: minutes on a large table). `execute` installs the
session's token here for that phase; the compiler and scan planning call
`raise_if_cancelled` at their checkpoints.

A context variable rather than a parameter: the checkpoints sit several layers below
`compile_to_native` (scan planning in the parquet reader), and threading the token
through every signature between them would touch every scan's plumbing for a flag read.
It is per thread, and the compile runs on the thread that called `execute`.
"""

from contextlib import contextmanager
from contextvars import ContextVar

_current = ContextVar("opteryx_cancel_token", default=None)


@contextmanager
def cancellable(token):
    """Make `token` the cancel request `raise_if_cancelled` answers to inside the block."""
    reset = _current.set(token)
    try:
        yield
    finally:
        _current.reset(reset)


def raise_if_cancelled() -> None:
    """Raise QueryCancelledError if the statement compiling on this thread was cancelled.
    A no-op outside `cancellable` (EXPLAIN, tests calling the planners directly)."""
    token = _current.get()
    if token is not None and token.cancelled:
        from opteryx.exceptions import QueryCancelledError

        raise QueryCancelledError("The query was cancelled.")
