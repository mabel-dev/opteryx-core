# isort: skip_file
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Opteryx is a SQL query engine optimized for speed and efficiency.

To get started:
    import opteryx
    results = opteryx.session().execute("SELECT * FROM my_table")

Opteryx handles parsing, planning, and execution of SQL queries with a focus on low-latency
analytics over local or remote data sources.

For more information check out https://opteryx.app.
"""

import datetime
import os
import platform
import secrets
import warnings
from pathlib import Path

from decimal import getcontext
from typing import Dict, Any, Iterable, Optional


# Set Decimal precision to 28 globally
getcontext().prec = 28


# end-of-stream marker
def _generate_eos_marker() -> int:
    """Generate a random 64-bit signed end-of-stream marker."""
    return secrets.randbits(64) - (1 << 63)


EOS: int = _generate_eos_marker()


def is_mac() -> bool:  # pragma: no cover
    """
    Check if the current platform is macOS.

    Returns:
        bool: True if the platform is macOS, False otherwise.
    """
    return platform.system().lower() == "darwin"


# we do a separate check for debug mode here so we don't load the config module just yet
OPTERYX_DEBUG = os.environ.get("OPTERYX_DEBUG") is not None


# python-dotenv allows us to create an environment file to store secrets.
# Only try to import dotenv if a .env file exists to avoid paying the
# import cost when no environment file is present.
_env_path = Path.cwd() / ".env"
if _env_path.exists():
    try:
        import dotenv  # type:ignore

        dotenv.load_dotenv(dotenv_path=_env_path)
        if OPTERYX_DEBUG:
            print(f"{datetime.datetime.now()} [LOADER] Loading `.env` file.")
    except ImportError:  # pragma: no cover
        # dotenv is optional; if it's not installed, just continue.
        pass


if OPTERYX_DEBUG:  # pragma: no cover
    from opteryx.debugging import OpteryxOrsoImportFinder

from opteryx.connectors import register_workspace
from opteryx.connectors import set_default_connector

from opteryx.__version__ import __author__
from opteryx.__version__ import __build__
from opteryx.__version__ import __version__
from opteryx.__version__ import __lib__


def session(
    *,
    user: Optional[str] = None,
    memberships: Optional[Iterable[str]] = None,
    schema: Optional[str] = None,
    access_policies: Optional[Iterable[str]] = None,
    query_id: Optional[str] = None,
) -> "Session":
    """
    Create and return a new `Session` object (the canonical execution object).

    Example:
        session = opteryx.session(user="alice", memberships=["opteryx"])
        session.execute("SELECT 1")
    """
    from opteryx.query_session import Session

    return Session(
        user=user,
        memberships=memberships,
        schema=schema,
        access_policies=access_policies,
        query_id=query_id,
    )


def analyze_query(sql: str) -> Dict[str, Any]:
    """
    Parse a SQL query and extract metadata without executing it.

    This function analyzes the SQL query structure to extract information such as:
    - Query type (SELECT, INSERT, UPDATE, DELETE, etc.)
    - Tables being queried
    - Other metadata available from the SQL syntax alone

    This is useful for:
    - Pre-flight permission checks
    - Query validation before queueing
    - Resource planning
    - Query analysis

    Parameters:
        sql: SQL query string to parse

    Returns:
        Dictionary containing:
        - query_type: Type of query (e.g., "Query", "Insert", "Update")
        - tables: List of table names referenced in the query
        - is_select: True if this is a SELECT query
        - is_mutation: True if this modifies data (INSERT, UPDATE, DELETE)

    Example:
        >>> info = opteryx.parse_query_info("SELECT * FROM users WHERE id = 1")
        >>> print(info["query_type"])
        'Query'
        >>> print(info["tables"])
        ['users']
    """
    from opteryx.utils.query_parser import parse_query_info as _parse_query_info

    return _parse_query_info(sql)


# Enable all warnings, including DeprecationWarning
warnings.simplefilter("once", DeprecationWarning)

__all__ = [
    "analyze_query",
    "session",
    "Session",
    "register_workspace",
    "set_default_connector",
    "__author__",
    "__build__",
    "__version__",
    "__lib__",
    "OPTERYX_DEBUG",
]
