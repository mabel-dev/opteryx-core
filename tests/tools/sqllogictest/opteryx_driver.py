#!/usr/bin/env python3
"""External-engine driver for sqllogictest-rs that runs queries through Opteryx.

Wire it up via:

    sqllogictest --engine external \
        --external-engine-command-template "python3 examples/opteryx/opteryx_driver.py" \
        path/to/tests/*.slt

Protocol (see sqllogictest-engines/src/external.rs):
  stdin  : stream of {"sql": "..."} JSON values
  stdout : stream of {"result": [["c1","c2"], ...]} or {"err": "..."} JSON values

One subprocess is spawned per .slt file; a single Opteryx Session is kept alive
for the lifetime of the process so any session-local state (e.g. variables set
via `let`) persists across queries within a file.
"""

from __future__ import annotations

import decimal
import json
import math
import os
import sys
from typing import Any, Iterable

# Opteryx resolves connectors like `testdata.satellites` against the current
# working directory. Resolve the repo root from either OPTERYX_HOME or this
# file's location, then use that for both imports and connector-relative paths.
_REPO_ROOT = os.environ.get("OPTERYX_HOME")
if _REPO_ROOT is None:
    _REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
else:
    _REPO_ROOT = os.path.abspath(_REPO_ROOT)

if _REPO_ROOT not in sys.path:
    sys.path.insert(1, _REPO_ROOT)
os.chdir(_REPO_ROOT)

# Per-process scratch workspace named ``ws`` so CTAS tests have somewhere to
# write. Each ``.slt`` file gets its own subprocess and therefore its own
# clean directory; cleanup happens when the process exits.
import atexit  # noqa: E402
import shutil  # noqa: E402
import tempfile  # noqa: E402

import opteryx  # noqa: E402  — must come after chdir
from opteryx.connectors import register_workspace  # noqa: E402

_WS_ROOT = tempfile.mkdtemp(prefix="opteryx-slt-ws-")
atexit.register(lambda: shutil.rmtree(_WS_ROOT, ignore_errors=True))


def _create_ws_connector(**kwargs):
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    return LocalStoreConnector(store_root=_WS_ROOT, **kwargs)


register_workspace("ws", _create_ws_connector)


def _format_cell(value: Any) -> str:
    """Format a single result cell per sqllogictest conventions.

    sqllogictest expects every cell as a string. NULL becomes "NULL", empty
    strings become "(empty)", floats are rendered with 3 decimals, and bytes
    are decoded as UTF-8 (Opteryx returns string columns as bytes).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value if value else "(empty)"
    if isinstance(value, decimal.Decimal):
        return f"{float(value):.3f}"
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        return f"{value:.3f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_format_cell(v) for v in value) + "]"
    return str(value)


def _rows_from_morsels(morsels: Iterable[Any]) -> list[list[str]]:
    rows: list[list[str]] = []
    for morsel in morsels:
        if morsel is None:
            continue
        try:
            table = morsel.to_arrow()
        except AttributeError:
            continue
        for record in table.to_pylist():
            rows.append([_format_cell(record[c]) for c in table.column_names])
    return rows


def _iter_json_values(stream):
    """Yield JSON values from a binary stream.

    The runner writes JSON values back-to-back without delimiters, so we feed
    bytes to ``raw_decode`` until it produces a value, then advance. ``read1``
    avoids blocking once a complete value is already available.
    """
    decoder = json.JSONDecoder()
    buffer = ""
    while True:
        chunk = stream.read1(4096)
        if not chunk:
            if buffer.strip():
                # Trailing garbage — surface it so the caller notices.
                raise ValueError(f"unparsed input remained: {buffer!r}")
            break
        buffer += chunk.decode("utf-8")
        while True:
            stripped = buffer.lstrip()
            if not stripped:
                buffer = ""
                break
            try:
                value, end = decoder.raw_decode(stripped)
            except json.JSONDecodeError:
                buffer = stripped
                break
            buffer = stripped[end:]
            yield value


def _new_session():
    # Membership "Apollo 11" unlocks visibility-filtered datasets like
    # ``testdata.astronauts``; matches what the Python shape harness uses.
    return opteryx.session(memberships=["Apollo 11", "opteryx"])


# A statement whose body is exactly this marker tells the driver to discard
# the current session and start a fresh one. Used by the shape-test converter
# at crash boundaries so the slt sees the same per-session state the
# validator did.
SESSION_RESET_MARKER = "/* @@opteryx-driver: reset-session */"


def main() -> int:
    session = _new_session()
    out = sys.stdout

    for message in _iter_json_values(sys.stdin.buffer):
        sql = message.get("sql", "") if isinstance(message, dict) else ""
        stripped = sql.strip()
        # Driver-level directives.
        if stripped == SESSION_RESET_MARKER:
            session = _new_session()
            response = {"result": []}
            out.write(json.dumps(response))
            out.write("\n")
            out.flush()
            continue
        # The CLI emits per-file ``CREATE DATABASE`` / ``DROP DATABASE``
        # statements for postgres/mysql multi-tenancy. Opteryx has no such
        # concept; treat both as no-ops.
        upper = stripped.upper()
        if upper.startswith("DROP DATABASE") or upper.startswith("CREATE DATABASE"):
            response = {"result": []}
        else:
            try:
                morsels = list(session.execute_to_morsels(sql))
                response = {"result": _rows_from_morsels(morsels)}
            except Exception as exc:  # noqa: BLE001 — we report any engine error back.
                response = {"err": f"{type(exc).__name__}: {exc}"}
        out.write(json.dumps(response))
        out.write("\n")
        out.flush()

    return 0


if __name__ == "__main__":
    sys.exit(main())
