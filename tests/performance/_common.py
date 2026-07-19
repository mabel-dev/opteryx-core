"""
Shared display + comparison helpers for the performance benchmark runners.

Used by clickbench / tpch / job / h2o `runner.py`. Each runner owns its own
query loading, table-name rewriting, and execution loop — this module only
covers the cross-cutting concerns:

    - loading a DuckDB baseline JSON keyed by query name
    - colour-coded [ratio] formatting with ClickBench-equivalent thresholds
    - a benchmark-style printable row layout that scales to long query names
    - writing per-iteration rows to CSV in `<bench>/results/<sha>-<ts>.csv`

Keeping the helpers small and additive — the runners stay independent and
each preserves its own quirks (timeouts, fresh sessions, etc.).
"""

from __future__ import annotations

import csv
import datetime
import json
import os
import subprocess
import sys
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# DuckDB baseline loading
# ---------------------------------------------------------------------------


def load_duckdb_baseline(path: str) -> tuple[dict[str, float], Optional[str]]:
    """Return (name → min_ms, machine_label) for a DuckDB results JSON.

    Returns ({}, None) if the file doesn't exist or doesn't have the expected
    shape — callers fall back to "no baseline" mode.
    """
    if not os.path.exists(path):
        return {}, None
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}, None

    by_name: dict[str, float] = {}
    for entry in data.get("result", []):
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        min_ms = entry.get("min_ms")
        if name is not None and min_ms is not None:
            by_name[str(name)] = float(min_ms)
    return by_name, data.get("machine")


# ---------------------------------------------------------------------------
# Colour-coded ratio formatting
# ---------------------------------------------------------------------------


def format_ratio(opteryx_ms: float, duckdb_ms: float) -> str:
    """Coloured `[ratio]` cell. Same thresholds as the ClickBench runner.

    deep green : faster than DuckDB
    teal       : within 10%
    orange     : 10–50% slower
    red        : 50%+ slower
    """
    if duckdb_ms is None or duckdb_ms <= 0:
        return ""
    ratio = opteryx_ms / duckdb_ms
    s = f"[{ratio:.2f}x]"
    if ratio < 1.0:
        return f"\033[38;2;34;197;94m{s}\033[0m"
    if ratio <= 1.1:
        return f"\033[38;2;72;209;204m{s}\033[0m"
    if ratio <= 1.5:
        return f"\033[38;2;255;165;0m{s}\033[0m"
    return f"\033[38;2;255;69;69m{s}\033[0m"


# ---------------------------------------------------------------------------
# Bench-row table layout
# ---------------------------------------------------------------------------

# Field widths sized to fit 5-digit ms times (e.g. "17106.7ms" is 9 chars)
# plus a 1-char gap so adjacent right-aligned cells don't run together.
# All cells right-align so digits line up across rows.
_MS_W = 10
_NAME_W = 8
_BASELINE_W = 10
_RATIO_W = 9


def print_header(name_label: str, n_iterations: int, has_baseline: bool) -> int:
    """Print the table header. Returns the rule width so the runner can
    print its own bottom rule of matching length."""
    parts: list[str] = [f"{name_label:<{_NAME_W}}"]
    for i in range(1, max(n_iterations, 1) + 1):
        parts.append(f"{f'Run {i}':>{_MS_W}}")
    parts.append("  ")
    parts.append(f"{'Min':>{_MS_W}}")
    parts.append(f"{'Avg':>{_MS_W}}")
    parts.append(f"{'Max':>{_MS_W}}")
    if has_baseline:
        parts.append(f"  {'DuckDB':>{_BASELINE_W}}")
        parts.append(f"  {'vs':<{_RATIO_W}}")
    line = "".join(parts)
    width = len(line) + (6 if has_baseline else 0)
    print(line)
    print("─" * width)
    return width


def print_row(
    name: str,
    iteration_times_ms: list[float],
    n_iterations: int,
    duckdb_min_ms: Optional[float],
) -> None:
    """Print one query row matching the header layout."""
    parts: list[str] = [f"{name:<{_NAME_W}}"]
    cells = [f"{t:>{_MS_W - 2}.1f}ms" for t in iteration_times_ms]
    while len(cells) < n_iterations:
        cells.append(f"{'-':>{_MS_W}}")
    for cell in cells[:n_iterations]:
        parts.append(f"{cell:>{_MS_W}}")
    parts.append("  ")
    if iteration_times_ms:
        mn = min(iteration_times_ms)
        mx = max(iteration_times_ms)
        avg = sum(iteration_times_ms) / len(iteration_times_ms)
        parts.append(f"{f'{mn:.1f}ms':>{_MS_W}}")
        parts.append(f"{f'{avg:.1f}ms':>{_MS_W}}")
        parts.append(f"{f'{mx:.1f}ms':>{_MS_W}}")
        if duckdb_min_ms is not None:
            parts.append(f"  {f'{duckdb_min_ms:.1f}ms':>{_BASELINE_W}}")
            parts.append(f"  {format_ratio(mn, duckdb_min_ms)}")
    else:
        parts.append(f"{'-':>{_MS_W}}{'-':>{_MS_W}}{'-':>{_MS_W}}")
    print("".join(parts))


def print_error_row(name: str, error: str) -> None:
    """Single-line ERROR row, kept compact so it stands out in a long table."""
    msg = error if len(error) <= 100 else error[:97] + "..."
    print(f"{name:<{_NAME_W}}  \033[0;31mERROR\033[0m  {msg}")


def print_skip_row(name: str, reason: str) -> None:
    print(f"{name:<{_NAME_W}}  \033[38;2;128;128;128mSKIP\033[0m   {reason}")


def print_total_row(
    opteryx_total_ms: float,
    duckdb_total_ms: float,
    n_compared: int,
    n_iterations: int,
) -> None:
    """Bottom-line row aggregating the per-query mins."""
    parts: list[str] = [f"{'TOTAL':<{_NAME_W}}"]
    parts.append(f"{'':>{_MS_W * n_iterations + 2}}")
    parts.append(f"{f'{opteryx_total_ms:.1f}ms':>{_MS_W}}")
    parts.append(f"{'':>{_MS_W}}")
    parts.append(f"{'':>{_MS_W}}")
    parts.append(f"  {f'{duckdb_total_ms:.1f}ms':>{_BASELINE_W}}")
    parts.append(f"  {format_ratio(opteryx_total_ms, duckdb_total_ms)}")
    parts.append(f"  ({n_compared} compared)")
    print("".join(parts))


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------


def git_sha_short() -> str:
    """Best-effort short git sha; falls back to 'nogit' if unavailable."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        return out.decode().strip() or "nogit"
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return "nogit"


def open_results_csv(results_dir: str, fieldnames: list[str]) -> tuple[csv.DictWriter, str, "object"]:
    """Open `results_dir/<sha>-<utc_iso>.csv` for append-style writes.

    Returns (writer, path, file_handle). The caller is responsible for
    `file_handle.close()` (or just lets process exit handle it).
    """
    os.makedirs(results_dir, exist_ok=True)
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"{git_sha_short()}-{timestamp}.csv"
    path = os.path.join(results_dir, filename)
    f = open(path, "w", newline="")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    f.flush()
    return writer, path, f


# ---------------------------------------------------------------------------
# Header banner
# ---------------------------------------------------------------------------


def print_banner(
    title: str,
    opteryx_version: str,
    metadata: Iterable[tuple[str, str]],
    duckdb_machine: Optional[str] = None,
    duckdb_query_count: Optional[int] = None,
) -> None:
    """Multi-line title banner displayed before the table.

    `metadata` is a sequence of (label, value) pairs printed left-aligned.
    """
    bar = "═" * 100
    print(bar)
    print(f"  {title} — Opteryx {opteryx_version}")
    print(bar)
    for label, value in metadata:
        print(f"  {label:<14} {value}")
    if duckdb_machine is not None:
        if duckdb_query_count is not None:
            print(
                f"  {'Baseline':<14} DuckDB on {duckdb_machine} "
                f"({duckdb_query_count} queries)"
            )
        else:
            print(f"  {'Baseline':<14} DuckDB on {duckdb_machine}")
    else:
        print(f"  {'Baseline':<14} (no DuckDB baseline available)")
    print(bar)
    print()
