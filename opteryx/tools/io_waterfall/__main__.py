# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
CLI tool for generating waterfall charts from IO traces.

Usage:
    python -m opteryx.tools.io_waterfall <trace_file> [--output OUTPUT]
    python -m opteryx.tools.io_waterfall stats <trace_file>
"""

import argparse
import sys
from pathlib import Path

from opteryx.tools.io_waterfall.generator import generate_waterfall_html
from opteryx.tools.io_waterfall.reader import TraceReader


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Generate waterfall charts from IO trace files",
        prog="python -m opteryx.tools.io_waterfall",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Trace command (default)
    trace_parser = subparsers.add_parser(
        "trace", help="Generate waterfall HTML chart from trace file"
    )
    trace_parser.add_argument("trace_file", help="Path to .jsonl trace file")
    trace_parser.add_argument(
        "--output", "-o", help="Output HTML file path (default: trace_file.html)"
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Print statistics from trace file")
    stats_parser.add_argument("trace_file", help="Path to .jsonl trace file")

    # Handle positional argument without subcommand (for convenience)
    parser.add_argument(
        "trace_file_pos",
        nargs="?",
        help=argparse.SUPPRESS,  # Hidden positional argument
    )
    parser.add_argument(
        "--output",
        "-o",
        help=argparse.SUPPRESS,  # Hidden when using subcommands
    )

    args = parser.parse_args()

    # Handle case where user doesn't specify subcommand
    trace_file = args.trace_file if hasattr(args, "trace_file") else args.trace_file_pos

    if not trace_file:
        parser.print_help()
        return 1

    if not Path(trace_file).exists():
        print(f"Error: Trace file not found: {trace_file}")
        return 1

    command = args.command or "trace"

    try:
        if command == "stats":
            cmd_stats(trace_file)
        else:
            cmd_trace(trace_file, args.output if hasattr(args, "output") else None)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


def cmd_trace(trace_file: str, output_file: str = None) -> None:
    """Generate waterfall HTML from trace file."""
    output = generate_waterfall_html(trace_file, output_file)
    print(f"✓ Generated waterfall chart: {output}")
    print(f"  Open in browser: {Path(output).absolute()}")


def cmd_stats(trace_file: str) -> None:
    """Print statistics from trace file."""
    reader = TraceReader(trace_file)
    stats = reader.statistics()

    print("\nIO Trace Statistics")
    print("=" * 50)
    print()
    print(f"  Total Files:             {stats['total_files']}")
    print(f"  Total Data:              {_format_bytes(stats['total_bytes'])}")
    print(f"  Total Rows:              {stats['total_rows']:,}")
    print()
    print(f"  Query Duration:          {_format_ms(stats['query_duration_ms'])}")
    print(f"  Download Phase:          {_format_ms(stats['download_phase_duration_ms'])}")
    print(f"  Decode Phase:            {_format_ms(stats['decode_phase_duration_ms'])}")
    print()
    print(f"  Avg Download/File:       {_format_ms(stats['avg_download_time_ms'])}")
    print(f"  Avg Decode/File:         {_format_ms(stats['avg_decode_time_ms'])}")
    print(f"  Max Concurrent Downloads: {stats['max_concurrent_downloads']}")
    print()


def _format_bytes(b: int) -> str:
    """Format bytes as human-readable."""
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _format_ms(ms: float) -> str:
    """Format milliseconds as human-readable."""
    if ms < 1000:
        return f"{ms:.0f} ms"
    return f"{ms / 1000:.2f} s"


if __name__ == "__main__":
    sys.exit(main())
