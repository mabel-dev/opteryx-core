# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
IO Waterfall Visualization Tools

This package provides tools for reading, analyzing, and visualizing
IO trace files from Opteryx queries. It includes:

- TraceReader: Parse JSONLines trace files
- generate_waterfall_html: Generate interactive HTML waterfall charts
- CLI interface: Command-line tool for trace analysis

Usage:
    # Generate chart from trace file
    python -m opteryx.tools.io_waterfall trace /path/to/trace.jsonl

    # View statistics
    python -m opteryx.tools.io_waterfall stats /path/to/trace.jsonl

    # Programmatic access
    from opteryx.tools.io_waterfall import TraceReader, generate_waterfall_html
    reader = TraceReader("trace.jsonl")
    html_path = generate_waterfall_html("trace.jsonl")
"""

from opteryx.tools.io_waterfall.generator import generate_waterfall_html
from opteryx.tools.io_waterfall.reader import TraceReader

__all__ = ["TraceReader", "generate_waterfall_html"]
