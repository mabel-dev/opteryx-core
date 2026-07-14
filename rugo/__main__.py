# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Enables `python -m rugo <verb> ...` — the same CLI as the installed `rugo`
console script, for use without a pip-installed wheel (e.g. from a source tree
or a notebook where `rugo` is importable but not on PATH).
"""

import sys

from rugo.cli import main

if __name__ == "__main__":
    sys.exit(main())
