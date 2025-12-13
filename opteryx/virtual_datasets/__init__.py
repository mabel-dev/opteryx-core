# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Virtual datasets package.

This package exposes several small built-in datasets. Importing the
individual dataset modules triggers imports of heavy libraries (pyarrow,
zstandard, etc.), so we lazily load those modules on first access.
"""

_MODULES = {
    "derived": "opteryx.virtual_datasets.derived_data",
    "no_table": "opteryx.virtual_datasets.no_table_data",
    "planets": "opteryx.virtual_datasets.planet_data",
    "variables": "opteryx.virtual_datasets.variables_data",
    "telemetry": "opteryx.virtual_datasets.telemetry",
    "stop_words": "opteryx.virtual_datasets.stop_words",
    "user": "opteryx.virtual_datasets.user",
}


def __getattr__(name: str):
    """Lazily import and return submodules like `planets`, `missions`, etc.

    This allows code to reference `opteryx.virtual_datasets.planets` without
    importing pyarrow until the dataset module is actually used.
    """
    if name in _MODULES:
        import importlib

        module = importlib.import_module(_MODULES[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
