"""Expose the version metadata from the opteryx package for the
opteryx_core compatibility shim.
"""
from opteryx.__version__ import __author__  # type: ignore
from opteryx.__version__ import __build__
from opteryx.__version__ import __version__

__all__ = ["__version__", "__author__", "__build__"]
