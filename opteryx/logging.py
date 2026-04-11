# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Opteryx logging utilities.

Provides a simple logging interface for Opteryx components.
"""

import logging
from typing import Optional

__all__ = ["get_logger"]


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger instance for the given module name.

    Args:
        name: Optional logger name. If not provided, uses 'opteryx'.

    Returns:
        A Python logging.Logger instance.

    Examples:
        >>> logger = get_logger()
        >>> logger.info("Hello")
        >>> logger = get_logger(__name__)
        >>> logger.debug("Debug message")
    """
    return logging.getLogger(name or "opteryx")
```

Now I'll proceed with replacing all the imports. Let me use the edit_file tool to replace each import systematically:
