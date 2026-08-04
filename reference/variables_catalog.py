"""Helpers for exporting a generated system-variable catalog.

The catalog is the source for the published settings reference. It records, for
every system variable, what it is and **who may set it** — a question
`SHOW VARIABLES` can only answer for the variables a given caller can already
see, and which was not documented anywhere.

**No runtime value is ever emitted for a variable that is configured from the
environment.** `opteryx/config.py` reads each of its values as
`environ.get(KEY, <shipped default>)`, and importing opteryx loads a `.env` from
the working directory — so reading the live value at generation time records the
generating machine's configuration. On a developer machine that means real
project IDs and key prefixes would be written into a checked-in file. Those
variables are resolved to the environment key that sets them instead, by static
analysis of the two source files, which cannot leak a value.

Host- and build-derived defaults (`cpu_count`, `version`, ...) are likewise
recorded by provenance rather than by value, so the catalog describes the
product rather than the machine that generated it.
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any

# `"name": (TYPE, config.CONSTANT, ...` — links a system variable to the config
# constant supplying its default.
_VARIABLE_TO_CONFIG = re.compile(r'"([a-z0-9_]+)":\s*\([^,]+,\s*config\.([A-Z0-9_]+)')

# config's OWN get()/get_bool() helpers, never `environ.get(...)` — matching the
# latter let a statement slice pick up an unrelated feature-flag key as a
# variable's configuration source.
_CONFIG_ENV_KEY = re.compile(r'(?<![.\w])get(?:_bool|_int|_float)?\("([A-Z0-9_]+)"')

# Start of a top-level config constant assignment, e.g. `PARQUET_IO_WORKERS: int =`.
# Used to slice the source into per-constant statements, because the `get("KEY")`
# call is frequently on a continuation line rather than the assignment line.
_CONFIG_ASSIGNMENT = re.compile(r"^([A-Z0-9_]+)\s*:[^=\n]*=", re.MULTILINE)

# Defaults computed from the host or the build at import time.
_ENVIRONMENT_DERIVED = {
    "architecture": "host",
    "cpu_count": "host",
    "memory_limit_bytes": "host",
    "operating_system": "host",
    "physical_memory_bytes": "host",
    "python_version": "host",
    "version": "build",
}

# Per-session identity stamped by ExecutionContext, not configuration. Listed so
# the catalog is complete; they are not settings and have no default.
_SESSION_IDENTITY = {
    "access_policies",
    "billing_account",
    "external_user",
    "user_entitlements",
    "user_memberships",
}


def _source_of(module) -> str:
    return Path(module.__file__).read_text(encoding="utf8")


def _variable_environment_keys() -> dict[str, str]:
    """Map each system variable to the environment key configuring it.

    Static analysis of the sources, never the live environment.
    """
    import opteryx.config as config_module
    import opteryx.variables as variables_module

    config_source = _source_of(config_module)
    config_keys = set(_CONFIG_ENV_KEY.findall(config_source))

    # Slice the source per constant: a constant's statement runs until the next
    # top-level assignment, so a `get("KEY")` on a continuation line is still found.
    matches = list(_CONFIG_ASSIGNMENT.finditer(config_source))
    config_to_env: dict[str, str] = {}
    for index, match in enumerate(matches):
        constant = match.group(1)
        end = matches[index + 1].start() if index + 1 < len(matches) else len(config_source)
        found = _CONFIG_ENV_KEY.search(config_source, match.end(), end)
        if found is not None:
            config_to_env[constant] = found.group(1)
        elif constant in config_keys:
            # Assigned from an intermediate (e.g. MAX_EXECUTION_WORKERS, whose
            # get() is on an earlier helper line) but the key shares its name.
            config_to_env[constant] = constant

    mapping: dict[str, str] = {}
    for variable, constant in _VARIABLE_TO_CONFIG.findall(_source_of(variables_module)):
        env_key = config_to_env.get(constant)
        if env_key is not None:
            mapping[variable] = env_key
    return mapping


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def export_variables_catalog() -> OrderedDict[str, dict[str, Any]]:
    from opteryx.variables import PLATFORM_ADMIN_ENTITLEMENT
    from opteryx.variables import SYSTEM_VARIABLES_DEFAULTS
    from opteryx.variables import VariableOwner
    from opteryx.variables import Visibility

    env_keys = _variable_environment_keys()
    exported: OrderedDict[str, dict[str, Any]] = OrderedDict()

    for name in sorted(SYSTEM_VARIABLES_DEFAULTS):
        variable_type, default, owner, visibility = SYSTEM_VARIABLES_DEFAULTS[name]

        # A session's container is created at the USER tier, so only USER-owned
        # variables are reachable by `SET`; INTERNAL and SERVER outrank it.
        settable = owner == VariableOwner.USER
        # RESTRICTED gates writes as well as reads, independently of tier.
        needs_entitlement = settable and visibility == Visibility.RESTRICTED

        if needs_entitlement:
            set_by = f"{PLATFORM_ADMIN_ENTITLEMENT} only"
        elif settable:
            set_by = "any session"
        else:
            set_by = "not settable via SQL"

        entry: dict[str, Any] = {
            "name": name,
            "type": str(variable_type),
            "owner": owner.name,
            "visibility": visibility.name,
            "settable": settable,
            "requires_entitlement": PLATFORM_ADMIN_ENTITLEMENT if needs_entitlement else None,
            "set_by": set_by,
            "default": None,
        }

        if name in _ENVIRONMENT_DERIVED:
            entry["default_source"] = _ENVIRONMENT_DERIVED[name]
        elif name in _SESSION_IDENTITY:
            entry["default_source"] = "session"
        elif name in env_keys:
            entry["default_source"] = "environment"
            entry["environment_key"] = env_keys[name]
        else:
            entry["default_source"] = "literal"
            entry["default"] = _json_safe(default)

        exported[name] = entry

    _assert_no_environment_values(exported)
    return exported


def _assert_no_environment_values(exported: dict[str, dict[str, Any]]) -> None:
    """No variable may publish a literal it could have picked up from the shell.

    Fails loud rather than shipping a catalog that silently records the
    generating machine — the diff for that looks like an ordinary value change.
    """
    import opteryx.config as config_module

    config_keys = set(_CONFIG_ENV_KEY.findall(_source_of(config_module)))
    leaked = [
        name
        for name, entry in exported.items()
        if entry["default_source"] == "literal" and name.upper() in config_keys
    ]
    if leaked:
        raise RuntimeError(
            "Refusing to generate the variable catalog: "
            + ", ".join(sorted(leaked))
            + " publish a literal default but are environment-configurable. Map them "
            "in _variable_environment_keys() so the key is recorded instead of a value."
        )


def write_variables_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_variables_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
