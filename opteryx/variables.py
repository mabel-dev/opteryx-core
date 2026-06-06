# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""

Owner meanings:
    SERVER - can only be set at the server level at start up (set in config)
    INTERNAL - the system can update this as it runs (defaulted in config)
    USER - the user can update this value (defaulted in config)

For variables we're creating and naming, use sensible defaults and if it's a
feature flag, name the variable for the state the user probably doesn't want -
e.g. disable_optimizer (default to False)
"""

from enum import Enum
from typing import Any, Dict, Tuple, Type

from opteryx import config
from opteryx.__version__ import __version__
from opteryx.compiled.simd_probe import cpu_architecture
from opteryx.constants.character_set import CharacterSet, Collation
from opteryx.exceptions import PermissionsError, VariableNotFoundError
from opteryx.types import SqlType


class VariableOwner(int, Enum):
    # Manually assign numbers because USER < INTERNAL < SERVER
    SERVER = 30  # set on the server, fixed per instantiation
    INTERNAL = 20  # set by the system, can be updated by the system
    USER = 10  # set by the user, can be updated by the user


class Visibility(str, Enum):
    RESTRICTED = "restricted"  # only visible to the server
    UNRESTRICTED = "unrestricted"  # visible to all users


VariableSchema = Tuple[Type, Any, VariableOwner, Visibility]

# fmt: off
SYSTEM_VARIABLES_DEFAULTS: Dict[str, VariableSchema] = {
    # These are the MySQL set of variables - we don't use all of them but have them for compatibility
    "auto_increment_increment": (SqlType.INTEGER, 1, VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    "autocommit": (SqlType.BOOLEAN, True, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_client": (SqlType.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_connection": (SqlType.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_database": (SqlType.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_results": (SqlType.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_server": (SqlType.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_connection": (SqlType.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_database": (SqlType.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_server": (SqlType.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "external_user": (SqlType.VARCHAR, "", VariableOwner.INTERNAL, Visibility.RESTRICTED),
    "init_connect": (SqlType.VARCHAR, "", VariableOwner.SERVER, Visibility.RESTRICTED),
    "interactive_timeout": (SqlType.INTEGER, 28800, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "license": (SqlType.VARCHAR, "MIT", VariableOwner.SERVER, Visibility.RESTRICTED),
    "lower_case_table_names": (SqlType.INTEGER, 0, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_allowed_packet": (SqlType.INTEGER, 67108864, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_execution_time": (SqlType.INTEGER, 0, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "net_buffer_length": (SqlType.INTEGER, 16384, VariableOwner.SERVER, Visibility.RESTRICTED),
    "net_write_timeout": (SqlType.INTEGER, 28800, VariableOwner.SERVER, Visibility.RESTRICTED),
    "performance_schema": (SqlType.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_auto_is_null": (SqlType.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_mode": (SqlType.VARCHAR, "ANSI", VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_select_limit": (SqlType.INTEGER, None, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "system_time_zone": (SqlType.VARCHAR, "UTC", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "time_zone": (SqlType.VARCHAR, "UTC", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "transaction_read_only": (SqlType.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "transaction_isolation": (SqlType.VARCHAR, "READ-COMMITTED", VariableOwner.SERVER, Visibility.RESTRICTED),
    "version": (SqlType.VARCHAR, __version__, VariableOwner.SERVER, Visibility.RESTRICTED),
    "version_comment": (SqlType.VARCHAR, "mesos", VariableOwner.SERVER, Visibility.RESTRICTED),
    "wait_timeout": (SqlType.INTEGER, 28800, VariableOwner.SERVER, Visibility.RESTRICTED),
    "event_scheduler": (SqlType.VARCHAR, "OFF", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "default_storage_engine": (SqlType.VARCHAR, "opteryx", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "default_tmp_storage_engine": (SqlType.VARCHAR, "opteryx", VariableOwner.SERVER, Visibility.UNRESTRICTED),

    # These are Opteryx specific variables
    "disable_optimizer": (SqlType.BOOLEAN, config.DISABLE_OPTIMIZER, VariableOwner.USER, Visibility.RESTRICTED),
    "concurrent_reads": (SqlType.INTEGER, config.CONCURRENT_READS, VariableOwner.SERVER, Visibility.RESTRICTED),
    "user_memberships": (SqlType.ARRAY, [[]], VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    "morsel_size": (SqlType.INTEGER, config.MORSEL_SIZE, VariableOwner.SERVER, Visibility.RESTRICTED),
    "architecture": (SqlType.ARRAY, cpu_architecture(), VariableOwner.SERVER, Visibility.RESTRICTED),
}
# fmt: on


class SystemVariablesContainer:
    def __init__(self, owner: VariableOwner = VariableOwner.USER):
        self._variables = SYSTEM_VARIABLES_DEFAULTS.copy()
        self._owner = owner

    def __getitem__(self, key: str) -> Any:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key][1]

    def __setitem__(self, key: str, value: Any) -> None:
        if key[0] == "@":
            variable_type = value.type
            owner = VariableOwner.USER
            visibility = Visibility.UNRESTRICTED
        else:
            if key not in self._variables:
                from opteryx.utils import suggest_alternative

                suggestion = suggest_alternative(key, list(self._variables.keys()))

                raise VariableNotFoundError(variable=key, suggestion=suggestion)
            variable_type, _, owner, visibility = self._variables[key]
            if owner > self._owner:
                raise PermissionsError(f"User does not have permission to set variable `{key}`")
            if variable_type != value.type:
                raise ValueError(f"Invalid type for `{key}`, {variable_type} expected.")

        self._variables[key] = (variable_type, value.value, owner, visibility)

    def details(self, key: str) -> VariableSchema:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key]

    def __contains__(self, key: str) -> bool:
        return key in self._variables

    def __iter__(self):
        return iter(self._variables)

    def __len__(self):
        return len(self._variables)

    def snapshot(self, owner: VariableOwner = VariableOwner.USER) -> "SystemVariablesContainer":
        return SystemVariablesContainer(owner)

    def as_column(self, key: str):
        """Return a variable as a CONSTANT column"""
        from opteryx.types.schema import ConstantColumn

        # system variables aren't stored with the @@
        variable = self._variables[key[2:]] if key.startswith("@@") else self._variables.get(key)
        if not variable:
            raise VariableNotFoundError(key)
        return ConstantColumn(name=key, type=variable[0], value=variable[1])


# load the base set
SystemVariables = SystemVariablesContainer(VariableOwner.INTERNAL)
