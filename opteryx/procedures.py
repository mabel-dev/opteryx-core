# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Procedure registry - the host application's hook for exposing a Python callable to SQL.

A PROCEDURE IS NOT A FUNCTION, and the distinction is the whole reason this module
exists rather than the host registering into `FunctionCatalog`.

A function is an EXPRESSION. The optimizer is licensed to fold it, duplicate it,
reorder it, push it and elide it, and the engine evaluates it once per row per morsel.
For a side-effecting callable every one of those licences is a defect: the number of
times the side effect happens becomes a function of cardinality, morsel size and which
optimizer strategies fired. Constant folding is the sharpest edge - it calls a kernel
on constant arguments at PLAN time, so `platform.notify_self('subject', 'body')`
registered as a function would send its notification during optimization, before
execution, possibly on a plan that is then discarded.

A procedure is a STATEMENT. `CALL <name>(<literals>)` runs exactly once, is reached by
no optimizer strategy, and executes on the non-tabular statement path alongside DDL -
never on the native per-morsel data path, so it does not put Python on the execution
path this engine keeps clear of it.

Registration is a HOST-PROCESS API and deliberately NOT SQL DDL. `register_procedure`
installs arbitrary Python into the engine's process; reaching that through SQL would
make arbitrary code execution a query surface.

NAMESPACES ARE CONVENTION. The `platform.` in `platform.notify_self` is part of the
name and nothing else: the registry stores the dotted name it is given, no prefix is
reserved, and no prefix carries authorization. A host that wants `platform.` to mean
something has to enforce that itself, on the registering side.
"""

import re
from dataclasses import dataclass
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

__all__ = [
    "ProcedureContext",
    "ProcedureDefinition",
    "register_procedure",
    "get_procedure",
]

#: The names `CALL` can actually reach. The parser builds a procedure name by joining
#: dotted identifier parts, so a registered name that is not of this shape could never
#: be called - it is refused at registration rather than becoming an entry nothing can
#: address.
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$")


@dataclass(frozen=True)
class ProcedureContext:
    """Who is running this statement, and under what query.

    Passed to EVERY handler as its first argument. A procedure like
    `platform.notify_self` has to address the caller, and the registry is
    process-global - one registration serves every session in a multi-tenant worker -
    so a closure captured at registration time cannot possibly know. Reading it from
    ambient state (a contextvar) would work and is worse: the engine would not know
    the identity was being used, and a handler invoked outside a request would
    silently see nothing instead of failing.

    Every field can be empty. An unauthenticated or embedded session carries no
    external user, and `user` is passed through as `None` rather than substituted -
    a handler that requires an identity must refuse, not invent one.
    """

    user: Optional[str]
    billing_account: Optional[str]
    query_id: Optional[str]


@dataclass(frozen=True)
class ProcedureDefinition:
    """A registered procedure.

    `name` is the canonical UPPER CASE form; SQL identifiers are not case sensitive in
    this engine, so lookup is on the folded name and the case the host registered is
    not preserved.

    `parameters` names the positional parameters DECLARED IN SQL. It fixes the arity
    `CALL` checks at plan time, so a mis-called procedure fails before anything runs.
    It does NOT declare types: the engine passes the literal values through as they
    were written and the handler is the only thing that validates them. It also does
    not count the context - the handler takes one more argument than `CALL` writes.
    """

    name: str
    handler: Callable
    parameters: Tuple[str, ...]


_PROCEDURES: Dict[str, ProcedureDefinition] = {}


def register_procedure(name: str, handler: Callable, *, parameters: Tuple[str, ...]) -> None:
    """Expose `handler` to SQL as `CALL <name>(...)`.

    Called by the host process at import time. `parameters` names the positional
    parameters written in SQL; the handler is invoked as

        handler(context, *values)

    where `context` is a `ProcedureContext` naming the caller and `values` are the
    literal values from the statement, in order. EVERY handler takes the context,
    whether it uses it or not: one signature, and no ambient state to read.

    A procedure signals FAILURE BY RAISING. There is no success return value to inspect:
    a handler that returned False to mean "it did not work" would let a failed statement
    report SQL_SUCCESS, which is the fake-green outcome this engine refuses. The
    exception propagates and the statement fails.

    Re-registering a name raises. A host that reloads its own registrations would
    otherwise silently install whichever copy imported last.
    """
    if not isinstance(name, str) or not _NAME.match(name):
        raise ValueError(
            f"Procedure name {name!r} is not a callable name. A procedure is named by "
            "dotted identifiers, as in 'platform.notify_self'."
        )
    if not callable(handler):
        raise ValueError(f"Procedure '{name}' was registered with a non-callable handler.")

    canonical = name.upper()
    if canonical in _PROCEDURES:
        raise ValueError(f"Procedure '{canonical}' already registered.")

    _PROCEDURES[canonical] = ProcedureDefinition(
        name=canonical, handler=handler, parameters=tuple(parameters)
    )


def get_procedure(name: str) -> Optional[ProcedureDefinition]:
    """The procedure registered under `name`, or None. `name` is folded to upper case."""
    return _PROCEDURES.get(name.upper())
