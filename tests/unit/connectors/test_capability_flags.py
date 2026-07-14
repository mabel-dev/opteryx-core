"""
A connector that inherits a capability mixin must actually advertise that capability.

BaseConnector defines every capability flag as a False default. A connector declared
`class C(BaseConnector, ..., Eidetic)` therefore resolves `C.eidetic` to False via the
MRO — the mixin is shadowed and the capability is silently OFF, with no error anywhere.
That is exactly what happened to LocalStoreConnector: it implemented get_view/create_view
in full, CREATE VIEW wrote a view.json to disk, and every attempt to read the view back
reported the relation as a missing dataset, because resolve_relation never even looked.

Capability mixins must be listed BEFORE BaseConnector in the bases.
"""

import inspect
import os
import pkgutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx.connectors as connectors_package
from opteryx.connectors.base.base_connector import BaseConnector


def _capability_flags():
    """Every bool class attribute BaseConnector defaults — these are the capability flags."""
    return [name for name, value in vars(BaseConnector).items() if isinstance(value, bool)]


def _connector_classes():
    for module_info in pkgutil.iter_modules(connectors_package.__path__):
        module = __import__(
            f"opteryx.connectors.{module_info.name}", fromlist=["_"]
        )  # noqa: F401
        for _name, obj in vars(module).items():
            if (
                inspect.isclass(obj)
                and issubclass(obj, BaseConnector)
                and obj is not BaseConnector
                and obj.__module__ == module.__name__
            ):
                yield obj


def test_capability_mixins_are_not_shadowed():
    """If a connector inherits a mixin that turns a flag on, the flag must BE on."""
    flags = _capability_flags()
    assert flags, "expected BaseConnector to define capability flags"

    shadowed = []
    for connector in _connector_classes():
        for flag in flags:
            declares_true = any(
                vars(base).get(flag) is True for base in connector.__mro__[1:]
            )
            if declares_true and getattr(connector, flag) is False:
                mro = " -> ".join(c.__name__ for c in connector.__mro__)
                shadowed.append(f"{connector.__name__}.{flag} is False despite a mixin ({mro})")

    assert not shadowed, "capability flags shadowed by BaseConnector defaults:\n  " + "\n  ".join(
        shadowed
    )


def test_local_store_connector_is_eidetic():
    """Explicit regression: views were dead on the local store for exactly this reason."""
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    assert LocalStoreConnector.eidetic is True


if __name__ == "__main__":  # pragma: no cover
    test_capability_mixins_are_not_shadowed()
    test_local_store_connector_is_eidetic()
    print("✅ okay")
