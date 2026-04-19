# yyjson package shim exposing a simple API compatible with existing call sites
try:
    from . import cyyjson as _cyy
except Exception:
    # If extension not built yet, lazy import fallback
    _cyy = None

class Parser:
    def __init__(self):
        if _cyy is None:
            from importlib import import_module
            _cyy = import_module('opteryx.third_party.yyjson.cyyjson')
        self._p = _cyy.Parser()
    def parse(self, data, recursive=True):
        return self._p.parse(data, recursive=recursive)
    def dump(self, obj):
        # returns bytes to match orjson behaviour
        return self._p.dump(obj)

# module-level helpers
def loads(s):
    return Parser().parse(s)

def dumps(obj):
    # default to bytes like orjson; callers expecting str should decode()
    return Parser().dump(obj)

# expose underlying Cython Parser as well
try:
    Parser = _cyy.Parser
except Exception:
    pass
