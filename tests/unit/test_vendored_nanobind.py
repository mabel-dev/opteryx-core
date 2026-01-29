import os


def _exists_any(paths):
    return any(os.path.exists(p) for p in paths)


def test_nanobind_vendored():
    candidates = [
        "third_party/nanobind/nanobind.h",
        "third_party/nanobind/nanobind/nanobind.h",
    ]
    assert _exists_any(candidates), (
        "Nanobind headers are missing. Run: python tools/vendor_nanobind.py --tag <tag>"
    )
