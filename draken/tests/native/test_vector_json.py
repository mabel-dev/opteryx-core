"""
Native correctness tests for Milestone E.18: vector_json_extract + vector_map_access
— JSON field extraction via yyjson, pure nanobind C++.

Coverage:
  vector_json_extract:
    dot-notation paths: nested objects, array indices, missing keys
    JSON Pointer paths: /a/b/0 form
    bare top-level key: "name" (no $ prefix)
    null TVL: null input row → null output
    JSON null value → null output (SQL convention)
    missing key → null output
    invalid JSON → RuntimeError (fail fast)
    complex output (object, array) → JSON text (composable)
    path is pre-computed once (verified by inspection)

  vector_map_access:
    top-level string key: found / missing / null value
    null TVL: null input row → null output
    non-object root → null output (not an error)
    invalid JSON → RuntimeError

  Both functions:
    non-Vector input → TypeError
    output type is DRAKEN_VARCHAR
"""

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Module loading (spec_from_file_location pattern — no opteryx import)
# ---------------------------------------------------------------------------

def _load_module(name, rel_path):
    base = os.path.join(os.path.dirname(__file__), "..", "..", "..", rel_path)
    import glob
    candidates = glob.glob(base + "*.so") + glob.glob(base + "*.pyd")
    if not candidates:
        raise FileNotFoundError(f"Compiled module not found: {base}*.so")
    spec = importlib.util.spec_from_file_location(name, candidates[0])
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_json = _load_module(
    "vector_json",
    "opteryx/compiled/nanobind/vector_json.cpython",
)
vector_json_extract = _json.vector_json_extract
vector_map_access   = _json.vector_map_access

import draken.draken_native as dn

DRAKEN_VARCHAR = dn.DrakenType.VARCHAR
DRAKEN_VARIANT = dn.DrakenType.VARIANT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    """Build a VARCHAR DrakenVector from a list of str/bytes/None values."""
    encoded = [v.encode("utf-8") if isinstance(v, str) else v for v in lst]
    return dn.vector_from_string_sequence(encoded)


def read(vec, i):
    """Read logical row i from a string Vector as bytes (or None if null)."""
    return vec[i]


def to_list(vec):
    """Read all rows from a string Vector."""
    return [vec[i] for i in range(len(vec))]


# ---------------------------------------------------------------------------
# vector_json_extract — dot-notation / JSON Pointer paths
# ---------------------------------------------------------------------------

class TestJsonExtractDotPath:
    def test_simple_top_level_key_dot(self):
        docs = make(['{"name":"alice","age":30}'])
        result = vector_json_extract(docs, b"$.name")
        assert read(result, 0) == '"alice"'

    def test_nested_path(self):
        docs = make(['{"a":{"b":42}}'])
        result = vector_json_extract(docs, b"$.a.b")
        assert read(result, 0) == "42"

    def test_array_index(self):
        docs = make(['[{"k":"v"},{"k":"w"}]'])
        result = vector_json_extract(docs, b"$[0].k")
        assert read(result, 0) == '"v"'

    def test_array_second_element(self):
        docs = make(['[{"k":"v"},{"k":"w"}]'])
        result = vector_json_extract(docs, b"$[1].k")
        assert read(result, 0) == '"w"'

    def test_missing_key_returns_null(self):
        docs = make(['{"a":1}'])
        result = vector_json_extract(docs, b"$.b")
        assert read(result, 0) is None

    def test_json_null_value_returns_sql_null(self):
        docs = make(['{"a":null}'])
        result = vector_json_extract(docs, b"$.a")
        assert read(result, 0) is None

    def test_integer_value(self):
        docs = make(['{"count":99}'])
        result = vector_json_extract(docs, b"$.count")
        assert read(result, 0) == "99"

    def test_boolean_value(self):
        docs = make(['{"ok":true}'])
        result = vector_json_extract(docs, b"$.ok")
        assert read(result, 0) == "true"

    def test_float_value(self):
        docs = make(['{"x":3.14}'])
        result = vector_json_extract(docs, b"$.x")
        val = read(result, 0)
        assert val is not None
        assert float(val) == pytest.approx(3.14)

    def test_object_value_is_json_text(self):
        import json
        docs = make(['{"a":{"b":1}}'])
        result = vector_json_extract(docs, b"$.a")
        val = read(result, 0)
        assert val is not None
        parsed = json.loads(val)
        assert parsed == {"b": 1}

    def test_array_value_is_json_text(self):
        import json
        docs = make(['{"arr":[1,2,3]}'])
        result = vector_json_extract(docs, b"$.arr")
        val = read(result, 0)
        assert val is not None
        parsed = json.loads(val)
        assert parsed == [1, 2, 3]

    def test_composable_chaining(self):
        # JSON_EXTRACT(JSON_EXTRACT(x, '$.a'), '$.b') should work.
        docs = make(['{"a":{"b":42}}'])
        inner = vector_json_extract(docs, b"$.a")
        # inner[0] should be '{"b":42}' — a valid JSON document.
        outer = vector_json_extract(inner, b"$.b")
        assert read(outer, 0) == "42"

    def test_bare_pointer_form(self):
        # Path already in JSON Pointer form.
        docs = make(['{"a":{"b":99}}'])
        result = vector_json_extract(docs, b"/a/b")
        assert read(result, 0) == "99"

    def test_bare_top_level_key_no_dollar(self):
        # "name" with no prefix is treated as a simple key → /name.
        docs = make(['{"name":"bob"}'])
        result = vector_json_extract(docs, b"name")
        assert read(result, 0) == '"bob"'

    def test_multiple_rows_mixed(self):
        docs = make(['{"x":1}', '{"x":2}', '{"y":3}'])
        result = vector_json_extract(docs, b"$.x")
        assert read(result, 0) == "1"
        assert read(result, 1) == "2"
        assert read(result, 2) is None   # missing key → null

    def test_output_type_is_variant(self):
        docs = make(['{"a":1}'])
        result = vector_json_extract(docs, b"$.a")
        assert result.type == DRAKEN_VARIANT

    def test_non_vector_input_raises_typeerror(self):
        with pytest.raises(TypeError):
            vector_json_extract("not a vector", b"$.a")

    def test_invalid_json_raises_runtime_error(self):
        docs = make(["{bad json}"])
        with pytest.raises(RuntimeError, match="invalid JSON"):
            vector_json_extract(docs, b"$.a")


class TestJsonExtractNullTVL:
    def test_null_row_propagates(self):
        docs = make([None, '{"a":1}', None])
        result = vector_json_extract(docs, b"$.a")
        assert read(result, 0) is None
        assert read(result, 1) == "1"
        assert read(result, 2) is None

    def test_all_null(self):
        docs = make([None, None])
        result = vector_json_extract(docs, b"$.x")
        assert read(result, 0) is None
        assert read(result, 1) is None

    def test_no_nulls_in_input(self):
        docs = make(['{"a":1}', '{"a":2}'])
        result = vector_json_extract(docs, b"$.a")
        assert read(result, 0) == "1"
        assert read(result, 1) == "2"


# ---------------------------------------------------------------------------
# vector_map_access — top-level key access
# ---------------------------------------------------------------------------

class TestMapAccess:
    def test_simple_key_found(self):
        docs = make(['{"name":"carol"}'])
        result = vector_map_access(docs, b"name")
        assert read(result, 0) == '"carol"'

    def test_simple_key_missing(self):
        docs = make(['{"a":1}'])
        result = vector_map_access(docs, b"b")
        assert read(result, 0) is None

    def test_json_null_value_is_sql_null(self):
        docs = make(['{"a":null}'])
        result = vector_map_access(docs, b"a")
        assert read(result, 0) is None

    def test_integer_value(self):
        docs = make(['{"count":7}'])
        result = vector_map_access(docs, b"count")
        assert read(result, 0) == "7"

    def test_nested_object_returned_as_json_text(self):
        import json
        docs = make(['{"obj":{"x":1}}'])
        result = vector_map_access(docs, b"obj")
        val = read(result, 0)
        assert val is not None
        assert json.loads(val) == {"x": 1}

    def test_null_row_propagates(self):
        docs = make([None, '{"a":1}'])
        result = vector_map_access(docs, b"a")
        assert read(result, 0) is None
        assert read(result, 1) == "1"

    def test_non_object_root_returns_null(self):
        # Root is an array, not an object — key access → null.
        docs = make(['[1,2,3]'])
        result = vector_map_access(docs, b"0")
        assert read(result, 0) is None

    def test_multiple_rows(self):
        docs = make(['{"k":"a"}', '{"k":"b"}', None, '{"k":"c"}'])
        result = vector_map_access(docs, b"k")
        assert read(result, 0) == '"a"'
        assert read(result, 1) == '"b"'
        assert read(result, 2) is None
        assert read(result, 3) == '"c"'

    def test_output_type_is_variant(self):
        docs = make(['{"a":1}'])
        result = vector_map_access(docs, b"a")
        assert result.type == DRAKEN_VARIANT

    def test_non_vector_input_raises_typeerror(self):
        with pytest.raises(TypeError):
            vector_map_access(42, b"key")

    def test_invalid_json_raises_runtime_error(self):
        docs = make(["not json at all"])
        with pytest.raises(RuntimeError, match="invalid JSON"):
            vector_map_access(docs, b"key")

    def test_large_string_value_extern_slot(self):
        # Value longer than 12 bytes goes through extern slot path.
        long_val = "x" * 50
        doc = '{"k":"' + long_val + '"}'
        docs = make([doc])
        result = vector_map_access(docs, b"k")
        val = read(result, 0)
        assert val is not None
        import json
        assert json.loads(val) == long_val
