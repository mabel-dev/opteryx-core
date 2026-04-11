import base64
import datetime
import os
import sys
from types import SimpleNamespace

import numpy
import pyarrow
import pytest

from opteryx.schema import FlatColumn
from opteryx.types import PYTHON_TO_ORSO_MAP, OrsoTypes

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.expression.functions.compat import fixed_value_function

import opteryx
from opteryx.expression.evaluator import apply_bounded_function
from opteryx.expression.functions.registrar import get_builtin_functions

ROW_COUNT = 3
TEMPORAL_VALUES = pyarrow.array(
    [
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        datetime.datetime(2024, 2, 3, 4, 5, 6),
        datetime.datetime(2024, 3, 4, 5, 6, 7),
    ],
    type=pyarrow.timestamp("us"),
)
VALID_BASE64 = numpy.array(
    [base64.b64encode(value).decode() for value in (b"alpha", b"beta", b"gamma")],
    dtype=object,
)
VALID_BASE85 = numpy.array(
    [base64.b85encode(value).decode() for value in (b"alpha", b"beta", b"gamma")],
    dtype=object,
)
VALID_HEX = numpy.array(
    [base64.b16encode(value).decode() for value in (b"alpha", b"beta", b"gamma")],
    dtype=object,
)
# These sets are derived from the registry so newly-added builtin functions are
# automatically included in this test.
BUILTIN_FUNCTIONS = list(get_builtin_functions())

CONSTANT_FUNCTIONS = {f.name for f in BUILTIN_FUNCTIONS if f.category == "constant"}
# These functions are handled specially by the planner and do not have a runtime kernel
# that can be called directly.
PLANNER_ONLY_FUNCTIONS = {"_ARRAY", "_TRY_ARRAY"}

FUNCTION_CASES = [
    (function_def, overload)
    for function_def in BUILTIN_FUNCTIONS
    for overload in function_def.overloads
]


class FakeEmbeddingProvider:
    def embed_texts(self, texts):
        return [[1.0, 0.0] if "a" in str(text).lower() else [0.0, 1.0] for text in texts]


@pytest.fixture(autouse=True)
def embedding_provider():
    opteryx.register_embedding_provider(FakeEmbeddingProvider())
    try:
        yield
    finally:
        opteryx.clear_embedding_provider()


CASE_IDS = [f"{function_def.name}:{overload.id}" for function_def, overload in FUNCTION_CASES]


def _make_context():
    return SimpleNamespace(
        execution_context=SimpleNamespace(
            connected_at=datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            query_id=7,
            schema="test",
            user="tester",
        )
    )


def _make_bound_node(function_name, overload):
    return SimpleNamespace(
        value=function_name,
        function_ref=SimpleNamespace(selected_overload=overload),
    )


# Override values for special-case parameters.
# Keys are (function_name, parameter_name). Overload-specific behavior should be rare;
# keep the default path based on type family for maximum coverage.
_OVERRIDE_ARGUMENTS: dict[tuple[str, str], object] = {
    # CONCAT/CONCAT_WS expect an array-of-arrays (one list-of-strings per row)
    ("CONCAT", "str1"): numpy.array([["alpha", "1"], ["beta", "2"], ["gamma", "3"]], dtype=object),
    ("CONCAT_WS", "sep"): "-",
    ("CONCAT_WS", "str1"): numpy.array(
        [["alpha", "1"], ["beta", "2"], ["gamma", "3"]], dtype=object
    ),
    # ASCII expects single-character strings.
    ("ASCII", "str"): numpy.array(["a", "b", "c"], dtype=object),
    # For RANDOM/NORMAL the kernel expects a scalar row count (not a vector).
    ("RANDOM", "n"): ROW_COUNT,
    ("NORMAL", "n"): ROW_COUNT,
    ("ROUND", "precision"): 2,
    ("POWER", "exp"): numpy.array([2.0, 2.0, 2.0], dtype=numpy.float64),
    ("TIME_BUCKET", "magnitude"): numpy.array([1], dtype=numpy.int64),
    ("TIME_BUCKET", "units"): "day",
    ("TRIM", "chars"): ["a"],
    ("LTRIM", "chars"): ["a"],
    ("RTRIM", "chars"): ["a"],
    ("LPAD", "fill"): ["x"],
    ("RPAD", "fill"): ["x"],
    ("SPLIT", "delimiter"): [","],
    ("SPLIT", "limit"): [2],
    # parameter name is "blob" for these decoding functions
    ("BASE64_DECODE", "blob"): VALID_BASE64,
    ("BASE85_DECODE", "blob"): VALID_BASE85,
    ("HEX_DECODE", "blob"): VALID_HEX,
    ("ARRAY_CONTAINS", "item"): 1,
    ("ARRAY_CONTAINS_ANY", "items"): [1, 5],
    ("ARRAY_CONTAINS_ALL", "items"): [1, 5],
    ("NULLIF", "compare"): numpy.array([9, 9, 9], dtype=object),
}


def _sample_argument(function_name, overload_id, parameter, index):
    name = parameter.name
    family = parameter.type_family

    # Special-case overrides (mostly shape/value things that don't fit cleanly into a type family).
    override = _OVERRIDE_ARGUMENTS.get((function_name, name))
    if override is not None:
        return override

    # Some functions are genuinely special and need per-overload logic.
    if function_name in {"RANDOM", "NORMAL"} and overload_id.endswith("_0"):
        return None

    if family == "string":
        if name == "part":
            return ["year"]
        if name in {"units", "unit"}:
            return ["day"]
        if name == "pattern" and function_name == "DATE_FORMAT":
            return ["%Y-%m-%d"]
        if name == "pattern" and function_name == "_MATCH_AGAINST":
            return ["alpha pattern"]
        if name == "type_name":
            return ["INTEGER"]
        if name == "key":
            return ["alpha"]
        return pyarrow.array(["alpha", "beta", "gamma"], type=pyarrow.string())

    if family == "integer":
        return numpy.array([1, 2, 3], dtype=numpy.int64)
    if family == "numeric":
        if function_name == "SIGN":
            return numpy.array([1, -2, 3], dtype=numpy.int64)
        return numpy.array([1.25, 2.5, 3.75], dtype=numpy.float64)
    if family == "boolean":
        return pyarrow.array([True, False, True], type=pyarrow.bool_())
    if family == "temporal":
        # TRUNC_temporal expects a timestamp input.
        if function_name == "TRUNC" and overload_id.endswith("_temporal") and name == "value":
            return TEMPORAL_VALUES
        return TEMPORAL_VALUES
    if family == "array":
        return numpy.array([[1, 2], [3, 4], [5, 6]], dtype=object)
    if family == "numeric_vector":
        return numpy.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]], dtype=object)
    if family == "any":
        if function_name == "_GET_STRING" and name == "struct":
            return numpy.array(
                [{"alpha": "one"}, {"alpha": "two"}, {"alpha": "three"}], dtype=object
            )
        if function_name == "JSONB_OBJECT_KEYS":
            return numpy.array([{"alpha": 1}, {"beta": 2}, {"gamma": 3}], dtype=object)
        if function_name == "HUMANIZE":
            return numpy.array([1000, 2000, 3000], dtype=numpy.int64)
        if function_name == "CHAR":
            return numpy.array([65, 66, 67], dtype=numpy.int64)
        if function_name in {
            "HASH",
            "MD5",
            "SHA1",
            "SHA224",
            "SHA256",
            "SHA384",
            "SHA512",
            "BASE64_ENCODE",
            "BASE85_ENCODE",
            "HEX_ENCODE",
        }:
            return numpy.array(["alpha", "beta", "gamma"], dtype=object)
        if function_name in {"COALESCE", "IFNULL", "IFNOTNULL", "_PASSTHRU"}:
            return numpy.array([1, None, 3], dtype=object)
        return numpy.array([1, 2, 3], dtype=object)

    raise ValueError((function_name, overload_id, family, name))


def _infer_logical_type(value):
    if hasattr(value, "to_arrow"):
        value = value.to_arrow()
    if isinstance(value, pyarrow.ChunkedArray):
        value = value.combine_chunks()
    if isinstance(value, pyarrow.Array):
        column = FlatColumn.from_arrow(pyarrow.field("result", value.type))
        return column.type, column.element_type
    if isinstance(value, numpy.ndarray):
        if value.ndim == 2 and value.dtype.kind in {"b", "i", "u", "f"}:
            element_type = OrsoTypes.DOUBLE if value.dtype.kind == "f" else OrsoTypes.INTEGER
            return OrsoTypes.VECTOR, element_type
        sample = next((item for item in value.tolist() if item is not None), None)
        if isinstance(sample, numpy.ndarray):
            sample = sample.tolist()
        if isinstance(sample, (list, tuple)):
            inner = next((item for item in sample if item is not None), None)
            return OrsoTypes.ARRAY, PYTHON_TO_ORSO_MAP.get(type(inner), OrsoTypes.NULL)
        if sample is None:
            return OrsoTypes.NULL, None
        return PYTHON_TO_ORSO_MAP.get(type(sample), OrsoTypes._MISSING_TYPE), None
    if isinstance(value, numpy.datetime64):
        return OrsoTypes.TIMESTAMP, None
    if isinstance(value, datetime.time):
        return OrsoTypes.TIME, None
    return PYTHON_TO_ORSO_MAP.get(type(value), OrsoTypes._MISSING_TYPE), None


def _expected_logical_type(function_def, overload, parameters):
    return_spec = overload.return_spec
    if return_spec.mode == "fixed":
        return return_spec.fixed_type, None

    argument_nodes = []
    for parameter, value in zip(overload.parameters, parameters, strict=True):
        literal_value = None
        if parameter.constant_only:
            if isinstance(value, pyarrow.Array):
                literal_value = value[0].as_py()
            elif isinstance(value, numpy.ndarray):
                literal_value = value[0].item() if hasattr(value[0], "item") else value[0]
            elif isinstance(value, (list, tuple)):
                literal_value = value[0]
            else:
                literal_value = value
        logical_type, element_type = _infer_logical_type(value)
        argument_nodes.append(
            SimpleNamespace(
                value=literal_value,
                schema_column=SimpleNamespace(type=logical_type, element_type=element_type),
                parameters=[],
            )
        )

    if return_spec.mode == "same_as_arg":
        return _infer_logical_type(parameters[return_spec.arg_index])

    resolved_type = return_spec.resolver(argument_nodes)
    if isinstance(resolved_type, tuple):
        return resolved_type
    return resolved_type, None


def _types_compatible(expected, actual):
    expected_type, expected_element = expected
    actual_type, actual_element = actual

    if {expected_type, actual_type} <= {OrsoTypes.VARCHAR, OrsoTypes.BLOB}:
        return True
    if expected_type in {OrsoTypes.INTEGER, OrsoTypes.DOUBLE} and actual_type in {
        OrsoTypes.INTEGER,
        OrsoTypes.DOUBLE,
    }:
        return True
    if {expected_type, actual_type} <= {OrsoTypes.ARRAY, OrsoTypes.VECTOR}:
        if expected_element in (None, OrsoTypes.NULL) or actual_element in (None, OrsoTypes.NULL):
            return True
        return expected_element == actual_element
    if expected_type != actual_type:
        return False
    if expected_element in (None, OrsoTypes.NULL) or actual_element in (None, OrsoTypes.NULL):
        return True
    return expected_element == actual_element


def _execute_case(function_def, overload):
    expected = (overload.return_spec.fixed_type, None)

    if function_def.name in CONSTANT_FUNCTIONS:
        returned_type, _ = fixed_value_function(function_def.name, _make_context())
        return expected, (returned_type, None)

    if function_def.name in PLANNER_ONLY_FUNCTIONS:
        assert overload.kernel.callable_ref([1, 2, 3], ["INTEGER"]) is None
        expected = _expected_logical_type(function_def, overload, [[1, 2, 3], ["INTEGER"]])
        return expected, expected

    if function_def.name == "_CASE":
        result = overload.kernel.callable_ref(
            [numpy.array([True, False, True])],
            [numpy.array([1, 2, 3])],
        )
        return (OrsoTypes.INTEGER, None), _infer_logical_type(result)

    parameters = [
        _sample_argument(function_def.name, overload.id, parameter, index)
        for index, parameter in enumerate(overload.parameters)
    ]
    expected = _expected_logical_type(function_def, overload, parameters)

    if function_def.name == "CONCAT":
        result = overload.kernel.callable_ref(parameters[0])
    elif function_def.name == "CONCAT_WS":
        result = overload.kernel.callable_ref(parameters[0], parameters[1])
    elif function_def.name == "TIME_BUCKET":
        result = overload.kernel.callable_ref(parameters[2], parameters[0], parameters[1])
    elif function_def.name in {"RANDOM", "NORMAL"} and overload.id.endswith("_0"):
        result = overload.kernel.callable_ref(ROW_COUNT)
    else:
        result = apply_bounded_function(_make_bound_node(function_def.name, overload), *parameters)

    return expected, _infer_logical_type(result)


@pytest.mark.filterwarnings(
    "ignore:no explicit representation of timezones available for np.datetime64"
)
@pytest.mark.parametrize(("function_def", "overload"), FUNCTION_CASES, ids=CASE_IDS)
def test_registered_function_overloads_return_compatible_types(function_def, overload):
    expected, actual = _execute_case(function_def, overload)

    assert _types_compatible(expected, actual), (
        f"{function_def.name}/{overload.id} returned {actual}, expected {expected}"
    )


if __name__ == "__main__":  # pragma: no cover
    # Running the file directly should behave like pytest run.
    import pytest

    pytest.main([__file__])
