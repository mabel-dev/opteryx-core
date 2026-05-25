"""scalar_constructors — create constant Draken vectors from Python scalar values.

Replaces old draken_old's from_scalar. Dispatches on Python type to the
appropriate draken_native factory.
"""
import datetime

from draken.draken_native import (
    vector_from_bool_constant,
    vector_from_constant,
    vector_float64_from_constant,
    vector_from_string_sequence,
    vector_date32_from_constant,
    vector_timestamp_from_constant,
)
from draken.vectors.vector import Vector


def from_scalar(value, num_rows):
    """Create a constant Draken vector of length num_rows with value `value`."""
    nb_vec = None
    if isinstance(value, bool):
        nb_vec = vector_from_bool_constant(value, num_rows)
    elif isinstance(value, int):
        nb_vec = vector_from_constant(value, num_rows)
    elif isinstance(value, float):
        nb_vec = vector_float64_from_constant(value, num_rows)
    elif isinstance(value, (str, bytes)):
        str_val = value.decode("utf-8") if isinstance(value, bytes) else value
        nb_vec = vector_from_string_sequence([str_val] * num_rows)
    elif isinstance(value, datetime.datetime):
        nb_vec = vector_timestamp_from_constant(value, num_rows)
    elif isinstance(value, datetime.date):
        nb_vec = vector_date32_from_constant(value, num_rows)
    if nb_vec is None:
        return None
    return wrap_nb_vector(nb_vec)


def wrap_nb_vector(nb_vec):
    """Wrap a raw nanobind VectorOwner in the appropriate typed Cython shim subclass."""
    type_name = nb_vec.type.name
    if type_name in ("VARCHAR", "NVARCHAR", "VARBINARY", "DICTIONARY"):
        from draken.vectors.string_vector import StringVector
        return StringVector(nb_vec)
    if type_name == "BOOL":
        from draken.vectors.bool_vector import BoolVector
        return BoolVector(nb_vec)
    if type_name in ("INT64", "INT8", "INT16", "INT32"):
        from draken.vectors.integer64_vector import Integer64Vector
        return Integer64Vector(nb_vec)
    if type_name in ("FLOAT32", "FLOAT64"):
        from draken.vectors.float64_vector import Float64Vector
        return Float64Vector(nb_vec)
    if type_name == "DECIMAL":
        from draken.vectors.decimal_vector import DecimalVector
        return DecimalVector(nb_vec)
    if type_name == "TIMESTAMP64":
        from draken.vectors.timestamp_vector import TimestampVector
        return TimestampVector(nb_vec)
    if type_name == "DATE32":
        from draken.vectors.date32_vector import Date32Vector
        return Date32Vector(nb_vec)
    if type_name == "INTERVAL":
        from draken.vectors.interval_vector import IntervalVector
        return IntervalVector(nb_vec)
    if type_name == "ARRAY":
        from draken.vectors.array_vector import ArrayVector
        return ArrayVector(nb_vec)
    return Vector(nb_vec)
