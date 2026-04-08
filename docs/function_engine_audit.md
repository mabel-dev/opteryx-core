# Function Engine Audit

This document is a function-by-function audit of the function implementations under:

- `opteryx/expression/functions/implementations`

For each function I list:
- Primary engine(s) used (one or more of: python, arrow, numpy, cython, draken, nanobind)
- Short rationale for the choice
- A code pointer showing the function signature and immediate lines (so you can jump to the implementation)

Legend
- python: pure Python (list comprehensions, per-row loops, stdlib)
- arrow: PyArrow APIs / `pyarrow.compute`
- numpy: NumPy arrays and linear algebra
- cython: compiled Cython kernels under `opteryx.compiled.*`
- draken: Draken SIMD/vector types (`StringVector`) + interop
- nanobind: native C++ nanobind bindings (e.g. `opteryx.compiled.nanobind`)

---

## opteryx/expression/functions/implementations/arithmetic.py

- File-level engines: `numpy`, `pyarrow.compute` (where used), `cython` (compiled vector ops)

_is_constant_like — engines: python  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L26-27
def _is_constant_like(value) -> bool:
    return getattr(value, "encoding", None) == _DRAKEN_ENCODING_CONSTANT
```
Rationale: simple attribute check to detect Draken-encoded constant vectors.

_constant_scalar — engines: python  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L30-35
def _constant_scalar(value):
    if getattr(value, "encoding", None) == _DRAKEN_ENCODING_CONSTANT:
        if len(value) == 0:
            return None
        return value[0]
    return value
```
Rationale: Python accessor returning scalar from Draken-encoded container.

round1 — engines: cython (compiled vector ops)  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L38-45
def round1(values):
    """ROUND(values)"""
    from opteryx.compiled.vector_ops import vector_round
    from opteryx.compiled.vector_ops import vector_round_constant

    if _is_constant_like(values):
        return vector_round_constant(values, 0)
    return vector_round(values)
```
Rationale: delegates to `vector_round*` compiled kernels for performance.

round2 — engines: cython (compiled vector ops), python for scalar handling  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L48-58
def round2(values, digits):
    """ROUND(values, digits)"""
    from opteryx.compiled.vector_ops import vector_round_constant
    from opteryx.compiled.vector_ops import vector_round_digits

    if _is_constant_like(digits):
        scalar = _constant_scalar(digits)
        d = int(scalar) if scalar is not None else 0
    else:
        d = int(digits)
```
Rationale: compute digit scalar in Python then call compiled kernel.

random_number — engines: numpy  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L65-66
def random_number(size):
    return numpy.random.uniform(size=size)
```

random_normal — engines: numpy  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L69-73
def random_normal(size):
    from numpy.random import default_rng

    rng = default_rng(831835)  # 8 days, 3 hours, 18 minutes, 35 seconds
    return rng.standard_normal(size)
```

random_strings — engines: cython (compiled vector ops)  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L76-86
def random_strings(items):
    if isinstance(items, int):
        row_count = items
        width = 16
    elif len(items) > 0:
        row_count = len(items)
        width = items[0]
    else:
        return []

    from opteryx.compiled.vector_ops import vector_random_strings
```
Rationale: uses compiled `vector_random_strings`.

safe_power — engines: pyarrow.compute, numpy  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L91-101
def safe_power(base_array, exponent_array):
    """
    Wrapper around pyarrow's compute.power function.
    If both base and exponent arrays are of int type, the result will be int.
    Otherwise, it'll return a float.
    """
    if len(numpy.unique(exponent_array)) != 1:
        raise ValueError("The exponent_array should have all identical values.")

    single_exponent = exponent_array[0]
```
Rationale: prefers Arrow `compute.power` for correct typed semantics, uses NumPy to inspect/cast.

log — engines: cython (compiled vector ops)  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L110-113
def log(values, bases):
    from opteryx.compiled.vector_ops import vector_log

    return vector_log(values, bases)
```

ceiling / floor / trunc — engines: numpy, python (scalar handling)  
```opteryx-core/opteryx/expression/functions/implementations/arithmetic.py#L116-126
def ceiling(values, scales=None) -> List:
    """Performs a 'ceiling' with a scale factor."""
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.ceil(values)
```
Rationale: NumPy elementwise math with Python broadcast/scale handling.

---

## opteryx/expression/functions/implementations/text.py

- File-level engines: `draken` + `cython` compiled kernels for string hot-paths, `pyarrow.compute` for some fallback compute, `numpy` and `python` for non-vector cases and bridging.

to_lower — engines: draken + arrow (I/O)  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L34-38
def to_lower(arr):
    """Fast lowercase using buffer-level SIMD operations."""
    vec = _as_string_vector(arr)
    return string_vector_lowercase(vec).to_arrow()
```
Rationale: converts to `StringVector` then runs SIMD kernel; returns Arrow.

to_upper — engines: draken + arrow (I/O)  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L41-45
def to_upper(arr):
    """Fast uppercase using buffer-level SIMD operations."""
    vec = _as_string_vector(arr)
    return string_vector_uppercase(vec).to_arrow()
```

vector_lengther — engines: draken, pyarrow.compute (for null handling)  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L52-70
def vector_lengther(arr):
    """Return string lengths using the Draken StringVector API.
    ...
    """
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if arr.__class__.__name__ in ("ArrayVector", "VectorVector"):
        return vector_length(arr).to_arrow()
```
Rationale: uses string vector length kernel; if Arrow input used, converts and uses `compute.if_else` to reapply nulls.

_initcap — engines: draken + cython  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L96-101
def _initcap(arr):
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(arr, StringVector):
        return vector_initcap(arr).to_arrow()
    return vector_initcap(_as_string_vector(arr)).to_arrow()
```

_reverse — engines: draken + cython  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L109-113
def _reverse(arr):
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(arr, StringVector):
        return vector_reverse(arr).to_arrow()
    return vector_reverse(_as_string_vector(arr)).to_arrow()
```

_soundex, _md5, _sha1, _sha256, _sha512 — engines: draken + cython  
(Example `_md5` shown)
```opteryx-core/opteryx/expression/functions/implementations/text.py#L121-129
def _md5(arr):
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(arr, StringVector):
        return vector_md5(arr).to_arrow()
    return vector_md5(_as_string_vector(arr)).to_arrow()
```
Rationale: string hashing via compiled kernels accepting `StringVector`.

_replace — engines: draken + cython  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L128-141
def _replace(data, search, replace_val):
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    data_vec = data if isinstance(data, StringVector) else _as_string_vector(data)
    if isinstance(search, numpy.ndarray):
        search = search[0]
    if isinstance(replace_val, numpy.ndarray):
        replace_val = replace_val[0]
    if isinstance(search, str):
        search = search.encode("utf-8")
    if isinstance(replace_val, str):
        replace_val = replace_val.encode("utf-8")
    return vector_replace(data_vec, search, replace_val).to_arrow()
```
Rationale: compiled string replace kernel.

_string_slice_left / _string_slice_right — engines: draken + cython  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L144-157
def _string_slice_left(arr, length):
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(length, numpy.ndarray):
        length = int(length[0])
    if isinstance(arr, StringVector):
        return vector_string_slice_left(arr, length).to_arrow()
    return vector_string_slice_left(_as_string_vector(arr), length).to_arrow()
```

_as_arrow_string_array (helper) — engines: pyarrow, numpy, python  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L189-197
def _as_arrow_string_array(value):
    if isinstance(value, pyarrow.ChunkedArray):
        return value.combine_chunks()
    if isinstance(value, pyarrow.Array):
        return value
    if hasattr(value, "to_arrow"):
        return _as_arrow_string_array(value.to_arrow())
    if isinstance(value, numpy.ndarray):
        return pyarrow.array(value.tolist())
    return pyarrow.array(value)
```
Rationale: normalizes many input forms to Arrow arrays.

_as_string_vector (helper) — engines: draken, pyarrow  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L199-206
def _as_string_vector(value) -> StringVector:
    if isinstance(value, StringVector):
        return value

    arrow_arr = _as_arrow_string_array(value)
    if pyarrow.types.is_dictionary(arrow_arr.type):
        return StringVector.from_arrow(arrow_arr.dictionary_decode())
    return StringVector.from_arrow(arrow_arr)
```
Rationale: key Arrow→Draken conversion.

_as_match_vector — engines: draken, pyarrow, python  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L224-244
def _as_match_vector(arr):
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow

    if _is_string_fastpath_vector(arr):
        return arr

    if hasattr(arr, "to_arrow"):
        arr = arr.to_arrow()
    elif isinstance(arr, (numpy.ndarray, list, tuple)):
        arr = pyarrow.array(arr)
    elif not isinstance(arr, pyarrow.Array):
        return None

    vec = vector_from_arrow(arr)
    if _is_string_fastpath_vector(vec):
        return vec
    return None
```
Rationale: returns a `StringVector` when possible so compiled match kernels are usable.

split — engines: cython (single-char fast path) and pyarrow.compute fallback  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L248-266
def split(arr, delimiter=",", limit=None):
    """
    Fast SIMD-based split for single-character delimiters.
    Falls back to PyArrow for multi-character patterns or limits.
    """
    if not isinstance(delimiter, str):
        delimiter = delimiter[0]

    if len(delimiter) == 1 and limit is None:
        from opteryx.compiled.vector_ops import vector_split

        return vector_split(_as_string_vector(arr), ord(delimiter))

    delimiter = delimiter[0] if isinstance(delimiter, list) else delimiter
    if limit is not None:
        limit = limit[0]
        if limit < 1:
            raise InvalidFunctionParameterError("SPLIT limit must be a greater than 0")
    return compute.split_pattern(arr, delimiter, max_splits=limit or None)
```
Rationale: compiled vector_split for fast single-character splits; Arrow compute for other cases.

levenshtein — engines: pyarrow conversion, draken + cython compiled `vector_levenshtein`  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L392-423
def levenshtein(a, b):
    from opteryx.compiled.vector_ops import vector_levenshtein

    if hasattr(a, "to_numpy"):
        a = a.to_numpy(zero_copy_only=False)
    if hasattr(b, "to_numpy"):
        b = b.to_numpy(zero_copy_only=False)

    if not isinstance(a, pyarrow.Array):
        if not isinstance(a, numpy.ndarray):
            a = numpy.array(a, dtype=object)
        elif a.dtype.kind in ["U", "S"]:
            a = a.astype(object)
        a = pyarrow.array(a)
    if not isinstance(b, pyarrow.Array):
        if not isinstance(b, numpy.ndarray):
            b = numpy.array(b, dtype=object)
        elif b.dtype.kind in ["U", "S"]:
            b = b.astype(object)
        b = pyarrow.array(b)

    return vector_levenshtein(_as_string_vector(a), _as_string_vector(b)).to_arrow()
```
Rationale: converts inputs to Arrow then to `StringVector` and calls compiled kernel.

to_char / to_ascii / left_pad / right_pad — engines: python (per-row operations)  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L425-440
def to_char(arr) -> List[str]:
    return [chr(a) for a in arr]

def to_ascii(arr) -> List[int]:
    # Arrow engine passes pyarrow Arrays; coerce them to Python strings.
    if hasattr(arr, "to_pylist"):
        arr = arr.to_pylist()
    return [ord(a) for a in arr]
```

match_against — engines: draken + compiled vector ops (fast path) or python/arrow fallbacks  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L450-494
def match_against(arr, val):
    """
    Semantic text match using cosine similarity over embedded text.
    """
    if isinstance(val, (str, bytes)):
        literal = val
    else:
        if len(val) == 0:
            return []
        literal = val[0]

    if literal is None:
        return []
    if isinstance(literal, bytes):
        literal = literal.decode("utf8", errors="ignore")

    query_text = str(literal).strip()
    match_vector = _as_match_vector(arr)
    if match_vector is None:
        return []
    if not query_text:
        return pyarrow.array([False] * len(match_vector), type=pyarrow.bool_())

    provider = get_embedding_provider()
    if provider is None:
        return pyarrow.array([False] * len(match_vector), type=pyarrow.bool_())
    return vector_match_against(
        match_vector,
        provider,
        query_text,
        _MATCH_AGAINST_MIN_SCORE,
    ).to_arrow()
```
Rationale: compiled `vector_match_against` when a `StringVector` is available.



_normalise_replacement — engines: python (bytes processing)  
```opteryx-core/opteryx/expression/functions/implementations/text.py#L520-...
def _normalise_replacement(repl: bytes) -> bytes:
    """
    Normalise regex replacement backreferences from double-backslash form to single.
    ...
```

---

## opteryx/expression/functions/implementations/logical.py

- File-level engines: `numpy`, `pyarrow`, `python`

array_contains — engines: python  
```opteryx-core/opteryx/expression/functions/implementations/logical.py#L22-26
def array_contains(array, item):
    """does array contain item"""
    if array is None:
        return False
    return item in set(array)
```

if_null — engines: numpy (masking), pyarrow bridging  
```opteryx-core/opteryx/expression/functions/implementations/logical.py#L29-39
def if_null(values, replacements):
    """
    Replace null values in the input array with corresponding values from the replacement array.
    """
    from opteryx.expression.unary_operations import _is_null

    # Broadcast scalar replacement to a 1-element numpy array so the length
    # checks below work uniformly regardless of caller.
    if not hasattr(replacements, "__len__") and not hasattr(replacements, "to_numpy"):
        replacements = numpy.array([replacements])
```
Rationale: uses `_is_null` masks and `numpy.where` for vectorized replacement.

if_not_null — engines: numpy  
```opteryx-core/opteryx/expression/functions/implementations/logical.py#L82-92
def if_not_null(values: numpy.ndarray, replacements: numpy.ndarray) -> numpy.ndarray:
    ...
    is_not_null_mask = _is_not_null(values)
    target_type = numpy.promote_types(values.dtype, replacements.dtype)
    return numpy.where(is_not_null_mask, replacements, values).astype(target_type)
```

null_if — engines: numpy, pyarrow, python  
```opteryx-core/opteryx/expression/functions/implementations/logical.py#L102-112
def null_if(col1, col2):
    """
    Returns null if col1 equals col2, otherwise returns col1.
    """
    # Convert draken vectors to numpy arrays
    if hasattr(col1, "to_arrow") and not isinstance(col1, pyarrow.Array):
        col1 = col1.to_arrow().to_pylist()
    if hasattr(col2, "to_arrow") and not isinstance(col2, pyarrow.Array):
        col2 = col2.to_arrow().to_pylist()
    if isinstance(col1, pyarrow.Array):
        col1 = col1.to_numpy(False)
```
Rationale: converts inputs to numpy arrays then uses boolean mask operations.

---

## opteryx/expression/functions/implementations/temporal.py

- File-level engines: `pyarrow.compute`, `draken` + `cython` compiled kernels, `numpy`, `python`

convert_int64_array_to_pyarrow_datetime — engines: pyarrow, numpy  
```opteryx-core/opteryx/expression/functions/implementations/temporal.py#L24-34
def convert_int64_array_to_pyarrow_datetime(values: numpy.ndarray) -> pyarrow.Array:
    """
    Convert a NumPy int64 array to PyArrow TimestampArray, inferring time unit.
    """
    if isinstance(values, pyarrow.ChunkedArray):
        values = values.to_numpy(zero_copy_only=False)

    if isinstance(values, pyarrow.Array):
        values = values.to_numpy(zero_copy_only=False)

    if not isinstance(values, numpy.ndarray):
        raise InvalidInternalStateError("Expected a NumPy int64 array.")
```
Rationale: determines appropriate timestamp unit then casts to Arrow timestamp array.

date_part — engines: draken + cython compiled vector kernels; pyarrow for conversion  
```opteryx-core/opteryx/expression/functions/implementations/temporal.py#L63-73
def date_part(part, arr):
    """
    Extract a part from a date/timestamp (EXTRACT function).

    Accepts Draken vectors (TimestampVector, Int64Vector) or PyArrow arrays.

    Compiled kernels only - NO Arrow compute fallback:
    - Raises InvalidFunctionParameterError if datepart is not supported
    - All extraction is done via compiled Cython kernels for performance
    - PyArrow inputs are converted to Draken vectors automatically
```
Rationale: explicit compiled-only behavior for performance; converts Arrow inputs to Draken vectors and routes to `vector_datepart_*` kernels.

trunc_temporal — engines: python (calls `opteryx.utils.dates.date_trunc`)  
```opteryx-core/opteryx/expression/functions/implementations/temporal.py#L202-211
def trunc_temporal(arr, part):
    """
    Truncate a temporal value to the start of the specified unit.
    ...
    """
    from opteryx.utils.dates import date_trunc

    return date_trunc(part, arr)
```

date_diff — engines: pyarrow.compute (when extractors available), draken + cython `vector_date_diff` otherwise  
```opteryx-core/opteryx/expression/functions/implementations/temporal.py#L214-224
def date_diff(part, start, end):
    """Calculate the difference between two timestamps.

    All inputs are normalised to pyarrow timestamp[us] arrays first so that
    no numpy datetime64 intermediates are needed.
    """
    from opteryx.compiled.vector_ops import vector_date_diff
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

    arrow_extractors = {
        "months": compute.month_interval_between,
        "quarters": compute.quarters_between,
        "weeks": compute.weeks_between,
        "years": compute.years_between,
    }
```
Rationale: when Arrow compute provides a specialized extractor, use it; otherwise convert to Draken and use compiled kernel.

date_floor, from_unixtimestamp, unixtime — engines: mixed Arrow/NumPy/python  
- `date_floor` uses `pyarrow.compute.floor_temporal`.
- `from_unixtimestamp` returns NumPy `datetime64[s]`.
- `unixtime` handles Arrow→NumPy conversion, `datetime64` math, or string parsing with NumPy vectorize.

---

## opteryx/expression/functions/implementations/utility.py

- File-level engines: `numpy`, `pyarrow`, `draken` interop, `nanobind` (optional), `python`

_sequence_rows — engines: python, pyarrow  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L7-17
def _sequence_rows(values):
    if isinstance(values, (str, bytes, bytearray)):
        return [values]
    if isinstance(values, pyarrow.Array):
        return values.to_pylist()
    if isinstance(values, numpy.ndarray):
        return values.tolist()
    if isinstance(values, (list, tuple)):
        return list(values)
    return list(values)
```

_as_python_value — engines: pyarrow, python  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L19-27
def _as_python_value(value):
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, pyarrow.Array):
        return value.to_pylist()
    return value
```

_normalize helpers — engines: python, numpy  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L29-66
def _normalize_array_row(value):
    value = _as_python_value(value)
    if value is None:
        return None
    if isinstance(value, numpy.ndarray):
        if value.ndim == 0:
            return [value.item()]
        return value.tolist()
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    return [value]
```

_coerce_numeric_vector — engines: numpy  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L68-100
def _coerce_numeric_vector(value):
    value = _as_python_value(value)
    if value is None:
        return None
    if isinstance(value, numpy.ndarray):
        if value.ndim != 1:
            return None
        if value.dtype.kind not in {"b", "i", "u", "f"}:
            try:
                value = value.astype(numpy.float32)
            except (TypeError, ValueError):
                return None
        return numpy.asarray(value, dtype=numpy.float32)
    if isinstance(value, (list, tuple)):
        try:
            return numpy.asarray(value, dtype=numpy.float32)
        except (TypeError, ValueError):
            return None
    return None
```

_as_text_vector — engines: draken interop, pyarrow  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L125-143
def _as_text_vector(values):
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.compiled.draken.vectors.string_vector import StringVector

    if isinstance(values, StringVector):
        return values
    if hasattr(values, "to_arrow"):
        values = values.to_arrow()
    elif isinstance(values, (numpy.ndarray, list, tuple)):
        values = pyarrow.array(values)
    elif not isinstance(values, pyarrow.Array):
        return None

    vector = vector_from_arrow(values)
    return vector if isinstance(vector, StringVector) else None
```
Rationale: try to get a `StringVector` for provider scorers or compiled string kernels.

_coerce_numeric_matrix / _coerce_aligned_numeric_matrices — engines: numpy  
(see code for building dense matrices via `numpy.vstack`)

_score_numeric_vectors — engines: nanobind (optional) + numpy fallback  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L198-274
def _score_numeric_vectors(left_rows, right_rows):
    if len(right_rows) == 0:
        return []

    query_vector = _coerce_numeric_vector(right_rows[0])
    if len(right_rows) == 1 and query_vector is not None and query_vector.size > 0:
        dense_vectors, valid_positions = _coerce_numeric_matrix(left_rows, query_vector.size)
        scores = numpy.zeros(len(left_rows), dtype=numpy.float32)
        if dense_vectors.shape[0] == 0:
            return scores.tolist()

        try:
            from opteryx.compiled.nanobind import vector_search

            valid_scores = numpy.asarray(
                vector_search.score_cosine(query_vector, dense_vectors), dtype=numpy.float32
            )
        except (ImportError, ValueError):
            query_norm = numpy.linalg.norm(query_vector)
            ...
```
Rationale: prefers native `vector_search.score_cosine` and falls back to NumPy linear algebra.

_cosine_similarity_text — engines: draken (provider scorers preferring StringVector), nanobind (optional), numpy, python  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L276-378
def _cosine_similarity_text(arr, val):
    if len(val) == 0:
        return []

    literal = _coerce_text_scalar(val[0])
    if literal is None:
        return [0.0] * len(arr)
    query_text = literal.strip()
    if not query_text:
        return [0.0] * len(arr)

    provider = get_embedding_provider()
    if provider is not None and getattr(provider, "prefer_score_string_vector", False):
        text_vector = _as_text_vector(arr)
        scorer = getattr(provider, "score_string_vector", None)
        if text_vector is not None and scorer is not None:
            positions, scores = scorer(query_text, text_vector)
            ...
            return result.tolist()
    ...
    embedded = embed_text_matrix([query_text, *active_texts])
    query_vector = embedded[0]
    row_vectors = embedded[1:]

    try:
        from opteryx.compiled.nanobind import vector_search

        scores = numpy.asarray(
            vector_search.score_cosine(query_vector, row_vectors), dtype=numpy.float32
        )
    except (ImportError, ValueError):
        ...
```
Rationale: use provider scorer against `StringVector` if available; otherwise embed and use nanobind/NumPy.

cosine_similarity / cosine_distance — engines: python + numpy + helpers above  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L380-432
def cosine_similarity(arr, val):
    """Cosine similarity over numeric vectors or semantic text embeddings."""
    left_rows = _sequence_rows(arr)
    right_rows = _sequence_rows(val)

    if len(left_rows) == 0:
        return []

    sample_left = next((row for row in left_rows if row is not None), None)
    sample_right = next((row for row in right_rows if row is not None), None)
    if (
        _coerce_numeric_vector(sample_left) is not None
        and _coerce_numeric_vector(sample_right) is not None
    ):
        return _score_numeric_vectors(left_rows, right_rows)

    return _cosine_similarity_text(arr, val)
```

embed — engines: provider embedder (external), python  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L462-491
def embed(arr):
    """Convert text values into numeric vectors using the configured embedding provider."""
    rows = _sequence_rows(arr)
    if len(rows) == 0:
        return []

    texts = []
    row_positions = []
    results = [None] * len(rows)
    for index, value in enumerate(rows):
        text_value = _coerce_text_scalar(value)
        if text_value is None:
            continue
        texts.append(text_value)
        row_positions.append(index)

    if not texts:
        return results

    embedded = embed_text_values(texts)
    for index, vector in zip(row_positions, embedded, strict=True):
        results[index] = vector
    return results
```

jsonb_object_keys — engines: numpy, pyarrow, simdjson (third-party C extension)  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L493-526
def jsonb_object_keys(arr: numpy.ndarray):
    """
    Extract the keys from a NumPy array of JSON objects or JSON strings/bytes.
    """
    if len(arr) == 0:
        return numpy.array([])

    if isinstance(arr, pyarrow.Array):
        arr = arr.to_numpy(zero_copy_only=False)

    result = numpy.empty(arr.shape, dtype=list)

    if isinstance(arr[0], dict):
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in row.keys()]  # noqa: SIM118
    elif isinstance(arr[0], (str, bytes)):
        parser = simdjson.Parser()
        for i, row in enumerate(arr):
            result[i] = [str(key) for key in parser.parse(row).keys()]  # noqa: SIM118
    else:
        raise ValueError("Unsupported dtype for array elements. Expected dict, str, or bytes.")

    return result
```
Rationale: per-row parsing using a C-extension JSON parser when input is text.

humanize — engines: python (per-item formatting)  
```opteryx-core/opteryx/expression/functions/implementations/utility.py#L528-...
def humanize(arr):
    def format_number(num: float) -> str:
        return f"{num:,.0f}" if isinstance(num, int) else f"{num:,.1f}"
    ...
```

---

## opteryx/expression/functions/implementations/hash_encoding.py

- Currently unimplemented/TODO. No engines used.

```opteryx-core/opteryx/expression/functions/implementations/hash_encoding.py#L1-5
"""Hash and encoding function kernels.

Includes:
- Cryptographic hashing: MD5, SHA1, SHA224, SHA256, SHA384, SHA512
- Generic hashing: HASH
- Base64 encoding: BASE64_ENCODE, BASE64_DECODE
- Base85 encoding: BASE85_ENCODE, BASE85_DECODE
- Hexadecimal encoding: HEX_ENCODE, HEX_DECODE
"""

# TODO: Implement hash and encoding kernels
```

---

## Overall observations & recommendations

1. Execution ordering and preference:
   - Prefer compiled Draken + Cython kernels for string/temporal hot paths (fastest).
   - Use PyArrow compute where it provides required semantics (especially temporal/interval arithmetic).
   - Use NumPy for numeric linear algebra and where compiled kernels aren’t present.
   - Use Python per-row operations only when no vectorized/compiled kernel exists.

2. Clear conversion boundaries exist:
   - Helpers (`_as_arrow_string_array`, `_as_string_vector`, `_as_match_vector`, `_as_text_vector`) centralize Arrow↔Draken conversion. Keep these robust and well-tested because they control hot-path routing.
   - Where possible, prefer returning Arrow/Draken types directly from compiled kernels (`.to_arrow()` is used by the Python wrappers).

3. Native optimizations with graceful fallback:
   - Functions like `_score_numeric_vectors` and `_cosine_similarity_text` attempt nanobind native paths and fall back to NumPy if the native extension is missing. Keep this pattern for optional high-performance paths.

4. No-fallback/strict functions:
   - Some temporal functions (notably `date_part`) intentionally raise for unsupported inputs rather than silently fallback — this matches the project rule "Always prefer failure over silent degradation."

5. Candidates for porting:
   - Python functions that iterate row-by-row on potentially large arrays (e.g., `substring` per-row list comprehensions, `jsonb_object_keys` loops when inputs are text) are prime candidates for compiled kernels or vectorized Arrow/NumPy implementations if profiling shows they are hot.

---

If you want, I can:
- Produce a CSV/JSON of the above mapping (function → engines → file → line range).
- Run a repo-wide grep and generate an exhaustive machine-readable mapping (if you want me to do that next).
- Mark candidate functions for prioritized porting (based on likely hot-path patterns).

Which output would you like next?
