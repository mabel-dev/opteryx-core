# cython: language_level=3, cdivision=True

# TODO: Phase 6 - implement Cython wrapper around C++ JsonlReader

def read_jsonl(
    data,
    columns=None,
    predicates=None,
    explicit_schema=None,
    infer_schema=True,
    infer_sample_size=5,
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True
):
    """
    Read JSONL data into Draken vectors with projection and predicate pushdown.

    Parameters:
      data: bytes or buffer-like (or file path)
      columns: list of column names to extract (None = all)
      predicates: list of (column, op, value) tuples; op in ['==', '!=', '<', '<=', '>', '>=']
      explicit_schema: dict mapping column names to types (skip inference)
      infer_schema: whether to infer schema if not explicit (default: True)
      infer_sample_size: rows to sample for inference (default: 5)
      parse_arrays: whether to parse arrays into Python lists (default: True)
      parse_objects: whether to parse objects into bytes (default: True)
      fail_on_error: raise on malformed records (True) or warn & continue (False)

    Returns:
      dict with keys:
        'success': bool
        'column_names': list[str] (only projected columns)
        'num_rows': int (only records matching predicates)
        'columns': list of Draken Vector objects
        'schema': dict of inferred/applied types
    """
    raise NotImplementedError("TODO: Phase 6 - implement wrapper")


def get_jsonl_schema(data, sample_size=5):
    """
    Infer schema from first N rows.

    Returns:
      list[dict] with keys: 'name', 'type', 'nullable'
    """
    raise NotImplementedError("TODO: Phase 6 - implement schema inference")
