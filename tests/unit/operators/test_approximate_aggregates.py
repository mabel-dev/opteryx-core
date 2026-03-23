# Licensed under the Apache License, Version 2.0

import pyarrow as pa

from opteryx.compiled.aggregations.scalar_kernels import approximate_count, approximate_percentile


def test_approximate_count_estimates_distinct_values():
    sketch = approximate_count(pa.array([1, 2, 2, 3, None, 3, 4, 4]), None)
    estimate = sketch.estimate()
    assert 3 <= estimate <= 5


def test_approximate_percentile_estimates_middle_value():
    sketch = approximate_percentile(pa.array([1, 2, 3, 4, 5]), None, 0.5)
    median = sketch.quantile()
    assert median is not None
    assert abs(median - 3.0) <= 0.5
