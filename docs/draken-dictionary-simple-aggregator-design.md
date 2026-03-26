# Draken-Native Dictionary Simple Aggregator Design

## Problem Statement

`SimpleAggregateCollector.collect()` calls `pyarrow.compute.sum(dictionary_array)`, which fails because Arrow's sum kernel doesn't support dictionary input. Current proposal to cast/decode defeats the performance benefit of dictionary encoding.

## Root Cause Analysis

**Affected Query**: ClickBench Q04

**Current Error**:
```
ArrowNotImplementedError: Function 'sum' has no kernel matching input types 
(dictionary<values=int64, indices=uint16, ordered=0>)
```

**Failure Location**: `opteryx/operators/simple_aggregate_node.py:57`

```python
class SimpleAggregateCollector:
    def collect(self, values):
        if self.current_value is None:
            if self.aggregate_type in ("SUM", "AVG"):
                self.current_value = pyarrow.compute.sum(values).as_py()
                # ❌ Fails when values is dictionary array
```

**Query Plan** (verified via EXPLAIN):
```
Projection → Aggregation Simple → Parquet Read
```

## Performance Opportunity

### Dictionary Aggregation Mathematics

Given a dictionary-encoded array:
```
indices: [0, 1, 0, 2, 1, 0]  (length N = 6)
values:  [100, 200, 300]     (length V = 3, where V << N)

SUM(dict) = Σ(values[i] × count(indices == i))
          = 100×3 + 200×2 + 300×1
          = 300 + 400 + 300
          = 1000

Complexity:
  Dictionary-aware: O(N) index counting + O(V) weighted sum
  Materialization:  O(N) decode + O(N) sum
  
Speedup Factor: When V << N (high compression ratio)
  - V = N/10  → ~1.1x faster (negligible)
  - V = N/100 → ~1.9x faster (worthwhile)
  - V = N/1000 → ~9.5x faster (significant)
```

### ClickBench Tiny Dataset Analysis

**UserID Column**:
- Type: `dictionary<values=int64, indices=uint16>`
- Index type `uint16` → at most 65,536 unique values
- Dataset size: ~100,000 rows
- Expected compression: ~1.5x to 10x (typical for user IDs)
- **Expected speedup**: 1.5x to 5x for AVG(UserID)

**EventTime Column**:
- Type: `dictionary<values=int64, indices=uint32>` 
- High cardinality (timestamps at second granularity)
- Expected compression: ~1.5x (many unique values)
- **Expected speedup**: Minimal (~1.2x)

**When Dictionary Aggregation Wins**:
- Low-cardinality categorical columns (country, status, type, etc.)
- Repeated dimensions in star schemas
- Time dimensions with bucketing (year, month, day)

**When It Doesn't Matter**:
- High-cardinality columns (UUIDs, timestamps)
- Unique/near-unique identifiers
- Columns with V ≈ N (poor compression)

## Proposed Solution: Draken-Native Dictionary Aggregator

### Architecture Overview

```
SimpleAggregateNode.execute()
    ↓
Check if column is DictionaryVector (Draken)
    ↓ YES: High cardinality? (V/N > 0.1)
        ↓ YES: Use standard path (decode)
        ↓ NO:  Use DrakenDictionaryAggregateCollector (fast path)
    ↓ NO: Use standard path
```

**Design Principle**: Opportunistic optimization with automatic fallback

### Implementation Strategy

#### Option A: Separate Collector Class (Recommended)

**Pros**:
- Clear separation of concerns
- Easier to test in isolation
- Can specialize for dictionary operations
- Fallback path remains unchanged

**Cons**:
- More code (~200 lines)
- Needs dispatch logic in SimpleAggregateNode

#### Option B: Extend SimpleAggregateCollector

**Pros**:
- Single collector class
- Less dispatch logic

**Cons**:
- Muddles two different algorithms
- Harder to test edge cases
- More conditionals in hot path

**Decision**: **Option A** - clarity and testability outweigh code size

## Detailed Design

### 1. New Collector Class

**Location**: `opteryx/operators/simple_aggregate_node.py` (insert before SimpleAggregateNode)

```python
class DrakenDictionaryAggregateCollector:
    """
    Fast aggregator for dictionary-encoded numeric columns.
    
    Exploits dictionary structure to avoid materialization:
    - SUM: weighted sum of unique values
    - AVG: weighted average  
    - COUNT: count non-null indices
    - MIN/MAX: extremum of dictionary values
    - COUNT(DISTINCT): cardinality of values (not indices)
    
    Automatically falls back to materialization if:
    - Dictionary values are non-numeric
    - Compression ratio is poor (V/N > threshold)
    - Operations require full materialization
    """
    
    # Numeric dictionary value types (from draken/core/buffers.h)
    NUMERIC_DICT_TYPES = frozenset({
        1,   # INT8
        2,   # INT16  
        3,   # INT32
        4,   # INT64
        5,   # UINT8
        6,   # UINT16
        7,   # UINT32
        11,  # UINT64
        20,  # FLOAT32
        21,  # FLOAT64
    })
    
    # Threshold: if unique_values/total_rows > this, use standard path
    COMPRESSION_THRESHOLD = 0.1  # 10% unique → fallback
    
    def __init__(
        self, 
        aggregate_type, 
        schema_column, 
        *, 
        count_nulls=False,
        duplicate_treatment="IGNORE", 
        telemetry=None
    ):
        self.aggregate_type = aggregate_type
        self.schema_column = schema_column
        self.count_nulls = count_nulls
        self.duplicate_treatment = duplicate_treatment
        self.telemetry = telemetry
        
        # Validate supported aggregate types
        if aggregate_type not in ("SUM", "AVG", "COUNT", "MIN", "MAX", "COUNT_DISTINCT"):
            raise ValueError(
                f"DrakenDictionaryAggregateCollector does not support {aggregate_type}"
            )
        
        # Accumulator state for weighted aggregation
        self.value_counts = {}  # value → count mapping
        self.total_count = 0
        self.current_min = None
        self.current_max = None
        self.distinct_set = None  # For COUNT(DISTINCT)
        
        # Performance tracking
        self.batches_processed = 0
        self.fast_path_hits = 0
        self.fallback_hits = 0
    
    def collect_dictionary_vector(self, dict_vec):
        """
        Collect from a DictionaryVector using fast dictionary-aware logic.
        
        Args:
            dict_vec: opteryx.compiled.draken.vectors.DictionaryVector instance
            
        Raises:
            ValueError: If dictionary values are not numeric
            TypeError: If input is not a DictionaryVector
        """
        from opteryx.compiled.draken.vectors.dictionary_vector import DictionaryVector
        
        if not isinstance(dict_vec, DictionaryVector):
            raise TypeError(f"Expected DictionaryVector, got {type(dict_vec).__name__}")
        
        self.batches_processed += 1
        
        # Check if dictionary is suitable for fast path
        value_type = dict_vec.dictionary_value_type
        if value_type not in self.NUMERIC_DICT_TYPES:
            raise ValueError(
                f"Dictionary values must be numeric for aggregation, "
                f"got type_id={value_type}"
            )
        
        # Extract dictionary components via zero-copy FFI
        num_rows = dict_vec.length
        unique_count = dict_vec.dictionary_size
        
        # Check compression ratio: if poor, caller should use fallback
        compression_ratio = unique_count / num_rows if num_rows > 0 else 1.0
        if compression_ratio > self.COMPRESSION_THRESHOLD:
            # Dictionary has too many unique values, not worth special handling
            # This should be caught by caller, but we track it
            self.fallback_hits += 1
            raise ValueError(
                f"Dictionary compression ratio {compression_ratio:.2%} exceeds "
                f"threshold {self.COMPRESSION_THRESHOLD:.2%}. Use standard path."
            )
        
        self.fast_path_hits += 1
        
        # Get raw numpy arrays (zero-copy)
        indices = dict_vec.indices_array()      # numpy array of index values
        values_dict = dict_vec.values_array()   # numpy array of unique values
        
        # Get null mask if present (some DictionaryVectors track nulls separately)
        has_nulls = hasattr(dict_vec, 'has_nulls') and dict_vec.has_nulls()
        null_mask = dict_vec.null_mask() if has_nulls else None
        
        # --- COUNT aggregation ---
        if self.aggregate_type == "COUNT":
            if self.count_nulls:
                self.total_count += len(indices)
            else:
                # Count non-null indices
                if null_mask is not None:
                    import numpy as np
                    self.total_count += int((~null_mask).sum())
                else:
                    self.total_count += len(indices)
            return
        
        # --- COUNT(DISTINCT) ---
        if self.aggregate_type == "COUNT_DISTINCT" or self.duplicate_treatment == "Distinct":
            if self.distinct_set is None:
                self.distinct_set = set()
            
            # For COUNT(DISTINCT), we want distinct *values*, not distinct indices
            # The dictionary already deduplicates, so we track unique values
            if null_mask is not None:
                import numpy as np
                valid_indices = indices[~null_mask]
                unique_values = np.unique(values_dict[valid_indices])
            else:
                # All values in dictionary are distinct by definition
                unique_values = values_dict
            
            self.distinct_set.update(unique_values.tolist())
            return
        
        # --- MIN/MAX aggregation ---
        if self.aggregate_type in ("MIN", "MAX"):
            # For MIN/MAX, we only need to look at dictionary values
            # Nulls don't participate (SQL standard)
            if len(values_dict) > 0:
                if self.aggregate_type == "MIN":
                    batch_min = values_dict.min()
                    if self.current_min is None:
                        self.current_min = batch_min
                    else:
                        self.current_min = min(self.current_min, batch_min)
                else:  # MAX
                    batch_max = values_dict.max()
                    if self.current_max is None:
                        self.current_max = batch_max
                    else:
                        self.current_max = max(self.current_max, batch_max)
            return
        
        # --- SUM/AVG aggregation (weighted by index frequency) ---
        if self.aggregate_type in ("SUM", "AVG"):
            import numpy as np
            
            # Filter out nulls if present
            if null_mask is not None:
                valid_indices = indices[~null_mask]
            else:
                valid_indices = indices
            
            # Count frequency of each index value (this is the key optimization)
            # numpy.unique is highly optimized (uses hash table internally)
            unique_idx, counts = np.unique(valid_indices, return_counts=True)
            
            # Accumulate weighted values
            # Instead of sum(values[indices]), compute sum(values[i] * count[i])
            for idx, count in zip(unique_idx, counts):
                value = float(values_dict[idx])  # Convert to Python float for accumulation
                self.value_counts[value] = self.value_counts.get(value, 0) + int(count)
                if self.aggregate_type == "AVG":
                    self.total_count += int(count)
            
            return
        
        # Should never reach here (validated in __init__)
        raise RuntimeError(f"Unsupported aggregate type: {self.aggregate_type}")
    
    def collect_fallback(self, arrow_array):
        """
        Fallback for non-DictionaryVector inputs.
        
        Decodes dictionaries and uses standard Arrow kernels.
        """
        import pyarrow
        import pyarrow.compute as compute
        
        self.batches_processed += 1
        self.fallback_hits += 1
        
        # Decode dictionary if needed
        if pyarrow.types.is_dictionary(arrow_array.type):
            arrow_array = arrow_array.dictionary_decode()
        
        # Use standard Arrow kernels
        if self.aggregate_type == "COUNT":
            if self.count_nulls:
                self.total_count += compute.count(arrow_array).as_py()
            else:
                self.total_count += compute.count(arrow_array, mode="only_valid").as_py()
        
        elif self.aggregate_type in ("SUM", "AVG"):
            sum_value = compute.sum(arrow_array).as_py()
            if sum_value is not None:
                # Accumulate as if single "value" with count=1
                self.value_counts[sum_value] = self.value_counts.get(sum_value, 0) + 1
            if self.aggregate_type == "AVG":
                self.total_count += compute.count(arrow_array, mode="only_valid").as_py()
        
        elif self.aggregate_type == "MIN":
            new_min = compute.min(arrow_array).as_py()
            if new_min is not None:
                self.current_min = new_min if self.current_min is None else min(self.current_min, new_min)
        
        elif self.aggregate_type == "MAX":
            new_max = compute.max(arrow_array).as_py()
            if new_max is not None:
                self.current_max = new_max if self.current_max is None else max(self.current_max, new_max)
        
        elif self.aggregate_type == "COUNT_DISTINCT" or self.duplicate_treatment == "Distinct":
            from opteryx.compiled.aggregations.count_distinct import count_distinct
            if self.distinct_set is None:
                self.distinct_set = set()
            # Note: count_distinct returns cardinality, need to extract values
            # This is simplified - real implementation would need to handle properly
            unique_values = compute.unique(arrow_array).to_pylist()
            self.distinct_set.update(v for v in unique_values if v is not None)
    
    def get_result(self):
        """Finalize and return aggregate result."""
        if self.aggregate_type == "COUNT":
            return self.total_count
        
        if self.aggregate_type == "COUNT_DISTINCT" or self.duplicate_treatment == "Distinct":
            return len(self.distinct_set) if self.distinct_set else 0
        
        if self.aggregate_type == "MIN":
            return self.current_min
        
        if self.aggregate_type == "MAX":
            return self.current_max
        
        if self.aggregate_type == "SUM":
            # Compute weighted sum: Σ(value × count)
            return sum(value * count for value, count in self.value_counts.items())
        
        if self.aggregate_type == "AVG":
            if self.total_count == 0:
                return None
            # Weighted average: Σ(value × count) / Σ(count)
            total_sum = sum(value * count for value, count in self.value_counts.items())
            return total_sum / self.total_count
        
        return None
    
    def get_performance_stats(self):
        """Return performance metrics for telemetry."""
        return {
            "batches_processed": self.batches_processed,
            "fast_path_hits": self.fast_path_hits,
            "fallback_hits": self.fallback_hits,
            "fast_path_ratio": (
                self.fast_path_hits / self.batches_processed 
                if self.batches_processed > 0 
                else 0.0
            ),
        }
```

### 2. Integration into SimpleAggregateNode

**Modify `SimpleAggregateNode.__init__`**:

```python
def __init__(self, properties: QueryProperties, **parameters):
    BasePlanNode.__init__(self, properties=properties, **parameters)
    self.aggregates = parameters.get("aggregates", [])
    self.evaluatable_nodes = extract_evaluations(self.aggregates)
    
    # Create collectors - start with dictionary-aware collectors
    self.accumulator = {}
    
    for aggregate in self.aggregates:
        aggregate_type = aggregate.value
        final_column_id = aggregate.schema_column.identity
        
        # Try dictionary-aware collector first
        # Will fallback automatically if not suitable
        try:
            self.accumulator[final_column_id] = DrakenDictionaryAggregateCollector(
                aggregate_type,
                aggregate.parameters[0].schema_column,
                duplicate_treatment=aggregate.duplicate_treatment,
                telemetry=self.telemetry,
            )
        except ValueError:
            # Aggregate type not supported by dictionary collector
            # Use standard collector
            self.accumulator[final_column_id] = SimpleAggregateCollector(
                aggregate_type,
                aggregate.parameters[0].schema_column,
                duplicate_treatment=aggregate.duplicate_treatment,
                telemetry=self.telemetry,
            )
```

**Modify `SimpleAggregateNode.execute`**:

```python
def execute(self, morsel, **kwargs):
    from opteryx.compiled.draken.morsels.morsel import Morsel
    from opteryx.compiled.draken.vectors.dictionary_vector import DictionaryVector
    
    # Ensure we have Morsel for dictionary fast path detection
    if not isinstance(morsel, Morsel):
        morsel_arrow = self.ensure_arrow_table(morsel)
        if morsel_arrow != EOS:
            morsel = Morsel.from_arrow(morsel_arrow)
        else:
            morsel = morsel_arrow
    
    if morsel == EOS:
        # Finalize - collect performance stats if available
        names = []
        values = []
        for k, v in self.accumulator.items():
            names.append(k)
            values.append([v.get_result()])
            
            # Log performance stats for dictionary collectors
            if isinstance(v, DrakenDictionaryAggregateCollector) and self.telemetry:
                stats = v.get_performance_stats()
                if stats["fast_path_hits"] > 0:
                    self.telemetry.log(
                        "dictionary_aggregate_fast_path",
                        column=k,
                        **stats
                    )
        
        yield pyarrow.Table.from_arrays(values, names=names)
        yield EOS
        return
    
    # Evaluate functions if needed
    if self.evaluatable_nodes:
        from opteryx.expression.evaluator import evaluate_and_append_draken
        morsel = evaluate_and_append_draken(self.evaluatable_nodes, morsel)
    
    # Process aggregates
    for aggregate in self.aggregates:
        if aggregate.node_type != NodeType.AGGREGATOR:
            continue
        
        column_node = aggregate.parameters[0]
        final_column_id = aggregate.schema_column.identity
        collector = self.accumulator[final_column_id]
        
        # Handle LITERAL/WILDCARD (unchanged)
        if column_node.node_type == NodeType.LITERAL:
            if hasattr(collector, 'collect_literal'):
                collector.collect_literal(column_node.value, morsel.num_rows)
            continue
        
        if column_node.node_type == NodeType.WILDCARD:
            if hasattr(collector, 'collect_literal'):
                collector.collect_literal(1, morsel.num_rows)
            continue
        
        # Get column vector
        column_identity = column_node.schema_column.identity
        column_bytes = column_identity.encode() if isinstance(column_identity, str) else column_identity
        column_vector = morsel.column(column_bytes)
        
        # Check if dictionary fast path is available
        if isinstance(collector, DrakenDictionaryAggregateCollector):
            if isinstance(column_vector, DictionaryVector):
                # Check compression ratio before attempting fast path
                compression_ratio = (
                    column_vector.dictionary_size / column_vector.length
                    if column_vector.length > 0
                    else 1.0
                )
                
                if compression_ratio <= DrakenDictionaryAggregateCollector.COMPRESSION_THRESHOLD:
                    # FAST PATH: direct dictionary aggregation
                    try:
                        collector.collect_dictionary_vector(column_vector)
                        continue
                    except (ValueError, TypeError) as e:
                        # Dictionary not suitable (logged but not fatal)
                        # Fall through to standard path
                        if self.telemetry:
                            self.telemetry.log(
                                "dictionary_aggregate_fast_path_failed",
                                column=column_identity,
                                reason=str(e)
                            )
            
            # FALLBACK PATH: convert to Arrow and use standard aggregation
            arrow_array = column_vector.to_arrow() if hasattr(column_vector, 'to_arrow') else column_vector
            collector.collect_fallback(arrow_array)
        
        else:
            # Standard collector path (SimpleAggregateCollector)
            arrow_array = column_vector.to_arrow() if hasattr(column_vector, 'to_arrow') else column_vector
            
            # If Arrow array is dictionary, decode it before passing to Arrow kernels
            if pyarrow.types.is_dictionary(arrow_array.type):
                arrow_array = arrow_array.dictionary_decode()
            
            collector.collect(arrow_array)
```

### 3. Strict Failure Policy

**No Silent Degradation**:

1. **Non-numeric dictionary values**:
   ```python
   # dictionary<string> passed to SUM
   raise ValueError("Dictionary values must be numeric for aggregation, got type_id=...")
   ```

2. **Poor compression ratio**:
   ```python
   # V/N > 0.1 → not worth fast path
   raise ValueError("Dictionary compression ratio exceeds threshold. Use standard path.")
   ```

3. **Unsupported aggregate**:
   ```python
   # HISTOGRAM passed to DrakenDictionaryAggregateCollector
   raise ValueError("DrakenDictionaryAggregateCollector does not support HISTOGRAM")
   ```

All failures are **explicit** and **logged** (if telemetry enabled), then fall back to standard path.

## Testing Strategy

### Unit Tests

```python
def test_dictionary_collector_sum_basic():
    """Test SUM on small dictionary array"""
    from opteryx.compiled.draken.vectors.dictionary_vector import DictionaryVector
    
    # Create dictionary: values=[10, 20, 30], indices=[0, 1, 0, 2, 1, 0]
    # Expected SUM = 10*3 + 20*2 + 30*1 = 100
    dict_vec = create_test_dictionary_vector(
        values=[10, 20, 30],
        indices=[0, 1, 0, 2, 1, 0]
    )
    
    collector = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
    collector.collect_dictionary_vector(dict_vec)
    
    assert collector.get_result() == 100

def test_dictionary_collector_avg_with_nulls():
    """Test AVG with null values in indices"""
    dict_vec = create_test_dictionary_vector(
        values=[10, 20, 30],
        indices=[0, 1, None, 2, 1, 0],  # One null
        null_mask=[False, False, True, False, False, False]
    )
    
    collector = DrakenDictionaryAggregateCollector("AVG", mock_schema_column())
    collector.collect_dictionary_vector(dict_vec)
    
    # (10 + 20 + 30 + 20 + 10) / 5 = 18
    assert collector.get_result() == 18.0

def test_dictionary_collector_count_distinct():
    """Test COUNT(DISTINCT) returns unique values, not indices"""
    dict_vec = create_test_dictionary_vector(
        values=[10, 20, 30],
        indices=[0, 1, 0, 2, 1, 0]  # 6 rows, 3 unique values
    )
    
    collector = DrakenDictionaryAggregateCollector(
        "COUNT", 
        mock_schema_column(),
        duplicate_treatment="Distinct"
    )
    collector.collect_dictionary_vector(dict_vec)
    
    assert collector.get_result() == 3  # Not 6

def test_dictionary_collector_min_max():
    """Test MIN/MAX on dictionary values"""
    dict_vec = create_test_dictionary_vector(
        values=[50, 10, 30, 20],  # Unordered
        indices=[0, 1, 2, 3, 1, 0]
    )
    
    collector_min = DrakenDictionaryAggregateCollector("MIN", mock_schema_column())
    collector_min.collect_dictionary_vector(dict_vec)
    assert collector_min.get_result() == 10
    
    collector_max = DrakenDictionaryAggregateCollector("MAX", mock_schema_column())
    collector_max.collect_dictionary_vector(dict_vec)
    assert collector_max.get_result() == 50

def test_dictionary_collector_poor_compression():
    """Test fallback when compression ratio is poor"""
    # Create dictionary with 90% unique values (poor compression)
    dict_vec = create_test_dictionary_vector(
        values=list(range(900)),
        indices=list(range(1000))  # 900/1000 = 90% unique
    )
    
    collector = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
    
    # Should raise ValueError about compression threshold
    with pytest.raises(ValueError, match="compression ratio.*exceeds threshold"):
        collector.collect_dictionary_vector(dict_vec)

def test_dictionary_collector_non_numeric():
    """Test error on non-numeric dictionary values"""
    dict_vec = create_test_dictionary_vector(
        values=["a", "b", "c"],  # Strings, not numbers
        indices=[0, 1, 0, 2, 1]
    )
    
    collector = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
    
    with pytest.raises(ValueError, match="Dictionary values must be numeric"):
        collector.collect_dictionary_vector(dict_vec)

def test_dictionary_collector_multiple_batches():
    """Test accumulation across multiple batches"""
    dict_vec1 = create_test_dictionary_vector(
        values=[10, 20],
        indices=[0, 1, 0, 1]  # 10*2 + 20*2 = 60
    )
    dict_vec2 = create_test_dictionary_vector(
        values=[10, 30],
        indices=[0, 1, 1]  # 10*1 + 30*2 = 70
    )
    
    collector = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
    collector.collect_dictionary_vector(dict_vec1)
    collector.collect_dictionary_vector(dict_vec2)
    
    # Total: 60 + 70 = 130
    assert collector.get_result() == 130
```

### Integration Tests

```python
def test_clickbench_q04_with_dictionary():
    """Test ClickBench Q04: AVG(UserID) with dictionary encoding"""
    sql = "SELECT AVG(UserID) FROM testdata.clickbench_tiny"
    
    result = opteryx.query(sql).fetchone()
    
    # Should execute without error
    assert result is not None
    assert isinstance(result[0], (int, float))

def test_simple_aggregate_node_dictionary_auto_fallback():
    """Test auto-fallback when dictionary unsuitable"""
    # Create table with both good and poor compression
    table = pyarrow.table({
        'good_dict': create_dictionary_column(values=range(10), size=1000),    # 1% unique
        'poor_dict': create_dictionary_column(values=range(900), size=1000),   # 90% unique
        'plain_col': pyarrow.array(range(1000))
    })
    
    result = opteryx.query("""
        SELECT 
            SUM(good_dict) as sum_good,
            SUM(poor_dict) as sum_poor,
            SUM(plain_col) as sum_plain
        FROM test_table
    """).fetchone()
    
    # All should produce correct results regardless of path taken
    assert result[0] == expected_sum_good
    assert result[1] == expected_sum_poor
    assert result[2] == expected_sum_plain
```

### Performance Tests

```python
def test_dictionary_aggregation_speedup():
    """Benchmark dictionary fast path vs materialization"""
    import time
    
    # Create high-compression dictionary (100 unique values, 1M rows)
    dict_vec = create_large_dictionary_vector(
        unique_values=100,
        total_rows=1_000_000
    )
    
    # Test fast path
    collector_fast = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
    start = time.perf_counter()
    collector_fast.collect_dictionary_vector(dict_vec)
    fast_result = collector_fast.get_result()
    fast_time = time.perf_counter() - start
    
    # Test standard path (decode + sum)
    arrow_array = dict_vec.to_arrow().dictionary_decode()
    start = time.perf_counter()
    standard_result = pyarrow.compute.sum(arrow_array).as_py()
    standard_time = time.perf_counter() - start
    
    # Results should match
    assert abs(fast_result - standard_result) < 1e-6
    
    # Fast path should be faster (at least 2x for 100x compression)
    speedup = standard_time / fast_time
    assert speedup > 2.0, f"Expected >2x speedup, got {speedup:.2f}x"
    
    print(f"Speedup: {speedup:.2f}x ({standard_time:.3f}s → {fast_time:.3f}s)")

def test_dictionary_aggregation_various_compressions():
    """Test performance at different compression ratios"""
    test_cases = [
        (10, 1_000_000, "10 unique, 1M rows"),      # 100,000x compression
        (100, 1_000_000, "100 unique, 1M rows"),    # 10,000x compression  
        (1000, 1_000_000, "1K unique, 1M rows"),    # 1,000x compression
        (10000, 1_000_000, "10K unique, 1M rows"),  # 100x compression
        (50000, 1_000_000, "50K unique, 1M rows"),  # 20x compression
    ]
    
    for unique, total, label in test_cases:
        dict_vec = create_large_dictionary_vector(unique, total)
        
        # Measure fast path
        collector = DrakenDictionaryAggregateCollector("SUM", mock_schema_column())
        start = time.perf_counter()
        collector.collect_dictionary_vector(dict_vec)
        fast_time = time.perf_counter() - start
        
        # Measure standard path
        arrow_array = dict_vec.to_arrow().dictionary_decode()
        start = time.perf_counter()
        pyarrow.compute.sum(arrow_array)
        standard_time = time.perf_counter() - start
        
        speedup = standard_time / fast_time
        print(f"{label}: {speedup:.2f}x speedup")
```

## Performance Impact

### Expected Speedups

| Compression Ratio | Unique Values | Total Rows | Expected Speedup | Use Case |
|-------------------|---------------|------------|------------------|----------|
| 10,000x | 100 | 1M | 50-100x | Country codes |
| 1,000x | 1K | 1M | 10-20x | Status/category |
| 100x | 10K | 1M | 5-10x | User segments |
| 10x | 100K | 1M | 2-3x | High-cardinality IDs |
| 2x | 500K | 1M | 1.2-1.5x | Timestamps (poor) |

### ClickBench Q04 Expectation

**Query**: `SELECT AVG(UserID) FROM testdata.clickbench_tiny`

**Before** (with decode workaround):
- Decode dictionary: ~5ms
- Sum 100K values: ~2ms
- Total: ~7ms

**After** (dictionary-aware):
- Count index frequencies: ~3ms (hash table)
- Weighted sum of ~10K values: ~0.5ms
- Total: ~3.5ms
- **Speedup: ~2x**

### Memory Impact

**Fast Path**:
- Input: dictionary vector (103KB for 100K × uint16 indices + 10K × int64 values)
- Intermediate: hash map (10K entries × 16 bytes = 160KB)
- Output: scalar (8 bytes)
- **Peak: ~270KB**

**Standard Path**:
- Input: dictionary vector (103KB)
- Decoded: materialized array (800KB for 100K × int64)
- Output: scalar (8 bytes)
- **Peak: ~900KB**

**Memory savings: ~3.3x**

## Implementation Checklist

- [ ] Implement `DrakenDictionaryAggregateCollector` class
- [ ] Add compression threshold detection
- [ ] Add numeric type validation
- [ ] Implement weighted SUM/AVG logic
- [ ] Implement MIN/MAX fast path
- [ ] Implement COUNT(DISTINCT) fast path
- [ ] Add fallback path with telemetry
- [ ] Integrate into `SimpleAggregateNode.__init__`
- [ ] Integrate into `SimpleAggregateNode.execute`
- [ ] Add unit tests for all aggregate types
- [ ] Add unit tests for edge cases (nulls, poor compression, non-numeric)
- [ ] Add integration test for ClickBench Q04
- [ ] Add performance benchmarks
- [ ] Verify Q04 passes and is faster
- [ ] Add telemetry for fast path usage tracking

## Future Enhancements

### Phase 2: GROUP BY Dictionary Aggregation

Extend dictionary-aware aggregation to `DrakenAggregateAndGroupNode`:

```python
# GROUP BY country, AVG(value) where country is dictionary
# Can aggregate within each dictionary bucket directly
```

### Phase 3: Multi-Column Dictionary Aggregation

Optimize queries with multiple dictionary columns:

```python
# SELECT country, status, AVG(value) 
# Both country and status are dictionaries
# Can use Cartesian product of dictionaries for grouping
```

### Phase 4: Compiled Draken Backend

Move hot loops to Rust/Cython:
- Index counting via compiled hash table
- Weighted aggregation in native code
- Expected additional speedup: 2-3x

## Related Issues

- ClickBench Q04 failure
- General dictionary encoding performance
- Memory efficiency for high-cardinality columns

## Alternative Approaches Considered

### 1. Always Decode Dictionaries

**Rejected**: Defeats purpose of dictionary encoding, wastes memory

### 2. Special-Case in Arrow Kernels

**Rejected**: Not our code, would require upstream contribution

### 3. Lazy Evaluation with Dictionary Algebra

**Rejected**: Too complex, limited applicability beyond aggregation

## Conclusion

Dictionary-aware aggregation is a **high-value optimization** for low-cardinality numeric columns. The design:

1. ✅ Opportunistic (auto-detects when beneficial)
2. ✅ Safe (strict validation, explicit fallback)
3. ✅ Testable (clear boundaries, easy to verify)
4. ✅ Measurable (telemetry tracks fast path usage)

Expected impact: **2-100x speedup** for categorical/dimensional aggregates, minimal overhead otherwise.
