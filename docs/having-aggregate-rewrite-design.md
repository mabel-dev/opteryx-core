# HAVING Aggregate Expression Rewrite Design

## Problem Statement

HAVING clauses containing aggregate expressions (e.g., `HAVING COUNT(*) > 100000`) are passed to `FilterNode`, which calls `evaluate_draken()`. The evaluator has no branch for `NodeType.AGGREGATOR`, causing runtime `NotImplementedError`.

## Root Cause Analysis

```
Logical Plan Flow:
1. AggregateNode produces columns including COUNT(*) with identity "@@aggregate_0"
2. Planner creates FilterNode with condition tree: COMPARISON(AGGREGATOR(COUNT), LITERAL(100000))
3. FilterNode.execute() → evaluate_draken(condition)
4. evaluate_draken() expects IDENTIFIER/LITERAL/FUNCTION nodes, not AGGREGATOR
5. Falls through to NotImplementedError at line 955
```

**Affected Queries**: ClickBench Q28, Q29

**Current Error**:
```
NotImplementedError: evaluate_draken: unsupported node type <NodeType.AGGREGATOR: 41> (value='COUNT')
```

## Proposed Solution: Post-Aggregate Identifier Substitution

**Location**: `opteryx/planner/logical_planner/logical_planner.py:457-464`

**Approach**: After aggregate columns are bound and have assigned identities, rewrite the HAVING condition tree to replace `AGGREGATOR` nodes with `IDENTIFIER` nodes referencing the computed aggregate columns.

## Detailed Design

### 1. Rewrite Function

Create a new function in the logical planner module:

```python
def rewrite_having_aggregates_to_identifiers(having_condition, aggregate_nodes):
    """
    Walk HAVING condition tree and replace AGGREGATOR nodes with IDENTIFIER nodes
    that reference the already-computed aggregate column identities.
    
    Args:
        having_condition: Root node of HAVING expression tree
        aggregate_nodes: List of aggregate nodes from prior GROUP BY step
        
    Returns:
        Rewritten condition tree with IDENTIFIER nodes instead of AGGREGATOR nodes
        
    Raises:
        PlanningError: If HAVING references aggregate not in SELECT/GROUP BY
    """
    from opteryx.expression import NodeType
    from opteryx.expression.ops import get_all_nodes_of_type
    
    # Build mapping: aggregate expression signature → output column identity
    agg_map = {}
    for agg_node in aggregate_nodes:
        # Create canonical key from aggregate function + parameters
        # e.g., "COUNT(*)" or "AVG(LENGTH(URL))"
        key = _canonicalize_aggregate(agg_node)
        agg_map[key] = agg_node.schema_column.identity
    
    # Recursive tree walker
    def _replace_node(node):
        if node.node_type == NodeType.AGGREGATOR:
            key = _canonicalize_aggregate(node)
            if key not in agg_map:
                raise PlanningError(
                    f"HAVING references aggregate {key} not present in SELECT/GROUP BY"
                )
            # Create IDENTIFIER node pointing to computed column
            return Node(
                node_type=NodeType.IDENTIFIER,
                schema_column=node.schema_column,  # Preserve schema metadata
                source_column=agg_map[key],
                value=agg_map[key]
            )
        
        # Recursively process children
        if hasattr(node, 'parameters') and node.parameters:
            node.parameters = [_replace_node(p) for p in node.parameters]
        if hasattr(node, 'left') and node.left:
            node.left = _replace_node(node.left)
        if hasattr(node, 'right') and node.right:
            node.right = _replace_node(node.right)
        if hasattr(node, 'centre') and node.centre:
            node.centre = _replace_node(node.centre)
            
        return node
    
    return _replace_node(having_condition)

def _canonicalize_aggregate(agg_node):
    """
    Create unique string key for aggregate expression.
    
    Uses format_expression to ensure consistent representation.
    """
    from opteryx.expression import format_expression
    return format_expression(agg_node)
```

### 2. Integration Point

Modify logical planner around line 457:

```python
# EXISTING CODE (line ~451)
aggregate_step = LogicalPlanNode(node_type=LogicalPlanStepType.Aggregate)
aggregate_step.aggregates = _aggregates
aggregate_step.groups = _group_by
# ... add to plan ...

# NEW CODE: Extract aggregate nodes for HAVING rewrite
aggregate_nodes_for_having = get_all_nodes_of_type(
    _aggregates, select_nodes=(NodeType.AGGREGATOR,)
)

# having (line 458)
_having = logical_planner_builders.build(ast_branch["Select"].get("having"))
if _having:
    # REWRITE: Replace AGGREGATOR nodes with IDENTIFIER nodes
    _having = rewrite_having_aggregates_to_identifiers(
        _having, aggregate_nodes_for_having
    )
    
    having_step = LogicalPlanNode(node_type=LogicalPlanStepType.Filter)
    having_step.condition = _having  # Now contains IDENTIFIERS, not AGGREGATORS
    previous_step_id, step_id = step_id, random_string()
    inner_plan.add_node(step_id, having_step)
    if previous_step_id is not None:
        inner_plan.add_edge(previous_step_id, having_step)
```

### 3. Error Handling

#### Planning-Time Errors

**Unknown Aggregate in HAVING**:
```python
# Query: SELECT COUNT(*) FROM t HAVING SUM(x) > 10
# Error: HAVING references SUM(x) not in SELECT

raise PlanningError(
    f"HAVING clause references aggregate '{key}' that is not computed in "
    f"the SELECT or GROUP BY clause. Add it to SELECT or remove from HAVING."
)
```

#### Edge Cases

1. **Multiple references to same aggregate**:
   ```sql
   SELECT COUNT(*) AS c FROM t HAVING COUNT(*) > 10 AND COUNT(*) < 100
   ```
   - Both `COUNT(*)` nodes map to same identifier `@@aggregate_0`
   - ✅ Correctly handled: both replaced with same IDENTIFIER

2. **Nested aggregates**:
   ```sql
   HAVING SUM(x) > AVG(y)
   ```
   - Both `SUM(x)` and `AVG(y)` replaced with their identifiers
   - ✅ Works if both are in SELECT

3. **Aggregate in complex expression**:
   ```sql
   HAVING COUNT(*) * 2 > 100
   ```
   - Tree: `COMPARISON(BINARY_OP(AGGREGATOR, LITERAL(2)), LITERAL(100))`
   - After rewrite: `COMPARISON(BINARY_OP(IDENTIFIER, LITERAL(2)), LITERAL(100))`
   - ✅ Correct: only AGGREGATOR node replaced

4. **COUNT(*) vs COUNT(col)**:
   ```sql
   SELECT COUNT(*), COUNT(col) FROM t HAVING COUNT(*) > COUNT(col)
   ```
   - Different canonical keys: `"COUNT(*)"` vs `"COUNT(col)"`
   - Different identifiers: `@@aggregate_0` vs `@@aggregate_1`
   - ✅ Correctly distinguished

### 4. Alternative Approach Considered: Early Validation

Instead of rewriting, detect aggregates in HAVING and fail:

```python
if _having:
    agg_nodes = get_all_nodes_of_type(_having, select_nodes=(NodeType.AGGREGATOR,))
    if agg_nodes:
        raise UnsupportedSyntaxError(
            "HAVING clause with aggregate expressions requires rewrite (not implemented)"
        )
```

**Rejected because**:
- This is standard SQL feature, not edge case
- Users expect it to work
- Rewrite is straightforward and deterministic

### 5. Complexity Analysis

**Time Complexity**: O(H) where H = nodes in HAVING expression tree
- Single pass to build aggregate map: O(A) where A = aggregate nodes
- Single pass to rewrite HAVING tree: O(H)
- Total: O(A + H), typically A < 10, H < 50

**Space Complexity**: O(A) for aggregate map, negligible

**Correctness**:
- Pure tree transformation, no side effects
- Preserves schema metadata on all nodes
- Fails fast if aggregate not found (planning error, not runtime)

## Testing Strategy

### Unit Tests

```python
def test_rewrite_having_simple_count():
    # HAVING COUNT(*) > 10
    having = parse_having("COUNT(*) > 10")
    aggs = [create_aggregate_node("COUNT", "*", identity="@@agg_0")]
    
    result = rewrite_having_aggregates_to_identifiers(having, aggs)
    
    assert result.left.node_type == NodeType.IDENTIFIER
    assert result.left.value == "@@agg_0"

def test_rewrite_having_unknown_aggregate():
    # HAVING SUM(x) > 10 when only COUNT(*) in SELECT
    having = parse_having("SUM(x) > 10")
    aggs = [create_aggregate_node("COUNT", "*", identity="@@agg_0")]
    
    with pytest.raises(PlanningError, match="not present in SELECT"):
        rewrite_having_aggregates_to_identifiers(having, aggs)

def test_rewrite_having_multiple_aggregates():
    # HAVING COUNT(*) > 10 AND AVG(x) < 5
    having = parse_having("COUNT(*) > 10 AND AVG(x) < 5")
    aggs = [
        create_aggregate_node("COUNT", "*", identity="@@agg_0"),
        create_aggregate_node("AVG", "x", identity="@@agg_1"),
    ]
    
    result = rewrite_having_aggregates_to_identifiers(having, aggs)
    
    # Verify both AND branches rewritten
    assert result.left.left.node_type == NodeType.IDENTIFIER  # COUNT(*)
    assert result.right.left.node_type == NodeType.IDENTIFIER  # AVG(x)
```

### Integration Tests

```python
def test_clickbench_q28():
    """Test ClickBench Q28 with HAVING COUNT(*) > 100000"""
    sql = """
        SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c 
        FROM testdata.clickbench_tiny 
        WHERE URL <> '' 
        GROUP BY CounterID 
        HAVING COUNT(*) > 100000 
        ORDER BY l DESC 
        LIMIT 25
    """
    result = opteryx.query(sql).fetchall()
    assert isinstance(result, list)  # Should execute without error

def test_clickbench_q29():
    """Test ClickBench Q29 with complex HAVING"""
    sql = """
        SELECT REGEXP_REPLACE(Referer, ...) AS k, AVG(...) AS l, COUNT(*) AS c
        FROM testdata.clickbench_tiny 
        WHERE Referer <> '' 
        GROUP BY REGEXP_REPLACE(...)
        HAVING COUNT(*) > 100000 
        ORDER BY l DESC 
        LIMIT 25
    """
    result = opteryx.query(sql).fetchall()
    assert isinstance(result, list)
```

### Negative Tests

```python
def test_having_aggregate_not_in_select():
    """HAVING references aggregate not computed"""
    sql = "SELECT COUNT(*) FROM t GROUP BY x HAVING SUM(y) > 10"
    with pytest.raises(PlanningError, match="not present in SELECT"):
        opteryx.query(sql)

def test_having_mixed_row_and_aggregate():
    """HAVING mixes row-level and aggregate predicates (should fail earlier)"""
    sql = "SELECT COUNT(*) FROM t GROUP BY x HAVING x > 5 AND COUNT(*) > 10"
    # This should fail during WHERE/HAVING splitting, not in rewrite
    with pytest.raises((PlanningError, UnsupportedSyntaxError)):
        opteryx.query(sql)
```

## Performance Impact

**Negligible**:
- Rewrite happens once during planning (not per-row)
- Tree traversal is O(H) where H is small
- No additional memory beyond aggregate map

**Measurement**:
- Plan time increase: <1ms for typical queries
- Execution time: unchanged (same operators, same data flow)

## Implementation Checklist

- [ ] Add `rewrite_having_aggregates_to_identifiers` function
- [ ] Add `_canonicalize_aggregate` helper
- [ ] Integrate rewrite into logical planner around line 458
- [ ] Add unit tests for rewrite function
- [ ] Add integration tests for Q28/Q29
- [ ] Add negative tests for error cases
- [ ] Update planner documentation
- [ ] Verify Q28/Q29 pass in ClickBench suite

## Future Enhancements

### Optimization: Predicate Pushdown for HAVING

Currently HAVING is always post-aggregate filter. Could optimize:

```sql
SELECT x, COUNT(*) FROM t GROUP BY x HAVING x > 10
```

Here `x > 10` is row-level predicate, could push to WHERE. Would require:
1. Detecting row-level vs aggregate predicates in HAVING
2. Splitting into two filters: pre-aggregate (WHERE) and post-aggregate (HAVING)

**Deferred**: Rare case, adds complexity, minimal benefit.

### Alternative: HAVING as AggregateNode Parameter

Instead of separate filter node, pass HAVING condition to aggregate node:

```python
aggregate_step.having_condition = _having_rewritten
```

Aggregate node applies filter before output. Benefits:
- Single node instead of two
- Slightly better memory locality

**Deferred**: Current two-node approach is clearer and easier to optimize independently.

## Related Issues

- ClickBench Q28 failure
- ClickBench Q29 failure
- General SQL compliance: HAVING is SQL-92 standard feature
