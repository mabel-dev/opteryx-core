
-----

**ARCHITECTURE CHANGE**: Legacy aggregate planner path removed
- Physical planner no longer falls back to `SimpleAggregateNode`, `SimpleAggregateAndGroupNode`, `AggregateNode`, or `AggregateAndGroupNode`
- Aggregate / GROUP BY planning is now Draken-only; unsupported shapes fail fast with `UnsupportedSyntaxError`
- The legacy aggregate operator files have now been deleted; only shared helper metadata remains in `aggregate_helpers.py`
- Inner join planning is now also Draken-only; `InnerJoinNode` and its feature flag have been removed
- This intentionally exposes currently hidden correctness gaps instead of masking them behind alternate execution paths
- Newly visible buckets from the quick battery rerun:
  - grouped aggregate shapes rejected by `DrakenAggregateAndGroupNode.supports(...)`
  - grouped `ROUND(...)` / grouped `CASE` execution failures
  - `HAVING` / grouped alias semantics returning wrong rows or wrong error types

-----

**INVESTIGATING**: Regression errors and segfaults in test suite
- Observable failures when running 'make t' (test suite):
  - Test 0049 in test_shapes_basic.py segfaults (GROUP BY operation)
  - Many testdata.* queries fail with DatasetNotFoundError
  - GENERATE_SERIES tests fail
  - Array operaton tests fail
- **Quick Win - FIXED**: testdata path resolution issue
  - Root cause: Tests were run from test directory, not repo root
  - Fix: Added `os.chdir()` to repo root in __main__ blocks of test files
  - Fixed files: test_shapes_basic.py, test_shapes_data_sources.py
  - Result: testdata.* queries now work, noise greatly reduced
- **Segfault Root Cause Identified**: Multiple aggregates in GROUP BY
  - Trigger: `SELECT planetId, COUNT(*), MAX(id) FROM testdata.satellites GROUP BY planetId`
  - Single aggregates work fine
  - Virtual data ($planets) with multi-aggregates works fine
  - Only fails with multiple aggregates on parquet data
  - Root cause: In `carchar_group_state_engine.pyx` at line 4894, `_build_chunk_morsel_multi()`
  - Issue: Logic for handling multiple aggregates with object state (MIN/MAX on strings) appears faulty
  - Likely cause: Improper vector initialization or bounds checking in multi-aggregate output building
  - Next: Need to debug the finalize path in carchar_group_state_engine for multi-agg case

---- 

'make clickbench' gets killed on Q14

----

Unary Ops aren't in a catalog

----

README

~~~
TOON           ████████████████████   27.7 acc%/1K tok  │  76.4% acc  │  2,759 tokens
JSON compact   █████████████████░░░   23.7 acc%/1K tok  │  73.7% acc  │  3,104 tokens
YAML           ██████████████░░░░░░   19.9 acc%/1K tok  │  74.5% acc  │  3,749 tokens
JSON           ████████████░░░░░░░░   16.4 acc%/1K tok  │  75.0% acc  │  4,587 tokens
XML            ██████████░░░░░░░░░░   13.8 acc%/1K tok  │  72.1% acc  │  5,221 tokens
~~~

> [!TIP]
> TOON achieves **76.4%** accuracy (vs JSON's 75.0%) while using **39.9% fewer tokens**.
