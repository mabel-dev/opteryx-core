# Group By, Explained Simply

This note is for reading, not for proving anything.

If you want the shortest possible summary:

- The query reads data in chunks.
- Each chunk gets a few extra computed columns if the `GROUP BY` uses expressions.
- Rows with the same group values are collected together.
- Each group keeps running totals like `COUNT`, `SUM`, `MIN`, `MAX`, or `AVG`.
- At the end, the engine turns those totals back into output rows.
- If the query also has `ORDER BY ... LIMIT`, a separate sort step picks the top rows.

The main files involved are:

- [`opteryx/operators/draken_aggregate_and_group_node.py`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L94)
- [`opteryx/compiled/aggregations/carchar_group_state_engine.pyx`](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/carchar_group_state_engine.pyx#L5801)
- [`opteryx/compiled/aggregations/group_state_store.pyx`](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/group_state_store.pyx#L1400)
- [`opteryx/operators/heap_sort_node.py`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/heap_sort_node.py#L270)

Code anchors worth reading first:

- [`DrakenAggregateAndGroupNode.__init__()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L94) decides which group-by backend gets used.
- [`CarcharGroupStateEngine.finalize_morsels()`](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/carchar_group_state_engine.pyx#L5801) turns grouped state back into output morsels.
- [`GroupStateStore.finalize_morsels()`](/Users/justin/Nextcloud/opteryx-core/opteryx/compiled/aggregations/group_state_store.pyx#L1400) is the fallback path that rebuilds output through Python lists and `vector_from_sequence`.
- [`HeapSortNode._top_n()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/heap_sort_node.py#L270) handles the `ORDER BY ... LIMIT` stage after grouping.

## What group by actually means

Imagine a pile of rows from a table.

`GROUP BY` says:

1. decide which rows belong in the same bucket
2. keep one bucket per unique combination of group values
3. update the bucket as more rows arrive
4. print one final row per bucket

So if you group by:

- `UserID`
- `extract(minute FROM EventTime)`
- `SearchPhrase`

then every row is sorted into a bucket based on that three-part key.

Rows that have the same `UserID`, the same minute, and the same search phrase go into the same bucket.

## What the engine does, in plain English

The important entry point is [`DrakenAggregateAndGroupNode.execute()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L347).

Think of it like this:

1. A chunk of rows arrives.
2. The engine makes sure the chunk is in Draken format.
3. If the query needs computed group values, it computes them first.
4. It adds a hidden `*` column when needed so `COUNT(*)` can be handled easily.
5. It passes the chunk into the group-by backend.
6. When the input ends, it asks the backend to finalize the groups.
7. Final rows are emitted.

That is the whole job.

## Why there are two backends

There are two main ways this can run:

- `CarcharGroupStateEngine` in compiled code, selected from [`__init__()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L157)
- `GroupStateStore` as the slower fallback, selected from the same constructor when expression group keys are present and the opt-in flag is off

The compiled engine is the fast path.
The fallback exists because some query shapes are harder to handle in the compiled path.

In practice, the code tries to stay on the compiled path as much as possible because that is where the speed lives.

## What happens to a single chunk

Here is the chunk flow in human terms:

1. The chunk is trimmed down to only the columns we need.
2. If the query has expressions in the `GROUP BY`, those expressions are computed now.
3. The chunk is handed to the group-by engine.
4. The engine looks at each row.
5. It finds the right bucket for that row.
6. It updates the bucket’s running totals.

Example:

```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*)
FROM testdata.clickbench_tiny
GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase
ORDER BY COUNT(*) DESC
LIMIT 10;
```

For a row like:

```text
UserID = 123
EventTime = 2013-07-14 10:42:11
SearchPhrase = "openai"
```

the engine first computes the minute value:

```text
m = 42
```

Then the group key becomes:

```text
(123, 42, "openai")
```

If that key already exists, the count goes up by one.
If it does not exist, a new bucket is created.

## What happens at the end

When the input is finished, the engine does the final pass:

1. each bucket becomes one output row
2. aggregate state is turned into final values
3. the rows are bundled into output morsels

So internal state like:

```text
count = 17
sum = 921
avg = sum / count
```

becomes a normal row again:

```text
UserID = 123
m = 42
SearchPhrase = "openai"
COUNT(*) = 17
```

## Why `ORDER BY ... LIMIT` shows up in the profile

After group-by finishes, the query often still needs sorting.

That work happens in [`HeapSortNode`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/heap_sort_node.py).

If the query says `ORDER BY COUNT(*) DESC LIMIT 10`, the sorter does not need every row in fully sorted order.
It only needs the best 10.

So it:

1. compares rows by the sort key
2. keeps only the best few
3. materializes the winning rows into the final output

That is why the profile often shows both aggregation time and `_materialize_rows()` time.

## Why expression group-by is special

`GROUP BY` is fastest when it uses plain columns.

It gets a little more expensive when it uses expressions like:

- `extract(minute FROM EventTime)`
- `ClientIP - 1`
- `CASE WHEN ...`

Those expressions must be evaluated before grouping.

That is why the code has a pre-evaluation step in [`DrakenAggregateAndGroupNode.execute()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L395):

- compute the expression
- store the result as a temporary column
- group by that temporary column instead of recomputing it for every comparison

## What the ClickBench 19 profile is telling us

Your ClickBench 19 query is roughly:

```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*)
FROM testdata.clickbench_tiny
GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase
ORDER BY COUNT(*) DESC
LIMIT 10;
```

That means the work splits into three parts:

1. read data
2. compute the minute expression and group rows
3. sort the grouped result and keep the top 10

When the profile shows time in:

- [`DrakenAggregateAndGroupNode.execute()`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/draken_aggregate_and_group_node.py#L347)
- [`heap_sort_node.py:205`](/Users/justin/Nextcloud/opteryx-core/opteryx/operators/heap_sort_node.py#L205)
- [`parquet_io/reader.py:883`](/Users/justin/Nextcloud/opteryx-core/opteryx/parquet_io/reader.py#L883)

it usually means:

- grouping is expensive
- sorting is expensive
- data decoding is expensive

So the query is not slow for one single reason.
It is slow because several normal steps are all doing a lot of work.

## A simple mental model

The group-by engine is basically a cashier with a notebook:

- every row is a customer
- the group key is the customer’s name tag
- the notebook is the hash table of current buckets
- `COUNT` and `SUM` are running tallies in the notebook
- finalization is copying the notebook into clean printed receipts

That is much easier to reason about than the code because the code has to handle:

- nulls
- dictionary-encoded columns
- multiple aggregate types
- expression group keys
- chunked input
- sorting after aggregation

## If you want to keep reading

- [`docs/draken-aggregate-groupby-design.md`](/Users/justin/Nextcloud/opteryx-core/docs/draken-aggregate-groupby-design.md) is the deeper design version.
- [`docs/draken-shuffle-groupby-v2-design.md`](/Users/justin/Nextcloud/opteryx-core/docs/draken-shuffle-groupby-v2-design.md) explains the shuffle-style version.
- [`docs/carchar-hash-table-explained.md`](/Users/justin/Nextcloud/opteryx-core/docs/carchar-hash-table-explained.md) explains the hash-table machinery underneath.
