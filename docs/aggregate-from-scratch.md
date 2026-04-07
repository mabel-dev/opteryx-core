
aggregates have two primary variations, grouped and ungrouped.

## Common

A structure to hold the aggregates should be created:


## Ungrouped

The aggregates are stored in a structure something like this:

~~~
aggregates = [
  min_aggregate(column_name="column"),
  max_aggregate(column_name="column")
]
~~~

For each aggregate, the local aggregate for the morsel is calculated and then applied to the dataset aggregate.

~~~
# for illustration only, not inteded as code to the used in the system
function apply(self, morsel)
  column_values = morsel[self.column_name]
  morsel_min = min(column_values)
  self.result = min(morsel_min, self.result)
~~~

Putting these two together we get something like

~~~
for aggregate in aggregates:
  aggregate.apply(morsel)
~~~

This doesn't need to be the same code as the grouped aggregates, this can be highly simplified for this situation.

Average is calculated by creating a sum and a count aggregate and determining the average at conclusion.

## Grouped

we're going to rewrite the grouped agg from scratch, in opteryx-core/opteryx/operators/grouped_aggregate_hashed/..

this is going to use the carcharindex as it's backbone and is going to be cython/cpp after the def execute(...) boundary.

morsels provide a hash function which we will use as our grouping, we will use carchar to store offsets in an array - the keys and aggregates in parallel arrays.

~~~
# this is illustrative, not meant to be code to used in the system
# it is not valid python, cython, c or cpp intentionally

aggregate_collectors = [
  min_aggregate(column_name="column", output_column_name="a"),
  max_aggregate(column_name="column", output_column_name="a")
]

class min_aggregate():

  function init(column_name, result_name, estimated_size):
    self.collector_array = vector[max_value_collector].size(estimated_size)
    self.column_name = column_name
    self.result_name = result_name

  function apply(morsel, index, is_new_group):
    value = morsel[self.column_name]
    if is_new_group:
      collector = max_value_collector(value)
    else:
      collector = self.collector_array[index]
      collector.value = max(value, collector.value)
    self.collector_array[index] = collector
    
  function finalize():
    return draken.Vector.from_sequence([collector.value for collector in self.collector_array], name=self.result_name, type=int)

for morsel in dataset:
  group_identities = morsel.hash(group_keys)
  for index in range(len(group_identities)):
    group_identity = group_identities[index]
    is_new_group = bloom_says_might_not_exist(group_identity)
    if is_new_group:
      group_key_store[group_identity] = byte_encoded(morsel, group_keys)
    for aggregate_collector in aggregate_collectors:
      aggregate_collector.apply(morsel, index, is_new_group)

column_collector = [len(group_key_store)]
for index in range(len(group_key_store)):
  group_values = unencode_bytes(group_key_store[index])
  for column_index in range(len(group_by_columns)):
    column_collector[index][column_index] = group_values[column_index]

for column_index in range(len(group_by_columns)):
  group_by_column = draken.Vector.from_sequence(column_collector)

~~~
