This is our example document, two records

~~~json
{"name": "bonnie", "age": 37}
{"name": "brian", "age": null}
~~~

The first thing we do is extract the structural markers

{ 0
" 1
" 6
: 7
" 9
" 16
, 17
" 19
" 23
: 24
} 28
\n 29
{ 30
" 31
" 36
: 37
" 39
" 45
, 46
" 48
" 52
: 53
} 59

This gives us enough information to build a map

[
  [
    field:
      key_start: 2
      key_width: 4
      value_start: 10
      value_width: 5
      value_type: quoted
    field:
      key_start: 20
      key_width: 3
      value_start: 25
      value_width: 3
      value_type: unquoted
  ],
  [
    field:
      key_start: 32
      key_width: 4
      value_start: 40
      value_width: 5
      value_type: quoted
    field:
      key_start: 49
      key_width: 3
      value_start: 54
      value_width: 58
      value_type: unquoted
  ]
]

So now when we read this chunk this is what we do, let's say we only want the 'name' column

last_seen = 0  // default to any value
key_width = len('name')
vector_buffer = malloced buffer
for map_of_record in list_of_maps:
  if last_seen <= len(map_of_record):
      map_of_field = map_of_record[last_seen]
      if key_width = map_of_field.key_width:
          if key = buffer[map_of_field.key_start:map_of_field.key_start+key_width]:
            value = buffer[map_of_field.value_start:map_of_field.value_start+map_of_field.value_width]
            malloced_buffer.append(value)
            continue
  for i in range(len(map_of_record)):
      map_of_field = map_of_record[i]
      if key_width = map_of_field.key_width:
          if key = buffer[map_of_field.key_start:map_of_field.key_start+key_width]:
            value = buffer[map_of_field.value_start:map_of_field.value_start+map_of_field.value_width]
            malloced_buffer.append(value)
            last_seen = i
            continue
  malloced_buffer.append(null)

string_vector = StringVector(malloced_buffer)
if column is float:
    return vector_ops_cast_float_from_string(string_vector)
if column is int:
    return vector_ops_cast_int_from_string(string_vector)
if column is bool:
    return vector_ops_cast_bool_from_string(string_vector)
