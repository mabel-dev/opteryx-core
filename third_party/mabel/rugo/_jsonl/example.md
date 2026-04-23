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

This is where the current step finishes - ask if you want to know how we use this to get actual values from the document
