suite: /Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/performance/clickbench/clickbench.py

DRAKEN GROUP BY ENABLED

Query    Iteration 1      Iteration 2      Iteration 3              Avg           Min           Max          
------------------------------------------------------------------------------------------------------
RUNNING CLICKBENCH BATTERY OF 43 QUERIES

Q01      258.72ms         -                -                   258.72ms      258.72ms      258.72ms
Q02      861.87ms         -                -                   861.87ms      861.87ms      861.87ms
Q03      951.66ms         -                -                   951.66ms      951.66ms      951.66ms
Q04      1206.32ms        -                -                  1206.32ms     1206.32ms     1206.32ms
Q05      2837.79ms        -                -                  2837.79ms     2837.79ms     2837.79ms ⚠️ SLOW
Q06      2441.96ms        -                -                  2441.96ms     2441.96ms     2441.96ms ⚠️ SLOW
Q07      814.31ms         -                -                   814.31ms      814.31ms      814.31ms
/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/performance/clickbench/../../../opteryx/operators/draken_aggregate_and_group_node.py:202: UserWarning: Draken fast-finalize chunked path unavailable; falling back to generic finalize_rows()
  for result in self._group_by.finalize_morsels(chunk_size=CHUNK_SIZE):
Q08      1707.41ms        -                -                  1707.41ms     1707.41ms     1707.41ms
Q09      4192.69ms        -                -                  4192.69ms     4192.69ms     4192.69ms ⚠️ SLOW
Q10      48142.45ms       -                -                 48142.45ms    48142.45ms    48142.45ms ⚠️ VERY SLOW
/Users/justin/Nextcloud/opteryx-cloud/opteryx-core/tests/performance/clickbench/../../../opteryx/operators/draken_aggregate_and_group_node.py:202: UserWarning: Draken fast-finalize path unavailable; falling back to generic finalize_rows()
  for result in self._group_by.finalize_morsels(chunk_size=CHUNK_SIZE):
Q11      4820.08ms        -                -                  4820.08ms     4820.08ms     4820.08ms ⚠️ SLOW
Q12      2153.66ms        -                -                  2153.66ms     2153.66ms     2153.66ms ⚠️ SLOW
Q13      14373.92ms       -                -                 14373.92ms    14373.92ms    14373.92ms ⚠️ VERY SLOW
Q14      28202.42ms       -                -                 28202.42ms    28202.42ms    28202.42ms ⚠️ VERY SLOW
Q15      17431.32ms       -                -                 17431.32ms    17431.32ms    17431.32ms ⚠️ VERY SLOW
Q16      4548.89ms        -                -                  4548.89ms     4548.89ms     4548.89ms ⚠️ SLOW
Q17      97523.77ms       -                -                 97523.77ms    97523.77ms    97523.77ms ⚠️ VERY SLOW
Q18      89949.49ms       -                -                 89949.49ms    89949.49ms    89949.49ms ⚠️ VERY SLOW
Q19      105228.48ms      -                -                105228.48ms   105228.48ms   105228.48ms ⚠️ VERY SLOW
Q20      3547.46ms        -                -                  3547.46ms     3547.46ms     3547.46ms ⚠️ SLOW
Q21      9845.26ms        -                -                  9845.26ms     9845.26ms     9845.26ms ⚠️ VERY SLOW
Q22      3047.53ms        -                -                  3047.53ms     3047.53ms     3047.53ms ⚠️ SLOW
Q23      5804.10ms        -                -                  5804.10ms     5804.10ms     5804.10ms ⚠️ VERY SLOW
Q24      34631.72ms       -                -                 34631.72ms    34631.72ms    34631.72ms ⚠️ VERY SLOW
Q25      2357.69ms        -                -                  2357.69ms     2357.69ms     2357.69ms ⚠️ SLOW
Q26      2976.43ms        -                -                  2976.43ms     2976.43ms     2976.43ms ⚠️ SLOW
Q27      2133.73ms        -                -                  2133.73ms     2133.73ms     2133.73ms ⚠️ SLOW
Q28      50064.57ms       -                -                 50064.57ms    50064.57ms    50064.57ms ⚠️ VERY SLOW
Q29      ERROR: Draken aggregator does not support this query shape
Q30      1059.91ms        -                -                  1059.91ms     1059.91ms     1059.91ms
Q31      31584.59ms       -                -                 31584.59ms    31584.59ms    31584.59ms ⚠️ VERY SLOW
Q32      130345.39ms      -                -                130345.39ms   130345.39ms   130345.39ms ⚠️ VERY SLOW
Q33      ERROR: No statement found
Q34      85990.52ms       -                -                 85990.52ms    85990.52ms    85990.52ms ⚠️ VERY SLOW
Q35      87050.24ms       -                -                 87050.24ms    87050.24ms    87050.24ms ⚠️ VERY SLOW
Q36      55846.45ms       -                -                 55846.45ms    55846.45ms    55846.45ms ⚠️ VERY SLOW
Q37      2165.13ms        -                -                  2165.13ms     2165.13ms     2165.13ms ⚠️ SLOW
Q38      1030.63ms        -                -                  1030.63ms     1030.63ms     1030.63ms
Q39      928.05ms         -                -                   928.05ms      928.05ms      928.05ms
Q40      2082.15ms        -                -                  2082.15ms     2082.15ms     2082.15ms ⚠️ SLOW
Q41      986.87ms         -                -                   986.87ms      986.87ms      986.87ms
Q42      983.70ms         -                -                   983.70ms      983.70ms      983.70ms
Q43      ERROR: Draken aggregator does not support this query shape

DRAKEN GROUP BY DISABLED

Query    Iteration 1      Iteration 2      Iteration 3              Avg           Min           Max          
------------------------------------------------------------------------------------------------------
RUNNING CLICKBENCH BATTERY OF 43 QUERIES

Q01      247.59ms         250.35ms         243.68ms            247.21ms      243.68ms      250.35ms
Q02      939.89ms         779.77ms         715.80ms            811.82ms      715.80ms      939.89ms
Q03      945.01ms         763.92ms         762.81ms            823.91ms      762.81ms      945.01ms
Q04      1192.45ms        742.52ms         736.22ms            890.40ms      736.22ms     1192.45ms
Q05      2954.26ms        2855.08ms        3110.03ms          2973.12ms     2855.08ms     3110.03ms ⚠️ SLOW
Q06      2277.07ms        1922.96ms        1968.15ms          2056.06ms     1922.96ms     2277.07ms
Q07      806.35ms         729.73ms         720.24ms            752.11ms      720.24ms      806.35ms
Q08      759.89ms         761.80ms         830.34ms            784.01ms      759.89ms      830.34ms
Q09      4285.48ms        3417.01ms        3263.32ms          3655.27ms     3263.32ms     4285.48ms ⚠️ SLOW
Q10      4364.18ms        4046.89ms        4020.54ms          4143.87ms     4020.54ms     4364.18ms ⚠️ SLOW
Q11      1261.15ms        1000.17ms        996.59ms           1085.97ms      996.59ms     1261.15ms
Q12      1074.42ms        1046.36ms        1053.79ms          1058.19ms     1046.36ms     1074.42ms
Q13      7104.49ms        6413.47ms        6490.23ms          6669.40ms     6413.47ms     7104.49ms ⚠️ VERY SLOW
Q14      7739.64ms        7187.98ms        7469.51ms          7465.71ms     7187.98ms     7739.64ms ⚠️ VERY SLOW
Q15      7676.30ms        7262.08ms        7128.35ms          7355.58ms     7128.35ms     7676.30ms ⚠️ VERY SLOW
Q16      8400.74ms        7514.23ms        7796.42ms          7903.79ms     7514.23ms     8400.74ms ⚠️ VERY SLOW
Q17      20924.54ms       19657.36ms       18016.96ms        19532.95ms    18016.96ms    20924.54ms ⚠️ VERY SLOW
Q18      18656.31ms       19252.24ms       18124.05ms        18677.53ms    18124.05ms    19252.24ms ⚠️ VERY SLOW
Q19      41247.17ms       38986.00ms       38416.79ms        39549.99ms    38416.79ms    41247.17ms ⚠️ VERY SLOW
Q20      1150.43ms        715.50ms         710.61ms            858.85ms      710.61ms     1150.43ms
Q21      9775.98ms        7388.31ms        7532.94ms          8232.41ms     7388.31ms     9775.98ms ⚠️ VERY SLOW
Q22      2875.60ms        2634.52ms        2740.17ms          2750.10ms     2634.52ms     2875.60ms ⚠️ SLOW
Q23      5087.72ms        5197.29ms        4429.51ms          4904.84ms     4429.51ms     5197.29ms ⚠️ SLOW
Q24      32046.01ms       32375.72ms       33000.90ms        32474.21ms    32046.01ms    33000.90ms ⚠️ VERY SLOW
Q25      2212.59ms        1340.49ms        1318.66ms          1623.91ms     1318.66ms     2212.59ms
Q26      2702.83ms        2701.17ms        2730.02ms          2711.34ms     2701.17ms     2730.02ms ⚠️ SLOW
Q27      1990.79ms        2028.55ms        1992.83ms          2004.06ms     1990.79ms     2028.55ms
Q28      34587.08ms       34992.79ms       35665.94ms        35081.94ms    34587.08ms    35665.94ms ⚠️ VERY SLOW
Q29      150514.33ms      150518.95ms      152321.21ms      151118.17ms   150514.33ms   152321.21ms ⚠️ VERY SLOW
Q30      1247.45ms        952.76ms         888.82ms           1029.68ms      888.82ms     1247.45ms
Q31      4893.14ms        4250.53ms        4083.47ms          4409.05ms     4083.47ms     4893.14ms ⚠️ SLOW
Q32      6755.59ms        5767.28ms        5819.95ms          6114.27ms     5767.28ms     6755.59ms ⚠️ VERY SLOW
Q33      ERROR: No statement found
Q34      61785.52ms       64382.69ms       63870.13ms        63346.11ms    61785.52ms    64382.69ms ⚠️ VERY SLOW
Q35      72673.28ms       70875.07ms       69133.09ms        70893.81ms    69133.09ms    72673.28ms ⚠️ VERY SLOW
Q36      27602.27ms       19717.75ms       19388.26ms        22236.10ms    19388.26ms    27602.27ms ⚠️ VERY SLOW
Q37      1205.61ms        1044.00ms        1066.38ms          1105.33ms     1044.00ms     1205.61ms
Q38      931.37ms         902.64ms         925.38ms            919.80ms      902.64ms      931.37ms
Q39      885.19ms         875.04ms         889.57ms            883.26ms      875.04ms      889.57ms
Q40      1742.17ms        1627.27ms        1618.51ms          1662.65ms     1618.51ms     1742.17ms
Q41      929.49ms         881.82ms         873.02ms            894.78ms      873.02ms      929.49ms
Q42      912.05ms         893.13ms         884.25ms            896.47ms      884.25ms      912.05ms
Q43      1038.27ms        1022.20ms        1033.27ms          1031.24ms     1022.20ms     1038.27ms






Feature	Impact	Queries
Multiple Aggregates	6 queries	Q10, Q22, Q23, Q29, Q31, Q32
MIN() Aggregation	3 queries	Q22, Q23, Q29
GROUP BY Expression (non-column)	2 queries	Q29, Q43
AVG() Aggregation	3 queries	Q10, Q23, Q31, Q32
SUM() Aggregation	3 queries	Q10, Q31, Q32