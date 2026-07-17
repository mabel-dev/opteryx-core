import os, sys
sys.path.insert(1, os.path.join(sys.path[0], ".."))
import opteryx
session = opteryx.session()
sql = '''
SELECT billing_account, TRUNC(mass_sum, 1) AS mass_trunc, SUM(x) AS billable
FROM (
    SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date,
           COUNT(*) - 3 AS x, SUM(mass) AS mass_sum
    FROM (SELECT name AS billing_account, FROM_UNIXTIME(id*3600) AS billing_hour, mass FROM $planets) AS inner_t
    GROUP BY billing_account, TRUNC(billing_hour, 'day')
) AS mid_t
GROUP BY ALL
'''
m = list(session.execute_to_morsels(sql))
print(m)
