import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "."))

import opteryx

session = opteryx.session()
SQL = "/* 34 */ SELECT URL, COUNT(*) AS c FROM scratch.hits GROUP BY URL ORDER BY c DESC LIMIT 10;"

morsels = session.execute_to_morsels(SQL)
for _ in morsels:
    pass
