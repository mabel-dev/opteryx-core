import os, sys
sys.path.insert(0, os.path.join(sys.path[0], ".."))
import opteryx

def test_scalar_correlated_subquery():
    session = opteryx.session()
    results = list(session.execute_to_morsels(
        "SELECT name FROM $planets p WHERE mass = (SELECT MAX(mass) FROM $planets p2 WHERE p2.id = p.id)"
    ))
    rows = [r for r in results if r is not None]
    assert sum(r.num_rows for r in rows) > 0

if __name__ == "__main__":
    test_scalar_correlated_subquery()
    print("OK")
