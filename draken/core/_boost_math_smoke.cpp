// draken/core/_boost_math_smoke.cpp — compile-time + runtime smoke test for vendored boost::math.
//
// Proves:
//   1. The vendor slice is complete enough to compile boost::math::round.
//   2. round(0.5) == 0.0  (half-to-even, not half-away-from-zero)
//   3. round(1.5) == 2.0  (half-to-even)
//   4. round(2.5) == 2.0  (half-to-even)
//   5. round(-0.5) == 0.0 (half-to-even: round toward even, which is 0)
//
// Compile standalone:
//   c++ -std=c++17 -Ithird_party/boost_math draken/core/_boost_math_smoke.cpp -o /tmp/boost_smoke && /tmp/boost_smoke
//
// This file is NOT a Python extension.  It is built manually in dev/vendor_boost_math.py
// and as part of the draken test suite.

#include <boost/math/special_functions/round.hpp>
#include <cassert>
#include <cstdio>

int main() {
    // Half-to-even: 0.5 rounds to 0 (nearest even)
    assert(boost::math::round(0.5)  == 0.0 && "round(0.5) must be 0 (half-to-even)");
    // Half-to-even: 1.5 rounds to 2 (nearest even)
    assert(boost::math::round(1.5)  == 2.0 && "round(1.5) must be 2 (half-to-even)");
    // Half-to-even: 2.5 rounds to 2 (nearest even)
    assert(boost::math::round(2.5)  == 2.0 && "round(2.5) must be 2 (half-to-even)");
    // Half-to-even: -0.5 rounds to 0 (nearest even)
    assert(boost::math::round(-0.5) == 0.0 && "round(-0.5) must be 0 (half-to-even)");
    // Half-to-even: -1.5 rounds to -2 (nearest even)
    assert(boost::math::round(-1.5) == -2.0 && "round(-1.5) must be -2 (half-to-even)");

    // Ordinary rounding (no half case)
    assert(boost::math::round(1.4)  == 1.0);
    assert(boost::math::round(1.6)  == 2.0);
    assert(boost::math::round(-1.4) == -1.0);
    assert(boost::math::round(-1.6) == -2.0);

    std::printf("boost::math::round smoke test: PASS\n");
    return 0;
}
