#ifndef PCG_PYHELPERS_HPP_INCLUDED
#define PCG_PYHELPERS_HPP_INCLUDED 1

#include <cstdint>
#include <random>

// OS-entropy seed for the Python-side utility RNG. This used to expose
// pcg_extras::static_arbitrary_seed, which is a compile-time constant (an FNV
// hash of __DATE__/__TIME__): every process of the same wheel replayed an
// identical stream, so "random" file names collided across fresh workers and
// silently overwrote committed data files (opteryx.app#164). Same seeding
// posture as draken's execution kernels (fn_thread_rng/fse_thread_rng).
inline uint64_t nondeterministic_seed() {
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) ^ rd();
}

#endif // PCG_PYHELPERS_HPP_INCLUDED
