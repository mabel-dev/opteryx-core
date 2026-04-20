#ifndef PCG_PYHELPERS_HPP_INCLUDED
#define PCG_PYHELPERS_HPP_INCLUDED 1

#include "pcg_extras.hpp"
#include <cstdint>

inline uint64_t static_arbitrary_seed() {
    return pcg_extras::static_arbitrary_seed<uint64_t>::value;
}

#endif // PCG_PYHELPERS_HPP_INCLUDED
