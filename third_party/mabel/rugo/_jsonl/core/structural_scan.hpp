#ifndef _JSONL_STRUCTURAL_SCAN_HPP_
#define _JSONL_STRUCTURAL_SCAN_HPP_

#include <vector>
#include <cstdint>
#include "markers.hpp"

namespace rugo::_jsonl {

// SIMD-assisted scan for all 9 marker character positions in a buffer
// Returns sorted array of (position, marker_type) tuples
std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool use_simd = true
);

}  // namespace rugo::_jsonl

#endif  // _JSONL_STRUCTURAL_SCAN_HPP_
