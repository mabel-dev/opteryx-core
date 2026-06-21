#include "structural_scan.hpp"

namespace rugo::_jsonl {

std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool masked) {

    std::vector<MarkerPosition> result;
    if (length == 0) return result;

    // Object-style JSON runs ~1 structural marker per 3 bytes; reserve for that
    // density so the vector does not reallocate (each realloc copies the whole grown
    // vector mid-scan).
    result.reserve(length / 3);

    const uint8_t* lut = structural_lut();
    auto emit = [&](uint32_t pos, uint8_t ch) {
        result.push_back(MarkerPosition(pos, static_cast<MarkerType>(lut[ch] - 1)));
    };
    // Unmasked is the default (fast); the masked scan drops in-string structurals but costs
    // ~1.4× scan, so it's only chosen for high in-string density (see sample_instring_density).
    if (masked) scan_structural_masked(data, length, emit);
    else        scan_structural(data, length, emit);
    return result;
}

}  // namespace rugo::_jsonl
