#include "structural_scan.hpp"

namespace rugo::_jsonl {

std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool /*use_simd*/) {

    std::vector<MarkerPosition> result;
    if (length == 0) return result;

    // Object-style JSON runs ~1 structural marker per 3 bytes; reserve for that
    // density so the vector does not reallocate (each realloc copies the whole grown
    // vector mid-scan).
    result.reserve(length / 3);

    const uint8_t* lut = structural_lut();
    // Unmasked is the default: masking (scan_structural_masked) costs ~1.4× scan and only
    // nets out above ~40% in-string density, so it's reserved for an adaptive high-density
    // path. Escaped-quote correctness is handled cheaply in the document-map FSM instead.
    scan_structural(data, length, [&](uint32_t pos, uint8_t ch) {
        result.push_back(MarkerPosition(pos, static_cast<MarkerType>(lut[ch] - 1)));
    });
    return result;
}

}  // namespace rugo::_jsonl
