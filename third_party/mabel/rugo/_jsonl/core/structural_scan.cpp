#include "structural_scan.hpp"
#include <algorithm>
#include <cstring>
#include <cstdint>

// Forward declaration of SIMD helper (C++ linkage)
std::vector<size_t> simd_find_all(const char* data, size_t length, char target = '\n');

namespace rugo::_jsonl {

std::vector<MarkerPosition> scan_structural_markers(
    const uint8_t* data,
    size_t length,
    bool use_simd) {

    if (length == 0) {
        return {};
    }

    std::vector<MarkerPosition> result;
    result.reserve(length / 4);  // Rough estimate: ~1 marker per 4 bytes

    // Find all positions of each marker character using SIMD
    const char* char_data = reinterpret_cast<const char*>(data);

    // Array of marker characters and their types
    struct MarkerChar {
        char ch;
        MarkerType type;
    };

    static const MarkerChar markers[] = {
        {'{', MarkerType::BRACE_OPEN},
        {'}', MarkerType::BRACE_CLOSE},
        {'[', MarkerType::BRACKET_OPEN},
        {']', MarkerType::BRACKET_CLOSE},
        {':', MarkerType::COLON},
        {',', MarkerType::COMMA},
        {'"', MarkerType::QUOTE},
        {'\\', MarkerType::BACKSLASH},
        {'\n', MarkerType::NEWLINE},
    };

    // For each marker type, find all occurrences
    for (const auto& marker : markers) {
        auto positions = simd_find_all(char_data, length, marker.ch);
        for (size_t pos : positions) {
            result.push_back(MarkerPosition(static_cast<uint32_t>(pos), marker.type));
        }
    }

    // Sort by position
    std::sort(result.begin(), result.end(), [](const MarkerPosition& a, const MarkerPosition& b) {
        return a.position < b.position;
    });

    return result;
}

}  // namespace rugo::_jsonl
