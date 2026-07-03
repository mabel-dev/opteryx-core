#include "jsonl_reader.hpp"
#include "volnitsky.h"     // SPIKE: raw prefilter
#include <cstring>

namespace rugo::_jsonl {

PrefilterResult volnitsky_prefilter(
    const uint8_t* buffer, size_t length,
    const uint8_t* needle, size_t needle_len) {
    PrefilterResult r;
    if (length < needle_len || needle_len < 2) return r;
    VolnitskyTable* t = volnitsky_alloc();
    volnitsky_build(t, needle, needle_len);
    r.candidates.reserve(length / 16);

    // Single whole-buffer Volnitsky pass: the bigram table skips ~needle_len-1 bytes across
    // every non-matching window, so a rare needle leaps over whole records. On a hit, copy
    // the enclosing line and jump past it (handles dedup of multiple hits in one record).
    size_t last_end = 0; bool any = false;
    for (size_t p = needle_len - 1; p < length; ) {
        const uint16_t h = (static_cast<uint16_t>(buffer[p - 1]) << 8) | buffer[p];
        const uint16_t k = t->entries[h];
        if (!k) { p += needle_len - 1; continue; }
        const size_t hs = p - k;
        if (hs + needle_len <= length && std::memcmp(buffer + hs, needle, needle_len) == 0
            && (!any || hs >= last_end)) {
            size_t ls = hs; while (ls > 0 && buffer[ls - 1] != '\n') --ls;
            size_t le = hs; while (le < length && buffer[le] != '\n') ++le;
            r.candidates.insert(r.candidates.end(), buffer + ls, buffer + le);
            r.candidates.push_back('\n');
            ++r.matched_records;
            last_end = le; any = true;
            p = (le + 1 > needle_len - 1) ? le + 1 : needle_len - 1;  // skip past the matched line
            continue;
        }
        p += 1;
    }
    volnitsky_free(t);
    return r;
}

}  // namespace rugo::_jsonl
