#pragma once
// Shared byte-DFA walk over a compiled blob (vector_dfa_compile.pyx format:
// u8 version=1, u8 flags[bit0 anchored_start, bit1 anchored_end], u16 num_states,
// accept_bitmap, then num_states*256 u16 transitions). Used by draken_rlike
// (function_rlike.cpp) and the length-adaptive LIKE path (function_kernels.cpp)
// — one implementation, no duplication (.claude/CLAUDE.md §2).
//
// Returns 1/0 for match/no-match, or -1 for a malformed blob (blob is entirely
// plan-time-compiler-produced, so -1 means compiler/kernel format drift — the
// caller fails loud rather than guessing).

#include <cstdint>
#include <cstddef>

namespace draken_dfa {

inline int match(const uint8_t* blob, size_t blob_len,
                 const uint8_t* sdata, uint32_t slen) {
    if (blob_len < 4) return -1;
    const uint8_t version = blob[0];
    const uint8_t flags = blob[1];
    const uint16_t num_states =
        static_cast<uint16_t>(blob[2]) | (static_cast<uint16_t>(blob[3]) << 8);
    if (version != 1) return -1;
    const bool has_end = (flags & 0x02) != 0;
    const size_t accept_bitmap_len = (static_cast<size_t>(num_states) + 7) / 8;
    const size_t expected_len =
        4 + accept_bitmap_len + static_cast<size_t>(num_states) * 256 * 2;
    if (blob_len != expected_len) return -1;

    const uint8_t* accept_bitmap = blob + 4;
    const uint16_t* table =
        reinterpret_cast<const uint16_t*>(blob + 4 + accept_bitmap_len);

    int state = 0;
    if (!has_end && ((accept_bitmap[0] >> 0) & 1u)) return 1;
    for (uint32_t i = 0; i < slen; ++i) {
        const uint16_t next_state = table[static_cast<size_t>(state) * 256 + sdata[i]];
        if (next_state == 0xFFFFu) return 0;
        state = static_cast<int>(next_state);
        if (!has_end && ((accept_bitmap[state >> 3] >> (state & 7)) & 1u)) return 1;
    }
    if (has_end) return ((accept_bitmap[state >> 3] >> (state & 7)) & 1u) ? 1 : 0;
    return 0;
}

}  // namespace draken_dfa
