// Standalone proof for MediusMap before it goes anywhere near the sink.
// The failure mode it must exclude is SILENT: a wrap-around tag-padding bug
// yields duplicate groups for the same key, which no timing test would catch.
#include <cstdio>
#include <cstdint>
#include <random>
#include <unordered_map>
#include <vector>
#include "medius.hpp"

using opteryx::medius::MediusMap;
using opteryx::medius::MediusInsert;

static int failures = 0;
static void check(bool ok, const char* what) {
    if (!ok) { std::printf("  FAIL %s\n", what); ++failures; }
}

int main() {
    // 1. Identity: same key must always return the same id, never a new one.
    {
        MediusMap<512> m;
        std::mt19937_64 rng(12345);
        std::unordered_map<uint64_t, int64_t> oracle;
        int64_t next = 0;
        bool hit_full = false;
        for (int i = 0; i < 5000 && !hit_full; ++i) {
            uint64_t k = rng() | 1ULL;
            int64_t got = -1;
            auto r = m.find_or_insert_id(k, next, got);
            if (r == MediusInsert::kFull) { hit_full = true; break; }
            auto it = oracle.find(k);
            if (it == oracle.end()) {
                check(r == MediusInsert::kInserted, "new key reported as inserted");
                check(got == next, "new key got the id we offered");
                oracle[k] = next; ++next;
            } else {
                check(r == MediusInsert::kFound, "existing key reported as found");
                check(got == it->second, "existing key returned its ORIGINAL id");
            }
        }
        check(m.size() == oracle.size(), "size matches distinct count");
        for (auto& kv : oracle) {
            int64_t got = -1;
            check(m.lookup_fast(kv.first, got) && got == kv.second, "lookup_fast agrees");
        }
        std::printf("  identity: %zu distinct keys stored\n", oracle.size());
    }

    // 2. Wrap-around: keys deliberately landing in the LAST group, where the
    //    control padding is the only thing preventing a duplicate.
    {
        MediusMap<512> m;
        std::vector<uint64_t> keys;
        for (uint64_t j = 0; j < 200; ++j) keys.push_back((511ULL - (j % 16)) | (j << 32));
        std::unordered_map<uint64_t, int64_t> oracle;
        int64_t next = 0;
        for (uint64_t k : keys) {
            int64_t got = -1;
            auto r = m.find_or_insert_id(k, next, got);
            if (r == MediusInsert::kFull) break;
            if (r == MediusInsert::kInserted) { oracle[k] = next; ++next; }
        }
        for (auto& kv : oracle) {   // every key must still resolve to its own id
            int64_t got = -1;
            check(m.lookup_fast(kv.first, got) && got == kv.second, "wrap-around key stable");
        }
        std::printf("  wrap-around: %zu keys in the tail group, all stable\n", oracle.size());
    }

    // 3. Bounded: must report kFull at the threshold and NEVER exceed it.
    {
        MediusMap<512> m;
        int64_t next = 0; bool saw_full = false;
        for (uint64_t k = 1; k <= 5000; ++k) {
            int64_t got = -1;
            if (m.find_or_insert_id(k * 0x9E3779B97F4A7C15ULL, next, got) == MediusInsert::kFull) {
                saw_full = true; break;
            }
            ++next;
        }
        check(saw_full, "reports kFull rather than overflowing");
        check(m.size() <= m.threshold(), "never exceeds threshold");
        std::printf("  bounded: stopped at %zu (threshold %zu, capacity %zu)\n",
                    m.size(), m.threshold(), m.capacity());
    }

    // 4. Promotion: drain_into must preserve every (key -> id) exactly.
    {
        MediusMap<512> m;
        std::unordered_map<uint64_t, int64_t> oracle;
        int64_t next = 0;
        for (uint64_t k = 1; k <= 300; ++k) {
            int64_t got = -1;
            uint64_t key = k * 0xD6E8FEB86659FD93ULL;
            if (m.find_or_insert_id(key, next, got) == MediusInsert::kInserted) {
                oracle[key] = next; ++next;
            }
        }
        opteryx::carchar::CarcharIndex target;
        m.drain_into(target);
        check(target.size() == oracle.size(), "drain preserves entry count");
        for (auto& kv : oracle) {
            int64_t got = -1;
            check(target.lookup(kv.first, got) && got == kv.second, "drain preserves id");
        }
        std::printf("  promotion: %zu entries drained intact\n", oracle.size());
    }

    std::printf(failures == 0 ? "ALL PASS\n" : "%d FAILURES\n", failures);
    return failures == 0 ? 0 : 1;
}
