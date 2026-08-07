// probe_version() is the hinge of the migration chain: a build reads two
// versions, so stepping a file forward several versions means running
// successively newer retained binaries — and that only works if ANY build can
// identify ANY file. These tests pin that guarantee.

#include <cstring>
#include <vector>

#include "harness.h"
#include "skene/format.h"
#include "skene/probe.h"

using namespace skene;

static std::vector<uint8_t> make_head(uint32_t magic, uint16_t version) {
    std::vector<uint8_t> b(kFileHeadBytes, 0);
    std::memcpy(b.data(), &magic, sizeof(magic));
    std::memcpy(b.data() + 4, &version, sizeof(version));
    return b;
}

static void test_reads_current_version() {
    auto head = make_head(kMagic, kVersion);
    uint16_t v = 0;
    Status st = probe_version(head.data(), head.size(), &v);
    CHECK(st.is_ok());
    CHECK_EQ(v, kVersion);
}

static void test_reports_versions_this_build_cannot_read() {
    // The whole point. A file from the future, and one from long before this
    // build's window, must both PROBE CLEANLY — otherwise an operator cannot
    // tell which retained binary to reach for, and the migration chain is
    // unusable exactly when it is needed.
    for (uint16_t v : {uint16_t{1}, uint16_t{7}, uint16_t{9999}, uint16_t{65535}}) {
        auto head = make_head(kMagic, v);
        uint16_t got = 0;
        Status st = probe_version(head.data(), head.size(), &got);
        CHECK(st.is_ok());
        CHECK_EQ(got, v);
    }

    // ... and unsupported versions are still correctly reported as unsupported
    // by the reader's own window. Probing is identification, not admission.
    CHECK(!version_is_supported(uint16_t{9999}));
    CHECK(version_is_supported(kVersion));
}

static void test_rejects_non_skene() {
    auto head = make_head(0xDEADBEEFu, kVersion);
    uint16_t v = 0xFFFFu;
    Status st = probe_version(head.data(), head.size(), &v);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kNotSkene);

    // A Parquet file, which is the object most likely to be handed to us by
    // mistake, since both live in the same buckets under the same manifests.
    std::vector<uint8_t> parquet = {'P', 'A', 'R', '1', 0, 0, 0, 0};
    st = probe_version(parquet.data(), parquet.size(), &v);
    CHECK(!st.is_ok());
    CHECK(st.code() == Code::kNotSkene);
}

static void test_rejects_truncated() {
    auto head = make_head(kMagic, kVersion);
    uint16_t v = 0;
    for (size_t n = 0; n < kProbeBytes; ++n) {
        Status st = probe_version(head.data(), n, &v);
        CHECK(!st.is_ok());
        CHECK(st.code() == Code::kTruncated);
    }
    // Exactly kProbeBytes is enough — a caller must not have to read more.
    CHECK(probe_version(head.data(), kProbeBytes, &v).is_ok());
}

static void test_probe_layout_is_frozen() {
    // probe_version reads magic at 0 and version at 4, and that must remain
    // true for every version ever written. If a change moves either field, the
    // migration chain silently misidentifies files.
    CHECK_EQ(offsetof(FileHead, magic), size_t{0});
    CHECK_EQ(offsetof(FileHead, version), size_t{4});
    CHECK_EQ(kProbeBytes, size_t{8});
}

static void test_version_window_is_exactly_two_wide() {
    // Ruled: a build reads what it writes and the version before it; anything
    // older is migrated, not read.
    if (kVersion > 1u) {
        CHECK_EQ(kMinReadVersion, static_cast<uint16_t>(kVersion - 1u));
        CHECK(version_is_migratable(static_cast<uint16_t>(kVersion - 1u)));
        CHECK(!version_is_supported(static_cast<uint16_t>(kVersion - 2u)));
    } else {
        // v1: there is no predecessor, so the window is a single version and
        // nothing is migratable yet.
        CHECK_EQ(kMinReadVersion, uint16_t{1});
        CHECK(!version_is_migratable(uint16_t{0}));
    }
    CHECK(!version_is_supported(static_cast<uint16_t>(kVersion + 1u)));
}

static void test_supported_versions_string_is_useful() {
    const char* s = supported_versions_string();
    CHECK(s != nullptr);
    // It must actually name numbers — this string is what an operator reads to
    // route a file to a binary, so an empty or generic message is a bug.
    CHECK(std::strstr(s, "writes v") != nullptr);
    CHECK(std::strstr(s, "reads v") != nullptr);
}

static void test_migration_advice_names_the_next_hop() {
    char buf[512];

    // A file this build can read needs no chain.
    migration_advice(kVersion, buf, sizeof(buf));
    CHECK(std::strstr(buf, "no migration needed") != nullptr);

    // A file from the future cannot be helped by any retained OLDER binary —
    // older binaries read older versions. Saying "migrate" here would send an
    // operator down a chain that does not exist.
    migration_advice(static_cast<uint16_t>(kVersion + 5u), buf, sizeof(buf));
    CHECK(std::strstr(buf, "NEWER") != nullptr);
    CHECK(std::strstr(buf, "upgrade") != nullptr);

    // A file older than the window: each binary vX migrates (X-1) -> X, so the
    // advice must name v(file_version + 1) as the FIRST hop — that is the
    // binary the operator has to fetch right now.
    if (kVersion >= 1u) {
        const uint16_t old_file = 1u;
        migration_advice(old_file, buf, sizeof(buf));
        if (!version_is_supported(old_file)) {
            char expect_first_hop[32];
            std::snprintf(expect_first_hop, sizeof(expect_first_hop), "writes v%u",
                          static_cast<unsigned>(old_file) + 1u);
            CHECK(std::strstr(buf, expect_first_hop) != nullptr);
            CHECK(std::strstr(buf, "one version at a time") != nullptr);
            // And it must be explicit that no shortcut exists, because the
            // obvious wrong assumption is that the newest binary reads everything.
            CHECK(std::strstr(buf, "No single binary") != nullptr);
        }
    }

    // Never overruns, and always terminates, even into a tiny buffer.
    char tiny[8];
    migration_advice(1u, tiny, sizeof(tiny));
    CHECK(tiny[sizeof(tiny) - 1] == '\0');
    migration_advice(1u, nullptr, 0);  // must not crash
}

int main() {
    test_migration_advice_names_the_next_hop();
    test_reads_current_version();
    test_reports_versions_this_build_cannot_read();
    test_rejects_non_skene();
    test_rejects_truncated();
    test_probe_layout_is_frozen();
    test_version_window_is_exactly_two_wide();
    test_supported_versions_string_is_useful();
    return skene_test::summary("test_probe");
}
