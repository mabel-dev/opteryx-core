#include "skene/probe.h"

#include <cstdio>
#include <cstring>

#include "skene/format.h"

namespace skene {

Status probe_version(const void* head, size_t head_bytes, uint16_t* out_version) {
    if (out_version == nullptr)
        return Status(Code::kMalformed, "probe_version: out_version is null");

    if (head == nullptr || head_bytes < kProbeBytes) {
        char msg[128];
        std::snprintf(msg, sizeof(msg),
                      "probe_version: need at least %zu bytes, got %zu",
                      kProbeBytes, head_bytes);
        return Status(Code::kTruncated, msg);
    }

    const uint8_t* p = static_cast<const uint8_t*>(head);

    // Magic first, always. An unrelated object is rejected before its bytes are
    // given any meaning at all.
    uint32_t magic = 0;
    std::memcpy(&magic, p, sizeof(magic));
    if (magic != kMagic)
        return Status(Code::kNotSkene,
                      "probe_version: not a .skene file (magic mismatch)");

    uint16_t version = 0;
    std::memcpy(&version, p + sizeof(magic), sizeof(version));

    // Deliberately NOT validated against this build's window. Reporting the
    // version of a file we cannot read is the entire purpose: it is how an
    // operator learns which retained binary to run.
    *out_version = version;
    return Status::ok();
}

void migration_advice(uint16_t file_version, char* out, size_t out_bytes) {
    if (out == nullptr || out_bytes == 0) return;

    if (version_is_supported(file_version)) {
        std::snprintf(out, out_bytes,
                      "file is v%u; this build reads v%u..v%u — no migration needed",
                      static_cast<unsigned>(file_version),
                      static_cast<unsigned>(kMinReadVersion),
                      static_cast<unsigned>(kVersion));
        return;
    }

    if (file_version > kVersion) {
        // Written by a newer build. No retained OLDER binary can help — older
        // binaries read older versions, not newer ones. The only move is up.
        std::snprintf(out, out_bytes,
                      "file is v%u but this build writes v%u and reads v%u..v%u. "
                      "The file is NEWER than this build — upgrade skene; no "
                      "retained older binary can read it.",
                      static_cast<unsigned>(file_version),
                      static_cast<unsigned>(kVersion),
                      static_cast<unsigned>(kMinReadVersion),
                      static_cast<unsigned>(kVersion));
        return;
    }

    // Older than this build's window. Each binary vX migrates (X-1) -> X, so
    // the file needs v(file_version+1) … v(kVersion) run in order. Name the
    // first hop explicitly — that is the one the operator has to fetch now.
    const unsigned first_hop = static_cast<unsigned>(file_version) + 1u;
    std::snprintf(out, out_bytes,
                  "file is v%u but this build reads v%u..v%u. Migrate it forward "
                  "one version at a time using the retained skene binaries: run "
                  "the binary that writes v%u first, then each version up to v%u "
                  "(%u migration step%s in total). No single binary can do this "
                  "in one hop.",
                  static_cast<unsigned>(file_version),
                  static_cast<unsigned>(kMinReadVersion),
                  static_cast<unsigned>(kVersion),
                  first_hop,
                  static_cast<unsigned>(kVersion),
                  static_cast<unsigned>(kVersion) - static_cast<unsigned>(file_version),
                  (static_cast<unsigned>(kVersion) - static_cast<unsigned>(file_version)) == 1u ? "" : "s");
}

const char* supported_versions_string() {
    static char buffer[64];
    static bool initialised = false;
    if (!initialised) {
        std::snprintf(buffer, sizeof(buffer),
                      "skene format: writes v%u, reads v%u..v%u",
                      static_cast<unsigned>(kVersion),
                      static_cast<unsigned>(kMinReadVersion),
                      static_cast<unsigned>(kVersion));
        initialised = true;
    }
    return buffer;
}

}  // namespace skene
