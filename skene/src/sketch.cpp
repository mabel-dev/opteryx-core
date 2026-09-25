#include "sketch.h"

#include <vector>

// draken — imported, never copied.
#include "core/string_slot.h"
#include "ops/hash.h"

namespace skene {

// format.h restates draken's family values so it can stay plain POD; they are
// one fact, so they are checked to agree.
static_assert(kSketchFamilyXxh3Value ==
                  static_cast<uint8_t>(draken::KmvHashFamily::kXxh3ValueBytes),
              "format.h sketch family drifted from draken::KmvHashFamily");
static_assert(kSketchFamilyDrakenVectorHash ==
                  static_cast<uint8_t>(draken::KmvHashFamily::kDrakenVectorHash),
              "format.h sketch family drifted from draken::KmvHashFamily");

bool sketch_supported(const DrakenVector& v) {
    const unsigned idx = static_cast<unsigned>(v.type);
    if (idx >= OpsTable::kSize || g_ops_table().entries[idx].hash == nullptr)
        return false;
    if (draken_type_is_string_storage(v.type)) {
        const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(v.data);
        if (sa == nullptr || sa->payloads_elided) return false;
    }
    return true;
}

Status sketch_add_rows(const DrakenVector& v, FileSketch* sketch) {
    if (!sketch_supported(v))
        return Status(Code::kUnsupportedType,
                      "sketch_add_rows: this column's type has no Vector.hash() kernel "
                      "or its string payloads are elided");
    if (v.length == 0) return Status::ok();
    std::vector<uint64_t> hashes(v.length);
    draken_hash(v, hashes.data(), v.length);
    for (uint64_t hash : hashes) sketch->add(hash);
    return Status::ok();
}

}  // namespace skene
