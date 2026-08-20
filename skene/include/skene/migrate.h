#pragma once
// skene/migrate.h — step a file forward exactly ONE format version.
//
// The version-support contract (format.h §version support window): a build
// reads the version it writes and the one before it, and MIGRATES the older to
// the newer. A file more than one version behind is stepped forward by running
// successively newer retained binaries, one hop each. This is that hop.
//
// Migration is a REWRITE, not a byte transform: each row group is read back
// through the retained older reader into draken vectors — which round-trip
// losslessly, that being the format's founding property — and written by the
// current writer. Whatever the current writer does (value ordering, slot
// lanes, codec stacking, statistics) the migrated file gets; the caller picks
// the posture exactly as any writer caller does.

#include <cstddef>
#include <vector>

#include "skene/status.h"
#include "skene/writer.h"

namespace skene {

// Rewrites `file` (which must be version kVersion - 1) as a version-kVersion
// file into `out`, one row group per source row group.
//
// Provenance is CARRIED, not reissued: file_uuid, created_at_unix_us and the
// original writer_tag come from the source file — the data's identity did not
// change, its encoding did. Any of those set on `posture` is rejected: two
// sources of truth for provenance is one too many. Everything else on
// `posture` (codec, read_acceleration, cluster_keys, bloom settings) is the
// caller's choice and is applied — including cluster_keys, which the writer
// verifies against the actual rows as always.
//
// A file already at kVersion is refused (there is nothing to migrate, and
// silently copying it would misreport what happened). A file below
// kVersion - 1 is refused with the multi-hop advice.
Status migrate_file(const void* file, size_t file_bytes,
                    const WriteOptions& posture, std::vector<uint8_t>* out);

}  // namespace skene
